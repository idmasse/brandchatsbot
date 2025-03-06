import json
import os
from datetime import datetime, timedelta
import gspread
from oauth2client.service_account import ServiceAccountCredentials
import logging
import looker_sdk
from openai import OpenAI
from dotenv import load_dotenv

load_dotenv()

# absolute paths for running by plist
script_dir = os.path.dirname(os.path.abspath(__file__))
log_path = os.path.join(script_dir, 'chat_categorization.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename=log_path
)

LOOK_ID = '764'
CREDENTIALS_FILE = os.path.join(script_dir, "gsheet_creds.json")
LAST_PROCESSED_FILE = os.path.join(script_dir, "chat_records.json")
GOOGLE_SHEET_ID = os.getenv('GOOGLE_SHEET_ID')
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
SHEET_TAB_ID = int(os.getenv('SHEET_TAB_ID'))

# init gpt client
client = OpenAI(api_key=OPENAI_API_KEY)

#cat options
MAIN_CATEGORIES = [
    "Doing Business with Flip",
    "MagicOS",
    "Add new fields to MagicOS",
    "Customer Driven",
    "Integrations",
    "Wholesale/fulfillment/beauty"
]

MAGICOS_ISSUES = [
    "How to add products to Fip",
    "Adding Gratis",
    "requested new products to be mapped.",
    "How to update items on Flip",
    "How to get the money out",
    "Incorrect/incosistent numbers - bugs",
    "How does MagicOS work",
    "Adding more users",
    "Pricing Inventory not updating",
    "How do I download a video",
    "MagicOS Language",
    "integration / Self-onboarding",
    "Discounts",
    "Connecting bank account",
    "OOS on Flip",
    "Financial reporting"
]

BUSINESS_ISSUES = [
    "Brand Official Creator Account",
    "Orders",
    "Gratis targeting",
    "Payment Terms",
    "ADS on Flip",
    "Shipping",
    "Returns",
    "Cancelling orders",
    "Content Policy",
    "Gift Feature",
    "Brand Social Profile"
]

# llm system prompt template
SYSTEM_PROMPT = """
You are an AI assistant that categorizes customer support chats for a platform called Flip with a CRM/CMS system called MagicOS. 
Your task is to analyze the content of customer chat conversations and categorize them appropriately.

For each conversation, provide the following:

1. PROBLEM: In one sentence, what was the customer's problem or inquiry?

2. MAIN_CATEGORY: Choose ONE of the following categories that best matches the conversation:
{main_categories}

3. SOLUTION: In one sentence, what was or should be the solution to the customer's problem?

4. MAGICOS_ISSUE: If applicable, choose ONE of the following specific MagicOS issues. If none apply, respond with "N/A":
{magicos_issues}

5. BUSINESS_ISSUE: If applicable, choose ONE of the following specific business issues. If none apply, respond with "N/A":
{business_issues}

Respond in JSON format with the following structure:
{{
  "problem": "Brief description of the problem",
  "main_category": "Selected Main Category",
  "solution": "Brief suggested solution",
  "magicos_issue": "Selected MagicOS Issue or leave blank",
  "business_issue": "Selected Business Issue or leave blank"
}}
"""

def looker_credentials():
    """Initialize and return a Looker SDK instance"""
    try:
        sdk = looker_sdk.init40() 
        logging.info("Looker SDK initialized successfully.")
        return sdk
    except Exception as e:
        logging.error(f"Failed to initialize Looker SDK: {e}")
        raise

def get_look_data(sdk, look_id):
    """Fetch data from Looker and return it as a Python list"""
    try:
        result = sdk.run_look(look_id=look_id, result_format="json")
        data = json.loads(result)
        return data
    except Exception as e:
        logging.error(f"Error fetching data for Look {look_id}: {e}")
        raise

def group_messages_by_brand(messages, hours=24):
    """
    Groups messages by brand that were created within the last 24 hours.
    Expects each message to have a timestamp in the 'brand_chats_core.message_created_at_time' key.
    """
    grouped = {}
    now = datetime.now()
    cutoff = now - timedelta(hours=hours)
    
    for message in messages:
        timestamp_str = message.get('brand_chats_core.message_created_at_time')
        try:
            timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")
        except Exception as e:
            logging.error(f"Error parsing timestamp {timestamp_str}: {e}")
            continue

        if timestamp < cutoff:
            continue

        brand = message.get('brands_core.name', 'Unknown Brand')
        if brand not in grouped:
            grouped[brand] = []
        grouped[brand].append((timestamp, message.get('brand_chats_core.content', '')))
    
    #sort messages for each brand by timestamp (ascending)
    for brand in grouped:
        grouped[brand].sort(key=lambda x: x[0])
    
    return grouped

def categorize_conversation(conversation_text):
    prompt = SYSTEM_PROMPT.format(
        main_categories=MAIN_CATEGORIES,
        magicos_issues=MAGICOS_ISSUES,
        business_issues=BUSINESS_ISSUES
    )
    messages = [
        {"role": "system", "content": prompt},
        {"role": "user", "content": f"Please analyze the following conversation and provide categorization:\n\n{conversation_text}"}
    ]
    try:
        response = client.chat.completions.create(
            model="gpt-4",
            messages=messages,
            temperature=0.2
        )
        result = response.choices[0].message.content.strip()
        return result
    except Exception as e:
        logging.error(f"Error during GPT categorization: {e}")
        print(f"Error during GPT categorization: {e}")  #debug
        return None

def update_google_sheet(row):
    """Append a new row to the Google Sheet with the given row data."""
    try:
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        credentials = ServiceAccountCredentials.from_json_keyfile_name(CREDENTIALS_FILE, scope)
        gs_client = gspread.authorize(credentials)
        sheet = gs_client.open_by_key(GOOGLE_SHEET_ID)
        worksheet = sheet.get_worksheet_by_id(SHEET_TAB_ID)
        worksheet.append_row(row)
        logging.info(f"Successfully appended row to sheet: {row}")
    except Exception as e:
        logging.error(f"Error updating Google Sheet: {e}")

def load_last_processed():
    """Load last processed timestamps from a JSON file."""
    if os.path.exists(LAST_PROCESSED_FILE):
        with open(LAST_PROCESSED_FILE, "r") as f:
            return json.load(f)
    else:
        return {}

def save_last_processed(last_processed):
    """Save last processed timestamps to a JSON file."""
    with open(LAST_PROCESSED_FILE, "w") as f:
        json.dump(last_processed, f)

if __name__ == '__main__':
    #load last processed timestamps
    last_processed = load_last_processed()

    # init Looker sdk and fetch data from the Look
    sdk_instance = looker_credentials()
    look_data = get_look_data(sdk_instance, LOOK_ID)
    
    # group messages by brand for the last 24 hours
    grouped = group_messages_by_brand(look_data, hours=24)
    
    processed_any = False  # flag to track if any conversation is processed
    
    # process each conversation one at a time
    for brand, messages in grouped.items():
        # concat messages with timestamps to form the conversation text
        conversation = "\n".join(f"{ts.strftime('%Y-%m-%d %H:%M:%S')}: {content}" for ts, content in messages)
        # get the latest timestamp from the conversation for Column A
        latest_timestamp = messages[-1][0].strftime('%Y-%m-%d %H:%M:%S')
        
        # check if we've already processed this conversation for the brand
        last_brand_timestamp = last_processed.get(brand)
        if last_brand_timestamp and latest_timestamp <= last_brand_timestamp:
            logging.info(f"Skipping brand {brand}: conversation already processed (last processed at {last_brand_timestamp}).")
            continue  # skip
        
        logging.info(f"Processing conversation for brand: {brand}")
        logging.info("Conversation:")
        logging.info(conversation)
        logging.info("Categorizing conversation...")
        
        categorization_result = categorize_conversation(conversation)
        if categorization_result:
            try:
                analysis = json.loads(categorization_result)
            except json.JSONDecodeError as je:
                logging.error(f"Error parsing JSON from GPT response for brand {brand}: {je}")
                continue
            
            row = [
                latest_timestamp,
                "",
                "",
                brand,
                "Chat",
                analysis.get("problem", ""),
                analysis.get("main_category", ""),
                analysis.get("solution", ""),
                analysis.get("magicos_issue", ""),
                analysis.get("business_issue", "")
            ]
            update_google_sheet(row)
            
            # update last processed timestamp for the brand
            last_processed[brand] = latest_timestamp
            save_last_processed(last_processed)
            
            processed_any = True
        else:
            logging.error(f"Failed to categorize conversation for {brand}.")
    
    if not processed_any:
        logging.info("No new chats processed during this run.")
