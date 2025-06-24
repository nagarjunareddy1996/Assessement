import azure.functions as func
import json
import logging
from datetime import datetime, timedelta

# Assume Cosmos DB SDK and ADLS Gen2 SDK are initialized

COSMOS_DB_TTL_FIELD = "ttl" # Cosmos DB Time-to-Live field
ARCHIVAL_THRESHOLD_DAYS = 90 # Records older than 90 days

def main(documents: func.DocumentList, context: func.Context):
    if not documents:
        return

    logging.info(f"Processing {len(documents)} documents from Cosmos DB Change Feed.")

    for doc in documents:
        record = doc.to_dict()
        record_id = record.get("id")
        created_at_str = record.get("createdAt") # Assuming a 'createdAt' timestamp
        
        if not record_id or not created_at_str:
            logging.warning(f"Skipping record due to missing ID or createdAt: {record_id}")
            continue

        try:
            created_at = datetime.fromisoformat(created_at_str.replace("Z", "+00:00")) # Handle Z for UTC
            if datetime.now(created_at.tzinfo) - created_at > timedelta(days=ARCHIVAL_THRESHOLD_DAYS):
                logging.info(f"Archiving record {record_id} to ADLS Gen2.")
                
                # Construct ADLS Gen2 path (e.g., /billing/year/month/day/record_id.json)
                year = created_at.year
                month = created_at.month
                day = created_at.day
                adls_path = f"billing/{year}/{month:02d}/{day:02d}/{record_id}.json"

                # Write record to ADLS Gen2
                # Example: adls_client.upload_data(data=json.dumps(record).encode('utf-8'), path=adls_path)
                logging.info(f"Record {record_id} archived to ADLS Gen2 at {adls_path}.")

                # Mark record for deletion in Cosmos DB by setting TTL to 1 second
                # (Cosmos DB will delete it shortly after)
                # Ensure the record structure allows for this (e.g., patch operation)
                # Example: cosmos_container.patch_item(item=record_id, operations=)
                # Or, if TTL is on a path, set that path to a time value relative to now.
                logging.info(f"Record {record_id} marked for deletion in Cosmos DB.")

            else:
                logging.debug(f"Record {record_id} is recent, no archival needed.")

        except Exception as e:
            logging.error(f"Error processing record {record_id}: {e}")
