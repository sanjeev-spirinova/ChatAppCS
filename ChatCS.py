import vertexai
from vertexai.preview.generative_models import GenerativeModel
import google.cloud.bigquery as bigquery
from google.cloud import storage



PROJECT_ID = "sincere-mission-410907"
LOCATION = "us-central1"
BUCKET_NAME = "bkt-gemini-sentiment"

vertexai.init(project=PROJECT_ID, location=LOCATION)

def generate_content(question,content):
  model = GenerativeModel("gemini-pro")
  responses = model.generate_content(
    [question,
    content],
    generation_config={
        "max_output_tokens": 256,
        "temperature": 0.1,
        "top_p": 0.9,
        "top_k": 40
    },
  )
  return responses.candidates[0].content.parts[0].text




def generate_and_load(event,context):
  filename = event['name']

  storage_client = storage.Client()

  bucket = storage_client.bucket(BUCKET_NAME)

  blob = bucket.blob(filename)
  content = blob.download_as_text()
  question = "Customer sentiment in below conversation with reason in format Customer Sentiment: & and in newline reason. Use only the information from the conversation."
  
  generated_response = generate_content(question,content)
  
  sentiment = generated_response.split('\n\n')[0].split(':')[1]
  reason = generated_response.split('\n\n')[1].split(':')[1]
##Bigquery Client is created and connected
  client = bigquery.Client()

  dataset_id = 'customer_behavior'
  table_id = 'Sentiment_ticket_reason'
  table_ref = client.dataset(dataset_id).table(table_id)
  table = client.get_table(table_ref)

  rows_to_insert = [ {'ticket':filename, 'Sentiment': sentiment, 'Reason': reason}]

  errors = client.insert_rows(table, rows_to_insert)

  if errors == []:
      print("New rows have been added.")
  else:
      print("Encountered errors while inserting rows: {}".format(errors))
