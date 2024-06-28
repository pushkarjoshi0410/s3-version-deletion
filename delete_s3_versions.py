import boto3
import logging
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError
# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger()
# Define the AWS SSO profile name
AWS_PROFILE = 'your-sso-profile' # Replace 'your-sso-profile' with your actual AWS SSO profile name
# Initialize the S3 client with SSO credentials
session = boto3.Session(profile_name=AWS_PROFILE)
s3 = session.client('s3')
def delete_all_versions(bucket_name):
 try:
 logger.info(f'Starting deletion of all versions and delete markers in bucket: {bucket_name}')
paginator = s3.get_paginator('list_object_versions')
 delete_marker_list = []
 version_list = []
for page in paginator.paginate(Bucket=bucket_name):
 if 'Versions' in page:
 for version in page['Versions']:
 version_list.append({
 'Key': version['Key'],
 'VersionId': version['VersionId']
 })
 if 'DeleteMarkers' in page:
 for marker in page['DeleteMarkers']:
 delete_marker_list.append({
 'Key': marker['Key'],
 'VersionId': marker['VersionId']
 })
delete_objects(bucket_name, delete_marker_list)
 delete_objects(bucket_name, version_list)
logger.info(f'All versions and delete markers deleted in bucket: {bucket_name}')
 except NoCredentialsError:
 logger.error('Credentials not available')
 except PartialCredentialsError:
 logger.error('Incomplete credentials provided')
 except ClientError as e:
 logger.error(f'Client error: {e}')
 except Exception as e:
 logger.error(f'An error occurred: {e}')
def delete_objects(bucket_name, objects):
 try:
 if not objects:
 logger.info('No objects to delete')
 return
for i in range(0, len(objects), 1000):
 batch = objects[i:i + 1000]
 s3.delete_objects(Bucket=bucket_name, Delete={'Objects': batch})
 logger.info(f'Deleted {len(batch)} objects from bucket: {bucket_name}')
 except ClientError as e:
 logger.error(f'Failed to delete objects: {e}')
def main():
 bucket_name = 'your-bucket-name' # Replace 'your-bucket-name' with your actual bucket name
 delete_all_versions(bucket_name)
if __name__ == '__main__':
 main()
