import boto3
import os
from botocore.exceptions import ClientError

def delete_all_objects(s3, bucket_name):
    try:
        # Delete all object versions (if versioning is enabled)
        versioning = s3.get_bucket_versioning(Bucket=bucket_name)
        if 'Status' in versioning and versioning['Status'] == 'Enabled':
            print("Bucket has versioning enabled. Deleting all versions...")
            versions = s3.list_object_versions(Bucket=bucket_name)
            while True:
                try:
                    objects_to_delete = []
                    
                    # Handle non-current versions
                    if 'Versions' in versions:
                        for version in versions['Versions']:
                            objects_to_delete.append({
                                'Key': version['Key'],
                                'VersionId': version['VersionId']
                            })
                    
                    # Handle delete markers
                    if 'DeleteMarkers' in versions:
                        for marker in versions['DeleteMarkers']:
                            objects_to_delete.append({
                                'Key': marker['Key'],
                                'VersionId': marker['VersionId']
                            })
                    
                    if objects_to_delete:
                        s3.delete_objects(
                            Bucket=bucket_name,
                            Delete={'Objects': objects_to_delete}
                        )
                        print(f"Deleted {len(objects_to_delete)} object versions")
                    
                    # Check if there are more versions to delete
                    if versions.get('IsTruncated'):
                        kwargs = {
                            'Bucket': bucket_name,
                            'KeyMarker': versions.get('NextKeyMarker'),
                        }
                        if versions.get('NextVersionIdMarker'):
                            kwargs['VersionIdMarker'] = versions.get('NextVersionIdMarker')
                        versions = s3.list_object_versions(**kwargs)
                    else:
                        break
                        
                except ClientError as e:
                    print(f"Error deleting versions: {e}")
                    raise
        
        # Delete remaining current objects
        paginator = s3.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=bucket_name):
            if 'Contents' in page:
                objects_to_delete = [{'Key': obj['Key']} for obj in page['Contents']]
                if objects_to_delete:
                    s3.delete_objects(
                        Bucket=bucket_name,
                        Delete={'Objects': objects_to_delete}
                    )
                    print(f"Deleted {len(objects_to_delete)} current objects")
        
        print(f"All objects and versions deleted from {bucket_name}")
        
    except ClientError as e:
        print(f"Error occurred while deleting objects: {e}")
        raise

def delete_bucket(bucket_name):
    try:
        s3 = boto3.client(
            's3',
            aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
            region_name=os.environ.get('AWS_REGION', 'us-east-1')
        )
        
        # First delete all objects and versions
        delete_all_objects(s3, bucket_name)
        
        # Then delete the bucket
        print(f"Deleting bucket {bucket_name}...")
        s3.delete_bucket(Bucket=bucket_name)
        print(f"Bucket {bucket_name} deleted successfully")
        
    except ClientError as e:
        print(f"Error occurred while deleting the bucket: {e}")
        raise

def main():
    bucket_name = 'weather-dashboard-17972'
    delete_bucket(bucket_name)

if __name__ == "__main__":
    main()
