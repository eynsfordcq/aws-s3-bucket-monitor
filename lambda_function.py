import os 
import json
import boto3
from datetime import datetime, timezone, timedelta

# CONFIGS
TIMEZONE_OFFSET = int(os.getenv('TIMEZONE_OFFSET', 0))
CUSTOM_TIMEZONE = timezone(timedelta(hours=TIMEZONE_OFFSET))
SNS_TOPIC_ARN = os.getenv('SNS_TOPIC_ARN')


# Initialize S3 and SNS clients
s3 = boto3.client('s3')
sns = boto3.client('sns')


def lambda_handler(event, context):
    
    config = load_config("./config.json")
    if not config:
        return
    
    print(f"Start checking: {datetime.now(CUSTOM_TIMEZONE)}")
    print(f"Config: {config}")

    alerts = []
    for bucket_name, checks in config.items():
        for check in checks:
            prefix = check['prefix']
            timedelta_days = check['timedelta_days']
            if not check_files_uploaded(bucket_name, prefix, timedelta_days):
                alerts.append({
                    "bucket_name": bucket_name,
                    "prefix": prefix,
                    "timedelta_days": timedelta_days
                })

    if alerts:
        send_sns_alert(alerts)
    
    print(f"Done checking: {datetime.now(CUSTOM_TIMEZONE)}")


def load_config(file_path):
    try:
        with open(file_path, 'r') as config_file:
            return json.load(config_file)
    except Exception as e:
        print(f"Error reading config file: {e}")
        return None 


def check_files_uploaded(bucket_name, prefix, timedelta_days):
    try:
        cutoff_date = datetime.now(CUSTOM_TIMEZONE) - timedelta(days=timedelta_days)
        print(
            f"check_files_uploaded(): "
            f"bucket_name: {bucket_name} "
            f"prefix: {prefix} "
            f"timedelta_days: {timedelta_days} "
            f"cutoff_date: {cutoff_date}"
        )

        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        
        # if empty, siao liao
        if not response.get('Contents'):
            print(f"check_files_uploaded(): empty directory")
            return False

        for obj in response.get('Contents'):
            # skip folder itself 
            if obj.get('Key').endswith('/'):
                continue 

            if obj.get('LastModified') >= cutoff_date:
                print(
                    f"check_files_uploaded(): "
                    f"found file within cutoff date: "
                    f"{obj.get('Key')} "
                )
                return True

        print(f"check_files_uploaded(): all files beyond cutoff date.")
        return False
    
    except Exception as e:
        print(f"Error checking files in s3: {e}")


def send_sns_alert(alerts) -> None:
    
    message = "\nThis alert was sent because there are missing backup files.\n\n"

    for alert in alerts:
        message += f"bucket_name: {alert['bucket_name']}\n"
        message += f"Prefix: {alert['prefix']}\n"
        message += f"Timedelta Days: {alert['timedelta_days']}\n\n"

    try:
        response = sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Message=message,
            Subject="AWS S3 Backup Alert: Missing backup file"
        )
        
        print(
            f"send_sns_alert(): "
            f"message_id: {response['MessageId']} "
        )
    
    except Exception as e:
        print(f"Error sending SNS alert: {e}")