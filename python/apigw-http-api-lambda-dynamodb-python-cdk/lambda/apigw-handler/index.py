# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: MIT-0

from aws_xray_sdk.core import xray_recorder
from aws_xray_sdk.core import patch_all

patch_all()

import boto3
import os
import json
import logging
import uuid

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb_client = boto3.client("dynamodb")


def log_info(message, **kwargs):
    """Helper function for structured logging"""
    log_entry = {"level": "INFO", "message": message, **kwargs}
    logger.info(json.dumps(log_entry))


def handler(event, context):
    table = os.environ.get("TABLE_NAME")
    log_info("Loaded table name from environment", table_name=table)
    
    if event["body"]:
        item = json.loads(event["body"])
        log_info("Received payload", payload=item)
        year = str(item["year"])
        title = str(item["title"])
        id = str(item["id"])
        dynamodb_client.put_item(
            TableName=table,
            Item={"year": {"N": year}, "title": {"S": title}, "id": {"S": id}},
        )
        message = "Successfully inserted data!"
        log_info("Data inserted successfully", item_id=id)
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"message": message}),
        }
    else:
        log_info("Received request without payload, using default data")
        default_id = str(uuid.uuid4())
        dynamodb_client.put_item(
            TableName=table,
            Item={
                "year": {"N": "2012"},
                "title": {"S": "The Amazing Spider-Man 2"},
                "id": {"S": default_id},
            },
        )
        message = "Successfully inserted data!"
        log_info("Default data inserted successfully", item_id=default_id)
        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps({"message": message}),
        }
