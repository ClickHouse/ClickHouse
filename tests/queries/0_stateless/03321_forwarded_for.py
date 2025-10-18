#!/usr/bin/env python3
import os
import sys

CURDIR = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, os.path.join(CURDIR, "helpers"))

from pure_http_client import ClickHouseClient

client = ClickHouseClient()

print(client.query("SELECT 1", headers={"X-Forwarded-For": "123.124.125.126"}, with_retries=False))
print(client.query("SELECT 1", headers={"X-Forwarded-For": "123.124.125.126:80"}, with_retries=False))
print(client.query("SELECT 1", headers={"X-Forwarded-For": "[::1.1.1.1]:80"}, with_retries=False))
print(client.query("SELECT 1", headers={"X-Forwarded-For": "::1.1.1.1"}, with_retries=False))
print(client.query("SELECT 1", headers={"X-Forwarded-For": "2a06:de00:50:cafe:10::1001"}, with_retries=False))
print(client.query("SELECT 1", headers={"X-Forwarded-For": "[2a06:de00:50:cafe:10::1001]:80"}, with_retries=False))
print(client.query("SELECT 1", headers={"X-Forwarded-For": "localhost:80"}, with_retries=False))
print(client.query("SELECT 1", headers={"X-Forwarded-For": "localhost"}, with_retries=False))
