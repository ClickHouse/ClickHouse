"""The shared code and types for all our CI lambdas
It exists as __init__.py and lambda_shared/__init__.py to work both in local and venv"""

import json
import logging
import time
from typing import List, Optional

import boto3  # type: ignore
import requests  # type: ignore

RUNNER_TYPE_LABELS = [
    "builder",
    "func-tester",
    "func-tester-aarch64",
    "fuzzer-unit-tester",
    "stress-tester",
    "style-checker",
    "style-checker-aarch64",
]


### VENDORING
def get_parameter_from_ssm(name, decrypt=True, client=None):
    if not client:
        client = boto3.client("ssm", region_name="us-east-1")
    return client.get_parameter(Name=name, WithDecryption=decrypt)["Parameter"]["Value"]


class CHException(Exception):
    pass


class ClickHouseHelper:
    def __init__(
        self,
        url: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
    ):
        self.url = url
        self.auth = {}
        if user:
            self.auth["X-ClickHouse-User"] = user
        if password:
            self.auth["X-ClickHouse-Key"] = password

    def _select_and_get_json_each_row(self, db: str, query: str) -> str:
        params = {
            "database": db,
            "query": query,
            "default_format": "JSONEachRow",
        }
        for i in range(5):
            response = None
            try:
                response = requests.get(self.url, params=params, headers=self.auth)
                response.raise_for_status()
                return response.text  # type: ignore
            except Exception as ex:
                logging.warning("Cannot fetch data with exception %s", str(ex))
                if response:
                    logging.warning("Reponse text %s", response.text)
                time.sleep(0.1 * i)

        raise CHException("Cannot fetch data from clickhouse")

    def select_json_each_row(self, db: str, query: str) -> List[dict]:  # type: ignore
        text = self._select_and_get_json_each_row(db, query)
        result = []
        for line in text.split("\n"):
            if line:
                result.append(json.loads(line))
        return result
