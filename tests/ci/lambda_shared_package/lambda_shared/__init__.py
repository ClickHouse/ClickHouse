"""The shared code and types for all our CI lambdas
It exists as __init__.py and lambda_shared/__init__.py to work both in local and venv"""

import json
import logging
import time
from collections import namedtuple
from typing import Any, Dict, Iterable, List, Optional

import boto3  # type: ignore
import requests

RUNNER_TYPE_LABELS = [
    "builder",
    "func-tester",
    "func-tester-aarch64",
    "fuzzer-unit-tester",
    "limited-tester",
    "stress-tester",
    "style-checker",
    "style-checker-aarch64",
    # private runners
    "private-builder",
    "private-clickpipes",
    "private-func-tester",
    "private-fuzzer-unit-tester",
    "private-stress-tester",
    "private-style-checker",
]


### VENDORING
def get_parameter_from_ssm(
    name: str, decrypt: bool = True, client: Optional[Any] = None
) -> str:
    if not client:
        client = boto3.client("ssm", region_name="us-east-1")
    return client.get_parameter(Name=name, WithDecryption=decrypt)[  # type: ignore
        "Parameter"
    ]["Value"]


class CHException(Exception):
    pass


class InsertException(CHException):
    pass


class ClickHouseHelper:
    def __init__(
        self,
        url: str,
        user: Optional[str] = None,
        password: Optional[str] = None,
    ):
        self.url = url
        self.auth = {}
        if user:
            self.auth["X-ClickHouse-User"] = user
        if password:
            self.auth["X-ClickHouse-Key"] = password

    @staticmethod
    def _insert_json_str_info_impl(
        url: str, auth: Dict[str, str], db: str, table: str, json_str: str
    ) -> None:
        params = {
            "database": db,
            "query": f"INSERT INTO {table} FORMAT JSONEachRow",
            "date_time_input_format": "best_effort",
            "send_logs_level": "warning",
        }

        for i in range(5):
            try:
                response = requests.post(
                    url, params=params, data=json_str, headers=auth
                )
            except Exception as e:
                error = f"Received exception while sending data to {url} on {i} attempt: {e}"
                logging.warning(error)
                continue

            logging.info("Response content '%s'", response.content)

            if response.ok:
                break

            error = (
                "Cannot insert data into clickhouse at try "
                + str(i)
                + ": HTTP code "
                + str(response.status_code)
                + ": '"
                + str(response.text)
                + "'"
            )

            if response.status_code >= 500:
                # A retriable error
                time.sleep(1)
                continue

            logging.info(
                "Request headers '%s', body '%s'",
                response.request.headers,
                response.request.body,
            )

            raise InsertException(error)
        else:
            raise InsertException(error)

    def _insert_json_str_info(self, db: str, table: str, json_str: str) -> None:
        self._insert_json_str_info_impl(self.url, self.auth, db, table, json_str)

    def insert_event_into(
        self, db: str, table: str, event: object, safe: bool = True
    ) -> None:
        event_str = json.dumps(event)
        try:
            self._insert_json_str_info(db, table, event_str)
        except InsertException as e:
            logging.error(
                "Exception happened during inserting data into clickhouse: %s", e
            )
            if not safe:
                raise

    def insert_events_into(
        self, db: str, table: str, events: Iterable[object], safe: bool = True
    ) -> None:
        jsons = []
        for event in events:
            jsons.append(json.dumps(event))

        try:
            self._insert_json_str_info(db, table, ",".join(jsons))
        except InsertException as e:
            logging.error(
                "Exception happened during inserting data into clickhouse: %s", e
            )
            if not safe:
                raise

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

    def select_json_each_row(self, db: str, query: str) -> List[dict]:
        text = self._select_and_get_json_each_row(db, query)
        result = []
        for line in text.split("\n"):
            if line:
                result.append(json.loads(line))
        return result


### Runners

RunnerDescription = namedtuple(
    "RunnerDescription", ["id", "name", "tags", "offline", "busy"]
)
RunnerDescriptions = List[RunnerDescription]


def list_runners(access_token: str) -> RunnerDescriptions:
    headers = {
        "Authorization": f"token {access_token}",
        "Accept": "application/vnd.github.v3+json",
    }
    per_page = 100
    response = requests.get(
        f"https://api.github.com/orgs/ClickHouse/actions/runners?per_page={per_page}",
        headers=headers,
    )
    response.raise_for_status()
    data = response.json()
    total_runners = data["total_count"]
    print("Expected total runners", total_runners)
    runners = data["runners"]

    # round to 0 for 0, 1 for 1..100, but to 2 for 101..200
    total_pages = (total_runners - 1) // per_page + 1

    print("Total pages", total_pages)
    for i in range(2, total_pages + 1):
        response = requests.get(
            "https://api.github.com/orgs/ClickHouse/actions/runners"
            f"?page={i}&per_page={per_page}",
            headers=headers,
        )
        response.raise_for_status()
        data = response.json()
        runners += data["runners"]

    print("Total runners", len(runners))
    result = []
    for runner in runners:
        tags = [tag["name"] for tag in runner["labels"]]
        desc = RunnerDescription(
            id=runner["id"],
            name=runner["name"],
            tags=tags,
            offline=runner["status"] == "offline",
            busy=runner["busy"],
        )
        result.append(desc)

    return result


def cached_value_is_valid(updated_at: float, ttl: float) -> bool:
    "a common function to identify if cachable value is still valid"
    if updated_at == 0:
        return False
    if time.time() - ttl < updated_at:
        return True
    return False
