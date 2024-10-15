#!/usr/bin/env python3
import fileinput
import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests

from get_robot_token import get_parameter_from_ssm
from pr_info import PRInfo
from report import TestResults


class CHException(Exception):
    pass


class InsertException(Exception):
    pass


class ClickHouseHelper:
    def __init__(
        self, url: Optional[str] = None, auth: Optional[Dict[str, str]] = None
    ):
        if url is None:
            url = get_parameter_from_ssm("clickhouse-test-stat-url")

        self.url = url
        self.auth = auth or {
            "X-ClickHouse-User": get_parameter_from_ssm("clickhouse-test-stat-login"),
            "X-ClickHouse-Key": get_parameter_from_ssm("clickhouse-test-stat-password"),
        }

    @staticmethod
    def insert_file(
        url: str,
        auth: Optional[Dict[str, str]],
        query: str,
        file: Path,
        additional_options: Optional[Dict[str, str]] = None,
        **kwargs: Any,
    ) -> None:
        params = {
            "query": query,
            "date_time_input_format": "best_effort",
            "send_logs_level": "warning",
        }
        if additional_options:
            for k, v in additional_options.items():
                params[k] = v

        with open(file, "rb") as data_fd:
            ClickHouseHelper._insert_post(
                url, params=params, data=data_fd, headers=auth, **kwargs
            )

    @staticmethod
    def insert_json_str(url, auth, db, table, json_str):
        params = {
            "database": db,
            "query": f"INSERT INTO {table} FORMAT JSONEachRow",
            "date_time_input_format": "best_effort",
            "send_logs_level": "warning",
        }
        ClickHouseHelper._insert_post(url, params=params, data=json_str, headers=auth)

    @staticmethod
    def _insert_post(*args, **kwargs):
        url = ""
        if args:
            url = args[0]
        url = kwargs.get("url", url)
        timeout = kwargs.pop("timeout", 100)

        for i in range(5):
            try:
                response = requests.post(*args, timeout=timeout, **kwargs)
            except Exception as e:
                error = f"Received exception while sending data to {url} on {i} attempt: {e}"
                logging.warning(error)
                continue

            logging.info("Response content '%s'", response.content)

            if response.ok:
                break

            error = (
                f"Cannot insert data into clickhouse at try {i}: HTTP code "
                f"{response.status_code}: '{response.text}'"
            )

            if response.status_code >= 500:
                # A retryable error
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

    def _insert_json_str_info(self, db, table, json_str):
        self.insert_json_str(self.url, self.auth, db, table, json_str)

    def insert_event_into(self, db, table, event, safe=True):
        event_str = json.dumps(event)
        try:
            self._insert_json_str_info(db, table, event_str)
        except InsertException as e:
            logging.error(
                "Exception happened during inserting data into clickhouse: %s", e
            )
            if not safe:
                raise

    def insert_events_into(self, db, table, events, safe=True):
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

    def _select_and_get_json_each_row(self, db, query, query_params):
        params = {
            "database": db,
            "query": query,
            "default_format": "JSONEachRow",
        }
        if query_params is not None:
            for name, value in query_params.items():
                params[f"param_{name}"] = str(value)

        for i in range(5):
            response = None
            try:
                response = requests.get(
                    self.url, params=params, headers=self.auth, timeout=100
                )
                response.raise_for_status()
                return response.text
            except Exception as ex:
                logging.warning("Select query failed with exception %s", str(ex))
                if response:
                    logging.warning("Response text %s", response.text)
                time.sleep(0.1 * i)

        raise CHException("Cannot fetch data from clickhouse")

    def select_json_each_row(self, db, query, query_params=None):
        text = self._select_and_get_json_each_row(db, query, query_params)
        result = []
        for line in text.split("\n"):
            if line:
                result.append(json.loads(line))
        return result


def _query_imds(path):
    url = f"http://169.254.169.254/{path}"
    for i in range(5):
        try:
            response = requests.get(url, timeout=1)
            if response.status_code == 200:
                return response.text
        except Exception as e:
            error = (
                f"Received exception while sending data to {url} on {i} attempt: {e}"
            )
            logging.warning(error)
            continue
    return ""


# Obtain the machine type from IMDS:
def get_instance_type():
    return _query_imds("latest/meta-data/instance-type")


# Obtain the instance id from IMDS:
def get_instance_id():
    return _query_imds("latest/meta-data/instance-id")


def get_instance_lifecycle():
    return _query_imds("latest/meta-data/instance-life-cycle")


def prepare_tests_results_for_clickhouse(
    pr_info: PRInfo,
    test_results: TestResults,
    check_status: str,
    check_duration: float,
    check_start_time: str,
    report_url: str,
    check_name: str,
) -> List[dict]:
    pull_request_url = "https://github.com/ClickHouse/ClickHouse/commits/master"
    base_ref = "master"
    head_ref = "master"
    base_repo = pr_info.repo_full_name
    head_repo = pr_info.repo_full_name
    if pr_info.number != 0:
        pull_request_url = pr_info.pr_html_url
        base_ref = pr_info.base_ref
        base_repo = pr_info.base_name
        head_ref = pr_info.head_ref
        head_repo = pr_info.head_name

    common_properties = {
        "pull_request_number": pr_info.number,
        "commit_sha": pr_info.sha,
        "commit_url": pr_info.commit_html_url,
        "check_name": check_name,
        "check_status": check_status,
        "check_duration_ms": int(float(check_duration) * 1000),
        "check_start_time": check_start_time,
        "report_url": report_url,
        "pull_request_url": pull_request_url,
        "base_ref": base_ref,
        "base_repo": base_repo,
        "head_ref": head_ref,
        "head_repo": head_repo,
        "task_url": pr_info.task_url,
        "instance_type": ",".join([get_instance_type(), get_instance_lifecycle()]),
        "instance_id": get_instance_id(),
    }

    # Always publish a total record for all checks. For checks with individual
    # tests, also publish a record per test.
    result = [common_properties]
    for test_result in test_results:
        current_row = common_properties.copy()
        test_name = test_result.name
        test_status = test_result.status

        test_time = test_result.time or 0
        current_row["test_duration_ms"] = int(test_time * 1000)
        current_row["test_name"] = test_name
        current_row["test_status"] = test_status
        if test_result.raw_logs:
            # Protect from too big blobs that contain garbage
            current_row["test_context_raw"] = test_result.raw_logs[: 32 * 1024]
        else:
            current_row["test_context_raw"] = ""
        result.append(current_row)

    return result


class CiLogsCredentials:
    def __init__(self, config_path: Path):
        self.config_path = config_path
        try:
            self._host = get_parameter_from_ssm("clickhouse_ci_logs_host")  # type: str
            self._password = get_parameter_from_ssm(
                "clickhouse_ci_logs_password"
            )  # type: str
        except:
            logging.warning(
                "Unable to retreive host and/or password from smm, all other "
                "methods will noop"
            )
            self._host = ""
            self._password = ""

    def create_ci_logs_credentials(self) -> None:
        if not (self.host and self.password):
            logging.info(
                "Hostname or password for CI logs instance are unknown, "
                "skipping creating of credentials file, removing existing"
            )
            self.config_path.unlink(missing_ok=True)
            return
        self.config_path.parent.mkdir(parents=True, exist_ok=True)
        self.config_path.write_text(
            f"CLICKHOUSE_CI_LOGS_HOST={self.host}\n"
            "CLICKHOUSE_CI_LOGS_USER=ci\n"
            f"CLICKHOUSE_CI_LOGS_PASSWORD={self.password}\n",
            encoding="utf-8",
        )

    def get_docker_arguments(
        self, pr_info: PRInfo, check_start_time: str, check_name: str
    ) -> str:
        run_by_hash_total = int(os.getenv("RUN_BY_HASH_TOTAL", "0"))
        if run_by_hash_total > 1:
            run_by_hash_num = int(os.getenv("RUN_BY_HASH_NUM", "0"))
            check_name = f"{check_name} [{run_by_hash_num + 1}/{run_by_hash_total}]"

        self.create_ci_logs_credentials()
        if not self.config_path.exists():
            logging.info("Do not use external logs pushing")
            return ""
        extra_columns = (
            f"CAST({pr_info.number} AS UInt32) AS pull_request_number, '{pr_info.sha}' AS commit_sha, "
            f"toDateTime('{check_start_time}', 'UTC') AS check_start_time, toLowCardinality('{check_name}') AS check_name, "
            f"toLowCardinality('{get_instance_type()}') AS instance_type, '{get_instance_id()}' AS instance_id"
        )
        return (
            f'-e EXTRA_COLUMNS_EXPRESSION="{extra_columns}" '
            f"-e CLICKHOUSE_CI_LOGS_CREDENTIALS=/tmp/export-logs-config.sh "
            f"--volume={self.config_path.absolute()}:/tmp/export-logs-config.sh:ro "
        )

    def clean_ci_logs_from_credentials(self, log_path: Path) -> None:
        if not (self.host or self.password):
            logging.info(
                "Hostname and password for CI logs instance are unknown, "
                "skipping cleaning %s",
                log_path,
            )
            return

        def process_line(line: str) -> str:
            if self.host and self.password:
                return line.replace(self.host, "CLICKHOUSE_CI_LOGS_HOST").replace(
                    self.password, "CLICKHOUSE_CI_LOGS_PASSWORD"
                )
            if self.host:
                return line.replace(self.host, "CLICKHOUSE_CI_LOGS_HOST")
            # the remaining is self.password
            return line.replace(self.password, "CLICKHOUSE_CI_LOGS_PASSWORD")

        # errors="surrogateescape" require python 3.10.
        # With ubuntu 22.04 we are safe
        with fileinput.input(
            log_path, inplace=True, errors="surrogateescape"
        ) as log_fd:
            for line in log_fd:
                print(process_line(line), end="")

    @property
    def host(self) -> str:
        return self._host

    @property
    def password(self) -> str:
        return self._password
