#!/usr/bin/env python3
import time
import logging
import json

import requests  # type: ignore
from get_robot_token import get_parameter_from_ssm


class InsertException(Exception):
    pass


class ClickHouseHelper:
    def __init__(self, url=None):
        if url is None:
            url = get_parameter_from_ssm("clickhouse-test-stat-url")

        self.url = url
        self.auth = {
            "X-ClickHouse-User": get_parameter_from_ssm("clickhouse-test-stat-login"),
            "X-ClickHouse-Key": get_parameter_from_ssm("clickhouse-test-stat-password"),
        }

    @staticmethod
    def _insert_json_str_info_impl(url, auth, db, table, json_str):
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
                logging.warning(
                    "Received exception while sending data to %s on %s attempt: %s",
                    url,
                    i,
                    e,
                )
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

    def _insert_json_str_info(self, db, table, json_str):
        self._insert_json_str_info_impl(self.url, self.auth, db, table, json_str)

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

    def _select_and_get_json_each_row(self, db, query):
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
                return response.text
            except Exception as ex:
                logging.warning("Cannot insert with exception %s", str(ex))
                if response:
                    logging.warning("Reponse text %s", response.text)
                time.sleep(0.1 * i)

        raise Exception("Cannot fetch data from clickhouse")

    def select_json_each_row(self, db, query):
        text = self._select_and_get_json_each_row(db, query)
        result = []
        for line in text.split("\n"):
            if line:
                result.append(json.loads(line))
        return result


def prepare_tests_results_for_clickhouse(
    pr_info,
    test_results,
    check_status,
    check_duration,
    check_start_time,
    report_url,
    check_name,
):

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

    common_properties = dict(
        pull_request_number=pr_info.number,
        commit_sha=pr_info.sha,
        commit_url=pr_info.commit_html_url,
        check_name=check_name,
        check_status=check_status,
        check_duration_ms=int(float(check_duration) * 1000),
        check_start_time=check_start_time,
        report_url=report_url,
        pull_request_url=pull_request_url,
        base_ref=base_ref,
        base_repo=base_repo,
        head_ref=head_ref,
        head_repo=head_repo,
        task_url=pr_info.task_url,
    )

    # Always publish a total record for all checks. For checks with individual
    # tests, also publish a record per test.
    result = [common_properties]
    for test_result in test_results:
        current_row = common_properties.copy()
        test_name = test_result[0]
        test_status = test_result[1]

        test_time = 0
        if len(test_result) > 2 and test_result[2]:
            test_time = test_result[2]
        current_row["test_duration_ms"] = int(float(test_time) * 1000)
        current_row["test_name"] = test_name
        current_row["test_status"] = test_status
        result.append(current_row)

    return result


def mark_flaky_tests(clickhouse_helper, check_name, test_results):
    try:
        query = f"""SELECT DISTINCT test_name
FROM checks
WHERE
    check_start_time BETWEEN now() - INTERVAL 3 DAY AND now()
    AND check_name = '{check_name}'
    AND (test_status = 'FAIL' OR test_status = 'FLAKY')
    AND pull_request_number = 0
"""

        tests_data = clickhouse_helper.select_json_each_row("default", query)
        master_failed_tests = {row["test_name"] for row in tests_data}
        logging.info("Found flaky tests: %s", ", ".join(master_failed_tests))

        for test_result in test_results:
            if test_result[1] == "FAIL" and test_result[0] in master_failed_tests:
                test_result[1] = "FLAKY"
    except Exception as ex:
        logging.error("Exception happened during flaky tests fetch %s", ex)
