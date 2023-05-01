#!/usr/bin/env python

"""
Lambda gets the workflow_job events, see
https://docs.github.com/en/developers/webhooks-and-events/webhooks/webhook-events-and-payloads#workflow_job

Then it either posts it as is to the play.clickhouse.com, or anonymizes the sensitive
fields for private repositories
"""

from base64 import b64decode
from dataclasses import dataclass
from typing import Any, List
import json
import logging
import time

import boto3  # type: ignore
import requests  # type: ignore

logging.getLogger().setLevel(logging.INFO)


@dataclass
class WorkflowJob:
    id: int
    run_id: int
    workflow_name: str
    head_branch: str
    run_url: str
    run_attempt: int
    node_id: str
    head_sha: str
    url: str
    html_url: str
    status: str
    conclusion: str
    started_at: str
    completed_at: str
    name: str
    steps: int  # just number of steps, we don't keep steps
    check_run_url: str
    labels: List[str]
    runner_id: int
    runner_name: str
    runner_group_id: int
    runner_group_name: str
    repository: str

    def anonimyze_url(self, url: str) -> str:
        return url.replace(self.repository, "ANONYMIZED_REPO")

    def anonimyze(self):
        anm = "ANONYMIZED"
        self.workflow_name = anm
        self.head_branch = anm
        self.run_url = self.anonimyze_url(self.run_url)
        self.node_id = anm
        self.url = self.anonimyze_url(self.url)
        self.html_url = self.anonimyze_url(self.html_url)
        self.name = anm
        self.check_run_url = self.anonimyze_url(self.check_run_url)
        self.repository = anm

    def as_dict(self) -> dict:
        return self.__dict__


### VENDORING
def get_parameter_from_ssm(name, decrypt=True, client=None):
    if not client:
        client = boto3.client("ssm", region_name="us-east-1")
    return client.get_parameter(Name=name, WithDecryption=decrypt)["Parameter"]["Value"]


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


### VENDORING END

clickhouse_client = ClickHouseHelper()


def send_event_workflow_job(workflow_job: WorkflowJob) -> None:
    # # SHOW CREATE TABLE default.workflow_jobs
    # CREATE TABLE default.workflow_jobs UUID 'c0351924-8ccd-47a6-9db0-e28a9eee2fdf'
    # (
    #     `id` UInt64,
    #     `run_id` UInt64,
    #     `workflow_name` LowCardinality(String),
    #     `head_branch` LowCardinality(String),
    #     `run_url` String,
    #     `run_attempt` UInt16,
    #     `node_id` String,
    #     `head_sha` String,
    #     `url` String,
    #     `html_url` String,
    #     `status` Enum8('waiting' = 1, 'queued' = 2, 'in_progress' = 3, 'completed' = 4),
    #     `conclusion` LowCardinality(String),
    #     `started_at` DateTime,
    #     `completed_at` DateTime,
    #     `name` LowCardinality(String),
    #     `steps` UInt16,
    #     `check_run_url` String,
    #     `labels` Array(LowCardinality(String)),
    #     `runner_id` UInt64,
    #     `runner_name` String,
    #     `runner_group_id` UInt64,
    #     `runner_group_name` LowCardinality(String),
    #     `repository` LowCardinality(String),
    #     `updated_at` DateTime DEFAULT now()
    # )
    # ENGINE = ReplicatedMergeTree('/clickhouse/tables/c0351924-8ccd-47a6-9db0-e28a9eee2fdf/{shard}', '{replica}')
    # PARTITION BY toStartOfMonth(started_at)
    # ORDER BY (id, updated_at)
    # SETTINGS index_granularity = 8192
    global clickhouse_client
    kwargs = {
        "db": "default",
        "table": "workflow_jobs",
        "event": workflow_job.as_dict(),
        "safe": False,
    }
    try:
        clickhouse_client.insert_event_into(**kwargs)
    except InsertException as ex:
        logging.exception(
            "Got an exception on insert, tryuing to update the client "
            "credentials and repeat",
            exc_info=ex,
        )
        clickhouse_client = ClickHouseHelper()
        clickhouse_client.insert_event_into(**kwargs)


def handler(event: dict, _: Any) -> dict:
    if event["isBase64Encoded"]:
        event_data = json.loads(b64decode(event["body"]))
    else:
        event_data = json.loads(event["body"])

    repo = event_data["repository"]
    wf_job = event_data["workflow_job"]
    workflow_job = WorkflowJob(
        wf_job["id"],
        wf_job["run_id"],
        wf_job["workflow_name"] or "",  # nullable
        wf_job["head_branch"],
        wf_job["run_url"],
        wf_job["run_attempt"],
        wf_job["node_id"],
        wf_job["head_sha"],
        wf_job["url"],
        wf_job["html_url"],
        wf_job["status"],
        wf_job["conclusion"] or "",  # nullable
        wf_job["started_at"],
        wf_job["completed_at"] or "1970-01-01T00:00:00",  # nullable date
        wf_job["name"],
        len(wf_job["steps"]),
        wf_job["check_run_url"],
        wf_job["labels"],
        wf_job["runner_id"] or 0,  # nullable
        wf_job["runner_name"] or "",  # nullable
        wf_job["runner_group_id"] or 0,  # nullable
        wf_job["runner_group_name"] or "",  # nullable
        repo["full_name"],
    )
    logging.info(
        "Got the next event (private_repo=%s): %s", repo["private"], workflow_job
    )
    if repo["private"]:
        workflow_job.anonimyze()

    send_event_workflow_job(workflow_job)

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": '{"status": "OK"}',
    }
