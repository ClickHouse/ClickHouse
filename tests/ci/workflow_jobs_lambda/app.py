#!/usr/bin/env python

"""
Lambda gets the workflow_job events, see
https://docs.github.com/en/developers/webhooks-and-events/webhooks/webhook-events-and-payloads#workflow_job

Then it either posts it as is to the play.clickhouse.com, or anonymizes the sensitive
fields for private repositories
"""

from base64 import b64decode
from dataclasses import dataclass
from typing import Any, List, Optional
import json
import logging

from lambda_shared import ClickHouseHelper, InsertException, get_parameter_from_ssm

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


CH_CLIENT = None  # type: Optional[ClickHouseHelper]


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
    global CH_CLIENT
    CH_CLIENT = CH_CLIENT or ClickHouseHelper(
        get_parameter_from_ssm("clickhouse-test-stat-url"),
        get_parameter_from_ssm("clickhouse-test-stat-login"),
        get_parameter_from_ssm("clickhouse-test-stat-password"),
    )
    try:
        CH_CLIENT.insert_event_into(
            "default", "workflow_jobs", workflow_job.as_dict(), False
        )
    except InsertException as ex:
        logging.exception(
            "Got an exception on insert, tryuing to update the client "
            "credentials and repeat",
            exc_info=ex,
        )
        CH_CLIENT = ClickHouseHelper(
            get_parameter_from_ssm("clickhouse-test-stat-url"),
            get_parameter_from_ssm("clickhouse-test-stat-login"),
            get_parameter_from_ssm("clickhouse-test-stat-password"),
        )
        CH_CLIENT.insert_event_into(
            "default", "workflow_jobs", workflow_job.as_dict(), False
        )


def handler(event: dict, context: Any) -> dict:
    if event["isBase64Encoded"]:
        event_data = json.loads(b64decode(event["body"]))
    else:
        event_data = json.loads(event["body"])

    logging.info("Got the next raw event from the github hook: %s", event_data)
    repo = event_data["repository"]
    try:
        wf_job = event_data["workflow_job"]
    except KeyError:
        logging.error("The event does not contain valid workflow_jobs data")
        logging.error("The event data: %s", event)
        logging.error("The context data: %s", context)

    # We record only finished steps
    steps = len([step for step in wf_job["steps"] if step["conclusion"] is not None])

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
        steps,
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
