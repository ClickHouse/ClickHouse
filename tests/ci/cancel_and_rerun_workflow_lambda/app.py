#!/usr/bin/env python3

import json
import time
from base64 import b64decode
from collections import namedtuple
from queue import Queue
from threading import Thread
from typing import Any, Dict, List, Optional

import requests
from lambda_shared.pr import Labels, check_pr_description
from lambda_shared.token import get_cached_access_token

NEED_RERUN_OR_CANCELL_WORKFLOWS = {
    "BackportPR",
    "DocsCheck",
    "MasterCI",
    "PullRequestCI",
}

MAX_RETRY = 5

DEBUG_INFO = {}  # type: Dict[str, Any]


class Worker(Thread):
    def __init__(
        self, request_queue: Queue, token: str, ignore_exception: bool = False
    ):
        Thread.__init__(self)
        self.queue = request_queue
        self.token = token
        self.ignore_exception = ignore_exception
        self.response = {}  # type: Dict

    def run(self):
        m = self.queue.get()
        try:
            self.response = _exec_get_with_retry(m, self.token)
        except Exception as e:
            if not self.ignore_exception:
                raise
            print(f"Exception occured, still continue: {e}")
        self.queue.task_done()


def _exec_get_with_retry(url: str, token: str) -> dict:
    headers = {"Authorization": f"token {token}"}
    e = Exception()
    for i in range(MAX_RETRY):
        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            return response.json()  # type: ignore
        except Exception as ex:
            print("Got exception executing request", ex)
            e = ex
            time.sleep(i + 1)

    raise requests.HTTPError("Cannot execute GET request with retries") from e


WorkflowDescription = namedtuple(
    "WorkflowDescription",
    [
        "url",
        "run_id",
        "name",
        "head_sha",
        "status",
        "rerun_url",
        "cancel_url",
        "conclusion",
    ],
)


def get_workflows_description_for_pull_request(
    pull_request_event: dict, token: str
) -> List[WorkflowDescription]:
    head_repo = pull_request_event["head"]["repo"]["full_name"]
    head_branch = pull_request_event["head"]["ref"]
    print("PR", pull_request_event["number"], "has head ref", head_branch)

    workflows_data = []
    repo_url = pull_request_event["base"]["repo"]["url"]
    request_url = f"{repo_url}/actions/runs?per_page=100"
    # Get all workflows for the current branch
    for i in range(1, 11):
        workflows = _exec_get_with_retry(
            f"{request_url}&event=pull_request&branch={head_branch}&page={i}", token
        )
        if not workflows["workflow_runs"]:
            break
        workflows_data += workflows["workflow_runs"]
        if i == 10:
            print("Too many workflows found")

    if not workflows_data:
        print("No workflows found by filter")
        return []

    print(f"Total workflows for the branch {head_branch} found: {len(workflows_data)}")

    DEBUG_INFO["workflows"] = []
    workflow_descriptions = []
    for workflow in workflows_data:
        # Some time workflow["head_repository"]["full_name"] is None
        if workflow["head_repository"] is None:
            continue
        DEBUG_INFO["workflows"].append(
            {
                "full_name": workflow["head_repository"]["full_name"],
                "name": workflow["name"],
                "branch": workflow["head_branch"],
            }
        )
        # unfortunately we cannot filter workflows from forks in request to API
        # so doing it manually
        if (
            workflow["head_repository"]["full_name"] == head_repo
            and workflow["name"] in NEED_RERUN_OR_CANCELL_WORKFLOWS
        ):
            workflow_descriptions.append(
                WorkflowDescription(
                    url=workflow["url"],
                    run_id=workflow["id"],
                    name=workflow["name"],
                    head_sha=workflow["head_sha"],
                    status=workflow["status"],
                    rerun_url=workflow["rerun_url"],
                    cancel_url=workflow["cancel_url"],
                    conclusion=workflow["conclusion"],
                )
            )

    return workflow_descriptions


def get_workflow_description_fallback(
    pull_request_event: dict, token: str
) -> List[WorkflowDescription]:
    head_repo = pull_request_event["head"]["repo"]["full_name"]
    head_branch = pull_request_event["head"]["ref"]
    print("Get last 500 workflows from API to search related there")
    # Fallback for a case of an already deleted branch and no workflows received
    repo_url = pull_request_event["base"]["repo"]["url"]
    request_url = f"{repo_url}/actions/runs?per_page=100"
    q = Queue()  # type: Queue
    workers = []
    workflows_data = []
    i = 1
    for i in range(1, 6):
        q.put(f"{request_url}&page={i}")
        worker = Worker(q, token, True)
        worker.start()
        workers.append(worker)

    for worker in workers:
        worker.join()
        if not worker.response:
            # We ignore get errors, so response can be empty
            continue
        # Prefilter workflows
        workflows_data += [
            wf
            for wf in worker.response["workflow_runs"]
            if wf["head_repository"] is not None
            and wf["head_repository"]["full_name"] == head_repo
            and wf["head_branch"] == head_branch
            and wf["name"] in NEED_RERUN_OR_CANCELL_WORKFLOWS
        ]

    print(f"Total workflows in last 500 actions matches: {len(workflows_data)}")

    DEBUG_INFO["workflows"] = [
        {
            "full_name": wf["head_repository"]["full_name"],
            "name": wf["name"],
            "branch": wf["head_branch"],
        }
        for wf in workflows_data
    ]

    workflow_descriptions = [
        WorkflowDescription(
            url=wf["url"],
            run_id=wf["id"],
            name=wf["name"],
            head_sha=wf["head_sha"],
            status=wf["status"],
            rerun_url=wf["rerun_url"],
            cancel_url=wf["cancel_url"],
            conclusion=wf["conclusion"],
        )
        for wf in workflows_data
    ]

    return workflow_descriptions


def get_workflow_description(workflow_url: str, token: str) -> WorkflowDescription:
    workflow = _exec_get_with_retry(workflow_url, token)
    return WorkflowDescription(
        url=workflow["url"],
        run_id=workflow["id"],
        name=workflow["name"],
        head_sha=workflow["head_sha"],
        status=workflow["status"],
        rerun_url=workflow["rerun_url"],
        cancel_url=workflow["cancel_url"],
        conclusion=workflow["conclusion"],
    )


def _exec_post_with_retry(url: str, token: str, json: Optional[Any] = None) -> Any:
    headers = {"Authorization": f"token {token}"}
    e = Exception()
    for i in range(MAX_RETRY):
        try:
            response = requests.post(url, headers=headers, json=json, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as ex:
            print("Got exception executing request", ex)
            e = ex
            time.sleep(i + 1)

    raise requests.HTTPError("Cannot execute POST request with retry") from e


def exec_workflow_url(urls_to_post, token):
    for url in urls_to_post:
        print("Post for workflow workflow using url", url)
        _exec_post_with_retry(url, token)
        print("Workflow post finished")


def main(event):
    token = get_cached_access_token()
    DEBUG_INFO["event"] = event
    if event["isBase64Encoded"]:
        event_data = json.loads(b64decode(event["body"]))
    else:
        event_data = json.loads(event["body"])

    print("Got event for PR", event_data["number"])
    action = event_data["action"]
    print("Got action", event_data["action"])
    pull_request = event_data["pull_request"]
    label = ""
    if action == "labeled":
        label = event_data["label"]["name"]
        print("Added label:", label)

    print("PR has labels", {label["name"] for label in pull_request["labels"]})
    if action == "opened" or (
        action == "labeled" and pull_request["created_at"] == pull_request["updated_at"]
    ):
        print("Freshly opened PR, nothing to do")
        return

    if action == "closed" or label == Labels.DO_NOT_TEST:
        print("PR merged/closed or manually labeled 'do not test', will kill workflows")
        workflow_descriptions = get_workflows_description_for_pull_request(
            pull_request, token
        )
        workflow_descriptions = (
            workflow_descriptions
            or get_workflow_description_fallback(pull_request, token)
        )
        urls_to_cancel = []
        for workflow_description in workflow_descriptions:
            if (
                workflow_description.status != "completed"
                and workflow_description.conclusion != "cancelled"
            ):
                urls_to_cancel.append(workflow_description.cancel_url)
        print(f"Found {len(urls_to_cancel)} workflows to cancel")
        exec_workflow_url(urls_to_cancel, token)
        return

    if label == Labels.CAN_BE_TESTED:
        print("PR marked with can be tested label, rerun workflow")
        workflow_descriptions = get_workflows_description_for_pull_request(
            pull_request, token
        )
        workflow_descriptions = (
            workflow_descriptions
            or get_workflow_description_fallback(pull_request, token)
        )
        if not workflow_descriptions:
            print("Not found any workflows")
            return

        workflow_descriptions.sort(key=lambda x: x.run_id)  # type: ignore
        most_recent_workflow = workflow_descriptions[-1]
        print("Latest workflow", most_recent_workflow)
        if (
            most_recent_workflow.status != "completed"
            and most_recent_workflow.conclusion != "cancelled"
        ):
            print("Latest workflow is not completed, cancelling")
            exec_workflow_url([most_recent_workflow.cancel_url], token)
            print("Cancelled")

        for _ in range(45):
            # If the number of retries is changed: tune the lambda limits accordingly
            latest_workflow_desc = get_workflow_description(
                most_recent_workflow.url, token
            )
            print("Checking latest workflow", latest_workflow_desc)
            if latest_workflow_desc.status in ("completed", "cancelled"):
                print("Finally latest workflow done, going to rerun")
                exec_workflow_url([most_recent_workflow.rerun_url], token)
                print("Rerun finished, exiting")
                break
            print("Still have strange status")
            time.sleep(3)
        return

    if action == "edited":
        print("PR is edited, check if the body is correct")
        error, _ = check_pr_description(
            pull_request["body"], pull_request["base"]["repo"]["full_name"]
        )
        if error:
            print(
                f"The PR's body is wrong, is going to comment it. The error is: {error}"
            )
            post_json = {
                "body": "This is an automatic comment. The PR descriptions does not "
                f"match the [template]({pull_request['base']['repo']['html_url']}/"
                "blob/master/.github/PULL_REQUEST_TEMPLATE.md?plain=1).\n\n"
                f"Please, edit it accordingly.\n\nThe error is: {error}"
            }
            _exec_post_with_retry(pull_request["comments_url"], token, json=post_json)
        return

    if action == "synchronize":
        print("PR is synchronized, going to stop old actions")
        workflow_descriptions = get_workflows_description_for_pull_request(
            pull_request, token
        )
        workflow_descriptions = (
            workflow_descriptions
            or get_workflow_description_fallback(pull_request, token)
        )
        urls_to_cancel = []
        for workflow_description in workflow_descriptions:
            if (
                workflow_description.status != "completed"
                and workflow_description.conclusion != "cancelled"
                and workflow_description.head_sha != pull_request["head"]["sha"]
            ):
                urls_to_cancel.append(workflow_description.cancel_url)
        print(f"Found {len(urls_to_cancel)} workflows to cancel")
        exec_workflow_url(urls_to_cancel, token)
        return

    print("Nothing to do")


def handler(event, _):
    try:
        main(event)

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": '{"status": "OK"}',
        }
    finally:
        for name, value in DEBUG_INFO.items():
            print(f"Value of {name}: ", value)
