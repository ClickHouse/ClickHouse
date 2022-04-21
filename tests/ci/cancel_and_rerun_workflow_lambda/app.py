#!/usr/bin/env python3

from collections import namedtuple
import json
import time

import jwt
import requests  # type: ignore
import boto3  # type: ignore

NEED_RERUN_OR_CANCELL_WORKFLOWS = {
    "PullRequestCI",
    "Docs",
    "DocsRelease",
    "BackportPR",
}

# https://docs.github.com/en/rest/reference/actions#cancel-a-workflow-run
#
API_URL = "https://api.github.com/repos/ClickHouse/ClickHouse"

MAX_RETRY = 5


def get_installation_id(jwt_token):
    headers = {
        "Authorization": f"Bearer {jwt_token}",
        "Accept": "application/vnd.github.v3+json",
    }
    response = requests.get("https://api.github.com/app/installations", headers=headers)
    response.raise_for_status()
    data = response.json()
    return data[0]["id"]


def get_access_token(jwt_token, installation_id):
    headers = {
        "Authorization": f"Bearer {jwt_token}",
        "Accept": "application/vnd.github.v3+json",
    }
    response = requests.post(
        f"https://api.github.com/app/installations/{installation_id}/access_tokens",
        headers=headers,
    )
    response.raise_for_status()
    data = response.json()
    return data["token"]


def get_key_and_app_from_aws():
    secret_name = "clickhouse_github_secret_key"
    session = boto3.session.Session()
    client = session.client(
        service_name="secretsmanager",
    )
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    data = json.loads(get_secret_value_response["SecretString"])
    return data["clickhouse-app-key"], int(data["clickhouse-app-id"])


def get_token_from_aws():
    private_key, app_id = get_key_and_app_from_aws()
    payload = {
        "iat": int(time.time()) - 60,
        "exp": int(time.time()) + (10 * 60),
        "iss": app_id,
    }

    encoded_jwt = jwt.encode(payload, private_key, algorithm="RS256")
    installation_id = get_installation_id(encoded_jwt)
    return get_access_token(encoded_jwt, installation_id)


def _exec_get_with_retry(url):
    for i in range(MAX_RETRY):
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except Exception as ex:
            print("Got exception executing request", ex)
            time.sleep(i + 1)

    raise Exception("Cannot execute GET request with retries")


WorkflowDescription = namedtuple(
    "WorkflowDescription", ["run_id", "status", "rerun_url", "cancel_url"]
)


def get_workflows_description_for_pull_request(pull_request_event):
    head_branch = pull_request_event["head"]["ref"]
    print("PR", pull_request_event["number"], "has head ref", head_branch)
    workflows_data = []
    workflows = _exec_get_with_retry(
        API_URL + f"/actions/runs?branch={head_branch}&event=pull_request&page=1"
    )
    workflows_data += workflows["workflow_runs"]
    i = 2
    while len(workflows["workflow_runs"]) > 0:
        workflows = _exec_get_with_retry(
            API_URL + f"/actions/runs?branch={head_branch}&event=pull_request&page={i}"
        )
        workflows_data += workflows["workflow_runs"]
        i += 1
        if i > 30:
            print("Too many workflows found")
            break

    workflow_descriptions = []
    for workflow in workflows_data:
        # unfortunately we cannot filter workflows from forks in request to API
        # so doing it manually
        if (
            workflow["head_repository"]["full_name"]
            == pull_request_event["head"]["repo"]["full_name"]
            and workflow["name"] in NEED_RERUN_OR_CANCELL_WORKFLOWS
        ):
            workflow_descriptions.append(
                WorkflowDescription(
                    run_id=workflow["id"],
                    status=workflow["status"],
                    rerun_url=workflow["rerun_url"],
                    cancel_url=workflow["cancel_url"],
                )
            )

    return workflow_descriptions


def get_workflow_description(workflow_id):
    workflow = _exec_get_with_retry(API_URL + f"/actions/runs/{workflow_id}")
    return WorkflowDescription(
        run_id=workflow["id"],
        status=workflow["status"],
        rerun_url=workflow["rerun_url"],
        cancel_url=workflow["cancel_url"],
    )


def _exec_post_with_retry(url, token):
    headers = {"Authorization": f"token {token}"}
    for i in range(MAX_RETRY):
        try:
            response = requests.post(url, headers=headers)
            response.raise_for_status()
            return response.json()
        except Exception as ex:
            print("Got exception executing request", ex)
            time.sleep(i + 1)

    raise Exception("Cannot execute POST request with retry")


def exec_workflow_url(urls_to_cancel, token):
    for url in urls_to_cancel:
        print("Post for workflow workflow using url", url)
        _exec_post_with_retry(url, token)
        print("Workflow post finished")


def main(event):
    token = get_token_from_aws()
    event_data = json.loads(event["body"])

    print("Got event for PR", event_data["number"])
    action = event_data["action"]
    print("Got action", event_data["action"])
    pull_request = event_data["pull_request"]
    labels = {label["name"] for label in pull_request["labels"]}
    print("PR has labels", labels)
    if action == "closed" or "do not test" in labels:
        print("PR merged/closed or manually labeled 'do not test' will kill workflows")
        workflow_descriptions = get_workflows_description_for_pull_request(pull_request)
        urls_to_cancel = []
        for workflow_description in workflow_descriptions:
            if workflow_description.status != "completed":
                urls_to_cancel.append(workflow_description.cancel_url)
        print(f"Found {len(urls_to_cancel)} workflows to cancel")
        exec_workflow_url(urls_to_cancel, token)
    elif action == "labeled" and "can be tested" in labels:
        print("PR marked with can be tested label, rerun workflow")
        workflow_descriptions = get_workflows_description_for_pull_request(pull_request)
        if not workflow_descriptions:
            print("Not found any workflows")
            return

        sorted_workflows = list(sorted(workflow_descriptions, key=lambda x: x.run_id))
        most_recent_workflow = sorted_workflows[-1]
        print("Latest workflow", most_recent_workflow)
        if most_recent_workflow.status != "completed":
            print("Latest workflow is not completed, cancelling")
            exec_workflow_url([most_recent_workflow.cancel_url], token)
            print("Cancelled")

        for _ in range(30):
            latest_workflow_desc = get_workflow_description(most_recent_workflow.run_id)
            print("Checking latest workflow", latest_workflow_desc)
            if latest_workflow_desc.status in ("completed", "cancelled"):
                print("Finally latest workflow done, going to rerun")
                exec_workflow_url([most_recent_workflow.rerun_url], token)
                print("Rerun finished, exiting")
                break
            print("Still have strange status")
            time.sleep(3)

    else:
        print("Nothing to do")


def handler(event, _):
    main(event)
