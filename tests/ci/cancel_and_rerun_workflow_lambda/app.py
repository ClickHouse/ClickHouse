#!/usr/bin/env python3

import json
import time
import jwt

import requests
import boto3

# https://docs.github.com/en/rest/reference/actions#cancel-a-workflow-run
#
API_URL = 'https://api.github.com/repos/ClickHouse/ClickHouse'

MAX_RETRY = 5

def get_installation_id(jwt_token):
    headers = {
        "Authorization": f"Bearer {jwt_token}",
        "Accept": "application/vnd.github.v3+json",
    }
    response = requests.get("https://api.github.com/app/installations", headers=headers)
    response.raise_for_status()
    data = response.json()
    return data[0]['id']

def get_access_token(jwt_token, installation_id):
    headers = {
        "Authorization": f"Bearer {jwt_token}",
        "Accept": "application/vnd.github.v3+json",
    }
    response = requests.post(f"https://api.github.com/app/installations/{installation_id}/access_tokens", headers=headers)
    response.raise_for_status()
    data = response.json()
    return data['token']

def get_key_and_app_from_aws():
    secret_name = "clickhouse_github_secret_key"
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
    )
    get_secret_value_response = client.get_secret_value(
        SecretId=secret_name
    )
    data = json.loads(get_secret_value_response['SecretString'])
    return data['clickhouse-app-key'], int(data['clickhouse-app-id'])

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


def get_workflows_urls_for_pull_request(pull_request_event, url_name, check_status):
    head_branch = pull_request_event['head']['ref']
    print("PR", pull_request_event['number'], "has head ref", head_branch)
    workflows = _exec_get_with_retry(API_URL + f"/actions/runs?branch={head_branch}")
    workflows_urls = set([])
    for workflow in workflows['workflow_runs']:
        if check_status(workflow['status']):
            print("Workflow", workflow['url'], "going to check workflow")
            workflows_urls.add(workflow[url_name])
        else:
            print("Workflow", workflow['url'], "doesn't satisfy status condition")

    return workflows_urls

def _exec_post_with_retry(url, token):
    headers = {
        "Authorization": f"token {token}"
    }
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
        print("Cancelling workflow using url", url)
        _exec_post_with_retry(url, token)
        print("Workflow cancelled")

def main(event):
    token = get_token_from_aws()
    event_data = json.loads(event['body'])

    print("Got event for PR", event_data['number'])
    action = event_data['action']
    print("Got action", event_data['action'])
    pull_request = event_data['pull_request']
    labels = { l['name'] for l in pull_request['labels'] }
    print("PR has labels", labels)
    if action == 'closed' or 'do not test' in labels:
        print("PR merged/closed or manually labeled 'do not test' will kill workflows")
        def check_status(status):
            return status != 'completed'
        workflows_to_cancel = get_workflows_urls_for_pull_request(pull_request, 'cancel_url', check_status)
        print(f"Found {len(workflows_to_cancel)} workflows to cancel")
        exec_workflow_url(workflows_to_cancel, token)
    elif action == 'labeled' and 'can be tested' in labels:
        print("PR marked with can be tested label, rerun workflow")

        def check_status_for_cancell(status):
            return status != 'completed'
        workflows_to_cancel = get_workflows_urls_for_pull_request(pull_request, 'cancel_url', check_status_for_cancell)
        print("Cancelling all previous workflows")
        print(f"Found {len(workflows_to_cancel)} workflows to cancel")
        exec_workflow_url(workflows_to_cancel, token)
        def check_status_for_rerun(status):
            return status in ('completed', 'cancelled')
        workflows_to_rerun = get_workflows_urls_for_pull_request(pull_request, 'rerun_url', check_status_for_rerun)
        print(f"Found {len(workflows_to_rerun)} workflows")
        exec_workflow_url(workflows_to_rerun, token)
    else:
        print("Nothing to do")

def handler(event, _):
    main(event)
