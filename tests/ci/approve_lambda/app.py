#!/usr/bin/env python3

import json
import time
import fnmatch
from collections import namedtuple
import jwt

import requests
import boto3

API_URL = 'https://api.github.com/repos/ClickHouse/ClickHouse'

SUSPICIOUS_CHANGED_FILES_NUMBER = 200

SUSPICIOUS_PATTERNS = [
    "tests/ci/*",
    "docs/tools/*",
    ".github/*",
    "utils/release/*",
    "docker/*",
    "release",
]

MAX_RETRY = 5

WorkflowDescription = namedtuple('WorkflowDescription',
                                 ['name', 'action', 'run_id', 'event', 'sender_login',
                                  'workflow_id', 'fork_owner_login', 'fork_branch', 'sender_orgs'])

TRUSTED_WORKFLOW_IDS = {
    14586616, # Cancel workflows, always trusted
}

TRUSTED_ORG_IDS = {
    7409213,   # yandex
    28471076,  # altinity
    54801242,  # clickhouse
}

# Individual trusted contirbutors who are not in any trusted organization.
# Can be changed in runtime: we will append users that we learned to be in
# a trusted org, to save GitHub API calls.
TRUSTED_CONTRIBUTORS = {
    "achimbab",
    "adevyatova ",  # DOCSUP
    "Algunenano",   # Raúl Marín, Tinybird
    "AnaUvarova",   # DOCSUP
    "anauvarova",   # technical writer, Yandex
    "annvsh",       # technical writer, Yandex
    "atereh",       # DOCSUP
    "azat",
    "bharatnc",     # Newbie, but already with many contributions.
    "bobrik",       # Seasoned contributor, CloundFlare
    "BohuTANG",
    "damozhaeva",   # DOCSUP
    "den-crane",
    "gyuton",       # DOCSUP
    "hagen1778",    # Roman Khavronenko, seasoned contributor
    "hczhcz",
    "hexiaoting",   # Seasoned contributor
    "ildus",        # adjust, ex-pgpro
    "javisantana",  # a Spanish ClickHouse enthusiast, ex-Carto
    "ka1bi4",       # DOCSUP
    "kirillikoff",  # DOCSUP
    "kreuzerkrieg",
    "lehasm",       # DOCSUP
    "michon470",    # DOCSUP
    "MyroTk",       # Tester in Altinity
    "myrrc",        # Michael Kot, Altinity
    "nikvas0",
    "nvartolomei",
    "olgarev",      # DOCSUP
    "otrazhenia",   # Yandex docs contractor
    "pdv-ru",       # DOCSUP
    "podshumok",    # cmake expert from QRator Labs
    "s-mx",         # Maxim Sabyanin, former employee, present contributor
    "sevirov",      # technical writer, Yandex
    "spongedu",     # Seasoned contributor
    "ucasfl",       # Amos Bird's friend
    "vdimir",       # Employee
    "vzakaznikov",
    "YiuRULE",
    "zlobober"      # Developer of YT
}


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


def is_trusted_sender(pr_user_login, pr_user_orgs):
    if pr_user_login in TRUSTED_CONTRIBUTORS:
        print(f"User '{pr_user_login}' is trusted")
        return True

    print(f"User '{pr_user_login}' is not trusted")

    for org_id in pr_user_orgs:
        if org_id in TRUSTED_ORG_IDS:
            print(f"Org '{org_id}' is trusted; will mark user {pr_user_login} as trusted")
            return True
        print(f"Org '{org_id}' is not trusted")

    return False

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

def _exec_post_with_retry(url, token, data=None):
    headers = {
        "Authorization": f"token {token}"
    }
    for i in range(MAX_RETRY):
        try:
            if data:
                response = requests.post(url, headers=headers, json=data)
            else:
                response = requests.post(url, headers=headers)
            if response.status_code == 403:
                data = response.json()
                if 'message' in data and data['message'] == 'This workflow run is not waiting for approval':
                    print("Workflow doesn't need approval")
                    return data
            response.raise_for_status()
            return response.json()
        except Exception as ex:
            print("Got exception executing request", ex)
            time.sleep(i + 1)

    raise Exception("Cannot execute POST request with retry")

def _get_pull_requests_from(owner, branch):
    url = f"{API_URL}/pulls?head={owner}:{branch}"
    return _exec_get_with_retry(url)

def get_workflow_description_from_event(event):
    action = event['action']
    sender_login = event['sender']['login']
    run_id = event['workflow_run']['id']
    event_type = event['workflow_run']['event']
    fork_owner = event['workflow_run']['head_repository']['owner']['login']
    fork_branch = event['workflow_run']['head_branch']
    orgs_data = _exec_get_with_retry(event['sender']['organizations_url'])
    sender_orgs = [org['id'] for org in orgs_data]
    name = event['workflow_run']['name']
    workflow_id = event['workflow_run']['workflow_id']
    return WorkflowDescription(
        name=name,
        action=action,
        sender_login=sender_login,
        run_id=run_id,
        event=event_type,
        fork_owner_login=fork_owner,
        fork_branch=fork_branch,
        sender_orgs=sender_orgs,
        workflow_id=workflow_id,
    )


def get_changed_files_for_pull_request(pull_request):
    number = pull_request['number']

    changed_files = set([])
    for i in range(1, 31):
        print("Requesting changed files page", i)
        url = f"{API_URL}/pulls/{number}/files?page={i}&per_page=100"
        data = _exec_get_with_retry(url)
        print(f"Got {len(data)} changed files")
        if len(data) == 0:
            print("No more changed files")
            break

        for change in data:
            #print("Adding changed file", change['filename'])
            changed_files.add(change['filename'])

        if len(changed_files) >= SUSPICIOUS_CHANGED_FILES_NUMBER:
            print(f"More than {len(changed_files)} changed files. Will stop fetching new files.")
            break

    return changed_files

def check_suspicious_changed_files(changed_files):
    if len(changed_files) >= SUSPICIOUS_CHANGED_FILES_NUMBER:
        print(f"Too many files changed {len(changed_files)}, need manual approve")
        return True

    for path in changed_files:
        for pattern in SUSPICIOUS_PATTERNS:
            if fnmatch.fnmatch(path, pattern):
                print(f"File {path} match suspicious pattern {pattern}, will not approve automatically")
                return True

    print("No changed files match suspicious patterns, run will be approved")
    return False

def approve_run(run_id, token):
    url = f"{API_URL}/actions/runs/{run_id}/approve"
    _exec_post_with_retry(url, token)

def label_manual_approve(pull_request, token):
    number = pull_request['number']
    url = f"{API_URL}/issues/{number}/labels"
    data = {"labels" : "manual approve"}

    _exec_post_with_retry(url, token, data)

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

def main(event):
    token = get_token_from_aws()
    event_data = json.loads(event['body'])
    workflow_description = get_workflow_description_from_event(event_data)

    print("Got workflow description", workflow_description)
    if workflow_description.action != "requested":
        print("Exiting, event action is", workflow_description.action)
        return

    if workflow_description.workflow_id in TRUSTED_WORKFLOW_IDS:
        print("Workflow in trusted list, approving run")
        approve_run(workflow_description.run_id, token)
        return

    if is_trusted_sender(workflow_description.sender_login, workflow_description.sender_orgs):
        print("Sender is trusted, approving run")
        approve_run(workflow_description.run_id, token)
        return

    pull_requests = _get_pull_requests_from(workflow_description.fork_owner_login, workflow_description.fork_branch)
    print("Got pull requests for workflow", len(pull_requests))
    if len(pull_requests) > 1:
        raise Exception("Received more than one PR for workflow run")

    if len(pull_requests) < 1:
        raise Exception("Cannot find any pull requests for workflow run")

    pull_request = pull_requests[0]
    print("Pull request for workflow number", pull_request['number'])

    changed_files = get_changed_files_for_pull_request(pull_request)
    print(f"Totally have {len(changed_files)} changed files in PR:", changed_files)
    if check_suspicious_changed_files(changed_files):
        print(f"Pull Request {pull_request['number']} has suspicious changes, label it for manuall approve")
        label_manual_approve(pull_request, token)
    else:
        print(f"Pull Request {pull_request['number']} has no suspicious changes")
        approve_run(workflow_description.run_id, token)

def handler(event, _):
    main(event)
