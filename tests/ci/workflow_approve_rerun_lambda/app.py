#!/usr/bin/env python3

from collections import namedtuple
import fnmatch
import json
import os
import time

import jwt
import requests  # type: ignore
import boto3  # type: ignore

API_URL = os.getenv("API_URL", "https://api.github.com/repos/ClickHouse/ClickHouse")

SUSPICIOUS_CHANGED_FILES_NUMBER = 200

SUSPICIOUS_PATTERNS = [
    "tests/ci/*",
    "docs/tools/*",
    ".github/*",
    "utils/release/*",
    "docker/*",
    "release",
]

# Number of retries for API calls.
MAX_RETRY = 5

# Number of times a check can re-run as a whole.
# It is needed, because we are using AWS "spot" instances, that are terminated very frequently.
MAX_WORKFLOW_RERUN = 20

WorkflowDescription = namedtuple(
    "WorkflowDescription",
    [
        "name",
        "action",
        "run_id",
        "event",
        "workflow_id",
        "conclusion",
        "status",
        "api_url",
        "fork_owner_login",
        "fork_branch",
        "rerun_url",
        "jobs_url",
        "attempt",
        "url",
    ],
)

# See https://api.github.com/orgs/{name}
TRUSTED_ORG_IDS = {
    7409213,  # yandex
    28471076,  # altinity
    54801242,  # clickhouse
}

# See https://api.github.com/repos/ClickHouse/ClickHouse/actions/workflows
# Use ID to not inject a malicious workflow
TRUSTED_WORKFLOW_IDS = {
    14586616,  # Cancel workflows, always trusted
}

NEED_RERUN_WORKFLOWS = {
    "BackportPR",
    "Docs",
    "DocsRelease",
    "MasterCI",
    "PullRequestCI",
    "ReleaseCI",
}

# Individual trusted contirbutors who are not in any trusted organization.
# Can be changed in runtime: we will append users that we learned to be in
# a trusted org, to save GitHub API calls.
TRUSTED_CONTRIBUTORS = {
    e.lower()
    for e in [
        "achimbab",
        "adevyatova ",  # DOCSUP
        "Algunenano",  # Raúl Marín, Tinybird
        "amosbird",
        "AnaUvarova",  # DOCSUP
        "anauvarova",  # technical writer, Yandex
        "annvsh",  # technical writer, Yandex
        "atereh",  # DOCSUP
        "azat",
        "bharatnc",  # Newbie, but already with many contributions.
        "bobrik",  # Seasoned contributor, CloudFlare
        "BohuTANG",
        "codyrobert",  # Flickerbox engineer
        "cwurm",  # Employee
        "damozhaeva",  # DOCSUP
        "den-crane",
        "flickerbox-tom",  # Flickerbox
        "gyuton",  # DOCSUP
        "hagen1778",  # Roman Khavronenko, seasoned contributor
        "hczhcz",
        "hexiaoting",  # Seasoned contributor
        "ildus",  # adjust, ex-pgpro
        "javisantana",  # a Spanish ClickHouse enthusiast, ex-Carto
        "ka1bi4",  # DOCSUP
        "kirillikoff",  # DOCSUP
        "kreuzerkrieg",
        "lehasm",  # DOCSUP
        "michon470",  # DOCSUP
        "MyroTk",  # Tester in Altinity
        "myrrc",  # Michael Kot, Altinity
        "nikvas0",
        "nvartolomei",
        "olgarev",  # DOCSUP
        "otrazhenia",  # Yandex docs contractor
        "pdv-ru",  # DOCSUP
        "podshumok",  # cmake expert from QRator Labs
        "s-mx",  # Maxim Sabyanin, former employee, present contributor
        "sevirov",  # technical writer, Yandex
        "spongedu",  # Seasoned contributor
        "taiyang-li",
        "ucasFL",  # Amos Bird's friend
        "vdimir",  # Employee
        "vzakaznikov",
        "YiuRULE",
        "zlobober",  # Developer of YT
        "ilejn",  # Arenadata, responsible for Kerberized Kafka
        "thomoco",  # ClickHouse
        "BoloniniD",  # Seasoned contributor, HSE
        "tonickkozlov",  # Cloudflare
        "tylerhannan",  # ClickHouse Employee
    ]
}


def get_installation_id(jwt_token):
    headers = {
        "Authorization": f"Bearer {jwt_token}",
        "Accept": "application/vnd.github.v3+json",
    }
    response = requests.get("https://api.github.com/app/installations", headers=headers)
    response.raise_for_status()
    data = response.json()
    for installation in data:
        if installation["account"]["login"] == "ClickHouse":
            installation_id = installation["id"]
    return installation_id


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


def is_trusted_contributor(pr_user_login, pr_user_orgs):
    if pr_user_login.lower() in TRUSTED_CONTRIBUTORS:
        print(f"User '{pr_user_login}' is trusted")
        return True

    print(f"User '{pr_user_login}' is not trusted")

    for org_id in pr_user_orgs:
        if org_id in TRUSTED_ORG_IDS:
            print(
                f"Org '{org_id}' is trusted; will mark user {pr_user_login} as trusted"
            )
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
    headers = {"Authorization": f"token {token}"}
    for i in range(MAX_RETRY):
        try:
            if data:
                response = requests.post(url, headers=headers, json=data)
            else:
                response = requests.post(url, headers=headers)
            if response.status_code == 403:
                data = response.json()
                if (
                    "message" in data
                    and data["message"]
                    == "This workflow run is not waiting for approval"
                ):
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
    action = event["action"]
    run_id = event["workflow_run"]["id"]
    event_type = event["workflow_run"]["event"]
    fork_owner = event["workflow_run"]["head_repository"]["owner"]["login"]
    fork_branch = event["workflow_run"]["head_branch"]
    name = event["workflow_run"]["name"]
    workflow_id = event["workflow_run"]["workflow_id"]
    conclusion = event["workflow_run"]["conclusion"]
    attempt = event["workflow_run"]["run_attempt"]
    status = event["workflow_run"]["status"]
    jobs_url = event["workflow_run"]["jobs_url"]
    rerun_url = event["workflow_run"]["rerun_url"]
    url = event["workflow_run"]["html_url"]
    api_url = event["workflow_run"]["url"]
    return WorkflowDescription(
        name=name,
        action=action,
        run_id=run_id,
        event=event_type,
        fork_owner_login=fork_owner,
        fork_branch=fork_branch,
        workflow_id=workflow_id,
        conclusion=conclusion,
        attempt=attempt,
        status=status,
        jobs_url=jobs_url,
        rerun_url=rerun_url,
        url=url,
        api_url=api_url,
    )


def get_pr_author_and_orgs(pull_request):
    author = pull_request["user"]["login"]
    orgs = _exec_get_with_retry(pull_request["user"]["organizations_url"])
    return author, [org["id"] for org in orgs]


def get_changed_files_for_pull_request(pull_request):
    number = pull_request["number"]

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
            # print("Adding changed file", change['filename'])
            changed_files.add(change["filename"])

        if len(changed_files) >= SUSPICIOUS_CHANGED_FILES_NUMBER:
            print(
                f"More than {len(changed_files)} changed files. "
                "Will stop fetching new files."
            )
            break

    return changed_files


def check_suspicious_changed_files(changed_files):
    if len(changed_files) >= SUSPICIOUS_CHANGED_FILES_NUMBER:
        print(f"Too many files changed {len(changed_files)}, need manual approve")
        return True

    for path in changed_files:
        for pattern in SUSPICIOUS_PATTERNS:
            if fnmatch.fnmatch(path, pattern):
                print(
                    f"File {path} match suspicious pattern {pattern}, "
                    "will not approve automatically"
                )
                return True

    print("No changed files match suspicious patterns, run will be approved")
    return False


def approve_run(run_id, token):
    url = f"{API_URL}/actions/runs/{run_id}/approve"
    _exec_post_with_retry(url, token)


def label_manual_approve(pull_request, token):
    number = pull_request["number"]
    url = f"{API_URL}/issues/{number}/labels"
    data = {"labels": "manual approve"}

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


def get_workflow_jobs(workflow_description):
    jobs_url = (
        workflow_description.api_url + f"/attempts/{workflow_description.attempt}/jobs"
    )
    jobs = []
    i = 1
    while True:
        got_jobs = _exec_get_with_retry(jobs_url + f"?page={i}")
        if len(got_jobs["jobs"]) == 0:
            break

        jobs += got_jobs["jobs"]
        i += 1

    return jobs


def check_need_to_rerun(workflow_description):
    if workflow_description.attempt >= MAX_WORKFLOW_RERUN:
        print(
            "Not going to rerun workflow because it's already tried more than two times"
        )
        return False
    print("Going to check jobs")

    jobs = get_workflow_jobs(workflow_description)
    print("Got jobs", len(jobs))
    for job in jobs:
        if job["conclusion"] not in ("success", "skipped"):
            print("Job", job["name"], "failed, checking steps")
            for step in job["steps"]:
                # always the last job
                if step["name"] == "Complete job":
                    print("Found Complete job step for job", job["name"])
                    break
            else:
                print(
                    "Checked all steps and doesn't found Complete job, going to rerun"
                )
                return True

    return False


def rerun_workflow(workflow_description, token):
    print("Going to rerun workflow")
    try:
        _exec_post_with_retry(f"{workflow_description.rerun_url}-failed-jobs", token)
    except Exception:
        _exec_post_with_retry(workflow_description.rerun_url, token)


def main(event):
    token = get_token_from_aws()
    event_data = json.loads(event["body"])
    print("The body received:", event["body"])
    workflow_description = get_workflow_description_from_event(event_data)

    print("Got workflow description", workflow_description)
    if (
        workflow_description.action == "completed"
        and workflow_description.conclusion == "failure"
    ):
        print(
            "Workflow",
            workflow_description.url,
            "completed and failed, let's check for rerun",
        )

        if workflow_description.name not in NEED_RERUN_WORKFLOWS:
            print(
                "Workflow",
                workflow_description.name,
                "not in list of rerunable workflows",
            )
            return

        if check_need_to_rerun(workflow_description):
            rerun_workflow(workflow_description, token)
            return

    if workflow_description.action != "requested":
        print("Exiting, event action is", workflow_description.action)
        return

    if workflow_description.workflow_id in TRUSTED_WORKFLOW_IDS:
        print("Workflow in trusted list, approving run")
        approve_run(workflow_description.run_id, token)
        return

    pull_requests = _get_pull_requests_from(
        workflow_description.fork_owner_login, workflow_description.fork_branch
    )

    print("Got pull requests for workflow", len(pull_requests))
    if len(pull_requests) > 1:
        raise Exception("Received more than one PR for workflow run")

    if len(pull_requests) < 1:
        raise Exception("Cannot find any pull requests for workflow run")

    pull_request = pull_requests[0]
    print("Pull request for workflow number", pull_request["number"])

    author, author_orgs = get_pr_author_and_orgs(pull_request)
    if is_trusted_contributor(author, author_orgs):
        print("Contributor is trusted, approving run")
        approve_run(workflow_description.run_id, token)
        return

    changed_files = get_changed_files_for_pull_request(pull_request)
    print(f"Totally have {len(changed_files)} changed files in PR:", changed_files)
    if check_suspicious_changed_files(changed_files):
        print(
            f"Pull Request {pull_request['number']} has suspicious changes, "
            "label it for manuall approve"
        )
        label_manual_approve(pull_request, token)
    else:
        print(f"Pull Request {pull_request['number']} has no suspicious changes")
        approve_run(workflow_description.run_id, token)


def handler(event, _):
    main(event)
