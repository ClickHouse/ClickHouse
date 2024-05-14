#!/usr/bin/env python3

import fnmatch
import json
import time
from collections import namedtuple
from urllib.parse import quote

import requests
from lambda_shared.pr import TRUSTED_CONTRIBUTORS
from lambda_shared.token import get_cached_access_token

SUSPICIOUS_CHANGED_FILES_NUMBER = 200

SUSPICIOUS_PATTERNS = [
    ".github/*",
    "docker/*",
    "docs/tools/*",
    "packages/*",
    "tests/ci/*",
]

# Number of retries for API calls.
MAX_RETRY = 5

# Number of times a check can re-run as a whole.
# It is needed, because we are using AWS "spot" instances, that are terminated often
MAX_WORKFLOW_RERUN = 30

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
        "repo_url",
        "url",
    ],
)

# See https://api.github.com/orgs/{name}
TRUSTED_ORG_IDS = {
    54801242,  # clickhouse
}

# See https://api.github.com/repos/ClickHouse/ClickHouse/actions/workflows
# Use ID to not inject a malicious workflow
TRUSTED_WORKFLOW_IDS = {
    14586616,  # Cancel workflows, always trusted
}

NEED_RERUN_WORKFLOWS = {
    "BackportPR",
    "DocsCheck",
    "MasterCI",
    "NightlyBuilds",
    "PublishedReleaseCI",
    "PullRequestCI",
    "ReleaseBranchCI",
}


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


def _exec_get_with_retry(url, token):
    headers = {"Authorization": f"token {token}"}
    e = Exception()
    for i in range(MAX_RETRY):
        try:
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            return response.json()
        except Exception as ex:
            print("Got exception executing request", ex)
            e = ex
            time.sleep(i + 1)

    raise requests.HTTPError("Cannot execute GET request with retries") from e


def _exec_post_with_retry(url, token, data=None):
    headers = {"Authorization": f"token {token}"}
    e = Exception()
    for i in range(MAX_RETRY):
        try:
            if data:
                response = requests.post(url, headers=headers, json=data, timeout=30)
            else:
                response = requests.post(url, headers=headers, timeout=30)
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
            e = ex
            time.sleep(i + 1)

    raise requests.HTTPError("Cannot execute POST request with retry") from e


def _get_pull_requests_from(repo_url, owner, branch, token):
    url = f"{repo_url}/pulls?head={quote(owner)}:{quote(branch)}"
    return _exec_get_with_retry(url, token)


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
    repo_url = event["repository"]["url"]
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
        repo_url=repo_url,
        api_url=api_url,
    )


def get_pr_author_and_orgs(pull_request, token):
    author = pull_request["user"]["login"]
    orgs = _exec_get_with_retry(pull_request["user"]["organizations_url"], token)
    return author, [org["id"] for org in orgs]


def get_changed_files_for_pull_request(pull_request, token):
    url = pull_request["url"]

    changed_files = set([])
    for i in range(1, 31):
        print("Requesting changed files page", i)
        data = _exec_get_with_retry(f"{url}/files?page={i}&per_page=100", token)
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

    print("No changed files match suspicious patterns, run could be approved")
    return False


def approve_run(workflow_description: WorkflowDescription, token: str) -> None:
    print("Approving run")
    url = f"{workflow_description.api_url}/approve"
    _exec_post_with_retry(url, token)


def label_manual_approve(pull_request, token):
    url = f"{pull_request['issue_url']}/labels"
    data = {"labels": ["manual approve"]}

    _exec_post_with_retry(url, token, data)


def get_workflow_jobs(workflow_description, token):
    jobs_url = (
        workflow_description.api_url + f"/attempts/{workflow_description.attempt}/jobs"
    )
    jobs = []
    i = 1
    while True:
        got_jobs = _exec_get_with_retry(jobs_url + f"?page={i}", token)
        if len(got_jobs["jobs"]) == 0:
            break

        jobs += got_jobs["jobs"]
        i += 1

    return jobs


def check_need_to_rerun(workflow_description, token):
    if workflow_description.attempt >= MAX_WORKFLOW_RERUN:
        print(
            "Not going to rerun workflow because it's already tried more than two times"
        )
        return False
    print("Going to check jobs")

    jobs = get_workflow_jobs(workflow_description, token)
    print("Got jobs", len(jobs))
    for job in jobs:
        print(f"Job {job['name']} has a conclusion '{job['conclusion']}'")
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


def check_workflow_completed(
    event_data: dict, workflow_description: WorkflowDescription, token: str
) -> bool:
    if workflow_description.action == "completed":
        attempt = 0
        # Nice and reliable GH API sends from time to time such events, e.g:
        # action='completed', conclusion=None, status='in_progress',
        # So let's try receiving a real workflow data
        while workflow_description.conclusion is None and attempt < MAX_RETRY:
            progressive_sleep = 3 * sum(i + 1 for i in range(attempt))
            time.sleep(progressive_sleep)
            event_data["workflow_run"] = _exec_get_with_retry(
                workflow_description.api_url, token
            )
            workflow_description = get_workflow_description_from_event(event_data)
            attempt += 1

        if workflow_description.conclusion != "failure":
            print(
                "Workflow finished with status "
                f"{workflow_description.conclusion}, exiting"
            )
            return True

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
            return True

        if check_need_to_rerun(workflow_description, token):
            rerun_workflow(workflow_description, token)
            return True

    return False


def main(event):
    token = get_cached_access_token()
    event_data = json.loads(event["body"])
    print("The body received:", event["body"])
    workflow_description = get_workflow_description_from_event(event_data)

    print("Got workflow description", workflow_description)
    if check_workflow_completed(event_data, workflow_description, token):
        return

    if workflow_description.action != "requested":
        print("Exiting, event action is", workflow_description.action)
        return

    if workflow_description.workflow_id in TRUSTED_WORKFLOW_IDS:
        print("Workflow in trusted list, approving run")
        approve_run(workflow_description, token)
        return

    pull_requests = _get_pull_requests_from(
        workflow_description.repo_url,
        workflow_description.fork_owner_login,
        workflow_description.fork_branch,
        token,
    )

    print("Got pull requests for workflow", len(pull_requests))
    if len(pull_requests) != 1:
        print(f"Can't continue with non-uniq PRs: {pull_requests}")
        return

    pull_request = pull_requests[0]
    print("Pull request for workflow number", pull_request["number"])

    author, author_orgs = get_pr_author_and_orgs(pull_request, token)
    if is_trusted_contributor(author, author_orgs):
        print("Contributor is trusted, approving run")
        approve_run(workflow_description, token)
        return

    labels = {label["name"] for label in pull_request["labels"]}
    if "can be tested" not in labels:
        print("Label 'can be tested' is required for untrusted users")
        return

    changed_files = get_changed_files_for_pull_request(pull_request, token)
    print(f"Totally have {len(changed_files)} changed files in PR:", changed_files)
    if check_suspicious_changed_files(changed_files):
        print(f"Pull Request {pull_request['number']} has suspicious changes")
        if "manual approve" not in labels:
            print("Label the PR as needed for manuall approve")
            label_manual_approve(pull_request, token)
    else:
        print(f"Pull Request {pull_request['number']} has no suspicious changes")
        approve_run(workflow_description, token)


def handler(event, _):
    try:
        main(event)

        return {
            "statusCode": 200,
            "headers": {"Content-Type": "application/json"},
            "body": '{"status": "OK"}',
        }
    except Exception:
        print("Received event: ", event)
        raise
