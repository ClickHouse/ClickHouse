"""
Triggered by the mintlify_preview.yml workflow_run workflow after the PR
workflow completes. Checks whether "Docs check (Mintlify)" specifically
passed in the triggering run, then triggers a Mintlify preview deployment
and posts the result as a GitHub commit status.

Runs on the CI runner host (not inside docker), so the AWS CLI can
authenticate via the EC2 metadata service. The workflow_run trigger always
runs in the base-repo context, so SSM access works for fork PRs too.
"""

import json
import os
import subprocess
import sys
import time
import urllib.request

DOCS_JOB_NAME = "Docs check (Mintlify)"
STATUS_CONTEXT = "Mintlify Preview"
MINTLIFY_API_URL = "https://api.mintlify.com/v1"
POLL_INTERVAL_SEC = 15
POLL_TIMEOUT_SEC = 600


def gh_api(method, path, token, **body):
    req = urllib.request.Request(
        f"https://api.github.com{path}",
        data=json.dumps(body).encode() if body else None,
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github+json",
            "Content-Type": "application/json",
        },
        method=method,
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())


def post_status(repo, sha, token, state, description, url=""):
    kwargs = dict(state=state, description=description, context=STATUS_CONTEXT)
    if url:
        kwargs["target_url"] = url
    gh_api("POST", f"/repos/{repo}/statuses/{sha}", token, **kwargs)


def ssm_get(name):
    return subprocess.check_output(
        [
            "aws", "ssm", "get-parameter",
            "--name", name,
            "--with-decryption",
            "--output", "text",
            "--query", "Parameter.Value",
        ],
        text=True,
    ).strip()


def mintlify_api(method, path, token, body=None):
    req = urllib.request.Request(
        f"{MINTLIFY_API_URL}{path}",
        data=json.dumps(body).encode() if body else None,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        method=method,
    )
    with urllib.request.urlopen(req, timeout=30) as r:
        return json.loads(r.read())


def main():
    repo = os.environ["WR_REPO"]
    sha = os.environ["WR_SHA"]
    branch = os.environ["WR_BRANCH"]
    run_id = os.environ["WR_RUN_ID"]
    gh_token = os.environ["GH_TOKEN"]

    # Check whether the docs check job ran and passed in the triggering run.
    # A cache-hit job has conclusion "skipped"; a job that did not run at all
    # is absent. Both cases mean there are no new docs to preview.
    jobs_resp = gh_api(
        "GET",
        f"/repos/{repo}/actions/runs/{run_id}/jobs?per_page=100",
        gh_token,
    )
    docs_job = next(
        (j for j in jobs_resp["jobs"] if j["name"] == DOCS_JOB_NAME), None
    )
    if not docs_job:
        print(f"[{DOCS_JOB_NAME}] was not part of this run — skipping preview")
        return
    if docs_job["conclusion"] != "success":
        print(
            f"[{DOCS_JOB_NAME}] concluded [{docs_job['conclusion']}] — skipping preview"
        )
        return

    print(f"[{DOCS_JOB_NAME}] passed — triggering preview for branch [{branch}]")
    post_status(repo, sha, gh_token, "pending", "Building Mintlify preview...")

    try:
        api_key = ssm_get("mintlify-api-key")
        project_id = ssm_get("mintlify-project-id")

        resp = mintlify_api(
            "POST",
            f"/project/preview/{project_id}",
            api_key,
            body={"branch": branch},
        )
        status_id = resp["statusId"]
        preview_url = resp.get("previewUrl", "")
        print(f"Triggered deployment [{status_id}], preview url: {preview_url}")

        deployment = {}
        status = ""
        deadline = time.time() + POLL_TIMEOUT_SEC
        while time.time() < deadline:
            time.sleep(POLL_INTERVAL_SEC)
            deployment = mintlify_api(
                "GET", f"/project/update-status/{status_id}", api_key
            )
            status = deployment.get("status", "")
            print(f"Deployment [{status_id}] status [{status}]")
            if status in ("success", "failure"):
                break

        if status == "success":
            post_status(repo, sha, gh_token, "success", "Preview ready", preview_url)
            print(f"Preview ready: {preview_url}")
        else:
            for log in deployment.get("logs") or []:
                print(log)
            summary = deployment.get("summary", "")
            post_status(
                repo, sha, gh_token,
                "failure",
                f"Preview deployment {status}: {summary}",
                preview_url,
            )
            sys.exit(1)
    except Exception as e:
        print(f"Preview failed: {type(e).__name__}: {e}")
        post_status(repo, sha, gh_token, "failure", f"Preview error: {type(e).__name__}")
        sys.exit(1)


if __name__ == "__main__":
    main()
