"""
Post-hook: trigger a Mintlify preview deployment for the current PR branch.

Runs on the CI runner host (not inside docker), so the AWS CLI can authenticate
via the EC2 metadata service without requiring --network=host in the container.
Only fires when the main docs check passed and this is a PR on the public repo.
"""
import json
import time
import urllib.request

from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.secret import Secret
from ci.praktika.settings import Settings
from ci.praktika.utils import Utils

PUBLIC_REPO = "ClickHouse/ClickHouse"
DOCS_JOB_NAME = "Docs check (Mintlify)"
MINTLIFY_API_URL = "https://api.mintlify.com/v1"
POLL_INTERVAL_SEC = 15
POLL_TIMEOUT_SEC = 600


def _api_request(method, url, token, body=None):
    request = urllib.request.Request(
        url,
        data=json.dumps(body).encode() if body is not None else None,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
        method=method,
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode())


def main():
    info = Info()

    if info.repo_name != PUBLIC_REPO:
        print(f"Skipping preview: repo [{info.repo_name}] is not [{PUBLIC_REPO}]")
        return
    if not info.pr_number:
        print("Skipping preview: not a PR run")
        return

    try:
        job_result = Result.from_fs(DOCS_JOB_NAME)
        if not job_result.is_ok():
            print(
                f"Skipping preview: docs check did not pass (status: {job_result.status})"
            )
            return
    except Exception as e:
        print(f"Skipping preview: could not read docs job result: {e}")
        return

    try:
        api_key, project_id = Secret.Config(
            name=["mintlify-api-key", "mintlify-project-id"],
            type=Secret.Type.AWS_SSM_PARAMETER,
        ).get_value()

        response = _api_request(
            "POST",
            f"{MINTLIFY_API_URL}/project/preview/{project_id}",
            api_key,
            body={"branch": info.git_branch},
        )
        status_id = response["statusId"]
        preview_url = response.get("previewUrl", "")
        print(
            f"Triggered preview deployment [{status_id}] for branch [{info.git_branch}], preview url: {preview_url}"
        )

        deployment = {}
        status = ""
        deadline = time.time() + POLL_TIMEOUT_SEC
        while time.time() < deadline:
            time.sleep(POLL_INTERVAL_SEC)
            deployment = _api_request(
                "GET",
                f"{MINTLIFY_API_URL}/project/update-status/{status_id}",
                api_key,
            )
            status = deployment.get("status", "")
            print(f"Preview deployment [{status_id}] status [{status}]")
            if status in ("success", "failure"):
                break

        if status == "success":
            print(f"Preview deployment succeeded: {preview_url}")
        else:
            for log in deployment.get("logs") or []:
                print(log)
            print(
                f"WARNING: preview deployment ended with status [{status}]: {deployment.get('summary', '')}"
            )
    except Exception as e:
        print(f"WARNING: preview not deployed (non-blocking): {type(e).__name__}: {e}")


if __name__ == "__main__":
    main()
