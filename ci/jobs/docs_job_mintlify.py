import json
import time
import urllib.request

from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.secret import Secret
from ci.praktika.utils import Utils

MINTLIFY_API_URL = "https://api.mintlify.com/v1"
PREVIEW_STEP_NAME = "Trigger preview deployment"
PUBLIC_REPO = "ClickHouse/ClickHouse"
PREVIEW_POLL_INTERVAL_SEC = 15
PREVIEW_POLL_TIMEOUT_SEC = 600


def api_request(method, url, token, body=None):
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


def trigger_preview_deployment():
    stop_watch = Utils.Stopwatch()
    info = Info()

    def make_result(status, result_info, links=None):
        return Result.create_from(
            name=PREVIEW_STEP_NAME,
            stopwatch=stop_watch,
            status=status,
            info=result_info,
            links=links,
        )

    if info.repo_name != PUBLIC_REPO:
        return make_result(
            Result.Status.SKIPPED,
            f"skipped: repo [{info.repo_name}] is not [{PUBLIC_REPO}]",
        )
    if not info.pr_number:
        return make_result(Result.Status.SKIPPED, "skipped: not a PR run")

    try:
        api_key, project_id = Secret.Config(
            name=["mintlify-api-key", "mintlify-project-id"],
            type=Secret.Type.AWS_SSM_PARAMETER,
        ).get_value()

        response = api_request(
            "POST",
            f"{MINTLIFY_API_URL}/project/preview/{project_id}",
            api_key,
            body={"branch": info.git_branch},
        )
        status_id = response["statusId"]
        preview_url = response.get("previewUrl", "")
        print(
            f"Triggered preview deployment [{status_id}] for branch [{info.git_branch}], preview url [{preview_url}]"
        )

        links = [preview_url] if preview_url else None
        deployment = {}
        status = ""
        deadline = time.time() + PREVIEW_POLL_TIMEOUT_SEC
        while time.time() < deadline:
            time.sleep(PREVIEW_POLL_INTERVAL_SEC)
            deployment = api_request(
                "GET", f"{MINTLIFY_API_URL}/project/update-status/{status_id}", api_key
            )
            status = deployment.get("status", "")
            print(f"Preview deployment [{status_id}] status [{status}]")
            if status in ("success", "failure"):
                break

        if status == "success":
            return make_result(Result.Status.OK, f"preview: {preview_url}", links)

        for log in deployment.get("logs") or []:
            print(log)
        if status == "failure":
            return make_result(
                Result.Status.OK,
                f"WARNING: preview deployment failed (non-blocking): {deployment.get('summary', '')}",
                links,
            )
        return make_result(
            Result.Status.OK,
            f"WARNING: preview deployment did not finish in {PREVIEW_POLL_TIMEOUT_SEC}s, last status [{status}] (non-blocking)",
            links,
        )
    except Exception as e:
        # The preview is informational - never fail the job because of it
        # (network errors, plan limits, missing secrets, rate limiting, ...)
        return make_result(
            Result.Status.OK,
            f"WARNING: preview not deployed (non-blocking): {type(e).__name__}: {e}",
        )


if __name__ == "__main__":

    results = []
    stop_watch = Utils.Stopwatch()
    temp_dir = f"{Utils.cwd()}/ci/tmp/"

    docs_dir = f"{Utils.cwd()}/docs"

    results.append(
        Result.from_commands_run(
            name="Verify Mintlify docs.json file is valid",
            command=[
                "mint validate",
            ],
            workdir=docs_dir,
        )
    )

    results.append(
        Result.from_commands_run(
            name="Check for broken links",
            command=[
                "mint broken-links",
            ],
            workdir=docs_dir,
        )
    )

    if all(r.is_ok() for r in results):
        results.append(trigger_preview_deployment())
    else:
        results.append(
            Result.create_from(
                name=PREVIEW_STEP_NAME,
                stopwatch=Utils.Stopwatch(),
                status=Result.Status.SKIPPED,
                info="skipped: docs validation failed",
            )
        )

    Result.create_from(results=results).complete_job()
