import json
import time
import urllib.request

from ci.jobs.scripts.docs.check_readonly_copies import check_readonly_copies
from ci.jobs.scripts.docs.mintlify_docs_check import DEFAULT_CHECKS
from ci.praktika.info import Info
from ci.praktika.result import Result
from ci.praktika.secret import Secret
from ci.praktika.utils import Utils

MINTLIFY_API_URL = "https://api.mintlify.com/v1"
PREVIEW_STEP_NAME = "Trigger preview deployment"
PUBLIC_REPO = "ClickHouse/ClickHouse"
PREVIEW_POLL_INTERVAL_SEC = 15
PREVIEW_POLL_TIMEOUT_SEC = 600


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
        # Credentials are GitHub Actions secrets injected by the workflow YAML.
        # GitHub withholds these from pull_request events from forks, so fork
        # PRs raise here and fall through to the non-blocking warning below.
        api_key = Secret.Config(
            name="MINTLIFY_API_KEY", type=Secret.Type.GH_SECRET
        ).get_value()
        project_id = Secret.Config(
            name="MINTLIFY_PROJECT_ID", type=Secret.Type.GH_SECRET
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

        links = [preview_url] if preview_url else None
        deployment = {}
        status = ""
        deadline = time.time() + PREVIEW_POLL_TIMEOUT_SEC
        while time.time() < deadline:
            time.sleep(PREVIEW_POLL_INTERVAL_SEC)
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
        # (missing secrets for fork PRs, network errors, plan limits, ...)
        return make_result(
            Result.Status.OK,
            f"WARNING: preview not deployed (non-blocking): {type(e).__name__}: {e}",
        )


def _readonly_copies_guard():
    # One-way sync: fail edits to docs folders whose canonical source lives in
    # another repo (declared in ci/jobs/scripts/docs/readonly_copies.json). This
    # is aggregator-only -- the consuming repos are the source of truth, so this
    # is deliberately not part of the shared DEFAULT_CHECKS.
    changed_files = Info().get_changed_files()
    if changed_files is None:
        # Fail close: without the changed-file list we cannot prove the PR does
        # not touch read-only copies, so do not report success.
        print("Error: the changed-file list is unavailable, cannot run the check.")
        return False
    return check_readonly_copies(changed_files)


if __name__ == "__main__":

    docs_dir = f"{Utils.cwd()}/docs"

    # The mint check definitions are shared with the standalone driver
    # (ci/jobs/scripts/docs/mintlify_docs_check.py). This job already runs inside
    # the docs-builder image with the docs present natively, so it runs them
    # directly; add new checks to DEFAULT_CHECKS, not here.
    results = [
        Result.from_commands_run(name=name, command=command, workdir=docs_dir)
        for name, command in DEFAULT_CHECKS
    ]

    results.append(
        Result.from_commands_run(
            name="No direct edits to read-only copied docs",
            command=_readonly_copies_guard,
        )
    )

    # The preview deployment is Praktika/aggregator-only on purpose: it is not
    # part of the shared DEFAULT_CHECKS, so consuming repos never trigger it.
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
