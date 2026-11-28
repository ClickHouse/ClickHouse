import json
import os
import signal
import traceback
import urllib.error
import urllib.request
from pathlib import Path

from praktika.info import Info

# Bounds the WHOLE hook, including the SSM fetch: the boto3 call runs
# in-process, so SIGALRM interrupts it directly (no child process to leak),
# and a pre_hook blocks every job in the workflow, so a stalled credential
# path must become a warning, not a hang.
TOTAL_TIMEOUT_SEC = 30


def namespace_for(branch):
    # Naming convention only. WHICH branches are indexed is loom-side
    # config; a refresh for anything unconfigured 404s and is skipped.
    if branch == "master":
        return "code-clickhouse"
    return "code-clickhouse-" + branch.replace(".", "-")


def refresh():
    info = Info()

    try:
        # get_secret returns a Secret.Config handle; join_with batches both
        # SSM parameters into one get_parameters call, values in request
        # order. SIGALRM (see __main__) bounds this in-process boto3 call.
        loom_url, token = (
            info.get_secret("loom-url")
            .join_with(info.get_secret("loom-ci-token"))
            .get_value()
        )
        loom_url = loom_url.rstrip("/")
    except Exception:
        info.add_workflow_warning(
            "loom secrets unavailable - skipping code index refresh"
        )
        return

    # Push event: ref names the branch; "after" is the push head
    # (advisory - the merge queue batches 1..N PRs per push; loom
    # ingests to the branch tip regardless).
    branch = ""
    expected_head = ""
    event_path = os.getenv("GITHUB_EVENT_PATH", "")
    if event_path and Path(event_path).is_file():
        with open(event_path, encoding="utf-8") as f:
            event = json.load(f)
        expected_head = event.get("after", "")
        ref = event.get("ref", "")
        if ref.startswith("refs/heads/"):
            branch = ref[len("refs/heads/") :]
    if not branch:
        # Both consuming workflows are branch-only push workflows, so this is
        # never an expected tag case: the payload was unreadable or the
        # trigger drifted. Surface it, or the index goes stale in silence.
        info.add_workflow_warning(
            "loom code.refresh: no pushed branch in GITHUB_EVENT_PATH"
            f" [{event_path or 'unset'}] - skipped"
        )
        return

    # org/namespace are REQUIRED in the body: the token is
    # namespace-scoped (no global roles), and a body without a
    # namespace makes loom's transport check global roles - every
    # call would 403.
    body = json.dumps(
        {
            "org": "clickhouse",
            "namespace": namespace_for(branch),
            "consumer": "clickhouse-ci",
            "expected_head": expected_head,
        }
    ).encode()
    req = urllib.request.Request(  # urllib, not curl: token stays out of process args/logs
        f"{loom_url}/v1/code.refresh",
        data=body,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            print(f"loom code.refresh[{branch}]: HTTP {r.status}")
    except urllib.error.HTTPError as e:
        if e.code == 401 or (e.code in (403, 404) and branch == "master"):
            # 401 on ANY branch is a dead credential - an expired or
            # revoked CI token, never an unindexed branch. On master,
            # 403/404 is drift too - an ACL change (403), or a bad loom
            # URL, deleted namespace, or route change (404): master's
            # namespace must always exist and be covered by the token.
            # Surface both; the index silently going stale is exactly what
            # this hook exists to prevent.
            info.add_workflow_warning(
                f"loom code.refresh: HTTP {e.code} for {branch} - loom URL,"
                " namespace, or CI token misconfigured or expired"
            )
            return
        if e.code in (403, 404):
            # loom doesn't index this branch (404), or the token doesn't
            # cover its namespace (403). Expected for most release
            # branches - not a warning.
            print(
                f"loom code.refresh[{branch}]: HTTP {e.code}"
                " - branch not indexed, skipped"
            )
            return
        raise


def _timed_out(signum, frame):
    raise TimeoutError(f"loom code.refresh exceeded {TOTAL_TIMEOUT_SEC}s")


if __name__ == "__main__":
    signal.signal(signal.SIGALRM, _timed_out)
    signal.alarm(TOTAL_TIMEOUT_SEC)
    try:
        refresh()
    except Exception:
        # Best-effort by design: loom's poller heals missed triggers.
        traceback.print_exc()
        try:
            Info().add_workflow_warning(
                "loom code.refresh failed - index freshness degraded until next poll"
            )
        except Exception:
            # A non-zero exit from a pre_hook fails the workflow's config
            # job; refreshing loom is a helper, never worth failing CI for,
            # so even a failure to emit the warning must not leak out.
            traceback.print_exc()
