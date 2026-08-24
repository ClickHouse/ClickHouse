import json
import os
import traceback
import urllib.error
import urllib.request
from pathlib import Path

from praktika.info import Info


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
        # SSM parameters into one get-parameters call, values in request order.
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
        print("loom code.refresh: no pushed branch in event - skipped")
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


if __name__ == "__main__":
    try:
        refresh()
    except Exception:
        # Best-effort by design: loom's poller heals missed triggers.
        traceback.print_exc()
        Info().add_workflow_warning(
            "loom code.refresh failed - index freshness degraded until next poll"
        )
