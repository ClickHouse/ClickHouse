import json
import os
import traceback
import urllib.request
from pathlib import Path

from praktika.info import Info


def refresh():
    info = Info()

    try:
        loom_url = info.get_secret("loom-url").rstrip("/")
        token = info.get_secret("loom-ci-token")
    except Exception:
        info.add_workflow_warning(
            "loom secrets unavailable - skipping code index refresh"
        )
        return

    # Advisory: the push head. The merge queue batches 1..N PRs per
    # push; loom ingests to the branch tip regardless, so no per-PR
    # handling is needed.
    expected_head = ""
    event_path = os.getenv("GITHUB_EVENT_PATH", "")
    if event_path and Path(event_path).is_file():
        with open(event_path, encoding="utf-8") as f:
            expected_head = json.load(f).get("after", "")

    # org/namespace are REQUIRED in the body: the token is
    # namespace-scoped (no global roles), and a body without a
    # namespace makes loom's transport check global roles - every
    # call would 403.
    body = json.dumps(
        {
            "org": "clickhouse",
            "namespace": "code-clickhouse",
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
    with urllib.request.urlopen(req, timeout=10) as r:
        print(f"loom code.refresh: HTTP {r.status}")


if __name__ == "__main__":
    try:
        refresh()
    except Exception:
        # Best-effort by design: loom's poller heals missed triggers.
        traceback.print_exc()
        Info().add_workflow_warning(
            "loom code.refresh failed - index freshness degraded until next poll"
        )
