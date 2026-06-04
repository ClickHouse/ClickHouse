"""Dedicated ``PromQL Compliance`` job: fetch PR compliance JSON from S3, compare to master baseline, write comment payload for the post-hook."""

from __future__ import annotations

import json
import math
import sys
import traceback
import urllib.error
import urllib.request
from pathlib import Path

from ci.jobs.scripts.job_hooks.promql_compliance_s3 import (
    fetch_baseline_from_s3,
    master_track_commits,
    result_url_for_pr_commit,
)
from ci.jobs.scripts.workflow_hooks.pr_labels_and_category import Labels
from ci.praktika.gh import GH
from ci.praktika.info import Info
from ci.praktika.result import Result

COMMENT_OUT = Path("./ci/tmp/promql_compliance_comment.json")

_EPS = 1e-4

_ZERO_BASELINE = {
    "pct": 0.0,
    "passed": 0,
    "failed": 0,
    "unsupported": 0,
}


def _finish_ok(info: str = "") -> int:
    """Finalize Praktika job Result (pre-run leaves status RUNNING until we dump)."""
    Result.create_from(status=Result.Status.OK, info=info).dump()
    return 0


def _finish_error(info: str) -> int:
    """Record a visible handoff failure (job has allow_failure=True)."""
    Result.create_from(status=Result.Status.ERROR, info=info).dump()
    return 1


def _http_get_json(url: str):
    """Return parsed JSON, or None only when the PR object is missing (HTTP 404)."""
    req = urllib.request.Request(
        url,
        headers={"User-Agent": "ClickHouse-CI-promql-compliance"},
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            raw = resp.read()
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return None
        raise
    try:
        return json.loads(raw.decode("utf-8"))
    except json.JSONDecodeError as e:
        raise ValueError(f"malformed compliance JSON at {url}: {e}") from e


def _pr_has_comp_promql(info: Info) -> bool:
    labels = list(info.pr_labels or [])
    if Labels.COMP_PROMQL in labels:
        return True
    if info.is_local_run or not info.pr_number:
        return False
    try:
        remote_labels = GH.get_pr_labels(pr=info.pr_number)
        if Labels.COMP_PROMQL in remote_labels:
            return True
    except Exception:
        pass
    return False


def main() -> int:
    info = Info()

    if info.pr_number <= 0:
        print(
            "PromQL compliance job: not a PR run; "
            "master baseline upload is handled by integration-test upload hook."
        )
        return _finish_ok("Not a PR run.")

    if not _pr_has_comp_promql(info):
        print(f"PromQL compliance job: PR not labeled '{Labels.COMP_PROMQL}', skip.")
        return _finish_ok(f"PR not labeled '{Labels.COMP_PROMQL}'.")

    sha = (info.sha or "").strip()
    if len(sha) != 40:
        print(f"PromQL compliance job: unexpected SHA [{sha!r}], skip.")
        return _finish_ok("Unexpected commit SHA.")

    url = result_url_for_pr_commit(info.pr_number, sha)
    try:
        new = _http_get_json(url)
    except Exception as e:
        print(f"PromQL compliance job: failed to fetch PR compliance JSON from S3: {e}")
        traceback.print_exc()
        return _finish_error(f"Failed to fetch PR compliance JSON: {e}")

    if new is None:
        print(
            "PromQL compliance job: no PR-scoped JSON in S3 "
            "(integration batch likely did not run test_compliance), skip."
        )
        return _finish_ok("No PR compliance JSON in S3 yet (404).")

    for _k in ("pct", "passed", "failed", "unsupported"):
        if _k not in new:
            print(f"PromQL compliance job: result JSON missing key {_k!r}")
            return _finish_error(f"Result JSON missing key {_k!r}.")


    commits = master_track_commits(info)
    base, s3_sha = fetch_baseline_from_s3(commits)
    if base is not None and s3_sha:
        baseline_source = f"S3 master `{s3_sha[:9]}`"
        from_zero = False
    else:
        base = dict(_ZERO_BASELINE)
        baseline_source = "none on S3 (baseline 0 — see note below)"
        from_zero = True
        s3_sha = None

    try:
        new_pct = float(new["pct"])
        base_pct = float(base["pct"])
        cur_passed = int(new["passed"])
        cur_failed = int(new["failed"])
        cur_unsup = int(new["unsupported"])
        base_passed = int(base["passed"])
        base_failed = int(base["failed"])
        base_unsup = int(base["unsupported"])
    except (KeyError, TypeError, ValueError) as e:
        print(
            "PromQL compliance job: invalid result or baseline "
            f"(missing keys or non-numeric pct/counts): {e}"
        )
        return _finish_error(f"Invalid numeric fields in result/baseline: {e}")

    if not math.isfinite(new_pct) or not math.isfinite(base_pct):
        print(
            "PromQL compliance job: result or baseline has non-finite 'pct'; skip."
        )
        return _finish_error("Non-finite pct in result or baseline.")

    delta = new_pct - base_pct
    if abs(delta) < _EPS:
        print("PromQL compliance job: delta vs baseline is ~0, skip comment payload.")
        return _finish_ok("Delta vs baseline ~0; no PR comment.")

    payload = {
        "baseline_source": baseline_source,
        "from_zero": from_zero,
        "s3_sha": s3_sha,
        "new_pct": new_pct,
        "base_pct": base_pct,
        "cur_passed": cur_passed,
        "cur_failed": cur_failed,
        "cur_unsup": cur_unsup,
        "base_passed": base_passed,
        "base_failed": base_failed,
        "base_unsup": base_unsup,
        "delta": delta,
        "result_json_url": url,
    }

    COMMENT_OUT.parent.mkdir(parents=True, exist_ok=True)
    with open(COMMENT_OUT, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
        f.write("\n")

    print(f"PromQL compliance job: wrote comment payload to {COMMENT_OUT}")
    return _finish_ok("Wrote comment payload for post-hook.")


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        traceback.print_exc()
        sys.exit(1)
