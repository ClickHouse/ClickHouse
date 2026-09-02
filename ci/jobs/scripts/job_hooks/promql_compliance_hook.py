"""Post LLVM-style PromQL compliance Baseline/Current/Δ comment on PRs (label-gated).

Runs as a post-hook after each Integration tests batch. Only the batch that
executed ``test_compliance.py`` leaves the JSON path given by
``COMPLIANCE_RESULT_FILE`` (default ``./ci/tmp/promql_compliance_result.json``,
same default as ``integration_test_job.py``).

Baseline (PRs): first available ``promql_compliance_result.json`` on S3 under
``REFs/master/<sha>/promql_compliance/`` for SHAs in ``master_track_commits_sha``
(same walk idea as LLVM coverage). If none exists, the baseline in the table is
zero (0% score, 0 / 0 / 0 counts) and the comment states that explicitly.

Upstream ``master`` pushes: uploads the result JSON to that S3 path so the next
PRs can use it as baseline.

Posts when the PR is labeled ``comp-promql`` and the score delta vs the chosen
baseline is non-zero. Never fails the job on GH errors.
"""

from __future__ import annotations

import json
import math
import os
import traceback
from pathlib import Path

from ci.jobs.scripts.job_hooks.promql_compliance_s3 import (
    fetch_baseline_from_s3,
    master_track_commits,
    should_publish_master_baseline,
    upload_master_result,
    result_url_for_master_commit,
)
from ci.jobs.scripts.workflow_hooks.pr_labels_and_category import Labels
from ci.praktika.gh import GH
from ci.praktika.info import Info

RESULT_FILE = Path(
    os.environ.get(
        "COMPLIANCE_RESULT_FILE",
        os.path.join(".", "ci", "tmp", "promql_compliance_result.json"),
    )
)
EPS = 1e-4
COMMENT_TAG = "promql-compliance"

_ZERO_BASELINE = {
    "pct": 0.0,
    "passed": 0,
    "failed": 0,
    "unsupported": 0,
}


def _bump_baseline_block_s3(new_pct: float, base_pct: float, base_sha: str) -> str:
    return (
        "\n### Baseline update\n\n"
        f"This run reports {new_pct:.2f}% vs master baseline {base_pct:.2f}% "
        f"([`{base_sha[:9]}`]({result_url_for_master_commit(base_sha)})). "
        "Merging to `master` will publish a new S3 object from the integration job that runs "
        "`test_promql_compliance`, which becomes the baseline for later PRs.\n"
    )


def _no_s3_baseline_block() -> str:
    return (
        "\n### No master baseline on S3 yet\n\n"
        "None of the checked `master` commits had "
        "`REFs/master/<sha>/promql_compliance/promql_compliance_result.json`. "
        "The table above uses zero as the baseline so you still see current "
        "scores and deltas. After a green `master` integration run uploads that "
        "object, PRs will compare against the published master snapshot instead.\n"
    )


def check() -> None:
    try:
        _check_impl()
    except Exception:
        print("WARNING: PromQL compliance hook failed with unexpected error")
        traceback.print_exc()


def _check_impl() -> None:
    if not RESULT_FILE.is_file():
        print(
            f"PromQL compliance hook: no result at {RESULT_FILE} (batch did not run test_compliance), skip"
        )
        return

    info = Info()

    if should_publish_master_baseline(info):
        sha = (info.sha or "").strip()
        if len(sha) == 40:
            upload_master_result(RESULT_FILE, sha)
        else:
            print(f"PromQL compliance hook: skip S3 upload, unexpected SHA [{sha!r}]")
        print("PromQL compliance hook: master / non-PR run, skip PR comment")
        return

    if info.pr_number <= 0:
        print("PromQL compliance hook: not a PR run, skip comment")
        return

    if not _pr_has_comp_promql(info):
        print(
            f"PromQL compliance hook: PR not labeled '{Labels.COMP_PROMQL}', skip comment"
        )
        return

    try:
        new = json.loads(RESULT_FILE.read_text())
    except Exception as e:
        print(f"PromQL compliance hook: failed to read result JSON: {e}")
        traceback.print_exc()
        return

    for _k in ("pct", "passed", "failed", "unsupported"):
        if _k not in new:
            print(f"PromQL compliance hook: result JSON missing key {_k!r}")
            return

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
            "PromQL compliance hook: invalid result or baseline "
            f"(missing keys or non-numeric pct/counts): {e}"
        )
        return

    if not math.isfinite(new_pct) or not math.isfinite(base_pct):
        print(
            "PromQL compliance hook: result or baseline has non-finite 'pct'; skip comment"
        )
        return

    delta = new_pct - base_pct
    if abs(delta) < EPS:
        print("PromQL compliance hook: delta vs baseline is ~0, skip comment")
        return

    dp = new_pct - base_pct
    d_passed = cur_passed - base_passed
    d_failed = cur_failed - base_failed
    d_unsup = cur_unsup - base_unsup

    body = (
        "### PromQL Compliance Report\n\n"
        f"Baseline: {baseline_source}\n\n"
        "| Metric | Baseline | Current | Δ |\n"
        "|--------|----------|---------|---|\n"
        f"| Score | {base_pct:.2f}% | {new_pct:.2f}% | {dp:+.2f}% |\n"
        f"| Passed | {base_passed} | {cur_passed} | {d_passed:+d} |\n"
        f"| Failed | {base_failed} | {cur_failed} | {d_failed:+d} |\n"
        f"| Unsupported | {base_unsup} | {cur_unsup} | {d_unsup:+d} |\n"
    )

    report_url = ""
    try:
        report_url = info.get_job_report_url(latest=False)
    except Exception:
        pass
    if report_url:
        body += f"\n[CI report for this job]({report_url})\n"

    if from_zero:
        body += _no_s3_baseline_block()
    elif delta > EPS and s3_sha:
        body += _bump_baseline_block_s3(new_pct, base_pct, s3_sha)
    elif not from_zero and delta < -EPS:
        body += (
            "\nNote: score is below the chosen baseline (informational only; "
            "this job does not enforce a hard floor).\n"
        )

    try:
        GH.post_fresh_comment(tag=COMMENT_TAG, body=body)
    except Exception:
        print("WARNING: PromQL compliance hook failed to post GitHub comment")
        traceback.print_exc()


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


if __name__ == "__main__":
    check()
