"""Post sticky PromQL compliance Baseline/Current/Δ comment on PRs (LLVM hook pattern).

Reads ``./ci/tmp/promql_compliance_comment.json`` produced by ``promql_compliance_job.py``.
Requires ``enable_gh_auth`` on the parent job (see ``JobConfigs.promql_compliance_job``).
"""

from __future__ import annotations

import json
import traceback
from pathlib import Path

from ci.jobs.scripts.job_hooks.promql_compliance_s3 import result_url_for_master_commit
from ci.praktika.gh import GH
from ci.praktika.info import Info

COMMENT_TAG = "promql-compliance"
COMMENT_FILE = Path("./ci/tmp/promql_compliance_comment.json")
_EPS = 1e-4


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


def _build_body(d: dict) -> str:
    baseline_source = d["baseline_source"]
    from_zero = d["from_zero"]
    s3_sha = d.get("s3_sha")
    new_pct = float(d["new_pct"])
    base_pct = float(d["base_pct"])
    cur_passed = int(d["cur_passed"])
    cur_failed = int(d["cur_failed"])
    cur_unsup = int(d["cur_unsup"])
    base_passed = int(d["base_passed"])
    base_failed = int(d["base_failed"])
    base_unsup = int(d["base_unsup"])
    delta = float(d["delta"])
    result_json_url = d.get("result_json_url") or ""

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

    if result_json_url:
        body += (
            f"\n[Uploaded compliance JSON for this PR commit]({result_json_url})\n"
        )

    if from_zero:
        body += _no_s3_baseline_block()
    elif delta > _EPS and s3_sha:
        body += _bump_baseline_block_s3(new_pct, base_pct, s3_sha)
    elif not from_zero and delta < -_EPS:
        body += (
            "\nNote: score is below the chosen baseline (informational only; "
            "this job does not enforce a hard floor).\n"
        )

    return body


def check() -> None:
    info = Info()

    if not COMMENT_FILE.is_file():
        print(
            f"PromQL compliance comment hook: data not found at {COMMENT_FILE}, skipping"
        )
        return

    if info.pr_number <= 0:
        print("PromQL compliance comment hook: not a PR run, skipping GitHub comment")
        return

    try:
        with open(COMMENT_FILE, encoding="utf-8") as f:
            d = json.load(f)

        for k in (
            "baseline_source",
            "from_zero",
            "new_pct",
            "base_pct",
            "cur_passed",
            "cur_failed",
            "cur_unsup",
            "base_passed",
            "base_failed",
            "base_unsup",
            "delta",
        ):
            if k not in d:
                print(f"PromQL compliance comment hook: payload missing key {k!r}")
                return

        body = _build_body(d)
        if not GH.post_fresh_comment(tag=COMMENT_TAG, body=body):
            raise RuntimeError(
                "PromQL compliance comment hook: failed to post GitHub comment "
                "(gh pr comment returned failure)"
            )
    except Exception:
        print("ERROR: PromQL compliance comment hook failed")
        traceback.print_exc()
        raise


if __name__ == "__main__":
    check()
