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
        "The Prometheus compliance row uses zero as the baseline so you still see current "
        "scores and deltas. Extended support shows no baseline until a master run uploads "
        "schema version 2. After a green `master` integration run uploads that "
        "object, PRs will compare against the published master snapshot instead.\n"
    )


def _fmt_pct(value) -> str:
    if value is None:
        return "n/a"
    return f"{float(value):.2f}%"


def _fmt_int(value) -> str:
    if value is None:
        return "n/a"
    return str(int(value))


def _fmt_delta_pct(delta) -> str:
    if delta is None:
        return "n/a"
    return f"{float(delta):+.2f}%"


def _fmt_delta_int(cur, base) -> str:
    if cur is None or base is None:
        return "n/a"
    return f"{int(cur) - int(base):+d}"


def _build_body(d: dict) -> str:
    baseline_source = d["baseline_source"]
    from_zero = d["from_zero"]
    s3_sha = d.get("s3_sha")
    result_json_url = d.get("result_json_url") or ""
    suites = d.get("suites") or []

    body = (
        "### PromQL Compliance Report\n\n"
        f"Baseline: {baseline_source}\n\n"
        "| Suite | Baseline | Current | Delta |\n"
        "|-------|----------|---------|-------|\n"
    )
    for row in suites:
        body += (
            f"| {row['title']} | {_fmt_pct(row.get('base_pct'))} | "
            f"{_fmt_pct(row.get('new_pct'))} | {_fmt_delta_pct(row.get('delta'))} |\n"
        )

    body += (
        "\n| Suite | Passed | Failed | Unsupported |\n"
        "|-------|--------|--------|-------------|\n"
    )
    for row in suites:
        title = row["title"]
        body += (
            f"| {title} | {_fmt_int(row.get('base_passed'))} -> {_fmt_int(row.get('cur_passed'))} "
            f"({_fmt_delta_int(row.get('cur_passed'), row.get('base_passed'))}) | "
            f"{_fmt_int(row.get('base_failed'))} -> {_fmt_int(row.get('cur_failed'))} "
            f"({_fmt_delta_int(row.get('cur_failed'), row.get('base_failed'))}) | "
            f"{_fmt_int(row.get('base_unsup'))} -> {_fmt_int(row.get('cur_unsup'))} "
            f"({_fmt_delta_int(row.get('cur_unsup'), row.get('base_unsup'))}) |\n"
        )

    for row in suites:
        extra = []
        if row.get("excluded_native_histogram"):
            extra.append(f"native-histogram exclusions: {row['excluded_native_histogram']}")
        if row.get("excluded_assertions"):
            extra.append(f"excluded assertions: {row['excluded_assertions']}")
        if row.get("upstream_sha"):
            extra.append(f"upstream `{row['upstream_sha'][:12]}`")
        if extra:
            body += f"\n{row['title']}: " + "; ".join(extra) + ".\n"

    compliance = next((r for r in suites if r.get("id") == "compliance"), None)
    delta = float(compliance["delta"]) if compliance and compliance.get("delta") is not None else 0.0
    if compliance and compliance.get("has_baseline") and abs(delta) < _EPS:
        body += "\nNote: Prometheus compliance score is unchanged vs baseline.\n"

    if result_json_url:
        body += (
            f"\n[Uploaded compliance JSON for this PR commit]({result_json_url})\n"
        )

    if from_zero:
        body += _no_s3_baseline_block()
    elif compliance and compliance.get("delta") is not None and delta > _EPS and s3_sha:
        body += _bump_baseline_block_s3(
            float(compliance["new_pct"]), float(compliance["base_pct"]), s3_sha
        )
    elif compliance and not from_zero and compliance.get("delta") is not None and delta < -_EPS:
        body += (
            "\nNote: Prometheus compliance score is below the chosen baseline (informational only; "
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

        for k in ("baseline_source", "from_zero", "suites"):
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
