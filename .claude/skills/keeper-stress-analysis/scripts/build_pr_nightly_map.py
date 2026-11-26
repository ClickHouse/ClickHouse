#!/usr/bin/env python3
"""Build PR -> first-post-merge nightly mapping with co-merged PR list.

The window edge (`THRESHOLD`) is sourced from `_common.KEEPER_SKILL_THRESHOLD`
so all scripts agree on which PRs are in-window."""
import csv
import datetime
import sys
from pathlib import Path

from _common import KEEPER_SKILL_THRESHOLD as THRESHOLD
from _common import KEEPER_SKILL_THRESHOLD_END as THRESHOLD_END
from _common import is_fault_scenario, parse_merged_at

ROOT = Path(__file__).parent

PR_TO_NIGHTLY_FIELDS = [
    "pr", "title", "branch", "merged_at", "merge_sha8",
    "pre_sha8", "pre_run", "post_sha8", "post_run",
    "pre_nofault_sha8", "post_nofault_sha8",
    "pre_fault_sha8", "post_fault_sha8",
    "co_merged",
]


def parse_pr_meta() -> list[dict]:
    rows = []
    skipped = 0
    src = ROOT.parent / "pr_meta.tsv"
    with open(src) as f:
        reader = csv.DictReader(f, delimiter="\t")
        for r in reader:
            merged_dt = parse_merged_at(r, source=src)
            if merged_dt is None:
                # Open / unmerged PR (empty `mergedAt`) — `gh pr list --state all`
                # returns these. Skip silently per row, summarize at end.
                skipped += 1
                continue
            r["merged_dt"] = merged_dt
            r["pr"] = int(r["pr"])
            rows.append(r)
    if skipped:
        print(
            f"parse_pr_meta: skipped {skipped} unmerged PR row(s) with empty `mergedAt`",
            file=sys.stderr,
        )
    rows.sort(key=lambda x: x["merged_dt"])
    return rows


def parse_nightlies() -> list[dict]:
    """Return list of distinct master commits with run timestamps from bench_summary."""
    by_sha: dict[str, dict] = {}
    with open(ROOT / "staging" / "bench_summary.tsv") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for r in reader:
            sha = r["commit_sha"]
            ended = datetime.datetime.fromisoformat(r["run_ended"]).replace(tzinfo=datetime.timezone.utc)
            kind = "fault" if is_fault_scenario(r["scenario"]) else "no-fault"
            if sha not in by_sha:
                by_sha[sha] = {"commit_sha": sha, "sha8": r["sha8"], "earliest": ended, "latest": ended, "kinds": set([kind])}
            else:
                by_sha[sha]["earliest"] = min(by_sha[sha]["earliest"], ended)
                by_sha[sha]["latest"]   = max(by_sha[sha]["latest"], ended)
                by_sha[sha]["kinds"].add(kind)
    nightlies = list(by_sha.values())
    nightlies.sort(key=lambda x: x["earliest"])
    return nightlies


def has_nofault(n: dict) -> bool:
    return "no-fault" in n["kinds"]


def has_fault(n: dict) -> bool:
    return "fault" in n["kinds"]


def main():
    prs = parse_pr_meta()
    nightlies = parse_nightlies()

    # Build PR -> first-post-merge nightly
    in_window = [p for p in prs if THRESHOLD <= p["merged_dt"] < THRESHOLD_END]
    out_window = [p for p in prs if not (THRESHOLD <= p["merged_dt"] < THRESHOLD_END)]

    print(f"PRs in window ({THRESHOLD.date()} <= ts < {THRESHOLD_END.date()}): {len(in_window)}", file=sys.stderr)
    print(f"PRs out of window: {len(out_window)}", file=sys.stderr)
    print(f"Distinct master nightlies in staging: {len(nightlies)}", file=sys.stderr)

    rows_out = []
    # For each in-window PR: find first nightly whose earliest run-time is >= merged_dt
    # AND find last nightly whose latest run-time is <= merged_dt (the pre-baseline).
    # Additionally compute fallback baselines that match the *kind* of run that the PR
    # missed: if a PR's `post` is fault-only, store the next no-fault nightly as
    # `post_nofault_sha8`; analogously for fault-only.
    for pr in in_window:
        # Use pr merged_dt as the cut-off
        post = next((n for n in nightlies if n["earliest"] >= pr["merged_dt"]), None)
        pre  = None
        for n in nightlies:
            if n["latest"] <= pr["merged_dt"]:
                pre = n
            else:
                break
        # First post-merge no-fault and fault nightlies (independent of which kind `post` is)
        post_nofault = next(
            (n for n in nightlies if n["earliest"] >= pr["merged_dt"] and has_nofault(n)),
            None)
        post_fault = next(
            (n for n in nightlies if n["earliest"] >= pr["merged_dt"] and has_fault(n)),
            None)
        pre_nofault = None
        pre_fault = None
        for n in nightlies:
            if n["latest"] <= pr["merged_dt"]:
                if has_nofault(n):
                    pre_nofault = n
                if has_fault(n):
                    pre_fault = n
            else:
                break
        # Co-merged PRs: any other in-window PR whose mergedAt falls in (pre.latest, post.earliest]
        co_merged = []
        if post is not None:
            lo = pre["latest"] if pre is not None else THRESHOLD
            for q in in_window:
                if q["pr"] == pr["pr"]:
                    continue
                if lo < q["merged_dt"] <= post["earliest"]:
                    co_merged.append(q["pr"])
        rows_out.append({
            "pr": pr["pr"],
            "title": pr["title"],
            "branch": pr.get("headRefName", ""),  # PR-branch name; needed by build_pr_branch_isolated.py
            "merged_at": pr["mergedAt"],
            "merge_sha8": pr["mergeCommit"][:8],
            "pre_sha8":  pre["sha8"] if pre else "",
            "pre_run":   pre["latest"].isoformat() if pre else "",
            "post_sha8": post["sha8"] if post else "",
            "post_run":  post["earliest"].isoformat() if post else "",
            "pre_nofault_sha8":  pre_nofault["sha8"] if pre_nofault else "",
            "post_nofault_sha8": post_nofault["sha8"] if post_nofault else "",
            "pre_fault_sha8":    pre_fault["sha8"] if pre_fault else "",
            "post_fault_sha8":   post_fault["sha8"] if post_fault else "",
            "co_merged": ",".join(str(x) for x in sorted(co_merged)),
        })

    out_path = ROOT / "pr_to_nightly.tsv"
    with open(out_path, "w") as f:
        w = csv.DictWriter(f, fieldnames=PR_TO_NIGHTLY_FIELDS, delimiter="\t")
        w.writeheader()
        w.writerows(rows_out)
    if not rows_out:
        print(f"Wrote {out_path} (header-only — no in-window PRs to map)", file=sys.stderr)
    else:
        print(f"Wrote {out_path} ({len(rows_out)} rows; {len(out_window)} PRs out-of-window omitted)", file=sys.stderr)


if __name__ == "__main__":
    main()
