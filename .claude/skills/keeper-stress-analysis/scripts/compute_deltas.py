#!/usr/bin/env python3
"""
Compute per-PR and per-nightly deltas from merged_metrics.tsv + pr_to_nightly.tsv.

Outputs:
- per_pr_scenario_deltas.tsv : (pr, scenario, backend, metrics + Δ)
- per_pr_summary.tsv         : one row per PR with worst regression / best improvement
- per_nightly_summary.tsv    : one row per nightly run with PRs landed
"""
import csv
import datetime
import sys
from collections import defaultdict
from pathlib import Path

from _common import KEEPER_SKILL_THRESHOLD as THRESHOLD
from _common import KEEPER_SKILL_THRESHOLD_END as THRESHOLD_END
from _common import (
    CLASSIFY_BANDS,
    DEFAULT_BANDS,
    HEADLINE_LOOKUP,
    HEADLINE_METRICS,
    classify,
    is_fault_scenario,
    parse_merged_at,
    to_float,
)

ROOT = Path(__file__).parent

PER_NIGHTLY_FIELDS = ["sha8", "earliest_ts", "scenarios_count", "kind", "prs_landed"]


def load_metrics():
    by_key = {}
    with open(ROOT / "merged_metrics.tsv") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for r in reader:
            key = (r["scenario"], r["backend"], r["commit_sha"])
            by_key[key] = r
    return by_key


def load_metrics_indexed_by_sha8():
    """Map sha8 -> {(scenario,backend) -> metrics}."""
    out = defaultdict(dict)
    with open(ROOT / "merged_metrics.tsv") as f:
        reader = csv.DictReader(f, delimiter="\t")
        for r in reader:
            sha8 = r["commit_sha"][:8]
            out[sha8][(r["scenario"], r["backend"])] = r
    return out


def load_pr_map():
    rows = []
    with open(ROOT / "pr_to_nightly.tsv") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            rows.append(r)
    return rows


def compute_pr_deltas():
    # `pr_to_nightly.tsv` is the PR-set-mode artefact (built by
    # `keeper_stress.py prmap`). In date-range-only mode it doesn't exist;
    # there are no per-PR deltas to compute, so skip cleanly with a stderr
    # note instead of crashing.
    if not (ROOT / "pr_to_nightly.tsv").exists():
        print(
            "compute_pr_deltas: pr_to_nightly.tsv not found "
            "(date-range-only mode); skipping per-PR delta computation.",
            file=sys.stderr,
        )
        return
    by_sha8 = load_metrics_indexed_by_sha8()
    prs = load_pr_map()
    scenarios_backends = set()
    for sb_map in by_sha8.values():
        for sb in sb_map:
            scenarios_backends.add(sb)

    deltas_rows = []  # one row per (PR, scenario, backend)
    pr_summary = []   # one row per PR

    # Pre-compute a list of nightlies with PRs that landed in each window
    # for the per-nightly summary
    for pr in prs:
        pre_sha = pr["pre_sha8"]
        post_sha = pr["post_sha8"]
        if not pre_sha or not post_sha:
            # No comparable nightly
            pr_summary.append({
                "pr": pr["pr"],
                "title": pr["title"],
                "merged_at": pr["merged_at"],
                "merge_sha8": pr["merge_sha8"],
                "pre_sha8": pre_sha,
                "post_sha8": post_sha,
                "co_merged": pr["co_merged"],
                "verdict": "not-yet-tested",
                "worst_regression": "",
                "best_improvement": "",
                "server_failures": 0,
            })
            continue

        pre_map  = by_sha8.get(pre_sha, {})
        post_map = by_sha8.get(post_sha, {})

        worst = None  # (metric, scenario, backend, pre, post, pct, verdict)
        best  = None
        scenario_verdicts = []
        server_failures = 0

        for sb in sorted(scenarios_backends):
            scenario, backend = sb
            pre_r  = pre_map.get(sb)
            post_r = post_map.get(sb)
            if pre_r is None or post_r is None:
                continue

            row = {
                "pr": pr["pr"],
                "title": pr["title"],
                "scenario": scenario,
                "backend": backend,
                "pre_sha8": pre_sha,
                "post_sha8": post_sha,
            }

            # Server-side failures
            # Per-PR check is post-merge only — we count failures present in
            # the first nightly that includes the PR. The "across the entire
            # window must be zero" gate documented in SKILL.md Phase 5 is a
            # separate pass over `prom_gauges.tsv`.
            sf_post = (to_float(post_r.get("CommitsFailed_max",0)) or 0) + \
                      (to_float(post_r.get("SnapshotApplysFailed_max",0)) or 0) + \
                      (to_float(post_r.get("SnapshotCreationsFailed_max",0)) or 0) + \
                      (to_float(post_r.get("RejectedSoftMemLimit_max",0)) or 0)
            row["server_failures_post"] = int(sf_post)
            server_failures += int(sf_post)

            for metric, _ in HEADLINE_METRICS:
                pre_v = to_float(pre_r.get(metric))
                post_v = to_float(post_r.get(metric))
                row[f"{metric}_pre"]  = pre_v if pre_v is not None else ""
                row[f"{metric}_post"] = post_v if post_v is not None else ""
                if pre_v is not None and post_v is not None:
                    if pre_v == 0:
                        delta_pct = float("inf") if post_v > 0 else 0.0
                    else:
                        delta_pct = (post_v - pre_v) / abs(pre_v) * 100.0
                    row[f"{metric}_delta_pct"] = round(delta_pct, 1)
                    verdict = classify(metric, pre_v, post_v)
                    row[f"{metric}_verdict"] = verdict
                    scenario_verdicts.append((metric, scenario, backend, pre_v, post_v, delta_pct, verdict))

            deltas_rows.append(row)

        # Determine worst regression and best improvement across all scenarios
        regressions = [v for v in scenario_verdicts if v[6] in ("regression", "watch")]
        if regressions:
            # rank by absolute pct change
            regressions.sort(key=lambda x: abs(x[5]) if x[5] != float("inf") else 1e18, reverse=True)
            worst = regressions[0]

        # Rank improvements that exceed the per-metric clean band — i.e. the
        # mirror of `classify`'s regression threshold. Reading `CLASSIFY_BANDS`
        # directly keeps presentation consistent with the per-metric bands
        # documented in `references/methodology.md` (and used by `classify`).
        ranked_improvements = []
        for v in scenario_verdicts:
            metric, sc, be, pre, post, pct, vd = v
            direction = HEADLINE_LOOKUP[metric]
            clean_band, _watch_band = CLASSIFY_BANDS.get(metric, DEFAULT_BANDS)
            if direction == "lower_better" and pct < -clean_band:
                ranked_improvements.append((v, -pct))
            elif direction == "higher_better" and pct > clean_band:
                ranked_improvements.append((v, pct))
        if ranked_improvements:
            ranked_improvements.sort(key=lambda x: x[1], reverse=True)
            best = ranked_improvements[0][0]

        # Assign overall verdict per PR. If both nightlies have data but
        # no `(scenario, backend)` pair is shared (e.g. partial scenario
        # coverage on one side), `scenario_verdicts` is empty — that's
        # `not-yet-tested`, not silently `clean`.
        if not scenario_verdicts:
            verdict = "not-yet-tested"
        elif any(v[6] == "regression" for v in scenario_verdicts):
            verdict = "regression"
        elif any(v[6] == "watch" for v in scenario_verdicts):
            verdict = "watch"
        else:
            verdict = "clean"
        if server_failures > 0:
            verdict = "regression(server-failure)"

        def fmt_v(v):
            if v is None: return ""
            metric, sc, be, pre, post, pct, vd = v
            if pct == float("inf"):
                return f"{sc}[{be}] {metric}: 0 → {post:.2f}"
            return f"{sc}[{be}] {metric}: {pre:.2f} → {post:.2f} ({pct:+.1f}%)"

        pr_summary.append({
            "pr": pr["pr"],
            "title": pr["title"],
            "merged_at": pr["merged_at"],
            "merge_sha8": pr["merge_sha8"],
            "pre_sha8": pre_sha,
            "post_sha8": post_sha,
            "co_merged": pr["co_merged"],
            "verdict": verdict,
            "worst_regression": fmt_v(worst),
            "best_improvement": fmt_v(best),
            "server_failures": server_failures,
        })

    # Write deltas. Always emit at least a header — if `deltas_rows` is empty
    # (e.g. every PR is `not-yet-tested` because no comparable pre/post nightly
    # exists), downstream consumers should still find a well-formed TSV.
    out_deltas = ROOT / "per_pr_scenario_deltas.tsv"
    base_cols = ["pr", "title", "scenario", "backend", "pre_sha8", "post_sha8", "server_failures_post"]
    if deltas_rows:
        all_keys = set()
        for r in deltas_rows:
            all_keys.update(r.keys())
        cols = base_cols + sorted([c for c in all_keys if c not in set(base_cols)])
    else:
        cols = base_cols
    with open(out_deltas, "w") as f:
        w = csv.DictWriter(f, fieldnames=cols, delimiter="\t")
        w.writeheader()
        for r in deltas_rows:
            w.writerow(r)
    if not deltas_rows:
        print(f"Wrote {out_deltas} (header-only — no comparable pre/post nightlies)", file=sys.stderr)
    else:
        print(f"Wrote {out_deltas} ({len(deltas_rows)} rows)", file=sys.stderr)

    # Write PR summary
    out_summary = ROOT / "per_pr_summary.tsv"
    cols = ["pr","title","merged_at","merge_sha8","pre_sha8","post_sha8","co_merged","verdict","server_failures","worst_regression","best_improvement"]
    with open(out_summary, "w") as f:
        w = csv.DictWriter(f, fieldnames=cols, delimiter="\t")
        w.writeheader()
        for r in pr_summary:
            w.writerow(r)
    print(f"Wrote {out_summary} ({len(pr_summary)} rows)", file=sys.stderr)


def compute_nightly_summary():
    """For each master nightly, list PRs that landed since the prior nightly."""
    # Get distinct nightlies (sha8 with earliest run_ended) sorted chronologically
    by_sha8 = defaultdict(lambda: {"earliest": None, "scenarios": set()})
    with open(ROOT / "merged_metrics.tsv") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            sha8 = r["commit_sha"][:8]
            ts = datetime.datetime.fromisoformat(r["run_ended"]).replace(tzinfo=datetime.timezone.utc)
            cur = by_sha8[sha8]
            if cur["earliest"] is None or ts < cur["earliest"]:
                cur["earliest"] = ts
            cur["scenarios"].add(r["scenario"])

    nightlies = sorted(by_sha8.items(), key=lambda x: x[1]["earliest"])

    # Map PR merge time. `pr_meta.tsv` is only present in PR-set mode; in
    # date-range-only mode the per-nightly summary is still useful (lists
    # nightlies + scenario kinds) but `prs_landed` is empty for every row.
    pr_meta_path = ROOT.parent / "pr_meta.tsv"
    pr_meta = []
    if pr_meta_path.exists():
        skipped = 0
        with open(pr_meta_path) as f:
            for r in csv.DictReader(f, delimiter="\t"):
                merged_dt = parse_merged_at(r, source=pr_meta_path)
                if merged_dt is None:
                    skipped += 1
                    continue
                r["merged_dt"] = merged_dt
                pr_meta.append(r)
        if skipped:
            print(
                f"compute_nightly_summary: skipped {skipped} unmerged PR row(s) "
                f"with empty `mergedAt`",
                file=sys.stderr,
            )
        pr_meta.sort(key=lambda x: x["merged_dt"])
    else:
        print(
            f"compute_nightly_summary: {pr_meta_path.name} not found; "
            f"per_nightly_summary.tsv will list nightlies but `prs_landed` "
            f"will be empty (date-range-only mode).",
            file=sys.stderr,
        )

    rows = []
    prev_ts = THRESHOLD
    for sha8, info in nightlies:
        prs_landed = []
        for p in pr_meta:
            if (prev_ts < p["merged_dt"] <= info["earliest"]
                    and THRESHOLD <= p["merged_dt"] < THRESHOLD_END):
                prs_landed.append(p["pr"])
        # Most nightlies run both fault and no-fault scenarios. Distinguish
        # `fault`-only / `no-fault`-only / `mixed` so downstream consumers
        # (e.g. kind-matched baseline picking in `build_per_pr_metrics_tsv`)
        # don't conflate a mixed nightly with a fault-only one.
        has_fault = any(is_fault_scenario(s) for s in info["scenarios"])
        has_nofault = any(not is_fault_scenario(s) for s in info["scenarios"])
        if has_fault and has_nofault:
            kind = "mixed"
        elif has_fault:
            kind = "fault"
        else:
            kind = "no-fault"
        rows.append({
            "sha8": sha8,
            "earliest_ts": info["earliest"].isoformat(),
            "scenarios_count": len(info["scenarios"]),
            "kind": kind,
            "prs_landed": ",".join(prs_landed),
        })
        prev_ts = info["earliest"]

    out = ROOT / "per_nightly_summary.tsv"
    with open(out, "w") as f:
        w = csv.DictWriter(f, fieldnames=PER_NIGHTLY_FIELDS, delimiter="\t")
        w.writeheader()
        w.writerows(rows)
    if not rows:
        print(f"Wrote {out} (header-only — no master nightlies in window)", file=sys.stderr)
    else:
        print(f"Wrote {out} ({len(rows)} rows)", file=sys.stderr)


if __name__ == "__main__":
    compute_pr_deltas()
    compute_nightly_summary()
