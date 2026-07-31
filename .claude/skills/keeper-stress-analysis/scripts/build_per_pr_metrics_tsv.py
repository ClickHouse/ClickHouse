#!/usr/bin/env python3
"""Flat TSV: one row per (PR, scenario, backend, metric) with pre, post, delta_abs, delta_pct.

`delta_abs` is the unitful absolute change (`post - pre`); `delta_pct` is the
relative change in percent. The output file is `per_pr_metrics_long.tsv` and
its header is the canonical schema for downstream consumers (the per-PR
markdown table is composed from it).
"""
import csv
import sys
from collections import defaultdict
from pathlib import Path

from _common import is_fault_scenario, to_float

ROOT = Path(__file__).parent

METRICS = [
    "rps", "read_rps", "write_rps",
    "read_p50_ms", "read_p95_ms", "read_p99_ms",
    "write_p50_ms", "write_p95_ms", "write_p99_ms",
    "error_pct", "errors", "ops",
    "peak_mem_gb", "p95_cpu_cores",
    "zk_max_latency_max", "zk_avg_latency_avg",
    "FileSync_us_per_s_avg", "ChangelogFsync_us_per_s_avg",
    "StorageLockWait_us_per_s_avg", "TotalElapsed_us_per_s_avg",
    "KeeperLatency_us_per_s_avg", "CommitWaitElapsed_us_per_s_avg",
    "OutstandingRequests_max", "AliveConnections_max",
    "ZnodeCount_max", "WatchCount_max",
    "RequestTotal_per_s_avg", "Commits_per_s_avg",
    "ChangelogWritten_B_per_s_avg", "SnapshotWritten_B_per_s_avg",
    "SnapshotCreations_per_s_avg", "SnapshotApplys_per_s_avg",
    "CommitsFailed_max", "SnapshotCreationsFailed_max",
    "SnapshotApplysFailed_max", "RejectedSoftMemLimit_max",
]


def main():
    by_sb_sha = defaultdict(dict)
    with open(ROOT / "merged_metrics.tsv") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            by_sb_sha[(r["scenario"], r["backend"])][r["commit_sha"][:8]] = r

    pr_to_nightly = []
    with open(ROOT / "pr_to_nightly.tsv") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            pr_to_nightly.append(r)

    out_path = ROOT / "per_pr_metrics_long.tsv"
    with open(out_path, "w", newline="") as fout:
        w = csv.writer(fout, delimiter="\t", lineterminator="\n")
        w.writerow(["pr", "title", "merged_at", "co_merged",
                    "pre_sha8", "post_sha8", "baseline_kind",
                    "scenario", "backend", "metric",
                    "pre", "post", "delta_abs", "delta_pct"])
        for pr in pr_to_nightly:
            # Two baseline pairs:
            #   - "primary": first nightly after merge regardless of kind (could be fault-only)
            #   - "kind-matched": for each scenario, use the nightly whose kind matches that scenario
            primary_pre  = pr["pre_sha8"]
            primary_post = pr["post_sha8"]
            nofault_pre  = pr.get("pre_nofault_sha8", "")
            nofault_post = pr.get("post_nofault_sha8", "")
            fault_pre    = pr.get("pre_fault_sha8", "")
            fault_post   = pr.get("post_fault_sha8", "")

            for (sc, be), sha_map in by_sb_sha.items():
                is_fault = is_fault_scenario(sc)
                # Pick the baseline pair appropriate for this scenario kind
                pre_sha  = fault_pre  if is_fault else nofault_pre
                post_sha = fault_post if is_fault else nofault_post
                baseline_kind = "fault" if is_fault else "no-fault"

                # Fall back to primary if kind-matched is missing
                if not pre_sha:
                    pre_sha = primary_pre; baseline_kind += "(fallback-primary-pre)"
                if not post_sha:
                    post_sha = primary_post; baseline_kind += "(fallback-primary-post)"
                if not pre_sha or not post_sha:
                    continue

                pre_r  = sha_map.get(pre_sha)
                post_r = sha_map.get(post_sha)
                if pre_r is None and post_r is None:
                    continue
                for m in METRICS:
                    pre_v = to_float((pre_r or {}).get(m))
                    post_v = to_float((post_r or {}).get(m))
                    delta_abs = "" if pre_v is None or post_v is None else f"{post_v - pre_v:.4f}"
                    if pre_v is None or post_v is None:
                        delta_pct = ""
                    elif pre_v == 0:
                        delta_pct = "" if post_v == 0 else "inf"
                    else:
                        delta_pct = f"{(post_v - pre_v)/abs(pre_v)*100:.2f}"
                    w.writerow([
                        pr["pr"], pr["title"], pr["merged_at"], pr["co_merged"],
                        pre_sha, post_sha, baseline_kind,
                        sc, be, m,
                        "" if pre_v is None else f"{pre_v:.4f}",
                        "" if post_v is None else f"{post_v:.4f}",
                        delta_abs, delta_pct,
                    ])
    print(f"Wrote {out_path}", file=sys.stderr)


if __name__ == "__main__":
    main()
