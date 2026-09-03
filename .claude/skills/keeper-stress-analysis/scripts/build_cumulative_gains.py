#!/usr/bin/env python3
"""Compute baseline-window vs current-window medians for every metric on every
scenario+backend, then emit per-scenario gain tables.

Outputs:
  cumulative_gains.tsv  — one row per (scenario, backend, metric)
  cumulative_gains_summary.tsv — pivoted view per scenario+backend with key metrics
"""
import csv
import datetime
import statistics
import sys
from collections import defaultdict
from pathlib import Path

from _common import to_float

ROOT = Path(__file__).parent

# Headline metrics & their "good direction"
METRICS = {
    "rps":                          "↑",  # higher better
    "read_rps":                     "↑",
    "write_rps":                    "↑",
    "read_p50_ms":                  "↓",
    "read_p95_ms":                  "↓",
    "read_p99_ms":                  "↓",
    "write_p50_ms":                 "↓",
    "write_p95_ms":                 "↓",
    "write_p99_ms":                 "↓",
    "error_pct":                    "↓",
    "peak_mem_gb":                  "↓",
    "avg_mem_gb":                   "↓",
    "p95_cpu_cores":                "↓",
    "zk_max_latency_max":           "↓",
    "zk_avg_latency_avg":           "↓",
    "FileSync_us_per_s_avg":        "↓",
    "ChangelogFsync_us_per_s_avg":  "↓",
    "StorageLockWait_us_per_s_avg": "↓",
    "TotalElapsed_us_per_s_avg":    "↓",
    "KeeperLatency_us_per_s_avg":   "↓",
    "OutstandingRequests_max":      "↓",
}

WINDOW_SIZE = 3  # take median of N earliest / N latest nightlies per scenario+backend


def main():
    # Load merged metrics
    by_sb = defaultdict(list)
    with open(ROOT / "merged_metrics.tsv") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            ts = r["run_ended"]
            by_sb[(r["scenario"], r["backend"])].append((ts, r))

    # For each (scenario, backend), sort by run_ended and take medians of edges
    out_rows = []  # one per (scenario, backend, metric)
    summary_rows = []  # one per (scenario, backend)
    skipped = []
    for (sc, be), runs in by_sb.items():
        runs.sort(key=lambda x: x[0])
        if len(runs) < 2 * WINDOW_SIZE:
            skipped.append((sc, be, len(runs)))
            continue
        baseline_runs = runs[:WINDOW_SIZE]
        current_runs  = runs[-WINDOW_SIZE:]

        baseline_dates = [r[0].split(" ")[0] for r in baseline_runs]
        current_dates  = [r[0].split(" ")[0] for r in current_runs]
        baseline_shas  = [r[1]["commit_sha"][:8] for r in baseline_runs]
        current_shas   = [r[1]["commit_sha"][:8] for r in current_runs]

        per_metric = {}
        for m, direction in METRICS.items():
            base_vals = [v for v in (to_float(r[1].get(m)) for r in baseline_runs) if v is not None]
            curr_vals = [v for v in (to_float(r[1].get(m)) for r in current_runs)  if v is not None]
            if not base_vals or not curr_vals:
                continue
            base_med = statistics.median(base_vals)
            curr_med = statistics.median(curr_vals)
            base_min, base_max = min(base_vals), max(base_vals)
            curr_min, curr_max = min(curr_vals), max(curr_vals)
            delta_abs = curr_med - base_med
            if base_med == 0:
                delta_pct = float("inf") if curr_med != 0 else 0.0
            else:
                delta_pct = (curr_med - base_med) / abs(base_med) * 100.0
            improved = ((direction == "↑" and delta_pct > 0) or
                        (direction == "↓" and delta_pct < 0))
            per_metric[m] = {
                "base_med": base_med, "curr_med": curr_med,
                "base_min": base_min, "base_max": base_max,
                "curr_min": curr_min, "curr_max": curr_max,
                "delta_abs": delta_abs, "delta_pct": delta_pct,
                "improved": improved, "direction": direction,
            }
            out_rows.append({
                "scenario": sc, "backend": be, "metric": m,
                "direction": direction,
                "baseline_window": ",".join(baseline_shas),
                "baseline_dates":  ",".join(baseline_dates),
                "current_window":  ",".join(current_shas),
                "current_dates":   ",".join(current_dates),
                "base_median": f"{base_med:.4f}",
                "curr_median": f"{curr_med:.4f}",
                "base_min": f"{base_min:.4f}", "base_max": f"{base_max:.4f}",
                "curr_min": f"{curr_min:.4f}", "curr_max": f"{curr_max:.4f}",
                "delta_abs": f"{delta_abs:.4f}",
                "delta_pct": f"{delta_pct:.2f}",
                "improved":  "yes" if improved else "no",
            })

        # Summary row: just the headline metrics
        summary_rows.append({
            "scenario": sc, "backend": be,
            "n_runs": len(runs),
            "baseline_window": " | ".join(f"{s} {d}" for s, d in zip(baseline_shas, baseline_dates)),
            "current_window":  " | ".join(f"{s} {d}" for s, d in zip(current_shas, current_dates)),
            "rps_pct":          f"{per_metric.get('rps', {}).get('delta_pct', float('nan')):+.2f}" if 'rps' in per_metric else "",
            "read_p99_pct":     f"{per_metric.get('read_p99_ms', {}).get('delta_pct', float('nan')):+.2f}" if 'read_p99_ms' in per_metric else "",
            "write_p99_pct":    f"{per_metric.get('write_p99_ms', {}).get('delta_pct', float('nan')):+.2f}" if 'write_p99_ms' in per_metric else "",
            "error_pct_diff":   f"{per_metric.get('error_pct', {}).get('delta_abs', float('nan')):+.4f}" if 'error_pct' in per_metric else "",
            "peak_mem_pct":     f"{per_metric.get('peak_mem_gb', {}).get('delta_pct', float('nan')):+.2f}" if 'peak_mem_gb' in per_metric else "",
            "lockwait_pct":     f"{per_metric.get('StorageLockWait_us_per_s_avg', {}).get('delta_pct', float('nan')):+.2f}" if 'StorageLockWait_us_per_s_avg' in per_metric else "",
        })

    out_path = ROOT / "cumulative_gains.tsv"
    with open(out_path, "w", newline="") as f:
        cols = ["scenario", "backend", "metric", "direction",
                "baseline_window", "baseline_dates", "current_window", "current_dates",
                "base_median", "curr_median", "base_min", "base_max", "curr_min", "curr_max",
                "delta_abs", "delta_pct", "improved"]
        w = csv.DictWriter(f, fieldnames=cols, delimiter="\t", lineterminator="\n")
        w.writeheader()
        w.writerows(out_rows)
    print(f"Wrote {out_path} ({len(out_rows)} rows)", file=sys.stderr)
    if skipped:
        print(
            f"Skipped {len(skipped)} (scenario,backend) pair(s) with "
            f"< 2*WINDOW_SIZE={2*WINDOW_SIZE} runs in the window:",
            file=sys.stderr,
        )
        for sc, be, n in sorted(skipped):
            print(f"  {sc} [{be}]: {n} run(s)", file=sys.stderr)

    sum_path = ROOT / "cumulative_gains_summary.tsv"
    summary_cols = [
        "scenario", "backend", "n_runs",
        "baseline_window", "current_window",
        "rps_pct", "read_p99_pct", "write_p99_pct",
        "error_pct_diff", "peak_mem_pct", "lockwait_pct",
    ]
    with open(sum_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=summary_cols, delimiter="\t", lineterminator="\n")
        w.writeheader()
        w.writerows(sorted(summary_rows, key=lambda r: (r["scenario"], r["backend"])))
    if not summary_rows:
        print(
            f"Wrote {sum_path} (header-only — no (scenario,backend) had "
            f">= 2*WINDOW_SIZE={2*WINDOW_SIZE} runs in the window)",
            file=sys.stderr,
        )
    else:
        print(f"Wrote {sum_path} ({len(summary_rows)} rows)", file=sys.stderr)


if __name__ == "__main__":
    main()
