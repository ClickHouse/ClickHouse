#!/usr/bin/env python3
"""Join the five staging TSVs into a wide per-(scenario, backend, commit_sha)
metrics table. Output: `merged_metrics.tsv`, one row per (scenario, backend,
commit), with one column per tracked metric.

Inputs (all under `staging/`, produced by the SQL queries in `queries/`):

  1. `bench_summary.tsv` (from `01_bench_summary.sql`) — seeds the row set;
     defines which (scenario, backend, commit) keys exist in the output.
  2. `container.tsv`    (`05_container.sql`)   — adds container memory + CPU.
  3. `prom_rates.tsv`   (`02_prom_rates.sql`)  — adds Keeper-prom rate metrics.
  4. `prom_gauges.tsv`  (`03_prom_gauges.sql`) — adds Keeper-prom gauge metrics
     including the four `*Failed_max` failure counters.
  5. `mntr.tsv`         (`04_mntr.sql`)        — adds ZK 4LW `mntr` metrics.

Join semantics: each loader updates a row only if `(scenario, backend,
commit_sha)` already exists in the seed set from `bench_summary.tsv`. So a
commit that appears only in container/prom/mntr but not in bench_summary is
silently dropped — by design, the bench summary is authoritative for which
runs to include.
"""
import csv
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).parent
STAGING = ROOT / "staging"


def load_bench():
    rows = {}
    with open(STAGING / "bench_summary.tsv") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            key = (r["scenario"], r["backend"], r["commit_sha"])
            rows[key] = {
                "sha8": r["sha8"],
                "run_ended": r["run_ended"],
                "rps": float(r["rps"] or 0),
                "read_rps": float(r["read_rps"] or 0),
                "write_rps": float(r["write_rps"] or 0),
                "read_p50_ms": float(r["read_p50_ms"] or 0),
                "read_p95_ms": float(r["read_p95_ms"] or 0),
                "read_p99_ms": float(r["read_p99_ms"] or 0),
                "read_p99_99_ms": float(r["read_p99_99_ms"] or 0),
                "write_p50_ms": float(r["write_p50_ms"] or 0),
                "write_p95_ms": float(r["write_p95_ms"] or 0),
                "write_p99_ms": float(r["write_p99_ms"] or 0),
                "write_p99_99_ms": float(r["write_p99_99_ms"] or 0),
                "errors": int(float(r["errors"] or 0)),
                "error_pct": float(r["error_pct"] or 0),
                "ops": int(float(r["ops"] or 0)),
                "write_bps": float(r["write_bps"] or 0),
                "read_bps": float(r["read_bps"] or 0),
                "znode_delta": int(float(r["znode_delta"] or 0)),
                "bench_duration_s": int(float(r["bench_duration_s"] or 0)),
            }
    return rows


def load_container(rows):
    with open(STAGING / "container.tsv") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            key = (r["scenario"], r["backend"], r["commit_sha"])
            if key in rows:
                rows[key]["peak_mem_gb"]    = float(r.get("peak_mem_gb") or 0)
                rows[key]["avg_mem_gb"]     = float(r.get("avg_mem_gb") or 0)
                rows[key]["avg_cpu_cores"]  = float(r.get("avg_cpu_cores") or 0)
                rows[key]["p95_cpu_cores"]  = float(r.get("p95_cpu_cores") or 0)
                rows[key]["max_cpu_cores"]  = float(r.get("max_cpu_cores") or 0)
    return rows


# Map of "short metric label" -> (full prom name, which column to take)
PROM_RATE_KEEPS = {
    "FileSync_us_per_s":        "ClickHouseProfileEvents_FileSyncElapsedMicroseconds",
    "ChangelogFsync_us_per_s":  "ClickHouseProfileEvents_KeeperChangelogFileSyncMicroseconds",
    "StorageLockWait_us_per_s": "ClickHouseProfileEvents_KeeperStorageLockWaitMicroseconds",
    "TotalElapsed_us_per_s":    "ClickHouseProfileEvents_KeeperTotalElapsedMicroseconds",
    "ProcessElapsed_us_per_s":  "ClickHouseProfileEvents_KeeperProcessElapsedMicroseconds",
    "PreprocessElapsed_us_per_s": "ClickHouseProfileEvents_KeeperPreprocessElapsedMicroseconds",
    "CommitWaitElapsed_us_per_s": "ClickHouseProfileEvents_KeeperCommitWaitElapsedMicroseconds",
    "KeeperLatency_us_per_s":   "ClickHouseProfileEvents_KeeperLatency",
    "RequestTotal_per_s":       "ClickHouseProfileEvents_KeeperRequestTotal",
    "Commits_per_s":            "ClickHouseProfileEvents_KeeperCommits",
    "PacketsSent_per_s":        "ClickHouseProfileEvents_KeeperPacketsSent",
    "PacketsRecv_per_s":        "ClickHouseProfileEvents_KeeperPacketsReceived",
    "ChangelogWritten_B_per_s": "ClickHouseProfileEvents_KeeperChangelogWrittenBytes",
    "SnapshotWritten_B_per_s":  "ClickHouseProfileEvents_KeeperSnapshotWrittenBytes",
    "SnapshotApplys_per_s":     "ClickHouseProfileEvents_KeeperSnapshotApplys",
    "SnapshotCreations_per_s":  "ClickHouseProfileEvents_KeeperSnapshotCreations",
    "LogsReadFromFile_per_s":   "ClickHouseProfileEvents_KeeperLogsEntryReadFromFile",
    "LogsPrefetched_per_s":     "ClickHouseProfileEvents_KeeperLogsPrefetchedEntries",
    "LogsFromCommitCache_per_s": "ClickHouseProfileEvents_KeeperLogsEntryReadFromCommitCache",
    "LogsFromLatestCache_per_s": "ClickHouseProfileEvents_KeeperLogsEntryReadFromLatestCache",
}


def load_prom_rates(rows):
    inverse = {v: k for k, v in PROM_RATE_KEEPS.items()}
    with open(STAGING / "prom_rates.tsv") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            short = inverse.get(r["name"])
            if not short:
                continue
            key = (r["scenario"], r["backend"], r["commit_sha"])
            if key in rows:
                rows[key][short + "_avg"] = float(r.get("avg_rate_max_node") or 0)
                rows[key][short + "_p95"] = float(r.get("p95_rate_max_node") or 0)
    return rows


PROM_GAUGE_KEEPS = {
    "AliveConnections_max":       "ClickHouseMetrics_KeeperAliveConnections",
    "OutstandingRequests_max":    "ClickHouseMetrics_KeeperOutstandingRequests",
    "ZnodeCount_max":             "ClickHouseAsyncMetrics_KeeperZnodeCount",
    "ApproxDataSize_max":         "ClickHouseAsyncMetrics_KeeperApproximateDataSize",
    "WatchCount_max":             "ClickHouseAsyncMetrics_KeeperWatchCount",
    "EphemeralsCount_max":        "ClickHouseAsyncMetrics_KeeperEphemeralsCount",
    "LastCommittedLogIdx_max":    "ClickHouseAsyncMetrics_KeeperLastCommittedLogIdx",
    "LastLogIdx_max":             "ClickHouseAsyncMetrics_KeeperLastLogIdx",
    "LastLogTerm_max":            "ClickHouseAsyncMetrics_KeeperLastLogTerm",
    "LatestLogsCacheEntries_max": "ClickHouseAsyncMetrics_KeeperLatestLogsCacheEntries",
    "LatestLogsCacheSize_max":    "ClickHouseAsyncMetrics_KeeperLatestLogsCacheSize",
    "CommitLogsCacheEntries_max": "ClickHouseAsyncMetrics_KeeperCommitLogsCacheEntries",
    "CommitLogsCacheSize_max":    "ClickHouseAsyncMetrics_KeeperCommitLogsCacheSize",
    "OpenFD_max":                 "ClickHouseAsyncMetrics_KeeperOpenFileDescriptorCount",
    "PathsWatched_max":           "ClickHouseAsyncMetrics_KeeperPathsWatched",
    "SessionWithWatches_max":     "ClickHouseAsyncMetrics_KeeperSessionWithWatches",
    "CommitsFailed_max":          "ClickHouseProfileEvents_KeeperCommitsFailed",
    "SnapshotCreationsFailed_max": "ClickHouseProfileEvents_KeeperSnapshotCreationsFailed",
    "SnapshotApplysFailed_max":   "ClickHouseProfileEvents_KeeperSnapshotApplysFailed",
    "RejectedSoftMemLimit_max":   "ClickHouseProfileEvents_KeeperRequestRejectedDueToSoftMemoryLimitCount",
}


def load_prom_gauges(rows):
    inverse = {v: k for k, v in PROM_GAUGE_KEEPS.items()}
    with open(STAGING / "prom_gauges.tsv") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            short = inverse.get(r["name"])
            if not short:
                continue
            key = (r["scenario"], r["backend"], r["commit_sha"])
            if key in rows:
                rows[key][short] = float(r.get("max_value") or 0)
    return rows


MNTR_KEEPS = {
    "zk_avg_latency_avg":    ("zk_avg_latency",       "avg_value"),
    "zk_max_latency_max":    ("zk_max_latency",       "max_value"),
    "zk_packets_sent_per_s": ("zk_packets_sent",      "avg_rate"),
    "zk_packets_recv_per_s": ("zk_packets_received",  "avg_rate"),
    "zk_outstanding_max":    ("zk_outstanding_requests", "max_value"),
    "zk_watch_count_max":    ("zk_watch_count",       "max_value"),
    "zk_znode_count_max":    ("zk_znode_count",       "max_value"),
    "zk_ephemerals_max":     ("zk_ephemerals_count",  "max_value"),
    "zk_data_size_max":      ("zk_approximate_data_size", "max_value"),
}


def load_mntr(rows):
    by_metric = defaultdict(dict)
    with open(STAGING / "mntr.tsv") as f:
        for r in csv.DictReader(f, delimiter="\t"):
            key = (r["scenario"], r["backend"], r["commit_sha"])
            by_metric[(key, r["name"])] = r
    for (key, mname), src_row in by_metric.items():
        if key not in rows:
            continue
        for short, (full, col) in MNTR_KEEPS.items():
            if mname == full:
                rows[key][short] = float(src_row.get(col) or 0)
    return rows


def main():
    rows = load_bench()
    print(f"Loaded {len(rows)} (scenario,backend,sha) rows from bench_summary", file=sys.stderr)
    rows = load_container(rows)
    rows = load_prom_rates(rows)
    rows = load_prom_gauges(rows)
    rows = load_mntr(rows)

    # Determine the union of all column keys
    all_keys = set()
    for r in rows.values():
        all_keys.update(r.keys())
    base_cols = ["scenario", "backend", "commit_sha", "sha8", "run_ended"]
    metric_cols = sorted(k for k in all_keys if k not in {"sha8", "run_ended"})
    cols = base_cols + [c for c in metric_cols if c not in base_cols]

    out = ROOT / "merged_metrics.tsv"
    with open(out, "w") as f:
        w = csv.writer(f, delimiter="\t")
        w.writerow(cols)
        for (sc, be, sha), v in sorted(rows.items(), key=lambda x: (x[0][0], x[0][1], x[1].get("run_ended", ""))):
            row_out = [sc, be, sha]
            for c in cols[3:]:
                row_out.append(v.get(c, ""))
            w.writerow(row_out)
    print(f"Wrote {out} with {len(rows)} rows and {len(cols)} columns", file=sys.stderr)


if __name__ == "__main__":
    main()
