# Metric glossary — what each `keeper_metrics_ts` column actually measures

Critical reference for correctly interpreting deltas. Many metrics have non-obvious gotchas.

## Sources

The `source` column distinguishes where a metric came from:
- `bench` — load-generator's own self-reporting at end of run (`stage='summary'`)
- `prom` — Keeper's Prometheus endpoint (`ClickHouseProfileEvents_*`, `ClickHouseAsyncMetrics_*`, `ClickHouseMetrics_*`)
- `mntr` — ZooKeeper-protocol 4LW `mntr` command output
- `container` — cgroup metrics from the Keeper container (CPU, memory)
- `dirs` — disk-usage metrics from the keeper data directory
- `lgif` — log-info dump
- `gate` — pass/fail gates from the framework

## Bench summary metrics (`source='bench' AND stage='summary'`)

| Metric | What it is | Gotcha |
|---|---|---|
| `rps` | total requests/sec averaged over the run | Includes warmup if `report_delay > 0` |
| `read_rps`, `write_rps` | split by op type | |
| `read_p50_ms`, `read_p99_ms`, `read_p99_99_ms` | latency percentiles for reads | Computed bench-side from observed latencies |
| `write_p50_ms`, `write_p99_ms`, `write_p99_99_ms` | latency percentiles for writes | Often bimodal: median is fast, p95 jumps to 100s of ms (queue knee) |
| `error_pct` | bench-counted errors as % of total | After 2026-04-04: includes client-timeout events that were previously hidden (#100670). Server-side errors are zero throughout — these are client-observed. |
| `errors` | absolute error count | |
| `ops` | total ops in the run | |

## Keeper memory — the most common misinterpretation

| Metric | Source | What it actually is |
|---|---|---|
| `container_memory_bytes` | container | **Cgroup memory.usage_in_bytes** — includes Keeper RSS + page cache + kernel slab + everything else in the container. Sensitive to bench workload patterns. |
| `ClickHouseAsyncMetrics_KeeperApproximateDataSize` | prom | **Keeper's own report of in-memory state size** — the znode tree, ephemerals, watches. Excludes page cache. |

**When the user asks about Keeper memory, use `KeeperApproximateDataSize`, not `container_memory_bytes`.** The cgroup metric can drop 26 % from a bench-harness change while Keeper's actual state is unchanged. See `known_confounds.md` for the #100670 example.

## Keeper CPU — counter-discontinuity hazard

| Metric | Source | Notes |
|---|---|---|
| `container_cpu_usage_usec` | container | Cumulative cgroup CPU time. To get cores, take rate-per-second and divide by 1e6. |
| `p95_cpu_cores` | derived | The 95th percentile of per-second cores. **Use this** for "how many cores was Keeper using". |
| `max_cpu_cores` | derived | The max of per-second cores. **DO NOT use this** — counter discontinuities (container restarts inside a run) can produce spurious 18-38-core spikes that aren't real. |

## Server-side failure counters (the trump cards)

These four prom counters must be **exactly zero** for Keeper to be healthy. Any non-zero value means the cluster broke a guarantee:

| Metric | What a non-zero value means |
|---|---|
| `ClickHouseProfileEvents_KeeperCommitsFailed` | Raft log-entry commit failed |
| `ClickHouseProfileEvents_KeeperSnapshotCreationsFailed` | Snapshot serialization failed |
| `ClickHouseProfileEvents_KeeperSnapshotApplysFailed` | Snapshot deserialization (apply) failed on a follower |
| `ClickHouseProfileEvents_KeeperRequestRejectedDueToSoftMemoryLimitCount` | Memory tracker rejected a request (Keeper had to refuse work) |

Always confirm 0 across all scenarios × all nightlies before declaring "shipped clean".

## Lock-contention metrics

| Metric | Source | Notes |
|---|---|---|
| `ClickHouseProfileEvents_KeeperStorageLockWaitMicroseconds` | prom | Total µs threads spent waiting on the storage mutex. Take rate-per-sec to get "µs of lock-wait per second of wall clock". Below 1000 µs/s = ~0.1 % of one core = essentially noise. |
| `ClickHouseProfileEvents_KeeperChangelogFileSyncMicroseconds` | prom | Time spent fsync'ing changelog. Dominated by disk speed. |
| `ClickHouseProfileEvents_FileSyncElapsedMicroseconds` | prom | Total fsync time across all files (changelog + snapshots). |

Movements on these are mostly informative for explaining WHY a rps/p99 changed, not standalone gates. Lock-wait reductions are the signature of `#100876` (`shared_mutex` for `KeeperLogStore`) and `#101502` (profiled-lock overhead reduction).

## ZK (mntr 4LW) metrics

| Metric | Notes |
|---|---|
| `zk_avg_latency` | average latency reported by `mntr`, in ms. Average over all requests since server start, so changes slowly. |
| `zk_max_latency` | **MAX latency observed by `mntr` since server start**. Single-tail outliers blow this up; treat changes ≤ 500 ms in absolute terms as noise. |
| `zk_outstanding_requests` | snapshot of queue depth at the moment `mntr` was run. Volatile. |
| `zk_packets_sent`, `zk_packets_received` | cumulative; take rate. |
| `zk_znode_count`, `zk_ephemerals_count`, `zk_watch_count` | snapshot gauges. |
| `zk_approximate_data_size` | snapshot in bytes. Approximately equivalent to `ClickHouseAsyncMetrics_KeeperApproximateDataSize` but sampled at `mntr` ticks rather than every metric flush. |

## Per-request-type counters

`ClickHouseProfileEvents_KeeperRequestTotal`, `KeeperMultiRequest`, `KeeperSetRequest`, `KeeperCreateRequest`, `KeeperRemoveRequest`, `KeeperGetRequest`, `KeeperListRequest`, `KeeperExistsRequest`, `KeeperCheckRequest`, `KeeperMultiReadRequest`, `KeeperAddWatchRequest`, `KeeperReconfigRequest`, etc.

These are cumulative ProfileEvents — take rate to get per-second values. Useful for confirming a workload composition matches expectations (e.g., `read-multi` should be dominated by `KeeperMultiReadRequest`).

## Throughput byte-rate

| Metric | What |
|---|---|
| `ClickHouseProfileEvents_KeeperChangelogWrittenBytes` | bytes written to changelog (rate) |
| `ClickHouseProfileEvents_KeeperSnapshotWrittenBytes` | bytes written to snapshots (rate) |
| `read_bytes_per_second`, `write_bytes_per_second` | bench-summary level byte throughput |

## Cache stats (Keeper internal)

| Metric | What |
|---|---|
| `ClickHouseAsyncMetrics_KeeperLatestLogsCacheEntries` / `KeeperLatestLogsCacheSize` | the in-memory cache for tail of changelog |
| `ClickHouseAsyncMetrics_KeeperCommitLogsCacheEntries` / `KeeperCommitLogsCacheSize` | cache for entries about to be committed |
| `ClickHouseProfileEvents_KeeperLogsEntryReadFromFile` | cache miss — reads that hit disk |
| `ClickHouseProfileEvents_KeeperLogsEntryReadFromLatestCache` | tail-cache hits |
| `ClickHouseProfileEvents_KeeperLogsEntryReadFromCommitCache` | commit-cache hits |
| `ClickHouseProfileEvents_KeeperLogsPrefetchedEntries` | prefetcher activity |

Lots of `KeeperLogsEntryReadFromFile` is bad — means cache misses dominate.

## Snapshot lifecycle

| Metric | What |
|---|---|
| `ClickHouseProfileEvents_KeeperSnapshotCreations` | how often a snapshot is created (rate) |
| `ClickHouseProfileEvents_KeeperSaveSnapshot` | how often save-snapshot is called |
| `ClickHouseProfileEvents_KeeperReadSnapshot` | how often a snapshot is read |
| `ClickHouseProfileEvents_KeeperSnapshotApplys` | how often a follower applies a snapshot |

These must correlate with `*Failed` counters (which must be 0).

## Leader/follower assignment

| Metric | What |
|---|---|
| `ClickHouseAsyncMetrics_KeeperIsLeader` | 1 if this node is leader, else 0 |
| `ClickHouseAsyncMetrics_KeeperIsFollower` | 1 if this node is follower, else 0 |

Exactly one of the 3 nodes should report `KeeperIsLeader=1` per run. Leadership is sticky within a run but rotates across runs — that's normal, not a regression.
