-- Per-keeper-node prom counters → rate per sec, then aggregate across run
WITH base AS (
  SELECT
    scenario, backend, commit_sha, name, node,
    toUnixTimestamp(ts) AS t,
    max(value) AS v
  FROM keeper_stress_tests.keeper_metrics_ts
  WHERE source = 'prom'
    AND branch = 'master'
    AND ts >= '{{TS_FILTER}}'
    AND ts < '{{TS_FILTER_END}}'
    AND node IN ('keeper1', 'keeper2', 'keeper3')
    AND name IN (
      'ClickHouseProfileEvents_KeeperRequestTotal',
      'ClickHouseProfileEvents_KeeperCommits',
      'ClickHouseProfileEvents_KeeperPacketsSent',
      'ClickHouseProfileEvents_KeeperPacketsReceived',
      'ClickHouseProfileEvents_KeeperTotalElapsedMicroseconds',
      'ClickHouseProfileEvents_KeeperProcessElapsedMicroseconds',
      'ClickHouseProfileEvents_KeeperPreprocessElapsedMicroseconds',
      'ClickHouseProfileEvents_KeeperStorageLockWaitMicroseconds',
      'ClickHouseProfileEvents_FileSyncElapsedMicroseconds',
      'ClickHouseProfileEvents_KeeperChangelogFileSyncMicroseconds',
      'ClickHouseProfileEvents_KeeperCommitWaitElapsedMicroseconds',
      'ClickHouseProfileEvents_KeeperChangelogWrittenBytes',
      'ClickHouseProfileEvents_KeeperSnapshotWrittenBytes',
      'ClickHouseProfileEvents_KeeperSnapshotApplys',
      'ClickHouseProfileEvents_KeeperSnapshotCreations',
      'ClickHouseProfileEvents_KeeperLogsEntryReadFromFile',
      'ClickHouseProfileEvents_KeeperLogsPrefetchedEntries',
      'ClickHouseProfileEvents_KeeperLogsEntryReadFromCommitCache',
      'ClickHouseProfileEvents_KeeperLogsEntryReadFromLatestCache',
      'ClickHouseProfileEvents_KeeperLatency'
    )
  GROUP BY scenario, backend, commit_sha, name, node, ts
),
rated AS (
  SELECT
    scenario, backend, commit_sha, name, node,
    (v - lagInFrame(v, 1) OVER (PARTITION BY scenario, backend, commit_sha, name, node ORDER BY t))
      / nullIf(t - lagInFrame(t, 1) OVER (PARTITION BY scenario, backend, commit_sha, name, node ORDER BY t), 0) AS rate
  FROM base
),
agg_per_node AS (
  SELECT scenario, backend, commit_sha, name, node,
    avg(rate)            AS avg_rate,
    quantile(0.95)(rate) AS p95_rate,
    max(rate)            AS max_rate
  FROM rated
  WHERE rate IS NOT NULL AND rate >= 0
  GROUP BY scenario, backend, commit_sha, name, node
)
SELECT
  scenario,
  backend,
  commit_sha,
  name,
  round(avg(avg_rate), 2)  AS avg_rate_avg_node,
  round(max(avg_rate), 2)  AS avg_rate_max_node,
  round(max(p95_rate), 2)  AS p95_rate_max_node,
  round(max(max_rate), 2)  AS max_rate_max_node
FROM agg_per_node
GROUP BY scenario, backend, commit_sha, name
ORDER BY scenario, backend, commit_sha, name
FORMAT TSVWithNames
