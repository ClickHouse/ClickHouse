-- Prom gauges + cumulative-failure counters: max value across run, per-node
SELECT
  scenario, backend, commit_sha, name,
  round(max(value), 0) AS max_value,
  round(avg(value), 1) AS avg_value
FROM keeper_stress_tests.keeper_metrics_ts
WHERE source = 'prom'
  AND branch = 'master'
  AND ts >= '{{TS_FILTER}}'
  AND ts < '{{TS_FILTER_END}}'
  AND node IN ('keeper1', 'keeper2', 'keeper3')
  AND name IN (
    'ClickHouseMetrics_KeeperAliveConnections',
    'ClickHouseMetrics_KeeperOutstandingRequests',
    'ClickHouseAsyncMetrics_KeeperZnodeCount',
    'ClickHouseAsyncMetrics_KeeperApproximateDataSize',
    'ClickHouseAsyncMetrics_KeeperWatchCount',
    'ClickHouseAsyncMetrics_KeeperEphemeralsCount',
    'ClickHouseAsyncMetrics_KeeperLastCommittedLogIdx',
    'ClickHouseAsyncMetrics_KeeperLastLogIdx',
    'ClickHouseAsyncMetrics_KeeperLastLogTerm',
    'ClickHouseAsyncMetrics_KeeperLatestLogsCacheEntries',
    'ClickHouseAsyncMetrics_KeeperLatestLogsCacheSize',
    'ClickHouseAsyncMetrics_KeeperCommitLogsCacheEntries',
    'ClickHouseAsyncMetrics_KeeperCommitLogsCacheSize',
    'ClickHouseAsyncMetrics_KeeperOpenFileDescriptorCount',
    'ClickHouseAsyncMetrics_KeeperPathsWatched',
    'ClickHouseAsyncMetrics_KeeperSessionWithWatches',
    'ClickHouseProfileEvents_KeeperCommitsFailed',
    'ClickHouseProfileEvents_KeeperSnapshotCreationsFailed',
    'ClickHouseProfileEvents_KeeperSnapshotApplysFailed',
    'ClickHouseProfileEvents_KeeperRequestRejectedDueToSoftMemoryLimitCount'
  )
GROUP BY scenario, backend, commit_sha, name
ORDER BY scenario, backend, commit_sha, name
FORMAT TSVWithNames
