-- mntr (4LW) metrics — gauges max + rates for packets
WITH base AS (
  SELECT scenario, backend, commit_sha, name, node,
    toUnixTimestamp(ts) AS t,
    max(value) AS v
  FROM keeper_stress_tests.keeper_metrics_ts
  WHERE source = 'mntr'
    AND branch = 'master'
    AND ts >= '{{TS_FILTER}}'
    AND ts < '{{TS_FILTER_END}}'
    AND node IN ('keeper1', 'keeper2', 'keeper3')
    AND name IN (
      'zk_avg_latency', 'zk_max_latency', 'zk_min_latency',
      'zk_packets_sent', 'zk_packets_received',
      'zk_outstanding_requests', 'zk_watch_count',
      'zk_znode_count', 'zk_ephemerals_count',
      'zk_approximate_data_size'
    )
  GROUP BY scenario, backend, commit_sha, name, node, ts
),
counters AS (
  SELECT scenario, backend, commit_sha, name, node, v,
    (v - lagInFrame(v, 1) OVER (PARTITION BY scenario, backend, commit_sha, name, node ORDER BY t))
      / nullIf(t - lagInFrame(t, 1) OVER (PARTITION BY scenario, backend, commit_sha, name, node ORDER BY t), 0) AS rate
  FROM base
)
SELECT
  scenario, backend, commit_sha, name,
  round(max(v), 0) AS max_value,
  round(avg(v), 1) AS avg_value,
  round(max(rate), 0) AS max_rate,
  round(avg(rate), 1) AS avg_rate
FROM counters
GROUP BY scenario, backend, commit_sha, name
ORDER BY scenario, backend, commit_sha, name
FORMAT TSVWithNames
