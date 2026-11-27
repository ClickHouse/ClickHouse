-- container metrics: peak memory bytes; CPU rate (cores) p95 & max
WITH base AS (
  SELECT scenario, backend, commit_sha, name, node,
    toUnixTimestamp(ts) AS t,
    max(value) AS v
  FROM keeper_stress_tests.keeper_metrics_ts
  WHERE source = 'container'
    AND branch = 'master'
    AND ts >= '{{TS_FILTER}}'
    AND ts < '{{TS_FILTER_END}}'
    AND node IN ('keeper1', 'keeper2', 'keeper3')
    AND name IN ('container_cpu_usage_usec', 'container_memory_bytes')
  GROUP BY scenario, backend, commit_sha, name, node, ts
),
cpu_rated AS (
  SELECT scenario, backend, commit_sha, node,
    (v - lagInFrame(v, 1) OVER (PARTITION BY scenario, backend, commit_sha, node ORDER BY t))
      / nullIf(t - lagInFrame(t, 1) OVER (PARTITION BY scenario, backend, commit_sha, node ORDER BY t), 0) / 1e6 AS cores
  FROM base
  WHERE name = 'container_cpu_usage_usec'
),
cpu_agg AS (
  SELECT scenario, backend, commit_sha,
    round(avg(cores), 2)             AS avg_cpu_cores,
    round(quantile(0.95)(cores), 2)  AS p95_cpu_cores,
    round(max(cores), 2)             AS max_cpu_cores
  FROM cpu_rated
  WHERE cores IS NOT NULL AND cores >= 0
  GROUP BY scenario, backend, commit_sha
),
mem_agg AS (
  SELECT scenario, backend, commit_sha,
    round(max(v) / 1e9, 3) AS peak_mem_gb,
    round(avg(v) / 1e9, 3) AS avg_mem_gb
  FROM base
  WHERE name = 'container_memory_bytes'
  GROUP BY scenario, backend, commit_sha
)
SELECT
  coalesce(m.scenario,   c.scenario)   AS scenario,
  coalesce(m.backend,    c.backend)    AS backend,
  coalesce(m.commit_sha, c.commit_sha) AS commit_sha,
  m.peak_mem_gb, m.avg_mem_gb,
  c.avg_cpu_cores, c.p95_cpu_cores, c.max_cpu_cores
FROM mem_agg m
FULL OUTER JOIN cpu_agg c
  ON m.scenario = c.scenario AND m.backend = c.backend AND m.commit_sha = c.commit_sha
ORDER BY scenario, backend, commit_sha
FORMAT TSVWithNames
