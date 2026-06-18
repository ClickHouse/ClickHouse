-- One row per (scenario, backend, commit_sha) with full bench summary
SELECT
  scenario,
  backend,
  commit_sha,
  substring(commit_sha, 1, 8) AS sha8,
  max(ts) AS run_ended,
  -- argMaxIf pulls each metric from the row where `ts` is max within rows
  -- of the requested `name` — i.e. the latest run's value. Plain `maxIf`
  -- would mix the latest `ts` (in `run_ended`) with peak-across-all-runs
  -- metric values when the same (scenario, backend, commit_sha) was retried.
  round(argMaxIf(value, ts, name = 'rps'), 1)                    AS rps,
  round(argMaxIf(value, ts, name = 'read_rps'), 1)               AS read_rps,
  round(argMaxIf(value, ts, name = 'write_rps'), 1)              AS write_rps,
  round(argMaxIf(value, ts, name = 'read_p50_ms'), 2)            AS read_p50_ms,
  round(argMaxIf(value, ts, name = 'read_p95_ms'), 2)            AS read_p95_ms,
  round(argMaxIf(value, ts, name = 'read_p99_ms'), 2)            AS read_p99_ms,
  round(argMaxIf(value, ts, name = 'read_p99_90_ms'), 2)         AS read_p99_90_ms,
  round(argMaxIf(value, ts, name = 'read_p99_99_ms'), 2)         AS read_p99_99_ms,
  round(argMaxIf(value, ts, name = 'write_p50_ms'), 2)           AS write_p50_ms,
  round(argMaxIf(value, ts, name = 'write_p95_ms'), 2)           AS write_p95_ms,
  round(argMaxIf(value, ts, name = 'write_p99_ms'), 2)           AS write_p99_ms,
  round(argMaxIf(value, ts, name = 'write_p99_90_ms'), 2)        AS write_p99_90_ms,
  round(argMaxIf(value, ts, name = 'write_p99_99_ms'), 2)        AS write_p99_99_ms,
  round(argMaxIf(value, ts, name = 'errors'), 0)                 AS errors,
  round(argMaxIf(value, ts, name = 'error_rate') * 100, 4)       AS error_pct,
  round(argMaxIf(value, ts, name = 'ops'), 0)                    AS ops,
  round(argMaxIf(value, ts, name = 'read_bytes_per_second'), 0)  AS read_bps,
  round(argMaxIf(value, ts, name = 'write_bytes_per_second'), 0) AS write_bps,
  round(argMaxIf(value, ts, name = 'total_znode_delta'), 0)      AS znode_delta,
  round(argMaxIf(value, ts, name = 'total_dirs_bytes_delta'), 0) AS dirs_bytes_delta,
  round(argMaxIf(value, ts, name = 'bench_duration'), 0)         AS bench_duration_s
FROM keeper_stress_tests.keeper_metrics_ts
WHERE source = 'bench'
  AND stage = 'summary'
  AND branch = 'master'
  AND ts >= '{{TS_FILTER}}'
  AND ts < '{{TS_FILTER_END}}'
GROUP BY scenario, backend, commit_sha
ORDER BY scenario, backend, run_ended
FORMAT TSVWithNames
