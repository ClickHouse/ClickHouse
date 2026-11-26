-- Per-PR-branch latest run on the 3 PR-level scenarios + master baseline at same date
SELECT
  branch,
  scenario,
  substring(commit_sha,1,8) AS sha8,
  max(ts)                                          AS run_ended,
  -- argMaxIf pulls each metric from the row where `ts` is max within rows
  -- of the requested `name` — i.e. the latest run's value. Plain `maxIf`
  -- would mix the latest `ts` (in `run_ended`) with peak-across-all-runs
  -- metric values when the same (branch, scenario, commit_sha) was retried.
  round(argMaxIf(value, ts, name='rps'), 0)              AS rps,
  round(argMaxIf(value, ts, name='read_p99_ms'), 1)      AS read_p99,
  round(argMaxIf(value, ts, name='write_p99_ms'), 1)     AS write_p99,
  round(argMaxIf(value, ts, name='error_rate')*100, 4)   AS error_pct,
  round(argMaxIf(value, ts, name='ops'), 0)              AS ops
FROM keeper_stress_tests.keeper_metrics_ts
WHERE source='bench' AND stage='summary'
  AND branch != 'master'
  AND ts >= '{{TS_FILTER}}'
  AND ts < '{{TS_FILTER_END}}'
GROUP BY branch, scenario, commit_sha
ORDER BY branch, scenario, run_ended DESC
FORMAT TSVWithNames
