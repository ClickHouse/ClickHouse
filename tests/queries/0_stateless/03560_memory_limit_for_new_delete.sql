SET max_memory_usage = 1000000000;
SET max_threads = 16;

DROP TABLE IF EXISTS table_visits;
CREATE TABLE IF NOT EXISTS table_visits (
  `visit_date` Nullable(Date),
  `visits` Nullable(Float64),
  `unique_visitors` Nullable(Int64)
) ENGINE = MergeTree() PARTITION BY tuple()
ORDER BY tuple();

WITH dateTrunc('month', visit_date) AS month,
  'nope' AS nope,
  'all' AS all,
  'lines' AS lines,
  sum(visits) AS sum_visits,
  sum(unique_visitors) AS sum_unique,
  sum(sum_visits) OVER (PARTITION BY all, nope ORDER BY month ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS win1,
  sum(sum_unique) OVER (PARTITION BY lines, all ORDER BY month ASC ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS win2
SELECT
    multiIf(win1 IS NULL, NULL, (sum_visits / ifnull(win1, 1))),
    multiIf(win2 IS NULL, NULL, (sum_unique / ifnull(win2, 1))),
    (sum_visits / sum(sum_visits) OVER (PARTITION BY nope, month)),
    (sum_unique / sum(sum_unique) OVER (PARTITION BY lines, month))
FROM table_visits
GROUP BY month
LIMIT 10 SETTINGS enable_analyzer = 1; -- { serverError MEMORY_LIMIT_EXCEEDED }
