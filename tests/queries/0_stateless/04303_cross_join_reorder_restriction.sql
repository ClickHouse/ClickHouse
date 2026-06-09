SET enable_analyzer = 1;

DROP TABLE IF EXISTS t04303;
CREATE TABLE t04303 (minute DateTime, value Float64) ENGINE = MergeTree ORDER BY minute;
INSERT INTO t04303 VALUES (now(), 1.0);

SELECT count()
FROM ( SELECT toStartOfHour(now()) - toIntervalHour(number) AS hour FROM numbers(1, 24) ) AS h
CROSS JOIN ( SELECT '24h' AS window, 24 * 60 AS minutes
             UNION ALL SELECT '7d', 7 * 24 * 60 ) AS w
INNER JOIN t04303 AS v
    ON v.minute >= h.hour - toIntervalMinute(w.minutes)
   AND v.minute <  h.hour
SETTINGS query_plan_optimize_join_order_limit = 10;

DROP TABLE t04303;
