-- https://github.com/ClickHouse/ClickHouse/issues/40976
-- Test that OR conditions in JOIN ON and IS NOT DISTINCT FROM work correctly.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t1;
DROP TABLE IF EXISTS t2;

CREATE TABLE t1 (counter_id UInt32, utm_campaign Nullable(String), utm_source Nullable(String), utm_medium Nullable(String)) ENGINE = MergeTree ORDER BY counter_id;
CREATE TABLE t2 (counter_id UInt32, utm_campaign Nullable(String), utm_source Nullable(String), utm_medium Nullable(String), sessions UInt32, pageviews UInt32) ENGINE = MergeTree ORDER BY counter_id;

INSERT INTO t1 VALUES (1, 'camp1', 'src1', 'med1'), (1, NULL, 'src2', NULL), (2, 'camp2', NULL, 'med2');
INSERT INTO t2 VALUES (1, 'camp1', 'src1', 'med1', 10, 20), (1, 'camp1', 'src2', 'med1', 5, 8), (2, 'camp2', 'src2', 'med2', 3, 6), (2, 'camp3', 'src3', 'med3', 1, 2);

-- Approach 1: OR conditions in JOIN ON (was INVALID_JOIN_ON_EXPRESSION)
SELECT t1.counter_id, sum(t2.sessions) AS sessions, sum(t2.pageviews) AS pageviews, t1.utm_campaign, t1.utm_source, t1.utm_medium
FROM t1
INNER JOIN t2 ON t1.counter_id = t2.counter_id
    AND (t1.utm_campaign = t2.utm_campaign OR t1.utm_campaign IS NULL)
    AND (t1.utm_source = t2.utm_source OR t1.utm_source IS NULL)
    AND (t1.utm_medium = t2.utm_medium OR t1.utm_medium IS NULL)
GROUP BY t1.counter_id, t1.utm_campaign, t1.utm_source, t1.utm_medium
ORDER BY t1.counter_id, t1.utm_campaign, t1.utm_source, t1.utm_medium;

-- Approach 2: IS NOT DISTINCT FROM in JOIN ON (was SYNTAX_ERROR)
SELECT t1.counter_id, sum(t2.sessions) AS sessions, sum(t2.pageviews) AS pageviews, t1.utm_campaign, t1.utm_source, t1.utm_medium
FROM t1
INNER JOIN t2 ON t1.counter_id = t2.counter_id
    AND t1.utm_campaign IS NOT DISTINCT FROM t2.utm_campaign
    AND t1.utm_source IS NOT DISTINCT FROM t2.utm_source
    AND t1.utm_medium IS NOT DISTINCT FROM t2.utm_medium
GROUP BY t1.counter_id, t1.utm_campaign, t1.utm_source, t1.utm_medium
ORDER BY t1.counter_id, t1.utm_campaign, t1.utm_source, t1.utm_medium;

-- Approach 3: Tuple IS NOT DISTINCT FROM in JOIN ON
SELECT t1.counter_id, sum(t2.sessions) AS sessions, sum(t2.pageviews) AS pageviews, t1.utm_campaign, t1.utm_source, t1.utm_medium
FROM t1
INNER JOIN t2 ON t1.counter_id = t2.counter_id
    AND (t1.utm_campaign, t1.utm_source, t1.utm_medium) IS NOT DISTINCT FROM (t2.utm_campaign, t2.utm_source, t2.utm_medium)
GROUP BY t1.counter_id, t1.utm_campaign, t1.utm_source, t1.utm_medium
ORDER BY t1.counter_id, t1.utm_campaign, t1.utm_source, t1.utm_medium;

DROP TABLE t1;
DROP TABLE t2;
