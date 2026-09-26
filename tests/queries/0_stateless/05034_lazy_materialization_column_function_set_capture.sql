-- https://github.com/ClickHouse/ClickHouse/issues/122043
-- Lambda with IN (ColumnSet) must squash under lazy materialization without structureEquals throwing.

DROP TABLE IF EXISTS t_lazy_cf_set;
CREATE TABLE t_lazy_cf_set (c1 String, c2 DateTime64(6), arr Array(String)) ENGINE = MergeTree ORDER BY c1;
INSERT INTO t_lazy_cf_set
SELECT leftPad(toString(number), 8, '0'), toDateTime64('2026-08-01 00:00:00', 6) + number,
       if(number % 2000 = 0, ['2', 'x'], ['zz'])
FROM numbers(1000000);

SELECT count()
FROM
(
    SELECT c1, arrayFirst(x -> x IN ('2', 'ab'), arr) AS c4
    FROM t_lazy_cf_set
    WHERE arrayExists(x -> x IN ('2', 'ab'), arr)
    ORDER BY c2 DESC
    LIMIT 1000
    SETTINGS query_plan_optimize_lazy_materialization = 1, max_threads = 2
);

SELECT count()
FROM
(
    SELECT c1, arrayFirst(x -> x IN ('2', 'ab'), arr) AS c4
    FROM t_lazy_cf_set
    WHERE arrayExists(x -> x IN ('2', 'ab'), arr)
    ORDER BY c2 DESC
    LIMIT 1000
    SETTINGS query_plan_optimize_lazy_materialization = 0, max_threads = 2
);

DROP TABLE t_lazy_cf_set;
