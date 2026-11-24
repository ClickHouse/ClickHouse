-- https://github.com/ClickHouse/ClickHouse/issues/122043
-- Same as 05033, but the shared lambda also holds a prepared set (`IN (SELECT ...)`) in its body. The set stays inside the
-- lambda expression rather than becoming a captured column, so the squashed `ColumnFunction`s must be recognized as the
-- same callable through `FunctionExpression::isEqual`, while the captured `c3` goes through `structureEquals`.
-- `max_threads = 1` with `max_block_size = 100` makes the 500 sorted rows arrive in several chunks deterministically.
-- The outer query uses `c4`, otherwise the lambda column is pruned and never reaches the squashing.

DROP TABLE IF EXISTS t_lazy_cf_set;
CREATE TABLE t_lazy_cf_set (c1 String, c2 DateTime64(6), c3 String, arr Array(String)) ENGINE = MergeTree ORDER BY c1;
INSERT INTO t_lazy_cf_set
SELECT leftPad(toString(number), 8, '0'), toDateTime64('2026-08-01 00:00:00', 6) + number, 'v', if(number % 200 = 0, ['2', 'x'], ['zz'])
FROM numbers(100000);

SELECT count(), countIf(c4 = '2'), min(c1), max(c1)
FROM
(
    SELECT c1, arrayFirst(x -> x IN (SELECT '2') AND c3 != '', arr) AS c4
    FROM t_lazy_cf_set
    WHERE arrayExists(x -> x IN (SELECT '2') AND c3 != '', arr)
    ORDER BY c2 DESC
    LIMIT 1000
)
SETTINGS query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 10000, max_threads = 1, max_block_size = 100;

SELECT count(), countIf(c4 = '2'), min(c1), max(c1)
FROM
(
    SELECT c1, arrayFirst(x -> x IN (SELECT '2') AND c3 != '', arr) AS c4
    FROM t_lazy_cf_set
    WHERE arrayExists(x -> x IN (SELECT '2') AND c3 != '', arr)
    ORDER BY c2 DESC
    LIMIT 1000
)
SETTINGS query_plan_optimize_lazy_materialization = 0, max_threads = 1, max_block_size = 100;

DROP TABLE t_lazy_cf_set;
