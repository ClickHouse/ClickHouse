-- https://github.com/ClickHouse/ClickHouse/issues/122043
-- A lambda shared by WHERE and the SELECT list is computed before the sort, so its `ColumnFunction` (capturing `c3`)
-- reaches `LazyMaterializingTransform`, which squashes the sorted main chunks and needs `ColumnFunction::structureEquals`.
-- `max_threads = 1` with `max_block_size = 100` makes the 500 sorted rows arrive in several chunks deterministically.
-- The outer query uses `c4`, otherwise the lambda column is pruned and never reaches the squashing.
-- `SETTINGS` sit on the outer query because the plan optimizations ignore them on a subquery.

DROP TABLE IF EXISTS t_lazy_cf;
CREATE TABLE t_lazy_cf (c1 String, c2 DateTime64(6), c3 String) ENGINE = MergeTree ORDER BY c1;
INSERT INTO t_lazy_cf SELECT leftPad(toString(number), 8, '0'), toDateTime64('2026-08-01 00:00:00', 6) + number, if(number % 200 = 0, 'x ab', 'zz') FROM numbers(100000);

SELECT count(), countIf(c4 = 'AB'), min(c1), max(c1)
FROM
(
    SELECT c1, arrayFirst(s -> endsWith(upper(c3), s), ['AB', 'CD']) AS c4
    FROM t_lazy_cf
    WHERE arrayExists(s -> endsWith(upper(c3), s), ['AB', 'CD'])
    ORDER BY c2 DESC
    LIMIT 1000
)
SETTINGS query_plan_optimize_lazy_materialization = 1, query_plan_max_limit_for_lazy_materialization = 10000, max_threads = 1, max_block_size = 100;

SELECT count(), countIf(c4 = 'AB'), min(c1), max(c1)
FROM
(
    SELECT c1, arrayFirst(s -> endsWith(upper(c3), s), ['AB', 'CD']) AS c4
    FROM t_lazy_cf
    WHERE arrayExists(s -> endsWith(upper(c3), s), ['AB', 'CD'])
    ORDER BY c2 DESC
    LIMIT 1000
)
SETTINGS query_plan_optimize_lazy_materialization = 0, max_threads = 1, max_block_size = 100;

DROP TABLE t_lazy_cf;
