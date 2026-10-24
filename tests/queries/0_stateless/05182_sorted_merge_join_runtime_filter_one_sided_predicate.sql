-- A `partial_merge` listed before `sorted_merge` wins algorithm selection, and a merge join cannot
-- consume a join runtime filter, so the filter must not be planted for such a join - otherwise the
-- earlier merge algorithms are erased from the list and the query runs as `hash`. The guard therefore
-- has to decide `partial_merge` selectability exactly as physicalization does. A conjunct of the `ON`
-- section that reads from at most one side never blocks `MergeJoin`: it is either pushed down below
-- the join or attached to the clause as a filter condition column that `MergeJoin` consumes as a mask
-- key. This holds for a unary predicate such as `isNotNull(r.flag)` too, including in an `ANY` join,
-- where it cannot be pushed down. Only a cross-side non-equality condition becomes the mixed
-- condition that `MergeJoin` cannot evaluate.

DROP TABLE IF EXISTS smj_os_left;
DROP TABLE IF EXISTS smj_os_right;

CREATE TABLE smj_os_left (id UInt64, a UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE smj_os_right (id UInt64, b UInt64, flag Nullable(UInt8)) ENGINE = MergeTree ORDER BY id;

INSERT INTO smj_os_left SELECT number, number FROM numbers(100000);
INSERT INTO smj_os_right SELECT number, number * 2, if(number % 3 = 0, NULL, 1) FROM numbers(1000);

SET enable_analyzer = 1;
-- Pin the settings randomized in CI that the plan shape depends on: the in-order read must be allowed,
-- the sides are not swapped, the reads are local, and the runtime filter is not skipped as too cheap.
SET optimize_read_in_order = 1, query_plan_read_in_order = 1, query_plan_join_shard_by_pk_ranges = 0,
    query_plan_join_swap_table = 0, enable_parallel_replicas = 0,
    enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0,
    query_plan_optimize_join_order_limit = 1, explain_query_plan_default = 'legacy';
-- Disable automatic spilling, otherwise the printed algorithm name depends on the randomized limits.
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;

SET join_algorithm = 'partial_merge,sorted_merge,hash';

SELECT '--- INNER ANY with a one-sided unary `ON` predicate: `partial_merge`, no runtime filter ---';

SELECT * FROM (
EXPLAIN actions = 1
SELECT count() FROM smj_os_left AS l INNER ANY JOIN smj_os_right AS r ON l.id = r.id AND isNotNull(r.flag)
) WHERE explain LIKE '%Algorithm: %Join%' OR explain LIKE '%RuntimeFilter%';

SELECT '--- INNER ALL with the same predicate: `partial_merge`, no runtime filter ---';

SELECT * FROM (
EXPLAIN actions = 1
SELECT count() FROM smj_os_left AS l INNER JOIN smj_os_right AS r ON l.id = r.id AND isNotNull(r.flag)
) WHERE explain LIKE '%Algorithm: %Join%' OR explain LIKE '%RuntimeFilter%';

SELECT '--- LEFT ALL with the same predicate: `partial_merge`, no runtime filter ---';

SELECT * FROM (
EXPLAIN actions = 1
SELECT count() FROM smj_os_left AS l LEFT JOIN smj_os_right AS r ON l.id = r.id AND isNotNull(r.flag)
) WHERE explain LIKE '%Algorithm: %Join%' OR explain LIKE '%RuntimeFilter%';

SELECT '--- A one-sided binary predicate behaves the same way ---';

SELECT * FROM (
EXPLAIN actions = 1
SELECT count() FROM smj_os_left AS l INNER ANY JOIN smj_os_right AS r ON l.id = r.id AND r.b > 10
) WHERE explain LIKE '%Algorithm: %Join%' OR explain LIKE '%RuntimeFilter%';

SELECT '--- A cross-side non-equality condition is not `partial_merge`-selectable: `hash` with the filter ---';

SELECT * FROM (
EXPLAIN actions = 1
SELECT count() FROM smj_os_left AS l INNER ANY JOIN smj_os_right AS r ON l.id = r.id AND l.a > r.b
) WHERE explain LIKE '%Algorithm: %Join%' OR explain LIKE '%RuntimeFilter%';

SELECT '--- A disjunctive `ON` section is not `partial_merge`-selectable either ---';

SELECT * FROM (
EXPLAIN actions = 1
SELECT count() FROM smj_os_left AS l INNER JOIN smj_os_right AS r ON l.id = r.id OR l.a = r.b
) WHERE explain LIKE '%Algorithm: %Join%' OR explain LIKE '%RuntimeFilter%';

SELECT '--- Results are the same as with plain `hash` ---';

SELECT count() FROM smj_os_left AS l INNER ANY JOIN smj_os_right AS r ON l.id = r.id AND isNotNull(r.flag);
SELECT count() FROM smj_os_left AS l INNER ANY JOIN smj_os_right AS r ON l.id = r.id AND isNotNull(r.flag) SETTINGS join_algorithm = 'hash';
SELECT count() FROM smj_os_left AS l INNER JOIN smj_os_right AS r ON l.id = r.id AND isNotNull(r.flag);
SELECT count() FROM smj_os_left AS l INNER JOIN smj_os_right AS r ON l.id = r.id AND isNotNull(r.flag) SETTINGS join_algorithm = 'hash';
SELECT count() FROM smj_os_left AS l LEFT JOIN smj_os_right AS r ON l.id = r.id AND isNotNull(r.flag);
SELECT count() FROM smj_os_left AS l LEFT JOIN smj_os_right AS r ON l.id = r.id AND isNotNull(r.flag) SETTINGS join_algorithm = 'hash';
SELECT count() FROM smj_os_left AS l INNER ANY JOIN smj_os_right AS r ON l.id = r.id AND r.b > 10;
SELECT count() FROM smj_os_left AS l INNER ANY JOIN smj_os_right AS r ON l.id = r.id AND r.b > 10 SETTINGS join_algorithm = 'hash';

DROP TABLE smj_os_left;
DROP TABLE smj_os_right;
