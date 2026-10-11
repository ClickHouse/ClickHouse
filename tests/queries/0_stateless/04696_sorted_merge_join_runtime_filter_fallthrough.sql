-- `sorted_merge` listed before `hash` suppresses the join runtime filter only when the join really can be
-- executed as a merge join: a merge join reads both sides concurrently, so it cannot consume a filter that
-- is complete only after the build side is read, and planting the filter would erase the merge algorithms
-- from the list. For a join shape the merge algorithm does not implement (`SEMI`, `ANTI`, a one-sided `ON`
-- condition, a disjunction), the selection falls through to `hash` - and then the runtime filter must stay,
-- otherwise `join_algorithm = 'sorted_merge,hash'` would be slower than plain `hash`.

DROP TABLE IF EXISTS smj_rf_left;
DROP TABLE IF EXISTS smj_rf_right;

CREATE TABLE smj_rf_left (id UInt64, a UInt64) ENGINE = MergeTree ORDER BY id;
CREATE TABLE smj_rf_right (id UInt64, b UInt64) ENGINE = MergeTree ORDER BY id;

INSERT INTO smj_rf_left SELECT number, number FROM numbers(100000);
INSERT INTO smj_rf_right SELECT number, number * 2 FROM numbers(1000);

SET enable_analyzer = 1;
-- Pin the settings randomized in CI that the plan shape depends on: the in-order read must be allowed,
-- the sides are not swapped, the reads are local, and the runtime filter is not skipped as too cheap.
SET optimize_read_in_order = 1, query_plan_read_in_order = 1, query_plan_join_shard_by_pk_ranges = 0,
    query_plan_join_swap_table = 0, enable_parallel_replicas = 0,
    enable_join_runtime_filters = 1, join_runtime_filter_min_probe_rows = 0,
    query_plan_optimize_join_order_limit = 1, explain_query_plan_default = 'legacy';
-- Disable automatic spilling, otherwise the printed algorithm name depends on the randomized limits.
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0;

SET join_algorithm = 'sorted_merge,hash';

SELECT '--- INNER ALL: merge join is selected, no runtime filter ---';

SELECT * FROM (
EXPLAIN actions = 1
SELECT count() FROM smj_rf_left AS l INNER JOIN smj_rf_right AS r ON l.id = r.id
) WHERE explain LIKE '%Algorithm: %Join%' OR explain LIKE '%RuntimeFilter%';

SELECT '--- LEFT SEMI: not supported by the merge algorithm, hash with a runtime filter ---';

SELECT * FROM (
EXPLAIN actions = 1
SELECT count() FROM smj_rf_left AS l LEFT SEMI JOIN smj_rf_right AS r ON l.id = r.id
) WHERE explain LIKE '%Algorithm: %Join%' OR explain LIKE '%RuntimeFilter%';

SELECT '--- LEFT ANTI: not supported by the merge algorithm, hash with a runtime filter ---';

SELECT * FROM (
EXPLAIN actions = 1
SELECT count() FROM smj_rf_left AS l LEFT ANTI JOIN smj_rf_right AS r ON l.id = r.id
) WHERE explain LIKE '%Algorithm: %Join%' OR explain LIKE '%RuntimeFilter%';

SELECT '--- Earlier `default` and `auto` win before `sorted_merge`, with a runtime filter ---';

SET join_algorithm = 'default,sorted_merge,hash';

SELECT * FROM (
EXPLAIN actions = 1
SELECT count() FROM smj_rf_left AS l INNER JOIN smj_rf_right AS r ON l.id = r.id
) WHERE explain LIKE '%Algorithm: %Join%' OR explain LIKE '%RuntimeFilter%';

SET join_algorithm = 'auto,sorted_merge,hash';

SELECT * FROM (
EXPLAIN actions = 1
SELECT count() FROM smj_rf_left AS l INNER JOIN smj_rf_right AS r ON l.id = r.id
) WHERE explain LIKE '%Algorithm: %Join%' OR explain LIKE '%RuntimeFilter%';

SELECT '--- Earlier `partial_merge` wins before `sorted_merge` ---';

SET join_algorithm = 'partial_merge,sorted_merge,hash';

SELECT * FROM (
EXPLAIN actions = 1
SELECT count() FROM smj_rf_left AS l INNER JOIN smj_rf_right AS r ON l.id = r.id
) WHERE explain LIKE '%Algorithm: %Join%' OR explain LIKE '%RuntimeFilter%';

SELECT '--- Unsupported earlier `partial_merge` falls through to `hash` with a runtime filter ---';

SELECT * FROM (
EXPLAIN actions = 1
SELECT count() FROM smj_rf_left AS l INNER JOIN smj_rf_right AS r ON l.id = r.id AND l.a > r.b
) WHERE explain LIKE '%Algorithm: %Join%' OR explain LIKE '%RuntimeFilter%';

SELECT '--- Results are the same as with plain `hash` ---';

SELECT count() FROM smj_rf_left AS l INNER JOIN smj_rf_right AS r ON l.id = r.id;
SELECT count() FROM smj_rf_left AS l INNER JOIN smj_rf_right AS r ON l.id = r.id SETTINGS join_algorithm = 'hash';
SELECT count() FROM smj_rf_left AS l LEFT SEMI JOIN smj_rf_right AS r ON l.id = r.id;
SELECT count() FROM smj_rf_left AS l LEFT SEMI JOIN smj_rf_right AS r ON l.id = r.id SETTINGS join_algorithm = 'hash';
SELECT count() FROM smj_rf_left AS l LEFT ANTI JOIN smj_rf_right AS r ON l.id = r.id;
SELECT count() FROM smj_rf_left AS l LEFT ANTI JOIN smj_rf_right AS r ON l.id = r.id SETTINGS join_algorithm = 'hash';

DROP TABLE smj_rf_left;
DROP TABLE smj_rf_right;
