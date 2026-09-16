SET explain_query_plan_default = 'legacy';
SET enable_analyzer = 1;
SET enable_parallel_replicas = 0;
SET query_plan_join_swap_table = 0;
SET enable_join_runtime_filters = 0;
SET query_plan_optimize_join_order_randomize = 0; -- the test asserts on the join plan
SET join_algorithm = 'hash';
SET max_bytes_before_external_join = 0, max_bytes_ratio_before_external_join = 0; -- Disable automatic spilling for this test
SET optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1;
SET query_plan_filter_push_down_over_any_inner_join = 0;

CREATE TABLE t1 (k UInt64, a UInt64) ENGINE = MergeTree ORDER BY k;
CREATE TABLE t2 (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY k;
INSERT INTO t1 VALUES (1, 100), (2, 200), (1, 200);
INSERT INTO t2 VALUES (1, 10), (1, 20), (2, 30);

SELECT '-- non-key filter must not be pushed to the right side';
SELECT explain FROM (
    EXPLAIN actions = 1
    SELECT * FROM t1 ANY INNER JOIN t2 ON t1.k = t2.k WHERE t2.v = 1
) WHERE explain ilike '%Filter column%' OR explain ilike '%Strictness%' OR explain ilike '%Type:%';
SELECT '-- non-key filter must not be pushed to the left side';
SELECT explain FROM (
    EXPLAIN actions = 1
    SELECT * FROM t1 ANY INNER JOIN t2 ON t1.k = t2.k WHERE t1.a = 1
) WHERE explain ilike '%Filter column%' OR explain ilike '%Strictness%' OR explain ilike '%Type:%';

SELECT '-- non-key filter may change the output set if pushed right';
SELECT
    (SELECT count() FROM t1 ANY JOIN t2 USING (k) WHERE t2.v > 10)
    = (SELECT sum(t2.v > 10) FROM t1 ANY JOIN t2 USING (k));

SELECT '-- non-key filter may change the output set if pushed left';
SELECT
    (SELECT count() FROM t1 ANY JOIN t2 USING (k) WHERE t1.a = 200)
    = (SELECT sum(t1.a = 200) FROM t1 ANY JOIN t2 USING (k));

SELECT '-- equi-key filter on the right column is still pushed to both sides';
SELECT explain FROM (
    EXPLAIN actions = 1
    SELECT * FROM t1 ANY INNER JOIN t2 ON t1.k = t2.k WHERE t2.k = 1
) WHERE explain ilike '%Filter column%' OR explain ilike '%Strictness%' OR explain ilike '%Type:%';

SELECT '-- equi-key filter on the left column is still pushed to both sides';
SELECT explain FROM (
    EXPLAIN actions = 1
    SELECT * FROM t1 ANY INNER JOIN t2 ON t1.k = t2.k WHERE t1.k = 1
) WHERE explain ilike '%Filter column%' OR explain ilike '%Strictness%' OR explain ilike '%Type:%';

SET query_plan_filter_push_down_over_any_inner_join = 1;

-- The same rows ordered by the non-key column, one row per granule:
-- the predicate prunes the primary key only if it reaches the read below the join.
CREATE TABLE t1_by_a (k UInt64, a UInt64) ENGINE = MergeTree ORDER BY a SETTINGS index_granularity = 1;
CREATE TABLE t2_by_v (k UInt64, v UInt64) ENGINE = MergeTree ORDER BY v SETTINGS index_granularity = 1;
INSERT INTO t1_by_a VALUES (1, 100), (2, 200), (1, 200);
INSERT INTO t2_by_v VALUES (1, 10), (1, 20), (2, 30);

SELECT '-- push-down allowed: non-key filter on the right column prunes the right side';
SELECT explain FROM (
    EXPLAIN indexes = 1
    SELECT * FROM t1_by_a ANY INNER JOIN t2_by_v ON t1_by_a.k = t2_by_v.k WHERE t2_by_v.v = 10
) WHERE explain ilike '%Condition:%' OR explain ilike '%Granules:%';

SELECT '-- push-down allowed: non-key filter on the left column prunes the left side';
SELECT explain FROM (
    EXPLAIN indexes = 1
    SELECT * FROM t1_by_a ANY INNER JOIN t2_by_v ON t1_by_a.k = t2_by_v.k WHERE t1_by_a.a = 100
) WHERE explain ilike '%Condition:%' OR explain ilike '%Granules:%';

-- The side is filtered before the join, so a key keeps a match whenever any of its rows qualifies:
-- both results are the number of keys with a qualifying row, whichever row the join takes.
SELECT '-- push-down allowed: a key keeps a match whenever any of its right rows qualifies';
SELECT count() FROM t1 ANY JOIN t2 USING (k) WHERE t2.v > 10;
SELECT '-- push-down allowed: a key keeps a match whenever any of its left rows qualifies';
SELECT count() FROM t1 ANY JOIN t2 USING (k) WHERE t1.a = 200;
