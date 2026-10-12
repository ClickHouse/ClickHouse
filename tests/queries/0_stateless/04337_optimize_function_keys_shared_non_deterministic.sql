-- { echo }

-- Two structurally identical non-deterministic expressions (here rand64(g)) are deduplicated in the
-- actions DAG and evaluated once per row, so a key that combines another key with deterministic
-- functions only (here rand64(g) + 1) is determined by that key and can be removed. The count checks
-- verify the shared evaluation: the partitioning (LIMIT BY) and the grouping (GROUP BY) are the same
-- with and without the rewrite.

SET enable_analyzer = 1;
SET optimize_limit_by_function_keys = 1;
SET optimize_group_by_function_keys = 1;

DROP TABLE IF EXISTS test;
CREATE TABLE test (g UInt32, x UInt32) ENGINE = MergeTree ORDER BY (g, x);
INSERT INTO test SELECT number % 3 AS g, number AS x FROM numbers(12);

-- LIMIT BY: rand64(g) + 1 is removed as a deterministic function of the key rand64(g).
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT g FROM test ORDER BY g LIMIT 1 BY rand64(g), rand64(g) + 1;

-- GROUP BY: rand64(g) + 1 is removed as a deterministic function of the key rand64(g).
EXPLAIN SYNTAX run_query_tree_passes = 1 SELECT count() FROM test GROUP BY rand64(g), rand64(g) + 1;

-- Both expressions read the same evaluation of rand64(number), so the second one always equals the
-- first one plus one.
SELECT count() FROM (SELECT rand64(number) AS a, rand64(number) + 1 AS b FROM numbers(1000) WHERE b != a + 1);

-- LIMIT BY partitions with and without the rewrite: both queries produce one row per value of
-- rand64(number) % 2, that is, two rows.
SET optimize_limit_by_function_keys = 0;
SELECT count() FROM (SELECT number FROM numbers(1000) LIMIT 1 BY rand64(number) % 2, rand64(number) % 2 + 1);
SET optimize_limit_by_function_keys = 1;
SELECT count() FROM (SELECT number FROM numbers(1000) LIMIT 1 BY rand64(number) % 2, rand64(number) % 2 + 1);

-- GROUP BY groups with and without the rewrite: both queries produce one group per value of
-- rand64(number) % 2, that is, two groups.
SET optimize_group_by_function_keys = 0;
SELECT count() FROM (SELECT count() FROM numbers(1000) GROUP BY rand64(number) % 2, rand64(number) % 2 + 1);
SET optimize_group_by_function_keys = 1;
SELECT count() FROM (SELECT count() FROM numbers(1000) GROUP BY rand64(number) % 2, rand64(number) % 2 + 1);

DROP TABLE test;
