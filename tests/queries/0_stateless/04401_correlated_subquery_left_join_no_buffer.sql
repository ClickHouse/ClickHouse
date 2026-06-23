-- Regression test for a logical error in correlated-subquery decorrelation with set operations.
-- The decorrelation builds a result JOIN; with correlated_subqueries_default_join_kind = 'left' the
-- subquery input ends up on the probe side of that join, so the in-memory result buffer (controlled by
-- correlated_subqueries_use_in_memory_buffer) cannot guarantee the producer finishes before the consumer
-- reads it. The buffer-vs-materialize decision used to be derived from the top-level default join kind,
-- which is not the per-branch join kind of a set operation, so a branch with a per-branch
-- SETTINGS correlated_subqueries_default_join_kind = 'left' still got buffered and aborted with
--   Logical error: Trying to extract chunk from ChunkBuffer before all inputs are finished
-- The fix decides materialization per decorrelation, so a left-join branch is never buffered.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_left_buffer_a;
DROP TABLE IF EXISTS t_left_buffer_b;

CREATE TABLE t_left_buffer_a (key UInt64, arr Array(String)) ENGINE = MergeTree ORDER BY key;
CREATE TABLE t_left_buffer_b (key UInt64, arr Array(String)) ENGINE = MergeTree ORDER BY key;

-- Several parts so the buffer would have multiple producer streams (the configuration that exposed the bug).
INSERT INTO t_left_buffer_a SELECT number,         ['a','b','c'] FROM numbers(2000);
INSERT INTO t_left_buffer_a SELECT number + 10000,  ['a','b','c'] FROM numbers(2000);
INSERT INTO t_left_buffer_a SELECT number + 20000,  ['a','b','c'] FROM numbers(2000);
INSERT INTO t_left_buffer_b SELECT number,         ['a','b','c'] FROM numbers(2000);
INSERT INTO t_left_buffer_b SELECT number + 10000,  ['a','b','c'] FROM numbers(2000);
INSERT INTO t_left_buffer_b SELECT number + 20000,  ['a','b','c'] FROM numbers(2000);

-- A left-join decorrelation branch must be materialized, not buffered: no ReadFromCommonBuffer in the pipeline.
SELECT count()
FROM
(
    EXPLAIN PIPELINE
    SELECT DISTINCT (SELECT arr WHERE 7) FROM t_left_buffer_a PREWHERE 38 QUALIFY -1
        SETTINGS allow_experimental_correlated_subqueries = 1, correlated_subqueries_use_in_memory_buffer = 1, correlated_subqueries_default_join_kind = 'left'
    INTERSECT
    SELECT DISTINCT (SELECT arr WHERE 7) FROM t_left_buffer_b PREWHERE 38 QUALIFY -1
        SETTINGS allow_experimental_correlated_subqueries = 1, correlated_subqueries_use_in_memory_buffer = 1, correlated_subqueries_default_join_kind = 'left'
)
WHERE explain ILIKE '%ReadFromCommonBuffer%';

-- A right-join decorrelation branch still uses the buffer (the optimization is intact for the safe case).
SELECT count() > 0
FROM
(
    EXPLAIN PIPELINE
    SELECT DISTINCT (SELECT arr WHERE 7) FROM t_left_buffer_a PREWHERE 38 QUALIFY -1
        SETTINGS allow_experimental_correlated_subqueries = 1, correlated_subqueries_use_in_memory_buffer = 1, correlated_subqueries_default_join_kind = 'right'
)
WHERE explain ILIKE '%ReadFromCommonBuffer%';

-- The left-join query must also execute without the logical error (run a few times: the bug was timing-dependent).
SELECT DISTINCT (SELECT arr WHERE 7) FROM t_left_buffer_a PREWHERE 38 QUALIFY -1
    SETTINGS allow_experimental_correlated_subqueries = 1, correlated_subqueries_use_in_memory_buffer = 1, correlated_subqueries_default_join_kind = 'left', max_threads = 8
INTERSECT
SELECT DISTINCT (SELECT arr WHERE 7) FROM t_left_buffer_b PREWHERE 38 QUALIFY -1
    SETTINGS allow_experimental_correlated_subqueries = 1, correlated_subqueries_use_in_memory_buffer = 1, correlated_subqueries_default_join_kind = 'left', max_threads = 8
FORMAT Null;

SELECT 1 AS ok;

DROP TABLE t_left_buffer_a;
DROP TABLE t_left_buffer_b;
