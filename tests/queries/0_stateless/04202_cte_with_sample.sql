-- Test: exercises `IdentifierNode::cloneImpl` preserving SAMPLE modifier through CTE
-- Covers: src/Analyzer/IdentifierNode.cpp:60 — clone of `table_expression_modifiers`
--         specifically the `sample_size_ratio` field, which the PR's own test does not exercise.
-- Risk: if `sample_size_ratio` is not preserved when an `IdentifierNode` is cloned during
--       CTE resolution, `SAMPLE 0.1` inside a CTE silently returns the full table → wrong results.

DROP TABLE IF EXISTS t_cte_sample;

CREATE TABLE t_cte_sample (a UInt64, b String)
ENGINE = MergeTree
ORDER BY (cityHash64(a), a)
SAMPLE BY cityHash64(a)
SETTINGS index_granularity = 1024;

INSERT INTO t_cte_sample SELECT number, toString(number) FROM numbers(100000);
OPTIMIZE TABLE t_cte_sample FINAL;

SET enable_analyzer = 1;

-- The query tree must show sample_size on the resolved table inside the CTE.
EXPLAIN QUERY TREE passes=1
WITH cte AS (
    SELECT * FROM t_cte_sample SAMPLE 0.1
)
SELECT count() FROM cte;

-- Data correctness: count via CTE must equal count via direct SAMPLE.
WITH cte AS (
    SELECT * FROM t_cte_sample SAMPLE 0.1
)
SELECT (SELECT count() FROM cte) = (SELECT count() FROM t_cte_sample SAMPLE 0.1) AS cte_matches_direct;

-- And the sampled count must be strictly less than the full table.
WITH cte AS (
    SELECT * FROM t_cte_sample SAMPLE 0.1
)
SELECT count() < 100000 AS sample_actually_applied FROM cte;

DROP TABLE t_cte_sample;
