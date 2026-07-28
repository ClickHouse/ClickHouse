-- `ANY INNER`, `RIGHT ANY` and `RIGHT SEMI` joins with a constant-true predicate join only the first probe
-- row: with a probe side of many blocks, exactly one block claims the match and all the others must emit
-- nothing.

-- These joins return different results with the old analyzer, so the test enforces the analyzer.
SET enable_analyzer = 1;
SET query_plan_join_swap_table = 0;
SET enable_parallel_replicas = 0;
SET max_block_size = 1000;

SELECT 'any_inner';
SELECT count() FROM numbers(100000) AS l ANY INNER JOIN (SELECT 1 AS r) AS rt ON 1;

SELECT 'right_any';
SELECT r FROM numbers(100000) AS l RIGHT ANY JOIN (SELECT 42 AS r) AS rt ON 1;

SELECT 'right_semi';
SELECT r FROM numbers(100000) AS l RIGHT SEMI JOIN (SELECT 42 AS r) AS rt ON 1;
