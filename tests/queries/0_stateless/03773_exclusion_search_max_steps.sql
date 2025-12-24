-- Tags: no-replicated-database, no-parallel-replicas
-- no-replicated-database: EXPLAIN output differs for replicated database.
-- no-parallel-replicas: EXPLAIN output differs for parallel replicas.

DROP TABLE IF EXISTS t_exclusion_search_max_steps;

CREATE TABLE t_exclusion_search_max_steps (a int, b int) ENGINE = MergeTree() ORDER BY (a, b)
SETTINGS index_granularity = 1;

INSERT INTO t_exclusion_search_max_steps
SELECT 
  multiIf(number < 333, 1, number < 666, number, 1001), number
FROM numbers(1000);

OPTIMIZE TABLE t_exclusion_search_max_steps FINAL;

SET merge_tree_coarse_index_granularity = 8;

SELECT '-- Limit = 15';
SELECT ltrim(explain) FROM (
    EXPLAIN indexes = 1
    SELECT * FROM t_exclusion_search_max_steps
    WHERE b = 5
    SETTINGS merge_tree_exclusion_search_max_steps = 15
) WHERE explain LIKE '%Granules:%';

SELECT '-- Limit = 150';
SELECT ltrim(explain) FROM (
    EXPLAIN indexes = 1
    SELECT * FROM t_exclusion_search_max_steps
    WHERE b = 5
    SETTINGS merge_tree_exclusion_search_max_steps = 150
) WHERE explain LIKE '%Granules:%';

SELECT '-- No limit';
SELECT ltrim(explain) FROM (
    EXPLAIN indexes = 1
    SELECT * FROM t_exclusion_search_max_steps
    WHERE b = 5
    SETTINGS merge_tree_exclusion_search_max_steps = 0
) WHERE explain LIKE '%Granules:%';

DROP TABLE t_exclusion_search_max_steps;
