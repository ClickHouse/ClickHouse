-- The sets of `GLOBAL IN (subquery)` in an explicit `PREWHERE` or a row policy of a `Memory` table are
-- evaluated inside the reading source, so they are built in place in `applyFilters`, the same way as
-- `ReadFromMergeTree` does. A set built in place does not need the `CreatingSet` branch and the
-- `DelayedPorts` gate in the pipeline, which is what this test asserts on. A `GLOBAL IN` condition in
-- `WHERE` is never moved to `PREWHERE` and keeps the pipeline-level set creation.

SET enable_analyzer = 1;
SET max_threads = 1;
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1;

DROP TABLE IF EXISTS t_memory_global_in_sets;
DROP TABLE IF EXISTS t_memory_global_in_keys;

CREATE TABLE t_memory_global_in_sets (k UInt64, s String) ENGINE = Memory;
INSERT INTO t_memory_global_in_sets SELECT number, toString(number) FROM numbers(10);

CREATE TABLE t_memory_global_in_keys (k UInt64) ENGINE = Memory;
INSERT INTO t_memory_global_in_keys VALUES (2), (4), (6);

SELECT '-- control: GLOBAL IN in WHERE stays above the source';
EXPLAIN PIPELINE SELECT k, s FROM t_memory_global_in_sets WHERE k GLOBAL IN (SELECT k FROM t_memory_global_in_keys);

SELECT '-- explicit PREWHERE with GLOBAL IN: the set is built in place';
EXPLAIN PIPELINE SELECT k, s FROM t_memory_global_in_sets PREWHERE k GLOBAL IN (SELECT k FROM t_memory_global_in_keys);

SELECT '-- a downstream JOIN with an empty right side closes its inputs early';
SELECT t.k, t.s
FROM t_memory_global_in_sets AS t
INNER JOIN (SELECT k FROM t_memory_global_in_keys WHERE k > 100) AS r ON t.k = r.k
PREWHERE t.k GLOBAL IN (SELECT k FROM t_memory_global_in_keys)
ORDER BY t.k;

SELECT k, s FROM t_memory_global_in_sets PREWHERE k GLOBAL NOT IN (SELECT k FROM t_memory_global_in_keys) ORDER BY k;

SELECT '-- a row policy with GLOBAL IN is built in place as well';
DROP ROW POLICY IF EXISTS p_memory_global_in_sets ON t_memory_global_in_sets;
CREATE ROW POLICY p_memory_global_in_sets ON t_memory_global_in_sets
    USING k GLOBAL IN (SELECT k FROM t_memory_global_in_keys) TO ALL;

EXPLAIN PIPELINE SELECT k, s FROM t_memory_global_in_sets;
SELECT k, s FROM t_memory_global_in_sets ORDER BY k;

SELECT t.k, t.s
FROM t_memory_global_in_sets AS t
INNER JOIN (SELECT k FROM t_memory_global_in_keys WHERE k > 100) AS r ON t.k = r.k
ORDER BY t.k;

DROP ROW POLICY p_memory_global_in_sets ON t_memory_global_in_sets;
DROP TABLE t_memory_global_in_keys;
DROP TABLE t_memory_global_in_sets;
