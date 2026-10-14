-- The row-level security filter and `PREWHERE` run inside the `Memory` reading source, so the sets of
-- their `IN (subquery)` conditions have to be built while the plan is optimized, in `applyFilters` and
-- `updatePrewhereInfo`, and not when the pipeline is built: by then `CreatingSetsStep` has already
-- taken the subquery plans. A set built in place does not need the `CreatingSet` branch and the
-- `DelayedPorts` gate in the pipeline, which is what this test asserts on.

SET enable_analyzer = 1;
SET max_threads = 1;
SET optimize_move_to_prewhere = 1;
SET query_plan_optimize_prewhere = 1;

DROP TABLE IF EXISTS t_memory_in_place_sets;
DROP TABLE IF EXISTS t_memory_in_place_keys;

CREATE TABLE t_memory_in_place_sets (k UInt64, s String) ENGINE = Memory;
INSERT INTO t_memory_in_place_sets SELECT number, toString(number) FROM numbers(10);

CREATE TABLE t_memory_in_place_keys (k UInt64) ENGINE = Memory;
INSERT INTO t_memory_in_place_keys VALUES (2), (4), (6);

SELECT '-- control: a filter that stays above the source keeps the pipeline-level set creation';
EXPLAIN PIPELINE SELECT k, s FROM t_memory_in_place_sets WHERE k IN (SELECT k FROM t_memory_in_place_keys) SETTINGS optimize_move_to_prewhere = 0;

SELECT '-- explicit PREWHERE: the set is built in place';
EXPLAIN PIPELINE SELECT k, s FROM t_memory_in_place_sets PREWHERE k IN (SELECT k FROM t_memory_in_place_keys);

SELECT '-- moved to PREWHERE by the optimizer, after applyFilters already ran';
EXPLAIN PIPELINE SELECT k, s FROM t_memory_in_place_sets WHERE k IN (SELECT k FROM t_memory_in_place_keys);

SELECT '-- the shape that motivated this: a downstream JOIN with an empty right side closes its inputs early';
SELECT t.k, t.s
FROM t_memory_in_place_sets AS t
INNER JOIN (SELECT k FROM t_memory_in_place_keys WHERE k > 100) AS r ON t.k = r.k
PREWHERE t.k IN (SELECT k FROM t_memory_in_place_keys)
ORDER BY t.k;

SELECT t.k, t.s
FROM t_memory_in_place_sets AS t
INNER JOIN (SELECT k FROM t_memory_in_place_keys WHERE k > 100) AS r ON t.k = r.k
WHERE t.k IN (SELECT k FROM t_memory_in_place_keys)
ORDER BY t.k;

SELECT t.k, t.s
FROM t_memory_in_place_sets AS t
INNER JOIN (SELECT k FROM t_memory_in_place_keys WHERE k > 100) AS r ON t.k = r.k
PREWHERE t.k NOT IN (SELECT k FROM t_memory_in_place_keys)
ORDER BY t.k;

SELECT '-- a row policy with IN (subquery) is built in place as well';
DROP ROW POLICY IF EXISTS p_memory_in_place_sets ON t_memory_in_place_sets;
CREATE ROW POLICY p_memory_in_place_sets ON t_memory_in_place_sets
    USING k IN (SELECT k FROM t_memory_in_place_keys) TO ALL;

EXPLAIN PIPELINE SELECT k, s FROM t_memory_in_place_sets;
SELECT k, s FROM t_memory_in_place_sets ORDER BY k;

SELECT t.k, t.s
FROM t_memory_in_place_sets AS t
INNER JOIN (SELECT k FROM t_memory_in_place_keys WHERE k > 100) AS r ON t.k = r.k
ORDER BY t.k;

DROP ROW POLICY p_memory_in_place_sets ON t_memory_in_place_sets;
DROP TABLE t_memory_in_place_keys;
DROP TABLE t_memory_in_place_sets;
