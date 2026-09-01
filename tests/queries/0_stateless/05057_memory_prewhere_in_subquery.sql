-- The row-level security filter and `PREWHERE` are evaluated inside the `Memory` reading source,
-- so a condition with `IN (subquery)` needs its set prepared before the source starts.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_memory_prewhere_in;
DROP TABLE IF EXISTS t_memory_prewhere_in_keys;

CREATE TABLE t_memory_prewhere_in (k UInt64, s String) ENGINE = Memory;
INSERT INTO t_memory_prewhere_in SELECT number, toString(number) FROM numbers(10);

CREATE TABLE t_memory_prewhere_in_keys (k UInt64) ENGINE = Memory;
INSERT INTO t_memory_prewhere_in_keys VALUES (2), (4), (6);

SELECT k, s FROM t_memory_prewhere_in PREWHERE k IN (SELECT k FROM t_memory_prewhere_in_keys) ORDER BY k;
SELECT k, s FROM t_memory_prewhere_in PREWHERE k NOT IN (SELECT k FROM t_memory_prewhere_in_keys) ORDER BY k;

-- The same condition, moved to `PREWHERE` by the optimizer instead of written explicitly.
SELECT k, s FROM t_memory_prewhere_in WHERE k IN (SELECT k FROM t_memory_prewhere_in_keys) ORDER BY k SETTINGS optimize_move_to_prewhere = 1;

-- A row policy with `IN (subquery)` is pushed into the source as well.
DROP ROW POLICY IF EXISTS p_memory_prewhere_in ON t_memory_prewhere_in;
CREATE ROW POLICY p_memory_prewhere_in ON t_memory_prewhere_in
    USING k IN (SELECT k FROM t_memory_prewhere_in_keys) TO ALL;

SELECT k, s FROM t_memory_prewhere_in ORDER BY k;
SELECT k, s FROM t_memory_prewhere_in PREWHERE s = '4' ORDER BY k;
SELECT count() FROM t_memory_prewhere_in;

DROP ROW POLICY p_memory_prewhere_in ON t_memory_prewhere_in;
DROP TABLE t_memory_prewhere_in_keys;
DROP TABLE t_memory_prewhere_in;
