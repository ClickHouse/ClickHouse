-- Random settings limits: optimize_move_to_prewhere=(1, None)
-- A Merge table reading another Merge table that declares narrower column types, with a condition
-- moved to PREWHERE by the second pass of optimize_prewhere_after_pushdown.

DROP TABLE IF EXISTS t_base;
DROP TABLE IF EXISTS m_inner;
DROP TABLE IF EXISTS m_outer;
DROP TABLE IF EXISTS t_nullable;

CREATE TABLE t_base (a UInt32, b UInt32, d LowCardinality(String), t LowCardinality(String)) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_base VALUES (1, 2, 'd2', 't8'), (3, 4, 'd1', 't1');
CREATE TABLE m_inner (a UInt32, b UInt32, d LowCardinality(String), t LowCardinality(String)) ENGINE = Merge(currentDatabase(), '^t_base$');
CREATE TABLE m_outer (a Nullable(UInt64), b Nullable(UInt64), d Nullable(String), t Nullable(String)) ENGINE = Merge(currentDatabase(), '^m_inner$');
CREATE TABLE t_nullable (a Nullable(UInt64), b Nullable(UInt64), d Nullable(String), t Nullable(String)) ENGINE = Memory;
INSERT INTO t_nullable VALUES (1, 2, 'd2', 't8');

SET optimize_prewhere_after_pushdown = 1, move_all_conditions_to_prewhere = 0, query_plan_optimize_prewhere = 1;

SELECT count() FROM m_outer WHERE a = 1 AND b = 2;
SELECT count() FROM m_outer WHERE b = 2 AND a = 1;
SELECT count() FROM m_outer WHERE d = 'd2' AND t = 't8' SETTINGS enable_multiple_prewhere_read_steps = 0;
SELECT count() FROM m_outer WHERE t = 't8' AND d = 'd2' SETTINGS enable_multiple_prewhere_read_steps = 1;
SELECT a, b, d, t FROM m_outer WHERE d = 'd2' AND t = 't8';
SELECT count() FROM m_outer WHERE d = 'd2' AND t = 't8' AND length(t) = 2;
-- The merge() table function derives Nullable types from the sibling table.
SELECT count() FROM merge(currentDatabase(), '^(m_inner|t_nullable)$') WHERE d = 'd2' AND t = 't8';
SELECT count() FROM merge(currentDatabase(), '^(m_inner|t_nullable)$') WHERE a = 1 AND b = 2;

DROP TABLE t_nullable;
DROP TABLE m_outer;
DROP TABLE m_inner;
DROP TABLE t_base;
