-- Regression test for https://github.com/ClickHouse/ClickHouse/issues/97771
-- (Invalid number of rows in Chunk with lazy materialization when an ALIAS column
-- shares its underlying expression with another selected column).
-- The bug fired for v25.8..v25.11 and surfaced as:
--   LOGICAL_ERROR: Invalid number of rows in Chunk ... String(size = 0) Lazy(size = 1)
--   at MergeTreeLazilyReader::transformLazyColumns / ColumnLazyTransform::transform.

DROP TABLE IF EXISTS t_97771_1;
DROP TABLE IF EXISTS t_97771_2;
DROP TABLE IF EXISTS t_97771_3;
DROP TABLE IF EXISTS t_97771_4;
DROP TABLE IF EXISTS t_97771_5;

-- { echoOn }

-- 1. Exact reproducer from the issue (https://fiddle.clickhouse.com/ba83d480-7fb5-4b7a-afbf-b1ca1d9f7aa9):
--    selecting both the underlying column and its ALIAS together with ORDER BY + LIMIT
--    on a tiny single-row table used to throw `Invalid number of rows in Chunk`.
CREATE TABLE t_97771_1 (c0 UInt64, c1 String, c2 String ALIAS c1) ENGINE = MergeTree() ORDER BY c0;
INSERT INTO t_97771_1 VALUES (1, 'a');
SELECT c1, c2 FROM t_97771_1 ORDER BY c0 LIMIT 1
SETTINGS query_plan_optimize_lazy_materialization = 1;

-- 2. Multi-row wide-part case with explicit lazy materialization. Wide parts and many rows
--    exercise the MergeTreeLazilyReader path more aggressively.
CREATE TABLE t_97771_2 (c0 UInt64, c1 String, c2 String ALIAS c1) ENGINE = MergeTree() ORDER BY c0
SETTINGS min_bytes_for_wide_part = 1, min_rows_for_wide_part = 1;
INSERT INTO t_97771_2 SELECT number, 'val_' || toString(number) FROM numbers(100);
SELECT c1, c2 FROM t_97771_2 ORDER BY c0 LIMIT 3
SETTINGS query_plan_optimize_lazy_materialization = 1;

-- 3. With a WHERE predicate that forces the LazyMaterializingTransform pipeline. Pre-fix this
--    is the path that built the malformed chunk reported in the stack trace.
CREATE TABLE t_97771_3 (c0 UInt64, c1 String, c2 String ALIAS c1) ENGINE = MergeTree() ORDER BY c0
SETTINGS min_bytes_for_wide_part = 1, min_rows_for_wide_part = 1;
INSERT INTO t_97771_3 SELECT number, 'val_' || toString(number) FROM numbers(100);
SELECT c1, c2 FROM t_97771_3 WHERE c0 > 0 ORDER BY c0 LIMIT 3
SETTINGS query_plan_optimize_lazy_materialization = 1;

-- 4. Selecting only the ALIAS column (no direct reference to the underlying column).
CREATE TABLE t_97771_4 (c0 UInt64, c1 String, c2 String ALIAS c1) ENGINE = MergeTree() ORDER BY c0;
INSERT INTO t_97771_4 VALUES (1, 'a'), (2, 'b');
SELECT c2 FROM t_97771_4 ORDER BY c0 LIMIT 1
SETTINGS query_plan_optimize_lazy_materialization = 1;

-- 5. ALIAS that wraps the underlying column in an expression - the lazy reader has to
--    materialize both columns through the alias DAG.
CREATE TABLE t_97771_5 (c0 UInt64, c1 String, c2 String ALIAS upperUTF8(c1)) ENGINE = MergeTree() ORDER BY c0;
INSERT INTO t_97771_5 VALUES (1, 'a'), (2, 'b');
SELECT c1, c2 FROM t_97771_5 ORDER BY c0 LIMIT 2
SETTINGS query_plan_optimize_lazy_materialization = 1;

-- { echoOff }

DROP TABLE t_97771_1;
DROP TABLE t_97771_2;
DROP TABLE t_97771_3;
DROP TABLE t_97771_4;
DROP TABLE t_97771_5;
