-- Tags: no-random-merge-tree-settings

-- Test that ALTER TABLE operations don't cause primary key type mismatches
-- when settings-dependent functions are used in the ORDER BY expression.
-- The bug: toMonday() returns Date or Date32 depending on
-- enable_extended_results_for_datetime_functions. If the table is created
-- with the setting enabled, ALTER TABLE (e.g., COMMENT COLUMN) would
-- re-evaluate the primary key expression with the table's global context
-- (where the setting is OFF), changing the type. Subsequent INSERT would
-- crash with "Bad cast from ColumnVector<unsigned short> to ColumnVector<int>".
-- See: https://github.com/ClickHouse/ClickHouse/issues/98135

SET enable_extended_results_for_datetime_functions = 1;
SET enable_parallel_replicas = 0;

-- Test 1: toMonday with Date32 + ALTER COMMENT COLUMN
-- Without fix, INSERT crashes in debug/sanitizer builds with:
-- "Bad cast from ColumnVector<unsigned short> to ColumnVector<int>"
DROP TABLE IF EXISTS t_pk_type_mismatch_1;
CREATE TABLE t_pk_type_mismatch_1 (c0 Date32) ENGINE = MergeTree() ORDER BY (toMonday(c0));
ALTER TABLE t_pk_type_mismatch_1 COMMENT COLUMN c0 'test comment';
INSERT INTO t_pk_type_mismatch_1 (c0) VALUES ('2024-01-01'), ('2024-06-15');
SELECT c0 FROM t_pk_type_mismatch_1 WHERE toMonday(c0) = '2024-01-01'::Date32 ORDER BY c0;
SELECT c0, toMonday(c0) FROM t_pk_type_mismatch_1 ORDER BY c0;
DROP TABLE t_pk_type_mismatch_1;

-- Test 2: Multiple date functions in ORDER BY
DROP TABLE IF EXISTS t_pk_type_mismatch_2;
CREATE TABLE t_pk_type_mismatch_2 (c0 Date32, c1 UInt64) ENGINE = MergeTree() ORDER BY (toStartOfMonth(c0), c1);
ALTER TABLE t_pk_type_mismatch_2 COMMENT COLUMN c0 'date col';
INSERT INTO t_pk_type_mismatch_2 VALUES ('2024-01-15', 1), ('2024-02-20', 2);
SELECT * FROM t_pk_type_mismatch_2 ORDER BY c0;
DROP TABLE t_pk_type_mismatch_2;

-- Test 3: With skip indices (combined expression path)
DROP TABLE IF EXISTS t_pk_type_mismatch_3;
CREATE TABLE t_pk_type_mismatch_3 (c0 Date32, val UInt64, INDEX idx_val val TYPE minmax GRANULARITY 1) ENGINE = MergeTree() ORDER BY (toMonday(c0));
ALTER TABLE t_pk_type_mismatch_3 COMMENT COLUMN c0 'with index';
INSERT INTO t_pk_type_mismatch_3 VALUES ('2024-01-01', 100), ('2024-03-15', 200);
SELECT c0, val FROM t_pk_type_mismatch_3 ORDER BY c0;
DROP TABLE t_pk_type_mismatch_3;

-- Test 4: DETACH/ATTACH path with toMonday (same type drift mechanism)
-- DETACH + ATTACH re-evaluates the sorting key expression during table reload.
-- If the reload uses a context without enable_extended_results_for_datetime_functions,
-- the key type drifts from Date32 to Date, causing the same mismatch.
DROP TABLE IF EXISTS t_pk_type_mismatch_4;
CREATE TABLE t_pk_type_mismatch_4 (c0 Date32) ENGINE = MergeTree() ORDER BY (toMonday(c0));
DETACH TABLE t_pk_type_mismatch_4;
ATTACH TABLE t_pk_type_mismatch_4;
INSERT INTO t_pk_type_mismatch_4 (c0) VALUES ('2024-01-01'), ('2024-06-15');
SELECT c0, toMonday(c0) FROM t_pk_type_mismatch_4 ORDER BY c0;
DROP TABLE t_pk_type_mismatch_4;

-- Test 5: Verify that other ALTER types also work
DROP TABLE IF EXISTS t_pk_type_mismatch_5;
CREATE TABLE t_pk_type_mismatch_5 (c0 Date32, c1 String DEFAULT 'x') ENGINE = MergeTree() ORDER BY (toMonday(c0));
INSERT INTO t_pk_type_mismatch_5 (c0) VALUES ('2024-01-01');
ALTER TABLE t_pk_type_mismatch_5 COMMENT COLUMN c1 'some comment';
INSERT INTO t_pk_type_mismatch_5 (c0) VALUES ('2024-06-15');
SELECT c0, c1 FROM t_pk_type_mismatch_5 ORDER BY c0;
DROP TABLE t_pk_type_mismatch_5;

-- Test 6: Skip index with settings-dependent expression (independent of sorting key)
-- The cast normalization must also cover skip-index expression outputs.
-- Here the ORDER BY key (c0) is not settings-dependent, but the skip index
-- expression (toMonday(c1)) changes return type based on
-- enable_extended_results_for_datetime_functions. Without the skip-index CAST fix,
-- INSERT after ALTER would crash during index serialization.
DROP TABLE IF EXISTS t_pk_type_mismatch_6;
CREATE TABLE t_pk_type_mismatch_6 (
    c0 UInt64,
    c1 Date32,
    INDEX idx_monday toMonday(c1) TYPE minmax GRANULARITY 1
) ENGINE = MergeTree() ORDER BY c0;
ALTER TABLE t_pk_type_mismatch_6 COMMENT COLUMN c0 'with skip index';
INSERT INTO t_pk_type_mismatch_6 VALUES (1, '2024-01-01'), (2, '2024-06-15');
SELECT c0, c1 FROM t_pk_type_mismatch_6 ORDER BY c0;
DROP TABLE t_pk_type_mismatch_6;

-- Test 7: Skip index with DETACH/ATTACH (settings-dependent skip index expression)
-- Same mechanism as Test 6 but using DETACH/ATTACH instead of ALTER.
DROP TABLE IF EXISTS t_pk_type_mismatch_7;
CREATE TABLE t_pk_type_mismatch_7 (
    c0 UInt64,
    c1 Date32,
    INDEX idx_month toStartOfMonth(c1) TYPE minmax GRANULARITY 1
) ENGINE = MergeTree() ORDER BY c0;
DETACH TABLE t_pk_type_mismatch_7;
ATTACH TABLE t_pk_type_mismatch_7;
INSERT INTO t_pk_type_mismatch_7 VALUES (1, '2024-01-15'), (2, '2024-06-20');
SELECT c0, c1 FROM t_pk_type_mismatch_7 ORDER BY c0;
DROP TABLE t_pk_type_mismatch_7;

-- Test 8: Both sorting key AND skip index with settings-dependent expressions
-- Verify that CAST normalization works for both simultaneously.
DROP TABLE IF EXISTS t_pk_type_mismatch_8;
CREATE TABLE t_pk_type_mismatch_8 (
    c0 Date32,
    c1 Date32,
    INDEX idx_monday toMonday(c1) TYPE minmax GRANULARITY 1
) ENGINE = MergeTree() ORDER BY toStartOfMonth(c0);
ALTER TABLE t_pk_type_mismatch_8 COMMENT COLUMN c0 'date col';
INSERT INTO t_pk_type_mismatch_8 VALUES ('2024-01-15', '2024-02-01'), ('2024-06-20', '2024-07-10');
SELECT c0, c1 FROM t_pk_type_mismatch_8 ORDER BY c0;
DROP TABLE t_pk_type_mismatch_8;
