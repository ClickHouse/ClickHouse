-- Tags: no-random-merge-tree-settings

-- Test that ALTER TABLE operations don't cause primary key type mismatches
-- when settings-dependent functions are used in the ORDER BY expression.
-- The bug: functions like toMonday() and toStartOfMinute() return different
-- types depending on enable_extended_results_for_datetime_functions.
-- If the table is created with the setting enabled, ALTER TABLE (e.g.,
-- COMMENT COLUMN) or DETACH/ATTACH would re-evaluate the primary key
-- expression with the table's global context (where the setting is OFF),
-- changing the type. Subsequent INSERT would crash with "Bad cast".
-- Variants:
--   Date32 key: "Bad cast from ColumnVector<unsigned short> to ColumnVector<int>"
--     (STID: 1499-*, see https://github.com/ClickHouse/ClickHouse/issues/98135)
--   DateTime64 key: "Bad cast from ColumnVector<unsigned int> to ColumnDecimal<DateTime64>"
--     (STID: 1559-62fb)

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

-- Test 9: DateTime64 primary key type mismatch (STID: 1559-62fb)
-- With enable_extended_results_for_datetime_functions=1:
--   toStartOfMinute(DateTime64(3)) returns DateTime64(3)
-- Without the setting (default=0):
--   toStartOfMinute(DateTime64(3)) returns DateTime (UInt32)
-- After ALTER, getCombinedIndicesExpression rebuilds with the table's global
-- context (setting=0), producing DateTime instead of DateTime64.
-- INSERT then crashes with:
-- "Bad cast from ColumnVector<unsigned int> to ColumnDecimal<DateTime64>"
DROP TABLE IF EXISTS t_pk_type_mismatch_9;
CREATE TABLE t_pk_type_mismatch_9 (c0 DateTime64(3)) ENGINE = MergeTree() ORDER BY (toStartOfMinute(c0));
ALTER TABLE t_pk_type_mismatch_9 COMMENT COLUMN c0 'timestamp col';
INSERT INTO t_pk_type_mismatch_9 VALUES ('2024-01-01 12:30:00.000'), ('2024-06-15 08:15:30.500');
SELECT c0, toStartOfMinute(c0) FROM t_pk_type_mismatch_9 ORDER BY c0;
DROP TABLE t_pk_type_mismatch_9;

-- Test 10: DateTime64 with DETACH/ATTACH (STID: 1559-62fb variant)
DROP TABLE IF EXISTS t_pk_type_mismatch_10;
CREATE TABLE t_pk_type_mismatch_10 (c0 DateTime64(3)) ENGINE = MergeTree() ORDER BY (toStartOfHour(c0));
DETACH TABLE t_pk_type_mismatch_10;
ATTACH TABLE t_pk_type_mismatch_10;
INSERT INTO t_pk_type_mismatch_10 VALUES ('2024-01-01 12:30:00.000'), ('2024-06-15 08:15:30.500');
SELECT c0, toStartOfHour(c0) FROM t_pk_type_mismatch_10 ORDER BY c0;
DROP TABLE t_pk_type_mismatch_10;

-- Test 11: DateTime64 with skip index (combined path)
DROP TABLE IF EXISTS t_pk_type_mismatch_11;
CREATE TABLE t_pk_type_mismatch_11 (
    c0 DateTime64(3),
    val UInt64,
    INDEX idx_hour toStartOfHour(c0) TYPE minmax GRANULARITY 1
) ENGINE = MergeTree() ORDER BY (toStartOfMinute(c0));
ALTER TABLE t_pk_type_mismatch_11 COMMENT COLUMN c0 'with DateTime64 index';
INSERT INTO t_pk_type_mismatch_11 VALUES ('2024-01-01 12:30:00.000', 100), ('2024-06-15 08:15:30.500', 200);
SELECT c0, val FROM t_pk_type_mismatch_11 ORDER BY c0;
DROP TABLE t_pk_type_mismatch_11;

-- Test 12: Mixed-settings lifecycle — key and skip index share the same expression
-- but with different expected types due to different settings at creation time.
-- The sorting key was created with enable_extended_results=1 (toMonday → Date32),
-- then the skip index is added with enable_extended_results=0 (toMonday → Date).
-- In getCombinedIndicesExpression, the DAG has one deduplicated output for toMonday(c0).
-- Without the collision fix, the index CAST overwrites the key CAST, and the primary
-- index serializer crashes reading Date (UInt16) as Date32 (Int32).
DROP TABLE IF EXISTS t_pk_type_mismatch_12;
CREATE TABLE t_pk_type_mismatch_12 (c0 Date32) ENGINE = MergeTree() ORDER BY (toMonday(c0));

SET enable_extended_results_for_datetime_functions = 0;
ALTER TABLE t_pk_type_mismatch_12 ADD INDEX idx_monday toMonday(c0) TYPE minmax GRANULARITY 1;

INSERT INTO t_pk_type_mismatch_12 (c0) VALUES ('2024-01-01'), ('2024-06-15');
SELECT c0, toMonday(c0) FROM t_pk_type_mismatch_12 ORDER BY c0;
DROP TABLE t_pk_type_mismatch_12;

-- Restore setting for subsequent tests
SET enable_extended_results_for_datetime_functions = 1;

-- Test 13: Same as Test 12 but with DateTime64 (toStartOfMinute)
-- Key created with setting=1: toStartOfMinute(DateTime64) → DateTime64
-- Index added with setting=0: toStartOfMinute(DateTime64) → DateTime (UInt32)
DROP TABLE IF EXISTS t_pk_type_mismatch_13;
CREATE TABLE t_pk_type_mismatch_13 (c0 DateTime64(3)) ENGINE = MergeTree() ORDER BY (toStartOfMinute(c0));

SET enable_extended_results_for_datetime_functions = 0;
ALTER TABLE t_pk_type_mismatch_13 ADD INDEX idx_minute toStartOfMinute(c0) TYPE minmax GRANULARITY 1;

INSERT INTO t_pk_type_mismatch_13 VALUES ('2024-01-01 12:30:00.000'), ('2024-06-15 08:15:30.500');
SELECT c0, toStartOfMinute(c0) FROM t_pk_type_mismatch_13 ORDER BY c0;
DROP TABLE t_pk_type_mismatch_13;

-- Restore setting
SET enable_extended_results_for_datetime_functions = 1;
