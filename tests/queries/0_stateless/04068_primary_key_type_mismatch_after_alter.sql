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

-- Test 1: toMonday with Date32 + ALTER COMMENT COLUMN
DROP TABLE IF EXISTS t_pk_type_mismatch_1;
CREATE TABLE t_pk_type_mismatch_1 (c0 Date32) ENGINE = MergeTree() ORDER BY (toMonday(c0));
ALTER TABLE t_pk_type_mismatch_1 COMMENT COLUMN c0 'test comment';
INSERT INTO t_pk_type_mismatch_1 (c0) VALUES ('2024-01-01');
INSERT INTO t_pk_type_mismatch_1 (c0) VALUES ('2024-06-15');
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

-- Test 4: DETACH/ATTACH with toTime and use_legacy_to_time
-- From: https://github.com/ClickHouse/ClickHouse/issues/98135#issuecomment-3983898898
-- The bug: toTime() resolves to different functions based on use_legacy_to_time.
-- Creating a table with default settings (legacy=true) stores DateTime key type.
-- DETACH + ATTACH with use_legacy_to_time=false re-evaluates toTime() as the
-- new Time function, changing the key type. Subsequent INSERT with legacy=true
-- would crash due to type mismatch between index serialization and evaluated key.
DROP TABLE IF EXISTS t_pk_type_mismatch_4;
CREATE TABLE t_pk_type_mismatch_4 (c0 DateTime) ENGINE = MergeTree() ORDER BY (toTime(c0));
SET use_legacy_to_time = 0;
DETACH TABLE t_pk_type_mismatch_4;
ATTACH TABLE t_pk_type_mismatch_4;
SET use_legacy_to_time = 1;
INSERT INTO TABLE t_pk_type_mismatch_4 (c0) VALUES ('2024-01-01 12:30:00');
INSERT INTO TABLE t_pk_type_mismatch_4 (c0) VALUES ('2024-06-15 08:45:00');
SELECT c0 FROM t_pk_type_mismatch_4 ORDER BY c0;
DROP TABLE t_pk_type_mismatch_4;

-- Test 5: Verify that other ALTER types also work
DROP TABLE IF EXISTS t_pk_type_mismatch_5;
CREATE TABLE t_pk_type_mismatch_5 (c0 Date32, c1 String DEFAULT 'x') ENGINE = MergeTree() ORDER BY (toMonday(c0));
INSERT INTO t_pk_type_mismatch_5 (c0) VALUES ('2024-01-01');
ALTER TABLE t_pk_type_mismatch_5 COMMENT COLUMN c1 'some comment';
INSERT INTO t_pk_type_mismatch_5 (c0) VALUES ('2024-06-15');
SELECT c0, c1 FROM t_pk_type_mismatch_5 ORDER BY c0;
DROP TABLE t_pk_type_mismatch_5;
