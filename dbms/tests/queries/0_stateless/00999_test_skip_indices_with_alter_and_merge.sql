SET allow_experimental_data_skipping_indices=1;
DROP TABLE IF EXISTS table_test_creation;

CREATE TABLE table_test_creation (
  k UInt64,
  val1 UInt64,
  val2 UInt64,
  INDEX idx1 val1 * val2 TYPE minmax GRANULARITY 1
) ENGINE MergeTree()
ORDER BY k; -- { serverError 473 }

CREATE TABLE table_test_creation (
  k UInt64,
  val1 UInt64,
  val2 UInt64
) ENGINE MergeTree()
ORDER BY k;

ALTER TABLE table_test_creation ADD INDEX idx1 val1 * val2 TYPE minmax GRANULARITY 1; -- { serverError 473 }

ALTER TABLE table_test_creation ADD INDEX idx1 val1 TYPE minmax GRANULARITY 1;

ALTER TABLE table_test_creation MODIFY SETTING enable_vertical_merge_algorithm=0;

ALTER TABLE table_test_creation ADD INDEX idx2 val1 * val2 TYPE minmax GRANULARITY 1;

DROP TABLE IF EXISTS table_test_creation;

CREATE TABLE table_test_creation (
  k UInt64,
  val1 UInt64,
  val2 UInt64,
  INDEX idx1 val1 * val2 TYPE minmax GRANULARITY 1
) ENGINE MergeTree()
ORDER BY k SETTINGS enable_vertical_merge_algorithm=0;

DROP TABLE IF EXISTS table_test_creation;

DROP TABLE IF EXISTS test_vertical_merge;

CREATE TABLE test_vertical_merge (
  k UInt64,
  val1 UInt64,
  val2 UInt64,
  INDEX idx1 val2 TYPE minmax GRANULARITY 1
) ENGINE MergeTree()
ORDER BY k
SETTINGS vertical_merge_algorithm_min_rows_to_activate = 1, vertical_merge_algorithm_min_columns_to_activate = 1;

INSERT INTO test_vertical_merge SELECT number, number + 5, number * 12 from numbers(1000);

SELECT COUNT() from test_vertical_merge WHERE val2 <= 2400;

OPTIMIZE TABLE test_vertical_merge FINAL;

SELECT COUNT() from test_vertical_merge WHERE val2 <= 2400;

DROP TABLE IF EXISTS test_vertical_merge;

--DROP TABLE IF EXISTS test_alter_multiple_columns;
--
--CREATE TABLE test_alter_multiple_columns (
--  k UInt64,
--  val1 UInt64,
--  val2 UInt64,
--  INDEX idx1 val2 * val1 TYPE minmax GRANULARITY 1
--) ENGINE MergeTree()
--ORDER BY k
--SETTINGS enable_vertical_merge_algorithm=0;
--
--INSERT INTO test_alter_multiple_columns SELECT number, number + 5, number * 12 from numbers(1000);
--
--SELECT COUNT() from test_alter_multiple_columns WHERE val2 <= 2400;
--
--ALTER TABLE test_alter_multiple_columns MODIFY COLUMN val2 UInt16;
--
--SELECT COUNT() from test_alter_multiple_columns WHERE val2 <= 2400;
--
--DROP TABLE IF EXISTS test_alter_multiple_columns;
