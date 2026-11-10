-- A mutation that rewrites only some columns must not keep the files of a column the new part does not
-- store, or a column of the same name made physical later reads them at its own type.

SET mutations_sync = 2;

DROP TABLE IF EXISTS t_not_physical;
DROP TABLE IF EXISTS t_dropped_while_detached;
DROP TABLE IF EXISTS t_renamed;
DROP TABLE IF EXISTS t_swapped;
DROP TABLE IF EXISTS t_nested;
DROP TABLE IF EXISTS t_nested_renamed;

CREATE TABLE t_not_physical (a UInt8, b Int64, c Int64, d Int64) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
    enable_block_number_column = 0, enable_block_offset_column = 0;
INSERT INTO t_not_physical VALUES (1, 5, 6, 7);
ALTER TABLE t_not_physical MODIFY COLUMN b Int64 ALIAS a, MODIFY COLUMN c Int64 ALIAS a, MODIFY COLUMN d Int64 EPHEMERAL;
ALTER TABLE t_not_physical ADD INDEX ia a TYPE minmax;
ALTER TABLE t_not_physical MATERIALIZE INDEX ia;
ALTER TABLE t_not_physical MODIFY COLUMN b Nullable(Int64) MATERIALIZED a;
ALTER TABLE t_not_physical MODIFY COLUMN c LowCardinality(String) MATERIALIZED toString(a);
ALTER TABLE t_not_physical MODIFY COLUMN d Int64 DEFAULT a + 100;
SELECT b FROM t_not_physical;
SELECT c FROM t_not_physical;
SELECT d FROM t_not_physical;

CREATE TABLE t_dropped_while_detached (a UInt8, w Int64) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
    enable_block_number_column = 0, enable_block_offset_column = 0;
INSERT INTO t_dropped_while_detached VALUES (1, 5);
ALTER TABLE t_dropped_while_detached DETACH PARTITION tuple();
ALTER TABLE t_dropped_while_detached DROP COLUMN w;
ALTER TABLE t_dropped_while_detached ATTACH PARTITION tuple();
ALTER TABLE t_dropped_while_detached ADD INDEX ia a TYPE minmax;
ALTER TABLE t_dropped_while_detached MATERIALIZE INDEX ia;
ALTER TABLE t_dropped_while_detached ADD COLUMN w Nullable(Int64) DEFAULT a;
SELECT w FROM t_dropped_while_detached;

CREATE TABLE t_renamed (a UInt8, r Int64) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
    enable_block_number_column = 0, enable_block_offset_column = 0;
INSERT INTO t_renamed VALUES (1, 5);
ALTER TABLE t_renamed MODIFY COLUMN r Int64 ALIAS a;
ALTER TABLE t_renamed RENAME COLUMN r TO r2;
ALTER TABLE t_renamed MODIFY COLUMN r2 Int64 DEFAULT a + 100;
SELECT r2 FROM t_renamed;

CREATE TABLE t_swapped (a UInt8, x Int64, y Int64) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
    enable_block_number_column = 0, enable_block_offset_column = 0;
INSERT INTO t_swapped VALUES (1, 5, 6);
ALTER TABLE t_swapped MODIFY COLUMN x Int64 ALIAS a;
ALTER TABLE t_swapped RENAME COLUMN x TO z, RENAME COLUMN y TO x;
SELECT x FROM t_swapped;
ALTER TABLE t_swapped MODIFY COLUMN z Int64 DEFAULT a + 100;
SELECT z FROM t_swapped;

CREATE TABLE t_nested (a UInt8, n Nested(x Int64, y Int64)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
    enable_block_number_column = 0, enable_block_offset_column = 0;
INSERT INTO t_nested VALUES (1, [1, 2], [3, 4]);
ALTER TABLE t_nested MODIFY COLUMN `n.x` Array(Int64) ALIAS [toInt64(a)];
ALTER TABLE t_nested ADD INDEX ia a TYPE minmax;
ALTER TABLE t_nested MATERIALIZE INDEX ia;
SELECT n.y FROM t_nested;

CREATE TABLE t_nested_renamed (a UInt8, n Nested(x Int64, y Int64)) ENGINE = MergeTree ORDER BY tuple()
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_bytes_for_full_part_storage = 0,
    enable_block_number_column = 0, enable_block_offset_column = 0;
INSERT INTO t_nested_renamed VALUES (1, [1, 2], [3, 4]);
ALTER TABLE t_nested_renamed MODIFY COLUMN `n.x` Array(Int64) ALIAS [toInt64(a)];
ALTER TABLE t_nested_renamed RENAME COLUMN `n.x` TO `n.z`;
SELECT n.y FROM t_nested_renamed;

DROP TABLE t_not_physical;
DROP TABLE t_dropped_while_detached;
DROP TABLE t_renamed;
DROP TABLE t_swapped;
DROP TABLE t_nested;
DROP TABLE t_nested_renamed;
