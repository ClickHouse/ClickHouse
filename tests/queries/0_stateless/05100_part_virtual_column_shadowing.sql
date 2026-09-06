-- https://github.com/ClickHouse/ClickHouse/issues/114214
-- A physical column named `_part` shadows the virtual one, which is then left out of the block used
-- to filter parts by virtual columns. Two places looked the virtual column up unconditionally and the
-- query failed with `NOT_FOUND_COLUMN_IN_BLOCK`.

DROP TABLE IF EXISTS t_part_shadow;
CREATE TABLE t_part_shadow (`_part` UInt32, x UInt32) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_part_shadow VALUES (1, 100), (2, 200), (3, 300);

SELECT count() FROM t_part_shadow WHERE isNull(_part > 0);
SELECT count() FROM t_part_shadow WHERE isNull(toNullable(_part) > 0);
SELECT count() FROM t_part_shadow WHERE _partition_id = 'all';
SELECT count() FROM t_part_shadow WHERE _part > 1;
SELECT sum(x) FROM t_part_shadow WHERE isNull(_part > 0);
SELECT sum(x) FROM t_part_shadow WHERE _part >= 2;
SELECT _part, x FROM t_part_shadow WHERE isNull(_part > 0);
SELECT _part, x FROM t_part_shadow ORDER BY _part;
SELECT count() FROM t_part_shadow;
SELECT count() FROM t_part_shadow WHERE isNull(_part > 0) SETTINGS optimize_use_implicit_projections = 0;
SELECT count() FROM t_part_shadow WHERE _partition_id = 'nonexistent';
DROP TABLE t_part_shadow;

SELECT 'the virtual column still works without a shadowing column';
DROP TABLE IF EXISTS t_part_virtual;
CREATE TABLE t_part_virtual (k UInt32, x UInt32) ENGINE = MergeTree PARTITION BY k ORDER BY tuple();
SYSTEM STOP MERGES t_part_virtual;
INSERT INTO t_part_virtual VALUES (1, 100);
INSERT INTO t_part_virtual VALUES (2, 200);
SELECT count() FROM t_part_virtual WHERE _part = '1_1_1_0';
SELECT count() FROM t_part_virtual WHERE _partition_id = '1';
SELECT uniqExact(_part) FROM t_part_virtual;
SELECT count() FROM t_part_virtual;
DROP TABLE t_part_virtual;
