-- A column added under the name of a subcolumn that a key or a skip index reads must not let old parts
-- be pruned or ordered by the values of the subcolumn they were written with.

-- A skip index over the subcolumn `a.x.y` of a `JSON` column `a` is not reused for the subcolumn of a new column `a.x`.
DROP TABLE IF EXISTS t_skip_index;
CREATE TABLE t_skip_index
(
    id UInt64,
    a JSON,
    INDEX i toUInt64(`a.x.y`) TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree ORDER BY id SETTINGS index_granularity = 1;
INSERT INTO t_skip_index VALUES (1, '{"x":{"y":11}}'), (2, '{"x":{"y":22}}');
ALTER TABLE t_skip_index ADD COLUMN `a.x` Tuple(y Dynamic) DEFAULT tuple(CAST(toUInt64(7), 'Dynamic'));
SELECT count() FROM t_skip_index WHERE toUInt64(`a.x.y`) = 7 SETTINGS use_skip_indexes = 1, optimize_move_to_prewhere = 0;
SELECT count() FROM t_skip_index WHERE toUInt64(`a.x.y`) = 7 SETTINGS use_skip_indexes = 0, optimize_move_to_prewhere = 0;
DROP TABLE t_skip_index;

-- A sorting key over the sizes of an `Array` column.
DROP TABLE IF EXISTS t_order_by;
CREATE TABLE t_order_by (a Array(UInt64)) ENGINE = MergeTree ORDER BY `a.size0` SETTINGS index_granularity = 1;
INSERT INTO t_order_by VALUES ([0]), ([1, 2]);
ALTER TABLE t_order_by ADD COLUMN `a.size0` UInt64 DEFAULT 7; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT count() FROM t_order_by WHERE `a.size0` = 2;
DROP TABLE t_order_by;

-- A partition key over the sizes of an `Array` column.
DROP TABLE IF EXISTS t_partition_by;
CREATE TABLE t_partition_by (a Array(UInt64)) ENGINE = MergeTree PARTITION BY `a.size0` ORDER BY tuple();
INSERT INTO t_partition_by VALUES ([0]), ([1, 2]);
ALTER TABLE t_partition_by ADD COLUMN `a.size0` UInt64 DEFAULT 7; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE t_partition_by;

-- A sorting key over a dynamic path of a `JSON` column.
DROP TABLE IF EXISTS t_json_key;
CREATE TABLE t_json_key (a JSON) ENGINE = MergeTree ORDER BY toUInt64(`a.x.y`);
INSERT INTO t_json_key VALUES ('{"x":{"y":11}}');
ALTER TABLE t_json_key ADD COLUMN `a.x` Tuple(y UInt64) DEFAULT tuple(7); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
DROP TABLE t_json_key;

-- A column whose name does not shadow a subcolumn used in the key is still added.
DROP TABLE IF EXISTS t_unrelated;
CREATE TABLE t_unrelated (a Array(UInt64), b Array(UInt64)) ENGINE = MergeTree ORDER BY `a.size0`;
INSERT INTO t_unrelated VALUES ([0], [1, 2]);
ALTER TABLE t_unrelated ADD COLUMN `b.size0` UInt64 DEFAULT 7;
SELECT `a.size0`, `b.size0` FROM t_unrelated;
DROP TABLE t_unrelated;
