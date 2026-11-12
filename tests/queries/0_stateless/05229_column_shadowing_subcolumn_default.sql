-- A stored column may not newly take the name of a subcolumn of another column (`a.size0` next to an
-- `Array` column `a`): the existing parts do not hold it, and their keys, skip indices, projections and
-- TTL infos may have been computed from the subcolumn of that name. `CREATE TABLE` and `ALIAS` columns
-- stay allowed, and a part that does not hold such a column reads its default, not the subcolumn.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_shadow_alter;
CREATE TABLE t_shadow_alter (a Array(UInt64), t Tuple(x UInt64), j JSON, s Nullable(String), `c.size0` UInt64, `n.x` Array(UInt64))
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_shadow_alter (a, t, j, s, `c.size0`, `n.x`) VALUES ([1, 2], tuple(3), '{"k":4}', 'x', 6, [7]);

SELECT 'ALTER that gives a stored column the name of a subcolumn is refused';
ALTER TABLE t_shadow_alter ADD COLUMN `a.size0` UInt64 DEFAULT 7; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t_shadow_alter ADD COLUMN `a.size0` UInt64 MATERIALIZED 7; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t_shadow_alter ADD COLUMN `t.x` UInt64; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t_shadow_alter ADD COLUMN `j.k` UInt64; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t_shadow_alter ADD COLUMN `j.k.l` UInt64; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t_shadow_alter ADD COLUMN `s.null` UInt8; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t_shadow_alter ADD COLUMN `n.x.size0` UInt64; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t_shadow_alter ADD COLUMN `a.d` UInt64, RENAME COLUMN `a.d` TO `a.size0`; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t_shadow_alter ADD COLUMN `a.d` UInt64;
ALTER TABLE t_shadow_alter RENAME COLUMN `a.d` TO `a.size0`; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t_shadow_alter DROP COLUMN `a.d`;

SELECT 'an ALIAS column and an unrelated dotted name keep working';
ALTER TABLE t_shadow_alter ADD COLUMN `a.size0` UInt64 ALIAS length(a) + 100;
ALTER TABLE t_shadow_alter ADD COLUMN `a.other` UInt64 DEFAULT 8;
ALTER TABLE t_shadow_alter ADD COLUMN `n.y` Array(UInt64);
ALTER TABLE t_shadow_alter MODIFY COLUMN `a.size0` UInt64 ALIAS length(a) + 200;
ALTER TABLE t_shadow_alter MODIFY COLUMN `a.size0` UInt64 DEFAULT 9; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT `a.size0`, `a.other`, `n.y`, `c.size0` FROM t_shadow_alter;

DROP TABLE t_shadow_alter;

-- The same pair of columns may appear from the other side: by adding or renaming the prefix column,
-- or by changing its type so that it gets such a subcolumn. With `share_nested_offsets`, a prefix of a
-- dotted name is taken for a `Nested` name and cannot be added or renamed to anyway.
SELECT 'ALTER that gives a stored column a prefix column with such a subcolumn is refused';
DROP TABLE IF EXISTS t_shadow_prefix;
CREATE TABLE t_shadow_prefix (s String, b JSON, `a.x` JSON, `t.y` UInt64, `c.size0` UInt64, `d.x` UInt64)
ENGINE = MergeTree ORDER BY tuple() SETTINGS share_nested_offsets = 0;
INSERT INTO t_shadow_prefix VALUES ('{"x":{"y":33}}', '{"x":1}', '{"y":11}', 1, 2, 3);
ALTER TABLE t_shadow_prefix ADD COLUMN a JSON; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t_shadow_prefix ADD COLUMN t Tuple(y UInt64); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t_shadow_prefix ADD COLUMN c Array(UInt64); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t_shadow_prefix RENAME COLUMN b TO a; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t_shadow_prefix RENAME COLUMN s TO a;
ALTER TABLE t_shadow_prefix MODIFY COLUMN a JSON; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t_shadow_prefix ADD COLUMN t Tuple(z UInt64);
ALTER TABLE t_shadow_prefix MODIFY COLUMN t Tuple(y UInt64); -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
ALTER TABLE t_shadow_prefix ADD COLUMN d UInt64 ALIAS 0;
ALTER TABLE t_shadow_prefix MODIFY COLUMN d JSON DEFAULT '{}'; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }
SELECT a, `a.x.y`, t, `t.y`, `c.size0`, `d.x` FROM t_shadow_prefix;

DROP TABLE t_shadow_prefix;

SELECT 'a pair of columns created together survives other ALTERs';
DROP TABLE IF EXISTS t_shadow_existing;
CREATE TABLE t_shadow_existing (a JSON, `a.x` UInt64, b Array(UInt64), `b.size0` UInt64)
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_shadow_existing VALUES ('{"x":1}', 2, [1, 2, 3], 4);
ALTER TABLE t_shadow_existing ADD COLUMN c UInt64 DEFAULT 5;
ALTER TABLE t_shadow_existing MODIFY COLUMN a JSON(max_dynamic_paths = 10);
ALTER TABLE t_shadow_existing MODIFY COLUMN `a.x` UInt32;
ALTER TABLE t_shadow_existing MODIFY COLUMN b Array(UInt32);
SELECT a, `a.x`, b, `b.size0`, c FROM t_shadow_existing;
ALTER TABLE t_shadow_existing RENAME COLUMN `a.x` TO `a.y`; -- { serverError ALTER_OF_COLUMN_IS_FORBIDDEN }

DROP TABLE t_shadow_existing;

-- A table created with such a column may still have parts that do not hold it: with
-- `skip_empty_columns_on_insert`, an `INSERT` that omits the column records it as missing.
SELECT 'a part that does not hold the column reads its default';
DROP TABLE IF EXISTS t_shadow_wide;
DROP TABLE IF EXISTS t_shadow_compact;
CREATE TABLE t_shadow_wide (k UInt64, a Array(UInt64), `a.size0` UInt64)
ENGINE = MergeTree ORDER BY k
SETTINGS skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns',
    min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
CREATE TABLE t_shadow_compact AS t_shadow_wide
ENGINE = MergeTree ORDER BY k
SETTINGS skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns',
    min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;

INSERT INTO t_shadow_wide (k, a) VALUES (1, [10, 20, 30]);
INSERT INTO t_shadow_wide (k, a, `a.size0`) VALUES (2, [40], 5);
INSERT INTO t_shadow_compact (k, a) VALUES (1, [10, 20, 30]);
INSERT INTO t_shadow_compact (k, a, `a.size0`) VALUES (2, [40], 5);

SELECT DISTINCT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_shadow_wide' AND active;
SELECT count() FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_shadow_wide' AND active AND column = 'a.size0';
SELECT k, a, `a.size0`, length(a) FROM t_shadow_wide ORDER BY k;
SELECT count() FROM t_shadow_wide WHERE `a.size0` = 0;
SELECT DISTINCT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_shadow_compact' AND active;
SELECT count() FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_shadow_compact' AND active AND column = 'a.size0';
SELECT k, a, `a.size0`, length(a) FROM t_shadow_compact ORDER BY k;
SELECT count() FROM t_shadow_compact WHERE `a.size0` = 0;

DROP TABLE t_shadow_wide;
DROP TABLE t_shadow_compact;

SELECT 'the same for a column named like a path of a JSON column, and its subcolumns';
DROP TABLE IF EXISTS t_shadow_json;
CREATE TABLE t_shadow_json (k UInt64, j JSON, `j.x` Tuple(y UInt64))
ENGINE = MergeTree ORDER BY k
SETTINGS skip_empty_columns_on_insert = 1, serialization_info_version = 'with_missing_columns',
    min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_shadow_json (k, j) VALUES (1, '{"x":{"y":11}}');
INSERT INTO t_shadow_json (k, j, `j.x`) VALUES (2, '{"x":{"y":22}}', tuple(8));
SELECT count() FROM system.parts_columns WHERE database = currentDatabase() AND table = 't_shadow_json' AND active AND column = 'j.x';
SELECT k, `j.x`, `j.x.y` FROM t_shadow_json ORDER BY k;

DROP TABLE t_shadow_json;

-- A projection that selects such a column stores it as a column of its own, rather than computing the
-- subcolumn of the other column in its place.
SELECT 'a projection stores the column itself';
DROP TABLE IF EXISTS t_shadow_projection;
CREATE TABLE t_shadow_projection
(
    k UInt64,
    a Array(UInt64),
    `a.size0` UInt64,
    PROJECTION p_index INDEX * TYPE basic(),
    PROJECTION p_order (SELECT k, `a.size0` ORDER BY `a.size0`)
)
ENGINE = MergeTree ORDER BY k;
INSERT INTO t_shadow_projection VALUES (1, [1, 2, 3], 5), (2, [1], 7);
SELECT k, a, `a.size0` FROM t_shadow_projection ORDER BY k;
SELECT k, `a.size0` FROM t_shadow_projection WHERE `a.size0` = 7 SETTINGS optimize_use_projections = 1;
SELECT k, `a.size0` FROM t_shadow_projection WHERE `a.size0` = 7 SETTINGS optimize_use_projections = 0;
OPTIMIZE TABLE t_shadow_projection FINAL;
SELECT k, a, `a.size0` FROM t_shadow_projection ORDER BY k;
SELECT k, `a.size0` FROM t_shadow_projection WHERE `a.size0` = 7 SETTINGS optimize_use_projections = 1;

DROP TABLE t_shadow_projection;

-- A `Nested` column is read through the same lookup: in a `Wide` part its flattened columns are
-- collected back into one column and read as its subcolumns, which must keep working.
SELECT 'a nested column and a column added after it';
DROP TABLE IF EXISTS t_nested_wide;
DROP TABLE IF EXISTS t_nested_compact;
CREATE TABLE t_nested_wide (id UInt64, n Nested(x UInt64, y String)) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0, min_level_for_wide_part = 0;
CREATE TABLE t_nested_compact (id UInt64, n Nested(x UInt64, y String)) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 1099511627776;
INSERT INTO t_nested_wide VALUES (1, [1, 2], ['a', 'b']), (2, [3], ['c']);
INSERT INTO t_nested_compact VALUES (1, [1, 2], ['a', 'b']), (2, [3], ['c']);
ALTER TABLE t_nested_wide ADD COLUMN z UInt64 DEFAULT 5, ADD COLUMN `n.z` Array(UInt64);
ALTER TABLE t_nested_compact ADD COLUMN z UInt64 DEFAULT 5, ADD COLUMN `n.z` Array(UInt64);
INSERT INTO t_nested_wide VALUES (3, [4], ['d'], 9, [6]);
INSERT INTO t_nested_compact VALUES (3, [4], ['d'], 9, [6]);
SELECT DISTINCT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_nested_wide' AND active;
SELECT id, n.x, n.y, n.z, z, length(n.x) FROM t_nested_wide ORDER BY id;
SELECT DISTINCT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_nested_compact' AND active;
SELECT id, n.x, n.y, n.z, z, length(n.x) FROM t_nested_compact ORDER BY id;

DROP TABLE t_nested_wide;
DROP TABLE t_nested_compact;
