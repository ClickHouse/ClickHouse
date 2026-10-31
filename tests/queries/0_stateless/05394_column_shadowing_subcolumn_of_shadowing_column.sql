-- A column `a.x` added next to a `JSON` column `a` shadows the path `x` of `a`, and so do its subcolumns:
-- `a.x.y` is the subcolumn `y` of `a.x`, never the path `x.y` of `a`, in old parts and in projections.

SET enable_analyzer = 1;

DROP TABLE IF EXISTS t_shadow_wide;
DROP TABLE IF EXISTS t_shadow_compact;
DROP TABLE IF EXISTS t_shadow_nullable;
DROP TABLE IF EXISTS t_shadow_projection;

CREATE TABLE t_shadow_wide (id UInt64, a JSON) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_shadow_wide VALUES (1, '{"x":{"y":11}}');
ALTER TABLE t_shadow_wide ADD COLUMN `a.x` Tuple(y UInt64) DEFAULT tuple(toUInt64(7));
SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_shadow_wide' AND active;
SELECT id, `a.x`, `a.x.y` FROM t_shadow_wide ORDER BY id;
INSERT INTO t_shadow_wide (id, a, `a.x`) VALUES (2, '{"x":{"y":22}}', tuple(8));
SELECT id, `a.x`, `a.x.y` FROM t_shadow_wide ORDER BY id;
OPTIMIZE TABLE t_shadow_wide FINAL;
SELECT id, `a.x`, `a.x.y` FROM t_shadow_wide ORDER BY id;

CREATE TABLE t_shadow_compact (id UInt64, a JSON) ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 1000000000, min_rows_for_wide_part = 1000000000;
INSERT INTO t_shadow_compact VALUES (1, '{"x":{"y":11}}');
ALTER TABLE t_shadow_compact ADD COLUMN `a.x` Tuple(y UInt64) DEFAULT tuple(toUInt64(7));
SELECT part_type FROM system.parts WHERE database = currentDatabase() AND table = 't_shadow_compact' AND active;
SELECT id, `a.x`, `a.x.y` FROM t_shadow_compact ORDER BY id;

CREATE TABLE t_shadow_nullable (id UInt64, a JSON) ENGINE = MergeTree ORDER BY id;
INSERT INTO t_shadow_nullable VALUES (1, '{"x":{"null":0}}');
ALTER TABLE t_shadow_nullable ADD COLUMN `a.x` Nullable(UInt64) DEFAULT NULL;
SELECT id, `a.x`, `a.x.null` FROM t_shadow_nullable ORDER BY id;

CREATE TABLE t_shadow_projection (id UInt64, a JSON, PROJECTION p (SELECT id, a ORDER BY id))
ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_shadow_projection VALUES (1, '{"x":{"y":11}}');
ALTER TABLE t_shadow_projection ADD COLUMN `a.x` Tuple(y UInt64) DEFAULT tuple(toUInt64(7));
INSERT INTO t_shadow_projection (id, a, `a.x`) VALUES (2, '{"x":{"y":22}}', tuple(8));
OPTIMIZE TABLE t_shadow_projection FINAL;
SELECT id, `a.x.y` FROM t_shadow_projection WHERE id IN (1, 2) ORDER BY id SETTINGS optimize_use_projections = 0;
SELECT id, `a.x.y` FROM t_shadow_projection WHERE id IN (1, 2) ORDER BY id SETTINGS optimize_use_projections = 1;
SELECT id, a.x.y FROM t_shadow_projection WHERE id IN (1, 2) ORDER BY id SETTINGS optimize_use_projections = 1;

DROP TABLE t_shadow_wide;
DROP TABLE t_shadow_compact;
DROP TABLE t_shadow_nullable;
DROP TABLE t_shadow_projection;
