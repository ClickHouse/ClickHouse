-- A mutation that rewrites only some columns of a Wide part keeps the lightweight-deleted rows in it,
-- and a projection it rebuilds must still leave them out.

SET mutations_sync = 2, lightweight_deletes_sync = 2;

DROP TABLE IF EXISTS t_upd_normal;
DROP TABLE IF EXISTS t_upd_aggregate;
DROP TABLE IF EXISTS t_upd_offset;
DROP TABLE IF EXISTS t_upd_compact;
DROP TABLE IF EXISTS t_materialize;
DROP TABLE IF EXISTS t_clear;
DROP TABLE IF EXISTS t_unrelated;

-- UPDATE of a column a normal projection stores
CREATE TABLE t_upd_normal (id UInt64, g UInt64, w UInt64, PROJECTION p (SELECT id, g ORDER BY g))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, lightweight_mutation_projection_mode = 'rebuild';
INSERT INTO t_upd_normal SELECT number, number % 7, number FROM numbers(10);
DELETE FROM t_upd_normal WHERE id < 3;
ALTER TABLE t_upd_normal UPDATE g = g + 100 WHERE 1;

SELECT 'update normal', part_type, sum(rows) FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_upd_normal' AND name = 'p' AND active GROUP BY part_type;
SELECT 'update normal projection', groupArray(id) FROM (SELECT id FROM t_upd_normal WHERE g >= 100 ORDER BY g, id) SETTINGS optimize_use_projections = 1, optimize_read_in_order = 1, force_optimize_projection_name = 'p';
SELECT 'update normal base', groupArray(id) FROM (SELECT id FROM t_upd_normal WHERE g >= 100 ORDER BY g, id) SETTINGS optimize_use_projections = 0;

-- the same through an aggregate projection
CREATE TABLE t_upd_aggregate (id UInt64, g UInt64, w UInt64, PROJECTION p (SELECT g, count() GROUP BY g))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, lightweight_mutation_projection_mode = 'rebuild';
INSERT INTO t_upd_aggregate SELECT number, number % 7, number FROM numbers(10);
DELETE FROM t_upd_aggregate WHERE id < 3;
ALTER TABLE t_upd_aggregate UPDATE g = g + 100 WHERE 1;

SELECT 'update aggregate projection', groupArray((g, c)) FROM (SELECT g, count() AS c FROM t_upd_aggregate GROUP BY g ORDER BY g) SETTINGS optimize_use_projections = 1, optimize_read_in_order = 1, force_optimize_projection_name = 'p';
SELECT 'update aggregate base', groupArray((g, c)) FROM (SELECT g, count() AS c FROM t_upd_aggregate GROUP BY g ORDER BY g) SETTINGS optimize_use_projections = 0;

-- a projection with `_part_offset` keeps the parent offset of each surviving row
CREATE TABLE t_upd_offset (id UInt64, g UInt64, w UInt64, PROJECTION p (SELECT g, _part_offset ORDER BY g))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, lightweight_mutation_projection_mode = 'rebuild';
INSERT INTO t_upd_offset SELECT number, number % 7, number FROM numbers(10);
DELETE FROM t_upd_offset WHERE id IN (2, 5, 8);
ALTER TABLE t_upd_offset UPDATE g = 10 - id WHERE 1;

SELECT 'update offset projection', groupArray(_parent_part_offset), groupArray(g)
FROM (SELECT _parent_part_offset, g FROM mergeTreeProjection(currentDatabase(), t_upd_offset, p) ORDER BY g);
SELECT 'update offset base', groupArray(_part_offset), groupArray(g)
FROM (SELECT _part_offset, g FROM t_upd_offset ORDER BY g) SETTINGS optimize_use_projections = 0;

-- a Compact part is rewritten as a whole
CREATE TABLE t_upd_compact (id UInt64, g UInt64, w UInt64, PROJECTION p (SELECT id, g ORDER BY g))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = '1G', min_bytes_for_full_part_storage = 0, lightweight_mutation_projection_mode = 'rebuild';
INSERT INTO t_upd_compact SELECT number, number % 7, number FROM numbers(10);
DELETE FROM t_upd_compact WHERE id < 3;
ALTER TABLE t_upd_compact UPDATE g = g + 100 WHERE 1;

SELECT 'update compact', part_type, sum(rows) FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_upd_compact' AND name = 'p' AND active GROUP BY part_type;
SELECT 'update compact projection', groupArray(id) FROM (SELECT id FROM t_upd_compact WHERE g >= 100 ORDER BY g, id) SETTINGS optimize_use_projections = 1, optimize_read_in_order = 1, force_optimize_projection_name = 'p';

-- MATERIALIZE PROJECTION of a projection added after the delete
CREATE TABLE t_materialize (id UInt64, g UInt64, w UInt64)
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, lightweight_mutation_projection_mode = 'rebuild';
INSERT INTO t_materialize SELECT number, number % 7, number FROM numbers(10);
DELETE FROM t_materialize WHERE id < 3;
ALTER TABLE t_materialize ADD PROJECTION p (SELECT id, g ORDER BY g);
ALTER TABLE t_materialize MATERIALIZE PROJECTION p;

SELECT 'materialize', sum(rows) FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_materialize' AND name = 'p' AND active;
SELECT 'materialize projection', groupArray(id) FROM (SELECT id FROM t_materialize WHERE g < 100 ORDER BY g, id) SETTINGS optimize_use_projections = 1, optimize_read_in_order = 1, force_optimize_projection_name = 'p';

-- CLEAR COLUMN of a column the projection stores
CREATE TABLE t_clear (id UInt64, g UInt64, w UInt64, PROJECTION p (SELECT id, g ORDER BY g))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, lightweight_mutation_projection_mode = 'rebuild';
INSERT INTO t_clear SELECT number, number % 7 + 1, number FROM numbers(10);
DELETE FROM t_clear WHERE id < 3;
ALTER TABLE t_clear CLEAR COLUMN g;

SELECT 'clear', sum(rows) FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_clear' AND name = 'p' AND active;
SELECT 'clear projection', groupArray((id, g)) FROM (SELECT id, g FROM t_clear WHERE g < 100 ORDER BY g, id) SETTINGS optimize_use_projections = 1, optimize_read_in_order = 1, force_optimize_projection_name = 'p';

-- an UPDATE of a column the projection does not use keeps the projection as it is
CREATE TABLE t_unrelated (id UInt64, g UInt64, w UInt64, PROJECTION p (SELECT id, g ORDER BY g))
ENGINE = MergeTree ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0, lightweight_mutation_projection_mode = 'rebuild';
INSERT INTO t_unrelated SELECT number, number % 7, number FROM numbers(10);
DELETE FROM t_unrelated WHERE id < 3;
ALTER TABLE t_unrelated UPDATE w = w + 1 WHERE 1;

SELECT 'unrelated', sum(rows) FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_unrelated' AND name = 'p' AND active;
SELECT 'unrelated base', count(), sum(w) FROM t_unrelated SETTINGS optimize_use_projections = 0;

DROP TABLE t_upd_normal;
DROP TABLE t_upd_aggregate;
DROP TABLE t_upd_offset;
DROP TABLE t_upd_compact;
DROP TABLE t_materialize;
DROP TABLE t_clear;
DROP TABLE t_unrelated;
