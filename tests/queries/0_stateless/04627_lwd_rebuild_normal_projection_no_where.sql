-- A lightweight DELETE in rebuild mode must rebuild a normal (non-aggregate) projection
-- that has no WHERE clause. Such a projection is calculated at FetchColumns stage, which
-- skips the injected `_row_exists = 1` filter, so before the fix the rebuilt projection
-- kept every deleted row and a projection read-in-order returned stale (deleted) rows.
-- See issue #111791. Both Compact (MutateAllPartColumns) and Wide (MutateSomePartColumns)
-- parts are covered. The projection sort key (g, id) matches the query ORDER BY so the
-- optimizer can serve the ORDER BY ... LIMIT read from the projection; force_optimize_projection
-- makes that read mandatory so the assertions actually exercise the projection.
SET mutations_sync = 2, lightweight_deletes_sync = 2;

-- Compact part
DROP TABLE IF EXISTS t_lwd_norm_proj_compact;
CREATE TABLE t_lwd_norm_proj_compact (id UInt64, g UInt8, v Int64,
    PROJECTION p (SELECT id, g, v ORDER BY (g, id)))
ENGINE = MergeTree ORDER BY id
SETTINGS lightweight_mutation_projection_mode = 'rebuild', index_granularity = 4,
         min_bytes_for_wide_part = 10485760, min_bytes_for_full_part_storage = 0;

INSERT INTO t_lwd_norm_proj_compact SELECT number, number % 7, number * 10 FROM numbers(100);
DELETE FROM t_lwd_norm_proj_compact WHERE id % 11 = 0;

SELECT 'compact storage', part_type, part_storage_type FROM system.parts
WHERE database = currentDatabase() AND table = 't_lwd_norm_proj_compact' AND active;

-- 100 inserted, 10 deleted (id in {0,11,...,99}) => 90 rows survive in both base and projection.
SELECT 'compact base rows', count() FROM t_lwd_norm_proj_compact;
SELECT 'compact projection rows', sum(rows) FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_lwd_norm_proj_compact' AND name = 'p' AND active;

-- A read forced through the projection must not return deleted rows, and must match the same
-- read without the projection.
SELECT 'compact deleted leaked via projection', countIf(id % 11 = 0)
FROM (SELECT id FROM t_lwd_norm_proj_compact WHERE g BETWEEN 2 AND 5 ORDER BY g, id LIMIT 20)
SETTINGS optimize_use_projections = 1, optimize_read_in_order = 1, force_optimize_projection = 1;

SELECT 'compact projection read', groupArray(id) FROM (SELECT id FROM t_lwd_norm_proj_compact WHERE g BETWEEN 2 AND 5 ORDER BY g, id LIMIT 20)
SETTINGS optimize_use_projections = 1, optimize_read_in_order = 1, force_optimize_projection = 1;
SELECT 'compact base read', groupArray(id) FROM (SELECT id FROM t_lwd_norm_proj_compact WHERE g BETWEEN 2 AND 5 ORDER BY g, id LIMIT 20)
SETTINGS optimize_use_projections = 0;

-- A second lightweight DELETE: the rebuild must keep the rows removed by the first one, which
-- `_row_exists` now carries too.
DELETE FROM t_lwd_norm_proj_compact WHERE id % 13 = 0;

SELECT 'compact second delete base rows', count() FROM t_lwd_norm_proj_compact SETTINGS optimize_use_projections = 0;
SELECT 'compact second delete projection rows', sum(rows) FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_lwd_norm_proj_compact' AND name = 'p' AND active;

SELECT 'compact second delete deleted leaked via projection', countIf(id % 11 = 0 OR id % 13 = 0)
FROM (SELECT id FROM t_lwd_norm_proj_compact WHERE g BETWEEN 2 AND 5 ORDER BY g, id LIMIT 20)
SETTINGS optimize_use_projections = 1, optimize_read_in_order = 1, force_optimize_projection = 1;

SELECT 'compact second delete projection read', groupArray(id) FROM (SELECT id FROM t_lwd_norm_proj_compact WHERE g BETWEEN 2 AND 5 ORDER BY g, id LIMIT 20)
SETTINGS optimize_use_projections = 1, optimize_read_in_order = 1, force_optimize_projection = 1;
SELECT 'compact second delete base read', groupArray(id) FROM (SELECT id FROM t_lwd_norm_proj_compact WHERE g BETWEEN 2 AND 5 ORDER BY g, id LIMIT 20)
SETTINGS optimize_use_projections = 0;

DROP TABLE t_lwd_norm_proj_compact;

-- Wide part (MutateSomePartColumns path). Pin min_bytes_for_full_part_storage = 0 so the runner
-- cannot randomize this small part into Packed storage, which would reroute to MutateAllPartColumns.
DROP TABLE IF EXISTS t_lwd_norm_proj_wide;
CREATE TABLE t_lwd_norm_proj_wide (id UInt64, g UInt8, v Int64,
    PROJECTION p (SELECT id, g, v ORDER BY (g, id)))
ENGINE = MergeTree ORDER BY id
SETTINGS lightweight_mutation_projection_mode = 'rebuild', index_granularity = 4,
         min_bytes_for_wide_part = 0, min_bytes_for_full_part_storage = 0;

INSERT INTO t_lwd_norm_proj_wide SELECT number, number % 7, number * 10 FROM numbers(100);
DELETE FROM t_lwd_norm_proj_wide WHERE id % 11 = 0;

SELECT 'wide storage', part_type, part_storage_type FROM system.parts
WHERE database = currentDatabase() AND table = 't_lwd_norm_proj_wide' AND active;

SELECT 'wide base rows', count() FROM t_lwd_norm_proj_wide;
SELECT 'wide projection rows', sum(rows) FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_lwd_norm_proj_wide' AND name = 'p' AND active;

SELECT 'wide deleted leaked via projection', countIf(id % 11 = 0)
FROM (SELECT id FROM t_lwd_norm_proj_wide WHERE g BETWEEN 2 AND 5 ORDER BY g, id LIMIT 20)
SETTINGS optimize_use_projections = 1, optimize_read_in_order = 1, force_optimize_projection = 1;

SELECT 'wide projection read', groupArray(id) FROM (SELECT id FROM t_lwd_norm_proj_wide WHERE g BETWEEN 2 AND 5 ORDER BY g, id LIMIT 20)
SETTINGS optimize_use_projections = 1, optimize_read_in_order = 1, force_optimize_projection = 1;
SELECT 'wide base read', groupArray(id) FROM (SELECT id FROM t_lwd_norm_proj_wide WHERE g BETWEEN 2 AND 5 ORDER BY g, id LIMIT 20)
SETTINGS optimize_use_projections = 0;

-- apply_deleted_mask = 0 reads the deleted rows, which the rebuilt projection does not store, so the
-- projection is not used.
SELECT id FROM t_lwd_norm_proj_wide WHERE g BETWEEN 2 AND 5 ORDER BY g, id LIMIT 20
SETTINGS apply_deleted_mask = 0, optimize_use_projections = 1, force_optimize_projection = 1; -- { serverError PROJECTION_NOT_USED }

SELECT 'wide apply_deleted_mask = 0', count(), countIf(id % 11 = 0)
FROM t_lwd_norm_proj_wide WHERE g BETWEEN 2 AND 5 SETTINGS apply_deleted_mask = 0;

-- A second lightweight DELETE: the rebuild must keep the rows removed by the first one, which
-- `_row_exists` now carries too.
DELETE FROM t_lwd_norm_proj_wide WHERE id % 13 = 0;

SELECT 'wide second delete base rows', count() FROM t_lwd_norm_proj_wide SETTINGS optimize_use_projections = 0;
SELECT 'wide second delete projection rows', sum(rows) FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_lwd_norm_proj_wide' AND name = 'p' AND active;

SELECT 'wide second delete deleted leaked via projection', countIf(id % 11 = 0 OR id % 13 = 0)
FROM (SELECT id FROM t_lwd_norm_proj_wide WHERE g BETWEEN 2 AND 5 ORDER BY g, id LIMIT 20)
SETTINGS optimize_use_projections = 1, optimize_read_in_order = 1, force_optimize_projection = 1;

SELECT 'wide second delete projection read', groupArray(id) FROM (SELECT id FROM t_lwd_norm_proj_wide WHERE g BETWEEN 2 AND 5 ORDER BY g, id LIMIT 20)
SETTINGS optimize_use_projections = 1, optimize_read_in_order = 1, force_optimize_projection = 1;
SELECT 'wide second delete base read', groupArray(id) FROM (SELECT id FROM t_lwd_norm_proj_wide WHERE g BETWEEN 2 AND 5 ORDER BY g, id LIMIT 20)
SETTINGS optimize_use_projections = 0;

DROP TABLE t_lwd_norm_proj_wide;

DROP TABLE IF EXISTS t_lwd_agg_proj;
CREATE TABLE t_lwd_agg_proj (id UInt64, g UInt8, PROJECTION p (SELECT g, count() GROUP BY g))
ENGINE = MergeTree ORDER BY id
SETTINGS lightweight_mutation_projection_mode = 'rebuild', index_granularity = 4;

INSERT INTO t_lwd_agg_proj SELECT number, number % 3 FROM numbers(12);
DELETE FROM t_lwd_agg_proj WHERE id % 2 = 0;

SELECT g, count() FROM t_lwd_agg_proj GROUP BY g ORDER BY g
SETTINGS apply_deleted_mask = 0, optimize_use_projections = 1, force_optimize_projection = 1; -- { serverError PROJECTION_NOT_USED }

SELECT 'aggregate apply_deleted_mask = 0', g, count() FROM t_lwd_agg_proj GROUP BY g ORDER BY g SETTINGS apply_deleted_mask = 0;
SELECT 'aggregate projection', g, count() FROM t_lwd_agg_proj GROUP BY g ORDER BY g SETTINGS optimize_use_projections = 1, force_optimize_projection = 1;

DROP TABLE t_lwd_agg_proj;

-- Projection index: stores the parent `_part_offset` ordered by the indexed column.
DROP TABLE IF EXISTS t_lwd_proj_index;
CREATE TABLE t_lwd_proj_index (a UInt32, b UInt32, PROJECTION p INDEX b TYPE basic)
ENGINE = MergeTree ORDER BY a
SETTINGS lightweight_mutation_projection_mode = 'rebuild', index_granularity = 4, min_bytes_for_full_part_storage = 0;

INSERT INTO t_lwd_proj_index SELECT number, number % 13 FROM numbers(100);
DELETE FROM t_lwd_proj_index WHERE a % 11 = 0;

-- `a` equals the base `_part_offset` here, so a deleted row's offset is a multiple of 11.
SELECT 'index projection rows', count(), countIf(_parent_part_offset % 11 = 0)
FROM mergeTreeProjection(currentDatabase(), t_lwd_proj_index, p);
SELECT 'index parent-offset matches', sum(l.b = r.b), count()
FROM t_lwd_proj_index l JOIN mergeTreeProjection(currentDatabase(), t_lwd_proj_index, p) r ON l._part_offset = r._parent_part_offset
SETTINGS enable_analyzer = 1, optimize_use_projections = 0;

DROP TABLE t_lwd_proj_index;

-- Projection that references no column: the source block must still carry the surviving row count.
DROP TABLE IF EXISTS t_lwd_count_proj;
CREATE TABLE t_lwd_count_proj (x UInt64, PROJECTION p (SELECT count() GROUP BY 0))
ENGINE = MergeTree ORDER BY x
SETTINGS lightweight_mutation_projection_mode = 'rebuild', index_granularity = 4;

INSERT INTO t_lwd_count_proj SELECT number FROM numbers(10);
DELETE FROM t_lwd_count_proj WHERE x < 3;

SELECT 'count projection', count() FROM t_lwd_count_proj
SETTINGS optimize_use_projections = 1, force_optimize_projection = 1, optimize_trivial_count_query = 0;

DROP TABLE t_lwd_count_proj;
