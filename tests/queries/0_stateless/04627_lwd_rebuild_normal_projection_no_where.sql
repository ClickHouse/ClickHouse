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
FROM (SELECT id FROM t_lwd_norm_proj_compact WHERE g BETWEEN 2 AND 5 ORDER BY g, id LIMIT 20
      SETTINGS optimize_use_projections = 1, force_optimize_projection = 1);

SELECT 'compact projection == no-projection',
    (SELECT groupArray(id) FROM (SELECT id FROM t_lwd_norm_proj_compact WHERE g BETWEEN 2 AND 5 ORDER BY g, id LIMIT 20 SETTINGS optimize_use_projections = 1, force_optimize_projection = 1))
  = (SELECT groupArray(id) FROM (SELECT id FROM t_lwd_norm_proj_compact WHERE g BETWEEN 2 AND 5 ORDER BY g, id LIMIT 20 SETTINGS optimize_use_projections = 0));

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
FROM (SELECT id FROM t_lwd_norm_proj_wide WHERE g BETWEEN 2 AND 5 ORDER BY g, id LIMIT 20
      SETTINGS optimize_use_projections = 1, force_optimize_projection = 1);

SELECT 'wide projection == no-projection',
    (SELECT groupArray(id) FROM (SELECT id FROM t_lwd_norm_proj_wide WHERE g BETWEEN 2 AND 5 ORDER BY g, id LIMIT 20 SETTINGS optimize_use_projections = 1, force_optimize_projection = 1))
  = (SELECT groupArray(id) FROM (SELECT id FROM t_lwd_norm_proj_wide WHERE g BETWEEN 2 AND 5 ORDER BY g, id LIMIT 20 SETTINGS optimize_use_projections = 0));

DROP TABLE t_lwd_norm_proj_wide;

-- Projection storing the parent `_part_offset` (with_parent_part_offset path). The deletion mask
-- must filter the generated offset column too, so surviving rows keep their original parent offsets.
DROP TABLE IF EXISTS t_lwd_norm_proj_offset;
CREATE TABLE t_lwd_norm_proj_offset (a Int32, b Int32,
    PROJECTION p (SELECT a, b, _part_offset ORDER BY b))
ENGINE = MergeTree ORDER BY a
SETTINGS lightweight_mutation_projection_mode = 'rebuild', index_granularity = 4, min_bytes_for_full_part_storage = 0;

INSERT INTO t_lwd_norm_proj_offset SELECT number, number % 13 FROM numbers(100);
DELETE FROM t_lwd_norm_proj_offset WHERE a % 11 = 0;

SELECT 'offset base rows', count() FROM t_lwd_norm_proj_offset;
SELECT 'offset projection rows', sum(rows) FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_lwd_norm_proj_offset' AND name = 'p' AND active;

-- No deleted row survives in the projection.
SELECT 'offset deleted in projection', count()
FROM mergeTreeProjection(currentDatabase(), t_lwd_norm_proj_offset, p) WHERE a % 11 = 0 SETTINGS enable_analyzer = 1;

-- Every surviving projection row's _parent_part_offset equals the base row's _part_offset.
SELECT 'offset parent-offset matches', sum(l._part_offset = r._parent_part_offset), count()
FROM t_lwd_norm_proj_offset l
JOIN mergeTreeProjection(currentDatabase(), t_lwd_norm_proj_offset, p) r USING (a)
SETTINGS enable_analyzer = 1;

DROP TABLE t_lwd_norm_proj_offset;
