SET lightweight_deletes_sync = 2;
SET mutations_sync = 2;
SET alter_sync = 2;

-- 'rebuild' mode: the projection is rebuilt but yields zero rows after the
-- lightweight delete wipes every visible source row, so no projection part is
-- produced. The inherited `<name>.proj` checksum entry must be scrubbed.
CREATE TABLE t_rebuild
(
    x UInt64,
    y UInt64,
    PROJECTION p (SELECT y, count() GROUP BY y)
)
ENGINE = MergeTree
ORDER BY x
SETTINGS min_bytes_for_wide_part = 0,
         lightweight_mutation_projection_mode = 'rebuild';

INSERT INTO t_rebuild SELECT number, toUInt64(200) FROM numbers(20);

DELETE FROM t_rebuild WHERE y > 100;

SELECT 'rebuild_after_delete_broken';
SELECT count() FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_rebuild' AND active AND is_broken;

DETACH TABLE t_rebuild;
ATTACH TABLE t_rebuild;

SELECT 'rebuild_after_reload_broken';
SELECT count() FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_rebuild' AND active AND is_broken;

SELECT 'rebuild_visible_rows';
SELECT count() FROM t_rebuild;

SELECT 'rebuild_check_table';
CHECK TABLE t_rebuild SETTINGS check_query_single_value_result = 1;

-- 'drop' mode: the projection is intentionally not carried over by the
-- lightweight delete (`projections_to_recalc` stays empty), but its directory is
-- still skipped during hardlinking. The inherited `<name>.proj` checksum entry
-- must be scrubbed on the same path.
CREATE TABLE t_drop
(
    x UInt64,
    y UInt64,
    PROJECTION p (SELECT y, count() GROUP BY y)
)
ENGINE = MergeTree
ORDER BY x
SETTINGS min_bytes_for_wide_part = 0,
         lightweight_mutation_projection_mode = 'drop';

INSERT INTO t_drop SELECT number, toUInt64(200) FROM numbers(20);

DELETE FROM t_drop WHERE y > 100;

SELECT 'drop_after_delete_broken';
SELECT count() FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_drop' AND active AND is_broken;

DETACH TABLE t_drop;
ATTACH TABLE t_drop;

SELECT 'drop_after_reload_broken';
SELECT count() FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_drop' AND active AND is_broken;

SELECT 'drop_visible_rows';
SELECT count() FROM t_drop;

SELECT 'drop_check_table';
CHECK TABLE t_drop SETTINGS check_query_single_value_result = 1;

-- 'rebuild' mode with a partial delete: the projection is on the rebuild list
-- (its directory is in `files_to_skip`) but the rebuild produces a non-empty
-- part, so it IS in `getProjectionParts()`. The scrub must NOT erase it — this
-- guards the second `if` clause against over-erasing a surviving projection.
CREATE TABLE t_survive
(
    x UInt64,
    y UInt64,
    PROJECTION p (SELECT y, count() GROUP BY y)
)
ENGINE = MergeTree
ORDER BY x
SETTINGS min_bytes_for_wide_part = 0,
         lightweight_mutation_projection_mode = 'rebuild';

INSERT INTO t_survive SELECT number, if(number % 2 = 0, 100, 200) FROM numbers(20);

DELETE FROM t_survive WHERE y = 100;

DETACH TABLE t_survive;
ATTACH TABLE t_survive;

SELECT 'survive_after_reload_broken';
SELECT count() FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_survive' AND active AND is_broken;

SELECT 'survive_active_projection_parts';
SELECT count() FROM system.projection_parts
WHERE database = currentDatabase() AND table = 't_survive' AND active AND NOT is_broken;

SELECT 'survive_visible_rows';
SELECT count() FROM t_survive;

-- The load-bearing assertion for the second `if` clause: over-erasing the
-- surviving projection leaves its `p.proj` directory on disk with no checksum
-- entry, which `is_broken` does NOT catch but `CHECK TABLE` reports as
-- `UNEXPECTED_FILE_IN_DATA_PART`.
SELECT 'survive_check_table';
CHECK TABLE t_survive SETTINGS check_query_single_value_result = 1;
