-- Tags: no-random-merge-tree-settings, no-parallel-replicas
-- A column whose TTL expired is dropped from the part by column ID: the id-keyed expired set is
-- matched against the part's own columns, so removal works on both merge paths and survives a
-- metadata-only RENAME. Wide parts only -- a Compact part keeps every expired column's file.

SET allow_experimental_column_ids = 1;

-- why: horizontal merge. Non-identity IDs (DROP + ADD) so the part's streams are named by the ID;
-- deriving them from the logical name instead would look for files that are not on disk.
CREATE TABLE t_ids_ttl_horizontal (d Date, k UInt64, v String TTL d + INTERVAL 1 MONTH)
ENGINE = MergeTree ORDER BY k
SETTINGS serialization_info_version = 'with_column_ids',
         min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
ALTER TABLE t_ids_ttl_horizontal DROP COLUMN v;
ALTER TABLE t_ids_ttl_horizontal ADD COLUMN v String TTL d + INTERVAL 1 MONTH;
-- The row is already expired, so a background merge would drop v before the next assertion reads it.
SYSTEM STOP MERGES t_ids_ttl_horizontal;
INSERT INTO t_ids_ttl_horizontal VALUES ('2010-01-01', 1, 'foo');
SELECT 'horizontal_id_non_identity', column_id != column
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_ids_ttl_horizontal' AND active AND column = 'v';
SYSTEM START MERGES t_ids_ttl_horizontal;
OPTIMIZE TABLE t_ids_ttl_horizontal FINAL;
SELECT 'horizontal_dropped_from_part', count()
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_ids_ttl_horizontal' AND active AND column = 'v';
SELECT 'horizontal_other_columns_kept', count()
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_ids_ttl_horizontal' AND active;
SELECT 'horizontal_reads_default', k, v = '' FROM t_ids_ttl_horizontal;
DROP TABLE t_ids_ttl_horizontal SYNC;

-- why: vertical merge takes the other removal path (MergedColumnOnlyOutputStream), which only
-- removes the columns its own writer produced -- the gathered set, where the expired column sits.
CREATE TABLE t_ids_ttl_vertical (d Date, k UInt64, v String TTL d + INTERVAL 1 MONTH)
ENGINE = MergeTree ORDER BY k
SETTINGS serialization_info_version = 'with_column_ids',
         min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0,
         vertical_merge_algorithm_min_rows_to_activate = 1,
         vertical_merge_algorithm_min_columns_to_activate = 1;
ALTER TABLE t_ids_ttl_vertical DROP COLUMN v;
ALTER TABLE t_ids_ttl_vertical ADD COLUMN v String TTL d + INTERVAL 1 MONTH;
INSERT INTO t_ids_ttl_vertical VALUES ('2010-01-01', 1, 'foo');
INSERT INTO t_ids_ttl_vertical VALUES ('2010-01-02', 2, 'bar');
OPTIMIZE TABLE t_ids_ttl_vertical FINAL;
SELECT 'vertical_dropped_from_part', count()
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_ids_ttl_vertical' AND active AND column = 'v';
SELECT 'vertical_reads_default', k, v = '' FROM t_ids_ttl_vertical ORDER BY k;
DROP TABLE t_ids_ttl_vertical SYNC;

-- why: the expired set is keyed by an ID the RENAME does not change, so the removal still finds
-- the column after the logical name it was declared under is gone.
CREATE TABLE t_ids_ttl_rename (d Date, k UInt64, v String TTL d + INTERVAL 1 MONTH)
ENGINE = MergeTree ORDER BY k
SETTINGS serialization_info_version = 'with_column_ids',
         min_bytes_for_wide_part = 0, min_rows_for_wide_part = 0;
INSERT INTO t_ids_ttl_rename VALUES ('2010-01-01', 1, 'foo');
ALTER TABLE t_ids_ttl_rename RENAME COLUMN v TO w;
OPTIMIZE TABLE t_ids_ttl_rename FINAL;
SELECT 'rename_dropped_from_part', count()
FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_ids_ttl_rename' AND active AND column = 'w';
SELECT 'rename_reads_default', k, w = '' FROM t_ids_ttl_rename;
DROP TABLE t_ids_ttl_rename SYNC;
