-- `OPTIMIZE ... FINAL CLEANUP` merged vertically returns exactly the rows of a horizontal merge, gathered columns
-- included. Keys interleave across three parts, so a gathered row taken from the wrong part changes the result.
-- `max_bytes_to_merge_at_max_space_in_pool = 1` keeps background merges away.

SET alter_sync = 2;
SET optimize_throw_if_noop = 0;
SET optimize_on_insert = 0;

-- Every key has three versions in each part. The newest version is a tombstone for every fourth key and for
-- the last ten keys, so tombstone winners sit in the middle and at the end of the merge. For other keys a tombstone
-- loses to a newer live version.
DROP TABLE IF EXISTS t_cleanup_v;
DROP TABLE IF EXISTS t_cleanup_h;
DROP VIEW IF EXISTS t_cleanup_gen;

CREATE TABLE t_cleanup_v
(
    id UInt64,
    ver UInt64,
    is_deleted UInt8,
    c1 String,
    c2 UInt64
)
ENGINE = ReplacingMergeTree(ver, is_deleted)
ORDER BY id
SETTINGS
    min_bytes_for_wide_part = 0,
    min_bytes_for_full_part_storage = 0,
    enable_block_number_column = 0,
    enable_block_offset_column = 0,
    index_granularity = 1000,
    merge_max_block_size = 1000,
    enable_vertical_merge_algorithm = 1,
    vertical_merge_algorithm_min_rows_to_activate = 1,
    vertical_merge_algorithm_min_columns_to_activate = 1,
    vertical_merge_algorithm_min_bytes_to_activate = 0,
    allow_experimental_replacing_merge_with_cleanup = 1,
    max_bytes_to_merge_at_max_space_in_pool = 1,
    ratio_of_defaults_for_sparse_serialization = 1.0;

-- The control: the same parts merged horizontally.
CREATE TABLE t_cleanup_h AS t_cleanup_v;
ALTER TABLE t_cleanup_h MODIFY SETTING enable_vertical_merge_algorithm = 0;

CREATE VIEW t_cleanup_gen AS
SELECT
    k,
    number % 1000 AS id,
    k * 10 + intDiv(number, 1000) AS ver,
    toUInt8((ver = 32 AND (id % 4 = 0 OR id >= 990)) OR (ver = 31 AND id % 4 = 1) OR (ver < 30 AND cityHash64(id, ver) % 3 = 0)) AS is_deleted,
    concat('p', toString(k), '_', toString(number)) AS c1,
    number * 10 + k AS c2
FROM numbers(3000) ARRAY JOIN [1, 2, 3] AS k;

INSERT INTO t_cleanup_v SELECT * EXCEPT k FROM t_cleanup_gen WHERE k = 1;
INSERT INTO t_cleanup_v SELECT * EXCEPT k FROM t_cleanup_gen WHERE k = 2;
INSERT INTO t_cleanup_v SELECT * EXCEPT k FROM t_cleanup_gen WHERE k = 3;
INSERT INTO t_cleanup_h SELECT * EXCEPT k FROM t_cleanup_gen WHERE k = 1;
INSERT INTO t_cleanup_h SELECT * EXCEPT k FROM t_cleanup_gen WHERE k = 2;
INSERT INTO t_cleanup_h SELECT * EXCEPT k FROM t_cleanup_gen WHERE k = 3;

OPTIMIZE TABLE t_cleanup_v FINAL CLEANUP;
OPTIMIZE TABLE t_cleanup_h FINAL CLEANUP;

SELECT 'expected_rows', arraySort(groupArray((id, ver))) = (SELECT arraySort(groupArray((id, winner.1))) FROM
    (SELECT id, argMax((ver, is_deleted), ver) AS winner FROM t_cleanup_gen GROUP BY id HAVING winner.2 = 0))
FROM t_cleanup_v;
SELECT 'same_rows', arraySort(groupArray((id, ver, is_deleted, c1, c2)))
    = (SELECT arraySort(groupArray((id, ver, is_deleted, c1, c2))) FROM t_cleanup_h)
FROM t_cleanup_v;

SYSTEM FLUSH LOGS part_log;
SELECT 'algo', table, merge_algorithm FROM system.part_log
    WHERE database = currentDatabase() AND table IN ('t_cleanup_v', 't_cleanup_h') AND event_type = 'MergeParts'
    ORDER BY table;

DROP TABLE t_cleanup_v;
DROP TABLE t_cleanup_h;
DROP VIEW t_cleanup_gen;
