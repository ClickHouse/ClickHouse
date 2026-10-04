-- Tags: no-random-merge-tree-settings
-- The test asserts horizontal/vertical merge selection and physical column materialization.

DROP TABLE IF EXISTS t_missing_marker_conflict;

CREATE TABLE t_missing_marker_conflict
(
    k UInt64,
    b UInt64,
    payload String
)
ENGINE = MergeTree
ORDER BY k
SETTINGS skip_empty_columns_on_insert = 1,
         serialization_info_version = 'with_missing_columns',
         min_bytes_for_wide_part = 0,
         min_rows_for_wide_part = 0,
         enable_vertical_merge_algorithm = 0,
         enable_block_number_column = 0,
         enable_block_offset_column = 0;

INSERT INTO t_missing_marker_conflict VALUES (1, 0, 'first');
ALTER TABLE t_missing_marker_conflict DETACH PARTITION tuple();
ALTER TABLE t_missing_marker_conflict MODIFY COLUMN b String;
ALTER TABLE t_missing_marker_conflict ATTACH PARTITION tuple();
INSERT INTO t_missing_marker_conflict VALUES (2, '', 'second');

SELECT 'different marker types before horizontal merge';
SELECT k, b, length(b) FROM t_missing_marker_conflict ORDER BY k;
OPTIMIZE TABLE t_missing_marker_conflict FINAL;
SELECT 'different marker types after horizontal merge';
SELECT k, b, length(b) FROM t_missing_marker_conflict ORDER BY k;
SELECT column FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_missing_marker_conflict' AND active
ORDER BY column;

DROP TABLE t_missing_marker_conflict;

CREATE TABLE t_missing_marker_conflict
(
    k UInt64,
    b UInt64,
    payload String
)
ENGINE = MergeTree
ORDER BY k
SETTINGS skip_empty_columns_on_insert = 1,
         serialization_info_version = 'with_missing_columns',
         min_bytes_for_wide_part = 0,
         min_rows_for_wide_part = 0,
         enable_vertical_merge_algorithm = 1,
         vertical_merge_algorithm_min_rows_to_activate = 0,
         vertical_merge_algorithm_min_bytes_to_activate = 0,
         vertical_merge_algorithm_min_columns_to_activate = 0,
         enable_block_number_column = 0,
         enable_block_offset_column = 0;

INSERT INTO t_missing_marker_conflict VALUES (1, 0, 'first');
ALTER TABLE t_missing_marker_conflict DETACH PARTITION tuple();
ALTER TABLE t_missing_marker_conflict MODIFY COLUMN b String;
ALTER TABLE t_missing_marker_conflict ATTACH PARTITION tuple();
INSERT INTO t_missing_marker_conflict VALUES (2, '', 'second');

SELECT 'different marker types before vertical merge';
SELECT k, b, length(b) FROM t_missing_marker_conflict ORDER BY k;
OPTIMIZE TABLE t_missing_marker_conflict FINAL;
SELECT 'different marker types after vertical merge';
SELECT k, b, length(b) FROM t_missing_marker_conflict ORDER BY k;
SYSTEM FLUSH LOGS part_log;
SELECT merge_algorithm FROM system.part_log
WHERE database = currentDatabase() AND table = 't_missing_marker_conflict' AND event_type = 'MergeParts'
ORDER BY event_time_microseconds DESC LIMIT 1;
SELECT column FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_missing_marker_conflict' AND active
ORDER BY column;

DROP TABLE t_missing_marker_conflict;

CREATE TABLE t_missing_marker_same
(
    k UInt64,
    b UInt64
)
ENGINE = MergeTree
ORDER BY k
SETTINGS skip_empty_columns_on_insert = 1,
         serialization_info_version = 'with_missing_columns',
         min_bytes_for_wide_part = 0,
         min_rows_for_wide_part = 0,
         enable_vertical_merge_algorithm = 0,
         enable_block_number_column = 0,
         enable_block_offset_column = 0;

INSERT INTO t_missing_marker_same VALUES (1, 0);
INSERT INTO t_missing_marker_same VALUES (2, 0);
OPTIMIZE TABLE t_missing_marker_same FINAL;

SELECT 'same marker types stay omitted';
SELECT k, b FROM t_missing_marker_same ORDER BY k;
SELECT column FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_missing_marker_same' AND active
ORDER BY column;

DROP TABLE t_missing_marker_same;

CREATE TABLE t_missing_marker_physical
(
    k UInt64,
    b UInt64
)
ENGINE = MergeTree
ORDER BY k
SETTINGS skip_empty_columns_on_insert = 1,
         serialization_info_version = 'with_missing_columns',
         min_bytes_for_wide_part = 0,
         min_rows_for_wide_part = 0,
         enable_vertical_merge_algorithm = 0,
         enable_block_number_column = 0,
         enable_block_offset_column = 0;

INSERT INTO t_missing_marker_physical VALUES (1, 0);
INSERT INTO t_missing_marker_physical VALUES (2, 7);
OPTIMIZE TABLE t_missing_marker_physical FINAL;

SELECT 'marker and physical column are materialized';
SELECT k, b FROM t_missing_marker_physical ORDER BY k;
SELECT column FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_missing_marker_physical' AND active
ORDER BY column;

DROP TABLE t_missing_marker_physical;

CREATE TABLE t_missing_marker_absent
(
    k UInt64
)
ENGINE = MergeTree
ORDER BY k
SETTINGS skip_empty_columns_on_insert = 1,
         serialization_info_version = 'with_missing_columns',
         min_bytes_for_wide_part = 0,
         min_rows_for_wide_part = 0,
         enable_vertical_merge_algorithm = 0,
         enable_block_number_column = 0,
         enable_block_offset_column = 0;

INSERT INTO t_missing_marker_absent VALUES (1);
ALTER TABLE t_missing_marker_absent ADD COLUMN b UInt64;
INSERT INTO t_missing_marker_absent VALUES (2, 0);
ALTER TABLE t_missing_marker_absent MODIFY COLUMN b String;

SELECT 'marker and absent before merge';
SELECT k, b, length(b) FROM t_missing_marker_absent ORDER BY k;
OPTIMIZE TABLE t_missing_marker_absent FINAL;
SELECT 'marker and absent after merge';
SELECT k, b, length(b) FROM t_missing_marker_absent ORDER BY k;
SELECT column FROM system.parts_columns
WHERE database = currentDatabase() AND table = 't_missing_marker_absent' AND active
ORDER BY column;

DROP TABLE t_missing_marker_absent;
