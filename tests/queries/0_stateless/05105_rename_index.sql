DROP TABLE IF EXISTS t_rename_index_wide;

CREATE TABLE t_rename_index_wide
(
    id UInt64,
    value UInt64,
    INDEX old_index value TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, packed_skip_index_max_bytes = 0, index_granularity = 1024;

INSERT INTO t_rename_index_wide SELECT number, number * 7 FROM numbers(2000);
ALTER TABLE t_rename_index_wide RENAME INDEX old_index TO new_index SETTINGS mutations_sync = 2;

SELECT 'wide_index', name
FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_rename_index_wide'
ORDER BY name;
SELECT 'wide_count', count() FROM t_rename_index_wide WHERE value BETWEEN 700 AND 1400;
CHECK TABLE t_rename_index_wide SETTINGS check_query_single_value_result = 1;

ALTER TABLE t_rename_index_wide RENAME INDEX IF EXISTS missing_index TO ignored_index;
ALTER TABLE t_rename_index_wide RENAME INDEX new_index TO renamed_index SETTINGS mutations_sync = 2;
SELECT 'wide_renamed_index', name
FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_rename_index_wide'
ORDER BY name;
SELECT 'wide_renamed_count', count() FROM t_rename_index_wide WHERE value = 700;
CHECK TABLE t_rename_index_wide SETTINGS check_query_single_value_result = 1;

ALTER TABLE t_rename_index_wide RENAME INDEX missing_index TO another_index; -- { serverError BAD_ARGUMENTS }

DROP TABLE t_rename_index_wide;

DROP TABLE IF EXISTS t_rename_index_packed;

CREATE TABLE t_rename_index_packed
(
    id UInt64,
    value UInt64,
    other UInt64,
    INDEX old_index value TYPE minmax GRANULARITY 1,
    INDEX survivor other TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = 0, packed_skip_index_max_bytes = '1M', index_granularity = 1024;

INSERT INTO t_rename_index_packed SELECT number, number * 7, number * 11 FROM numbers(2000);
ALTER TABLE t_rename_index_packed RENAME INDEX old_index TO new_index SETTINGS mutations_sync = 2;

SELECT 'packed_indexes', arraySort(groupArray(name))
FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_rename_index_packed';
SELECT 'packed_count', count() FROM t_rename_index_packed WHERE value = 700;
CHECK TABLE t_rename_index_packed SETTINGS check_query_single_value_result = 1;

DROP TABLE t_rename_index_packed;

DROP TABLE IF EXISTS t_rename_index_compact;

CREATE TABLE t_rename_index_compact
(
    id UInt64,
    value UInt64,
    INDEX old_index value TYPE minmax GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS min_bytes_for_wide_part = '1G', min_rows_for_wide_part = 100000000,
         packed_skip_index_max_bytes = '1M', index_granularity = 1024;

INSERT INTO t_rename_index_compact SELECT number, number * 7 FROM numbers(2000);
ALTER TABLE t_rename_index_compact RENAME INDEX old_index TO new_index SETTINGS mutations_sync = 2;

SELECT 'compact_index', name
FROM system.data_skipping_indices
WHERE database = currentDatabase() AND table = 't_rename_index_compact'
ORDER BY name;
SELECT 'compact_count', count() FROM t_rename_index_compact WHERE value = 700;
CHECK TABLE t_rename_index_compact SETTINGS check_query_single_value_result = 1;

DROP TABLE t_rename_index_compact;
