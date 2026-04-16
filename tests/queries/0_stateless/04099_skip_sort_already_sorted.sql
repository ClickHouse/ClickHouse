-- Tags: no-random-settings

-- Test that per-chunk sorting is skipped when the chunk carries ChunkSortDescription
-- metadata indicating it is already sorted (O(1) check in PartialSortingTransform).

DROP TABLE IF EXISTS t_skip_sort_04099;

CREATE TABLE t_skip_sort_04099 (x UInt64, y String) ENGINE = MergeTree ORDER BY x;

-- Insert enough rows to produce multiple chunks at max_block_size = 10000.
INSERT INTO t_skip_sort_04099 SELECT number, toString(number) FROM numbers(50000);

-- Positive case: ORDER BY matches the table's sort key.
-- PartialSortingTransform should skip per-chunk sorting.
SELECT x, y FROM t_skip_sort_04099 ORDER BY x
FORMAT Null
SETTINGS optimize_read_in_order = 0, max_threads = 1, max_block_size = 10000;

SYSTEM FLUSH LOGS;

SELECT ProfileEvents['SortBlockAlreadySortedByChunkInfo'] > 0 AS sort_skipped_by_metadata
FROM system.query_log
WHERE
    current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query LIKE '%FROM t_skip_sort_04099 ORDER BY x%'
    AND query NOT LIKE '%system.query_log%'
ORDER BY event_time_microseconds DESC
LIMIT 1;

-- Negative case: ORDER BY does not match the sorting key.
SELECT x, y FROM t_skip_sort_04099 ORDER BY y
FORMAT Null
SETTINGS optimize_read_in_order = 0, max_threads = 1, max_block_size = 10000;

SYSTEM FLUSH LOGS;

SELECT ProfileEvents['SortBlockAlreadySortedByChunkInfo'] AS metadata_skips
FROM system.query_log
WHERE
    current_database = currentDatabase()
    AND type = 'QueryFinish'
    AND query LIKE '%FROM t_skip_sort_04099 ORDER BY y%'
    AND query NOT LIKE '%system.query_log%'
ORDER BY event_time_microseconds DESC
LIMIT 1;

DROP TABLE t_skip_sort_04099;
