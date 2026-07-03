#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-random-settings, no-random-merge-tree-settings, no-flaky-check
# no-flaky-check: the test is long and timeouts because of thread-fuzzer

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

$CLICKHOUSE_CLIENT -m -q "
DROP TABLE IF EXISTS test;
CREATE TABLE test (a Int32, b String)
ENGINE = MergeTree()
ORDER BY a
SETTINGS disk = disk(type = cache,
                     max_size = '1Gi',
                     max_file_segment_size = '40Mi',
                     boundary_alignment = '20Mi',
                     path = '$CLICKHOUSE_TEST_UNIQUE_NAME',
                     name = '$CLICKHOUSE_TEST_UNIQUE_NAME',
                     disk = 's3_disk');

INSERT INTO test SELECT number, randomString(100) FROM numbers(1000000);
"

QUERY_ID=$RANDOM
$CLICKHOUSE_CLIENT --query_id "$QUERY_ID" -m -q "
SET enable_filesystem_cache_log = 1;
SET read_through_distributed_cache=0;
SYSTEM CLEAR FILESYSTEM CACHE;
SELECT * FROM test WHERE NOT ignore() LIMIT 1 FORMAT Null;
SYSTEM FLUSH LOGS filesystem_cache_log;
"

# Materialize the file-segment view once instead of re-deriving it for every
# assertion below. Scanning system.remote_data_paths walks the metadata of all
# disks, which is slow under thread fuzzer; evaluating it once keeps the test
# from timing out.
$CLICKHOUSE_CLIENT -m -q "
DROP TABLE IF EXISTS file_segments;
CREATE TABLE file_segments ENGINE = Memory AS
SELECT cache_path, file_size,
    tupleElement(file_segment_range, 2) - tupleElement(file_segment_range, 1) + 1 as file_segment_size,
    formatReadableSize(file_size) as formatted_file_size,
    formatReadableSize(file_segment_size) as formatted_file_segment_size,
    tupleElement(file_segment_range, 2) as end_offset
FROM (
    SELECT arrayJoin(cache_paths) AS cache_path,
           local_path,
           remote_path,
           size as file_size
    FROM system.remote_data_paths
    WHERE endsWith(local_path, '.bin')
) AS data_paths
INNER JOIN system.filesystem_cache_log AS cache_log
ON data_paths.remote_path = cache_log.source_file_path
WHERE query_id = '$QUERY_ID';
"

# File segments cannot be less that 20Mi,
# except for last file segment in a file or if file size is less.
$CLICKHOUSE_CLIENT -m -q "
SELECT count() FROM file_segments
WHERE file_segment_size < file_size
AND end_offset + 1 != file_size
AND file_segment_size < 20 * 1024 * 1024;
"

all=$($CLICKHOUSE_CLIENT -m -q "
SELECT count() FROM file_segments
WHERE file_segment_size < file_size AND end_offset + 1 != file_size;
")
#echo $all

if [ "$all" -gt "1" ]; then
  echo "OK"
else
  echo "FAIL"
fi

count=$($CLICKHOUSE_CLIENT -m -q "
SELECT count() FROM file_segments
WHERE file_segment_size < file_size
AND end_offset + 1 != file_size
AND formatted_file_segment_size in ('20.00 MiB', '40.00 MiB')
")

if [ "$count" = "$all" ]; then
  echo "OK"
else
  echo "FAIL"
fi

# Same idea for the join against system.filesystem_cache: materialize once.
$CLICKHOUSE_CLIENT -m -q "
DROP TABLE IF EXISTS file_segments_cache;
CREATE TABLE file_segments_cache ENGINE = Memory AS
SELECT *
FROM file_segments AS cache_log
INNER JOIN system.filesystem_cache AS cache
ON cache_log.cache_path = cache.cache_path AND cache.cache_name = '$CLICKHOUSE_TEST_UNIQUE_NAME';
"

$CLICKHOUSE_CLIENT -m -q "
SELECT count() FROM file_segments_cache
WHERE file_segment_range_begin - file_segment_range_end + 1 < file_size
AND file_segment_range_end + 1 != file_size
AND downloaded_size < 20 * 1024 * 1024;
"

$CLICKHOUSE_CLIENT -m -q "
SELECT count() FROM file_segments_cache
WHERE file_segment_range_begin - file_segment_range_end + 1 < file_size
AND file_segment_range_end + 1 != file_size
AND formatReadableSize(downloaded_size) not in ('20.00 MiB', '40.00 MiB');
"

all=$($CLICKHOUSE_CLIENT -m -q "
SELECT count() FROM file_segments_cache
WHERE file_segment_size < file_size AND file_segment_range_end + 1 != file_size;
")

if [ "$all" -gt "1" ]; then
  echo "OK"
else
  echo "FAIL"
fi

count2=$($CLICKHOUSE_CLIENT -m -q "
SELECT count() FROM file_segments_cache
WHERE file_segment_range_begin - file_segment_range_end + 1 < file_size
AND file_segment_range_end + 1 != file_size
AND formatReadableSize(downloaded_size) in ('20.00 MiB', '40.00 MiB');
")

if [ "$count2" = "$all" ]; then
  echo "OK"
else
  echo "FAIL"
fi
