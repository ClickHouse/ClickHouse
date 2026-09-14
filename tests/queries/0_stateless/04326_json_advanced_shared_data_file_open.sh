#!/usr/bin/env bash
# Test that reading the whole JSON column with ADVANCED shared data serialization
# does not open per-bucket data/marks/substreams files that are not needed for reading.
# When reading the whole JSON column, only Structure (per bucket) and Copy streams are used;
# the per-bucket Data, PathsMarks, Substreams, SubstreamsMarks, PathsSubstreamsMetadata
# files should not have their marks loaded.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE_NAME="test_json_file_open_${CLICKHOUSE_DATABASE}"
NUM_BUCKETS=2

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${TABLE_NAME}"
${CLICKHOUSE_CLIENT} -q "
    CREATE TABLE ${TABLE_NAME} (json JSON(max_dynamic_paths=0))
    ENGINE = MergeTree ORDER BY tuple()
    SETTINGS
        min_bytes_for_wide_part = 0,
        min_rows_for_wide_part = 0,
        ratio_of_defaults_for_sparse_serialization = 1,
        object_serialization_version = 'v3',
        object_shared_data_serialization_version = 'advanced',
        object_shared_data_serialization_version_for_zero_level_parts = 'advanced',
        object_shared_data_buckets_for_wide_part = ${NUM_BUCKETS}
"

${CLICKHOUSE_CLIENT} -q "SYSTEM STOP MERGES ${TABLE_NAME}"

# Insert data with many paths. All go to shared data (max_dynamic_paths=0).
${CLICKHOUSE_CLIENT} -q "
    INSERT INTO ${TABLE_NAME} FORMAT JSONAsObject
{\"a\": 1, \"b\": 2, \"c\": 3, \"d\": 4, \"e\": 5, \"f\": 6}
{\"a\": 10, \"b\": 20, \"c\": 30, \"d\": 40, \"e\": 50, \"f\": 60}
{\"a\": 100, \"b\": 200, \"c\": 300, \"d\": 400, \"e\": 500, \"f\": 600}
{\"a\": 1000, \"b\": 2000, \"c\": 3000, \"d\": 4000, \"e\": 5000, \"f\": 6000}
"

# With max_dynamic_paths=0, the mark files needed to read the whole JSON column are:
#   1 object_structure
#   NUM_BUCKETS structure files (one per bucket)
#   3 copy streams (sizes, paths_indexes, values)
# Total: NUM_BUCKETS + 4
#
# Without the fix, prefetchForColumn also opens per-bucket data streams
# (Data, PathsMarks, Substreams, SubstreamsMarks, PathsSubstreamsMetadata = 5 per bucket),
# giving NUM_BUCKETS * 6 + 4 total. With the fix these are skipped.
EXPECTED_MARKS=$((NUM_BUCKETS + 4))

# Clear mark cache and run SELECT json with prefetch enabled, counting MarkCacheMisses.
${CLICKHOUSE_CLIENT} -q "SYSTEM CLEAR MARK CACHE"

QUERY_ID=$(${CLICKHOUSE_CLIENT} -q "SELECT lower(hex(reverse(reinterpretAsString(generateUUIDv4()))))")

${CLICKHOUSE_CLIENT} --query_id "${QUERY_ID}" -q "
    SELECT json FROM ${TABLE_NAME} FORMAT Null
    SETTINGS max_threads = 1, load_marks_asynchronously = 0, local_filesystem_read_prefetch = 1, remote_filesystem_read_prefetch = 1, enable_parallel_replicas = 0
"

${CLICKHOUSE_CLIENT} -q "SYSTEM FLUSH LOGS query_log"

MARK_CACHE_MISSES=$(${CLICKHOUSE_CLIENT} -q "
    SELECT ProfileEvents['MarkCacheMisses']
    FROM system.query_log
    WHERE event_date >= yesterday()
        AND event_time >= now() - 600
        AND query_id = '${QUERY_ID}'
        AND type = 'QueryFinish'
        AND current_database = currentDatabase()
")

# MarkCacheMisses should be at most EXPECTED_MARKS.
# It can be lower if some marks were already cached, but must not be higher —
# that would mean unnecessary per-bucket data streams were loaded.
if [ "${MARK_CACHE_MISSES}" -le "${EXPECTED_MARKS}" ]; then
    echo "OK"
else
    echo "FAIL: MarkCacheMisses (${MARK_CACHE_MISSES}) > expected (${EXPECTED_MARKS})"
fi

${CLICKHOUSE_CLIENT} -q "DROP TABLE IF EXISTS ${TABLE_NAME}"
