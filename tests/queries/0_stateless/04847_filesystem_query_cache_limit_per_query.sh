#!/usr/bin/env bash
# Tags: no-fasttest

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# The budget is per query, not shared: two queries each get their own, and a query which reuses
# the query id of a finished one starts with a fresh budget too.

disk_name="04847_query_limit_per_query_${CLICKHOUSE_DATABASE}"
limit=524288
cache_settings="enable_filesystem_cache = 1, read_from_filesystem_cache_if_exists_otherwise_bypass_cache = 0"

$CLICKHOUSE_CLIENT -m --query "
DROP TABLE IF EXISTS test_first;
DROP TABLE IF EXISTS test_second;
DROP TABLE IF EXISTS test_third;
DROP TABLE IF EXISTS test_fourth;
CREATE TABLE test_first (key UInt32, value String)
ENGINE = MergeTree() ORDER BY key
SETTINGS min_bytes_for_wide_part = 0, disk = disk(
    type = cache,
    name = '${disk_name}',
    path = '${disk_name}',
    max_size = '100Mi',
    max_file_segment_size = '64Ki',
    boundary_alignment = '64Ki',
    background_download_threads = 0,
    background_download_queue_size_limit = 0,
    load_metadata_asynchronously = 0,
    enable_filesystem_query_cache_limit = 1,
    disk = disk(type = object_storage, object_storage_type = local, metadata_type = local, path = '${disk_name}_data/'));
CREATE TABLE test_second AS test_first;
CREATE TABLE test_third AS test_first;
CREATE TABLE test_fourth AS test_first;

SET enable_filesystem_cache_on_write_operations = 0;
SET max_insert_threads = 1;
INSERT INTO test_first SELECT number, toString(rand64()) FROM numbers(300000);
INSERT INTO test_second SELECT number, toString(rand64()) FROM numbers(300000);
INSERT INTO test_third SELECT number, toString(rand64()) FROM numbers(300000);
INSERT INTO test_fourth SELECT number, toString(rand64()) FROM numbers(300000);
SYSTEM STOP MERGES test_first;
SYSTEM STOP MERGES test_second;
SYSTEM STOP MERGES test_third;
SYSTEM STOP MERGES test_fourth;
"

written_bytes() {
    $CLICKHOUSE_CLIENT -m --query "
    SYSTEM FLUSH LOGS query_log;
    SELECT max(ProfileEvents['CachedReadBufferCacheWriteBytes'])
    FROM (SELECT ProfileEvents FROM system.query_log
          WHERE query_id = '$1' AND type = 'QueryFinish' AND current_database = currentDatabase()
          ORDER BY event_time_microseconds DESC LIMIT 1);"
}

read_limited() {
    $CLICKHOUSE_CLIENT --query_id "$1" \
        --query "SELECT * FROM $2 SETTINGS ${cache_settings}, filesystem_cache_query_limit_bytes = ${limit} FORMAT Null"
}

# Each query fills its own budget on its own table. Both tables are far bigger than the budget, so
# each query must use up most of it - if the budget were shared, the second query would cache almost
# nothing.
read_limited "first_${CLICKHOUSE_DATABASE}" test_first
read_limited "second_${CLICKHOUSE_DATABASE}" test_second
first_written=$(written_bytes "first_${CLICKHOUSE_DATABASE}")
second_written=$(written_bytes "second_${CLICKHOUSE_DATABASE}")
# Neither exceeds the budget, and together they write more than one budget: the budget is per query,
# not one shared by the cache.
echo "each query uses its own budget  $(( first_written <= limit && second_written <= limit \
    && first_written + second_written > limit ))"

# A new query reusing a finished query's id must not inherit its spent budget: it reads a table
# which is not cached yet, so it caches again up to its own budget.
read_limited "reused_${CLICKHOUSE_DATABASE}" test_third
read_limited "reused_${CLICKHOUSE_DATABASE}" test_fourth
reused_written=$(written_bytes "reused_${CLICKHOUSE_DATABASE}")
echo "a reused query id starts fresh  $(( reused_written > limit / 2 && reused_written <= limit ))"

$CLICKHOUSE_CLIENT -m --query "DROP TABLE test_first; DROP TABLE test_second; DROP TABLE test_third; DROP TABLE test_fourth;"
