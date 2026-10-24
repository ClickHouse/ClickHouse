#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-parallel-replicas, no-random-settings
# no-fasttest: Parquet support is not built in fast-test images
# no-parallel: the columns cache is system-wide, and the test drops it and checks what it holds
# no-parallel-replicas: profile events are not available on the second replica
# no-random-settings: which columns are cached depends on the PREWHERE and on the cache settings

# The columns cache for `Parquet` files read by the `file` table function.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"
FILE_PATH="${USER_FILES_PATH}/${FILE}"

function write_file()
{
    # $1 - multiplier of the values, $2 - how long ago the file was last modified.
    $CLICKHOUSE_CLIENT -q "
        INSERT INTO FUNCTION file('${FILE}', Parquet)
        SELECT
            number * $1 AS id,
            toString(number * $1) AS s,
            [number, number * $1] AS arr,
            if(number % 3 = 0, NULL, number * $1) AS n,
            (number, toString(number * $1)) AS t,
            map(toString(number % 5), number * $1) AS m
        FROM numbers(10000)
        SETTINGS output_format_parquet_row_group_size = 1000, engine_file_truncate_on_insert = 1"
    # Only a file that has not been modified for a few seconds is cached: its version token
    # identifies its content only then.
    if [ -n "$2" ]; then
        touch -d "$2" "${FILE_PATH}"
    fi
}

QUERY="SELECT sum(id), sum(length(s)), sum(arraySum(arr)), sum(n), count(n), sum(t.1), sum(length(t.2)), sum(arraySum(mapValues(m))) FROM file('${FILE}')"

function run()
{
    # $1 - log comment, $2 - settings.
    $CLICKHOUSE_CLIENT --log_comment "${CLICKHOUSE_TEST_UNIQUE_NAME}_$1" -q "${QUERY} SETTINGS $2"
}

function events()
{
    # The cache events of the query with the log comment $1.
    $CLICKHOUSE_CLIENT -q "
        SYSTEM FLUSH LOGS query_log;
        SELECT '$1', ProfileEvents['ColumnsCacheHits'] AS hits, ProfileEvents['ColumnsCacheMisses'] AS misses
        FROM system.query_log
        WHERE log_comment = '${CLICKHOUSE_TEST_UNIQUE_NAME}_$1' AND type = 'QueryFinish' AND current_database = currentDatabase()"
}

function cache_content()
{
    $CLICKHOUSE_CLIENT -q "
        SELECT column, count(), sum(rows), min(row_begin), max(row_end)
        FROM system.columns_cache
        WHERE endsWith(part, '/${FILE}') AND table_uuid = toUUID('00000000-0000-0000-0000-000000000000')
        GROUP BY column ORDER BY column"
}

write_file 1 "1 minute ago"
$CLICKHOUSE_CLIENT -q "SYSTEM DROP COLUMNS CACHE"

echo "--- without the cache"
run no_cache "use_columns_cache = 0"
events no_cache
cache_content

echo "--- the first read writes every column of every row group, read in several subgroups"
run first "use_columns_cache = 1, input_format_parquet_max_block_size = 300"
events first
cache_content

echo "--- the second read is served from the cache"
run second "use_columns_cache = 1"
events second

echo "--- served from the cache in several subgroups"
run second_subgroups "use_columns_cache = 1, input_format_parquet_max_block_size = 300"
events second_subgroups

echo "--- reads disabled"
run no_reads "use_columns_cache = 1, enable_reads_from_columns_cache = 0"
events no_reads

echo "--- PREWHERE: the columns of the filter are written, the columns read after it are not"
$CLICKHOUSE_CLIENT -q "SYSTEM DROP COLUMNS CACHE"
PREWHERE_QUERY="SELECT sum(id), sum(length(s)), count() FROM file('${FILE}') PREWHERE id % 7 = 0"
$CLICKHOUSE_CLIENT -q "${PREWHERE_QUERY} SETTINGS use_columns_cache = 0"
$CLICKHOUSE_CLIENT --log_comment "${CLICKHOUSE_TEST_UNIQUE_NAME}_prewhere_first" -q "${PREWHERE_QUERY} SETTINGS use_columns_cache = 1"
events prewhere_first
cache_content
$CLICKHOUSE_CLIENT --log_comment "${CLICKHOUSE_TEST_UNIQUE_NAME}_prewhere_second" -q "${PREWHERE_QUERY} SETTINGS use_columns_cache = 1, input_format_parquet_max_block_size = 300"
events prewhere_second

echo "--- the same column read as another type is another entry"
$CLICKHOUSE_CLIENT -q "SELECT sum(id), toTypeName(any(id)) FROM file('${FILE}', Parquet, 'id Int32') SETTINGS use_columns_cache = 1"
$CLICKHOUSE_CLIENT --log_comment "${CLICKHOUSE_TEST_UNIQUE_NAME}_other_type" -q "SELECT sum(id), toTypeName(any(id)) FROM file('${FILE}', Parquet, 'id Int32') SETTINGS use_columns_cache = 1"
events other_type
cache_content

echo "--- a column with nulls replaced with defaults is not written"
$CLICKHOUSE_CLIENT -q "SYSTEM DROP COLUMNS CACHE"
DEFAULTS_QUERY="SELECT sum(id), sum(n) FROM file('${FILE}', Parquet, 'id UInt64, n UInt64 DEFAULT 42')"
$CLICKHOUSE_CLIENT -q "${DEFAULTS_QUERY} SETTINGS use_columns_cache = 0"
$CLICKHOUSE_CLIENT -q "${DEFAULTS_QUERY} SETTINGS use_columns_cache = 1"
$CLICKHOUSE_CLIENT -q "${DEFAULTS_QUERY} SETTINGS use_columns_cache = 1"
cache_content

echo "--- a rewritten file is read anew"
write_file 2 "30 seconds ago"
run rewritten "use_columns_cache = 1"
run rewritten_no_cache "use_columns_cache = 0"
events rewritten

echo "--- a file modified just now is not cached"
write_file 3 ""
run recent "use_columns_cache = 1"
events recent
run recent_no_cache "use_columns_cache = 0"

rm -f "${FILE_PATH}"
