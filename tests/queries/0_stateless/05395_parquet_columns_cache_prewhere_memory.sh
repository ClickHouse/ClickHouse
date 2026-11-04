#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# no-fasttest: Parquet support is not built in fast-test images
# no-parallel: the columns cache is system-wide, and the test drops it to reset its admission control

# A column served from the columns cache is not decoded, but its rows are still copied to the output
# of the `Parquet` reader (and kept until the block is delivered). The reader must account for the
# copies against its memory watermarks, otherwise a `PREWHERE` read queues the copies of many row
# groups and exceeds the memory limit of the query, unlike the same read without the cache.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE="${CLICKHOUSE_TEST_UNIQUE_NAME}.parquet"
FILE_PATH="${USER_FILES_PATH}/${FILE}"

# The file (32 row groups of about 1 MiB) must fit in the columns cache of the test server: 100 MB,
# a half of which holds the new entries with the `SLRU` policy.
$CLICKHOUSE_CLIENT -q "
    INSERT INTO FUNCTION file('${FILE}', Parquet) SELECT leftPad(toString(number), 1024, 'a') AS s FROM numbers(32768)
    SETTINGS output_format_parquet_row_group_size = 1024, engine_file_truncate_on_insert = 1"
# Only a file that has not been modified for a few seconds is cached.
touch -d "1 minute ago" "${FILE_PATH}"

QUERY="SELECT sum(length(s)) FROM file('${FILE}', Parquet, 's String') PREWHERE startsWith(s, 'a')"
LIMITS="max_threads = 1, max_parsing_threads = 1, input_format_parquet_memory_low_watermark = '4Mi',
    input_format_parquet_memory_high_watermark = '8Mi'"

ROW_GROUPS=$($CLICKHOUSE_CLIENT -q "SELECT num_row_groups FROM file('${FILE}', ParquetMetadata)")

# Other tests may have lowered the admission probability of the cache; dropping the cache resets it.
$CLICKHOUSE_CLIENT -q "SYSTEM DROP COLUMNS CACHE"

# The admission control of the cache can skip a write: read the file until all its row groups are there.
for _ in {1..20}; do
    $CLICKHOUSE_CLIENT -q "${QUERY} FORMAT Null SETTINGS ${LIMITS},
        use_columns_cache = 1, enable_reads_from_columns_cache = 1, enable_writes_to_columns_cache = 1,
        columns_cache_max_bytes_to_write_to_cache = 0, columns_cache_max_estimated_bytes_to_write_to_cache = 0"
    [ "$($CLICKHOUSE_CLIENT -q "SELECT count() FROM system.columns_cache WHERE database = '' AND endsWith(part, '/${FILE}')")" = "${ROW_GROUPS}" ] && break
done

echo "--- without the cache"
$CLICKHOUSE_CLIENT -q "${QUERY} SETTINGS ${LIMITS}, max_memory_usage = '16Mi', use_columns_cache = 0"
echo "--- from the cache"
$CLICKHOUSE_CLIENT -q "${QUERY} SETTINGS ${LIMITS}, max_memory_usage = '16Mi',
    use_columns_cache = 1, enable_reads_from_columns_cache = 1, enable_writes_to_columns_cache = 0"
$CLICKHOUSE_CLIENT -q "SELECT count() = ${ROW_GROUPS} FROM system.columns_cache WHERE database = '' AND endsWith(part, '/${FILE}')"

rm -f "${FILE_PATH}"
