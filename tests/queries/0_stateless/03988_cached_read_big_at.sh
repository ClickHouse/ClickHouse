#!/usr/bin/env bash
# Tags: no-fasttest, no-random-settings
# - no-fasttest: requires S3
# - no-random-settings: the read must go through the filesystem cache with fixed settings

# Regression test for https://github.com/ClickHouse/ClickHouse/issues/97325:
# reading Parquet through the filesystem cache failed with `CANNOT_SEEK_THROUGH_FILE`
# ("Seek is allowed only before first read attempt from the buffer").
#
# The Parquet reader reads column chunks with concurrent `readBigAt` calls. When two adjacent
# ranges share a file segment of the cache, one call downloads the beginning of the segment and
# leaves it partially downloaded (with its remote reader kept for the background download), and the
# other call, which waited for it, continues downloading the segment with the same remote reader.
# `CachedOnDiskReadBufferFromFile::readBigAt` left that reader in an inconsistent state.
#
# A small `remote_read_min_bytes_for_seek` makes the reader split the file into many small read
# tasks, so that many file segments (1 MiB in `cache_for_readbigat`) are shared by two tasks.
# The file name is unique per run, so its cache key (path + etag) is fresh and the cache starts empty
# even when the test is rerun in the same database.
# Parallel replicas are disabled, so that the reads (and their profile events) happen in this query.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

RUN_ID="${CLICKHOUSE_DATABASE}_03988_$($CLICKHOUSE_CLIENT -q "SELECT generateUUIDv4()")"
FILE="${RUN_ID}.parquet"
ROWS=50000

# `sipHash128` rather than `MD5`: `MD5` throws `SUPPORT_IS_DISABLED` in OpenSSL FIPS builds.
$CLICKHOUSE_CLIENT -q "
    INSERT INTO FUNCTION s3(s3_conn, filename = '$FILE', format = Parquet)
    SELECT
        hex(SHA256(toString(number))) AS a,
        substring(hex(sipHash128(toString(number))), 1, 16) AS b,
        substring(hex(SHA256(toString(number + 1))), 1, 40) AS c
    FROM numbers($ROWS)
    SETTINGS
        s3_truncate_on_insert = 1,
        output_format_parquet_row_group_size = 5000,
        output_format_parquet_compression_method = 'none'
"

# The expected checksums, computed from the source data.
EXPECTED=$($CLICKHOUSE_CLIENT -q "
    SELECT
        sum(cityHash64(hex(SHA256(toString(number))))),
        sum(cityHash64(substring(hex(sipHash128(toString(number))), 1, 16))),
        sum(cityHash64(substring(hex(SHA256(toString(number + 1))), 1, 40)))
    FROM numbers($ROWS)
")

function check()
{
    local name=$1
    local result
    result=$($CLICKHOUSE_CLIENT --query_id "${RUN_ID}_${name}" -q "
        SELECT sum(cityHash64(a)), sum(cityHash64(b)), sum(cityHash64(c))
        FROM s3(s3_conn, filename = '$FILE', format = Parquet, structure = 'a String, b String, c String')
        SETTINGS
            filesystem_cache_name = 'cache_for_readbigat',
            enable_filesystem_cache = 1,
            max_download_threads = 1,
            enable_parallel_replicas = 0,
            use_columns_cache = 0,
            remote_read_min_bytes_for_seek = 65536" 2>&1)
    if [ "$result" == "$EXPECTED" ]; then
        echo "$name OK"
    else
        echo "$name FAIL: got '$result', expected '$EXPECTED'"
    fi
}

# The cache is empty for this file: the data is downloaded into the cache.
check download
# Everything is in the cache now.
check cached

$CLICKHOUSE_CLIENT -q "SYSTEM FLUSH LOGS query_log"

# Make sure that the reads actually went through the filesystem cache.
$CLICKHOUSE_CLIENT -q "
    SELECT
        replaceOne(query_id, '${RUN_ID}_', '') AS name,
        ProfileEvents['CachedReadBufferCacheWriteBytes'] > 0,
        ProfileEvents['CachedReadBufferReadFromCacheBytes'] > 0
    FROM system.query_log
    WHERE event_date >= yesterday() AND current_database = currentDatabase() AND type = 'QueryFinish'
        AND query_id IN ('${RUN_ID}_download', '${RUN_ID}_cached')
    ORDER BY name DESC
"
