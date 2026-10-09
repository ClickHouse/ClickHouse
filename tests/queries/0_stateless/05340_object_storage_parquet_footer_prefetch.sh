#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: needs S3

# A read of many Parquet objects from object storage reads the footers of the next objects in the background.
# The result, the number of requests and the Parquet metadata cache hits and misses of the query do not change.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

DIR="${CLICKHOUSE_DATABASE}_05340"

# 12 objects of about 1.5 MB: a smaller object is read whole, so its footer is not read ahead.
for prefix in data cache
do
    ${CLICKHOUSE_CLIENT} --query "
        INSERT INTO FUNCTION s3(s3_conn, filename = '${DIR}/${prefix}/f_{_partition_id}.parquet', format = Parquet)
        PARTITION BY number % 12
        SELECT number, concat(toString(cityHash64(number)), repeat('x', 40)) AS s FROM numbers(240000)
        SETTINGS s3_truncate_on_insert = 1, output_format_parquet_compression_method = 'none', output_format_parquet_row_group_size = 5000"
done

function query()
{
    echo "SELECT count(), sum(number), sum(length(s)) FROM s3(s3_conn, filename = '${DIR}/$1/f_*.parquet', format = Parquet, structure = 'number UInt64, s String') WHERE number % 7 = 3"
}

# One stream: the footers are read ahead from its third object on.
OFF="max_threads = 1, use_query_condition_cache = 0, use_parquet_metadata_cache = 0, remote_filesystem_read_prefetch = 0, use_page_cache_for_object_storage = 0, enable_parallel_replicas = 0, s3_validate_etag_on_read = 1"
ON="max_threads = 1, use_query_condition_cache = 0, use_parquet_metadata_cache = 0, remote_filesystem_read_prefetch = 1, use_page_cache_for_object_storage = 0, enable_parallel_replicas = 0, s3_validate_etag_on_read = 1"
# The footer and the data are read by different requests, so nothing is read ahead unless both are pinned to the listed ETag.
UNPINNED="max_threads = 1, use_query_condition_cache = 0, use_parquet_metadata_cache = 0, remote_filesystem_read_prefetch = 1, use_page_cache_for_object_storage = 0, enable_parallel_replicas = 0, s3_validate_etag_on_read = 0"
CACHE="max_threads = 1, use_query_condition_cache = 0, use_parquet_metadata_cache = 1, remote_filesystem_read_prefetch = 1, use_page_cache_for_object_storage = 0, enable_parallel_replicas = 0, s3_validate_etag_on_read = 1"

function run()
{
    ${CLICKHOUSE_CLIENT} --query "$2, log_comment = '${CLICKHOUSE_DATABASE}_05340_$1'"
}

run off "$(query data) SETTINGS ${OFF}"
run on "$(query data) SETTINGS ${ON}"
run unpinned "$(query data) SETTINGS ${UNPINNED}"

# Stops in the third object, while the footers of the next ones are being read.
run limit "SELECT count() FROM (SELECT number FROM s3(s3_conn, filename = '${DIR}/data/f_*.parquet', format = Parquet, structure = 'number UInt64, s String') LIMIT 50000) SETTINGS ${ON}"
run limit_1 "SELECT count() FROM (SELECT number FROM s3(s3_conn, filename = '${DIR}/data/f_*.parquet', format = Parquet, structure = 'number UInt64, s String') LIMIT 1) SETTINGS ${ON}"

# A TopK read does not use the query condition cache with `use_query_condition_cache_for_top_k = 0`, so the entries written by
# another read of the table must not stop its footers from being read ahead.
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_05340 (number UInt64, s String) ENGINE = S3(s3_conn, filename = '${DIR}/data/f_*.parquet', format = Parquet)"
TOP_K="max_threads = 1, use_query_condition_cache = 1, use_query_condition_cache_for_top_k = 0, use_top_k_dynamic_filtering = 1, use_parquet_metadata_cache = 0, remote_filesystem_read_prefetch = 1, use_page_cache_for_object_storage = 0, enable_parallel_replicas = 0, s3_validate_etag_on_read = 1"
run qcc_fill "SELECT count() FROM t_05340 WHERE number < 100000 SETTINGS ${TOP_K}"
run qcc_top_k "SELECT number FROM t_05340 WHERE number < 100000 ORDER BY number DESC LIMIT 1 SETTINGS ${TOP_K}"

# The objects of this query are not in the metadata cache before its first run.
run cache_1 "$(query cache) SETTINGS ${CACHE}"
run cache_2 "$(query cache) SETTINGS ${CACHE}"

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"

${CLICKHOUSE_CLIENT} --query "
    SELECT
        sumIf(ProfileEvents['S3GetObject'], arm = 'on') = sumIf(ProfileEvents['S3GetObject'], arm = 'off'),
        sumIf(ProfileEvents['ParquetPrefetchedFooters'], arm = 'on') > 0,
        sumIf(ProfileEvents['ParquetPrefetchedFooters'], arm = 'off') = 0,
        sumIf(ProfileEvents['ParquetFooterPrefetchRequests'], arm = 'on') > 0,
        sumIf(ProfileEvents['ParquetFooterPrefetchRequests'], arm = 'limit_1') = 0,
        sumIf(ProfileEvents['ParquetFooterPrefetchRequests'], arm = 'unpinned') = 0,
        sumIf(ProfileEvents['ParquetFooterPrefetchRequests'], arm = 'qcc_top_k') > 0
    FROM
    (
        SELECT replaceOne(log_comment, '${CLICKHOUSE_DATABASE}_05340_', '') AS arm, ProfileEvents
        FROM system.query_log
        WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '${CLICKHOUSE_DATABASE}_05340_%'
    )"

${CLICKHOUSE_CLIENT} --query "
    SELECT replaceOne(log_comment, '${CLICKHOUSE_DATABASE}_05340_', ''), ProfileEvents['ParquetMetadataCacheMisses'], ProfileEvents['ParquetMetadataCacheHits']
    FROM system.query_log
    WHERE current_database = currentDatabase() AND type = 'QueryFinish' AND log_comment LIKE '${CLICKHOUSE_DATABASE}_05340_cache_%'
    ORDER BY log_comment"
