#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# - no-fasttest: needs S3
# - no-parallel: enables a failpoint

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
OFF="max_threads = 1, use_query_condition_cache = 0, use_parquet_metadata_cache = 0, remote_filesystem_read_prefetch = 0"
ON="max_threads = 1, use_query_condition_cache = 0, use_parquet_metadata_cache = 0, remote_filesystem_read_prefetch = 1"
CACHE="max_threads = 1, use_query_condition_cache = 0, use_parquet_metadata_cache = 1, remote_filesystem_read_prefetch = 1"

function run()
{
    ${CLICKHOUSE_CLIENT} --query "$2, log_comment = '${CLICKHOUSE_DATABASE}_05340_$1'"
}

run off "$(query data) SETTINGS ${OFF}"
run on "$(query data) SETTINGS ${ON}"

${CLICKHOUSE_CLIENT} --query "SYSTEM ENABLE FAILPOINT object_storage_parquet_footer_prefetch_reject"
run rejected "$(query data) SETTINGS ${ON}"
${CLICKHOUSE_CLIENT} --query "SYSTEM DISABLE FAILPOINT object_storage_parquet_footer_prefetch_reject"

# Stops in the third object, while the footers of the next ones are being read.
run limit "SELECT count() FROM (SELECT number FROM s3(s3_conn, filename = '${DIR}/data/f_*.parquet', format = Parquet, structure = 'number UInt64, s String') LIMIT 50000) SETTINGS ${ON}"

# The objects of this query are not in the metadata cache before its first run.
run cache_1 "$(query cache) SETTINGS ${CACHE}"
run cache_2 "$(query cache) SETTINGS ${CACHE}"

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"

${CLICKHOUSE_CLIENT} --query "
    SELECT
        sumIf(ProfileEvents['S3GetObject'], arm = 'on') = sumIf(ProfileEvents['S3GetObject'], arm = 'off'),
        sumIf(ProfileEvents['ParquetPrefetchedFooters'], arm = 'on') > 0,
        sumIf(ProfileEvents['ParquetPrefetchedFooters'], arm = 'off') = 0
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
