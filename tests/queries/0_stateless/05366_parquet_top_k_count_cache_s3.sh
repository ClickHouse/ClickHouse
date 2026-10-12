#!/usr/bin/env bash
# Tags: no-fasttest
# Tag no-fasttest: needs S3 and Parquet

# Same as `05355_parquet_top_k_count_cache`, but for the object storage path: an `ORDER BY ... LIMIT n`
# read of a Parquet object with TopN dynamic filtering (`__topKFilter`) returns only the rows that
# survive the running threshold. Their number must not be stored in the cache for `count()` from
# files as the total of the whole object, otherwise a later cached `count()` undercounts.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

FILE="${CLICKHOUSE_DATABASE}_05366.parquet"

${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION s3(s3_conn, filename = '${FILE}', format = 'Parquet')
    SELECT number AS k FROM numbers(100000)
    SETTINGS output_format_parquet_row_group_size = 1000, s3_truncate_on_insert = 1"

SETTINGS="use_cache_for_count_from_files = 1, use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 1000, max_threads = 1"

${CLICKHOUSE_CLIENT} --query "SELECT k FROM s3(s3_conn, filename = '${FILE}', format = 'Parquet') ORDER BY k DESC LIMIT 1 SETTINGS ${SETTINGS}"
${CLICKHOUSE_CLIENT} --query "SELECT count() FROM s3(s3_conn, filename = '${FILE}', format = 'Parquet') SETTINGS ${SETTINGS}"
