#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel
# - no-fasttest: needs S3 (MinIO)
# - no-parallel: other tests drop the query condition cache, which would remove the entries checked here

# The Parquet reader does not apply TopN dynamic filtering to a file that does not store the sort
# column. Such a file is read as without TopN, so a TopK read must still populate the query condition
# cache for it, and a later plain read must find these entries.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

S3_DIR="${CLICKHOUSE_DATABASE}_05332"

# The first file stores the sort column `k`, the second does not; no row of the second file has `v < 1000`.
${CLICKHOUSE_CLIENT} --query "
    INSERT INTO FUNCTION s3(s3_conn, filename = '${S3_DIR}/a.parquet', format = Parquet)
    SELECT toInt64(number) AS k, toInt64(number) AS v FROM numbers(3000)
    SETTINGS s3_truncate_on_insert = 1, output_format_parquet_row_group_size = 1000;
    INSERT INTO FUNCTION s3(s3_conn, filename = '${S3_DIR}/b.parquet', format = Parquet)
    SELECT toInt64(3000 + number) AS v FROM numbers(3000)
    SETTINGS s3_truncate_on_insert = 1, output_format_parquet_row_group_size = 1000;
"

# A table, not a table function: the query condition cache is keyed by the table UUID.
${CLICKHOUSE_CLIENT} --query "CREATE TABLE t_05332_all (k Int64, v Int64) ENGINE = S3(s3_conn, filename = '${S3_DIR}/{a,b}.parquet', format = Parquet)"

SETTINGS="use_query_condition_cache = 1, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1,
    use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 1000,
    input_format_parquet_use_native_reader_v3 = 1, input_format_parquet_filter_push_down = 1,
    input_format_parquet_allow_missing_columns = 1, max_threads = 1, max_parsing_threads = 1"

${CLICKHOUSE_CLIENT} --log_comment "05332_top_k" --query "
    SELECT k, v FROM t_05332_all WHERE v < 1000 ORDER BY k DESC LIMIT 3 SETTINGS ${SETTINGS}"
${CLICKHOUSE_CLIENT} --log_comment "05332_plain" --query "
    SELECT count() FROM t_05332_all WHERE v < 1000 SETTINGS ${SETTINGS}"

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} --query "
    SELECT log_comment, ProfileEvents['QueryConditionCacheHits'] > 0
    FROM system.query_log
    WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish'
        AND log_comment = '05332_plain'"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_05332_all"
