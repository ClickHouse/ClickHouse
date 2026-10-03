#!/usr/bin/env bash
# Tags: no-fasttest
# - no-fasttest: needs Iceberg (USE_AVRO)

# TopN dynamic filtering is not applied to the files of an Iceberg table sorted by an identity partition
# column (the partition value comes from the manifest, not from the file). Such files are read as without
# TopN, so they must keep using the query condition cache: populate it, and consult it even with
# `use_query_condition_cache_for_top_k = 0`, which only concerns the reads that apply the filter.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

ICEBERG_DIR="${CLICKHOUSE_USER_FILES}/lakehouses/${CLICKHOUSE_DATABASE}_05320"
rm -rf "${ICEBERG_DIR}"

# Two inserts: the files of the second one have no row with `k < 3000`, so their row groups are
# recorded in the query condition cache as not matching it.
${CLICKHOUSE_CLIENT} --query "
    SET allow_experimental_insert_into_iceberg = 1;
    SET max_insert_threads = 1;
    CREATE TABLE t_05320 (p Int64, k Int64) ENGINE = IcebergLocal('${ICEBERG_DIR}/partitioned', 'Parquet')
    PARTITION BY (p);
    INSERT INTO t_05320 SELECT number % 3, number FROM numbers(3000);
    INSERT INTO t_05320 SELECT number % 3, number FROM numbers(3000, 27000);
"

# No Iceberg pruning by the manifests, which would skip the files of the second insert before the cache.
SETTINGS="use_iceberg_partition_pruning = 0, use_query_condition_cache = 1, optimize_move_to_prewhere = 1, query_plan_optimize_prewhere = 1,
    use_top_k_dynamic_filtering = 1, query_plan_max_limit_for_top_k_optimization = 1000,
    input_format_parquet_use_native_reader_v3 = 1, input_format_parquet_filter_push_down = 1,
    max_threads = 1, max_parsing_threads = 1"

${CLICKHOUSE_CLIENT} --log_comment "05320_write" --query "
    SELECT p, k FROM t_05320 WHERE k < 3000 ORDER BY p DESC, k LIMIT 3 SETTINGS ${SETTINGS}"
${CLICKHOUSE_CLIENT} --log_comment "05320_plain" --query "
    SELECT count() FROM t_05320 WHERE k < 3000 SETTINGS ${SETTINGS}"
${CLICKHOUSE_CLIENT} --log_comment "05320_top_k_off" --query "
    SELECT p, k FROM t_05320 WHERE k < 3000 ORDER BY p, k DESC LIMIT 3 SETTINGS ${SETTINGS}, use_query_condition_cache_for_top_k = 0"

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS query_log"
${CLICKHOUSE_CLIENT} --query "
    SELECT log_comment, ProfileEvents['QueryConditionCacheHits'] > 0
    FROM system.query_log
    WHERE current_database = currentDatabase() AND event_date >= yesterday() AND type = 'QueryFinish'
        AND log_comment IN ('05320_plain', '05320_top_k_off')
    ORDER BY log_comment"

${CLICKHOUSE_CLIENT} --query "DROP TABLE t_05320"
rm -rf "${ICEBERG_DIR}"
