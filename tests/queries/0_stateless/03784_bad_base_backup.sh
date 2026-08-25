#!/usr/bin/env bash

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A test for a race condition in backups.
# It does weird stuff, it's expected to have errors due to concurrently created files.
# But in no circumstances it can crash the server.

TIMEOUT=10

function thread()
{
  I=0
  while true
  do
    [[ $SECONDS -gt $TIMEOUT ]] && break
    I=$((I+1))

    #rm "${CLICKHOUSE_DATABASE}_${I}/backup0.tar.zst"

    $CLICKHOUSE_CLIENT --query="
    SET allow_suspicious_low_cardinality_types = 1;

    DROP DATABASE IF EXISTS d1_$CLICKHOUSE_DATABASE;
    DROP DATABASE IF EXISTS d2_$CLICKHOUSE_DATABASE;

    CREATE DATABASE d1_$CLICKHOUSE_DATABASE ENGINE = Replicated('/clickhouse/databases/d1_$CLICKHOUSE_DATABASE', '{shard}', '{replica}');
    CREATE DATABASE d2_$CLICKHOUSE_DATABASE ENGINE = Replicated('/clickhouse/databases/d2_$CLICKHOUSE_DATABASE', 's0', 'd0');

    CREATE TABLE d1_$CLICKHOUSE_DATABASE.\"t4\" (\"c0\" Array(UInt64), \"c1\" Nullable(UInt256), \"c2\" Nullable(UInt8), \"c3\" LowCardinality(Nullable(Time))) ENGINE = Set();

    CREATE TABLE d2_$CLICKHOUSE_DATABASE.\"t9\" (\"c0\" String COMMENT '😉', \"c1\" String, \"c2\" Nullable(UInt256), \"c3\" Nullable(JSON(max_dynamic_types=21))) ENGINE = DeltaLakeLocal('${CLICKHOUSE_USER_FILES_UNIQUE}_${I}', Parquet) SETTINGS iceberg_recent_metadata_file_by_last_updated_ms_field = 0;

    BACKUP DATABASE d2_$CLICKHOUSE_DATABASE TO File('${CLICKHOUSE_DATABASE}_${I}/backup0.tar.zst') SETTINGS query_plan_enable_optimizations = 1, max_network_bandwidth = 32768, hdfs_skip_empty_files = 0, format_binary_max_array_size = 8064, force_remove_data_recursively_on_drop = 1, input_format_orc_filter_push_down = 1, parallel_replicas_index_analysis_only_on_coordinator = 1, max_threads_for_indexes = 13, output_format_decimal_trailing_zeros = 0, remote_filesystem_read_prefetch = 0, output_format_parquet_geometadata = 1, optimize_extract_common_expressions = 0, merge_tree_min_rows_for_seek = 5313, max_bytes_ratio_before_external_group_by = 0.010000, optimize_respect_aliases = 0, use_skip_indexes = 0, join_to_sort_minimum_perkey_rows = 5969, parallel_replicas_for_cluster_engines = 0, rewrite_in_to_join = 0, merge_tree_min_bytes_for_seek = 0, parallel_replica_offset = 6, update_insert_deduplication_token_in_dependent_materialized_views = 0, distributed_aggregation_memory_efficient = 0, max_number_of_partitions_for_independent_aggregation = 4222, implicit_select = 0, max_result_bytes = 0, delta_lake_throw_on_engine_predicate_error = 0, apply_row_policy_after_final = 0, input_format_defaults_for_omitted_fields = 1, filesystem_cache_enable_background_download_during_fetch = 1, output_format_sql_insert_include_column_names = 1, input_format_tsv_detect_header = 1, asterisk_include_alias_columns = 0, show_create_query_identifier_quoting_rule = 'always', log_formatted_queries = 1, database_atomic_wait_for_drop_and_detach_synchronously = 0, hdfs_ignore_file_doesnt_exist = 1, cloud_mode = 1 ASYNC;

    BACKUP TABLE d1_$CLICKHOUSE_DATABASE.\"t4\" TO Memory('${CLICKHOUSE_DATABASE}_${I}/backup2.tar') SETTINGS base_backup = File('${CLICKHOUSE_DATABASE}_${I}/backup0.tar.zst'), async_insert_max_data_size = 5977266, check_table_dependencies = 1, input_format_custom_detect_header = 0, fsync_metadata = 0, group_by_use_nulls = 1 ASYNC FORMAT TabSeparatedRawWithNames;
    "
  done >/dev/null 2>&1
}

thread &
thread &
thread &
thread &

wait
