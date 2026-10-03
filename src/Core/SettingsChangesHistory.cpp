#include <Core/SettingsChangesHistory.h>

#include <Common/Exception.h>

#include <optional>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void addSettingChangesHistory(
    VersionToSettingsChangesMap & history,
    std::string_view setting_name,
    std::initializer_list<SettingsChangesHistory::SettingChangeRecord> records)
{
    std::optional<ClickHouseVersion> newer_version;
    for (const auto & record : records)
    {
        // clang-format off
        /// History of settings changes that controls some backward incompatible changes
        /// across all ClickHouse versions. It maps ClickHouse version to settings changes that were done
        /// in this version. This history contains both changes to existing settings and newly added settings.
        /// Settings changes is a vector of structs
        ///     {setting_name, previous_value, new_value, reason}.
        /// For newly added setting choose the most appropriate previous_value (for example, if new setting
        /// controls new feature and it's 'true' by default, use 'false' as previous_value).
        /// It's used to implement `compatibility` setting (see https://github.com/ClickHouse/ClickHouse/issues/35972)
        /// Note: please check if the key already exists to prevent duplicate entries.
        addSettingsChanges(settings_changes_history, "26.9",
        {
            {"ast_fuzzer_oracle", false, false, "New setting to enable correctness oracle checks in the server-side AST fuzzer."},
            {"enable_hash_join_row_store", false, true, "New setting to enable transforming the payload of a hash join into a row-major layout."},
            {"min_rows_ratio_for_hash_join_row_store", 5.0, 5.0, "New setting to control the minimum estimated ratio of join output rows to build-side rows to enable transforming hash join payload to row-major. 0 means the transformation is always allowed."},
            {"enable_sharding_aggregator", false, false, "Obsolete setting, the sharded aggregator has been removed in favor of the adaptive aggregator (`enable_adaptive_aggregator`)."},
            {"query_plan_fuse_filter_into_array_join", false, true, "New optimization to fuse a filter on ARRAY JOINed columns into the ARRAY JOIN step, enabled by default."},
            {"adaptive_aggregator_freeze_threshold_bytes", 4194304, 4194304, "New setting bounding the adaptive aggregator's frozen local tables in bytes, whichever of it and the key-count threshold is reached first; 0 disables the byte bound."},
            {"allow_experimental_ai_functions", false, false, "The setting is obsolete, AI functions are beta now and enabled by default."},
            {"ai_function_max_retries", 0, 1, "Retry a transient API error once by default, so a single 429 or 5xx from the provider does not fail the query."},
            {"query_plan_aggregation_bucket_top_k", false, true, "New setting to toggle the plan optimization that materializes only each two-level bucket's best n groups when a final aggregation feeds ORDER BY over its outputs with LIMIT n and the per-bucket selection is provably exact."},
            {"distributed_cache_client_id", "", "", "New setting (CI tests only) to override the distributed cache client id per query."},
            {"read_through_distributed_cache", false, false, "The setting moved to the server configuration and is ignored as a profile setting: reading from the distributed cache is now switched by the server setting `enable_read_through_distributed_cache`, which is applied without a restart (so that merges, mutations and `Buffer` flushes follow it too, instead of being pinned to the value the server started with). Use `force_read_through_distributed_cache` to deviate from the server setting per query."},
            {"write_through_distributed_cache", false, false, "The setting moved to the server configuration and is ignored as a profile setting: writing to the distributed cache is now switched by the server setting `enable_write_through_distributed_cache`, which is applied without a restart (so that merges, mutations and `Buffer` flushes follow it too, instead of being pinned to the value the server started with). Use `force_write_through_distributed_cache` to deviate from the server setting per query."},
            {"force_read_through_distributed_cache", "auto", "auto", "New setting overriding the server setting `enable_read_through_distributed_cache` for a single query."},
            {"force_write_through_distributed_cache", "auto", "auto", "New setting overriding the server setting `enable_write_through_distributed_cache` for a single query."},
            {"distributed_cache_min_inflight_bytes_to_discard_connection_on_seek", 0, 4 * 1024 * 1024, "New setting to drop and reopen a distributed cache connection on a seek when too many in-flight bytes would otherwise be discarded. Defaults to 4 MiB; 0 restores the previous behavior (always reuse the connection via the read range id)."},
            {"distributed_plan_workers_provisioning_timeout_ms", 0, 10000, "New setting bounding how long a query waits for leased stateless workers to become reachable before execution; `compatibility` below 26.9 restores the previous no-wait behavior."},
            {"query_plan_optimize_lazy_materialization_for_object_storage", false, true, "New setting to use lazy materialization for `ORDER BY ... LIMIT n` queries reading Parquet files from object storage (including Iceberg tables)."},
            {"iceberg_compaction_commit_batch_size", 100, 100, "New setting"},
            {"iceberg_compaction_max_rows_in_data_file", std::numeric_limits<UInt64>::max(), std::numeric_limits<UInt64>::max(), "New setting for the max rows of an iceberg data file produced by compaction, separate from the insert-time limit."},
            {"iceberg_compaction_max_bytes_in_data_file", std::numeric_limits<UInt64>::max(), std::numeric_limits<UInt64>::max(), "New setting for the max bytes of an iceberg data file produced by compaction, separate from the insert-time limit."},
            {"optimize_mutations_with_partition_pruning", false, true, "New setting to automatically prune partitions for mutations based on WHERE clause"},
        });
        addSettingsChanges(settings_changes_history, "26.8",
        {
            {"enable_group_by_top_k_optimization", false, true, "New setting to control the TopK filtering optimization during aggregation in `GROUP BY key ORDER BY key LIMIT N` queries."},
            {"group_by_top_k_optimization_observation_rows", 65536, 65536, "New experimental setting: rows each aggregation stream observes before declaring a full top-K heap that never rejected anything pure overhead and freezing it."},
            {"time_series_prefer_recent_samples_table", true, true, "New setting to read from the recent samples table of a TimeSeries table when the requested time range fits in its TTL window."},
            {"query_plan_push_down_volume_reducing_functions", false, true, "New setting to push volume-reducing functions (`length`, `lengthUTF8`, `empty`, `notEmpty`) below `Sorting` and `Filter` steps, so the wide argument column is replaced by the fixed-size result. previous_value=false so `compatibility` with versions before 26.8 restores the pre-existing behavior (no push down)."},
            {"enable_alp_codec", false, false, "New setting to enable the experimental `ALP` compression codec individually, without the `allow_experimental_codecs`."},
            {"enable_quantized_codec", false, false, "New setting to enable the experimental `Quantized` compression codec individually, without the `allow_experimental_codecs`."},
            {"enable_sz3_codec", false, false, "New setting to enable the experimental `SZ3` compression codec individually, without the `allow_experimental_codecs`."},
            {"enable_zxc_codec", false, false, "New setting to enable the experimental `ZXC` compression codec individually, without the `allow_experimental_codecs`."},
            {"resumable_backup_from_snapshot", false, false, "New experimental setting to enable resumable `BACKUP FROM SNAPSHOT`."},
            {"framing_output_format", "None", "None", "New setting to select a framing format that multiplexes data, totals, extremes, progress, logs, and profile events packets in a single output stream over HTTP."},
            {"output_format_image_time_multiplier_seconds", 1, 1, "New setting controlling the numerator of the time unit of the `t` column, which makes image output formats such as `PNG` produce an animation."},
            {"output_format_image_time_divisor_seconds", 60, 60, "New setting controlling the denominator of the time unit of the `t` column, which makes image output formats such as `PNG` produce an animation."},
            {"output_format_image_streaming_animation", false, false, "New setting controlling whether image output formats such as `PNG` write each frame of an animation as soon as the next value of `t` is seen, instead of buffering all the frames in memory."},
            {"reader_executor_window_size", 4194304, 4194304, "New experimental ReaderExecutor setting: bytes served per read window."},
            {"reader_executor_block_size", 1048576, 1048576, "New experimental ReaderExecutor setting: buffer chunk size for source reads."},
            {"distributed_cache_client_id", "", "", "New setting (CI tests only) to override the distributed cache client id per query."},
            {"read_through_distributed_cache", false, false, "The setting moved to the server configuration and is ignored as a profile setting: reading from the distributed cache is now switched by the server setting `enable_read_through_distributed_cache`, which is applied without a restart (so that merges, mutations and `Buffer` flushes follow it too, instead of being pinned to the value the server started with). Use `force_read_through_distributed_cache` to deviate from the server setting per query."},
            {"write_through_distributed_cache", false, false, "The setting moved to the server configuration and is ignored as a profile setting: writing to the distributed cache is now switched by the server setting `enable_write_through_distributed_cache`, which is applied without a restart (so that merges, mutations and `Buffer` flushes follow it too, instead of being pinned to the value the server started with). Use `force_write_through_distributed_cache` to deviate from the server setting per query."},
            {"force_read_through_distributed_cache", "auto", "auto", "New setting overriding the server setting `enable_read_through_distributed_cache` for a single query."},
            {"force_write_through_distributed_cache", "auto", "auto", "New setting overriding the server setting `enable_write_through_distributed_cache` for a single query."},
            {"input_format_json_max_string_column_growth_step", 0, 0, "New setting to cap the power-of-two growth of the JSON column's internal String buffers while materializing JSON, bounding over-allocation."},
            {"max_insert_threads", 1, 0, "Changed the default from 1 (no parallel execution) to auto (0), which resolves to the number of CPU cores available to the server, reduced under memory pressure via `max_insert_threads_min_free_memory_per_thread`. This parallelizes `INSERT SELECT` by default. Set to 1 to restore the previous single-threaded behavior."},
            {"join_algorithm", "direct,parallel_hash,hash", "direct,parallel_hash,hash,ie_join", "Appended `ie_join` to the default list, so a join whose `ON` section has only inequality conditions is executed with IEJoin instead of a `CROSS JOIN` with a filter. Being last, it is used only when the other algorithms do not apply."},
            {"unique_key_probe_implementation", "auto", "auto", "New setting: selects the UNIQUE KEY probe implementation (currently only the simple baseline exists)"},
            {"statistics_max_set_size_for_exact_selectivity_estimation", 0, 10000, "New setting to bound the cost of estimating the selectivity of `IN` with a large set: above the limit the estimator uses the size of the set and its bounding range instead of the exact ranges. Before 26.8 the estimation was uncapped, so the previous value is 0 (no limit) and `compatibility` with an earlier version restores the exact ranges for sets of any size."},
            {"enable_adaptive_aggregator", false, true, "New setting to enable adaptive `GROUP BY` aggregation that freezes each thread's local hash table once it reaches `adaptive_aggregator_freeze_threshold` keys, so frequent keys keep aggregating in the small local tables while rare keys are routed by their hash into per-bucket backlogs and aggregated exactly once, inside the bucket-parallel merge; this stores and processes rare keys once instead of once per thread."},
            {"adaptive_aggregator_freeze_threshold", 16384, 16384, "New setting to set the number of keys at which the adaptive aggregator (`enable_adaptive_aggregator`) freezes a thread's local hash table."},
            {"use_indexes_refiner_in_read_pools", false, false, "New setting to drop mark ranges fully filtered out by skip indexes or a projection index before read tasks are created in MergeTree read pools."},
            {"s3_base", "", "", "New setting to specify the base URL for resolving relative URLs in the s3 table function and the S3 table engine."},
            {"use_query_condition_cache_for_top_k", false, true, "New setting to gate the query condition cache for `ORDER BY ... LIMIT n` (TopK) reads; enabled by default. The gate is disabled in 26.7, so `compatibility` with an earlier version keeps it off."},
            {"use_projection_index_in_read_pools", false, false, "Obsolete setting, renamed to `use_indexes_refiner_in_read_pools`."},
            {"allow_distinct_partitions_independently", false, true, "New setting to enable independent per-partition evaluation of `DISTINCT` when the partition expression is a deterministic function of the `DISTINCT` columns."},
            {"force_distinct_partitions_independently", false, false, "New setting to force independent per-partition evaluation of `DISTINCT` even when the cost heuristic would skip it."},
            {"max_number_of_partitions_for_independent_distinct", 128, 128, "New setting: maximal number of partitions to apply independent per-partition `DISTINCT`."},
            {"allow_window_partitions_independently", false, true, "New setting to evaluate window functions per partition independently (skipping the hash scatter) when the partition expression is a deterministic function of the window `PARTITION BY` columns."},
            {"force_window_partitions_independently", false, false, "New setting to force independent per-partition evaluation of window functions even when the cost heuristic would skip it."},
            {"max_number_of_partitions_for_independent_window", 128, 128, "New setting: maximal number of partitions to apply independent per-partition evaluation of window functions."},
            {"allow_creating_set_partitions_independently", false, true, "New setting to enable parallel per-partition pre-deduplication of the subquery result when building the set for `IN (subquery)`, when the partition expression is a deterministic function of the subquery output columns."},
            {"force_creating_set_partitions_independently", false, false, "New setting to force per-partition pre-deduplication for `IN (subquery)` set building even when the partition-skew check would skip it."},
            {"allow_lossy_numeric_supertype", false, false, "New setting that lets if/multiIf/coalesce/ifNull/array/map resolve all-numeric branches with no lossless common type (e.g. Decimal + Float64) to a numeric supertype (Float64, with possible precision loss), so the result can be aggregated. Independent of use_variant_as_common_type: with it off such branches previously raised NO_COMMON_TYPE, with it on they became a Variant; either way they now resolve to Float64."},
            {"explain_syntax_single_record", false, true, "From 26.8, `EXPLAIN SYNTAX` returns the reformatted query as a single record (with embedded newlines) instead of one record per line. Set this to `false` to restore the pre-26.8 one-record-per-line output."},
            {"query_plan_optimize_count_from_text_index", false, true, "New setting"},
            {"materialize_statistics_on_insert", false, true, "Materialize column statistics on INSERT by default for tables below `materialize_statistics_on_insert_max_table_size`, so cost-based join reordering has good estimates on freshly-loaded dimension tables."},
            {"materialize_statistics_on_insert_max_table_size", 0, 26843545600, "New setting."},
            {"throw_on_hive_partitioning_resolution_failure", false, true, "New setting to fail the query when Hive-style partitioning detection for an object storage table cannot list the storage, instead of running without the Hive partition columns."},
            {"enable_json_ast_dialect", false, false, "New setting to enable the `clickhouse_json` value of the `dialect` setting, which interprets queries as JSON ASTs (the output of `parseQueryToJSON`) instead of SQL text."},
            {"allow_metadata_only_named_tuple_alter", false, false, "New setting to control metadata-only ALTER for named Tuple subfield additions."},
            {"analyzer_compatibility_apply_final_to_all_joined_tables", false, false, "New setting on master (default false = the fixed behavior). The behavior flip itself is recorded under 26.6, and the introduction for backports to older release branches (with default true) under 26.4."},
            {"enable_parallel_single_level_merge", false, true, "New setting to parallelize the final merge of the single-level aggregation hash tables by splitting the key space into disjoint hash partitions that the threads merge independently."},
            {"ai_function_text_default_credentials", "", "", "New setting"},
            {"ai_function_embedding_default_credentials", "", "", "New setting"},
            {"shrink_over_allocated_columns_min_waste_ratio", 1.0, 1.0, "New setting to shrink over-allocated columns to fit on INSERT to reduce peak memory usage. Disabled by default (1.0)."},
            {"shrink_over_allocated_columns_min_waste_bytes", 16 * 1024 * 1024, 16 * 1024 * 1024, "New setting: minimum absolute wasted memory in a column for it to be shrunk to fit on INSERT."},
            {"ai_function_allow_insecure_endpoint", true, false, "AI functions now reject insecure (http) endpoints to remote hosts by default."},
            {"ai_function_max_api_calls_per_query", 0, 1000, "Bound outbound AI function HTTP calls per query by default (previously 0 - unlimited)."},
            {"join_runtime_filter_min_probe_rows", 0, 1000, "New setting to control minimum probe side size for installing JOIN runtime filters. It wasn't limited before, so previous value is 0 meaning always install."},
            {"read_in_order_use_virtual_row", false, true, "Enable the virtual row optimization by default. When reading in order of the primary key over many parts, it lets `MergingSortedTransform` reprioritize sources using primary key values from the sparse index, so parts that are not relevant for the query are not read, plus a bounded read-ahead window of at most `max_threads` parts that keeps reads parallel. This significantly reduces peak memory consumption (see https://github.com/ClickHouse/ClickHouse/issues/52624)."},
            {"page", 0., 0., "New setting for paginated HTTP responses, equivalent to offset = limit * (page - 1). Float so it can hold negative or fractional values (passed through to SQL `LIMIT`/`OFFSET`)."},
            {"limit", 0., 0., "Type widened from UInt64 to Float to support negative and fractional values, passed through to ClickHouse's native negative/fractional `LIMIT` support."},
            {"offset", 0., 0., "Type widened from UInt64 to Float to support negative and fractional values, passed through to ClickHouse's native negative/fractional `OFFSET` support."},
            {"select", "", "", "New setting to wrap a query in `SELECT <expr_list> FROM (<query>)`."},
            {"order", "", "", "New setting to add an ORDER BY clause around a query."},
            {"sort", "", "", "New setting to add a simple ORDER BY clause around a query."},
            {"filter", "", "", "New setting to add a WHERE clause around a query."},
            {"database", "", "", "New setting that specifies the current database for the query. Replaces the special handling of the HTTP `database` URL parameter and `X-ClickHouse-Database` header."},
            {"default_format", "", "", "New setting for the default format when the query has no FORMAT clause."},
            {"format", "", "", "New setting to override the FORMAT of the query for both input and output."},
            {"input_format", "", "", "New setting to override the input format of the query."},
            {"output_format", "", "", "New setting to override the output format of the query."},
            {"compression", "", "", "New setting to apply generic compression to the response body."},
            {"http_allow_database_as_path", false, false, "New setting to recognize a database name in the URL path of HTTP requests."},
            {"http_allow_table_as_file", false, false, "New setting to recognize a table name in the URL path of HTTP requests, with optional format/compression extensions."},
            {"http_allow_filters_as_path", false, false, "New setting to recognize hive-style `name=value` filters in the URL path of HTTP requests."},
            {"http_allow_filters_as_unrecognized_url_parameters", false, false, "New setting to treat unrecognized URL parameters as filter expressions in HTTP requests."},
            {"ignore_on_cluster_for_replicated_handler_queries", false, false, "New setting to ignore the ON CLUSTER clause for handler management queries when handlers are backed by replicated (Keeper) storage."},
            {"materialized_views_populate_atomically", false, true, "New setting that makes plain `CREATE MATERIALIZED VIEW ... POPULATE` locally atomic: existing data is snapshotted and the view is subscribed to new inserts together, under a brief exclusive lock on the source, so rows inserted through the same server are neither missed nor duplicated. The guarantee covers the local insert path only - inserts arriving on another replica or through a distributed write are outside the cut - and it requires a source that can provide a pinned snapshot (the `MergeTree` family and `Memory`); other sources, as well as `CREATE OR REPLACE` / `REPLACE`, keep the legacy non-atomic population. Set to `false` for the legacy non-atomic behavior everywhere."},
            {"input_format_json_max_object_size", 512 * 1024 * 1024, 512 * 1024 * 1024, "New setting to limit the maximum size of a single JSON object in bytes"},
            {"format_hive_text_rows_delimiter", "\n", "\n", "New setting for the delimiter at the end of each row in the HiveText output format."},
            {"output_format_parquet_wide_integer_as_decimal", false, false, "New setting to write `Int128`, `UInt128`, `Int256`, and `UInt256` values as standard Parquet `DECIMAL` values. The previous value keeps the legacy little-endian `FIXED_LEN_BYTE_ARRAY` representation."},
            {"optimize_trivial_count_with_sparsity_filter", false, true, "Promote to BETA and enable by default: serve `SELECT count() FROM t WHERE <pred>` from the persisted per-column `num_defaults` / `num_rows` counters when `<pred>` partitions rows into defaults vs non-defaults. Requires the MergeTree setting `compute_exact_num_defaults_for_sparse_columns` (also enabled by default now)."},
            {"input_format_parquet_dictionary_filter_push_down", 0, 1024 * 1024, "New setting enabling Parquet row-group pruning based on dictionary page contents (reader v3). The value is the maximum dictionary page size in bytes for which the optimization applies; 0 (the previous behavior) disables it."},
            {"input_format_read_datetime_number_as_raw_value", true, false, "From 26.8, an unquoted number for a `DateTime`/`DateTime64` column in the `JSON` and `Values`/`Quoted` paths (and in `JSONExtract` and typed `JSON`) is a Unix timestamp in seconds, consistent with the `Values` format, `CAST` and `toDateTime64`. Set this to `true` (or `SET compatibility = '26.7'`) to restore the pre-26.8 behavior, where a bare unquoted integer fed to a `DateTime64` column was read as the raw scaled value (ticks). The tab-separated, CSV and other escaped/whole-text formats are not governed by this setting."},
            {"query_plan_short_circuit_constant_false_join", false, true, "New setting to short-circuit a JOIN with a constant-false ON condition so the non-contributing side is not read. previous_value=false so `compatibility` with versions before 26.8 restores the pre-existing behavior (no short-circuit)."},
            {"query_plan_optimize_lazy_materialization_for_object_storage", false, true, "New setting to use lazy materialization for `ORDER BY ... LIMIT n` queries reading Parquet files from object storage (including Iceberg tables)."},
            {"query_plan_optimize_lazy_materialization_for_file", false, true, "New setting to use lazy materialization for `ORDER BY ... LIMIT n` queries reading local Parquet files with the `file` table function and the `File` table engine."},
            {"enable_packed_string_keys_in_aggregation", false, true, "New setting to toggle the `PackedStringRef`-based hash table for single-`String`-key GROUP BY. previous_value=false so `compatibility` with versions before 26.8 restores the legacy `StringHashTable`-based method, including its two-level bucketing."},
            {"allow_delta_kernel_rs", true, true, "New name of `allow_experimental_delta_kernel_rs`, which is no longer experimental and is kept as an alias. The default is unchanged."},
            {"input_format_arrow_use_native_reader", true, true, "Obsolete setting, the native ClickHouse reader is now always used for the `Arrow` and `ArrowStream` formats (the Apache Arrow library-based reader has been removed)."},
            {"output_format_arrow_use_native_writer", true, true, "Obsolete setting, the native ClickHouse writer is now always used for the `Arrow` and `ArrowStream` formats (the Apache Arrow library-based writer has been removed)."},
            {"distributed_cache_min_inflight_bytes_to_discard_connection_on_seek", 0, 4 * 1024 * 1024, "New setting to drop and reopen a distributed cache connection on a seek when too many in-flight bytes would otherwise be discarded. Defaults to 4 MiB; 0 restores the previous behavior (always reuse the connection via the read range id)."},
            {"iceberg_delete_manifest_decode_concurrency", 2, 4, "New setting bounding how many Iceberg delete manifest files are decoded concurrently before the first row is read. Before 26.8 one manifest was decoded at a time with the next one's fetch already in flight, so `2` is the closest equivalent of the previous behavior, which put the sum of their object storage round-trips on the critical path before the first row."},
            {"run_query_in_background", false, false, "New setting to run a query in the background, detached from the connection that submitted it, discarding the result."},
            {"enable_cascades_optimizer", false, false, "New experimental setting."},
            {"merge_tree_min_bytes_per_read_stream", 0, (64 * 1024), "New setting to cap the number of streams for ordinary local unordered `MergeTree` narrow-column scans using a sqrt cost model, reducing per-stream overhead on high-core-count machines. previous_value=0 (disabled) so `compatibility` with versions before 26.8 restores the pre-existing stream count."},
            {"analyzer_compatibility_multiple_joins_qualify_column_names", false, false, "New compatibility setting. When enabled, the analyzer mimics the old analyzer's qualified result column names for queries whose FROM clause has two or more JOINs."},
            {"input_format_parquet_spatial_filter_push_down", false, true, "New setting: skip GeoParquet row groups and pages based on spatial predicates and bounding box statistics"},
            {"use_text_index_negative_tokens_cache", false, true, "New setting to cache absent text index tokens and avoid repeated dictionary lookups."},
            {"optimize_trivial_view_pushdown_to_distributed", false, true, "New setting to push the full outer query to shards for trivial views over Distributed tables."},
            {"filesystem_cache_wait_for_concurrent_download_timeout_milliseconds", 60000, 1000, "New setting to bound how long a read waits for a file segment being downloaded to the filesystem cache by a concurrent query; on timeout the read bypasses the cache instead of waiting indefinitely. The previous value 60000 corresponds to the old behavior (one full 60 s wait cycle on the downloader)."},
            {"text_index_posting_list_apply_mode", "materialize", "lazy", "Text index queries now decode posting lists on demand with a cursor instead of materializing them into Roaring Bitmaps, which reduces memory usage and CPU time for selective queries."},
            {"filesystem_cache_verbose_logging", false, false, "New setting gating the per-buffer-refill TEST-level log messages of the filesystem cache read buffer, which were previously emitted unconditionally once the log level allowed them."},
            {"enable_function_early_short_circuit", false, false, "New setting"},
            {"merge_tree_prefetch_json_shared_data_substreams", true, true, "New setting to control prefetching of JSON shared data substreams that are read by seeking to a mark in Wide parts."},
            {"iceberg_compaction_commit_batch_size", 100, 100, "New setting"},
        });
        addSettingsChanges(settings_changes_history, "26.7",
        {
            {"analyzer_compatibility_allow_non_aggregate_in_having", false, false, "New compatibility setting. When enabled, the analyzer mimics the legacy `HAVING`-to-`WHERE` rewrite for non-aggregate AND-conjuncts instead of raising `NOT_AN_AGGREGATE`."},
            {"query_plan_optimize_lazy_materialization_for_object_storage", false, true, "New setting to use lazy materialization for `ORDER BY ... LIMIT n` queries reading Parquet files from object storage (including Iceberg tables)."},
            {"iceberg_compaction_max_rows_in_data_file", std::numeric_limits<UInt64>::max(), std::numeric_limits<UInt64>::max(), "New setting for the max rows of an iceberg data file produced by compaction, separate from the insert-time limit."},
            {"iceberg_compaction_max_bytes_in_data_file", std::numeric_limits<UInt64>::max(), std::numeric_limits<UInt64>::max(), "New setting for the max bytes of an iceberg data file produced by compaction, separate from the insert-time limit."},
            {"dictionary_lazy_load", "auto", "auto", "New setting overriding the server setting `dictionaries_lazy_load` for an individual dictionary."},
            {"discard_query_data", false, false, "New setting to skip sending query result rows to the client over the native TCP protocol."},
            {"optimize_trivial_count_with_sparsity_filter", false, false, "New (experimental) setting to serve `SELECT count() FROM t WHERE <pred>` from per-column `num_defaults` / `num_rows` recorded in `serialization.json` when `<pred>` partitions rows into defaults vs non-defaults."},
            {"merge_tree_generic_exclusion_search_max_steps", 0, 0, "New setting to limit the number of steps of the generic exclusion search over the primary key index."},
            {"use_streaming_marks_compression", false, false, "New setting to compress marks into in-memory representation one block at a time (streaming) instead of materializing the full plain marks array, reducing peak memory during marks loading for compact parts with many substreams."},
            {"s3_validate_etag_on_read", false, true, "New setting to detect concurrent in-place overwrites of S3/GCS objects during a read by validating the GET response ETag against the listed one. previous_value=false so `compatibility` with versions before 26.7 restores the pre-existing behavior (no validation)."},
            {"dead_blobs_to_delay_insert", 0, 0, "New setting to override the `MergeTree` setting with the same name per query."},
            {"dead_blobs_to_throw_insert", 0, 0, "New setting to override the `MergeTree` setting with the same name per query."},
            {"input_format_csv_missing_nullable_as_empty_string", false, false, "New setting to read a missing value of `Nullable(String)` from CSV as an empty string instead of NULL."},
            {"use_legacy_to_time", true, false, "Use the new `toTime` function (converting values to the `Time` data type) by default instead of the legacy `toTime` (which is still available as `toTimeWithFixedDate`)."},
            {"reserve_memory", 0, 0, "New setting to reserve memory for specific workload before starting a query."},
            {"parallel_replicas_plan_based", false, false, "New setting"},
            {"use_paimon_metadata_files_cache", false, false, "New setting to enable in-memory caching of parsed Paimon metadata files (manifest lists and manifests). For persistent Paimon table engines it must be enabled before metadata initialization; table functions evaluate it per query. Avoids repeated downloads and deserialization of metadata files from object storage on subsequent queries."},
            {"optimize_or_like_chain", false, true, "Enable by default: optimize OR chains of LIKE/ILIKE/match into multiSearchAny (pure-substring patterns) or multiMatchAny (other patterns, when Hyperscan/Vectorscan is permitted); when neither fast path applies the original OR chain is kept unchanged."},
            {"optimize_or_like_chain_min_patterns", 0, 10, "New setting controlling the minimum number of non-pure-substring LIKE/ILIKE/match branches (sharing the same LHS expression) required for optimize_or_like_chain to rewrite a chain into multiMatchAny. Shorter chains are kept as-is because the multiMatchAny (Hyperscan) rewrite only becomes faster than short-circuit OR evaluation from about nine branches."},
            {"optimize_or_like_chain_min_substrings", 0, 4, "New setting controlling the minimum number of pure-substring (%needle%) LIKE/ILIKE branches (sharing the same LHS expression) required for optimize_or_like_chain to rewrite a chain into multiSearchAny."},
            {"input_format_arrow_use_native_reader", false, true, "New setting to use the native ClickHouse reader for the Arrow and ArrowStream formats instead of the Apache Arrow library."},
            {"input_format_orc_use_fast_decoder", true, true, "Obsolete setting, the native ClickHouse ORC decoder is now always used (the Apache Arrow-based ORC reader has been removed)."},
            {"output_format_arrow_use_native_writer", false, true, "New setting to use the native ClickHouse writer for the Arrow and ArrowStream formats instead of the Apache Arrow library."},
            {"allow_minmax_index_for_json", true, false, "Forbid creating minmax skip index on JSON columns by default because the index serialization cannot handle heterogeneous Field values"},
            {"s3_allow_server_credentials_in_user_queries", true, false, "New setting to block S3 access from user SQL from resolving the server's own ambient credentials (environment/IMDS/IRSA/instance-profile/AWS-config-file/GCP-OAuth-metadata). Explicit role_arn-based STS assume-role is still allowed. The previous behavior (allowed) is restored with compatibility settings."},
            {"query_plan_merge_expression_into_join", false, true, "New setting. Allow to merge Expression step into JOIN step during join reordering optimization."},
            {"skip_unavailable_shards_mode", "unavailable_or_table_missing", "unavailable_or_table_missing", "New setting to control which exceptions from a remote shard are ignored when `skip_unavailable_shards` is enabled. The default matches the historical behavior: a shard whose table is missing is treated as unavailable."},
            {"use_text_index_tokens_cache", false, true, "Enabled the text index tokens cache globally."},
            {"use_text_index_header_cache", false, true, "Enabled the text index header cache globally."},
            {"optimize_aggregation_in_order_limit", false, true, "New setting to push the `LIMIT` into aggregation-in-order for early termination when the `ORDER BY` is a prefix of the `GROUP BY` sort description."},
            {"explain_query_plan_default", "legacy", "pretty", "From 26.7, `EXPLAIN PLAN` defaults to `actions=1, compact=1, pretty=1`. Set this to `legacy` to restore the pre-26.7 output."},
            {"format_geojson_validate_geometry", true, true, "New setting that controls whether the GeoJSON format enforces RFC 7946 geometry validity (minimum points per line and ring, ring closure, non-empty multi-geometries) when reading and writing"},
            {"use_partition_minmax_for_primary_key_pruning", false, true, "New setting to use the part's partition minmax to prune more granules during primary key analysis for `MergeTree` tables, when a primary key column is also an input column of the partition key."},
            {"allow_delta_lake_writes", false, false, "Added an alias for setting `allow_experimental_delta_lake_writes`, which was moved to Beta."},
            {"allow_experimental_delta_lake_writes", false, false, "Delta Lake writes were moved to Beta."},
            {"optimize_redundant_comparisons", false, true, "New setting to detect conflicting and redundant comparison conditions on the same expression within AND chains."},
            {"mysql_datatypes_support_level", "decimal,datetime64,date2Date32", "decimal,datetime64,date2Date32,geometry", "Map MySQL's concrete spatial types (LINESTRING, POLYGON, MULTILINESTRING, MULTIPOLYGON, MULTIPOINT) and the generic GEOMETRY type to the corresponding ClickHouse geometric types by default. The generic GEOMETRY column maps to the umbrella Geometry type; reading a value whose subtype has no ClickHouse counterpart (GEOMETRYCOLLECTION) throws at read time."},
            {"snappy_mode", "basic", "basic", "New setting to control the wire format used for snappy compression in generic file/URL I/O. The default `basic` preserves backward-compatible Hadoop snappy block format reads; HTTP `Content-Encoding: snappy` always uses the framing format independently of this setting."},
            {"compile_regular_expressions", false, true, "New setting to enable JIT compilation of simple regular expressions in functions like `match` and `extract`."},
            {"min_count_to_compile_regular_expression", 3, 3, "New setting controlling how many times a regular expression must be used before it is JIT-compiled."},
            {"allow_aggregate_partitions_independently", false, true, "Enable independent per-partition aggregation by default when the partition key suits the GROUP BY key. The existing runtime heuristics in `ReadFromMergeTree::requestOutputEachPartitionThroughSeparatePortForAggregation` already skip the optimization when the partition layout is unfavorable (too few partitions, too many partitions, or significantly skewed partition sizes), so enabling the setting is safe in the cases where it would otherwise be a no-op."},
            {"text_index_lazy_intersection_density_threshold", 0.2, 0.2, "Renamed from `text_index_density_threshold` (kept as an alias); selects the posting list intersection algorithm in lazy posting list apply mode."},
            {"allow_experimental_text_index_lazy_apply", false, true, "Lazy posting list apply mode for the text index is no longer experimental; the setting is now obsolete and has no effect (lazy mode is selected via `text_index_posting_list_apply_mode = 'lazy'`)."},
            {"allow_experimental_url_wildcard_from_index_pages", false, false, "New setting to enable expanding wildcards in the `url` table function by listing HTTP index pages."},
            {"url_wildcard_max_directories_to_read", 100000, 100000, "New setting to limit the number of directories read when expanding wildcards in the `url` table function."},
            {"allow_experimental_eval_table_function", false, false, "New setting to enable the experimental table function `eval`."},
            {"output_format_csv_header_serialize_tuple_into_separate_columns", false, true, "New setting. When output_format_csv_serialize_tuple_into_separate_columns is enabled, the CSVWithNames/CSVWithNamesAndTypes header now flattens Tuple columns into their leaf fields so the header width matches the data. Set to false to restore the previous single-name header."},
            {"enable_join_runtime_filters_index_analysis", false, false, "New setting to enable join filtering using dynamic index analysis"},
            {"vector_search_use_quantized_codes", false, false, "New setting to opt into the two-stage approximate vector-search optimization over a Quantize(...) column codec; queries stay exact by default."},
            {"reader_executor_use_long_connections", false, false, "New experimental ReaderExecutor setting (off by default): reuse a held source connection across sequential windows."},
            {"reader_executor_min_bytes_for_seek", 2097152, 2097152, "New experimental ReaderExecutor setting: forward-gap bound for bridging on a held source connection."},
            {"reader_executor_max_tail_for_drain", 1048576, 1048576, "New experimental ReaderExecutor setting: drain bound for completing a dropped long connection."},
            {"precise_float_parsing", false, true, "Use the precise (closest-representable) float parsing algorithm by default, now that it is faster than the previous fast algorithm. Set to false to restore the pre-26.7 fast-but-less-accurate parsing in conversion functions."},
            {"optimize_and_compare_chain_max_hash_work", 0, 5'000'000, "New setting that bounds the work of the `optimize_and_compare_chain` optimization (measured in query-tree nodes hashed) so it cannot dominate analysis of queries with very many or very large `AND`-chains of comparisons. The previous value `0` (unlimited) reproduces the pre-26.7 behavior where the optimization was uncapped, so `compatibility` set to an earlier version keeps deriving transitive predicates without a budget. Set to `0` to disable the budget."},
            {"distributed_cache_min_inflight_bytes_to_discard_connection_on_seek", 0, 4 * 1024 * 1024, "New setting to drop and reopen a distributed cache connection on a seek when too many in-flight bytes would otherwise be discarded. Defaults to 4 MiB; 0 restores the previous behavior (always reuse the connection via the read range id)."},
            {"iceberg_manifest_min_count_to_compact", 30, 30, "New setting to control manifest compaction for Iceberg tables."},
            {"show_remote_databases_in_system_tables", true, true, "New setting to control whether `MySQL` and `PostgreSQL` databases are shown in `system.tables`, `system.columns` and `system.completions`."},
            {"use_constant_folding_in_index_analysis", false, false, "New setting to fold partition-level constants into the filter predicate per part during MergeTree index analysis, improving pruning for filters whose branches depend on partition values."},
            {"join_runtime_filter_size_from_hash_table_stats", false, true, "Use hash table size statistics collected from previous executions to size the JOIN runtime filter. When disabled, fall back to the fixed `join_runtime_bloom_filter_bytes`."},
        });
        ClickHouseVersion version(record.version);
        if (newer_version && version >= *newer_version)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "The history of setting '{}' must list each version once, newest first, but '{}' follows '{}'",
                setting_name, version.toString(), newer_version->toString());
        newer_version = version;

        history[version].push_back(
            {String(setting_name), record.previous_value, record.new_value, String(record.reason), record.compatibility_mode});
    }
}

}
