const SessionSettingsExplorer = () => {
  // Mintlify's production renderer evaluates the exported component without
  // preserving module-scope bindings. Lazy state keeps the generated data in
  // that evaluation scope while constructing it only once per mount.
  const [entries] = useState(() => [
    {
      label: "additional_*",
      count: 2,
      settings: [
        { name: "additional_result_filter", href: "/zh/reference/settings/session-settings/additional#additional_result_filter" },
        { name: "additional_table_filters", href: "/zh/reference/settings/session-settings/additional#additional_table_filters" }
      ],
      children: []
    },
    {
      label: "aggregate_*",
      count: 2,
      settings: [
        { name: "aggregate_function_input_format", href: "/zh/reference/settings/session-settings/aggregate#aggregate_function_input_format" },
        { name: "aggregate_functions_null_for_empty", href: "/zh/reference/settings/session-settings/aggregate#aggregate_functions_null_for_empty" }
      ],
      children: []
    },
    {
      label: "aggregation_*",
      count: 2,
      settings: [
        { name: "aggregation_in_order_max_block_bytes", href: "/zh/reference/settings/session-settings/aggregation#aggregation_in_order_max_block_bytes" },
        { name: "aggregation_memory_efficient_merge_threads", href: "/zh/reference/settings/session-settings/aggregation#aggregation_memory_efficient_merge_threads" }
      ],
      children: []
    },
    {
      label: "ai_function_*",
      count: 11,
      settings: [
        { name: "ai_function_embedding_default_credentials", href: "/zh/reference/settings/session-settings/ai-function#ai_function_embedding_default_credentials" },
        { name: "ai_function_embedding_max_batch_size", href: "/zh/reference/settings/session-settings/ai-function#ai_function_embedding_max_batch_size" },
        { name: "ai_function_max_api_calls_per_query", href: "/zh/reference/settings/session-settings/ai-function#ai_function_max_api_calls_per_query" },
        { name: "ai_function_max_input_tokens_per_query", href: "/zh/reference/settings/session-settings/ai-function#ai_function_max_input_tokens_per_query" },
        { name: "ai_function_max_output_tokens_per_query", href: "/zh/reference/settings/session-settings/ai-function#ai_function_max_output_tokens_per_query" },
        { name: "ai_function_max_retries", href: "/zh/reference/settings/session-settings/ai-function#ai_function_max_retries" },
        { name: "ai_function_request_timeout_sec", href: "/zh/reference/settings/session-settings/ai-function#ai_function_request_timeout_sec" },
        { name: "ai_function_retry_initial_delay_ms", href: "/zh/reference/settings/session-settings/ai-function#ai_function_retry_initial_delay_ms" },
        { name: "ai_function_text_default_credentials", href: "/zh/reference/settings/session-settings/ai-function#ai_function_text_default_credentials" },
        { name: "ai_function_throw_on_error", href: "/zh/reference/settings/session-settings/ai-function#ai_function_throw_on_error" },
        { name: "ai_function_throw_on_quota_exceeded", href: "/zh/reference/settings/session-settings/ai-function#ai_function_throw_on_quota_exceeded" }
      ],
      children: []
    },
    {
      label: "allow_*",
      count: 34,
      settings: [
        { name: "allow_aggregate_partitions_independently", href: "/zh/reference/settings/session-settings/allow#allow_aggregate_partitions_independently" },
        { name: "allow_archive_path_syntax", href: "/zh/reference/settings/session-settings/allow#allow_archive_path_syntax" },
        { name: "allow_asynchronous_read_from_io_pool_for_merge_tree", href: "/zh/reference/settings/session-settings/allow#allow_asynchronous_read_from_io_pool_for_merge_tree" },
        { name: "allow_calculating_subcolumns_sizes_for_merge_tree_reading", href: "/zh/reference/settings/session-settings/allow#allow_calculating_subcolumns_sizes_for_merge_tree_reading" },
        { name: "allow_changing_replica_until_first_data_packet", href: "/zh/reference/settings/session-settings/allow#allow_changing_replica_until_first_data_packet" },
        { name: "allow_create_index_without_type", href: "/zh/reference/settings/session-settings/allow#allow_create_index_without_type" },
        { name: "allow_custom_error_code_in_throwif", href: "/zh/reference/settings/session-settings/allow#allow_custom_error_code_in_throwif" },
        { name: "allow_ddl", href: "/zh/reference/settings/session-settings/allow#allow_ddl" },
        { name: "allow_distributed_ddl", href: "/zh/reference/settings/session-settings/allow#allow_distributed_ddl" },
        { name: "allow_drop_detached", href: "/zh/reference/settings/session-settings/allow#allow_drop_detached" },
        { name: "allow_dynamic_type_in_join_keys", href: "/zh/reference/settings/session-settings/allow#allow_dynamic_type_in_join_keys" },
        { name: "allow_execute_multiif_columnar", href: "/zh/reference/settings/session-settings/allow#allow_execute_multiif_columnar" },
        { name: "allow_fuzz_query_functions", href: "/zh/reference/settings/session-settings/allow#allow_fuzz_query_functions" },
        { name: "allow_general_join_planning", href: "/zh/reference/settings/session-settings/allow#allow_general_join_planning" },
        { name: "allow_get_client_http_header", href: "/zh/reference/settings/session-settings/allow#allow_get_client_http_header" },
        { name: "allow_hyperscan", href: "/zh/reference/settings/session-settings/allow#allow_hyperscan" },
        { name: "allow_iceberg_remove_orphan_files", href: "/zh/reference/settings/session-settings/allow#allow_iceberg_remove_orphan_files" },
        { name: "allow_insert_into_iceberg", href: "/zh/reference/settings/session-settings/allow#allow_insert_into_iceberg" },
        { name: "allow_introspection_functions", href: "/zh/reference/settings/session-settings/allow#allow_introspection_functions" },
        { name: "allow_key_condition_coalesce_rewrite", href: "/zh/reference/settings/session-settings/allow#allow_key_condition_coalesce_rewrite" },
        { name: "allow_limit_by_partitions_independently", href: "/zh/reference/settings/session-settings/allow#allow_limit_by_partitions_independently" },
        { name: "allow_materialized_view_with_bad_select", href: "/zh/reference/settings/session-settings/allow#allow_materialized_view_with_bad_select" },
        { name: "allow_minmax_index_for_json", href: "/zh/reference/settings/session-settings/allow#allow_minmax_index_for_json" },
        { name: "allow_named_collection_override_by_default", href: "/zh/reference/settings/session-settings/allow#allow_named_collection_override_by_default" },
        { name: "allow_non_metadata_alters", href: "/zh/reference/settings/session-settings/allow#allow_non_metadata_alters" },
        { name: "allow_nonconst_timezone_arguments", href: "/zh/reference/settings/session-settings/allow#allow_nonconst_timezone_arguments" },
        { name: "allow_nullable_tuple_in_extracted_subcolumns", href: "/zh/reference/settings/session-settings/allow#allow_nullable_tuple_in_extracted_subcolumns" },
        { name: "allow_rank_dense_rank_arguments", href: "/zh/reference/settings/session-settings/allow#allow_rank_dense_rank_arguments" },
        { name: "allow_reorder_prewhere_conditions", href: "/zh/reference/settings/session-settings/allow#allow_reorder_prewhere_conditions" },
        { name: "allow_replace_partition_from_empty_source", href: "/zh/reference/settings/session-settings/allow#allow_replace_partition_from_empty_source" },
        { name: "allow_settings_after_format_in_insert", href: "/zh/reference/settings/session-settings/allow#allow_settings_after_format_in_insert" },
        { name: "allow_simdjson", href: "/zh/reference/settings/session-settings/allow#allow_simdjson" },
        { name: "allow_special_serialization_kinds_in_output_formats", href: "/zh/reference/settings/session-settings/allow#allow_special_serialization_kinds_in_output_formats" },
        { name: "allow_unrestricted_reads_from_keeper", href: "/zh/reference/settings/session-settings/allow#allow_unrestricted_reads_from_keeper" }
      ],
      children: []
    },
    {
      label: "allow_deprecated_*",
      count: 3,
      settings: [
        { name: "allow_deprecated_database_ordinary", href: "/zh/reference/settings/session-settings/allow-deprecated#allow_deprecated_database_ordinary" },
        { name: "allow_deprecated_error_prone_window_functions", href: "/zh/reference/settings/session-settings/allow-deprecated#allow_deprecated_error_prone_window_functions" },
        { name: "allow_deprecated_syntax_for_merge_tree", href: "/zh/reference/settings/session-settings/allow-deprecated#allow_deprecated_syntax_for_merge_tree" }
      ],
      children: []
    },
    {
      label: "allow_experimental_*",
      count: 39,
      settings: [
        { name: "allow_experimental_ai_functions", href: "/zh/reference/settings/session-settings/allow-experimental#allow_experimental_ai_functions" },
        { name: "allow_experimental_analyzer", href: "/zh/reference/settings/session-settings/allow-experimental#allow_experimental_analyzer" },
        { name: "allow_experimental_cleanup_old_data_files_compaction", href: "/zh/reference/settings/session-settings/allow-experimental#allow_experimental_cleanup_old_data_files_compaction" },
        { name: "allow_experimental_codecs", href: "/zh/reference/settings/session-settings/allow-experimental#allow_experimental_codecs" },
        { name: "allow_experimental_correlated_subqueries", href: "/zh/reference/settings/session-settings/allow-experimental#allow_experimental_correlated_subqueries" },
        { name: "allow_experimental_database_glue_catalog", href: "/zh/reference/settings/session-settings/allow-experimental#allow_experimental_database_glue_catalog" },
        { name: "allow_experimental_database_hms_catalog", href: "/zh/reference/settings/session-settings/allow-experimental#allow_experimental_database_hms_catalog" },
        { name: "allow_experimental_database_iceberg", href: "/zh/reference/settings/session-settings/allow-experimental#allow_experimental_database_iceberg" },
        { name: "allow_experimental_database_materialized_postgresql", href: "/zh/reference/settings/session-settings/allow-experimental#allow_experimental_database_materialized_postgresql" },
        { name: "allow_experimental_database_paimon_rest_catalog", href: "/zh/reference/settings/session-settings/allow-experimental#allow_experimental_database_paimon_rest_catalog" },
        { name: "allow_experimental_database_unity_catalog", href: "/zh/reference/settings/session-settings/allow-experimental#allow_experimental_database_unity_catalog" },
        { name: "allow_experimental_delta_kernel_rs", href: "/zh/reference/settings/session-settings/allow-experimental#allow_experimental_delta_kernel_rs" },
        { name: "allow_experimental_delta_lake_writes", href: "/zh/reference/settings/session-settings/allow-experimental#allow_experimental_delta_lake_writes" },
        { name: "allow_experimental_eval_table_function", href: "/zh/reference/settings/session-settings/allow-experimental#allow_experimental_eval_table_function" },
        { name: "allow_experimental_expire_snapshots", href: "/zh/reference/settings/session-settings/allow-experimental#allow_experimental_expire_snapshots" },
        { name: "allow_experimental_funnel_functions", href: "/zh/reference/settings/session-settings/allow-experimental#allow_experimental_funnel_functions" },
        { name: "allow_experimental_geo_types_in_iceberg", href: "/zh/reference/settings/session-settings/allow-experimental#allow_experimental_geo_types_in_iceberg" },
        { name: "allow_experimental_hash_functions", href: "/zh/reference/settings/session-settings/allow-experimental#allow_experimental_hash_functions" },
        { name: "allow_experimental_iceberg_compaction", href: "/zh/reference/settings/session-settings/allow-experimental#allow_experimental_iceberg_compaction" },
        { name: "allow_experimental_join_right_table_sorting", href: "/zh/reference/settings/session-settings/allow-experimental#allow_experimental_join_right_table_sorting" },
        { name: "allow_experimental_json_lazy_type_hints", href: "/zh/reference/settings/session-settings/allow-experimental#allow_experimental_json_lazy_type_hints" },
        { name: "allow_experimental_kafka_offsets_storage_in_keeper", href: "/zh/reference/settings/session-settings/allow-experimental#allow_experimental_kafka_offsets_storage_in_keeper" },
        { name: "allow_experimental_kusto_dialect", href: "/zh/reference/settings/session-settings/allow-experimental#allow_experimental_kusto_dialect" },
        { name: "allow_experimental_materialized_postgresql_table", href: "/zh/reference/settings/session-settings/allow-experimental#allow_experimental_materialized_postgresql_table" },
        { name: "allow_experimental_nlp_functions", href: "/zh/reference/settings/session-settings/allow-experimental#allow_experimental_nlp_functions" },
        { name: "allow_experimental_nullable_tuple_type", href: "/zh/reference/settings/session-settings/allow-experimental#allow_experimental_nullable_tuple_type" },
        {
          name: "allow_experimental_object_storage_queue_hive_partitioning",
          href: "/zh/reference/settings/session-settings/allow-experimental#allow_experimental_object_storage_queue_hive_partitioning"
        },
        { name: "allow_experimental_paimon_storage_engine", href: "/zh/reference/settings/session-settings/allow-experimental#allow_experimental_paimon_storage_engine" },
        { name: "allow_experimental_parallel_reading_from_replicas", href: "/zh/reference/settings/session-settings/allow-experimental#allow_experimental_parallel_reading_from_replicas" },
        { name: "allow_experimental_polyglot_dialect", href: "/zh/reference/settings/session-settings/allow-experimental#allow_experimental_polyglot_dialect" },
        { name: "allow_experimental_prql_dialect", href: "/zh/reference/settings/session-settings/allow-experimental#allow_experimental_prql_dialect" },
        { name: "allow_experimental_time_series_aggregate_functions", href: "/zh/reference/settings/session-settings/allow-experimental#allow_experimental_time_series_aggregate_functions" },
        { name: "allow_experimental_time_series_table", href: "/zh/reference/settings/session-settings/allow-experimental#allow_experimental_time_series_table" },
        { name: "allow_experimental_unique_key", href: "/zh/reference/settings/session-settings/allow-experimental#allow_experimental_unique_key" },
        { name: "allow_experimental_url_wildcard_from_index_pages", href: "/zh/reference/settings/session-settings/allow-experimental#allow_experimental_url_wildcard_from_index_pages" },
        { name: "allow_experimental_window_view", href: "/zh/reference/settings/session-settings/allow-experimental#allow_experimental_window_view" },
        { name: "allow_experimental_ytsaurus_dictionary_source", href: "/zh/reference/settings/session-settings/allow-experimental#allow_experimental_ytsaurus_dictionary_source" },
        { name: "allow_experimental_ytsaurus_table_engine", href: "/zh/reference/settings/session-settings/allow-experimental#allow_experimental_ytsaurus_table_engine" },
        { name: "allow_experimental_ytsaurus_table_function", href: "/zh/reference/settings/session-settings/allow-experimental#allow_experimental_ytsaurus_table_function" }
      ],
      children: []
    },
    {
      label: "allow_nondeterministic_*",
      count: 2,
      settings: [
        { name: "allow_nondeterministic_mutations", href: "/zh/reference/settings/session-settings/allow-nondeterministic#allow_nondeterministic_mutations" },
        { name: "allow_nondeterministic_optimize_skip_unused_shards", href: "/zh/reference/settings/session-settings/allow-nondeterministic#allow_nondeterministic_optimize_skip_unused_shards" }
      ],
      children: []
    },
    {
      label: "allow_prefetched_*",
      count: 2,
      settings: [
        { name: "allow_prefetched_read_pool_for_local_filesystem", href: "/zh/reference/settings/session-settings/allow-prefetched#allow_prefetched_read_pool_for_local_filesystem" },
        { name: "allow_prefetched_read_pool_for_remote_filesystem", href: "/zh/reference/settings/session-settings/allow-prefetched#allow_prefetched_read_pool_for_remote_filesystem" }
      ],
      children: []
    },
    {
      label: "allow_push_*",
      count: 2,
      settings: [
        { name: "allow_push_predicate_ast_for_distributed_subqueries", href: "/zh/reference/settings/session-settings/allow-push#allow_push_predicate_ast_for_distributed_subqueries" },
        { name: "allow_push_predicate_when_subquery_contains_with", href: "/zh/reference/settings/session-settings/allow-push#allow_push_predicate_when_subquery_contains_with" }
      ],
      children: []
    },
    {
      label: "allow_statistics_*",
      count: 2,
      settings: [
        { name: "allow_statistics", href: "/zh/reference/settings/session-settings/allow-statistics#allow_statistics" },
        { name: "allow_statistics_optimize", href: "/zh/reference/settings/session-settings/allow-statistics#allow_statistics_optimize" }
      ],
      children: []
    },
    {
      label: "allow_suspicious_*",
      count: 9,
      settings: [
        { name: "allow_suspicious_codecs", href: "/zh/reference/settings/session-settings/allow-suspicious#allow_suspicious_codecs" },
        { name: "allow_suspicious_fixed_string_types", href: "/zh/reference/settings/session-settings/allow-suspicious#allow_suspicious_fixed_string_types" },
        { name: "allow_suspicious_indices", href: "/zh/reference/settings/session-settings/allow-suspicious#allow_suspicious_indices" },
        { name: "allow_suspicious_low_cardinality_types", href: "/zh/reference/settings/session-settings/allow-suspicious#allow_suspicious_low_cardinality_types" },
        { name: "allow_suspicious_primary_key", href: "/zh/reference/settings/session-settings/allow-suspicious#allow_suspicious_primary_key" },
        { name: "allow_suspicious_ttl_expressions", href: "/zh/reference/settings/session-settings/allow-suspicious#allow_suspicious_ttl_expressions" },
        { name: "allow_suspicious_types_in_group_by", href: "/zh/reference/settings/session-settings/allow-suspicious#allow_suspicious_types_in_group_by" },
        { name: "allow_suspicious_types_in_order_by", href: "/zh/reference/settings/session-settings/allow-suspicious#allow_suspicious_types_in_order_by" },
        { name: "allow_suspicious_variant_types", href: "/zh/reference/settings/session-settings/allow-suspicious#allow_suspicious_variant_types" }
      ],
      children: []
    },
    {
      label: "alter_*",
      count: 4,
      settings: [
        { name: "alter_move_to_space_execute_async", href: "/zh/reference/settings/session-settings/alter#alter_move_to_space_execute_async" },
        { name: "alter_partition_verbose_result", href: "/zh/reference/settings/session-settings/alter#alter_partition_verbose_result" },
        { name: "alter_sync", href: "/zh/reference/settings/session-settings/alter#alter_sync" },
        { name: "alter_update_mode", href: "/zh/reference/settings/session-settings/alter#alter_update_mode" }
      ],
      children: []
    },
    {
      label: "analyzer_compatibility_*",
      count: 4,
      settings: [
        {
          name: "analyzer_compatibility_allow_compound_identifiers_in_unflatten_nested",
          href: "/zh/reference/settings/session-settings/analyzer-compatibility#analyzer_compatibility_allow_compound_identifiers_in_unflatten_nested"
        },
        { name: "analyzer_compatibility_allow_non_aggregate_in_having", href: "/zh/reference/settings/session-settings/analyzer-compatibility#analyzer_compatibility_allow_non_aggregate_in_having" },
        { name: "analyzer_compatibility_join_using_top_level_identifier", href: "/zh/reference/settings/session-settings/analyzer-compatibility#analyzer_compatibility_join_using_top_level_identifier" },
        { name: "analyzer_compatibility_prefer_alias_over_subcolumn", href: "/zh/reference/settings/session-settings/analyzer-compatibility#analyzer_compatibility_prefer_alias_over_subcolumn" }
      ],
      children: []
    },
    {
      label: "apply_*",
      count: 5,
      settings: [
        { name: "apply_deleted_mask", href: "/zh/reference/settings/session-settings/apply#apply_deleted_mask" },
        { name: "apply_mutations_on_fly", href: "/zh/reference/settings/session-settings/apply#apply_mutations_on_fly" },
        { name: "apply_prewhere_after_final", href: "/zh/reference/settings/session-settings/apply#apply_prewhere_after_final" },
        { name: "apply_row_policy_after_final", href: "/zh/reference/settings/session-settings/apply#apply_row_policy_after_final" },
        { name: "apply_settings_from_server", href: "/zh/reference/settings/session-settings/apply#apply_settings_from_server" }
      ],
      children: []
    },
    {
      label: "apply_patch_parts_*",
      count: 2,
      settings: [
        { name: "apply_patch_parts", href: "/zh/reference/settings/session-settings/apply-patch-parts#apply_patch_parts" },
        { name: "apply_patch_parts_join_cache_buckets", href: "/zh/reference/settings/session-settings/apply-patch-parts#apply_patch_parts_join_cache_buckets" }
      ],
      children: []
    },
    {
      label: "ast_fuzzer_*",
      count: 2,
      settings: [
        { name: "ast_fuzzer_any_query", href: "/zh/reference/settings/session-settings/ast-fuzzer#ast_fuzzer_any_query" },
        { name: "ast_fuzzer_runs", href: "/zh/reference/settings/session-settings/ast-fuzzer#ast_fuzzer_runs" }
      ],
      children: []
    },
    {
      label: "asterisk_include_*",
      count: 3,
      settings: [
        { name: "asterisk_include_alias_columns", href: "/zh/reference/settings/session-settings/asterisk-include#asterisk_include_alias_columns" },
        { name: "asterisk_include_materialized_columns", href: "/zh/reference/settings/session-settings/asterisk-include#asterisk_include_materialized_columns" },
        { name: "asterisk_include_virtual_columns", href: "/zh/reference/settings/session-settings/asterisk-include#asterisk_include_virtual_columns" }
      ],
      children: []
    },
    {
      label: "async_*",
      count: 2,
      settings: [
        { name: "async_query_sending_for_remote", href: "/zh/reference/settings/session-settings/async#async_query_sending_for_remote" },
        { name: "async_socket_for_remote", href: "/zh/reference/settings/session-settings/async#async_socket_for_remote" }
      ],
      children: []
    },
    {
      label: "async_insert_*",
      count: 10,
      settings: [
        { name: "async_insert", href: "/zh/reference/settings/session-settings/async-insert#async_insert" },
        { name: "async_insert_busy_timeout_decrease_rate", href: "/zh/reference/settings/session-settings/async-insert#async_insert_busy_timeout_decrease_rate" },
        { name: "async_insert_busy_timeout_increase_rate", href: "/zh/reference/settings/session-settings/async-insert#async_insert_busy_timeout_increase_rate" },
        { name: "async_insert_busy_timeout_max_ms", href: "/zh/reference/settings/session-settings/async-insert#async_insert_busy_timeout_max_ms" },
        { name: "async_insert_busy_timeout_min_ms", href: "/zh/reference/settings/session-settings/async-insert#async_insert_busy_timeout_min_ms" },
        { name: "async_insert_deduplicate", href: "/zh/reference/settings/session-settings/async-insert#async_insert_deduplicate" },
        { name: "async_insert_max_data_size", href: "/zh/reference/settings/session-settings/async-insert#async_insert_max_data_size" },
        { name: "async_insert_max_query_number", href: "/zh/reference/settings/session-settings/async-insert#async_insert_max_query_number" },
        { name: "async_insert_poll_timeout_ms", href: "/zh/reference/settings/session-settings/async-insert#async_insert_poll_timeout_ms" },
        { name: "async_insert_use_adaptive_busy_timeout", href: "/zh/reference/settings/session-settings/async-insert#async_insert_use_adaptive_busy_timeout" }
      ],
      children: []
    },
    {
      label: "automatic_parallel_*",
      count: 2,
      settings: [
        { name: "automatic_parallel_replicas_min_bytes_per_replica", href: "/zh/reference/settings/session-settings/automatic-parallel#automatic_parallel_replicas_min_bytes_per_replica" },
        { name: "automatic_parallel_replicas_mode", href: "/zh/reference/settings/session-settings/automatic-parallel#automatic_parallel_replicas_mode" }
      ],
      children: []
    },
    {
      label: "azure_*",
      count: 13,
      settings: [
        { name: "azure_allow_parallel_part_upload", href: "/zh/reference/settings/session-settings/azure#azure_allow_parallel_part_upload" },
        { name: "azure_check_objects_after_upload", href: "/zh/reference/settings/session-settings/azure#azure_check_objects_after_upload" },
        { name: "azure_connect_timeout_ms", href: "/zh/reference/settings/session-settings/azure#azure_connect_timeout_ms" },
        { name: "azure_create_new_file_on_insert", href: "/zh/reference/settings/session-settings/azure#azure_create_new_file_on_insert" },
        { name: "azure_ignore_file_doesnt_exist", href: "/zh/reference/settings/session-settings/azure#azure_ignore_file_doesnt_exist" },
        { name: "azure_list_object_keys_size", href: "/zh/reference/settings/session-settings/azure#azure_list_object_keys_size" },
        { name: "azure_min_upload_part_size", href: "/zh/reference/settings/session-settings/azure#azure_min_upload_part_size" },
        { name: "azure_request_timeout_ms", href: "/zh/reference/settings/session-settings/azure#azure_request_timeout_ms" },
        { name: "azure_skip_empty_files", href: "/zh/reference/settings/session-settings/azure#azure_skip_empty_files" },
        { name: "azure_strict_upload_part_size", href: "/zh/reference/settings/session-settings/azure#azure_strict_upload_part_size" },
        { name: "azure_throw_on_zero_files_match", href: "/zh/reference/settings/session-settings/azure#azure_throw_on_zero_files_match" },
        { name: "azure_truncate_on_insert", href: "/zh/reference/settings/session-settings/azure#azure_truncate_on_insert" },
        { name: "azure_use_adaptive_timeouts", href: "/zh/reference/settings/session-settings/azure#azure_use_adaptive_timeouts" }
      ],
      children: []
    },
    {
      label: "azure_max_*",
      count: 12,
      settings: [
        { name: "azure_max_blocks_in_multipart_upload", href: "/zh/reference/settings/session-settings/azure-max#azure_max_blocks_in_multipart_upload" },
        { name: "azure_max_get_burst", href: "/zh/reference/settings/session-settings/azure-max#azure_max_get_burst" },
        { name: "azure_max_get_rps", href: "/zh/reference/settings/session-settings/azure-max#azure_max_get_rps" },
        { name: "azure_max_inflight_parts_for_one_file", href: "/zh/reference/settings/session-settings/azure-max#azure_max_inflight_parts_for_one_file" },
        { name: "azure_max_put_burst", href: "/zh/reference/settings/session-settings/azure-max#azure_max_put_burst" },
        { name: "azure_max_put_rps", href: "/zh/reference/settings/session-settings/azure-max#azure_max_put_rps" },
        { name: "azure_max_redirects", href: "/zh/reference/settings/session-settings/azure-max#azure_max_redirects" },
        { name: "azure_max_single_part_copy_size", href: "/zh/reference/settings/session-settings/azure-max#azure_max_single_part_copy_size" },
        { name: "azure_max_single_part_upload_size", href: "/zh/reference/settings/session-settings/azure-max#azure_max_single_part_upload_size" },
        { name: "azure_max_single_read_retries", href: "/zh/reference/settings/session-settings/azure-max#azure_max_single_read_retries" },
        { name: "azure_max_unexpected_write_error_retries", href: "/zh/reference/settings/session-settings/azure-max#azure_max_unexpected_write_error_retries" },
        { name: "azure_max_upload_part_size", href: "/zh/reference/settings/session-settings/azure-max#azure_max_upload_part_size" }
      ],
      children: []
    },
    {
      label: "azure_sdk_*",
      count: 3,
      settings: [
        { name: "azure_sdk_max_retries", href: "/zh/reference/settings/session-settings/azure-sdk#azure_sdk_max_retries" },
        { name: "azure_sdk_retry_initial_backoff_ms", href: "/zh/reference/settings/session-settings/azure-sdk#azure_sdk_retry_initial_backoff_ms" },
        { name: "azure_sdk_retry_max_backoff_ms", href: "/zh/reference/settings/session-settings/azure-sdk#azure_sdk_retry_max_backoff_ms" }
      ],
      children: []
    },
    {
      label: "azure_upload_*",
      count: 2,
      settings: [
        { name: "azure_upload_part_size_multiply_factor", href: "/zh/reference/settings/session-settings/azure-upload#azure_upload_part_size_multiply_factor" },
        { name: "azure_upload_part_size_multiply_parts_count_threshold", href: "/zh/reference/settings/session-settings/azure-upload#azure_upload_part_size_multiply_parts_count_threshold" }
      ],
      children: []
    },
    {
      label: "backup_restore_*",
      count: 16,
      settings: [
        { name: "backup_restore_batch_size_for_keeper_multi", href: "/zh/reference/settings/session-settings/backup-restore#backup_restore_batch_size_for_keeper_multi" },
        { name: "backup_restore_batch_size_for_keeper_multiread", href: "/zh/reference/settings/session-settings/backup-restore#backup_restore_batch_size_for_keeper_multiread" },
        { name: "backup_restore_failure_after_host_disconnected_for_seconds", href: "/zh/reference/settings/session-settings/backup-restore#backup_restore_failure_after_host_disconnected_for_seconds" },
        { name: "backup_restore_finish_timeout_after_error_sec", href: "/zh/reference/settings/session-settings/backup-restore#backup_restore_finish_timeout_after_error_sec" },
        { name: "backup_restore_keeper_fault_injection_probability", href: "/zh/reference/settings/session-settings/backup-restore#backup_restore_keeper_fault_injection_probability" },
        { name: "backup_restore_keeper_fault_injection_seed", href: "/zh/reference/settings/session-settings/backup-restore#backup_restore_keeper_fault_injection_seed" },
        { name: "backup_restore_keeper_max_retries", href: "/zh/reference/settings/session-settings/backup-restore#backup_restore_keeper_max_retries" },
        { name: "backup_restore_keeper_max_retries_while_handling_error", href: "/zh/reference/settings/session-settings/backup-restore#backup_restore_keeper_max_retries_while_handling_error" },
        { name: "backup_restore_keeper_max_retries_while_initializing", href: "/zh/reference/settings/session-settings/backup-restore#backup_restore_keeper_max_retries_while_initializing" },
        { name: "backup_restore_keeper_retry_initial_backoff_ms", href: "/zh/reference/settings/session-settings/backup-restore#backup_restore_keeper_retry_initial_backoff_ms" },
        { name: "backup_restore_keeper_retry_max_backoff_ms", href: "/zh/reference/settings/session-settings/backup-restore#backup_restore_keeper_retry_max_backoff_ms" },
        { name: "backup_restore_keeper_value_max_size", href: "/zh/reference/settings/session-settings/backup-restore#backup_restore_keeper_value_max_size" },
        { name: "backup_restore_s3_retry_attempts", href: "/zh/reference/settings/session-settings/backup-restore#backup_restore_s3_retry_attempts" },
        { name: "backup_restore_s3_retry_initial_backoff_ms", href: "/zh/reference/settings/session-settings/backup-restore#backup_restore_s3_retry_initial_backoff_ms" },
        { name: "backup_restore_s3_retry_jitter_factor", href: "/zh/reference/settings/session-settings/backup-restore#backup_restore_s3_retry_jitter_factor" },
        { name: "backup_restore_s3_retry_max_backoff_ms", href: "/zh/reference/settings/session-settings/backup-restore#backup_restore_s3_retry_max_backoff_ms" }
      ],
      children: []
    },
    {
      label: "cast_*",
      count: 2,
      settings: [
        { name: "cast_ipv4_ipv6_default_on_conversion_error", href: "/zh/reference/settings/session-settings/cast#cast_ipv4_ipv6_default_on_conversion_error" },
        { name: "cast_keep_nullable", href: "/zh/reference/settings/session-settings/cast#cast_keep_nullable" }
      ],
      children: []
    },
    {
      label: "cast_string_*",
      count: 3,
      settings: [
        { name: "cast_string_to_date_time_mode", href: "/zh/reference/settings/session-settings/cast-string#cast_string_to_date_time_mode" },
        { name: "cast_string_to_dynamic_use_inference", href: "/zh/reference/settings/session-settings/cast-string#cast_string_to_dynamic_use_inference" },
        { name: "cast_string_to_variant_use_inference", href: "/zh/reference/settings/session-settings/cast-string#cast_string_to_variant_use_inference" }
      ],
      children: []
    },
    {
      label: "check_*",
      count: 4,
      settings: [
        { name: "check_named_collection_dependencies", href: "/zh/reference/settings/session-settings/check#check_named_collection_dependencies" },
        { name: "check_query_single_value_result", href: "/zh/reference/settings/session-settings/check#check_query_single_value_result" },
        { name: "check_referential_table_dependencies", href: "/zh/reference/settings/session-settings/check#check_referential_table_dependencies" },
        { name: "check_table_dependencies", href: "/zh/reference/settings/session-settings/check#check_table_dependencies" }
      ],
      children: []
    },
    {
      label: "cloud_mode_*",
      count: 2,
      settings: [
        { name: "cloud_mode", href: "/zh/reference/settings/session-settings/cloud-mode#cloud_mode" },
        { name: "cloud_mode_engine", href: "/zh/reference/settings/session-settings/cloud-mode#cloud_mode_engine" }
      ],
      children: []
    },
    {
      label: "cluster_*",
      count: 2,
      settings: [
        { name: "cluster_for_parallel_replicas", href: "/zh/reference/settings/session-settings/cluster#cluster_for_parallel_replicas" },
        { name: "cluster_function_process_archive_on_multiple_nodes", href: "/zh/reference/settings/session-settings/cluster#cluster_function_process_archive_on_multiple_nodes" }
      ],
      children: []
    },
    {
      label: "cluster_table_*",
      count: 2,
      settings: [
        { name: "cluster_table_function_buckets_batch_size", href: "/zh/reference/settings/session-settings/cluster-table#cluster_table_function_buckets_batch_size" },
        { name: "cluster_table_function_split_granularity", href: "/zh/reference/settings/session-settings/cluster-table#cluster_table_function_split_granularity" }
      ],
      children: []
    },
    {
      label: "collect_hash_*",
      count: 2,
      settings: [
        { name: "collect_hash_table_stats_during_aggregation", href: "/zh/reference/settings/session-settings/collect-hash#collect_hash_table_stats_during_aggregation" },
        { name: "collect_hash_table_stats_during_joins", href: "/zh/reference/settings/session-settings/collect-hash#collect_hash_table_stats_during_joins" }
      ],
      children: []
    },
    {
      label: "compatibility_*",
      count: 2,
      settings: [
        { name: "compatibility", href: "/zh/reference/settings/session-settings/compatibility#compatibility" },
        { name: "compatibility_s3_presigned_url_query_in_path", href: "/zh/reference/settings/session-settings/compatibility#compatibility_s3_presigned_url_query_in_path" }
      ],
      children: []
    },
    {
      label: "compatibility_ignore_*",
      count: 2,
      settings: [
        { name: "compatibility_ignore_auto_increment_in_create_table", href: "/zh/reference/settings/session-settings/compatibility-ignore#compatibility_ignore_auto_increment_in_create_table" },
        { name: "compatibility_ignore_collation_in_create_table", href: "/zh/reference/settings/session-settings/compatibility-ignore#compatibility_ignore_collation_in_create_table" }
      ],
      children: []
    },
    {
      label: "compile_*",
      count: 4,
      settings: [
        { name: "compile_aggregate_expressions", href: "/zh/reference/settings/session-settings/compile#compile_aggregate_expressions" },
        { name: "compile_expressions", href: "/zh/reference/settings/session-settings/compile#compile_expressions" },
        { name: "compile_regular_expressions", href: "/zh/reference/settings/session-settings/compile#compile_regular_expressions" },
        { name: "compile_sort_description", href: "/zh/reference/settings/session-settings/compile#compile_sort_description" }
      ],
      children: []
    },
    {
      label: "connect_timeout_*",
      count: 3,
      settings: [
        { name: "connect_timeout", href: "/zh/reference/settings/session-settings/connect-timeout#connect_timeout" },
        { name: "connect_timeout_with_failover_ms", href: "/zh/reference/settings/session-settings/connect-timeout#connect_timeout_with_failover_ms" },
        { name: "connect_timeout_with_failover_secure_ms", href: "/zh/reference/settings/session-settings/connect-timeout#connect_timeout_with_failover_secure_ms" }
      ],
      children: []
    },
    {
      label: "correlated_subqueries_*",
      count: 3,
      settings: [
        { name: "correlated_subqueries_default_join_kind", href: "/zh/reference/settings/session-settings/correlated-subqueries#correlated_subqueries_default_join_kind" },
        { name: "correlated_subqueries_substitute_equivalent_expressions", href: "/zh/reference/settings/session-settings/correlated-subqueries#correlated_subqueries_substitute_equivalent_expressions" },
        { name: "correlated_subqueries_use_in_memory_buffer", href: "/zh/reference/settings/session-settings/correlated-subqueries#correlated_subqueries_use_in_memory_buffer" }
      ],
      children: []
    },
    {
      label: "count_distinct_*",
      count: 2,
      settings: [
        { name: "count_distinct_implementation", href: "/zh/reference/settings/session-settings/count-distinct#count_distinct_implementation" },
        { name: "count_distinct_optimization", href: "/zh/reference/settings/session-settings/count-distinct#count_distinct_optimization" }
      ],
      children: []
    },
    {
      label: "create_*",
      count: 4,
      settings: [
        { name: "create_if_not_exists", href: "/zh/reference/settings/session-settings/create#create_if_not_exists" },
        { name: "create_index_ignore_unique", href: "/zh/reference/settings/session-settings/create#create_index_ignore_unique" },
        { name: "create_replicated_merge_tree_fault_injection_probability", href: "/zh/reference/settings/session-settings/create#create_replicated_merge_tree_fault_injection_probability" },
        { name: "create_table_empty_primary_key_by_default", href: "/zh/reference/settings/session-settings/create#create_table_empty_primary_key_by_default" }
      ],
      children: []
    },
    {
      label: "cross_join_*",
      count: 2,
      settings: [
        { name: "cross_join_min_bytes_to_compress", href: "/zh/reference/settings/session-settings/cross-join#cross_join_min_bytes_to_compress" },
        { name: "cross_join_min_rows_to_compress", href: "/zh/reference/settings/session-settings/cross-join#cross_join_min_rows_to_compress" }
      ],
      children: []
    },
    {
      label: "database_*",
      count: 3,
      settings: [
        { name: "database_atomic_wait_for_drop_and_detach_synchronously", href: "/zh/reference/settings/session-settings/database#database_atomic_wait_for_drop_and_detach_synchronously" },
        { name: "database_datalake_require_metadata_access", href: "/zh/reference/settings/session-settings/database#database_datalake_require_metadata_access" },
        { name: "database_shared_drop_table_delay_seconds", href: "/zh/reference/settings/session-settings/database#database_shared_drop_table_delay_seconds" }
      ],
      children: []
    },
    {
      label: "database_replicated_*",
      count: 7,
      settings: [
        { name: "database_replicated_allow_explicit_uuid", href: "/zh/reference/settings/session-settings/database-replicated#database_replicated_allow_explicit_uuid" },
        { name: "database_replicated_allow_heavy_create", href: "/zh/reference/settings/session-settings/database-replicated#database_replicated_allow_heavy_create" },
        { name: "database_replicated_allow_only_replicated_engine", href: "/zh/reference/settings/session-settings/database-replicated#database_replicated_allow_only_replicated_engine" },
        { name: "database_replicated_allow_replicated_engine_arguments", href: "/zh/reference/settings/session-settings/database-replicated#database_replicated_allow_replicated_engine_arguments" },
        { name: "database_replicated_always_detach_permanently", href: "/zh/reference/settings/session-settings/database-replicated#database_replicated_always_detach_permanently" },
        { name: "database_replicated_enforce_synchronous_settings", href: "/zh/reference/settings/session-settings/database-replicated#database_replicated_enforce_synchronous_settings" },
        { name: "database_replicated_initial_query_timeout_sec", href: "/zh/reference/settings/session-settings/database-replicated#database_replicated_initial_query_timeout_sec" }
      ],
      children: []
    },
    {
      label: "dead_blobs_*",
      count: 2,
      settings: [
        { name: "dead_blobs_to_delay_insert", href: "/zh/reference/settings/session-settings/dead-blobs#dead_blobs_to_delay_insert" },
        { name: "dead_blobs_to_throw_insert", href: "/zh/reference/settings/session-settings/dead-blobs#dead_blobs_to_throw_insert" }
      ],
      children: []
    },
    {
      label: "deduplicate_insert_*",
      count: 2,
      settings: [
        { name: "deduplicate_insert", href: "/zh/reference/settings/session-settings/deduplicate-insert#deduplicate_insert" },
        { name: "deduplicate_insert_select", href: "/zh/reference/settings/session-settings/deduplicate-insert#deduplicate_insert_select" }
      ],
      children: []
    },
    {
      label: "default_*",
      count: 6,
      settings: [
        { name: "default_materialized_view_sql_security", href: "/zh/reference/settings/session-settings/default#default_materialized_view_sql_security" },
        { name: "default_max_bytes_in_join", href: "/zh/reference/settings/session-settings/default#default_max_bytes_in_join" },
        { name: "default_normal_view_sql_security", href: "/zh/reference/settings/session-settings/default#default_normal_view_sql_security" },
        { name: "default_table_engine", href: "/zh/reference/settings/session-settings/default#default_table_engine" },
        { name: "default_temporary_table_engine", href: "/zh/reference/settings/session-settings/default#default_temporary_table_engine" },
        { name: "default_view_definer", href: "/zh/reference/settings/session-settings/default#default_view_definer" }
      ],
      children: []
    },
    {
      label: "delta_lake_*",
      count: 10,
      settings: [
        { name: "delta_lake_enable_engine_predicate", href: "/zh/reference/settings/session-settings/delta-lake#delta_lake_enable_engine_predicate" },
        { name: "delta_lake_enable_expression_visitor_logging", href: "/zh/reference/settings/session-settings/delta-lake#delta_lake_enable_expression_visitor_logging" },
        { name: "delta_lake_insert_max_bytes_in_data_file", href: "/zh/reference/settings/session-settings/delta-lake#delta_lake_insert_max_bytes_in_data_file" },
        { name: "delta_lake_insert_max_rows_in_data_file", href: "/zh/reference/settings/session-settings/delta-lake#delta_lake_insert_max_rows_in_data_file" },
        { name: "delta_lake_log_metadata", href: "/zh/reference/settings/session-settings/delta-lake#delta_lake_log_metadata" },
        { name: "delta_lake_reload_schema_for_consistency", href: "/zh/reference/settings/session-settings/delta-lake#delta_lake_reload_schema_for_consistency" },
        { name: "delta_lake_snapshot_end_version", href: "/zh/reference/settings/session-settings/delta-lake#delta_lake_snapshot_end_version" },
        { name: "delta_lake_snapshot_start_version", href: "/zh/reference/settings/session-settings/delta-lake#delta_lake_snapshot_start_version" },
        { name: "delta_lake_snapshot_version", href: "/zh/reference/settings/session-settings/delta-lake#delta_lake_snapshot_version" },
        { name: "delta_lake_throw_on_engine_predicate_error", href: "/zh/reference/settings/session-settings/delta-lake#delta_lake_throw_on_engine_predicate_error" }
      ],
      children: []
    },
    {
      label: "describe_include_*",
      count: 2,
      settings: [
        { name: "describe_include_subcolumns", href: "/zh/reference/settings/session-settings/describe-include#describe_include_subcolumns" },
        { name: "describe_include_virtual_columns", href: "/zh/reference/settings/session-settings/describe-include#describe_include_virtual_columns" }
      ],
      children: []
    },
    {
      label: "dictionary_*",
      count: 3,
      settings: [
        { name: "dictionary_lazy_load", href: "/zh/reference/settings/session-settings/dictionary#dictionary_lazy_load" },
        { name: "dictionary_use_async_executor", href: "/zh/reference/settings/session-settings/dictionary#dictionary_use_async_executor" },
        { name: "dictionary_validate_primary_key_type", href: "/zh/reference/settings/session-settings/dictionary#dictionary_validate_primary_key_type" }
      ],
      children: []
    },
    {
      label: "distributed_*",
      count: 7,
      settings: [
        { name: "distributed_aggregation_memory_efficient", href: "/zh/reference/settings/session-settings/distributed#distributed_aggregation_memory_efficient" },
        { name: "distributed_connections_pool_size", href: "/zh/reference/settings/session-settings/distributed#distributed_connections_pool_size" },
        { name: "distributed_foreground_insert", href: "/zh/reference/settings/session-settings/distributed#distributed_foreground_insert" },
        { name: "distributed_group_by_no_merge", href: "/zh/reference/settings/session-settings/distributed#distributed_group_by_no_merge" },
        { name: "distributed_insert_skip_read_only_replicas", href: "/zh/reference/settings/session-settings/distributed#distributed_insert_skip_read_only_replicas" },
        { name: "distributed_product_mode", href: "/zh/reference/settings/session-settings/distributed#distributed_product_mode" },
        { name: "distributed_push_down_limit", href: "/zh/reference/settings/session-settings/distributed#distributed_push_down_limit" }
      ],
      children: []
    },
    {
      label: "distributed_background_*",
      count: 5,
      settings: [
        { name: "distributed_background_insert_batch", href: "/zh/reference/settings/session-settings/distributed-background#distributed_background_insert_batch" },
        { name: "distributed_background_insert_max_sleep_time_ms", href: "/zh/reference/settings/session-settings/distributed-background#distributed_background_insert_max_sleep_time_ms" },
        { name: "distributed_background_insert_sleep_time_ms", href: "/zh/reference/settings/session-settings/distributed-background#distributed_background_insert_sleep_time_ms" },
        { name: "distributed_background_insert_split_batch_on_failure", href: "/zh/reference/settings/session-settings/distributed-background#distributed_background_insert_split_batch_on_failure" },
        { name: "distributed_background_insert_timeout", href: "/zh/reference/settings/session-settings/distributed-background#distributed_background_insert_timeout" }
      ],
      children: []
    },
    {
      label: "distributed_cache_*",
      count: 28,
      settings: [
        { name: "distributed_cache_alignment", href: "/zh/reference/settings/session-settings/distributed-cache#distributed_cache_alignment" },
        { name: "distributed_cache_bypass_connection_pool", href: "/zh/reference/settings/session-settings/distributed-cache#distributed_cache_bypass_connection_pool" },
        { name: "distributed_cache_connect_backoff_max_ms", href: "/zh/reference/settings/session-settings/distributed-cache#distributed_cache_connect_backoff_max_ms" },
        { name: "distributed_cache_connect_backoff_min_ms", href: "/zh/reference/settings/session-settings/distributed-cache#distributed_cache_connect_backoff_min_ms" },
        { name: "distributed_cache_connect_max_tries", href: "/zh/reference/settings/session-settings/distributed-cache#distributed_cache_connect_max_tries" },
        { name: "distributed_cache_connect_timeout_ms", href: "/zh/reference/settings/session-settings/distributed-cache#distributed_cache_connect_timeout_ms" },
        { name: "distributed_cache_credentials_refresh_period_seconds", href: "/zh/reference/settings/session-settings/distributed-cache#distributed_cache_credentials_refresh_period_seconds" },
        { name: "distributed_cache_data_packet_ack_window", href: "/zh/reference/settings/session-settings/distributed-cache#distributed_cache_data_packet_ack_window" },
        { name: "distributed_cache_discard_connection_if_unread_data", href: "/zh/reference/settings/session-settings/distributed-cache#distributed_cache_discard_connection_if_unread_data" },
        { name: "distributed_cache_fetch_metrics_only_from_current_az", href: "/zh/reference/settings/session-settings/distributed-cache#distributed_cache_fetch_metrics_only_from_current_az" },
        { name: "distributed_cache_file_cache_name", href: "/zh/reference/settings/session-settings/distributed-cache#distributed_cache_file_cache_name" },
        { name: "distributed_cache_log_mode", href: "/zh/reference/settings/session-settings/distributed-cache#distributed_cache_log_mode" },
        { name: "distributed_cache_max_unacked_inflight_packets", href: "/zh/reference/settings/session-settings/distributed-cache#distributed_cache_max_unacked_inflight_packets" },
        { name: "distributed_cache_min_bytes_for_seek", href: "/zh/reference/settings/session-settings/distributed-cache#distributed_cache_min_bytes_for_seek" },
        { name: "distributed_cache_pool_behaviour_on_limit", href: "/zh/reference/settings/session-settings/distributed-cache#distributed_cache_pool_behaviour_on_limit" },
        { name: "distributed_cache_prefer_bigger_buffer_size", href: "/zh/reference/settings/session-settings/distributed-cache#distributed_cache_prefer_bigger_buffer_size" },
        { name: "distributed_cache_read_only_from_current_az", href: "/zh/reference/settings/session-settings/distributed-cache#distributed_cache_read_only_from_current_az" },
        { name: "distributed_cache_read_request_max_tries", href: "/zh/reference/settings/session-settings/distributed-cache#distributed_cache_read_request_max_tries" },
        { name: "distributed_cache_receive_response_wait_milliseconds", href: "/zh/reference/settings/session-settings/distributed-cache#distributed_cache_receive_response_wait_milliseconds" },
        { name: "distributed_cache_receive_timeout_milliseconds", href: "/zh/reference/settings/session-settings/distributed-cache#distributed_cache_receive_timeout_milliseconds" },
        { name: "distributed_cache_receive_timeout_ms", href: "/zh/reference/settings/session-settings/distributed-cache#distributed_cache_receive_timeout_ms" },
        { name: "distributed_cache_registry_show_certificate_and_signature", href: "/zh/reference/settings/session-settings/distributed-cache#distributed_cache_registry_show_certificate_and_signature" },
        { name: "distributed_cache_send_timeout_ms", href: "/zh/reference/settings/session-settings/distributed-cache#distributed_cache_send_timeout_ms" },
        { name: "distributed_cache_tcp_keep_alive_timeout_ms", href: "/zh/reference/settings/session-settings/distributed-cache#distributed_cache_tcp_keep_alive_timeout_ms" },
        { name: "distributed_cache_throw_on_error", href: "/zh/reference/settings/session-settings/distributed-cache#distributed_cache_throw_on_error" },
        { name: "distributed_cache_use_clients_cache_for_read", href: "/zh/reference/settings/session-settings/distributed-cache#distributed_cache_use_clients_cache_for_read" },
        { name: "distributed_cache_wait_connection_from_pool_milliseconds", href: "/zh/reference/settings/session-settings/distributed-cache#distributed_cache_wait_connection_from_pool_milliseconds" },
        { name: "distributed_cache_write_request_max_tries", href: "/zh/reference/settings/session-settings/distributed-cache#distributed_cache_write_request_max_tries" }
      ],
      children: []
    },
    {
      label: "distributed_ddl_*",
      count: 3,
      settings: [
        { name: "distributed_ddl_entry_format_version", href: "/zh/reference/settings/session-settings/distributed-ddl#distributed_ddl_entry_format_version" },
        { name: "distributed_ddl_output_mode", href: "/zh/reference/settings/session-settings/distributed-ddl#distributed_ddl_output_mode" },
        { name: "distributed_ddl_task_timeout", href: "/zh/reference/settings/session-settings/distributed-ddl#distributed_ddl_task_timeout" }
      ],
      children: []
    },
    {
      label: "distributed_index_analysis_*",
      count: 3,
      settings: [
        { name: "distributed_index_analysis", href: "/zh/reference/settings/session-settings/distributed-index-analysis#distributed_index_analysis" },
        { name: "distributed_index_analysis_for_non_shared_merge_tree", href: "/zh/reference/settings/session-settings/distributed-index-analysis#distributed_index_analysis_for_non_shared_merge_tree" },
        { name: "distributed_index_analysis_only_on_coordinator", href: "/zh/reference/settings/session-settings/distributed-index-analysis#distributed_index_analysis_only_on_coordinator" }
      ],
      children: []
    },
    {
      label: "distributed_plan_*",
      count: 9,
      settings: [
        { name: "distributed_plan_default_reader_bucket_count", href: "/zh/reference/settings/session-settings/distributed-plan#distributed_plan_default_reader_bucket_count" },
        { name: "distributed_plan_default_shuffle_join_bucket_count", href: "/zh/reference/settings/session-settings/distributed-plan#distributed_plan_default_shuffle_join_bucket_count" },
        { name: "distributed_plan_execute_locally", href: "/zh/reference/settings/session-settings/distributed-plan#distributed_plan_execute_locally" },
        { name: "distributed_plan_force_exchange_kind", href: "/zh/reference/settings/session-settings/distributed-plan#distributed_plan_force_exchange_kind" },
        { name: "distributed_plan_force_shuffle_aggregation", href: "/zh/reference/settings/session-settings/distributed-plan#distributed_plan_force_shuffle_aggregation" },
        { name: "distributed_plan_max_rows_to_broadcast", href: "/zh/reference/settings/session-settings/distributed-plan#distributed_plan_max_rows_to_broadcast" },
        { name: "distributed_plan_optimize_exchanges", href: "/zh/reference/settings/session-settings/distributed-plan#distributed_plan_optimize_exchanges" },
        { name: "distributed_plan_prefer_replicas_over_workers", href: "/zh/reference/settings/session-settings/distributed-plan#distributed_plan_prefer_replicas_over_workers" },
        { name: "distributed_plan_workers_num", href: "/zh/reference/settings/session-settings/distributed-plan#distributed_plan_workers_num" }
      ],
      children: []
    },
    {
      label: "distributed_replica_*",
      count: 3,
      settings: [
        { name: "distributed_replica_error_cap", href: "/zh/reference/settings/session-settings/distributed-replica#distributed_replica_error_cap" },
        { name: "distributed_replica_error_half_life", href: "/zh/reference/settings/session-settings/distributed-replica#distributed_replica_error_half_life" },
        { name: "distributed_replica_max_ignored_errors", href: "/zh/reference/settings/session-settings/distributed-replica#distributed_replica_max_ignored_errors" }
      ],
      children: []
    },
    {
      label: "dynamic_disk_*",
      count: 3,
      settings: [
        { name: "dynamic_disk_allow_from_env", href: "/zh/reference/settings/session-settings/dynamic-disk#dynamic_disk_allow_from_env" },
        { name: "dynamic_disk_allow_from_zk", href: "/zh/reference/settings/session-settings/dynamic-disk#dynamic_disk_allow_from_zk" },
        { name: "dynamic_disk_allow_include", href: "/zh/reference/settings/session-settings/dynamic-disk#dynamic_disk_allow_include" }
      ],
      children: []
    },
    {
      label: "empty_result_*",
      count: 2,
      settings: [
        { name: "empty_result_for_aggregation_by_constant_keys_on_empty_set", href: "/zh/reference/settings/session-settings/empty-result#empty_result_for_aggregation_by_constant_keys_on_empty_set" },
        { name: "empty_result_for_aggregation_by_empty_set", href: "/zh/reference/settings/session-settings/empty-result#empty_result_for_aggregation_by_empty_set" }
      ],
      children: []
    },
    {
      label: "enable_*",
      count: 33,
      settings: [
        { name: "enable_adaptive_memory_spill_scheduler", href: "/zh/reference/settings/session-settings/enable#enable_adaptive_memory_spill_scheduler" },
        { name: "enable_add_distinct_to_in_subqueries", href: "/zh/reference/settings/session-settings/enable#enable_add_distinct_to_in_subqueries" },
        {
          name: "enable_automatic_decision_for_merging_across_partitions_for_final",
          href: "/zh/reference/settings/session-settings/enable#enable_automatic_decision_for_merging_across_partitions_for_final"
        },
        { name: "enable_early_constant_folding", href: "/zh/reference/settings/session-settings/enable#enable_early_constant_folding" },
        { name: "enable_extended_results_for_datetime_functions", href: "/zh/reference/settings/session-settings/enable#enable_extended_results_for_datetime_functions" },
        { name: "enable_full_text_index", href: "/zh/reference/settings/session-settings/enable#enable_full_text_index" },
        { name: "enable_global_with_statement", href: "/zh/reference/settings/session-settings/enable#enable_global_with_statement" },
        { name: "enable_hdfs_pread", href: "/zh/reference/settings/session-settings/enable#enable_hdfs_pread" },
        { name: "enable_http_compression", href: "/zh/reference/settings/session-settings/enable#enable_http_compression" },
        { name: "enable_identifier_resolve_cache", href: "/zh/reference/settings/session-settings/enable#enable_identifier_resolve_cache" },
        { name: "enable_job_stack_trace", href: "/zh/reference/settings/session-settings/enable#enable_job_stack_trace" },
        { name: "enable_lazy_columns_replication", href: "/zh/reference/settings/session-settings/enable#enable_lazy_columns_replication" },
        { name: "enable_materialized_cte", href: "/zh/reference/settings/session-settings/enable#enable_materialized_cte" },
        { name: "enable_memory_bound_merging_of_aggregation_results", href: "/zh/reference/settings/session-settings/enable#enable_memory_bound_merging_of_aggregation_results" },
        { name: "enable_multiple_prewhere_read_steps", href: "/zh/reference/settings/session-settings/enable#enable_multiple_prewhere_read_steps" },
        { name: "enable_named_columns_in_function_tuple", href: "/zh/reference/settings/session-settings/enable#enable_named_columns_in_function_tuple" },
        { name: "enable_order_by_all", href: "/zh/reference/settings/session-settings/enable#enable_order_by_all" },
        { name: "enable_parallel_blocks_marshalling", href: "/zh/reference/settings/session-settings/enable#enable_parallel_blocks_marshalling" },
        { name: "enable_parsing_to_custom_serialization", href: "/zh/reference/settings/session-settings/enable#enable_parsing_to_custom_serialization" },
        { name: "enable_producing_buckets_out_of_order_in_aggregation", href: "/zh/reference/settings/session-settings/enable#enable_producing_buckets_out_of_order_in_aggregation" },
        { name: "enable_reads_from_query_cache", href: "/zh/reference/settings/session-settings/enable#enable_reads_from_query_cache" },
        { name: "enable_s3_requests_logging", href: "/zh/reference/settings/session-settings/enable#enable_s3_requests_logging" },
        { name: "enable_scalar_subquery_optimization", href: "/zh/reference/settings/session-settings/enable#enable_scalar_subquery_optimization" },
        { name: "enable_scopes_for_with_statement", href: "/zh/reference/settings/session-settings/enable#enable_scopes_for_with_statement" },
        { name: "enable_sharding_aggregator", href: "/zh/reference/settings/session-settings/enable#enable_sharding_aggregator" },
        { name: "enable_shared_storage_snapshot_in_query", href: "/zh/reference/settings/session-settings/enable#enable_shared_storage_snapshot_in_query" },
        { name: "enable_sharing_sets_for_mutations", href: "/zh/reference/settings/session-settings/enable#enable_sharing_sets_for_mutations" },
        { name: "enable_streaming_queries", href: "/zh/reference/settings/session-settings/enable#enable_streaming_queries" },
        { name: "enable_time_time64_type", href: "/zh/reference/settings/session-settings/enable#enable_time_time64_type" },
        { name: "enable_unaligned_array_join", href: "/zh/reference/settings/session-settings/enable#enable_unaligned_array_join" },
        { name: "enable_url_encoding", href: "/zh/reference/settings/session-settings/enable#enable_url_encoding" },
        { name: "enable_vertical_final", href: "/zh/reference/settings/session-settings/enable#enable_vertical_final" },
        { name: "enable_writes_to_query_cache", href: "/zh/reference/settings/session-settings/enable#enable_writes_to_query_cache" }
      ],
      children: []
    },
    {
      label: "enable_blob_storage_log_*",
      count: 2,
      settings: [
        { name: "enable_blob_storage_log", href: "/zh/reference/settings/session-settings/enable-blob-storage-log#enable_blob_storage_log" },
        { name: "enable_blob_storage_log_for_read_operations", href: "/zh/reference/settings/session-settings/enable-blob-storage-log#enable_blob_storage_log_for_read_operations" }
      ],
      children: []
    },
    {
      label: "enable_filesystem_*",
      count: 4,
      settings: [
        { name: "enable_filesystem_cache", href: "/zh/reference/settings/session-settings/enable-filesystem#enable_filesystem_cache" },
        { name: "enable_filesystem_cache_log", href: "/zh/reference/settings/session-settings/enable-filesystem#enable_filesystem_cache_log" },
        { name: "enable_filesystem_cache_on_write_operations", href: "/zh/reference/settings/session-settings/enable-filesystem#enable_filesystem_cache_on_write_operations" },
        { name: "enable_filesystem_read_prefetches_log", href: "/zh/reference/settings/session-settings/enable-filesystem#enable_filesystem_read_prefetches_log" }
      ],
      children: []
    },
    {
      label: "enable_join_*",
      count: 4,
      settings: [
        { name: "enable_join_fixed_hash_table_conversion", href: "/zh/reference/settings/session-settings/enable-join#enable_join_fixed_hash_table_conversion" },
        { name: "enable_join_runtime_filters", href: "/zh/reference/settings/session-settings/enable-join#enable_join_runtime_filters" },
        { name: "enable_join_runtime_filters_index_analysis", href: "/zh/reference/settings/session-settings/enable-join#enable_join_runtime_filters_index_analysis" },
        { name: "enable_join_transitive_predicates", href: "/zh/reference/settings/session-settings/enable-join#enable_join_transitive_predicates" }
      ],
      children: []
    },
    {
      label: "enable_lightweight_*",
      count: 2,
      settings: [
        { name: "enable_lightweight_delete", href: "/zh/reference/settings/session-settings/enable-lightweight#enable_lightweight_delete" },
        { name: "enable_lightweight_update", href: "/zh/reference/settings/session-settings/enable-lightweight#enable_lightweight_update" }
      ],
      children: []
    },
    {
      label: "enable_optimize_predicate_expression_*",
      count: 2,
      settings: [
        { name: "enable_optimize_predicate_expression", href: "/zh/reference/settings/session-settings/enable-optimize-predicate-expression#enable_optimize_predicate_expression" },
        {
          name: "enable_optimize_predicate_expression_to_final_subquery",
          href: "/zh/reference/settings/session-settings/enable-optimize-predicate-expression#enable_optimize_predicate_expression_to_final_subquery"
        }
      ],
      children: []
    },
    {
      label: "enable_positional_arguments_*",
      count: 2,
      settings: [
        { name: "enable_positional_arguments", href: "/zh/reference/settings/session-settings/enable-positional-arguments#enable_positional_arguments" },
        { name: "enable_positional_arguments_for_projections", href: "/zh/reference/settings/session-settings/enable-positional-arguments#enable_positional_arguments_for_projections" }
      ],
      children: []
    },
    {
      label: "enable_software_*",
      count: 2,
      settings: [
        { name: "enable_software_prefetch_in_aggregation", href: "/zh/reference/settings/session-settings/enable-software#enable_software_prefetch_in_aggregation" },
        { name: "enable_software_prefetch_in_join", href: "/zh/reference/settings/session-settings/enable-software#enable_software_prefetch_in_join" }
      ],
      children: []
    },
    {
      label: "engine_file_*",
      count: 4,
      settings: [
        { name: "engine_file_allow_create_multiple_files", href: "/zh/reference/settings/session-settings/engine-file#engine_file_allow_create_multiple_files" },
        { name: "engine_file_empty_if_not_exists", href: "/zh/reference/settings/session-settings/engine-file#engine_file_empty_if_not_exists" },
        { name: "engine_file_skip_empty_files", href: "/zh/reference/settings/session-settings/engine-file#engine_file_skip_empty_files" },
        { name: "engine_file_truncate_on_insert", href: "/zh/reference/settings/session-settings/engine-file#engine_file_truncate_on_insert" }
      ],
      children: []
    },
    {
      label: "external_storage_*",
      count: 4,
      settings: [
        { name: "external_storage_connect_timeout_sec", href: "/zh/reference/settings/session-settings/external-storage#external_storage_connect_timeout_sec" },
        { name: "external_storage_max_read_bytes", href: "/zh/reference/settings/session-settings/external-storage#external_storage_max_read_bytes" },
        { name: "external_storage_max_read_rows", href: "/zh/reference/settings/session-settings/external-storage#external_storage_max_read_rows" },
        { name: "external_storage_rw_timeout_sec", href: "/zh/reference/settings/session-settings/external-storage#external_storage_rw_timeout_sec" }
      ],
      children: []
    },
    {
      label: "external_table_*",
      count: 2,
      settings: [
        { name: "external_table_functions_use_nulls", href: "/zh/reference/settings/session-settings/external-table#external_table_functions_use_nulls" },
        { name: "external_table_strict_query", href: "/zh/reference/settings/session-settings/external-table#external_table_strict_query" }
      ],
      children: []
    },
    {
      label: "filesystem_cache_*",
      count: 10,
      settings: [
        { name: "filesystem_cache_allow_background_download", href: "/zh/reference/settings/session-settings/filesystem-cache#filesystem_cache_allow_background_download" },
        { name: "filesystem_cache_boundary_alignment", href: "/zh/reference/settings/session-settings/filesystem-cache#filesystem_cache_boundary_alignment" },
        { name: "filesystem_cache_enable_background_download_during_fetch", href: "/zh/reference/settings/session-settings/filesystem-cache#filesystem_cache_enable_background_download_during_fetch" },
        {
          name: "filesystem_cache_enable_background_download_for_metadata_files_in_packed_storage",
          href: "/zh/reference/settings/session-settings/filesystem-cache#filesystem_cache_enable_background_download_for_metadata_files_in_packed_storage"
        },
        { name: "filesystem_cache_max_download_size", href: "/zh/reference/settings/session-settings/filesystem-cache#filesystem_cache_max_download_size" },
        { name: "filesystem_cache_name", href: "/zh/reference/settings/session-settings/filesystem-cache#filesystem_cache_name" },
        { name: "filesystem_cache_prefer_bigger_buffer_size", href: "/zh/reference/settings/session-settings/filesystem-cache#filesystem_cache_prefer_bigger_buffer_size" },
        {
          name: "filesystem_cache_reserve_space_wait_lock_timeout_milliseconds",
          href: "/zh/reference/settings/session-settings/filesystem-cache#filesystem_cache_reserve_space_wait_lock_timeout_milliseconds"
        },
        { name: "filesystem_cache_segments_batch_size", href: "/zh/reference/settings/session-settings/filesystem-cache#filesystem_cache_segments_batch_size" },
        {
          name: "filesystem_cache_skip_download_if_exceeds_per_query_cache_write_limit",
          href: "/zh/reference/settings/session-settings/filesystem-cache#filesystem_cache_skip_download_if_exceeds_per_query_cache_write_limit"
        }
      ],
      children: []
    },
    {
      label: "filesystem_prefetch_*",
      count: 3,
      settings: [
        { name: "filesystem_prefetch_max_memory_usage", href: "/zh/reference/settings/session-settings/filesystem-prefetch#filesystem_prefetch_max_memory_usage" },
        { name: "filesystem_prefetch_step_bytes", href: "/zh/reference/settings/session-settings/filesystem-prefetch#filesystem_prefetch_step_bytes" },
        { name: "filesystem_prefetch_step_marks", href: "/zh/reference/settings/session-settings/filesystem-prefetch#filesystem_prefetch_step_marks" }
      ],
      children: []
    },
    {
      label: "force_*",
      count: 7,
      settings: [
        { name: "force_aggregate_partitions_independently", href: "/zh/reference/settings/session-settings/force#force_aggregate_partitions_independently" },
        { name: "force_aggregation_in_order", href: "/zh/reference/settings/session-settings/force#force_aggregation_in_order" },
        { name: "force_data_skipping_indices", href: "/zh/reference/settings/session-settings/force#force_data_skipping_indices" },
        { name: "force_grouping_standard_compatibility", href: "/zh/reference/settings/session-settings/force#force_grouping_standard_compatibility" },
        { name: "force_index_by_date", href: "/zh/reference/settings/session-settings/force#force_index_by_date" },
        { name: "force_primary_key", href: "/zh/reference/settings/session-settings/force#force_primary_key" },
        { name: "force_remove_data_recursively_on_drop", href: "/zh/reference/settings/session-settings/force#force_remove_data_recursively_on_drop" }
      ],
      children: []
    },
    {
      label: "force_optimize_*",
      count: 4,
      settings: [
        { name: "force_optimize_projection", href: "/zh/reference/settings/session-settings/force-optimize#force_optimize_projection" },
        { name: "force_optimize_projection_name", href: "/zh/reference/settings/session-settings/force-optimize#force_optimize_projection_name" },
        { name: "force_optimize_skip_unused_shards", href: "/zh/reference/settings/session-settings/force-optimize#force_optimize_skip_unused_shards" },
        { name: "force_optimize_skip_unused_shards_nesting", href: "/zh/reference/settings/session-settings/force-optimize#force_optimize_skip_unused_shards_nesting" }
      ],
      children: []
    },
    {
      label: "formatdatetime_*",
      count: 3,
      settings: [
        { name: "formatdatetime_e_with_space_padding", href: "/zh/reference/settings/session-settings/formatdatetime#formatdatetime_e_with_space_padding" },
        { name: "formatdatetime_format_without_leading_zeros", href: "/zh/reference/settings/session-settings/formatdatetime#formatdatetime_format_without_leading_zeros" },
        { name: "formatdatetime_parsedatetime_m_is_month_name", href: "/zh/reference/settings/session-settings/formatdatetime#formatdatetime_parsedatetime_m_is_month_name" }
      ],
      children: []
    },
    {
      label: "formatdatetime_f_*",
      count: 2,
      settings: [
        { name: "formatdatetime_f_prints_scale_number_of_digits", href: "/zh/reference/settings/session-settings/formatdatetime-f#formatdatetime_f_prints_scale_number_of_digits" },
        { name: "formatdatetime_f_prints_single_zero", href: "/zh/reference/settings/session-settings/formatdatetime-f#formatdatetime_f_prints_single_zero" }
      ],
      children: []
    },
    {
      label: "function_*",
      count: 7,
      settings: [
        { name: "function_base58_max_input_size", href: "/zh/reference/settings/session-settings/function#function_base58_max_input_size" },
        { name: "function_date_trunc_return_type_behavior", href: "/zh/reference/settings/session-settings/function#function_date_trunc_return_type_behavior" },
        { name: "function_implementation", href: "/zh/reference/settings/session-settings/function#function_implementation" },
        { name: "function_locate_has_mysql_compatible_argument_order", href: "/zh/reference/settings/session-settings/function#function_locate_has_mysql_compatible_argument_order" },
        { name: "function_range_max_elements_in_block", href: "/zh/reference/settings/session-settings/function#function_range_max_elements_in_block" },
        { name: "function_sleep_max_microseconds_per_block", href: "/zh/reference/settings/session-settings/function#function_sleep_max_microseconds_per_block" },
        { name: "function_visible_width_behavior", href: "/zh/reference/settings/session-settings/function#function_visible_width_behavior" }
      ],
      children: []
    },
    {
      label: "function_json_*",
      count: 2,
      settings: [
        { name: "function_json_value_return_type_allow_complex", href: "/zh/reference/settings/session-settings/function-json#function_json_value_return_type_allow_complex" },
        { name: "function_json_value_return_type_allow_nullable", href: "/zh/reference/settings/session-settings/function-json#function_json_value_return_type_allow_nullable" }
      ],
      children: []
    },
    {
      label: "grace_hash_*",
      count: 2,
      settings: [
        { name: "grace_hash_join_initial_buckets", href: "/zh/reference/settings/session-settings/grace-hash#grace_hash_join_initial_buckets" },
        { name: "grace_hash_join_max_buckets", href: "/zh/reference/settings/session-settings/grace-hash#grace_hash_join_max_buckets" }
      ],
      children: []
    },
    {
      label: "group_by_*",
      count: 4,
      settings: [
        { name: "group_by_overflow_mode", href: "/zh/reference/settings/session-settings/group-by#group_by_overflow_mode" },
        { name: "group_by_two_level_threshold", href: "/zh/reference/settings/session-settings/group-by#group_by_two_level_threshold" },
        { name: "group_by_two_level_threshold_bytes", href: "/zh/reference/settings/session-settings/group-by#group_by_two_level_threshold_bytes" },
        { name: "group_by_use_nulls", href: "/zh/reference/settings/session-settings/group-by#group_by_use_nulls" }
      ],
      children: []
    },
    {
      label: "hdfs_*",
      count: 6,
      settings: [
        { name: "hdfs_create_new_file_on_insert", href: "/zh/reference/settings/session-settings/hdfs#hdfs_create_new_file_on_insert" },
        { name: "hdfs_ignore_file_doesnt_exist", href: "/zh/reference/settings/session-settings/hdfs#hdfs_ignore_file_doesnt_exist" },
        { name: "hdfs_replication", href: "/zh/reference/settings/session-settings/hdfs#hdfs_replication" },
        { name: "hdfs_skip_empty_files", href: "/zh/reference/settings/session-settings/hdfs#hdfs_skip_empty_files" },
        { name: "hdfs_throw_on_zero_files_match", href: "/zh/reference/settings/session-settings/hdfs#hdfs_throw_on_zero_files_match" },
        { name: "hdfs_truncate_on_insert", href: "/zh/reference/settings/session-settings/hdfs#hdfs_truncate_on_insert" }
      ],
      children: []
    },
    {
      label: "http_*",
      count: 9,
      settings: [
        { name: "http_connection_timeout", href: "/zh/reference/settings/session-settings/http#http_connection_timeout" },
        { name: "http_make_head_request", href: "/zh/reference/settings/session-settings/http#http_make_head_request" },
        { name: "http_native_compression_disable_checksumming_on_decompress", href: "/zh/reference/settings/session-settings/http#http_native_compression_disable_checksumming_on_decompress" },
        { name: "http_receive_timeout", href: "/zh/reference/settings/session-settings/http#http_receive_timeout" },
        { name: "http_send_timeout", href: "/zh/reference/settings/session-settings/http#http_send_timeout" },
        { name: "http_skip_not_found_url_for_globs", href: "/zh/reference/settings/session-settings/http#http_skip_not_found_url_for_globs" },
        { name: "http_wait_end_of_query", href: "/zh/reference/settings/session-settings/http#http_wait_end_of_query" },
        { name: "http_write_exception_in_output_format", href: "/zh/reference/settings/session-settings/http#http_write_exception_in_output_format" },
        { name: "http_zlib_compression_level", href: "/zh/reference/settings/session-settings/http#http_zlib_compression_level" }
      ],
      children: []
    },
    {
      label: "http_headers_*",
      count: 2,
      settings: [
        { name: "http_headers_progress_interval_ms", href: "/zh/reference/settings/session-settings/http-headers#http_headers_progress_interval_ms" },
        { name: "http_headers_read_timeout", href: "/zh/reference/settings/session-settings/http-headers#http_headers_read_timeout" }
      ],
      children: []
    },
    {
      label: "http_max_*",
      count: 8,
      settings: [
        { name: "http_max_field_name_size", href: "/zh/reference/settings/session-settings/http-max#http_max_field_name_size" },
        { name: "http_max_field_value_size", href: "/zh/reference/settings/session-settings/http-max#http_max_field_value_size" },
        { name: "http_max_fields", href: "/zh/reference/settings/session-settings/http-max#http_max_fields" },
        { name: "http_max_multipart_form_data_size", href: "/zh/reference/settings/session-settings/http-max#http_max_multipart_form_data_size" },
        { name: "http_max_request_header_size", href: "/zh/reference/settings/session-settings/http-max#http_max_request_header_size" },
        { name: "http_max_request_param_data_size", href: "/zh/reference/settings/session-settings/http-max#http_max_request_param_data_size" },
        { name: "http_max_tries", href: "/zh/reference/settings/session-settings/http-max#http_max_tries" },
        { name: "http_max_uri_size", href: "/zh/reference/settings/session-settings/http-max#http_max_uri_size" }
      ],
      children: []
    },
    {
      label: "http_response_*",
      count: 2,
      settings: [
        { name: "http_response_buffer_size", href: "/zh/reference/settings/session-settings/http-response#http_response_buffer_size" },
        { name: "http_response_headers", href: "/zh/reference/settings/session-settings/http-response#http_response_headers" }
      ],
      children: []
    },
    {
      label: "http_retry_*",
      count: 2,
      settings: [
        { name: "http_retry_initial_backoff_ms", href: "/zh/reference/settings/session-settings/http-retry#http_retry_initial_backoff_ms" },
        { name: "http_retry_max_backoff_ms", href: "/zh/reference/settings/session-settings/http-retry#http_retry_max_backoff_ms" }
      ],
      children: []
    },
    {
      label: "iceberg_*",
      count: 6,
      settings: [
        { name: "iceberg_delete_data_on_drop", href: "/zh/reference/settings/session-settings/iceberg#iceberg_delete_data_on_drop" },
        { name: "iceberg_manifest_min_count_to_compact", href: "/zh/reference/settings/session-settings/iceberg#iceberg_manifest_min_count_to_compact" },
        { name: "iceberg_max_number_datafiles_to_compact", href: "/zh/reference/settings/session-settings/iceberg#iceberg_max_number_datafiles_to_compact" },
        { name: "iceberg_orphan_files_older_than_seconds", href: "/zh/reference/settings/session-settings/iceberg#iceberg_orphan_files_older_than_seconds" },
        { name: "iceberg_snapshot_id", href: "/zh/reference/settings/session-settings/iceberg#iceberg_snapshot_id" },
        { name: "iceberg_timestamp_ms", href: "/zh/reference/settings/session-settings/iceberg#iceberg_timestamp_ms" }
      ],
      children: []
    },
    {
      label: "iceberg_compaction_*",
      count: 2,
      settings: [
        { name: "iceberg_compaction_data_cleanup", href: "/zh/reference/settings/session-settings/iceberg-compaction#iceberg_compaction_data_cleanup" },
        { name: "iceberg_compaction_delay_bias", href: "/zh/reference/settings/session-settings/iceberg-compaction#iceberg_compaction_delay_bias" }
      ],
      children: []
    },
    {
      label: "iceberg_data_*",
      count: 2,
      settings: [
        { name: "iceberg_data_file_size_lower_threshold_compaction", href: "/zh/reference/settings/session-settings/iceberg-data#iceberg_data_file_size_lower_threshold_compaction" },
        { name: "iceberg_data_file_size_upper_threshold_compaction", href: "/zh/reference/settings/session-settings/iceberg-data#iceberg_data_file_size_upper_threshold_compaction" }
      ],
      children: []
    },
    {
      label: "iceberg_expire_*",
      count: 3,
      settings: [
        { name: "iceberg_expire_default_max_ref_age_ms", href: "/zh/reference/settings/session-settings/iceberg-expire#iceberg_expire_default_max_ref_age_ms" },
        { name: "iceberg_expire_default_max_snapshot_age_ms", href: "/zh/reference/settings/session-settings/iceberg-expire#iceberg_expire_default_max_snapshot_age_ms" },
        { name: "iceberg_expire_default_min_snapshots_to_keep", href: "/zh/reference/settings/session-settings/iceberg-expire#iceberg_expire_default_min_snapshots_to_keep" }
      ],
      children: []
    },
    {
      label: "iceberg_insert_*",
      count: 3,
      settings: [
        { name: "iceberg_insert_max_bytes_in_data_file", href: "/zh/reference/settings/session-settings/iceberg-insert#iceberg_insert_max_bytes_in_data_file" },
        { name: "iceberg_insert_max_partitions", href: "/zh/reference/settings/session-settings/iceberg-insert#iceberg_insert_max_partitions" },
        { name: "iceberg_insert_max_rows_in_data_file", href: "/zh/reference/settings/session-settings/iceberg-insert#iceberg_insert_max_rows_in_data_file" }
      ],
      children: []
    },
    {
      label: "iceberg_metadata_*",
      count: 3,
      settings: [
        { name: "iceberg_metadata_compression_method", href: "/zh/reference/settings/session-settings/iceberg-metadata#iceberg_metadata_compression_method" },
        { name: "iceberg_metadata_log_level", href: "/zh/reference/settings/session-settings/iceberg-metadata#iceberg_metadata_log_level" },
        { name: "iceberg_metadata_staleness_ms", href: "/zh/reference/settings/session-settings/iceberg-metadata#iceberg_metadata_staleness_ms" }
      ],
      children: []
    },
    {
      label: "ignore_*",
      count: 5,
      settings: [
        { name: "ignore_cold_parts_seconds", href: "/zh/reference/settings/session-settings/ignore#ignore_cold_parts_seconds" },
        { name: "ignore_data_skipping_indices", href: "/zh/reference/settings/session-settings/ignore#ignore_data_skipping_indices" },
        { name: "ignore_drop_queries_probability", href: "/zh/reference/settings/session-settings/ignore#ignore_drop_queries_probability" },
        { name: "ignore_format_null_for_explain", href: "/zh/reference/settings/session-settings/ignore#ignore_format_null_for_explain" },
        { name: "ignore_materialized_views_with_dropped_target_table", href: "/zh/reference/settings/session-settings/ignore#ignore_materialized_views_with_dropped_target_table" }
      ],
      children: []
    },
    {
      label: "ignore_on_*",
      count: 4,
      settings: [
        { name: "ignore_on_cluster_for_replicated_access_entities_queries", href: "/zh/reference/settings/session-settings/ignore-on#ignore_on_cluster_for_replicated_access_entities_queries" },
        { name: "ignore_on_cluster_for_replicated_database", href: "/zh/reference/settings/session-settings/ignore-on#ignore_on_cluster_for_replicated_database" },
        { name: "ignore_on_cluster_for_replicated_named_collections_queries", href: "/zh/reference/settings/session-settings/ignore-on#ignore_on_cluster_for_replicated_named_collections_queries" },
        { name: "ignore_on_cluster_for_replicated_udf_queries", href: "/zh/reference/settings/session-settings/ignore-on#ignore_on_cluster_for_replicated_udf_queries" }
      ],
      children: []
    },
    {
      label: "implicit_*",
      count: 3,
      settings: [
        { name: "implicit_select", href: "/zh/reference/settings/session-settings/implicit#implicit_select" },
        { name: "implicit_table_at_top_level", href: "/zh/reference/settings/session-settings/implicit#implicit_table_at_top_level" },
        { name: "implicit_transaction", href: "/zh/reference/settings/session-settings/implicit#implicit_transaction" }
      ],
      children: []
    },
    {
      label: "insert_*",
      count: 5,
      settings: [
        { name: "insert_allow_materialized_columns", href: "/zh/reference/settings/session-settings/insert#insert_allow_materialized_columns" },
        { name: "insert_deduplicate", href: "/zh/reference/settings/session-settings/insert#insert_deduplicate" },
        { name: "insert_deduplication_token", href: "/zh/reference/settings/session-settings/insert#insert_deduplication_token" },
        { name: "insert_null_as_default", href: "/zh/reference/settings/session-settings/insert#insert_null_as_default" },
        { name: "insert_shard_id", href: "/zh/reference/settings/session-settings/insert#insert_shard_id" }
      ],
      children: []
    },
    {
      label: "insert_keeper_*",
      count: 5,
      settings: [
        { name: "insert_keeper_fault_injection_probability", href: "/zh/reference/settings/session-settings/insert-keeper#insert_keeper_fault_injection_probability" },
        { name: "insert_keeper_fault_injection_seed", href: "/zh/reference/settings/session-settings/insert-keeper#insert_keeper_fault_injection_seed" },
        { name: "insert_keeper_max_retries", href: "/zh/reference/settings/session-settings/insert-keeper#insert_keeper_max_retries" },
        { name: "insert_keeper_retry_initial_backoff_ms", href: "/zh/reference/settings/session-settings/insert-keeper#insert_keeper_retry_initial_backoff_ms" },
        { name: "insert_keeper_retry_max_backoff_ms", href: "/zh/reference/settings/session-settings/insert-keeper#insert_keeper_retry_max_backoff_ms" }
      ],
      children: []
    },
    {
      label: "insert_quorum_*",
      count: 3,
      settings: [
        { name: "insert_quorum", href: "/zh/reference/settings/session-settings/insert-quorum#insert_quorum" },
        { name: "insert_quorum_parallel", href: "/zh/reference/settings/session-settings/insert-quorum#insert_quorum_parallel" },
        { name: "insert_quorum_timeout", href: "/zh/reference/settings/session-settings/insert-quorum#insert_quorum_timeout" }
      ],
      children: []
    },
    {
      label: "jemalloc_*",
      count: 2,
      settings: [
        { name: "jemalloc_collect_profile_samples_in_trace_log", href: "/zh/reference/settings/session-settings/jemalloc#jemalloc_collect_profile_samples_in_trace_log" },
        { name: "jemalloc_enable_profiler", href: "/zh/reference/settings/session-settings/jemalloc#jemalloc_enable_profiler" }
      ],
      children: []
    },
    {
      label: "jemalloc_profile_*",
      count: 3,
      settings: [
        { name: "jemalloc_profile_text_collapsed_use_count", href: "/zh/reference/settings/session-settings/jemalloc-profile#jemalloc_profile_text_collapsed_use_count" },
        { name: "jemalloc_profile_text_output_format", href: "/zh/reference/settings/session-settings/jemalloc-profile#jemalloc_profile_text_output_format" },
        { name: "jemalloc_profile_text_symbolize_with_inline", href: "/zh/reference/settings/session-settings/jemalloc-profile#jemalloc_profile_text_symbolize_with_inline" }
      ],
      children: []
    },
    {
      label: "join_*",
      count: 7,
      settings: [
        { name: "join_algorithm", href: "/zh/reference/settings/session-settings/join#join_algorithm" },
        { name: "join_any_take_last_row", href: "/zh/reference/settings/session-settings/join#join_any_take_last_row" },
        { name: "join_default_strictness", href: "/zh/reference/settings/session-settings/join#join_default_strictness" },
        { name: "join_on_disk_max_files_to_merge", href: "/zh/reference/settings/session-settings/join#join_on_disk_max_files_to_merge" },
        { name: "join_output_by_rowlist_perkey_rows_threshold", href: "/zh/reference/settings/session-settings/join#join_output_by_rowlist_perkey_rows_threshold" },
        { name: "join_overflow_mode", href: "/zh/reference/settings/session-settings/join#join_overflow_mode" },
        { name: "join_use_nulls", href: "/zh/reference/settings/session-settings/join#join_use_nulls" }
      ],
      children: []
    },
    {
      label: "join_runtime_*",
      count: 8,
      settings: [
        { name: "join_runtime_bloom_filter_bytes", href: "/zh/reference/settings/session-settings/join-runtime#join_runtime_bloom_filter_bytes" },
        { name: "join_runtime_bloom_filter_hash_functions", href: "/zh/reference/settings/session-settings/join-runtime#join_runtime_bloom_filter_hash_functions" },
        { name: "join_runtime_bloom_filter_max_ratio_of_set_bits", href: "/zh/reference/settings/session-settings/join-runtime#join_runtime_bloom_filter_max_ratio_of_set_bits" },
        { name: "join_runtime_filter_blocks_to_skip_before_reenabling", href: "/zh/reference/settings/session-settings/join-runtime#join_runtime_filter_blocks_to_skip_before_reenabling" },
        { name: "join_runtime_filter_exact_values_limit", href: "/zh/reference/settings/session-settings/join-runtime#join_runtime_filter_exact_values_limit" },
        { name: "join_runtime_filter_from_fixed_hash_table", href: "/zh/reference/settings/session-settings/join-runtime#join_runtime_filter_from_fixed_hash_table" },
        { name: "join_runtime_filter_pass_ratio_threshold_for_disabling", href: "/zh/reference/settings/session-settings/join-runtime#join_runtime_filter_pass_ratio_threshold_for_disabling" },
        { name: "join_runtime_filter_size_from_hash_table_stats", href: "/zh/reference/settings/session-settings/join-runtime#join_runtime_filter_size_from_hash_table_stats" }
      ],
      children: []
    },
    {
      label: "join_to_*",
      count: 2,
      settings: [
        { name: "join_to_sort_maximum_table_rows", href: "/zh/reference/settings/session-settings/join-to#join_to_sort_maximum_table_rows" },
        { name: "join_to_sort_minimum_perkey_rows", href: "/zh/reference/settings/session-settings/join-to#join_to_sort_minimum_perkey_rows" }
      ],
      children: []
    },
    {
      label: "joined_*",
      count: 2,
      settings: [
        { name: "joined_block_split_single_row", href: "/zh/reference/settings/session-settings/joined#joined_block_split_single_row" },
        { name: "joined_subquery_requires_alias", href: "/zh/reference/settings/session-settings/joined#joined_subquery_requires_alias" }
      ],
      children: []
    },
    {
      label: "kafka_*",
      count: 2,
      settings: [
        { name: "kafka_disable_num_consumers_limit", href: "/zh/reference/settings/session-settings/kafka#kafka_disable_num_consumers_limit" },
        { name: "kafka_max_wait_ms", href: "/zh/reference/settings/session-settings/kafka#kafka_max_wait_ms" }
      ],
      children: []
    },
    {
      label: "keeper_*",
      count: 2,
      settings: [
        { name: "keeper_map_strict_mode", href: "/zh/reference/settings/session-settings/keeper#keeper_map_strict_mode" },
        { name: "keeper_max_retries", href: "/zh/reference/settings/session-settings/keeper#keeper_max_retries" }
      ],
      children: []
    },
    {
      label: "keeper_retry_*",
      count: 2,
      settings: [
        { name: "keeper_retry_initial_backoff_ms", href: "/zh/reference/settings/session-settings/keeper-retry#keeper_retry_initial_backoff_ms" },
        { name: "keeper_retry_max_backoff_ms", href: "/zh/reference/settings/session-settings/keeper-retry#keeper_retry_max_backoff_ms" }
      ],
      children: []
    },
    {
      label: "lightweight_*",
      count: 2,
      settings: [
        { name: "lightweight_delete_mode", href: "/zh/reference/settings/session-settings/lightweight#lightweight_delete_mode" },
        { name: "lightweight_deletes_sync", href: "/zh/reference/settings/session-settings/lightweight#lightweight_deletes_sync" }
      ],
      children: []
    },
    {
      label: "load_balancing_*",
      count: 2,
      settings: [
        { name: "load_balancing", href: "/zh/reference/settings/session-settings/load-balancing#load_balancing" },
        { name: "load_balancing_first_offset", href: "/zh/reference/settings/session-settings/load-balancing#load_balancing_first_offset" }
      ],
      children: []
    },
    {
      label: "local_filesystem_*",
      count: 2,
      settings: [
        { name: "local_filesystem_read_method", href: "/zh/reference/settings/session-settings/local-filesystem#local_filesystem_read_method" },
        { name: "local_filesystem_read_prefetch", href: "/zh/reference/settings/session-settings/local-filesystem#local_filesystem_read_prefetch" }
      ],
      children: []
    },
    {
      label: "log_*",
      count: 4,
      settings: [
        { name: "log_comment", href: "/zh/reference/settings/session-settings/log#log_comment" },
        { name: "log_formatted_queries", href: "/zh/reference/settings/session-settings/log#log_formatted_queries" },
        { name: "log_processors_profiles", href: "/zh/reference/settings/session-settings/log#log_processors_profiles" },
        { name: "log_profile_events", href: "/zh/reference/settings/session-settings/log#log_profile_events" }
      ],
      children: []
    },
    {
      label: "log_queries_*",
      count: 5,
      settings: [
        { name: "log_queries", href: "/zh/reference/settings/session-settings/log-queries#log_queries" },
        { name: "log_queries_cut_to_length", href: "/zh/reference/settings/session-settings/log-queries#log_queries_cut_to_length" },
        { name: "log_queries_min_query_duration_ms", href: "/zh/reference/settings/session-settings/log-queries#log_queries_min_query_duration_ms" },
        { name: "log_queries_min_type", href: "/zh/reference/settings/session-settings/log-queries#log_queries_min_type" },
        { name: "log_queries_probability", href: "/zh/reference/settings/session-settings/log-queries#log_queries_probability" }
      ],
      children: []
    },
    {
      label: "log_query_*",
      count: 3,
      settings: [
        { name: "log_query_settings", href: "/zh/reference/settings/session-settings/log-query#log_query_settings" },
        { name: "log_query_threads", href: "/zh/reference/settings/session-settings/log-query#log_query_threads" },
        { name: "log_query_views", href: "/zh/reference/settings/session-settings/log-query#log_query_views" }
      ],
      children: []
    },
    {
      label: "low_cardinality_*",
      count: 3,
      settings: [
        { name: "low_cardinality_allow_in_native_format", href: "/zh/reference/settings/session-settings/low-cardinality#low_cardinality_allow_in_native_format" },
        { name: "low_cardinality_max_dictionary_size", href: "/zh/reference/settings/session-settings/low-cardinality#low_cardinality_max_dictionary_size" },
        { name: "low_cardinality_use_single_dictionary_for_part", href: "/zh/reference/settings/session-settings/low-cardinality#low_cardinality_use_single_dictionary_for_part" }
      ],
      children: []
    },
    {
      label: "materialize_*",
      count: 3,
      settings: [
        { name: "materialize_skip_indexes_on_insert", href: "/zh/reference/settings/session-settings/materialize#materialize_skip_indexes_on_insert" },
        { name: "materialize_statistics_on_insert", href: "/zh/reference/settings/session-settings/materialize#materialize_statistics_on_insert" },
        { name: "materialize_ttl_after_modify", href: "/zh/reference/settings/session-settings/materialize#materialize_ttl_after_modify" }
      ],
      children: []
    },
    {
      label: "materialized_views_*",
      count: 2,
      settings: [
        { name: "materialized_views_ignore_errors", href: "/zh/reference/settings/session-settings/materialized-views#materialized_views_ignore_errors" },
        { name: "materialized_views_squash_parallel_inserts", href: "/zh/reference/settings/session-settings/materialized-views#materialized_views_squash_parallel_inserts" }
      ],
      children: []
    },
    {
      label: "max_*",
      count: 29,
      settings: [
        { name: "max_analyze_depth", href: "/zh/reference/settings/session-settings/max#max_analyze_depth" },
        { name: "max_autoincrement_series", href: "/zh/reference/settings/session-settings/max#max_autoincrement_series" },
        { name: "max_backup_bandwidth", href: "/zh/reference/settings/session-settings/max#max_backup_bandwidth" },
        { name: "max_block_size", href: "/zh/reference/settings/session-settings/max#max_block_size" },
        { name: "max_columns_to_read", href: "/zh/reference/settings/session-settings/max#max_columns_to_read" },
        { name: "max_compress_block_size", href: "/zh/reference/settings/session-settings/max#max_compress_block_size" },
        { name: "max_consume_snapshots", href: "/zh/reference/settings/session-settings/max#max_consume_snapshots" },
        { name: "max_estimated_execution_time", href: "/zh/reference/settings/session-settings/max#max_estimated_execution_time" },
        { name: "max_expanded_ast_elements", href: "/zh/reference/settings/session-settings/max#max_expanded_ast_elements" },
        { name: "max_fetch_partition_retries_count", href: "/zh/reference/settings/session-settings/max#max_fetch_partition_retries_count" },
        { name: "max_final_threads", href: "/zh/reference/settings/session-settings/max#max_final_threads" },
        { name: "max_http_get_redirects", href: "/zh/reference/settings/session-settings/max#max_http_get_redirects" },
        { name: "max_limit_for_vector_search_queries", href: "/zh/reference/settings/session-settings/max#max_limit_for_vector_search_queries" },
        { name: "max_number_of_partitions_for_independent_aggregation", href: "/zh/reference/settings/session-settings/max#max_number_of_partitions_for_independent_aggregation" },
        { name: "max_os_cpu_wait_time_ratio_to_throw", href: "/zh/reference/settings/session-settings/max#max_os_cpu_wait_time_ratio_to_throw" },
        { name: "max_parallel_replicas", href: "/zh/reference/settings/session-settings/max#max_parallel_replicas" },
        { name: "max_parsing_threads", href: "/zh/reference/settings/session-settings/max#max_parsing_threads" },
        { name: "max_partition_size_to_drop", href: "/zh/reference/settings/session-settings/max#max_partition_size_to_drop" },
        { name: "max_parts_to_move", href: "/zh/reference/settings/session-settings/max#max_parts_to_move" },
        { name: "max_projection_rows_to_use_projection_index", href: "/zh/reference/settings/session-settings/max#max_projection_rows_to_use_projection_index" },
        { name: "max_query_size", href: "/zh/reference/settings/session-settings/max#max_query_size" },
        { name: "max_recursive_cte_evaluation_depth", href: "/zh/reference/settings/session-settings/max#max_recursive_cte_evaluation_depth" },
        { name: "max_replica_delay_for_distributed_queries", href: "/zh/reference/settings/session-settings/max#max_replica_delay_for_distributed_queries" },
        { name: "max_reverse_dictionary_lookup_cache_size_bytes", href: "/zh/reference/settings/session-settings/max#max_reverse_dictionary_lookup_cache_size_bytes" },
        { name: "max_sessions_for_user", href: "/zh/reference/settings/session-settings/max#max_sessions_for_user" },
        { name: "max_subquery_depth", href: "/zh/reference/settings/session-settings/max#max_subquery_depth" },
        { name: "max_table_size_to_drop", href: "/zh/reference/settings/session-settings/max#max_table_size_to_drop" },
        { name: "max_untracked_memory", href: "/zh/reference/settings/session-settings/max#max_untracked_memory" },
        { name: "max_wkb_geometry_elements", href: "/zh/reference/settings/session-settings/max#max_wkb_geometry_elements" }
      ],
      children: []
    },
    {
      label: "max_ast_*",
      count: 2,
      settings: [
        { name: "max_ast_depth", href: "/zh/reference/settings/session-settings/max-ast#max_ast_depth" },
        { name: "max_ast_elements", href: "/zh/reference/settings/session-settings/max-ast#max_ast_elements" }
      ],
      children: []
    },
    {
      label: "max_bytes_*",
      count: 15,
      settings: [
        { name: "max_bytes_before_external_group_by", href: "/zh/reference/settings/session-settings/max-bytes#max_bytes_before_external_group_by" },
        { name: "max_bytes_before_external_join", href: "/zh/reference/settings/session-settings/max-bytes#max_bytes_before_external_join" },
        { name: "max_bytes_before_external_sort", href: "/zh/reference/settings/session-settings/max-bytes#max_bytes_before_external_sort" },
        { name: "max_bytes_before_remerge_sort", href: "/zh/reference/settings/session-settings/max-bytes#max_bytes_before_remerge_sort" },
        { name: "max_bytes_for_lazy_final", href: "/zh/reference/settings/session-settings/max-bytes#max_bytes_for_lazy_final" },
        { name: "max_bytes_in_distinct", href: "/zh/reference/settings/session-settings/max-bytes#max_bytes_in_distinct" },
        { name: "max_bytes_in_join", href: "/zh/reference/settings/session-settings/max-bytes#max_bytes_in_join" },
        { name: "max_bytes_in_set", href: "/zh/reference/settings/session-settings/max-bytes#max_bytes_in_set" },
        { name: "max_bytes_ratio_before_external_group_by", href: "/zh/reference/settings/session-settings/max-bytes#max_bytes_ratio_before_external_group_by" },
        { name: "max_bytes_ratio_before_external_join", href: "/zh/reference/settings/session-settings/max-bytes#max_bytes_ratio_before_external_join" },
        { name: "max_bytes_ratio_before_external_sort", href: "/zh/reference/settings/session-settings/max-bytes#max_bytes_ratio_before_external_sort" },
        { name: "max_bytes_to_read", href: "/zh/reference/settings/session-settings/max-bytes#max_bytes_to_read" },
        { name: "max_bytes_to_read_leaf", href: "/zh/reference/settings/session-settings/max-bytes#max_bytes_to_read_leaf" },
        { name: "max_bytes_to_sort", href: "/zh/reference/settings/session-settings/max-bytes#max_bytes_to_sort" },
        { name: "max_bytes_to_transfer", href: "/zh/reference/settings/session-settings/max-bytes#max_bytes_to_transfer" }
      ],
      children: []
    },
    {
      label: "max_concurrent_*",
      count: 2,
      settings: [
        { name: "max_concurrent_queries_for_all_users", href: "/zh/reference/settings/session-settings/max-concurrent#max_concurrent_queries_for_all_users" },
        { name: "max_concurrent_queries_for_user", href: "/zh/reference/settings/session-settings/max-concurrent#max_concurrent_queries_for_user" }
      ],
      children: []
    },
    {
      label: "max_distributed_*",
      count: 2,
      settings: [
        { name: "max_distributed_connections", href: "/zh/reference/settings/session-settings/max-distributed#max_distributed_connections" },
        { name: "max_distributed_depth", href: "/zh/reference/settings/session-settings/max-distributed#max_distributed_depth" }
      ],
      children: []
    },
    {
      label: "max_download_*",
      count: 2,
      settings: [
        { name: "max_download_buffer_size", href: "/zh/reference/settings/session-settings/max-download#max_download_buffer_size" },
        { name: "max_download_threads", href: "/zh/reference/settings/session-settings/max-download#max_download_threads" }
      ],
      children: []
    },
    {
      label: "max_execution_*",
      count: 4,
      settings: [
        { name: "max_execution_speed", href: "/zh/reference/settings/session-settings/max-execution#max_execution_speed" },
        { name: "max_execution_speed_bytes", href: "/zh/reference/settings/session-settings/max-execution#max_execution_speed_bytes" },
        { name: "max_execution_time", href: "/zh/reference/settings/session-settings/max-execution#max_execution_time" },
        { name: "max_execution_time_leaf", href: "/zh/reference/settings/session-settings/max-execution#max_execution_time_leaf" }
      ],
      children: []
    },
    {
      label: "max_hyperscan_*",
      count: 2,
      settings: [
        { name: "max_hyperscan_regexp_length", href: "/zh/reference/settings/session-settings/max-hyperscan#max_hyperscan_regexp_length" },
        { name: "max_hyperscan_regexp_total_length", href: "/zh/reference/settings/session-settings/max-hyperscan#max_hyperscan_regexp_total_length" }
      ],
      children: []
    },
    {
      label: "max_insert_*",
      count: 5,
      settings: [
        { name: "max_insert_block_size", href: "/zh/reference/settings/session-settings/max-insert#max_insert_block_size" },
        { name: "max_insert_block_size_bytes", href: "/zh/reference/settings/session-settings/max-insert#max_insert_block_size_bytes" },
        { name: "max_insert_delayed_streams_for_parallel_write", href: "/zh/reference/settings/session-settings/max-insert#max_insert_delayed_streams_for_parallel_write" },
        { name: "max_insert_threads", href: "/zh/reference/settings/session-settings/max-insert#max_insert_threads" },
        { name: "max_insert_threads_min_free_memory_per_thread", href: "/zh/reference/settings/session-settings/max-insert#max_insert_threads_min_free_memory_per_thread" }
      ],
      children: []
    },
    {
      label: "max_joined_*",
      count: 2,
      settings: [
        { name: "max_joined_block_size_bytes", href: "/zh/reference/settings/session-settings/max-joined#max_joined_block_size_bytes" },
        { name: "max_joined_block_size_rows", href: "/zh/reference/settings/session-settings/max-joined#max_joined_block_size_rows" }
      ],
      children: []
    },
    {
      label: "max_local_*",
      count: 2,
      settings: [
        { name: "max_local_read_bandwidth", href: "/zh/reference/settings/session-settings/max-local#max_local_read_bandwidth" },
        { name: "max_local_write_bandwidth", href: "/zh/reference/settings/session-settings/max-local#max_local_write_bandwidth" }
      ],
      children: []
    },
    {
      label: "max_memory_usage_*",
      count: 2,
      settings: [
        { name: "max_memory_usage", href: "/zh/reference/settings/session-settings/max-memory-usage#max_memory_usage" },
        { name: "max_memory_usage_for_user", href: "/zh/reference/settings/session-settings/max-memory-usage#max_memory_usage_for_user" }
      ],
      children: []
    },
    {
      label: "max_network_*",
      count: 4,
      settings: [
        { name: "max_network_bandwidth", href: "/zh/reference/settings/session-settings/max-network#max_network_bandwidth" },
        { name: "max_network_bandwidth_for_all_users", href: "/zh/reference/settings/session-settings/max-network#max_network_bandwidth_for_all_users" },
        { name: "max_network_bandwidth_for_user", href: "/zh/reference/settings/session-settings/max-network#max_network_bandwidth_for_user" },
        { name: "max_network_bytes", href: "/zh/reference/settings/session-settings/max-network#max_network_bytes" }
      ],
      children: []
    },
    {
      label: "max_parser_*",
      count: 2,
      settings: [
        { name: "max_parser_backtracks", href: "/zh/reference/settings/session-settings/max-parser#max_parser_backtracks" },
        { name: "max_parser_depth", href: "/zh/reference/settings/session-settings/max-parser#max_parser_depth" }
      ],
      children: []
    },
    {
      label: "max_partitions_*",
      count: 2,
      settings: [
        { name: "max_partitions_per_insert_block", href: "/zh/reference/settings/session-settings/max-partitions#max_partitions_per_insert_block" },
        { name: "max_partitions_to_read", href: "/zh/reference/settings/session-settings/max-partitions#max_partitions_to_read" }
      ],
      children: []
    },
    {
      label: "max_rand_*",
      count: 2,
      settings: [
        { name: "max_rand_distribution_parameter", href: "/zh/reference/settings/session-settings/max-rand#max_rand_distribution_parameter" },
        { name: "max_rand_distribution_trials", href: "/zh/reference/settings/session-settings/max-rand#max_rand_distribution_trials" }
      ],
      children: []
    },
    {
      label: "max_read_buffer_size_*",
      count: 3,
      settings: [
        { name: "max_read_buffer_size", href: "/zh/reference/settings/session-settings/max-read-buffer-size#max_read_buffer_size" },
        { name: "max_read_buffer_size_local_fs", href: "/zh/reference/settings/session-settings/max-read-buffer-size#max_read_buffer_size_local_fs" },
        { name: "max_read_buffer_size_remote_fs", href: "/zh/reference/settings/session-settings/max-read-buffer-size#max_read_buffer_size_remote_fs" }
      ],
      children: []
    },
    {
      label: "max_remote_*",
      count: 2,
      settings: [
        { name: "max_remote_read_network_bandwidth", href: "/zh/reference/settings/session-settings/max-remote#max_remote_read_network_bandwidth" },
        { name: "max_remote_write_network_bandwidth", href: "/zh/reference/settings/session-settings/max-remote#max_remote_write_network_bandwidth" }
      ],
      children: []
    },
    {
      label: "max_result_*",
      count: 2,
      settings: [
        { name: "max_result_bytes", href: "/zh/reference/settings/session-settings/max-result#max_result_bytes" },
        { name: "max_result_rows", href: "/zh/reference/settings/session-settings/max-result#max_result_rows" }
      ],
      children: []
    },
    {
      label: "max_rows_*",
      count: 10,
      settings: [
        { name: "max_rows_for_lazy_final", href: "/zh/reference/settings/session-settings/max-rows#max_rows_for_lazy_final" },
        { name: "max_rows_in_distinct", href: "/zh/reference/settings/session-settings/max-rows#max_rows_in_distinct" },
        { name: "max_rows_in_join", href: "/zh/reference/settings/session-settings/max-rows#max_rows_in_join" },
        { name: "max_rows_in_set", href: "/zh/reference/settings/session-settings/max-rows#max_rows_in_set" },
        { name: "max_rows_in_set_to_optimize_join", href: "/zh/reference/settings/session-settings/max-rows#max_rows_in_set_to_optimize_join" },
        { name: "max_rows_to_group_by", href: "/zh/reference/settings/session-settings/max-rows#max_rows_to_group_by" },
        { name: "max_rows_to_read", href: "/zh/reference/settings/session-settings/max-rows#max_rows_to_read" },
        { name: "max_rows_to_read_leaf", href: "/zh/reference/settings/session-settings/max-rows#max_rows_to_read_leaf" },
        { name: "max_rows_to_sort", href: "/zh/reference/settings/session-settings/max-rows#max_rows_to_sort" },
        { name: "max_rows_to_transfer", href: "/zh/reference/settings/session-settings/max-rows#max_rows_to_transfer" }
      ],
      children: []
    },
    {
      label: "max_size_*",
      count: 2,
      settings: [
        { name: "max_size_to_preallocate_for_aggregation", href: "/zh/reference/settings/session-settings/max-size#max_size_to_preallocate_for_aggregation" },
        { name: "max_size_to_preallocate_for_joins", href: "/zh/reference/settings/session-settings/max-size#max_size_to_preallocate_for_joins" }
      ],
      children: []
    },
    {
      label: "max_skip_*",
      count: 2,
      settings: [
        { name: "max_skip_unavailable_shards_num", href: "/zh/reference/settings/session-settings/max-skip#max_skip_unavailable_shards_num" },
        { name: "max_skip_unavailable_shards_ratio", href: "/zh/reference/settings/session-settings/max-skip#max_skip_unavailable_shards_ratio" }
      ],
      children: []
    },
    {
      label: "max_streams_*",
      count: 6,
      settings: [
        { name: "max_streams_for_files_processing_in_cluster_functions", href: "/zh/reference/settings/session-settings/max-streams#max_streams_for_files_processing_in_cluster_functions" },
        { name: "max_streams_for_merge_tree_reading", href: "/zh/reference/settings/session-settings/max-streams#max_streams_for_merge_tree_reading" },
        { name: "max_streams_for_union_step", href: "/zh/reference/settings/session-settings/max-streams#max_streams_for_union_step" },
        { name: "max_streams_for_union_step_to_max_threads_ratio", href: "/zh/reference/settings/session-settings/max-streams#max_streams_for_union_step_to_max_threads_ratio" },
        { name: "max_streams_multiplier_for_merge_tables", href: "/zh/reference/settings/session-settings/max-streams#max_streams_multiplier_for_merge_tables" },
        { name: "max_streams_to_max_threads_ratio", href: "/zh/reference/settings/session-settings/max-streams#max_streams_to_max_threads_ratio" }
      ],
      children: []
    },
    {
      label: "max_temporary_*",
      count: 4,
      settings: [
        { name: "max_temporary_columns", href: "/zh/reference/settings/session-settings/max-temporary#max_temporary_columns" },
        { name: "max_temporary_data_on_disk_size_for_query", href: "/zh/reference/settings/session-settings/max-temporary#max_temporary_data_on_disk_size_for_query" },
        { name: "max_temporary_data_on_disk_size_for_user", href: "/zh/reference/settings/session-settings/max-temporary#max_temporary_data_on_disk_size_for_user" },
        { name: "max_temporary_non_const_columns", href: "/zh/reference/settings/session-settings/max-temporary#max_temporary_non_const_columns" }
      ],
      children: []
    },
    {
      label: "max_threads_*",
      count: 3,
      settings: [
        { name: "max_threads", href: "/zh/reference/settings/session-settings/max-threads#max_threads" },
        { name: "max_threads_for_indexes", href: "/zh/reference/settings/session-settings/max-threads#max_threads_for_indexes" },
        { name: "max_threads_min_free_memory_per_thread", href: "/zh/reference/settings/session-settings/max-threads#max_threads_min_free_memory_per_thread" }
      ],
      children: []
    },
    {
      label: "memory_*",
      count: 2,
      settings: [
        { name: "memory_tracker_fault_probability", href: "/zh/reference/settings/session-settings/memory#memory_tracker_fault_probability" },
        { name: "memory_usage_overcommit_max_wait_microseconds", href: "/zh/reference/settings/session-settings/memory#memory_usage_overcommit_max_wait_microseconds" }
      ],
      children: []
    },
    {
      label: "memory_overcommit_ratio_denominator_*",
      count: 2,
      settings: [
        { name: "memory_overcommit_ratio_denominator", href: "/zh/reference/settings/session-settings/memory-overcommit-ratio-denominator#memory_overcommit_ratio_denominator" },
        { name: "memory_overcommit_ratio_denominator_for_user", href: "/zh/reference/settings/session-settings/memory-overcommit-ratio-denominator#memory_overcommit_ratio_denominator_for_user" }
      ],
      children: []
    },
    {
      label: "memory_profiler_*",
      count: 4,
      settings: [
        { name: "memory_profiler_sample_max_allocation_size", href: "/zh/reference/settings/session-settings/memory-profiler#memory_profiler_sample_max_allocation_size" },
        { name: "memory_profiler_sample_min_allocation_size", href: "/zh/reference/settings/session-settings/memory-profiler#memory_profiler_sample_min_allocation_size" },
        { name: "memory_profiler_sample_probability", href: "/zh/reference/settings/session-settings/memory-profiler#memory_profiler_sample_probability" },
        { name: "memory_profiler_step", href: "/zh/reference/settings/session-settings/memory-profiler#memory_profiler_step" }
      ],
      children: []
    },
    {
      label: "merge_tree_*",
      count: 20,
      settings: [
        { name: "merge_tree_coarse_index_granularity", href: "/zh/reference/settings/session-settings/merge-tree#merge_tree_coarse_index_granularity" },
        { name: "merge_tree_compact_parts_min_granules_to_multibuffer_read", href: "/zh/reference/settings/session-settings/merge-tree#merge_tree_compact_parts_min_granules_to_multibuffer_read" },
        { name: "merge_tree_determine_task_size_by_prewhere_columns", href: "/zh/reference/settings/session-settings/merge-tree#merge_tree_determine_task_size_by_prewhere_columns" },
        { name: "merge_tree_generic_exclusion_search_max_steps", href: "/zh/reference/settings/session-settings/merge-tree#merge_tree_generic_exclusion_search_max_steps" },
        { name: "merge_tree_max_bytes_to_use_cache", href: "/zh/reference/settings/session-settings/merge-tree#merge_tree_max_bytes_to_use_cache" },
        { name: "merge_tree_max_rows_to_use_cache", href: "/zh/reference/settings/session-settings/merge-tree#merge_tree_max_rows_to_use_cache" },
        { name: "merge_tree_min_bytes_for_concurrent_read", href: "/zh/reference/settings/session-settings/merge-tree#merge_tree_min_bytes_for_concurrent_read" },
        {
          name: "merge_tree_min_bytes_for_concurrent_read_for_remote_filesystem",
          href: "/zh/reference/settings/session-settings/merge-tree#merge_tree_min_bytes_for_concurrent_read_for_remote_filesystem"
        },
        { name: "merge_tree_min_bytes_for_seek", href: "/zh/reference/settings/session-settings/merge-tree#merge_tree_min_bytes_for_seek" },
        { name: "merge_tree_min_bytes_per_task_for_remote_reading", href: "/zh/reference/settings/session-settings/merge-tree#merge_tree_min_bytes_per_task_for_remote_reading" },
        { name: "merge_tree_min_read_task_size", href: "/zh/reference/settings/session-settings/merge-tree#merge_tree_min_read_task_size" },
        { name: "merge_tree_min_rows_for_concurrent_read", href: "/zh/reference/settings/session-settings/merge-tree#merge_tree_min_rows_for_concurrent_read" },
        {
          name: "merge_tree_min_rows_for_concurrent_read_for_remote_filesystem",
          href: "/zh/reference/settings/session-settings/merge-tree#merge_tree_min_rows_for_concurrent_read_for_remote_filesystem"
        },
        { name: "merge_tree_min_rows_for_seek", href: "/zh/reference/settings/session-settings/merge-tree#merge_tree_min_rows_for_seek" },
        {
          name: "merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability",
          href: "/zh/reference/settings/session-settings/merge-tree#merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability"
        },
        { name: "merge_tree_storage_snapshot_sleep_ms", href: "/zh/reference/settings/session-settings/merge-tree#merge_tree_storage_snapshot_sleep_ms" },
        { name: "merge_tree_use_const_size_tasks_for_remote_reading", href: "/zh/reference/settings/session-settings/merge-tree#merge_tree_use_const_size_tasks_for_remote_reading" },
        { name: "merge_tree_use_deserialization_prefixes_cache", href: "/zh/reference/settings/session-settings/merge-tree#merge_tree_use_deserialization_prefixes_cache" },
        { name: "merge_tree_use_prefixes_deserialization_thread_pool", href: "/zh/reference/settings/session-settings/merge-tree#merge_tree_use_prefixes_deserialization_thread_pool" },
        { name: "merge_tree_use_v1_object_and_dynamic_serialization", href: "/zh/reference/settings/session-settings/merge-tree#merge_tree_use_v1_object_and_dynamic_serialization" }
      ],
      children: []
    },
    {
      label: "metrics_perf_*",
      count: 2,
      settings: [
        { name: "metrics_perf_events_enabled", href: "/zh/reference/settings/session-settings/metrics-perf#metrics_perf_events_enabled" },
        { name: "metrics_perf_events_list", href: "/zh/reference/settings/session-settings/metrics-perf#metrics_perf_events_list" }
      ],
      children: []
    },
    {
      label: "min_*",
      count: 7,
      settings: [
        { name: "min_chunk_bytes_for_parallel_parsing", href: "/zh/reference/settings/session-settings/min#min_chunk_bytes_for_parallel_parsing" },
        { name: "min_compress_block_size", href: "/zh/reference/settings/session-settings/min#min_compress_block_size" },
        { name: "min_filtered_ratio_for_lazy_final", href: "/zh/reference/settings/session-settings/min#min_filtered_ratio_for_lazy_final" },
        { name: "min_hit_rate_to_use_consecutive_keys_optimization", href: "/zh/reference/settings/session-settings/min#min_hit_rate_to_use_consecutive_keys_optimization" },
        { name: "min_os_cpu_wait_time_ratio_to_throw", href: "/zh/reference/settings/session-settings/min#min_os_cpu_wait_time_ratio_to_throw" },
        { name: "min_outstreams_per_resize_after_split", href: "/zh/reference/settings/session-settings/min#min_outstreams_per_resize_after_split" },
        { name: "min_table_rows_to_use_projection_index", href: "/zh/reference/settings/session-settings/min#min_table_rows_to_use_projection_index" }
      ],
      children: []
    },
    {
      label: "min_bytes_*",
      count: 2,
      settings: [
        { name: "min_bytes_to_use_direct_io", href: "/zh/reference/settings/session-settings/min-bytes#min_bytes_to_use_direct_io" },
        { name: "min_bytes_to_use_mmap_io", href: "/zh/reference/settings/session-settings/min-bytes#min_bytes_to_use_mmap_io" }
      ],
      children: []
    },
    {
      label: "min_count_*",
      count: 4,
      settings: [
        { name: "min_count_to_compile_aggregate_expression", href: "/zh/reference/settings/session-settings/min-count#min_count_to_compile_aggregate_expression" },
        { name: "min_count_to_compile_expression", href: "/zh/reference/settings/session-settings/min-count#min_count_to_compile_expression" },
        { name: "min_count_to_compile_regular_expression", href: "/zh/reference/settings/session-settings/min-count#min_count_to_compile_regular_expression" },
        { name: "min_count_to_compile_sort_description", href: "/zh/reference/settings/session-settings/min-count#min_count_to_compile_sort_description" }
      ],
      children: []
    },
    {
      label: "min_execution_speed_*",
      count: 2,
      settings: [
        { name: "min_execution_speed", href: "/zh/reference/settings/session-settings/min-execution-speed#min_execution_speed" },
        { name: "min_execution_speed_bytes", href: "/zh/reference/settings/session-settings/min-execution-speed#min_execution_speed_bytes" }
      ],
      children: []
    },
    {
      label: "min_external_*",
      count: 2,
      settings: [
        { name: "min_external_table_block_size_bytes", href: "/zh/reference/settings/session-settings/min-external#min_external_table_block_size_bytes" },
        { name: "min_external_table_block_size_rows", href: "/zh/reference/settings/session-settings/min-external#min_external_table_block_size_rows" }
      ],
      children: []
    },
    {
      label: "min_free_*",
      count: 3,
      settings: [
        { name: "min_free_disk_bytes_to_perform_insert", href: "/zh/reference/settings/session-settings/min-free#min_free_disk_bytes_to_perform_insert" },
        { name: "min_free_disk_ratio_to_perform_insert", href: "/zh/reference/settings/session-settings/min-free#min_free_disk_ratio_to_perform_insert" },
        { name: "min_free_disk_space_for_temporary_data", href: "/zh/reference/settings/session-settings/min-free#min_free_disk_space_for_temporary_data" }
      ],
      children: []
    },
    {
      label: "min_insert_*",
      count: 4,
      settings: [
        { name: "min_insert_block_size_bytes", href: "/zh/reference/settings/session-settings/min-insert#min_insert_block_size_bytes" },
        { name: "min_insert_block_size_bytes_for_materialized_views", href: "/zh/reference/settings/session-settings/min-insert#min_insert_block_size_bytes_for_materialized_views" },
        { name: "min_insert_block_size_rows", href: "/zh/reference/settings/session-settings/min-insert#min_insert_block_size_rows" },
        { name: "min_insert_block_size_rows_for_materialized_views", href: "/zh/reference/settings/session-settings/min-insert#min_insert_block_size_rows_for_materialized_views" }
      ],
      children: []
    },
    {
      label: "min_joined_*",
      count: 2,
      settings: [
        { name: "min_joined_block_size_bytes", href: "/zh/reference/settings/session-settings/min-joined#min_joined_block_size_bytes" },
        { name: "min_joined_block_size_rows", href: "/zh/reference/settings/session-settings/min-joined#min_joined_block_size_rows" }
      ],
      children: []
    },
    {
      label: "move_*",
      count: 2,
      settings: [
        { name: "move_all_conditions_to_prewhere", href: "/zh/reference/settings/session-settings/move#move_all_conditions_to_prewhere" },
        { name: "move_primary_key_columns_to_end_of_prewhere", href: "/zh/reference/settings/session-settings/move#move_primary_key_columns_to_end_of_prewhere" }
      ],
      children: []
    },
    {
      label: "mutations_*",
      count: 2,
      settings: [
        { name: "mutations_max_literal_size_to_replace", href: "/zh/reference/settings/session-settings/mutations#mutations_max_literal_size_to_replace" },
        { name: "mutations_sync", href: "/zh/reference/settings/session-settings/mutations#mutations_sync" }
      ],
      children: []
    },
    {
      label: "mutations_execute_*",
      count: 2,
      settings: [
        { name: "mutations_execute_nondeterministic_on_initiator", href: "/zh/reference/settings/session-settings/mutations-execute#mutations_execute_nondeterministic_on_initiator" },
        { name: "mutations_execute_subqueries_on_initiator", href: "/zh/reference/settings/session-settings/mutations-execute#mutations_execute_subqueries_on_initiator" }
      ],
      children: []
    },
    {
      label: "mysql_*",
      count: 2,
      settings: [
        { name: "mysql_datatypes_support_level", href: "/zh/reference/settings/session-settings/mysql#mysql_datatypes_support_level" },
        { name: "mysql_max_rows_to_insert", href: "/zh/reference/settings/session-settings/mysql#mysql_max_rows_to_insert" }
      ],
      children: []
    },
    {
      label: "mysql_map_*",
      count: 2,
      settings: [
        { name: "mysql_map_fixed_string_to_text_in_show_columns", href: "/zh/reference/settings/session-settings/mysql-map#mysql_map_fixed_string_to_text_in_show_columns" },
        { name: "mysql_map_string_to_text_in_show_columns", href: "/zh/reference/settings/session-settings/mysql-map#mysql_map_string_to_text_in_show_columns" }
      ],
      children: []
    },
    {
      label: "network_*",
      count: 2,
      settings: [
        { name: "network_compression_method", href: "/zh/reference/settings/session-settings/network#network_compression_method" },
        { name: "network_zstd_compression_level", href: "/zh/reference/settings/session-settings/network#network_zstd_compression_level" }
      ],
      children: []
    },
    {
      label: "number_of_*",
      count: 2,
      settings: [
        { name: "number_of_mutations_to_delay", href: "/zh/reference/settings/session-settings/number-of#number_of_mutations_to_delay" },
        { name: "number_of_mutations_to_throw", href: "/zh/reference/settings/session-settings/number-of#number_of_mutations_to_throw" }
      ],
      children: []
    },
    {
      label: "odbc_bridge_*",
      count: 2,
      settings: [
        { name: "odbc_bridge_connection_pool_size", href: "/zh/reference/settings/session-settings/odbc-bridge#odbc_bridge_connection_pool_size" },
        { name: "odbc_bridge_use_connection_pooling", href: "/zh/reference/settings/session-settings/odbc-bridge#odbc_bridge_use_connection_pooling" }
      ],
      children: []
    },
    {
      label: "opentelemetry_start_*",
      count: 2,
      settings: [
        { name: "opentelemetry_start_keeper_trace_probability", href: "/zh/reference/settings/session-settings/opentelemetry-start#opentelemetry_start_keeper_trace_probability" },
        { name: "opentelemetry_start_trace_probability", href: "/zh/reference/settings/session-settings/opentelemetry-start#opentelemetry_start_trace_probability" }
      ],
      children: []
    },
    {
      label: "opentelemetry_trace_*",
      count: 2,
      settings: [
        { name: "opentelemetry_trace_cpu_scheduling", href: "/zh/reference/settings/session-settings/opentelemetry-trace#opentelemetry_trace_cpu_scheduling" },
        { name: "opentelemetry_trace_processors", href: "/zh/reference/settings/session-settings/opentelemetry-trace#opentelemetry_trace_processors" }
      ],
      children: []
    },
    {
      label: "optimize_*",
      count: 28,
      settings: [
        { name: "optimize_aggregators_of_group_by_keys", href: "/zh/reference/settings/session-settings/optimize#optimize_aggregators_of_group_by_keys" },
        { name: "optimize_append_index", href: "/zh/reference/settings/session-settings/optimize#optimize_append_index" },
        { name: "optimize_arithmetic_operations_in_aggregate_functions", href: "/zh/reference/settings/session-settings/optimize#optimize_arithmetic_operations_in_aggregate_functions" },
        { name: "optimize_const_name_size", href: "/zh/reference/settings/session-settings/optimize#optimize_const_name_size" },
        { name: "optimize_count_from_files", href: "/zh/reference/settings/session-settings/optimize#optimize_count_from_files" },
        { name: "optimize_dictget_tuple_element", href: "/zh/reference/settings/session-settings/optimize#optimize_dictget_tuple_element" },
        { name: "optimize_distinct_in_order", href: "/zh/reference/settings/session-settings/optimize#optimize_distinct_in_order" },
        { name: "optimize_distributed_group_by_sharding_key", href: "/zh/reference/settings/session-settings/optimize#optimize_distributed_group_by_sharding_key" },
        { name: "optimize_dry_run_check_part", href: "/zh/reference/settings/session-settings/optimize#optimize_dry_run_check_part" },
        { name: "optimize_empty_string_comparisons", href: "/zh/reference/settings/session-settings/optimize#optimize_empty_string_comparisons" },
        { name: "optimize_extract_common_expressions", href: "/zh/reference/settings/session-settings/optimize#optimize_extract_common_expressions" },
        { name: "optimize_functions_to_subcolumns", href: "/zh/reference/settings/session-settings/optimize#optimize_functions_to_subcolumns" },
        { name: "optimize_inverse_dictionary_lookup", href: "/zh/reference/settings/session-settings/optimize#optimize_inverse_dictionary_lookup" },
        { name: "optimize_multiif_to_if", href: "/zh/reference/settings/session-settings/optimize#optimize_multiif_to_if" },
        { name: "optimize_normalize_count_variants", href: "/zh/reference/settings/session-settings/optimize#optimize_normalize_count_variants" },
        { name: "optimize_on_insert", href: "/zh/reference/settings/session-settings/optimize#optimize_on_insert" },
        { name: "optimize_prewhere_after_pushdown", href: "/zh/reference/settings/session-settings/optimize#optimize_prewhere_after_pushdown" },
        { name: "optimize_qbit_distance_function_reads", href: "/zh/reference/settings/session-settings/optimize#optimize_qbit_distance_function_reads" },
        { name: "optimize_read_in_order", href: "/zh/reference/settings/session-settings/optimize#optimize_read_in_order" },
        { name: "optimize_respect_aliases", href: "/zh/reference/settings/session-settings/optimize#optimize_respect_aliases" },
        { name: "optimize_sorting_by_input_stream_properties", href: "/zh/reference/settings/session-settings/optimize#optimize_sorting_by_input_stream_properties" },
        { name: "optimize_substitute_columns", href: "/zh/reference/settings/session-settings/optimize#optimize_substitute_columns" },
        { name: "optimize_syntax_fuse_functions", href: "/zh/reference/settings/session-settings/optimize#optimize_syntax_fuse_functions" },
        { name: "optimize_throw_if_noop", href: "/zh/reference/settings/session-settings/optimize#optimize_throw_if_noop" },
        { name: "optimize_time_filter_with_preimage", href: "/zh/reference/settings/session-settings/optimize#optimize_time_filter_with_preimage" },
        { name: "optimize_truncate_order_by_after_group_by_keys", href: "/zh/reference/settings/session-settings/optimize#optimize_truncate_order_by_after_group_by_keys" },
        { name: "optimize_uniq_to_count", href: "/zh/reference/settings/session-settings/optimize#optimize_uniq_to_count" },
        { name: "optimize_using_constraints", href: "/zh/reference/settings/session-settings/optimize#optimize_using_constraints" }
      ],
      children: []
    },
    {
      label: "optimize_aggregation_in_order_*",
      count: 2,
      settings: [
        { name: "optimize_aggregation_in_order", href: "/zh/reference/settings/session-settings/optimize-aggregation-in-order#optimize_aggregation_in_order" },
        { name: "optimize_aggregation_in_order_limit", href: "/zh/reference/settings/session-settings/optimize-aggregation-in-order#optimize_aggregation_in_order_limit" }
      ],
      children: []
    },
    {
      label: "optimize_and_compare_chain_*",
      count: 2,
      settings: [
        { name: "optimize_and_compare_chain", href: "/zh/reference/settings/session-settings/optimize-and-compare-chain#optimize_and_compare_chain" },
        { name: "optimize_and_compare_chain_max_hash_work", href: "/zh/reference/settings/session-settings/optimize-and-compare-chain#optimize_and_compare_chain_max_hash_work" }
      ],
      children: []
    },
    {
      label: "optimize_group_*",
      count: 2,
      settings: [
        { name: "optimize_group_by_constant_keys", href: "/zh/reference/settings/session-settings/optimize-group#optimize_group_by_constant_keys" },
        { name: "optimize_group_by_function_keys", href: "/zh/reference/settings/session-settings/optimize-group#optimize_group_by_function_keys" }
      ],
      children: []
    },
    {
      label: "optimize_if_*",
      count: 2,
      settings: [
        { name: "optimize_if_chain_to_multiif", href: "/zh/reference/settings/session-settings/optimize-if#optimize_if_chain_to_multiif" },
        { name: "optimize_if_transform_strings_to_enum", href: "/zh/reference/settings/session-settings/optimize-if#optimize_if_transform_strings_to_enum" }
      ],
      children: []
    },
    {
      label: "optimize_injective_*",
      count: 3,
      settings: [
        { name: "optimize_injective_functions_in_group_by", href: "/zh/reference/settings/session-settings/optimize-injective#optimize_injective_functions_in_group_by" },
        { name: "optimize_injective_functions_in_limit_by", href: "/zh/reference/settings/session-settings/optimize-injective#optimize_injective_functions_in_limit_by" },
        { name: "optimize_injective_functions_inside_uniq", href: "/zh/reference/settings/session-settings/optimize-injective#optimize_injective_functions_inside_uniq" }
      ],
      children: []
    },
    {
      label: "optimize_limit_*",
      count: 2,
      settings: [
        { name: "optimize_limit_by_function_keys", href: "/zh/reference/settings/session-settings/optimize-limit#optimize_limit_by_function_keys" },
        { name: "optimize_limit_by_in_order", href: "/zh/reference/settings/session-settings/optimize-limit#optimize_limit_by_in_order" }
      ],
      children: []
    },
    {
      label: "optimize_min_*",
      count: 2,
      settings: [
        { name: "optimize_min_equality_disjunction_chain_length", href: "/zh/reference/settings/session-settings/optimize-min#optimize_min_equality_disjunction_chain_length" },
        { name: "optimize_min_inequality_conjunction_chain_length", href: "/zh/reference/settings/session-settings/optimize-min#optimize_min_inequality_conjunction_chain_length" }
      ],
      children: []
    },
    {
      label: "optimize_move_to_prewhere_*",
      count: 2,
      settings: [
        { name: "optimize_move_to_prewhere", href: "/zh/reference/settings/session-settings/optimize-move-to-prewhere#optimize_move_to_prewhere" },
        { name: "optimize_move_to_prewhere_if_final", href: "/zh/reference/settings/session-settings/optimize-move-to-prewhere#optimize_move_to_prewhere_if_final" }
      ],
      children: []
    },
    {
      label: "optimize_or_like_chain_*",
      count: 3,
      settings: [
        { name: "optimize_or_like_chain", href: "/zh/reference/settings/session-settings/optimize-or-like-chain#optimize_or_like_chain" },
        { name: "optimize_or_like_chain_min_patterns", href: "/zh/reference/settings/session-settings/optimize-or-like-chain#optimize_or_like_chain_min_patterns" },
        { name: "optimize_or_like_chain_min_substrings", href: "/zh/reference/settings/session-settings/optimize-or-like-chain#optimize_or_like_chain_min_substrings" }
      ],
      children: []
    },
    {
      label: "optimize_redundant_*",
      count: 2,
      settings: [
        { name: "optimize_redundant_comparisons", href: "/zh/reference/settings/session-settings/optimize-redundant#optimize_redundant_comparisons" },
        { name: "optimize_redundant_functions_in_order_by", href: "/zh/reference/settings/session-settings/optimize-redundant#optimize_redundant_functions_in_order_by" }
      ],
      children: []
    },
    {
      label: "optimize_rewrite_*",
      count: 6,
      settings: [
        { name: "optimize_rewrite_aggregate_function_with_if", href: "/zh/reference/settings/session-settings/optimize-rewrite#optimize_rewrite_aggregate_function_with_if" },
        { name: "optimize_rewrite_array_exists_to_has", href: "/zh/reference/settings/session-settings/optimize-rewrite#optimize_rewrite_array_exists_to_has" },
        { name: "optimize_rewrite_has_to_in", href: "/zh/reference/settings/session-settings/optimize-rewrite#optimize_rewrite_has_to_in" },
        { name: "optimize_rewrite_like_perfect_affix", href: "/zh/reference/settings/session-settings/optimize-rewrite#optimize_rewrite_like_perfect_affix" },
        { name: "optimize_rewrite_regexp_functions", href: "/zh/reference/settings/session-settings/optimize-rewrite#optimize_rewrite_regexp_functions" },
        { name: "optimize_rewrite_sum_if_to_count_if", href: "/zh/reference/settings/session-settings/optimize-rewrite#optimize_rewrite_sum_if_to_count_if" }
      ],
      children: []
    },
    {
      label: "optimize_skip_*",
      count: 5,
      settings: [
        { name: "optimize_skip_merged_partitions", href: "/zh/reference/settings/session-settings/optimize-skip#optimize_skip_merged_partitions" },
        { name: "optimize_skip_unused_shards", href: "/zh/reference/settings/session-settings/optimize-skip#optimize_skip_unused_shards" },
        { name: "optimize_skip_unused_shards_limit", href: "/zh/reference/settings/session-settings/optimize-skip#optimize_skip_unused_shards_limit" },
        { name: "optimize_skip_unused_shards_nesting", href: "/zh/reference/settings/session-settings/optimize-skip#optimize_skip_unused_shards_nesting" },
        { name: "optimize_skip_unused_shards_rewrite_in", href: "/zh/reference/settings/session-settings/optimize-skip#optimize_skip_unused_shards_rewrite_in" }
      ],
      children: []
    },
    {
      label: "optimize_trivial_*",
      count: 5,
      settings: [
        { name: "optimize_trivial_approximate_count_query", href: "/zh/reference/settings/session-settings/optimize-trivial#optimize_trivial_approximate_count_query" },
        { name: "optimize_trivial_count_query", href: "/zh/reference/settings/session-settings/optimize-trivial#optimize_trivial_count_query" },
        { name: "optimize_trivial_count_with_sparsity_filter", href: "/zh/reference/settings/session-settings/optimize-trivial#optimize_trivial_count_with_sparsity_filter" },
        { name: "optimize_trivial_group_by_limit_query", href: "/zh/reference/settings/session-settings/optimize-trivial#optimize_trivial_group_by_limit_query" },
        { name: "optimize_trivial_insert_select", href: "/zh/reference/settings/session-settings/optimize-trivial#optimize_trivial_insert_select" }
      ],
      children: []
    },
    {
      label: "optimize_use_*",
      count: 3,
      settings: [
        { name: "optimize_use_implicit_projections", href: "/zh/reference/settings/session-settings/optimize-use#optimize_use_implicit_projections" },
        { name: "optimize_use_projection_filtering", href: "/zh/reference/settings/session-settings/optimize-use#optimize_use_projection_filtering" },
        { name: "optimize_use_projections", href: "/zh/reference/settings/session-settings/optimize-use#optimize_use_projections" }
      ],
      children: []
    },
    {
      label: "os_threads_*",
      count: 2,
      settings: [
        { name: "os_threads_nice_value_materialized_view", href: "/zh/reference/settings/session-settings/os-threads#os_threads_nice_value_materialized_view" },
        { name: "os_threads_nice_value_query", href: "/zh/reference/settings/session-settings/os-threads#os_threads_nice_value_query" }
      ],
      children: []
    },
    {
      label: "page_cache_*",
      count: 4,
      settings: [
        { name: "page_cache_block_size", href: "/zh/reference/settings/session-settings/page-cache#page_cache_block_size" },
        { name: "page_cache_inject_eviction", href: "/zh/reference/settings/session-settings/page-cache#page_cache_inject_eviction" },
        { name: "page_cache_lookahead_blocks", href: "/zh/reference/settings/session-settings/page-cache#page_cache_lookahead_blocks" },
        { name: "page_cache_max_coalesced_bytes", href: "/zh/reference/settings/session-settings/page-cache#page_cache_max_coalesced_bytes" }
      ],
      children: []
    },
    {
      label: "parallel_*",
      count: 5,
      settings: [
        { name: "parallel_distributed_insert_select", href: "/zh/reference/settings/session-settings/parallel#parallel_distributed_insert_select" },
        { name: "parallel_hash_join_threshold", href: "/zh/reference/settings/session-settings/parallel#parallel_hash_join_threshold" },
        { name: "parallel_non_joined_rows_processing", href: "/zh/reference/settings/session-settings/parallel#parallel_non_joined_rows_processing" },
        { name: "parallel_replica_offset", href: "/zh/reference/settings/session-settings/parallel#parallel_replica_offset" },
        { name: "parallel_view_processing", href: "/zh/reference/settings/session-settings/parallel#parallel_view_processing" }
      ],
      children: []
    },
    {
      label: "parallel_replicas_*",
      count: 22,
      settings: [
        { name: "parallel_replicas_allow_in_with_subquery", href: "/zh/reference/settings/session-settings/parallel-replicas#parallel_replicas_allow_in_with_subquery" },
        { name: "parallel_replicas_allow_materialized_views", href: "/zh/reference/settings/session-settings/parallel-replicas#parallel_replicas_allow_materialized_views" },
        { name: "parallel_replicas_allow_view_over_mergetree", href: "/zh/reference/settings/session-settings/parallel-replicas#parallel_replicas_allow_view_over_mergetree" },
        { name: "parallel_replicas_connect_timeout_ms", href: "/zh/reference/settings/session-settings/parallel-replicas#parallel_replicas_connect_timeout_ms" },
        { name: "parallel_replicas_count", href: "/zh/reference/settings/session-settings/parallel-replicas#parallel_replicas_count" },
        { name: "parallel_replicas_custom_key", href: "/zh/reference/settings/session-settings/parallel-replicas#parallel_replicas_custom_key" },
        { name: "parallel_replicas_custom_key_range_lower", href: "/zh/reference/settings/session-settings/parallel-replicas#parallel_replicas_custom_key_range_lower" },
        { name: "parallel_replicas_custom_key_range_upper", href: "/zh/reference/settings/session-settings/parallel-replicas#parallel_replicas_custom_key_range_upper" },
        { name: "parallel_replicas_filter_pushdown", href: "/zh/reference/settings/session-settings/parallel-replicas#parallel_replicas_filter_pushdown" },
        { name: "parallel_replicas_for_cluster_engines", href: "/zh/reference/settings/session-settings/parallel-replicas#parallel_replicas_for_cluster_engines" },
        { name: "parallel_replicas_for_non_replicated_merge_tree", href: "/zh/reference/settings/session-settings/parallel-replicas#parallel_replicas_for_non_replicated_merge_tree" },
        { name: "parallel_replicas_index_analysis_only_on_coordinator", href: "/zh/reference/settings/session-settings/parallel-replicas#parallel_replicas_index_analysis_only_on_coordinator" },
        { name: "parallel_replicas_insert_select_local_pipeline", href: "/zh/reference/settings/session-settings/parallel-replicas#parallel_replicas_insert_select_local_pipeline" },
        { name: "parallel_replicas_local_plan", href: "/zh/reference/settings/session-settings/parallel-replicas#parallel_replicas_local_plan" },
        { name: "parallel_replicas_mark_segment_size", href: "/zh/reference/settings/session-settings/parallel-replicas#parallel_replicas_mark_segment_size" },
        { name: "parallel_replicas_min_number_of_rows_per_replica", href: "/zh/reference/settings/session-settings/parallel-replicas#parallel_replicas_min_number_of_rows_per_replica" },
        { name: "parallel_replicas_mode", href: "/zh/reference/settings/session-settings/parallel-replicas#parallel_replicas_mode" },
        { name: "parallel_replicas_only_with_analyzer", href: "/zh/reference/settings/session-settings/parallel-replicas#parallel_replicas_only_with_analyzer" },
        { name: "parallel_replicas_plan_based", href: "/zh/reference/settings/session-settings/parallel-replicas#parallel_replicas_plan_based" },
        { name: "parallel_replicas_prefer_local_join", href: "/zh/reference/settings/session-settings/parallel-replicas#parallel_replicas_prefer_local_join" },
        { name: "parallel_replicas_prefer_local_replica", href: "/zh/reference/settings/session-settings/parallel-replicas#parallel_replicas_prefer_local_replica" },
        { name: "parallel_replicas_support_projection", href: "/zh/reference/settings/session-settings/parallel-replicas#parallel_replicas_support_projection" }
      ],
      children: []
    },
    {
      label: "parsedatetime_*",
      count: 2,
      settings: [
        { name: "parsedatetime_e_requires_space_padding", href: "/zh/reference/settings/session-settings/parsedatetime#parsedatetime_e_requires_space_padding" },
        { name: "parsedatetime_parse_without_leading_zeros", href: "/zh/reference/settings/session-settings/parsedatetime#parsedatetime_parse_without_leading_zeros" }
      ],
      children: []
    },
    {
      label: "partial_merge_*",
      count: 2,
      settings: [
        { name: "partial_merge_join_left_table_buffer_bytes", href: "/zh/reference/settings/session-settings/partial-merge#partial_merge_join_left_table_buffer_bytes" },
        { name: "partial_merge_join_rows_in_right_blocks", href: "/zh/reference/settings/session-settings/partial-merge#partial_merge_join_rows_in_right_blocks" }
      ],
      children: []
    },
    {
      label: "parts_to_*",
      count: 2,
      settings: [
        { name: "parts_to_delay_insert", href: "/zh/reference/settings/session-settings/parts-to#parts_to_delay_insert" },
        { name: "parts_to_throw_insert", href: "/zh/reference/settings/session-settings/parts-to#parts_to_throw_insert" }
      ],
      children: []
    },
    {
      label: "postgresql_connection_*",
      count: 5,
      settings: [
        { name: "postgresql_connection_attempt_timeout", href: "/zh/reference/settings/session-settings/postgresql-connection#postgresql_connection_attempt_timeout" },
        { name: "postgresql_connection_pool_auto_close_connection", href: "/zh/reference/settings/session-settings/postgresql-connection#postgresql_connection_pool_auto_close_connection" },
        { name: "postgresql_connection_pool_retries", href: "/zh/reference/settings/session-settings/postgresql-connection#postgresql_connection_pool_retries" },
        { name: "postgresql_connection_pool_size", href: "/zh/reference/settings/session-settings/postgresql-connection#postgresql_connection_pool_size" },
        { name: "postgresql_connection_pool_wait_timeout", href: "/zh/reference/settings/session-settings/postgresql-connection#postgresql_connection_pool_wait_timeout" }
      ],
      children: []
    },
    {
      label: "prefer_*",
      count: 5,
      settings: [
        { name: "prefer_column_name_to_alias", href: "/zh/reference/settings/session-settings/prefer#prefer_column_name_to_alias" },
        { name: "prefer_external_sort_block_bytes", href: "/zh/reference/settings/session-settings/prefer#prefer_external_sort_block_bytes" },
        { name: "prefer_global_in_and_join", href: "/zh/reference/settings/session-settings/prefer#prefer_global_in_and_join" },
        { name: "prefer_localhost_replica", href: "/zh/reference/settings/session-settings/prefer#prefer_localhost_replica" },
        { name: "prefer_warmed_unmerged_parts_seconds", href: "/zh/reference/settings/session-settings/prefer#prefer_warmed_unmerged_parts_seconds" }
      ],
      children: []
    },
    {
      label: "preferred_*",
      count: 3,
      settings: [
        { name: "preferred_block_size_bytes", href: "/zh/reference/settings/session-settings/preferred#preferred_block_size_bytes" },
        { name: "preferred_max_column_in_block_size_bytes", href: "/zh/reference/settings/session-settings/preferred#preferred_max_column_in_block_size_bytes" },
        { name: "preferred_optimize_projection_name", href: "/zh/reference/settings/session-settings/preferred#preferred_optimize_projection_name" }
      ],
      children: []
    },
    {
      label: "promql_*",
      count: 3,
      settings: [
        { name: "promql_database", href: "/zh/reference/settings/session-settings/promql#promql_database" },
        { name: "promql_evaluation_time", href: "/zh/reference/settings/session-settings/promql#promql_evaluation_time" },
        { name: "promql_table", href: "/zh/reference/settings/session-settings/promql#promql_table" }
      ],
      children: []
    },
    {
      label: "query_cache_*",
      count: 12,
      settings: [
        { name: "query_cache_compress_entries", href: "/zh/reference/settings/session-settings/query-cache#query_cache_compress_entries" },
        { name: "query_cache_for_subqueries", href: "/zh/reference/settings/session-settings/query-cache#query_cache_for_subqueries" },
        { name: "query_cache_max_entries", href: "/zh/reference/settings/session-settings/query-cache#query_cache_max_entries" },
        { name: "query_cache_max_size_in_bytes", href: "/zh/reference/settings/session-settings/query-cache#query_cache_max_size_in_bytes" },
        { name: "query_cache_min_query_duration", href: "/zh/reference/settings/session-settings/query-cache#query_cache_min_query_duration" },
        { name: "query_cache_min_query_runs", href: "/zh/reference/settings/session-settings/query-cache#query_cache_min_query_runs" },
        { name: "query_cache_nondeterministic_function_handling", href: "/zh/reference/settings/session-settings/query-cache#query_cache_nondeterministic_function_handling" },
        { name: "query_cache_share_between_users", href: "/zh/reference/settings/session-settings/query-cache#query_cache_share_between_users" },
        { name: "query_cache_squash_partial_results", href: "/zh/reference/settings/session-settings/query-cache#query_cache_squash_partial_results" },
        { name: "query_cache_system_table_handling", href: "/zh/reference/settings/session-settings/query-cache#query_cache_system_table_handling" },
        { name: "query_cache_tag", href: "/zh/reference/settings/session-settings/query-cache#query_cache_tag" },
        { name: "query_cache_ttl", href: "/zh/reference/settings/session-settings/query-cache#query_cache_ttl" }
      ],
      children: []
    },
    {
      label: "query_plan_*",
      count: 44,
      settings: [
        { name: "query_plan_aggregation_in_order", href: "/zh/reference/settings/session-settings/query-plan#query_plan_aggregation_in_order" },
        { name: "query_plan_convert_any_join_to_semi_or_anti_join", href: "/zh/reference/settings/session-settings/query-plan#query_plan_convert_any_join_to_semi_or_anti_join" },
        { name: "query_plan_convert_join_to_in", href: "/zh/reference/settings/session-settings/query-plan#query_plan_convert_join_to_in" },
        { name: "query_plan_convert_outer_join_to_inner_join", href: "/zh/reference/settings/session-settings/query-plan#query_plan_convert_outer_join_to_inner_join" },
        { name: "query_plan_direct_read_from_text_index", href: "/zh/reference/settings/session-settings/query-plan#query_plan_direct_read_from_text_index" },
        { name: "query_plan_display_internal_aliases", href: "/zh/reference/settings/session-settings/query-plan#query_plan_display_internal_aliases" },
        { name: "query_plan_enable_multithreading_after_window_functions", href: "/zh/reference/settings/session-settings/query-plan#query_plan_enable_multithreading_after_window_functions" },
        { name: "query_plan_enable_optimizations", href: "/zh/reference/settings/session-settings/query-plan#query_plan_enable_optimizations" },
        { name: "query_plan_execute_functions_after_sorting", href: "/zh/reference/settings/session-settings/query-plan#query_plan_execute_functions_after_sorting" },
        { name: "query_plan_filter_push_down", href: "/zh/reference/settings/session-settings/query-plan#query_plan_filter_push_down" },
        { name: "query_plan_join_shard_by_pk_ranges", href: "/zh/reference/settings/session-settings/query-plan#query_plan_join_shard_by_pk_ranges" },
        { name: "query_plan_join_swap_table", href: "/zh/reference/settings/session-settings/query-plan#query_plan_join_swap_table" },
        { name: "query_plan_lift_up_array_join", href: "/zh/reference/settings/session-settings/query-plan#query_plan_lift_up_array_join" },
        { name: "query_plan_lift_up_union", href: "/zh/reference/settings/session-settings/query-plan#query_plan_lift_up_union" },
        { name: "query_plan_max_limit_for_join_lazy_indexing", href: "/zh/reference/settings/session-settings/query-plan#query_plan_max_limit_for_join_lazy_indexing" },
        { name: "query_plan_max_limit_for_lazy_materialization", href: "/zh/reference/settings/session-settings/query-plan#query_plan_max_limit_for_lazy_materialization" },
        { name: "query_plan_max_limit_for_top_k_optimization", href: "/zh/reference/settings/session-settings/query-plan#query_plan_max_limit_for_top_k_optimization" },
        { name: "query_plan_max_optimizations_to_apply", href: "/zh/reference/settings/session-settings/query-plan#query_plan_max_optimizations_to_apply" },
        { name: "query_plan_max_set_size_for_projection_match", href: "/zh/reference/settings/session-settings/query-plan#query_plan_max_set_size_for_projection_match" },
        { name: "query_plan_max_step_description_length", href: "/zh/reference/settings/session-settings/query-plan#query_plan_max_step_description_length" },
        { name: "query_plan_merge_expression_into_join", href: "/zh/reference/settings/session-settings/query-plan#query_plan_merge_expression_into_join" },
        { name: "query_plan_merge_expressions", href: "/zh/reference/settings/session-settings/query-plan#query_plan_merge_expressions" },
        { name: "query_plan_merge_filter_into_join_condition", href: "/zh/reference/settings/session-settings/query-plan#query_plan_merge_filter_into_join_condition" },
        { name: "query_plan_merge_filters", href: "/zh/reference/settings/session-settings/query-plan#query_plan_merge_filters" },
        { name: "query_plan_min_columns_for_join_lazy_indexing", href: "/zh/reference/settings/session-settings/query-plan#query_plan_min_columns_for_join_lazy_indexing" },
        { name: "query_plan_optimize_join_order_algorithm", href: "/zh/reference/settings/session-settings/query-plan#query_plan_optimize_join_order_algorithm" },
        { name: "query_plan_optimize_join_order_limit", href: "/zh/reference/settings/session-settings/query-plan#query_plan_optimize_join_order_limit" },
        { name: "query_plan_optimize_join_order_max_searched_plans", href: "/zh/reference/settings/session-settings/query-plan#query_plan_optimize_join_order_max_searched_plans" },
        { name: "query_plan_optimize_join_order_randomize", href: "/zh/reference/settings/session-settings/query-plan#query_plan_optimize_join_order_randomize" },
        { name: "query_plan_optimize_lazy_final", href: "/zh/reference/settings/session-settings/query-plan#query_plan_optimize_lazy_final" },
        { name: "query_plan_optimize_lazy_materialization", href: "/zh/reference/settings/session-settings/query-plan#query_plan_optimize_lazy_materialization" },
        { name: "query_plan_optimize_prewhere", href: "/zh/reference/settings/session-settings/query-plan#query_plan_optimize_prewhere" },
        { name: "query_plan_push_down_limit", href: "/zh/reference/settings/session-settings/query-plan#query_plan_push_down_limit" },
        { name: "query_plan_push_limit_by_into_sort", href: "/zh/reference/settings/session-settings/query-plan#query_plan_push_limit_by_into_sort" },
        { name: "query_plan_read_in_order", href: "/zh/reference/settings/session-settings/query-plan#query_plan_read_in_order" },
        { name: "query_plan_read_in_order_through_join", href: "/zh/reference/settings/session-settings/query-plan#query_plan_read_in_order_through_join" },
        { name: "query_plan_remove_redundant_distinct", href: "/zh/reference/settings/session-settings/query-plan#query_plan_remove_redundant_distinct" },
        { name: "query_plan_remove_redundant_sorting", href: "/zh/reference/settings/session-settings/query-plan#query_plan_remove_redundant_sorting" },
        { name: "query_plan_remove_unused_columns", href: "/zh/reference/settings/session-settings/query-plan#query_plan_remove_unused_columns" },
        { name: "query_plan_reuse_storage_ordering_for_window_functions", href: "/zh/reference/settings/session-settings/query-plan#query_plan_reuse_storage_ordering_for_window_functions" },
        { name: "query_plan_split_filter", href: "/zh/reference/settings/session-settings/query-plan#query_plan_split_filter" },
        { name: "query_plan_text_index_add_hint", href: "/zh/reference/settings/session-settings/query-plan#query_plan_text_index_add_hint" },
        { name: "query_plan_top_k_through_join", href: "/zh/reference/settings/session-settings/query-plan#query_plan_top_k_through_join" },
        { name: "query_plan_try_use_vector_search", href: "/zh/reference/settings/session-settings/query-plan#query_plan_try_use_vector_search" }
      ],
      children: []
    },
    {
      label: "query_profiler_*",
      count: 2,
      settings: [
        { name: "query_profiler_cpu_time_period_ns", href: "/zh/reference/settings/session-settings/query-profiler#query_profiler_cpu_time_period_ns" },
        { name: "query_profiler_real_time_period_ns", href: "/zh/reference/settings/session-settings/query-profiler#query_profiler_real_time_period_ns" }
      ],
      children: []
    },
    {
      label: "read_*",
      count: 2,
      settings: [
        { name: "read_priority", href: "/zh/reference/settings/session-settings/read#read_priority" },
        { name: "read_through_distributed_cache", href: "/zh/reference/settings/session-settings/read#read_through_distributed_cache" }
      ],
      children: []
    },
    {
      label: "read_backoff_*",
      count: 5,
      settings: [
        { name: "read_backoff_max_throughput", href: "/zh/reference/settings/session-settings/read-backoff#read_backoff_max_throughput" },
        { name: "read_backoff_min_concurrency", href: "/zh/reference/settings/session-settings/read-backoff#read_backoff_min_concurrency" },
        { name: "read_backoff_min_events", href: "/zh/reference/settings/session-settings/read-backoff#read_backoff_min_events" },
        { name: "read_backoff_min_interval_between_events_ms", href: "/zh/reference/settings/session-settings/read-backoff#read_backoff_min_interval_between_events_ms" },
        { name: "read_backoff_min_latency_ms", href: "/zh/reference/settings/session-settings/read-backoff#read_backoff_min_latency_ms" }
      ],
      children: []
    },
    {
      label: "read_from_*",
      count: 3,
      settings: [
        { name: "read_from_distributed_cache_if_exists_otherwise_bypass_cache", href: "/zh/reference/settings/session-settings/read-from#read_from_distributed_cache_if_exists_otherwise_bypass_cache" },
        { name: "read_from_filesystem_cache_if_exists_otherwise_bypass_cache", href: "/zh/reference/settings/session-settings/read-from#read_from_filesystem_cache_if_exists_otherwise_bypass_cache" },
        { name: "read_from_page_cache_if_exists_otherwise_bypass_cache", href: "/zh/reference/settings/session-settings/read-from#read_from_page_cache_if_exists_otherwise_bypass_cache" }
      ],
      children: []
    },
    {
      label: "read_in_*",
      count: 4,
      settings: [
        { name: "read_in_order_two_level_merge_threshold", href: "/zh/reference/settings/session-settings/read-in#read_in_order_two_level_merge_threshold" },
        { name: "read_in_order_use_buffering", href: "/zh/reference/settings/session-settings/read-in#read_in_order_use_buffering" },
        { name: "read_in_order_use_virtual_row", href: "/zh/reference/settings/session-settings/read-in#read_in_order_use_virtual_row" },
        { name: "read_in_order_use_virtual_row_per_block", href: "/zh/reference/settings/session-settings/read-in#read_in_order_use_virtual_row_per_block" }
      ],
      children: []
    },
    {
      label: "read_overflow_mode_*",
      count: 2,
      settings: [
        { name: "read_overflow_mode", href: "/zh/reference/settings/session-settings/read-overflow-mode#read_overflow_mode" },
        { name: "read_overflow_mode_leaf", href: "/zh/reference/settings/session-settings/read-overflow-mode#read_overflow_mode_leaf" }
      ],
      children: []
    },
    {
      label: "reader_executor_*",
      count: 3,
      settings: [
        { name: "reader_executor_max_tail_for_drain", href: "/zh/reference/settings/session-settings/reader-executor#reader_executor_max_tail_for_drain" },
        { name: "reader_executor_min_bytes_for_seek", href: "/zh/reference/settings/session-settings/reader-executor#reader_executor_min_bytes_for_seek" },
        { name: "reader_executor_use_long_connections", href: "/zh/reference/settings/session-settings/reader-executor#reader_executor_use_long_connections" }
      ],
      children: []
    },
    {
      label: "receive_*",
      count: 2,
      settings: [
        { name: "receive_data_timeout_ms", href: "/zh/reference/settings/session-settings/receive#receive_data_timeout_ms" },
        { name: "receive_timeout", href: "/zh/reference/settings/session-settings/receive#receive_timeout" }
      ],
      children: []
    },
    {
      label: "regexp_dict_*",
      count: 3,
      settings: [
        { name: "regexp_dict_allow_hyperscan", href: "/zh/reference/settings/session-settings/regexp-dict#regexp_dict_allow_hyperscan" },
        { name: "regexp_dict_flag_case_insensitive", href: "/zh/reference/settings/session-settings/regexp-dict#regexp_dict_flag_case_insensitive" },
        { name: "regexp_dict_flag_dotall", href: "/zh/reference/settings/session-settings/regexp-dict#regexp_dict_flag_dotall" }
      ],
      children: []
    },
    {
      label: "remote_filesystem_*",
      count: 2,
      settings: [
        { name: "remote_filesystem_read_method", href: "/zh/reference/settings/session-settings/remote-filesystem#remote_filesystem_read_method" },
        { name: "remote_filesystem_read_prefetch", href: "/zh/reference/settings/session-settings/remote-filesystem#remote_filesystem_read_prefetch" }
      ],
      children: []
    },
    {
      label: "remote_fs_*",
      count: 2,
      settings: [
        { name: "remote_fs_read_backoff_max_tries", href: "/zh/reference/settings/session-settings/remote-fs#remote_fs_read_backoff_max_tries" },
        { name: "remote_fs_read_max_backoff_ms", href: "/zh/reference/settings/session-settings/remote-fs#remote_fs_read_max_backoff_ms" }
      ],
      children: []
    },
    {
      label: "replace_running_query_*",
      count: 2,
      settings: [
        { name: "replace_running_query", href: "/zh/reference/settings/session-settings/replace-running-query#replace_running_query" },
        { name: "replace_running_query_max_wait_ms", href: "/zh/reference/settings/session-settings/replace-running-query#replace_running_query_max_wait_ms" }
      ],
      children: []
    },
    {
      label: "restore_replace_*",
      count: 3,
      settings: [
        { name: "restore_replace_external_dictionary_source_to_null", href: "/zh/reference/settings/session-settings/restore-replace#restore_replace_external_dictionary_source_to_null" },
        { name: "restore_replace_external_engines_to_null", href: "/zh/reference/settings/session-settings/restore-replace#restore_replace_external_engines_to_null" },
        { name: "restore_replace_external_table_functions_to_null", href: "/zh/reference/settings/session-settings/restore-replace#restore_replace_external_table_functions_to_null" }
      ],
      children: []
    },
    {
      label: "rewrite_*",
      count: 2,
      settings: [
        { name: "rewrite_count_distinct_if_with_count_distinct_implementation", href: "/zh/reference/settings/session-settings/rewrite#rewrite_count_distinct_if_with_count_distinct_implementation" },
        { name: "rewrite_in_to_join", href: "/zh/reference/settings/session-settings/rewrite#rewrite_in_to_join" }
      ],
      children: []
    },
    {
      label: "s3_*",
      count: 16,
      settings: [
        { name: "s3_check_objects_after_upload", href: "/zh/reference/settings/session-settings/s3#s3_check_objects_after_upload" },
        { name: "s3_connect_timeout_ms", href: "/zh/reference/settings/session-settings/s3#s3_connect_timeout_ms" },
        { name: "s3_create_new_file_on_insert", href: "/zh/reference/settings/session-settings/s3#s3_create_new_file_on_insert" },
        { name: "s3_disable_checksum", href: "/zh/reference/settings/session-settings/s3#s3_disable_checksum" },
        { name: "s3_ignore_file_doesnt_exist", href: "/zh/reference/settings/session-settings/s3#s3_ignore_file_doesnt_exist" },
        { name: "s3_list_object_keys_size", href: "/zh/reference/settings/session-settings/s3#s3_list_object_keys_size" },
        { name: "s3_min_upload_part_size", href: "/zh/reference/settings/session-settings/s3#s3_min_upload_part_size" },
        { name: "s3_path_filter_limit", href: "/zh/reference/settings/session-settings/s3#s3_path_filter_limit" },
        { name: "s3_request_timeout_ms", href: "/zh/reference/settings/session-settings/s3#s3_request_timeout_ms" },
        { name: "s3_skip_empty_files", href: "/zh/reference/settings/session-settings/s3#s3_skip_empty_files" },
        { name: "s3_slow_all_threads_after_network_error", href: "/zh/reference/settings/session-settings/s3#s3_slow_all_threads_after_network_error" },
        { name: "s3_strict_upload_part_size", href: "/zh/reference/settings/session-settings/s3#s3_strict_upload_part_size" },
        { name: "s3_throw_on_zero_files_match", href: "/zh/reference/settings/session-settings/s3#s3_throw_on_zero_files_match" },
        { name: "s3_truncate_on_insert", href: "/zh/reference/settings/session-settings/s3#s3_truncate_on_insert" },
        { name: "s3_uri_style", href: "/zh/reference/settings/session-settings/s3#s3_uri_style" },
        { name: "s3_use_adaptive_timeouts", href: "/zh/reference/settings/session-settings/s3#s3_use_adaptive_timeouts" }
      ],
      children: []
    },
    {
      label: "s3_allow_*",
      count: 3,
      settings: [
        { name: "s3_allow_multipart_copy", href: "/zh/reference/settings/session-settings/s3-allow#s3_allow_multipart_copy" },
        { name: "s3_allow_parallel_part_upload", href: "/zh/reference/settings/session-settings/s3-allow#s3_allow_parallel_part_upload" },
        { name: "s3_allow_server_credentials_in_user_queries", href: "/zh/reference/settings/session-settings/s3-allow#s3_allow_server_credentials_in_user_queries" }
      ],
      children: []
    },
    {
      label: "s3_max_*",
      count: 12,
      settings: [
        { name: "s3_max_connections", href: "/zh/reference/settings/session-settings/s3-max#s3_max_connections" },
        { name: "s3_max_get_burst", href: "/zh/reference/settings/session-settings/s3-max#s3_max_get_burst" },
        { name: "s3_max_get_rps", href: "/zh/reference/settings/session-settings/s3-max#s3_max_get_rps" },
        { name: "s3_max_inflight_parts_for_one_file", href: "/zh/reference/settings/session-settings/s3-max#s3_max_inflight_parts_for_one_file" },
        { name: "s3_max_part_number", href: "/zh/reference/settings/session-settings/s3-max#s3_max_part_number" },
        { name: "s3_max_put_burst", href: "/zh/reference/settings/session-settings/s3-max#s3_max_put_burst" },
        { name: "s3_max_put_rps", href: "/zh/reference/settings/session-settings/s3-max#s3_max_put_rps" },
        { name: "s3_max_single_operation_copy_size", href: "/zh/reference/settings/session-settings/s3-max#s3_max_single_operation_copy_size" },
        { name: "s3_max_single_part_upload_size", href: "/zh/reference/settings/session-settings/s3-max#s3_max_single_part_upload_size" },
        { name: "s3_max_single_read_retries", href: "/zh/reference/settings/session-settings/s3-max#s3_max_single_read_retries" },
        { name: "s3_max_unexpected_write_error_retries", href: "/zh/reference/settings/session-settings/s3-max#s3_max_unexpected_write_error_retries" },
        { name: "s3_max_upload_part_size", href: "/zh/reference/settings/session-settings/s3-max#s3_max_upload_part_size" }
      ],
      children: []
    },
    {
      label: "s3_upload_*",
      count: 2,
      settings: [
        { name: "s3_upload_part_size_multiply_factor", href: "/zh/reference/settings/session-settings/s3-upload#s3_upload_part_size_multiply_factor" },
        { name: "s3_upload_part_size_multiply_parts_count_threshold", href: "/zh/reference/settings/session-settings/s3-upload#s3_upload_part_size_multiply_parts_count_threshold" }
      ],
      children: []
    },
    {
      label: "s3_validate_*",
      count: 2,
      settings: [
        { name: "s3_validate_etag_on_read", href: "/zh/reference/settings/session-settings/s3-validate#s3_validate_etag_on_read" },
        { name: "s3_validate_request_settings", href: "/zh/reference/settings/session-settings/s3-validate#s3_validate_request_settings" }
      ],
      children: []
    },
    {
      label: "s3queue_*",
      count: 4,
      settings: [
        { name: "s3queue_default_zookeeper_path", href: "/zh/reference/settings/session-settings/s3queue#s3queue_default_zookeeper_path" },
        { name: "s3queue_enable_logging_to_s3queue_log", href: "/zh/reference/settings/session-settings/s3queue#s3queue_enable_logging_to_s3queue_log" },
        { name: "s3queue_keeper_fault_injection_probability", href: "/zh/reference/settings/session-settings/s3queue#s3queue_keeper_fault_injection_probability" },
        { name: "s3queue_migrate_old_metadata_to_buckets", href: "/zh/reference/settings/session-settings/s3queue#s3queue_migrate_old_metadata_to_buckets" }
      ],
      children: []
    },
    {
      label: "schema_inference_*",
      count: 6,
      settings: [
        { name: "schema_inference_cache_require_modification_time_for_url", href: "/zh/reference/settings/session-settings/schema-inference#schema_inference_cache_require_modification_time_for_url" },
        { name: "schema_inference_use_cache_for_azure", href: "/zh/reference/settings/session-settings/schema-inference#schema_inference_use_cache_for_azure" },
        { name: "schema_inference_use_cache_for_file", href: "/zh/reference/settings/session-settings/schema-inference#schema_inference_use_cache_for_file" },
        { name: "schema_inference_use_cache_for_hdfs", href: "/zh/reference/settings/session-settings/schema-inference#schema_inference_use_cache_for_hdfs" },
        { name: "schema_inference_use_cache_for_s3", href: "/zh/reference/settings/session-settings/schema-inference#schema_inference_use_cache_for_s3" },
        { name: "schema_inference_use_cache_for_url", href: "/zh/reference/settings/session-settings/schema-inference#schema_inference_use_cache_for_url" }
      ],
      children: []
    },
    {
      label: "send_*",
      count: 4,
      settings: [
        { name: "send_profile_events", href: "/zh/reference/settings/session-settings/send#send_profile_events" },
        { name: "send_progress_in_http_headers", href: "/zh/reference/settings/session-settings/send#send_progress_in_http_headers" },
        { name: "send_table_structure_on_insert_with_inline_data", href: "/zh/reference/settings/session-settings/send#send_table_structure_on_insert_with_inline_data" },
        { name: "send_timeout", href: "/zh/reference/settings/session-settings/send#send_timeout" }
      ],
      children: []
    },
    {
      label: "send_logs_*",
      count: 2,
      settings: [
        { name: "send_logs_level", href: "/zh/reference/settings/session-settings/send-logs#send_logs_level" },
        { name: "send_logs_source_regexp", href: "/zh/reference/settings/session-settings/send-logs#send_logs_source_regexp" }
      ],
      children: []
    },
    {
      label: "serialize_*",
      count: 2,
      settings: [
        { name: "serialize_query_plan", href: "/zh/reference/settings/session-settings/serialize#serialize_query_plan" },
        { name: "serialize_string_in_memory_with_zero_byte", href: "/zh/reference/settings/session-settings/serialize#serialize_string_in_memory_with_zero_byte" }
      ],
      children: []
    },
    {
      label: "shared_merge_*",
      count: 4,
      settings: [
        {
          name: "shared_merge_tree_sequential_consistency_initial_parts_update_backoff_ms",
          href: "/zh/reference/settings/session-settings/shared-merge#shared_merge_tree_sequential_consistency_initial_parts_update_backoff_ms"
        },
        {
          name: "shared_merge_tree_sequential_consistency_max_parts_update_backoff_ms",
          href: "/zh/reference/settings/session-settings/shared-merge#shared_merge_tree_sequential_consistency_max_parts_update_backoff_ms"
        },
        {
          name: "shared_merge_tree_sequential_consistency_parts_update_max_retries",
          href: "/zh/reference/settings/session-settings/shared-merge#shared_merge_tree_sequential_consistency_parts_update_max_retries"
        },
        { name: "shared_merge_tree_sync_parts_on_partition_operations", href: "/zh/reference/settings/session-settings/shared-merge#shared_merge_tree_sync_parts_on_partition_operations" }
      ],
      children: []
    },
    {
      label: "short_circuit_function_evaluation_*",
      count: 3,
      settings: [
        { name: "short_circuit_function_evaluation", href: "/zh/reference/settings/session-settings/short-circuit-function-evaluation#short_circuit_function_evaluation" },
        { name: "short_circuit_function_evaluation_for_nulls", href: "/zh/reference/settings/session-settings/short-circuit-function-evaluation#short_circuit_function_evaluation_for_nulls" },
        {
          name: "short_circuit_function_evaluation_for_nulls_threshold",
          href: "/zh/reference/settings/session-settings/short-circuit-function-evaluation#short_circuit_function_evaluation_for_nulls_threshold"
        }
      ],
      children: []
    },
    {
      label: "show_*",
      count: 4,
      settings: [
        { name: "show_data_lake_catalogs_in_system_tables", href: "/zh/reference/settings/session-settings/show#show_data_lake_catalogs_in_system_tables" },
        { name: "show_processlist_include_internal", href: "/zh/reference/settings/session-settings/show#show_processlist_include_internal" },
        { name: "show_remote_databases_in_system_tables", href: "/zh/reference/settings/session-settings/show#show_remote_databases_in_system_tables" },
        { name: "show_table_uuid_in_table_create_query_if_not_nil", href: "/zh/reference/settings/session-settings/show#show_table_uuid_in_table_create_query_if_not_nil" }
      ],
      children: []
    },
    {
      label: "skip_unavailable_shards_*",
      count: 2,
      settings: [
        { name: "skip_unavailable_shards", href: "/zh/reference/settings/session-settings/skip-unavailable-shards#skip_unavailable_shards" },
        { name: "skip_unavailable_shards_mode", href: "/zh/reference/settings/session-settings/skip-unavailable-shards#skip_unavailable_shards_mode" }
      ],
      children: []
    },
    {
      label: "sleep_in_*",
      count: 2,
      settings: [
        { name: "sleep_in_send_data_ms", href: "/zh/reference/settings/session-settings/sleep-in#sleep_in_send_data_ms" },
        { name: "sleep_in_send_tables_status_ms", href: "/zh/reference/settings/session-settings/sleep-in#sleep_in_send_tables_status_ms" }
      ],
      children: []
    },
    {
      label: "split_*",
      count: 2,
      settings: [
        { name: "split_intersecting_parts_ranges_into_layers_final", href: "/zh/reference/settings/session-settings/split#split_intersecting_parts_ranges_into_layers_final" },
        { name: "split_parts_ranges_into_intersecting_and_non_intersecting_final", href: "/zh/reference/settings/session-settings/split#split_parts_ranges_into_intersecting_and_non_intersecting_final" }
      ],
      children: []
    },
    {
      label: "storage_*",
      count: 2,
      settings: [
        { name: "storage_file_read_method", href: "/zh/reference/settings/session-settings/storage#storage_file_read_method" },
        { name: "storage_system_stack_trace_pipe_read_timeout_ms", href: "/zh/reference/settings/session-settings/storage#storage_system_stack_trace_pipe_read_timeout_ms" }
      ],
      children: []
    },
    {
      label: "stream_*",
      count: 2,
      settings: [
        { name: "stream_flush_interval_ms", href: "/zh/reference/settings/session-settings/stream#stream_flush_interval_ms" },
        { name: "stream_poll_timeout_ms", href: "/zh/reference/settings/session-settings/stream#stream_poll_timeout_ms" }
      ],
      children: []
    },
    {
      label: "stream_like_*",
      count: 2,
      settings: [
        { name: "stream_like_engine_allow_direct_select", href: "/zh/reference/settings/session-settings/stream-like#stream_like_engine_allow_direct_select" },
        { name: "stream_like_engine_insert_queue", href: "/zh/reference/settings/session-settings/stream-like#stream_like_engine_insert_queue" }
      ],
      children: []
    },
    {
      label: "system_*",
      count: 2,
      settings: [
        { name: "system_events_show_zero_values", href: "/zh/reference/settings/session-settings/system#system_events_show_zero_values" },
        { name: "system_metric_log_show_zero_values_in_histograms", href: "/zh/reference/settings/session-settings/system#system_metric_log_show_zero_values_in_histograms" }
      ],
      children: []
    },
    {
      label: "table_*",
      count: 2,
      settings: [
        { name: "table_engine_read_through_distributed_cache", href: "/zh/reference/settings/session-settings/table#table_engine_read_through_distributed_cache" },
        { name: "table_function_remote_max_addresses", href: "/zh/reference/settings/session-settings/table#table_function_remote_max_addresses" }
      ],
      children: []
    },
    {
      label: "temporary_files_*",
      count: 2,
      settings: [
        { name: "temporary_files_buffer_size", href: "/zh/reference/settings/session-settings/temporary-files#temporary_files_buffer_size" },
        { name: "temporary_files_codec", href: "/zh/reference/settings/session-settings/temporary-files#temporary_files_codec" }
      ],
      children: []
    },
    {
      label: "text_index_*",
      count: 5,
      settings: [
        { name: "text_index_hint_max_selectivity", href: "/zh/reference/settings/session-settings/text-index#text_index_hint_max_selectivity" },
        { name: "text_index_lazy_intersection_density_threshold", href: "/zh/reference/settings/session-settings/text-index#text_index_lazy_intersection_density_threshold" },
        { name: "text_index_like_max_postings_to_read", href: "/zh/reference/settings/session-settings/text-index#text_index_like_max_postings_to_read" },
        { name: "text_index_like_min_pattern_length", href: "/zh/reference/settings/session-settings/text-index#text_index_like_min_pattern_length" },
        { name: "text_index_posting_list_apply_mode", href: "/zh/reference/settings/session-settings/text-index#text_index_posting_list_apply_mode" }
      ],
      children: []
    },
    {
      label: "throw_on_*",
      count: 3,
      settings: [
        { name: "throw_on_error_from_cache_on_write_operations", href: "/zh/reference/settings/session-settings/throw-on#throw_on_error_from_cache_on_write_operations" },
        { name: "throw_on_max_partitions_per_insert_block", href: "/zh/reference/settings/session-settings/throw-on#throw_on_max_partitions_per_insert_block" },
        { name: "throw_on_unsupported_query_inside_transaction", href: "/zh/reference/settings/session-settings/throw-on#throw_on_unsupported_query_inside_transaction" }
      ],
      children: []
    },
    {
      label: "timeout_overflow_mode_*",
      count: 2,
      settings: [
        { name: "timeout_overflow_mode", href: "/zh/reference/settings/session-settings/timeout-overflow-mode#timeout_overflow_mode" },
        { name: "timeout_overflow_mode_leaf", href: "/zh/reference/settings/session-settings/timeout-overflow-mode#timeout_overflow_mode_leaf" }
      ],
      children: []
    },
    {
      label: "totals_*",
      count: 2,
      settings: [
        { name: "totals_auto_threshold", href: "/zh/reference/settings/session-settings/totals#totals_auto_threshold" },
        { name: "totals_mode", href: "/zh/reference/settings/session-settings/totals#totals_mode" }
      ],
      children: []
    },
    {
      label: "trace_profile_events_*",
      count: 2,
      settings: [
        { name: "trace_profile_events", href: "/zh/reference/settings/session-settings/trace-profile-events#trace_profile_events" },
        { name: "trace_profile_events_list", href: "/zh/reference/settings/session-settings/trace-profile-events#trace_profile_events_list" }
      ],
      children: []
    },
    {
      label: "update_*",
      count: 2,
      settings: [
        { name: "update_parallel_mode", href: "/zh/reference/settings/session-settings/update#update_parallel_mode" },
        { name: "update_sequential_consistency", href: "/zh/reference/settings/session-settings/update#update_sequential_consistency" }
      ],
      children: []
    },
    {
      label: "url_*",
      count: 2,
      settings: [
        { name: "url_base", href: "/zh/reference/settings/session-settings/url#url_base" },
        { name: "url_wildcard_max_directories_to_read", href: "/zh/reference/settings/session-settings/url#url_wildcard_max_directories_to_read" }
      ],
      children: []
    },
    {
      label: "use_*",
      count: 21,
      settings: [
        { name: "use_async_executor_for_materialized_views", href: "/zh/reference/settings/session-settings/use#use_async_executor_for_materialized_views" },
        { name: "use_cache_for_count_from_files", href: "/zh/reference/settings/session-settings/use#use_cache_for_count_from_files" },
        { name: "use_client_time_zone", href: "/zh/reference/settings/session-settings/use#use_client_time_zone" },
        { name: "use_compact_format_in_distributed_parts_names", href: "/zh/reference/settings/session-settings/use#use_compact_format_in_distributed_parts_names" },
        { name: "use_concurrency_control", href: "/zh/reference/settings/session-settings/use#use_concurrency_control" },
        { name: "use_constant_folding_in_index_analysis", href: "/zh/reference/settings/session-settings/use#use_constant_folding_in_index_analysis" },
        { name: "use_hash_table_stats_for_join_reordering", href: "/zh/reference/settings/session-settings/use#use_hash_table_stats_for_join_reordering" },
        { name: "use_hedged_requests", href: "/zh/reference/settings/session-settings/use#use_hedged_requests" },
        { name: "use_hive_partitioning", href: "/zh/reference/settings/session-settings/use#use_hive_partitioning" },
        { name: "use_join_disjunctions_push_down", href: "/zh/reference/settings/session-settings/use#use_join_disjunctions_push_down" },
        { name: "use_legacy_to_time", href: "/zh/reference/settings/session-settings/use#use_legacy_to_time" },
        { name: "use_lightweight_primary_key_index_analysis", href: "/zh/reference/settings/session-settings/use#use_lightweight_primary_key_index_analysis" },
        { name: "use_parquet_metadata_cache", href: "/zh/reference/settings/session-settings/use#use_parquet_metadata_cache" },
        { name: "use_primary_key", href: "/zh/reference/settings/session-settings/use#use_primary_key" },
        { name: "use_reader_executor", href: "/zh/reference/settings/session-settings/use#use_reader_executor" },
        { name: "use_roaring_bitmap_iceberg_positional_deletes", href: "/zh/reference/settings/session-settings/use#use_roaring_bitmap_iceberg_positional_deletes" },
        { name: "use_streaming_marks_compression", href: "/zh/reference/settings/session-settings/use#use_streaming_marks_compression" },
        { name: "use_strict_insert_block_limits", href: "/zh/reference/settings/session-settings/use#use_strict_insert_block_limits" },
        { name: "use_structure_from_insertion_table_in_table_functions", href: "/zh/reference/settings/session-settings/use#use_structure_from_insertion_table_in_table_functions" },
        { name: "use_uncompressed_cache", href: "/zh/reference/settings/session-settings/use#use_uncompressed_cache" },
        { name: "use_with_fill_by_sorting_prefix", href: "/zh/reference/settings/session-settings/use#use_with_fill_by_sorting_prefix" }
      ],
      children: []
    },
    {
      label: "use_iceberg_*",
      count: 2,
      settings: [
        { name: "use_iceberg_metadata_files_cache", href: "/zh/reference/settings/session-settings/use-iceberg#use_iceberg_metadata_files_cache" },
        { name: "use_iceberg_partition_pruning", href: "/zh/reference/settings/session-settings/use-iceberg#use_iceberg_partition_pruning" }
      ],
      children: []
    },
    {
      label: "use_index_for_in_with_subqueries_*",
      count: 2,
      settings: [
        { name: "use_index_for_in_with_subqueries", href: "/zh/reference/settings/session-settings/use-index-for-in-with-subqueries#use_index_for_in_with_subqueries" },
        { name: "use_index_for_in_with_subqueries_max_values", href: "/zh/reference/settings/session-settings/use-index-for-in-with-subqueries#use_index_for_in_with_subqueries_max_values" }
      ],
      children: []
    },
    {
      label: "use_page_*",
      count: 4,
      settings: [
        { name: "use_page_cache_for_disks_without_file_cache", href: "/zh/reference/settings/session-settings/use-page#use_page_cache_for_disks_without_file_cache" },
        { name: "use_page_cache_for_local_disks", href: "/zh/reference/settings/session-settings/use-page#use_page_cache_for_local_disks" },
        { name: "use_page_cache_for_object_storage", href: "/zh/reference/settings/session-settings/use-page#use_page_cache_for_object_storage" },
        { name: "use_page_cache_with_distributed_cache", href: "/zh/reference/settings/session-settings/use-page#use_page_cache_with_distributed_cache" }
      ],
      children: []
    },
    {
      label: "use_paimon_*",
      count: 2,
      settings: [
        { name: "use_paimon_metadata_files_cache", href: "/zh/reference/settings/session-settings/use-paimon#use_paimon_metadata_files_cache" },
        { name: "use_paimon_partition_pruning", href: "/zh/reference/settings/session-settings/use-paimon#use_paimon_partition_pruning" }
      ],
      children: []
    },
    {
      label: "use_partition_*",
      count: 2,
      settings: [
        { name: "use_partition_minmax_for_primary_key_pruning", href: "/zh/reference/settings/session-settings/use-partition#use_partition_minmax_for_primary_key_pruning" },
        { name: "use_partition_pruning", href: "/zh/reference/settings/session-settings/use-partition#use_partition_pruning" }
      ],
      children: []
    },
    {
      label: "use_query_*",
      count: 2,
      settings: [
        { name: "use_query_cache", href: "/zh/reference/settings/session-settings/use-query#use_query_cache" },
        { name: "use_query_condition_cache", href: "/zh/reference/settings/session-settings/use-query#use_query_condition_cache" }
      ],
      children: []
    },
    {
      label: "use_skip_indexes_*",
      count: 6,
      settings: [
        { name: "use_skip_indexes", href: "/zh/reference/settings/session-settings/use-skip-indexes#use_skip_indexes" },
        { name: "use_skip_indexes_for_disjunctions", href: "/zh/reference/settings/session-settings/use-skip-indexes#use_skip_indexes_for_disjunctions" },
        { name: "use_skip_indexes_for_top_k", href: "/zh/reference/settings/session-settings/use-skip-indexes#use_skip_indexes_for_top_k" },
        { name: "use_skip_indexes_if_final", href: "/zh/reference/settings/session-settings/use-skip-indexes#use_skip_indexes_if_final" },
        { name: "use_skip_indexes_if_final_exact_mode", href: "/zh/reference/settings/session-settings/use-skip-indexes#use_skip_indexes_if_final_exact_mode" },
        { name: "use_skip_indexes_on_data_read", href: "/zh/reference/settings/session-settings/use-skip-indexes#use_skip_indexes_on_data_read" }
      ],
      children: []
    },
    {
      label: "use_statistics_*",
      count: 3,
      settings: [
        { name: "use_statistics", href: "/zh/reference/settings/session-settings/use-statistics#use_statistics" },
        { name: "use_statistics_cache", href: "/zh/reference/settings/session-settings/use-statistics#use_statistics_cache" },
        { name: "use_statistics_for_part_pruning", href: "/zh/reference/settings/session-settings/use-statistics#use_statistics_for_part_pruning" }
      ],
      children: []
    },
    {
      label: "use_text_*",
      count: 4,
      settings: [
        { name: "use_text_index_header_cache", href: "/zh/reference/settings/session-settings/use-text#use_text_index_header_cache" },
        { name: "use_text_index_like_evaluation_by_dictionary_scan", href: "/zh/reference/settings/session-settings/use-text#use_text_index_like_evaluation_by_dictionary_scan" },
        { name: "use_text_index_postings_cache", href: "/zh/reference/settings/session-settings/use-text#use_text_index_postings_cache" },
        { name: "use_text_index_tokens_cache", href: "/zh/reference/settings/session-settings/use-text#use_text_index_tokens_cache" }
      ],
      children: []
    },
    {
      label: "use_top_k_dynamic_filtering_*",
      count: 2,
      settings: [
        { name: "use_top_k_dynamic_filtering", href: "/zh/reference/settings/session-settings/use-top-k-dynamic-filtering#use_top_k_dynamic_filtering" },
        {
          name: "use_top_k_dynamic_filtering_for_variable_length_types",
          href: "/zh/reference/settings/session-settings/use-top-k-dynamic-filtering#use_top_k_dynamic_filtering_for_variable_length_types"
        }
      ],
      children: []
    },
    {
      label: "use_variant_*",
      count: 2,
      settings: [
        { name: "use_variant_as_common_type", href: "/zh/reference/settings/session-settings/use-variant#use_variant_as_common_type" },
        { name: "use_variant_default_implementation_for_comparisons", href: "/zh/reference/settings/session-settings/use-variant#use_variant_default_implementation_for_comparisons" }
      ],
      children: []
    },
    {
      label: "validate_*",
      count: 3,
      settings: [
        { name: "validate_enum_literals_in_operators", href: "/zh/reference/settings/session-settings/validate#validate_enum_literals_in_operators" },
        { name: "validate_mutation_query", href: "/zh/reference/settings/session-settings/validate#validate_mutation_query" },
        { name: "validate_polygons", href: "/zh/reference/settings/session-settings/validate#validate_polygons" }
      ],
      children: []
    },
    {
      label: "vector_search_*",
      count: 4,
      settings: [
        { name: "vector_search_filter_strategy", href: "/zh/reference/settings/session-settings/vector-search#vector_search_filter_strategy" },
        { name: "vector_search_index_fetch_multiplier", href: "/zh/reference/settings/session-settings/vector-search#vector_search_index_fetch_multiplier" },
        { name: "vector_search_use_quantized_codes", href: "/zh/reference/settings/session-settings/vector-search#vector_search_use_quantized_codes" },
        { name: "vector_search_with_rescoring", href: "/zh/reference/settings/session-settings/vector-search#vector_search_with_rescoring" }
      ],
      children: []
    },
    {
      label: "wait_for_*",
      count: 4,
      settings: [
        { name: "wait_for_async_insert", href: "/zh/reference/settings/session-settings/wait-for#wait_for_async_insert" },
        { name: "wait_for_async_insert_timeout", href: "/zh/reference/settings/session-settings/wait-for#wait_for_async_insert_timeout" },
        { name: "wait_for_part_commit_in_dependent_materialized_views", href: "/zh/reference/settings/session-settings/wait-for#wait_for_part_commit_in_dependent_materialized_views" },
        { name: "wait_for_window_view_fire_signal_timeout", href: "/zh/reference/settings/session-settings/wait-for#wait_for_window_view_fire_signal_timeout" }
      ],
      children: []
    },
    {
      label: "webassembly_udf_*",
      count: 4,
      settings: [
        { name: "webassembly_udf_max_fuel", href: "/zh/reference/settings/session-settings/webassembly-udf#webassembly_udf_max_fuel" },
        { name: "webassembly_udf_max_input_block_size", href: "/zh/reference/settings/session-settings/webassembly-udf#webassembly_udf_max_input_block_size" },
        { name: "webassembly_udf_max_instances", href: "/zh/reference/settings/session-settings/webassembly-udf#webassembly_udf_max_instances" },
        { name: "webassembly_udf_max_memory", href: "/zh/reference/settings/session-settings/webassembly-udf#webassembly_udf_max_memory" }
      ],
      children: []
    },
    {
      label: "window_view_*",
      count: 2,
      settings: [
        { name: "window_view_clean_interval", href: "/zh/reference/settings/session-settings/window-view#window_view_clean_interval" },
        { name: "window_view_heartbeat_interval", href: "/zh/reference/settings/session-settings/window-view#window_view_heartbeat_interval" }
      ],
      children: []
    },
    {
      label: "write_through_distributed_cache_*",
      count: 2,
      settings: [
        { name: "write_through_distributed_cache", href: "/zh/reference/settings/session-settings/write-through-distributed-cache#write_through_distributed_cache" },
        { name: "write_through_distributed_cache_buffer_size", href: "/zh/reference/settings/session-settings/write-through-distributed-cache#write_through_distributed_cache_buffer_size" }
      ],
      children: []
    },
    {
      label: "Other",
      count: 121,
      settings: [
        { name: "add_http_cors_header", href: "/zh/reference/settings/session-settings/other#add_http_cors_header" },
        { name: "analyze_index_with_space_filling_curves", href: "/zh/reference/settings/session-settings/other#analyze_index_with_space_filling_curves" },
        { name: "analyzer_inline_views", href: "/zh/reference/settings/session-settings/other#analyzer_inline_views" },
        { name: "any_join_distinct_right_table_keys", href: "/zh/reference/settings/session-settings/other#any_join_distinct_right_table_keys" },
        { name: "archive_adaptive_buffer_max_size_bytes", href: "/zh/reference/settings/session-settings/other#archive_adaptive_buffer_max_size_bytes" },
        { name: "arrow_flight_request_descriptor_type", href: "/zh/reference/settings/session-settings/other#arrow_flight_request_descriptor_type" },
        { name: "backup_slow_all_threads_after_retryable_s3_error", href: "/zh/reference/settings/session-settings/other#backup_slow_all_threads_after_retryable_s3_error" },
        { name: "cache_warmer_threads", href: "/zh/reference/settings/session-settings/other#cache_warmer_threads" },
        { name: "calculate_text_stack_trace", href: "/zh/reference/settings/session-settings/other#calculate_text_stack_trace" },
        { name: "cancel_http_readonly_queries_on_client_close", href: "/zh/reference/settings/session-settings/other#cancel_http_readonly_queries_on_client_close" },
        { name: "checksum_on_read", href: "/zh/reference/settings/session-settings/other#checksum_on_read" },
        { name: "connection_pool_max_wait_ms", href: "/zh/reference/settings/session-settings/other#connection_pool_max_wait_ms" },
        { name: "connections_with_failover_max_tries", href: "/zh/reference/settings/session-settings/other#connections_with_failover_max_tries" },
        { name: "convert_query_to_cnf", href: "/zh/reference/settings/session-settings/other#convert_query_to_cnf" },
        { name: "count_matches_stop_at_empty_match", href: "/zh/reference/settings/session-settings/other#count_matches_stop_at_empty_match" },
        { name: "cross_to_inner_join_rewrite", href: "/zh/reference/settings/session-settings/other#cross_to_inner_join_rewrite" },
        { name: "data_type_default_nullable", href: "/zh/reference/settings/session-settings/other#data_type_default_nullable" },
        { name: "decimal_check_overflow", href: "/zh/reference/settings/session-settings/other#decimal_check_overflow" },
        { name: "deduplicate_blocks_in_dependent_materialized_views", href: "/zh/reference/settings/session-settings/other#deduplicate_blocks_in_dependent_materialized_views" },
        { name: "defer_partition_pruning_after_final", href: "/zh/reference/settings/session-settings/other#defer_partition_pruning_after_final" },
        { name: "describe_compact_output", href: "/zh/reference/settings/session-settings/other#describe_compact_output" },
        { name: "dialect", href: "/zh/reference/settings/session-settings/other#dialect" },
        { name: "discard_query_data", href: "/zh/reference/settings/session-settings/other#discard_query_data" },
        { name: "distinct_overflow_mode", href: "/zh/reference/settings/session-settings/other#distinct_overflow_mode" },
        { name: "do_not_merge_across_partitions_select_final", href: "/zh/reference/settings/session-settings/other#do_not_merge_across_partitions_select_final" },
        { name: "dynamic_throw_on_type_mismatch", href: "/zh/reference/settings/session-settings/other#dynamic_throw_on_type_mismatch" },
        { name: "enforce_strict_identifier_format", href: "/zh/reference/settings/session-settings/other#enforce_strict_identifier_format" },
        { name: "engine_url_skip_empty_files", href: "/zh/reference/settings/session-settings/other#engine_url_skip_empty_files" },
        { name: "exact_rows_before_limit", href: "/zh/reference/settings/session-settings/other#exact_rows_before_limit" },
        { name: "except_default_mode", href: "/zh/reference/settings/session-settings/other#except_default_mode" },
        { name: "exclude_materialize_skip_indexes_on_insert", href: "/zh/reference/settings/session-settings/other#exclude_materialize_skip_indexes_on_insert" },
        { name: "execute_exists_as_scalar_subquery", href: "/zh/reference/settings/session-settings/other#execute_exists_as_scalar_subquery" },
        { name: "explain_query_plan_default", href: "/zh/reference/settings/session-settings/other#explain_query_plan_default" },
        { name: "extract_key_value_pairs_max_pairs_per_row", href: "/zh/reference/settings/session-settings/other#extract_key_value_pairs_max_pairs_per_row" },
        { name: "extremes", href: "/zh/reference/settings/session-settings/other#extremes" },
        { name: "fallback_to_stale_replicas_for_distributed_queries", href: "/zh/reference/settings/session-settings/other#fallback_to_stale_replicas_for_distributed_queries" },
        { name: "file_like_engine_default_partition_strategy", href: "/zh/reference/settings/session-settings/other#file_like_engine_default_partition_strategy" },
        { name: "filesystem_prefetches_limit", href: "/zh/reference/settings/session-settings/other#filesystem_prefetches_limit" },
        { name: "final", href: "/zh/reference/settings/session-settings/other#final" },
        { name: "finalize_projection_parts_synchronously", href: "/zh/reference/settings/session-settings/other#finalize_projection_parts_synchronously" },
        { name: "flatten_nested", href: "/zh/reference/settings/session-settings/other#flatten_nested" },
        { name: "fsync_metadata", href: "/zh/reference/settings/session-settings/other#fsync_metadata" },
        { name: "functions_h3_default_if_invalid", href: "/zh/reference/settings/session-settings/other#functions_h3_default_if_invalid" },
        { name: "geo_distance_returns_float64_on_float64_arguments", href: "/zh/reference/settings/session-settings/other#geo_distance_returns_float64_on_float64_arguments" },
        { name: "geotoh3_argument_order", href: "/zh/reference/settings/session-settings/other#geotoh3_argument_order" },
        { name: "glob_expansion_max_elements", href: "/zh/reference/settings/session-settings/other#glob_expansion_max_elements" },
        { name: "h3togeo_lon_lat_result_order", href: "/zh/reference/settings/session-settings/other#h3togeo_lon_lat_result_order" },
        { name: "handshake_timeout_ms", href: "/zh/reference/settings/session-settings/other#handshake_timeout_ms" },
        { name: "hedged_connection_timeout_ms", href: "/zh/reference/settings/session-settings/other#hedged_connection_timeout_ms" },
        { name: "highlight_max_matches_per_row", href: "/zh/reference/settings/session-settings/other#highlight_max_matches_per_row" },
        { name: "hnsw_candidate_list_size_for_search", href: "/zh/reference/settings/session-settings/other#hnsw_candidate_list_size_for_search" },
        { name: "hsts_max_age", href: "/zh/reference/settings/session-settings/other#hsts_max_age" },
        { name: "idle_connection_timeout", href: "/zh/reference/settings/session-settings/other#idle_connection_timeout" },
        { name: "inject_random_order_for_select_without_order_by", href: "/zh/reference/settings/session-settings/other#inject_random_order_for_select_without_order_by" },
        { name: "interactive_delay", href: "/zh/reference/settings/session-settings/other#interactive_delay" },
        { name: "intersect_default_mode", href: "/zh/reference/settings/session-settings/other#intersect_default_mode" },
        { name: "least_greatest_legacy_null_behavior", href: "/zh/reference/settings/session-settings/other#least_greatest_legacy_null_behavior" },
        { name: "legacy_column_name_of_tuple_literal", href: "/zh/reference/settings/session-settings/other#legacy_column_name_of_tuple_literal" },
        { name: "limit", href: "/zh/reference/settings/session-settings/other#limit" },
        { name: "load_marks_asynchronously", href: "/zh/reference/settings/session-settings/other#load_marks_asynchronously" },
        { name: "lock_acquire_timeout", href: "/zh/reference/settings/session-settings/other#lock_acquire_timeout" },
        { name: "low_priority_query_wait_time_ms", href: "/zh/reference/settings/session-settings/other#low_priority_query_wait_time_ms" },
        { name: "make_distributed_plan", href: "/zh/reference/settings/session-settings/other#make_distributed_plan" },
        { name: "merge_table_max_tables_to_look_for_schema_inference", href: "/zh/reference/settings/session-settings/other#merge_table_max_tables_to_look_for_schema_inference" },
        { name: "mongodb_throw_on_unsupported_query", href: "/zh/reference/settings/session-settings/other#mongodb_throw_on_unsupported_query" },
        { name: "multiple_joins_try_to_keep_original_names", href: "/zh/reference/settings/session-settings/other#multiple_joins_try_to_keep_original_names" },
        { name: "normalize_function_names", href: "/zh/reference/settings/session-settings/other#normalize_function_names" },
        { name: "offset", href: "/zh/reference/settings/session-settings/other#offset" },
        { name: "paimon_target_snapshot_id", href: "/zh/reference/settings/session-settings/other#paimon_target_snapshot_id" },
        { name: "parallelize_output_from_storages", href: "/zh/reference/settings/session-settings/other#parallelize_output_from_storages" },
        { name: "partial_result_on_first_cancel", href: "/zh/reference/settings/session-settings/other#partial_result_on_first_cancel" },
        { name: "per_part_index_stats", href: "/zh/reference/settings/session-settings/other#per_part_index_stats" },
        { name: "poll_interval", href: "/zh/reference/settings/session-settings/other#poll_interval" },
        { name: "polyglot_dialect", href: "/zh/reference/settings/session-settings/other#polyglot_dialect" },
        { name: "postgresql_fault_injection_probability", href: "/zh/reference/settings/session-settings/other#postgresql_fault_injection_probability" },
        { name: "predicate_statistics_sample_rate", href: "/zh/reference/settings/session-settings/other#predicate_statistics_sample_rate" },
        { name: "prefetch_buffer_size", href: "/zh/reference/settings/session-settings/other#prefetch_buffer_size" },
        { name: "print_pretty_type_names", href: "/zh/reference/settings/session-settings/other#print_pretty_type_names" },
        { name: "priority", href: "/zh/reference/settings/session-settings/other#priority" },
        { name: "push_external_roles_in_interserver_queries", href: "/zh/reference/settings/session-settings/other#push_external_roles_in_interserver_queries" },
        { name: "query_metric_log_interval", href: "/zh/reference/settings/session-settings/other#query_metric_log_interval" },
        { name: "queue_max_wait_ms", href: "/zh/reference/settings/session-settings/other#queue_max_wait_ms" },
        { name: "rabbitmq_max_wait_ms", href: "/zh/reference/settings/session-settings/other#rabbitmq_max_wait_ms" },
        { name: "readonly", href: "/zh/reference/settings/session-settings/other#readonly" },
        { name: "recursive_cte_max_steps_in_type_inference", href: "/zh/reference/settings/session-settings/other#recursive_cte_max_steps_in_type_inference" },
        { name: "regexp_max_matches_per_row", href: "/zh/reference/settings/session-settings/other#regexp_max_matches_per_row" },
        { name: "reject_expensive_hyperscan_regexps", href: "/zh/reference/settings/session-settings/other#reject_expensive_hyperscan_regexps" },
        { name: "remerge_sort_lowered_memory_bytes_ratio", href: "/zh/reference/settings/session-settings/other#remerge_sort_lowered_memory_bytes_ratio" },
        { name: "remote_read_min_bytes_for_seek", href: "/zh/reference/settings/session-settings/other#remote_read_min_bytes_for_seek" },
        { name: "rename_files_after_processing", href: "/zh/reference/settings/session-settings/other#rename_files_after_processing" },
        { name: "replication_wait_for_inactive_replica_timeout", href: "/zh/reference/settings/session-settings/other#replication_wait_for_inactive_replica_timeout" },
        { name: "reserve_memory", href: "/zh/reference/settings/session-settings/other#reserve_memory" },
        { name: "restore_replicated_merge_tree_to_shared_merge_tree", href: "/zh/reference/settings/session-settings/other#restore_replicated_merge_tree_to_shared_merge_tree" },
        { name: "result_overflow_mode", href: "/zh/reference/settings/session-settings/other#result_overflow_mode" },
        { name: "rows_before_aggregation", href: "/zh/reference/settings/session-settings/other#rows_before_aggregation" },
        { name: "secondary_indices_enable_bulk_filtering", href: "/zh/reference/settings/session-settings/other#secondary_indices_enable_bulk_filtering" },
        { name: "select_sequential_consistency", href: "/zh/reference/settings/session-settings/other#select_sequential_consistency" },
        { name: "session_timezone", href: "/zh/reference/settings/session-settings/other#session_timezone" },
        { name: "set_overflow_mode", href: "/zh/reference/settings/session-settings/other#set_overflow_mode" },
        { name: "single_join_prefer_left_table", href: "/zh/reference/settings/session-settings/other#single_join_prefer_left_table" },
        { name: "skip_redundant_aliases_in_udf", href: "/zh/reference/settings/session-settings/other#skip_redundant_aliases_in_udf" },
        { name: "sleep_after_receiving_query_ms", href: "/zh/reference/settings/session-settings/other#sleep_after_receiving_query_ms" },
        { name: "snappy_mode", href: "/zh/reference/settings/session-settings/other#snappy_mode" },
        { name: "sort_overflow_mode", href: "/zh/reference/settings/session-settings/other#sort_overflow_mode" },
        { name: "splitby_max_substrings_includes_remaining_string", href: "/zh/reference/settings/session-settings/other#splitby_max_substrings_includes_remaining_string" },
        { name: "stop_refreshable_materialized_views_on_startup", href: "/zh/reference/settings/session-settings/other#stop_refreshable_materialized_views_on_startup" },
        { name: "tcp_keep_alive_timeout", href: "/zh/reference/settings/session-settings/other#tcp_keep_alive_timeout" },
        {
          name: "temporary_data_in_cache_reserve_space_wait_lock_timeout_milliseconds",
          href: "/zh/reference/settings/session-settings/other#temporary_data_in_cache_reserve_space_wait_lock_timeout_milliseconds"
        },
        { name: "throw_if_no_data_to_insert", href: "/zh/reference/settings/session-settings/other#throw_if_no_data_to_insert" },
        { name: "timeout_before_checking_execution_speed", href: "/zh/reference/settings/session-settings/other#timeout_before_checking_execution_speed" },
        { name: "transfer_overflow_mode", href: "/zh/reference/settings/session-settings/other#transfer_overflow_mode" },
        { name: "transform_null_in", href: "/zh/reference/settings/session-settings/other#transform_null_in" },
        { name: "traverse_shadow_remote_data_paths", href: "/zh/reference/settings/session-settings/other#traverse_shadow_remote_data_paths" },
        { name: "union_default_mode", href: "/zh/reference/settings/session-settings/other#union_default_mode" },
        { name: "unique_key_max_encoded_size", href: "/zh/reference/settings/session-settings/other#unique_key_max_encoded_size" },
        { name: "unknown_packet_in_send_data", href: "/zh/reference/settings/session-settings/other#unknown_packet_in_send_data" },
        { name: "variant_throw_on_type_mismatch", href: "/zh/reference/settings/session-settings/other#variant_throw_on_type_mismatch" },
        { name: "wait_changes_become_visible_after_commit_mode", href: "/zh/reference/settings/session-settings/other#wait_changes_become_visible_after_commit_mode" },
        { name: "workload", href: "/zh/reference/settings/session-settings/other#workload" },
        { name: "write_full_path_in_iceberg_metadata", href: "/zh/reference/settings/session-settings/other#write_full_path_in_iceberg_metadata" },
        { name: "zstd_window_log_max", href: "/zh/reference/settings/session-settings/other#zstd_window_log_max" }
      ],
      children: []
    }
  ])
  const [anchorRoutes] = useState(() => ({
    add_http_cors_header: "/reference/settings/session-settings/other",
    additional_result_filter: "/reference/settings/session-settings/additional",
    additional_table_filters: "/reference/settings/session-settings/additional",
    aggregate_function_input_format: "/reference/settings/session-settings/aggregate",
    aggregate_functions_null_for_empty: "/reference/settings/session-settings/aggregate",
    aggregation_in_order_max_block_bytes: "/reference/settings/session-settings/aggregation",
    aggregation_memory_efficient_merge_threads: "/reference/settings/session-settings/aggregation",
    ai_function_embedding_default_credentials: "/reference/settings/session-settings/ai-function",
    ai_function_embedding_max_batch_size: "/reference/settings/session-settings/ai-function",
    ai_function_max_api_calls_per_query: "/reference/settings/session-settings/ai-function",
    ai_function_max_input_tokens_per_query: "/reference/settings/session-settings/ai-function",
    ai_function_max_output_tokens_per_query: "/reference/settings/session-settings/ai-function",
    ai_function_max_retries: "/reference/settings/session-settings/ai-function",
    ai_function_request_timeout_sec: "/reference/settings/session-settings/ai-function",
    ai_function_retry_initial_delay_ms: "/reference/settings/session-settings/ai-function",
    ai_function_text_default_credentials: "/reference/settings/session-settings/ai-function",
    ai_function_throw_on_error: "/reference/settings/session-settings/ai-function",
    ai_function_throw_on_quota_exceeded: "/reference/settings/session-settings/ai-function",
    allow_aggregate_partitions_independently: "/reference/settings/session-settings/allow",
    allow_archive_path_syntax: "/reference/settings/session-settings/allow",
    allow_asynchronous_read_from_io_pool_for_merge_tree: "/reference/settings/session-settings/allow",
    allow_calculating_subcolumns_sizes_for_merge_tree_reading: "/reference/settings/session-settings/allow",
    allow_changing_replica_until_first_data_packet: "/reference/settings/session-settings/allow",
    allow_create_index_without_type: "/reference/settings/session-settings/allow",
    allow_custom_error_code_in_throwif: "/reference/settings/session-settings/allow",
    allow_ddl: "/reference/settings/session-settings/allow",
    allow_deprecated_database_ordinary: "/reference/settings/session-settings/allow-deprecated",
    allow_deprecated_error_prone_window_functions: "/reference/settings/session-settings/allow-deprecated",
    allow_deprecated_syntax_for_merge_tree: "/reference/settings/session-settings/allow-deprecated",
    allow_distributed_ddl: "/reference/settings/session-settings/allow",
    allow_drop_detached: "/reference/settings/session-settings/allow",
    allow_dynamic_type_in_join_keys: "/reference/settings/session-settings/allow",
    allow_execute_multiif_columnar: "/reference/settings/session-settings/allow",
    allow_experimental_ai_functions: "/reference/settings/session-settings/allow-experimental",
    allow_experimental_analyzer: "/reference/settings/session-settings/allow-experimental",
    allow_experimental_cleanup_old_data_files_compaction: "/reference/settings/session-settings/allow-experimental",
    allow_experimental_codecs: "/reference/settings/session-settings/allow-experimental",
    allow_experimental_correlated_subqueries: "/reference/settings/session-settings/allow-experimental",
    allow_experimental_database_glue_catalog: "/reference/settings/session-settings/allow-experimental",
    allow_experimental_database_hms_catalog: "/reference/settings/session-settings/allow-experimental",
    allow_experimental_database_iceberg: "/reference/settings/session-settings/allow-experimental",
    allow_experimental_database_materialized_postgresql: "/reference/settings/session-settings/allow-experimental",
    allow_experimental_database_paimon_rest_catalog: "/reference/settings/session-settings/allow-experimental",
    allow_experimental_database_unity_catalog: "/reference/settings/session-settings/allow-experimental",
    allow_experimental_delta_kernel_rs: "/reference/settings/session-settings/allow-experimental",
    allow_experimental_delta_lake_writes: "/reference/settings/session-settings/allow-experimental",
    allow_experimental_eval_table_function: "/reference/settings/session-settings/allow-experimental",
    allow_experimental_expire_snapshots: "/reference/settings/session-settings/allow-experimental",
    allow_experimental_funnel_functions: "/reference/settings/session-settings/allow-experimental",
    allow_experimental_geo_types_in_iceberg: "/reference/settings/session-settings/allow-experimental",
    allow_experimental_hash_functions: "/reference/settings/session-settings/allow-experimental",
    allow_experimental_iceberg_compaction: "/reference/settings/session-settings/allow-experimental",
    allow_experimental_join_right_table_sorting: "/reference/settings/session-settings/allow-experimental",
    allow_experimental_json_lazy_type_hints: "/reference/settings/session-settings/allow-experimental",
    allow_experimental_kafka_offsets_storage_in_keeper: "/reference/settings/session-settings/allow-experimental",
    allow_experimental_kusto_dialect: "/reference/settings/session-settings/allow-experimental",
    allow_experimental_materialized_postgresql_table: "/reference/settings/session-settings/allow-experimental",
    allow_experimental_nlp_functions: "/reference/settings/session-settings/allow-experimental",
    allow_experimental_nullable_tuple_type: "/reference/settings/session-settings/allow-experimental",
    allow_experimental_object_storage_queue_hive_partitioning: "/reference/settings/session-settings/allow-experimental",
    allow_experimental_paimon_storage_engine: "/reference/settings/session-settings/allow-experimental",
    allow_experimental_parallel_reading_from_replicas: "/reference/settings/session-settings/allow-experimental",
    allow_experimental_polyglot_dialect: "/reference/settings/session-settings/allow-experimental",
    allow_experimental_prql_dialect: "/reference/settings/session-settings/allow-experimental",
    allow_experimental_time_series_aggregate_functions: "/reference/settings/session-settings/allow-experimental",
    allow_experimental_time_series_table: "/reference/settings/session-settings/allow-experimental",
    allow_experimental_unique_key: "/reference/settings/session-settings/allow-experimental",
    allow_experimental_url_wildcard_from_index_pages: "/reference/settings/session-settings/allow-experimental",
    allow_experimental_window_view: "/reference/settings/session-settings/allow-experimental",
    allow_experimental_ytsaurus_dictionary_source: "/reference/settings/session-settings/allow-experimental",
    allow_experimental_ytsaurus_table_engine: "/reference/settings/session-settings/allow-experimental",
    allow_experimental_ytsaurus_table_function: "/reference/settings/session-settings/allow-experimental",
    allow_fuzz_query_functions: "/reference/settings/session-settings/allow",
    allow_general_join_planning: "/reference/settings/session-settings/allow",
    allow_get_client_http_header: "/reference/settings/session-settings/allow",
    allow_hyperscan: "/reference/settings/session-settings/allow",
    allow_iceberg_remove_orphan_files: "/reference/settings/session-settings/allow",
    allow_insert_into_iceberg: "/reference/settings/session-settings/allow",
    allow_introspection_functions: "/reference/settings/session-settings/allow",
    allow_key_condition_coalesce_rewrite: "/reference/settings/session-settings/allow",
    allow_limit_by_partitions_independently: "/reference/settings/session-settings/allow",
    allow_materialized_view_with_bad_select: "/reference/settings/session-settings/allow",
    allow_minmax_index_for_json: "/reference/settings/session-settings/allow",
    allow_named_collection_override_by_default: "/reference/settings/session-settings/allow",
    allow_non_metadata_alters: "/reference/settings/session-settings/allow",
    allow_nonconst_timezone_arguments: "/reference/settings/session-settings/allow",
    allow_nondeterministic_mutations: "/reference/settings/session-settings/allow-nondeterministic",
    allow_nondeterministic_optimize_skip_unused_shards: "/reference/settings/session-settings/allow-nondeterministic",
    allow_nullable_tuple_in_extracted_subcolumns: "/reference/settings/session-settings/allow",
    allow_prefetched_read_pool_for_local_filesystem: "/reference/settings/session-settings/allow-prefetched",
    allow_prefetched_read_pool_for_remote_filesystem: "/reference/settings/session-settings/allow-prefetched",
    allow_push_predicate_ast_for_distributed_subqueries: "/reference/settings/session-settings/allow-push",
    allow_push_predicate_when_subquery_contains_with: "/reference/settings/session-settings/allow-push",
    allow_rank_dense_rank_arguments: "/reference/settings/session-settings/allow",
    allow_reorder_prewhere_conditions: "/reference/settings/session-settings/allow",
    allow_replace_partition_from_empty_source: "/reference/settings/session-settings/allow",
    allow_settings_after_format_in_insert: "/reference/settings/session-settings/allow",
    allow_simdjson: "/reference/settings/session-settings/allow",
    allow_special_serialization_kinds_in_output_formats: "/reference/settings/session-settings/allow",
    allow_statistics: "/reference/settings/session-settings/allow-statistics",
    allow_statistics_optimize: "/reference/settings/session-settings/allow-statistics",
    allow_suspicious_codecs: "/reference/settings/session-settings/allow-suspicious",
    allow_suspicious_fixed_string_types: "/reference/settings/session-settings/allow-suspicious",
    allow_suspicious_indices: "/reference/settings/session-settings/allow-suspicious",
    allow_suspicious_low_cardinality_types: "/reference/settings/session-settings/allow-suspicious",
    allow_suspicious_primary_key: "/reference/settings/session-settings/allow-suspicious",
    allow_suspicious_ttl_expressions: "/reference/settings/session-settings/allow-suspicious",
    allow_suspicious_types_in_group_by: "/reference/settings/session-settings/allow-suspicious",
    allow_suspicious_types_in_order_by: "/reference/settings/session-settings/allow-suspicious",
    allow_suspicious_variant_types: "/reference/settings/session-settings/allow-suspicious",
    allow_unrestricted_reads_from_keeper: "/reference/settings/session-settings/allow",
    alter_move_to_space_execute_async: "/reference/settings/session-settings/alter",
    alter_partition_verbose_result: "/reference/settings/session-settings/alter",
    alter_sync: "/reference/settings/session-settings/alter",
    alter_update_mode: "/reference/settings/session-settings/alter",
    analyze_index_with_space_filling_curves: "/reference/settings/session-settings/other",
    analyzer_compatibility_allow_compound_identifiers_in_unflatten_nested: "/reference/settings/session-settings/analyzer-compatibility",
    analyzer_compatibility_allow_non_aggregate_in_having: "/reference/settings/session-settings/analyzer-compatibility",
    analyzer_compatibility_join_using_top_level_identifier: "/reference/settings/session-settings/analyzer-compatibility",
    analyzer_compatibility_prefer_alias_over_subcolumn: "/reference/settings/session-settings/analyzer-compatibility",
    analyzer_inline_views: "/reference/settings/session-settings/other",
    any_join_distinct_right_table_keys: "/reference/settings/session-settings/other",
    apply_deleted_mask: "/reference/settings/session-settings/apply",
    apply_mutations_on_fly: "/reference/settings/session-settings/apply",
    apply_patch_parts: "/reference/settings/session-settings/apply-patch-parts",
    apply_patch_parts_join_cache_buckets: "/reference/settings/session-settings/apply-patch-parts",
    apply_prewhere_after_final: "/reference/settings/session-settings/apply",
    apply_row_policy_after_final: "/reference/settings/session-settings/apply",
    apply_settings_from_server: "/reference/settings/session-settings/apply",
    archive_adaptive_buffer_max_size_bytes: "/reference/settings/session-settings/other",
    arrow_flight_request_descriptor_type: "/reference/settings/session-settings/other",
    ast_fuzzer_any_query: "/reference/settings/session-settings/ast-fuzzer",
    ast_fuzzer_runs: "/reference/settings/session-settings/ast-fuzzer",
    asterisk_include_alias_columns: "/reference/settings/session-settings/asterisk-include",
    asterisk_include_materialized_columns: "/reference/settings/session-settings/asterisk-include",
    asterisk_include_virtual_columns: "/reference/settings/session-settings/asterisk-include",
    async_insert: "/reference/settings/session-settings/async-insert",
    async_insert_busy_timeout_decrease_rate: "/reference/settings/session-settings/async-insert",
    async_insert_busy_timeout_increase_rate: "/reference/settings/session-settings/async-insert",
    async_insert_busy_timeout_max_ms: "/reference/settings/session-settings/async-insert",
    async_insert_busy_timeout_min_ms: "/reference/settings/session-settings/async-insert",
    async_insert_deduplicate: "/reference/settings/session-settings/async-insert",
    async_insert_max_data_size: "/reference/settings/session-settings/async-insert",
    async_insert_max_query_number: "/reference/settings/session-settings/async-insert",
    async_insert_poll_timeout_ms: "/reference/settings/session-settings/async-insert",
    async_insert_use_adaptive_busy_timeout: "/reference/settings/session-settings/async-insert",
    async_query_sending_for_remote: "/reference/settings/session-settings/async",
    async_socket_for_remote: "/reference/settings/session-settings/async",
    automatic_parallel_replicas_min_bytes_per_replica: "/reference/settings/session-settings/automatic-parallel",
    automatic_parallel_replicas_mode: "/reference/settings/session-settings/automatic-parallel",
    azure_allow_parallel_part_upload: "/reference/settings/session-settings/azure",
    azure_check_objects_after_upload: "/reference/settings/session-settings/azure",
    azure_connect_timeout_ms: "/reference/settings/session-settings/azure",
    azure_create_new_file_on_insert: "/reference/settings/session-settings/azure",
    azure_ignore_file_doesnt_exist: "/reference/settings/session-settings/azure",
    azure_list_object_keys_size: "/reference/settings/session-settings/azure",
    azure_max_blocks_in_multipart_upload: "/reference/settings/session-settings/azure-max",
    azure_max_get_burst: "/reference/settings/session-settings/azure-max",
    azure_max_get_rps: "/reference/settings/session-settings/azure-max",
    azure_max_inflight_parts_for_one_file: "/reference/settings/session-settings/azure-max",
    azure_max_put_burst: "/reference/settings/session-settings/azure-max",
    azure_max_put_rps: "/reference/settings/session-settings/azure-max",
    azure_max_redirects: "/reference/settings/session-settings/azure-max",
    azure_max_single_part_copy_size: "/reference/settings/session-settings/azure-max",
    azure_max_single_part_upload_size: "/reference/settings/session-settings/azure-max",
    azure_max_single_read_retries: "/reference/settings/session-settings/azure-max",
    azure_max_unexpected_write_error_retries: "/reference/settings/session-settings/azure-max",
    azure_max_upload_part_size: "/reference/settings/session-settings/azure-max",
    azure_min_upload_part_size: "/reference/settings/session-settings/azure",
    azure_request_timeout_ms: "/reference/settings/session-settings/azure",
    azure_sdk_max_retries: "/reference/settings/session-settings/azure-sdk",
    azure_sdk_retry_initial_backoff_ms: "/reference/settings/session-settings/azure-sdk",
    azure_sdk_retry_max_backoff_ms: "/reference/settings/session-settings/azure-sdk",
    azure_skip_empty_files: "/reference/settings/session-settings/azure",
    azure_strict_upload_part_size: "/reference/settings/session-settings/azure",
    azure_throw_on_zero_files_match: "/reference/settings/session-settings/azure",
    azure_truncate_on_insert: "/reference/settings/session-settings/azure",
    azure_upload_part_size_multiply_factor: "/reference/settings/session-settings/azure-upload",
    azure_upload_part_size_multiply_parts_count_threshold: "/reference/settings/session-settings/azure-upload",
    azure_use_adaptive_timeouts: "/reference/settings/session-settings/azure",
    backup_restore_batch_size_for_keeper_multi: "/reference/settings/session-settings/backup-restore",
    backup_restore_batch_size_for_keeper_multiread: "/reference/settings/session-settings/backup-restore",
    backup_restore_failure_after_host_disconnected_for_seconds: "/reference/settings/session-settings/backup-restore",
    backup_restore_finish_timeout_after_error_sec: "/reference/settings/session-settings/backup-restore",
    backup_restore_keeper_fault_injection_probability: "/reference/settings/session-settings/backup-restore",
    backup_restore_keeper_fault_injection_seed: "/reference/settings/session-settings/backup-restore",
    backup_restore_keeper_max_retries: "/reference/settings/session-settings/backup-restore",
    backup_restore_keeper_max_retries_while_handling_error: "/reference/settings/session-settings/backup-restore",
    backup_restore_keeper_max_retries_while_initializing: "/reference/settings/session-settings/backup-restore",
    backup_restore_keeper_retry_initial_backoff_ms: "/reference/settings/session-settings/backup-restore",
    backup_restore_keeper_retry_max_backoff_ms: "/reference/settings/session-settings/backup-restore",
    backup_restore_keeper_value_max_size: "/reference/settings/session-settings/backup-restore",
    backup_restore_s3_retry_attempts: "/reference/settings/session-settings/backup-restore",
    backup_restore_s3_retry_initial_backoff_ms: "/reference/settings/session-settings/backup-restore",
    backup_restore_s3_retry_jitter_factor: "/reference/settings/session-settings/backup-restore",
    backup_restore_s3_retry_max_backoff_ms: "/reference/settings/session-settings/backup-restore",
    backup_slow_all_threads_after_retryable_s3_error: "/reference/settings/session-settings/other",
    cache_warmer_threads: "/reference/settings/session-settings/other",
    calculate_text_stack_trace: "/reference/settings/session-settings/other",
    cancel_http_readonly_queries_on_client_close: "/reference/settings/session-settings/other",
    cast_ipv4_ipv6_default_on_conversion_error: "/reference/settings/session-settings/cast",
    cast_keep_nullable: "/reference/settings/session-settings/cast",
    cast_string_to_date_time_mode: "/reference/settings/session-settings/cast-string",
    cast_string_to_dynamic_use_inference: "/reference/settings/session-settings/cast-string",
    cast_string_to_variant_use_inference: "/reference/settings/session-settings/cast-string",
    check_named_collection_dependencies: "/reference/settings/session-settings/check",
    check_query_single_value_result: "/reference/settings/session-settings/check",
    check_referential_table_dependencies: "/reference/settings/session-settings/check",
    check_table_dependencies: "/reference/settings/session-settings/check",
    checksum_on_read: "/reference/settings/session-settings/other",
    cloud_mode: "/reference/settings/session-settings/cloud-mode",
    cloud_mode_engine: "/reference/settings/session-settings/cloud-mode",
    cluster_for_parallel_replicas: "/reference/settings/session-settings/cluster",
    cluster_function_process_archive_on_multiple_nodes: "/reference/settings/session-settings/cluster",
    cluster_table_function_buckets_batch_size: "/reference/settings/session-settings/cluster-table",
    cluster_table_function_split_granularity: "/reference/settings/session-settings/cluster-table",
    collect_hash_table_stats_during_aggregation: "/reference/settings/session-settings/collect-hash",
    collect_hash_table_stats_during_joins: "/reference/settings/session-settings/collect-hash",
    compatibility: "/reference/settings/session-settings/compatibility",
    compatibility_ignore_auto_increment_in_create_table: "/reference/settings/session-settings/compatibility-ignore",
    compatibility_ignore_collation_in_create_table: "/reference/settings/session-settings/compatibility-ignore",
    compatibility_s3_presigned_url_query_in_path: "/reference/settings/session-settings/compatibility",
    compile_aggregate_expressions: "/reference/settings/session-settings/compile",
    compile_expressions: "/reference/settings/session-settings/compile",
    compile_regular_expressions: "/reference/settings/session-settings/compile",
    compile_sort_description: "/reference/settings/session-settings/compile",
    connect_timeout: "/reference/settings/session-settings/connect-timeout",
    connect_timeout_with_failover_ms: "/reference/settings/session-settings/connect-timeout",
    connect_timeout_with_failover_secure_ms: "/reference/settings/session-settings/connect-timeout",
    connection_pool_max_wait_ms: "/reference/settings/session-settings/other",
    connections_with_failover_max_tries: "/reference/settings/session-settings/other",
    convert_query_to_cnf: "/reference/settings/session-settings/other",
    correlated_subqueries_default_join_kind: "/reference/settings/session-settings/correlated-subqueries",
    correlated_subqueries_substitute_equivalent_expressions: "/reference/settings/session-settings/correlated-subqueries",
    correlated_subqueries_use_in_memory_buffer: "/reference/settings/session-settings/correlated-subqueries",
    count_distinct_implementation: "/reference/settings/session-settings/count-distinct",
    count_distinct_optimization: "/reference/settings/session-settings/count-distinct",
    count_matches_stop_at_empty_match: "/reference/settings/session-settings/other",
    create_if_not_exists: "/reference/settings/session-settings/create",
    create_index_ignore_unique: "/reference/settings/session-settings/create",
    create_replicated_merge_tree_fault_injection_probability: "/reference/settings/session-settings/create",
    create_table_empty_primary_key_by_default: "/reference/settings/session-settings/create",
    cross_join_min_bytes_to_compress: "/reference/settings/session-settings/cross-join",
    cross_join_min_rows_to_compress: "/reference/settings/session-settings/cross-join",
    cross_to_inner_join_rewrite: "/reference/settings/session-settings/other",
    data_type_default_nullable: "/reference/settings/session-settings/other",
    database_atomic_wait_for_drop_and_detach_synchronously: "/reference/settings/session-settings/database",
    database_datalake_require_metadata_access: "/reference/settings/session-settings/database",
    database_replicated_allow_explicit_uuid: "/reference/settings/session-settings/database-replicated",
    database_replicated_allow_heavy_create: "/reference/settings/session-settings/database-replicated",
    database_replicated_allow_only_replicated_engine: "/reference/settings/session-settings/database-replicated",
    database_replicated_allow_replicated_engine_arguments: "/reference/settings/session-settings/database-replicated",
    database_replicated_always_detach_permanently: "/reference/settings/session-settings/database-replicated",
    database_replicated_enforce_synchronous_settings: "/reference/settings/session-settings/database-replicated",
    database_replicated_initial_query_timeout_sec: "/reference/settings/session-settings/database-replicated",
    database_shared_drop_table_delay_seconds: "/reference/settings/session-settings/database",
    dead_blobs_to_delay_insert: "/reference/settings/session-settings/dead-blobs",
    dead_blobs_to_throw_insert: "/reference/settings/session-settings/dead-blobs",
    decimal_check_overflow: "/reference/settings/session-settings/other",
    deduplicate_blocks_in_dependent_materialized_views: "/reference/settings/session-settings/other",
    deduplicate_insert: "/reference/settings/session-settings/deduplicate-insert",
    deduplicate_insert_select: "/reference/settings/session-settings/deduplicate-insert",
    default_materialized_view_sql_security: "/reference/settings/session-settings/default",
    default_max_bytes_in_join: "/reference/settings/session-settings/default",
    default_normal_view_sql_security: "/reference/settings/session-settings/default",
    default_table_engine: "/reference/settings/session-settings/default",
    default_temporary_table_engine: "/reference/settings/session-settings/default",
    default_view_definer: "/reference/settings/session-settings/default",
    defer_partition_pruning_after_final: "/reference/settings/session-settings/other",
    delta_lake_enable_engine_predicate: "/reference/settings/session-settings/delta-lake",
    delta_lake_enable_expression_visitor_logging: "/reference/settings/session-settings/delta-lake",
    delta_lake_insert_max_bytes_in_data_file: "/reference/settings/session-settings/delta-lake",
    delta_lake_insert_max_rows_in_data_file: "/reference/settings/session-settings/delta-lake",
    delta_lake_log_metadata: "/reference/settings/session-settings/delta-lake",
    delta_lake_reload_schema_for_consistency: "/reference/settings/session-settings/delta-lake",
    delta_lake_snapshot_end_version: "/reference/settings/session-settings/delta-lake",
    delta_lake_snapshot_start_version: "/reference/settings/session-settings/delta-lake",
    delta_lake_snapshot_version: "/reference/settings/session-settings/delta-lake",
    delta_lake_throw_on_engine_predicate_error: "/reference/settings/session-settings/delta-lake",
    describe_compact_output: "/reference/settings/session-settings/other",
    describe_include_subcolumns: "/reference/settings/session-settings/describe-include",
    describe_include_virtual_columns: "/reference/settings/session-settings/describe-include",
    dialect: "/reference/settings/session-settings/other",
    dictionary_lazy_load: "/reference/settings/session-settings/dictionary",
    dictionary_use_async_executor: "/reference/settings/session-settings/dictionary",
    dictionary_validate_primary_key_type: "/reference/settings/session-settings/dictionary",
    "disabling-the-setting": "/reference/settings/session-settings/min",
    discard_query_data: "/reference/settings/session-settings/other",
    distinct_overflow_mode: "/reference/settings/session-settings/other",
    distributed_aggregation_memory_efficient: "/reference/settings/session-settings/distributed",
    distributed_background_insert_batch: "/reference/settings/session-settings/distributed-background",
    distributed_background_insert_max_sleep_time_ms: "/reference/settings/session-settings/distributed-background",
    distributed_background_insert_sleep_time_ms: "/reference/settings/session-settings/distributed-background",
    distributed_background_insert_split_batch_on_failure: "/reference/settings/session-settings/distributed-background",
    distributed_background_insert_timeout: "/reference/settings/session-settings/distributed-background",
    distributed_cache_alignment: "/reference/settings/session-settings/distributed-cache",
    distributed_cache_bypass_connection_pool: "/reference/settings/session-settings/distributed-cache",
    distributed_cache_connect_backoff_max_ms: "/reference/settings/session-settings/distributed-cache",
    distributed_cache_connect_backoff_min_ms: "/reference/settings/session-settings/distributed-cache",
    distributed_cache_connect_max_tries: "/reference/settings/session-settings/distributed-cache",
    distributed_cache_connect_timeout_ms: "/reference/settings/session-settings/distributed-cache",
    distributed_cache_credentials_refresh_period_seconds: "/reference/settings/session-settings/distributed-cache",
    distributed_cache_data_packet_ack_window: "/reference/settings/session-settings/distributed-cache",
    distributed_cache_discard_connection_if_unread_data: "/reference/settings/session-settings/distributed-cache",
    distributed_cache_fetch_metrics_only_from_current_az: "/reference/settings/session-settings/distributed-cache",
    distributed_cache_file_cache_name: "/reference/settings/session-settings/distributed-cache",
    distributed_cache_log_mode: "/reference/settings/session-settings/distributed-cache",
    distributed_cache_max_unacked_inflight_packets: "/reference/settings/session-settings/distributed-cache",
    distributed_cache_min_bytes_for_seek: "/reference/settings/session-settings/distributed-cache",
    distributed_cache_pool_behaviour_on_limit: "/reference/settings/session-settings/distributed-cache",
    distributed_cache_prefer_bigger_buffer_size: "/reference/settings/session-settings/distributed-cache",
    distributed_cache_read_only_from_current_az: "/reference/settings/session-settings/distributed-cache",
    distributed_cache_read_request_max_tries: "/reference/settings/session-settings/distributed-cache",
    distributed_cache_receive_response_wait_milliseconds: "/reference/settings/session-settings/distributed-cache",
    distributed_cache_receive_timeout_milliseconds: "/reference/settings/session-settings/distributed-cache",
    distributed_cache_receive_timeout_ms: "/reference/settings/session-settings/distributed-cache",
    distributed_cache_registry_show_certificate_and_signature: "/reference/settings/session-settings/distributed-cache",
    distributed_cache_send_timeout_ms: "/reference/settings/session-settings/distributed-cache",
    distributed_cache_tcp_keep_alive_timeout_ms: "/reference/settings/session-settings/distributed-cache",
    distributed_cache_throw_on_error: "/reference/settings/session-settings/distributed-cache",
    distributed_cache_use_clients_cache_for_read: "/reference/settings/session-settings/distributed-cache",
    distributed_cache_wait_connection_from_pool_milliseconds: "/reference/settings/session-settings/distributed-cache",
    distributed_cache_write_request_max_tries: "/reference/settings/session-settings/distributed-cache",
    distributed_connections_pool_size: "/reference/settings/session-settings/distributed",
    distributed_ddl_entry_format_version: "/reference/settings/session-settings/distributed-ddl",
    distributed_ddl_output_mode: "/reference/settings/session-settings/distributed-ddl",
    distributed_ddl_task_timeout: "/reference/settings/session-settings/distributed-ddl",
    distributed_foreground_insert: "/reference/settings/session-settings/distributed",
    distributed_group_by_no_merge: "/reference/settings/session-settings/distributed",
    distributed_index_analysis: "/reference/settings/session-settings/distributed-index-analysis",
    distributed_index_analysis_for_non_shared_merge_tree: "/reference/settings/session-settings/distributed-index-analysis",
    distributed_index_analysis_only_on_coordinator: "/reference/settings/session-settings/distributed-index-analysis",
    distributed_insert_skip_read_only_replicas: "/reference/settings/session-settings/distributed",
    distributed_plan_default_reader_bucket_count: "/reference/settings/session-settings/distributed-plan",
    distributed_plan_default_shuffle_join_bucket_count: "/reference/settings/session-settings/distributed-plan",
    distributed_plan_execute_locally: "/reference/settings/session-settings/distributed-plan",
    distributed_plan_force_exchange_kind: "/reference/settings/session-settings/distributed-plan",
    distributed_plan_force_shuffle_aggregation: "/reference/settings/session-settings/distributed-plan",
    distributed_plan_max_rows_to_broadcast: "/reference/settings/session-settings/distributed-plan",
    distributed_plan_optimize_exchanges: "/reference/settings/session-settings/distributed-plan",
    distributed_plan_prefer_replicas_over_workers: "/reference/settings/session-settings/distributed-plan",
    distributed_plan_workers_num: "/reference/settings/session-settings/distributed-plan",
    distributed_product_mode: "/reference/settings/session-settings/distributed",
    distributed_push_down_limit: "/reference/settings/session-settings/distributed",
    distributed_replica_error_cap: "/reference/settings/session-settings/distributed-replica",
    distributed_replica_error_half_life: "/reference/settings/session-settings/distributed-replica",
    distributed_replica_max_ignored_errors: "/reference/settings/session-settings/distributed-replica",
    do_not_merge_across_partitions_select_final: "/reference/settings/session-settings/other",
    dynamic_disk_allow_from_env: "/reference/settings/session-settings/dynamic-disk",
    dynamic_disk_allow_from_zk: "/reference/settings/session-settings/dynamic-disk",
    dynamic_disk_allow_include: "/reference/settings/session-settings/dynamic-disk",
    dynamic_throw_on_type_mismatch: "/reference/settings/session-settings/other",
    empty_result_for_aggregation_by_constant_keys_on_empty_set: "/reference/settings/session-settings/empty-result",
    empty_result_for_aggregation_by_empty_set: "/reference/settings/session-settings/empty-result",
    enable_adaptive_memory_spill_scheduler: "/reference/settings/session-settings/enable",
    enable_add_distinct_to_in_subqueries: "/reference/settings/session-settings/enable",
    enable_automatic_decision_for_merging_across_partitions_for_final: "/reference/settings/session-settings/enable",
    enable_blob_storage_log: "/reference/settings/session-settings/enable-blob-storage-log",
    enable_blob_storage_log_for_read_operations: "/reference/settings/session-settings/enable-blob-storage-log",
    enable_early_constant_folding: "/reference/settings/session-settings/enable",
    enable_extended_results_for_datetime_functions: "/reference/settings/session-settings/enable",
    enable_filesystem_cache: "/reference/settings/session-settings/enable-filesystem",
    enable_filesystem_cache_log: "/reference/settings/session-settings/enable-filesystem",
    enable_filesystem_cache_on_write_operations: "/reference/settings/session-settings/enable-filesystem",
    enable_filesystem_read_prefetches_log: "/reference/settings/session-settings/enable-filesystem",
    enable_full_text_index: "/reference/settings/session-settings/enable",
    enable_global_with_statement: "/reference/settings/session-settings/enable",
    enable_hdfs_pread: "/reference/settings/session-settings/enable",
    enable_http_compression: "/reference/settings/session-settings/enable",
    enable_identifier_resolve_cache: "/reference/settings/session-settings/enable",
    enable_job_stack_trace: "/reference/settings/session-settings/enable",
    enable_join_fixed_hash_table_conversion: "/reference/settings/session-settings/enable-join",
    enable_join_runtime_filters: "/reference/settings/session-settings/enable-join",
    enable_join_runtime_filters_index_analysis: "/reference/settings/session-settings/enable-join",
    enable_join_transitive_predicates: "/reference/settings/session-settings/enable-join",
    enable_lazy_columns_replication: "/reference/settings/session-settings/enable",
    enable_lightweight_delete: "/reference/settings/session-settings/enable-lightweight",
    enable_lightweight_update: "/reference/settings/session-settings/enable-lightweight",
    enable_materialized_cte: "/reference/settings/session-settings/enable",
    enable_memory_bound_merging_of_aggregation_results: "/reference/settings/session-settings/enable",
    enable_multiple_prewhere_read_steps: "/reference/settings/session-settings/enable",
    enable_named_columns_in_function_tuple: "/reference/settings/session-settings/enable",
    enable_optimize_predicate_expression: "/reference/settings/session-settings/enable-optimize-predicate-expression",
    enable_optimize_predicate_expression_to_final_subquery: "/reference/settings/session-settings/enable-optimize-predicate-expression",
    enable_order_by_all: "/reference/settings/session-settings/enable",
    enable_parallel_blocks_marshalling: "/reference/settings/session-settings/enable",
    enable_parsing_to_custom_serialization: "/reference/settings/session-settings/enable",
    enable_positional_arguments: "/reference/settings/session-settings/enable-positional-arguments",
    enable_positional_arguments_for_projections: "/reference/settings/session-settings/enable-positional-arguments",
    enable_producing_buckets_out_of_order_in_aggregation: "/reference/settings/session-settings/enable",
    enable_reads_from_query_cache: "/reference/settings/session-settings/enable",
    enable_s3_requests_logging: "/reference/settings/session-settings/enable",
    enable_scalar_subquery_optimization: "/reference/settings/session-settings/enable",
    enable_scopes_for_with_statement: "/reference/settings/session-settings/enable",
    enable_sharding_aggregator: "/reference/settings/session-settings/enable",
    enable_shared_storage_snapshot_in_query: "/reference/settings/session-settings/enable",
    enable_sharing_sets_for_mutations: "/reference/settings/session-settings/enable",
    enable_software_prefetch_in_aggregation: "/reference/settings/session-settings/enable-software",
    enable_software_prefetch_in_join: "/reference/settings/session-settings/enable-software",
    enable_streaming_queries: "/reference/settings/session-settings/enable",
    enable_time_time64_type: "/reference/settings/session-settings/enable",
    enable_unaligned_array_join: "/reference/settings/session-settings/enable",
    enable_url_encoding: "/reference/settings/session-settings/enable",
    enable_vertical_final: "/reference/settings/session-settings/enable",
    enable_writes_to_query_cache: "/reference/settings/session-settings/enable",
    enforce_strict_identifier_format: "/reference/settings/session-settings/other",
    engine_file_allow_create_multiple_files: "/reference/settings/session-settings/engine-file",
    engine_file_empty_if_not_exists: "/reference/settings/session-settings/engine-file",
    engine_file_skip_empty_files: "/reference/settings/session-settings/engine-file",
    engine_file_truncate_on_insert: "/reference/settings/session-settings/engine-file",
    engine_url_skip_empty_files: "/reference/settings/session-settings/other",
    exact_rows_before_limit: "/reference/settings/session-settings/other",
    example: "/reference/settings/session-settings/other",
    except_default_mode: "/reference/settings/session-settings/other",
    exclude_materialize_skip_indexes_on_insert: "/reference/settings/session-settings/other",
    execute_exists_as_scalar_subquery: "/reference/settings/session-settings/other",
    explain_query_plan_default: "/reference/settings/session-settings/other",
    external_storage_connect_timeout_sec: "/reference/settings/session-settings/external-storage",
    external_storage_max_read_bytes: "/reference/settings/session-settings/external-storage",
    external_storage_max_read_rows: "/reference/settings/session-settings/external-storage",
    external_storage_rw_timeout_sec: "/reference/settings/session-settings/external-storage",
    external_table_functions_use_nulls: "/reference/settings/session-settings/external-table",
    external_table_strict_query: "/reference/settings/session-settings/external-table",
    extract_key_value_pairs_max_pairs_per_row: "/reference/settings/session-settings/other",
    extremes: "/reference/settings/session-settings/other",
    fallback_to_stale_replicas_for_distributed_queries: "/reference/settings/session-settings/other",
    file_like_engine_default_partition_strategy: "/reference/settings/session-settings/other",
    filesystem_cache_allow_background_download: "/reference/settings/session-settings/filesystem-cache",
    filesystem_cache_boundary_alignment: "/reference/settings/session-settings/filesystem-cache",
    filesystem_cache_enable_background_download_during_fetch: "/reference/settings/session-settings/filesystem-cache",
    filesystem_cache_enable_background_download_for_metadata_files_in_packed_storage: "/reference/settings/session-settings/filesystem-cache",
    filesystem_cache_max_download_size: "/reference/settings/session-settings/filesystem-cache",
    filesystem_cache_name: "/reference/settings/session-settings/filesystem-cache",
    filesystem_cache_prefer_bigger_buffer_size: "/reference/settings/session-settings/filesystem-cache",
    filesystem_cache_reserve_space_wait_lock_timeout_milliseconds: "/reference/settings/session-settings/filesystem-cache",
    filesystem_cache_segments_batch_size: "/reference/settings/session-settings/filesystem-cache",
    filesystem_cache_skip_download_if_exceeds_per_query_cache_write_limit: "/reference/settings/session-settings/filesystem-cache",
    filesystem_prefetch_max_memory_usage: "/reference/settings/session-settings/filesystem-prefetch",
    filesystem_prefetch_step_bytes: "/reference/settings/session-settings/filesystem-prefetch",
    filesystem_prefetch_step_marks: "/reference/settings/session-settings/filesystem-prefetch",
    filesystem_prefetches_limit: "/reference/settings/session-settings/other",
    final: "/reference/settings/session-settings/other",
    finalize_projection_parts_synchronously: "/reference/settings/session-settings/other",
    flatten_nested: "/reference/settings/session-settings/other",
    force_aggregate_partitions_independently: "/reference/settings/session-settings/force",
    force_aggregation_in_order: "/reference/settings/session-settings/force",
    force_data_skipping_indices: "/reference/settings/session-settings/force",
    force_grouping_standard_compatibility: "/reference/settings/session-settings/force",
    force_index_by_date: "/reference/settings/session-settings/force",
    force_optimize_projection: "/reference/settings/session-settings/force-optimize",
    force_optimize_projection_name: "/reference/settings/session-settings/force-optimize",
    force_optimize_skip_unused_shards: "/reference/settings/session-settings/force-optimize",
    force_optimize_skip_unused_shards_nesting: "/reference/settings/session-settings/force-optimize",
    force_primary_key: "/reference/settings/session-settings/force",
    force_remove_data_recursively_on_drop: "/reference/settings/session-settings/force",
    formatdatetime_e_with_space_padding: "/reference/settings/session-settings/formatdatetime",
    formatdatetime_f_prints_scale_number_of_digits: "/reference/settings/session-settings/formatdatetime-f",
    formatdatetime_f_prints_single_zero: "/reference/settings/session-settings/formatdatetime-f",
    formatdatetime_format_without_leading_zeros: "/reference/settings/session-settings/formatdatetime",
    formatdatetime_parsedatetime_m_is_month_name: "/reference/settings/session-settings/formatdatetime",
    fsync_metadata: "/reference/settings/session-settings/other",
    function_base58_max_input_size: "/reference/settings/session-settings/function",
    function_date_trunc_return_type_behavior: "/reference/settings/session-settings/function",
    function_implementation: "/reference/settings/session-settings/function",
    function_json_value_return_type_allow_complex: "/reference/settings/session-settings/function-json",
    function_json_value_return_type_allow_nullable: "/reference/settings/session-settings/function-json",
    function_locate_has_mysql_compatible_argument_order: "/reference/settings/session-settings/function",
    function_range_max_elements_in_block: "/reference/settings/session-settings/function",
    function_sleep_max_microseconds_per_block: "/reference/settings/session-settings/function",
    function_visible_width_behavior: "/reference/settings/session-settings/function",
    functions_h3_default_if_invalid: "/reference/settings/session-settings/other",
    geo_distance_returns_float64_on_float64_arguments: "/reference/settings/session-settings/other",
    geotoh3_argument_order: "/reference/settings/session-settings/other",
    glob_expansion_max_elements: "/reference/settings/session-settings/other",
    grace_hash_join_initial_buckets: "/reference/settings/session-settings/grace-hash",
    grace_hash_join_max_buckets: "/reference/settings/session-settings/grace-hash",
    group_by_overflow_mode: "/reference/settings/session-settings/group-by",
    group_by_two_level_threshold: "/reference/settings/session-settings/group-by",
    group_by_two_level_threshold_bytes: "/reference/settings/session-settings/group-by",
    group_by_use_nulls: "/reference/settings/session-settings/group-by",
    h3togeo_lon_lat_result_order: "/reference/settings/session-settings/other",
    handshake_timeout_ms: "/reference/settings/session-settings/other",
    hdfs_create_new_file_on_insert: "/reference/settings/session-settings/hdfs",
    hdfs_ignore_file_doesnt_exist: "/reference/settings/session-settings/hdfs",
    hdfs_replication: "/reference/settings/session-settings/hdfs",
    hdfs_skip_empty_files: "/reference/settings/session-settings/hdfs",
    hdfs_throw_on_zero_files_match: "/reference/settings/session-settings/hdfs",
    hdfs_truncate_on_insert: "/reference/settings/session-settings/hdfs",
    hedged_connection_timeout_ms: "/reference/settings/session-settings/other",
    highlight_max_matches_per_row: "/reference/settings/session-settings/other",
    hnsw_candidate_list_size_for_search: "/reference/settings/session-settings/other",
    "how-the-resize-node-gets-split": "/reference/settings/session-settings/min",
    hsts_max_age: "/reference/settings/session-settings/other",
    http_connection_timeout: "/reference/settings/session-settings/http",
    http_headers_progress_interval_ms: "/reference/settings/session-settings/http-headers",
    http_headers_read_timeout: "/reference/settings/session-settings/http-headers",
    http_make_head_request: "/reference/settings/session-settings/http",
    http_max_field_name_size: "/reference/settings/session-settings/http-max",
    http_max_field_value_size: "/reference/settings/session-settings/http-max",
    http_max_fields: "/reference/settings/session-settings/http-max",
    http_max_multipart_form_data_size: "/reference/settings/session-settings/http-max",
    http_max_request_header_size: "/reference/settings/session-settings/http-max",
    http_max_request_param_data_size: "/reference/settings/session-settings/http-max",
    http_max_tries: "/reference/settings/session-settings/http-max",
    http_max_uri_size: "/reference/settings/session-settings/http-max",
    http_native_compression_disable_checksumming_on_decompress: "/reference/settings/session-settings/http",
    http_receive_timeout: "/reference/settings/session-settings/http",
    http_response_buffer_size: "/reference/settings/session-settings/http-response",
    http_response_headers: "/reference/settings/session-settings/http-response",
    http_retry_initial_backoff_ms: "/reference/settings/session-settings/http-retry",
    http_retry_max_backoff_ms: "/reference/settings/session-settings/http-retry",
    http_send_timeout: "/reference/settings/session-settings/http",
    http_skip_not_found_url_for_globs: "/reference/settings/session-settings/http",
    http_wait_end_of_query: "/reference/settings/session-settings/http",
    http_write_exception_in_output_format: "/reference/settings/session-settings/http",
    http_zlib_compression_level: "/reference/settings/session-settings/http",
    iceberg_compaction_data_cleanup: "/reference/settings/session-settings/iceberg-compaction",
    iceberg_compaction_delay_bias: "/reference/settings/session-settings/iceberg-compaction",
    iceberg_data_file_size_lower_threshold_compaction: "/reference/settings/session-settings/iceberg-data",
    iceberg_data_file_size_upper_threshold_compaction: "/reference/settings/session-settings/iceberg-data",
    iceberg_delete_data_on_drop: "/reference/settings/session-settings/iceberg",
    iceberg_expire_default_max_ref_age_ms: "/reference/settings/session-settings/iceberg-expire",
    iceberg_expire_default_max_snapshot_age_ms: "/reference/settings/session-settings/iceberg-expire",
    iceberg_expire_default_min_snapshots_to_keep: "/reference/settings/session-settings/iceberg-expire",
    iceberg_insert_max_bytes_in_data_file: "/reference/settings/session-settings/iceberg-insert",
    iceberg_insert_max_partitions: "/reference/settings/session-settings/iceberg-insert",
    iceberg_insert_max_rows_in_data_file: "/reference/settings/session-settings/iceberg-insert",
    iceberg_manifest_min_count_to_compact: "/reference/settings/session-settings/iceberg",
    iceberg_max_number_datafiles_to_compact: "/reference/settings/session-settings/iceberg",
    iceberg_metadata_compression_method: "/reference/settings/session-settings/iceberg-metadata",
    iceberg_metadata_log_level: "/reference/settings/session-settings/iceberg-metadata",
    iceberg_metadata_staleness_ms: "/reference/settings/session-settings/iceberg-metadata",
    iceberg_orphan_files_older_than_seconds: "/reference/settings/session-settings/iceberg",
    iceberg_snapshot_id: "/reference/settings/session-settings/iceberg",
    iceberg_timestamp_ms: "/reference/settings/session-settings/iceberg",
    idle_connection_timeout: "/reference/settings/session-settings/other",
    ignore_cold_parts_seconds: "/reference/settings/session-settings/ignore",
    ignore_data_skipping_indices: "/reference/settings/session-settings/ignore",
    ignore_drop_queries_probability: "/reference/settings/session-settings/ignore",
    ignore_format_null_for_explain: "/reference/settings/session-settings/ignore",
    ignore_materialized_views_with_dropped_target_table: "/reference/settings/session-settings/ignore",
    ignore_on_cluster_for_replicated_access_entities_queries: "/reference/settings/session-settings/ignore-on",
    ignore_on_cluster_for_replicated_database: "/reference/settings/session-settings/ignore-on",
    ignore_on_cluster_for_replicated_named_collections_queries: "/reference/settings/session-settings/ignore-on",
    ignore_on_cluster_for_replicated_udf_queries: "/reference/settings/session-settings/ignore-on",
    implicit_select: "/reference/settings/session-settings/implicit",
    implicit_table_at_top_level: "/reference/settings/session-settings/implicit",
    implicit_transaction: "/reference/settings/session-settings/implicit",
    inject_random_order_for_select_without_order_by: "/reference/settings/session-settings/other",
    insert_allow_materialized_columns: "/reference/settings/session-settings/insert",
    insert_deduplicate: "/reference/settings/session-settings/insert",
    insert_deduplication_token: "/reference/settings/session-settings/insert",
    insert_keeper_fault_injection_probability: "/reference/settings/session-settings/insert-keeper",
    insert_keeper_fault_injection_seed: "/reference/settings/session-settings/insert-keeper",
    insert_keeper_max_retries: "/reference/settings/session-settings/insert-keeper",
    insert_keeper_retry_initial_backoff_ms: "/reference/settings/session-settings/insert-keeper",
    insert_keeper_retry_max_backoff_ms: "/reference/settings/session-settings/insert-keeper",
    insert_null_as_default: "/reference/settings/session-settings/insert",
    insert_quorum: "/reference/settings/session-settings/insert-quorum",
    insert_quorum_parallel: "/reference/settings/session-settings/insert-quorum",
    insert_quorum_timeout: "/reference/settings/session-settings/insert-quorum",
    insert_shard_id: "/reference/settings/session-settings/insert",
    interactive_delay: "/reference/settings/session-settings/other",
    intersect_default_mode: "/reference/settings/session-settings/other",
    jemalloc_collect_profile_samples_in_trace_log: "/reference/settings/session-settings/jemalloc",
    jemalloc_enable_profiler: "/reference/settings/session-settings/jemalloc",
    jemalloc_profile_text_collapsed_use_count: "/reference/settings/session-settings/jemalloc-profile",
    jemalloc_profile_text_output_format: "/reference/settings/session-settings/jemalloc-profile",
    jemalloc_profile_text_symbolize_with_inline: "/reference/settings/session-settings/jemalloc-profile",
    join_algorithm: "/reference/settings/session-settings/join",
    join_any_take_last_row: "/reference/settings/session-settings/join",
    join_default_strictness: "/reference/settings/session-settings/join",
    join_on_disk_max_files_to_merge: "/reference/settings/session-settings/join",
    join_output_by_rowlist_perkey_rows_threshold: "/reference/settings/session-settings/join",
    join_overflow_mode: "/reference/settings/session-settings/join",
    join_runtime_bloom_filter_bytes: "/reference/settings/session-settings/join-runtime",
    join_runtime_bloom_filter_hash_functions: "/reference/settings/session-settings/join-runtime",
    join_runtime_bloom_filter_max_ratio_of_set_bits: "/reference/settings/session-settings/join-runtime",
    join_runtime_filter_blocks_to_skip_before_reenabling: "/reference/settings/session-settings/join-runtime",
    join_runtime_filter_exact_values_limit: "/reference/settings/session-settings/join-runtime",
    join_runtime_filter_from_fixed_hash_table: "/reference/settings/session-settings/join-runtime",
    join_runtime_filter_pass_ratio_threshold_for_disabling: "/reference/settings/session-settings/join-runtime",
    join_runtime_filter_size_from_hash_table_stats: "/reference/settings/session-settings/join-runtime",
    join_to_sort_maximum_table_rows: "/reference/settings/session-settings/join-to",
    join_to_sort_minimum_perkey_rows: "/reference/settings/session-settings/join-to",
    join_use_nulls: "/reference/settings/session-settings/join",
    joined_block_split_single_row: "/reference/settings/session-settings/joined",
    joined_subquery_requires_alias: "/reference/settings/session-settings/joined",
    kafka_disable_num_consumers_limit: "/reference/settings/session-settings/kafka",
    kafka_max_wait_ms: "/reference/settings/session-settings/kafka",
    keeper_map_strict_mode: "/reference/settings/session-settings/keeper",
    keeper_max_retries: "/reference/settings/session-settings/keeper",
    keeper_retry_initial_backoff_ms: "/reference/settings/session-settings/keeper-retry",
    keeper_retry_max_backoff_ms: "/reference/settings/session-settings/keeper-retry",
    least_greatest_legacy_null_behavior: "/reference/settings/session-settings/other",
    legacy_column_name_of_tuple_literal: "/reference/settings/session-settings/other",
    lightweight_delete_mode: "/reference/settings/session-settings/lightweight",
    lightweight_deletes_sync: "/reference/settings/session-settings/lightweight",
    limit: "/reference/settings/session-settings/other",
    load_balancing: "/reference/settings/session-settings/load-balancing",
    "load_balancing-first_or_random": "/reference/settings/session-settings/load-balancing",
    "load_balancing-hostname_levenshtein_distance": "/reference/settings/session-settings/load-balancing",
    "load_balancing-hostname_longest_common_prefix": "/reference/settings/session-settings/load-balancing",
    "load_balancing-hostname_longest_common_suffix": "/reference/settings/session-settings/load-balancing",
    "load_balancing-in_order": "/reference/settings/session-settings/load-balancing",
    "load_balancing-nearest_hostname": "/reference/settings/session-settings/load-balancing",
    "load_balancing-random": "/reference/settings/session-settings/load-balancing",
    "load_balancing-round_robin": "/reference/settings/session-settings/load-balancing",
    load_balancing_first_offset: "/reference/settings/session-settings/load-balancing",
    load_marks_asynchronously: "/reference/settings/session-settings/other",
    local_filesystem_read_method: "/reference/settings/session-settings/local-filesystem",
    local_filesystem_read_prefetch: "/reference/settings/session-settings/local-filesystem",
    lock_acquire_timeout: "/reference/settings/session-settings/other",
    log_comment: "/reference/settings/session-settings/log",
    log_formatted_queries: "/reference/settings/session-settings/log",
    log_processors_profiles: "/reference/settings/session-settings/log",
    log_profile_events: "/reference/settings/session-settings/log",
    log_queries: "/reference/settings/session-settings/log-queries",
    log_queries_cut_to_length: "/reference/settings/session-settings/log-queries",
    log_queries_min_query_duration_ms: "/reference/settings/session-settings/log-queries",
    log_queries_min_type: "/reference/settings/session-settings/log-queries",
    log_queries_probability: "/reference/settings/session-settings/log-queries",
    log_query_settings: "/reference/settings/session-settings/log-query",
    log_query_threads: "/reference/settings/session-settings/log-query",
    log_query_views: "/reference/settings/session-settings/log-query",
    low_cardinality_allow_in_native_format: "/reference/settings/session-settings/low-cardinality",
    low_cardinality_max_dictionary_size: "/reference/settings/session-settings/low-cardinality",
    low_cardinality_use_single_dictionary_for_part: "/reference/settings/session-settings/low-cardinality",
    low_priority_query_wait_time_ms: "/reference/settings/session-settings/other",
    make_distributed_plan: "/reference/settings/session-settings/other",
    materialize_skip_indexes_on_insert: "/reference/settings/session-settings/materialize",
    materialize_statistics_on_insert: "/reference/settings/session-settings/materialize",
    materialize_ttl_after_modify: "/reference/settings/session-settings/materialize",
    materialized_views_ignore_errors: "/reference/settings/session-settings/materialized-views",
    materialized_views_squash_parallel_inserts: "/reference/settings/session-settings/materialized-views",
    max_analyze_depth: "/reference/settings/session-settings/max",
    max_ast_depth: "/reference/settings/session-settings/max-ast",
    max_ast_elements: "/reference/settings/session-settings/max-ast",
    max_autoincrement_series: "/reference/settings/session-settings/max",
    max_backup_bandwidth: "/reference/settings/session-settings/max",
    max_block_size: "/reference/settings/session-settings/max",
    max_bytes_before_external_group_by: "/reference/settings/session-settings/max-bytes",
    max_bytes_before_external_join: "/reference/settings/session-settings/max-bytes",
    max_bytes_before_external_sort: "/reference/settings/session-settings/max-bytes",
    max_bytes_before_remerge_sort: "/reference/settings/session-settings/max-bytes",
    max_bytes_for_lazy_final: "/reference/settings/session-settings/max-bytes",
    max_bytes_in_distinct: "/reference/settings/session-settings/max-bytes",
    max_bytes_in_join: "/reference/settings/session-settings/max-bytes",
    max_bytes_in_set: "/reference/settings/session-settings/max-bytes",
    max_bytes_ratio_before_external_group_by: "/reference/settings/session-settings/max-bytes",
    max_bytes_ratio_before_external_join: "/reference/settings/session-settings/max-bytes",
    max_bytes_ratio_before_external_sort: "/reference/settings/session-settings/max-bytes",
    max_bytes_to_read: "/reference/settings/session-settings/max-bytes",
    max_bytes_to_read_leaf: "/reference/settings/session-settings/max-bytes",
    max_bytes_to_sort: "/reference/settings/session-settings/max-bytes",
    max_bytes_to_transfer: "/reference/settings/session-settings/max-bytes",
    max_columns_to_read: "/reference/settings/session-settings/max",
    max_compress_block_size: "/reference/settings/session-settings/max",
    max_concurrent_queries_for_all_users: "/reference/settings/session-settings/max-concurrent",
    max_concurrent_queries_for_user: "/reference/settings/session-settings/max-concurrent",
    max_consume_snapshots: "/reference/settings/session-settings/max",
    max_distributed_connections: "/reference/settings/session-settings/max-distributed",
    max_distributed_depth: "/reference/settings/session-settings/max-distributed",
    max_download_buffer_size: "/reference/settings/session-settings/max-download",
    max_download_threads: "/reference/settings/session-settings/max-download",
    max_estimated_execution_time: "/reference/settings/session-settings/max",
    max_execution_speed: "/reference/settings/session-settings/max-execution",
    max_execution_speed_bytes: "/reference/settings/session-settings/max-execution",
    max_execution_time: "/reference/settings/session-settings/max-execution",
    max_execution_time_leaf: "/reference/settings/session-settings/max-execution",
    max_expanded_ast_elements: "/reference/settings/session-settings/max",
    max_fetch_partition_retries_count: "/reference/settings/session-settings/max",
    max_final_threads: "/reference/settings/session-settings/max",
    max_http_get_redirects: "/reference/settings/session-settings/max",
    max_hyperscan_regexp_length: "/reference/settings/session-settings/max-hyperscan",
    max_hyperscan_regexp_total_length: "/reference/settings/session-settings/max-hyperscan",
    max_insert_block_size: "/reference/settings/session-settings/max-insert",
    max_insert_block_size_bytes: "/reference/settings/session-settings/max-insert",
    max_insert_delayed_streams_for_parallel_write: "/reference/settings/session-settings/max-insert",
    max_insert_threads: "/reference/settings/session-settings/max-insert",
    max_insert_threads_min_free_memory_per_thread: "/reference/settings/session-settings/max-insert",
    max_joined_block_size_bytes: "/reference/settings/session-settings/max-joined",
    max_joined_block_size_rows: "/reference/settings/session-settings/max-joined",
    max_limit_for_vector_search_queries: "/reference/settings/session-settings/max",
    max_local_read_bandwidth: "/reference/settings/session-settings/max-local",
    max_local_write_bandwidth: "/reference/settings/session-settings/max-local",
    max_memory_usage: "/reference/settings/session-settings/max-memory-usage",
    max_memory_usage_for_user: "/reference/settings/session-settings/max-memory-usage",
    max_network_bandwidth: "/reference/settings/session-settings/max-network",
    max_network_bandwidth_for_all_users: "/reference/settings/session-settings/max-network",
    max_network_bandwidth_for_user: "/reference/settings/session-settings/max-network",
    max_network_bytes: "/reference/settings/session-settings/max-network",
    max_number_of_partitions_for_independent_aggregation: "/reference/settings/session-settings/max",
    max_os_cpu_wait_time_ratio_to_throw: "/reference/settings/session-settings/max",
    max_parallel_replicas: "/reference/settings/session-settings/max",
    max_parser_backtracks: "/reference/settings/session-settings/max-parser",
    max_parser_depth: "/reference/settings/session-settings/max-parser",
    max_parsing_threads: "/reference/settings/session-settings/max",
    max_partition_size_to_drop: "/reference/settings/session-settings/max",
    max_partitions_per_insert_block: "/reference/settings/session-settings/max-partitions",
    max_partitions_to_read: "/reference/settings/session-settings/max-partitions",
    max_parts_to_move: "/reference/settings/session-settings/max",
    max_projection_rows_to_use_projection_index: "/reference/settings/session-settings/max",
    max_query_size: "/reference/settings/session-settings/max",
    max_rand_distribution_parameter: "/reference/settings/session-settings/max-rand",
    max_rand_distribution_trials: "/reference/settings/session-settings/max-rand",
    max_read_buffer_size: "/reference/settings/session-settings/max-read-buffer-size",
    max_read_buffer_size_local_fs: "/reference/settings/session-settings/max-read-buffer-size",
    max_read_buffer_size_remote_fs: "/reference/settings/session-settings/max-read-buffer-size",
    max_recursive_cte_evaluation_depth: "/reference/settings/session-settings/max",
    max_remote_read_network_bandwidth: "/reference/settings/session-settings/max-remote",
    max_remote_write_network_bandwidth: "/reference/settings/session-settings/max-remote",
    max_replica_delay_for_distributed_queries: "/reference/settings/session-settings/max",
    max_result_bytes: "/reference/settings/session-settings/max-result",
    max_result_rows: "/reference/settings/session-settings/max-result",
    max_reverse_dictionary_lookup_cache_size_bytes: "/reference/settings/session-settings/max",
    max_rows_for_lazy_final: "/reference/settings/session-settings/max-rows",
    max_rows_in_distinct: "/reference/settings/session-settings/max-rows",
    max_rows_in_join: "/reference/settings/session-settings/max-rows",
    max_rows_in_set: "/reference/settings/session-settings/max-rows",
    max_rows_in_set_to_optimize_join: "/reference/settings/session-settings/max-rows",
    max_rows_to_group_by: "/reference/settings/session-settings/max-rows",
    max_rows_to_read: "/reference/settings/session-settings/max-rows",
    max_rows_to_read_leaf: "/reference/settings/session-settings/max-rows",
    max_rows_to_sort: "/reference/settings/session-settings/max-rows",
    max_rows_to_transfer: "/reference/settings/session-settings/max-rows",
    max_sessions_for_user: "/reference/settings/session-settings/max",
    max_size_to_preallocate_for_aggregation: "/reference/settings/session-settings/max-size",
    max_size_to_preallocate_for_joins: "/reference/settings/session-settings/max-size",
    max_skip_unavailable_shards_num: "/reference/settings/session-settings/max-skip",
    max_skip_unavailable_shards_ratio: "/reference/settings/session-settings/max-skip",
    max_streams_for_files_processing_in_cluster_functions: "/reference/settings/session-settings/max-streams",
    max_streams_for_merge_tree_reading: "/reference/settings/session-settings/max-streams",
    max_streams_for_union_step: "/reference/settings/session-settings/max-streams",
    max_streams_for_union_step_to_max_threads_ratio: "/reference/settings/session-settings/max-streams",
    max_streams_multiplier_for_merge_tables: "/reference/settings/session-settings/max-streams",
    max_streams_to_max_threads_ratio: "/reference/settings/session-settings/max-streams",
    max_subquery_depth: "/reference/settings/session-settings/max",
    max_table_size_to_drop: "/reference/settings/session-settings/max",
    max_temporary_columns: "/reference/settings/session-settings/max-temporary",
    max_temporary_data_on_disk_size_for_query: "/reference/settings/session-settings/max-temporary",
    max_temporary_data_on_disk_size_for_user: "/reference/settings/session-settings/max-temporary",
    max_temporary_non_const_columns: "/reference/settings/session-settings/max-temporary",
    max_threads: "/reference/settings/session-settings/max-threads",
    max_threads_for_indexes: "/reference/settings/session-settings/max-threads",
    max_threads_min_free_memory_per_thread: "/reference/settings/session-settings/max-threads",
    max_untracked_memory: "/reference/settings/session-settings/max",
    max_wkb_geometry_elements: "/reference/settings/session-settings/max",
    memory_overcommit_ratio_denominator: "/reference/settings/session-settings/memory-overcommit-ratio-denominator",
    memory_overcommit_ratio_denominator_for_user: "/reference/settings/session-settings/memory-overcommit-ratio-denominator",
    memory_profiler_sample_max_allocation_size: "/reference/settings/session-settings/memory-profiler",
    memory_profiler_sample_min_allocation_size: "/reference/settings/session-settings/memory-profiler",
    memory_profiler_sample_probability: "/reference/settings/session-settings/memory-profiler",
    memory_profiler_step: "/reference/settings/session-settings/memory-profiler",
    memory_tracker_fault_probability: "/reference/settings/session-settings/memory",
    memory_usage_overcommit_max_wait_microseconds: "/reference/settings/session-settings/memory",
    merge_table_max_tables_to_look_for_schema_inference: "/reference/settings/session-settings/other",
    merge_tree_coarse_index_granularity: "/reference/settings/session-settings/merge-tree",
    merge_tree_compact_parts_min_granules_to_multibuffer_read: "/reference/settings/session-settings/merge-tree",
    merge_tree_determine_task_size_by_prewhere_columns: "/reference/settings/session-settings/merge-tree",
    merge_tree_generic_exclusion_search_max_steps: "/reference/settings/session-settings/merge-tree",
    merge_tree_max_bytes_to_use_cache: "/reference/settings/session-settings/merge-tree",
    merge_tree_max_rows_to_use_cache: "/reference/settings/session-settings/merge-tree",
    merge_tree_min_bytes_for_concurrent_read: "/reference/settings/session-settings/merge-tree",
    merge_tree_min_bytes_for_concurrent_read_for_remote_filesystem: "/reference/settings/session-settings/merge-tree",
    merge_tree_min_bytes_for_seek: "/reference/settings/session-settings/merge-tree",
    merge_tree_min_bytes_per_task_for_remote_reading: "/reference/settings/session-settings/merge-tree",
    merge_tree_min_read_task_size: "/reference/settings/session-settings/merge-tree",
    merge_tree_min_rows_for_concurrent_read: "/reference/settings/session-settings/merge-tree",
    merge_tree_min_rows_for_concurrent_read_for_remote_filesystem: "/reference/settings/session-settings/merge-tree",
    merge_tree_min_rows_for_seek: "/reference/settings/session-settings/merge-tree",
    merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability: "/reference/settings/session-settings/merge-tree",
    merge_tree_storage_snapshot_sleep_ms: "/reference/settings/session-settings/merge-tree",
    merge_tree_use_const_size_tasks_for_remote_reading: "/reference/settings/session-settings/merge-tree",
    merge_tree_use_deserialization_prefixes_cache: "/reference/settings/session-settings/merge-tree",
    merge_tree_use_prefixes_deserialization_thread_pool: "/reference/settings/session-settings/merge-tree",
    merge_tree_use_v1_object_and_dynamic_serialization: "/reference/settings/session-settings/merge-tree",
    metrics_perf_events_enabled: "/reference/settings/session-settings/metrics-perf",
    metrics_perf_events_list: "/reference/settings/session-settings/metrics-perf",
    min_bytes_to_use_direct_io: "/reference/settings/session-settings/min-bytes",
    min_bytes_to_use_mmap_io: "/reference/settings/session-settings/min-bytes",
    min_chunk_bytes_for_parallel_parsing: "/reference/settings/session-settings/min",
    min_compress_block_size: "/reference/settings/session-settings/min",
    min_count_to_compile_aggregate_expression: "/reference/settings/session-settings/min-count",
    min_count_to_compile_expression: "/reference/settings/session-settings/min-count",
    min_count_to_compile_regular_expression: "/reference/settings/session-settings/min-count",
    min_count_to_compile_sort_description: "/reference/settings/session-settings/min-count",
    min_execution_speed: "/reference/settings/session-settings/min-execution-speed",
    min_execution_speed_bytes: "/reference/settings/session-settings/min-execution-speed",
    min_external_table_block_size_bytes: "/reference/settings/session-settings/min-external",
    min_external_table_block_size_rows: "/reference/settings/session-settings/min-external",
    min_filtered_ratio_for_lazy_final: "/reference/settings/session-settings/min",
    min_free_disk_bytes_to_perform_insert: "/reference/settings/session-settings/min-free",
    min_free_disk_ratio_to_perform_insert: "/reference/settings/session-settings/min-free",
    min_free_disk_space_for_temporary_data: "/reference/settings/session-settings/min-free",
    min_hit_rate_to_use_consecutive_keys_optimization: "/reference/settings/session-settings/min",
    min_insert_block_size_bytes: "/reference/settings/session-settings/min-insert",
    min_insert_block_size_bytes_for_materialized_views: "/reference/settings/session-settings/min-insert",
    min_insert_block_size_rows: "/reference/settings/session-settings/min-insert",
    min_insert_block_size_rows_for_materialized_views: "/reference/settings/session-settings/min-insert",
    min_joined_block_size_bytes: "/reference/settings/session-settings/min-joined",
    min_joined_block_size_rows: "/reference/settings/session-settings/min-joined",
    min_os_cpu_wait_time_ratio_to_throw: "/reference/settings/session-settings/min",
    min_outstreams_per_resize_after_split: "/reference/settings/session-settings/min",
    min_table_rows_to_use_projection_index: "/reference/settings/session-settings/min",
    mongodb_throw_on_unsupported_query: "/reference/settings/session-settings/other",
    move_all_conditions_to_prewhere: "/reference/settings/session-settings/move",
    move_primary_key_columns_to_end_of_prewhere: "/reference/settings/session-settings/move",
    multiple_joins_try_to_keep_original_names: "/reference/settings/session-settings/other",
    mutations_execute_nondeterministic_on_initiator: "/reference/settings/session-settings/mutations-execute",
    mutations_execute_subqueries_on_initiator: "/reference/settings/session-settings/mutations-execute",
    mutations_max_literal_size_to_replace: "/reference/settings/session-settings/mutations",
    mutations_sync: "/reference/settings/session-settings/mutations",
    mysql_datatypes_support_level: "/reference/settings/session-settings/mysql",
    mysql_map_fixed_string_to_text_in_show_columns: "/reference/settings/session-settings/mysql-map",
    mysql_map_string_to_text_in_show_columns: "/reference/settings/session-settings/mysql-map",
    mysql_max_rows_to_insert: "/reference/settings/session-settings/mysql",
    network_compression_method: "/reference/settings/session-settings/network",
    network_zstd_compression_level: "/reference/settings/session-settings/network",
    normalize_function_names: "/reference/settings/session-settings/other",
    number_of_mutations_to_delay: "/reference/settings/session-settings/number-of",
    number_of_mutations_to_throw: "/reference/settings/session-settings/number-of",
    odbc_bridge_connection_pool_size: "/reference/settings/session-settings/odbc-bridge",
    odbc_bridge_use_connection_pooling: "/reference/settings/session-settings/odbc-bridge",
    offset: "/reference/settings/session-settings/other",
    opentelemetry_start_keeper_trace_probability: "/reference/settings/session-settings/opentelemetry-start",
    opentelemetry_start_trace_probability: "/reference/settings/session-settings/opentelemetry-start",
    opentelemetry_trace_cpu_scheduling: "/reference/settings/session-settings/opentelemetry-trace",
    opentelemetry_trace_processors: "/reference/settings/session-settings/opentelemetry-trace",
    optimize_aggregation_in_order: "/reference/settings/session-settings/optimize-aggregation-in-order",
    optimize_aggregation_in_order_limit: "/reference/settings/session-settings/optimize-aggregation-in-order",
    optimize_aggregators_of_group_by_keys: "/reference/settings/session-settings/optimize",
    optimize_and_compare_chain: "/reference/settings/session-settings/optimize-and-compare-chain",
    optimize_and_compare_chain_max_hash_work: "/reference/settings/session-settings/optimize-and-compare-chain",
    optimize_append_index: "/reference/settings/session-settings/optimize",
    optimize_arithmetic_operations_in_aggregate_functions: "/reference/settings/session-settings/optimize",
    optimize_const_name_size: "/reference/settings/session-settings/optimize",
    optimize_count_from_files: "/reference/settings/session-settings/optimize",
    optimize_dictget_tuple_element: "/reference/settings/session-settings/optimize",
    optimize_distinct_in_order: "/reference/settings/session-settings/optimize",
    optimize_distributed_group_by_sharding_key: "/reference/settings/session-settings/optimize",
    optimize_dry_run_check_part: "/reference/settings/session-settings/optimize",
    optimize_empty_string_comparisons: "/reference/settings/session-settings/optimize",
    optimize_extract_common_expressions: "/reference/settings/session-settings/optimize",
    optimize_functions_to_subcolumns: "/reference/settings/session-settings/optimize",
    optimize_group_by_constant_keys: "/reference/settings/session-settings/optimize-group",
    optimize_group_by_function_keys: "/reference/settings/session-settings/optimize-group",
    optimize_if_chain_to_multiif: "/reference/settings/session-settings/optimize-if",
    optimize_if_transform_strings_to_enum: "/reference/settings/session-settings/optimize-if",
    optimize_injective_functions_in_group_by: "/reference/settings/session-settings/optimize-injective",
    optimize_injective_functions_in_limit_by: "/reference/settings/session-settings/optimize-injective",
    optimize_injective_functions_inside_uniq: "/reference/settings/session-settings/optimize-injective",
    optimize_inverse_dictionary_lookup: "/reference/settings/session-settings/optimize",
    optimize_limit_by_function_keys: "/reference/settings/session-settings/optimize-limit",
    optimize_limit_by_in_order: "/reference/settings/session-settings/optimize-limit",
    optimize_min_equality_disjunction_chain_length: "/reference/settings/session-settings/optimize-min",
    optimize_min_inequality_conjunction_chain_length: "/reference/settings/session-settings/optimize-min",
    optimize_move_to_prewhere: "/reference/settings/session-settings/optimize-move-to-prewhere",
    optimize_move_to_prewhere_if_final: "/reference/settings/session-settings/optimize-move-to-prewhere",
    optimize_multiif_to_if: "/reference/settings/session-settings/optimize",
    optimize_normalize_count_variants: "/reference/settings/session-settings/optimize",
    optimize_on_insert: "/reference/settings/session-settings/optimize",
    optimize_or_like_chain: "/reference/settings/session-settings/optimize-or-like-chain",
    optimize_or_like_chain_min_patterns: "/reference/settings/session-settings/optimize-or-like-chain",
    optimize_or_like_chain_min_substrings: "/reference/settings/session-settings/optimize-or-like-chain",
    optimize_prewhere_after_pushdown: "/reference/settings/session-settings/optimize",
    optimize_qbit_distance_function_reads: "/reference/settings/session-settings/optimize",
    optimize_read_in_order: "/reference/settings/session-settings/optimize",
    optimize_redundant_comparisons: "/reference/settings/session-settings/optimize-redundant",
    optimize_redundant_functions_in_order_by: "/reference/settings/session-settings/optimize-redundant",
    optimize_respect_aliases: "/reference/settings/session-settings/optimize",
    optimize_rewrite_aggregate_function_with_if: "/reference/settings/session-settings/optimize-rewrite",
    optimize_rewrite_array_exists_to_has: "/reference/settings/session-settings/optimize-rewrite",
    optimize_rewrite_has_to_in: "/reference/settings/session-settings/optimize-rewrite",
    optimize_rewrite_like_perfect_affix: "/reference/settings/session-settings/optimize-rewrite",
    optimize_rewrite_regexp_functions: "/reference/settings/session-settings/optimize-rewrite",
    optimize_rewrite_sum_if_to_count_if: "/reference/settings/session-settings/optimize-rewrite",
    optimize_skip_merged_partitions: "/reference/settings/session-settings/optimize-skip",
    optimize_skip_unused_shards: "/reference/settings/session-settings/optimize-skip",
    optimize_skip_unused_shards_limit: "/reference/settings/session-settings/optimize-skip",
    optimize_skip_unused_shards_nesting: "/reference/settings/session-settings/optimize-skip",
    optimize_skip_unused_shards_rewrite_in: "/reference/settings/session-settings/optimize-skip",
    optimize_sorting_by_input_stream_properties: "/reference/settings/session-settings/optimize",
    optimize_substitute_columns: "/reference/settings/session-settings/optimize",
    optimize_syntax_fuse_functions: "/reference/settings/session-settings/optimize",
    optimize_throw_if_noop: "/reference/settings/session-settings/optimize",
    optimize_time_filter_with_preimage: "/reference/settings/session-settings/optimize",
    optimize_trivial_approximate_count_query: "/reference/settings/session-settings/optimize-trivial",
    optimize_trivial_count_query: "/reference/settings/session-settings/optimize-trivial",
    optimize_trivial_count_with_sparsity_filter: "/reference/settings/session-settings/optimize-trivial",
    optimize_trivial_group_by_limit_query: "/reference/settings/session-settings/optimize-trivial",
    optimize_trivial_insert_select: "/reference/settings/session-settings/optimize-trivial",
    optimize_truncate_order_by_after_group_by_keys: "/reference/settings/session-settings/optimize",
    optimize_uniq_to_count: "/reference/settings/session-settings/optimize",
    optimize_use_implicit_projections: "/reference/settings/session-settings/optimize-use",
    optimize_use_projection_filtering: "/reference/settings/session-settings/optimize-use",
    optimize_use_projections: "/reference/settings/session-settings/optimize-use",
    optimize_using_constraints: "/reference/settings/session-settings/optimize",
    os_threads_nice_value_materialized_view: "/reference/settings/session-settings/os-threads",
    os_threads_nice_value_query: "/reference/settings/session-settings/os-threads",
    page_cache_block_size: "/reference/settings/session-settings/page-cache",
    page_cache_inject_eviction: "/reference/settings/session-settings/page-cache",
    page_cache_lookahead_blocks: "/reference/settings/session-settings/page-cache",
    page_cache_max_coalesced_bytes: "/reference/settings/session-settings/page-cache",
    paimon_target_snapshot_id: "/reference/settings/session-settings/other",
    "parallel-processing-using-parallel_replicas_custom_key": "/reference/settings/session-settings/max",
    "parallel-processing-using-sample-key": "/reference/settings/session-settings/max",
    parallel_distributed_insert_select: "/reference/settings/session-settings/parallel",
    parallel_hash_join_threshold: "/reference/settings/session-settings/parallel",
    parallel_non_joined_rows_processing: "/reference/settings/session-settings/parallel",
    parallel_replica_offset: "/reference/settings/session-settings/parallel",
    parallel_replicas_allow_in_with_subquery: "/reference/settings/session-settings/parallel-replicas",
    parallel_replicas_allow_materialized_views: "/reference/settings/session-settings/parallel-replicas",
    parallel_replicas_allow_view_over_mergetree: "/reference/settings/session-settings/parallel-replicas",
    parallel_replicas_connect_timeout_ms: "/reference/settings/session-settings/parallel-replicas",
    parallel_replicas_count: "/reference/settings/session-settings/parallel-replicas",
    parallel_replicas_custom_key: "/reference/settings/session-settings/parallel-replicas",
    parallel_replicas_custom_key_range_lower: "/reference/settings/session-settings/parallel-replicas",
    parallel_replicas_custom_key_range_upper: "/reference/settings/session-settings/parallel-replicas",
    parallel_replicas_filter_pushdown: "/reference/settings/session-settings/parallel-replicas",
    parallel_replicas_for_cluster_engines: "/reference/settings/session-settings/parallel-replicas",
    parallel_replicas_for_non_replicated_merge_tree: "/reference/settings/session-settings/parallel-replicas",
    parallel_replicas_index_analysis_only_on_coordinator: "/reference/settings/session-settings/parallel-replicas",
    parallel_replicas_insert_select_local_pipeline: "/reference/settings/session-settings/parallel-replicas",
    parallel_replicas_local_plan: "/reference/settings/session-settings/parallel-replicas",
    parallel_replicas_mark_segment_size: "/reference/settings/session-settings/parallel-replicas",
    parallel_replicas_min_number_of_rows_per_replica: "/reference/settings/session-settings/parallel-replicas",
    parallel_replicas_mode: "/reference/settings/session-settings/parallel-replicas",
    parallel_replicas_only_with_analyzer: "/reference/settings/session-settings/parallel-replicas",
    parallel_replicas_plan_based: "/reference/settings/session-settings/parallel-replicas",
    parallel_replicas_prefer_local_join: "/reference/settings/session-settings/parallel-replicas",
    parallel_replicas_prefer_local_replica: "/reference/settings/session-settings/parallel-replicas",
    parallel_replicas_support_projection: "/reference/settings/session-settings/parallel-replicas",
    parallel_view_processing: "/reference/settings/session-settings/parallel",
    parallelize_output_from_storages: "/reference/settings/session-settings/other",
    parsedatetime_e_requires_space_padding: "/reference/settings/session-settings/parsedatetime",
    parsedatetime_parse_without_leading_zeros: "/reference/settings/session-settings/parsedatetime",
    partial_merge_join_left_table_buffer_bytes: "/reference/settings/session-settings/partial-merge",
    partial_merge_join_rows_in_right_blocks: "/reference/settings/session-settings/partial-merge",
    partial_result_on_first_cancel: "/reference/settings/session-settings/other",
    parts_to_delay_insert: "/reference/settings/session-settings/parts-to",
    parts_to_throw_insert: "/reference/settings/session-settings/parts-to",
    per_part_index_stats: "/reference/settings/session-settings/other",
    placeholders: "/reference/settings/session-settings/other",
    poll_interval: "/reference/settings/session-settings/other",
    polyglot_dialect: "/reference/settings/session-settings/other",
    postgresql_connection_attempt_timeout: "/reference/settings/session-settings/postgresql-connection",
    postgresql_connection_pool_auto_close_connection: "/reference/settings/session-settings/postgresql-connection",
    postgresql_connection_pool_retries: "/reference/settings/session-settings/postgresql-connection",
    postgresql_connection_pool_size: "/reference/settings/session-settings/postgresql-connection",
    postgresql_connection_pool_wait_timeout: "/reference/settings/session-settings/postgresql-connection",
    postgresql_fault_injection_probability: "/reference/settings/session-settings/other",
    predicate_statistics_sample_rate: "/reference/settings/session-settings/other",
    prefer_column_name_to_alias: "/reference/settings/session-settings/prefer",
    prefer_external_sort_block_bytes: "/reference/settings/session-settings/prefer",
    prefer_global_in_and_join: "/reference/settings/session-settings/prefer",
    prefer_localhost_replica: "/reference/settings/session-settings/prefer",
    prefer_warmed_unmerged_parts_seconds: "/reference/settings/session-settings/prefer",
    preferred_block_size_bytes: "/reference/settings/session-settings/preferred",
    preferred_max_column_in_block_size_bytes: "/reference/settings/session-settings/preferred",
    preferred_optimize_projection_name: "/reference/settings/session-settings/preferred",
    prefetch_buffer_size: "/reference/settings/session-settings/other",
    print_pretty_type_names: "/reference/settings/session-settings/other",
    priority: "/reference/settings/session-settings/other",
    promql_database: "/reference/settings/session-settings/promql",
    promql_evaluation_time: "/reference/settings/session-settings/promql",
    promql_table: "/reference/settings/session-settings/promql",
    "purpose-of-the-setting": "/reference/settings/session-settings/min",
    push_external_roles_in_interserver_queries: "/reference/settings/session-settings/other",
    query_cache_compress_entries: "/reference/settings/session-settings/query-cache",
    query_cache_for_subqueries: "/reference/settings/session-settings/query-cache",
    query_cache_max_entries: "/reference/settings/session-settings/query-cache",
    query_cache_max_size_in_bytes: "/reference/settings/session-settings/query-cache",
    query_cache_min_query_duration: "/reference/settings/session-settings/query-cache",
    query_cache_min_query_runs: "/reference/settings/session-settings/query-cache",
    query_cache_nondeterministic_function_handling: "/reference/settings/session-settings/query-cache",
    query_cache_share_between_users: "/reference/settings/session-settings/query-cache",
    query_cache_squash_partial_results: "/reference/settings/session-settings/query-cache",
    query_cache_system_table_handling: "/reference/settings/session-settings/query-cache",
    query_cache_tag: "/reference/settings/session-settings/query-cache",
    query_cache_ttl: "/reference/settings/session-settings/query-cache",
    query_metric_log_interval: "/reference/settings/session-settings/other",
    query_plan_aggregation_in_order: "/reference/settings/session-settings/query-plan",
    query_plan_convert_any_join_to_semi_or_anti_join: "/reference/settings/session-settings/query-plan",
    query_plan_convert_join_to_in: "/reference/settings/session-settings/query-plan",
    query_plan_convert_outer_join_to_inner_join: "/reference/settings/session-settings/query-plan",
    query_plan_direct_read_from_text_index: "/reference/settings/session-settings/query-plan",
    query_plan_display_internal_aliases: "/reference/settings/session-settings/query-plan",
    query_plan_enable_multithreading_after_window_functions: "/reference/settings/session-settings/query-plan",
    query_plan_enable_optimizations: "/reference/settings/session-settings/query-plan",
    query_plan_execute_functions_after_sorting: "/reference/settings/session-settings/query-plan",
    query_plan_filter_push_down: "/reference/settings/session-settings/query-plan",
    query_plan_join_shard_by_pk_ranges: "/reference/settings/session-settings/query-plan",
    query_plan_join_swap_table: "/reference/settings/session-settings/query-plan",
    query_plan_lift_up_array_join: "/reference/settings/session-settings/query-plan",
    query_plan_lift_up_union: "/reference/settings/session-settings/query-plan",
    query_plan_max_limit_for_join_lazy_indexing: "/reference/settings/session-settings/query-plan",
    query_plan_max_limit_for_lazy_materialization: "/reference/settings/session-settings/query-plan",
    query_plan_max_limit_for_top_k_optimization: "/reference/settings/session-settings/query-plan",
    query_plan_max_optimizations_to_apply: "/reference/settings/session-settings/query-plan",
    query_plan_max_set_size_for_projection_match: "/reference/settings/session-settings/query-plan",
    query_plan_max_step_description_length: "/reference/settings/session-settings/query-plan",
    query_plan_merge_expression_into_join: "/reference/settings/session-settings/query-plan",
    query_plan_merge_expressions: "/reference/settings/session-settings/query-plan",
    query_plan_merge_filter_into_join_condition: "/reference/settings/session-settings/query-plan",
    query_plan_merge_filters: "/reference/settings/session-settings/query-plan",
    query_plan_min_columns_for_join_lazy_indexing: "/reference/settings/session-settings/query-plan",
    query_plan_optimize_join_order_algorithm: "/reference/settings/session-settings/query-plan",
    query_plan_optimize_join_order_limit: "/reference/settings/session-settings/query-plan",
    query_plan_optimize_join_order_max_searched_plans: "/reference/settings/session-settings/query-plan",
    query_plan_optimize_join_order_randomize: "/reference/settings/session-settings/query-plan",
    query_plan_optimize_lazy_final: "/reference/settings/session-settings/query-plan",
    query_plan_optimize_lazy_materialization: "/reference/settings/session-settings/query-plan",
    query_plan_optimize_prewhere: "/reference/settings/session-settings/query-plan",
    query_plan_push_down_limit: "/reference/settings/session-settings/query-plan",
    query_plan_push_limit_by_into_sort: "/reference/settings/session-settings/query-plan",
    query_plan_read_in_order: "/reference/settings/session-settings/query-plan",
    query_plan_read_in_order_through_join: "/reference/settings/session-settings/query-plan",
    query_plan_remove_redundant_distinct: "/reference/settings/session-settings/query-plan",
    query_plan_remove_redundant_sorting: "/reference/settings/session-settings/query-plan",
    query_plan_remove_unused_columns: "/reference/settings/session-settings/query-plan",
    query_plan_reuse_storage_ordering_for_window_functions: "/reference/settings/session-settings/query-plan",
    query_plan_split_filter: "/reference/settings/session-settings/query-plan",
    query_plan_text_index_add_hint: "/reference/settings/session-settings/query-plan",
    query_plan_top_k_through_join: "/reference/settings/session-settings/query-plan",
    query_plan_try_use_vector_search: "/reference/settings/session-settings/query-plan",
    query_profiler_cpu_time_period_ns: "/reference/settings/session-settings/query-profiler",
    query_profiler_real_time_period_ns: "/reference/settings/session-settings/query-profiler",
    queue_max_wait_ms: "/reference/settings/session-settings/other",
    rabbitmq_max_wait_ms: "/reference/settings/session-settings/other",
    read_backoff_max_throughput: "/reference/settings/session-settings/read-backoff",
    read_backoff_min_concurrency: "/reference/settings/session-settings/read-backoff",
    read_backoff_min_events: "/reference/settings/session-settings/read-backoff",
    read_backoff_min_interval_between_events_ms: "/reference/settings/session-settings/read-backoff",
    read_backoff_min_latency_ms: "/reference/settings/session-settings/read-backoff",
    read_from_distributed_cache_if_exists_otherwise_bypass_cache: "/reference/settings/session-settings/read-from",
    read_from_filesystem_cache_if_exists_otherwise_bypass_cache: "/reference/settings/session-settings/read-from",
    read_from_page_cache_if_exists_otherwise_bypass_cache: "/reference/settings/session-settings/read-from",
    read_in_order_two_level_merge_threshold: "/reference/settings/session-settings/read-in",
    read_in_order_use_buffering: "/reference/settings/session-settings/read-in",
    read_in_order_use_virtual_row: "/reference/settings/session-settings/read-in",
    read_in_order_use_virtual_row_per_block: "/reference/settings/session-settings/read-in",
    read_overflow_mode: "/reference/settings/session-settings/read-overflow-mode",
    read_overflow_mode_leaf: "/reference/settings/session-settings/read-overflow-mode",
    read_priority: "/reference/settings/session-settings/read",
    read_through_distributed_cache: "/reference/settings/session-settings/read",
    reader_executor_max_tail_for_drain: "/reference/settings/session-settings/reader-executor",
    reader_executor_min_bytes_for_seek: "/reference/settings/session-settings/reader-executor",
    reader_executor_use_long_connections: "/reference/settings/session-settings/reader-executor",
    readonly: "/reference/settings/session-settings/other",
    receive_data_timeout_ms: "/reference/settings/session-settings/receive",
    receive_timeout: "/reference/settings/session-settings/receive",
    recursive_cte_max_steps_in_type_inference: "/reference/settings/session-settings/other",
    regexp_dict_allow_hyperscan: "/reference/settings/session-settings/regexp-dict",
    regexp_dict_flag_case_insensitive: "/reference/settings/session-settings/regexp-dict",
    regexp_dict_flag_dotall: "/reference/settings/session-settings/regexp-dict",
    regexp_max_matches_per_row: "/reference/settings/session-settings/other",
    reject_expensive_hyperscan_regexps: "/reference/settings/session-settings/other",
    remerge_sort_lowered_memory_bytes_ratio: "/reference/settings/session-settings/other",
    remote_filesystem_read_method: "/reference/settings/session-settings/remote-filesystem",
    remote_filesystem_read_prefetch: "/reference/settings/session-settings/remote-filesystem",
    remote_fs_read_backoff_max_tries: "/reference/settings/session-settings/remote-fs",
    remote_fs_read_max_backoff_ms: "/reference/settings/session-settings/remote-fs",
    remote_read_min_bytes_for_seek: "/reference/settings/session-settings/other",
    rename_files_after_processing: "/reference/settings/session-settings/other",
    replace_running_query: "/reference/settings/session-settings/replace-running-query",
    replace_running_query_max_wait_ms: "/reference/settings/session-settings/replace-running-query",
    replication_wait_for_inactive_replica_timeout: "/reference/settings/session-settings/other",
    reserve_memory: "/reference/settings/session-settings/other",
    restore_replace_external_dictionary_source_to_null: "/reference/settings/session-settings/restore-replace",
    restore_replace_external_engines_to_null: "/reference/settings/session-settings/restore-replace",
    restore_replace_external_table_functions_to_null: "/reference/settings/session-settings/restore-replace",
    restore_replicated_merge_tree_to_shared_merge_tree: "/reference/settings/session-settings/other",
    result_overflow_mode: "/reference/settings/session-settings/other",
    rewrite_count_distinct_if_with_count_distinct_implementation: "/reference/settings/session-settings/rewrite",
    rewrite_in_to_join: "/reference/settings/session-settings/rewrite",
    rows_before_aggregation: "/reference/settings/session-settings/other",
    s3_allow_multipart_copy: "/reference/settings/session-settings/s3-allow",
    s3_allow_parallel_part_upload: "/reference/settings/session-settings/s3-allow",
    s3_allow_server_credentials_in_user_queries: "/reference/settings/session-settings/s3-allow",
    s3_check_objects_after_upload: "/reference/settings/session-settings/s3",
    s3_connect_timeout_ms: "/reference/settings/session-settings/s3",
    s3_create_new_file_on_insert: "/reference/settings/session-settings/s3",
    s3_disable_checksum: "/reference/settings/session-settings/s3",
    s3_ignore_file_doesnt_exist: "/reference/settings/session-settings/s3",
    s3_list_object_keys_size: "/reference/settings/session-settings/s3",
    s3_max_connections: "/reference/settings/session-settings/s3-max",
    s3_max_get_burst: "/reference/settings/session-settings/s3-max",
    s3_max_get_rps: "/reference/settings/session-settings/s3-max",
    s3_max_inflight_parts_for_one_file: "/reference/settings/session-settings/s3-max",
    s3_max_part_number: "/reference/settings/session-settings/s3-max",
    s3_max_put_burst: "/reference/settings/session-settings/s3-max",
    s3_max_put_rps: "/reference/settings/session-settings/s3-max",
    s3_max_single_operation_copy_size: "/reference/settings/session-settings/s3-max",
    s3_max_single_part_upload_size: "/reference/settings/session-settings/s3-max",
    s3_max_single_read_retries: "/reference/settings/session-settings/s3-max",
    s3_max_unexpected_write_error_retries: "/reference/settings/session-settings/s3-max",
    s3_max_upload_part_size: "/reference/settings/session-settings/s3-max",
    s3_min_upload_part_size: "/reference/settings/session-settings/s3",
    s3_path_filter_limit: "/reference/settings/session-settings/s3",
    s3_request_timeout_ms: "/reference/settings/session-settings/s3",
    s3_skip_empty_files: "/reference/settings/session-settings/s3",
    s3_slow_all_threads_after_network_error: "/reference/settings/session-settings/s3",
    s3_strict_upload_part_size: "/reference/settings/session-settings/s3",
    s3_throw_on_zero_files_match: "/reference/settings/session-settings/s3",
    s3_truncate_on_insert: "/reference/settings/session-settings/s3",
    s3_upload_part_size_multiply_factor: "/reference/settings/session-settings/s3-upload",
    s3_upload_part_size_multiply_parts_count_threshold: "/reference/settings/session-settings/s3-upload",
    s3_uri_style: "/reference/settings/session-settings/s3",
    s3_use_adaptive_timeouts: "/reference/settings/session-settings/s3",
    s3_validate_etag_on_read: "/reference/settings/session-settings/s3-validate",
    s3_validate_request_settings: "/reference/settings/session-settings/s3-validate",
    s3queue_default_zookeeper_path: "/reference/settings/session-settings/s3queue",
    s3queue_enable_logging_to_s3queue_log: "/reference/settings/session-settings/s3queue",
    s3queue_keeper_fault_injection_probability: "/reference/settings/session-settings/s3queue",
    s3queue_migrate_old_metadata_to_buckets: "/reference/settings/session-settings/s3queue",
    schema_inference_cache_require_modification_time_for_url: "/reference/settings/session-settings/schema-inference",
    schema_inference_use_cache_for_azure: "/reference/settings/session-settings/schema-inference",
    schema_inference_use_cache_for_file: "/reference/settings/session-settings/schema-inference",
    schema_inference_use_cache_for_hdfs: "/reference/settings/session-settings/schema-inference",
    schema_inference_use_cache_for_s3: "/reference/settings/session-settings/schema-inference",
    schema_inference_use_cache_for_url: "/reference/settings/session-settings/schema-inference",
    secondary_indices_enable_bulk_filtering: "/reference/settings/session-settings/other",
    select_sequential_consistency: "/reference/settings/session-settings/other",
    send_logs_level: "/reference/settings/session-settings/send-logs",
    send_logs_source_regexp: "/reference/settings/session-settings/send-logs",
    send_profile_events: "/reference/settings/session-settings/send",
    send_progress_in_http_headers: "/reference/settings/session-settings/send",
    send_table_structure_on_insert_with_inline_data: "/reference/settings/session-settings/send",
    send_timeout: "/reference/settings/session-settings/send",
    serialize_query_plan: "/reference/settings/session-settings/serialize",
    serialize_string_in_memory_with_zero_byte: "/reference/settings/session-settings/serialize",
    session_timezone: "/reference/settings/session-settings/other",
    set_overflow_mode: "/reference/settings/session-settings/other",
    shared_merge_tree_sequential_consistency_initial_parts_update_backoff_ms: "/reference/settings/session-settings/shared-merge",
    shared_merge_tree_sequential_consistency_max_parts_update_backoff_ms: "/reference/settings/session-settings/shared-merge",
    shared_merge_tree_sequential_consistency_parts_update_max_retries: "/reference/settings/session-settings/shared-merge",
    shared_merge_tree_sync_parts_on_partition_operations: "/reference/settings/session-settings/shared-merge",
    short_circuit_function_evaluation: "/reference/settings/session-settings/short-circuit-function-evaluation",
    short_circuit_function_evaluation_for_nulls: "/reference/settings/session-settings/short-circuit-function-evaluation",
    short_circuit_function_evaluation_for_nulls_threshold: "/reference/settings/session-settings/short-circuit-function-evaluation",
    show_data_lake_catalogs_in_system_tables: "/reference/settings/session-settings/show",
    show_processlist_include_internal: "/reference/settings/session-settings/show",
    show_remote_databases_in_system_tables: "/reference/settings/session-settings/show",
    show_table_uuid_in_table_create_query_if_not_nil: "/reference/settings/session-settings/show",
    single_join_prefer_left_table: "/reference/settings/session-settings/other",
    skip_redundant_aliases_in_udf: "/reference/settings/session-settings/other",
    skip_unavailable_shards: "/reference/settings/session-settings/skip-unavailable-shards",
    skip_unavailable_shards_mode: "/reference/settings/session-settings/skip-unavailable-shards",
    sleep_after_receiving_query_ms: "/reference/settings/session-settings/other",
    sleep_in_send_data_ms: "/reference/settings/session-settings/sleep-in",
    sleep_in_send_tables_status_ms: "/reference/settings/session-settings/sleep-in",
    snappy_mode: "/reference/settings/session-settings/other",
    sort_overflow_mode: "/reference/settings/session-settings/other",
    split_intersecting_parts_ranges_into_layers_final: "/reference/settings/session-settings/split",
    split_parts_ranges_into_intersecting_and_non_intersecting_final: "/reference/settings/session-settings/split",
    splitby_max_substrings_includes_remaining_string: "/reference/settings/session-settings/other",
    "splitting-resize-node-with-arbitrary-inputs/outputs": "/reference/settings/session-settings/min",
    stop_refreshable_materialized_views_on_startup: "/reference/settings/session-settings/other",
    storage_file_read_method: "/reference/settings/session-settings/storage",
    storage_system_stack_trace_pipe_read_timeout_ms: "/reference/settings/session-settings/storage",
    stream_flush_interval_ms: "/reference/settings/session-settings/stream",
    stream_like_engine_allow_direct_select: "/reference/settings/session-settings/stream-like",
    stream_like_engine_insert_queue: "/reference/settings/session-settings/stream-like",
    stream_poll_timeout_ms: "/reference/settings/session-settings/stream",
    system_events_show_zero_values: "/reference/settings/session-settings/system",
    system_metric_log_show_zero_values_in_histograms: "/reference/settings/session-settings/system",
    table_engine_read_through_distributed_cache: "/reference/settings/session-settings/table",
    table_function_remote_max_addresses: "/reference/settings/session-settings/table",
    tcp_keep_alive_timeout: "/reference/settings/session-settings/other",
    temporary_data_in_cache_reserve_space_wait_lock_timeout_milliseconds: "/reference/settings/session-settings/other",
    temporary_files_buffer_size: "/reference/settings/session-settings/temporary-files",
    temporary_files_codec: "/reference/settings/session-settings/temporary-files",
    text_index_hint_max_selectivity: "/reference/settings/session-settings/text-index",
    text_index_lazy_intersection_density_threshold: "/reference/settings/session-settings/text-index",
    text_index_like_max_postings_to_read: "/reference/settings/session-settings/text-index",
    text_index_like_min_pattern_length: "/reference/settings/session-settings/text-index",
    text_index_posting_list_apply_mode: "/reference/settings/session-settings/text-index",
    throw_if_no_data_to_insert: "/reference/settings/session-settings/other",
    throw_on_error_from_cache_on_write_operations: "/reference/settings/session-settings/throw-on",
    throw_on_max_partitions_per_insert_block: "/reference/settings/session-settings/throw-on",
    throw_on_unsupported_query_inside_transaction: "/reference/settings/session-settings/throw-on",
    timeout_before_checking_execution_speed: "/reference/settings/session-settings/other",
    timeout_overflow_mode: "/reference/settings/session-settings/timeout-overflow-mode",
    timeout_overflow_mode_leaf: "/reference/settings/session-settings/timeout-overflow-mode",
    totals_auto_threshold: "/reference/settings/session-settings/totals",
    totals_mode: "/reference/settings/session-settings/totals",
    trace_profile_events: "/reference/settings/session-settings/trace-profile-events",
    trace_profile_events_list: "/reference/settings/session-settings/trace-profile-events",
    transfer_overflow_mode: "/reference/settings/session-settings/other",
    transform_null_in: "/reference/settings/session-settings/other",
    traverse_shadow_remote_data_paths: "/reference/settings/session-settings/other",
    union_default_mode: "/reference/settings/session-settings/other",
    unique_key_max_encoded_size: "/reference/settings/session-settings/other",
    unknown_packet_in_send_data: "/reference/settings/session-settings/other",
    update_parallel_mode: "/reference/settings/session-settings/update",
    update_sequential_consistency: "/reference/settings/session-settings/update",
    url_base: "/reference/settings/session-settings/url",
    url_wildcard_max_directories_to_read: "/reference/settings/session-settings/url",
    use_async_executor_for_materialized_views: "/reference/settings/session-settings/use",
    use_cache_for_count_from_files: "/reference/settings/session-settings/use",
    use_client_time_zone: "/reference/settings/session-settings/use",
    use_compact_format_in_distributed_parts_names: "/reference/settings/session-settings/use",
    use_concurrency_control: "/reference/settings/session-settings/use",
    use_constant_folding_in_index_analysis: "/reference/settings/session-settings/use",
    use_hash_table_stats_for_join_reordering: "/reference/settings/session-settings/use",
    use_hedged_requests: "/reference/settings/session-settings/use",
    use_hive_partitioning: "/reference/settings/session-settings/use",
    use_iceberg_metadata_files_cache: "/reference/settings/session-settings/use-iceberg",
    use_iceberg_partition_pruning: "/reference/settings/session-settings/use-iceberg",
    use_index_for_in_with_subqueries: "/reference/settings/session-settings/use-index-for-in-with-subqueries",
    use_index_for_in_with_subqueries_max_values: "/reference/settings/session-settings/use-index-for-in-with-subqueries",
    use_join_disjunctions_push_down: "/reference/settings/session-settings/use",
    use_legacy_to_time: "/reference/settings/session-settings/use",
    use_lightweight_primary_key_index_analysis: "/reference/settings/session-settings/use",
    use_page_cache_for_disks_without_file_cache: "/reference/settings/session-settings/use-page",
    use_page_cache_for_local_disks: "/reference/settings/session-settings/use-page",
    use_page_cache_for_object_storage: "/reference/settings/session-settings/use-page",
    use_page_cache_with_distributed_cache: "/reference/settings/session-settings/use-page",
    use_paimon_metadata_files_cache: "/reference/settings/session-settings/use-paimon",
    use_paimon_partition_pruning: "/reference/settings/session-settings/use-paimon",
    use_parquet_metadata_cache: "/reference/settings/session-settings/use",
    use_partition_minmax_for_primary_key_pruning: "/reference/settings/session-settings/use-partition",
    use_partition_pruning: "/reference/settings/session-settings/use-partition",
    use_primary_key: "/reference/settings/session-settings/use",
    use_query_cache: "/reference/settings/session-settings/use-query",
    use_query_condition_cache: "/reference/settings/session-settings/use-query",
    use_reader_executor: "/reference/settings/session-settings/use",
    use_roaring_bitmap_iceberg_positional_deletes: "/reference/settings/session-settings/use",
    use_skip_indexes: "/reference/settings/session-settings/use-skip-indexes",
    use_skip_indexes_for_disjunctions: "/reference/settings/session-settings/use-skip-indexes",
    use_skip_indexes_for_top_k: "/reference/settings/session-settings/use-skip-indexes",
    use_skip_indexes_if_final: "/reference/settings/session-settings/use-skip-indexes",
    use_skip_indexes_if_final_exact_mode: "/reference/settings/session-settings/use-skip-indexes",
    use_skip_indexes_on_data_read: "/reference/settings/session-settings/use-skip-indexes",
    use_statistics: "/reference/settings/session-settings/use-statistics",
    use_statistics_cache: "/reference/settings/session-settings/use-statistics",
    use_statistics_for_part_pruning: "/reference/settings/session-settings/use-statistics",
    use_streaming_marks_compression: "/reference/settings/session-settings/use",
    use_strict_insert_block_limits: "/reference/settings/session-settings/use",
    use_structure_from_insertion_table_in_table_functions: "/reference/settings/session-settings/use",
    use_text_index_header_cache: "/reference/settings/session-settings/use-text",
    use_text_index_like_evaluation_by_dictionary_scan: "/reference/settings/session-settings/use-text",
    use_text_index_postings_cache: "/reference/settings/session-settings/use-text",
    use_text_index_tokens_cache: "/reference/settings/session-settings/use-text",
    use_top_k_dynamic_filtering: "/reference/settings/session-settings/use-top-k-dynamic-filtering",
    use_top_k_dynamic_filtering_for_variable_length_types: "/reference/settings/session-settings/use-top-k-dynamic-filtering",
    use_uncompressed_cache: "/reference/settings/session-settings/use",
    use_variant_as_common_type: "/reference/settings/session-settings/use-variant",
    use_variant_default_implementation_for_comparisons: "/reference/settings/session-settings/use-variant",
    use_with_fill_by_sorting_prefix: "/reference/settings/session-settings/use",
    validate_enum_literals_in_operators: "/reference/settings/session-settings/validate",
    validate_mutation_query: "/reference/settings/session-settings/validate",
    validate_polygons: "/reference/settings/session-settings/validate",
    variant_throw_on_type_mismatch: "/reference/settings/session-settings/other",
    vector_search_filter_strategy: "/reference/settings/session-settings/vector-search",
    vector_search_index_fetch_multiplier: "/reference/settings/session-settings/vector-search",
    vector_search_use_quantized_codes: "/reference/settings/session-settings/vector-search",
    vector_search_with_rescoring: "/reference/settings/session-settings/vector-search",
    wait_changes_become_visible_after_commit_mode: "/reference/settings/session-settings/other",
    wait_for_async_insert: "/reference/settings/session-settings/wait-for",
    wait_for_async_insert_timeout: "/reference/settings/session-settings/wait-for",
    wait_for_part_commit_in_dependent_materialized_views: "/reference/settings/session-settings/wait-for",
    wait_for_window_view_fire_signal_timeout: "/reference/settings/session-settings/wait-for",
    webassembly_udf_max_fuel: "/reference/settings/session-settings/webassembly-udf",
    webassembly_udf_max_input_block_size: "/reference/settings/session-settings/webassembly-udf",
    webassembly_udf_max_instances: "/reference/settings/session-settings/webassembly-udf",
    webassembly_udf_max_memory: "/reference/settings/session-settings/webassembly-udf",
    "what-is-a-resize-node": "/reference/settings/session-settings/min",
    "why-the-resize-node-needs-to-be-split": "/reference/settings/session-settings/min",
    window_view_clean_interval: "/reference/settings/session-settings/window-view",
    window_view_heartbeat_interval: "/reference/settings/session-settings/window-view",
    workload: "/reference/settings/session-settings/other",
    write_full_path_in_iceberg_metadata: "/reference/settings/session-settings/other",
    write_through_distributed_cache: "/reference/settings/session-settings/write-through-distributed-cache",
    write_through_distributed_cache_buffer_size: "/reference/settings/session-settings/write-through-distributed-cache",
    zstd_window_log_max: "/reference/settings/session-settings/other"
  }))

  useEffect(() => {
    const rawHash = window.location.hash.slice(1)
    if (!rawHash) return

    let decodedHash
    try {
      decodedHash = decodeURIComponent(rawHash)
    } catch {
      return
    }

    const canonicalAnchor = (value) => {
      const candidates = [value, value.replace(/[?,;:!'"()[\]{}]/g, "")]
      for (const candidate of candidates) {
        if (anchorRoutes[candidate]) return candidate
        const lowerValue = candidate.toLowerCase()
        if (anchorRoutes[lowerValue]) return lowerValue
      }
      return null
    }

    const directAnchor = canonicalAnchor(decodedHash)
    const baseAnchor = directAnchor || canonicalAnchor(decodedHash.split("-", 1)[0])
    if (!baseAnchor) return
    const target = anchorRoutes[baseAnchor]
    const targetHash = directAnchor || rawHash

    const marker = "/reference/settings/session-settings"
    const markerIndex = window.location.pathname.indexOf(marker)
    let basePath = ""
    if (markerIndex >= 0) {
      basePath = window.location.pathname.slice(0, markerIndex)
    } else if (window.location.pathname.startsWith("/docs/")) {
      basePath = "/docs"
    }

    window.location.replace(`${basePath}${target}${window.location.search}#${targetHash}`)
  }, [])

  const [expandedGroups, setExpandedGroups] = useState(() => new Set())
  const [searchTerm, setSearchTerm] = useState("")
  const normalizedSearch = searchTerm.trim().toLowerCase()
  const toPlainSearchTerms = (value) =>
    value
      .replace(/([a-z0-9])([A-Z])/g, "$1 $2")
      .toLowerCase()
      .split(/[^a-z0-9]+/)
      .filter((term) => term.length > 1)
      .map((term) => (term.length > 3 && term.endsWith("s") ? term.slice(0, -1) : term))
  const usesWildcard = normalizedSearch.includes("%")
  const plainSearchTerms = toPlainSearchTerms(searchTerm)
  const isSearching = usesWildcard ? normalizedSearch.replaceAll("%", "").trim().length > 0 : plainSearchTerms.length > 0

  const matchesSearch = (value) => {
    const candidate = value.toLowerCase()
    if (!isSearching) return true
    if (!usesWildcard) {
      const candidateTerms = toPlainSearchTerms(value)
      return plainSearchTerms.every((searchTerm) => candidateTerms.some((candidateTerm) => candidateTerm.startsWith(searchTerm)))
    }

    const parts = normalizedSearch.split("%")
    let position = 0
    for (let index = 0; index < parts.length; index += 1) {
      const part = parts[index]
      if (!part) continue
      const matchPosition = candidate.indexOf(part, position)
      if (matchPosition < 0) return false
      if (index === 0 && !normalizedSearch.startsWith("%") && matchPosition !== 0) {
        return false
      }
      position = matchPosition + part.length
    }

    const lastPart = parts[parts.length - 1]
    return normalizedSearch.endsWith("%") || !lastPart || position === candidate.length
  }

  const filterEntry = (entry) => {
    const settings = entry.settings.filter((setting) => matchesSearch(setting.name))
    const children = entry.children.map(filterEntry).filter(Boolean)
    const count = settings.length + children.reduce((total, child) => total + child.count, 0)
    if (!count) return null
    return { ...entry, count, settings, children }
  }

  const filteredEntries = isSearching ? entries.map(filterEntry).filter(Boolean) : entries
  const matchingCount = filteredEntries.reduce((total, entry) => total + entry.count, 0)

  const toggleGroup = (key) => {
    setExpandedGroups((current) => {
      const next = new Set(current)
      if (next.has(key)) next.delete(key)
      else next.add(key)
      return next
    })
  }

  const branchPrefix = (continuations, isLast) => {
    const prefix = continuations.map((continued) => (continued ? "│  " : "   ")).join("")
    return `${prefix}${isLast ? "└─ " : "├─ "}`
  }

  const branch = (value) => (
    <span aria-hidden="true" className="select-none text-gray-400 dark:text-gray-600" style={{ whiteSpace: "pre" }}>
      {value}
    </span>
  )

  const renderGroup = (entry, continuations = [], isLast = false, path = []) => {
    const key = [...path, entry.label].join("/")
    const isOpen = isSearching || expandedGroups.has(key)
    const items = [...entry.settings.map((setting) => ({ type: "setting", value: setting })), ...entry.children.map((child) => ({ type: "group", value: child }))]
    const countLabel = `${entry.count} ${entry.count === 1 ? "个设置" : "个设置"}`

    return (
      <div key={key} className="min-w-max">
        <button
          type="button"
          aria-expanded={isOpen}
          disabled={isSearching}
          onClick={() => toggleGroup(key)}
          className="flex min-w-max items-baseline whitespace-nowrap text-left"
          style={{
            appearance: "none",
            background: "transparent",
            border: 0,
            color: "inherit",
            cursor: isSearching ? "default" : "pointer",
            font: "inherit",
            lineHeight: "inherit",
            padding: 0
          }}
        >
          <span aria-hidden="true" style={{ display: "inline-block", width: "1rem" }}>
            {isOpen ? "▾" : "▸"}
          </span>
          {branch(branchPrefix(continuations, isLast))}
          <span className="font-medium">{entry.label}</span>
          <span className="ml-3 text-xs text-gray-500 dark:text-gray-400">{countLabel}</span>
        </button>
        {isOpen &&
          items.map((item, index) => {
            const itemIsLast = index === items.length - 1
            const childContinuations = [...continuations, !isLast]
            if (item.type === "group") {
              return renderGroup(item.value, childContinuations, itemIsLast, [...path, entry.label])
            }
            return (
              <div key={item.value.name} className="flex min-w-max items-baseline whitespace-nowrap">
                <span aria-hidden="true" style={{ display: "inline-block", width: "1rem" }} />
                {branch(branchPrefix(childContinuations, itemIsLast))}
                <a href={item.value.href} className="no-underline hover:underline">
                  {item.value.name}
                </a>
              </div>
            )
          })}
      </div>
    )
  }

  return (
    <div className="not-prose my-6 w-full">
      <input
        aria-label="搜索设置"
        type="search"
        value={searchTerm}
        onChange={(event) => setSearchTerm(event.target.value)}
        placeholder="搜索设置，例如 parallel replicas 或 %materialized%"
        className="w-full rounded-lg border border-gray-300 bg-white px-3 py-2 text-sm text-gray-900 outline-none placeholder:text-gray-500 focus:border-yellow-400 focus:ring-2 focus:ring-yellow-400/20 dark:border-white/10 dark:bg-transparent dark:text-white dark:placeholder:text-gray-500"
      />
      {isSearching && (
        <div className="mt-2 text-right text-xs text-gray-500 dark:text-gray-400">
          <span>
            找到 {matchingCount} 个匹配的设置
          </span>
        </div>
      )}
      <div className="mt-3 w-full overflow-x-auto rounded-xl border border-gray-200 bg-gray-50/50 px-4 py-3 font-mono text-sm leading-6 dark:border-white/10 dark:bg-transparent">
        <div className="min-w-max font-semibold">/session-settings</div>
        {filteredEntries.length > 0 ? (
          filteredEntries.map((entry, index) => renderGroup(entry, [], index === filteredEntries.length - 1))
        ) : (
          <div className="py-2 text-gray-500 dark:text-gray-400">未找到匹配的设置</div>
        )}
      </div>
    </div>
  )
}

export default SessionSettingsExplorer;