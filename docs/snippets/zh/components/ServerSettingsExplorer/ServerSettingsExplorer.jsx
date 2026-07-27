const ServerSettingsExplorer = () => {
  // Mintlify's production renderer evaluates the exported component without
  // preserving module-scope bindings. Lazy state keeps the generated data in
  // that evaluation scope while constructing it only once per mount.
  const [entries] = useState(() => [
    {
      label: "access_control_*",
      count: 2,
      settings: [
        { name: "access_control_improvements", href: "/zh/reference/settings/server-settings/settings/access-control#access_control_improvements" },
        { name: "access_control_path", href: "/zh/reference/settings/server-settings/settings/access-control#access_control_path" }
      ],
      children: []
    },
    {
      label: "aggregate_function_*",
      count: 2,
      settings: [
        {
          name: "aggregate_function_group_array_action_when_limit_is_reached",
          href: "/zh/reference/settings/server-settings/settings/aggregate-function#aggregate_function_group_array_action_when_limit_is_reached"
        },
        { name: "aggregate_function_group_array_max_element_size", href: "/zh/reference/settings/server-settings/settings/aggregate-function#aggregate_function_group_array_max_element_size" }
      ],
      children: []
    },
    {
      label: "allow_*",
      count: 6,
      settings: [
        { name: "allow_feature_tier", href: "/zh/reference/settings/server-settings/settings/allow#allow_feature_tier" },
        { name: "allow_impersonate_user", href: "/zh/reference/settings/server-settings/settings/allow#allow_impersonate_user" },
        { name: "allow_implicit_no_password", href: "/zh/reference/settings/server-settings/settings/allow#allow_implicit_no_password" },
        { name: "allow_no_password", href: "/zh/reference/settings/server-settings/settings/allow#allow_no_password" },
        { name: "allow_plaintext_password", href: "/zh/reference/settings/server-settings/settings/allow#allow_plaintext_password" },
        { name: "allow_use_jemalloc_memory", href: "/zh/reference/settings/server-settings/settings/allow#allow_use_jemalloc_memory" }
      ],
      children: []
    },
    {
      label: "allow_experimental_*",
      count: 3,
      settings: [
        { name: "allow_experimental_executable_udf_drivers", href: "/zh/reference/settings/server-settings/settings/allow-experimental#allow_experimental_executable_udf_drivers" },
        { name: "allow_experimental_webassembly_udf", href: "/zh/reference/settings/server-settings/settings/allow-experimental#allow_experimental_webassembly_udf" },
        { name: "allow_experimental_webterminal", href: "/zh/reference/settings/server-settings/settings/allow-experimental#allow_experimental_webterminal" }
      ],
      children: []
    },
    {
      label: "async_insert_*",
      count: 2,
      settings: [
        { name: "async_insert_queue_flush_on_shutdown", href: "/zh/reference/settings/server-settings/settings/async-insert#async_insert_queue_flush_on_shutdown" },
        { name: "async_insert_threads", href: "/zh/reference/settings/server-settings/settings/async-insert#async_insert_threads" }
      ],
      children: []
    },
    {
      label: "async_load_*",
      count: 2,
      settings: [
        { name: "async_load_databases", href: "/zh/reference/settings/server-settings/settings/async-load#async_load_databases" },
        { name: "async_load_system_database", href: "/zh/reference/settings/server-settings/settings/async-load#async_load_system_database" }
      ],
      children: []
    },
    {
      label: "asynchronous_*",
      count: 3,
      settings: [
        { name: "asynchronous_heavy_metrics_update_period_s", href: "/zh/reference/settings/server-settings/settings/asynchronous#asynchronous_heavy_metrics_update_period_s" },
        { name: "asynchronous_insert_log", href: "/zh/reference/settings/server-settings/settings/asynchronous#asynchronous_insert_log" },
        { name: "asynchronous_metric_log", href: "/zh/reference/settings/server-settings/settings/asynchronous#asynchronous_metric_log" }
      ],
      children: []
    },
    {
      label: "asynchronous_metrics_*",
      count: 3,
      settings: [
        { name: "asynchronous_metrics_enable_heavy_metrics", href: "/zh/reference/settings/server-settings/settings/asynchronous-metrics#asynchronous_metrics_enable_heavy_metrics" },
        { name: "asynchronous_metrics_keeper_metrics_only", href: "/zh/reference/settings/server-settings/settings/asynchronous-metrics#asynchronous_metrics_keeper_metrics_only" },
        { name: "asynchronous_metrics_update_period_s", href: "/zh/reference/settings/server-settings/settings/asynchronous-metrics#asynchronous_metrics_update_period_s" }
      ],
      children: []
    },
    {
      label: "background_*",
      count: 8,
      settings: [
        { name: "background_buffer_flush_schedule_pool_size", href: "/zh/reference/settings/server-settings/settings/background#background_buffer_flush_schedule_pool_size" },
        { name: "background_common_pool_size", href: "/zh/reference/settings/server-settings/settings/background#background_common_pool_size" },
        { name: "background_distributed_schedule_pool_size", href: "/zh/reference/settings/server-settings/settings/background#background_distributed_schedule_pool_size" },
        { name: "background_fetches_pool_size", href: "/zh/reference/settings/server-settings/settings/background#background_fetches_pool_size" },
        { name: "background_message_broker_schedule_pool_size", href: "/zh/reference/settings/server-settings/settings/background#background_message_broker_schedule_pool_size" },
        { name: "background_move_pool_size", href: "/zh/reference/settings/server-settings/settings/background#background_move_pool_size" },
        { name: "background_pool_size", href: "/zh/reference/settings/server-settings/settings/background#background_pool_size" },
        { name: "background_streaming_schedule_pool_size", href: "/zh/reference/settings/server-settings/settings/background#background_streaming_schedule_pool_size" }
      ],
      children: []
    },
    {
      label: "background_merges_*",
      count: 2,
      settings: [
        { name: "background_merges_mutations_concurrency_ratio", href: "/zh/reference/settings/server-settings/settings/background-merges#background_merges_mutations_concurrency_ratio" },
        { name: "background_merges_mutations_scheduling_policy", href: "/zh/reference/settings/server-settings/settings/background-merges#background_merges_mutations_scheduling_policy" }
      ],
      children: []
    },
    {
      label: "background_schedule_*",
      count: 4,
      settings: [
        { name: "background_schedule_pool_initial_size", href: "/zh/reference/settings/server-settings/settings/background-schedule#background_schedule_pool_initial_size" },
        { name: "background_schedule_pool_log", href: "/zh/reference/settings/server-settings/settings/background-schedule#background_schedule_pool_log" },
        {
          name: "background_schedule_pool_max_parallel_tasks_per_type_ratio",
          href: "/zh/reference/settings/server-settings/settings/background-schedule#background_schedule_pool_max_parallel_tasks_per_type_ratio"
        },
        { name: "background_schedule_pool_size", href: "/zh/reference/settings/server-settings/settings/background-schedule#background_schedule_pool_size" }
      ],
      children: []
    },
    {
      label: "backup_*",
      count: 2,
      settings: [
        { name: "backup_log", href: "/zh/reference/settings/server-settings/settings/backup#backup_log" },
        { name: "backup_threads", href: "/zh/reference/settings/server-settings/settings/backup#backup_threads" }
      ],
      children: []
    },
    {
      label: "backups_*",
      count: 2,
      settings: [
        { name: "backups", href: "/zh/reference/settings/server-settings/settings/backups#backups" },
        { name: "backups_io_thread_pool_queue_size", href: "/zh/reference/settings/server-settings/settings/backups#backups_io_thread_pool_queue_size" }
      ],
      children: []
    },
    {
      label: "compiled_expression_*",
      count: 2,
      settings: [
        { name: "compiled_expression_cache_elements_size", href: "/zh/reference/settings/server-settings/settings/compiled-expression#compiled_expression_cache_elements_size" },
        { name: "compiled_expression_cache_size", href: "/zh/reference/settings/server-settings/settings/compiled-expression#compiled_expression_cache_size" }
      ],
      children: []
    },
    {
      label: "concurrent_threads_*",
      count: 4,
      settings: [
        { name: "concurrent_threads_lazy_allocation", href: "/zh/reference/settings/server-settings/settings/concurrent-threads#concurrent_threads_lazy_allocation" },
        { name: "concurrent_threads_scheduler", href: "/zh/reference/settings/server-settings/settings/concurrent-threads#concurrent_threads_scheduler" },
        { name: "concurrent_threads_soft_limit_num", href: "/zh/reference/settings/server-settings/settings/concurrent-threads#concurrent_threads_soft_limit_num" },
        { name: "concurrent_threads_soft_limit_ratio_to_cores", href: "/zh/reference/settings/server-settings/settings/concurrent-threads#concurrent_threads_soft_limit_ratio_to_cores" }
      ],
      children: []
    },
    {
      label: "cpu_slot_*",
      count: 3,
      settings: [
        { name: "cpu_slot_preemption", href: "/zh/reference/settings/server-settings/settings/cpu-slot#cpu_slot_preemption" },
        { name: "cpu_slot_preemption_timeout_ms", href: "/zh/reference/settings/server-settings/settings/cpu-slot#cpu_slot_preemption_timeout_ms" },
        { name: "cpu_slot_quantum_ns", href: "/zh/reference/settings/server-settings/settings/cpu-slot#cpu_slot_quantum_ns" }
      ],
      children: []
    },
    {
      label: "custom_*",
      count: 2,
      settings: [
        { name: "custom_cached_disks_base_directory", href: "/zh/reference/settings/server-settings/settings/custom#custom_cached_disks_base_directory" },
        { name: "custom_settings_prefixes", href: "/zh/reference/settings/server-settings/settings/custom#custom_settings_prefixes" }
      ],
      children: []
    },
    {
      label: "database_catalog_*",
      count: 5,
      settings: [
        { name: "database_catalog_drop_error_cooldown_sec", href: "/zh/reference/settings/server-settings/settings/database-catalog#database_catalog_drop_error_cooldown_sec" },
        { name: "database_catalog_drop_table_concurrency", href: "/zh/reference/settings/server-settings/settings/database-catalog#database_catalog_drop_table_concurrency" },
        { name: "database_catalog_unused_dir_cleanup_period_sec", href: "/zh/reference/settings/server-settings/settings/database-catalog#database_catalog_unused_dir_cleanup_period_sec" },
        { name: "database_catalog_unused_dir_hide_timeout_sec", href: "/zh/reference/settings/server-settings/settings/database-catalog#database_catalog_unused_dir_hide_timeout_sec" },
        { name: "database_catalog_unused_dir_rm_timeout_sec", href: "/zh/reference/settings/server-settings/settings/database-catalog#database_catalog_unused_dir_rm_timeout_sec" }
      ],
      children: []
    },
    {
      label: "database_replicated_*",
      count: 2,
      settings: [
        { name: "database_replicated_allow_detach_permanently", href: "/zh/reference/settings/server-settings/settings/database-replicated#database_replicated_allow_detach_permanently" },
        { name: "database_replicated_drop_broken_tables", href: "/zh/reference/settings/server-settings/settings/database-replicated#database_replicated_drop_broken_tables" }
      ],
      children: []
    },
    {
      label: "default_*",
      count: 4,
      settings: [
        { name: "default_database", href: "/zh/reference/settings/server-settings/settings/default#default_database" },
        { name: "default_password_type", href: "/zh/reference/settings/server-settings/settings/default#default_password_type" },
        { name: "default_profile", href: "/zh/reference/settings/server-settings/settings/default#default_profile" },
        { name: "default_session_timeout", href: "/zh/reference/settings/server-settings/settings/default#default_session_timeout" }
      ],
      children: []
    },
    {
      label: "default_replica_*",
      count: 2,
      settings: [
        { name: "default_replica_name", href: "/zh/reference/settings/server-settings/settings/default-replica#default_replica_name" },
        { name: "default_replica_path", href: "/zh/reference/settings/server-settings/settings/default-replica#default_replica_path" }
      ],
      children: []
    },
    {
      label: "dictionaries_*",
      count: 3,
      settings: [
        { name: "dictionaries_config", href: "/zh/reference/settings/server-settings/settings/dictionaries#dictionaries_config" },
        { name: "dictionaries_lazy_load", href: "/zh/reference/settings/server-settings/settings/dictionaries#dictionaries_lazy_load" },
        { name: "dictionaries_lib_path", href: "/zh/reference/settings/server-settings/settings/dictionaries#dictionaries_lib_path" }
      ],
      children: []
    },
    {
      label: "disable_*",
      count: 3,
      settings: [
        { name: "disable_insertion_and_mutation", href: "/zh/reference/settings/server-settings/settings/disable#disable_insertion_and_mutation" },
        { name: "disable_internal_dns_cache", href: "/zh/reference/settings/server-settings/settings/disable#disable_internal_dns_cache" },
        { name: "disable_tunneling_for_https_requests_over_http_proxy", href: "/zh/reference/settings/server-settings/settings/disable#disable_tunneling_for_https_requests_over_http_proxy" }
      ],
      children: []
    },
    {
      label: "disk_connections_*",
      count: 6,
      settings: [
        { name: "disk_connections_hard_limit", href: "/zh/reference/settings/server-settings/settings/disk-connections#disk_connections_hard_limit" },
        { name: "disk_connections_rcvbuf", href: "/zh/reference/settings/server-settings/settings/disk-connections#disk_connections_rcvbuf" },
        { name: "disk_connections_sndbuf", href: "/zh/reference/settings/server-settings/settings/disk-connections#disk_connections_sndbuf" },
        { name: "disk_connections_soft_limit", href: "/zh/reference/settings/server-settings/settings/disk-connections#disk_connections_soft_limit" },
        { name: "disk_connections_store_limit", href: "/zh/reference/settings/server-settings/settings/disk-connections#disk_connections_store_limit" },
        { name: "disk_connections_warn_limit", href: "/zh/reference/settings/server-settings/settings/disk-connections#disk_connections_warn_limit" }
      ],
      children: []
    },
    {
      label: "distributed_*",
      count: 7,
      settings: [
        { name: "distributed_ddl.cleanup_delay_period", href: "/zh/reference/settings/server-settings/settings/distributed#distributed_ddl.cleanup_delay_period" },
        { name: "distributed_ddl.max_tasks_in_queue", href: "/zh/reference/settings/server-settings/settings/distributed#distributed_ddl.max_tasks_in_queue" },
        { name: "distributed_ddl.path", href: "/zh/reference/settings/server-settings/settings/distributed#distributed_ddl.path" },
        { name: "distributed_ddl.pool_size", href: "/zh/reference/settings/server-settings/settings/distributed#distributed_ddl.pool_size" },
        { name: "distributed_ddl.profile", href: "/zh/reference/settings/server-settings/settings/distributed#distributed_ddl.profile" },
        { name: "distributed_ddl.replicas_path", href: "/zh/reference/settings/server-settings/settings/distributed#distributed_ddl.replicas_path" },
        { name: "distributed_ddl.task_max_lifetime", href: "/zh/reference/settings/server-settings/settings/distributed#distributed_ddl.task_max_lifetime" }
      ],
      children: []
    },
    {
      label: "distributed_cache_*",
      count: 3,
      settings: [
        {
          name: "distributed_cache_apply_throttling_settings_from_client",
          href: "/zh/reference/settings/server-settings/settings/distributed-cache#distributed_cache_apply_throttling_settings_from_client"
        },
        { name: "distributed_cache_keep_up_free_connections_ratio", href: "/zh/reference/settings/server-settings/settings/distributed-cache#distributed_cache_keep_up_free_connections_ratio" },
        { name: "distributed_cache_write_pool_size", href: "/zh/reference/settings/server-settings/settings/distributed-cache#distributed_cache_write_pool_size" }
      ],
      children: []
    },
    {
      label: "distributed_ddl_*",
      count: 2,
      settings: [
        { name: "distributed_ddl", href: "/zh/reference/settings/server-settings/settings/distributed-ddl#distributed_ddl" },
        { name: "distributed_ddl_use_initial_user_and_roles", href: "/zh/reference/settings/server-settings/settings/distributed-ddl#distributed_ddl_use_initial_user_and_roles" }
      ],
      children: []
    },
    {
      label: "dns_allow_*",
      count: 2,
      settings: [
        { name: "dns_allow_resolve_names_to_ipv4", href: "/zh/reference/settings/server-settings/settings/dns-allow#dns_allow_resolve_names_to_ipv4" },
        { name: "dns_allow_resolve_names_to_ipv6", href: "/zh/reference/settings/server-settings/settings/dns-allow#dns_allow_resolve_names_to_ipv6" }
      ],
      children: []
    },
    {
      label: "dns_cache_*",
      count: 2,
      settings: [
        { name: "dns_cache_max_entries", href: "/zh/reference/settings/server-settings/settings/dns-cache#dns_cache_max_entries" },
        { name: "dns_cache_update_period", href: "/zh/reference/settings/server-settings/settings/dns-cache#dns_cache_update_period" }
      ],
      children: []
    },
    {
      label: "drop_distributed_*",
      count: 2,
      settings: [
        { name: "drop_distributed_cache_pool_size", href: "/zh/reference/settings/server-settings/settings/drop-distributed#drop_distributed_cache_pool_size" },
        { name: "drop_distributed_cache_queue_size", href: "/zh/reference/settings/server-settings/settings/drop-distributed#drop_distributed_cache_queue_size" }
      ],
      children: []
    },
    {
      label: "enable_*",
      count: 2,
      settings: [
        { name: "enable_azure_sdk_logging", href: "/zh/reference/settings/server-settings/settings/enable#enable_azure_sdk_logging" },
        { name: "enable_webterminal", href: "/zh/reference/settings/server-settings/settings/enable#enable_webterminal" }
      ],
      children: []
    },
    {
      label: "encryption_header_*",
      count: 3,
      settings: [
        { name: "encryption_header_cache_policy", href: "/zh/reference/settings/server-settings/settings/encryption-header#encryption_header_cache_policy" },
        { name: "encryption_header_cache_size", href: "/zh/reference/settings/server-settings/settings/encryption-header#encryption_header_cache_size" },
        { name: "encryption_header_cache_size_ratio", href: "/zh/reference/settings/server-settings/settings/encryption-header#encryption_header_cache_size_ratio" }
      ],
      children: []
    },
    {
      label: "format_*",
      count: 2,
      settings: [
        { name: "format_parsing_thread_pool_queue_size", href: "/zh/reference/settings/server-settings/settings/format#format_parsing_thread_pool_queue_size" },
        { name: "format_schema_path", href: "/zh/reference/settings/server-settings/settings/format#format_schema_path" }
      ],
      children: []
    },
    {
      label: "global_profiler_*",
      count: 2,
      settings: [
        { name: "global_profiler_cpu_time_period_ns", href: "/zh/reference/settings/server-settings/settings/global-profiler#global_profiler_cpu_time_period_ns" },
        { name: "global_profiler_real_time_period_ns", href: "/zh/reference/settings/server-settings/settings/global-profiler#global_profiler_real_time_period_ns" }
      ],
      children: []
    },
    {
      label: "graphite_*",
      count: 2,
      settings: [
        { name: "graphite", href: "/zh/reference/settings/server-settings/settings/graphite#graphite" },
        { name: "graphite_rollup", href: "/zh/reference/settings/server-settings/settings/graphite#graphite_rollup" }
      ],
      children: []
    },
    {
      label: "http_*",
      count: 3,
      settings: [
        { name: "http_handlers", href: "/zh/reference/settings/server-settings/settings/http#http_handlers" },
        { name: "http_options_response", href: "/zh/reference/settings/server-settings/settings/http#http_options_response" },
        { name: "http_server_default_response", href: "/zh/reference/settings/server-settings/settings/http#http_server_default_response" }
      ],
      children: []
    },
    {
      label: "http_connections_*",
      count: 6,
      settings: [
        { name: "http_connections_hard_limit", href: "/zh/reference/settings/server-settings/settings/http-connections#http_connections_hard_limit" },
        { name: "http_connections_rcvbuf", href: "/zh/reference/settings/server-settings/settings/http-connections#http_connections_rcvbuf" },
        { name: "http_connections_sndbuf", href: "/zh/reference/settings/server-settings/settings/http-connections#http_connections_sndbuf" },
        { name: "http_connections_soft_limit", href: "/zh/reference/settings/server-settings/settings/http-connections#http_connections_soft_limit" },
        { name: "http_connections_store_limit", href: "/zh/reference/settings/server-settings/settings/http-connections#http_connections_store_limit" },
        { name: "http_connections_warn_limit", href: "/zh/reference/settings/server-settings/settings/http-connections#http_connections_warn_limit" }
      ],
      children: []
    },
    {
      label: "iceberg_catalog_*",
      count: 2,
      settings: [
        { name: "iceberg_catalog_threadpool_pool_size", href: "/zh/reference/settings/server-settings/settings/iceberg-catalog#iceberg_catalog_threadpool_pool_size" },
        { name: "iceberg_catalog_threadpool_queue_size", href: "/zh/reference/settings/server-settings/settings/iceberg-catalog#iceberg_catalog_threadpool_queue_size" }
      ],
      children: []
    },
    {
      label: "iceberg_compaction_*",
      count: 2,
      settings: [
        { name: "iceberg_compaction_threadpool_pool_size", href: "/zh/reference/settings/server-settings/settings/iceberg-compaction#iceberg_compaction_threadpool_pool_size" },
        { name: "iceberg_compaction_threadpool_queue_size", href: "/zh/reference/settings/server-settings/settings/iceberg-compaction#iceberg_compaction_threadpool_queue_size" }
      ],
      children: []
    },
    {
      label: "iceberg_metadata_*",
      count: 4,
      settings: [
        { name: "iceberg_metadata_files_cache_max_entries", href: "/zh/reference/settings/server-settings/settings/iceberg-metadata#iceberg_metadata_files_cache_max_entries" },
        { name: "iceberg_metadata_files_cache_policy", href: "/zh/reference/settings/server-settings/settings/iceberg-metadata#iceberg_metadata_files_cache_policy" },
        { name: "iceberg_metadata_files_cache_size", href: "/zh/reference/settings/server-settings/settings/iceberg-metadata#iceberg_metadata_files_cache_size" },
        { name: "iceberg_metadata_files_cache_size_ratio", href: "/zh/reference/settings/server-settings/settings/iceberg-metadata#iceberg_metadata_files_cache_size_ratio" }
      ],
      children: []
    },
    {
      label: "iceberg_scheduler_*",
      count: 2,
      settings: [
        { name: "iceberg_scheduler_compaction_threadpool_pool_size", href: "/zh/reference/settings/server-settings/settings/iceberg-scheduler#iceberg_scheduler_compaction_threadpool_pool_size" },
        { name: "iceberg_scheduler_compaction_threadpool_queue_size", href: "/zh/reference/settings/server-settings/settings/iceberg-scheduler#iceberg_scheduler_compaction_threadpool_queue_size" }
      ],
      children: []
    },
    {
      label: "index_mark_*",
      count: 4,
      settings: [
        { name: "index_mark_cache_policy", href: "/zh/reference/settings/server-settings/settings/index-mark#index_mark_cache_policy" },
        { name: "index_mark_cache_prewarm_ratio", href: "/zh/reference/settings/server-settings/settings/index-mark#index_mark_cache_prewarm_ratio" },
        { name: "index_mark_cache_size", href: "/zh/reference/settings/server-settings/settings/index-mark#index_mark_cache_size" },
        { name: "index_mark_cache_size_ratio", href: "/zh/reference/settings/server-settings/settings/index-mark#index_mark_cache_size_ratio" }
      ],
      children: []
    },
    {
      label: "index_uncompressed_*",
      count: 3,
      settings: [
        { name: "index_uncompressed_cache_policy", href: "/zh/reference/settings/server-settings/settings/index-uncompressed#index_uncompressed_cache_policy" },
        { name: "index_uncompressed_cache_size", href: "/zh/reference/settings/server-settings/settings/index-uncompressed#index_uncompressed_cache_size" },
        { name: "index_uncompressed_cache_size_ratio", href: "/zh/reference/settings/server-settings/settings/index-uncompressed#index_uncompressed_cache_size_ratio" }
      ],
      children: []
    },
    {
      label: "interserver_*",
      count: 2,
      settings: [
        { name: "interserver_listen_host", href: "/zh/reference/settings/server-settings/settings/interserver#interserver_listen_host" },
        { name: "interserver_tables_status_require_auth", href: "/zh/reference/settings/server-settings/settings/interserver#interserver_tables_status_require_auth" }
      ],
      children: []
    },
    {
      label: "interserver_http_*",
      count: 3,
      settings: [
        { name: "interserver_http_credentials", href: "/zh/reference/settings/server-settings/settings/interserver-http#interserver_http_credentials" },
        { name: "interserver_http_host", href: "/zh/reference/settings/server-settings/settings/interserver-http#interserver_http_host" },
        { name: "interserver_http_port", href: "/zh/reference/settings/server-settings/settings/interserver-http#interserver_http_port" }
      ],
      children: []
    },
    {
      label: "interserver_https_*",
      count: 2,
      settings: [
        { name: "interserver_https_host", href: "/zh/reference/settings/server-settings/settings/interserver-https#interserver_https_host" },
        { name: "interserver_https_port", href: "/zh/reference/settings/server-settings/settings/interserver-https#interserver_https_port" }
      ],
      children: []
    },
    {
      label: "jemalloc_*",
      count: 3,
      settings: [
        { name: "jemalloc_collect_global_profile_samples_in_trace_log", href: "/zh/reference/settings/server-settings/settings/jemalloc#jemalloc_collect_global_profile_samples_in_trace_log" },
        { name: "jemalloc_max_background_threads_num", href: "/zh/reference/settings/server-settings/settings/jemalloc#jemalloc_max_background_threads_num" },
        { name: "jemalloc_profiler_sampling_rate", href: "/zh/reference/settings/server-settings/settings/jemalloc#jemalloc_profiler_sampling_rate" }
      ],
      children: []
    },
    {
      label: "jemalloc_enable_*",
      count: 2,
      settings: [
        { name: "jemalloc_enable_background_threads", href: "/zh/reference/settings/server-settings/settings/jemalloc-enable#jemalloc_enable_background_threads" },
        { name: "jemalloc_enable_global_profiler", href: "/zh/reference/settings/server-settings/settings/jemalloc-enable#jemalloc_enable_global_profiler" }
      ],
      children: []
    },
    {
      label: "jemalloc_flush_*",
      count: 3,
      settings: [
        { name: "jemalloc_flush_profile_interval_bytes", href: "/zh/reference/settings/server-settings/settings/jemalloc-flush#jemalloc_flush_profile_interval_bytes" },
        { name: "jemalloc_flush_profile_on_memory_exceeded", href: "/zh/reference/settings/server-settings/settings/jemalloc-flush#jemalloc_flush_profile_on_memory_exceeded" },
        { name: "jemalloc_flush_profile_on_memory_exceeded_interval", href: "/zh/reference/settings/server-settings/settings/jemalloc-flush#jemalloc_flush_profile_on_memory_exceeded_interval" }
      ],
      children: []
    },
    {
      label: "keeper_*",
      count: 2,
      settings: [
        { name: "keeper_hosts", href: "/zh/reference/settings/server-settings/settings/keeper#keeper_hosts" },
        { name: "keeper_multiread_batch_size", href: "/zh/reference/settings/server-settings/settings/keeper#keeper_multiread_batch_size" }
      ],
      children: []
    },
    {
      label: "keeper_server.socket_*",
      count: 2,
      settings: [
        { name: "keeper_server.socket_receive_timeout_sec", href: "/zh/reference/settings/server-settings/settings/keeper-server-socket#keeper_server.socket_receive_timeout_sec" },
        { name: "keeper_server.socket_send_timeout_sec", href: "/zh/reference/settings/server-settings/settings/keeper-server-socket#keeper_server.socket_send_timeout_sec" }
      ],
      children: []
    },
    {
      label: "license_*",
      count: 2,
      settings: [
        { name: "license_file", href: "/zh/reference/settings/server-settings/settings/license#license_file" },
        { name: "license_public_key_for_testing", href: "/zh/reference/settings/server-settings/settings/license#license_public_key_for_testing" }
      ],
      children: []
    },
    {
      label: "listen_*",
      count: 4,
      settings: [
        { name: "listen_backlog", href: "/zh/reference/settings/server-settings/settings/listen#listen_backlog" },
        { name: "listen_host", href: "/zh/reference/settings/server-settings/settings/listen#listen_host" },
        { name: "listen_reuse_port", href: "/zh/reference/settings/server-settings/settings/listen#listen_reuse_port" },
        { name: "listen_try", href: "/zh/reference/settings/server-settings/settings/listen#listen_try" }
      ],
      children: []
    },
    {
      label: "load_marks_*",
      count: 2,
      settings: [
        { name: "load_marks_threadpool_pool_size", href: "/zh/reference/settings/server-settings/settings/load-marks#load_marks_threadpool_pool_size" },
        { name: "load_marks_threadpool_queue_size", href: "/zh/reference/settings/server-settings/settings/load-marks#load_marks_threadpool_queue_size" }
      ],
      children: []
    },
    {
      label: "logger.async_*",
      count: 2,
      settings: [
        { name: "logger.async", href: "/zh/reference/settings/server-settings/settings/logger-async#logger.async" },
        { name: "logger.async_queye_max_size", href: "/zh/reference/settings/server-settings/settings/logger-async#logger.async_queye_max_size" }
      ],
      children: []
    },
    {
      label: "logger.console_*",
      count: 2,
      settings: [
        { name: "logger.console", href: "/zh/reference/settings/server-settings/settings/logger-console#logger.console" },
        { name: "logger.console_log_level", href: "/zh/reference/settings/server-settings/settings/logger-console#logger.console_log_level" }
      ],
      children: []
    },
    {
      label: "mark_cache_*",
      count: 4,
      settings: [
        { name: "mark_cache_policy", href: "/zh/reference/settings/server-settings/settings/mark-cache#mark_cache_policy" },
        { name: "mark_cache_prewarm_ratio", href: "/zh/reference/settings/server-settings/settings/mark-cache#mark_cache_prewarm_ratio" },
        { name: "mark_cache_size", href: "/zh/reference/settings/server-settings/settings/mark-cache#mark_cache_size" },
        { name: "mark_cache_size_ratio", href: "/zh/reference/settings/server-settings/settings/mark-cache#mark_cache_size_ratio" }
      ],
      children: []
    },
    {
      label: "max_*",
      count: 25,
      settings: [
        { name: "max_active_parts_loading_thread_pool_size", href: "/zh/reference/settings/server-settings/settings/max#max_active_parts_loading_thread_pool_size" },
        { name: "max_authentication_methods_per_user", href: "/zh/reference/settings/server-settings/settings/max#max_authentication_methods_per_user" },
        { name: "max_backup_bandwidth_for_server", href: "/zh/reference/settings/server-settings/settings/max#max_backup_bandwidth_for_server" },
        { name: "max_build_vector_similarity_index_thread_pool_size", href: "/zh/reference/settings/server-settings/settings/max#max_build_vector_similarity_index_thread_pool_size" },
        { name: "max_connections", href: "/zh/reference/settings/server-settings/settings/max#max_connections" },
        { name: "max_entries_for_hash_table_stats", href: "/zh/reference/settings/server-settings/settings/max#max_entries_for_hash_table_stats" },
        { name: "max_fetch_partition_thread_pool_size", href: "/zh/reference/settings/server-settings/settings/max#max_fetch_partition_thread_pool_size" },
        { name: "max_held_snapshots", href: "/zh/reference/settings/server-settings/settings/max#max_held_snapshots" },
        { name: "max_http_index_page_size", href: "/zh/reference/settings/server-settings/settings/max#max_http_index_page_size" },
        { name: "max_keep_alive_requests", href: "/zh/reference/settings/server-settings/settings/max#max_keep_alive_requests" },
        { name: "max_materialized_views_count_for_table", href: "/zh/reference/settings/server-settings/settings/max#max_materialized_views_count_for_table" },
        { name: "max_merges_bandwidth_for_server", href: "/zh/reference/settings/server-settings/settings/max#max_merges_bandwidth_for_server" },
        { name: "max_mutations_bandwidth_for_server", href: "/zh/reference/settings/server-settings/settings/max#max_mutations_bandwidth_for_server" },
        { name: "max_open_files", href: "/zh/reference/settings/server-settings/settings/max#max_open_files" },
        { name: "max_os_cpu_wait_time_ratio_to_drop_connection", href: "/zh/reference/settings/server-settings/settings/max#max_os_cpu_wait_time_ratio_to_drop_connection" },
        { name: "max_outdated_parts_loading_thread_pool_size", href: "/zh/reference/settings/server-settings/settings/max#max_outdated_parts_loading_thread_pool_size" },
        { name: "max_part_num_to_warn", href: "/zh/reference/settings/server-settings/settings/max#max_part_num_to_warn" },
        { name: "max_partition_size_to_drop", href: "/zh/reference/settings/server-settings/settings/max#max_partition_size_to_drop" },
        { name: "max_parts_cleaning_thread_pool_size", href: "/zh/reference/settings/server-settings/settings/max#max_parts_cleaning_thread_pool_size" },
        { name: "max_per_cpu_untracked_memory", href: "/zh/reference/settings/server-settings/settings/max#max_per_cpu_untracked_memory" },
        { name: "max_session_timeout", href: "/zh/reference/settings/server-settings/settings/max#max_session_timeout" },
        { name: "max_temporary_data_on_disk_size", href: "/zh/reference/settings/server-settings/settings/max#max_temporary_data_on_disk_size" },
        { name: "max_unexpected_parts_loading_thread_pool_size", href: "/zh/reference/settings/server-settings/settings/max#max_unexpected_parts_loading_thread_pool_size" },
        { name: "max_waiting_queries", href: "/zh/reference/settings/server-settings/settings/max#max_waiting_queries" },
        { name: "max_zookeeper_pooled_connections", href: "/zh/reference/settings/server-settings/settings/max#max_zookeeper_pooled_connections" }
      ],
      children: []
    },
    {
      label: "max_backups_*",
      count: 2,
      settings: [
        { name: "max_backups_io_thread_pool_free_size", href: "/zh/reference/settings/server-settings/settings/max-backups#max_backups_io_thread_pool_free_size" },
        { name: "max_backups_io_thread_pool_size", href: "/zh/reference/settings/server-settings/settings/max-backups#max_backups_io_thread_pool_size" }
      ],
      children: []
    },
    {
      label: "max_concurrent_*",
      count: 3,
      settings: [
        { name: "max_concurrent_insert_queries", href: "/zh/reference/settings/server-settings/settings/max-concurrent#max_concurrent_insert_queries" },
        { name: "max_concurrent_queries", href: "/zh/reference/settings/server-settings/settings/max-concurrent#max_concurrent_queries" },
        { name: "max_concurrent_select_queries", href: "/zh/reference/settings/server-settings/settings/max-concurrent#max_concurrent_select_queries" }
      ],
      children: []
    },
    {
      label: "max_database_*",
      count: 3,
      settings: [
        { name: "max_database_num_to_throw", href: "/zh/reference/settings/server-settings/settings/max-database#max_database_num_to_throw" },
        { name: "max_database_num_to_warn", href: "/zh/reference/settings/server-settings/settings/max-database#max_database_num_to_warn" },
        { name: "max_database_replicated_create_table_thread_pool_size", href: "/zh/reference/settings/server-settings/settings/max-database#max_database_replicated_create_table_thread_pool_size" }
      ],
      children: []
    },
    {
      label: "max_dictionary_*",
      count: 2,
      settings: [
        { name: "max_dictionary_num_to_throw", href: "/zh/reference/settings/server-settings/settings/max-dictionary#max_dictionary_num_to_throw" },
        { name: "max_dictionary_num_to_warn", href: "/zh/reference/settings/server-settings/settings/max-dictionary#max_dictionary_num_to_warn" }
      ],
      children: []
    },
    {
      label: "max_distributed_*",
      count: 2,
      settings: [
        { name: "max_distributed_cache_read_bandwidth_for_server", href: "/zh/reference/settings/server-settings/settings/max-distributed#max_distributed_cache_read_bandwidth_for_server" },
        { name: "max_distributed_cache_write_bandwidth_for_server", href: "/zh/reference/settings/server-settings/settings/max-distributed#max_distributed_cache_write_bandwidth_for_server" }
      ],
      children: []
    },
    {
      label: "max_format_*",
      count: 2,
      settings: [
        { name: "max_format_parsing_thread_pool_free_size", href: "/zh/reference/settings/server-settings/settings/max-format#max_format_parsing_thread_pool_free_size" },
        { name: "max_format_parsing_thread_pool_size", href: "/zh/reference/settings/server-settings/settings/max-format#max_format_parsing_thread_pool_size" }
      ],
      children: []
    },
    {
      label: "max_io_*",
      count: 2,
      settings: [
        { name: "max_io_thread_pool_free_size", href: "/zh/reference/settings/server-settings/settings/max-io#max_io_thread_pool_free_size" },
        { name: "max_io_thread_pool_size", href: "/zh/reference/settings/server-settings/settings/max-io#max_io_thread_pool_size" }
      ],
      children: []
    },
    {
      label: "max_local_*",
      count: 2,
      settings: [
        { name: "max_local_read_bandwidth_for_server", href: "/zh/reference/settings/server-settings/settings/max-local#max_local_read_bandwidth_for_server" },
        { name: "max_local_write_bandwidth_for_server", href: "/zh/reference/settings/server-settings/settings/max-local#max_local_write_bandwidth_for_server" }
      ],
      children: []
    },
    {
      label: "max_named_*",
      count: 2,
      settings: [
        { name: "max_named_collection_num_to_throw", href: "/zh/reference/settings/server-settings/settings/max-named#max_named_collection_num_to_throw" },
        { name: "max_named_collection_num_to_warn", href: "/zh/reference/settings/server-settings/settings/max-named#max_named_collection_num_to_warn" }
      ],
      children: []
    },
    {
      label: "max_pending_*",
      count: 2,
      settings: [
        { name: "max_pending_mutations_execution_time_to_warn", href: "/zh/reference/settings/server-settings/settings/max-pending#max_pending_mutations_execution_time_to_warn" },
        { name: "max_pending_mutations_to_warn", href: "/zh/reference/settings/server-settings/settings/max-pending#max_pending_mutations_to_warn" }
      ],
      children: []
    },
    {
      label: "max_prefixes_*",
      count: 2,
      settings: [
        { name: "max_prefixes_deserialization_thread_pool_free_size", href: "/zh/reference/settings/server-settings/settings/max-prefixes#max_prefixes_deserialization_thread_pool_free_size" },
        { name: "max_prefixes_deserialization_thread_pool_size", href: "/zh/reference/settings/server-settings/settings/max-prefixes#max_prefixes_deserialization_thread_pool_size" }
      ],
      children: []
    },
    {
      label: "max_remote_*",
      count: 3,
      settings: [
        { name: "max_remote_read_connections", href: "/zh/reference/settings/server-settings/settings/max-remote#max_remote_read_connections" },
        { name: "max_remote_read_network_bandwidth_for_server", href: "/zh/reference/settings/server-settings/settings/max-remote#max_remote_read_network_bandwidth_for_server" },
        { name: "max_remote_write_network_bandwidth_for_server", href: "/zh/reference/settings/server-settings/settings/max-remote#max_remote_write_network_bandwidth_for_server" }
      ],
      children: []
    },
    {
      label: "max_replicated_*",
      count: 3,
      settings: [
        { name: "max_replicated_fetches_network_bandwidth_for_server", href: "/zh/reference/settings/server-settings/settings/max-replicated#max_replicated_fetches_network_bandwidth_for_server" },
        { name: "max_replicated_sends_network_bandwidth_for_server", href: "/zh/reference/settings/server-settings/settings/max-replicated#max_replicated_sends_network_bandwidth_for_server" },
        { name: "max_replicated_table_num_to_throw", href: "/zh/reference/settings/server-settings/settings/max-replicated#max_replicated_table_num_to_throw" }
      ],
      children: []
    },
    {
      label: "max_server_memory_usage_*",
      count: 2,
      settings: [
        { name: "max_server_memory_usage", href: "/zh/reference/settings/server-settings/settings/max-server-memory-usage#max_server_memory_usage" },
        { name: "max_server_memory_usage_to_ram_ratio", href: "/zh/reference/settings/server-settings/settings/max-server-memory-usage#max_server_memory_usage_to_ram_ratio" }
      ],
      children: []
    },
    {
      label: "max_snapshot_*",
      count: 2,
      settings: [
        { name: "max_snapshot_commit_thread_pool_free_size", href: "/zh/reference/settings/server-settings/settings/max-snapshot#max_snapshot_commit_thread_pool_free_size" },
        { name: "max_snapshot_commit_thread_pool_size", href: "/zh/reference/settings/server-settings/settings/max-snapshot#max_snapshot_commit_thread_pool_size" }
      ],
      children: []
    },
    {
      label: "max_table_*",
      count: 3,
      settings: [
        { name: "max_table_num_to_throw", href: "/zh/reference/settings/server-settings/settings/max-table#max_table_num_to_throw" },
        { name: "max_table_num_to_warn", href: "/zh/reference/settings/server-settings/settings/max-table#max_table_num_to_warn" },
        { name: "max_table_size_to_drop", href: "/zh/reference/settings/server-settings/settings/max-table#max_table_size_to_drop" }
      ],
      children: []
    },
    {
      label: "max_thread_*",
      count: 2,
      settings: [
        { name: "max_thread_pool_free_size", href: "/zh/reference/settings/server-settings/settings/max-thread#max_thread_pool_free_size" },
        { name: "max_thread_pool_size", href: "/zh/reference/settings/server-settings/settings/max-thread#max_thread_pool_size" }
      ],
      children: []
    },
    {
      label: "max_view_*",
      count: 2,
      settings: [
        { name: "max_view_num_to_throw", href: "/zh/reference/settings/server-settings/settings/max-view#max_view_num_to_throw" },
        { name: "max_view_num_to_warn", href: "/zh/reference/settings/server-settings/settings/max-view#max_view_num_to_warn" }
      ],
      children: []
    },
    {
      label: "memory_worker_*",
      count: 8,
      settings: [
        { name: "memory_worker_correct_memory_tracker", href: "/zh/reference/settings/server-settings/settings/memory-worker#memory_worker_correct_memory_tracker" },
        { name: "memory_worker_decay_adjustment_period_ms", href: "/zh/reference/settings/server-settings/settings/memory-worker#memory_worker_decay_adjustment_period_ms" },
        { name: "memory_worker_dynamic_hard_limit", href: "/zh/reference/settings/server-settings/settings/memory-worker#memory_worker_dynamic_hard_limit" },
        { name: "memory_worker_period_ms", href: "/zh/reference/settings/server-settings/settings/memory-worker#memory_worker_period_ms" },
        { name: "memory_worker_purge_dirty_pages_threshold_ratio", href: "/zh/reference/settings/server-settings/settings/memory-worker#memory_worker_purge_dirty_pages_threshold_ratio" },
        { name: "memory_worker_purge_total_memory_threshold_ratio", href: "/zh/reference/settings/server-settings/settings/memory-worker#memory_worker_purge_total_memory_threshold_ratio" },
        { name: "memory_worker_rss_speculative_reserve_ratio", href: "/zh/reference/settings/server-settings/settings/memory-worker#memory_worker_rss_speculative_reserve_ratio" },
        { name: "memory_worker_use_cgroup", href: "/zh/reference/settings/server-settings/settings/memory-worker#memory_worker_use_cgroup" }
      ],
      children: []
    },
    {
      label: "merge_*",
      count: 2,
      settings: [
        { name: "merge_tree", href: "/zh/reference/settings/server-settings/settings/merge#merge_tree" },
        { name: "merge_workload", href: "/zh/reference/settings/server-settings/settings/merge#merge_workload" }
      ],
      children: []
    },
    {
      label: "merges_mutations_*",
      count: 2,
      settings: [
        { name: "merges_mutations_memory_usage_soft_limit", href: "/zh/reference/settings/server-settings/settings/merges-mutations#merges_mutations_memory_usage_soft_limit" },
        { name: "merges_mutations_memory_usage_to_ram_ratio", href: "/zh/reference/settings/server-settings/settings/merges-mutations#merges_mutations_memory_usage_to_ram_ratio" }
      ],
      children: []
    },
    {
      label: "min_*",
      count: 2,
      settings: [
        { name: "min_allocation_size_to_throw_on_memory_limit", href: "/zh/reference/settings/server-settings/settings/min#min_allocation_size_to_throw_on_memory_limit" },
        { name: "min_os_cpu_wait_time_ratio_to_drop_connection", href: "/zh/reference/settings/server-settings/settings/min#min_os_cpu_wait_time_ratio_to_drop_connection" }
      ],
      children: []
    },
    {
      label: "mlock_executable_*",
      count: 2,
      settings: [
        { name: "mlock_executable", href: "/zh/reference/settings/server-settings/settings/mlock-executable#mlock_executable" },
        { name: "mlock_executable_min_total_memory_amount_bytes", href: "/zh/reference/settings/server-settings/settings/mlock-executable#mlock_executable_min_total_memory_amount_bytes" }
      ],
      children: []
    },
    {
      label: "mysql_*",
      count: 2,
      settings: [
        { name: "mysql_port", href: "/zh/reference/settings/server-settings/settings/mysql#mysql_port" },
        { name: "mysql_require_secure_transport", href: "/zh/reference/settings/server-settings/settings/mysql#mysql_require_secure_transport" }
      ],
      children: []
    },
    {
      label: "oom_canary_*",
      count: 6,
      settings: [
        { name: "oom_canary_enable", href: "/zh/reference/settings/server-settings/settings/oom-canary#oom_canary_enable" },
        { name: "oom_canary_initial_backoff_seconds", href: "/zh/reference/settings/server-settings/settings/oom-canary#oom_canary_initial_backoff_seconds" },
        { name: "oom_canary_max_backoff_seconds", href: "/zh/reference/settings/server-settings/settings/oom-canary#oom_canary_max_backoff_seconds" },
        { name: "oom_canary_max_rapid_relaunches", href: "/zh/reference/settings/server-settings/settings/oom-canary#oom_canary_max_rapid_relaunches" },
        { name: "oom_canary_relaunch", href: "/zh/reference/settings/server-settings/settings/oom-canary#oom_canary_relaunch" },
        { name: "oom_canary_size", href: "/zh/reference/settings/server-settings/settings/oom-canary#oom_canary_size" }
      ],
      children: []
    },
    {
      label: "openSSL.client.requireTLSv1_*",
      count: 3,
      settings: [
        { name: "openSSL.client.requireTLSv1", href: "/zh/reference/settings/server-settings/settings/openssl-client-requiretlsv1#openssl.client.requiretlsv1" },
        { name: "openSSL.client.requireTLSv1_1", href: "/zh/reference/settings/server-settings/settings/openssl-client-requiretlsv1#openssl.client.requiretlsv1_1" },
        { name: "openSSL.client.requireTLSv1_2", href: "/zh/reference/settings/server-settings/settings/openssl-client-requiretlsv1#openssl.client.requiretlsv1_2" }
      ],
      children: []
    },
    {
      label: "openSSL.server.requireTLSv1_*",
      count: 3,
      settings: [
        { name: "openSSL.server.requireTLSv1", href: "/zh/reference/settings/server-settings/settings/openssl-server-requiretlsv1#openssl.server.requiretlsv1" },
        { name: "openSSL.server.requireTLSv1_1", href: "/zh/reference/settings/server-settings/settings/openssl-server-requiretlsv1#openssl.server.requiretlsv1_1" },
        { name: "openSSL.server.requireTLSv1_2", href: "/zh/reference/settings/server-settings/settings/openssl-server-requiretlsv1#openssl.server.requiretlsv1_2" }
      ],
      children: []
    },
    {
      label: "os_*",
      count: 2,
      settings: [
        { name: "os_collect_psi_metrics", href: "/zh/reference/settings/server-settings/settings/os#os_collect_psi_metrics" },
        { name: "os_cpu_busy_time_threshold", href: "/zh/reference/settings/server-settings/settings/os#os_cpu_busy_time_threshold" }
      ],
      children: []
    },
    {
      label: "os_threads_*",
      count: 3,
      settings: [
        { name: "os_threads_nice_value_distributed_cache_tcp_handler", href: "/zh/reference/settings/server-settings/settings/os-threads#os_threads_nice_value_distributed_cache_tcp_handler" },
        { name: "os_threads_nice_value_merge_mutate", href: "/zh/reference/settings/server-settings/settings/os-threads#os_threads_nice_value_merge_mutate" },
        { name: "os_threads_nice_value_zookeeper_client_send_receive", href: "/zh/reference/settings/server-settings/settings/os-threads#os_threads_nice_value_zookeeper_client_send_receive" }
      ],
      children: []
    },
    {
      label: "page_cache_*",
      count: 7,
      settings: [
        { name: "page_cache_free_memory_ratio", href: "/zh/reference/settings/server-settings/settings/page-cache#page_cache_free_memory_ratio" },
        { name: "page_cache_history_window_ms", href: "/zh/reference/settings/server-settings/settings/page-cache#page_cache_history_window_ms" },
        { name: "page_cache_max_size", href: "/zh/reference/settings/server-settings/settings/page-cache#page_cache_max_size" },
        { name: "page_cache_min_size", href: "/zh/reference/settings/server-settings/settings/page-cache#page_cache_min_size" },
        { name: "page_cache_policy", href: "/zh/reference/settings/server-settings/settings/page-cache#page_cache_policy" },
        { name: "page_cache_shards", href: "/zh/reference/settings/server-settings/settings/page-cache#page_cache_shards" },
        { name: "page_cache_size_ratio", href: "/zh/reference/settings/server-settings/settings/page-cache#page_cache_size_ratio" }
      ],
      children: []
    },
    {
      label: "paimon_metadata_*",
      count: 4,
      settings: [
        { name: "paimon_metadata_files_cache_max_entries", href: "/zh/reference/settings/server-settings/settings/paimon-metadata#paimon_metadata_files_cache_max_entries" },
        { name: "paimon_metadata_files_cache_policy", href: "/zh/reference/settings/server-settings/settings/paimon-metadata#paimon_metadata_files_cache_policy" },
        { name: "paimon_metadata_files_cache_size", href: "/zh/reference/settings/server-settings/settings/paimon-metadata#paimon_metadata_files_cache_size" },
        { name: "paimon_metadata_files_cache_size_ratio", href: "/zh/reference/settings/server-settings/settings/paimon-metadata#paimon_metadata_files_cache_size_ratio" }
      ],
      children: []
    },
    {
      label: "parquet_metadata_*",
      count: 4,
      settings: [
        { name: "parquet_metadata_cache_max_entries", href: "/zh/reference/settings/server-settings/settings/parquet-metadata#parquet_metadata_cache_max_entries" },
        { name: "parquet_metadata_cache_policy", href: "/zh/reference/settings/server-settings/settings/parquet-metadata#parquet_metadata_cache_policy" },
        { name: "parquet_metadata_cache_size", href: "/zh/reference/settings/server-settings/settings/parquet-metadata#parquet_metadata_cache_size" },
        { name: "parquet_metadata_cache_size_ratio", href: "/zh/reference/settings/server-settings/settings/parquet-metadata#parquet_metadata_cache_size_ratio" }
      ],
      children: []
    },
    {
      label: "parts_kill_delay_period_*",
      count: 2,
      settings: [
        { name: "parts_kill_delay_period", href: "/zh/reference/settings/server-settings/settings/parts-kill-delay-period#parts_kill_delay_period" },
        { name: "parts_kill_delay_period_random_add", href: "/zh/reference/settings/server-settings/settings/parts-kill-delay-period#parts_kill_delay_period_random_add" }
      ],
      children: []
    },
    {
      label: "parts_killer_*",
      count: 2,
      settings: [
        { name: "parts_killer_max_condemned_parts_per_batch", href: "/zh/reference/settings/server-settings/settings/parts-killer#parts_killer_max_condemned_parts_per_batch" },
        { name: "parts_killer_pool_size", href: "/zh/reference/settings/server-settings/settings/parts-killer#parts_killer_pool_size" }
      ],
      children: []
    },
    {
      label: "postgresql_*",
      count: 2,
      settings: [
        { name: "postgresql_port", href: "/zh/reference/settings/server-settings/settings/postgresql#postgresql_port" },
        { name: "postgresql_require_secure_transport", href: "/zh/reference/settings/server-settings/settings/postgresql#postgresql_require_secure_transport" }
      ],
      children: []
    },
    {
      label: "prefetch_threadpool_*",
      count: 2,
      settings: [
        { name: "prefetch_threadpool_pool_size", href: "/zh/reference/settings/server-settings/settings/prefetch-threadpool#prefetch_threadpool_pool_size" },
        { name: "prefetch_threadpool_queue_size", href: "/zh/reference/settings/server-settings/settings/prefetch-threadpool#prefetch_threadpool_queue_size" }
      ],
      children: []
    },
    {
      label: "primary_index_*",
      count: 4,
      settings: [
        { name: "primary_index_cache_policy", href: "/zh/reference/settings/server-settings/settings/primary-index#primary_index_cache_policy" },
        { name: "primary_index_cache_prewarm_ratio", href: "/zh/reference/settings/server-settings/settings/primary-index#primary_index_cache_prewarm_ratio" },
        { name: "primary_index_cache_size", href: "/zh/reference/settings/server-settings/settings/primary-index#primary_index_cache_size" },
        { name: "primary_index_cache_size_ratio", href: "/zh/reference/settings/server-settings/settings/primary-index#primary_index_cache_size_ratio" }
      ],
      children: []
    },
    {
      label: "query_*",
      count: 6,
      settings: [
        { name: "query_cache", href: "/zh/reference/settings/server-settings/settings/query#query_cache" },
        { name: "query_log", href: "/zh/reference/settings/server-settings/settings/query#query_log" },
        { name: "query_masking_rules", href: "/zh/reference/settings/server-settings/settings/query#query_masking_rules" },
        { name: "query_metric_log", href: "/zh/reference/settings/server-settings/settings/query#query_metric_log" },
        { name: "query_thread_log", href: "/zh/reference/settings/server-settings/settings/query#query_thread_log" },
        { name: "query_views_log", href: "/zh/reference/settings/server-settings/settings/query#query_views_log" }
      ],
      children: []
    },
    {
      label: "query_cache.max_*",
      count: 4,
      settings: [
        { name: "query_cache.max_entries", href: "/zh/reference/settings/server-settings/settings/query-cache-max#query_cache.max_entries" },
        { name: "query_cache.max_entry_size_in_bytes", href: "/zh/reference/settings/server-settings/settings/query-cache-max#query_cache.max_entry_size_in_bytes" },
        { name: "query_cache.max_entry_size_in_rows", href: "/zh/reference/settings/server-settings/settings/query-cache-max#query_cache.max_entry_size_in_rows" },
        { name: "query_cache.max_size_in_bytes", href: "/zh/reference/settings/server-settings/settings/query-cache-max#query_cache.max_size_in_bytes" }
      ],
      children: []
    },
    {
      label: "query_condition_*",
      count: 3,
      settings: [
        { name: "query_condition_cache_policy", href: "/zh/reference/settings/server-settings/settings/query-condition#query_condition_cache_policy" },
        { name: "query_condition_cache_size", href: "/zh/reference/settings/server-settings/settings/query-condition#query_condition_cache_size" },
        { name: "query_condition_cache_size_ratio", href: "/zh/reference/settings/server-settings/settings/query-condition#query_condition_cache_size_ratio" }
      ],
      children: []
    },
    {
      label: "remote_*",
      count: 2,
      settings: [
        { name: "remote_servers", href: "/zh/reference/settings/server-settings/settings/remote#remote_servers" },
        { name: "remote_url_allow_hosts", href: "/zh/reference/settings/server-settings/settings/remote#remote_url_allow_hosts" }
      ],
      children: []
    },
    {
      label: "replicated_fetches_*",
      count: 3,
      settings: [
        { name: "replicated_fetches_http_connection_timeout", href: "/zh/reference/settings/server-settings/settings/replicated-fetches#replicated_fetches_http_connection_timeout" },
        { name: "replicated_fetches_http_receive_timeout", href: "/zh/reference/settings/server-settings/settings/replicated-fetches#replicated_fetches_http_receive_timeout" },
        { name: "replicated_fetches_http_send_timeout", href: "/zh/reference/settings/server-settings/settings/replicated-fetches#replicated_fetches_http_send_timeout" }
      ],
      children: []
    },
    {
      label: "s3_*",
      count: 5,
      settings: [
        { name: "s3_allow_server_credentials_for_system_table_disks", href: "/zh/reference/settings/server-settings/settings/s3#s3_allow_server_credentials_for_system_table_disks" },
        { name: "s3_credentials_provider_max_cache_size", href: "/zh/reference/settings/server-settings/settings/s3#s3_credentials_provider_max_cache_size" },
        { name: "s3_load_table_anonymously_if_credentials_restricted", href: "/zh/reference/settings/server-settings/settings/s3#s3_load_table_anonymously_if_credentials_restricted" },
        { name: "s3_max_redirects", href: "/zh/reference/settings/server-settings/settings/s3#s3_max_redirects" },
        { name: "s3_retry_attempts", href: "/zh/reference/settings/server-settings/settings/s3#s3_retry_attempts" }
      ],
      children: []
    },
    {
      label: "s3queue_*",
      count: 2,
      settings: [
        { name: "s3queue_disable_streaming", href: "/zh/reference/settings/server-settings/settings/s3queue#s3queue_disable_streaming" },
        { name: "s3queue_log", href: "/zh/reference/settings/server-settings/settings/s3queue#s3queue_log" }
      ],
      children: []
    },
    {
      label: "show_*",
      count: 2,
      settings: [
        { name: "show_addresses_in_stack_traces", href: "/zh/reference/settings/server-settings/settings/show#show_addresses_in_stack_traces" },
        { name: "show_license_expiration_warnings", href: "/zh/reference/settings/server-settings/settings/show#show_license_expiration_warnings" }
      ],
      children: []
    },
    {
      label: "shutdown_wait_*",
      count: 3,
      settings: [
        { name: "shutdown_wait_backups_and_restores", href: "/zh/reference/settings/server-settings/settings/shutdown-wait#shutdown_wait_backups_and_restores" },
        { name: "shutdown_wait_unfinished", href: "/zh/reference/settings/server-settings/settings/shutdown-wait#shutdown_wait_unfinished" },
        { name: "shutdown_wait_unfinished_queries", href: "/zh/reference/settings/server-settings/settings/shutdown-wait#shutdown_wait_unfinished_queries" }
      ],
      children: []
    },
    {
      label: "skip_*",
      count: 2,
      settings: [
        { name: "skip_binary_checksum_checks", href: "/zh/reference/settings/server-settings/settings/skip#skip_binary_checksum_checks" },
        { name: "skip_check_for_incorrect_settings", href: "/zh/reference/settings/server-settings/settings/skip#skip_check_for_incorrect_settings" }
      ],
      children: []
    },
    {
      label: "snapshot_cleaner_*",
      count: 2,
      settings: [
        { name: "snapshot_cleaner_period", href: "/zh/reference/settings/server-settings/settings/snapshot-cleaner#snapshot_cleaner_period" },
        { name: "snapshot_cleaner_pool_size", href: "/zh/reference/settings/server-settings/settings/snapshot-cleaner#snapshot_cleaner_pool_size" }
      ],
      children: []
    },
    {
      label: "startup_*",
      count: 2,
      settings: [
        { name: "startup_mv_delay_ms", href: "/zh/reference/settings/server-settings/settings/startup#startup_mv_delay_ms" },
        { name: "startup_scripts.throw_on_error", href: "/zh/reference/settings/server-settings/settings/startup#startup_scripts.throw_on_error" }
      ],
      children: []
    },
    {
      label: "storage_*",
      count: 3,
      settings: [
        { name: "storage_configuration", href: "/zh/reference/settings/server-settings/settings/storage#storage_configuration" },
        { name: "storage_metadata_write_full_object_key", href: "/zh/reference/settings/server-settings/settings/storage#storage_metadata_write_full_object_key" },
        { name: "storage_shared_set_join_use_inner_uuid", href: "/zh/reference/settings/server-settings/settings/storage#storage_shared_set_join_use_inner_uuid" }
      ],
      children: []
    },
    {
      label: "storage_connections_*",
      count: 6,
      settings: [
        { name: "storage_connections_hard_limit", href: "/zh/reference/settings/server-settings/settings/storage-connections#storage_connections_hard_limit" },
        { name: "storage_connections_rcvbuf", href: "/zh/reference/settings/server-settings/settings/storage-connections#storage_connections_rcvbuf" },
        { name: "storage_connections_sndbuf", href: "/zh/reference/settings/server-settings/settings/storage-connections#storage_connections_sndbuf" },
        { name: "storage_connections_soft_limit", href: "/zh/reference/settings/server-settings/settings/storage-connections#storage_connections_soft_limit" },
        { name: "storage_connections_store_limit", href: "/zh/reference/settings/server-settings/settings/storage-connections#storage_connections_store_limit" },
        { name: "storage_connections_warn_limit", href: "/zh/reference/settings/server-settings/settings/storage-connections#storage_connections_warn_limit" }
      ],
      children: []
    },
    {
      label: "tables_loader_*",
      count: 2,
      settings: [
        { name: "tables_loader_background_pool_size", href: "/zh/reference/settings/server-settings/settings/tables-loader#tables_loader_background_pool_size" },
        { name: "tables_loader_foreground_pool_size", href: "/zh/reference/settings/server-settings/settings/tables-loader#tables_loader_foreground_pool_size" }
      ],
      children: []
    },
    {
      label: "tcp_close_*",
      count: 2,
      settings: [
        { name: "tcp_close_connection_after_queries_num", href: "/zh/reference/settings/server-settings/settings/tcp-close#tcp_close_connection_after_queries_num" },
        { name: "tcp_close_connection_after_queries_seconds", href: "/zh/reference/settings/server-settings/settings/tcp-close#tcp_close_connection_after_queries_seconds" }
      ],
      children: []
    },
    {
      label: "tcp_port_*",
      count: 2,
      settings: [
        { name: "tcp_port", href: "/zh/reference/settings/server-settings/settings/tcp-port#tcp_port" },
        { name: "tcp_port_secure", href: "/zh/reference/settings/server-settings/settings/tcp-port#tcp_port_secure" }
      ],
      children: []
    },
    {
      label: "temporary_data_*",
      count: 2,
      settings: [
        { name: "temporary_data_in_cache", href: "/zh/reference/settings/server-settings/settings/temporary-data#temporary_data_in_cache" },
        { name: "temporary_data_in_distributed_cache", href: "/zh/reference/settings/server-settings/settings/temporary-data#temporary_data_in_distributed_cache" }
      ],
      children: []
    },
    {
      label: "text_index_*",
      count: 12,
      settings: [
        { name: "text_index_header_cache_max_entries", href: "/zh/reference/settings/server-settings/settings/text-index#text_index_header_cache_max_entries" },
        { name: "text_index_header_cache_policy", href: "/zh/reference/settings/server-settings/settings/text-index#text_index_header_cache_policy" },
        { name: "text_index_header_cache_size", href: "/zh/reference/settings/server-settings/settings/text-index#text_index_header_cache_size" },
        { name: "text_index_header_cache_size_ratio", href: "/zh/reference/settings/server-settings/settings/text-index#text_index_header_cache_size_ratio" },
        { name: "text_index_postings_cache_max_entries", href: "/zh/reference/settings/server-settings/settings/text-index#text_index_postings_cache_max_entries" },
        { name: "text_index_postings_cache_policy", href: "/zh/reference/settings/server-settings/settings/text-index#text_index_postings_cache_policy" },
        { name: "text_index_postings_cache_size", href: "/zh/reference/settings/server-settings/settings/text-index#text_index_postings_cache_size" },
        { name: "text_index_postings_cache_size_ratio", href: "/zh/reference/settings/server-settings/settings/text-index#text_index_postings_cache_size_ratio" },
        { name: "text_index_tokens_cache_max_entries", href: "/zh/reference/settings/server-settings/settings/text-index#text_index_tokens_cache_max_entries" },
        { name: "text_index_tokens_cache_policy", href: "/zh/reference/settings/server-settings/settings/text-index#text_index_tokens_cache_policy" },
        { name: "text_index_tokens_cache_size", href: "/zh/reference/settings/server-settings/settings/text-index#text_index_tokens_cache_size" },
        { name: "text_index_tokens_cache_size_ratio", href: "/zh/reference/settings/server-settings/settings/text-index#text_index_tokens_cache_size_ratio" }
      ],
      children: []
    },
    {
      label: "threadpool_local_*",
      count: 2,
      settings: [
        { name: "threadpool_local_fs_reader_pool_size", href: "/zh/reference/settings/server-settings/settings/threadpool-local#threadpool_local_fs_reader_pool_size" },
        { name: "threadpool_local_fs_reader_queue_size", href: "/zh/reference/settings/server-settings/settings/threadpool-local#threadpool_local_fs_reader_queue_size" }
      ],
      children: []
    },
    {
      label: "threadpool_remote_*",
      count: 2,
      settings: [
        { name: "threadpool_remote_fs_reader_pool_size", href: "/zh/reference/settings/server-settings/settings/threadpool-remote#threadpool_remote_fs_reader_pool_size" },
        { name: "threadpool_remote_fs_reader_queue_size", href: "/zh/reference/settings/server-settings/settings/threadpool-remote#threadpool_remote_fs_reader_queue_size" }
      ],
      children: []
    },
    {
      label: "threadpool_writer_*",
      count: 2,
      settings: [
        { name: "threadpool_writer_pool_size", href: "/zh/reference/settings/server-settings/settings/threadpool-writer#threadpool_writer_pool_size" },
        { name: "threadpool_writer_queue_size", href: "/zh/reference/settings/server-settings/settings/threadpool-writer#threadpool_writer_queue_size" }
      ],
      children: []
    },
    {
      label: "tmp_*",
      count: 2,
      settings: [
        { name: "tmp_path", href: "/zh/reference/settings/server-settings/settings/tmp#tmp_path" },
        { name: "tmp_policy", href: "/zh/reference/settings/server-settings/settings/tmp#tmp_policy" }
      ],
      children: []
    },
    {
      label: "top_level_*",
      count: 2,
      settings: [
        { name: "top_level_domains_list", href: "/zh/reference/settings/server-settings/settings/top-level#top_level_domains_list" },
        { name: "top_level_domains_path", href: "/zh/reference/settings/server-settings/settings/top-level#top_level_domains_path" }
      ],
      children: []
    },
    {
      label: "total_memory_*",
      count: 4,
      settings: [
        { name: "total_memory_profiler_sample_max_allocation_size", href: "/zh/reference/settings/server-settings/settings/total-memory#total_memory_profiler_sample_max_allocation_size" },
        { name: "total_memory_profiler_sample_min_allocation_size", href: "/zh/reference/settings/server-settings/settings/total-memory#total_memory_profiler_sample_min_allocation_size" },
        { name: "total_memory_profiler_step", href: "/zh/reference/settings/server-settings/settings/total-memory#total_memory_profiler_step" },
        { name: "total_memory_tracker_sample_probability", href: "/zh/reference/settings/server-settings/settings/total-memory#total_memory_tracker_sample_probability" }
      ],
      children: []
    },
    {
      label: "uncompressed_cache_*",
      count: 3,
      settings: [
        { name: "uncompressed_cache_policy", href: "/zh/reference/settings/server-settings/settings/uncompressed-cache#uncompressed_cache_policy" },
        { name: "uncompressed_cache_size", href: "/zh/reference/settings/server-settings/settings/uncompressed-cache#uncompressed_cache_size" },
        { name: "uncompressed_cache_size_ratio", href: "/zh/reference/settings/server-settings/settings/uncompressed-cache#uncompressed_cache_size_ratio" }
      ],
      children: []
    },
    {
      label: "unique_key_*",
      count: 6,
      settings: [
        { name: "unique_key_bitmap_cache_policy", href: "/zh/reference/settings/server-settings/settings/unique-key#unique_key_bitmap_cache_policy" },
        { name: "unique_key_bitmap_cache_size_bytes", href: "/zh/reference/settings/server-settings/settings/unique-key#unique_key_bitmap_cache_size_bytes" },
        { name: "unique_key_bitmap_cache_size_ratio", href: "/zh/reference/settings/server-settings/settings/unique-key#unique_key_bitmap_cache_size_ratio" },
        { name: "unique_key_index_cache_policy", href: "/zh/reference/settings/server-settings/settings/unique-key#unique_key_index_cache_policy" },
        { name: "unique_key_index_cache_size_bytes", href: "/zh/reference/settings/server-settings/settings/unique-key#unique_key_index_cache_size_bytes" },
        { name: "unique_key_index_cache_size_ratio", href: "/zh/reference/settings/server-settings/settings/unique-key#unique_key_index_cache_size_ratio" }
      ],
      children: []
    },
    {
      label: "use_*",
      count: 3,
      settings: [
        { name: "use_minimalistic_part_header_in_zookeeper", href: "/zh/reference/settings/server-settings/settings/use#use_minimalistic_part_header_in_zookeeper" },
        { name: "use_separate_cache_arena", href: "/zh/reference/settings/server-settings/settings/use#use_separate_cache_arena" },
        { name: "use_shared_merge_tree_log_pipeline", href: "/zh/reference/settings/server-settings/settings/use#use_shared_merge_tree_log_pipeline" }
      ],
      children: []
    },
    {
      label: "user_*",
      count: 4,
      settings: [
        { name: "user_directories", href: "/zh/reference/settings/server-settings/settings/user#user_directories" },
        { name: "user_files_path", href: "/zh/reference/settings/server-settings/settings/user#user_files_path" },
        { name: "user_profile_events_per_cpu", href: "/zh/reference/settings/server-settings/settings/user#user_profile_events_per_cpu" },
        { name: "user_scripts_path", href: "/zh/reference/settings/server-settings/settings/user#user_scripts_path" }
      ],
      children: []
    },
    {
      label: "user_defined_*",
      count: 2,
      settings: [
        { name: "user_defined_executable_functions_config", href: "/zh/reference/settings/server-settings/settings/user-defined#user_defined_executable_functions_config" },
        { name: "user_defined_path", href: "/zh/reference/settings/server-settings/settings/user-defined#user_defined_path" }
      ],
      children: []
    },
    {
      label: "users_*",
      count: 2,
      settings: [
        { name: "users_config", href: "/zh/reference/settings/server-settings/settings/users#users_config" },
        { name: "users_to_ignore_early_memory_limit_check", href: "/zh/reference/settings/server-settings/settings/users#users_to_ignore_early_memory_limit_check" }
      ],
      children: []
    },
    {
      label: "vector_similarity_*",
      count: 4,
      settings: [
        { name: "vector_similarity_index_cache_max_entries", href: "/zh/reference/settings/server-settings/settings/vector-similarity#vector_similarity_index_cache_max_entries" },
        { name: "vector_similarity_index_cache_policy", href: "/zh/reference/settings/server-settings/settings/vector-similarity#vector_similarity_index_cache_policy" },
        { name: "vector_similarity_index_cache_size", href: "/zh/reference/settings/server-settings/settings/vector-similarity#vector_similarity_index_cache_size" },
        { name: "vector_similarity_index_cache_size_ratio", href: "/zh/reference/settings/server-settings/settings/vector-similarity#vector_similarity_index_cache_size_ratio" }
      ],
      children: []
    },
    {
      label: "workload_*",
      count: 2,
      settings: [
        { name: "workload_path", href: "/zh/reference/settings/server-settings/settings/workload#workload_path" },
        { name: "workload_zookeeper_path", href: "/zh/reference/settings/server-settings/settings/workload#workload_zookeeper_path" }
      ],
      children: []
    },
    {
      label: "zookeeper_*",
      count: 2,
      settings: [
        { name: "zookeeper", href: "/zh/reference/settings/server-settings/settings/zookeeper#zookeeper" },
        { name: "zookeeper_log", href: "/zh/reference/settings/server-settings/settings/zookeeper#zookeeper_log" }
      ],
      children: []
    },
    {
      label: "Other",
      count: 118,
      settings: [
        { name: "abort_on_logical_error", href: "/zh/reference/settings/server-settings/settings/other#abort_on_logical_error" },
        { name: "allowed_disks_for_table_engines", href: "/zh/reference/settings/server-settings/settings/other#allowed_disks_for_table_engines" },
        { name: "auth_use_forwarded_address", href: "/zh/reference/settings/server-settings/settings/other#auth_use_forwarded_address" },
        { name: "bcrypt_workfactor", href: "/zh/reference/settings/server-settings/settings/other#bcrypt_workfactor" },
        { name: "blob_storage_log", href: "/zh/reference/settings/server-settings/settings/other#blob_storage_log" },
        { name: "builtin_dictionaries_reload_interval", href: "/zh/reference/settings/server-settings/settings/other#builtin_dictionaries_reload_interval" },
        { name: "cache_size_to_ram_max_ratio", href: "/zh/reference/settings/server-settings/settings/other#cache_size_to_ram_max_ratio" },
        { name: "cannot_allocate_thread_fault_injection_probability", href: "/zh/reference/settings/server-settings/settings/other#cannot_allocate_thread_fault_injection_probability" },
        { name: "cgroups_memory_usage_observer_wait_time", href: "/zh/reference/settings/server-settings/settings/other#cgroups_memory_usage_observer_wait_time" },
        { name: "compression", href: "/zh/reference/settings/server-settings/settings/other#compression" },
        { name: "config_reload_interval_ms", href: "/zh/reference/settings/server-settings/settings/other#config_reload_interval_ms" },
        { name: "config-file", href: "/zh/reference/settings/server-settings/settings/other#config-file" },
        { name: "core_dump", href: "/zh/reference/settings/server-settings/settings/other#core_dump" },
        { name: "crash_log", href: "/zh/reference/settings/server-settings/settings/other#crash_log" },
        { name: "database_atomic_delay_before_drop_table_sec", href: "/zh/reference/settings/server-settings/settings/other#database_atomic_delay_before_drop_table_sec" },
        { name: "dead_letter_queue", href: "/zh/reference/settings/server-settings/settings/other#dead_letter_queue" },
        { name: "dictionary_background_reconnect_interval", href: "/zh/reference/settings/server-settings/settings/other#dictionary_background_reconnect_interval" },
        { name: "disk_transaction_wait_for_blob_removal", href: "/zh/reference/settings/server-settings/settings/other#disk_transaction_wait_for_blob_removal" },
        { name: "display_secrets_in_show_and_select", href: "/zh/reference/settings/server-settings/settings/other#display_secrets_in_show_and_select" },
        { name: "dns_max_consecutive_failures", href: "/zh/reference/settings/server-settings/settings/other#dns_max_consecutive_failures" },
        { name: "dynamic_user_defined_executable_functions_path", href: "/zh/reference/settings/server-settings/settings/other#dynamic_user_defined_executable_functions_path" },
        { name: "encryption", href: "/zh/reference/settings/server-settings/settings/other#encryption" },
        { name: "enforce_keeper_component_tracking", href: "/zh/reference/settings/server-settings/settings/other#enforce_keeper_component_tracking" },
        { name: "error_log", href: "/zh/reference/settings/server-settings/settings/other#error_log" },
        { name: "filesystem_caches_path", href: "/zh/reference/settings/server-settings/settings/other#filesystem_caches_path" },
        { name: "google_protos_path", href: "/zh/reference/settings/server-settings/settings/other#google_protos_path" },
        { name: "handshake_timeout_milliseconds", href: "/zh/reference/settings/server-settings/settings/other#handshake_timeout_milliseconds" },
        { name: "hdfs.libhdfs3_conf", href: "/zh/reference/settings/server-settings/settings/other#hdfs.libhdfs3_conf" },
        { name: "hsts_max_age", href: "/zh/reference/settings/server-settings/settings/other#hsts_max_age" },
        { name: "iceberg_background_schedule_pool_size", href: "/zh/reference/settings/server-settings/settings/other#iceberg_background_schedule_pool_size" },
        { name: "ignore_empty_sql_security_in_create_view_query", href: "/zh/reference/settings/server-settings/settings/other#ignore_empty_sql_security_in_create_view_query" },
        { name: "include_from", href: "/zh/reference/settings/server-settings/settings/other#include_from" },
        { name: "insert_deduplication_version", href: "/zh/reference/settings/server-settings/settings/other#insert_deduplication_version" },
        { name: "io_thread_pool_queue_size", href: "/zh/reference/settings/server-settings/settings/other#io_thread_pool_queue_size" },
        { name: "keep_alive_timeout", href: "/zh/reference/settings/server-settings/settings/other#keep_alive_timeout" },
        { name: "ldap_servers", href: "/zh/reference/settings/server-settings/settings/other#ldap_servers" },
        { name: "logger", href: "/zh/reference/settings/server-settings/settings/other#logger" },
        { name: "logger.count", href: "/zh/reference/settings/server-settings/settings/other#logger.count" },
        { name: "logger.errorlog", href: "/zh/reference/settings/server-settings/settings/other#logger.errorlog" },
        { name: "logger.formatting.type", href: "/zh/reference/settings/server-settings/settings/other#logger.formatting.type" },
        { name: "logger.level", href: "/zh/reference/settings/server-settings/settings/other#logger.level" },
        { name: "logger.log", href: "/zh/reference/settings/server-settings/settings/other#logger.log" },
        { name: "logger.rotation", href: "/zh/reference/settings/server-settings/settings/other#logger.rotation" },
        { name: "logger.shutdown_level", href: "/zh/reference/settings/server-settings/settings/other#logger.shutdown_level" },
        { name: "logger.size", href: "/zh/reference/settings/server-settings/settings/other#logger.size" },
        { name: "logger.startup_level", href: "/zh/reference/settings/server-settings/settings/other#logger.startup_level" },
        { name: "logger.stream_compress", href: "/zh/reference/settings/server-settings/settings/other#logger.stream_compress" },
        { name: "logger.syslog_level", href: "/zh/reference/settings/server-settings/settings/other#logger.syslog_level" },
        { name: "logger.use_syslog", href: "/zh/reference/settings/server-settings/settings/other#logger.use_syslog" },
        { name: "macros", href: "/zh/reference/settings/server-settings/settings/other#macros" },
        { name: "message_queue_disable_insertion", href: "/zh/reference/settings/server-settings/settings/other#message_queue_disable_insertion" },
        { name: "metric_log", href: "/zh/reference/settings/server-settings/settings/other#metric_log" },
        { name: "mmap_cache_size", href: "/zh/reference/settings/server-settings/settings/other#mmap_cache_size" },
        { name: "mutation_workload", href: "/zh/reference/settings/server-settings/settings/other#mutation_workload" },
        { name: "oom_score", href: "/zh/reference/settings/server-settings/settings/other#oom_score" },
        { name: "openSSL", href: "/zh/reference/settings/server-settings/settings/other#openssl" },
        { name: "openSSL.client.caConfig", href: "/zh/reference/settings/server-settings/settings/other#openssl.client.caconfig" },
        { name: "openSSL.client.cacheSessions", href: "/zh/reference/settings/server-settings/settings/other#openssl.client.cachesessions" },
        { name: "openSSL.client.certificateFile", href: "/zh/reference/settings/server-settings/settings/other#openssl.client.certificatefile" },
        { name: "openSSL.client.cipherList", href: "/zh/reference/settings/server-settings/settings/other#openssl.client.cipherlist" },
        { name: "openSSL.client.disableProtocols", href: "/zh/reference/settings/server-settings/settings/other#openssl.client.disableprotocols" },
        { name: "openSSL.client.extendedVerification", href: "/zh/reference/settings/server-settings/settings/other#openssl.client.extendedverification" },
        { name: "openSSL.client.fips", href: "/zh/reference/settings/server-settings/settings/other#openssl.client.fips" },
        { name: "openSSL.client.invalidCertificateHandler.name", href: "/zh/reference/settings/server-settings/settings/other#openssl.client.invalidcertificatehandler.name" },
        { name: "openSSL.client.loadDefaultCAFile", href: "/zh/reference/settings/server-settings/settings/other#openssl.client.loaddefaultcafile" },
        { name: "openSSL.client.preferServerCiphers", href: "/zh/reference/settings/server-settings/settings/other#openssl.client.preferserverciphers" },
        { name: "openSSL.client.privateKeyFile", href: "/zh/reference/settings/server-settings/settings/other#openssl.client.privatekeyfile" },
        { name: "openSSL.client.privateKeyPassphraseHandler.name", href: "/zh/reference/settings/server-settings/settings/other#openssl.client.privatekeypassphrasehandler.name" },
        { name: "openSSL.client.verificationDepth", href: "/zh/reference/settings/server-settings/settings/other#openssl.client.verificationdepth" },
        { name: "openSSL.client.verificationMode", href: "/zh/reference/settings/server-settings/settings/other#openssl.client.verificationmode" },
        { name: "openSSL.server.caConfig", href: "/zh/reference/settings/server-settings/settings/other#openssl.server.caconfig" },
        { name: "openSSL.server.cacheSessions", href: "/zh/reference/settings/server-settings/settings/other#openssl.server.cachesessions" },
        { name: "openSSL.server.certificateFile", href: "/zh/reference/settings/server-settings/settings/other#openssl.server.certificatefile" },
        { name: "openSSL.server.cipherList", href: "/zh/reference/settings/server-settings/settings/other#openssl.server.cipherlist" },
        { name: "openSSL.server.disableProtocols", href: "/zh/reference/settings/server-settings/settings/other#openssl.server.disableprotocols" },
        { name: "openSSL.server.extendedVerification", href: "/zh/reference/settings/server-settings/settings/other#openssl.server.extendedverification" },
        { name: "openSSL.server.fips", href: "/zh/reference/settings/server-settings/settings/other#openssl.server.fips" },
        { name: "openSSL.server.invalidCertificateHandler.name", href: "/zh/reference/settings/server-settings/settings/other#openssl.server.invalidcertificatehandler.name" },
        { name: "openSSL.server.loadDefaultCAFile", href: "/zh/reference/settings/server-settings/settings/other#openssl.server.loaddefaultcafile" },
        { name: "openSSL.server.preferServerCiphers", href: "/zh/reference/settings/server-settings/settings/other#openssl.server.preferserverciphers" },
        { name: "openSSL.server.privateKeyFile", href: "/zh/reference/settings/server-settings/settings/other#openssl.server.privatekeyfile" },
        { name: "openSSL.server.privateKeyPassphraseHandler.name", href: "/zh/reference/settings/server-settings/settings/other#openssl.server.privatekeypassphrasehandler.name" },
        { name: "openSSL.server.sessionCacheSize", href: "/zh/reference/settings/server-settings/settings/other#openssl.server.sessioncachesize" },
        { name: "openSSL.server.sessionIdContext", href: "/zh/reference/settings/server-settings/settings/other#openssl.server.sessionidcontext" },
        { name: "openSSL.server.sessionTimeout", href: "/zh/reference/settings/server-settings/settings/other#openssl.server.sessiontimeout" },
        { name: "openSSL.server.verificationDepth", href: "/zh/reference/settings/server-settings/settings/other#openssl.server.verificationdepth" },
        { name: "openSSL.server.verificationMode", href: "/zh/reference/settings/server-settings/settings/other#openssl.server.verificationmode" },
        { name: "opentelemetry_span_log", href: "/zh/reference/settings/server-settings/settings/other#opentelemetry_span_log" },
        { name: "part_log", href: "/zh/reference/settings/server-settings/settings/other#part_log" },
        { name: "path", href: "/zh/reference/settings/server-settings/settings/other#path" },
        { name: "per_cpu_untracked_memory_thread_buffer", href: "/zh/reference/settings/server-settings/settings/other#per_cpu_untracked_memory_thread_buffer" },
        { name: "point_in_polygon_cache_size", href: "/zh/reference/settings/server-settings/settings/other#point_in_polygon_cache_size" },
        { name: "prefixes_deserialization_thread_pool_thread_pool_queue_size", href: "/zh/reference/settings/server-settings/settings/other#prefixes_deserialization_thread_pool_thread_pool_queue_size" },
        { name: "prepare_system_log_tables_on_startup", href: "/zh/reference/settings/server-settings/settings/other#prepare_system_log_tables_on_startup" },
        { name: "process_query_plan_packet", href: "/zh/reference/settings/server-settings/settings/other#process_query_plan_packet" },
        { name: "processors_profile_log", href: "/zh/reference/settings/server-settings/settings/other#processors_profile_log" },
        { name: "prometheus", href: "/zh/reference/settings/server-settings/settings/other#prometheus" },
        { name: "prometheus.keeper_metrics_only", href: "/zh/reference/settings/server-settings/settings/other#prometheus.keeper_metrics_only" },
        { name: "proxy", href: "/zh/reference/settings/server-settings/settings/other#proxy" },
        { name: "remap_executable", href: "/zh/reference/settings/server-settings/settings/other#remap_executable" },
        { name: "replica_group_name", href: "/zh/reference/settings/server-settings/settings/other#replica_group_name" },
        { name: "replicated_merge_tree", href: "/zh/reference/settings/server-settings/settings/other#replicated_merge_tree" },
        { name: "restore_threads", href: "/zh/reference/settings/server-settings/settings/other#restore_threads" },
        { name: "send_crash_reports", href: "/zh/reference/settings/server-settings/settings/other#send_crash_reports" },
        { name: "series_keeper_path", href: "/zh/reference/settings/server-settings/settings/other#series_keeper_path" },
        { name: "ssh_server", href: "/zh/reference/settings/server-settings/settings/other#ssh_server" },
        { name: "table_engines_require_grant", href: "/zh/reference/settings/server-settings/settings/other#table_engines_require_grant" },
        { name: "tcp_ssh_port", href: "/zh/reference/settings/server-settings/settings/other#tcp_ssh_port" },
        { name: "text_log", href: "/zh/reference/settings/server-settings/settings/other#text_log" },
        { name: "thread_pool_queue_size", href: "/zh/reference/settings/server-settings/settings/other#thread_pool_queue_size" },
        { name: "throw_on_unknown_workload", href: "/zh/reference/settings/server-settings/settings/other#throw_on_unknown_workload" },
        { name: "timezone", href: "/zh/reference/settings/server-settings/settings/other#timezone" },
        { name: "trace_log", href: "/zh/reference/settings/server-settings/settings/other#trace_log" },
        { name: "url_scheme_mappers", href: "/zh/reference/settings/server-settings/settings/other#url_scheme_mappers" },
        { name: "validate_tcp_client_information", href: "/zh/reference/settings/server-settings/settings/other#validate_tcp_client_information" },
        { name: "wait_dictionaries_load_at_startup", href: "/zh/reference/settings/server-settings/settings/other#wait_dictionaries_load_at_startup" },
        { name: "webassembly_udf_engine", href: "/zh/reference/settings/server-settings/settings/other#webassembly_udf_engine" },
        { name: "webterminal_allowed_origins", href: "/zh/reference/settings/server-settings/settings/other#webterminal_allowed_origins" }
      ],
      children: []
    }
  ])
  const [anchorRoutes] = useState(() => ({
    abort_on_logical_error: "/reference/settings/server-settings/settings/other",
    access_control_improvements: "/reference/settings/server-settings/settings/access-control",
    access_control_path: "/reference/settings/server-settings/settings/access-control",
    aggregate_function_group_array_action_when_limit_is_reached: "/reference/settings/server-settings/settings/aggregate-function",
    aggregate_function_group_array_max_element_size: "/reference/settings/server-settings/settings/aggregate-function",
    allow_experimental_executable_udf_drivers: "/reference/settings/server-settings/settings/allow-experimental",
    allow_experimental_webassembly_udf: "/reference/settings/server-settings/settings/allow-experimental",
    allow_experimental_webterminal: "/reference/settings/server-settings/settings/allow-experimental",
    allow_feature_tier: "/reference/settings/server-settings/settings/allow",
    allow_impersonate_user: "/reference/settings/server-settings/settings/allow",
    allow_implicit_no_password: "/reference/settings/server-settings/settings/allow",
    allow_no_password: "/reference/settings/server-settings/settings/allow",
    allow_plaintext_password: "/reference/settings/server-settings/settings/allow",
    allow_use_jemalloc_memory: "/reference/settings/server-settings/settings/allow",
    allowed_disks_for_table_engines: "/reference/settings/server-settings/settings/other",
    async_insert_queue_flush_on_shutdown: "/reference/settings/server-settings/settings/async-insert",
    async_insert_threads: "/reference/settings/server-settings/settings/async-insert",
    async_load_databases: "/reference/settings/server-settings/settings/async-load",
    async_load_system_database: "/reference/settings/server-settings/settings/async-load",
    asynchronous_heavy_metrics_update_period_s: "/reference/settings/server-settings/settings/asynchronous",
    asynchronous_insert_log: "/reference/settings/server-settings/settings/asynchronous",
    asynchronous_metric_log: "/reference/settings/server-settings/settings/asynchronous",
    asynchronous_metrics_enable_heavy_metrics: "/reference/settings/server-settings/settings/asynchronous-metrics",
    asynchronous_metrics_keeper_metrics_only: "/reference/settings/server-settings/settings/asynchronous-metrics",
    asynchronous_metrics_update_period_s: "/reference/settings/server-settings/settings/asynchronous-metrics",
    auth_use_forwarded_address: "/reference/settings/server-settings/settings/other",
    background_buffer_flush_schedule_pool_size: "/reference/settings/server-settings/settings/background",
    background_common_pool_size: "/reference/settings/server-settings/settings/background",
    background_distributed_schedule_pool_size: "/reference/settings/server-settings/settings/background",
    background_fetches_pool_size: "/reference/settings/server-settings/settings/background",
    background_merges_mutations_concurrency_ratio: "/reference/settings/server-settings/settings/background-merges",
    background_merges_mutations_scheduling_policy: "/reference/settings/server-settings/settings/background-merges",
    background_message_broker_schedule_pool_size: "/reference/settings/server-settings/settings/background",
    background_move_pool_size: "/reference/settings/server-settings/settings/background",
    background_pool_size: "/reference/settings/server-settings/settings/background",
    background_schedule_pool_initial_size: "/reference/settings/server-settings/settings/background-schedule",
    background_schedule_pool_log: "/reference/settings/server-settings/settings/background-schedule",
    background_schedule_pool_max_parallel_tasks_per_type_ratio: "/reference/settings/server-settings/settings/background-schedule",
    background_schedule_pool_size: "/reference/settings/server-settings/settings/background-schedule",
    background_streaming_schedule_pool_size: "/reference/settings/server-settings/settings/background",
    backup_log: "/reference/settings/server-settings/settings/backup",
    backup_threads: "/reference/settings/server-settings/settings/backup",
    backups: "/reference/settings/server-settings/settings/backups",
    backups_io_thread_pool_queue_size: "/reference/settings/server-settings/settings/backups",
    bcrypt_workfactor: "/reference/settings/server-settings/settings/other",
    blob_storage_log: "/reference/settings/server-settings/settings/other",
    builtin_dictionaries_reload_interval: "/reference/settings/server-settings/settings/other",
    cache_size_to_ram_max_ratio: "/reference/settings/server-settings/settings/other",
    cannot_allocate_thread_fault_injection_probability: "/reference/settings/server-settings/settings/other",
    cgroups_memory_usage_observer_wait_time: "/reference/settings/server-settings/settings/other",
    compiled_expression_cache_elements_size: "/reference/settings/server-settings/settings/compiled-expression",
    compiled_expression_cache_size: "/reference/settings/server-settings/settings/compiled-expression",
    compression: "/reference/settings/server-settings/settings/other",
    concurrent_threads_lazy_allocation: "/reference/settings/server-settings/settings/concurrent-threads",
    concurrent_threads_scheduler: "/reference/settings/server-settings/settings/concurrent-threads",
    concurrent_threads_soft_limit_num: "/reference/settings/server-settings/settings/concurrent-threads",
    concurrent_threads_soft_limit_ratio_to_cores: "/reference/settings/server-settings/settings/concurrent-threads",
    "config-file": "/reference/settings/server-settings/settings/other",
    config_reload_interval_ms: "/reference/settings/server-settings/settings/other",
    "configuration-of-disks": "/reference/settings/server-settings/settings/storage",
    "configuration-of-policies": "/reference/settings/server-settings/settings/storage",
    core_dump: "/reference/settings/server-settings/settings/other",
    cpu_slot_preemption: "/reference/settings/server-settings/settings/cpu-slot",
    cpu_slot_preemption_timeout_ms: "/reference/settings/server-settings/settings/cpu-slot",
    cpu_slot_quantum_ns: "/reference/settings/server-settings/settings/cpu-slot",
    crash_log: "/reference/settings/server-settings/settings/other",
    custom_cached_disks_base_directory: "/reference/settings/server-settings/settings/custom",
    custom_settings_prefixes: "/reference/settings/server-settings/settings/custom",
    database_atomic_delay_before_drop_table_sec: "/reference/settings/server-settings/settings/other",
    database_catalog_drop_error_cooldown_sec: "/reference/settings/server-settings/settings/database-catalog",
    database_catalog_drop_table_concurrency: "/reference/settings/server-settings/settings/database-catalog",
    database_catalog_unused_dir_cleanup_period_sec: "/reference/settings/server-settings/settings/database-catalog",
    database_catalog_unused_dir_hide_timeout_sec: "/reference/settings/server-settings/settings/database-catalog",
    database_catalog_unused_dir_rm_timeout_sec: "/reference/settings/server-settings/settings/database-catalog",
    database_replicated_allow_detach_permanently: "/reference/settings/server-settings/settings/database-replicated",
    database_replicated_drop_broken_tables: "/reference/settings/server-settings/settings/database-replicated",
    dead_letter_queue: "/reference/settings/server-settings/settings/other",
    default_database: "/reference/settings/server-settings/settings/default",
    default_password_type: "/reference/settings/server-settings/settings/default",
    default_profile: "/reference/settings/server-settings/settings/default",
    default_replica_name: "/reference/settings/server-settings/settings/default-replica",
    default_replica_path: "/reference/settings/server-settings/settings/default-replica",
    default_session_timeout: "/reference/settings/server-settings/settings/default",
    dictionaries_config: "/reference/settings/server-settings/settings/dictionaries",
    dictionaries_lazy_load: "/reference/settings/server-settings/settings/dictionaries",
    dictionaries_lib_path: "/reference/settings/server-settings/settings/dictionaries",
    dictionary_background_reconnect_interval: "/reference/settings/server-settings/settings/other",
    disable_insertion_and_mutation: "/reference/settings/server-settings/settings/disable",
    disable_internal_dns_cache: "/reference/settings/server-settings/settings/disable",
    disable_tunneling_for_https_requests_over_http_proxy: "/reference/settings/server-settings/settings/disable",
    disk_connections_hard_limit: "/reference/settings/server-settings/settings/disk-connections",
    disk_connections_rcvbuf: "/reference/settings/server-settings/settings/disk-connections",
    disk_connections_sndbuf: "/reference/settings/server-settings/settings/disk-connections",
    disk_connections_soft_limit: "/reference/settings/server-settings/settings/disk-connections",
    disk_connections_store_limit: "/reference/settings/server-settings/settings/disk-connections",
    disk_connections_warn_limit: "/reference/settings/server-settings/settings/disk-connections",
    disk_transaction_wait_for_blob_removal: "/reference/settings/server-settings/settings/other",
    display_secrets_in_show_and_select: "/reference/settings/server-settings/settings/other",
    distributed_cache_apply_throttling_settings_from_client: "/reference/settings/server-settings/settings/distributed-cache",
    distributed_cache_keep_up_free_connections_ratio: "/reference/settings/server-settings/settings/distributed-cache",
    distributed_cache_write_pool_size: "/reference/settings/server-settings/settings/distributed-cache",
    distributed_ddl: "/reference/settings/server-settings/settings/distributed-ddl",
    "distributed_ddl.cleanup_delay_period": "/reference/settings/server-settings/settings/distributed",
    "distributed_ddl.max_tasks_in_queue": "/reference/settings/server-settings/settings/distributed",
    "distributed_ddl.path": "/reference/settings/server-settings/settings/distributed",
    "distributed_ddl.pool_size": "/reference/settings/server-settings/settings/distributed",
    "distributed_ddl.profile": "/reference/settings/server-settings/settings/distributed",
    "distributed_ddl.replicas_path": "/reference/settings/server-settings/settings/distributed",
    "distributed_ddl.task_max_lifetime": "/reference/settings/server-settings/settings/distributed",
    distributed_ddl_use_initial_user_and_roles: "/reference/settings/server-settings/settings/distributed-ddl",
    dns_allow_resolve_names_to_ipv4: "/reference/settings/server-settings/settings/dns-allow",
    dns_allow_resolve_names_to_ipv6: "/reference/settings/server-settings/settings/dns-allow",
    dns_cache_max_entries: "/reference/settings/server-settings/settings/dns-cache",
    dns_cache_update_period: "/reference/settings/server-settings/settings/dns-cache",
    dns_max_consecutive_failures: "/reference/settings/server-settings/settings/other",
    drop_distributed_cache_pool_size: "/reference/settings/server-settings/settings/drop-distributed",
    drop_distributed_cache_queue_size: "/reference/settings/server-settings/settings/drop-distributed",
    dynamic_user_defined_executable_functions_path: "/reference/settings/server-settings/settings/other",
    enable_azure_sdk_logging: "/reference/settings/server-settings/settings/enable",
    enable_webterminal: "/reference/settings/server-settings/settings/enable",
    encryption: "/reference/settings/server-settings/settings/other",
    encryption_header_cache_policy: "/reference/settings/server-settings/settings/encryption-header",
    encryption_header_cache_size: "/reference/settings/server-settings/settings/encryption-header",
    encryption_header_cache_size_ratio: "/reference/settings/server-settings/settings/encryption-header",
    enforce_keeper_component_tracking: "/reference/settings/server-settings/settings/other",
    error_log: "/reference/settings/server-settings/settings/other",
    filesystem_caches_path: "/reference/settings/server-settings/settings/other",
    format_parsing_thread_pool_queue_size: "/reference/settings/server-settings/settings/format",
    format_schema_path: "/reference/settings/server-settings/settings/format",
    global_profiler_cpu_time_period_ns: "/reference/settings/server-settings/settings/global-profiler",
    global_profiler_real_time_period_ns: "/reference/settings/server-settings/settings/global-profiler",
    google_protos_path: "/reference/settings/server-settings/settings/other",
    graphite: "/reference/settings/server-settings/settings/graphite",
    graphite_rollup: "/reference/settings/server-settings/settings/graphite",
    handshake_timeout_milliseconds: "/reference/settings/server-settings/settings/other",
    "hdfs.libhdfs3_conf": "/reference/settings/server-settings/settings/other",
    hsts_max_age: "/reference/settings/server-settings/settings/other",
    http_connections_hard_limit: "/reference/settings/server-settings/settings/http-connections",
    http_connections_rcvbuf: "/reference/settings/server-settings/settings/http-connections",
    http_connections_sndbuf: "/reference/settings/server-settings/settings/http-connections",
    http_connections_soft_limit: "/reference/settings/server-settings/settings/http-connections",
    http_connections_store_limit: "/reference/settings/server-settings/settings/http-connections",
    http_connections_warn_limit: "/reference/settings/server-settings/settings/http-connections",
    http_handlers: "/reference/settings/server-settings/settings/http",
    http_options_response: "/reference/settings/server-settings/settings/http",
    http_server_default_response: "/reference/settings/server-settings/settings/http",
    iceberg_background_schedule_pool_size: "/reference/settings/server-settings/settings/other",
    iceberg_catalog_threadpool_pool_size: "/reference/settings/server-settings/settings/iceberg-catalog",
    iceberg_catalog_threadpool_queue_size: "/reference/settings/server-settings/settings/iceberg-catalog",
    iceberg_compaction_threadpool_pool_size: "/reference/settings/server-settings/settings/iceberg-compaction",
    iceberg_compaction_threadpool_queue_size: "/reference/settings/server-settings/settings/iceberg-compaction",
    iceberg_metadata_files_cache_max_entries: "/reference/settings/server-settings/settings/iceberg-metadata",
    iceberg_metadata_files_cache_policy: "/reference/settings/server-settings/settings/iceberg-metadata",
    iceberg_metadata_files_cache_size: "/reference/settings/server-settings/settings/iceberg-metadata",
    iceberg_metadata_files_cache_size_ratio: "/reference/settings/server-settings/settings/iceberg-metadata",
    iceberg_scheduler_compaction_threadpool_pool_size: "/reference/settings/server-settings/settings/iceberg-scheduler",
    iceberg_scheduler_compaction_threadpool_queue_size: "/reference/settings/server-settings/settings/iceberg-scheduler",
    ignore_empty_sql_security_in_create_view_query: "/reference/settings/server-settings/settings/other",
    include_from: "/reference/settings/server-settings/settings/other",
    index_mark_cache_policy: "/reference/settings/server-settings/settings/index-mark",
    index_mark_cache_prewarm_ratio: "/reference/settings/server-settings/settings/index-mark",
    index_mark_cache_size: "/reference/settings/server-settings/settings/index-mark",
    index_mark_cache_size_ratio: "/reference/settings/server-settings/settings/index-mark",
    index_uncompressed_cache_policy: "/reference/settings/server-settings/settings/index-uncompressed",
    index_uncompressed_cache_size: "/reference/settings/server-settings/settings/index-uncompressed",
    index_uncompressed_cache_size_ratio: "/reference/settings/server-settings/settings/index-uncompressed",
    insert_deduplication_version: "/reference/settings/server-settings/settings/other",
    interserver_http_credentials: "/reference/settings/server-settings/settings/interserver-http",
    interserver_http_host: "/reference/settings/server-settings/settings/interserver-http",
    interserver_http_port: "/reference/settings/server-settings/settings/interserver-http",
    interserver_https_host: "/reference/settings/server-settings/settings/interserver-https",
    interserver_https_port: "/reference/settings/server-settings/settings/interserver-https",
    interserver_listen_host: "/reference/settings/server-settings/settings/interserver",
    interserver_tables_status_require_auth: "/reference/settings/server-settings/settings/interserver",
    io_thread_pool_queue_size: "/reference/settings/server-settings/settings/other",
    jemalloc_collect_global_profile_samples_in_trace_log: "/reference/settings/server-settings/settings/jemalloc",
    jemalloc_enable_background_threads: "/reference/settings/server-settings/settings/jemalloc-enable",
    jemalloc_enable_global_profiler: "/reference/settings/server-settings/settings/jemalloc-enable",
    jemalloc_flush_profile_interval_bytes: "/reference/settings/server-settings/settings/jemalloc-flush",
    jemalloc_flush_profile_on_memory_exceeded: "/reference/settings/server-settings/settings/jemalloc-flush",
    jemalloc_flush_profile_on_memory_exceeded_interval: "/reference/settings/server-settings/settings/jemalloc-flush",
    jemalloc_max_background_threads_num: "/reference/settings/server-settings/settings/jemalloc",
    jemalloc_profiler_sampling_rate: "/reference/settings/server-settings/settings/jemalloc",
    keep_alive_timeout: "/reference/settings/server-settings/settings/other",
    keeper_hosts: "/reference/settings/server-settings/settings/keeper",
    keeper_multiread_batch_size: "/reference/settings/server-settings/settings/keeper",
    "keeper_server.socket_receive_timeout_sec": "/reference/settings/server-settings/settings/keeper-server-socket",
    "keeper_server.socket_send_timeout_sec": "/reference/settings/server-settings/settings/keeper-server-socket",
    ldap_servers: "/reference/settings/server-settings/settings/other",
    license_file: "/reference/settings/server-settings/settings/license",
    license_public_key_for_testing: "/reference/settings/server-settings/settings/license",
    listen_backlog: "/reference/settings/server-settings/settings/listen",
    listen_host: "/reference/settings/server-settings/settings/listen",
    listen_reuse_port: "/reference/settings/server-settings/settings/listen",
    listen_try: "/reference/settings/server-settings/settings/listen",
    load_marks_threadpool_pool_size: "/reference/settings/server-settings/settings/load-marks",
    load_marks_threadpool_queue_size: "/reference/settings/server-settings/settings/load-marks",
    logger: "/reference/settings/server-settings/settings/other",
    "logger.async": "/reference/settings/server-settings/settings/logger-async",
    "logger.async_queye_max_size": "/reference/settings/server-settings/settings/logger-async",
    "logger.console": "/reference/settings/server-settings/settings/logger-console",
    "logger.console_log_level": "/reference/settings/server-settings/settings/logger-console",
    "logger.count": "/reference/settings/server-settings/settings/other",
    "logger.errorlog": "/reference/settings/server-settings/settings/other",
    "logger.formatting.type": "/reference/settings/server-settings/settings/other",
    "logger.level": "/reference/settings/server-settings/settings/other",
    "logger.log": "/reference/settings/server-settings/settings/other",
    "logger.rotation": "/reference/settings/server-settings/settings/other",
    "logger.shutdown_level": "/reference/settings/server-settings/settings/other",
    "logger.size": "/reference/settings/server-settings/settings/other",
    "logger.startup_level": "/reference/settings/server-settings/settings/other",
    "logger.stream_compress": "/reference/settings/server-settings/settings/other",
    "logger.syslog_level": "/reference/settings/server-settings/settings/other",
    "logger.use_syslog": "/reference/settings/server-settings/settings/other",
    macros: "/reference/settings/server-settings/settings/other",
    mark_cache_policy: "/reference/settings/server-settings/settings/mark-cache",
    mark_cache_prewarm_ratio: "/reference/settings/server-settings/settings/mark-cache",
    mark_cache_size: "/reference/settings/server-settings/settings/mark-cache",
    mark_cache_size_ratio: "/reference/settings/server-settings/settings/mark-cache",
    max_active_parts_loading_thread_pool_size: "/reference/settings/server-settings/settings/max",
    max_authentication_methods_per_user: "/reference/settings/server-settings/settings/max",
    max_backup_bandwidth_for_server: "/reference/settings/server-settings/settings/max",
    max_backups_io_thread_pool_free_size: "/reference/settings/server-settings/settings/max-backups",
    max_backups_io_thread_pool_size: "/reference/settings/server-settings/settings/max-backups",
    max_build_vector_similarity_index_thread_pool_size: "/reference/settings/server-settings/settings/max",
    max_concurrent_insert_queries: "/reference/settings/server-settings/settings/max-concurrent",
    max_concurrent_queries: "/reference/settings/server-settings/settings/max-concurrent",
    max_concurrent_select_queries: "/reference/settings/server-settings/settings/max-concurrent",
    max_connections: "/reference/settings/server-settings/settings/max",
    max_database_num_to_throw: "/reference/settings/server-settings/settings/max-database",
    max_database_num_to_warn: "/reference/settings/server-settings/settings/max-database",
    max_database_replicated_create_table_thread_pool_size: "/reference/settings/server-settings/settings/max-database",
    max_dictionary_num_to_throw: "/reference/settings/server-settings/settings/max-dictionary",
    max_dictionary_num_to_warn: "/reference/settings/server-settings/settings/max-dictionary",
    max_distributed_cache_read_bandwidth_for_server: "/reference/settings/server-settings/settings/max-distributed",
    max_distributed_cache_write_bandwidth_for_server: "/reference/settings/server-settings/settings/max-distributed",
    max_entries_for_hash_table_stats: "/reference/settings/server-settings/settings/max",
    max_fetch_partition_thread_pool_size: "/reference/settings/server-settings/settings/max",
    max_format_parsing_thread_pool_free_size: "/reference/settings/server-settings/settings/max-format",
    max_format_parsing_thread_pool_size: "/reference/settings/server-settings/settings/max-format",
    max_held_snapshots: "/reference/settings/server-settings/settings/max",
    max_http_index_page_size: "/reference/settings/server-settings/settings/max",
    max_io_thread_pool_free_size: "/reference/settings/server-settings/settings/max-io",
    max_io_thread_pool_size: "/reference/settings/server-settings/settings/max-io",
    max_keep_alive_requests: "/reference/settings/server-settings/settings/max",
    max_local_read_bandwidth_for_server: "/reference/settings/server-settings/settings/max-local",
    max_local_write_bandwidth_for_server: "/reference/settings/server-settings/settings/max-local",
    max_materialized_views_count_for_table: "/reference/settings/server-settings/settings/max",
    max_merges_bandwidth_for_server: "/reference/settings/server-settings/settings/max",
    max_mutations_bandwidth_for_server: "/reference/settings/server-settings/settings/max",
    max_named_collection_num_to_throw: "/reference/settings/server-settings/settings/max-named",
    max_named_collection_num_to_warn: "/reference/settings/server-settings/settings/max-named",
    max_open_files: "/reference/settings/server-settings/settings/max",
    max_os_cpu_wait_time_ratio_to_drop_connection: "/reference/settings/server-settings/settings/max",
    max_outdated_parts_loading_thread_pool_size: "/reference/settings/server-settings/settings/max",
    max_part_num_to_warn: "/reference/settings/server-settings/settings/max",
    max_partition_size_to_drop: "/reference/settings/server-settings/settings/max",
    max_parts_cleaning_thread_pool_size: "/reference/settings/server-settings/settings/max",
    max_pending_mutations_execution_time_to_warn: "/reference/settings/server-settings/settings/max-pending",
    max_pending_mutations_to_warn: "/reference/settings/server-settings/settings/max-pending",
    max_per_cpu_untracked_memory: "/reference/settings/server-settings/settings/max",
    max_prefixes_deserialization_thread_pool_free_size: "/reference/settings/server-settings/settings/max-prefixes",
    max_prefixes_deserialization_thread_pool_size: "/reference/settings/server-settings/settings/max-prefixes",
    max_remote_read_connections: "/reference/settings/server-settings/settings/max-remote",
    max_remote_read_network_bandwidth_for_server: "/reference/settings/server-settings/settings/max-remote",
    max_remote_write_network_bandwidth_for_server: "/reference/settings/server-settings/settings/max-remote",
    max_replicated_fetches_network_bandwidth_for_server: "/reference/settings/server-settings/settings/max-replicated",
    max_replicated_sends_network_bandwidth_for_server: "/reference/settings/server-settings/settings/max-replicated",
    max_replicated_table_num_to_throw: "/reference/settings/server-settings/settings/max-replicated",
    max_server_memory_usage: "/reference/settings/server-settings/settings/max-server-memory-usage",
    max_server_memory_usage_to_ram_ratio: "/reference/settings/server-settings/settings/max-server-memory-usage",
    max_session_timeout: "/reference/settings/server-settings/settings/max",
    max_snapshot_commit_thread_pool_free_size: "/reference/settings/server-settings/settings/max-snapshot",
    max_snapshot_commit_thread_pool_size: "/reference/settings/server-settings/settings/max-snapshot",
    max_table_num_to_throw: "/reference/settings/server-settings/settings/max-table",
    max_table_num_to_warn: "/reference/settings/server-settings/settings/max-table",
    max_table_size_to_drop: "/reference/settings/server-settings/settings/max-table",
    max_temporary_data_on_disk_size: "/reference/settings/server-settings/settings/max",
    max_thread_pool_free_size: "/reference/settings/server-settings/settings/max-thread",
    max_thread_pool_size: "/reference/settings/server-settings/settings/max-thread",
    max_unexpected_parts_loading_thread_pool_size: "/reference/settings/server-settings/settings/max",
    max_view_num_to_throw: "/reference/settings/server-settings/settings/max-view",
    max_view_num_to_warn: "/reference/settings/server-settings/settings/max-view",
    max_waiting_queries: "/reference/settings/server-settings/settings/max",
    max_zookeeper_pooled_connections: "/reference/settings/server-settings/settings/max",
    memory_worker_correct_memory_tracker: "/reference/settings/server-settings/settings/memory-worker",
    memory_worker_decay_adjustment_period_ms: "/reference/settings/server-settings/settings/memory-worker",
    memory_worker_dynamic_hard_limit: "/reference/settings/server-settings/settings/memory-worker",
    memory_worker_period_ms: "/reference/settings/server-settings/settings/memory-worker",
    memory_worker_purge_dirty_pages_threshold_ratio: "/reference/settings/server-settings/settings/memory-worker",
    memory_worker_purge_total_memory_threshold_ratio: "/reference/settings/server-settings/settings/memory-worker",
    memory_worker_rss_speculative_reserve_ratio: "/reference/settings/server-settings/settings/memory-worker",
    memory_worker_use_cgroup: "/reference/settings/server-settings/settings/memory-worker",
    merge_tree: "/reference/settings/server-settings/settings/merge",
    merge_workload: "/reference/settings/server-settings/settings/merge",
    merges_mutations_memory_usage_soft_limit: "/reference/settings/server-settings/settings/merges-mutations",
    merges_mutations_memory_usage_to_ram_ratio: "/reference/settings/server-settings/settings/merges-mutations",
    message_queue_disable_insertion: "/reference/settings/server-settings/settings/other",
    metric_log: "/reference/settings/server-settings/settings/other",
    min_allocation_size_to_throw_on_memory_limit: "/reference/settings/server-settings/settings/min",
    min_os_cpu_wait_time_ratio_to_drop_connection: "/reference/settings/server-settings/settings/min",
    mlock_executable: "/reference/settings/server-settings/settings/mlock-executable",
    mlock_executable_min_total_memory_amount_bytes: "/reference/settings/server-settings/settings/mlock-executable",
    mmap_cache_size: "/reference/settings/server-settings/settings/other",
    mutation_workload: "/reference/settings/server-settings/settings/other",
    mysql_port: "/reference/settings/server-settings/settings/mysql",
    mysql_require_secure_transport: "/reference/settings/server-settings/settings/mysql",
    oom_canary_enable: "/reference/settings/server-settings/settings/oom-canary",
    oom_canary_initial_backoff_seconds: "/reference/settings/server-settings/settings/oom-canary",
    oom_canary_max_backoff_seconds: "/reference/settings/server-settings/settings/oom-canary",
    oom_canary_max_rapid_relaunches: "/reference/settings/server-settings/settings/oom-canary",
    oom_canary_relaunch: "/reference/settings/server-settings/settings/oom-canary",
    oom_canary_size: "/reference/settings/server-settings/settings/oom-canary",
    oom_score: "/reference/settings/server-settings/settings/other",
    openssl: "/reference/settings/server-settings/settings/other",
    "openssl.client.cachesessions": "/reference/settings/server-settings/settings/other",
    "openssl.client.caconfig": "/reference/settings/server-settings/settings/other",
    "openssl.client.certificatefile": "/reference/settings/server-settings/settings/other",
    "openssl.client.cipherlist": "/reference/settings/server-settings/settings/other",
    "openssl.client.disableprotocols": "/reference/settings/server-settings/settings/other",
    "openssl.client.extendedverification": "/reference/settings/server-settings/settings/other",
    "openssl.client.fips": "/reference/settings/server-settings/settings/other",
    "openssl.client.invalidcertificatehandler.name": "/reference/settings/server-settings/settings/other",
    "openssl.client.loaddefaultcafile": "/reference/settings/server-settings/settings/other",
    "openssl.client.preferserverciphers": "/reference/settings/server-settings/settings/other",
    "openssl.client.privatekeyfile": "/reference/settings/server-settings/settings/other",
    "openssl.client.privatekeypassphrasehandler.name": "/reference/settings/server-settings/settings/other",
    "openssl.client.requiretlsv1": "/reference/settings/server-settings/settings/openssl-client-requiretlsv1",
    "openssl.client.requiretlsv1_1": "/reference/settings/server-settings/settings/openssl-client-requiretlsv1",
    "openssl.client.requiretlsv1_2": "/reference/settings/server-settings/settings/openssl-client-requiretlsv1",
    "openssl.client.verificationdepth": "/reference/settings/server-settings/settings/other",
    "openssl.client.verificationmode": "/reference/settings/server-settings/settings/other",
    "openssl.server.cachesessions": "/reference/settings/server-settings/settings/other",
    "openssl.server.caconfig": "/reference/settings/server-settings/settings/other",
    "openssl.server.certificatefile": "/reference/settings/server-settings/settings/other",
    "openssl.server.cipherlist": "/reference/settings/server-settings/settings/other",
    "openssl.server.disableprotocols": "/reference/settings/server-settings/settings/other",
    "openssl.server.extendedverification": "/reference/settings/server-settings/settings/other",
    "openssl.server.fips": "/reference/settings/server-settings/settings/other",
    "openssl.server.invalidcertificatehandler.name": "/reference/settings/server-settings/settings/other",
    "openssl.server.loaddefaultcafile": "/reference/settings/server-settings/settings/other",
    "openssl.server.preferserverciphers": "/reference/settings/server-settings/settings/other",
    "openssl.server.privatekeyfile": "/reference/settings/server-settings/settings/other",
    "openssl.server.privatekeypassphrasehandler.name": "/reference/settings/server-settings/settings/other",
    "openssl.server.requiretlsv1": "/reference/settings/server-settings/settings/openssl-server-requiretlsv1",
    "openssl.server.requiretlsv1_1": "/reference/settings/server-settings/settings/openssl-server-requiretlsv1",
    "openssl.server.requiretlsv1_2": "/reference/settings/server-settings/settings/openssl-server-requiretlsv1",
    "openssl.server.sessioncachesize": "/reference/settings/server-settings/settings/other",
    "openssl.server.sessionidcontext": "/reference/settings/server-settings/settings/other",
    "openssl.server.sessiontimeout": "/reference/settings/server-settings/settings/other",
    "openssl.server.verificationdepth": "/reference/settings/server-settings/settings/other",
    "openssl.server.verificationmode": "/reference/settings/server-settings/settings/other",
    opentelemetry_span_log: "/reference/settings/server-settings/settings/other",
    os_collect_psi_metrics: "/reference/settings/server-settings/settings/os",
    os_cpu_busy_time_threshold: "/reference/settings/server-settings/settings/os",
    os_threads_nice_value_distributed_cache_tcp_handler: "/reference/settings/server-settings/settings/os-threads",
    os_threads_nice_value_merge_mutate: "/reference/settings/server-settings/settings/os-threads",
    os_threads_nice_value_zookeeper_client_send_receive: "/reference/settings/server-settings/settings/os-threads",
    page_cache_free_memory_ratio: "/reference/settings/server-settings/settings/page-cache",
    page_cache_history_window_ms: "/reference/settings/server-settings/settings/page-cache",
    page_cache_max_size: "/reference/settings/server-settings/settings/page-cache",
    page_cache_min_size: "/reference/settings/server-settings/settings/page-cache",
    page_cache_policy: "/reference/settings/server-settings/settings/page-cache",
    page_cache_shards: "/reference/settings/server-settings/settings/page-cache",
    page_cache_size_ratio: "/reference/settings/server-settings/settings/page-cache",
    paimon_metadata_files_cache_max_entries: "/reference/settings/server-settings/settings/paimon-metadata",
    paimon_metadata_files_cache_policy: "/reference/settings/server-settings/settings/paimon-metadata",
    paimon_metadata_files_cache_size: "/reference/settings/server-settings/settings/paimon-metadata",
    paimon_metadata_files_cache_size_ratio: "/reference/settings/server-settings/settings/paimon-metadata",
    parquet_metadata_cache_max_entries: "/reference/settings/server-settings/settings/parquet-metadata",
    parquet_metadata_cache_policy: "/reference/settings/server-settings/settings/parquet-metadata",
    parquet_metadata_cache_size: "/reference/settings/server-settings/settings/parquet-metadata",
    parquet_metadata_cache_size_ratio: "/reference/settings/server-settings/settings/parquet-metadata",
    part_log: "/reference/settings/server-settings/settings/other",
    parts_kill_delay_period: "/reference/settings/server-settings/settings/parts-kill-delay-period",
    parts_kill_delay_period_random_add: "/reference/settings/server-settings/settings/parts-kill-delay-period",
    parts_killer_max_condemned_parts_per_batch: "/reference/settings/server-settings/settings/parts-killer",
    parts_killer_pool_size: "/reference/settings/server-settings/settings/parts-killer",
    path: "/reference/settings/server-settings/settings/other",
    per_cpu_untracked_memory_thread_buffer: "/reference/settings/server-settings/settings/other",
    point_in_polygon_cache_size: "/reference/settings/server-settings/settings/other",
    postgresql_port: "/reference/settings/server-settings/settings/postgresql",
    postgresql_require_secure_transport: "/reference/settings/server-settings/settings/postgresql",
    prefetch_threadpool_pool_size: "/reference/settings/server-settings/settings/prefetch-threadpool",
    prefetch_threadpool_queue_size: "/reference/settings/server-settings/settings/prefetch-threadpool",
    prefixes_deserialization_thread_pool_thread_pool_queue_size: "/reference/settings/server-settings/settings/other",
    prepare_system_log_tables_on_startup: "/reference/settings/server-settings/settings/other",
    primary_index_cache_policy: "/reference/settings/server-settings/settings/primary-index",
    primary_index_cache_prewarm_ratio: "/reference/settings/server-settings/settings/primary-index",
    primary_index_cache_size: "/reference/settings/server-settings/settings/primary-index",
    primary_index_cache_size_ratio: "/reference/settings/server-settings/settings/primary-index",
    process_query_plan_packet: "/reference/settings/server-settings/settings/other",
    processors_profile_log: "/reference/settings/server-settings/settings/other",
    prometheus: "/reference/settings/server-settings/settings/other",
    "prometheus.keeper_metrics_only": "/reference/settings/server-settings/settings/other",
    proxy: "/reference/settings/server-settings/settings/other",
    query_cache: "/reference/settings/server-settings/settings/query",
    "query_cache.max_entries": "/reference/settings/server-settings/settings/query-cache-max",
    "query_cache.max_entry_size_in_bytes": "/reference/settings/server-settings/settings/query-cache-max",
    "query_cache.max_entry_size_in_rows": "/reference/settings/server-settings/settings/query-cache-max",
    "query_cache.max_size_in_bytes": "/reference/settings/server-settings/settings/query-cache-max",
    query_condition_cache_policy: "/reference/settings/server-settings/settings/query-condition",
    query_condition_cache_size: "/reference/settings/server-settings/settings/query-condition",
    query_condition_cache_size_ratio: "/reference/settings/server-settings/settings/query-condition",
    query_log: "/reference/settings/server-settings/settings/query",
    query_masking_rules: "/reference/settings/server-settings/settings/query",
    query_metric_log: "/reference/settings/server-settings/settings/query",
    query_thread_log: "/reference/settings/server-settings/settings/query",
    query_views_log: "/reference/settings/server-settings/settings/query",
    remap_executable: "/reference/settings/server-settings/settings/other",
    remote_servers: "/reference/settings/server-settings/settings/remote",
    remote_url_allow_hosts: "/reference/settings/server-settings/settings/remote",
    replica_group_name: "/reference/settings/server-settings/settings/other",
    replicated_fetches_http_connection_timeout: "/reference/settings/server-settings/settings/replicated-fetches",
    replicated_fetches_http_receive_timeout: "/reference/settings/server-settings/settings/replicated-fetches",
    replicated_fetches_http_send_timeout: "/reference/settings/server-settings/settings/replicated-fetches",
    replicated_merge_tree: "/reference/settings/server-settings/settings/other",
    restore_threads: "/reference/settings/server-settings/settings/other",
    s3_allow_server_credentials_for_system_table_disks: "/reference/settings/server-settings/settings/s3",
    s3_credentials_provider_max_cache_size: "/reference/settings/server-settings/settings/s3",
    s3_load_table_anonymously_if_credentials_restricted: "/reference/settings/server-settings/settings/s3",
    s3_max_redirects: "/reference/settings/server-settings/settings/s3",
    s3_retry_attempts: "/reference/settings/server-settings/settings/s3",
    s3queue_disable_streaming: "/reference/settings/server-settings/settings/s3queue",
    s3queue_log: "/reference/settings/server-settings/settings/s3queue",
    send_crash_reports: "/reference/settings/server-settings/settings/other",
    series_keeper_path: "/reference/settings/server-settings/settings/other",
    show_addresses_in_stack_traces: "/reference/settings/server-settings/settings/show",
    show_license_expiration_warnings: "/reference/settings/server-settings/settings/show",
    shutdown_wait_backups_and_restores: "/reference/settings/server-settings/settings/shutdown-wait",
    shutdown_wait_unfinished: "/reference/settings/server-settings/settings/shutdown-wait",
    shutdown_wait_unfinished_queries: "/reference/settings/server-settings/settings/shutdown-wait",
    skip_binary_checksum_checks: "/reference/settings/server-settings/settings/skip",
    skip_check_for_incorrect_settings: "/reference/settings/server-settings/settings/skip",
    snapshot_cleaner_period: "/reference/settings/server-settings/settings/snapshot-cleaner",
    snapshot_cleaner_pool_size: "/reference/settings/server-settings/settings/snapshot-cleaner",
    ssh_server: "/reference/settings/server-settings/settings/other",
    startup_mv_delay_ms: "/reference/settings/server-settings/settings/startup",
    "startup_scripts.throw_on_error": "/reference/settings/server-settings/settings/startup",
    storage_configuration: "/reference/settings/server-settings/settings/storage",
    storage_connections_hard_limit: "/reference/settings/server-settings/settings/storage-connections",
    storage_connections_rcvbuf: "/reference/settings/server-settings/settings/storage-connections",
    storage_connections_sndbuf: "/reference/settings/server-settings/settings/storage-connections",
    storage_connections_soft_limit: "/reference/settings/server-settings/settings/storage-connections",
    storage_connections_store_limit: "/reference/settings/server-settings/settings/storage-connections",
    storage_connections_warn_limit: "/reference/settings/server-settings/settings/storage-connections",
    storage_metadata_write_full_object_key: "/reference/settings/server-settings/settings/storage",
    storage_shared_set_join_use_inner_uuid: "/reference/settings/server-settings/settings/storage",
    table_engines_require_grant: "/reference/settings/server-settings/settings/other",
    tables_loader_background_pool_size: "/reference/settings/server-settings/settings/tables-loader",
    tables_loader_foreground_pool_size: "/reference/settings/server-settings/settings/tables-loader",
    tcp_close_connection_after_queries_num: "/reference/settings/server-settings/settings/tcp-close",
    tcp_close_connection_after_queries_seconds: "/reference/settings/server-settings/settings/tcp-close",
    tcp_port: "/reference/settings/server-settings/settings/tcp-port",
    tcp_port_secure: "/reference/settings/server-settings/settings/tcp-port",
    tcp_ssh_port: "/reference/settings/server-settings/settings/other",
    temporary_data_in_cache: "/reference/settings/server-settings/settings/temporary-data",
    temporary_data_in_distributed_cache: "/reference/settings/server-settings/settings/temporary-data",
    text_index_header_cache_max_entries: "/reference/settings/server-settings/settings/text-index",
    text_index_header_cache_policy: "/reference/settings/server-settings/settings/text-index",
    text_index_header_cache_size: "/reference/settings/server-settings/settings/text-index",
    text_index_header_cache_size_ratio: "/reference/settings/server-settings/settings/text-index",
    text_index_postings_cache_max_entries: "/reference/settings/server-settings/settings/text-index",
    text_index_postings_cache_policy: "/reference/settings/server-settings/settings/text-index",
    text_index_postings_cache_size: "/reference/settings/server-settings/settings/text-index",
    text_index_postings_cache_size_ratio: "/reference/settings/server-settings/settings/text-index",
    text_index_tokens_cache_max_entries: "/reference/settings/server-settings/settings/text-index",
    text_index_tokens_cache_policy: "/reference/settings/server-settings/settings/text-index",
    text_index_tokens_cache_size: "/reference/settings/server-settings/settings/text-index",
    text_index_tokens_cache_size_ratio: "/reference/settings/server-settings/settings/text-index",
    text_log: "/reference/settings/server-settings/settings/other",
    thread_pool_queue_size: "/reference/settings/server-settings/settings/other",
    threadpool_local_fs_reader_pool_size: "/reference/settings/server-settings/settings/threadpool-local",
    threadpool_local_fs_reader_queue_size: "/reference/settings/server-settings/settings/threadpool-local",
    threadpool_remote_fs_reader_pool_size: "/reference/settings/server-settings/settings/threadpool-remote",
    threadpool_remote_fs_reader_queue_size: "/reference/settings/server-settings/settings/threadpool-remote",
    threadpool_writer_pool_size: "/reference/settings/server-settings/settings/threadpool-writer",
    threadpool_writer_queue_size: "/reference/settings/server-settings/settings/threadpool-writer",
    throw_on_unknown_workload: "/reference/settings/server-settings/settings/other",
    timezone: "/reference/settings/server-settings/settings/other",
    tmp_path: "/reference/settings/server-settings/settings/tmp",
    tmp_policy: "/reference/settings/server-settings/settings/tmp",
    top_level_domains_list: "/reference/settings/server-settings/settings/top-level",
    top_level_domains_path: "/reference/settings/server-settings/settings/top-level",
    total_memory_profiler_sample_max_allocation_size: "/reference/settings/server-settings/settings/total-memory",
    total_memory_profiler_sample_min_allocation_size: "/reference/settings/server-settings/settings/total-memory",
    total_memory_profiler_step: "/reference/settings/server-settings/settings/total-memory",
    total_memory_tracker_sample_probability: "/reference/settings/server-settings/settings/total-memory",
    trace_log: "/reference/settings/server-settings/settings/other",
    uncompressed_cache_policy: "/reference/settings/server-settings/settings/uncompressed-cache",
    uncompressed_cache_size: "/reference/settings/server-settings/settings/uncompressed-cache",
    uncompressed_cache_size_ratio: "/reference/settings/server-settings/settings/uncompressed-cache",
    unique_key_bitmap_cache_policy: "/reference/settings/server-settings/settings/unique-key",
    unique_key_bitmap_cache_size_bytes: "/reference/settings/server-settings/settings/unique-key",
    unique_key_bitmap_cache_size_ratio: "/reference/settings/server-settings/settings/unique-key",
    unique_key_index_cache_policy: "/reference/settings/server-settings/settings/unique-key",
    unique_key_index_cache_size_bytes: "/reference/settings/server-settings/settings/unique-key",
    unique_key_index_cache_size_ratio: "/reference/settings/server-settings/settings/unique-key",
    url_scheme_mappers: "/reference/settings/server-settings/settings/other",
    use_minimalistic_part_header_in_zookeeper: "/reference/settings/server-settings/settings/use",
    use_separate_cache_arena: "/reference/settings/server-settings/settings/use",
    use_shared_merge_tree_log_pipeline: "/reference/settings/server-settings/settings/use",
    user_defined_executable_functions_config: "/reference/settings/server-settings/settings/user-defined",
    user_defined_path: "/reference/settings/server-settings/settings/user-defined",
    user_directories: "/reference/settings/server-settings/settings/user",
    user_files_path: "/reference/settings/server-settings/settings/user",
    user_profile_events_per_cpu: "/reference/settings/server-settings/settings/user",
    user_scripts_path: "/reference/settings/server-settings/settings/user",
    users_config: "/reference/settings/server-settings/settings/users",
    users_to_ignore_early_memory_limit_check: "/reference/settings/server-settings/settings/users",
    validate_tcp_client_information: "/reference/settings/server-settings/settings/other",
    vector_similarity_index_cache_max_entries: "/reference/settings/server-settings/settings/vector-similarity",
    vector_similarity_index_cache_policy: "/reference/settings/server-settings/settings/vector-similarity",
    vector_similarity_index_cache_size: "/reference/settings/server-settings/settings/vector-similarity",
    vector_similarity_index_cache_size_ratio: "/reference/settings/server-settings/settings/vector-similarity",
    wait_dictionaries_load_at_startup: "/reference/settings/server-settings/settings/other",
    webassembly_udf_engine: "/reference/settings/server-settings/settings/other",
    webterminal_allowed_origins: "/reference/settings/server-settings/settings/other",
    workload_path: "/reference/settings/server-settings/settings/workload",
    workload_zookeeper_path: "/reference/settings/server-settings/settings/workload",
    zookeeper: "/reference/settings/server-settings/settings/zookeeper",
    zookeeper_log: "/reference/settings/server-settings/settings/zookeeper"
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

    const marker = "/reference/settings/server-settings/settings"
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
    const countLabel = `${entry.count} ${entry.count === 1 ? "个配置项" : "个配置项"}`

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
        aria-label="搜索配置项"
        type="search"
        value={searchTerm}
        onChange={(event) => setSearchTerm(event.target.value)}
        placeholder="搜索配置项，例如 parallel replicas 或 %materialized%"
        className="w-full rounded-lg border border-gray-300 bg-white px-3 py-2 text-sm text-gray-900 outline-none placeholder:text-gray-500 focus:border-yellow-400 focus:ring-2 focus:ring-yellow-400/20 dark:border-white/10 dark:bg-transparent dark:text-white dark:placeholder:text-gray-500"
      />
      {isSearching && (
        <div className="mt-2 text-right text-xs text-gray-500 dark:text-gray-400">
          <span>
            找到 {matchingCount} 个匹配的配置项
          </span>
        </div>
      )}
      <div className="mt-3 w-full overflow-x-auto rounded-xl border border-gray-200 bg-gray-50/50 px-4 py-3 font-mono text-sm leading-6 dark:border-white/10 dark:bg-transparent">
        <div className="min-w-max font-semibold">/server-settings</div>
        {filteredEntries.length > 0 ? (
          filteredEntries.map((entry, index) => renderGroup(entry, [], index === filteredEntries.length - 1))
        ) : (
          <div className="py-2 text-gray-500 dark:text-gray-400">未找到匹配的配置项</div>
        )}
      </div>
    </div>
  )
}

export default ServerSettingsExplorer;