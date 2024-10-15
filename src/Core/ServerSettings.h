#pragma once


#include <Core/BaseSettings.h>
#include <Core/Defines.h>
#include <Core/SettingsEnums.h>


namespace Poco::Util
{
class AbstractConfiguration;
}

namespace DB
{

// clang-format off
#define SERVER_SETTINGS(M, ALIAS) \
    M(Bool, show_addresses_in_stack_traces, true, "If it is set true will show addresses in stack traces", 0) \
    M(Bool, shutdown_wait_unfinished_queries, false, "If set true ClickHouse will wait for running queries finish before shutdown.", 0) \
    M(UInt64, shutdown_wait_unfinished, 5, "Delay in seconds to wait for unfinished queries", 0) \
    M(UInt64, max_thread_pool_size, 10000, "The maximum number of threads that could be allocated from the OS and used for query execution and background operations.", 0) \
    M(UInt64, max_thread_pool_free_size, 1000, "The maximum number of threads that will always stay in a global thread pool once allocated and remain idle in case of insufficient number of tasks.", 0) \
    M(UInt64, thread_pool_queue_size, 10000, "The maximum number of tasks that will be placed in a queue and wait for execution.", 0) \
    M(UInt64, max_io_thread_pool_size, 100, "The maximum number of threads that would be used for IO operations", 0) \
    M(UInt64, max_io_thread_pool_free_size, 0, "Max free size for IO thread pool.", 0) \
    M(UInt64, io_thread_pool_queue_size, 10000, "Queue size for IO thread pool.", 0) \
    M(UInt64, max_active_parts_loading_thread_pool_size, 64, "The number of threads to load active set of data parts (Active ones) at startup.", 0) \
    M(UInt64, max_outdated_parts_loading_thread_pool_size, 32, "The number of threads to load inactive set of data parts (Outdated ones) at startup.", 0) \
    M(UInt64, max_unexpected_parts_loading_thread_pool_size, 8, "The number of threads to load inactive set of data parts (Unexpected ones) at startup.", 0) \
    M(UInt64, max_parts_cleaning_thread_pool_size, 128, "The number of threads for concurrent removal of inactive data parts.", 0) \
    M(UInt64, max_mutations_bandwidth_for_server, 0, "The maximum read speed of all mutations on server in bytes per second. Zero means unlimited.", 0) \
    M(UInt64, max_merges_bandwidth_for_server, 0, "The maximum read speed of all merges on server in bytes per second. Zero means unlimited.", 0) \
    M(UInt64, max_replicated_fetches_network_bandwidth_for_server, 0, "The maximum speed of data exchange over the network in bytes per second for replicated fetches. Zero means unlimited.", 0) \
    M(UInt64, max_replicated_sends_network_bandwidth_for_server, 0, "The maximum speed of data exchange over the network in bytes per second for replicated sends. Zero means unlimited.", 0) \
    M(UInt64, max_remote_read_network_bandwidth_for_server, 0, "The maximum speed of data exchange over the network in bytes per second for read. Zero means unlimited.", 0) \
    M(UInt64, max_remote_write_network_bandwidth_for_server, 0, "The maximum speed of data exchange over the network in bytes per second for write. Zero means unlimited.", 0) \
    M(UInt64, max_local_read_bandwidth_for_server, 0, "The maximum speed of local reads in bytes per second. Zero means unlimited.", 0) \
    M(UInt64, max_local_write_bandwidth_for_server, 0, "The maximum speed of local writes in bytes per second. Zero means unlimited.", 0) \
    M(UInt64, max_backups_io_thread_pool_size, 1000, "The maximum number of threads that would be used for IO operations for BACKUP queries", 0) \
    M(UInt64, max_backups_io_thread_pool_free_size, 0, "Max free size for backups IO thread pool.", 0) \
    M(UInt64, backups_io_thread_pool_queue_size, 0, "Queue size for backups IO thread pool.", 0) \
    M(UInt64, backup_threads, 16, "The maximum number of threads to execute BACKUP requests.", 0) \
    M(UInt64, max_backup_bandwidth_for_server, 0, "The maximum read speed in bytes per second for all backups on server. Zero means unlimited.", 0) \
    M(UInt64, restore_threads, 16, "The maximum number of threads to execute RESTORE requests.", 0) \
    M(Bool, shutdown_wait_backups_and_restores, true, "If set to true ClickHouse will wait for running backups and restores to finish before shutdown.", 0) \
    M(Double, cannot_allocate_thread_fault_injection_probability, 0, "For testing purposes.", 0) \
    M(Int32, max_connections, 1024, "Max server connections.", 0) \
    M(UInt32, asynchronous_metrics_update_period_s, 1, "Period in seconds for updating asynchronous metrics.", 0) \
    M(UInt32, asynchronous_heavy_metrics_update_period_s, 120, "Period in seconds for updating heavy asynchronous metrics.", 0) \
    M(String, default_database, "default", "Default database name.", 0) \
    M(String, tmp_policy, "", "Policy for storage with temporary data.", 0) \
    M(UInt64, max_temporary_data_on_disk_size, 0, "The maximum amount of storage that could be used for external aggregation, joins or sorting.", 0) \
    M(String, temporary_data_in_cache, "", "Cache disk name for temporary data.", 0) \
    M(UInt64, aggregate_function_group_array_max_element_size, 0xFFFFFF, "Max array element size in bytes for groupArray function. This limit is checked at serialization and help to avoid large state size.", 0) \
    M(GroupArrayActionWhenLimitReached, aggregate_function_group_array_action_when_limit_is_reached, GroupArrayActionWhenLimitReached::THROW, "Action to execute when max array element size is exceeded in groupArray: `throw` exception, or `discard` extra values", 0) \
    M(UInt64, max_server_memory_usage, 0, "Maximum total memory usage of the server in bytes. Zero means unlimited.", 0) \
    M(Double, max_server_memory_usage_to_ram_ratio, 0.9, "Same as max_server_memory_usage but in to RAM ratio. Allows to lower max memory on low-memory systems.", 0) \
    M(UInt64, merges_mutations_memory_usage_soft_limit, 0, "Maximum total memory usage for merges and mutations in bytes. Zero means unlimited.", 0) \
    M(Double, merges_mutations_memory_usage_to_ram_ratio, 0.5, "Same as merges_mutations_memory_usage_soft_limit but in to RAM ratio. Allows to lower memory limit on low-memory systems.", 0) \
    M(Bool, allow_use_jemalloc_memory, true, "Allows to use jemalloc memory.", 0) \
    M(UInt64, cgroups_memory_usage_observer_wait_time, 15, "Polling interval in seconds to read the current memory usage from cgroups. Zero means disabled.", 0) \
    M(Double, cgroup_memory_watcher_hard_limit_ratio, 0.95, "Hard memory limit ratio for cgroup memory usage observer", 0) \
    M(Double, cgroup_memory_watcher_soft_limit_ratio, 0.9, "Soft memory limit ratio limit for cgroup memory usage observer", 0) \
    M(UInt64, async_insert_threads, 16, "Maximum number of threads to actually parse and insert data in background. Zero means asynchronous mode is disabled", 0) \
    M(Bool, async_insert_queue_flush_on_shutdown, true, "If true queue of asynchronous inserts is flushed on graceful shutdown", 0) \
    M(Bool, ignore_empty_sql_security_in_create_view_query, true, "If true, ClickHouse doesn't write defaults for empty SQL security statement in CREATE VIEW queries. This setting is only necessary for the migration period and will become obsolete in 24.4", 0)  \
    M(UInt64, max_build_vector_similarity_index_thread_pool_size, 16, "The maximum number of threads to use to build vector similarity indexes. 0 means all cores.", 0) \
    \
    /* Database Catalog */ \
    M(UInt64, database_atomic_delay_before_drop_table_sec, 8 * 60, "The delay during which a dropped table can be restored using the UNDROP statement. If DROP TABLE ran with a SYNC modifier, the setting is ignored.", 0) \
    M(UInt64, database_catalog_unused_dir_hide_timeout_sec, 60 * 60, "Parameter of a task that cleans up garbage from store/ directory. If some subdirectory is not used by clickhouse-server and this directory was not modified for last database_catalog_unused_dir_hide_timeout_sec seconds, the task will 'hide' this directory by removing all access rights. It also works for directories that clickhouse-server does not expect to see inside store/. Zero means 'immediately'.", 0) \
    M(UInt64, database_catalog_unused_dir_rm_timeout_sec, 30 * 24 * 60 * 60, "Parameter of a task that cleans up garbage from store/ directory. If some subdirectory is not used by clickhouse-server and it was previously 'hidden' (see database_catalog_unused_dir_hide_timeout_sec) and this directory was not modified for last database_catalog_unused_dir_rm_timeout_sec seconds, the task will remove this directory. It also works for directories that clickhouse-server does not expect to see inside store/. Zero means 'never'.", 0) \
    M(UInt64, database_catalog_unused_dir_cleanup_period_sec, 24 * 60 * 60, "Parameter of a task that cleans up garbage from store/ directory. Sets scheduling period of the task. Zero means 'never'.", 0) \
    M(UInt64, database_catalog_drop_error_cooldown_sec, 5, "In case if drop table failed, ClickHouse will wait for this timeout before retrying the operation.", 0) \
    M(UInt64, database_catalog_drop_table_concurrency, 16, "The size of the threadpool used for dropping tables.", 0) \
    \
    \
    M(UInt64, max_concurrent_queries, 0, "Maximum number of concurrently executed queries. Zero means unlimited.", 0) \
    M(UInt64, max_concurrent_insert_queries, 0, "Maximum number of concurrently INSERT queries. Zero means unlimited.", 0) \
    M(UInt64, max_concurrent_select_queries, 0, "Maximum number of concurrently SELECT queries. Zero means unlimited.", 0) \
    M(UInt64, max_waiting_queries, 0, "Maximum number of concurrently waiting queries blocked due to `async_load_databases`. Note that waiting queries are not considered by `max_concurrent_*queries*` limits. Zero means unlimited.", 0) \
    \
    M(Double, cache_size_to_ram_max_ratio, 0.5, "Set cache size to RAM max ratio. Allows to lower cache size on low-memory systems.", 0) \
    M(String, uncompressed_cache_policy, DEFAULT_UNCOMPRESSED_CACHE_POLICY, "Uncompressed cache policy name.", 0) \
    M(UInt64, uncompressed_cache_size, DEFAULT_UNCOMPRESSED_CACHE_MAX_SIZE, "Size of cache for uncompressed blocks. Zero means disabled.", 0) \
    M(Double, uncompressed_cache_size_ratio, DEFAULT_UNCOMPRESSED_CACHE_SIZE_RATIO, "The size of the protected queue in the uncompressed cache relative to the cache's total size.", 0) \
    M(String, mark_cache_policy, DEFAULT_MARK_CACHE_POLICY, "Mark cache policy name.", 0) \
    M(UInt64, mark_cache_size, DEFAULT_MARK_CACHE_MAX_SIZE, "Size of cache for marks (index of MergeTree family of tables).", 0) \
    M(Double, mark_cache_size_ratio, DEFAULT_MARK_CACHE_SIZE_RATIO, "The size of the protected queue in the mark cache relative to the cache's total size.", 0) \
    M(String, index_uncompressed_cache_policy, DEFAULT_INDEX_UNCOMPRESSED_CACHE_POLICY, "Secondary index uncompressed cache policy name.", 0) \
    M(UInt64, index_uncompressed_cache_size, DEFAULT_INDEX_UNCOMPRESSED_CACHE_MAX_SIZE, "Size of cache for uncompressed blocks of secondary indices. Zero means disabled.", 0) \
    M(Double, index_uncompressed_cache_size_ratio, DEFAULT_INDEX_UNCOMPRESSED_CACHE_SIZE_RATIO, "The size of the protected queue in the secondary index uncompressed cache relative to the cache's total size.", 0) \
    M(String, index_mark_cache_policy, DEFAULT_INDEX_MARK_CACHE_POLICY, "Secondary index mark cache policy name.", 0) \
    M(UInt64, index_mark_cache_size, DEFAULT_INDEX_MARK_CACHE_MAX_SIZE, "Size of cache for secondary index marks. Zero means disabled.", 0) \
    M(Double, index_mark_cache_size_ratio, DEFAULT_INDEX_MARK_CACHE_SIZE_RATIO, "The size of the protected queue in the secondary index mark cache relative to the cache's total size.", 0) \
    M(UInt64, page_cache_chunk_size, 2 << 20, "Bytes per chunk in userspace page cache. Rounded up to a multiple of page size (typically 4 KiB) or huge page size (typically 2 MiB, only if page_cache_use_thp is enabled).", 0) \
    M(UInt64, page_cache_mmap_size, 1 << 30, "Bytes per memory mapping in userspace page cache. Not important.", 0) \
    M(UInt64, page_cache_size, 0, "Amount of virtual memory to map for userspace page cache. If page_cache_use_madv_free is enabled, it's recommended to set this higher than the machine's RAM size. Use 0 to disable userspace page cache.", 0) \
    M(Bool, page_cache_use_madv_free, DBMS_DEFAULT_PAGE_CACHE_USE_MADV_FREE, "If true, the userspace page cache will allow the OS to automatically reclaim memory from the cache on memory pressure (using MADV_FREE).", 0) \
    M(Bool, page_cache_use_transparent_huge_pages, true, "Userspace will attempt to use transparent huge pages on Linux. This is best-effort.", 0) \
    M(UInt64, mmap_cache_size, DEFAULT_MMAP_CACHE_MAX_SIZE, "A cache for mmapped files.", 0) \
    M(UInt64, compiled_expression_cache_size, DEFAULT_COMPILED_EXPRESSION_CACHE_MAX_SIZE, "Byte size of compiled expressions cache.", 0) \
    M(UInt64, compiled_expression_cache_elements_size, DEFAULT_COMPILED_EXPRESSION_CACHE_MAX_ENTRIES, "Maximum entries in compiled expressions cache.", 0) \
    \
    M(Bool,   disable_internal_dns_cache, false, "Disable internal DNS caching at all.", 0) \
    M(UInt64, dns_cache_max_entries, 10000, "Internal DNS cache max entries.", 0) \
    M(Int32,  dns_cache_update_period, 15, "Internal DNS cache update period in seconds.", 0) \
    M(UInt32, dns_max_consecutive_failures, 10, "Max DNS resolve failures of a hostname before dropping the hostname from ClickHouse DNS cache.", 0) \
    M(Bool, dns_allow_resolve_names_to_ipv4, true, "Allows resolve names to ipv4 addresses.", 0) \
    M(Bool, dns_allow_resolve_names_to_ipv6, true, "Allows resolve names to ipv6 addresses.", 0) \
    \
    M(UInt64, max_table_size_to_drop, 50000000000lu, "If size of a table is greater than this value (in bytes) than table could not be dropped with any DROP query.", 0) \
    M(UInt64, max_partition_size_to_drop, 50000000000lu, "Same as max_table_size_to_drop, but for the partitions.", 0) \
    M(UInt64, max_table_num_to_warn, 5000lu, "If the number of tables is greater than this value, the server will create a warning that will displayed to user.", 0) \
    M(UInt64, max_view_num_to_warn, 10000lu, "If the number of views is greater than this value, the server will create a warning that will displayed to user.", 0) \
    M(UInt64, max_dictionary_num_to_warn, 1000lu, "If the number of dictionaries is greater than this value, the server will create a warning that will displayed to user.", 0) \
    M(UInt64, max_database_num_to_warn, 1000lu, "If the number of databases is greater than this value, the server will create a warning that will displayed to user.", 0) \
    M(UInt64, max_part_num_to_warn, 100000lu, "If the number of parts is greater than this value, the server will create a warning that will displayed to user.", 0) \
    M(UInt64, max_table_num_to_throw, 0lu, "If number of tables is greater than this value, server will throw an exception. 0 means no limitation. View, remote tables, dictionary, system tables are not counted. Only count table in Atomic/Ordinary/Replicated/Lazy database engine.", 0) \
    M(UInt64, max_database_num_to_throw, 0lu, "If number of databases is greater than this value, server will throw an exception. 0 means no limitation.", 0) \
    M(UInt64, max_authentication_methods_per_user, 100, "The maximum number of authentication methods a user can be created with or altered. Changing this setting does not affect existing users. Zero means unlimited", 0) \
    M(UInt64, concurrent_threads_soft_limit_num, 0, "Sets how many concurrent thread can be allocated before applying CPU pressure. Zero means unlimited.", 0) \
    M(UInt64, concurrent_threads_soft_limit_ratio_to_cores, 0, "Same as concurrent_threads_soft_limit_num, but with ratio to cores.", 0) \
    \
    M(UInt64, background_pool_size, 16, "The maximum number of threads what will be used for merging or mutating data parts for *MergeTree-engine tables in a background.", 0) \
    M(Float, background_merges_mutations_concurrency_ratio, 2, "The number of part mutation tasks that can be executed concurrently by each thread in background pool.", 0) \
    M(String, background_merges_mutations_scheduling_policy, "round_robin", "The policy on how to perform a scheduling for background merges and mutations. Possible values are: `round_robin` and `shortest_task_first`. ", 0) \
    M(UInt64, background_move_pool_size, 8, "The maximum number of threads that will be used for moving data parts to another disk or volume for *MergeTree-engine tables in a background.", 0) \
    M(UInt64, background_fetches_pool_size, 16, "The maximum number of threads that will be used for fetching data parts from another replica for *MergeTree-engine tables in a background.", 0) \
    M(UInt64, background_common_pool_size, 8, "The maximum number of threads that will be used for performing a variety of operations (mostly garbage collection) for *MergeTree-engine tables in a background.", 0) \
    M(UInt64, background_buffer_flush_schedule_pool_size, 16, "The maximum number of threads that will be used for performing flush operations for Buffer-engine tables in a background.", 0) \
    M(UInt64, background_schedule_pool_size, 512, "The maximum number of threads that will be used for constantly executing some lightweight periodic operations.", 0) \
    M(UInt64, background_message_broker_schedule_pool_size, 16, "The maximum number of threads that will be used for executing background operations for message streaming.", 0) \
    M(UInt64, background_distributed_schedule_pool_size, 16, "The maximum number of threads that will be used for executing distributed sends.", 0) \
    M(UInt64, tables_loader_foreground_pool_size, 0, "The maximum number of threads that will be used for foreground (that is being waited for by a query) loading of tables. Also used for synchronous loading of tables before the server start. Zero means use all CPUs.", 0) \
    M(UInt64, tables_loader_background_pool_size, 0, "The maximum number of threads that will be used for background async loading of tables. Zero means use all CPUs.", 0) \
    M(Bool, async_load_databases, false, "Enable asynchronous loading of databases and tables to speedup server startup. Queries to not yet loaded entity will be blocked until load is finished.", 0) \
    M(Bool, display_secrets_in_show_and_select, false, "Allow showing secrets in SHOW and SELECT queries via a format setting and a grant", 0) \
    M(Seconds, keep_alive_timeout, DEFAULT_HTTP_KEEP_ALIVE_TIMEOUT, "The number of seconds that ClickHouse waits for incoming requests before closing the connection.", 0) \
    M(UInt64, max_keep_alive_requests, 10000, "The maximum number of requests handled via a single http keepalive connection before the server closes this connection.", 0) \
    M(Seconds, replicated_fetches_http_connection_timeout, 0, "HTTP connection timeout for part fetch requests. Inherited from default profile `http_connection_timeout` if not set explicitly.", 0) \
    M(Seconds, replicated_fetches_http_send_timeout, 0, "HTTP send timeout for part fetch requests. Inherited from default profile `http_send_timeout` if not set explicitly.", 0) \
    M(Seconds, replicated_fetches_http_receive_timeout, 0, "HTTP receive timeout for fetch part requests. Inherited from default profile `http_receive_timeout` if not set explicitly.", 0) \
    M(UInt64, total_memory_profiler_step, 0, "Whenever server memory usage becomes larger than every next step in number of bytes the memory profiler will collect the allocating stack trace. Zero means disabled memory profiler. Values lower than a few megabytes will slow down server.", 0) \
    M(Double, total_memory_tracker_sample_probability, 0, "Collect random allocations and deallocations and write them into system.trace_log with 'MemorySample' trace_type. The probability is for every alloc/free regardless to the size of the allocation (can be changed with `memory_profiler_sample_min_allocation_size` and `memory_profiler_sample_max_allocation_size`). Note that sampling happens only when the amount of untracked memory exceeds 'max_untracked_memory'. You may want to set 'max_untracked_memory' to 0 for extra fine grained sampling.", 0) \
    M(UInt64, total_memory_profiler_sample_min_allocation_size, 0, "Collect random allocations of size greater or equal than specified value with probability equal to `total_memory_profiler_sample_probability`. 0 means disabled. You may want to set 'max_untracked_memory' to 0 to make this threshold to work as expected.", 0) \
    M(UInt64, total_memory_profiler_sample_max_allocation_size, 0, "Collect random allocations of size less or equal than specified value with probability equal to `total_memory_profiler_sample_probability`. 0 means disabled. You may want to set 'max_untracked_memory' to 0 to make this threshold to work as expected.", 0) \
    M(Bool, validate_tcp_client_information, false, "Validate client_information in the query packet over the native TCP protocol.", 0) \
    M(Bool, storage_metadata_write_full_object_key, false, "Write disk metadata files with VERSION_FULL_OBJECT_KEY format", 0) \
    M(UInt64, max_materialized_views_count_for_table, 0, "A limit on the number of materialized views attached to a table.", 0) \
    M(UInt32, max_database_replicated_create_table_thread_pool_size, 1, "The number of threads to create tables during replica recovery in DatabaseReplicated. Zero means number of threads equal number of cores.", 0) \
    M(Bool, database_replicated_allow_detach_permanently, true, "Allow detaching tables permanently in Replicated databases", 0) \
    M(Bool, format_alter_operations_with_parentheses, false, "If enabled, each operation in alter queries will be surrounded with parentheses in formatted queries to make them less ambiguous.", 0) \
    M(String, default_replica_path, "/clickhouse/tables/{uuid}/{shard}", "The path to the table in ZooKeeper", 0) \
    M(String, default_replica_name, "{replica}", "The replica name in ZooKeeper", 0) \
    M(UInt64, disk_connections_soft_limit, 5000, "Connections above this limit have significantly shorter time to live. The limit applies to the disks connections.", 0) \
    M(UInt64, disk_connections_warn_limit, 10000, "Warning massages are written to the logs if number of in-use connections are higher than this limit. The limit applies to the disks connections.", 0) \
    M(UInt64, disk_connections_store_limit, 30000, "Connections above this limit reset after use. Set to 0 to turn connection cache off. The limit applies to the disks connections.", 0) \
    M(UInt64, storage_connections_soft_limit, 100, "Connections above this limit have significantly shorter time to live. The limit applies to the storages connections.", 0) \
    M(UInt64, storage_connections_warn_limit, 1000, "Warning massages are written to the logs if number of in-use connections are higher than this limit. The limit applies to the storages connections.", 0) \
    M(UInt64, storage_connections_store_limit, 5000, "Connections above this limit reset after use. Set to 0 to turn connection cache off. The limit applies to the storages connections.", 0) \
    M(UInt64, http_connections_soft_limit, 100, "Connections above this limit have significantly shorter time to live. The limit applies to the http connections which do not belong to any disk or storage.", 0) \
    M(UInt64, http_connections_warn_limit, 1000, "Warning massages are written to the logs if number of in-use connections are higher than this limit. The limit applies to the http connections which do not belong to any disk or storage.", 0) \
    M(UInt64, http_connections_store_limit, 5000, "Connections above this limit reset after use. Set to 0 to turn connection cache off. The limit applies to the http connections which do not belong to any disk or storage.", 0) \
    M(UInt64, global_profiler_real_time_period_ns, 0, "Period for real clock timer of global profiler (in nanoseconds). Set 0 value to turn off the real clock global profiler. Recommended value is at least 10000000 (100 times a second) for single queries or 1000000000 (once a second) for cluster-wide profiling.", 0) \
    M(UInt64, global_profiler_cpu_time_period_ns, 0, "Period for CPU clock timer of global profiler (in nanoseconds). Set 0 value to turn off the CPU clock global profiler. Recommended value is at least 10000000 (100 times a second) for single queries or 1000000000 (once a second) for cluster-wide profiling.", 0) \
    M(Bool, enable_azure_sdk_logging, false, "Enables logging from Azure sdk", 0) \
    M(UInt64, max_entries_for_hash_table_stats, 10'000, "How many entries hash table statistics collected during aggregation is allowed to have", 0) \
    M(String, merge_workload, "default", "Name of workload to be used to access resources for all merges (may be overridden by a merge tree setting)", 0) \
    M(String, mutation_workload, "default", "Name of workload to be used to access resources for all mutations (may be overridden by a merge tree setting)", 0) \
    M(Bool, prepare_system_log_tables_on_startup, false, "If true, ClickHouse creates all configured `system.*_log` tables before the startup. It can be helpful if some startup scripts depend on these tables.", 0) \
    M(Double, gwp_asan_force_sample_probability, 0.0003, "Probability that an allocation from specific places will be sampled by GWP Asan (i.e. PODArray allocations)", 0) \
    M(UInt64, config_reload_interval_ms, 2000, "How often clickhouse will reload config and check for new changes", 0) \
    M(UInt64, memory_worker_period_ms, 0, "Tick period of background memory worker which corrects memory tracker memory usages and cleans up unused pages during higher memory usage. If set to 0, default value will be used depending on the memory usage source", 0) \
    M(Bool, disable_insertion_and_mutation, false, "Disable all insert/alter/delete queries. This setting will be enabled if someone needs read-only nodes to prevent insertion and mutation affect reading performance.", 0) \
    M(UInt64, keeper_multiread_batch_size, 10'000, "Maximum size of batch for MultiRead request to [Zoo]Keeper that support batching. If set to 0, batching is disabled. Available only in ClickHouse Cloud.", 0) \
    M(Bool, use_legacy_mongodb_integration, true, "Use the legacy MongoDB integration implementation. Note: it's highly recommended to set this option to false, since legacy implementation will be removed in the future. Please submit any issues you encounter with the new implementation.", 0) \

/// If you add a setting which can be updated at runtime, please update 'changeable_settings' map in StorageSystemServerSettings.cpp

DECLARE_SETTINGS_TRAITS(ServerSettingsTraits, SERVER_SETTINGS)

struct ServerSettings : public BaseSettings<ServerSettingsTraits>
{
    void loadSettingsFromConfig(const Poco::Util::AbstractConfiguration & config);
};

}
