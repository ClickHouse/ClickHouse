#pragma once


#include <Core/BaseSettings.h>
#include <Core/Defines.h>


namespace Poco::Util
{
class AbstractConfiguration;
}

namespace DB
{

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
    M(Int32, max_connections, 1024, "Max server connections.", 0) \
    M(UInt32, asynchronous_metrics_update_period_s, 1, "Period in seconds for updating asynchronous metrics.", 0) \
    M(UInt32, asynchronous_heavy_metrics_update_period_s, 120, "Period in seconds for updating heavy asynchronous metrics.", 0) \
    M(String, default_database, "default", "Default database name.", 0) \
    M(String, tmp_policy, "", "Policy for storage with temporary data.", 0) \
    M(UInt64, max_temporary_data_on_disk_size, 0, "The maximum amount of storage that could be used for external aggregation, joins or sorting., ", 0) \
    M(String, temporary_data_in_cache, "", "Cache disk name for temporary data.", 0) \
    M(UInt64, aggregate_function_group_array_max_element_size, 0xFFFFFF, "Max array element size in bytes for groupArray function. This limit is checked at serialization and help to avoid large state size.", 0) \
    M(UInt64, max_server_memory_usage, 0, "Maximum total memory usage of the server in bytes. Zero means unlimited.", 0) \
    M(Double, max_server_memory_usage_to_ram_ratio, 0.9, "Same as max_server_memory_usage but in to RAM ratio. Allows to lower max memory on low-memory systems.", 0) \
    M(UInt64, merges_mutations_memory_usage_soft_limit, 0, "Maximum total memory usage for merges and mutations in bytes. Zero means unlimited.", 0) \
    M(Double, merges_mutations_memory_usage_to_ram_ratio, 0.5, "Same as merges_mutations_memory_usage_soft_limit but in to RAM ratio. Allows to lower memory limit on low-memory systems.", 0) \
    M(Bool, allow_use_jemalloc_memory, true, "Allows to use jemalloc memory.", 0) \
    M(UInt64, async_insert_threads, 16, "Maximum number of threads to actually parse and insert data in background. Zero means asynchronous mode is disabled", 0) \
    M(Bool, async_insert_queue_flush_on_shutdown, true, "If true queue of asynchronous inserts is flushed on graceful shutdown", 0) \
    \
    M(UInt64, max_concurrent_queries, 0, "Maximum number of concurrently executed queries. Zero means unlimited.", 0) \
    M(UInt64, max_concurrent_insert_queries, 0, "Maximum number of concurrently INSERT queries. Zero means unlimited.", 0) \
    M(UInt64, max_concurrent_select_queries, 0, "Maximum number of concurrently SELECT queries. Zero means unlimited.", 0) \
    \
    M(Double, cache_size_to_ram_max_ratio, 0.5, "Set cache size ro RAM max ratio. Allows to lower cache size on low-memory systems.", 0) \
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
    M(UInt64, mmap_cache_size, DEFAULT_MMAP_CACHE_MAX_SIZE, "A cache for mmapped files.", 0) \
    \
    M(Bool, disable_internal_dns_cache, false, "Disable internal DNS caching at all.", 0) \
    M(Int32, dns_cache_update_period, 15, "Internal DNS cache update period in seconds.", 0) \
    M(UInt32, dns_max_consecutive_failures, 10, "Max DNS resolve failures of a hostname before dropping the hostname from ClickHouse DNS cache.", 0) \
    \
    M(UInt64, max_table_size_to_drop, 50000000000lu, "If size of a table is greater than this value (in bytes) than table could not be dropped with any DROP query.", 0) \
    M(UInt64, max_partition_size_to_drop, 50000000000lu, "Same as max_table_size_to_drop, but for the partitions.", 0) \
    M(UInt64, max_table_num_to_warn, 5000lu, "If number of tables is greater than this value, server will create a warning that will displayed to user.", 0) \
    M(UInt64, max_database_num_to_warn, 1000lu, "If number of databases is greater than this value, server will create a warning that will displayed to user.", 0) \
    M(UInt64, max_part_num_to_warn, 100000lu, "If number of databases is greater than this value, server will create a warning that will displayed to user.", 0) \
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
    \
    M(Seconds, keep_alive_timeout, DEFAULT_HTTP_KEEP_ALIVE_TIMEOUT, "The number of seconds that ClickHouse waits for incoming requests before closing the connection.", 0) \
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

    /// If you add a setting which can be updated at runtime, please update 'changeable_settings' map in StorageSystemServerSettings.cpp

DECLARE_SETTINGS_TRAITS(ServerSettingsTraits, SERVER_SETTINGS)

struct ServerSettings : public BaseSettings<ServerSettingsTraits>
{
    void loadSettingsFromConfig(const Poco::Util::AbstractConfiguration & config);
};

}
