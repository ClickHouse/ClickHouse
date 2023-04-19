#pragma once


#include <Core/BaseSettings.h>


namespace Poco::Util
{
class AbstractConfiguration;
}

namespace DB
{

#define SERVER_SETTINGS(M, ALIAS) \
    M(Bool, show_addresses_in_stack_traces, true, "If it is set true will show addresses in stack traces", 0) \
    M(Bool, shutdown_wait_unfinished_queries, false, "If set true ClickHouse will wait for running queries finish before shutdown.", 0) \
    M(UInt64, max_thread_pool_size, 10000, "The maximum number of threads that could be allocated from the OS and used for query execution and background operations.", 0) \
    M(UInt64, max_thread_pool_free_size, 1000, "The maximum number of threads that will always stay in a global thread pool once allocated and remain idle in case of insufficient number of tasks.", 0) \
    M(UInt64, thread_pool_queue_size, 10000, "The maximum number of tasks that will be placed in a queue and wait for execution.", 0) \
    M(UInt64, max_io_thread_pool_size, 100, "The maximum number of threads that would be used for IO operations", 0) \
    M(UInt64, max_io_thread_pool_free_size, 0, "Max free size for IO thread pool.", 0) \
    M(UInt64, io_thread_pool_queue_size, 10000, "Queue size for IO thread pool.", 0) \
    M(UInt64, max_backups_io_thread_pool_size, 1000, "The maximum number of threads that would be used for IO operations for BACKUP queries", 0) \
    M(UInt64, max_backups_io_thread_pool_free_size, 0, "Max free size for backups IO thread pool.", 0) \
    M(UInt64, backups_io_thread_pool_queue_size, 0, "Queue size for backups IO thread pool.", 0) \
    M(UInt64, backup_threads, 16, "The maximum number of threads to execute BACKUP requests.", 0) \
    M(UInt64, restore_threads, 16, "The maximum number of threads to execute RESTORE requests.", 0) \
    M(Int32, max_connections, 1024, "Max server connections.", 0) \
    M(UInt32, asynchronous_metrics_update_period_s, 1, "Period in seconds for updating asynchronous metrics.", 0) \
    M(UInt32, asynchronous_heavy_metrics_update_period_s, 120, "Period in seconds for updating asynchronous metrics.", 0) \
    M(String, default_database, "default", "Default database name.", 0) \
    M(String, tmp_policy, "", "Policy for storage with temporary data.", 0) \
    M(UInt64, max_temporary_data_on_disk_size, 0, "The maximum amount of storage that could be used for external aggregation, joins or sorting., ", 0) \
    M(String, temporary_data_in_cache, "", "Cache disk name for temporary data.", 0) \
    M(UInt64, max_server_memory_usage, 0, "Limit on total memory usage. Zero means Unlimited.", 0) \
    M(Double, max_server_memory_usage_to_ram_ratio, 0.9, "Same as max_server_memory_usage but in to ram ratio. Allows to lower max memory on low-memory systems.", 0) \
    M(Bool, allow_use_jemalloc_memory, true, "Allows to use jemalloc memory.", 0) \
    \
    M(UInt64, max_concurrent_queries, 0, "Limit on total number of concurrently executed queries. Zero means Unlimited.", 0) \
    M(UInt64, max_concurrent_insert_queries, 0, "Limit on total number of concurrently insert queries. Zero means Unlimited.", 0) \
    M(UInt64, max_concurrent_select_queries, 0, "Limit on total number of concurrently select queries. Zero means Unlimited.", 0) \
    \
    M(Double, cache_size_to_ram_max_ratio, 0.5, "Set cache size ro ram max ratio. Allows to lower cache size on low-memory systems.", 0) \
    M(String, uncompressed_cache_policy, "SLRU", "Uncompressed cache policy name.", 0) \
    M(UInt64, uncompressed_cache_size, 0, "Size of cache for uncompressed blocks. Zero means disabled.", 0) \
    M(UInt64, mark_cache_size, 5368709120, "Size of cache for marks (index of MergeTree family of tables).", 0) \
    M(String, mark_cache_policy, "SLRU", "Mark cache policy name.", 0) \
    M(UInt64, index_uncompressed_cache_size, 0, "Size of cache for uncompressed blocks of MergeTree indices. Zero means disabled.", 0) \
    M(UInt64, index_mark_cache_size, 0, "Size of cache for index marks. Zero means disabled.", 0) \
    M(UInt64, mmap_cache_size, 1000, "A cache for mmapped files.", 0)  /* The choice of default is arbitrary. */ \
    \
    M(Bool, disable_internal_dns_cache, false, "Disable internal DNS caching at all.", 0) \
    M(Int32, dns_cache_update_period, 15, "Internal DNS cache update period in seconds.", 0) \
    M(UInt32, dns_max_consecutive_failures, 1024, "Max server connections.", 0) \
    \
    M(UInt64, max_table_size_to_drop, 50000000000lu, "If size of a table is greater than this value (in bytes) than table could not be dropped with any DROP query.", 0) \
    M(UInt64, max_partition_size_to_drop, 50000000000lu, "Same as max_table_size_to_drop, but for the partitions.", 0) \
    M(UInt64, concurrent_threads_soft_limit_num, 0, "Sets how many concurrent thread can be allocated before applying CPU pressure. Zero means Unlimited.", 0) \
    M(UInt64, concurrent_threads_soft_limit_ratio_to_cores, 0, "Same as concurrent_threads_soft_limit_num, but with ratio to cores.", 0) \
    \
    M(UInt64, background_pool_size, 16, "The maximum number of threads what will be used for merging or mutating data parts for *MergeTree-engine tables in a background.", 0) \
    M(Float, background_merges_mutations_concurrency_ratio, 2, "The number of part mutation tasks that can be executed concurrently by each thread in background pool.", 0) \
    M(String, background_merges_mutations_scheduling_policy, "round_robin", "The policy on how to perform a scheduling for background merges and mutations. Possible values are: `round_robin` and `shortest_task_first`. ", 0) \
    M(UInt64, background_move_pool_size, 8, "The maximum number of threads that will be used for moving data parts to another disk or volume for *MergeTree-engine tables in a background.", 0) \
    M(UInt64, background_fetches_pool_size, 8, "The maximum number of threads that will be used for fetching data parts from another replica for *MergeTree-engine tables in a background.", 0) \
    M(UInt64, background_common_pool_size, 8, "The maximum number of threads that will be used for performing a variety of operations (mostly garbage collection) for *MergeTree-engine tables in a background.", 0) \
    M(UInt64, background_buffer_flush_schedule_pool_size, 16, "The maximum number of threads that will be used for performing flush operations for Buffer-engine tables in a background.", 0) \
    M(UInt64, background_schedule_pool_size, 128, "The maximum number of threads that will be used for constantly executing some lightweight periodic operations.", 0) \
    M(UInt64, background_message_broker_schedule_pool_size, 16, "The maximum number of threads that will be used for executing background operations for message streaming.", 0) \
    M(UInt64, background_distributed_schedule_pool_size, 16, "The maximum number of threads that will be used for executing distributed sends.", 0) \


DECLARE_SETTINGS_TRAITS(ServerSettingsTraits, SERVER_SETTINGS)

struct ServerSettings : public BaseSettings<ServerSettingsTraits>
{
    void loadSettingsFromConfig(const Poco::Util::AbstractConfiguration & config);
};

}
