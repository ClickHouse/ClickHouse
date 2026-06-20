#include <Access/AccessControl.h>
#include <Columns/IColumn.h>
#include <Core/BaseSettings.h>
#include <Core/BaseSettingsFwdMacrosImpl.h>
#include <Core/ServerSettings.h>
#include <IO/MMappedFileCache.h>
#include <Interpreters/Cache/QueryConditionCache.h>
#include <IO/UncompressedCache.h>
#include <IO/SharedThreadPools.h>
#include <IO/S3Defines.h>
#include <Interpreters/Context.h>
#include <Interpreters/ProcessList.h>
#include <Storages/MarkCache.h>
#include <Storages/MergeTree/MergeTreeBackgroundExecutor.h>
#include <Storages/MergeTree/PrimaryIndexCache.h>
#include <Storages/MergeTree/VectorSimilarityIndexCache.h>
#include <Storages/System/ServerSettingColumnsParams.h>
#include <base/types.h>
#include <Common/Config/ConfigReloader.h>
#include <Common/MemoryTracker.h>

#include <Common/DNSResolver.h>

#include <Poco/Util/AbstractConfiguration.h>


namespace CurrentMetrics
{
extern const Metric BackgroundSchedulePoolSize;
extern const Metric BackgroundBufferFlushSchedulePoolSize;
extern const Metric BackgroundDistributedSchedulePoolSize;
extern const Metric BackgroundMessageBrokerSchedulePoolSize;
}

namespace DB
{


namespace
{
    constexpr int getDefaultOomScore() {
#if defined(OS_LINUX) && !defined(NDEBUG)
        /// In debug version on Linux, increase oom score so that clickhouse is killed
        /// first, instead of some service. Use a carefully chosen random score of 555:
        /// the maximum is 1000, and chromium uses 300 for its tab processes. Ignore
        /// whatever errors that occur, because it's just a debugging aid and we don't
        /// care if it breaks.
        return 555;
#else
        return 0;
#endif
    }
}

// clang-format off

/// Settings without path are top-level server settings (no nesting).
#define LIST_OF_SERVER_SETTINGS_WITHOUT_PATH(DECLARE, ALIAS) \
    DECLARE(InsertDeduplicationVersions, insert_deduplication_version, InsertDeduplicationVersions::OLD_SEPARATE_HASHES, R"(
        This setting makes it possible to migrate from the code version which makes insert deduplication for sync and async inserts totally different not transparent way to the code version where inserted data would be deduplicated across sync and async inserts.
        The default value is `old_separate_hashes`, which means that ClickHouse will use different deduplication hashes for sync and async inserts (the same as before).
        This value should be used as a default value to be backward compatible. All existing instances of Clickhouse should use this value to avoid breaking changes.
        The value `compatible_double_hashes` means that ClickHouse will use two deduplication hashes: the old one for sync or async inserts and another the new one for all inserts. This value should be used to migrate existing instances to the new behavior in a safe way.
        This value should be enabled for some time (see replicated_deduplication_window and non_replicated_deduplication_window settings) to make sure that no sync or async inserts are lost during migration.
        Finally the value `new_unified_hash` means that ClickHouse will use the new deduplication hash for sync and async inserts. This value could be enabled on new instances of ClickHouse or on instances which already used `compatible_double_hashes` value for some time.
    )", 0) \
    DECLARE(UInt64, dictionary_background_reconnect_interval, 1000, "Interval in milliseconds for reconnection attempts of failed MySQL and Postgres dictionaries having `background_reconnect` enabled.", 0) \
    DECLARE(Bool, show_addresses_in_stack_traces, true, R"(If it is set true will show addresses in stack traces)", 0) \
    DECLARE(Bool, shutdown_wait_unfinished_queries, false, R"(If set true ClickHouse will wait for running queries finish before shutdown.)", 0) \
    DECLARE(UInt64, shutdown_wait_unfinished, 5, R"(Delay in seconds to wait for unfinished queries)", 0) \
    DECLARE(UInt64, max_thread_pool_size, 10000, R"(
    ClickHouse uses threads from the Global Thread pool to process queries. If there is no idle thread to process a query, then a new thread is created in the pool. `max_thread_pool_size` limits the maximum number of threads in the pool.

    **Example**

    ```xml
    <max_thread_pool_size>12000</max_thread_pool_size>
    ```
    )", 0) \
    DECLARE(UInt64, max_thread_pool_free_size, 1000, R"(
    If the number of **idle** threads in the Global Thread pool is greater than [`max_thread_pool_free_size`](/operations/server-configuration-parameters/settings#max_thread_pool_free_size), then ClickHouse releases resources occupied by some threads and the pool size is decreased. Threads can be created again if necessary.

    **Example**

    ```xml
    <max_thread_pool_free_size>1200</max_thread_pool_free_size>
    ```
    )", 0) \
    DECLARE(UInt64, thread_pool_queue_size, 10000, R"(
    The maximum number of jobs that can be scheduled on the Global Thread pool. Increasing queue size leads to larger memory usage. It is recommended to keep this value equal to [`max_thread_pool_size`](/operations/server-configuration-parameters/settings#max_thread_pool_size).

    :::note
    A value of `0` means unlimited.
    :::

    **Example**

    ```xml
    <thread_pool_queue_size>12000</thread_pool_queue_size>
    ```
    )", 0) \
    DECLARE(UInt64, max_io_thread_pool_size, 100, R"(
    ClickHouse uses threads from the IO Thread pool to do some IO operations (e.g. to interact with S3). `max_io_thread_pool_size` limits the maximum number of threads in the pool.
    )", 0) \
    DECLARE(UInt64, max_io_thread_pool_free_size, 0, R"(
    If the number of **idle** threads in the IO Thread pool exceeds `max_io_thread_pool_free_size`, ClickHouse will release resources occupied by idling threads and decrease the pool size. Threads can be created again if necessary.
    )", 0) \
    DECLARE(UInt64, io_thread_pool_queue_size, 10000, R"(
    The maximum number of jobs that can be scheduled on the IO Thread pool.

    :::note
    A value of `0` means unlimited.
    :::
    )", 0) \
    DECLARE(UInt64, max_prefixes_deserialization_thread_pool_size, 100, R"(
    ClickHouse uses threads from the prefixes deserialization Thread pool for parallel reading of metadata of columns and subcolumns from file prefixes in Wide parts in MergeTree. `max_prefixes_deserialization_thread_pool_size` limits the maximum number of threads in the pool.
    )", 0) \
    DECLARE(UInt64, max_prefixes_deserialization_thread_pool_free_size, 0, R"(
    If the number of **idle** threads in the prefixes deserialization Thread pool exceeds `max_prefixes_deserialization_thread_pool_free_size`, ClickHouse will release resources occupied by idling threads and decrease the pool size. Threads can be created again if necessary.
    )", 0) \
    DECLARE(UInt64, prefixes_deserialization_thread_pool_thread_pool_queue_size, 10000, R"(
    The maximum number of jobs that can be scheduled on the prefixes deserialization Thread pool.

    :::note
    A value of `0` means unlimited.
    :::
    )", 0) \
    DECLARE(UInt64, max_format_parsing_thread_pool_size, 100, R"(
    Maximum total number of threads to use for parsing input.
    )", 0) \
    DECLARE(UInt64, max_format_parsing_thread_pool_free_size, 0, R"(
    Maximum number of idle standby threads to keep in the thread pool for parsing input.
    )", 0) \
    DECLARE(UInt64, format_parsing_thread_pool_queue_size, 10000, R"(
    The maximum number of jobs that can be scheduled on thread pool for parsing input.

    :::note
    A value of `0` means unlimited.
    :::
    )", 0) \
    DECLARE(UInt64, max_fetch_partition_thread_pool_size, 64, R"(The number of threads for ALTER TABLE FETCH PARTITION.)", 0) \
    DECLARE(UInt64, max_active_parts_loading_thread_pool_size, 64, R"(The number of threads to load active set of data parts (Active ones) at startup.)", 0) \
    DECLARE(UInt64, max_outdated_parts_loading_thread_pool_size, 32, R"(The number of threads to load inactive set of data parts (Outdated ones) at startup.)", 0) \
    DECLARE(UInt64, max_unexpected_parts_loading_thread_pool_size, 8, R"(The number of threads to load inactive set of data parts (Unexpected ones) at startup.)", 0) \
    DECLARE(UInt64, max_parts_cleaning_thread_pool_size, 128, R"(The number of threads for concurrent removal of inactive data parts.)", 0) \
    DECLARE(UInt64, max_mutations_bandwidth_for_server, 0, R"(The maximum read speed of all mutations on server in bytes per second. Zero means unlimited.)", 0) \
    DECLARE(UInt64, max_merges_bandwidth_for_server, 0, R"(The maximum read speed of all merges on server in bytes per second. Zero means unlimited.)", 0) \
    DECLARE(UInt64, max_replicated_fetches_network_bandwidth_for_server, 0, R"(The maximum speed of data exchange over the network in bytes per second for replicated fetches. Zero means unlimited.)", 0) \
    DECLARE(UInt64, max_replicated_sends_network_bandwidth_for_server, 0, R"(The maximum speed of data exchange over the network in bytes per second for replicated sends. Zero means unlimited.)", 0) \
    DECLARE(UInt64, max_distributed_cache_read_bandwidth_for_server, 0, R"(The maximum total read speed from distributed cache on server in bytes per second. Zero means unlimited.)", 0) \
    DECLARE(UInt64, max_distributed_cache_write_bandwidth_for_server, 0, R"(The maximum total write speed to distributed cache on server in bytes per second. Zero means unlimited.)", 0) \
    DECLARE(UInt64, max_remote_read_network_bandwidth_for_server, 0, R"(
    The maximum speed of data exchange over the network in bytes per second for read.

    :::note
    A value of `0` (default) means unlimited.
    :::
    )", 0) \
    DECLARE(UInt64, max_remote_write_network_bandwidth_for_server, 0, R"(
    The maximum speed of data exchange over the network in bytes per second for write.

    :::note
    A value of `0` (default) means unlimited.
    :::
    )", 0) \
    DECLARE(UInt64, max_local_read_bandwidth_for_server, 0, R"(
    The maximum speed of local reads in bytes per second.

    :::note
    A value of `0` means unlimited.
    :::
    )", 0) \
    DECLARE(UInt64, max_local_write_bandwidth_for_server, 0, R"(
    The maximum speed of local writes in bytes per seconds.

    :::note
    A value of `0` means unlimited.
    :::
    )", 0) \
    DECLARE(UInt64, max_backups_io_thread_pool_size, 1000, R"(ClickHouse uses threads from the Backups IO Thread pool to do S3 backup IO operations. `max_backups_io_thread_pool_size` limits the maximum number of threads in the pool.)", 0) \
    DECLARE(UInt64, max_backups_io_thread_pool_free_size, 0, R"(If the number of **idle** threads in the Backups IO Thread pool exceeds `max_backup_io_thread_pool_free_size`, ClickHouse will release resources occupied by idling threads and decrease the pool size. Threads can be created again if necessary.)", 0) \
    DECLARE(UInt64, backups_io_thread_pool_queue_size, 0, R"(
    The maximum number of jobs that can be scheduled on the Backups IO Thread pool. It is recommended to keep this queue unlimited due to the current S3 backup logic.

    :::note
    A value of `0` (default) means unlimited.
    :::
    )", 0) \
    DECLARE(NonZeroUInt64, backup_threads, 16, R"(The maximum number of threads to execute `BACKUP` requests.)", 0) \
    DECLARE(UInt64, max_backup_bandwidth_for_server, 0, R"(The maximum read speed in bytes per second for all backups on server. Zero means unlimited.)", 0) \
    DECLARE(NonZeroUInt64, restore_threads, 16, R"(The maximum number of threads to execute RESTORE requests.)", 0) \
    DECLARE(Bool, shutdown_wait_backups_and_restores, true, R"(If set to true ClickHouse will wait for running backups and restores to finish before shutdown.)", 0) \
    DECLARE(Double, cannot_allocate_thread_fault_injection_probability, 0, R"(For testing purposes.)", 0) \
    DECLARE(Int32, max_connections, 4096, R"(Max server connections.)", 0) \
    DECLARE(UInt32, asynchronous_metrics_update_period_s, 1, R"(Period in seconds for updating asynchronous metrics.)", 0) \
    DECLARE(Bool, asynchronous_metrics_enable_heavy_metrics, false, R"(Enable the calculation of heavy asynchronous metrics.)", 0) \
    DECLARE(UInt32, asynchronous_heavy_metrics_update_period_s, 120, R"(Period in seconds for updating heavy asynchronous metrics.)", 0) \
    DECLARE(Bool, asynchronous_metrics_keeper_metrics_only, false, R"(Make asynchronous metrics calculate the keeper-related metrics only.)", 0) \
    DECLARE(String, default_database, "default", R"(The default database name.)", 0) \
    DECLARE(String, tmp_policy, "", R"(
    Policy for storage with temporary data. All files with `tmp` prefix will be removed at start.

    :::note
    Recommendations for using object storage as `tmp_policy`:
    - Use separate `bucket:path` on each server
    - Use `metadata_type=plain`
    - You may also want to set TTL for this bucket
    :::

    :::note
    - Only one option can be used to configure temporary data storage: `tmp_path` ,`tmp_policy`, `temporary_data_in_cache`.
    - `move_factor`, `keep_free_space_bytes`,`max_data_part_size_bytes` and are ignored.
    - Policy should have exactly *one volume*

    For more information see the [MergeTree Table Engine](/engines/table-engines/mergetree-family/mergetree) documentation.
    :::

    **Example**

    When `/disk1` is full, temporary data will be stored on `/disk2`.

    ```xml
    <clickhouse>
    <storage_configuration>
        <disks>
            <disk1>
                <path>/disk1/</path>
            </disk1>
            <disk2>
                <path>/disk2/</path>
            </disk2>
        </disks>

        <policies>
            <!-- highlight-start -->
            <tmp_two_disks>
                <volumes>
                    <main>
                        <disk>disk1</disk>
                        <disk>disk2</disk>
                    </main>
                </volumes>
            </tmp_two_disks>
            <!-- highlight-end -->
        </policies>
    </storage_configuration>

    <!-- highlight-start -->
    <tmp_policy>tmp_two_disks</tmp_policy>
    <!-- highlight-end -->
    </clickhouse>
    ```
    )", 0) \
    DECLARE(UInt64, max_temporary_data_on_disk_size, 0, R"(
    The maximum amount of storage that could be used for external aggregation, joins or sorting.
    Queries that exceed this limit will fail with an exception.

    :::note
    A value of `0` means unlimited.
    :::

    See also:
    - [`max_temporary_data_on_disk_size_for_user`](/operations/settings/settings#max_temporary_data_on_disk_size_for_user)
    - [`max_temporary_data_on_disk_size_for_query`](/operations/settings/settings#max_temporary_data_on_disk_size_for_query)
    )", 0) \
    DECLARE(String, temporary_data_in_cache, "", R"(
    With this option, temporary data will be stored in the cache for the particular disk.
    In this section, you should specify the disk name with the type `cache`.
    In that case, the cache and temporary data will share the same space, and the disk cache can be evicted to create temporary data.

    :::note
    Only one option can be used to configure temporary data storage: `tmp_path` ,`tmp_policy`, `temporary_data_in_cache`.
    :::

    **Example**

    Both the cache for `local_disk`, and temporary data will be stored in `/tiny_local_cache` on the filesystem, managed by `tiny_local_cache`.

    ```xml
    <clickhouse>
    <storage_configuration>
        <disks>
            <local_disk>
                <type>local</type>
                <path>/local_disk/</path>
            </local_disk>

            <!-- highlight-start -->
            <tiny_local_cache>
                <type>cache</type>
                <disk>local_disk</disk>
                <path>/tiny_local_cache/</path>
                <max_size_rows>10M</max_size_rows>
                <max_file_segment_size>1M</max_file_segment_size>
                <cache_on_write_operations>1</cache_on_write_operations>
            </tiny_local_cache>
            <!-- highlight-end -->
        </disks>
    </storage_configuration>

    <!-- highlight-start -->
    <temporary_data_in_cache>tiny_local_cache</temporary_data_in_cache>
    <!-- highlight-end -->
    </clickhouse>
    ```
    )", 0) \
    DECLARE(Bool, temporary_data_in_distributed_cache, 0, R"(Store temporary data in the distributed cache.)", 0) \
    DECLARE(UInt64, aggregate_function_group_array_max_element_size, 0xFFFFFF, R"(Max array element size in bytes for groupArray function. This limit is checked at serialization and help to avoid large state size.)", 0) \
    DECLARE(GroupArrayActionWhenLimitReached, aggregate_function_group_array_action_when_limit_is_reached, GroupArrayActionWhenLimitReached::THROW, R"(Action to execute when max array element size is exceeded in groupArray: `throw` exception, or `discard` extra values)", 0) \
    DECLARE(UInt64, max_server_memory_usage, 0, R"(
    The maximum amount of memory the server is allowed to use, expressed in bytes.

    :::note
    The maximum memory consumption of the server is further restricted by setting `max_server_memory_usage_to_ram_ratio`.
    :::

    As a special case, a value of `0` (default) means the server may consume all available memory (excluding further restrictions imposed by `max_server_memory_usage_to_ram_ratio`).
    )", 0) \
    DECLARE(Double, max_server_memory_usage_to_ram_ratio, 0.9, R"(
    The maximum amount of memory the server is allowed to use, expressed as a ratio to all available memory.

    For example, a value of `0.9` (default) means that the server may consume 90% of the available memory.

    Allows lowering the memory usage on low-memory systems.
    On hosts with low RAM and swap, you may possibly need setting [`max_server_memory_usage_to_ram_ratio`](#max_server_memory_usage_to_ram_ratio) set larger than 1.

    :::note
    The maximum memory consumption of the server is further restricted by setting `max_server_memory_usage`.
    :::
    )", 0) \
    DECLARE(UInt64, merges_mutations_memory_usage_soft_limit, 0, R"(
    Sets the limit on how much RAM is allowed to use for performing merge and mutation operations.
    If ClickHouse reaches the limit set, it won't schedule any new background merge or mutation operations but will continue to execute already scheduled tasks.

    :::note
    A value of `0` means unlimited.
    :::

    **Example**

    ```xml
    <merges_mutations_memory_usage_soft_limit>0</merges_mutations_memory_usage_soft_limit>
    ```
    )", 0) \
    DECLARE(Double, merges_mutations_memory_usage_to_ram_ratio, 0.5, R"(
    The default `merges_mutations_memory_usage_soft_limit` value is calculated as `memory_amount * merges_mutations_memory_usage_to_ram_ratio`.

    **See also:**

    - [max_memory_usage](/operations/settings/settings#max_memory_usage)
    - [merges_mutations_memory_usage_soft_limit](/operations/server-configuration-parameters/settings#merges_mutations_memory_usage_soft_limit)
    )", 0) \
    DECLARE(Bool, allow_use_jemalloc_memory, true, R"(Allows to use jemalloc memory.)", 0) \
    DECLARE(UInt64, cgroups_memory_usage_observer_wait_time, 15, R"(
    Interval in seconds during which the server's maximum allowed memory consumption is adjusted by the corresponding threshold in cgroups.

    To disable the cgroup observer, set this value to `0`.
    )", 0) \
    DECLARE(UInt64, async_insert_threads, 16, R"(Maximum number of threads to actually parse and insert data in background. Zero means asynchronous mode is disabled)", 0) \
    DECLARE(Bool, async_insert_queue_flush_on_shutdown, true, R"(If true queue of asynchronous inserts is flushed on graceful shutdown)", 0) \
    DECLARE(Bool, ignore_empty_sql_security_in_create_view_query, true, R"(
    If true, ClickHouse doesn't write defaults for empty SQL security statement in `CREATE VIEW` queries.

    :::note
    This setting is only necessary for the migration period and will become obsolete in 24.4
    :::
    )", 0)  \
    DECLARE(UInt64, max_build_vector_similarity_index_thread_pool_size, 16, R"(
    The maximum number of threads to use for building vector indexes.

    :::note
    A value of `0` means all cores.
    :::
    )", 0) \
    \
    /* Database Catalog */ \
    DECLARE(UInt64, database_atomic_delay_before_drop_table_sec, 8 * 60, R"(
    The delay during which a dropped table can be restored using the [`UNDROP`](/sql-reference/statements/undrop.md) statement. If `DROP TABLE` ran with a `SYNC` modifier, the setting is ignored.
    The default for this setting is `480` (8 minutes).
    )", 0) \
    DECLARE(UInt64, database_catalog_unused_dir_hide_timeout_sec, 60 * 60, R"(
    Parameter of a task that cleans up garbage from `store/` directory.
    If some subdirectory is not used by clickhouse-server and this directory was not modified for last
    [`database_catalog_unused_dir_hide_timeout_sec`](/operations/server-configuration-parameters/settings#database_catalog_unused_dir_hide_timeout_sec) seconds, the task will "hide" this directory by
    removing all access rights. It also works for directories that clickhouse-server does not
    expect to see inside `store/`.

    :::note
    A value of `0` means "immediately".
    :::
    )", 0) \
    DECLARE(UInt64, database_catalog_unused_dir_rm_timeout_sec, 30 * 24 * 60 * 60, R"(
    Parameter of a task that cleans up garbage from `store/` directory.
    If some subdirectory is not used by clickhouse-server and it was previously "hidden"
    (see [database_catalog_unused_dir_hide_timeout_sec](/operations/server-configuration-parameters/settings#database_catalog_unused_dir_hide_timeout_sec))
    and this directory was not modified for last
    [`database_catalog_unused_dir_rm_timeout_sec`]/operations/server-configuration-parameters/settings#database_catalog_unused_dir_rm_timeout_sec) seconds, the task will remove this directory.
    It also works for directories that clickhouse-server does not
    expect to see inside `store/`.

    :::note
    A value of `0` means "never". The default value corresponds to 30 days.
    :::
    )", 0) \
    DECLARE(UInt64, database_catalog_unused_dir_cleanup_period_sec, 24 * 60 * 60, R"(
    Parameter of a task that cleans up garbage from `store/` directory.
    Sets scheduling period of the task.

    :::note
    A value of `0` means "never". The default value corresponds to 1 day.
    :::
    )", 0) \
    DECLARE(UInt64, database_catalog_drop_error_cooldown_sec, 5, R"(In case of a failed table drop, ClickHouse will wait for this time-out before retrying the operation.)", 0) \
    DECLARE(UInt64, database_catalog_drop_table_concurrency, 16, R"(The size of the threadpool used for dropping tables.)", 0) \
    \
    \
    DECLARE(UInt64, max_concurrent_queries, 0, R"(
    Limit on total number of concurrently executed queries. Note that limits on `INSERT` and `SELECT` queries, and on the maximum number of queries for users must also be considered.

    See also:
    - [`max_concurrent_insert_queries`](/operations/server-configuration-parameters/settings#max_concurrent_insert_queries)
    - [`max_concurrent_select_queries`](/operations/server-configuration-parameters/settings#max_concurrent_select_queries)
    - [`max_concurrent_queries_for_all_users`](/operations/settings/settings#max_concurrent_queries_for_all_users)

    :::note

    A value of `0` (default) means unlimited.

    This setting can be modified at runtime and will take effect immediately. Queries that are already running will remain unchanged.
    :::
    )", 0) \
    DECLARE(UInt64, max_concurrent_insert_queries, 0, R"(
    Limit on total number of concurrent insert queries.

    :::note

    A value of `0` (default) means unlimited.

    This setting can be modified at runtime and will take effect immediately. Queries that are already running will remain unchanged.
    :::
    )", 0) \
    DECLARE(UInt64, max_concurrent_select_queries, 0, R"(
    Limit on total number of concurrently select queries.

    :::note

    A value of `0` (default) means unlimited.

    This setting can be modified at runtime and will take effect immediately. Queries that are already running will remain unchanged.
    :::
    )", 0) \
    DECLARE(UInt64, max_waiting_queries, 0, R"(
    Limit on total number of concurrently waiting queries.
    Execution of a waiting query is blocked while required tables are loading asynchronously (see [`async_load_databases`](/operations/server-configuration-parameters/settings#async_load_databases).

    :::note
    Waiting queries are not counted when limits controlled by the following settings are checked:

    - [`max_concurrent_queries`](/operations/server-configuration-parameters/settings#max_concurrent_queries)
    - [`max_concurrent_insert_queries`](/operations/server-configuration-parameters/settings#max_concurrent_insert_queries)
    - [`max_concurrent_select_queries`](/operations/server-configuration-parameters/settings#max_concurrent_select_queries)
    - [`max_concurrent_queries_for_user`](/operations/settings/settings#max_concurrent_queries_for_user)
    - [`max_concurrent_queries_for_all_users`](/operations/settings/settings#max_concurrent_queries_for_all_users)

    This correction is done to avoid hitting these limits just after server startup.
    :::

    :::note

    A value of `0` (default) means unlimited.

    This setting can be modified at runtime and will take effect immediately. Queries that are already running will remain unchanged.
    :::
    )", 0) \
    \
    DECLARE(Double, cache_size_to_ram_max_ratio, 0.5, R"(Set cache size to RAM max ratio. Allows lowering the cache size on low-memory systems.)", 0) \
    DECLARE(String, uncompressed_cache_policy, DEFAULT_UNCOMPRESSED_CACHE_POLICY, R"(Uncompressed cache policy name.)", 0) \
    DECLARE(UInt64, uncompressed_cache_size, DEFAULT_UNCOMPRESSED_CACHE_MAX_SIZE, R"(
    Maximum size (in bytes) for uncompressed data used by table engines from the MergeTree family.

    There is one shared cache for the server. Memory is allocated on demand. The cache is used if the option `use_uncompressed_cache` is enabled.

    The uncompressed cache is advantageous for very short queries in individual cases.

    :::note
    A value of `0` means disabled.

    This setting can be modified at runtime and will take effect immediately.
    :::
    )", 0) \
    DECLARE(Double, uncompressed_cache_size_ratio, DEFAULT_UNCOMPRESSED_CACHE_SIZE_RATIO, R"(The size of the protected queue (in case of SLRU policy) in the uncompressed cache relative to the cache's total size.)", 0) \
    DECLARE(String, mark_cache_policy, DEFAULT_MARK_CACHE_POLICY, R"(Mark cache policy name.)", 0) \
    DECLARE(UInt64, mark_cache_size, DEFAULT_MARK_CACHE_MAX_SIZE, R"(
    Maximum size of cache for marks (index of [`MergeTree`](/engines/table-engines/mergetree-family) family of tables).

    :::note
    This setting can be modified at runtime and will take effect immediately.
    :::
    )", 0) \
    DECLARE(Double, mark_cache_size_ratio, DEFAULT_MARK_CACHE_SIZE_RATIO, R"(The size of the protected queue (in case of SLRU policy) in the mark cache relative to the cache's total size.)", 0) \
    DECLARE(Double, mark_cache_prewarm_ratio, 0.95, R"(The ratio of total size of mark cache to fill during prewarm.)", 0) \
    DECLARE(String, primary_index_cache_policy, DEFAULT_PRIMARY_INDEX_CACHE_POLICY, R"(Primary index cache policy name.)", 0) \
    DECLARE(UInt64, primary_index_cache_size, DEFAULT_PRIMARY_INDEX_CACHE_MAX_SIZE, R"(Maximum size of cache for primary index (index of MergeTree family of tables).)", 0) \
    DECLARE(Double, primary_index_cache_size_ratio, DEFAULT_PRIMARY_INDEX_CACHE_SIZE_RATIO, R"(The size of the protected queue (in case of SLRU policy) in the primary index cache relative to the cache's total size.)", 0) \
    DECLARE(Double, primary_index_cache_prewarm_ratio, 0.95, R"(The ratio of total size of mark cache to fill during prewarm.)", 0) \
    DECLARE(String, iceberg_metadata_files_cache_policy, DEFAULT_ICEBERG_METADATA_CACHE_POLICY, "Iceberg metadata cache policy name.", 0) \
    DECLARE(UInt64, iceberg_metadata_files_cache_size, DEFAULT_ICEBERG_METADATA_CACHE_MAX_SIZE, "Maximum size of iceberg metadata cache in bytes. Zero means disabled.", 0) \
    DECLARE(UInt64, iceberg_metadata_files_cache_max_entries, DEFAULT_ICEBERG_METADATA_CACHE_MAX_ENTRIES, "Maximum size of iceberg metadata files cache in entries. Zero means disabled.", 0) \
    DECLARE(Double, iceberg_metadata_files_cache_size_ratio, DEFAULT_ICEBERG_METADATA_CACHE_SIZE_RATIO, "The size of the protected queue (in case of SLRU policy) in the iceberg metadata cache relative to the cache's total size.", 0) \
    DECLARE(String, allowed_disks_for_table_engines, "", "List of disks allowed for use with Iceberg", 0) \
    DECLARE(String, vector_similarity_index_cache_policy, DEFAULT_VECTOR_SIMILARITY_INDEX_CACHE_POLICY, "Vector similarity index cache policy name.", 0) \
    DECLARE(UInt64, vector_similarity_index_cache_size, DEFAULT_VECTOR_SIMILARITY_INDEX_CACHE_MAX_SIZE, R"(Size of cache for vector similarity indexes. Zero means disabled.

    :::note
    This setting can be modified at runtime and will take effect immediately.
    :::)", 0) \
    DECLARE(UInt64, vector_similarity_index_cache_max_entries, DEFAULT_VECTOR_SIMILARITY_INDEX_CACHE_MAX_ENTRIES, "Size of cache for vector similarity index in entries. Zero means disabled.", 0) \
    DECLARE(Double, vector_similarity_index_cache_size_ratio, DEFAULT_VECTOR_SIMILARITY_INDEX_CACHE_SIZE_RATIO, "The size of the protected queue (in case of SLRU policy) in the vector similarity index cache relative to the cache's total size.", 0) \
    DECLARE(String, text_index_dictionary_block_cache_policy, DEFAULT_TEXT_INDEX_DICTIONARY_BLOCK_CACHE_POLICY, "Text index dictionary block cache policy name.", 0) \
    DECLARE(UInt64, text_index_dictionary_block_cache_size, DEFAULT_TEXT_INDEX_DICTIONARY_BLOCK_CACHE_MAX_SIZE, R"(Size of cache for text index dictionary blocks. Zero means disabled.

    :::note
    This setting can be modified at runtime and will take effect immediately.
    :::)", 0) \
    DECLARE(UInt64, text_index_dictionary_block_cache_max_entries, DEFAULT_TEXT_INDEX_DICTIONARY_BLOCK_CACHE_MAX_ENTRIES, "Size of cache for text index dictionary block in entries. Zero means disabled.", 0) \
    DECLARE(Double, text_index_dictionary_block_cache_size_ratio, DEFAULT_TEXT_INDEX_DICTIONARY_BLOCK_CACHE_SIZE_RATIO, "The size of the protected queue (in case of SLRU policy) in the text index dictionary block cache relative to the cache's total size.", 0) \
    DECLARE(String, text_index_header_cache_policy, DEFAULT_TEXT_INDEX_HEADER_CACHE_POLICY, "Text index header cache policy name.", 0) \
    DECLARE(UInt64, text_index_header_cache_size, DEFAULT_TEXT_INDEX_HEADER_CACHE_MAX_SIZE, R"(Size of cache for text index headers. Zero means disabled.

    :::note
    This setting can be modified at runtime and will take effect immediately.
    :::)", 0) \
    DECLARE(UInt64, text_index_header_cache_max_entries, DEFAULT_TEXT_INDEX_HEADER_CACHE_MAX_ENTRIES, "Size of cache for text index header in entries. Zero means disabled.", 0) \
    DECLARE(Double, text_index_header_cache_size_ratio, DEFAULT_TEXT_INDEX_HEADER_CACHE_SIZE_RATIO, "The size of the protected queue (in case of SLRU policy) in the text index header cache relative to the cache's total size.", 0) \
    DECLARE(String, text_index_postings_cache_policy, DEFAULT_TEXT_INDEX_POSTINGS_CACHE_POLICY, "Text index posting list cache policy name.", 0) \
    DECLARE(UInt64, text_index_postings_cache_size, DEFAULT_TEXT_INDEX_POSTINGS_CACHE_MAX_SIZE, R"(Size of cache for text index posting lists. Zero means disabled.

    :::note
    This setting can be modified at runtime and will take effect immediately.
    :::)", 0) \
    DECLARE(UInt64, text_index_postings_cache_max_entries, DEFAULT_TEXT_INDEX_POSTINGS_CACHE_MAX_ENTRIES, "Size of cache for text index posting list in entries. Zero means disabled.", 0) \
    DECLARE(Double, text_index_postings_cache_size_ratio, DEFAULT_TEXT_INDEX_POSTINGS_CACHE_SIZE_RATIO, "The size of the protected queue (in case of SLRU policy) in the text index posting list cache relative to the cache's total size.", 0) \
    DECLARE(String, index_uncompressed_cache_policy, DEFAULT_INDEX_UNCOMPRESSED_CACHE_POLICY, R"(Secondary index uncompressed cache policy name.)", 0) \
    DECLARE(UInt64, index_uncompressed_cache_size, DEFAULT_INDEX_UNCOMPRESSED_CACHE_MAX_SIZE, R"(
    Maximum size of cache for uncompressed blocks of `MergeTree` indices.

    :::note
    A value of `0` means disabled.

    This setting can be modified at runtime and will take effect immediately.
    :::
    )", 0) \
    DECLARE(Double, index_uncompressed_cache_size_ratio, DEFAULT_INDEX_UNCOMPRESSED_CACHE_SIZE_RATIO, R"(The size of the protected queue (in case of SLRU policy) in the secondary index uncompressed cache relative to the cache's total size.)", 0) \
    DECLARE(String, index_mark_cache_policy, DEFAULT_INDEX_MARK_CACHE_POLICY, R"(Secondary index mark cache policy name.)", 0) \
    DECLARE(UInt64, index_mark_cache_size, DEFAULT_INDEX_MARK_CACHE_MAX_SIZE, R"(
    Maximum size of cache for index marks.

    :::note

    A value of `0` means disabled.

    This setting can be modified at runtime and will take effect immediately.
    :::
    )", 0) \
    DECLARE(Double, index_mark_cache_size_ratio, DEFAULT_INDEX_MARK_CACHE_SIZE_RATIO, R"(The size of the protected queue (in case of SLRU policy) in the secondary index mark cache relative to the cache's total size.)", 0) \
    DECLARE(UInt64, page_cache_history_window_ms, 1000, "Delay before freed memory can be used by userspace page cache.", 0) \
    DECLARE(String, page_cache_policy, DEFAULT_PAGE_CACHE_POLICY, "Userspace page cache policy name.", 0) \
    DECLARE(Double, page_cache_size_ratio, DEFAULT_PAGE_CACHE_SIZE_RATIO, "The size of the protected queue in the userspace page cache relative to the cache's total size.", 0) \
    DECLARE(UInt64, page_cache_min_size, DEFAULT_PAGE_CACHE_MIN_SIZE, "Minimum size of the userspace page cache.", 0) \
    DECLARE(UInt64, page_cache_max_size, DEFAULT_PAGE_CACHE_MAX_SIZE, "Maximum size of the userspace page cache. Set to 0 to disable the cache. If greater than page_cache_min_size, the cache size will be continuously adjusted within this range, to use most of the available memory while keeping the total memory usage below the limit (max_server_memory_usage[_to_ram_ratio]).", 0) \
    DECLARE(Double, page_cache_free_memory_ratio, 0.15, "Fraction of the memory limit to keep free from the userspace page cache. Analogous to Linux min_free_kbytes setting.", 0) \
    DECLARE(UInt64, page_cache_shards, 4, "Stripe userspace page cache over this many shards to reduce mutex contention. Experimental, not likely to improve performance.", 0) \
    DECLARE(UInt64, mmap_cache_size, DEFAULT_MMAP_CACHE_MAX_SIZE, R"(
    This setting allows avoiding frequent open/close calls (which are very expensive due to consequent page faults), and to reuse mappings from several threads and queries. The setting value is the number of mapped regions (usually equal to the number of mapped files).

    The amount of data in mapped files can be monitored in the following system tables with the following metrics:

    - `MMappedFiles`/`MMappedFileBytes`/`MMapCacheCells` in [`system.metrics`](/operations/system-tables/metrics), [`system.metric_log`](/operations/system-tables/metric_log)
    - `CreatedReadBufferMMap`/`CreatedReadBufferMMapFailed`/`MMappedFileCacheHits`/`MMappedFileCacheMisses` in [`system.events`](/operations/system-tables/events), [`system.processes`](/operations/system-tables/processes), [`system.query_log`](/operations/system-tables/query_log), [`system.query_thread_log`](/operations/system-tables/query_thread_log), [`system.query_views_log`](/operations/system-tables/query_views_log)

    :::note
    The amount of data in mapped files does not consume memory directly and is not accounted for in query or server memory usage — because this memory can be discarded similar to the OS page cache. The cache is dropped (the files are closed) automatically on the removal of old parts in tables of the MergeTree family, also it can be dropped manually by the `SYSTEM DROP MMAP CACHE` query.

    This setting can be modified at runtime and will take effect immediately.
    :::
    )", 0) \
    DECLARE(UInt64, compiled_expression_cache_size, DEFAULT_COMPILED_EXPRESSION_CACHE_MAX_SIZE, R"(Sets the cache size (in bytes) for [compiled expressions](../../operations/caches.md).)", 0) \
    \
    DECLARE(UInt64, compiled_expression_cache_elements_size, DEFAULT_COMPILED_EXPRESSION_CACHE_MAX_ENTRIES, R"(Sets the cache size (in elements) for [compiled expressions](../../operations/caches.md).)", 0) \
    DECLARE(String, query_condition_cache_policy, DEFAULT_QUERY_CONDITION_CACHE_POLICY, "Query condition cache policy name.", 0) \
    DECLARE(UInt64, query_condition_cache_size, DEFAULT_QUERY_CONDITION_CACHE_MAX_SIZE, R"(
    Maximum size of the query condition cache.
    :::note
    This setting can be modified at runtime and will take effect immediately.
    :::
    )", 0) \
    DECLARE(Double, query_condition_cache_size_ratio, DEFAULT_QUERY_CONDITION_CACHE_SIZE_RATIO, "The size of the protected queue (in case of SLRU policy) in the query condition cache relative to the cache's total size.", 0) \
    \
    DECLARE(Bool, disable_internal_dns_cache, false, "Disables the internal DNS cache. Recommended for operating ClickHouse in systems with frequently changing infrastructure such as Kubernetes.", 0) \
    DECLARE(UInt64, dns_cache_max_entries, 10000, R"(Internal DNS cache max entries.)", 0) \
    DECLARE(Int32, dns_cache_update_period, 15, "Internal DNS cache update period in seconds.", 0) \
    DECLARE(UInt32, dns_max_consecutive_failures, 5, R"(
    Stop further attempts to update a hostname's DNS cache after this number of consecutive failures. The information still remains in the DNS cache. Zero means unlimited.

    **See also**

    - [`SYSTEM DROP DNS CACHE`](../../sql-reference/statements/system#drop-dns-cache)
    )", 0) \
    DECLARE(Bool, dns_allow_resolve_names_to_ipv4, true, "Allows resolve names to ipv4 addresses.", 0) \
    DECLARE(Bool, dns_allow_resolve_names_to_ipv6, true, "Allows resolve names to ipv6 addresses.", 0) \
    \
    DECLARE(UInt64, max_table_size_to_drop, 50000000000lu, R"(
    Restriction on deleting tables.

    If the size of a [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) table exceeds `max_table_size_to_drop` (in bytes), you can't delete it using a [`DROP`](../../sql-reference/statements/drop.md) query or [`TRUNCATE`](../../sql-reference/statements/truncate.md) query.

    :::note
    A value of `0` means that you can delete all tables without any restrictions.

    This setting does not require a restart of the ClickHouse server to apply. Another way to disable the restriction is to create the `<clickhouse-path>/flags/force_drop_table` file.
    :::

    **Example**

    ```xml
    <max_table_size_to_drop>0</max_table_size_to_drop>
    ```
    )", 0) \
    DECLARE(UInt64, max_partition_size_to_drop, 50000000000lu, R"(
    Restriction on dropping partitions.

    If the size of a [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) table exceeds [`max_partition_size_to_drop`](#max_partition_size_to_drop) (in bytes), you can't drop a partition using a [DROP PARTITION](../../sql-reference/statements/alter/partition.md#drop-partitionpart) query.
    This setting does not require a restart of the ClickHouse server to apply. Another way to disable the restriction is to create the `<clickhouse-path>/flags/force_drop_table` file.

    :::note
    The value `0` means that you can drop partitions without any restrictions.

    This limitation does not restrict drop table and truncate table, see [max_table_size_to_drop](/operations/settings/settings#max_table_size_to_drop)
    :::

    **Example**

    ```xml
    <max_partition_size_to_drop>0</max_partition_size_to_drop>
    ```
    )", 0) \
    DECLARE(UInt64, max_named_collection_num_to_warn, 1000lu, R"(
    If the number of named collections exceeds the specified value, clickhouse server will add warning messages to `system.warnings` table.

    **Example**

    ```xml
    <max_named_collection_num_to_warn>400</max_named_collection_num_to_warn>
    ```
    )", 0) \
    DECLARE(UInt64, max_table_num_to_warn, 5000lu, R"(
    If the number of attached tables exceeds the specified value, clickhouse server will add warning messages to `system.warnings` table.

    **Example**

    ```xml
    <max_table_num_to_warn>400</max_table_num_to_warn>
    ```
    )", 0) \
    DECLARE(UInt64, max_pending_mutations_to_warn, 500lu, R"(
    If the number of pending mutations exceeds the specified value, clickhouse server will add warning messages to `system.warnings` table.

    **Example**

    ```xml
    <max_pending_mutations_to_warn>400</max_pending_mutations_to_warn>
    ```
    )", 0) \
    DECLARE(UInt64, max_pending_mutations_execution_time_to_warn, 86400lu, R"(
    If any of the pending mutations exceeds the specified value in seconds, clickhouse server will add warning messages to `system.warnings` table.

    **Example**

    ```xml
    <max_pending_mutations_execution_time_to_warn>10000</max_pending_mutations_execution_time_to_warn>
    ```
    )", 0) \
    DECLARE(UInt64, max_view_num_to_warn, 10000lu, R"(
    If the number of attached views exceeds the specified value, clickhouse server will add warning messages to `system.warnings` table.

    **Example**

    ```xml
    <max_view_num_to_warn>400</max_view_num_to_warn>
    ```
    )", 0) \
    DECLARE(UInt64, max_dictionary_num_to_warn, 1000lu, R"(
    If the number of attached dictionaries exceeds the specified value, clickhouse server will add warning messages to `system.warnings` table.

    **Example**

    ```xml
    <max_dictionary_num_to_warn>400</max_dictionary_num_to_warn>
    ```
    )", 0) \
    DECLARE(UInt64, max_database_num_to_warn, 1000lu, R"(
    If the number of attached databases exceeds the specified value, clickhouse server will add warning messages to `system.warnings` table.

    **Example**

    ```xml
    <max_database_num_to_warn>50</max_database_num_to_warn>
    ```
    )", 0) \
    DECLARE(UInt64, max_part_num_to_warn, 100000lu, R"(
    If the number of active parts exceeds the specified value, clickhouse server will add warning messages to `system.warnings` table.

    **Example**

    ```xml
    <max_part_num_to_warn>400</max_part_num_to_warn>
    ```
    )", 0) \
    DECLARE(UInt64, max_named_collection_num_to_throw, 0lu, R"(
    If number of named collections is greater than this value, server will throw an exception.

    :::note
    A value of `0` means no limitation.
    :::

    **Example**
    ```xml
    <max_named_collection_num_to_throw>400</max_named_collection_num_to_throw>
    ```
    )", 0) \
    DECLARE(UInt64, max_table_num_to_throw, 0lu, R"(
    If number of tables is greater than this value, server will throw an exception.

    The following tables are not counted:
    - view
    - remote
    - dictionary
    - system

    Only counts tables for database engines:
    - Atomic
    - Ordinary
    - Replicated
    - Lazy

    :::note
    A value of `0` means no limitation.
    :::

    **Example**
    ```xml
    <max_table_num_to_throw>400</max_table_num_to_throw>
    ```
    )", 0) \
    DECLARE(UInt64, max_replicated_table_num_to_throw, 0lu, R"(
    If the number of replicated tables is greater than this value, the server will throw an exception.

    Only counts tables for database engines:
    - Atomic
    - Ordinary
    - Replicated
    - Lazy

    :::note
    A value of `0` means no limitation.
    :::

    **Example**
    ```xml
    <max_replicated_table_num_to_throw>400</max_replicated_table_num_to_throw>
    ```
    )", 0) \
    DECLARE(UInt64, max_dictionary_num_to_throw, 0lu, R"(
    If the number of dictionaries is greater than this value, the server will throw an exception.

    Only counts tables for database engines:
    - Atomic
    - Ordinary
    - Replicated
    - Lazy

    :::note
    A value of `0` means no limitation.
    :::

    **Example**
    ```xml
    <max_dictionary_num_to_throw>400</max_dictionary_num_to_throw>
    ```
    )", 0) \
    DECLARE(UInt64, max_view_num_to_throw, 0lu, R"(
    If the number of views is greater than this value, the server will throw an exception.

    Only counts tables for database engines:
    - Atomic
    - Ordinary
    - Replicated
    - Lazy

    :::note
    A value of `0` means no limitation.
    :::

    **Example**
    ```xml
    <max_view_num_to_throw>400</max_view_num_to_throw>
    ```
    )", 0) \
    DECLARE(UInt64, max_database_num_to_throw, 0lu, R"(If number of databases is greater than this value, server will throw an exception. 0 means no limitation.)", 0) \
    DECLARE(UInt64, max_authentication_methods_per_user, 100, R"(
    The maximum number of authentication methods a user can be created with or altered to.
    Changing this setting does not affect existing users. Create/alter authentication-related queries will fail if they exceed the limit specified in this setting.
    Non authentication create/alter queries will succeed.

    :::note
    A value of `0` means unlimited.
    :::
    )", 0) \
    DECLARE(UInt64, concurrent_threads_soft_limit_num, 0, R"(
    The maximum number of query processing threads, excluding threads for retrieving data from remote servers, allowed to run all queries. This is not a hard limit. In case if the limit is reached the query will still get at least one thread to run. Query can upscale to desired number of threads during execution if more threads become available.

    :::note
    A value of `0` (default) means unlimited.
    :::
    )", 0) \
    DECLARE(UInt64, concurrent_threads_soft_limit_ratio_to_cores, 0, "Same as [`concurrent_threads_soft_limit_num`](#concurrent_threads_soft_limit_num), but with ratio to cores.", 0) \
    DECLARE(String, concurrent_threads_scheduler, "max_min_fair", R"(
The policy on how to perform a scheduling of CPU slots specified by `concurrent_threads_soft_limit_num` and `concurrent_threads_soft_limit_ratio_to_cores`. Algorithm used to govern how limited number of CPU slots are distributed among concurrent queries. Scheduler may be changed at runtime without server restart.

    Possible values:

    - `round_robin` — Every query with setting `use_concurrency_control` = 1 allocates up to `max_threads` CPU slots. One slot per thread. On contention CPU slot are granted to queries using round-robin. Note that the first slot is granted unconditionally, which could lead to unfairness and increased latency of queries having high `max_threads` in presence of high number of queries with `max_threads` = 1.
    - `fair_round_robin` — Every query with setting `use_concurrency_control` = 1 allocates up to `max_threads - 1` CPU slots. Variation of `round_robin` that does not require a CPU slot for the first thread of every query. This way queries having `max_threads` = 1 do not require any slot and could not unfairly allocate all slots. There are no slots granted unconditionally.
    - `max_min_fair` — Every query with setting `use_concurrency_control` = 1 allocates up to `max_threads - 1` CPU slots. Similar to `fair_round_robin`, but released slots are always granted to the query with the minimum number of currently allocated slots. This provides better fairness under high oversubscription, where many queries compete for limited CPU slots. Short-running queries are not penalized by long-running queries that have accumulated more slots over time.
    )", 0) \
    DECLARE(UInt64, background_pool_size, 16, R"(
    Sets the number of threads performing background merges and mutations for tables with MergeTree engines.

    :::note
    - This setting could also be applied at server startup from the `default` profile configuration for backward compatibility at the ClickHouse server start.
    - You can only increase the number of threads at runtime.
    - To lower the number of threads you have to restart the server.
    - By adjusting this setting, you manage CPU and disk load.
    :::

    :::danger
    Smaller pool size utilizes less CPU and disk resources, but background processes advance slower which might eventually impact query performance.
    :::

    Before changing it, please also take a look at related MergeTree settings, such as:
    - [`number_of_free_entries_in_pool_to_lower_max_size_of_merge`](../../operations/settings/merge-tree-settings.md#number_of_free_entries_in_pool_to_lower_max_size_of_merge).
    - [`number_of_free_entries_in_pool_to_execute_mutation`](../../operations/settings/merge-tree-settings.md#number_of_free_entries_in_pool_to_execute_mutation).
    - [`number_of_free_entries_in_pool_to_execute_optimize_entire_partition`](/operations/settings/merge-tree-settings#number_of_free_entries_in_pool_to_execute_optimize_entire_partition)

    **Example**

    ```xml
    <background_pool_size>16</background_pool_size>
    ```
    )", 0) \
    DECLARE(Float, background_merges_mutations_concurrency_ratio, 2, R"(
    Sets a ratio between the number of threads and the number of background merges and mutations that can be executed concurrently.

    For example, if the ratio equals to 2 and [`background_pool_size`](/operations/server-configuration-parameters/settings#background_pool_size) is set to 16 then ClickHouse can execute 32 background merges concurrently. This is possible, because background operations could be suspended and postponed. This is needed to give small merges more execution priority.

    :::note
    You can only increase this ratio at runtime. To lower it you have to restart the server.

    As with the [`background_pool_size`](/operations/server-configuration-parameters/settings#background_pool_size) setting [`background_merges_mutations_concurrency_ratio`](/operations/server-configuration-parameters/settings#background_merges_mutations_concurrency_ratio) could be applied from the `default` profile for backward compatibility.
    :::
    )", 0) \
    DECLARE(String, background_merges_mutations_scheduling_policy, "round_robin", R"(
    The policy on how to perform a scheduling for background merges and mutations. Possible values are: `round_robin` and `shortest_task_first`.

    Algorithm used to select next merge or mutation to be executed by background thread pool. Policy may be changed at runtime without server restart.
    Could be applied from the `default` profile for backward compatibility.

    Possible values:

    - `round_robin` — Every concurrent merge and mutation is executed in round-robin order to ensure starvation-free operation. Smaller merges are completed faster than bigger ones just because they have fewer blocks to merge.
    - `shortest_task_first` — Always execute smaller merge or mutation. Merges and mutations are assigned priorities based on their resulting size. Merges with smaller sizes are strictly preferred over bigger ones. This policy ensures the fastest possible merge of small parts but can lead to indefinite starvation of big merges in partitions heavily overloaded by `INSERT`s.
    )", 0) \
    DECLARE(UInt64, background_move_pool_size, 8, R"(The maximum number of threads that will be used for moving data parts to another disk or volume for *MergeTree-engine tables in a background.)", 0) \
    DECLARE(UInt64, background_fetches_pool_size, 16, R"(The maximum number of threads that will be used for fetching data parts from another replica for [*MergeTree-engine](/engines/table-engines/mergetree-family) tables in the background.)", 0) \
    DECLARE(UInt64, background_common_pool_size, 8, R"(The maximum number of threads that will be used for performing a variety of operations (mostly garbage collection) for [*MergeTree-engine](/engines/table-engines/mergetree-family) tables in the background.)", 0) \
    DECLARE(UInt64, background_buffer_flush_schedule_pool_size, 16, R"(The maximum number of threads that will be used for performing flush operations for [Buffer-engine tables](/engines/table-engines/special/buffer) in the background.)", 0) \
    DECLARE(UInt64, background_schedule_pool_size, 512, R"(The maximum number of threads that will be used for constantly executing some lightweight periodic operations for replicated tables, Kafka streaming, and DNS cache updates.)", 0) \
    DECLARE(Float, background_schedule_pool_max_parallel_tasks_per_type_ratio, 0.8f, R"(The maximum ratio of threads in the pool that can execute tasks of the same type simultaneously.)", 0) \
    DECLARE(UInt64, background_message_broker_schedule_pool_size, 16, R"(The maximum number of threads that will be used for executing background operations for message streaming.)", 0) \
    DECLARE(UInt64, background_distributed_schedule_pool_size, 16, R"(The maximum number of threads that will be used for executing distributed sends.)", 0) \
    DECLARE(UInt64, tables_loader_foreground_pool_size, 0, R"(
    Sets the number of threads performing load jobs in foreground pool. The foreground pool is used for loading table synchronously before server start listening on a port and for loading tables that are waited for. Foreground pool has higher priority than background pool. It means that no job starts in background pool while there are jobs running in foreground pool.

    :::note
    A value of `0` means all available CPUs will be used.
    :::
    )", 0) \
    DECLARE(UInt64, tables_loader_background_pool_size, 0, R"(
    Sets the number of threads performing asynchronous load jobs in background pool. The background pool is used for loading tables asynchronously after server start in case there are no queries waiting for the table. It could be beneficial to keep low number of threads in background pool if there are a lot of tables. It will reserve CPU resources for concurrent query execution.

    :::note
    A value of `0` means all available CPUs will be used.
    :::
    )", 0) \
    DECLARE(Bool, async_load_databases, true, R"(
    Asynchronous loading of databases and tables.

    - If `true` all non-system databases with `Ordinary`, `Atomic` and `Replicated` engine will be loaded asynchronously after the ClickHouse server start up. See `system.asynchronous_loader` table, `tables_loader_background_pool_size` and `tables_loader_foreground_pool_size` server settings. Any query that tries to access a table, that is not yet loaded, will wait for exactly this table to be started up. If load job fails, query will rethrow an error (instead of shutting down the whole server in case of `async_load_databases = false`). The table that is waited for by at least one query will be loaded with higher priority. DDL queries on a database will wait for exactly that database to be started up. Also consider setting a limit `max_waiting_queries` for the total number of waiting queries.
    - If `false`, all databases are loaded when the server starts.

    **Example**

    ```xml
    <async_load_databases>true</async_load_databases>
    ```
    )", 0) \
    DECLARE(Bool, async_load_system_database, false, R"(
    Asynchronous loading of system tables. Helpful if there is a high amount of log tables and parts in the `system` database. Independent of the `async_load_databases` setting.

    - If set to `true`, all system databases with `Ordinary`, `Atomic`, and `Replicated` engines will be loaded asynchronously after the ClickHouse server starts. See `system.asynchronous_loader` table, `tables_loader_background_pool_size` and `tables_loader_foreground_pool_size` server settings. Any query that tries to access a system table, that is not yet loaded, will wait for exactly this table to be started up. The table that is waited for by at least one query will be loaded with higher priority. Also consider setting the `max_waiting_queries` setting to limit the total number of waiting queries.
    - If set to `false`, system database loads before server start.

    **Example**

    ```xml
    <async_load_system_database>true</async_load_system_database>
    ```
    )", 0) \
    DECLARE(Bool, display_secrets_in_show_and_select, false, R"(
    Enables or disables showing secrets in `SHOW` and `SELECT` queries for tables, databases, table functions, and dictionaries.

    User wishing to see secrets must also have
    [`format_display_secrets_in_show_and_select` format setting](../settings/formats#format_display_secrets_in_show_and_select)
    turned on and a
    [`displaySecretsInShowAndSelect`](/sql-reference/statements/grant#displaysecretsinshowandselect) privilege.

    Possible values:

    - `0` — Disabled.
    - `1` — Enabled.
    )", 0) \
    DECLARE(Seconds, keep_alive_timeout, DEFAULT_HTTP_KEEP_ALIVE_TIMEOUT, R"(
    The number of seconds that ClickHouse waits for incoming requests for HTTP protocol before closing the connection.

    **Example**

    ```xml
    <keep_alive_timeout>10</keep_alive_timeout>
    ```
    )", 0) \
    DECLARE(UInt64, max_keep_alive_requests, 10000, R"(
    Maximal number of requests through a single keep-alive connection until it will be closed by ClickHouse server.

    **Example**

    ```xml
    <max_keep_alive_requests>10</max_keep_alive_requests>
    ```
    )", 0) \
    DECLARE(Seconds, replicated_fetches_http_connection_timeout, 0, R"(HTTP connection timeout for part fetch requests. Inherited from default profile `http_connection_timeout` if not set explicitly.)", 0) \
    DECLARE(Seconds, replicated_fetches_http_send_timeout, 0, R"(HTTP send timeout for part fetch requests. Inherited from default profile `http_send_timeout` if not set explicitly.)", 0) \
    DECLARE(Seconds, replicated_fetches_http_receive_timeout, 0, R"(HTTP receive timeout for fetch part requests. Inherited from default profile `http_receive_timeout` if not set explicitly.)", 0) \
    DECLARE(UInt64, total_memory_profiler_step, 0, R"(Whenever server memory usage becomes larger than every next step in number of bytes the memory profiler will collect the allocating stack trace. Zero means disabled memory profiler. Values lower than a few megabytes will slow down server.)", 0) \
    DECLARE(Double, total_memory_tracker_sample_probability, 0, R"(
    Allows to collect random allocations and de-allocations and writes them in the [system.trace_log](../../operations/system-tables/trace_log.md) system table with `trace_type` equal to a `MemorySample` with the specified probability. The probability is for every allocation or deallocations, regardless of the size of the allocation. Note that sampling happens only when the amount of untracked memory exceeds the untracked memory limit (default value is `4` MiB). It can be lowered if [total_memory_profiler_step](/operations/server-configuration-parameters/settings#total_memory_profiler_step) is lowered. You can set `total_memory_profiler_step` equal to `1` for extra fine-grained sampling.

    Possible values:

    - Positive double.
    - `0` — Writing of random allocations and de-allocations in the `system.trace_log` system table is disabled.
    )", 0) \
    DECLARE(UInt64, total_memory_profiler_sample_min_allocation_size, 0, R"(Collect random allocations of size greater or equal than specified value with probability equal to `total_memory_profiler_sample_probability`. 0 means disabled. You may want to set 'max_untracked_memory' to 0 to make this threshold to work as expected.)", 0) \
    DECLARE(UInt64, total_memory_profiler_sample_max_allocation_size, 0, R"(Collect random allocations of size less or equal than specified value with probability equal to `total_memory_profiler_sample_probability`. 0 means disabled. You may want to set 'max_untracked_memory' to 0 to make this threshold to work as expected.)", 0) \
    DECLARE(Bool, validate_tcp_client_information, false, R"(Determines whether validation of client information is enabled when a query packet is received.

    By default, it is `false`:

    ```xml
    <validate_tcp_client_information>false</validate_tcp_client_information>
    ```)", 0) \
    DECLARE(Bool, storage_metadata_write_full_object_key, true, R"(Write disk metadata files with VERSION_FULL_OBJECT_KEY format. This is enabled by default. The setting is deprecated.)", SettingsTierType::OBSOLETE) \
    DECLARE(UInt64, max_materialized_views_count_for_table, 0, R"(
    A limit on the number of materialized views attached to a table.

    :::note
    Only directly dependent views are considered here, and the creation of one view on top of another view is not considered.
    :::
    )", 0) \
    DECLARE(UInt32, max_database_replicated_create_table_thread_pool_size, 1, R"(The number of threads to create tables during replica recovery in DatabaseReplicated. Zero means number of threads equal number of cores.)", 0) \
    DECLARE(Bool, database_replicated_allow_detach_permanently, true, R"(Allow detaching tables permanently in Replicated databases)", 0) \
    DECLARE(Bool, database_replicated_drop_broken_tables, false, R"(Drop unexpected tables from Replicated databases instead of moving them to a separate local database)", 0) \
    DECLARE(Bool, distributed_ddl_use_initial_user_and_roles, false, R"(If enabled, ON CLUSTER queries will preserve and use the initiator's user and roles for execution on remote shards. This ensures consistent access control across the cluster but requires that the user and roles exist on all nodes.)", 0) \
    DECLARE(String, default_replica_path, "/clickhouse/tables/{uuid}/{shard}", R"(
    The path to the table in ZooKeeper.

    **Example**

    ```xml
    <default_replica_path>/clickhouse/tables/{uuid}/{shard}</default_replica_path>
    ```
    )", 0) \
    DECLARE(String, default_replica_name, "{replica}", R"(
    The replica name in ZooKeeper.

    **Example**

    ```xml
    <default_replica_name>{replica}</default_replica_name>
    ```
    )", 0) \
    DECLARE(UInt64, disk_connections_soft_limit, 5000, R"(Connections above this limit have significantly shorter time to live. The limit applies to the disks connections.)", 0) \
    DECLARE(UInt64, disk_connections_warn_limit, 8000, R"(Warning massages are written to the logs if number of in-use connections are higher than this limit. The limit applies to the disks connections.)", 0) \
    DECLARE(UInt64, disk_connections_store_limit, 10000, R"(Connections above this limit reset after use. Set to 0 to turn connection cache off. The limit applies to the disks connections.)", 0) \
    DECLARE(UInt64, disk_connections_hard_limit, 200000, R"(Exception is thrown at a creation attempt when this limit is reached. Set to 0 to turn off hard limitation. The limit applies to the disks connections.)", 0) \
    DECLARE(UInt64, storage_connections_soft_limit, 100, R"(Connections above this limit have significantly shorter time to live. The limit applies to the storages connections.)", 0) \
    DECLARE(UInt64, storage_connections_warn_limit, 500, R"(Warning massages are written to the logs if number of in-use connections are higher than this limit. The limit applies to the storages connections.)", 0) \
    DECLARE(UInt64, storage_connections_store_limit, 1000, R"(Connections above this limit reset after use. Set to 0 to turn connection cache off. The limit applies to the storages connections.)", 0) \
    DECLARE(UInt64, storage_connections_hard_limit, 200000, R"(Exception is thrown at a creation attempt when this limit is reached. Set to 0 to turn off hard limitation. The limit applies to the storages connections.)", 0) \
    DECLARE(UInt64, http_connections_soft_limit, 100, R"(Connections above this limit have significantly shorter time to live. The limit applies to the http connections which do not belong to any disk or storage.)", 0) \
    DECLARE(UInt64, http_connections_warn_limit, 500, R"(Warning massages are written to the logs if number of in-use connections are higher than this limit. The limit applies to the http connections which do not belong to any disk or storage.)", 0) \
    DECLARE(UInt64, http_connections_store_limit, 1000, R"(Connections above this limit reset after use. Set to 0 to turn connection cache off. The limit applies to the http connections which do not belong to any disk or storage.)", 0) \
    DECLARE(UInt64, http_connections_hard_limit, 200000, R"(Exception is thrown at a creation attempt when this limit is reached. Set to 0 to turn off hard limitation. The limit applies to the http connections which do not belong to any disk or storage.)", 0) \
    DECLARE(UInt64, global_profiler_real_time_period_ns, 10000000000, R"(Period for real clock timer of global profiler (in nanoseconds). Set 0 value to turn off the real clock global profiler. Recommended value is at least 10000000 (100 times a second) for single queries or 1000000000 (once a second) for cluster-wide profiling.)", 0) \
    DECLARE(UInt64, global_profiler_cpu_time_period_ns, 10000000000, R"(Period for CPU clock timer of global profiler (in nanoseconds). Set 0 value to turn off the CPU clock global profiler. Recommended value is at least 10000000 (100 times a second) for single queries or 1000000000 (once a second) for cluster-wide profiling.)", 0) \
    DECLARE(Bool, enable_azure_sdk_logging, false, R"(Enables logging from Azure sdk)", 0) \
    DECLARE(Bool, s3queue_disable_streaming, false, "Disable streaming in S3Queue even if the table is created and there are attached materiaized views", 0) \
    DECLARE(UInt64, max_entries_for_hash_table_stats, 10'000, R"(How many entries hash table statistics collected during aggregation is allowed to have)", 0) \
    DECLARE(String, merge_workload, "default", R"(
    Used to regulate how resources are utilized and shared between merges and other workloads. Specified value is used as `workload` setting value for all background merges. Can be overridden by a merge tree setting.

    **See Also**
    - [Workload Scheduling](/operations/workload-scheduling.md)
    )", 0) \
    DECLARE(String, mutation_workload, "default", R"(
    Used to regulate how resources are utilized and shared between mutations and other workloads. Specified value is used as `workload` setting value for all background mutations. Can be overridden by a merge tree setting.

    **See Also**
    - [Workload Scheduling](/operations/workload-scheduling.md)
    )", 0) \
    DECLARE(Bool, throw_on_unknown_workload, false, R"(
    Defines behaviour on access to unknown WORKLOAD with query setting 'workload'.

    - If `true`, RESOURCE_ACCESS_DENIED exception is thrown from a query that is trying to access unknown workload. Useful to enforce resource scheduling for all queries after WORKLOAD hierarchy is established and contains WORKLOAD default.
    - If `false` (default), unlimited access w/o resource scheduling is provided to a query with 'workload' setting pointing to unknown WORKLOAD. This is important during setting up hierarchy of WORKLOAD, before WORKLOAD default is added.

    **Example**

    ```xml
    <throw_on_unknown_workload>true</throw_on_unknown_workload>
    ```

    **See Also**
    - [Workload Scheduling](/operations/workload-scheduling.md)
    )", 0) \
    DECLARE(Bool, cpu_slot_preemption, true, R"(
    Defines how workload scheduling for CPU resources (MASTER THREAD and WORKER THREAD) is done.

    - If `true` (default), accounting is done based on actual CPU time consumed. A fair number of CPU time would be allocated to competing workloads. Slots are allocated for a limited amount of time and re-requested after expiry. Slot requesting may block thread execution in case of CPU resource overload, i.e., preemption may happen. This ensures CPU-time fairness.
    - If `false`, accounting is based on the number of CPU slots allocated. A fair number of CPU slots would be allocated to competing workloads. A slot is allocated when a thread starts, held continuously, and released when the thread ends execution. The number of threads allocated for query execution may only increase from 1 to `max_threads` and never decrease. This is more favorable to long-running queries and may lead to CPU starvation of short queries.

    **Example**

    ```xml
    <cpu_slot_preemption>true</cpu_slot_preemption>
    ```

    **See Also**
    - [Workload Scheduling](/operations/workload-scheduling.md)
    )", 0) \
    DECLARE(UInt64, cpu_slot_quantum_ns, 10000000, R"(
    It defines how many CPU nanoseconds a thread is allowed to consume after acquired a CPU slot and before it should request another CPU slot. Makes sense only if `cpu_slot_preemption` is enabled and CPU resource is defined for MASTER THREAD or WORKER THREAD.

    **Example**

    ```xml
    <cpu_slot_quantum_ns>10000000</cpu_slot_quantum_ns>
    ```

    **See Also**
    - [Workload Scheduling](/operations/workload-scheduling.md)
    )", 0) \
    DECLARE(UInt64, cpu_slot_preemption_timeout_ms, 1000, R"(
    It defines how many milliseconds could a worker thread wait during preemption, i.e. while waiting for another CPU slot to be granted. After this timeout, if thread was unable to acquire a new CPU slot it will exit and the query is scaled down to a lower number of concurrently executing threads dynamically. Note that master thread never downscaled, but could be preempted indefinitely. Makes sense only when `cpu_slot_preemption` is enabled and CPU resource is defined for WORKER THREAD.

    **Example**

    ```xml
    <cpu_slot_preemption_timeout_ms>1000</cpu_slot_preemption_timeout_ms>
    ```

    **See Also**
    - [Workload Scheduling](/operations/workload-scheduling.md)
    )", 0) \
    DECLARE(String, series_keeper_path, "/clickhouse/series", R"(
    Path in Keeper with auto-incremental numbers, generated by the `generateSerialID` function. Each series will be a node under this path.
    )", 0) \
    DECLARE(String, users_to_ignore_early_memory_limit_check, "", R"(
    Comma-separated list of users to ignore an early memory limit check. If user is not in this list, a query will be rejected if the total memory usage exceeds the limit.
    )", 0) \
    DECLARE(Bool, prepare_system_log_tables_on_startup, false, R"(
    If true, ClickHouse creates all configured `system.*_log` tables before the startup. It can be helpful if some startup scripts depend on these tables.
    )", 0) \
    DECLARE(UInt64, config_reload_interval_ms, 2000, R"(
    How often clickhouse will reload config and check for new changes
    )", 0) \
    DECLARE(UInt64, memory_worker_period_ms, 0, R"(
    Tick period of background memory worker which corrects memory tracker memory usages and cleans up unused pages during higher memory usage. If set to 0, default value will be used depending on the memory usage source
    )", 0) \
    DECLARE(Double, memory_worker_purge_dirty_pages_threshold_ratio, 0.2, R"(
    The threshold ratio for jemalloc dirty pages relative to the memory available to ClickHouse server. When dirty pages size exceeds this ratio, the background memory worker forces purging of dirty pages. If set to 0, forced purging based on dirty pages ratio is disabled.
    )", 0) \
    DECLARE(Double, memory_worker_purge_total_memory_threshold_ratio, 0.9, R"(
    The threshold ratio for purging jemalloc relative to the memory available to ClickHouse server. When total memory usage exceeds this ratio, the background memory worker forces purging of dirty pages. If set to 0, forced purging based on total memory is disabled.
    )", 0) \
    DECLARE(UInt64, memory_worker_decay_adjustment_period_ms, 5000, R"(
    Duration in milliseconds that memory pressure must persist before dynamically adjusting jemalloc's `dirty_decay_ms`. When memory usage remains above the purge threshold for this period, automatic dirty page decay is disabled (`dirty_decay_ms=0`) to aggressively reclaim memory. When usage stays below the threshold for this period, the default decay behavior is restored. Set to 0 to disable dynamic adjustment and use jemalloc's default decay settings.
    )", 0) \
    DECLARE(Bool, memory_worker_correct_memory_tracker, 0, R"(
    Whether background memory worker should correct internal memory tracker based on the information from external sources like jemalloc and cgroups
    )", 0) \
    DECLARE(Bool, memory_worker_use_cgroup, true, "Use current cgroup memory usage information to correct memory tracking.", 0) \
    DECLARE(Bool, disable_insertion_and_mutation, false, R"(
    Disable insert/alter/delete queries. This setting will be enabled if someone needs read-only nodes to prevent insertion and mutation affect reading performance. Inserts into external engines (S3, DataLake, MySQL, PostrgeSQL, Kafka, etc) are allowed despite this setting.
    )", 0) \
    DECLARE(UInt64, parts_kill_delay_period, 30, R"(
    Period to completely remove parts for SharedMergeTree. Only available in ClickHouse Cloud
    )", 0) \
    DECLARE(UInt64, parts_kill_delay_period_random_add, 10, R"(
    Add uniformly distributed value from 0 to x seconds to kill_delay_period to avoid thundering herd effect and subsequent DoS of ZooKeeper in case of very large number of tables. Only available in ClickHouse Cloud
    )", 0) \
    DECLARE(UInt64, parts_killer_pool_size, 128, R"(
    Threads for cleanup of shared merge tree parts killer threads. Only available in ClickHouse Cloud
    )", 0) \
    DECLARE(UInt64, snapshot_cleaner_period, 120, R"(
    Period to completely remove snapshot parts for SharedMergeTree. Only available in ClickHouse Cloud
    )", 0) \
    DECLARE(UInt64, snapshot_cleaner_pool_size, 128, R"(
    Threads for cleanup of shared merge tree snapshot cleaner threads. Only available in ClickHouse Cloud
    )", 0) \
    DECLARE(UInt64, keeper_multiread_batch_size, 10'000, R"(
    Maximum size of batch for MultiRead request to [Zoo]Keeper that support batching. If set to 0, batching is disabled. Available only in ClickHouse Cloud.
    )", 0) \
    DECLARE(String, license_file, "", "License file contents for ClickHouse Enterprise Edition", 0) \
    DECLARE(String, license_public_key_for_testing, "", "Licensing demo key, for CI use only", 0) \
    DECLARE(NonZeroUInt64, prefetch_threadpool_pool_size, 100, R"(Size of background pool for prefetches for remote object storages)", 0) \
    DECLARE(UInt64, prefetch_threadpool_queue_size, 10000, R"(Number of tasks which is possible to push into prefetches pool)", 0) \
    DECLARE(UInt64, load_marks_threadpool_pool_size, 50, R"(Size of background pool for marks loading)", 0) \
    DECLARE(UInt64, load_marks_threadpool_queue_size, 10000, R"(Number of tasks which is possible to push into prefetches pool)", 0) \
    DECLARE(NonZeroUInt64, threadpool_writer_pool_size, 100, R"(Size of background pool for write requests to object storages)", 0) \
    DECLARE(UInt64, threadpool_writer_queue_size, 10000, R"(Number of tasks which is possible to push into background pool for write requests to object storages)", 0) \
    DECLARE(UInt64, iceberg_catalog_threadpool_pool_size, 50, R"(Size of background pool for iceberg catalog)", 0) \
    DECLARE(UInt64, iceberg_catalog_threadpool_queue_size, 10000, R"(Number of tasks which is possible to push into iceberg catalog pool)", 0) \
    DECLARE(UInt64, drop_distributed_cache_pool_size, 8, R"(The size of the threadpool used for dropping distributed cache.)", 0) \
    DECLARE(UInt64, drop_distributed_cache_queue_size, 1000, R"(The queue size of the threadpool used for dropping distributed cache.)", 0) \
    DECLARE(Bool, distributed_cache_apply_throttling_settings_from_client, true, R"(Whether cache server should apply throttling settings received from client.)", 0) \
    DECLARE(UInt32, allow_feature_tier, 0, R"(
    Controls if the user can change settings related to the different feature tiers.

    - `0` - Changes to any setting are allowed (experimental, beta, production).
    - `1` - Only changes to beta and production feature settings are allowed. Changes to experimental settings are rejected.
    - `2` - Only changes to production settings are allowed. Changes to experimental or beta settings are rejected.

    This is equivalent to setting a readonly constraint on all `EXPERIMENTAL` / `BETA` features.

    :::note
    A value of `0` means that all settings can be changed.
    :::
    )", 0) \
    DECLARE(Bool, dictionaries_lazy_load, 1, R"(
    Lazy loading of dictionaries.

    - If `true`, then each dictionary is loaded on the first use. If the loading is failed, the function that was using the dictionary throws an exception.
    - If `false`, then the server loads all dictionaries at startup.

    :::note
    The server will wait at startup until all the dictionaries finish their loading before receiving any connections
    (exception: if [`wait_dictionaries_load_at_startup`](/operations/server-configuration-parameters/settings#wait_dictionaries_load_at_startup) is set to `false`).
    :::

    **Example**

    ```xml
    <dictionaries_lazy_load>true</dictionaries_lazy_load>
    ```
    )", 0) \
    DECLARE(Bool, wait_dictionaries_load_at_startup, 1, R"(
    This setting allows to specify behavior if `dictionaries_lazy_load` is `false`.
    (If `dictionaries_lazy_load` is `true` this setting doesn't affect anything.)

    If `wait_dictionaries_load_at_startup` is `false`, then the server
    will start loading all the dictionaries at startup and it will receive connections in parallel with that loading.
    When a dictionary is used in a query for the first time then the query will wait until the dictionary is loaded if it's not loaded yet.
    Setting `wait_dictionaries_load_at_startup` to `false` can make ClickHouse start faster, however some queries can be executed slower
    (because they will have to wait for some dictionaries to be loaded).

    If `wait_dictionaries_load_at_startup` is `true`, then the server will wait at startup
    until all the dictionaries finish their loading (successfully or not) before receiving any connections.

    **Example**

    ```xml
    <wait_dictionaries_load_at_startup>true</wait_dictionaries_load_at_startup>
    ```
    )", 0) \
    DECLARE(Bool, process_query_plan_packet, false, R"(
    This setting allows reading QueryPlan packet. This packet is sent for distributed queries when serialize_query_plan is enabled.
    Disabled by default to avoid possible security issues which can be caused by bugs in query plan binary deserialization.

    **Example**

    ```xml
    <process_query_plan_packet>true</process_query_plan_packet>
    ```
    )", 0) \
    DECLARE(Bool, storage_shared_set_join_use_inner_uuid, true, "If enabled, an inner UUID is generated during the creation of SharedSet and SharedJoin. ClickHouse Cloud only", 0) \
    DECLARE(UInt64, startup_mv_delay_ms, 0, R"(Debug parameter to simulate materizlied view creation delay)", 0) \
    DECLARE(UInt64, os_cpu_busy_time_threshold, 1'000'000, "Threshold of OS CPU busy time in microseconds (OSCPUVirtualTimeMicroseconds metric) to consider CPU doing some useful work, no CPU overload would be considered if busy time was below this value.", 0) \
    DECLARE(Bool, os_collect_psi_metrics, true, "Enable accounting PSI metrics from /proc/pressure/ files.", 0) \
    DECLARE(Float, min_os_cpu_wait_time_ratio_to_drop_connection, 0, R"(
    Min ratio between OS CPU wait (OSCPUWaitMicroseconds metric) and busy (OSCPUVirtualTimeMicroseconds metric) times to consider dropping connections. Linear interpolation between min and max ratio is used to calculate the probability, the probability is 0 at this point.
    See [Controlling behavior on server CPU overload](/operations/settings/server-overload) for more details.
    )", 0) \
    DECLARE(Float, max_os_cpu_wait_time_ratio_to_drop_connection, 0, R"(
    Max ratio between OS CPU wait (OSCPUWaitMicroseconds metric) and busy (OSCPUVirtualTimeMicroseconds metric) times to consider dropping connections. Linear interpolation between min and max ratio is used to calculate the probability, the probability is 1 at this point.
    See [Controlling behavior on server CPU overload](/operations/settings/server-overload) for more details.
    )", 0) \
    DECLARE(Float, distributed_cache_keep_up_free_connections_ratio, 0.1f, "Soft limit for number of active connection distributed cache will try to keep free. After the number of free connections goes below distributed_cache_keep_up_free_connections_ratio * max_connections, connections with oldest activity will be closed until the number goes above the limit.", 0) \
    DECLARE(UInt64, tcp_close_connection_after_queries_num, 0, R"(Maximum number of queries allowed per TCP connection before the connection is closed. Set to 0 for unlimited queries.)", 0) \
    DECLARE(UInt64, tcp_close_connection_after_queries_seconds, 0, R"(Maximum lifetime of a TCP connection in seconds before it is closed. Set to 0 for unlimited connection lifetime.)", 0) \
    DECLARE(Bool, skip_binary_checksum_checks, false, R"(Skips ClickHouse binary checksum integrity checks)", 0) \
    DECLARE(Bool, abort_on_logical_error, false, R"(Crash the server on LOGICAL_ERROR exceptions. Only for experts.)", 0) \
    DECLARE(UInt64, jemalloc_flush_profile_interval_bytes, 0, R"(Flushing jemalloc profile will be done after global peak memory usage increased by jemalloc_flush_profile_interval_bytes)", 0) \
    DECLARE(Bool, jemalloc_flush_profile_on_memory_exceeded, 0, R"(Flushing jemalloc profile will be done on total memory exceeded errors)", 0) \
    DECLARE(Bool, jemalloc_enable_global_profiler, 0, R"(Enable jemalloc's allocation profiler for all threads. Jemalloc will sample allocations and all deallocations for sampled allocations.
    Profiles can be flushed using SYSTEM JEMALLOC FLUSH PROFILE which can be used for allocation analysis.
    Samples can also be stored in system.trace_log using config jemalloc_collect_global_profile_samples_in_trace_log or with query setting jemalloc_collect_profile_samples_in_trace_log.
    See [Allocation Profiling](/operations/allocation-profiling))", 0) \
    DECLARE(Bool, jemalloc_collect_global_profile_samples_in_trace_log, 0, R"(Store jemalloc's sampled allocations in system.trace_log)", 0) \
    DECLARE(Bool, jemalloc_enable_background_threads, 1, R"(Enable jemalloc background threads. Jemalloc uses background threads to cleanup unused memory pages. Disabling it could lead to performance degradation.)", 0) \
    DECLARE(UInt64, jemalloc_max_background_threads_num, 0, R"(Maximum amount of jemalloc background threads to create, set to 0 to use jemalloc's default value)", 0) \
    DECLARE(NonZeroUInt64, threadpool_local_fs_reader_pool_size, 100, R"(The number of threads in the thread pool for reading from local filesystem when `local_filesystem_read_method = 'pread_threadpool'`.)", 0) \
    DECLARE(UInt64, threadpool_local_fs_reader_queue_size, 10000, R"(The maximum number of jobs that can be scheduled on the thread pool for reading from local filesystem.)", 0) \
    DECLARE(NonZeroUInt64, threadpool_remote_fs_reader_pool_size, 250, R"(Number of threads in the Thread pool used for reading from remote filesystem when `remote_filesystem_read_method = 'threadpool'`.)", 0) \
    DECLARE(UInt64, threadpool_remote_fs_reader_queue_size, 10000, R"(The maximum number of jobs that can be scheduled on the thread pool for reading from remote filesystem.)", 0) \
\
    DECLARE(UInt64, s3_max_redirects, S3::DEFAULT_MAX_REDIRECTS, R"(Max number of S3 redirects hops allowed.)", 0) \
    DECLARE(UInt64, s3_retry_attempts, S3::DEFAULT_RETRY_ATTEMPTS, R"(Setting for Aws::Client::RetryStrategy, Aws::Client does retries itself, 0 means no retries)", 0) \
    DECLARE(Int32, os_threads_nice_value_merge_mutate, 0, R"(
    Linux nice value for merge and mutation threads. Lower values mean higher CPU priority.

    Requires CAP_SYS_NICE capability, otherwise no-op.

    Possible values: -20 to 19.
    )", 0) \
    DECLARE(Int32, os_threads_nice_value_zookeeper_client_send_receive, 0, R"(
    Linux nice value for send and receive threads in ZooKeeper client. Lower values mean higher CPU priority.

    Requires CAP_SYS_NICE capability, otherwise no-op.

    Possible values: -20 to 19.
    )", 0) \
    DECLARE(Int32, os_threads_nice_value_distributed_cache_tcp_handler, 0, R"(
    Linux nice value for the threads of distributed cache TCP handler. Lower values mean higher CPU priority.

    Requires CAP_SYS_NICE capability, otherwise no-op.

    Possible values: -20 to 19.
    )", 0) \
    DECLARE(String, keeper_hosts, "", R"(Dynamic setting. Contains a set of [Zoo]Keeper hosts ClickHouse can potentially connect to. Doesn't expose information from `<auxiliary_zookeepers>`)", 0) \
    DECLARE(Bool, allow_impersonate_user, false, R"(Enable/disable the IMPERSONATE feature (EXECUTE AS target_user). The setting is deprecated.)", SettingsTierType::OBSOLETE) \
    DECLARE(UInt64, s3_credentials_provider_max_cache_size, 100, R"(The maximum number of S3 credentials providers that can be cached)", 0) \
    DECLARE(UInt64, max_open_files, 0, R"(
    The maximum number of open files.

    :::note
    We recommend using this option in macOS since the getrlimit() function returns an incorrect value.
    :::
    )", 0) \
    DECLARE(String, path, DBMS_DEFAULT_PATH, R"(
    The path to the directory containing data.

    :::note
    The trailing slash is mandatory.
    :::

    **Example**

    ```xml
    <path>/var/lib/clickhouse/</path>
    ```
    )", 0) \
    DECLARE(String, user_files_path, "/var/lib/clickhouse/user_files/", R"(
    The directory with user files. Used in the table function [file()](/sql-reference/table-functions/file), [fileCluster()](/sql-reference/table-functions/fileCluster).

    **Example**

    ```xml
    <user_files_path>/var/lib/clickhouse/user_files/</user_files_path>
    ```
    )", 0) \
    DECLARE(String, dictionaries_lib_path, "/var/lib/clickhouse/dictionaries_lib/", R"(
    The directory with dictionaries lib.

    **Example**

    ```xml
    <dictionaries_lib_path>/var/lib/clickhouse/dictionaries_lib/</dictionaries_lib_path>
    ```
    )", 0) \
    DECLARE(String, user_scripts_path, "/var/lib/clickhouse/user_scripts/", R"(
    The directory with user scripts files. Used for Executable user defined functions [Executable User Defined Functions](/sql-reference/functions/udf#executable-user-defined-functions).

    **Example**

    ```xml
    <user_scripts_path>/var/lib/clickhouse/user_scripts/</user_scripts_path>
    ```
    )", 0) \
    DECLARE(String, top_level_domains_path, "/var/lib/clickhouse/top_level_domains/", R"(
    The directory with top level domains.

    **Example**

    ```xml
    <top_level_domains_path>/var/lib/clickhouse/top_level_domains/</top_level_domains_path>
    ```
    )", 0) \
    DECLARE(String, interserver_http_host, "", R"(
    The hostname that can be used by other servers to access this server.

    If omitted, it is defined in the same way as the `<hostname -f>` command.

    Useful for breaking away from a specific network interface.

    **Example**

    ```xml
    <interserver_http_host>example.clickhouse.com</interserver_http_host>
    ```
    )", 0) \
    DECLARE(UInt64, interserver_http_port, 0, R"(
    Port for exchanging data between ClickHouse servers.

    **Example**

    ```xml
    <interserver_http_port>9009</interserver_http_port>
    ```
    )", 0) \
    DECLARE(String, interserver_https_host, "", R"(
    Similar to `<interserver_http_host>`, except that this hostname can be used by other servers to access this server over `<HTTPS>`.

    **Example**

    ```xml
    <interserver_https_host>example.clickhouse.com</interserver_https_host>
    ```
    )", 0) \
    DECLARE(UInt64, interserver_https_port, 0, R"(
    Port for exchanging data between ClickHouse servers over `<HTTPS>`.

    **Example**

    ```xml
    <interserver_https_port>9010</interserver_https_port>
    ```
    )", 0) \
    DECLARE(String, include_from, "/etc/metrika.xml", R"(
    The path to the file with substitutions. Both XML and YAML formats are supported.

    For more information, see the section [Configuration files](/operations/configuration-files).

    **Example**

    ```xml
    <include_from>/etc/metrica.xml</include_from>
    ```
    )", 0) \
    DECLARE(String, tmp_path, "/var/lib/clickhouse/tmp/", R"(
    Path on the local filesystem to store temporary data for processing large queries.

    :::note
    - Only one option can be used to configure temporary data storage: tmp_path, tmp_policy, temporary_data_in_cache.
    - The trailing slash is mandatory.
    :::

    **Example**

    ```xml
    <tmp_path>/var/lib/clickhouse/tmp/</tmp_path>
    ```
    )", 0) \
    DECLARE(String, format_schema_path, "/var/lib/clickhouse/format_schemas/", R"(
    The path to the directory with the schemes for the input data, such as schemas for the [CapnProto](/interfaces/formats/CapnProto) format.

    **Example**

    ```xml
    <!-- Directory containing schema files for various input formats. -->
    <format_schema_path>/var/lib/clickhouse/format_schemas/</format_schema_path>
    ```
    )", 0) \
    DECLARE(String, google_protos_path, "/usr/share/clickhouse/protos/", R"(
    Defines a directory containing proto files for Protobuf types.

    **Example**

    ```xml
    <google_protos_path>/usr/share/clickhouse/protos/</google_protos_path>
    ```
    )", 0) \
    DECLARE(String, filesystem_caches_path, "", R"(
    This setting specifies the cache path.

    **Example**

    ```xml
    <filesystem_caches_path>/var/lib/clickhouse/filesystem_caches/</filesystem_caches_path>
    ```
    )", 0) \
    DECLARE(Int32, oom_score, getDefaultOomScore(), R"(On Linux systems this can control the behavior of OOM killer.)", 0) \
    DECLARE(Bool, remap_executable, false, R"(
    Setting to reallocate memory for machine code ("text") using huge pages.

    :::note
    This feature is highly experimental.
    :::

    **Example**

    ```xml
    <remap_executable>false</remap_executable>
    ```
    )", 0) \
    DECLARE(Bool, mlock_executable, false, R"(
    Perform `<mlockall>` after startup to lower first queries latency and to prevent clickhouse executable from being paged out under high IO load.

    :::note
    Enabling this option is recommended but will lead to increased startup time for up to a few seconds. Keep in mind that this setting would not work without "CAP_IPC_LOCK" capability.
    :::

    **Example**

    ```xml
    <mlock_executable>false</mlock_executable>
    ```
    )", 0) \
    DECLARE(UInt64, mlock_executable_min_total_memory_amount_bytes, 5000000000, R"(The minimum memory threshold for performing `<mlockall>`)", 0) \
    DECLARE(UInt32, listen_backlog, 4096, R"(
    Backlog (queue size of pending connections) of the listen socket. The default value of `<4096>` is the same as that of linux 5.4+).

    Usually this value does not need to be changed, since:
    - The default value is large enough,
    - For accepting client's connections server has separate thread.

    So even if you have `<TcpExtListenOverflows>` (from `<nstat>`) non-zero and this counter grows for ClickHouse server it does not mean that this value needs to be increased, since:
    - Usually if `<4096>` is not enough it shows some internal ClickHouse scaling issue, so it is better to report an issue.
    - It does not mean that the server can handle more connections later (and even if it could, by that moment clients may be gone or disconnected).

    **Example**

    ```xml
    <listen_backlog>4096</listen_backlog>
    ```
    )", 0) \
    DECLARE(Bool, listen_reuse_port, false, R"(
    Allow multiple servers to listen on the same address:port. Requests will be routed to a random server by the operating system. Enabling this setting is not recommended.

    **Example**

    ```xml
    <listen_reuse_port>0</listen_reuse_port>
    ```
    )", 0) \
    DECLARE(Bool, listen_try, false, R"(
    The server will not exit if IPv6 or IPv4 networks are unavailable while trying to listen.

    **Example**

    ```xml
    <listen_try>0</listen_try>
    ```
    )", 0) \
    DECLARE(Bool, mysql_require_secure_transport, false, R"(If set to true, secure communication is required with clients over [mysql_port](/operations/server-configuration-parameters/settings#mysql_port). Connection with option `<--ssl-mode=none>` will be refused. Use it with [OpenSSL](/operations/server-configuration-parameters/settings#openssl) settings.)", 0) \
    DECLARE(Bool, postgresql_require_secure_transport, false, R"(If set to true, secure communication is required with clients over [postgresql_port](/operations/server-configuration-parameters/settings#postgresql_port). Connection with option `<sslmode=disable>` will be refused. Use it with [OpenSSL](/operations/server-configuration-parameters/settings#openssl) settings.)", 0) \
    DECLARE(Bool, skip_check_for_incorrect_settings, false, R"(
    If set to true, server settings will not be checked for correctness.

    **Example**

    ```xml
    <skip_check_for_incorrect_settings>1</skip_check_for_incorrect_settings>
    ```
    )", 0)

/// Settings with a path are server settings with at least one layer of nesting that have a fixed structure (no lists, lists, enumerations, repetitions, ...).
#define LIST_OF_SERVER_SETTINGS_WITH_PATH(DECLARE, ALIAS) \
    DECLARE(UInt64, query_cache_max_size_in_bytes, 1073741824, R"(The maximum cache size in bytes. 0 means the query cache is disabled.)", 0, "query_cache.max_size_in_bytes") \
    DECLARE(UInt64, query_cache_max_entries, 1024, R"(The maximum number of SELECT query results stored in the cache.)", 0, "query_cache.max_entries") \
    DECLARE(UInt64, query_cache_max_entry_size_in_bytes, 1048576, R"(The maximum size in bytes SELECT query results may have to be saved in the cache.)", 0, "query_cache.max_entry_size_in_bytes") \
    DECLARE(UInt64, query_cache_max_entry_size_in_rows, 30000000, R"(The maximum number of rows SELECT query results may have to be saved in the cache.)", 0, "query_cache.max_entry_size_in_rows") \
    DECLARE(String, logger_level, "trace", R"(Log level. Acceptable values: `<none>` (turn logging off), `<fatal>`, `<critical>`, `<error>`, `<warning>`, `<notice>`, `<information>`, `<debug>`, `<trace>`, `<test>`.)", 0, "logger.level") \
    DECLARE(String, logger_log, "", R"(The path to the log file.)", 0, "logger.log") \
    DECLARE(String, logger_errorlog, "", R"(The path to the error log file.)", 0, "logger.errorlog") \
    DECLARE(String, logger_size, "100M", R"(Rotation policy: Maximum size of the log files in bytes. Once the log file size exceeds this threshold, it is renamed and archived, and a new log file is created.)", 0, "logger.size") \
    DECLARE(String, logger_rotation, "100M", R"(Rotation policy: Controls when log files are rotated. Rotation can be based on size, time, or a combination of both. Examples: 100M, daily, 100M,daily. Once the log file exceeds the specified size or when the specified time interval is reached, it is renamed and archived, and a new log file is created.)", 0, "logger.rotation") \
    DECLARE(UInt64, logger_count, 1, R"(Rotation policy: How many historical log files ClickHouse are kept at most.)", 0, "logger.count") \
    DECLARE(Bool, logger_stream_compress, false, R"(Compress log messages using LZ4. Set to `<1>` or `<true>` to enable.)", 0, "logger.stream_compress") \
    DECLARE(Bool, logger_console, false, R"(Enable logging to the console. Set to `<1>` or `<true>` to enable. Default is `<1>` if ClickHouse does not run in daemon mode, `<0>` otherwise.)", 0, "logger.console") \
    DECLARE(String, logger_console_log_level, "trace", R"(Log level for console output. Defaults to `<level>`.)", 0, "logger.console_log_level") \
    DECLARE(String, logger_formatting_type, "json", R"(Log format for console output. Currently, only `<json>` is supported.)", 0, "logger.formatting.type") \
    DECLARE(Bool, logger_use_syslog, false, R"(Also forward log output to syslog.)", 0, "logger.use_syslog") \
    DECLARE(String, logger_syslog_level, "trace", R"(Log level for logging to syslog.)", 0, "logger.syslog_level") \
    DECLARE(Bool, logger_async, true, R"(When `<true>` (default) logging will happen asynchronously (one background thread per output channel). Otherwise it will log inside the thread calling LOG.)", 0, "logger.async") \
    DECLARE(UInt64, logger_async_queue_max_size, 65536, R"(When using async logging, the max amount of messages that will be kept in the the queue waiting for flushing. Extra messages will be dropped.)", 0, "logger.async_queye_max_size") \
    DECLARE(String, logger_startup_level, "", R"(Startup level is used to set the root logger level at server startup. After startup log level is reverted to the `<level>` setting.)", 0, "logger.startup_level") \
    DECLARE(String, logger_shutdown_level, "", R"(Shutdown level is used to set the root logger level at server Shutdown.)", 0, "logger.shutdown_level") \
    DECLARE(String, openssl_server_private_key_file, "", R"(Path to the file with the secret key of the PEM certificate. The file may contain a key and certificate at the same time.)", 0, "openSSL.server.privateKeyFile") \
    DECLARE(String, openssl_server_certificate_file, "", R"(Path to the client/server certificate file in PEM format. You can omit it if `<privateKeyFile>` contains the certificate.)", 0, "openSSL.server.certificateFile") \
    DECLARE(String, openssl_server_ca_config, "", R"(Path to the file or directory that contains trusted CA certificates. If this points to a file, it must be in PEM format and can contain several CA certificates. If this points to a directory, it must contain one .pem file per CA certificate. The filenames are looked up by the CA subject name hash value. Details can be found in the man page of [SSL_CTX_load_verify_locations](https://docs.openssl.org/3.0/man3/SSL_CTX_load_verify_locations/).)", 0, "openSSL.server.caConfig") \
    DECLARE(String, openssl_server_verification_mode, "relaxed", R"(The method for checking the node's certificates. Details are in the description of the [Context](https://github.com/ClickHouse/poco/blob/master/NetSSL_OpenSSL/include/Poco/Net/Context.h) class. Possible values: `<none>`, `<relaxed>`, `<strict>`, `<once>`.)", 0, "openSSL.server.verificationMode") \
    DECLARE(UInt64, openssl_server_verification_depth, 9, R"(The maximum length of the verification chain. Verification will fail if the certificate chain length exceeds the set value.)", 0, "openSSL.server.verificationDepth") \
    DECLARE(Bool, openssl_server_load_default_ca_file, true, R"(Determines whether built-in CA certificates for OpenSSL will be used. ClickHouse assumes that builtin CA certificates are in the file `</etc/ssl/cert.pem>` (resp. the directory `</etc/ssl/certs>`) or in file (resp. directory) specified by the environment variable `<SSL_CERT_FILE>` (resp. `<SSL_CERT_DIR>`).)", 0, "openSSL.server.loadDefaultCAFile") \
    DECLARE(String, openssl_server_chipher_list, "ALL:!ADH:!LOW:!EXP:!MD5:!3DES:@STRENGTH", R"(Supported OpenSSL encryptions.)", 0, "openSSL.server.cipherList") \
    DECLARE(Bool, openssl_server_cache_sessions, false, R"(Enables or disables caching sessions. Must be used in combination with `<sessionIdContext>`. Acceptable values: `<true>`, `<false>`.)", 0, "openSSL.server.cacheSessions") \
    DECLARE(String, openssl_server_session_id_context, "application.name", R"(A unique set of random characters that the server appends to each generated identifier. The length of the string must not exceed `<SSL_MAX_SSL_SESSION_ID_LENGTH>`. This parameter is always recommended since it helps avoid problems both if the server caches the session and if the client requested caching.)", 0, "openSSL.server.sessionIdContext") \
    DECLARE(UInt64, openssl_server_session_cache_size, 20480, R"(The maximum number of sessions that the server caches. A value of 0 means unlimited sessions.)", 0, "openSSL.server.sessionCacheSize") \
    DECLARE(UInt64, openssl_server_session_timeout, 2, R"(Time for caching the session on the server in hours.)", 0, "openSSL.server.sessionTimeout") \
    DECLARE(Bool, openssl_server_extended_verification, false, R"(If enabled, verify that the certificate CN or SAN matches the peer hostname.)", 0, "openSSL.server.extendedVerification") \
    DECLARE(Bool, openssl_server_required_tls_v1, false, R"(Require a TLSv1 connection. Acceptable values: `<true>`, `<false>`.)", 0, "openSSL.server.requireTLSv1") \
    DECLARE(Bool, openssl_server_required_tls_v1_1, false, R"(Require a TLSv1.1 connection. Acceptable values: `<true>`, `<false>`.)", 0, "openSSL.server.requireTLSv1_1") \
    DECLARE(Bool, openssl_server_required_tls_v1_2, false, R"(Require a TLSv1.2 connection. Acceptable values: `<true>`, `<false>`.)", 0, "openSSL.server.requireTLSv1_2") \
    DECLARE(Bool, openssl_server_fips, false, R"(Activates OpenSSL FIPS mode. Supported if the library's OpenSSL version supports FIPS.)", 0, "openSSL.server.fips") \
    DECLARE(String, openssl_server_private_key_passphrase_handler, "KeyConsoleHandler", R"(Class (PrivateKeyPassphraseHandler subclass) that requests the passphrase for accessing the private key. For example: `<<privateKeyPassphraseHandler>>`, `<<name>KeyFileHandler</name>>`, `<<options><password>test</password></options>>`, `<</privateKeyPassphraseHandler>>`)", 0, "openSSL.server.privateKeyPassphraseHandler.name") \
    DECLARE(String, openssl_server_invalid_certificate_handler, "RejectCertificateHandler", R"(Class (a subclass of CertificateHandler) for verifying invalid certificates. For example: `<<invalidCertificateHandler> <name>RejectCertificateHandler</name> </invalidCertificateHandler>>`.)", 0, "openSSL.server.invalidCertificateHandler.name") \
    DECLARE(String, openssl_server_disable_protocols, "", R"(Protocols that are not allowed to be used.)", 0, "openSSL.server.disableProtocols") \
    DECLARE(Bool, openssl_server_prefer_server_ciphers, false, R"(Client-preferred server ciphers.)", 0, "openSSL.server.preferServerCiphers") \
    DECLARE(String, openssl_client_private_key_file, "", R"(Path to the file with the secret key of the PEM certificate. The file may contain a key and certificate at the same time.)", 0, "openSSL.client.privateKeyFile") \
    DECLARE(String, openssl_client_certificate_file, "", R"(Path to the client/server certificate file in PEM format. You can omit it if `<privateKeyFile>` contains the certificate.)", 0, "openSSL.client.certificateFile") \
    DECLARE(String, openssl_client_ca_config, "", R"(Path to the file or directory that contains trusted CA certificates. If this points to a file, it must be in PEM format and can contain several CA certificates. If this points to a directory, it must contain one .pem file per CA certificate. The filenames are looked up by the CA subject name hash value. Details can be found in the man page of [SSL_CTX_load_verify_locations](https://docs.openssl.org/3.0/man3/SSL_CTX_load_verify_locations/).)", 0, "openSSL.client.caConfig") \
    DECLARE(String, openssl_client_verification_mode, "relaxed", R"(The method for checking the node's certificates. Details are in the description of the [Context](https://github.com/ClickHouse/poco/blob/master/NetSSL_OpenSSL/include/Poco/Net/Context.h) class. Possible values: `<none>`, `<relaxed>`, `<strict>`, `<once>`.)", 0, "openSSL.client.verificationMode") \
    DECLARE(UInt64, openssl_client_verification_depth, 9, R"(The maximum length of the verification chain. Verification will fail if the certificate chain length exceeds the set value.)", 0, "openSSL.client.verificationDepth") \
    DECLARE(Bool, openssl_client_load_default_ca_file, true, R"(Determines whether built-in CA certificates for OpenSSL will be used. ClickHouse assumes that builtin CA certificates are in the file `</etc/ssl/cert.pem>` (resp. the directory `</etc/ssl/certs>`) or in file (resp. directory) specified by the environment variable `<SSL_CERT_FILE>` (resp. `<SSL_CERT_DIR>`).)", 0, "openSSL.client.loadDefaultCAFile") \
    DECLARE(String, openssl_client_chipher_list, "ALL:!ADH:!LOW:!EXP:!MD5:!3DES:@STRENGTH", R"(Supported OpenSSL encryptions.)", 0, "openSSL.client.cipherList") \
    DECLARE(Bool, openssl_client_cache_sessions, false, R"(Enables or disables caching sessions. Must be used in combination with `<sessionIdContext>`. Acceptable values: `<true>`, `<false>`.)", 0, "openSSL.client.cacheSessions") \
    DECLARE(Bool, openssl_client_extended_verification, false, R"(If enabled, verify that the certificate CN or SAN matches the peer hostname.)", 0, "openSSL.client.extendedVerification") \
    DECLARE(Bool, openssl_client_required_tls_v1, false, R"(Require a TLSv1 connection. Acceptable values: `<true>`, `<false>`.)", 0, "openSSL.client.requireTLSv1") \
    DECLARE(Bool, openssl_client_required_tls_v1_1, false, R"(Require a TLSv1.1 connection. Acceptable values: `<true>`, `<false>`.)", 0, "openSSL.client.requireTLSv1_1") \
    DECLARE(Bool, openssl_client_required_tls_v1_2, false, R"(Require a TLSv1.2 connection. Acceptable values: `<true>`, `<false>`.)", 0, "openSSL.client.requireTLSv1_2") \
    DECLARE(Bool, openssl_client_fips, false, R"(Activates OpenSSL FIPS mode. Supported if the library's OpenSSL version supports FIPS.)", 0, "openSSL.client.fips") \
    DECLARE(String, openssl_client_private_key_passphrase_handler, "KeyConsoleHandler", R"(Class (PrivateKeyPassphraseHandler subclass) that requests the passphrase for accessing the private key. For example: `<<privateKeyPassphraseHandler>>`, `<<name>KeyFileHandler</name>>`, `<<options><password>test</password></options>>`, `<</privateKeyPassphraseHandler>>`)", 0, "openSSL.client.privateKeyPassphraseHandler.name") \
    DECLARE(String, openssl_client_invalid_certificate_handler, "RejectCertificateHandler", R"(Class (a subclass of CertificateHandler) for verifying invalid certificates. For example: `<<invalidCertificateHandler> <name>RejectCertificateHandler</name> </invalidCertificateHandler>>`.)", 0, "openSSL.client.invalidCertificateHandler.name") \
    DECLARE(String, openssl_client_disable_protocols, "", R"(Protocols that are not allowed to be used.)", 0, "openSSL.client.disableProtocols") \
    DECLARE(Bool, openssl_client_prefer_server_ciphers, false, R"(Client-preferred server ciphers.)", 0, "openSSL.client.preferServerCiphers") \
    DECLARE(String, distributed_ddl_path, "/clickhouse/task_queue/ddl/", R"(the path in Keeper for the `<task_queue>` for DDL queries)", 0, "distributed_ddl.path") \
    DECLARE(String, distributed_ddl_replicas_path, "/clickhouse/task_queue/replicas/", R"(the path in Keeper for the `<task_queue>` for replicas)", 0, "distributed_ddl.replicas_path") \
    DECLARE(String, distributed_ddl_profile, "", R"(the profile used to execute the DDL queries)", 0, "distributed_ddl.profile") \
    DECLARE(Int32, distributed_ddl_pool_size, 1, R"(how many `<ON CLUSTER>` queries can be run simultaneously)", 0, "distributed_ddl.pool_size") \
    DECLARE(UInt64, distributed_ddl_max_tasks_in_queue, 1000, R"(the maximum number of tasks that can be in the queue.)", 0, "distributed_ddl.max_tasks_in_queue") \
    DECLARE(UInt64, distributed_ddl_task_max_lifetime, 604800, R"(delete node if its age is greater than this value.)", 0, "distributed_ddl.task_max_lifetime") \
    DECLARE(UInt64, distributed_ddl_cleanup_delay_period, 60, R"(cleaning starts after new node event is received if the last cleaning wasn't made sooner than `<cleanup_delay_period>` seconds ago.)", 0, "distributed_ddl.cleanup_delay_period") \
    DECLARE(Bool, prometheus_keeper_metrics_only, false, R"(Expose the keeper related metrics)", 0, "prometheus.keeper_metrics_only") \
    DECLARE(Bool, startup_scripts_throw_on_error, false, R"(If set to true, the server will not start if an error occurs during script execution.)", 0, "startup_scripts.throw_on_error") \
    DECLARE(UInt64, keeper_server_socket_receive_timeout_sec, DBMS_DEFAULT_RECEIVE_TIMEOUT_SEC, R"(Keeper socket receive timeout.)", 0, "keeper_server.socket_receive_timeout_sec") \
    DECLARE(UInt64, keeper_server_socket_send_timeout_sec, DBMS_DEFAULT_SEND_TIMEOUT_SEC, R"(Keeper socket send timeout.)", 0, "keeper_server.socket_send_timeout_sec") \
    DECLARE(String, hdfs_libhdfs3_conf, "", R"(Points libhdfs3 to the right location for its config.)", 0, "hdfs.libhdfs3_conf") \
    DECLARE(String, config_file, "config.xml", R"(Points to the server config file.)", 0, "config-file")

// clang-format on

/// If you add a setting which can be updated at runtime, please update 'changeable_settings' map in dumpToSystemServerSettingsColumns below

DECLARE_SETTINGS_TRAITS_WITH_PATH(ServerSettingsTraits, LIST_OF_SERVER_SETTINGS_WITHOUT_PATH, LIST_OF_SERVER_SETTINGS_WITH_PATH)
IMPLEMENT_SETTINGS_TRAITS_WITH_PATH(ServerSettingsTraits, LIST_OF_SERVER_SETTINGS_WITHOUT_PATH, LIST_OF_SERVER_SETTINGS_WITH_PATH)

#define LIST_OF_SERVER_SETTINGS(DECLARE, ALIAS) \
    LIST_OF_SERVER_SETTINGS_WITHOUT_PATH(DECLARE, ALIAS) \
    LIST_OF_SERVER_SETTINGS_WITH_PATH(DECLARE, ALIAS) \

struct ServerSettingsImpl : public BaseSettings<ServerSettingsTraits>
{
    void loadSettingsFromConfig(const Poco::Util::AbstractConfiguration & config);
};

void ServerSettingsImpl::loadSettingsFromConfig(const Poco::Util::AbstractConfiguration & config)
{
    // settings which can be loaded from the the default profile, see also MAKE_DEPRECATED_BY_SERVER_CONFIG in src/Core/Settings.h
    std::unordered_set<std::string> settings_from_profile_allowlist = {
        "background_pool_size",
        "background_merges_mutations_concurrency_ratio",
        "background_merges_mutations_scheduling_policy",
        "background_move_pool_size",
        "background_fetches_pool_size",
        "background_common_pool_size",
        "background_buffer_flush_schedule_pool_size",
        "background_schedule_pool_size",
        "background_message_broker_schedule_pool_size",
        "background_distributed_schedule_pool_size",

        "max_remote_read_network_bandwidth_for_server",
        "max_remote_write_network_bandwidth_for_server",
        "max_local_read_bandwidth_for_server",
        "max_local_write_bandwidth_for_server",
    };

    for (const auto & setting : all())
    {
        const auto & name = setting.getName();
        String path {setting.getPath()};
        const String * path_or_name = path.empty() ? &name : &path;
        try
        {
            if (config.has(*path_or_name))
                set(name, config.getString(*path_or_name));
            else if (settings_from_profile_allowlist.contains(name) && config.has("profiles.default." + *path_or_name))
                set(name, config.getString("profiles.default." + *path_or_name));
        }
        catch (Exception & e)
        {
            e.addMessage("while parsing setting '{}' value", name);
            throw;
        }
    }
}


#define INITIALIZE_SETTING_EXTERN(TYPE, NAME, DEFAULT, DESCRIPTION, FLAGS, ...) ServerSettings##TYPE NAME = &ServerSettingsImpl ::NAME;

namespace ServerSetting
{
LIST_OF_SERVER_SETTINGS(INITIALIZE_SETTING_EXTERN, INITIALIZE_SETTING_EXTERN)
}

#undef INITIALIZE_SETTING_EXTERN

ServerSettings::ServerSettings() : impl(std::make_unique<ServerSettingsImpl>())
{
}

ServerSettings::ServerSettings(const ServerSettings & settings) : impl(std::make_unique<ServerSettingsImpl>(*settings.impl))
{
}

ServerSettings::~ServerSettings() = default;

SERVER_SETTINGS_SUPPORTED_TYPES(ServerSettings, IMPLEMENT_SETTING_SUBSCRIPT_OPERATOR)

Field ServerSettings::get(std::string_view name) const
{
    return impl->get(name);
}

void ServerSettings::set(std::string_view name, const Field & value)
{
    impl->set(name, value);
}

void ServerSettings::loadSettingsFromConfig(const Poco::Util::AbstractConfiguration & config)
{
    impl->loadSettingsFromConfig(config);
}


void ServerSettings::dumpToSystemServerSettingsColumns(ServerSettingColumnsParams & params) const
{
    MutableColumns & res_columns = params.res_columns;
    ContextPtr context = params.context;

    /// When the server configuration file is periodically re-loaded from disk, the server components (e.g. memory tracking) are updated
    /// with new the setting values but the settings themselves are not stored between re-loads. As a result, if one wants to know the
    /// current setting values, one needs to ask the components directly.
    std::unordered_map<String, std::pair<String, ChangeableWithoutRestart>> changeable_settings
        = {
            {"max_server_memory_usage", {std::to_string(total_memory_tracker.getHardLimit()), ChangeableWithoutRestart::Yes}},

            {"max_table_size_to_drop", {std::to_string(context->getMaxTableSizeToDrop()), ChangeableWithoutRestart::Yes}},
            {"max_named_collection_num_to_warn", {std::to_string(context->getMaxNamedCollectionNumToWarn()), ChangeableWithoutRestart::Yes}},
            {"max_table_num_to_warn", {std::to_string(context->getMaxTableNumToWarn()), ChangeableWithoutRestart::Yes}},
            {"max_view_num_to_warn", {std::to_string(context->getMaxViewNumToWarn()), ChangeableWithoutRestart::Yes}},
            {"max_dictionary_num_to_warn", {std::to_string(context->getMaxDictionaryNumToWarn()), ChangeableWithoutRestart::Yes}},
            {"max_database_num_to_warn", {std::to_string(context->getMaxDatabaseNumToWarn()), ChangeableWithoutRestart::Yes}},
            {"max_part_num_to_warn", {std::to_string(context->getMaxPartNumToWarn()), ChangeableWithoutRestart::Yes}},
            {"max_pending_mutations_to_warn", {std::to_string(context->getMaxPendingMutationsToWarn()), ChangeableWithoutRestart::Yes}},
            {"max_pending_mutations_execution_time_to_warn", {std::to_string(context->getMaxPendingMutationsExecutionTimeToWarn()), ChangeableWithoutRestart::Yes}},
            {"max_partition_size_to_drop", {std::to_string(context->getMaxPartitionSizeToDrop()), ChangeableWithoutRestart::Yes}},

            {"min_os_cpu_wait_time_ratio_to_drop_connection", {std::to_string(context->getMinOSCPUWaitTimeRatioToDropConnection()), ChangeableWithoutRestart::Yes}},
            {"max_os_cpu_wait_time_ratio_to_drop_connection", {std::to_string(context->getMaxOSCPUWaitTimeRatioToDropConnection()), ChangeableWithoutRestart::Yes}},

            {"max_concurrent_queries", {std::to_string(context->getProcessList().getMaxSize()), ChangeableWithoutRestart::Yes}},
            {"max_concurrent_insert_queries",
            {std::to_string(context->getProcessList().getMaxInsertQueriesAmount()), ChangeableWithoutRestart::Yes}},
            {"max_concurrent_select_queries",
            {std::to_string(context->getProcessList().getMaxSelectQueriesAmount()), ChangeableWithoutRestart::Yes}},
            {"max_waiting_queries", {std::to_string(context->getProcessList().getMaxWaitingQueriesAmount()), ChangeableWithoutRestart::Yes}},
            {"concurrent_threads_soft_limit_num", {std::to_string(context->getConcurrentThreadsSoftLimitNum()), ChangeableWithoutRestart::Yes}},
            {"concurrent_threads_soft_limit_ratio_to_cores", {std::to_string(context->getConcurrentThreadsSoftLimitRatioToCores()), ChangeableWithoutRestart::Yes}},
            {"concurrent_threads_scheduler", {context->getConcurrentThreadsScheduler(), ChangeableWithoutRestart::Yes}},

            {"background_buffer_flush_schedule_pool_size",
                {std::to_string(CurrentMetrics::get(CurrentMetrics::BackgroundBufferFlushSchedulePoolSize)), ChangeableWithoutRestart::IncreaseOnly}},
            {"background_schedule_pool_size",
                {std::to_string(CurrentMetrics::get(CurrentMetrics::BackgroundSchedulePoolSize)), ChangeableWithoutRestart::IncreaseOnly}},
            {"background_message_broker_schedule_pool_size",
                {std::to_string(CurrentMetrics::get(CurrentMetrics::BackgroundMessageBrokerSchedulePoolSize)), ChangeableWithoutRestart::IncreaseOnly}},
            {"background_distributed_schedule_pool_size",
                {std::to_string(CurrentMetrics::get(CurrentMetrics::BackgroundDistributedSchedulePoolSize)), ChangeableWithoutRestart::IncreaseOnly}},

            {"mark_cache_size", {std::to_string(context->getMarkCache()->maxSizeInBytes()), ChangeableWithoutRestart::Yes}},
            {"uncompressed_cache_size", {std::to_string(context->getUncompressedCache()->maxSizeInBytes()), ChangeableWithoutRestart::Yes}},
            {"index_mark_cache_size", {std::to_string(context->getIndexMarkCache()->maxSizeInBytes()), ChangeableWithoutRestart::Yes}},
            {"index_uncompressed_cache_size", {std::to_string(context->getIndexUncompressedCache()->maxSizeInBytes()), ChangeableWithoutRestart::Yes}},
            {"mmap_cache_size", {std::to_string(context->getMMappedFileCache()->maxSizeInBytes()), ChangeableWithoutRestart::Yes}},
            {"query_condition_cache_size", {std::to_string(context->getQueryConditionCache()->maxSizeInBytes()), ChangeableWithoutRestart::Yes}},
            {"primary_index_cache_size", {std::to_string(context->getPrimaryIndexCache()->maxSizeInBytes()), ChangeableWithoutRestart::Yes}},
            {"vector_similarity_index_cache_size", {std::to_string(context->getVectorSimilarityIndexCache()->maxSizeInBytes()), ChangeableWithoutRestart::Yes}},

            {"merge_workload", {context->getMergeWorkload(), ChangeableWithoutRestart::Yes}},
            {"mutation_workload", {context->getMutationWorkload(), ChangeableWithoutRestart::Yes}},
            {"throw_on_unknown_workload", {std::to_string(context->getThrowOnUnknownWorkload()), ChangeableWithoutRestart::Yes}},
            {"cpu_slot_preemption", {std::to_string(context->getCPUSlotPreemption()), ChangeableWithoutRestart::Yes}},
            {"cpu_slot_quantum_ns", {std::to_string(context->getCPUSlotQuantum()), ChangeableWithoutRestart::Yes}},
            {"cpu_slot_preemption_timeout_ms", {std::to_string(context->getCPUSlotPreemptionTimeout()), ChangeableWithoutRestart::Yes}},
            {"config_reload_interval_ms", {std::to_string(context->getConfigReloaderInterval()), ChangeableWithoutRestart::Yes}},

            {"allow_feature_tier",
                {std::to_string(context->getAccessControl().getAllowTierSettings()), ChangeableWithoutRestart::Yes}},
            {"s3queue_disable_streaming", {"0", ChangeableWithoutRestart::Yes}},

            {"max_remote_read_network_bandwidth_for_server",
             {context->getRemoteReadThrottler() ? std::to_string(context->getRemoteReadThrottler()->getMaxSpeed()) : "0", ChangeableWithoutRestart::Yes}},
            {"max_remote_write_network_bandwidth_for_server",
             {context->getRemoteWriteThrottler() ? std::to_string(context->getRemoteWriteThrottler()->getMaxSpeed()) : "0", ChangeableWithoutRestart::Yes}},
            {"max_local_read_bandwidth_for_server",
             {context->getLocalReadThrottler() ? std::to_string(context->getLocalReadThrottler()->getMaxSpeed()) : "0", ChangeableWithoutRestart::Yes}},
            {"max_local_write_bandwidth_for_server",
             {context->getLocalWriteThrottler() ? std::to_string(context->getLocalWriteThrottler()->getMaxSpeed()) : "0", ChangeableWithoutRestart::Yes}},
            {"max_distributed_cache_read_bandwidth_for_server",
             {context->getDistributedCacheReadThrottler() ? std::to_string(context->getDistributedCacheReadThrottler()->getMaxSpeed()) : "0", ChangeableWithoutRestart::Yes}},
            {"max_distributed_cache_write_bandwidth_for_server",
             {context->getDistributedCacheWriteThrottler() ? std::to_string(context->getDistributedCacheWriteThrottler()->getMaxSpeed()) : "0", ChangeableWithoutRestart::Yes}},
            {"max_io_thread_pool_size",
             {getIOThreadPool().isInitialized() ? std::to_string(getIOThreadPool().get().getMaxThreads()) : "0", ChangeableWithoutRestart::Yes}},
            {"max_io_thread_pool_free_size",
             {getIOThreadPool().isInitialized() ? std::to_string(getIOThreadPool().get().getMaxFreeThreads()) : "0", ChangeableWithoutRestart::Yes}},
            {"io_thread_pool_queue_size",
             {getIOThreadPool().isInitialized() ? std::to_string(getIOThreadPool().get().getQueueSize()) : "0", ChangeableWithoutRestart::Yes}},
            {"max_backups_io_thread_pool_size",
             {getBackupsIOThreadPool().isInitialized() ? std::to_string(getBackupsIOThreadPool().get().getMaxThreads()) : "0", ChangeableWithoutRestart::Yes}},
            {"max_backups_io_thread_pool_free_size",
             {getBackupsIOThreadPool().isInitialized() ? std::to_string(getBackupsIOThreadPool().get().getMaxFreeThreads()) : "0", ChangeableWithoutRestart::Yes}},
            {"backups_io_thread_pool_queue_size",
             {getBackupsIOThreadPool().isInitialized() ? std::to_string(getBackupsIOThreadPool().get().getQueueSize()) : "0", ChangeableWithoutRestart::Yes}},
            {"max_fetch_partition_thread_pool_size",
             {getFetchPartitionThreadPool().isInitialized() ? std::to_string(getFetchPartitionThreadPool().get().getMaxThreads()) : "0", ChangeableWithoutRestart::Yes}},
            {"max_active_parts_loading_thread_pool_size",
             {getActivePartsLoadingThreadPool().isInitialized() ? std::to_string(getActivePartsLoadingThreadPool().get().getMaxThreads()) : "0", ChangeableWithoutRestart::Yes}},
            {"max_outdated_parts_loading_thread_pool_size",
             {getOutdatedPartsLoadingThreadPool().isInitialized() ? std::to_string(getOutdatedPartsLoadingThreadPool().get().getMaxThreads()) : "0", ChangeableWithoutRestart::Yes}},
            {"max_parts_cleaning_thread_pool_size",
             {getPartsCleaningThreadPool().isInitialized() ? std::to_string(getPartsCleaningThreadPool().get().getMaxThreads()) : "0", ChangeableWithoutRestart::Yes}},
            {"max_prefixes_deserialization_thread_pool_size",
             {getMergeTreePrefixesDeserializationThreadPool().isInitialized() ? std::to_string(getMergeTreePrefixesDeserializationThreadPool().get().getMaxThreads()) : "0", ChangeableWithoutRestart::Yes}},
            {"max_prefixes_deserialization_thread_pool_free_size",
             {getMergeTreePrefixesDeserializationThreadPool().isInitialized() ? std::to_string(getMergeTreePrefixesDeserializationThreadPool().get().getMaxFreeThreads()) : "0", ChangeableWithoutRestart::Yes}},
            {"prefixes_deserialization_thread_pool_thread_pool_queue_size",
             {getMergeTreePrefixesDeserializationThreadPool().isInitialized() ? std::to_string(getMergeTreePrefixesDeserializationThreadPool().get().getQueueSize()) : "0", ChangeableWithoutRestart::Yes}},
            {"max_format_parsing_thread_pool_size",
             {getFormatParsingThreadPool().isInitialized() ? std::to_string(getFormatParsingThreadPool().get().getMaxThreads()) : "0", ChangeableWithoutRestart::Yes}},
            {"max_format_parsing_thread_pool_free_size",
             {getFormatParsingThreadPool().isInitialized() ? std::to_string(getFormatParsingThreadPool().get().getMaxFreeThreads()) : "0", ChangeableWithoutRestart::Yes}},
            {"format_parsing_thread_pool_queue_size",
             {getFormatParsingThreadPool().isInitialized() ? std::to_string(getFormatParsingThreadPool().get().getQueueSize()) : "0", ChangeableWithoutRestart::Yes}},

            {"abort_on_logical_error", {std::to_string(DB::abort_on_logical_error), ChangeableWithoutRestart::Yes}},

            {"dns_allow_resolve_names_to_ipv4", {std::to_string(DNSResolver::instance().getFilterIPv4()), ChangeableWithoutRestart::Yes}},
            {"dns_allow_resolve_names_to_ipv6", {std::to_string(DNSResolver::instance().getFilterIPv6()), ChangeableWithoutRestart::Yes}},
    };

    if (context->areBackgroundExecutorsInitialized())
    {
        changeable_settings.insert(
            {"background_pool_size",
             {std::to_string(context->getMergeMutateExecutor()->getMaxThreads()), ChangeableWithoutRestart::IncreaseOnly}});
        changeable_settings.insert(
            {"background_move_pool_size",
             {std::to_string(context->getMovesExecutor()->getMaxThreads()), ChangeableWithoutRestart::IncreaseOnly}});
        changeable_settings.insert(
            {"background_fetches_pool_size",
             {std::to_string(context->getFetchesExecutor()->getMaxThreads()), ChangeableWithoutRestart::IncreaseOnly}});
        changeable_settings.insert(
            {"background_common_pool_size",
             {std::to_string(context->getCommonExecutor()->getMaxThreads()), ChangeableWithoutRestart::IncreaseOnly}});
    }

    for (const auto & setting : impl->all())
    {
        const auto & setting_name = setting.getName();
        String setting_path {setting.getPath()};

        const auto & changeable_settings_it = changeable_settings.find(setting_name);
        const bool is_changeable = (changeable_settings_it != changeable_settings.end());

        res_columns[0]->insert(setting_path.empty() ? setting_name : setting_path);
        res_columns[1]->insert(is_changeable ? changeable_settings_it->second.first : setting.getValueString());
        res_columns[2]->insert(setting.getDefaultValueString());
        res_columns[3]->insert(setting.isValueChanged());
        res_columns[4]->insert(setting.getDescription());
        res_columns[5]->insert(setting.getTypeName());
        res_columns[6]->insert(is_changeable ? changeable_settings_it->second.second : ChangeableWithoutRestart::No);
        res_columns[7]->insert(setting.getTier() == SettingsTierType::OBSOLETE);
    }
}
}
