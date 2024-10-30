---
slug: /en/operations/server-configuration-parameters/settings
sidebar_position: 57
sidebar_label: Global Server Settings
description: This section contains descriptions of server settings that cannot be changed at the session or query level.
---

# Global Server Settings

This section contains descriptions of server settings that cannot be changed at the session or query level.

These settings are stored in the `config.xml` file on the ClickHouse server.

Other settings are described in the “[Settings](../../operations/settings/index.md#session-settings-intro)” section.

Before studying the settings, read the [Configuration files](../../operations/configuration-files.md#configuration_files) section and note the use of substitutions (the `incl` and `optional` attributes).

## allow_use_jemalloc_memory

Allows to use jemalloc memory.

Type: Bool

Default: 1


## asynchronous_heavy_metrics_update_period_s

Period in seconds for updating asynchronous metrics.

Type: UInt32

Default: 120


## asynchronous_metrics_update_period_s

Period in seconds for updating asynchronous metrics.

Type: UInt32

Default: 1


## auth_use_forwarded_address

Use originating address for authentication for clients connected through proxy.

:::note
This setting should be used with extra caution since forwarded address can be easily spoofed - server accepting such authentication should not be accessed directly but rather exclusively through a trusted proxy.
:::

Type: Bool

Default: 0


## background_buffer_flush_schedule_pool_size

The maximum number of threads that will be used for performing flush operations for Buffer-engine tables in the background.

Type: UInt64

Default: 16


## background_common_pool_size

The maximum number of threads that will be used for performing a variety of operations (mostly garbage collection) for \*MergeTree-engine tables in a background.

Type: UInt64

Default: 8

## background_distributed_schedule_pool_size

The maximum number of threads that will be used for executing distributed sends.

Type: UInt64

Default: 16


## background_fetches_pool_size

The maximum number of threads that will be used for fetching data parts from another replica for \*MergeTree-engine tables in a background.

Type: UInt64

Default: 16

## background_merges_mutations_concurrency_ratio

Sets a ratio between the number of threads and the number of background merges and mutations that can be executed concurrently. For example, if the ratio equals to 2 and
`background_pool_size` is set to 16 then ClickHouse can execute 32 background merges concurrently. This is possible, because background operations could be suspended and postponed. This is needed to give small merges more execution priority. You can only increase this ratio at runtime. To lower it you have to restart the server.
The same as for `background_pool_size` setting `background_merges_mutations_concurrency_ratio` could be applied from the `default` profile for backward compatibility.

Type: Float

Default: 2

## background_merges_mutations_scheduling_policy

The policy on how to perform a scheduling for background merges and mutations. Possible values are: `round_robin` and `shortest_task_first`.

Algorithm used to select next merge or mutation to be executed by background thread pool. Policy may be changed at runtime without server restart.
Could be applied from the `default` profile for backward compatibility.

Possible values:

- "round_robin" — Every concurrent merge and mutation is executed in round-robin order to ensure starvation-free operation. Smaller merges are completed faster than bigger ones just because they have fewer blocks to merge.
- "shortest_task_first" — Always execute smaller merge or mutation. Merges and mutations are assigned priorities based on their resulting size. Merges with smaller sizes are strictly preferred over bigger ones. This policy ensures the fastest possible merge of small parts but can lead to indefinite starvation of big merges in partitions heavily overloaded by INSERTs.

Type: String

Default: round_robin

## background_message_broker_schedule_pool_size

The maximum number of threads that will be used for executing background operations for message streaming.

Type: UInt64

Default: 16

## background_move_pool_size

The maximum number of threads that will be used for moving data parts to another disk or volume for *MergeTree-engine tables in a background.

Type: UInt64

Default: 8

## background_pool_size

Sets the number of threads performing background merges and mutations for tables with MergeTree engines. You can only increase the number of threads at runtime. To lower the number of threads you have to restart the server. By adjusting this setting, you manage CPU and disk load. Smaller pool size utilizes less CPU and disk resources, but background processes advance slower which might eventually impact query performance.

Before changing it, please also take a look at related MergeTree settings, such as `number_of_free_entries_in_pool_to_lower_max_size_of_merge` and `number_of_free_entries_in_pool_to_execute_mutation`.

Type: UInt64

Default: 16

## background_schedule_pool_size

The maximum number of threads that will be used for constantly executing some lightweight periodic operations for replicated tables, Kafka streaming, and DNS cache updates.

Type: UInt64

Default: 512

## backup_threads

The maximum number of threads to execute BACKUP requests.

Type: UInt64

Default: 16


## backups_io_thread_pool_queue_size

The maximum number of jobs that can be scheduled on the Backups IO Thread pool. It is recommended to keep this queue unlimited (0) due to the current S3 backup logic.

Type: UInt64

Default: 0

## cache_size_to_ram_max_ratio

Set cache size to RAM max ratio. Allows lowering the cache size on low-memory systems.

Type: Double

Default: 0.5

## concurrent_threads_soft_limit_num

The maximum number of query processing threads, excluding threads for retrieving data from remote servers, allowed to run all queries. This is not a hard limit. In case if the limit is reached the query will still get at least one thread to run. Query can upscale to desired number of threads during execution if more threads become available.

Zero means Unlimited.

Type: UInt64

Default: 0

## concurrent_threads_soft_limit_ratio_to_cores

Same as concurrent_threads_soft_limit_num, but with ratio to cores.

Type: UInt64

Default: 0


## default_database

Default database name.

Type: String

Default: default

## disable_internal_dns_cache

Disables the internal DNS cache. Recommended for operating ClickHouse in systems
with frequently changing infrastructure such as Kubernetes.

Type: Bool

Default: 0


## dns_cache_max_entries

Internal DNS cache max entries.

Type: UInt64

Default: 10000


## dns_cache_update_period

Internal DNS cache update period in seconds.

Type: Int32

Default: 15


## dns_max_consecutive_failures

Max consecutive resolving failures before dropping a host from ClickHouse DNS cache

Type: UInt32

Default: 10


## index_mark_cache_policy

Index mark cache policy name.

Type: String

Default: SLRU

## index_mark_cache_size

Size of cache for index marks. Zero means disabled.

:::note
This setting can be modified at runtime and will take effect immediately.
:::

Type: UInt64

Default: 0

## index_mark_cache_size_ratio

The size of the protected queue in the index mark cache relative to the cache's total size.

Type: Double

Default: 0.5

## index_uncompressed_cache_policy

Index uncompressed cache policy name.

Type: String

Default: SLRU

## index_uncompressed_cache_size

Size of cache for uncompressed blocks of MergeTree indices. Zero means disabled.

:::note
This setting can be modified at runtime and will take effect immediately.
:::

Type: UInt64

Default: 0

## index_uncompressed_cache_size_ratio

The size of the protected queue in the index uncompressed cache relative to the cache's total size.

Type: Double

Default: 0.5

## io_thread_pool_queue_size

Queue size for IO thread pool. Zero means unlimited.

Type: UInt64

Default: 10000

## mark_cache_policy

Mark cache policy name.

Type: String

Default: SLRU

## mark_cache_size

Size of cache for marks (index of MergeTree family of tables).

:::note
This setting can be modified at runtime and will take effect immediately.
:::

Type: UInt64

Default: 5368709120

## mark_cache_size_ratio

The size of the protected queue in the mark cache relative to the cache's total size.

Type: Double

Default: 0.5

## max_backup_bandwidth_for_server

The maximum read speed in bytes per second for all backups on server. Zero means unlimited.

Type: UInt64

Default: 0

## max_backups_io_thread_pool_free_size

If the number of **idle** threads in the Backups IO Thread pool exceeds `max_backup_io_thread_pool_free_size`, ClickHouse will release resources occupied by idling threads and decrease the pool size. Threads can be created again if necessary.

Type: UInt64

Default: 0

## max_backups_io_thread_pool_size

The maximum number of threads that would be used for IO operations for BACKUP queries

Type: UInt64

Default: 1000

## max_concurrent_queries

Limit on total number of concurrently executed queries. Zero means Unlimited. Note that limits on insert and select queries, and on the maximum number of queries for users must also be considered.  See also max_concurrent_insert_queries, max_concurrent_select_queries, max_concurrent_queries_for_all_users. Zero means unlimited.

:::note
This setting can be modified at runtime and will take effect immediately. Queries that are already running will remain unchanged.
:::

Type: UInt64

Default: 0

## max_concurrent_insert_queries

Limit on total number of concurrent insert queries. Zero means Unlimited.

:::note
This setting can be modified at runtime and will take effect immediately. Queries that are already running will remain unchanged.
:::

Type: UInt64

Default: 0

## max_concurrent_select_queries

Limit on total number of concurrently select queries. Zero means Unlimited.

:::note
This setting can be modified at runtime and will take effect immediately. Queries that are already running will remain unchanged.
:::

Type: UInt64

Default: 0

## max_waiting_queries

Limit on total number of concurrently waiting queries. Execution of a waiting query is blocked while required tables are loading asynchronously (see `async_load_databases`). Note that waiting queries are not counted when `max_concurrent_queries`, `max_concurrent_insert_queries`, `max_concurrent_select_queries`, `max_concurrent_queries_for_user` and `max_concurrent_queries_for_all_users` limits are checked. This correction is done to avoid hitting these limits just after server startup. Zero means unlimited.

:::note
This setting can be modified at runtime and will take effect immediately. Queries that are already running will remain unchanged.
:::

Type: UInt64

Default: 0

## max_connections

Max server connections.

Type: Int32

Default: 1024

## max_io_thread_pool_free_size

Max free size for IO thread pool.

Type: UInt64

Default: 0

## max_io_thread_pool_size

The maximum number of threads that would be used for IO operations

Type: UInt64

Default: 100

## max_local_read_bandwidth_for_server

The maximum speed of local reads in bytes per second. Zero means unlimited.

Type: UInt64

Default: 0

## max_local_write_bandwidth_for_server

The maximum speed of local writes in bytes per second. Zero means unlimited.

Type: UInt64

Default: 0

## max_partition_size_to_drop

Restriction on dropping partitions.

If the size of a [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) table exceeds `max_partition_size_to_drop` (in bytes), you can’t drop a partition using a [DROP PARTITION](../../sql-reference/statements/alter/partition.md#drop-partitionpart) query.
This setting does not require a restart of the ClickHouse server to apply. Another way to disable the restriction is to create the `<clickhouse-path>/flags/force_drop_table` file.
Default value: 50 GB.
The value 0 means that you can drop partitions without any restrictions.

:::note
This limitation does not restrict drop table and truncate table, see [max_table_size_to_drop](#max-table-size-to-drop)
:::

## max_remote_read_network_bandwidth_for_server

The maximum speed of data exchange over the network in bytes per second for read. Zero means unlimited.

Type: UInt64

Default: 0

## max_remote_write_network_bandwidth_for_server

The maximum speed of data exchange over the network in bytes per second for write. Zero means unlimited.

Type: UInt64

Default: 0

## max_server_memory_usage

Limit on total memory usage. Zero means Unlimited.

The default `max_server_memory_usage` value is calculated as `memory_amount * max_server_memory_usage_to_ram_ratio`.

Type: UInt64

Default: 0

## max_server_memory_usage_to_ram_ratio

Same as max_server_memory_usage but in a ratio to physical RAM. Allows lowering the memory usage on low-memory systems. Zero means unlimited.

On hosts with low RAM and swap, you possibly need setting `max_server_memory_usage_to_ram_ratio` larger than 1.

Type: Double

Default: 0.9

## max_build_vector_similarity_index_thread_pool_size {#server_configuration_parameters_max_build_vector_similarity_index_thread_pool_size}

The maximum number of threads to use for building vector indexes. 0 means all cores.

Type: UInt64

Default: 16

## cgroups_memory_usage_observer_wait_time

Interval in seconds during which the server's maximum allowed memory consumption is adjusted by the corresponding threshold in cgroups. (see
settings `cgroup_memory_watcher_hard_limit_ratio` and `cgroup_memory_watcher_soft_limit_ratio`).

To disable the cgroup observer, set this value to `0`.

Type: UInt64

Default: 15

## cgroup_memory_watcher_hard_limit_ratio

Specifies the "hard" threshold with regards to the memory consumption of the server process according to cgroups after which the server's
maximum memory consumption is adjusted to the threshold value.

See settings `cgroups_memory_usage_observer_wait_time` and `cgroup_memory_watcher_soft_limit_ratio`

Type: Double

Default: 0.95

## cgroup_memory_watcher_soft_limit_ratio

Specifies the "soft" threshold with regards to the memory consumption of the server process according to cgroups after which arenas in
jemalloc are purged.


See settings `cgroups_memory_usage_observer_wait_time` and `cgroup_memory_watcher_hard_limit_ratio`

Type: Double

Default: 0.9

## max_table_size_to_drop

Restriction on deleting tables.

If the size of a [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) table exceeds `max_table_size_to_drop` (in bytes), you can’t delete it using a [DROP](../../sql-reference/statements/drop.md) query or [TRUNCATE](../../sql-reference/statements/truncate.md) query.

This setting does not require a restart of the ClickHouse server to apply. Another way to disable the restriction is to create the `<clickhouse-path>/flags/force_drop_table` file.

Default value: 50 GB.
The value 0 means that you can delete all tables without any restrictions.
**Example**
``` xml
<max_table_size_to_drop>0</max_table_size_to_drop>
```


## max\_database\_num\_to\_warn {#max-database-num-to-warn}
If the number of attached databases exceeds the specified value, clickhouse server will add warning messages to `system.warnings` table.
Default value: 1000

**Example**

``` xml
<max_database_num_to_warn>50</max_database_num_to_warn>
```

## max\_table\_num\_to\_warn {#max-table-num-to-warn}
If the number of attached tables exceeds the specified value, clickhouse server will add warning messages to `system.warnings` table.
Default value: 5000

**Example**

``` xml
<max_table_num_to_warn>400</max_table_num_to_warn>
```

## max\_view\_num\_to\_warn {#max-view-num-to-warn}
If the number of attached views exceeds the specified value, clickhouse server will add warning messages to `system.warnings` table.
Default value: 10000

**Example**

``` xml
<max_view_num_to_warn>400</max_view_num_to_warn>
```

## max\_dictionary\_num\_to\_warn {#max-dictionary-num-to-warn}
If the number of attached dictionaries exceeds the specified value, clickhouse server will add warning messages to `system.warnings` table.
Default value: 1000

**Example**

``` xml
<max_dictionary_num_to_warn>400</max_dictionary_num_to_warn>
```

## max\_part\_num\_to\_warn {#max-part-num-to-warn}
If the number of active parts exceeds the specified value, clickhouse server will add warning messages to `system.warnings` table.
Default value: 100000

**Example**

``` xml
<max_part_num_to_warn>400</max_part_num_to_warn>
```

## max\_table\_num\_to\_throw {#max-table-num-to-throw}
If number of tables is greater than this value, server will throw an exception. 0 means no limitation. View, remote tables, dictionary, system tables are not counted. Only count table in Atomic/Ordinary/Replicated/Lazy database engine.Default value: 0

**Example**
```xml
<max_table_num_to_throw>400</max_table_num_to_throw>
```

## max\_database\_num\_to\_throw {#max-table-num-to-throw}
If number of _database is greater than this value, server will throw an exception. 0 means no limitation.
Default value: 0

**Example**
```xml
<max_database_num_to_throw>400</max_database_num_to_throw>
```

## max_temporary_data_on_disk_size

The maximum amount of storage that could be used for external aggregation, joins or sorting.
Queries that exceed this limit will fail with an exception. Zero means unlimited.

Also see `max_temporary_data_on_disk_size_for_user` and `max_temporary_data_on_disk_size_for_query`.

Type: UInt64

Default: 0

## max_thread_pool_free_size

If the number of **idle** threads in the Global Thread pool is greater than `max_thread_pool_free_size`, then ClickHouse releases resources occupied by some threads and the pool size is decreased. Threads can be created again if necessary.

Type: UInt64

Default: 1000

## max_thread_pool_size

The maximum number of threads that could be allocated from the OS and used for query execution and background operations.

Type: UInt64

Default: 10000

## mmap_cache_size

Sets the cache size (in bytes) for mapped files. This setting allows avoiding frequent open/close calls (which are very expensive due to consequent page faults), and to reuse mappings from several threads and queries. The setting value is the number of mapped regions (usually equal to the number of mapped files). The amount of data in mapped files can be monitored in the tables system.metrics and system.metric_log with the `MMappedFiles` and `MMappedFileBytes` metrics.  Also, in system.asynchronous_metrics and system.asynchronous_metrics_log by the `MMapCacheCells` metric, and in system.events, system.processes, system.query_log, system.query_thread_log, system.query_views_log by the `CreatedReadBufferMMap`, `CreatedReadBufferMMapFailed`, `MMappedFileCacheHits`, `MMappedFileCacheMisses` events.

Note that the amount of data in mapped files does not consume memory directly and is not accounted for in query or server memory usage — because this memory can be discarded similar to the OS page cache. The cache is dropped (the files are closed) automatically on the removal of old parts in tables of the MergeTree family, also it can be dropped manually by the `SYSTEM DROP MMAP CACHE` query.

:::note
This setting can be modified at runtime and will take effect immediately.
:::

Type: UInt64

Default: 1000

## restore_threads

The maximum number of threads to execute RESTORE requests.

Type: UInt64

Default: 16

## show_addresses_in_stack_traces

If it is set true will show addresses in stack traces

Type: Bool

Default: 1

## shutdown_wait_unfinished_queries

If set true ClickHouse will wait for running queries finish before shutdown.

Type: Bool

Default: 0

## temporary_data_in_cache

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

Type: String

Default:

## thread_pool_queue_size

TThe maximum number of jobs that can be scheduled on the Global Thread pool. Increasing queue size leads to larger memory usage. It is recommended to keep this value equal to `max_thread_pool_size`. Zero means unlimited.

Type: UInt64

Default: 10000

## tmp_policy

Policy for storage with temporary data. Also see the MergeTree Table Engine documentation.

:::note
- Only one option can be used to configure temporary data storage: `tmp_path` ,`tmp_policy`, `temporary_data_in_cache`.
- `move_factor`, `keep_free_space_bytes`,`max_data_part_size_bytes` and are ignored.
- Policy should have exactly *one volume* with *local* disks.
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
Type: String

Default:

## uncompressed_cache_policy

Uncompressed cache policy name.

Type: String

Default: SLRU


## uncompressed_cache_size

Cache size (in bytes) for uncompressed data used by table engines from the MergeTree family. Zero means disabled.

There is one shared cache for the server. Memory is allocated on demand. The cache is used if the option use_uncompressed_cache is enabled.

The uncompressed cache is advantageous for very short queries in individual cases.

:::note
This setting can be modified at runtime and will take effect immediately.
:::

Type: UInt64

Default: 0

## uncompressed_cache_size_ratio

The size of the protected queue in the uncompressed cache relative to the cache's total size.

Type: Double

Default: 0.5

## builtin_dictionaries_reload_interval {#builtin-dictionaries-reload-interval}

The interval in seconds before reloading built-in dictionaries.

ClickHouse reloads built-in dictionaries every x seconds. This makes it possible to edit dictionaries “on the fly” without restarting the server.

Default value: 3600.

**Example**

``` xml
<builtin_dictionaries_reload_interval>3600</builtin_dictionaries_reload_interval>
```

## compression {#server-settings-compression}

Data compression settings for [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md)-engine tables.

:::note
Don’t use it if you have just started using ClickHouse.
:::

Configuration template:

``` xml
<compression>
    <case>
      <min_part_size>...</min_part_size>
      <min_part_size_ratio>...</min_part_size_ratio>
      <method>...</method>
      <level>...</level>
    </case>
    ...
</compression>
```

`<case>` fields:

- `min_part_size` – The minimum size of a data part.
- `min_part_size_ratio` – The ratio of the data part size to the table size.
- `method` – Compression method. Acceptable values: `lz4`, `lz4hc`, `zstd`,`deflate_qpl`.
- `level` – Compression level. See [Codecs](../../sql-reference/statements/create/table.md#create-query-general-purpose-codecs).

You can configure multiple `<case>` sections.

Actions when conditions are met:

- If a data part matches a condition set, ClickHouse uses the specified compression method.
- If a data part matches multiple condition sets, ClickHouse uses the first matched condition set.

If no conditions met for a data part, ClickHouse uses the `lz4` compression.

**Example**

``` xml
<compression incl="clickhouse_compression">
    <case>
        <min_part_size>10000000000</min_part_size>
        <min_part_size_ratio>0.01</min_part_size_ratio>
        <method>zstd</method>
        <level>1</level>
    </case>
</compression>
```

## encryption {#server-settings-encryption}

Configures a command to obtain a key to be used by [encryption codecs](../../sql-reference/statements/create/table.md#create-query-encryption-codecs). Key (or keys) should be written in environment variables or set in the configuration file.

Keys can be hex or string with a length equal to 16 bytes.

**Example**

Loading from config:

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <key>1234567812345678</key>
    </aes_128_gcm_siv>
</encryption_codecs>
```

:::note
Storing keys in the configuration file is not recommended. It isn't secure. You can move the keys into a separate config file on a secure disk and put a symlink to that config file to `config.d/` folder.
:::

Loading from config, when the key is in hex:

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <key_hex>00112233445566778899aabbccddeeff</key_hex>
    </aes_128_gcm_siv>
</encryption_codecs>
```

Loading key from the environment variable:

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <key_hex from_env="ENVVAR"></key_hex>
    </aes_128_gcm_siv>
</encryption_codecs>
```

Here `current_key_id` sets the current key for encryption, and all specified keys can be used for decryption.

Each of these methods can be applied for multiple keys:

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <key_hex id="0">00112233445566778899aabbccddeeff</key_hex>
        <key_hex id="1" from_env="ENVVAR"></key_hex>
        <current_key_id>1</current_key_id>
    </aes_128_gcm_siv>
</encryption_codecs>
```

Here `current_key_id` shows current key for encryption.

Also, users can add nonce that must be 12 bytes long (by default encryption and decryption processes use nonce that consists of zero bytes):

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <nonce>012345678910</nonce>
    </aes_128_gcm_siv>
</encryption_codecs>
```

Or it can be set in hex:

```xml
<encryption_codecs>
    <aes_128_gcm_siv>
        <nonce_hex>abcdefabcdef</nonce_hex>
    </aes_128_gcm_siv>
</encryption_codecs>
```

Everything mentioned above can be applied for `aes_256_gcm_siv` (but the key must be 32 bytes long).

## error_log {#error_log}

It is disabled by default.

**Enabling**

To manually turn on error history collection [`system.error_log`](../../operations/system-tables/error_log.md), create `/etc/clickhouse-server/config.d/error_log.xml` with the following content:

``` xml
<clickhouse>
    <error_log>
        <database>system</database>
        <table>error_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <collect_interval_milliseconds>1000</collect_interval_milliseconds>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
    </error_log>
</clickhouse>
```

**Disabling**

To disable `error_log` setting, you should create the following file `/etc/clickhouse-server/config.d/disable_error_log.xml` with the following content:

``` xml
<clickhouse>
<error_log remove="1" />
</clickhouse>
```

## custom_settings_prefixes {#custom_settings_prefixes}

List of prefixes for [custom settings](../../operations/settings/index.md#custom_settings). The prefixes must be separated with commas.

**Example**

```xml
<custom_settings_prefixes>custom_</custom_settings_prefixes>
```

**See Also**

- [Custom settings](../../operations/settings/index.md#custom_settings)

## core_dump {#core_dump}

Configures soft limit for core dump file size.

Possible values:

- Positive integer.

Default value: `1073741824` (1 GB).

:::note
Hard limit is configured via system tools
:::

**Example**

```xml
<core_dump>
    <size_limit>1073741824</size_limit>
</core_dump>
```

## database_atomic_delay_before_drop_table_sec {#database_atomic_delay_before_drop_table_sec}

The delay during which a dropped table can be restored using the [UNDROP](/docs/en/sql-reference/statements/undrop.md) statement. If `DROP TABLE` ran with a `SYNC` modifier, the setting is ignored.

Default value: `480` (8 minutes).

## database_catalog_unused_dir_hide_timeout_sec {#database_catalog_unused_dir_hide_timeout_sec}

Parameter of a task that cleans up garbage from `store/` directory.
If some subdirectory is not used by clickhouse-server and this directory was not modified for last
`database_catalog_unused_dir_hide_timeout_sec` seconds, the task will "hide" this directory by
removing all access rights. It also works for directories that clickhouse-server does not
expect to see inside `store/`. Zero means "immediately".

Default value: `3600` (1 hour).

## database_catalog_unused_dir_rm_timeout_sec {#database_catalog_unused_dir_rm_timeout_sec}

Parameter of a task that cleans up garbage from `store/` directory.
If some subdirectory is not used by clickhouse-server and it was previously "hidden"
(see [database_catalog_unused_dir_hide_timeout_sec](../../operations/server-configuration-parameters/settings.md#database_catalog_unused_dir_hide_timeout_sec))
and this directory was not modified for last
`database_catalog_unused_dir_rm_timeout_sec` seconds, the task will remove this directory.
It also works for directories that clickhouse-server does not
expect to see inside `store/`. Zero means "never".

Default value: `2592000` (30 days).

## database_catalog_unused_dir_cleanup_period_sec {#database_catalog_unused_dir_cleanup_period_sec}

Parameter of a task that cleans up garbage from `store/` directory.
Sets scheduling period of the task. Zero means "never".

Default value: `86400` (1 day).

## default_profile {#default-profile}

Default settings profile.

Settings profiles are located in the file specified in the parameter `user_config`.

**Example**

``` xml
<default_profile>default</default_profile>
```

## default_replica_path {#default_replica_path}

The path to the table in ZooKeeper.

**Example**

``` xml
<default_replica_path>/clickhouse/tables/{uuid}/{shard}</default_replica_path>
```

## default_replica_name {#default_replica_name}

 The replica name in ZooKeeper.

**Example**

``` xml
<default_replica_name>{replica}</default_replica_name>
```

## dictionaries_config {#dictionaries_config}

The path to the config file for dictionaries.

Path:

- Specify the absolute path or the path relative to the server config file.
- The path can contain wildcards \* and ?.

See also “[Dictionaries](../../sql-reference/dictionaries/index.md)”.

**Example**

``` xml
<dictionaries_config>*_dictionary.xml</dictionaries_config>
```

## user_defined_executable_functions_config {#user_defined_executable_functions_config}

The path to the config file for executable user defined functions.

Path:

- Specify the absolute path or the path relative to the server config file.
- The path can contain wildcards \* and ?.

See also “[Executable User Defined Functions](../../sql-reference/functions/index.md#executable-user-defined-functions).”.

**Example**

``` xml
<user_defined_executable_functions_config>*_function.xml</user_defined_executable_functions_config>
```

## dictionaries_lazy_load {#dictionaries_lazy_load}

Lazy loading of dictionaries.

If `true`, then each dictionary is loaded on the first use. If the loading is failed, the function that was using the dictionary throws an exception.

If `false`, then the server loads all dictionaries at startup.
The server will wait at startup until all the dictionaries finish their loading before receiving any connections
(exception: if `wait_dictionaries_load_at_startup` is set to `false` - see below).

The default is `true`.

**Example**

``` xml
<dictionaries_lazy_load>true</dictionaries_lazy_load>
```

## format_schema_path {#format_schema_path}

The path to the directory with the schemes for the input data, such as schemas for the [CapnProto](../../interfaces/formats.md#capnproto) format.

**Example**

``` xml
  <!-- Directory containing schema files for various input formats. -->
  <format_schema_path>format_schemas/</format_schema_path>
```

## graphite {#graphite}

Sending data to [Graphite](https://github.com/graphite-project).

Settings:

- host – The Graphite server.
- port – The port on the Graphite server.
- interval – The interval for sending, in seconds.
- timeout – The timeout for sending data, in seconds.
- root_path – Prefix for keys.
- metrics – Sending data from the [system.metrics](../../operations/system-tables/metrics.md#system_tables-metrics) table.
- events – Sending deltas data accumulated for the time period from the [system.events](../../operations/system-tables/events.md#system_tables-events) table.
- events_cumulative – Sending cumulative data from the [system.events](../../operations/system-tables/events.md#system_tables-events) table.
- asynchronous_metrics – Sending data from the [system.asynchronous_metrics](../../operations/system-tables/asynchronous_metrics.md#system_tables-asynchronous_metrics) table.

You can configure multiple `<graphite>` clauses. For instance, you can use this for sending different data at different intervals.

**Example**

``` xml
<graphite>
    <host>localhost</host>
    <port>42000</port>
    <timeout>0.1</timeout>
    <interval>60</interval>
    <root_path>one_min</root_path>
    <metrics>true</metrics>
    <events>true</events>
    <events_cumulative>false</events_cumulative>
    <asynchronous_metrics>true</asynchronous_metrics>
</graphite>
```

## graphite_rollup {#graphite-rollup}

Settings for thinning data for Graphite.

For more details, see [GraphiteMergeTree](../../engines/table-engines/mergetree-family/graphitemergetree.md).

**Example**

``` xml
<graphite_rollup_example>
    <default>
        <function>max</function>
        <retention>
            <age>0</age>
            <precision>60</precision>
        </retention>
        <retention>
            <age>3600</age>
            <precision>300</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>3600</precision>
        </retention>
    </default>
</graphite_rollup_example>
```

## http_port/https_port {#http-porthttps-port}

The port for connecting to the server over HTTP(s).

If `https_port` is specified, [openSSL](#openssl) must be configured.

If `http_port` is specified, the OpenSSL configuration is ignored even if it is set.

**Example**

``` xml
<https_port>9999</https_port>
```

## http_server_default_response {#http_server_default_response}

The page that is shown by default when you access the ClickHouse HTTP(s) server.
The default value is “Ok.” (with a line feed at the end)

**Example**

Opens `https://tabix.io/` when accessing `http://localhost: http_port`.

``` xml
<http_server_default_response>
  <![CDATA[<html ng-app="SMI2"><head><base href="http://ui.tabix.io/"></head><body><div ui-view="" class="content-ui"></div><script src="http://loader.tabix.io/master.js"></script></body></html>]]>
</http_server_default_response>
```

## hsts_max_age  {#hsts-max-age}

Expired time for HSTS in seconds. The default value is 0 means clickhouse disabled HSTS. If you set a positive number, the HSTS will be enabled and the max-age is the number you set.

**Example**

```xml
<hsts_max_age>600000</hsts_max_age>
```

## mlock_executable {#mlock_executable}

Perform mlockall after startup to lower first queries latency and to prevent clickhouse executable from being paged out under high IO load. Enabling this option is recommended but will lead to increased startup time for up to a few seconds.
Keep in mind that this parameter would not work without "CAP_IPC_LOCK" capability.
**Example**

``` xml
<mlock_executable>false</mlock_executable>
```

## include_from {#include_from}

The path to the file with substitutions. Both XML and YAML formats are supported.

For more information, see the section “[Configuration files](../../operations/configuration-files.md#configuration_files)”.

**Example**

``` xml
<include_from>/etc/metrica.xml</include_from>
```

## interserver_listen_host {#interserver-listen-host}

Restriction on hosts that can exchange data between ClickHouse servers.
If Keeper is used, the same restriction will be applied to the communication
between different Keeper instances.
The default value equals to `listen_host` setting.

Examples:

``` xml
<interserver_listen_host>::ffff:a00:1</interserver_listen_host>
<interserver_listen_host>10.0.0.1</interserver_listen_host>
```

## interserver_http_port {#interserver-http-port}

Port for exchanging data between ClickHouse servers.

**Example**

``` xml
<interserver_http_port>9009</interserver_http_port>
```

## interserver_http_host {#interserver-http-host}

The hostname that can be used by other servers to access this server.

If omitted, it is defined in the same way as the `hostname -f` command.

Useful for breaking away from a specific network interface.

**Example**

``` xml
<interserver_http_host>example.clickhouse.com</interserver_http_host>
```

## interserver_https_port {#interserver-https-port}

Port for exchanging data between ClickHouse servers over `HTTPS`.

**Example**

``` xml
<interserver_https_port>9010</interserver_https_port>
```

## interserver_https_host {#interserver-https-host}

Similar to `interserver_http_host`, except that this hostname can be used by other servers to access this server over `HTTPS`.

**Example**

``` xml
<interserver_https_host>example.clickhouse.com</interserver_https_host>
```

## interserver_http_credentials {#server-settings-interserver-http-credentials}

A username and a password used to connect to other servers during [replication](../../engines/table-engines/mergetree-family/replication.md). Also the server authenticates other replicas using these credentials. So, `interserver_http_credentials` must be the same for all replicas in a cluster.

By default, if `interserver_http_credentials` section is omitted, authentication is not used during replication.

:::note
`interserver_http_credentials` settings do not relate to a ClickHouse client credentials [configuration](../../interfaces/cli.md#configuration_files).
:::

:::note
These credentials are common for replication via `HTTP` and `HTTPS`.
:::

The section contains the following parameters:

- `user` — Username.
- `password` — Password.
- `allow_empty` — If `true`, then other replicas are allowed to connect without authentication even if credentials are set. If `false`, then connections without authentication are refused. Default value: `false`.
- `old` — Contains old `user` and `password` used during credential rotation. Several `old` sections can be specified.

**Credentials Rotation**

ClickHouse supports dynamic interserver credentials rotation without stopping all replicas at the same time to update their configuration. Credentials can be changed in several steps.

To enable authentication, set `interserver_http_credentials.allow_empty` to `true` and add credentials. This allows connections with authentication and without it.

``` xml
<interserver_http_credentials>
    <user>admin</user>
    <password>111</password>
    <allow_empty>true</allow_empty>
</interserver_http_credentials>
```

After configuring all replicas set `allow_empty` to `false` or remove this setting. It makes authentication with new credentials mandatory.

To change existing credentials, move the username and the password to `interserver_http_credentials.old` section and update `user` and `password` with new values. At this point the server uses new credentials to connect to other replicas and accepts connections with either new or old credentials.

``` xml
<interserver_http_credentials>
    <user>admin</user>
    <password>222</password>
    <old>
        <user>admin</user>
        <password>111</password>
    </old>
    <old>
        <user>temp</user>
        <password>000</password>
    </old>
</interserver_http_credentials>
```

When new credentials are applied to all replicas, old credentials may be removed.

## keep_alive_timeout {#keep-alive-timeout}

The number of seconds that ClickHouse waits for incoming requests before closing the connection. Defaults to 10 seconds.

**Example**

``` xml
<keep_alive_timeout>10</keep_alive_timeout>
```

## max_keep_alive_requests {#max-keep-alive-requests}

Maximal number of requests through a single keep-alive connection until it will be closed by ClickHouse server. Default to 10000.

**Example**

``` xml
<max_keep_alive_requests>10</max_keep_alive_requests>
```

## listen_host {#listen_host}

Restriction on hosts that requests can come from. If you want the server to answer all of them, specify `::`.

Examples:

``` xml
<listen_host>::1</listen_host>
<listen_host>127.0.0.1</listen_host>
```

## listen_try {#listen_try}

The server will not exit if IPv6 or IPv4 networks are unavailable while trying to listen.

Examples:

``` xml
<listen_try>0</listen_try>
```

## listen_reuse_port {#listen_reuse_port}

Allow multiple servers to listen on the same address:port. Requests will be routed to a random server by the operating system. Enabling this setting is not recommended.

Examples:

``` xml
<listen_reuse_port>0</listen_reuse_port>
```

## listen_backlog {#listen_backlog}

Backlog (queue size of pending connections) of the listen socket.

Default value: `4096` (as in linux [5.4+](https://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git/commit/?id=19f92a030ca6d772ab44b22ee6a01378a8cb32d4)).

Usually this value does not need to be changed, since:
- default value is large enough,
- and for accepting client's connections server has separate thread.

So even if you have `TcpExtListenOverflows` (from `nstat`) non zero and this counter grows for ClickHouse server it does not mean that this value need to be increased, since:
- usually if 4096 is not enough it shows some internal ClickHouse scaling issue, so it is better to report an issue.
- and it does not mean that the server can handle more connections later (and even if it could, by that moment clients may be gone or disconnected).

Examples:

``` xml
<listen_backlog>4096</listen_backlog>
```

## logger {#logger}

The location and format of log messages.

Keys:

- `level` – Log level. Acceptable values: `none` (turn logging off), `fatal`, `critical`, `error`, `warning`, `notice`, `information`,
  `debug`, `trace`, `test`
- `log` – The path to the log file.
- `errorlog` – The path to the error log file.
- `size` – Rotation policy: Maximum size of the log files in bytes. Once the log file size exceeds this threshold, it is renamed and archived, and a new log file is created.
- `count` – Rotation policy: How many historical log files Clickhouse are kept at most.
- `stream_compress` – Compress log messages using LZ4. Set to `1` or `true` to enable.
- `console` – Do not write log messages to log files, instead print them in the console. Set to `1` or `true` to enable. Default is
  `1` if Clickhouse does not run in daemon mode, `0` otherwise.
- `console_log_level` – Log level for console output. Defaults to `level`.
- `formatting` – Log format for console output. Currently, only `json` is supported).
- `use_syslog` - Also forward log output to syslog.
- `syslog_level` - Log level for logging to syslog.
- `message_regexp` - Only log messages that match this regular expression. Defaults to `""`, indicating no filtering.
- `message_regexp_negative` - Only log messages that don't match this regular expression. Defaults to `""`, indicating no filtering.

**Log format specifiers**

File names in `log` and `errorLog` paths support below format specifiers for the resulting file name (the directory part does not support them).

Column “Example” shows the output at `2023-07-06 18:32:07`.

| Specifier   | Description                                                                                                         | Example                  |
|-------------|---------------------------------------------------------------------------------------------------------------------|--------------------------|
| %%          | Literal %                                                                                                           | %                        |
| %n          | New-line character                                                                                                  |                          |
| %t          | Horizontal tab character                                                                                            |                          |
| %Y          | Year as a decimal number, e.g. 2017                                                                                 | 2023                     |
| %y          | Last 2 digits of year as a decimal number (range [00,99])                                                           | 23                       |
| %C          | First 2 digits of year as a decimal number (range [00,99])                                                          | 20                       |
| %G          | Four-digit [ISO 8601 week-based year](https://en.wikipedia.org/wiki/ISO_8601#Week_dates), i.e. the year that contains the specified week. Normally useful only with %V  | 2023       |
| %g          | Last 2 digits of [ISO 8601 week-based year](https://en.wikipedia.org/wiki/ISO_8601#Week_dates), i.e. the year that contains the specified week.                         | 23         |
| %b          | Abbreviated month name, e.g. Oct (locale dependent)                                                                 | Jul                      |
| %h          | Synonym of %b                                                                                                       | Jul                      |
| %B          | Full month name, e.g. October (locale dependent)                                                                    | July                     |
| %m          | Month as a decimal number (range [01,12])                                                                           | 07                       |
| %U          | Week of the year as a decimal number (Sunday is the first day of the week) (range [00,53])                          | 27                       |
| %W          | Week of the year as a decimal number (Monday is the first day of the week) (range [00,53])                          | 27                       |
| %V          | ISO 8601 week number (range [01,53])                                                                                | 27                       |
| %j          | Day of the year as a decimal number (range [001,366])                                                               | 187                      |
| %d          | Day of the month as a zero-padded decimal number (range [01,31]). Single digit is preceded by zero.                 | 06                       |
| %e          | Day of the month as a space-padded decimal number (range [1,31]). Single digit is preceded by a space.              | &nbsp; 6                 |
| %a          | Abbreviated weekday name, e.g. Fri (locale dependent)                                                               | Thu                      |
| %A          | Full weekday name, e.g. Friday (locale dependent)                                                                   | Thursday                 |
| %w          | Weekday as a integer number with Sunday as 0 (range [0-6])                                                          | 4                        |
| %u          | Weekday as a decimal number, where Monday is 1 (ISO 8601 format) (range [1-7])                                      | 4                        |
| %H          | Hour as a decimal number, 24 hour clock (range [00-23])                                                             | 18                       |
| %I          | Hour as a decimal number, 12 hour clock (range [01,12])                                                             | 06                       |
| %M          | Minute as a decimal number (range [00,59])                                                                          | 32                       |
| %S          | Second as a decimal number (range [00,60])                                                                          | 07                       |
| %c          | Standard date and time string, e.g. Sun Oct 17 04:41:13 2010 (locale dependent)                                     | Thu Jul  6 18:32:07 2023 |
| %x          | Localized date representation (locale dependent)                                                                    | 07/06/23                 |
| %X          | Localized time representation, e.g. 18:40:20 or 6:40:20 PM (locale dependent)                                       | 18:32:07                 |
| %D          | Short MM/DD/YY date, equivalent to %m/%d/%y                                                                         | 07/06/23                 |
| %F          | Short YYYY-MM-DD date, equivalent to %Y-%m-%d                                                                       | 2023-07-06               |
| %r          | Localized 12-hour clock time (locale dependent)                                                                     | 06:32:07 PM              |
| %R          | Equivalent to "%H:%M"                                                                                               | 18:32                    |
| %T          | Equivalent to "%H:%M:%S" (the ISO 8601 time format)                                                                 | 18:32:07                 |
| %p          | Localized a.m. or p.m. designation (locale dependent)                                                               | PM                       |
| %z          | Offset from UTC in the ISO 8601 format (e.g. -0430), or no characters if the time zone information is not available | +0800                    |
| %Z          | Locale-dependent time zone name or abbreviation, or no characters if the time zone information is not available     | Z AWST                   |

**Example**

``` xml
<logger>
    <level>trace</level>
    <log>/var/log/clickhouse-server/clickhouse-server-%F-%T.log</log>
    <errorlog>/var/log/clickhouse-server/clickhouse-server-%F-%T.err.log</errorlog>
    <size>1000M</size>
    <count>10</count>
    <stream_compress>true</stream_compress>
</logger>
```

To print log messages only in the console:

``` xml
<logger>
    <level>information</level>
    <console>true</console>
</logger>
```

**Per-level Overrides**

The log level of individual log names can be overridden. For example, to mute all messages of loggers "Backup" and "RBAC".

```xml
<logger>
    <levels>
        <logger>
            <name>Backup</name>
            <level>none</level>
        </logger>
        <logger>
            <name>RBAC</name>
            <level>none</level>
        </logger>
    </levels>
</logger>
```

**Regular Expression Filtering**

The messages logged can be filtered using regular expressions using `message_regexp` and `message_regexp_negative`. This can be done on a per-level basis or globally. If both a global and logger-specific pattern is specified, the global pattern is overridden (ignored) and only the logger-specific pattern applies. The positive and negative patterns are considered independently for this situation. Note: Using this feature may cause a slight slowdown in performance.


```xml
    <logger>
        <level>trace</level>
        <!-- Global: Don't log Trace messages -->
        <message_regexp_negative>.*Trace.*</message_regexp_negative>

        <message_regexps>
            <logger>
                <!-- For the executeQuery logger, only log if message has "Read", but not "from" -->
                <name>executeQuery</name>
                <message_regexp>.*Read.*</message_regexp>
                <message_regexp_negative>.*from.*</message_regexp_negative>
            </logger>
        </message_regexps>
    </logger>
```

### syslog

To write log messages additionally to syslog:

``` xml
<logger>
    <use_syslog>1</use_syslog>
    <syslog>
        <address>syslog.remote:10514</address>
        <hostname>myhost.local</hostname>
        <facility>LOG_LOCAL6</facility>
        <format>syslog</format>
    </syslog>
</logger>
```

Keys for `<syslog>`:

- `address` — The address of syslog in format `host\[:port\]`. If omitted, the local daemon is used.
- `hostname` — The name of the host from which logs are send. Optional.
- `facility` — The syslog [facility keyword](https://en.wikipedia.org/wiki/Syslog#Facility). Must be specified uppercase with a “LOG_” prefix, e.g. `LOG_USER`, `LOG_DAEMON`, `LOG_LOCAL3`, etc. Default value: `LOG_USER` if `address` is specified, `LOG_DAEMON` otherwise.
- `format` – Log message format. Possible values: `bsd` and `syslog.`

### Log formats

You can specify the log format that will be outputted in the console log. Currently, only JSON is supported. Here is an example of an output JSON log:

```json
{
  "date_time": "1650918987.180175",
  "thread_name": "#1",
  "thread_id": "254545",
  "level": "Trace",
  "query_id": "",
  "logger_name": "BaseDaemon",
  "message": "Received signal 2",
  "source_file": "../base/daemon/BaseDaemon.cpp; virtual void SignalListener::run()",
  "source_line": "192"
}
```

To enable JSON logging support, use the following snippet:

```xml
<logger>
    <formatting>
        <type>json</type>
        <names>
            <date_time>date_time</date_time>
            <thread_name>thread_name</thread_name>
            <thread_id>thread_id</thread_id>
            <level>level</level>
            <query_id>query_id</query_id>
            <logger_name>logger_name</logger_name>
            <message>message</message>
            <source_file>source_file</source_file>
            <source_line>source_line</source_line>
        </names>
    </formatting>
</logger>
```

**Renaming keys for JSON logs**

Key names can be modified by changing tag values inside the `<names>` tag. For example, to change `DATE_TIME` to `MY_DATE_TIME`, you can use `<date_time>MY_DATE_TIME</date_time>`.

**Omitting keys for JSON logs**

Log properties can be omitted by commenting out the property.  For example, if you do not want your log to print `query_id`, you can comment out the `<query_id>` tag.

## send_crash_reports {#send_crash_reports}

Settings for opt-in sending crash reports to the ClickHouse core developers team via [Sentry](https://sentry.io).
Enabling it, especially in pre-production environments, is highly appreciated.

The server will need access to the public Internet via IPv4 (at the time of writing IPv6 is not supported by Sentry) for this feature to be functioning properly.

Keys:

- `enabled` – Boolean flag to enable the feature, `false` by default. Set to `true` to allow sending crash reports.
- `send_logical_errors` – `LOGICAL_ERROR` is like an `assert`, it is a bug in ClickHouse. This boolean flag enables sending this exceptions to sentry (default: `false`).
- `endpoint` – You can override the Sentry endpoint URL for sending crash reports. It can be either a separate Sentry account or your self-hosted Sentry instance. Use the [Sentry DSN](https://docs.sentry.io/error-reporting/quickstart/?platform=native#configure-the-sdk) syntax.
- `anonymize` - Avoid attaching the server hostname to the crash report.
- `http_proxy` - Configure HTTP proxy for sending crash reports.
- `debug` - Sets the Sentry client into debug mode.
- `tmp_path` - Filesystem path for temporary crash report state.
- `environment` - An arbitrary name of an environment in which the ClickHouse server is running. It will be mentioned in each crash report. The default value is `test` or `prod` depending on the version of ClickHouse.

**Recommended way to use**

``` xml
<send_crash_reports>
    <enabled>true</enabled>
</send_crash_reports>
```

## macros {#macros}

Parameter substitutions for replicated tables.

Can be omitted if replicated tables are not used.

For more information, see the section [Creating replicated tables](../../engines/table-engines/mergetree-family/replication.md#creating-replicated-tables).

**Example**

``` xml
<macros incl="macros" optional="true" />
```

## replica_group_name {#replica_group_name}

Replica group name for database Replicated.

The cluster created by Replicated database will consist of replicas in the same group.
DDL queries will only wait for the replicas in the same group.

Empty by default.

**Example**

``` xml
<replica_group_name>backups</replica_group_name>
```

Default value: ``.

## max_open_files {#max-open-files}

The maximum number of open files.

By default: `maximum`.

We recommend using this option in macOS since the `getrlimit()` function returns an incorrect value.

**Example**

``` xml
<max_open_files>262144</max_open_files>
```

## max_table_size_to_drop {#max-table-size-to-drop}

Restriction on deleting tables.

If the size of a [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) table exceeds `max_table_size_to_drop` (in bytes), you can’t delete it using a [DROP](../../sql-reference/statements/drop.md) query or [TRUNCATE](../../sql-reference/statements/truncate.md) query.

This setting does not require a restart of the ClickHouse server to apply. Another way to disable the restriction is to create the `<clickhouse-path>/flags/force_drop_table` file.

Default value: 50 GB.

The value 0 means that you can delete all tables without any restrictions.

**Example**

``` xml
<max_table_size_to_drop>0</max_table_size_to_drop>
```

## max_partition_size_to_drop {#max-partition-size-to-drop}

Restriction on dropping partitions.

If the size of a [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) table exceeds `max_partition_size_to_drop` (in bytes), you can’t drop a partition using a [DROP PARTITION](../../sql-reference/statements/alter/partition.md#drop-partitionpart) query.

This setting does not require a restart of the ClickHouse server to apply. Another way to disable the restriction is to create the `<clickhouse-path>/flags/force_drop_table` file.

Default value: 50 GB.

The value 0 means that you can drop partitions without any restrictions.

:::note
This limitation does not restrict drop table and truncate table, see [max_table_size_to_drop](#max-table-size-to-drop)
:::

**Example**

``` xml
<max_partition_size_to_drop>0</max_partition_size_to_drop>
```

## max_thread_pool_size {#max-thread-pool-size}

ClickHouse uses threads from the Global Thread pool to process queries. If there is no idle thread to process a query, then a new thread is created in the pool. `max_thread_pool_size` limits the maximum number of threads in the pool.

Possible values:

- Positive integer.

Default value: `10000`.

**Example**

``` xml
<max_thread_pool_size>12000</max_thread_pool_size>
```

## max_thread_pool_free_size {#max-thread-pool-free-size}

If the number of **idle** threads in the Global Thread pool is greater than `max_thread_pool_free_size`, then ClickHouse releases resources occupied by some threads and the pool size is decreased. Threads can be created again if necessary.

Possible values:

- Positive integer.

Default value: `1000`.

**Example**

``` xml
<max_thread_pool_free_size>1200</max_thread_pool_free_size>
```

## thread_pool_queue_size {#thread-pool-queue-size}

The maximum number of jobs that can be scheduled on the Global Thread pool. Increasing queue size leads to larger memory usage. It is recommended to keep this value equal to [max_thread_pool_size](#max-thread-pool-size).

Possible values:

- Positive integer.
- 0 — No limit.

Default value: `10000`.

**Example**

``` xml
<thread_pool_queue_size>12000</thread_pool_queue_size>
```

## max_io_thread_pool_size {#max-io-thread-pool-size}

ClickHouse uses threads from the IO Thread pool to do some IO operations (e.g. to interact with S3). `max_io_thread_pool_size` limits the maximum number of threads in the pool.

Possible values:

- Positive integer.

Default value: `100`.

## max_io_thread_pool_free_size {#max-io-thread-pool-free-size}

If the number of **idle** threads in the IO Thread pool exceeds `max_io_thread_pool_free_size`, ClickHouse will release resources occupied by idling threads and decrease the pool size. Threads can be created again if necessary.

Possible values:

- Positive integer.

Default value: `0`.

## io_thread_pool_queue_size {#io-thread-pool-queue-size}

The maximum number of jobs that can be scheduled on the IO Thread pool.

Possible values:

- Positive integer.
- 0 — No limit.

Default value: `10000`.

## max_backups_io_thread_pool_size {#max-backups-io-thread-pool-size}

ClickHouse uses threads from the Backups IO Thread pool to do S3 backup IO operations. `max_backups_io_thread_pool_size` limits the maximum number of threads in the pool.

Possible values:

- Positive integer.

Default value: `1000`.

## max_backups_io_thread_pool_free_size {#max-backups-io-thread-pool-free-size}

If the number of **idle** threads in the Backups IO Thread pool exceeds `max_backup_io_thread_pool_free_size`, ClickHouse will release resources occupied by idling threads and decrease the pool size. Threads can be created again if necessary.

Possible values:

- Positive integer.
- Zero.

Default value: `0`.

## backups_io_thread_pool_queue_size {#backups-io-thread-pool-queue-size}

The maximum number of jobs that can be scheduled on the Backups IO Thread pool. It is recommended to keep this queue unlimited due to the current S3 backup logic.

Possible values:

- Positive integer.
- 0 — No limit.

Default value: `0`.

## background_pool_size {#background_pool_size}

Sets the number of threads performing background merges and mutations for tables with MergeTree engines. This setting is also could be applied at server startup from the `default` profile configuration for backward compatibility at the ClickHouse server start. You can only increase the number of threads at runtime. To lower the number of threads you have to restart the server. By adjusting this setting, you manage CPU and disk load. Smaller pool size utilizes less CPU and disk resources, but background processes advance slower which might eventually impact query performance.

Before changing it, please also take a look at related MergeTree settings, such as [number_of_free_entries_in_pool_to_lower_max_size_of_merge](../../operations/settings/merge-tree-settings.md#number-of-free-entries-in-pool-to-lower-max-size-of-merge) and [number_of_free_entries_in_pool_to_execute_mutation](../../operations/settings/merge-tree-settings.md#number-of-free-entries-in-pool-to-execute-mutation).

Possible values:

- Any positive integer.

Default value: 16.

**Example**

```xml
<background_pool_size>16</background_pool_size>
```

## background_merges_mutations_concurrency_ratio {#background_merges_mutations_concurrency_ratio}

Sets a ratio between the number of threads and the number of background merges and mutations that can be executed concurrently. For example, if the ratio equals to 2 and
`background_pool_size` is set to 16 then ClickHouse can execute 32 background merges concurrently. This is possible, because background operations could be suspended and postponed. This is needed to give small merges more execution priority. You can only increase this ratio at runtime. To lower it you have to restart the server.
The same as for `background_pool_size` setting `background_merges_mutations_concurrency_ratio` could be applied from the `default` profile for backward compatibility.

Possible values:

- Any positive integer.

Default value: 2.

**Example**

```xml
<background_merges_mutations_concurrency_ratio>3</background_merges_mutations_concurrency_ratio>
```

## merges_mutations_memory_usage_soft_limit {#merges_mutations_memory_usage_soft_limit}

Sets the limit on how much RAM is allowed to use for performing merge and mutation operations.
Zero means unlimited.
If ClickHouse reaches this limit, it won't schedule any new background merge or mutation operations but will continue to execute already scheduled tasks.

Possible values:

-   Any positive integer.

**Example**

```xml
<merges_mutations_memory_usage_soft_limit>0</merges_mutations_memory_usage_soft_limit>
```

## merges_mutations_memory_usage_to_ram_ratio {#merges_mutations_memory_usage_to_ram_ratio}

The default `merges_mutations_memory_usage_soft_limit` value is calculated as `memory_amount * merges_mutations_memory_usage_to_ram_ratio`.

Default value: `0.5`.

**See also**

-   [max_memory_usage](../../operations/settings/query-complexity.md#settings_max_memory_usage)
-   [merges_mutations_memory_usage_soft_limit](#merges_mutations_memory_usage_soft_limit)




## async_load_databases {#async_load_databases}

Asynchronous loading of databases and tables.

If `true` all non-system databases with `Ordinary`, `Atomic` and `Replicated` engine will be loaded asynchronously after the ClickHouse server start up. See `system.asynchronous_loader` table, `tables_loader_background_pool_size` and `tables_loader_foreground_pool_size` server settings. Any query that tries to access a table, that is not yet loaded, will wait for exactly this table to be started up. If load job fails, query will rethrow an error (instead of shutting down the whole server in case of `async_load_databases = false`). The table that is waited for by at least one query will be loaded with higher priority. DDL queries on a database will wait for exactly that database to be started up. Also consider setting a limit `max_waiting_queries` for the total number of waiting queries.

If `false`, all databases are loaded when the server starts.

The default is `false`.

**Example**

``` xml
<async_load_databases>true</async_load_databases>
```

## async_load_system_database {#async_load_system_database}

Asynchronous loading of system tables. Helpful if there is a high amount of log tables and parts in the `system` database. Independent of the `async_load_databases` setting.

If set to `true`, all system databases with `Ordinary`, `Atomic`, and `Replicated` engines will be loaded asynchronously after the ClickHouse server starts. See `system.asynchronous_loader` table, `tables_loader_background_pool_size` and `tables_loader_foreground_pool_size` server settings. Any query that tries to access a system table, that is not yet loaded, will wait for exactly this table to be started up. The table that is waited for by at least one query will be loaded with higher priority. Also consider setting the `max_waiting_queries` setting to limit the total number of waiting queries.

If `false`, system database loads before server start.

The default is `false`.

**Example**

``` xml
<async_load_system_database>true</async_load_system_database>
```

## tables_loader_foreground_pool_size {#tables_loader_foreground_pool_size}

Sets the number of threads performing load jobs in foreground pool. The foreground pool is used for loading table synchronously before server start listening on a port and for loading tables that are waited for. Foreground pool has higher priority than background pool. It means that no job starts in background pool while there are jobs running in foreground pool.

Possible values:

-   Any positive integer.
-   Zero. Use all available CPUs.

Default value: 0.


## tables_loader_background_pool_size {#tables_loader_background_pool_size}

Sets the number of threads performing asynchronous load jobs in background pool. The background pool is used for loading tables asynchronously after server start in case there are no queries waiting for the table. It could be beneficial to keep low number of threads in background pool if there are a lot of tables. It will reserve CPU resources for concurrent query execution.

Possible values:

-   Any positive integer.
-   Zero. Use all available CPUs.

Default value: 0.


## merge_tree {#merge_tree}

Fine tuning for tables in the [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md).

For more information, see the MergeTreeSettings.h header file.

**Example**

``` xml
<merge_tree>
    <max_suspicious_broken_parts>5</max_suspicious_broken_parts>
</merge_tree>
```

## metric_log {#metric_log}

It is disabled by default.

**Enabling**

To manually turn on metrics history collection [`system.metric_log`](../../operations/system-tables/metric_log.md), create `/etc/clickhouse-server/config.d/metric_log.xml` with the following content:

``` xml
<clickhouse>
    <metric_log>
        <database>system</database>
        <table>metric_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <collect_interval_milliseconds>1000</collect_interval_milliseconds>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
    </metric_log>
</clickhouse>
```

**Disabling**

To disable `metric_log` setting, you should create the following file `/etc/clickhouse-server/config.d/disable_metric_log.xml` with the following content:

``` xml
<clickhouse>
<metric_log remove="1" />
</clickhouse>
```

## replicated_merge_tree {#replicated_merge_tree}

Fine tuning for tables in the [ReplicatedMergeTree](../../engines/table-engines/mergetree-family/mergetree.md).

This setting has a higher priority.

For more information, see the MergeTreeSettings.h header file.

**Example**

``` xml
<replicated_merge_tree>
    <max_suspicious_broken_parts>5</max_suspicious_broken_parts>
</replicated_merge_tree>
```

## openSSL {#openssl}

SSL client/server configuration.

Support for SSL is provided by the `libpoco` library. The available configuration options are explained in [SSLManager.h](https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/include/Poco/Net/SSLManager.h). Default values can be found in [SSLManager.cpp](https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/src/SSLManager.cpp).

Keys for server/client settings:

- privateKeyFile – The path to the file with the secret key of the PEM certificate. The file may contain a key and certificate at the same time.
- certificateFile – The path to the client/server certificate file in PEM format. You can omit it if `privateKeyFile` contains the certificate.
- caConfig (default: none) – The path to the file or directory that contains trusted CA certificates. If this points to a file, it must be in PEM format and can contain several CA certificates. If this points to a directory, it must contain one .pem file per CA certificate. The filenames are looked up by the CA subject name hash value. Details can be found in the man page of [SSL_CTX_load_verify_locations](https://www.openssl.org/docs/man3.0/man3/SSL_CTX_load_verify_locations.html).
- verificationMode (default: relaxed) – The method for checking the node’s certificates. Details are in the description of the [Context](https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/include/Poco/Net/Context.h) class. Possible values: `none`, `relaxed`, `strict`, `once`.
- verificationDepth (default: 9) – The maximum length of the verification chain. Verification will fail if the certificate chain length exceeds the set value.
- loadDefaultCAFile (default: true) – Wether built-in CA certificates for OpenSSL will be used. ClickHouse assumes that builtin CA certificates are in the file `/etc/ssl/cert.pem` (resp. the directory `/etc/ssl/certs`) or in file (resp. directory) specified by the environment variable `SSL_CERT_FILE` (resp. `SSL_CERT_DIR`).
- cipherList (default: `ALL:!ADH:!LOW:!EXP:!MD5:!3DES:@STRENGTH`) - Supported OpenSSL encryptions.
- cacheSessions (default: false) – Enables or disables caching sessions. Must be used in combination with `sessionIdContext`. Acceptable values: `true`, `false`.
- sessionIdContext (default: `${application.name}`) – A unique set of random characters that the server appends to each generated identifier. The length of the string must not exceed `SSL_MAX_SSL_SESSION_ID_LENGTH`. This parameter is always recommended since it helps avoid problems both if the server caches the session and if the client requested caching. Default value: `${application.name}`.
- sessionCacheSize (default: [1024\*20](https://github.com/ClickHouse/boringssl/blob/master/include/openssl/ssl.h#L1978)) – The maximum number of sessions that the server caches. A value of 0 means unlimited sessions.
- sessionTimeout (default: [2h](https://github.com/ClickHouse/boringssl/blob/master/include/openssl/ssl.h#L1926)) – Time for caching the session on the server.
- extendedVerification (default: false) – If enabled, verify that the certificate CN or SAN matches the peer hostname.
- requireTLSv1 (default: false) – Require a TLSv1 connection. Acceptable values: `true`, `false`.
- requireTLSv1_1 (default: false) – Require a TLSv1.1 connection. Acceptable values: `true`, `false`.
- requireTLSv1_2 (default: false) – Require a TLSv1.2 connection. Acceptable values: `true`, `false`.
- fips (default: false) – Activates OpenSSL FIPS mode. Supported if the library’s OpenSSL version supports FIPS.
- privateKeyPassphraseHandler (default: `KeyConsoleHandler`)– Class (PrivateKeyPassphraseHandler subclass) that requests the passphrase for accessing the private key. For example: `<privateKeyPassphraseHandler>`, `<name>KeyFileHandler</name>`, `<options><password>test</password></options>`, `</privateKeyPassphraseHandler>`.
- invalidCertificateHandler (default: `RejectCertificateHandler`) – Class (a subclass of CertificateHandler) for verifying invalid certificates. For example: `<invalidCertificateHandler> <name>RejectCertificateHandler</name> </invalidCertificateHandler>` .
- disableProtocols (default: "") – Protocols that are not allowed to use.
- preferServerCiphers (default: false) – Preferred server ciphers on the client.

**Example of settings:**

``` xml
<openSSL>
    <server>
        <!-- openssl req -subj "/CN=localhost" -new -newkey rsa:2048 -days 365 -nodes -x509 -keyout /etc/clickhouse-server/server.key -out /etc/clickhouse-server/server.crt -->
        <certificateFile>/etc/clickhouse-server/server.crt</certificateFile>
        <privateKeyFile>/etc/clickhouse-server/server.key</privateKeyFile>
        <!-- openssl dhparam -out /etc/clickhouse-server/dhparam.pem 4096 -->
        <dhParamsFile>/etc/clickhouse-server/dhparam.pem</dhParamsFile>
        <verificationMode>none</verificationMode>
        <loadDefaultCAFile>true</loadDefaultCAFile>
        <cacheSessions>true</cacheSessions>
        <disableProtocols>sslv2,sslv3</disableProtocols>
        <preferServerCiphers>true</preferServerCiphers>
    </server>
    <client>
        <loadDefaultCAFile>true</loadDefaultCAFile>
        <cacheSessions>true</cacheSessions>
        <disableProtocols>sslv2,sslv3</disableProtocols>
        <preferServerCiphers>true</preferServerCiphers>
        <!-- Use for self-signed: <verificationMode>none</verificationMode> -->
        <invalidCertificateHandler>
            <!-- Use for self-signed: <name>AcceptCertificateHandler</name> -->
            <name>RejectCertificateHandler</name>
        </invalidCertificateHandler>
    </client>
</openSSL>
```

## part_log {#part-log}

Logging events that are associated with [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md). For instance, adding or merging data. You can use the log to simulate merge algorithms and compare their characteristics. You can visualize the merge process.

Queries are logged in the [system.part_log](../../operations/system-tables/part_log.md#system_tables-part-log) table, not in a separate file. You can configure the name of this table in the `table` parameter (see below).

Use the following parameters to configure logging:

- `database` – Name of the database.
- `table` – Name of the system table.
- `partition_by` — [Custom partitioning key](../../engines/table-engines/mergetree-family/custom-partitioning-key.md) for a system table. Can't be used if `engine` defined.
- `order_by` - [Custom sorting key](../../engines/table-engines/mergetree-family/mergetree.md#order_by) for a system table. Can't be used if `engine` defined.
- `engine` - [MergeTree Engine Definition](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table) for a system table. Can't be used if `partition_by` or `order_by` defined.
- `flush_interval_milliseconds` – Interval for flushing data from the buffer in memory to the table.
- `max_size_rows` – Maximal size in lines for the logs. When non-flushed logs amount reaches max_size, logs dumped to the disk.
Default: 1048576.
- `reserved_size_rows` –  Pre-allocated memory size in lines for the logs.
Default: 8192.
- `buffer_size_rows_flush_threshold` – Lines amount threshold, reaching it launches flushing logs to the disk in background.
Default: `max_size_rows / 2`.
- `flush_on_crash` - Indication whether logs should be dumped to the disk in case of a crash.
Default: false.
- `storage_policy` – Name of storage policy to use for the table (optional)
- `settings` - [Additional parameters](../../engines/table-engines/mergetree-family/mergetree.md/#settings) that control the behavior of the MergeTree (optional).

**Example**

``` xml
<part_log>
    <database>system</database>
    <table>part_log</table>
    <partition_by>toMonday(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</part_log>
```

## path {#path}

The path to the directory containing data.

:::note
The trailing slash is mandatory.
:::

**Example**

``` xml
<path>/var/lib/clickhouse/</path>
```

## query_log {#query-log}

Setting for logging queries received with the [log_queries=1](../../operations/settings/settings.md) setting.

Queries are logged in the [system.query_log](../../operations/system-tables/query_log.md#system_tables-query_log) table, not in a separate file. You can change the name of the table in the `table` parameter (see below).

Use the following parameters to configure logging:

- `database` – Name of the database.
- `table` – Name of the system table the queries will be logged in.
- `partition_by` — [Custom partitioning key](../../engines/table-engines/mergetree-family/custom-partitioning-key.md) for a system table. Can't be used if `engine` defined.
- `order_by` - [Custom sorting key](../../engines/table-engines/mergetree-family/mergetree.md#order_by) for a system table. Can't be used if `engine` defined.
- `engine` - [MergeTree Engine Definition](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table) for a system table. Can't be used if `partition_by` or `order_by` defined.
- `flush_interval_milliseconds` – Interval for flushing data from the buffer in memory to the table.
- `max_size_rows` – Maximal size in lines for the logs. When non-flushed logs amount reaches max_size, logs dumped to the disk.
Default: 1048576.
- `reserved_size_rows` –  Pre-allocated memory size in lines for the logs.
Default: 8192.
- `buffer_size_rows_flush_threshold` – Lines amount threshold, reaching it launches flushing logs to the disk in background.
Default: `max_size_rows / 2`.
- `flush_on_crash` - Indication whether logs should be dumped to the disk in case of a crash.
Default: false.
- `storage_policy` – Name of storage policy to use for the table (optional)
- `settings` - [Additional parameters](../../engines/table-engines/mergetree-family/mergetree.md/#settings) that control the behavior of the MergeTree (optional).

If the table does not exist, ClickHouse will create it. If the structure of the query log changed when the ClickHouse server was updated, the table with the old structure is renamed, and a new table is created automatically.

**Example**

``` xml
<query_log>
    <database>system</database>
    <table>query_log</table>
    <engine>Engine = MergeTree PARTITION BY event_date ORDER BY event_time TTL event_date + INTERVAL 30 day</engine>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</query_log>
```

# query_metric_log {#query_metric_log}

It is disabled by default.

**Enabling**

To manually turn on metrics history collection [`system.query_metric_log`](../../operations/system-tables/query_metric_log.md), create `/etc/clickhouse-server/config.d/query_metric_log.xml` with the following content:

``` xml
<clickhouse>
    <query_metric_log>
        <database>system</database>
        <table>query_metric_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <collect_interval_milliseconds>1000</collect_interval_milliseconds>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
    </query_metric_log>
</clickhouse>
```

**Disabling**

To disable `query_metric_log` setting, you should create the following file `/etc/clickhouse-server/config.d/disable_query_metric_log.xml` with the following content:

``` xml
<clickhouse>
<query_metric_log remove="1" />
</clickhouse>
```

## query_cache {#server_configuration_parameters_query-cache}

[Query cache](../query-cache.md) configuration.

The following settings are available:

- `max_size_in_bytes`: The maximum cache size in bytes. 0 means the query cache is disabled. Default value: `1073741824` (1 GiB).
- `max_entries`: The maximum number of `SELECT` query results stored in the cache. Default value: `1024`.
- `max_entry_size_in_bytes`: The maximum size in bytes `SELECT` query results may have to be saved in the cache. Default value: `1048576` (1 MiB).
- `max_entry_size_in_rows`: The maximum number of rows `SELECT` query results may have to be saved in the cache. Default value: `30000000` (30 mil).

Changed settings take effect immediately.

:::note
Data for the query cache is allocated in DRAM. If memory is scarce, make sure to set a small value for `max_size_in_bytes` or disable the query cache altogether.
:::

**Example**

```xml
<query_cache>
    <max_size_in_bytes>1073741824</max_size_in_bytes>
    <max_entries>1024</max_entries>
    <max_entry_size_in_bytes>1048576</max_entry_size_in_bytes>
    <max_entry_size_in_rows>30000000</max_entry_size_in_rows>
</query_cache>
```

## query_thread_log {#query_thread_log}

Setting for logging threads of queries received with the [log_query_threads=1](../../operations/settings/settings.md#log-query-threads) setting.

Queries are logged in the [system.query_thread_log](../../operations/system-tables/query_thread_log.md#system_tables-query_thread_log) table, not in a separate file. You can change the name of the table in the `table` parameter (see below).

Use the following parameters to configure logging:

- `database` – Name of the database.
- `table` – Name of the system table the queries will be logged in.
- `partition_by` — [Custom partitioning key](../../engines/table-engines/mergetree-family/custom-partitioning-key.md) for a system table. Can't be used if `engine` defined.
- `order_by` - [Custom sorting key](../../engines/table-engines/mergetree-family/mergetree.md#order_by) for a system table. Can't be used if `engine` defined.
- `engine` - [MergeTree Engine Definition](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table) for a system table. Can't be used if `partition_by` or `order_by` defined.
- `flush_interval_milliseconds` – Interval for flushing data from the buffer in memory to the table.
- `max_size_rows` – Maximal size in lines for the logs. When non-flushed logs amount reaches max_size_rows, logs dumped to the disk.
Default: 1048576.
- `reserved_size_rows` –  Pre-allocated memory size in lines for the logs.
Default: 8192.
- `buffer_size_rows_flush_threshold` – Lines amount threshold, reaching it launches flushing logs to the disk in background.
Default: `max_size_rows / 2`.
- `flush_on_crash` - Indication whether logs should be dumped to the disk in case of a crash.
Default: false.
- `storage_policy` – Name of storage policy to use for the table (optional)
- `settings` - [Additional parameters](../../engines/table-engines/mergetree-family/mergetree.md/#settings) that control the behavior of the MergeTree (optional).

If the table does not exist, ClickHouse will create it. If the structure of the query thread log changed when the ClickHouse server was updated, the table with the old structure is renamed, and a new table is created automatically.

**Example**

``` xml
<query_thread_log>
    <database>system</database>
    <table>query_thread_log</table>
    <partition_by>toMonday(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</query_thread_log>
```

## query_views_log {#query_views_log}

Setting for logging views (live, materialized etc) dependant of queries received with the [log_query_views=1](../../operations/settings/settings.md#log-query-views) setting.

Queries are logged in the [system.query_views_log](../../operations/system-tables/query_views_log.md#system_tables-query_views_log) table, not in a separate file. You can change the name of the table in the `table` parameter (see below).

Use the following parameters to configure logging:

- `database` – Name of the database.
- `table` – Name of the system table the queries will be logged in.
- `partition_by` — [Custom partitioning key](../../engines/table-engines/mergetree-family/custom-partitioning-key.md) for a system table. Can't be used if `engine` defined.
- `order_by` - [Custom sorting key](../../engines/table-engines/mergetree-family/mergetree.md#order_by) for a system table. Can't be used if `engine` defined.
- `engine` - [MergeTree Engine Definition](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table) for a system table. Can't be used if `partition_by` or `order_by` defined.
- `flush_interval_milliseconds` – Interval for flushing data from the buffer in memory to the table.
- `max_size_rows` – Maximal size in lines for the logs. When non-flushed logs amount reaches max_size, logs dumped to the disk.
Default: 1048576.
- `reserved_size_rows` –  Pre-allocated memory size in lines for the logs.
Default: 8192.
- `buffer_size_rows_flush_threshold` – Lines amount threshold, reaching it launches flushing logs to the disk in background.
Default: `max_size_rows / 2`.
- `flush_on_crash` - Indication whether logs should be dumped to the disk in case of a crash.
Default: false.
- `storage_policy` – Name of storage policy to use for the table (optional)
- `settings` - [Additional parameters](../../engines/table-engines/mergetree-family/mergetree.md/#settings) that control the behavior of the MergeTree (optional).

If the table does not exist, ClickHouse will create it. If the structure of the query views log changed when the ClickHouse server was updated, the table with the old structure is renamed, and a new table is created automatically.

**Example**

``` xml
<query_views_log>
    <database>system</database>
    <table>query_views_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</query_views_log>
```

## text_log {#text_log}

Settings for the [text_log](../../operations/system-tables/text_log.md#system_tables-text_log) system table for logging text messages.

Parameters:

- `level` — Maximum Message Level (by default `Trace`) which will be stored in a table.
- `database` — Database name.
- `table` — Table name.
- `partition_by` — [Custom partitioning key](../../engines/table-engines/mergetree-family/custom-partitioning-key.md) for a system table. Can't be used if `engine` defined.
- `order_by` - [Custom sorting key](../../engines/table-engines/mergetree-family/mergetree.md#order_by) for a system table. Can't be used if `engine` defined.
- `engine` - [MergeTree Engine Definition](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table) for a system table. Can't be used if `partition_by` or `order_by` defined.
- `flush_interval_milliseconds` — Interval for flushing data from the buffer in memory to the table.
- `max_size_rows` – Maximal size in lines for the logs. When non-flushed logs amount reaches max_size, logs dumped to the disk.
Default: 1048576.
- `reserved_size_rows` –  Pre-allocated memory size in lines for the logs.
Default: 8192.
- `buffer_size_rows_flush_threshold` – Lines amount threshold, reaching it launches flushing logs to the disk in background.
Default: `max_size_rows / 2`.
- `flush_on_crash` - Indication whether logs should be dumped to the disk in case of a crash.
Default: false.
- `storage_policy` – Name of storage policy to use for the table (optional)
- `settings` - [Additional parameters](../../engines/table-engines/mergetree-family/mergetree.md/#settings) that control the behavior of the MergeTree (optional).

**Example**
```xml
<clickhouse>
    <text_log>
        <level>notice</level>
        <database>system</database>
        <table>text_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
        <!-- <partition_by>event_date</partition_by> -->
        <engine>Engine = MergeTree PARTITION BY event_date ORDER BY event_time TTL event_date + INTERVAL 30 day</engine>
    </text_log>
</clickhouse>
```

## trace_log {#trace_log}

Settings for the [trace_log](../../operations/system-tables/trace_log.md#system_tables-trace_log) system table operation.

Parameters:

- `database` — Database for storing a table.
- `table` — Table name.
- `partition_by` — [Custom partitioning key](../../engines/table-engines/mergetree-family/custom-partitioning-key.md) for a system table. Can't be used if `engine` defined.
- `order_by` - [Custom sorting key](../../engines/table-engines/mergetree-family/mergetree.md#order_by) for a system table. Can't be used if `engine` defined.
- `engine` - [MergeTree Engine Definition](../../engines/table-engines/mergetree-family/index.md) for a system table. Can't be used if `partition_by` or `order_by` defined.
- `flush_interval_milliseconds` — Interval for flushing data from the buffer in memory to the table.
- `max_size_rows` – Maximal size in lines for the logs. When non-flushed logs amount reaches max_size, logs dumped to the disk.
Default: 1048576.
- `reserved_size_rows` –  Pre-allocated memory size in lines for the logs.
Default: 8192.
- `buffer_size_rows_flush_threshold` – Lines amount threshold, reaching it launches flushing logs to the disk in background.
Default: `max_size_rows / 2`.
- `storage_policy` – Name of storage policy to use for the table (optional)
- `settings` - [Additional parameters](../../engines/table-engines/mergetree-family/mergetree.md/#settings) that control the behavior of the MergeTree (optional).

The default server configuration file `config.xml` contains the following settings section:

``` xml
<trace_log>
    <database>system</database>
    <table>trace_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1048576</max_size_rows>
    <reserved_size_rows>8192</reserved_size_rows>
    <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</trace_log>
```

## asynchronous_insert_log {#asynchronous_insert_log}

Settings for the [asynchronous_insert_log](../../operations/system-tables/asynchronous_insert_log.md#system_tables-asynchronous_insert_log) system table for logging async inserts.

Parameters:

- `database` — Database name.
- `table` — Table name.
- `partition_by` — [Custom partitioning key](../../engines/table-engines/mergetree-family/custom-partitioning-key.md) for a system table. Can't be used if `engine` defined.
- `engine` - [MergeTree Engine Definition](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table) for a system table. Can't be used if `partition_by` defined.
- `flush_interval_milliseconds` — Interval for flushing data from the buffer in memory to the table.
- `max_size_rows` – Maximal size in lines for the logs. When non-flushed logs amount reaches max_size, logs dumped to the disk.
Default: 1048576.
- `reserved_size_rows` –  Pre-allocated memory size in lines for the logs.
Default: 8192.
- `buffer_size_rows_flush_threshold` – Lines amount threshold, reaching it launches flushing logs to the disk in background.
Default: `max_size_rows / 2`.
- `flush_on_crash` - Indication whether logs should be dumped to the disk in case of a crash.
Default: false.
- `storage_policy` – Name of storage policy to use for the table (optional)

**Example**

```xml
<clickhouse>
    <asynchronous_insert_log>
        <database>system</database>
        <table>asynchronous_insert_log</table>
        <flush_interval_milliseconds>7500</flush_interval_milliseconds>
        <partition_by>toYYYYMM(event_date)</partition_by>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
        <!-- <engine>Engine = MergeTree PARTITION BY event_date ORDER BY event_time TTL event_date + INTERVAL 30 day</engine> -->
    </asynchronous_insert_log>
</clickhouse>
```

## crash_log {#crash_log}

Settings for the [crash_log](../../operations/system-tables/crash-log.md) system table operation.

Parameters:

- `database` — Database for storing a table.
- `table` — Table name.
- `partition_by` — [Custom partitioning key](../../engines/table-engines/mergetree-family/custom-partitioning-key.md) for a system table. Can't be used if `engine` defined.
- `order_by` - [Custom sorting key](../../engines/table-engines/mergetree-family/mergetree.md#order_by) for a system table. Can't be used if `engine` defined.
- `engine` - [MergeTree Engine Definition](../../engines/table-engines/mergetree-family/index.md) for a system table. Can't be used if `partition_by` or `order_by` defined.
- `flush_interval_milliseconds` — Interval for flushing data from the buffer in memory to the table.
- `max_size_rows` – Maximal size in lines for the logs. When non-flushed logs amount reaches max_size, logs dumped to the disk.
Default: 1048576.
- `reserved_size_rows` –  Pre-allocated memory size in lines for the logs.
Default: 8192.
- `buffer_size_rows_flush_threshold` – Lines amount threshold, reaching it launches flushing logs to the disk in background.
Default: `max_size_rows / 2`.
- `flush_on_crash` - Indication whether logs should be dumped to the disk in case of a crash.
Default: false.
- `storage_policy` – Name of storage policy to use for the table (optional)
- `settings` - [Additional parameters](../../engines/table-engines/mergetree-family/mergetree.md/#settings) that control the behavior of the MergeTree (optional).

The default server configuration file `config.xml` contains the following settings section:

``` xml
<crash_log>
    <database>system</database>
    <table>crash_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
    <max_size_rows>1024</max_size_rows>
    <reserved_size_rows>1024</reserved_size_rows>
    <buffer_size_rows_flush_threshold>512</buffer_size_rows_flush_threshold>
    <flush_on_crash>false</flush_on_crash>
</crash_log>
```

## backup_log {#backup_log}

Settings for the [backup_log](../../operations/system-tables/backup_log.md) system table for logging `BACKUP` and `RESTORE` operations.

Parameters:

- `database` — Database name.
- `table` — Table name.
- `partition_by` — [Custom partitioning key](../../engines/table-engines/mergetree-family/custom-partitioning-key.md) for a system table. Can't be used if `engine` is defined.
- `order_by` - [Custom sorting key](../../engines/table-engines/mergetree-family/mergetree.md#order_by) for a system table. Can't be used if `engine` is defined.
- `engine` - [MergeTree Engine Definition](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table) for a system table. Can't be used if `partition_by` or `order_by` is defined.
- `flush_interval_milliseconds` — Interval for flushing data from the buffer in memory to the table.
- `max_size_rows` – Maximal size in lines for the logs. When non-flushed logs amount reaches max_size, logs dumped to the disk.
Default: 1048576.
- `reserved_size_rows` –  Pre-allocated memory size in lines for the logs.
Default: 8192.
- `buffer_size_rows_flush_threshold` – Lines amount threshold, reaching it launches flushing logs to the disk in background.
Default: `max_size_rows / 2`.
- `flush_on_crash` - Indication whether logs should be dumped to the disk in case of a crash.
Default: false.
- `storage_policy` – Name of storage policy to use for the table (optional).
- `settings` - [Additional parameters](../../engines/table-engines/mergetree-family/mergetree.md#settings) that control the behavior of the MergeTree (optional).

**Example**

```xml
<clickhouse>
    <backup_log>
        <database>system</database>
        <table>backup_log</table>
        <flush_interval_milliseconds>1000</flush_interval_milliseconds>
        <partition_by>toYYYYMM(event_date)</partition_by>
        <max_size_rows>1048576</max_size_rows>
        <reserved_size_rows>8192</reserved_size_rows>
        <buffer_size_rows_flush_threshold>524288</buffer_size_rows_flush_threshold>
        <flush_on_crash>false</flush_on_crash>
        <!-- <engine>Engine = MergeTree PARTITION BY event_date ORDER BY event_time TTL event_date + INTERVAL 30 day</engine> -->
    </backup_log>
</clickhouse>
```

## query_masking_rules {#query-masking-rules}

Regexp-based rules, which will be applied to queries as well as all log messages before storing them in server logs,
`system.query_log`, `system.text_log`, `system.processes` tables, and in logs sent to the client. That allows preventing
sensitive data leakage from SQL queries (like names, emails, personal identifiers or credit card numbers) to logs.

**Example**

``` xml
<query_masking_rules>
    <rule>
        <name>hide SSN</name>
        <regexp>(^|\D)\d{3}-\d{2}-\d{4}($|\D)</regexp>
        <replace>000-00-0000</replace>
    </rule>
</query_masking_rules>
```

Config fields:
- `name` - name for the rule (optional)
- `regexp` - RE2 compatible regular expression (mandatory)
- `replace` - substitution string for sensitive data (optional, by default - six asterisks)

The masking rules are applied to the whole query (to prevent leaks of sensitive data from malformed / non-parseable queries).

`system.events` table have counter `QueryMaskingRulesMatch` which have an overall number of query masking rules matches.

For distributed queries each server have to be configured separately, otherwise, subqueries passed to other
nodes will be stored without masking.

## remote_servers {#server-settings-remote-servers}

Configuration of clusters used by the [Distributed](../../engines/table-engines/special/distributed.md) table engine and by the `cluster` table function.

**Example**

``` xml
<remote_servers incl="clickhouse_remote_servers" />
```

For the value of the `incl` attribute, see the section “[Configuration files](../../operations/configuration-files.md#configuration_files)”.

**See Also**

- [skip_unavailable_shards](../../operations/settings/settings.md#skip_unavailable_shards)
- [Cluster Discovery](../../operations/cluster-discovery.md)
- [Replicated database engine](../../engines/database-engines/replicated.md)

## timezone {#timezone}

The server’s time zone.

Specified as an IANA identifier for the UTC timezone or geographic location (for example, Africa/Abidjan).

The time zone is necessary for conversions between String and DateTime formats when DateTime fields are output to text format (printed on the screen or in a file), and when getting DateTime from a string. Besides, the time zone is used in functions that work with the time and date if they didn’t receive the time zone in the input parameters.

**Example**

``` xml
<timezone>Asia/Istanbul</timezone>
```

**See also**

- [session_timezone](../settings/settings.md#session_timezone)

## tcp_port {#tcp_port}

Port for communicating with clients over the TCP protocol.

**Example**

``` xml
<tcp_port>9000</tcp_port>
```

## tcp_port_secure {#tcp_port_secure}

TCP port for secure communication with clients. Use it with [OpenSSL](#openssl) settings.

**Possible values**

Positive integer.

**Default value**

``` xml
<tcp_port_secure>9440</tcp_port_secure>
```

## mysql_port {#mysql_port}

Port for communicating with clients over MySQL protocol.

**Possible values**

Positive integer to specify the port number to listen to or empty value to disable.

Example

``` xml
<mysql_port>9004</mysql_port>
```

## postgresql_port {#postgresql_port}

Port for communicating with clients over PostgreSQL protocol.

**Possible values**

Positive integer to specify the port number to listen to or empty value to disable.

Example

``` xml
<postgresql_port>9005</postgresql_port>
```


## tmp_path {#tmp-path}

Path on the local filesystem to store temporary data for processing large queries.

:::note
- Only one option can be used to configure temporary data storage: `tmp_path` ,`tmp_policy`, `temporary_data_in_cache`.
- The trailing slash is mandatory.
:::

**Example**

```xml
<tmp_path>/var/lib/clickhouse/tmp/</tmp_path>
```


## user_files_path {#user_files_path}

The directory with user files. Used in the table function [file()](../../sql-reference/table-functions/file.md), [fileCluster()](../../sql-reference/table-functions/fileCluster.md).

**Example**

``` xml
<user_files_path>/var/lib/clickhouse/user_files/</user_files_path>
```

## user_scripts_path {#user_scripts_path}

The directory with user scripts files. Used for Executable user defined functions [Executable User Defined Functions](../../sql-reference/functions/index.md#executable-user-defined-functions).

**Example**

``` xml
<user_scripts_path>/var/lib/clickhouse/user_scripts/</user_scripts_path>
```

## user_defined_path {#user_defined_path}

The directory with user defined files. Used for SQL user defined functions [SQL User Defined Functions](../../sql-reference/functions/index.md#user-defined-functions).

**Example**

``` xml
<user_defined_path>/var/lib/clickhouse/user_defined/</user_defined_path>
```

## users_config {#users-config}

Path to the file that contains:

- User configurations.
- Access rights.
- Settings profiles.
- Quota settings.

**Example**

``` xml
<users_config>users.xml</users_config>
```

## wait_dictionaries_load_at_startup {#wait_dictionaries_load_at_startup}

This setting allows to specify behavior if `dictionaries_lazy_load` is `false`.
(If `dictionaries_lazy_load` is `true` this setting doesn't affect anything.)

If `wait_dictionaries_load_at_startup` is `false`, then the server
will start loading all the dictionaries at startup and it will receive connections in parallel with that loading.
When a dictionary is used in a query for the first time then the query will wait until the dictionary is loaded if it's not loaded yet.
Setting `wait_dictionaries_load_at_startup` to `false` can make ClickHouse start faster, however some queries can be executed slower
(because they will have to wait for some dictionaries to be loaded).

If `wait_dictionaries_load_at_startup` is `true`, then the server will wait at startup
until all the dictionaries finish their loading (successfully or not) before receiving any connections.

The default is `true`.

**Example**

``` xml
<wait_dictionaries_load_at_startup>true</wait_dictionaries_load_at_startup>
```

## zookeeper {#server-settings_zookeeper}

Contains settings that allow ClickHouse to interact with a [ZooKeeper](http://zookeeper.apache.org/) cluster.

ClickHouse uses ZooKeeper for storing metadata of replicas when using replicated tables. If replicated tables are not used, this section of parameters can be omitted.

This section contains the following parameters:

- `node` — ZooKeeper endpoint. You can set multiple endpoints.

    For example:

<!-- -->

``` xml
    <node index="1">
        <host>example_host</host>
        <port>2181</port>
    </node>
```

      The `index` attribute specifies the node order when trying to connect to the ZooKeeper cluster.

- `session_timeout_ms` — Maximum timeout for the client session in milliseconds.
- `operation_timeout_ms` — Maximum timeout for one operation in milliseconds.
- `root` — The [znode](http://zookeeper.apache.org/doc/r3.5.5/zookeeperOver.html#Nodes+and+ephemeral+nodes) that is used as the root for znodes used by the ClickHouse server. Optional.
- `fallback_session_lifetime.min` - If the first zookeeper host resolved by zookeeper_load_balancing strategy is unavailable, limit the lifetime of a zookeeper session to the fallback node. This is done for load-balancing purposes to avoid excessive load on one of zookeeper hosts. This setting sets the minimal duration of the fallback session. Set in seconds. Optional. Default is 3 hours.
- `fallback_session_lifetime.max` - If the first zookeeper host resolved by zookeeper_load_balancing strategy is unavailable, limit the lifetime of a zookeeper session to the fallback node. This is done for load-balancing purposes to avoid excessive load on one of zookeeper hosts. This setting sets the maximum duration of the fallback session. Set in seconds. Optional. Default is 6 hours.
- `identity` — User and password, that can be required by ZooKeeper to give access to requested znodes. Optional.
- zookeeper_load_balancing - Specifies the algorithm of ZooKeeper node selection.
  * random - randomly selects one of ZooKeeper nodes.
  * in_order - selects the first ZooKeeper node, if it's not available then the second, and so on.
  * nearest_hostname - selects a ZooKeeper node with a hostname that is most similar to the server’s hostname, hostname is compared with name prefix.
  * hostname_levenshtein_distance - just like nearest_hostname, but it compares hostname in a levenshtein distance manner.
  * first_or_random - selects the first ZooKeeper node, if it's not available then randomly selects one of remaining ZooKeeper nodes.
  * round_robin - selects the first ZooKeeper node, if reconnection happens selects the next.
- `use_compression` — If set to true, enables compression in Keeper protocol.


**Example configuration**

``` xml
<zookeeper>
    <node>
        <host>example1</host>
        <port>2181</port>
    </node>
    <node>
        <host>example2</host>
        <port>2181</port>
    </node>
    <session_timeout_ms>30000</session_timeout_ms>
    <operation_timeout_ms>10000</operation_timeout_ms>
    <!-- Optional. Chroot suffix. Should exist. -->
    <root>/path/to/zookeeper/node</root>
    <!-- Optional. Zookeeper digest ACL string. -->
    <identity>user:password</identity>
    <!--<zookeeper_load_balancing>random / in_order / nearest_hostname / hostname_levenshtein_distance / first_or_random / round_robin</zookeeper_load_balancing>-->
    <zookeeper_load_balancing>random</zookeeper_load_balancing>
</zookeeper>
```

**See Also**

- [Replication](../../engines/table-engines/mergetree-family/replication.md)
- [ZooKeeper Programmer’s Guide](http://zookeeper.apache.org/doc/current/zookeeperProgrammers.html)
- [Optional secured communication between ClickHouse and Zookeeper](../ssl-zookeeper.md#secured-communication-with-zookeeper)

## use_minimalistic_part_header_in_zookeeper {#server-settings-use_minimalistic_part_header_in_zookeeper}

Storage method for data part headers in ZooKeeper.

This setting only applies to the `MergeTree` family. It can be specified:

- Globally in the [merge_tree](#merge_tree) section of the `config.xml` file.

    ClickHouse uses the setting for all the tables on the server. You can change the setting at any time. Existing tables change their behaviour when the setting changes.

- For each table.

    When creating a table, specify the corresponding [engine setting](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table). The behaviour of an existing table with this setting does not change, even if the global setting changes.

**Possible values**

- 0 — Functionality is turned off.
- 1 — Functionality is turned on.

If `use_minimalistic_part_header_in_zookeeper = 1`, then [replicated](../../engines/table-engines/mergetree-family/replication.md) tables store the headers of the data parts compactly using a single `znode`. If the table contains many columns, this storage method significantly reduces the volume of the data stored in Zookeeper.

:::note
After applying `use_minimalistic_part_header_in_zookeeper = 1`, you can’t downgrade the ClickHouse server to a version that does not support this setting. Be careful when upgrading ClickHouse on servers in a cluster. Don’t upgrade all the servers at once. It is safer to test new versions of ClickHouse in a test environment, or on just a few servers of a cluster.

Data part headers already stored with this setting can't be restored to their previous (non-compact) representation.
:::

**Default value:** 0.

## distributed_ddl {#server-settings-distributed_ddl}

Manage executing [distributed ddl queries](../../sql-reference/distributed-ddl.md)  (CREATE, DROP, ALTER, RENAME) on cluster.
Works only if [ZooKeeper](#server-settings_zookeeper) is enabled.

The configurable settings within `<distributed_ddl>` include:

- **path**: the path in Keeper for the `task_queue` for DDL queries
- **profile**: the profile used to execute the DDL queries
- **pool_size**: how many `ON CLUSTER` queries can be run simultaneously
- **max_tasks_in_queue**: the maximum number of tasks that can be in the queue. Default is 1,000
- **task_max_lifetime**: delete node if its age is greater than this value. Default is `7 * 24 * 60 * 60` (a week in seconds)
- **cleanup_delay_period**:  cleaning starts after new node event is received if the last cleaning wasn't made sooner than `cleanup_delay_period` seconds ago. Default is 60 seconds


**Example**

```xml
<distributed_ddl>
    <!-- Path in ZooKeeper to queue with DDL queries -->
    <path>/clickhouse/task_queue/ddl</path>

    <!-- Settings from this profile will be used to execute DDL queries -->
    <profile>default</profile>

    <!-- Controls how much ON CLUSTER queries can be run simultaneously. -->
    <pool_size>1</pool_size>

    <!--
         Cleanup settings (active tasks will not be removed)
    -->

    <!-- Controls task TTL (default 1 week) -->
    <task_max_lifetime>604800</task_max_lifetime>

    <!-- Controls how often cleanup should be performed (in seconds) -->
    <cleanup_delay_period>60</cleanup_delay_period>

    <!-- Controls how many tasks could be in the queue -->
    <max_tasks_in_queue>1000</max_tasks_in_queue>
</distributed_ddl>
```

## access_control_path {#access_control_path}

Path to a folder where a ClickHouse server stores user and role configurations created by SQL commands.

Default value: `/var/lib/clickhouse/access/`.

**See also**

- [Access Control and Account Management](../../guides/sre/user-management/index.md#access-control)

## user_directories {#user_directories}

Section of the configuration file that contains settings:
- Path to configuration file with predefined users.
- Path to folder where users created by SQL commands are stored.
- ZooKeeper node path where users created by SQL commands are stored and replicated (experimental).

If this section is specified, the path from [users_config](../../operations/server-configuration-parameters/settings.md#users-config) and [access_control_path](../../operations/server-configuration-parameters/settings.md#access_control_path) won't be used.

The `user_directories` section can contain any number of items, the order of the items means their precedence (the higher the item the higher the precedence).

**Examples**

``` xml
<user_directories>
    <users_xml>
        <path>/etc/clickhouse-server/users.xml</path>
    </users_xml>
    <local_directory>
        <path>/var/lib/clickhouse/access/</path>
    </local_directory>
</user_directories>
```

Users, roles, row policies, quotas, and profiles can be also stored in ZooKeeper:

``` xml
<user_directories>
    <users_xml>
        <path>/etc/clickhouse-server/users.xml</path>
    </users_xml>
    <replicated>
        <zookeeper_path>/clickhouse/access/</zookeeper_path>
    </replicated>
</user_directories>
```

You can also define sections `memory` — means storing information only in memory, without writing to disk, and `ldap` — means storing information on an LDAP server.

To add an LDAP server as a remote user directory of users that are not defined locally, define a single `ldap` section with a following parameters:
- `server` — one of LDAP server names defined in `ldap_servers` config section. This parameter is mandatory and cannot be empty.
- `roles` — section with a list of locally defined roles that will be assigned to each user retrieved from the LDAP server. If no roles are specified, user will not be able to perform any actions after authentication. If any of the listed roles is not defined locally at the time of authentication, the authentication attempt will fail as if the provided password was incorrect.

**Example**

``` xml
<ldap>
    <server>my_ldap_server</server>
        <roles>
            <my_local_role1 />
            <my_local_role2 />
        </roles>
</ldap>
```

## total_memory_profiler_step {#total-memory-profiler-step}

Sets the memory size (in bytes) for a stack trace at every peak allocation step. The data is stored in the [system.trace_log](../../operations/system-tables/trace_log.md) system table with `query_id` equal to an empty string.

Possible values:

- Positive integer.

Default value: `4194304`.

## total_memory_tracker_sample_probability {#total-memory-tracker-sample-probability}

Allows to collect random allocations and deallocations and writes them in the [system.trace_log](../../operations/system-tables/trace_log.md) system table with `trace_type` equal to a `MemorySample` with the specified probability. The probability is for every allocation or deallocations, regardless of the size of the allocation. Note that sampling happens only when the amount of untracked memory exceeds the untracked memory limit (default value is `4` MiB). It can be lowered if [total_memory_profiler_step](#total-memory-profiler-step) is lowered. You can set `total_memory_profiler_step` equal to `1` for extra fine-grained sampling.

Possible values:

- Positive integer.
- 0 — Writing of random allocations and deallocations in the `system.trace_log` system table is disabled.

Default value: `0`.

## compiled_expression_cache_size {#compiled-expression-cache-size}

Sets the cache size (in bytes) for [compiled expressions](../../operations/caches.md).

Possible values:

- Positive integer.

Default value: `134217728`.

## compiled_expression_cache_elements_size {#compiled_expression_cache_elements_size}

Sets the cache size (in elements) for [compiled expressions](../../operations/caches.md).

Possible values:

- Positive integer.

Default value: `10000`.

## display_secrets_in_show_and_select {#display_secrets_in_show_and_select}

Enables or disables showing secrets in `SHOW` and `SELECT` queries for tables, databases,
table functions, and dictionaries.

User wishing to see secrets must also have
[`format_display_secrets_in_show_and_select` format setting](../settings/formats#format_display_secrets_in_show_and_select)
turned on and a
[`displaySecretsInShowAndSelect`](../../sql-reference/statements/grant#display-secrets) privilege.

Possible values:

-   0 — Disabled.
-   1 — Enabled.

Default value: 0.

## proxy {#proxy}

Define proxy servers for HTTP and HTTPS requests, currently supported by S3 storage, S3 table functions, and URL functions.

There are three ways to define proxy servers: environment variables, proxy lists, and remote proxy resolvers.

Bypassing proxy servers for specific hosts is also supported with the use of `no_proxy`.

### Environment variables

The `http_proxy` and `https_proxy` environment variables allow you to specify a
proxy server for a given protocol. If you have it set on your system, it should work seamlessly.

This is the simplest approach if a given protocol has
only one proxy server and that proxy server doesn't change.

### Proxy lists

This approach allows you to specify one or more
proxy servers for a protocol. If more than one proxy server is defined,
ClickHouse uses the different proxies on a round-robin basis, balancing the
load across the servers. This is the simplest approach if there is more than
one proxy server for a protocol and the list of proxy servers doesn't change.

### Configuration template

``` xml
<proxy>
    <http>
        <uri>http://proxy1</uri>
        <uri>http://proxy2:3128</uri>
    </http>
    <https>
        <uri>http://proxy1:3128</uri>
    </https>
</proxy>
```

`<proxy>` fields

* `<http>` - A list of one or more HTTP proxies
* `<https>` - A list of one or more HTTPS proxies

`<http>` and `<https>` fields

* `<uri>` - The URI of the proxy

### Remote proxy resolvers

It's possible that the proxy servers change dynamically. In that
case, you can define the endpoint of a resolver. ClickHouse sends
an empty GET request to that endpoint, the remote resolver should return the proxy host.
ClickHouse will use it to form the proxy URI using the following template: `{proxy_scheme}://{proxy_host}:{proxy_port}`

### Configuration template

``` xml
<proxy>
    <http>
        <resolver>
            <endpoint>http://resolver:8080/hostname</endpoint>
            <proxy_scheme>http</proxy_scheme>
            <proxy_port>80</proxy_port>
            <proxy_cache_time>10</proxy_cache_time>
        </resolver>
    </http>

    <https>
        <resolver>
            <endpoint>http://resolver:8080/hostname</endpoint>
            <proxy_scheme>http</proxy_scheme>
            <proxy_port>3128</proxy_port>
            <proxy_cache_time>10</proxy_cache_time>
        </resolver>
    </https>

</proxy>
```

`<proxy>` fields

* `<http>` - A list of one or more resolvers*
* `<https>` - A list of one or more resolvers*

`<http>` and `<https>` fields

* `<resolver>` - The endpoint and other details for a resolver.
  You can have multiple `<resolver>` elements, but only the first
  `<resolver>` for a given protocol is used. Any other `<resolver>`
  elements for that protocol are ignored. That means load balancing
  (if needed) should be implemented by the remote resolver.

`<resolver>` fields

* `<endpoint>` - The URI of the proxy resolver
* `<proxy_scheme>` - The protocol of the final proxy URI. This can be either `http` or `https`.
* `<proxy_port>` - The port number of the proxy resolver
* `<proxy_cache_time>` - The time in seconds that values from the resolver
  should be cached by ClickHouse. Setting this value to `0` causes ClickHouse
  to contact the resolver for every HTTP or HTTPS request.

### Precedence

Proxy settings are determined in the following order:

1. Remote proxy resolvers
2. Proxy lists
3. Environment variables

ClickHouse will check the highest priority resolver type for the request protocol. If it is not defined,
it will check the next highest priority resolver type, until it reaches the environment resolver.
This also allows a mix of resolver types can be used.

### disable_tunneling_for_https_requests_over_http_proxy {#disable_tunneling_for_https_requests_over_http_proxy}

By default, tunneling (i.e, `HTTP CONNECT`) is used to make `HTTPS` requests over `HTTP` proxy. This setting can be used to disable it.

### no_proxy
By default, all requests will go through the proxy. In order to disable it for specific hosts, the `no_proxy` variable must be set.
It can be set inside the `<proxy>` clause for list and remote resolvers and as an environment variable for environment resolver.
It supports IP addresses, domains, subdomains and `'*'` wildcard for full bypass. Leading dots are stripped just like curl does.

Example:

The below configuration bypasses proxy requests to `clickhouse.cloud` and all of its subdomains (e.g, `auth.clickhouse.cloud`).
The same applies to GitLab, even though it has a leading dot. Both `gitlab.com` and `about.gitlab.com` would bypass the proxy.

``` xml
<proxy>
    <no_proxy>clickhouse.cloud,.gitlab.com</no_proxy>
    <http>
        <uri>http://proxy1</uri>
        <uri>http://proxy2:3128</uri>
    </http>
    <https>
        <uri>http://proxy1:3128</uri>
    </https>
</proxy>
```

## max_materialized_views_count_for_table {#max_materialized_views_count_for_table}

A limit on the number of materialized views attached to a table.
Note that only directly dependent views are considered here, and the creation of one view on top of another view is not considered.

Default value: `0`.

## format_alter_operations_with_parentheses {#format_alter_operations_with_parentheses}

If set to true, then alter operations will be surrounded by parentheses in formatted queries. This makes the parsing of formatted alter queries less ambiguous.

Type: Bool

Default: 0

## ignore_empty_sql_security_in_create_view_query {#ignore_empty_sql_security_in_create_view_query}

If true, ClickHouse doesn't write defaults for empty SQL security statement in CREATE VIEW queries.

:::note
This setting is only necessary for the migration period and will become obsolete in 24.4
:::

Type: Bool

Default: 1

## merge_workload {#merge_workload}

Used to regulate how resources are utilized and shared between merges and other workloads. Specified value is used as `workload` setting value for all background merges. Can be overridden by a merge tree setting.

Default value: "default"

**See Also**
- [Workload Scheduling](/docs/en/operations/workload-scheduling.md)

## mutation_workload {#mutation_workload}

Used to regulate how resources are utilized and shared between mutations and other workloads. Specified value is used as `workload` setting value for all background mutations. Can be overridden by a merge tree setting.

Default value: "default"

**See Also**
- [Workload Scheduling](/docs/en/operations/workload-scheduling.md)

## workload_path {#workload_path}

The directory used as a storage for all `CREATE WORKLOAD` and `CREATE RESOURCE` queries. By default `/workload/` folder under server working directory is used.

**Example**

``` xml
<workload_path>/var/lib/clickhouse/workload/</workload_path>
```

**See Also**
- [Workload Hierarchy](/docs/en/operations/workload-scheduling.md#workloads)
- [workload_zookeeper_path](#workload_zookeeper_path)

## workload_zookeeper_path {#workload_zookeeper_path}

The path to a ZooKeeper node, which is used as a storage for all `CREATE WORKLOAD` and `CREATE RESOURCE` queries. For consistency all SQL definitions are stored as a value of this single znode. By default ZooKeeper is not used and definitions are stored on [disk](#workload_path).

**Example**

``` xml
<workload_zookeeper_path>/clickhouse/workload/definitions.sql</workload_zookeeper_path>
```

**See Also**
- [Workload Hierarchy](/docs/en/operations/workload-scheduling.md#workloads)
- [workload_path](#workload_path)

## max_authentication_methods_per_user {#max_authentication_methods_per_user}

The maximum number of authentication methods a user can be created with or altered to.
Changing this setting does not affect existing users. Create/alter authentication-related queries will fail if they exceed the limit specified in this setting.
Non authentication create/alter queries will succeed.

Type: UInt64

Default value: 100

Zero means unlimited

## use_legacy_mongodb_integration

Use the legacy MongoDB integration implementation. Deprecated.

Type: Bool

Default value: `true`.
