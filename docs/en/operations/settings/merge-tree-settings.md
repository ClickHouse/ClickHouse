---
slug: /en/operations/settings/merge-tree-settings
title: "MergeTree tables settings"
---

System table `system.merge_tree_settings` shows the globally set MergeTree settings.

MergeTree settings can be set in the `merge_tree` section of the server config file, or specified for each `MergeTree` table individually in
the `SETTINGS` clause of the `CREATE TABLE` statement.

Example for customizing setting `max_suspicious_broken_parts`:

Configure the default for all `MergeTree` tables in the server configuration file:

``` text
<merge_tree>
    <max_suspicious_broken_parts>5</max_suspicious_broken_parts>
</merge_tree>
```

Set for a particular table:

``` sql
CREATE TABLE tab
(
    `A` Int64
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS max_suspicious_broken_parts = 500;
```

Change the settings for a particular table using `ALTER TABLE ... MODIFY SETTING`:

```sql
ALTER TABLE tab MODIFY SETTING max_suspicious_broken_parts = 100;

-- reset to global default (value from system.merge_tree_settings)
ALTER TABLE tab RESET SETTING max_suspicious_broken_parts;
```

## index_granularity

Maximum number of data rows between the marks of an index.

Default value: 8192.

## index_granularity_bytes

Maximum size of data granules in bytes.

Default value: 10485760 (ca. 10 MiB).

To restrict the granule size only by number of rows, set to 0 (not recommended).

## min_index_granularity_bytes

Min allowed size of data granules in bytes.

Default value: 1024b.

To provide a safeguard against accidentally creating tables with very low index_granularity_bytes.

## enable_mixed_granularity_parts

Enables or disables transitioning to control the granule size with the `index_granularity_bytes` setting. Before version 19.11, there was only the `index_granularity` setting for restricting granule size. The `index_granularity_bytes` setting improves ClickHouse performance when selecting data from tables with big rows (tens and hundreds of megabytes). If you have tables with big rows, you can enable this setting for the tables to improve the efficiency of `SELECT` queries.

## use_minimalistic_part_header_in_zookeeper

Storage method of the data parts headers in ZooKeeper. If enabled, ZooKeeper stores less data. For details, see [here](../server-configuration-parameters/settings.md/#server-settings-use_minimalistic_part_header_in_zookeeper).

## min_merge_bytes_to_use_direct_io

The minimum data volume for merge operation that is required for using direct I/O access to the storage disk.
When merging data parts, ClickHouse calculates the total storage volume of all the data to be merged.
If the volume exceeds `min_merge_bytes_to_use_direct_io` bytes, ClickHouse reads and writes the data to the storage disk using the direct I/O interface (`O_DIRECT` option).
If `min_merge_bytes_to_use_direct_io = 0`, then direct I/O is disabled.

Default value: `10 * 1024 * 1024 * 1024` bytes.

## merge_with_ttl_timeout

Minimum delay in seconds before repeating a merge with delete TTL.

Default value: `14400` seconds (4 hours).

## merge_with_recompression_ttl_timeout

Minimum delay in seconds before repeating a merge with recompression TTL.

Default value: `14400` seconds (4 hours).

## write_final_mark

Enables or disables writing the final index mark at the end of data part (after the last byte).

Default value: 1.

Don’t change or bad things will happen.

## storage_policy

Storage policy.

## min_bytes_for_wide_part

Minimum number of bytes/rows in a data part that can be stored in `Wide` format.
You can set one, both or none of these settings.

## max_compress_block_size

Maximum size of blocks of uncompressed data before compressing for writing to a table.
You can also specify this setting in the global settings (see [max_compress_block_size](/docs/en/operations/settings/settings.md/#max-compress-block-size) setting).
The value specified when table is created overrides the global value for this setting.

## min_compress_block_size

Minimum size of blocks of uncompressed data required for compression when writing the next mark.
You can also specify this setting in the global settings (see [min_compress_block_size](/docs/en/operations/settings/settings.md/#min-compress-block-size) setting).
The value specified when table is created overrides the global value for this setting.

## max_suspicious_broken_parts

If the number of broken parts in a single partition exceeds the `max_suspicious_broken_parts` value, automatic deletion is denied.

Possible values:

- Any positive integer.

Default value: 100.

## parts_to_throw_insert {#parts-to-throw-insert}

If the number of active parts in a single partition exceeds the `parts_to_throw_insert` value, `INSERT` is interrupted with the `Too many parts (N). Merges are processing significantly slower than inserts` exception.

Possible values:

- Any positive integer.

Default value: 3000.

To achieve maximum performance of `SELECT` queries, it is necessary to minimize the number of parts processed, see [Merge Tree](../../development/architecture.md#merge-tree).

Prior to 23.6 this setting was set to 300. You can set a higher different value, it will reduce the probability of the `Too many parts` error, but at the same time `SELECT` performance might degrade. Also in case of a merge issue (for example, due to insufficient disk space) you will notice it later than it could be with the original 300.


## parts_to_delay_insert {#parts-to-delay-insert}

If the number of active parts in a single partition exceeds the `parts_to_delay_insert` value, an `INSERT` artificially slows down.

Possible values:

- Any positive integer.

Default value: 1000.

ClickHouse artificially executes `INSERT` longer (adds ‘sleep’) so that the background merge process can merge parts faster than they are added.

## inactive_parts_to_throw_insert {#inactive-parts-to-throw-insert}

If the number of inactive parts in a single partition more than the `inactive_parts_to_throw_insert` value, `INSERT` is interrupted with the "Too many inactive parts (N). Parts cleaning are processing significantly slower than inserts" exception.

Possible values:

- Any positive integer.

Default value: 0 (unlimited).

## inactive_parts_to_delay_insert {#inactive-parts-to-delay-insert}

If the number of inactive parts in a single partition in the table at least that many the `inactive_parts_to_delay_insert` value, an `INSERT` artificially slows down. It is useful when a server fails to clean up parts quickly enough.

Possible values:

- Any positive integer.

Default value: 0 (unlimited).

## max_delay_to_insert {#max-delay-to-insert}

The value in seconds, which is used to calculate the `INSERT` delay, if the number of active parts in a single partition exceeds the [parts_to_delay_insert](#parts-to-delay-insert) value.

Possible values:

- Any positive integer.

Default value: 1.

The delay (in milliseconds) for `INSERT` is calculated by the formula:
```code
max_k = parts_to_throw_insert - parts_to_delay_insert
k = 1 + parts_count_in_partition - parts_to_delay_insert
delay_milliseconds = pow(max_delay_to_insert * 1000, k / max_k)
```
For example, if a partition has 299 active parts and parts_to_throw_insert = 300, parts_to_delay_insert = 150, max_delay_to_insert = 1, `INSERT` is delayed for `pow( 1 * 1000, (1 + 299 - 150) / (300 - 150) ) = 1000` milliseconds.

Starting from version 23.1 formula has been changed to:
```code
allowed_parts_over_threshold = parts_to_throw_insert - parts_to_delay_insert
parts_over_threshold = parts_count_in_partition - parts_to_delay_insert + 1
delay_milliseconds = max(min_delay_to_insert_ms, (max_delay_to_insert * 1000) * parts_over_threshold / allowed_parts_over_threshold)
```
For example, if a partition has 224 active parts and parts_to_throw_insert = 300, parts_to_delay_insert = 150, max_delay_to_insert = 1, min_delay_to_insert_ms = 10, `INSERT` is delayed for `max( 10, 1 * 1000 * (224 - 150 + 1) / (300 - 150) ) = 500` milliseconds.

## max_parts_in_total {#max-parts-in-total}

If the total number of active parts in all partitions of a table exceeds the `max_parts_in_total` value `INSERT` is interrupted with the `Too many parts (N)` exception.

Possible values:

- Any positive integer.

Default value: 100000.

A large number of parts in a table reduces performance of ClickHouse queries and increases ClickHouse boot time. Most often this is a consequence of an incorrect design (mistakes when choosing a partitioning strategy - too small partitions).

## simultaneous_parts_removal_limit {#simultaneous-parts-removal-limit}

If there are a lot of outdated parts cleanup thread will try to delete up to `simultaneous_parts_removal_limit` parts during one iteration.
`simultaneous_parts_removal_limit` set to `0` means unlimited.

Default value: 0.

## replicated_deduplication_window {#replicated-deduplication-window}

The number of most recently inserted blocks for which ClickHouse Keeper stores hash sums to check for duplicates.

Possible values:

- Any positive integer.
- 0 (disable deduplication)

Default value: 1000.

The `Insert` command creates one or more blocks (parts). For [insert deduplication](../../engines/table-engines/mergetree-family/replication.md), when writing into replicated tables, ClickHouse writes the hash sums of the created parts into ClickHouse Keeper. Hash sums are stored only for the most recent `replicated_deduplication_window` blocks. The oldest hash sums are removed from ClickHouse Keeper.
A large number of `replicated_deduplication_window` slows down `Inserts` because it needs to compare more entries.
The hash sum is calculated from the composition of the field names and types and the data of the inserted part (stream of bytes).

## non_replicated_deduplication_window {#non-replicated-deduplication-window}

The number of the most recently inserted blocks in the non-replicated [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) table for which hash sums are stored to check for duplicates.

Possible values:

- Any positive integer.
- 0 (disable deduplication).

Default value: 0.

A deduplication mechanism is used, similar to replicated tables (see [replicated_deduplication_window](#replicated-deduplication-window) setting). The hash sums of the created parts are written to a local file on a disk.

## replicated_deduplication_window_seconds {#replicated-deduplication-window-seconds}

The number of seconds after which the hash sums of the inserted blocks are removed from ClickHouse Keeper.

Possible values:

- Any positive integer.

Default value: 604800 (1 week).

Similar to [replicated_deduplication_window](#replicated-deduplication-window), `replicated_deduplication_window_seconds` specifies how long to store hash sums of blocks for insert deduplication. Hash sums older than `replicated_deduplication_window_seconds` are removed from ClickHouse Keeper, even if they are less than ` replicated_deduplication_window`.

The time is relative to the time of the most recent record, not to the wall time. If it's the only record it will be stored forever.

## replicated_deduplication_window_for_async_inserts {#replicated-deduplication-window-for-async-inserts}

The number of most recently async inserted blocks for which ClickHouse Keeper stores hash sums to check for duplicates.

Possible values:

- Any positive integer.
- 0 (disable deduplication for async_inserts)

Default value: 10000.

The [Async Insert](./settings.md#async-insert) command will be cached in one or more blocks (parts). For [insert deduplication](../../engines/table-engines/mergetree-family/replication.md), when writing into replicated tables, ClickHouse writes the hash sums of each insert into ClickHouse Keeper. Hash sums are stored only for the most recent `replicated_deduplication_window_for_async_inserts` blocks. The oldest hash sums are removed from ClickHouse Keeper.
A large number of `replicated_deduplication_window_for_async_inserts` slows down `Async Inserts` because it needs to compare more entries.
The hash sum is calculated from the composition of the field names and types and the data of the insert (stream of bytes).

## replicated_deduplication_window_seconds_for_async_inserts {#replicated-deduplication-window-seconds-for-async_inserts}

The number of seconds after which the hash sums of the async inserts are removed from ClickHouse Keeper.

Possible values:

- Any positive integer.

Default value: 604800 (1 week).

Similar to [replicated_deduplication_window_for_async_inserts](#replicated-deduplication-window-for-async-inserts), `replicated_deduplication_window_seconds_for_async_inserts` specifies how long to store hash sums of blocks for async insert deduplication. Hash sums older than `replicated_deduplication_window_seconds_for_async_inserts` are removed from ClickHouse Keeper, even if they are less than ` replicated_deduplication_window_for_async_inserts`.

The time is relative to the time of the most recent record, not to the wall time. If it's the only record it will be stored forever.

## use_async_block_ids_cache {#use-async-block-ids-cache}

If true, we cache the hash sums of the async inserts.

Possible values:

- true, false

Default value: false.

A block bearing multiple async inserts will generate multiple hash sums. When some of the inserts are duplicated, keeper will only return one duplicated hash sum in one RPC, which will cause unnecessary RPC retries. This cache will watch the hash sums path in Keeper. If updates are watched in the Keeper, the cache will update as soon as possible, so that we are able to filter the duplicated inserts in the memory.

## async_block_ids_cache_min_update_interval_ms

The minimum interval (in milliseconds) to update the `use_async_block_ids_cache`

Possible values:

- Any positive integer.

Default value: 100.

Normally, the `use_async_block_ids_cache` updates as soon as there are updates in the watching keeper path. However, the cache updates might be too frequent and become a heavy burden. This minimum interval prevents the cache from updating too fast. Note that if we set this value too long, the block with duplicated inserts will have a longer retry time.

## max_replicated_logs_to_keep

How many records may be in the ClickHouse Keeper log if there is inactive replica. An inactive replica becomes lost when when this number exceed.

Possible values:

- Any positive integer.

Default value: 1000

## min_replicated_logs_to_keep

Keep about this number of last records in ZooKeeper log, even if they are obsolete. It doesn't affect work of tables: used only to diagnose ZooKeeper log before cleaning.

Possible values:

- Any positive integer.

Default value: 10

## prefer_fetch_merged_part_time_threshold

If the time passed since a replication log (ClickHouse Keeper or ZooKeeper) entry creation exceeds this threshold, and the sum of the size of parts is greater than `prefer_fetch_merged_part_size_threshold`, then prefer fetching merged part from a replica instead of doing merge locally. This is to speed up very long merges.

Possible values:

- Any positive integer.

Default value: 3600

## prefer_fetch_merged_part_size_threshold

If the sum of the size of parts exceeds this threshold and the time since a replication log entry creation is greater than `prefer_fetch_merged_part_time_threshold`, then prefer fetching merged part from a replica instead of doing merge locally. This is to speed up very long merges.

Possible values:

- Any positive integer.

Default value: 10,737,418,240

## execute_merges_on_single_replica_time_threshold

When this setting has a value greater than zero, only a single replica starts the merge immediately, and other replicas wait up to that amount of time to download the result instead of doing merges locally. If the chosen replica doesn't finish the merge during that amount of time, fallback to standard behavior happens.

Possible values:

- Any positive integer.

Default value: 0 (seconds)

## remote_fs_execute_merges_on_single_replica_time_threshold

When this setting has a value greater than zero only a single replica starts the merge immediately if merged part on shared storage and `allow_remote_fs_zero_copy_replication` is enabled.

:::note Zero-copy replication is not ready for production
Zero-copy replication is disabled by default in ClickHouse version 22.8 and higher.  This feature is not recommended for production use.
:::

Possible values:

- Any positive integer.

Default value: 10800

## try_fetch_recompressed_part_timeout

Timeout (in seconds) before starting merge with recompression. During this time ClickHouse tries to fetch recompressed part from replica which assigned this merge with recompression.

Recompression works slow in most cases, so we don't start merge with recompression until this timeout and trying to fetch recompressed part from replica which assigned this merge with recompression.

Possible values:

- Any positive integer.

Default value: 7200

## always_fetch_merged_part

If true, this replica never merges parts and always downloads merged parts from other replicas.

Possible values:

- true, false

Default value: false

## max_suspicious_broken_parts

Max broken parts, if more - deny automatic deletion.

Possible values:

- Any positive integer.

Default value: 100

## max_suspicious_broken_parts_bytes


Max size of all broken parts, if more - deny automatic deletion.

Possible values:

- Any positive integer.

Default value: 1,073,741,824

## max_files_to_modify_in_alter_columns

Do not apply ALTER if number of files for modification(deletion, addition) is greater than this setting.

Possible values:

- Any positive integer.

Default value: 75

## max_files_to_remove_in_alter_columns

Do not apply ALTER, if the number of files for deletion is greater than this setting.

Possible values:

- Any positive integer.

Default value: 50

## replicated_max_ratio_of_wrong_parts

If the ratio of wrong parts to total number of parts is less than this - allow to start.

Possible values:

- Float, 0.0 - 1.0

Default value: 0.5

## replicated_max_parallel_fetches_for_host

Limit parallel fetches from endpoint (actually pool size).

Possible values:

- Any positive integer.

Default value: 15

## replicated_fetches_http_connection_timeout

HTTP connection timeout for part fetch requests. Inherited from default profile `http_connection_timeout` if not set explicitly.

Possible values:

- Any positive integer.

Default value: Inherited from default profile `http_connection_timeout` if not set explicitly.

## replicated_can_become_leader

If true, replicated tables replicas on this node will try to acquire leadership.

Possible values:

- true, false

Default value: true

## zookeeper_session_expiration_check_period

ZooKeeper session expiration check period, in seconds.

Possible values:

- Any positive integer.

Default value: 60

## detach_old_local_parts_when_cloning_replica

Do not remove old local parts when repairing lost replica.

Possible values:

- true, false

Default value: true

## replicated_fetches_http_connection_timeout {#replicated_fetches_http_connection_timeout}

HTTP connection timeout (in seconds) for part fetch requests. Inherited from default profile [http_connection_timeout](./settings.md#http_connection_timeout) if not set explicitly.

Possible values:

- Any positive integer.
- 0 - Use value of `http_connection_timeout`.

Default value: 0.

## replicated_fetches_http_send_timeout {#replicated_fetches_http_send_timeout}

HTTP send timeout (in seconds) for part fetch requests. Inherited from default profile [http_send_timeout](./settings.md#http_send_timeout) if not set explicitly.

Possible values:

- Any positive integer.
- 0 - Use value of `http_send_timeout`.

Default value: 0.

## replicated_fetches_http_receive_timeout {#replicated_fetches_http_receive_timeout}

HTTP receive timeout (in seconds) for fetch part requests. Inherited from default profile [http_receive_timeout](./settings.md#http_receive_timeout) if not set explicitly.

Possible values:

- Any positive integer.
- 0 - Use value of `http_receive_timeout`.

Default value: 0.

## max_replicated_fetches_network_bandwidth {#max_replicated_fetches_network_bandwidth}

Limits the maximum speed of data exchange over the network in bytes per second for [replicated](../../engines/table-engines/mergetree-family/replication.md) fetches. This setting is applied to a particular table, unlike the [max_replicated_fetches_network_bandwidth_for_server](settings.md#max_replicated_fetches_network_bandwidth_for_server) setting, which is applied to the server.

You can limit both server network and network for a particular table, but for this the value of the table-level setting should be less than server-level one. Otherwise the server considers only the `max_replicated_fetches_network_bandwidth_for_server` setting.

The setting isn't followed perfectly accurately.

Possible values:

- Positive integer.
- 0 — Unlimited.

Default value: `0`.

**Usage**

Could be used for throttling speed when replicating data to add or replace new nodes.

## max_replicated_sends_network_bandwidth {#max_replicated_sends_network_bandwidth}

Limits the maximum speed of data exchange over the network in bytes per second for [replicated](../../engines/table-engines/mergetree-family/replication.md) sends. This setting is applied to a particular table, unlike the [max_replicated_sends_network_bandwidth_for_server](settings.md#max_replicated_sends_network_bandwidth_for_server) setting, which is applied to the server.

You can limit both server network and network for a particular table, but for this the value of the table-level setting should be less than server-level one. Otherwise the server considers only the `max_replicated_sends_network_bandwidth_for_server` setting.

The setting isn't followed perfectly accurately.

Possible values:

- Positive integer.
- 0 — Unlimited.

Default value: `0`.

**Usage**

Could be used for throttling speed when replicating data to add or replace new nodes.

## old_parts_lifetime {#old-parts-lifetime}

The time (in seconds) of storing inactive parts to protect against data loss during spontaneous server reboots.

Possible values:

- Any positive integer.

Default value: 480.

After merging several parts into a new part, ClickHouse marks the original parts as inactive and deletes them only after `old_parts_lifetime` seconds.
Inactive parts are removed if they are not used by current queries, i.e. if the `refcount` of the part is 1.

`fsync` is not called for new parts, so for some time new parts exist only in the server's RAM (OS cache). If the server is rebooted spontaneously, new parts can be lost or damaged.
To protect data inactive parts are not deleted immediately.

During startup ClickHouse checks the integrity of the parts.
If the merged part is damaged ClickHouse returns the inactive parts to the active list, and later merges them again. Then the damaged part is renamed (the `broken_` prefix is added) and moved to the `detached` folder.
If the merged part is not damaged, then the original inactive parts are renamed (the `ignored_` prefix is added) and moved to the `detached` folder.

The default `dirty_expire_centisecs` value (a Linux kernel setting) is 30 seconds (the maximum time that written data is stored only in RAM), but under heavy loads on the disk system data can be written much later. Experimentally, a value of 480 seconds was chosen for `old_parts_lifetime`, during which a new part is guaranteed to be written to disk.

## max_bytes_to_merge_at_max_space_in_pool {#max-bytes-to-merge-at-max-space-in-pool}

The maximum total parts size (in bytes) to be merged into one part, if there are enough resources available.
`max_bytes_to_merge_at_max_space_in_pool` -- roughly corresponds to the maximum possible part size created by an automatic background merge.

Possible values:

- Any positive integer.

Default value: 161061273600 (150 GB).

The merge scheduler periodically analyzes the sizes and number of parts in partitions, and if there is enough free resources in the pool, it starts background merges. Merges occur until the total size of the source parts is larger than `max_bytes_to_merge_at_max_space_in_pool`.

Merges initiated by [OPTIMIZE FINAL](../../sql-reference/statements/optimize.md) ignore `max_bytes_to_merge_at_max_space_in_pool` and merge parts only taking into account available resources (free disk's space) until one part remains in the partition.

## max_bytes_to_merge_at_min_space_in_pool {#max-bytes-to-merge-at-min-space-in-pool}

The maximum total part size (in bytes) to be merged into one part, with the minimum available resources in the background pool.

Possible values:

- Any positive integer.

Default value: 1048576 (1 MB)

`max_bytes_to_merge_at_min_space_in_pool` defines the maximum total size of parts which can be merged despite the lack of available disk space (in pool). This is necessary to reduce the number of small parts and the chance of `Too many parts` errors.
Merges book disk space by doubling the total merged parts sizes. Thus, with a small amount of free disk space, a situation may happen that there is free space, but this space is already booked by ongoing large merges, so other merges unable to start, and the number of small parts grows with every insert.

## merge_max_block_size {#merge-max-block-size}

The number of rows that are read from the merged parts into memory.

Possible values:

- Any positive integer.

Default value: 8192

Merge reads rows from parts in blocks of `merge_max_block_size` rows, then merges and writes the result into a new part. The read block is placed in RAM, so `merge_max_block_size` affects the size of the RAM required for the merge. Thus, merges can consume a large amount of RAM for tables with very wide rows (if the average row size is 100kb, then when merging 10 parts, (100kb * 10 * 8192) = ~ 8GB of RAM). By decreasing `merge_max_block_size`, you can reduce the amount of RAM required for a merge but slow down a merge.

## number_of_free_entries_in_pool_to_lower_max_size_of_merge {#number-of-free-entries-in-pool-to-lower-max-size-of-merge}

When there is less than specified number of free entries in pool (or replicated queue), start to lower maximum size of merge to process (or to put in queue).
This is to allow small merges to process - not filling the pool with long running merges.

Possible values:

- Any positive integer.

Default value: 8

## number_of_free_entries_in_pool_to_execute_mutation {#number-of-free-entries-in-pool-to-execute-mutation}

When there is less than specified number of free entries in pool, do not execute part mutations.
This is to leave free threads for regular merges and avoid "Too many parts".

Possible values:

- Any positive integer.

Default value: 20

**Usage**

The value of the `number_of_free_entries_in_pool_to_execute_mutation` setting should be less than the value of the [background_pool_size](/docs/en/operations/server-configuration-parameters/settings.md/#background_pool_size) * [background_merges_mutations_concurrency_ratio](/docs/en/operations/server-configuration-parameters/settings.md/#background_merges_mutations_concurrency_ratio). Otherwise, ClickHouse throws an exception.

## max_part_loading_threads {#max-part-loading-threads}

The maximum number of threads that read parts when ClickHouse starts.

Possible values:

- Any positive integer.

Default value: auto (number of CPU cores).

During startup ClickHouse reads all parts of all tables (reads files with metadata of parts) to build a list of all parts in memory. In some systems with a large number of parts this process can take a long time, and this time might be shortened by increasing `max_part_loading_threads` (if this process is not CPU and disk I/O bound).

## max_partitions_to_read {#max-partitions-to-read}

Limits the maximum number of partitions that can be accessed in one query.

The setting value specified when the table is created can be overridden via query-level setting.

Possible values:

- Any positive integer.

Default value: -1 (unlimited).

You can also specify a query complexity setting [max_partitions_to_read](query-complexity#max-partitions-to-read) at a query / session / profile level.

## min_age_to_force_merge_seconds {#min_age_to_force_merge_seconds}

Merge parts if every part in the range is older than the value of `min_age_to_force_merge_seconds`.

Possible values:

- Positive integer.

Default value: 0 — Disabled.

## min_age_to_force_merge_on_partition_only {#min_age_to_force_merge_on_partition_only}

Whether `min_age_to_force_merge_seconds` should be applied only on the entire partition and not on subset.

Possible values:

- true, false

Default value: false

## number_of_free_entries_in_pool_to_execute_optimize_entire_partition {#number_of_free_entries_in_pool_to_execute_optimize_entire_partition}

When there is less than specified number of free entries in pool, do not execute optimizing entire partition in the background (this task generated when set `min_age_to_force_merge_seconds` and enable `min_age_to_force_merge_on_partition_only`). This is to leave free threads for regular merges and avoid "Too many parts".

Possible values:

- Positive integer.

Default value: 25

The value of the `number_of_free_entries_in_pool_to_execute_optimize_entire_partition` setting should be less than the value of the [background_pool_size](/docs/en/operations/server-configuration-parameters/settings.md/#background_pool_size) * [background_merges_mutations_concurrency_ratio](/docs/en/operations/server-configuration-parameters/settings.md/#background_merges_mutations_concurrency_ratio). Otherwise, ClickHouse throws an exception.


## allow_floating_point_partition_key {#allow_floating_point_partition_key}

Enables to allow floating-point number as a partition key.

Possible values:

- 0 — Floating-point partition key not allowed.
- 1 — Floating-point partition key allowed.

Default value: `0`.

## check_sample_column_is_correct {#check_sample_column_is_correct}

Enables the check at table creation, that the data type of a column for sampling or sampling expression is correct. The data type must be one of unsigned [integer types](../../sql-reference/data-types/int-uint.md): `UInt8`, `UInt16`, `UInt32`, `UInt64`.

Possible values:

- true  — The check is enabled.
- false — The check is disabled at table creation.

Default value: `true`.

By default, the ClickHouse server checks at table creation the data type of a column for sampling or sampling expression. If you already have tables with incorrect sampling expression and do not want the server to raise an exception during startup, set `check_sample_column_is_correct` to `false`.

## min_bytes_to_rebalance_partition_over_jbod {#min-bytes-to-rebalance-partition-over-jbod}

Sets minimal amount of bytes to enable balancing when distributing new big parts over volume disks [JBOD](https://en.wikipedia.org/wiki/Non-RAID_drive_architectures).

Possible values:

- Positive integer.
- 0 — Balancing is disabled.

Default value: `0`.

**Usage**

The value of the `min_bytes_to_rebalance_partition_over_jbod` setting should not be less than the value of the [max_bytes_to_merge_at_max_space_in_pool](../../operations/settings/merge-tree-settings.md#max-bytes-to-merge-at-max-space-in-pool) / 1024. Otherwise, ClickHouse throws an exception.

## detach_not_byte_identical_parts {#detach_not_byte_identical_parts}

Enables or disables detaching a data part on a replica after a merge or a mutation, if it is not byte-identical to data parts on other replicas. If disabled, the data part is removed. Activate this setting if you want to analyze such parts later.

The setting is applicable to `MergeTree` tables with enabled [data replication](../../engines/table-engines/mergetree-family/replication.md).

Possible values:

- 0 — Parts are removed.
- 1 — Parts are detached.

Default value: `0`.

## merge_tree_clear_old_temporary_directories_interval_seconds {#setting-merge-tree-clear-old-temporary-directories-interval-seconds}

Sets the interval in seconds for ClickHouse to execute the cleanup of old temporary directories.

Possible values:

- Any positive integer.

Default value: `60` seconds.

## merge_tree_clear_old_parts_interval_seconds {#setting-merge-tree-clear-old-parts-interval-seconds}

Sets the interval in seconds for ClickHouse to execute the cleanup of old parts, WALs, and mutations.

Possible values:

- Any positive integer.

Default value: `1` second.

## max_concurrent_queries {#max-concurrent-queries}

Max number of concurrently executed queries related to the MergeTree table. Queries will still be limited by other `max_concurrent_queries` settings.

Possible values:

- Positive integer.
- 0 — No limit.

Default value: `0` (no limit).

**Example**

``` xml
<max_concurrent_queries>50</max_concurrent_queries>
```

## min_marks_to_honor_max_concurrent_queries {#min-marks-to-honor-max-concurrent-queries}

The minimal number of marks read by the query for applying the [max_concurrent_queries](#max-concurrent-queries) setting. Note that queries will still be limited by other `max_concurrent_queries` settings.

Possible values:

- Positive integer.
- 0 — Disabled (`max_concurrent_queries` limit applied to no queries).

Default value: `0` (limit never applied).

**Example**

``` xml
<min_marks_to_honor_max_concurrent_queries>10</min_marks_to_honor_max_concurrent_queries>
```

## ratio_of_defaults_for_sparse_serialization {#ratio_of_defaults_for_sparse_serialization}

Minimal ratio of the number of _default_ values to the number of _all_ values in a column. Setting this value causes the column to be stored using sparse serializations.

If a column is sparse (contains mostly zeros), ClickHouse can encode it in a sparse format and automatically optimize calculations - the data does not require full decompression during queries. To enable this sparse serialization, define the `ratio_of_defaults_for_sparse_serialization` setting to be less than 1.0. If the value is greater than or equal to 1.0, then the columns will be always written using the normal full serialization.

Possible values:

- Float between 0 and 1 to enable sparse serialization
- 1.0 (or greater) if you do not want to use sparse serialization

Default value: `0.9375`

**Example**

Notice the `s` column in the following table is an empty string for 95% of the rows. In `my_regular_table` we do not use sparse serialization, and in `my_sparse_table` we set `ratio_of_defaults_for_sparse_serialization` to 0.95:

```sql
CREATE TABLE my_regular_table
(
    `id` UInt64,
    `s` String
)
ENGINE = MergeTree
ORDER BY id;

INSERT INTO my_regular_table
SELECT
    number AS id,
    number % 20 = 0 ? toString(number): '' AS s
FROM
    numbers(10000000);


CREATE TABLE my_sparse_table
(
    `id` UInt64,
    `s` String
)
ENGINE = MergeTree
ORDER BY id
SETTINGS ratio_of_defaults_for_sparse_serialization = 0.95;

INSERT INTO my_sparse_table
SELECT
    number,
    number % 20 = 0 ? toString(number): ''
FROM
    numbers(10000000);
```

Notice the `s` column in `my_sparse_table` uses less storage space on disk:

```sql
SELECT table, name, data_compressed_bytes, data_uncompressed_bytes FROM system.columns
WHERE table LIKE 'my_%_table';
```

```response
┌─table────────────┬─name─┬─data_compressed_bytes─┬─data_uncompressed_bytes─┐
│ my_regular_table │ id   │              37790741 │                75488328 │
│ my_regular_table │ s    │               2451377 │                12683106 │
│ my_sparse_table  │ id   │              37790741 │                75488328 │
│ my_sparse_table  │ s    │               2283454 │                 9855751 │
└──────────────────┴──────┴───────────────────────┴─────────────────────────┘
```

You can verify if a column is using the sparse encoding by viewing the `serialization_kind` column of the `system.parts_columns` table:

```sql
SELECT column, serialization_kind FROM system.parts_columns
WHERE table LIKE 'my_sparse_table';
```

You can see which parts of `s` were stored using the sparse serialization:

```response
┌─column─┬─serialization_kind─┐
│ id     │ Default            │
│ s      │ Default            │
│ id     │ Default            │
│ s      │ Default            │
│ id     │ Default            │
│ s      │ Sparse             │
│ id     │ Default            │
│ s      │ Sparse             │
│ id     │ Default            │
│ s      │ Sparse             │
│ id     │ Default            │
│ s      │ Sparse             │
│ id     │ Default            │
│ s      │ Sparse             │
│ id     │ Default            │
│ s      │ Sparse             │
│ id     │ Default            │
│ s      │ Sparse             │
│ id     │ Default            │
│ s      │ Sparse             │
│ id     │ Default            │
│ s      │ Sparse             │
└────────┴────────────────────┘
```

## replace_long_file_name_to_hash {#replace_long_file_name_to_hash}
If the file name for column is too long (more than `max_file_name_length` bytes) replace it to SipHash128. Default value: `false`.

## max_file_name_length {#max_file_name_length}

The maximal length of the file name to keep it as is without hashing. Takes effect only if setting `replace_long_file_name_to_hash` is enabled. The value of this setting does not include the length of file extension. So, it is recommended to set it below the maximum filename length (usually 255 bytes) with some gap to avoid filesystem errors. Default value: 127.

## allow_experimental_block_number_column

Persists virtual column `_block_number` on merges.

Default value: false.

## exclude_deleted_rows_for_part_size_in_merge {#exclude_deleted_rows_for_part_size_in_merge}

If enabled, estimated actual size of data parts (i.e., excluding those rows that have been deleted through `DELETE FROM`) will be used when selecting parts to merge. Note that this behavior is only triggered for data parts affected by `DELETE FROM` executed after this setting is enabled.

Possible values:

- true, false

Default value: false

**See Also**

- [load_existing_rows_count_for_old_parts](#load_existing_rows_count_for_old_parts) setting

## load_existing_rows_count_for_old_parts {#load_existing_rows_count_for_old_parts}

If enabled along with [exclude_deleted_rows_for_part_size_in_merge](#exclude_deleted_rows_for_part_size_in_merge), deleted rows count for existing data parts will be calculated during table starting up. Note that it may slow down start up table loading.

Possible values:

- true, false

Default value: false

**See Also**

- [exclude_deleted_rows_for_part_size_in_merge](#exclude_deleted_rows_for_part_size_in_merge) setting

## use_compact_variant_discriminators_serialization {#use_compact_variant_discriminators_serialization}

Enables compact mode for binary serialization of discriminators in Variant data type.
This mode allows to use significantly less memory for storing discriminators in parts when there is mostly one variant or a lot of NULL values.

Default value: true

## merge_workload

Used to regulate how resources are utilized and shared between merges and other workloads. Specified value is used as `workload` setting value for background merges of this table. If not specified (empty string), then server setting `merge_workload` is used instead.

Default value: an empty string

**See Also**
- [Workload Scheduling](/docs/en/operations/workload-scheduling.md)

## mutation_workload

Used to regulate how resources are utilized and shared between mutations and other workloads. Specified value is used as `workload` setting value for background mutations of this table. If not specified (empty string), then server setting `mutation_workload` is used instead.

Default value: an empty string

**See Also**
- [Workload Scheduling](/docs/en/operations/workload-scheduling.md)

### optimize_row_order

Controls if the row order should be optimized during inserts to improve the compressability of the newly inserted table part.

Only has an effect for ordinary MergeTree-engine tables. Does nothing for specialized MergeTree engine tables (e.g. CollapsingMergeTree).

MergeTree tables are (optionally) compressed using [compression codecs](../../sql-reference/statements/create/table.md#column_compression_codec).
Generic compression codecs such as LZ4 and ZSTD achieve maximum compression rates if the data exposes patterns.
Long runs of the same value typically compress very well.

If this setting is enabled, ClickHouse attempts to store the data in newly inserted parts in a row order that minimizes the number of equal-value runs across the columns of the new table part.
In other words, a small number of equal-value runs mean that individual runs are long and compress well.

Finding the optimal row order is computationally infeasible (NP hard).
Therefore, ClickHouse uses a heuristics to quickly find a row order which still improves compression rates over the original row order.

<details markdown="1">

<summary>Heuristics for finding a row order</summary>

It is generally possible to shuffle the rows of a table (or table part) freely as SQL considers the same table (table part) in different row order equivalent.

This freedom of shuffling rows is restricted when a primary key is defined for the table.
In ClickHouse, a primary key `C1, C2, ..., CN` enforces that the table rows are sorted by columns `C1`, `C2`, ... `Cn` ([clustered index](https://en.wikipedia.org/wiki/Database_index#Clustered)).
As a result, rows can only be shuffled within "equivalence classes" of row, i.e. rows which have the same values in their primary key columns.
The intuition is that primary keys with high-cardinality, e.g. primary keys involving a `DateTime64` timestamp column, lead to many small equivalence classes.
Likewise, tables with a low-cardinality primary key, create few and large equivalence classes.
A table with no primary key represents the extreme case of a single equivalence class which spans all rows.

The fewer and the larger the equivalence classes are, the higher the degree of freedom when re-shuffling rows.

The heuristics applied to find the best row order within each equivalence class is suggested by D. Lemire, O. Kaser in [Reordering columns for smaller indexes](https://doi.org/10.1016/j.ins.2011.02.002) and based on sorting the rows within each equivalence class by ascending cardinality of the non-primary key columns.
It performs three steps:
1. Find all equivalence classes based on the row values in primary key columns.
2. For each equivalence class, calculate (usually estimate) the cardinalities of the non-primary-key columns.
3. For each equivalence class, sort the rows in order of ascending non-primary-key column cardinality.

</details>

If enabled, insert operations incur additional CPU costs to analyze and optimize the row order of the new data.
INSERTs are expected to take 30-50% longer depending on the data characteristics.
Compression rates of LZ4 or ZSTD improve on average by 20-40%.

This setting works best for tables with no primary key or a low-cardinality primary key, i.e. a table with only few distinct primary key values.
High-cardinality primary keys, e.g. involving timestamp columns of type `DateTime64`, are not expected to benefit from this setting.

## lightweight_mutation_projection_mode

By default, lightweight delete `DELETE` does not work for tables with projections. This is because rows in a projection may be affected by a `DELETE` operation. So the default value would be `throw`.
However, this option can change the behavior. With the value either `drop` or `rebuild`, deletes will work with projections. `drop` would delete the projection so it might be fast in the current query as projection gets deleted but slow in future queries as no projection attached.
`rebuild` would rebuild the projection which might affect the performance of the current query, but might speedup for future queries. A good thing is that these options would only work in the part level,
which means projections in the part that don't get touched would stay intact instead of triggering any action like drop or rebuild.

Possible values:

- throw, drop, rebuild

Default value: throw

## deduplicate_merge_projection_mode

Whether to allow create projection for the table with non-classic MergeTree, that is not (Replicated, Shared) MergeTree. Ignore option is purely for compatibility which might result in incorrect answer. Otherwise, if allowed, what is the action when merge projections, either drop or rebuild. So classic MergeTree would ignore this setting.
It also controls `OPTIMIZE DEDUPLICATE` as well, but has effect on all MergeTree family members. Similar to the option `lightweight_mutation_projection_mode`, it is also part level.

Possible values:

- ignore, throw, drop, rebuild

Default value: throw

## min_free_disk_bytes_to_perform_insert

The minimum number of bytes that should be free in disk space in order to insert data. If the number of available free bytes is less than `min_free_disk_bytes_to_throw_insert` then an exception is thrown and the insert is not executed. Note that this setting:
- takes into account the `keep_free_space_bytes` setting.
- does not take into account the amount of data that will be written by the `INSERT` operation.
- is only checked if a positive (non-zero) number of bytes is specified

Possible values:

- Any positive integer.

Default value: 0 bytes.

Note that if both `min_free_disk_bytes_to_perform_insert` and `min_free_disk_ratio_to_perform_insert` are specified, ClickHouse will count on the value that will allow to perform inserts on a bigger amount of free memory.

## min_free_disk_ratio_to_perform_insert 

The minimum free to total disk space ratio to perform an `INSERT`. Must be a floating point value between 0 and 1. Note that this setting:
- takes into account the `keep_free_space_bytes` setting.
- does not take into account the amount of data that will be written by the `INSERT` operation.
- is only checked if a positive (non-zero) ratio is specified

Possible values:

- Float, 0.0 - 1.0

Default value: 0.0

Note that if both `min_free_disk_ratio_to_perform_insert` and `min_free_disk_bytes_to_perform_insert` are specified, ClickHouse will count on the value that will allow to perform inserts on a bigger amount of free memory.
