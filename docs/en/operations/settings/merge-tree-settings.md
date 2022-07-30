# MergeTree tables settings

The values of `merge_tree` settings (for all MergeTree tables) can be viewed in the table `system.merge_tree_settings`, they can be overridden in `config.xml` in the `merge_tree` section, or set in the `SETTINGS` section of each table.

These are example overrides for `max_suspicious_broken_parts`:

## max_suspicious_broken_parts

If the number of broken parts in a single partition exceeds the `max_suspicious_broken_parts` value, automatic deletion is denied.

Possible values:

-   Any positive integer.

Default value: 10.

Override example in `config.xml`:

``` text
<merge_tree>
    <max_suspicious_broken_parts>5</max_suspicious_broken_parts>
</merge_tree>
```

An example to set in `SETTINGS` for a particular table:

``` sql
CREATE TABLE foo
(
    `A` Int64
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS max_suspicious_broken_parts = 500;
```

An example of changing the settings for a specific table with the `ALTER TABLE ... MODIFY SETTING` command:

``` sql
ALTER TABLE foo
    MODIFY SETTING max_suspicious_broken_parts = 100;

-- reset to default (use value from system.merge_tree_settings)
ALTER TABLE foo
    RESET SETTING max_suspicious_broken_parts;
```

## parts_to_throw_insert {#parts-to-throw-insert}

If the number of active parts in a single partition exceeds the `parts_to_throw_insert` value, `INSERT` is interrupted with the `Too many parts (N). Merges are processing significantly slower than inserts` exception.

Possible values:

-   Any positive integer.

Default value: 300.

To achieve maximum performance of `SELECT` queries, it is necessary to minimize the number of parts processed, see [Merge Tree](../../development/architecture.md#merge-tree).

You can set a larger value to 600 (1200), this will reduce the probability of the `Too many parts` error, but at the same time `SELECT` performance might degrade. Also in case of a merge issue (for example, due to insufficient disk space) you will notice it later than it could be with the original 300.


## parts_to_delay_insert {#parts-to-delay-insert}

If the number of active parts in a single partition exceeds the `parts_to_delay_insert` value, an `INSERT` artificially slows down.

Possible values:

-   Any positive integer.

Default value: 150.

ClickHouse artificially executes `INSERT` longer (adds ‘sleep’) so that the background merge process can merge parts faster than they are added.

## inactive_parts_to_throw_insert {#inactive-parts-to-throw-insert}

If the number of inactive parts in a single partition more than the `inactive_parts_to_throw_insert` value, `INSERT` is interrupted with the "Too many inactive parts (N). Parts cleaning are processing significantly slower than inserts" exception.

Possible values:

-   Any positive integer.

Default value: 0 (unlimited).

## inactive_parts_to_delay_insert {#inactive-parts-to-delay-insert}

If the number of inactive parts in a single partition in the table at least that many the `inactive_parts_to_delay_insert` value, an `INSERT` artificially slows down. It is useful when a server fails to clean up parts quickly enough.

Possible values:

-   Any positive integer.

Default value: 0 (unlimited).

## max_delay_to_insert {#max-delay-to-insert}

The value in seconds, which is used to calculate the `INSERT` delay, if the number of active parts in a single partition exceeds the [parts_to_delay_insert](#parts-to-delay-insert) value.

Possible values:

-   Any positive integer.

Default value: 1.

The delay (in milliseconds) for `INSERT` is calculated by the formula:

```code
max_k = parts_to_throw_insert - parts_to_delay_insert
k = 1 + parts_count_in_partition - parts_to_delay_insert
delay_milliseconds = pow(max_delay_to_insert * 1000, k / max_k)
```

For example if a partition has 299 active parts and parts_to_throw_insert = 300, parts_to_delay_insert = 150, max_delay_to_insert = 1, `INSERT` is delayed for `pow( 1 * 1000, (1 + 299 - 150) / (300 - 150) ) = 1000` milliseconds.

## max_parts_in_total {#max-parts-in-total}

If the total number of active parts in all partitions of a table exceeds the `max_parts_in_total` value `INSERT` is interrupted with the `Too many parts (N)` exception.

Possible values:

-   Any positive integer.

Default value: 100000.

A large number of parts in a table reduces performance of ClickHouse queries and increases ClickHouse boot time. Most often this is a consequence of an incorrect design (mistakes when choosing a partitioning strategy - too small partitions).

## replicated_deduplication_window {#replicated-deduplication-window}

The number of most recently inserted blocks for which ClickHouse Keeper stores hash sums to check for duplicates.

Possible values:

-   Any positive integer.
-   0 (disable deduplication)

Default value: 100.

The `Insert` command creates one or more blocks (parts). For [insert deduplication](../../engines/table-engines/mergetree-family/replication/), when writing into replicated tables, ClickHouse writes the hash sums of the created parts into ClickHouse Keeper. Hash sums are stored only for the most recent `replicated_deduplication_window` blocks. The oldest hash sums are removed from ClickHouse Keeper.
A large number of `replicated_deduplication_window` slows down `Inserts` because it needs to compare more entries.
The hash sum is calculated from the composition of the field names and types and the data of the inserted part (stream of bytes).

## non_replicated_deduplication_window {#non-replicated-deduplication-window}

The number of the most recently inserted blocks in the non-replicated [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) table for which hash sums are stored to check for duplicates.

Possible values:

-   Any positive integer.
-   0 (disable deduplication).

Default value: 0.

A deduplication mechanism is used, similar to replicated tables (see [replicated_deduplication_window](#replicated-deduplication-window) setting). The hash sums of the created parts are written to a local file on a disk.

## replicated_deduplication_window_seconds {#replicated-deduplication-window-seconds}

The number of seconds after which the hash sums of the inserted blocks are removed from ClickHouse Keeper.

Possible values:

-   Any positive integer.

Default value: 604800 (1 week).

Similar to [replicated_deduplication_window](#replicated-deduplication-window), `replicated_deduplication_window_seconds` specifies how long to store hash sums of blocks for insert deduplication. Hash sums older than `replicated_deduplication_window_seconds` are removed from ClickHouse Keeper, even if they are less than ` replicated_deduplication_window`.

## max_replicated_logs_to_keep

How many records may be in the ClickHouse Keeper log if there is inactive replica. An inactive replica becomes lost when when this number exceed.

Possible values:

-   Any positive integer.

Default value: 1000

## min_replicated_logs_to_keep

Keep about this number of last records in ZooKeeper log, even if they are obsolete. It doesn't affect work of tables: used only to diagnose ZooKeeper log before cleaning.

Possible values:

-   Any positive integer.

Default value: 10

## prefer_fetch_merged_part_time_threshold

If the time passed since a replication log (ClickHouse Keeper or ZooKeeper) entry creation exceeds this threshold, and the sum of the size of parts is greater than `prefer_fetch_merged_part_size_threshold`, then prefer fetching merged part from a replica instead of doing merge locally. This is to speed up very long merges.

Possible values:

-   Any positive integer.

Default value: 3600

## prefer_fetch_merged_part_size_threshold

If the sum of the size of parts exceeds this threshold and the time since a replication log entry creation is greater than `prefer_fetch_merged_part_time_threshold`, then prefer fetching merged part from a replica instead of doing merge locally. This is to speed up very long merges.

Possible values:

-   Any positive integer.

Default value: 10,737,418,240

## execute_merges_on_single_replica_time_threshold

When this setting has a value greater than zero, only a single replica starts the merge immediately, and other replicas wait up to that amount of time to download the result instead of doing merges locally. If the chosen replica doesn't finish the merge during that amount of time, fallback to standard behavior happens.

Possible values:

-   Any positive integer.

Default value: 0 (seconds)

## remote_fs_execute_merges_on_single_replica_time_threshold

When this setting has a value greater than than zero only a single replica starts the merge immediately if merged part on shared storage and `allow_remote_fs_zero_copy_replication` is enabled.

Possible values:

-   Any positive integer.

Default value: 1800

## try_fetch_recompressed_part_timeout

Recompression works slow in most cases, so we don't start merge with recompression until this timeout and trying to fetch recompressed part from replica which assigned this merge with recompression.

Possible values:

-   Any positive integer.

Default value: 7200

## always_fetch_merged_part

If true, this replica never merges parts and always downloads merged parts from other replicas.

Possible values:

-   true, false

Default value: false

## max_suspicious_broken_parts

Max broken parts, if more - deny automatic deletion.

Possible values:

-   Any positive integer.

Default value: 10

## max_suspicious_broken_parts_bytes


Max size of all broken parts, if more - deny automatic deletion.

Possible values:

-   Any positive integer.

Default value: 1,073,741,824

## max_files_to_modify_in_alter_columns

Do not apply ALTER if number of files for modification(deletion, addition) is greater than this setting.

Possible values:

-   Any positive integer.

Default value: 75

## max_files_to_remove_in_alter_columns

Do not apply ALTER, if the number of files for deletion is greater than this setting.

Possible values:

-   Any positive integer.

Default value: 50

## replicated_max_ratio_of_wrong_parts

If the ratio of wrong parts to total number of parts is less than this - allow to start.

Possible values:

-   Float, 0.0 - 1.0

Default value: 0.5

## replicated_max_parallel_fetches_for_host 

Limit parallel fetches from endpoint (actually pool size).

Possible values:

-   Any positive integer.

Default value: 15

## replicated_fetches_http_connection_timeout

HTTP connection timeout for part fetch requests. Inherited from default profile `http_connection_timeout` if not set explicitly.

Possible values:

-   Any positive integer.

Default value: Inherited from default profile `http_connection_timeout` if not set explicitly.

## replicated_can_become_leader

If true, replicated tables replicas on this node will try to acquire leadership.

Possible values:

-   true, false

Default value: true

## zookeeper_session_expiration_check_period

ZooKeeper session expiration check period, in seconds.

Possible values:

-   Any positive integer.

Default value: 60

## detach_old_local_parts_when_cloning_replica

Do not remove old local parts when repairing lost replica.

Possible values:

-   true, false

Default value: true

## replicated_fetches_http_connection_timeout {#replicated_fetches_http_connection_timeout}

HTTP connection timeout (in seconds) for part fetch requests. Inherited from default profile [http_connection_timeout](./settings.md#http_connection_timeout) if not set explicitly.

Possible values:

-   Any positive integer.
-   0 - Use value of `http_connection_timeout`.

Default value: 0.

## replicated_fetches_http_send_timeout {#replicated_fetches_http_send_timeout}

HTTP send timeout (in seconds) for part fetch requests. Inherited from default profile [http_send_timeout](./settings.md#http_send_timeout) if not set explicitly.

Possible values:

-   Any positive integer.
-   0 - Use value of `http_send_timeout`.

Default value: 0.

## replicated_fetches_http_receive_timeout {#replicated_fetches_http_receive_timeout}

HTTP receive timeout (in seconds) for fetch part requests. Inherited from default profile [http_receive_timeout](./settings.md#http_receive_timeout) if not set explicitly.

Possible values:

-   Any positive integer.
-   0 - Use value of `http_receive_timeout`.

Default value: 0.

## max_replicated_fetches_network_bandwidth {#max_replicated_fetches_network_bandwidth}

Limits the maximum speed of data exchange over the network in bytes per second for [replicated](../../engines/table-engines/mergetree-family/replication.md) fetches. This setting is applied to a particular table, unlike the [max_replicated_fetches_network_bandwidth_for_server](settings.md#max_replicated_fetches_network_bandwidth_for_server) setting, which is applied to the server.

You can limit both server network and network for a particular table, but for this the value of the table-level setting should be less than server-level one. Otherwise the server considers only the `max_replicated_fetches_network_bandwidth_for_server` setting.

The setting isn't followed perfectly accurately.

Possible values:

-   Positive integer.
-   0 — Unlimited.

Default value: `0`.

**Usage**

Could be used for throttling speed when replicating data to add or replace new nodes.

## max_replicated_sends_network_bandwidth {#max_replicated_sends_network_bandwidth}

Limits the maximum speed of data exchange over the network in bytes per second for [replicated](../../engines/table-engines/mergetree-family/replication.md) sends. This setting is applied to a particular table, unlike the [max_replicated_sends_network_bandwidth_for_server](settings.md#max_replicated_sends_network_bandwidth_for_server) setting, which is applied to the server.

You can limit both server network and network for a particular table, but for this the value of the table-level setting should be less than server-level one. Otherwise the server considers only the `max_replicated_sends_network_bandwidth_for_server` setting.

The setting isn't followed perfectly accurately.

Possible values:

-   Positive integer.
-   0 — Unlimited.

Default value: `0`.

**Usage**

Could be used for throttling speed when replicating data to add or replace new nodes.

## old_parts_lifetime {#old-parts-lifetime}

The time (in seconds) of storing inactive parts to protect against data loss during spontaneous server reboots.

Possible values:

-   Any positive integer.

Default value: 480.

After merging several parts into a new part, ClickHouse marks the original parts as inactive and deletes them only after `old_parts_lifetime` seconds.
Inactive parts are removed if they are not used by current queries, i.e. if the `refcount` of the part is zero.

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

-   Any positive integer.

Default value: 161061273600 (150 GB).

The merge scheduler periodically analyzes the sizes and number of parts in partitions, and if there is enough free resources in the pool, it starts background merges. Merges occur until the total size of the source parts is larger than `max_bytes_to_merge_at_max_space_in_pool`.

Merges initiated by [OPTIMIZE FINAL](../../sql-reference/statements/optimize.md) ignore `max_bytes_to_merge_at_max_space_in_pool` and merge parts only taking into account available resources (free disk's space) until one part remains in the partition.

## max_bytes_to_merge_at_min_space_in_pool {#max-bytes-to-merge-at-min-space-in-pool}

The maximum total part size (in bytes) to be merged into one part, with the minimum available resources in the background pool.

Possible values:

-   Any positive integer.

Default value: 1048576 (1 MB)

`max_bytes_to_merge_at_min_space_in_pool` defines the maximum total size of parts which can be merged despite the lack of available disk space (in pool). This is necessary to reduce the number of small parts and the chance of `Too many parts` errors.
Merges book disk space by doubling the total merged parts sizes. Thus, with a small amount of free disk space, a situation may happen that there is free space, but this space is already booked by ongoing large merges, so other merges unable to start, and the number of small parts grows with every insert.

## merge_max_block_size {#merge-max-block-size}

The number of rows that are read from the merged parts into memory.

Possible values:

-   Any positive integer.

Default value: 8192

Merge reads rows from parts in blocks of `merge_max_block_size` rows, then merges and writes the result into a new part. The read block is placed in RAM, so `merge_max_block_size` affects the size of the RAM required for the merge. Thus, merges can consume a large amount of RAM for tables with very wide rows (if the average row size is 100kb, then when merging 10 parts, (100kb * 10 * 8192) = ~ 8GB of RAM). By decreasing `merge_max_block_size`, you can reduce the amount of RAM required for a merge but slow down a merge.

## max_part_loading_threads {#max-part-loading-threads}

The maximum number of threads that read parts when ClickHouse starts.

Possible values:

-   Any positive integer.

Default value: auto (number of CPU cores).

During startup ClickHouse reads all parts of all tables (reads files with metadata of parts) to build a list of all parts in memory. In some systems with a large number of parts this process can take a long time, and this time might be shortened by increasing `max_part_loading_threads` (if this process is not CPU and disk I/O bound).

## max_partitions_to_read {#max-partitions-to-read}

Limits the maximum number of partitions that can be accessed in one query.

The setting value specified when the table is created can be overridden via query-level setting.

Possible values:

-   Any positive integer.

Default value: -1 (unlimited).

## allow_floating_point_partition_key {#allow_floating_point_partition_key}

Enables to allow floating-point number as a partition key.

Possible values:

-   0 — Floating-point partition key not allowed.
-   1 — Floating-point partition key allowed.

Default value: `0`.

## check_sample_column_is_correct {#check_sample_column_is_correct}

Enables the check at table creation, that the data type of a column for sampling or sampling expression is correct. The data type must be one of unsigned [integer types](../../sql-reference/data-types/int-uint.md): `UInt8`, `UInt16`, `UInt32`, `UInt64`.

Possible values:

-   true  — The check is enabled.
-   false — The check is disabled at table creation.

Default value: `true`.

By default, the ClickHouse server checks at table creation the data type of a column for sampling or sampling expression. If you already have tables with incorrect sampling expression and do not want the server to raise an exception during startup, set `check_sample_column_is_correct` to `false`.

## min_bytes_to_rebalance_partition_over_jbod {#min-bytes-to-rebalance-partition-over-jbod}

Sets minimal amount of bytes to enable balancing when distributing new big parts over volume disks [JBOD](https://en.wikipedia.org/wiki/Non-RAID_drive_architectures).

Possible values:

-   Positive integer.
-   0 — Balancing is disabled.

Default value: `0`.

**Usage**

The value of the `min_bytes_to_rebalance_partition_over_jbod` setting should not be less than the value of the [max_bytes_to_merge_at_max_space_in_pool](../../operations/settings/merge-tree-settings.md#max-bytes-to-merge-at-max-space-in-pool) / 1024. Otherwise, ClickHouse throws an exception.

## detach_not_byte_identical_parts {#detach_not_byte_identical_parts}

Enables or disables detaching a data part on a replica after a merge or a mutation, if it is not byte-identical to data parts on other replicas. If disabled, the data part is removed. Activate this setting if you want to analyze such parts later.

The setting is applicable to `MergeTree` tables with enabled [data replication](../../engines/table-engines/mergetree-family/replication.md).

Possible values:

-   0 — Parts are removed.
-   1 — Parts are detached.

Default value: `0`.

## merge_tree_clear_old_temporary_directories_interval_seconds {#setting-merge-tree-clear-old-temporary-directories-interval-seconds}

Sets the interval in seconds for ClickHouse to execute the cleanup of old temporary directories.

Possible values:

-   Any positive integer.

Default value: `60` seconds.

## merge_tree_clear_old_parts_interval_seconds {#setting-merge-tree-clear-old-parts-interval-seconds}

Sets the interval in seconds for ClickHouse to execute the cleanup of old parts, WALs, and mutations.

Possible values:

-   Any positive integer.

Default value: `1` second.

## max_concurrent_queries {#max-concurrent-queries}

Max number of concurrently executed queries related to the MergeTree table. Queries will still be limited by other `max_concurrent_queries` settings.

Possible values:

-   Positive integer.
-   0 — No limit.

Default value: `0` (no limit).

**Example**

``` xml
<max_concurrent_queries>50</max_concurrent_queries>
```

## min_marks_to_honor_max_concurrent_queries {#min-marks-to-honor-max-concurrent-queries}

The minimal number of marks read by the query for applying the [max_concurrent_queries](#max-concurrent-queries) setting. Note that queries will still be limited by other `max_concurrent_queries` settings.

Possible values:

-   Positive integer.
-   0 — Disabled (`max_concurrent_queries` limit applied to no queries).

Default value: `0` (limit never applied).

**Example**

``` xml
<min_marks_to_honor_max_concurrent_queries>10</min_marks_to_honor_max_concurrent_queries>
```
