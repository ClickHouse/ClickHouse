# MergeTree tables settings {#merge-tree-settings}

The values of `merge_tree` settings (for all MergeTree tables) can be viewed in the table `system.merge_tree_settings`, they can be overridden in `config.xml` in the `merge_tree` section, or set in the `SETTINGS` section of each table.

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

The number of most recently inserted blocks for which Zookeeper stores hash sums to check for duplicates.

Possible values:

-   Any positive integer.
-   0 (disable deduplication)

Default value: 100.

The `Insert` command creates one or more blocks (parts). When inserting into Replicated tables, ClickHouse for [insert deduplication](../../engines/table-engines/mergetree-family/replication/) writes the hash sums of the created parts into Zookeeper. Hash sums are stored only for the most recent `replicated_deduplication_window` blocks. The oldest hash sums are removed from Zookeeper.
A large number of `replicated_deduplication_window` slows down `Inserts` because it needs to compare more entries.
The hash sum is calculated from the composition of the field names and types and the data of the inserted part (stream of bytes).

## replicated_deduplication_window_seconds {#replicated-deduplication-window-seconds}

The number of seconds after which the hash sums of the inserted blocks are removed from Zookeeper.

Possible values:

-   Any positive integer.

Default value: 604800 (1 week).

Similar to [replicated_deduplication_window](#replicated-deduplication-window), `replicated_deduplication_window_seconds` specifies how long to store hash sums of blocks for insert deduplication. Hash sums older than `replicated_deduplication_window_seconds` are removed from Zookeeper, even if they are less than ` replicated_deduplication_window`.

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

## old_parts_lifetime {#old-parts-lifetime}

The time (in seconds) of storing inactive parts to protect against data loss during spontaneous server reboots.

Possible values:

-   Any positive integer.

Default value: 480.

`fsync` is not called for new parts, so for some time new parts exist only in the server's RAM (OS cache). If the server is rebooted spontaneously, new parts can be lost or damaged.
To protect data parts created by merges source parts are not deleted immediately. After merging several parts into a new part, ClickHouse marks the original parts as inactive and deletes them only after `old_parts_lifetime` seconds.
Inactive parts are removed if they are not used by current queries, i.e. if the `refcount` of the part is zero.

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

The merge scheduler periodically analyzes the sizes and number of parts in partitions, and if there is enough free resources in the pool, it starts background merges. Merges occur until the total size of the source parts is less than `max_bytes_to_merge_at_max_space_in_pool`.

Merges initiated by `optimize final` ignore `max_bytes_to_merge_at_max_space_in_pool` and merge parts only taking into account available resources (free disk's space) until one part remains in the partition.

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

[Original article](https://clickhouse.tech/docs/en/operations/settings/merge_tree_settings/) <!--hide-->
