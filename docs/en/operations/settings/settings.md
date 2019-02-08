# Settings


## distributed_product_mode

Changes the behavior of [distributed subqueries](../../query_language/select.md).

ClickHouse applies this setting when the query contains the product of distributed tables, i.e. when the query for a distributed table contains a non-GLOBAL subquery for the distributed table.

Restrictions:

- Only applied for IN and JOIN subqueries.
- Only if the FROM section uses a distributed table containing more than one shard.
- If the subquery concerns a distributed table containing more than one shard,
- Not used for a table-valued [remote](../../query_language/table_functions/remote.md) function.

The possible values are:

- `deny`  — Default value. Prohibits using these types of subqueries (returns the "Double-distributed in/JOIN subqueries is denied" exception).
- `local`  — Replaces the database and table in the subquery with local ones for the destination server (shard), leaving the normal `IN` / `JOIN.`
- `global` — Replaces the `IN` / `JOIN` query with `GLOBAL IN` / `GLOBAL JOIN.`
- `allow`  — Allows the use of these types of subqueries.


## fallback_to_stale_replicas_for_distributed_queries {#settings-fallback_to_stale_replicas_for_distributed_queries}

Forces a query to an out-of-date replica if updated data is not available. See "[Replication](../../operations/table_engines/replication.md)".

ClickHouse selects the most relevant from the outdated replicas of the table.

Used when performing `SELECT` from a distributed table that points to replicated tables.

By default, 1 (enabled).

## force_index_by_date {#settings-force_index_by_date}

Disables query execution if the index can't be used by date.

Works with tables in the MergeTree family.

If `force_index_by_date=1`, ClickHouse checks whether the query has a date key condition that can be used for restricting data ranges. If there is no suitable condition, it throws an exception. However, it does not check whether the condition actually reduces the amount of data to read. For example, the condition `Date != ' 2000-01-01 '` is acceptable even when it matches all the data in the table (i.e., running the query requires a full scan). For more information about ranges of data in MergeTree tables, see "[MergeTree](../../operations/table_engines/mergetree.md)".


## force_primary_key

Disables query execution if indexing by the primary key is not possible.

Works with tables in the MergeTree family.

If `force_primary_key=1`, ClickHouse checks to see if the query has a primary key condition that can be used for restricting data ranges. If there is no suitable condition, it throws an exception. However, it does not check whether the condition actually reduces the amount of data to read. For more information about data ranges in MergeTree tables, see "[MergeTree](../../operations/table_engines/mergetree.md)".


## fsync_metadata

Enable or disable fsync when writing .sql files. Enabled by default.

It makes sense to disable it if the server has millions of tiny table chunks that are constantly being created and destroyed.

## input_format_allow_errors_num

Sets the maximum number of acceptable errors when reading from text formats (CSV, TSV, etc.).

The default value is 0.

Always pair it with `input_format_allow_errors_ratio`. To skip errors, both settings must be greater than 0.

If an error occurred while reading rows but the error counter is still less than `input_format_allow_errors_num`, ClickHouse ignores the row and moves on to the next one.

If `input_format_allow_errors_num` is exceeded, ClickHouse throws an exception.

## input_format_allow_errors_ratio

Sets the maximum percentage of errors allowed when reading from text formats (CSV, TSV, etc.).
The percentage of errors is set as a floating-point number between 0 and 1.

The default value is 0.

Always pair it with `input_format_allow_errors_num`. To skip errors, both settings must be greater than 0.

If an error occurred while reading rows but the error counter is still less than `input_format_allow_errors_ratio`, ClickHouse ignores the row and moves on to the next one.

If `input_format_allow_errors_ratio` is exceeded, ClickHouse throws an exception.

## insert_sample_with_metadata

For INSERT queries, specifies that the server need to send metadata about column defaults to the client. This will be used to calculate default expressions. Disabled by default.

## join_default_strictness

Sets default strictness for [JOIN clause](../../query_language/select.md).

**Possible values**

- `ALL` — If the right table has several matching rows, the data will be multiplied by the number of these rows. It is a normal `JOIN` behavior from standard SQL.
- `ANY` — If the right table has several matching rows, only the first one found is joined. If the right table has only one matching row, the results of `ANY` and `ALL` are the same.
- `Empty string` — If `ALL` or `ANY` not specified in query, ClickHouse throws exception.

**Default value**

`ALL`

## max_block_size

In ClickHouse, data is processed by blocks (sets of column parts). The internal processing cycles for a single block are efficient enough, but there are noticeable expenditures on each block. `max_block_size` is a recommendation for what size of block (in number of rows) to load from tables. The block size shouldn't be too small, so that the expenditures on each block are still noticeable, but not too large, so that the query with LIMIT that is completed after the first block is processed quickly, so that too much memory isn't consumed when extracting a large number of columns in multiple threads, and so that at least some cache locality is preserved.

By default, 65,536.

Blocks the size of `max_block_size` are not always loaded from the table. If it is obvious that less data needs to be retrieved, a smaller block is processed.

## preferred_block_size_bytes

Used for the same purpose as `max_block_size`, but it sets the recommended block size in bytes by adapting it to the number of rows in the block.
However, the block size cannot be more than `max_block_size` rows.
By default: 1,000,000. It only works when reading from MergeTree engines.

## merge_tree_uniform_read_distribution {#setting-merge_tree_uniform_read_distribution}

When reading from [MergeTree*](../table_engines/mergetree.md) tables, ClickHouse uses several threads. This setting turns on/off the uniform distribution of reading tasks over the working threads. The algorithm of the uniform distribution aims to make execution time for all the threads approximately equal in a `SELECT` query.

**Possible values**

- 0 — Uniform read distribution turned off.
- 1 — Uniform read distribution turned on.

**Default value** — 1.

## merge_tree_min_rows_for_concurrent_read {#setting-merge_tree_min_rows_for_concurrent_read}

If a number of rows to be read from a file of [MergeTree*](../table_engines/mergetree.md) table exceeds `merge_tree_min_rows_for_concurrent_read` then ClickHouse tries to perform a concurrent reading from this file by several threads.

**Possible values**

Any positive integer.

**Default value** — 163840.

## merge_tree_min_rows_for_seek {#setting-merge_tree_min_rows_for_seek}

If the distance between two data blocks to be read in one file less than `merge_tree_min_rows_for_seek` rows, then ClickHouse does not seek through the file, it reads the data sequentially.

**Possible values**

Any positive integer.

**Default value** — 0.

## merge_tree_coarse_index_granularity {#setting-merge_tree_coarse_index_granularity}

When searching data, ClickHouse checks the data marks in the index file. If ClickHouse finds that required keys are in some range, it divides this range for `merge_tree_coarse_index_granularity` subranges and searches the required keys there recursively.

**Possible values**

Any positive even integer.

**Default value** — 8.

## merge_tree_max_rows_to_use_cache {#setting-merge_tree_max_rows_to_use_cache}

If ClickHouse should read more than `merge_tree_max_rows_to_use_cache` rows in one query, it does not use the cash of uncompressed blocks. The [uncompressed_cache_size](../server_settings/settings.md#server-settings-uncompressed_cache_size) server setting defines the size of the cache of uncompressed blocks.

**Possible values**

Any positive integer.

**Default value** — 1048576.

## log_queries

Setting up query logging.

Queries sent to ClickHouse with this setup are logged according to the rules in the [query_log](../server_settings/settings.md) server configuration parameter.

**Example**:

    log_queries=1

## max_insert_block_size {#settings-max_insert_block_size}

The size of blocks to form for insertion into a table.
This setting only applies in cases when the server forms the blocks.
For example, for an INSERT via the HTTP interface, the server parses the data format and forms blocks of the specified size.
But when using clickhouse-client, the client parses the data itself, and the 'max_insert_block_size' setting on the server doesn't affect the size of the inserted blocks.
The setting also doesn't have a purpose when using INSERT SELECT, since data is inserted using the same blocks that are formed after SELECT.

By default, it is 1,048,576.

This is slightly more than `max_block_size`. The reason for this is because certain table engines (`*MergeTree`) form a data part on the disk for each inserted block, which is a fairly large entity. Similarly, `*MergeTree` tables sort data during insertion, and a large enough block size allows sorting more data in RAM.

## max_replica_delay_for_distributed_queries {#settings-max_replica_delay_for_distributed_queries}

Disables lagging replicas for distributed queries. See "[Replication](../../operations/table_engines/replication.md)".

Sets the time in seconds. If a replica lags more than the set value, this replica is not used.

Default value: 300.

Used when performing `SELECT` from a distributed table that points to replicated tables.

## max_threads {#settings-max_threads}

The maximum number of query processing threads

- excluding threads for retrieving data from remote servers (see the 'max_distributed_connections' parameter).

This parameter applies to threads that perform the same stages of the query processing pipeline in parallel.
For example, if reading from a table, evaluating expressions with functions, filtering with WHERE and pre-aggregating for GROUP BY can all be done in parallel using at least 'max_threads' number of threads, then 'max_threads' are used.

By default, 2.

If less than one SELECT query is normally run on a server at a time, set this parameter to a value slightly less than the actual number of processor cores.

For queries that are completed quickly because of a LIMIT, you can set a lower 'max_threads'. For example, if the necessary number of entries are located in every block and max_threads = 8, 8 blocks are retrieved, although it would have been enough to read just one.

The smaller the `max_threads` value, the less memory is consumed.

## max_compress_block_size

The maximum size of blocks of uncompressed data before compressing for writing to a table. By default, 1,048,576 (1 MiB). If the size is reduced, the compression rate is significantly reduced, the compression and decompression speed increases slightly due to cache locality, and memory consumption is reduced. There usually isn't any reason to change this setting.

Don't confuse blocks for compression (a chunk of memory consisting of bytes) and blocks for query processing (a set of rows from a table).

## min_compress_block_size

For [MergeTree](../../operations/table_engines/mergetree.md)" tables. In order to reduce latency when processing queries, a block is compressed when writing the next mark if its size is at least 'min_compress_block_size'. By default, 65,536.

The actual size of the block, if the uncompressed data is less than 'max_compress_block_size', is no less than this value and no less than the volume of data for one mark.

Let's look at an example. Assume that 'index_granularity' was set to 8192 during table creation.

We are writing a UInt32-type column (4 bytes per value). When writing 8192 rows, the total will be 32 KB of data. Since min_compress_block_size = 65,536, a compressed block will be formed for every two marks.

We are writing a URL column with the String type (average size of 60 bytes per value). When writing 8192 rows, the average will be slightly less than 500 KB of data. Since this is more than 65,536, a compressed block will be formed for each mark. In this case, when reading data from the disk in the range of a single mark, extra data won't be decompressed.

There usually isn't any reason to change this setting.

## max_query_size

The maximum part of a query that can be taken to RAM for parsing with the SQL parser.
The INSERT query also contains data for INSERT that is processed by a separate stream parser (that consumes O(1) RAM), which is not included in this restriction.

The default is 256 KiB.

## interactive_delay

The interval in microseconds for checking whether request execution has been canceled and sending the progress.

By default, 100,000 (check for canceling and send progress ten times per second).

## connect_timeout, receive_timeout, send_timeout

Timeouts in seconds on the socket used for communicating with the client.

By default, 10, 300, 300.

## poll_interval

Lock in a wait loop for the specified number of seconds.

By default, 10.

## max_distributed_connections

The maximum number of simultaneous connections with remote servers for distributed processing of a single query to a single Distributed table. We recommend setting a value no less than the number of servers in the cluster.

By default, 1024.

The following parameters are only used when creating Distributed tables (and when launching a server), so there is no reason to change them at runtime.

## distributed_connections_pool_size

The maximum number of simultaneous connections with remote servers for distributed processing of all queries to a single Distributed table. We recommend setting a value no less than the number of servers in the cluster.

By default, 1024.

## connect_timeout_with_failover_ms

The timeout in milliseconds for connecting to a remote server for a Distributed table engine, if the 'shard' and 'replica' sections are used in the cluster definition.
If unsuccessful, several attempts are made to connect to various replicas.

By default, 50.

## connections_with_failover_max_tries

The maximum number of connection attempts with each replica, for the Distributed table engine.

By default, 3.

## extremes

Whether to count extreme values (the minimums and maximums in columns of a query result). Accepts 0 or 1. By default, 0 (disabled).
For more information, see the section "Extreme values".

## use_uncompressed_cache {#setting-use_uncompressed_cache}

Whether to use a cache of uncompressed blocks. Accepts 0 or 1. By default, 1 (enabled).
The uncompressed cache (only for tables in the MergeTree family) allows significantly reducing latency and increasing throughput when working with a large number of short queries. Enable this setting for users who send frequent short requests. Also pay attention to the [uncompressed_cache_size](../server_settings/settings.md#server-settings-uncompressed_cache_size) configuration parameter (only set in the config file) – the size of uncompressed cache blocks. By default, it is 8 GiB. The uncompressed cache is filled in as needed; the least-used data is automatically deleted.

For queries that read at least a somewhat large volume of data (one million rows or more), the uncompressed cache is disabled automatically in order to save space for truly small queries. So you can keep the 'use_uncompressed_cache' setting always set to 1.

## replace_running_query

When using the HTTP interface, the 'query_id' parameter can be passed. This is any string that serves as the query identifier.
If a query from the same user with the same 'query_id' already exists at this time, the behavior depends on the 'replace_running_query' parameter.

`0` (default) – Throw an exception (don't allow the query to run if a query with the same 'query_id' is already running).

`1` – Cancel the old query and start running the new one.

Yandex.Metrica uses this parameter set to 1 for implementing suggestions for segmentation conditions. After entering the next character, if the old query hasn't finished yet, it should be canceled.

## schema

This parameter is useful when you are using formats that require a schema definition, such as [Cap'n Proto](https://capnproto.org/). The value depends on the format.


## stream_flush_interval_ms

Works for tables with streaming in the case of a timeout, or when a thread generates [max_insert_block_size](#settings-max_insert_block_size) rows.

The default value is 7500.

The smaller the value, the more often data is flushed into the table. Setting the value too low leads to poor performance.


## load_balancing

Which replicas (among healthy replicas) to preferably send a query to (on the first attempt) for distributed processing.

### random (default)

The number of errors is counted for each replica. The query is sent to the replica with the fewest errors, and if there are several of these, to any one of them.
Disadvantages: Server proximity is not accounted for; if the replicas have different data, you will also get different data.

### nearest_hostname

The number of errors is counted for each replica. Every 5 minutes, the number of errors is integrally divided by 2. Thus, the number of errors is calculated for a recent time with exponential smoothing. If there is one replica with a minimal number of errors (i.e. errors occurred recently on the other replicas), the query is sent to it. If there are multiple replicas with the same minimal number of errors, the query is sent to the replica with a host name that is most similar to the server's host name in the config file (for the number of different characters in identical positions, up to the minimum length of both host names).

For instance, example01-01-1 and example01-01-2.yandex.ru are different in one position, while example01-01-1 and example01-02-2 differ in two places.
This method might seem a little stupid, but it doesn't use external data about network topology, and it doesn't compare IP addresses, which would be complicated for our IPv6 addresses.

Thus, if there are equivalent replicas, the closest one by name is preferred.
We can also assume that when sending a query to the same server, in the absence of failures, a distributed query will also go to the same servers. So even if different data is placed on the replicas, the query will return mostly the same results.

### in_order

Replicas are accessed in the same order as they are specified. The number of errors does not matter.
This method is appropriate when you know exactly which replica is preferable.

## totals_mode

How to calculate TOTALS when HAVING is present, as well as when max_rows_to_group_by and group_by_overflow_mode = 'any' are present.
See the section "WITH TOTALS modifier".

## totals_auto_threshold

The threshold for `totals_mode = 'auto'`.
See the section "WITH TOTALS modifier".

## max_parallel_replicas

The maximum number of replicas for each shard when executing a query.
For consistency (to get different parts of the same data split), this option only works when the sampling key is set.
Replica lag is not controlled.

## compile

Enable compilation of queries. By default, 0 (disabled).

Compilation is only used for part of the query-processing pipeline: for the first stage of aggregation (GROUP BY).
If this portion of the pipeline was compiled, the query may run faster due to deployment of short cycles and inlining aggregate function calls. The maximum performance improvement (up to four times faster in rare cases) is seen for queries with multiple simple aggregate functions. Typically, the performance gain is insignificant. In very rare cases, it may slow down query execution.

## min_count_to_compile

How many times to potentially use a compiled chunk of code before running compilation. By default, 3.
If the value is zero, then compilation runs synchronously and the query waits for the end of the compilation process before continuing execution. This can be used for testing; otherwise, use values ​​starting with 1. Compilation normally takes about 5-10 seconds.
If the value is 1 or more, compilation occurs asynchronously in a separate thread. The result will be used as soon as it is ready, including by queries that are currently running.

Compiled code is required for each different combination of aggregate functions used in the query and the type of keys in the GROUP BY clause.
The results of compilation are saved in the build directory in the form of .so files. There is no restriction on the number of compilation results, since they don't use very much space. Old results will be used after server restarts, except in the case of a server upgrade – in this case, the old results are deleted.

## input_format_skip_unknown_fields

If the value is true, running INSERT skips input data from columns with unknown names. Otherwise, this situation will generate an exception.
It works for JSONEachRow and TSKV formats.

## output_format_json_quote_64bit_integers

If the value is true, integers appear in quotes when using JSON\* Int64 and UInt64 formats (for compatibility with most JavaScript implementations); otherwise, integers are output without the quotes.

## format_csv_delimiter {#settings-format_csv_delimiter}

The character interpreted as a delimiter in the CSV data. By default, the delimiter is `,`.

## join_use_nulls

Affects the behavior of [JOIN](../../query_language/select.md).

With `join_use_nulls=1,` `JOIN` behaves like in standard SQL, i.e. if empty cells appear when merging, the type of the corresponding field is converted to [Nullable](../../data_types/nullable.md#data_type-nullable), and empty cells are filled with [NULL](../../query_language/syntax.md).

## insert_quorum

Enables quorum writes.

  - If `insert_quorum < 2`, the quorum writes are disabled.
  - If `insert_quorum >= 2`, the quorum writes are enabled.

The default value is 0.

**Quorum writes**

`INSERT` succeeds only when ClickHouse manages to correctly write data to the `insert_quorum` of replicas during the `insert_quorum_timeout`. If for any reason the number of replicas with successful writes does not reach the `insert_quorum`, the write is considered failed and ClickHouse will delete the inserted block from all the replicas where data has already been written.

All the replicas in the quorum are consistent, i.e., they contain data from all previous `INSERT` queries. The `INSERT` sequence is linearized.

When reading the data written from the `insert_quorum`, you can use the [select_sequential_consistency](#select-sequential-consistency) option.

**ClickHouse generates an exception**

- If the number of available replicas at the time of the query is less than the `insert_quorum`.
- At an attempt to write data when the previous block has not yet been inserted in the `insert_quorum` of replicas. This situation may occur if the user tries to perform an `INSERT` before the previous one with the `insert_quorum` is completed.

**See also the following parameters:**

- [insert_quorum_timeout](#insert-quorum-timeout)
- [select_sequential_consistency](#select-sequential-consistency)


## insert_quorum_timeout

Quorum write timeout in seconds. If the timeout has passed and no write has taken place yet, ClickHouse will generate an exception and the client must repeat the query to write the same block to the same or any other replica.

By default, 60 seconds.

**See also the following parameters:**

- [insert_quorum](#insert-quorum)
- [select_sequential_consistency](#select-sequential-consistency)


## select_sequential_consistency

Enables/disables sequential consistency for `SELECT` queries:

- 0 — disabled. The default value is 0.
- 1 — enabled.

When sequential consistency is enabled, ClickHouse allows the client to execute the `SELECT` query only for those replicas that contain data from all previous `INSERT` queries executed with `insert_quorum`. If the client refers to a partial replica, ClickHouse will generate an exception. The SELECT query will not include data that has not yet been written to the quorum of replicas.

See also the following parameters:

- [insert_quorum](#insert-quorum)
- [insert_quorum_timeout](#insert-quorum-timeout)


[Original article](https://clickhouse.yandex/docs/en/operations/settings/settings/) <!--hide-->
