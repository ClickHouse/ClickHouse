# Settings

<a name="settings-distributed_product_mode"></a>

## distributed_product_mode

Changes the behavior of [distributed subqueries](../../query_language/select.md#queries-distributed-subrequests), i.e. in cases when the query contains the product of distributed tables.

ClickHouse applies the configuration if the subqueries on any level have a distributed table that exists on the local server and has more than one shard.

Restrictions:

- Only applied for IN and JOIN subqueries.
- Used only if a distributed table is used in the FROM clause.
- Not used for a table-valued [ remote](../../query_language/table_functions/remote.md#table_functions-remote) function.

The possible values ​​are:

<a name="settings-settings-fallback_to_stale_replicas_for_distributed_queries"></a>

## fallback_to_stale_replicas_for_distributed_queries

Forces a query to an out-of-date replica if updated data is not available. See "[Replication](../../operations/table_engines/replication.md#table_engines-replication)".

ClickHouse selects the most relevant from the outdated replicas of the table.

Used when performing ` SELECT`  from a distributed table that points to replicated tables.

By default, 1 (enabled).

<a name="settings-settings-force_index_by_date"></a>

## force_index_by_date

Disables query execution if the index can't be used by date.

Works with tables in the MergeTree family.

If `force_index_by_date=1`,  ClickHouse checks whether the query has a date key condition that can be used for restricting data ranges. If there is no suitable condition, it throws an exception. However, it does not check whether the condition actually reduces the amount of data to read. For example, the condition `Date != ' 2000-01-01 '` is acceptable even when it matches all the data in the table (i.e., running the query requires a full scan). For more information about ranges of data in MergeTree tables, see "[MergeTree](../../operations/table_engines/mergetree.md#table_engines-mergetree)".

<a name="settings-settings-force_primary_key"></a>

## force_primary_key

Disables query execution if indexing by the primary key is not possible.

Works with tables in the MergeTree family.

If `force_primary_key=1`,  ClickHouse checks to see if the query has a primary key condition that can be used for restricting data ranges. If there is no suitable condition, it throws an exception. However, it does not check whether the condition actually reduces the amount of data to read. For more information about data ranges in MergeTree tables, see "[MergeTree](../../operations/table_engines/mergetree.md#table_engines-mergetree)".

<a name="settings_settings_fsync_metadata"></a>

## fsync_metadata

Enable or disable fsync when writing .sql files. By default, it is enabled.

It makes sense to disable it if the server has millions of tiny table chunks that are constantly being created and destroyed.

## input_format_allow_errors_num

Sets the maximum number of acceptable errors when reading from text formats (CSV, TSV, etc.).

The default value is 0.

Always pair it with `input_format_allow_errors_ratio`. To skip errors, both settings must be greater than 0.

If an error occurred while reading rows but the error counter is still less than `input_format_allow_errors_num`, ClickHouse ignores the row and moves on to the next one.

If `input_format_allow_errors_num`is exceeded, ClickHouse throws an exception.

## input_format_allow_errors_ratio

Sets the maximum percentage of errors allowed when reading from text formats (CSV, TSV, etc.).
The percentage of errors is set as a floating-point number between 0 and 1.

The default value is 0.

Always pair it with `input_format_allow_errors_num`. To skip errors, both settings must be greater than 0.

If an error occurred while reading rows but the error counter is still less than `input_format_allow_errors_ratio`, ClickHouse ignores the row and moves on to the next one.

If `input_format_allow_errors_ratio` is exceeded, ClickHouse throws an exception.

## max_block_size

In ClickHouse, data is processed by blocks (sets of column parts). The internal processing cycles for a single block are efficient enough, but there are noticeable expenditures on each block. `max_block_size` is a recommendation for what size of block (in number of rows) to load from tables. The block size shouldn't be too small, so that the expenditures on each block are still noticeable, but not too large, so that the query with LIMIT that is completed after the first block is processed quickly, so that too much memory isn't consumed when extracting a large number of columns in multiple threads, and so that at least some cache locality is preserved.

By default, 65,536.

Blocks the size of `max_block_size` are not always loaded from the table. If it is obvious that less data needs to be retrieved, a smaller block is processed.

## preferred_block_size_bytes

Used for the same purpose as `max_block_size`, but it sets the recommended block size in bytes by adapting it to the number of rows in the block.
However, the block size cannot be more than `max_block_size` rows.
Disabled by default (set to 0). It only works when reading from MergeTree engines.

<a name="settings_settings-log_queries"></a>

## log_queries

Setting up query the logging.

Queries sent to ClickHouse with this setup are logged according to the rules in the [query_log](../server_settings/settings.md#server_settings-query_log) server configuration parameter.

**Example**:

    log_queries=1

<a name="settings-settings-max_insert_block_size"></a>

## max_insert_block_size

The size of blocks to form for insertion into a table.
This setting only applies in cases when the server forms the blocks.
For example, for an INSERT via the HTTP interface, the server parses the data format and forms blocks of the specified size.
But when using clickhouse-client, the client parses the data itself, and the 'max_insert_block_size' setting on the server doesn't affect the size of the inserted blocks.
The setting also doesn't have a purpose when using INSERT SELECT, since data is inserted using the same blocks that are formed after SELECT.

By default, it is 1,048,576.

This is slightly more than `max_block_size`. The reason for this is because certain table engines (`*MergeTree`) form a data part on the disk for each inserted block, which is a fairly large entity. Similarly, `*MergeTree` tables sort data during insertion, and a large enough block size allows sorting more data in RAM.

<a name="settings_settings_max_replica_delay_for_distributed_queries"></a>

## max_replica_delay_for_distributed_queries

Disables lagging replicas for distributed queries. See "[Replication](../../operations/table_engines/replication.md#table_engines-replication)".

Sets the time in seconds. If a replica lags more than the set value, this replica is not used.

Default value: 0 (off).

Used when performing ` SELECT`  from a distributed table that points to replicated tables.

## max_threads

The maximum number of query processing threads

- excluding threads for retrieving data from remote servers (see the 'max_distributed_connections' parameter).

This parameter applies to threads that perform the same stages of the query processing pipeline in parallel.
For example, if reading from a table, evaluating expressions with functions, filtering with WHERE and pre-aggregating for GROUP BY can all be done in parallel using at least 'max_threads' number of threads, then 'max_threads' are used.

By default, 8.

If less than one SELECT query is normally run on a server at a time, set this parameter to a value slightly less than the actual number of processor cores.

For queries that are completed quickly because of a LIMIT, you can set a lower 'max_threads'. For example, if the necessary number of entries are located in every block and max_threads = 8, 8 blocks are retrieved, although it would have been enough to read just one.

The smaller the `max_threads` value, the less memory is consumed.

## max_compress_block_size

The maximum size of blocks of uncompressed data before compressing for writing to a table. By default, 1,048,576 (1 MiB). If the size is reduced, the compression rate is significantly reduced, the compression and decompression speed increases slightly due to cache locality, and memory consumption is reduced. There usually isn't any reason to change this setting.

Don't confuse blocks for compression (a chunk of memory consisting of bytes) and blocks for query processing (a set of rows from a table).

## min_compress_block_size

For [MergeTree](../../operations/table_engines/mergetree.md#table_engines-mergetree)" tables. In order to reduce latency when processing queries, a block is compressed when writing the next mark if its size is at least 'min_compress_block_size'. By default, 65,536.

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

## connect_timeout

## receive_timeout

## send_timeout

Timeouts in seconds on the socket used for communicating with the client.

By default, 10, 300, 300.

## poll_interval

Lock in a wait loop for the specified number of seconds.

By default, 10.

## max_distributed_connections

The maximum number of simultaneous connections with remote servers for distributed processing of a single query to a single Distributed table. We recommend setting a value no less than the number of servers in the cluster.

By default, 100.

The following parameters are only used when creating Distributed tables (and when launching a server), so there is no reason to change them at runtime.

## distributed_connections_pool_size

The maximum number of simultaneous connections with remote servers for distributed processing of all queries to a single Distributed table. We recommend setting a value no less than the number of servers in the cluster.

By default, 128.

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

<a name="settings-use_uncompressed_cache"></a>

## use_uncompressed_cache

Whether to use a cache of uncompressed blocks. Accepts 0 or 1. By default, 0 (disabled).
The uncompressed cache (only for tables in the MergeTree family) allows significantly reducing latency and increasing throughput when working with a large number of short queries. Enable this setting for users who send frequent short requests. Also pay attention to the 'uncompressed_cache_size' configuration parameter (only set in the config file) – the size of uncompressed cache blocks. By default, it is 8 GiB. The uncompressed cache is filled in as needed; the least-used data is automatically deleted.

For queries that read at least a somewhat large volume of data (one million rows or more), the uncompressed cache is disabled automatically in order to save space for truly small queries. So you can keep the 'use_uncompressed_cache' setting always set to 1.

## replace_running_query

When using the HTTP interface, the 'query_id' parameter can be passed. This is any string that serves as the query identifier.
If a query from the same user with the same 'query_id' already exists at this time, the behavior depends on the 'replace_running_query' parameter.

`0` (default) – Throw an exception (don't allow the query to run if a query with the same 'query_id' is already running).

`1` – Cancel the old query and start running the new one.

Yandex.Metrica uses this parameter set to 1 for implementing suggestions for segmentation conditions. After entering the next character, if the old query hasn't finished yet, it should be canceled.

## schema

This parameter is useful when you are using formats that require a schema definition, such as [Cap'n Proto](https://capnproto.org/). The value depends on the format.

<a name="settings-settings_stream_flush_interval_ms"></a>

## stream_flush_interval_ms

Works for tables with streaming in the case of a timeout, or when a thread generates[max_insert_block_size](#settings-settings-max_insert_block_size) rows.

The default value is 7500.

The smaller the value, the more often data is flushed into the table. Setting the value too low leads to poor performance.

<a name="settings-load_balancing"></a>

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

The threshold for ` totals_mode = 'auto'`.
See the section "WITH TOTALS modifier".

## default_sample

Floating-point number from 0 to 1. By default, 1.
Allows you to set the default sampling ratio for all SELECT queries.
(For tables that do not support sampling, it throws an exception.)
If set to 1, sampling is not performed by default.

## max_parallel_replicas

The maximum number of replicas for each shard when executing a query.
For consistency (to get different parts of the same data split), this option only works when the sampling key is set.
Replica lag is not controlled.

## compile

Enable compilation of queries. By default, 0 (disabled).

Compilation is only used for part of the query-processing pipeline: for the first stage of aggregation (GROUP BY).
If this portion of the pipeline was compiled, the query may run faster due to  deployment of short cycles and inlining aggregate function calls. The maximum performance improvement (up to four times faster in rare cases) is seen for queries with multiple simple aggregate functions. Typically, the performance gain is insignificant. In very rare cases, it may slow down query execution.

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

If the value is true, integers appear in quotes when using JSON\* Int64 and UInt64 formats  (for compatibility with most JavaScript implementations); otherwise, integers are output without the quotes.

<a name="format_csv_delimiter"></a>

## format_csv_delimiter

The character to be considered as a delimiter in CSV data. By default, `,`.
