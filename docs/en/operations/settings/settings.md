# Settings {#settings}

## distributed\_product\_mode {#distributed-product-mode}

Changes the behavior of [distributed subqueries](../../query_language/select.md).

ClickHouse applies this setting when the query contains the product of distributed tables, i.e. when the query for a distributed table contains a non-GLOBAL subquery for the distributed table.

Restrictions:

-   Only applied for IN and JOIN subqueries.
-   Only if the FROM section uses a distributed table containing more than one shard.
-   If the subquery concerns a distributed table containing more than one shard.
-   Not used for a table-valued [remote](../../query_language/table_functions/remote.md) function.

Possible values:

-   `deny` — Default value. Prohibits using these types of subqueries (returns the “Double-distributed in/JOIN subqueries is denied” exception).
-   `local` — Replaces the database and table in the subquery with local ones for the destination server (shard), leaving the normal `IN`/`JOIN.`
-   `global` — Replaces the `IN`/`JOIN` query with `GLOBAL IN`/`GLOBAL JOIN.`
-   `allow` — Allows the use of these types of subqueries.

## enable\_optimize\_predicate\_expression {#enable-optimize-predicate-expression}

Turns on predicate pushdown in `SELECT` queries.

Predicate pushdown may significantly reduce network traffic for distributed queries.

Possible values:

-   0 — Disabled.
-   1 — Enabled.

Default value: 1.

Usage

Consider the following queries:

1.  `SELECT count() FROM test_table WHERE date = '2018-10-10'`
2.  `SELECT count() FROM (SELECT * FROM test_table) WHERE date = '2018-10-10'`

If `enable_optimize_predicate_expression = 1`, then the execution time of these queries is equal because ClickHouse applies `WHERE` to the subquery when processing it.

If `enable_optimize_predicate_expression = 0`, then the execution time of the second query is much longer, because the `WHERE` clause applies to all the data after the subquery finishes.

## fallback\_to\_stale\_replicas\_for\_distributed\_queries {#settings-fallback-to-stale-replicas-for-distributed-queries}

Forces a query to an out-of-date replica if updated data is not available. See [Replication](../table_engines/replication.md).

ClickHouse selects the most relevant from the outdated replicas of the table.

Used when performing `SELECT` from a distributed table that points to replicated tables.

By default, 1 (enabled).

## force\_index\_by\_date {#settings-force-index-by-date}

Disables query execution if the index can’t be used by date.

Works with tables in the MergeTree family.

If `force_index_by_date=1`, ClickHouse checks whether the query has a date key condition that can be used for restricting data ranges. If there is no suitable condition, it throws an exception. However, it does not check whether the condition reduces the amount of data to read. For example, the condition `Date != ' 2000-01-01 '` is acceptable even when it matches all the data in the table (i.e., running the query requires a full scan). For more information about ranges of data in MergeTree tables, see [MergeTree](../table_engines/mergetree.md).

## force\_primary\_key {#force-primary-key}

Disables query execution if indexing by the primary key is not possible.

Works with tables in the MergeTree family.

If `force_primary_key=1`, ClickHouse checks to see if the query has a primary key condition that can be used for restricting data ranges. If there is no suitable condition, it throws an exception. However, it does not check whether the condition reduces the amount of data to read. For more information about data ranges in MergeTree tables, see [MergeTree](../table_engines/mergetree.md).

## format\_schema {#format-schema}

This parameter is useful when you are using formats that require a schema definition, such as [Cap’n Proto](https://capnproto.org/) or [Protobuf](https://developers.google.com/protocol-buffers/). The value depends on the format.

## fsync\_metadata {#fsync-metadata}

Enables or disables [fsync](http://pubs.opengroup.org/onlinepubs/9699919799/functions/fsync.html) when writing `.sql` files. Enabled by default.

It makes sense to disable it if the server has millions of tiny tables that are constantly being created and destroyed.

## enable\_http\_compression {#settings-enable-http-compression}

Enables or disables data compression in the response to an HTTP request.

For more information, read the [HTTP interface description](../../interfaces/http.md).

Possible values:

-   0 — Disabled.
-   1 — Enabled.

Default value: 0.

## http\_zlib\_compression\_level {#settings-http-zlib-compression-level}

Sets the level of data compression in the response to an HTTP request if [enable\_http\_compression = 1](#settings-enable_http_compression).

Possible values: Numbers from 1 to 9.

Default value: 3.

## http\_native\_compression\_disable\_checksumming\_on\_decompress {#settings-http-native-compression-disable-checksumming-on-decompress}

Enables or disables checksum verification when decompressing the HTTP POST data from the client. Used only for ClickHouse native compression format (not used with `gzip` or `deflate`).

For more information, read the [HTTP interface description](../../interfaces/http.md).

Possible values:

-   0 — Disabled.
-   1 — Enabled.

Default value: 0.

## send\_progress\_in\_http\_headers {#settings-send-progress-in-http-headers}

Enables or disables `X-ClickHouse-Progress` HTTP response headers in `clickhouse-server` responses.

For more information, read the [HTTP interface description](../../interfaces/http.md).

Possible values:

-   0 — Disabled.
-   1 — Enabled.

Default value: 0.

## max\_http\_get\_redirects {#setting-max-http-get-redirects}

Limits the maximum number of HTTP GET redirect hops for [URL](../table_engines/url.md)-engine tables. The setting applies to both types of tables: those created by the [CREATE TABLE](../../query_language/create/#create-table-query) query and by the [url](../../query_language/table_functions/url.md) table function.

Possible values:

-   Any positive integer number of hops.
-   0 — No hops allowed.

Default value: 0.

## input\_format\_allow\_errors\_num {#settings-input-format-allow-errors-num}

Sets the maximum number of acceptable errors when reading from text formats (CSV, TSV, etc.).

The default value is 0.

Always pair it with `input_format_allow_errors_ratio`.

If an error occurred while reading rows but the error counter is still less than `input_format_allow_errors_num`, ClickHouse ignores the row and moves on to the next one.

If both `input_format_allow_errors_num` and `input_format_allow_errors_ratio` are exceeded, ClickHouse throws an exception.

## input\_format\_allow\_errors\_ratio {#settings-input-format-allow-errors-ratio}

Sets the maximum percentage of errors allowed when reading from text formats (CSV, TSV, etc.).
The percentage of errors is set as a floating-point number between 0 and 1.

The default value is 0.

Always pair it with `input_format_allow_errors_num`.

If an error occurred while reading rows but the error counter is still less than `input_format_allow_errors_ratio`, ClickHouse ignores the row and moves on to the next one.

If both `input_format_allow_errors_num` and `input_format_allow_errors_ratio` are exceeded, ClickHouse throws an exception.

## input\_format\_values\_interpret\_expressions {#settings-input-format-values-interpret-expressions}

Enables or disables the full SQL parser if the fast stream parser can’t parse the data. This setting is used only for the [Values](../../interfaces/formats.md#data-format-values) format at the data insertion. For more information about syntax parsing, see the [Syntax](../../query_language/syntax.md) section.

Possible values:

-   0 — Disabled.

    In this case, you must provide formatted data. See the [Formats](../../interfaces/formats.md) section.

-   1 — Enabled.

    In this case, you can use an SQL expression as a value, but data insertion is much slower this way. If you insert only formatted data, then ClickHouse behaves as if the setting value is 0.

Default value: 1.

Example of Use

Insert the [DateTime](../../data_types/datetime.md) type value with the different settings.

``` sql
SET input_format_values_interpret_expressions = 0;
INSERT INTO datetime_t VALUES (now())
```

``` text
Exception on client:
Code: 27. DB::Exception: Cannot parse input: expected ) before: now()): (at row 1)
```

``` sql
SET input_format_values_interpret_expressions = 1;
INSERT INTO datetime_t VALUES (now())
```

``` text
Ok.
```

The last query is equivalent to the following:

``` sql
SET input_format_values_interpret_expressions = 0;
INSERT INTO datetime_t SELECT now()
```

``` text
Ok.
```

## input\_format\_values\_deduce\_templates\_of\_expressions {#settings-input-format-values-deduce-templates-of-expressions}

Enables or disables template deduction for an SQL expressions in [Values](../../interfaces/formats.md#data-format-values) format. It allows to parse and interpret expressions in `Values` much faster if expressions in consecutive rows have the same structure. ClickHouse will try to deduce template of an expression, parse the following rows using this template and evaluate the expression on a batch of successfully parsed rows. For the following query:

``` sql
INSERT INTO test VALUES (lower('Hello')), (lower('world')), (lower('INSERT')), (upper('Values')), ...
```

-   if `input_format_values_interpret_expressions=1` and `format_values_deduce_templates_of_expressions=0` expressions will be interpreted separately for each row (this is very slow for large number of rows)
-   if `input_format_values_interpret_expressions=0` and `format_values_deduce_templates_of_expressions=1` expressions in the first, second and third rows will be parsed using template `lower(String)` and interpreted together, expression is the forth row will be parsed with another template (`upper(String)`)
-   if `input_format_values_interpret_expressions=1` and `format_values_deduce_templates_of_expressions=1` - the same as in previous case, but also allows fallback to interpreting expressions separately if it’s not possible to deduce template.

Enabled by default.

## input\_format\_values\_accurate\_types\_of\_literals {#settings-input-format-values-accurate-types-of-literals}

This setting is used only when `input_format_values_deduce_templates_of_expressions = 1`. It can happen, that expressions for some column have the same structure, but contain numeric literals of different types, e.g

``` sql
(..., abs(0), ...),             -- UInt64 literal
(..., abs(3.141592654), ...),   -- Float64 literal
(..., abs(-1), ...),            -- Int64 literal
```

When this setting is enabled, ClickHouse will check the actual type of literal and will use an expression template of the corresponding type. In some cases, it may significantly slow down expression evaluation in `Values`.
When disabled, ClickHouse may use more general type for some literals (e.g. `Float64` or `Int64` instead of `UInt64` for `42`), but it may cause overflow and precision issues.
Enabled by default.

## input\_format\_defaults\_for\_omitted\_fields {#session-settings-input-format-defaults-for-omitted-fields}

When performing `INSERT` queries, replace omitted input column values with default values of the respective columns. This option only applies to [JSONEachRow](../../interfaces/formats.md#jsoneachrow), [CSV](../../interfaces/formats.md#csv) and [TabSeparated](../../interfaces/formats.md#tabseparated) formats.

!!! note "Note"
    When this option is enabled, extended table metadata are sent from server to client. It consumes additional computing resources on the server and can reduce performance.

Possible values:

-   0 — Disabled.
-   1 — Enabled.

Default value: 1.

## input\_format\_tsv\_empty\_as\_default {#settings-input-format-tsv-empty-as-default}

When enabled, replace empty input fields in TSV with default values. For complex default expressions `input_format_defaults_for_omitted_fields` must be enabled too.

Disabled by default.

## input\_format\_null\_as\_default {#settings-input-format-null-as-default}

Enables or disables using default values if input data contain `NULL`, but data type of the corresponding column in not `Nullable(T)` (for text input formats).

## input\_format\_skip\_unknown\_fields {#settings-input-format-skip-unknown-fields}

Enables or disables skipping insertion of extra data.

When writing data, ClickHouse throws an exception if input data contain columns that do not exist in the target table. If skipping is enabled, ClickHouse doesn’t insert extra data and doesn’t throw an exception.

Supported formats:

-   [JSONEachRow](../../interfaces/formats.md#jsoneachrow)
-   [CSVWithNames](../../interfaces/formats.md#csvwithnames)
-   [TabSeparatedWithNames](../../interfaces/formats.md#tabseparatedwithnames)
-   [TSKV](../../interfaces/formats.md#tskv)

Possible values:

-   0 — Disabled.
-   1 — Enabled.

Default value: 0.

## input\_format\_import\_nested\_json {#settings-input-format-import-nested-json}

Enables or disables the insertion of JSON data with nested objects.

Supported formats:

-   [JSONEachRow](../../interfaces/formats.md#jsoneachrow)

Possible values:

-   0 — Disabled.
-   1 — Enabled.

Default value: 0.

See also:

-   [Usage of Nested Structures](../../interfaces/formats.md#jsoneachrow-nested) with the `JSONEachRow` format.

## input\_format\_with\_names\_use\_header {#settings-input-format-with-names-use-header}

Enables or disables checking the column order when inserting data.

To improve insert performance, we recommend disabling this check if you are sure that the column order of the input data is the same as in the target table.

Supported formats:

-   [CSVWithNames](../../interfaces/formats.md#csvwithnames)
-   [TabSeparatedWithNames](../../interfaces/formats.md#tabseparatedwithnames)

Possible values:

-   0 — Disabled.
-   1 — Enabled.

Default value: 1.

## date\_time\_input\_format {#settings-date-time-input-format}

Allows choosing a parser of the text representation of date and time.

The setting doesn’t apply to [date and time functions](../../query_language/functions/date_time_functions.md).

Possible values:

-   `'best_effort'` — Enables extended parsing.

    ClickHouse can parse the basic `YYYY-MM-DD HH:MM:SS` format and all [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) date and time formats. For example, `'2018-06-08T01:02:03.000Z'`.

-   `'basic'` — Use basic parser.

    ClickHouse can parse only the basic `YYYY-MM-DD HH:MM:SS` format. For example, `'2019-08-20 10:18:56'`.

Default value: `'basic'`.

See also:

-   [DateTime data type.](../../data_types/datetime.md)
-   [Functions for working with dates and times.](../../query_language/functions/date_time_functions.md)

## join\_default\_strictness {#settings-join-default-strictness}

Sets default strictness for [JOIN clauses](../../query_language/select.md#select-join).

Possible values:

-   `ALL` — If the right table has several matching rows, ClickHouse creates a [Cartesian product](https://en.wikipedia.org/wiki/Cartesian_product) from matching rows. This is the normal `JOIN` behaviour from standard SQL.
-   `ANY` — If the right table has several matching rows, only the first one found is joined. If the right table has only one matching row, the results of `ANY` and `ALL` are the same.
-   `ASOF` — For joining sequences with an uncertain match.
-   `Empty string` — If `ALL` or `ANY` is not specified in the query, ClickHouse throws an exception.

Default value: `ALL`.

## join\_any\_take\_last\_row {#settings-join-any-take-last-row}

Changes behaviour of join operations with `ANY` strictness.

!!! warning "Attention"
    This setting applies only for `JOIN` operations with [Join](../table_engines/join.md) engine tables.

Possible values:

-   0 — If the right table has more than one matching row, only the first one found is joined.
-   1 — If the right table has more than one matching row, only the last one found is joined.

Default value: 0.

See also:

-   [JOIN clause](../../query_language/select.md#select-join)
-   [Join table engine](../table_engines/join.md)
-   [join\_default\_strictness](#settings-join_default_strictness)

## join\_use\_nulls {#join-use-nulls}

Sets the type of [JOIN](../../query_language/select.md) behavior. When merging tables, empty cells may appear. ClickHouse fills them differently based on this setting.

Possible values:

-   0 — The empty cells are filled with the default value of the corresponding field type.
-   1 — `JOIN` behaves the same way as in standard SQL. The type of the corresponding field is converted to [Nullable](../../data_types/nullable.md#data_type-nullable), and empty cells are filled with [NULL](../../query_language/syntax.md).

Default value: 0.

## max\_block\_size {#setting-max-block-size}

In ClickHouse, data is processed by blocks (sets of column parts). The internal processing cycles for a single block are efficient enough, but there are noticeable expenditures on each block. The `max_block_size` setting is a recommendation for what size of the block (in a count of rows) to load from tables. The block size shouldn’t be too small, so that the expenditures on each block are still noticeable, but not too large so that the query with LIMIT that is completed after the first block is processed quickly. The goal is to avoid consuming too much memory when extracting a large number of columns in multiple threads and to preserve at least some cache locality.

Default value: 65,536.

Blocks the size of `max_block_size` are not always loaded from the table. If it is obvious that less data needs to be retrieved, a smaller block is processed.

## preferred\_block\_size\_bytes {#preferred-block-size-bytes}

Used for the same purpose as `max_block_size`, but it sets the recommended block size in bytes by adapting it to the number of rows in the block.
However, the block size cannot be more than `max_block_size` rows.
By default: 1,000,000. It only works when reading from MergeTree engines.

## merge\_tree\_min\_rows\_for\_concurrent\_read {#setting-merge-tree-min-rows-for-concurrent-read}

If the number of rows to be read from a file of a [MergeTree](../table_engines/mergetree.md) table exceeds `merge_tree_min_rows_for_concurrent_read` then ClickHouse tries to perform a concurrent reading from this file on several threads.

Possible values:

-   Any positive integer.

Default value: 163840.

## merge\_tree\_min\_bytes\_for\_concurrent\_read {#setting-merge-tree-min-bytes-for-concurrent-read}

If the number of bytes to read from one file of a [MergeTree](../table_engines/mergetree.md)-engine table exceeds `merge_tree_min_bytes_for_concurrent_read`, then ClickHouse tries to concurrently read from this file in several threads.

Possible value:

-   Any positive integer.

Default value: 251658240.

## merge\_tree\_min\_rows\_for\_seek {#setting-merge-tree-min-rows-for-seek}

If the distance between two data blocks to be read in one file is less than `merge_tree_min_rows_for_seek` rows, then ClickHouse does not seek through the file but reads the data sequentially.

Possible values:

-   Any positive integer.

Default value: 0.

## merge\_tree\_min\_bytes\_for\_seek {#setting-merge-tree-min-bytes-for-seek}

If the distance between two data blocks to be read in one file is less than `merge_tree_min_bytes_for_seek` bytes, then ClickHouse sequentially reads a range of file that contains both blocks, thus avoiding extra seek.

Possible values:

-   Any positive integer.

Default value: 0.

## merge\_tree\_coarse\_index\_granularity {#setting-merge-tree-coarse-index-granularity}

When searching for data, ClickHouse checks the data marks in the index file. If ClickHouse finds that required keys are in some range, it divides this range into `merge_tree_coarse_index_granularity` subranges and searches the required keys there recursively.

Possible values:

-   Any positive even integer.

Default value: 8.

## merge\_tree\_max\_rows\_to\_use\_cache {#setting-merge-tree-max-rows-to-use-cache}

If ClickHouse should read more than `merge_tree_max_rows_to_use_cache` rows in one query, it doesn’t use the cache of uncompressed blocks.

The cache of uncompressed blocks stores data extracted for queries. ClickHouse uses this cache to speed up responses to repeated small queries. This setting protects the cache from trashing by queries that read a large amount of data. The [uncompressed\_cache\_size](../server_settings/settings.md#server-settings-uncompressed_cache_size) server setting defines the size of the cache of uncompressed blocks.

Possible values:

-   Any positive integer.

Default value: 128 ✕ 8192.

## merge\_tree\_max\_bytes\_to\_use\_cache {#setting-merge-tree-max-bytes-to-use-cache}

If ClickHouse should read more than `merge_tree_max_bytes_to_use_cache` bytes in one query, it doesn’t use the cache of uncompressed blocks.

The cache of uncompressed blocks stores data extracted for queries. ClickHouse uses this cache to speed up responses to repeated small queries. This setting protects the cache from trashing by queries that read a large amount of data. The [uncompressed\_cache\_size](../server_settings/settings.md#server-settings-uncompressed_cache_size) server setting defines the size of the cache of uncompressed blocks.

Possible value:

-   Any positive integer.

Default value: 2013265920.

## min\_bytes\_to\_use\_direct\_io {#settings-min-bytes-to-use-direct-io}

The minimum data volume required for using direct I/O access to the storage disk.

ClickHouse uses this setting when reading data from tables. If the total storage volume of all the data to be read exceeds `min_bytes_to_use_direct_io` bytes, then ClickHouse reads the data from the storage disk with the `O_DIRECT` option.

Possible values:

-   0 — Direct I/O is disabled.
-   Positive integer.

Default value: 0.

## log\_queries {#settings-log-queries}

Setting up query logging.

Queries sent to ClickHouse with this setup are logged according to the rules in the [query\_log](../server_settings/settings.md#server_settings-query-log) server configuration parameter.

Example:

``` text
log_queries=1
```

## log\_query\_threads {#settings-log-query-threads}

Setting up query threads logging.

Queries’ threads runned by ClickHouse with this setup are logged according to the rules in the [query\_thread\_log](../server_settings/settings.md#server_settings-query-thread-log) server configuration parameter.

Example:

``` text
log_query_threads=1
```

## max\_insert\_block\_size {#settings-max-insert-block-size}

The size of blocks to form for insertion into a table.
This setting only applies in cases when the server forms the blocks.
For example, for an INSERT via the HTTP interface, the server parses the data format and forms blocks of the specified size.
But when using clickhouse-client, the client parses the data itself, and the ‘max\_insert\_block\_size’ setting on the server doesn’t affect the size of the inserted blocks.
The setting also doesn’t have a purpose when using INSERT SELECT, since data is inserted using the same blocks that are formed after SELECT.

Default value: 1,048,576.

The default is slightly more than `max_block_size`. The reason for this is because certain table engines (`*MergeTree`) form a data part on the disk for each inserted block, which is a fairly large entity. Similarly, `*MergeTree` tables sort data during insertion and a large enough block size allow sorting more data in RAM.

## max\_replica\_delay\_for\_distributed\_queries {#settings-max-replica-delay-for-distributed-queries}

Disables lagging replicas for distributed queries. See [Replication](../../operations/table_engines/replication.md).

Sets the time in seconds. If a replica lags more than the set value, this replica is not used.

Default value: 300.

Used when performing `SELECT` from a distributed table that points to replicated tables.

## max\_threads {#settings-max-threads}

The maximum number of query processing threads, excluding threads for retrieving data from remote servers (see the ‘max\_distributed\_connections’ parameter).

This parameter applies to threads that perform the same stages of the query processing pipeline in parallel.
For example, when reading from a table, if it is possible to evaluate expressions with functions, filter with WHERE and pre-aggregate for GROUP BY in parallel using at least ‘max\_threads’ number of threads, then ‘max\_threads’ are used.

Default value: the number of physical CPU cores.

If less than one SELECT query is normally run on a server at a time, set this parameter to a value slightly less than the actual number of processor cores.

For queries that are completed quickly because of a LIMIT, you can set a lower ‘max\_threads’. For example, if the necessary number of entries are located in every block and max\_threads = 8, then 8 blocks are retrieved, although it would have been enough to read just one.

The smaller the `max_threads` value, the less memory is consumed.

## max\_insert\_threads {#settings-max-insert-threads}

The maximum number of threads to execute the `INSERT SELECT` query.

Possible values:

-   0 (or 1) — `INSERT SELECT` no parallel execution.
-   Positive integer. Bigger than 1.

Default value: 0.

Parallel `INSERT SELECT` has effect only if the `SELECT` part is executed in parallel, see [max\_threads](#settings-max_threads) setting.
Higher values will lead to higher memory usage.

## max\_compress\_block\_size {#max-compress-block-size}

The maximum size of blocks of uncompressed data before compressing for writing to a table. By default, 1,048,576 (1 MiB). If the size is reduced, the compression rate is significantly reduced, the compression and decompression speed increases slightly due to cache locality, and memory consumption is reduced. There usually isn’t any reason to change this setting.

Don’t confuse blocks for compression (a chunk of memory consisting of bytes) with blocks for query processing (a set of rows from a table).

## min\_compress\_block\_size {#min-compress-block-size}

For [MergeTree](../table_engines/mergetree.md)" tables. In order to reduce latency when processing queries, a block is compressed when writing the next mark if its size is at least ‘min\_compress\_block\_size’. By default, 65,536.

The actual size of the block, if the uncompressed data is less than ‘max\_compress\_block\_size’, is no less than this value and no less than the volume of data for one mark.

Let’s look at an example. Assume that ‘index\_granularity’ was set to 8192 during table creation.

We are writing a UInt32-type column (4 bytes per value). When writing 8192 rows, the total will be 32 KB of data. Since min\_compress\_block\_size = 65,536, a compressed block will be formed for every two marks.

We are writing a URL column with the String type (average size of 60 bytes per value). When writing 8192 rows, the average will be slightly less than 500 KB of data. Since this is more than 65,536, a compressed block will be formed for each mark. In this case, when reading data from the disk in the range of a single mark, extra data won’t be decompressed.

There usually isn’t any reason to change this setting.

## max\_query\_size {#settings-max-query-size}

The maximum part of a query that can be taken to RAM for parsing with the SQL parser.
The INSERT query also contains data for INSERT that is processed by a separate stream parser (that consumes O(1) RAM), which is not included in this restriction.

Default value: 256 KiB.

## interactive\_delay {#interactive-delay}

The interval in microseconds for checking whether request execution has been cancelled and sending the progress.

Default value: 100,000 (checks for cancelling and sends the progress ten times per second).

## connect\_timeout, receive\_timeout, send\_timeout {#connect-timeout-receive-timeout-send-timeout}

Timeouts in seconds on the socket used for communicating with the client.

Default value: 10, 300, 300.

## cancel\_http\_readonly\_queries\_on\_client\_close {#cancel-http-readonly-queries-on-client-close}

Cancels HTTP read-only queries (e.g. SELECT) when a client closes the connection without waiting for the response.

Default value: 0

## poll\_interval {#poll-interval}

Lock in a wait loop for the specified number of seconds.

Default value: 10.

## max\_distributed\_connections {#max-distributed-connections}

The maximum number of simultaneous connections with remote servers for distributed processing of a single query to a single Distributed table. We recommend setting a value no less than the number of servers in the cluster.

Default value: 1024.

The following parameters are only used when creating Distributed tables (and when launching a server), so there is no reason to change them at runtime.

## distributed\_connections\_pool\_size {#distributed-connections-pool-size}

The maximum number of simultaneous connections with remote servers for distributed processing of all queries to a single Distributed table. We recommend setting a value no less than the number of servers in the cluster.

Default value: 1024.

## connect\_timeout\_with\_failover\_ms {#connect-timeout-with-failover-ms}

The timeout in milliseconds for connecting to a remote server for a Distributed table engine, if the ‘shard’ and ‘replica’ sections are used in the cluster definition.
If unsuccessful, several attempts are made to connect to various replicas.

Default value: 50.

## connections\_with\_failover\_max\_tries {#connections-with-failover-max-tries}

The maximum number of connection attempts with each replica for the Distributed table engine.

Default value: 3.

## extremes {#extremes}

Whether to count extreme values (the minimums and maximums in columns of a query result). Accepts 0 or 1. By default, 0 (disabled).
For more information, see the section “Extreme values”.

## use\_uncompressed\_cache {#setting-use-uncompressed-cache}

Whether to use a cache of uncompressed blocks. Accepts 0 or 1. By default, 0 (disabled).
Using the uncompressed cache (only for tables in the MergeTree family) can significantly reduce latency and increase throughput when working with a large number of short queries. Enable this setting for users who send frequent short requests. Also pay attention to the [uncompressed\_cache\_size](../server_settings/settings.md#server-settings-uncompressed_cache_size) configuration parameter (only set in the config file) – the size of uncompressed cache blocks. By default, it is 8 GiB. The uncompressed cache is filled in as needed and the least-used data is automatically deleted.

For queries that read at least a somewhat large volume of data (one million rows or more), the uncompressed cache is disabled automatically to save space for truly small queries. This means that you can keep the ‘use\_uncompressed\_cache’ setting always set to 1.

## replace\_running\_query {#replace-running-query}

When using the HTTP interface, the ‘query\_id’ parameter can be passed. This is any string that serves as the query identifier.
If a query from the same user with the same ‘query\_id’ already exists at this time, the behaviour depends on the ‘replace\_running\_query’ parameter.

`0` (default) – Throw an exception (don’t allow the query to run if a query with the same ‘query\_id’ is already running).

`1` – Cancel the old query and start running the new one.

Yandex.Metrica uses this parameter set to 1 for implementing suggestions for segmentation conditions. After entering the next character, if the old query hasn’t finished yet, it should be cancelled.

## stream\_flush\_interval\_ms {#stream-flush-interval-ms}

Works for tables with streaming in the case of a timeout, or when a thread generates [max\_insert\_block\_size](#settings-max_insert_block_size) rows.

The default value is 7500.

The smaller the value, the more often data is flushed into the table. Setting the value too low leads to poor performance.

## load\_balancing {#settings-load-balancing}

Specifies the algorithm of replicas selection that is used for distributed query processing.

ClickHouse supports the following algorithms of choosing replicas:

-   [Random](#load_balancing-random) (by default)
-   [Nearest hostname](#load_balancing-nearest_hostname)
-   [In order](#load_balancing-in_order)
-   [First or random](#load_balancing-first_or_random)

### Random (by default) {#load-balancing-random}

``` sql
load_balancing = random
```

The number of errors is counted for each replica. The query is sent to the replica with the fewest errors, and if there are several of these, to anyone of them.
Disadvantages: Server proximity is not accounted for; if the replicas have different data, you will also get different data.

### Nearest Hostname {#load-balancing-nearest-hostname}

``` sql
load_balancing = nearest_hostname
```

The number of errors is counted for each replica. Every 5 minutes, the number of errors is integrally divided by 2. Thus, the number of errors is calculated for a recent time with exponential smoothing. If there is one replica with a minimal number of errors (i.e. errors occurred recently on the other replicas), the query is sent to it. If there are multiple replicas with the same minimal number of errors, the query is sent to the replica with a hostname that is most similar to the server’s hostname in the config file (for the number of different characters in identical positions, up to the minimum length of both hostnames).

For instance, example01-01-1 and example01-01-2.yandex.ru are different in one position, while example01-01-1 and example01-02-2 differ in two places.
This method might seem primitive, but it doesn’t require external data about network topology, and it doesn’t compare IP addresses, which would be complicated for our IPv6 addresses.

Thus, if there are equivalent replicas, the closest one by name is preferred.
We can also assume that when sending a query to the same server, in the absence of failures, a distributed query will also go to the same servers. So even if different data is placed on the replicas, the query will return mostly the same results.

### In Order {#load-balancing-in-order}

``` sql
load_balancing = in_order
```

Replicas with the same number of errors are accessed in the same order as they are specified in the configuration.
This method is appropriate when you know exactly which replica is preferable.

### First or Random {#load-balancing-first-or-random}

``` sql
load_balancing = first_or_random
```

This algorithm chooses the first replica in the set or a random replica if the first is unavailable. It’s effective in cross-replication topology setups, but useless in other configurations.

The `first_or_random` algorithm solves the problem of the `in_order` algorithm. With `in_order`, if one replica goes down, the next one gets a double load while the remaining replicas handle the usual amount of traffic. When using the `first_or_random` algorithm, the load is evenly distributed among replicas that are still available.

## prefer\_localhost\_replica {#settings-prefer-localhost-replica}

Enables/disables preferable using the localhost replica when processing distributed queries.

Possible values:

-   1 — ClickHouse always sends a query to the localhost replica if it exists.
-   0 — ClickHouse uses the balancing strategy specified by the [load\_balancing](#settings-load_balancing) setting.

Default value: 1.

!!! warning "Warning"
    Disable this setting if you use [max\_parallel\_replicas](#settings-max_parallel_replicas).

## totals\_mode {#totals-mode}

How to calculate TOTALS when HAVING is present, as well as when max\_rows\_to\_group\_by and group\_by\_overflow\_mode = ‘any’ are present.
See the section “WITH TOTALS modifier”.

## totals\_auto\_threshold {#totals-auto-threshold}

The threshold for `totals_mode = 'auto'`.
See the section “WITH TOTALS modifier”.

## max\_parallel\_replicas {#settings-max-parallel-replicas}

The maximum number of replicas for each shard when executing a query.
For consistency (to get different parts of the same data split), this option only works when the sampling key is set.
Replica lag is not controlled.

## compile {#compile}

Enable compilation of queries. By default, 0 (disabled).

The compilation is only used for part of the query-processing pipeline: for the first stage of aggregation (GROUP BY).
If this portion of the pipeline was compiled, the query may run faster due to deployment of short cycles and inlining aggregate function calls. The maximum performance improvement (up to four times faster in rare cases) is seen for queries with multiple simple aggregate functions. Typically, the performance gain is insignificant. In very rare cases, it may slow down query execution.

## min\_count\_to\_compile {#min-count-to-compile}

How many times to potentially use a compiled chunk of code before running compilation. By default, 3.
For testing, the value can be set to 0: compilation runs synchronously and the query waits for the end of the compilation process before continuing execution. For all other cases, use values ​​starting with 1. Compilation normally takes about 5-10 seconds.
If the value is 1 or more, compilation occurs asynchronously in a separate thread. The result will be used as soon as it is ready, including queries that are currently running.

Compiled code is required for each different combination of aggregate functions used in the query and the type of keys in the GROUP BY clause.
The results of the compilation are saved in the build directory in the form of .so files. There is no restriction on the number of compilation results since they don’t use very much space. Old results will be used after server restarts, except in the case of a server upgrade – in this case, the old results are deleted.

## output\_format\_json\_quote\_64bit\_integers {#session-settings-output-format-json-quote-64bit-integers}

If the value is true, integers appear in quotes when using JSON\* Int64 and UInt64 formats (for compatibility with most JavaScript implementations); otherwise, integers are output without the quotes.

## format\_csv\_delimiter {#settings-format-csv-delimiter}

The character interpreted as a delimiter in the CSV data. By default, the delimiter is `,`.

## input\_format\_csv\_unquoted\_null\_literal\_as\_null {#settings-input-format-csv-unquoted-null-literal-as-null}

For CSV input format enables or disables parsing of unquoted `NULL` as literal (synonym for `\N`).

## output\_format\_csv\_crlf\_end\_of\_line {#settings-output-format-csv-crlf-end-of-line}

Use DOS/Windows-style line separator (CRLF) in CSV instead of Unix style (LF).

## output\_format\_tsv\_crlf\_end\_of\_line {#settings-output-format-tsv-crlf-end-of-line}

Use DOC/Windows-style line separator (CRLF) in TSV instead of Unix style (LF).

## insert\_quorum {#settings-insert-quorum}

Enables the quorum writes.

-   If `insert_quorum < 2`, the quorum writes are disabled.
-   If `insert_quorum >= 2`, the quorum writes are enabled.

Default value: 0.

Quorum writes

`INSERT` succeeds only when ClickHouse manages to correctly write data to the `insert_quorum` of replicas during the `insert_quorum_timeout`. If for any reason the number of replicas with successful writes does not reach the `insert_quorum`, the write is considered failed and ClickHouse will delete the inserted block from all the replicas where data has already been written.

All the replicas in the quorum are consistent, i.e., they contain data from all previous `INSERT` queries. The `INSERT` sequence is linearized.

When reading the data written from the `insert_quorum`, you can use the [select\_sequential\_consistency](#settings-select_sequential_consistency) option.

ClickHouse generates an exception

-   If the number of available replicas at the time of the query is less than the `insert_quorum`.
-   At an attempt to write data when the previous block has not yet been inserted in the `insert_quorum` of replicas. This situation may occur if the user tries to perform an `INSERT` before the previous one with the `insert_quorum` is completed.

See also:

-   [insert\_quorum\_timeout](#settings-insert_quorum_timeout)
-   [select\_sequential\_consistency](#settings-select_sequential_consistency)

## insert\_quorum\_timeout {#settings-insert-quorum-timeout}

Write to quorum timeout in seconds. If the timeout has passed and no write has taken place yet, ClickHouse will generate an exception and the client must repeat the query to write the same block to the same or any other replica.

Default value: 60 seconds.

See also:

-   [insert\_quorum](#settings-insert_quorum)
-   [select\_sequential\_consistency](#settings-select_sequential_consistency)

## select\_sequential\_consistency {#settings-select-sequential-consistency}

Enables or disables sequential consistency for `SELECT` queries:

Possible values:

-   0 — Disabled.
-   1 — Enabled.

Default value: 0.

Usage

When sequential consistency is enabled, ClickHouse allows the client to execute the `SELECT` query only for those replicas that contain data from all previous `INSERT` queries executed with `insert_quorum`. If the client refers to a partial replica, ClickHouse will generate an exception. The SELECT query will not include data that has not yet been written to the quorum of replicas.

See also:

-   [insert\_quorum](#settings-insert_quorum)
-   [insert\_quorum\_timeout](#settings-insert_quorum_timeout)

## insert\_deduplicate {#settings-insert-deduplicate}

Enables or disables block deduplication of `INSERT` (for Replicated\* tables).

Possible values:

-   0 — Disabled.
-   1 — Enabled.

Default value: 1.

By default, blocks inserted into replicated tables by the `INSERT` statement are deduplicated (see \[Data Replication\] (../ table\_engines/replication.md).

## deduplicate\_blocks\_in\_dependent\_materialized\_views {#settings-deduplicate-blocks-in-dependent-materialized-views}

Enables or disables the deduplication check for materialized views that receive data from Replicated\* tables.

Possible values:

      0 — Disabled.
      1 — Enabled.

Default value: 0.

Usage

By default, deduplication is not performed for materialized views but is done upstream, in the source table.
If an INSERTed block is skipped due to deduplication in the source table, there will be no insertion into attached materialized views. This behaviour exists to enable insertion of highly aggregated data into materialized views, for cases where inserted blocks are the same after materialized view aggregation but derived from different INSERTs into the source table.
At the same time, this behaviour “breaks” `INSERT` idempotency. If an `INSERT` into the main table was successful and `INSERT` into a materialized view failed (e.g. because of communication failure with Zookeeper) a client will get an error and can retry the operation. However, the materialized view won’t receive the second insert because it will be discarded by deduplication in the main (source) table. The setting `deduplicate_blocks_in_dependent_materialized_views` allows for changing this behaviour. On retry, a materialized view will receive the repeat insert and will perform deduplication check by itself,
ignoring check result for the source table, and will insert rows lost because of the first failure.

## max\_network\_bytes {#settings-max-network-bytes}

Limits the data volume (in bytes) that is received or transmitted over the network when executing a query. This setting applies to every individual query.

Possible values:

-   Positive integer.
-   0 — Data volume control is disabled.

Default value: 0.

## max\_network\_bandwidth {#settings-max-network-bandwidth}

Limits the speed of the data exchange over the network in bytes per second. This setting applies to every query.

Possible values:

-   Positive integer.
-   0 — Bandwidth control is disabled.

Default value: 0.

## max\_network\_bandwidth\_for\_user {#settings-max-network-bandwidth-for-user}

Limits the speed of the data exchange over the network in bytes per second. This setting applies to all concurrently running queries performed by a single user.

Possible values:

-   Positive integer.
-   0 — Control of the data speed is disabled.

Default value: 0.

## max\_network\_bandwidth\_for\_all\_users {#settings-max-network-bandwidth-for-all-users}

Limits the speed that data is exchanged at over the network in bytes per second. This setting applies to all concurrently running queries on the server.

Possible values:

-   Positive integer.
-   0 — Control of the data speed is disabled.

Default value: 0.

## count\_distinct\_implementation {#settings-count-distinct-implementation}

Specifies which of the `uniq*` functions should be used to perform the [COUNT(DISTINCT …)](../../query_language/agg_functions/reference.md#agg_function-count) construction.

Possible values:

-   [uniq](../../query_language/agg_functions/reference.md#agg_function-uniq)
-   [uniqCombined](../../query_language/agg_functions/reference.md#agg_function-uniqcombined)
-   [uniqCombined64](../../query_language/agg_functions/reference.md#agg_function-uniqcombined64)
-   [uniqHLL12](../../query_language/agg_functions/reference.md#agg_function-uniqhll12)
-   [uniqExact](../../query_language/agg_functions/reference.md#agg_function-uniqexact)

Default value: `uniqExact`.

## skip\_unavailable\_shards {#settings-skip-unavailable-shards}

Enables or disables silently skipping of unavailable shards.

Shard is considered unavailable if all its replicas are unavailable. A replica is unavailable in the following cases:

-   ClickHouse can’t connect to replica for any reason.

    When connecting to a replica, ClickHouse performs several attempts. If all these attempts fail, the replica is considered unavailable.

-   Replica can’t be resolved through DNS.

    If replica’s hostname can’t be resolved through DNS, it can indicate the following situations:

    -   Replica’s host has no DNS record. It can occur in systems with dynamic DNS, for example, [Kubernetes](https://kubernetes.io), where nodes can be unresolvable during downtime, and this is not an error.

    -   Configuration error. ClickHouse configuration file contains a wrong hostname.

Possible values:

-   1 — skipping enabled.

    If a shard is unavailable, ClickHouse returns a result based on partial data and doesn’t report node availability issues.

-   0 — skipping disabled.

    If a shard is unavailable, ClickHouse throws an exception.

Default value: 0.

## optimize\_skip\_unused\_shards {#settings-optimize-skip-unused-shards}

Enables or disables skipping of unused shards for SELECT queries that have sharding key condition in PREWHERE/WHERE (assumes that the data is distributed by sharding key, otherwise do nothing).

Default value: 0

## force\_optimize\_skip\_unused\_shards {#settings-force-optimize-skip-unused-shards}

Enables or disables query execution if [`optimize_skip_unused_shards`](#settings-optimize_skip_unused_shards) enabled and skipping of unused shards is not possible. If the skipping is not possible and the setting is enabled exception will be thrown.

Possible values:

-   0 - Disabled (do not throws)
-   1 - Disable query execution only if the table has sharding key
-   2 - Disable query execution regardless sharding key is defined for the table

Default value: 0

## optimize\_throw\_if\_noop {#setting-optimize-throw-if-noop}

Enables or disables throwing an exception if an [OPTIMIZE](../../query_language/misc.md#misc_operations-optimize) query didn’t perform a merge.

By default, `OPTIMIZE` returns successfully even if it didn’t do anything. This setting lets you differentiate these situations and get the reason in an exception message.

Possible values:

-   1 — Throwing an exception is enabled.
-   0 — Throwing an exception is disabled.

Default value: 0.

## distributed\_replica\_error\_half\_life {#settings-distributed-replica-error-half-life}

-   Type: seconds
-   Default value: 60 seconds

Controls how fast errors in distributed tables are zeroed. If a replica is unavailable for some time, accumulates 5 errors, and distributed\_replica\_error\_half\_life is set to 1 second, then the replica is considered normal 3 seconds after last error.

See also:

-   [Table engine Distributed](../../operations/table_engines/distributed.md)
-   [distributed\_replica\_error\_cap](#settings-distributed_replica_error_cap)

## distributed\_replica\_error\_cap {#settings-distributed-replica-error-cap}

-   Type: unsigned int
-   Default value: 1000

Error count of each replica is capped at this value, preventing a single replica from accumulating too many errors.

See also:

-   [Table engine Distributed](../../operations/table_engines/distributed.md)
-   [distributed\_replica\_error\_half\_life](#settings-distributed_replica_error_half_life)

## distributed\_directory\_monitor\_sleep\_time\_ms {#distributed-directory-monitor-sleep-time-ms}

Base interval for the [Distributed](../table_engines/distributed.md) table engine to send data. The actual interval grows exponentially in the event of errors.

Possible values:

-   A positive integer number of milliseconds.

Default value: 100 milliseconds.

## distributed\_directory\_monitor\_max\_sleep\_time\_ms {#distributed-directory-monitor-max-sleep-time-ms}

Maximum interval for the [Distributed](../table_engines/distributed.md) table engine to send data. Limits exponential growth of the interval set in the [distributed\_directory\_monitor\_sleep\_time\_ms](#distributed_directory_monitor_sleep_time_ms) setting.

Possible values:

-   A positive integer number of milliseconds.

Default value: 30000 milliseconds (30 seconds).

## distributed\_directory\_monitor\_batch\_inserts {#distributed-directory-monitor-batch-inserts}

Enables/disables sending of inserted data in batches.

When batch sending is enabled, the [Distributed](../table_engines/distributed.md) table engine tries to send multiple files of inserted data in one operation instead of sending them separately. Batch sending improves cluster performance by better-utilizing server and network resources.

Possible values:

-   1 — Enabled.
-   0 — Disabled.

Default value: 0.

## os\_thread\_priority {#setting-os-thread-priority}

Sets the priority ([nice](https://en.wikipedia.org/wiki/Nice_(Unix))) for threads that execute queries. The OS scheduler considers this priority when choosing the next thread to run on each available CPU core.

!!! warning "Warning"
    To use this setting, you need to set the `CAP_SYS_NICE` capability. The `clickhouse-server` package sets it up during installation. Some virtual environments don’t allow you to set the `CAP_SYS_NICE` capability. In this case, `clickhouse-server` shows a message about it at the start.

Possible values:

-   You can set values in the range `[-20, 19]`.

Lower values mean higher priority. Threads with low `nice` priority values are executed more frequently than threads with high values. High values are preferable for long-running non-interactive queries because it allows them to quickly give up resources in favour of short interactive queries when they arrive.

Default value: 0.

## query\_profiler\_real\_time\_period\_ns {#query-profiler-real-time-period-ns}

Sets the period for a real clock timer of the [query profiler](../../operations/performance/sampling_query_profiler.md). Real clock timer counts wall-clock time.

Possible values:

-   Positive integer number, in nanoseconds.

    Recommended values:

            - 10000000 (100 times a second) nanoseconds and less for single queries.
            - 1000000000 (once a second) for cluster-wide profiling.

-   0 for turning off the timer.

Type: [UInt64](../../data_types/int_uint.md).

Default value: 1000000000 nanoseconds (once a second).

See also:

-   System table [trace\_log](../system_tables.md#system_tables-trace_log)

## query\_profiler\_cpu\_time\_period\_ns {#query-profiler-cpu-time-period-ns}

Sets the period for a CPU clock timer of the [query profiler](../../operations/performance/sampling_query_profiler.md). This timer counts only CPU time.

Possible values:

-   A positive integer number of nanoseconds.

    Recommended values:

            - 10000000 (100 times a second) nanoseconds and more for single queries.
            - 1000000000 (once a second) for cluster-wide profiling.

-   0 for turning off the timer.

Type: [UInt64](../../data_types/int_uint.md).

Default value: 1000000000 nanoseconds.

See also:

-   System table [trace\_log](../system_tables.md#system_tables-trace_log)

## allow\_introspection\_functions {#settings-allow-introspection-functions}

Enables of disables [introspections functions](../../query_language/functions/introspection.md) for query profiling.

Possible values:

-   1 — Introspection functions enabled.
-   0 — Introspection functions disabled.

Default value: 0.

**See Also**

-   [Sampling Query Profiler](../performance/sampling_query_profiler.md)
-   System table [trace\_log](../system_tables.md#system_tables-trace_log)

## input\_format\_parallel\_parsing {#input-format-parallel-parsing}

-   Type: bool
-   Default value: True

Enable order-preserving parallel parsing of data formats. Supported only for TSV, TKSV, CSV and JSONEachRow formats.

## min\_chunk\_bytes\_for\_parallel\_parsing {#min-chunk-bytes-for-parallel-parsing}

-   Type: unsigned int
-   Default value: 1 MiB

The minimum chunk size in bytes, which each thread will parse in parallel.

## output\_format\_avro\_codec {#settings-output-format-avro-codec}

Sets the compression codec used for output Avro file.

Type: string

Possible values:

-   `null` — No compression
-   `deflate` — Compress with Deflate (zlib)
-   `snappy` — Compress with [Snappy](https://google.github.io/snappy/)

Default value: `snappy` (if available) or `deflate`.

## output\_format\_avro\_sync\_interval {#settings-output-format-avro-sync-interval}

Sets minimum data size (in bytes) between synchronization markers for output Avro file.

Type: unsigned int

Possible values: 32 (32 bytes) - 1073741824 (1 GiB)

Default value: 32768 (32 KiB)

## format\_avro\_schema\_registry\_url {#settings-format-avro-schema-registry-url}

Sets Confluent Schema Registry URL to use with [AvroConfluent](../../interfaces/formats.md#data-format-avro-confluent) format

Type: URL

Default value: Empty

[Original article](https://clickhouse.tech/docs/en/operations/settings/settings/) <!-- hide -->
