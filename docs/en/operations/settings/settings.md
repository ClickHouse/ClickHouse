# Settings {#settings}

## distributed_product_mode {#distributed-product-mode}

Changes the behaviour of [distributed subqueries](../../sql-reference/operators/in.md).

ClickHouse applies this setting when the query contains the product of distributed tables, i.e. when the query for a distributed table contains a non-GLOBAL subquery for the distributed table.

Restrictions:

-   Only applied for IN and JOIN subqueries.
-   Only if the FROM section uses a distributed table containing more than one shard.
-   If the subquery concerns a distributed table containing more than one shard.
-   Not used for a table-valued [remote](../../sql-reference/table-functions/remote.md) function.

Possible values:

-   `deny` — Default value. Prohibits using these types of subqueries (returns the “Double-distributed in/JOIN subqueries is denied” exception).
-   `local` — Replaces the database and table in the subquery with local ones for the destination server (shard), leaving the normal `IN`/`JOIN.`
-   `global` — Replaces the `IN`/`JOIN` query with `GLOBAL IN`/`GLOBAL JOIN.`
-   `allow` — Allows the use of these types of subqueries.

## enable_optimize_predicate_expression {#enable-optimize-predicate-expression}

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

If `enable_optimize_predicate_expression = 0`, then the execution time of the second query is much longer because the `WHERE` clause applies to all the data after the subquery finishes.

## fallback_to_stale_replicas_for_distributed_queries {#settings-fallback_to_stale_replicas_for_distributed_queries}

Forces a query to an out-of-date replica if updated data is not available. See [Replication](../../engines/table-engines/mergetree-family/replication.md).

ClickHouse selects the most relevant from the outdated replicas of the table.

Used when performing `SELECT` from a distributed table that points to replicated tables.

By default, 1 (enabled).

## force_index_by_date {#settings-force_index_by_date}

Disables query execution if the index can’t be used by date.

Works with tables in the MergeTree family.

If `force_index_by_date=1`, ClickHouse checks whether the query has a date key condition that can be used for restricting data ranges. If there is no suitable condition, it throws an exception. However, it does not check whether the condition reduces the amount of data to read. For example, the condition `Date != ' 2000-01-01 '` is acceptable even when it matches all the data in the table (i.e., running the query requires a full scan). For more information about ranges of data in MergeTree tables, see [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md).

## force_primary_key {#force-primary-key}

Disables query execution if indexing by the primary key is not possible.

Works with tables in the MergeTree family.

If `force_primary_key=1`, ClickHouse checks to see if the query has a primary key condition that can be used for restricting data ranges. If there is no suitable condition, it throws an exception. However, it does not check whether the condition reduces the amount of data to read. For more information about data ranges in MergeTree tables, see [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md).

## force_data_skipping_indices {#settings-force_data_skipping_indices}

Disables query execution if passed data skipping indices wasn't used.

Consider the following example:

```sql
CREATE TABLE data
(
    key Int,
    d1 Int,
    d1_null Nullable(Int),
    INDEX d1_idx d1 TYPE minmax GRANULARITY 1,
    INDEX d1_null_idx assumeNotNull(d1_null) TYPE minmax GRANULARITY 1
)
Engine=MergeTree()
ORDER BY key;

SELECT * FROM data_01515;
SELECT * FROM data_01515 SETTINGS force_data_skipping_indices=''; -- query will produce CANNOT_PARSE_TEXT error.
SELECT * FROM data_01515 SETTINGS force_data_skipping_indices='d1_idx'; -- query will produce INDEX_NOT_USED error.
SELECT * FROM data_01515 WHERE d1 = 0 SETTINGS force_data_skipping_indices='d1_idx'; -- Ok.
SELECT * FROM data_01515 WHERE d1 = 0 SETTINGS force_data_skipping_indices='`d1_idx`'; -- Ok (example of full featured parser).
SELECT * FROM data_01515 WHERE d1 = 0 SETTINGS force_data_skipping_indices='`d1_idx`, d1_null_idx'; -- query will produce INDEX_NOT_USED error, since d1_null_idx is not used.
SELECT * FROM data_01515 WHERE d1 = 0 AND assumeNotNull(d1_null) = 0 SETTINGS force_data_skipping_indices='`d1_idx`, d1_null_idx'; -- Ok.
```

Works with tables in the MergeTree family.

## format_schema {#format-schema}

This parameter is useful when you are using formats that require a schema definition, such as [Cap’n Proto](https://capnproto.org/) or [Protobuf](https://developers.google.com/protocol-buffers/). The value depends on the format.

## fsync_metadata {#fsync-metadata}

Enables or disables [fsync](http://pubs.opengroup.org/onlinepubs/9699919799/functions/fsync.html) when writing `.sql` files. Enabled by default.

It makes sense to disable it if the server has millions of tiny tables that are constantly being created and destroyed.

## enable_http_compression {#settings-enable_http_compression}

Enables or disables data compression in the response to an HTTP request.

For more information, read the [HTTP interface description](../../interfaces/http.md).

Possible values:

-   0 — Disabled.
-   1 — Enabled.

Default value: 0.

## http_zlib_compression_level {#settings-http_zlib_compression_level}

Sets the level of data compression in the response to an HTTP request if [enable_http_compression = 1](#settings-enable_http_compression).

Possible values: Numbers from 1 to 9.

Default value: 3.

## http_native_compression_disable_checksumming_on_decompress {#settings-http_native_compression_disable_checksumming_on_decompress}

Enables or disables checksum verification when decompressing the HTTP POST data from the client. Used only for ClickHouse native compression format (not used with `gzip` or `deflate`).

For more information, read the [HTTP interface description](../../interfaces/http.md).

Possible values:

-   0 — Disabled.
-   1 — Enabled.

Default value: 0.

## send_progress_in_http_headers {#settings-send_progress_in_http_headers}

Enables or disables `X-ClickHouse-Progress` HTTP response headers in `clickhouse-server` responses.

For more information, read the [HTTP interface description](../../interfaces/http.md).

Possible values:

-   0 — Disabled.
-   1 — Enabled.

Default value: 0.

## max_http_get_redirects {#setting-max_http_get_redirects}

Limits the maximum number of HTTP GET redirect hops for [URL](../../engines/table-engines/special/url.md)-engine tables. The setting applies to both types of tables: those created by the [CREATE TABLE](../../sql-reference/statements/create/table.md) query and by the [url](../../sql-reference/table-functions/url.md) table function.

Possible values:

-   Any positive integer number of hops.
-   0 — No hops allowed.

Default value: 0.

## input_format_allow_errors_num {#settings-input_format_allow_errors_num}

Sets the maximum number of acceptable errors when reading from text formats (CSV, TSV, etc.).

The default value is 0.

Always pair it with `input_format_allow_errors_ratio`.

If an error occurred while reading rows but the error counter is still less than `input_format_allow_errors_num`, ClickHouse ignores the row and moves on to the next one.

If both `input_format_allow_errors_num` and `input_format_allow_errors_ratio` are exceeded, ClickHouse throws an exception.

## input_format_allow_errors_ratio {#settings-input_format_allow_errors_ratio}

Sets the maximum percentage of errors allowed when reading from text formats (CSV, TSV, etc.).
The percentage of errors is set as a floating-point number between 0 and 1.

The default value is 0.

Always pair it with `input_format_allow_errors_num`.

If an error occurred while reading rows but the error counter is still less than `input_format_allow_errors_ratio`, ClickHouse ignores the row and moves on to the next one.

If both `input_format_allow_errors_num` and `input_format_allow_errors_ratio` are exceeded, ClickHouse throws an exception.

## input_format_values_interpret_expressions {#settings-input_format_values_interpret_expressions}

Enables or disables the full SQL parser if the fast stream parser can’t parse the data. This setting is used only for the [Values](../../interfaces/formats.md#data-format-values) format at the data insertion. For more information about syntax parsing, see the [Syntax](../../sql-reference/syntax.md) section.

Possible values:

-   0 — Disabled.

    In this case, you must provide formatted data. See the [Formats](../../interfaces/formats.md) section.

-   1 — Enabled.

    In this case, you can use an SQL expression as a value, but data insertion is much slower this way. If you insert only formatted data, then ClickHouse behaves as if the setting value is 0.

Default value: 1.

Example of Use

Insert the [DateTime](../../sql-reference/data-types/datetime.md) type value with the different settings.

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

## input_format_values_deduce_templates_of_expressions {#settings-input_format_values_deduce_templates_of_expressions}

Enables or disables template deduction for SQL expressions in [Values](../../interfaces/formats.md#data-format-values) format. It allows parsing and interpreting expressions in `Values` much faster if expressions in consecutive rows have the same structure. ClickHouse tries to deduce the template of an expression, parse the following rows using this template and evaluate the expression on a batch of successfully parsed rows.

Possible values:

-   0 — Disabled.
-   1 — Enabled.

Default value: 1.

For the following query:

``` sql
INSERT INTO test VALUES (lower('Hello')), (lower('world')), (lower('INSERT')), (upper('Values')), ...
```

-   If `input_format_values_interpret_expressions=1` and `format_values_deduce_templates_of_expressions=0`, expressions are interpreted separately for each row (this is very slow for large number of rows).
-   If `input_format_values_interpret_expressions=0` and `format_values_deduce_templates_of_expressions=1`, expressions in the first, second and third rows are parsed using template `lower(String)` and interpreted together, expression in the forth row is parsed with another template (`upper(String)`).
-   If `input_format_values_interpret_expressions=1` and `format_values_deduce_templates_of_expressions=1`, the same as in previous case, but also allows fallback to interpreting expressions separately if it’s not possible to deduce template.

## input_format_values_accurate_types_of_literals {#settings-input-format-values-accurate-types-of-literals}

This setting is used only when `input_format_values_deduce_templates_of_expressions = 1`. Expressions for some column may have the same structure, but contain numeric literals of different types, e.g.

``` sql
(..., abs(0), ...),             -- UInt64 literal
(..., abs(3.141592654), ...),   -- Float64 literal
(..., abs(-1), ...),            -- Int64 literal
```

Possible values:

-   0 — Disabled.

    In this case, ClickHouse may use a more general type for some literals (e.g., `Float64` or `Int64` instead of `UInt64` for `42`), but it may cause overflow and precision issues.

-   1 — Enabled.

    In this case, ClickHouse checks the actual type of literal and uses an expression template of the corresponding type. In some cases, it may significantly slow down expression evaluation in `Values`.

Default value: 1.

## input_format_defaults_for_omitted_fields {#session_settings-input_format_defaults_for_omitted_fields}

When performing `INSERT` queries, replace omitted input column values with default values of the respective columns. This option only applies to [JSONEachRow](../../interfaces/formats.md#jsoneachrow), [CSV](../../interfaces/formats.md#csv) and [TabSeparated](../../interfaces/formats.md#tabseparated) formats.

!!! note "Note"
    When this option is enabled, extended table metadata are sent from server to client. It consumes additional computing resources on the server and can reduce performance.

Possible values:

-   0 — Disabled.
-   1 — Enabled.

Default value: 1.

## input_format_tsv_empty_as_default {#settings-input-format-tsv-empty-as-default}

When enabled, replace empty input fields in TSV with default values. For complex default expressions `input_format_defaults_for_omitted_fields` must be enabled too.

Disabled by default.

## input_format_tsv_enum_as_number {#settings-input_format_tsv_enum_as_number}

Enables or disables parsing enum values as enum ids for TSV input format.

Possible values:

-   0 — Enum values are parsed as values.
-   1 — Enum values are parsed as enum IDs

Default value: 0.

**Example**

Consider the table:

```sql
CREATE TABLE table_with_enum_column_for_tsv_insert (Id Int32,Value Enum('first' = 1, 'second' = 2)) ENGINE=Memory();
```

When the `input_format_tsv_enum_as_number` setting is enabled:  

```sql
SET input_format_tsv_enum_as_number = 1;
INSERT INTO table_with_enum_column_for_tsv_insert FORMAT TSV 102	2;
INSERT INTO table_with_enum_column_for_tsv_insert FORMAT TSV 103	1;
SELECT * FROM table_with_enum_column_for_tsv_insert;
```

Result:

```text
┌──Id─┬─Value──┐
│ 102 │ second │
└─────┴────────┘
┌──Id─┬─Value──┐
│ 103 │ first  │
└─────┴────────┘
```

When the `input_format_tsv_enum_as_number` setting is disabled, the `INSERT` query:

```sql
SET input_format_tsv_enum_as_number = 0;
INSERT INTO table_with_enum_column_for_tsv_insert FORMAT TSV 102	2;
```

throws an exception.

## input_format_null_as_default {#settings-input-format-null-as-default}

Enables or disables using default values if input data contain `NULL`, but the data type of the corresponding column in not `Nullable(T)` (for text input formats).

## input_format_skip_unknown_fields {#settings-input-format-skip-unknown-fields}

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

## input_format_import_nested_json {#settings-input_format_import_nested_json}

Enables or disables the insertion of JSON data with nested objects.

Supported formats:

-   [JSONEachRow](../../interfaces/formats.md#jsoneachrow)

Possible values:

-   0 — Disabled.
-   1 — Enabled.

Default value: 0.

See also:

-   [Usage of Nested Structures](../../interfaces/formats.md#jsoneachrow-nested) with the `JSONEachRow` format.

## input_format_with_names_use_header {#settings-input-format-with-names-use-header}

Enables or disables checking the column order when inserting data.

To improve insert performance, we recommend disabling this check if you are sure that the column order of the input data is the same as in the target table.

Supported formats:

-   [CSVWithNames](../../interfaces/formats.md#csvwithnames)
-   [TabSeparatedWithNames](../../interfaces/formats.md#tabseparatedwithnames)

Possible values:

-   0 — Disabled.
-   1 — Enabled.

Default value: 1.

## date_time_input_format {#settings-date_time_input_format}

Allows choosing a parser of the text representation of date and time.

The setting doesn’t apply to [date and time functions](../../sql-reference/functions/date-time-functions.md).

Possible values:

-   `'best_effort'` — Enables extended parsing.

    ClickHouse can parse the basic `YYYY-MM-DD HH:MM:SS` format and all [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) date and time formats. For example, `'2018-06-08T01:02:03.000Z'`.

-   `'basic'` — Use basic parser.

    ClickHouse can parse only the basic `YYYY-MM-DD HH:MM:SS` or `YYYY-MM-DD` format. For example, `'2019-08-20 10:18:56'` or `2019-08-20`.

Default value: `'basic'`.

See also:

-   [DateTime data type.](../../sql-reference/data-types/datetime.md)
-   [Functions for working with dates and times.](../../sql-reference/functions/date-time-functions.md)

## date_time_output_format {#settings-date_time_output_format}

Allows choosing different output formats of the text representation of date and time.

Possible values:

-   `'simple'` - Simple output format.

    Clickhouse output date and time `YYYY-MM-DD hh:mm:ss` format. For example, `'2019-08-20 10:18:56'`. Calculation is performed according to the data type's time zone (if present) or server time zone.

-   `'iso'` - ISO output format.

    Clickhouse output date and time in [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) `YYYY-MM-DDThh:mm:ssZ` format. For example, `'2019-08-20T10:18:56Z'`. Note that output is in UTC (`Z` means UTC).

-   `'unix_timestamp'` - Unix timestamp output format.

    Clickhouse output date and time in [Unix timestamp](https://en.wikipedia.org/wiki/Unix_time) format. For example `'1566285536'`.

Default value: `'simple'`.

See also:

-   [DateTime data type.](../../sql-reference/data-types/datetime.md)
-   [Functions for working with dates and times.](../../sql-reference/functions/date-time-functions.md)

## join_default_strictness {#settings-join_default_strictness}

Sets default strictness for [JOIN clauses](../../sql-reference/statements/select/join.md#select-join).

Possible values:

-   `ALL` — If the right table has several matching rows, ClickHouse creates a [Cartesian product](https://en.wikipedia.org/wiki/Cartesian_product) from matching rows. This is the normal `JOIN` behaviour from standard SQL.
-   `ANY` — If the right table has several matching rows, only the first one found is joined. If the right table has only one matching row, the results of `ANY` and `ALL` are the same.
-   `ASOF` — For joining sequences with an uncertain match.
-   `Empty string` — If `ALL` or `ANY` is not specified in the query, ClickHouse throws an exception.

Default value: `ALL`.

## join_any_take_last_row {#settings-join_any_take_last_row}

Changes behaviour of join operations with `ANY` strictness.

!!! warning "Attention"
    This setting applies only for `JOIN` operations with [Join](../../engines/table-engines/special/join.md) engine tables.

Possible values:

-   0 — If the right table has more than one matching row, only the first one found is joined.
-   1 — If the right table has more than one matching row, only the last one found is joined.

Default value: 0.

See also:

-   [JOIN clause](../../sql-reference/statements/select/join.md#select-join)
-   [Join table engine](../../engines/table-engines/special/join.md)
-   [join_default_strictness](#settings-join_default_strictness)

## join_use_nulls {#join_use_nulls}

Sets the type of [JOIN](../../sql-reference/statements/select/join.md) behaviour. When merging tables, empty cells may appear. ClickHouse fills them differently based on this setting.

Possible values:

-   0 — The empty cells are filled with the default value of the corresponding field type.
-   1 — `JOIN` behaves the same way as in standard SQL. The type of the corresponding field is converted to [Nullable](../../sql-reference/data-types/nullable.md#data_type-nullable), and empty cells are filled with [NULL](../../sql-reference/syntax.md).

Default value: 0.

## partial_merge_join_optimizations {#partial_merge_join_optimizations}

Disables optimizations in partial merge join algorithm for [JOIN](../../sql-reference/statements/select/join.md) queries.

By default, this setting enables improvements that could lead to wrong results. If you see suspicious results in your queries, disable optimizations by this setting. Optimizations can be different in different versions of the ClickHouse server.

Possible values:

-   0 — Optimizations disabled.
-   1 — Optimizations enabled.

Default value: 1.

## partial_merge_join_rows_in_right_blocks {#partial_merge_join_rows_in_right_blocks}

Limits sizes of right-hand join data blocks in partial merge join algorithm for [JOIN](../../sql-reference/statements/select/join.md) queries.

ClickHouse server:

1.  Splits right-hand join data into blocks with up to the specified number of rows.
2.  Indexes each block with its minimum and maximum values.
3.  Unloads prepared blocks to disk if it is possible.

Possible values:

-   Any positive integer. Recommended range of values: \[1000, 100000\].

Default value: 65536.

## join_on_disk_max_files_to_merge {#join_on_disk_max_files_to_merge}

Limits the number of files allowed for parallel sorting in MergeJoin operations when they are executed on disk.

The bigger the value of the setting, the more RAM used and the less disk I/O needed.

Possible values:

-   Any positive integer, starting from 2.

Default value: 64.

## any_join_distinct_right_table_keys {#any_join_distinct_right_table_keys}

Enables legacy ClickHouse server behaviour in `ANY INNER|LEFT JOIN` operations.

!!! note "Warning"
    Use this setting only for backward compatibility if your use cases depend on legacy `JOIN` behaviour.

When the legacy behaviour enabled:

-   Results of `t1 ANY LEFT JOIN t2` and `t2 ANY RIGHT JOIN t1` operations are not equal because ClickHouse uses the logic with many-to-one left-to-right table keys mapping.
-   Results of `ANY INNER JOIN` operations contain all rows from the left table like the `SEMI LEFT JOIN` operations do.

When the legacy behaviour disabled:

-   Results of `t1 ANY LEFT JOIN t2` and `t2 ANY RIGHT JOIN t1` operations are equal because ClickHouse uses the logic which provides one-to-many keys mapping in `ANY RIGHT JOIN` operations.
-   Results of `ANY INNER JOIN` operations contain one row per key from both the left and right tables.

Possible values:

-   0 — Legacy behaviour is disabled.
-   1 — Legacy behaviour is enabled.

Default value: 0.

See also:

-   [JOIN strictness](../../sql-reference/statements/select/join.md#join-settings)

## temporary_files_codec {#temporary_files_codec}

Sets compression codec for temporary files used in sorting and joining operations on disk.

Possible values:

-   LZ4 — [LZ4](https://en.wikipedia.org/wiki/LZ4_(compression_algorithm)) compression is applied.
-   NONE — No compression is applied.

Default value: LZ4.

## max_block_size {#setting-max_block_size}

In ClickHouse, data is processed by blocks (sets of column parts). The internal processing cycles for a single block are efficient enough, but there are noticeable expenditures on each block. The `max_block_size` setting is a recommendation for what size of the block (in a count of rows) to load from tables. The block size shouldn’t be too small, so that the expenditures on each block are still noticeable, but not too large so that the query with LIMIT that is completed after the first block is processed quickly. The goal is to avoid consuming too much memory when extracting a large number of columns in multiple threads and to preserve at least some cache locality.

Default value: 65,536.

Blocks the size of `max_block_size` are not always loaded from the table. If it is obvious that less data needs to be retrieved, a smaller block is processed.

## preferred_block_size_bytes {#preferred-block-size-bytes}

Used for the same purpose as `max_block_size`, but it sets the recommended block size in bytes by adapting it to the number of rows in the block.
However, the block size cannot be more than `max_block_size` rows.
By default: 1,000,000. It only works when reading from MergeTree engines.

## merge_tree_min_rows_for_concurrent_read {#setting-merge-tree-min-rows-for-concurrent-read}

If the number of rows to be read from a file of a [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) table exceeds `merge_tree_min_rows_for_concurrent_read` then ClickHouse tries to perform a concurrent reading from this file on several threads.

Possible values:

-   Any positive integer.

Default value: 163840.

## merge_tree_min_bytes_for_concurrent_read {#setting-merge-tree-min-bytes-for-concurrent-read}

If the number of bytes to read from one file of a [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md)-engine table exceeds `merge_tree_min_bytes_for_concurrent_read`, then ClickHouse tries to concurrently read from this file in several threads.

Possible value:

-   Any positive integer.

Default value: 251658240.

## merge_tree_min_rows_for_seek {#setting-merge-tree-min-rows-for-seek}

If the distance between two data blocks to be read in one file is less than `merge_tree_min_rows_for_seek` rows, then ClickHouse does not seek through the file but reads the data sequentially.

Possible values:

-   Any positive integer.

Default value: 0.

## merge_tree_min_bytes_for_seek {#setting-merge-tree-min-bytes-for-seek}

If the distance between two data blocks to be read in one file is less than `merge_tree_min_bytes_for_seek` bytes, then ClickHouse sequentially reads a range of file that contains both blocks, thus avoiding extra seek.

Possible values:

-   Any positive integer.

Default value: 0.

## merge_tree_coarse_index_granularity {#setting-merge-tree-coarse-index-granularity}

When searching for data, ClickHouse checks the data marks in the index file. If ClickHouse finds that required keys are in some range, it divides this range into `merge_tree_coarse_index_granularity` subranges and searches the required keys there recursively.

Possible values:

-   Any positive even integer.

Default value: 8.

## merge_tree_max_rows_to_use_cache {#setting-merge-tree-max-rows-to-use-cache}

If ClickHouse should read more than `merge_tree_max_rows_to_use_cache` rows in one query, it doesn’t use the cache of uncompressed blocks.

The cache of uncompressed blocks stores data extracted for queries. ClickHouse uses this cache to speed up responses to repeated small queries. This setting protects the cache from trashing by queries that read a large amount of data. The [uncompressed_cache_size](../../operations/server-configuration-parameters/settings.md#server-settings-uncompressed_cache_size) server setting defines the size of the cache of uncompressed blocks.

Possible values:

-   Any positive integer.

Default value: 128 ✕ 8192.

## merge_tree_max_bytes_to_use_cache {#setting-merge-tree-max-bytes-to-use-cache}

If ClickHouse should read more than `merge_tree_max_bytes_to_use_cache` bytes in one query, it doesn’t use the cache of uncompressed blocks.

The cache of uncompressed blocks stores data extracted for queries. ClickHouse uses this cache to speed up responses to repeated small queries. This setting protects the cache from trashing by queries that read a large amount of data. The [uncompressed_cache_size](../../operations/server-configuration-parameters/settings.md#server-settings-uncompressed_cache_size) server setting defines the size of the cache of uncompressed blocks.

Possible value:

-   Any positive integer.

Default value: 2013265920.

## min_bytes_to_use_direct_io {#settings-min-bytes-to-use-direct-io}

The minimum data volume required for using direct I/O access to the storage disk.

ClickHouse uses this setting when reading data from tables. If the total storage volume of all the data to be read exceeds `min_bytes_to_use_direct_io` bytes, then ClickHouse reads the data from the storage disk with the `O_DIRECT` option.

Possible values:

-   0 — Direct I/O is disabled.
-   Positive integer.

Default value: 0.

## network_compression_method {#network_compression_method}

Sets the method of data compression that is used for communication between servers and between server and [clickhouse-client](../../interfaces/cli.md).

Possible values:

-   `LZ4` — sets LZ4 compression method.
-   `ZSTD` — sets ZSTD compression method.

Default value: `LZ4`.

**See Also**

-   [network_zstd_compression_level](#network_zstd_compression_level)

## network_zstd_compression_level {#network_zstd_compression_level}

Adjusts the level of ZSTD compression. Used only when [network_compression_method](#network_compression_method) is set to `ZSTD`.

Possible values:

-   Positive integer from 1 to 15.

Default value: `1`.

## log_queries {#settings-log-queries}

Setting up query logging.

Queries sent to ClickHouse with this setup are logged according to the rules in the [query_log](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-query-log) server configuration parameter.

Example:

``` text
log_queries=1
```

## log_queries_min_query_duration_ms {#settings-log-queries-min-query-duration-ms}

Minimal time for the query to run to get to the following tables:

- `system.query_log`
- `system.query_thread_log`

Only the queries with the following type will get to the log:

- `QUERY_FINISH`
- `EXCEPTION_WHILE_PROCESSING`

-   Type: milliseconds
-   Default value: 0 (any query)

## log_queries_min_type {#settings-log-queries-min-type}

`query_log` minimal type to log.

Possible values:
- `QUERY_START` (`=1`)
- `QUERY_FINISH` (`=2`)
- `EXCEPTION_BEFORE_START` (`=3`)
- `EXCEPTION_WHILE_PROCESSING` (`=4`)

Default value: `QUERY_START`.

Can be used to limit which entities will go to `query_log`, say you are interested only in errors, then you can use `EXCEPTION_WHILE_PROCESSING`:

``` text
log_queries_min_type='EXCEPTION_WHILE_PROCESSING'
```

## log_query_threads {#settings-log-query-threads}

Setting up query threads logging.

Queries’ threads runned by ClickHouse with this setup are logged according to the rules in the [query_thread_log](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-query_thread_log) server configuration parameter.

Example:

``` text
log_query_threads=1
```

## max_insert_block_size {#settings-max_insert_block_size}

The size of blocks (in a count of rows) to form for insertion into a table.
This setting only applies in cases when the server forms the blocks.
For example, for an INSERT via the HTTP interface, the server parses the data format and forms blocks of the specified size.
But when using clickhouse-client, the client parses the data itself, and the ‘max_insert_block_size’ setting on the server doesn’t affect the size of the inserted blocks.
The setting also doesn’t have a purpose when using INSERT SELECT, since data is inserted using the same blocks that are formed after SELECT.

Default value: 1,048,576.

The default is slightly more than `max_block_size`. The reason for this is because certain table engines (`*MergeTree`) form a data part on the disk for each inserted block, which is a fairly large entity. Similarly, `*MergeTree` tables sort data during insertion, and a large enough block size allow sorting more data in RAM.

## min_insert_block_size_rows {#min-insert-block-size-rows}

Sets the minimum number of rows in the block which can be inserted into a table by an `INSERT` query. Smaller-sized blocks are squashed into bigger ones.

Possible values:

-   Positive integer.
-   0 — Squashing disabled.

Default value: 1048576.

## min_insert_block_size_bytes {#min-insert-block-size-bytes}

Sets the minimum number of bytes in the block which can be inserted into a table by an `INSERT` query. Smaller-sized blocks are squashed into bigger ones.

Possible values:

-   Positive integer.
-   0 — Squashing disabled.

Default value: 268435456.

## max_replica_delay_for_distributed_queries {#settings-max_replica_delay_for_distributed_queries}

Disables lagging replicas for distributed queries. See [Replication](../../engines/table-engines/mergetree-family/replication.md).

Sets the time in seconds. If a replica lags more than the set value, this replica is not used.

Default value: 300.

Used when performing `SELECT` from a distributed table that points to replicated tables.

## max_threads {#settings-max_threads}

The maximum number of query processing threads, excluding threads for retrieving data from remote servers (see the ‘max_distributed_connections’ parameter).

This parameter applies to threads that perform the same stages of the query processing pipeline in parallel.
For example, when reading from a table, if it is possible to evaluate expressions with functions, filter with WHERE and pre-aggregate for GROUP BY in parallel using at least ‘max_threads’ number of threads, then ‘max_threads’ are used.

Default value: the number of physical CPU cores.

If less than one SELECT query is normally run on a server at a time, set this parameter to a value slightly less than the actual number of processor cores.

For queries that are completed quickly because of a LIMIT, you can set a lower ‘max_threads’. For example, if the necessary number of entries are located in every block and max_threads = 8, then 8 blocks are retrieved, although it would have been enough to read just one.

The smaller the `max_threads` value, the less memory is consumed.

## max_insert_threads {#settings-max-insert-threads}

The maximum number of threads to execute the `INSERT SELECT` query.

Possible values:

-   0 (or 1) — `INSERT SELECT` no parallel execution.
-   Positive integer. Bigger than 1.

Default value: 0.

Parallel `INSERT SELECT` has effect only if the `SELECT` part is executed in parallel, see [max_threads](#settings-max_threads) setting.
Higher values will lead to higher memory usage.

## max_compress_block_size {#max-compress-block-size}

The maximum size of blocks of uncompressed data before compressing for writing to a table. By default, 1,048,576 (1 MiB). If the size is reduced, the compression rate is significantly reduced, the compression and decompression speed increases slightly due to cache locality, and memory consumption is reduced. There usually isn’t any reason to change this setting.

Don’t confuse blocks for compression (a chunk of memory consisting of bytes) with blocks for query processing (a set of rows from a table).

## min_compress_block_size {#min-compress-block-size}

For [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md)" tables. In order to reduce latency when processing queries, a block is compressed when writing the next mark if its size is at least ‘min_compress_block_size’. By default, 65,536.

The actual size of the block, if the uncompressed data is less than ‘max_compress_block_size’, is no less than this value and no less than the volume of data for one mark.

Let’s look at an example. Assume that ‘index_granularity’ was set to 8192 during table creation.

We are writing a UInt32-type column (4 bytes per value). When writing 8192 rows, the total will be 32 KB of data. Since min_compress_block_size = 65,536, a compressed block will be formed for every two marks.

We are writing a URL column with the String type (average size of 60 bytes per value). When writing 8192 rows, the average will be slightly less than 500 KB of data. Since this is more than 65,536, a compressed block will be formed for each mark. In this case, when reading data from the disk in the range of a single mark, extra data won’t be decompressed.

There usually isn’t any reason to change this setting.

## max_query_size {#settings-max_query_size}

The maximum part of a query that can be taken to RAM for parsing with the SQL parser.
The INSERT query also contains data for INSERT that is processed by a separate stream parser (that consumes O(1) RAM), which is not included in this restriction.

Default value: 256 KiB.

## max_parser_depth {#max_parser_depth}

Limits maximum recursion depth in the recursive descent parser. Allows controlling the stack size.

Possible values:

-   Positive integer.
-   0 — Recursion depth is unlimited.

Default value: 1000.

## interactive_delay {#interactive-delay}

The interval in microseconds for checking whether request execution has been cancelled and sending the progress.

Default value: 100,000 (checks for cancelling and sends the progress ten times per second).

## connect_timeout, receive_timeout, send_timeout {#connect-timeout-receive-timeout-send-timeout}

Timeouts in seconds on the socket used for communicating with the client.

Default value: 10, 300, 300.

## cancel_http_readonly_queries_on_client_close {#cancel-http-readonly-queries-on-client-close}

Cancels HTTP read-only queries (e.g. SELECT) when a client closes the connection without waiting for the response.

Default value: 0

## poll_interval {#poll-interval}

Lock in a wait loop for the specified number of seconds.

Default value: 10.

## max_distributed_connections {#max-distributed-connections}

The maximum number of simultaneous connections with remote servers for distributed processing of a single query to a single Distributed table. We recommend setting a value no less than the number of servers in the cluster.

Default value: 1024.

The following parameters are only used when creating Distributed tables (and when launching a server), so there is no reason to change them at runtime.

## distributed_connections_pool_size {#distributed-connections-pool-size}

The maximum number of simultaneous connections with remote servers for distributed processing of all queries to a single Distributed table. We recommend setting a value no less than the number of servers in the cluster.

Default value: 1024.

## connect_timeout_with_failover_ms {#connect-timeout-with-failover-ms}

The timeout in milliseconds for connecting to a remote server for a Distributed table engine, if the ‘shard’ and ‘replica’ sections are used in the cluster definition.
If unsuccessful, several attempts are made to connect to various replicas.

Default value: 50.

## connection_pool_max_wait_ms {#connection-pool-max-wait-ms}

The wait time in milliseconds for a connection when the connection pool is full.

Possible values:

- Positive integer.
- 0 — Infinite timeout.

Default value: 0.

## connections_with_failover_max_tries {#connections-with-failover-max-tries}

The maximum number of connection attempts with each replica for the Distributed table engine.

Default value: 3.

## extremes {#extremes}

Whether to count extreme values (the minimums and maximums in columns of a query result). Accepts 0 or 1. By default, 0 (disabled).
For more information, see the section “Extreme values”.

## kafka_max_wait_ms {#kafka-max-wait-ms}

The wait time in milliseconds for reading messages from [Kafka](../../engines/table-engines/integrations/kafka.md#kafka) before retry.

Possible values:

- Positive integer.
- 0 — Infinite timeout.

Default value: 5000.

See also:

-   [Apache Kafka](https://kafka.apache.org/)

## use_uncompressed_cache {#setting-use_uncompressed_cache}

Whether to use a cache of uncompressed blocks. Accepts 0 or 1. By default, 0 (disabled).
Using the uncompressed cache (only for tables in the MergeTree family) can significantly reduce latency and increase throughput when working with a large number of short queries. Enable this setting for users who send frequent short requests. Also pay attention to the [uncompressed_cache_size](../../operations/server-configuration-parameters/settings.md#server-settings-uncompressed_cache_size) configuration parameter (only set in the config file) – the size of uncompressed cache blocks. By default, it is 8 GiB. The uncompressed cache is filled in as needed and the least-used data is automatically deleted.

For queries that read at least a somewhat large volume of data (one million rows or more), the uncompressed cache is disabled automatically to save space for truly small queries. This means that you can keep the ‘use_uncompressed_cache’ setting always set to 1.

## replace_running_query {#replace-running-query}

When using the HTTP interface, the ‘query_id’ parameter can be passed. This is any string that serves as the query identifier.
If a query from the same user with the same ‘query_id’ already exists at this time, the behaviour depends on the ‘replace_running_query’ parameter.

`0` (default) – Throw an exception (don’t allow the query to run if a query with the same ‘query_id’ is already running).

`1` – Cancel the old query and start running the new one.

Yandex.Metrica uses this parameter set to 1 for implementing suggestions for segmentation conditions. After entering the next character, if the old query hasn’t finished yet, it should be cancelled.

## replace_running_query_max_wait_ms {#replace-running-query-max-wait-ms}

The wait time for running the query with the same `query_id` to finish, when the [replace_running_query](#replace-running-query) setting is active.

Possible values:

- Positive integer.
- 0 — Throwing an exception that does not allow to run a new query if the server already executes a query with the same `query_id`.

Default value: 5000.

## stream_flush_interval_ms {#stream-flush-interval-ms}

Works for tables with streaming in the case of a timeout, or when a thread generates [max_insert_block_size](#settings-max_insert_block_size) rows.

The default value is 7500.

The smaller the value, the more often data is flushed into the table. Setting the value too low leads to poor performance.

## load_balancing {#settings-load_balancing}

Specifies the algorithm of replicas selection that is used for distributed query processing.

ClickHouse supports the following algorithms of choosing replicas:

-   [Random](#load_balancing-random) (by default)
-   [Nearest hostname](#load_balancing-nearest_hostname)
-   [In order](#load_balancing-in_order)
-   [First or random](#load_balancing-first_or_random)
-   [Round robin](#load_balancing-round_robin)

See also:

-   [distributed_replica_max_ignored_errors](#settings-distributed_replica_max_ignored_errors)

### Random (by Default) {#load_balancing-random}

``` sql
load_balancing = random
```

The number of errors is counted for each replica. The query is sent to the replica with the fewest errors, and if there are several of these, to anyone of them.
Disadvantages: Server proximity is not accounted for; if the replicas have different data, you will also get different data.

### Nearest Hostname {#load_balancing-nearest_hostname}

``` sql
load_balancing = nearest_hostname
```

The number of errors is counted for each replica. Every 5 minutes, the number of errors is integrally divided by 2. Thus, the number of errors is calculated for a recent time with exponential smoothing. If there is one replica with a minimal number of errors (i.e. errors occurred recently on the other replicas), the query is sent to it. If there are multiple replicas with the same minimal number of errors, the query is sent to the replica with a hostname that is most similar to the server’s hostname in the config file (for the number of different characters in identical positions, up to the minimum length of both hostnames).

For instance, example01-01-1 and example01-01-2.yandex.ru are different in one position, while example01-01-1 and example01-02-2 differ in two places.
This method might seem primitive, but it doesn’t require external data about network topology, and it doesn’t compare IP addresses, which would be complicated for our IPv6 addresses.

Thus, if there are equivalent replicas, the closest one by name is preferred.
We can also assume that when sending a query to the same server, in the absence of failures, a distributed query will also go to the same servers. So even if different data is placed on the replicas, the query will return mostly the same results.

### In Order {#load_balancing-in_order}

``` sql
load_balancing = in_order
```

Replicas with the same number of errors are accessed in the same order as they are specified in the configuration.
This method is appropriate when you know exactly which replica is preferable.

### First or Random {#load_balancing-first_or_random}

``` sql
load_balancing = first_or_random
```

This algorithm chooses the first replica in the set or a random replica if the first is unavailable. It’s effective in cross-replication topology setups, but useless in other configurations.

The `first_or_random` algorithm solves the problem of the `in_order` algorithm. With `in_order`, if one replica goes down, the next one gets a double load while the remaining replicas handle the usual amount of traffic. When using the `first_or_random` algorithm, the load is evenly distributed among replicas that are still available.

It's possible to explicitly define what the first replica is by using the setting `load_balancing_first_offset`. This gives more control to rebalance query workloads among replicas.

### Round Robin {#load_balancing-round_robin}

``` sql
load_balancing = round_robin
```

This algorithm uses a round-robin policy across replicas with the same number of errors (only the queries with `round_robin` policy is accounted).

## prefer_localhost_replica {#settings-prefer-localhost-replica}

Enables/disables preferable using the localhost replica when processing distributed queries.

Possible values:

-   1 — ClickHouse always sends a query to the localhost replica if it exists.
-   0 — ClickHouse uses the balancing strategy specified by the [load_balancing](#settings-load_balancing) setting.

Default value: 1.

!!! warning "Warning"
    Disable this setting if you use [max_parallel_replicas](#settings-max_parallel_replicas).

## totals_mode {#totals-mode}

How to calculate TOTALS when HAVING is present, as well as when max_rows_to_group_by and group_by_overflow_mode = ‘any’ are present.
See the section “WITH TOTALS modifier”.

## totals_auto_threshold {#totals-auto-threshold}

The threshold for `totals_mode = 'auto'`.
See the section “WITH TOTALS modifier”.

## max_parallel_replicas {#settings-max_parallel_replicas}

The maximum number of replicas for each shard when executing a query.
For consistency (to get different parts of the same data split), this option only works when the sampling key is set.
Replica lag is not controlled.

## compile {#compile}

Enable compilation of queries. By default, 0 (disabled).

The compilation is only used for part of the query-processing pipeline: for the first stage of aggregation (GROUP BY).
If this portion of the pipeline was compiled, the query may run faster due to the deployment of short cycles and inlining aggregate function calls. The maximum performance improvement (up to four times faster in rare cases) is seen for queries with multiple simple aggregate functions. Typically, the performance gain is insignificant. In very rare cases, it may slow down query execution.

## min_count_to_compile {#min-count-to-compile}

How many times to potentially use a compiled chunk of code before running compilation. By default, 3.
For testing, the value can be set to 0: compilation runs synchronously and the query waits for the end of the compilation process before continuing execution. For all other cases, use values ​​starting with 1. Compilation normally takes about 5-10 seconds.
If the value is 1 or more, compilation occurs asynchronously in a separate thread. The result will be used as soon as it is ready, including queries that are currently running.

Compiled code is required for each different combination of aggregate functions used in the query and the type of keys in the GROUP BY clause.
The results of the compilation are saved in the build directory in the form of .so files. There is no restriction on the number of compilation results since they don’t use very much space. Old results will be used after server restarts, except in the case of a server upgrade – in this case, the old results are deleted.

## output_format_json_quote_64bit_integers {#session_settings-output_format_json_quote_64bit_integers}

If the value is true, integers appear in quotes when using JSON\* Int64 and UInt64 formats (for compatibility with most JavaScript implementations); otherwise, integers are output without the quotes.

## output_format_json_quote_denormals {#settings-output_format_json_quote_denormals}

Enables `+nan`, `-nan`, `+inf`, `-inf` outputs in [JSON](../../interfaces/formats.md#json) output format.

Possible values:

-   0 — Disabled.
-   1 — Enabled.

Default value: 0.

**Example**

Consider the following table `account_orders`:

```text
┌─id─┬─name───┬─duration─┬─period─┬─area─┐
│  1 │ Andrew │       20 │      0 │  400 │
│  2 │ John   │       40 │      0 │    0 │
│  3 │ Bob    │       15 │      0 │ -100 │
└────┴────────┴──────────┴────────┴──────┘
```

When `output_format_json_quote_denormals = 0`, the query returns `null` values in output:

```sql
SELECT area/period FROM account_orders FORMAT JSON;
```

```json
{
        "meta":
        [
                {
                        "name": "divide(area, period)",
                        "type": "Float64"
                }
        ],

        "data":
        [
                {
                        "divide(area, period)": null
                },
                {
                        "divide(area, period)": null
                },
                {
                        "divide(area, period)": null
                }
        ],

        "rows": 3,

        "statistics":
        {
                "elapsed": 0.003648093,
                "rows_read": 3,
                "bytes_read": 24
        }
}
```

When `output_format_json_quote_denormals = 1`, the query returns:

```json
{
        "meta":
        [
                {
                        "name": "divide(area, period)",
                        "type": "Float64"
                }
        ],

        "data":
        [
                {
                        "divide(area, period)": "inf"
                },
                {
                        "divide(area, period)": "-nan"
                },
                {
                        "divide(area, period)": "-inf"
                }
        ],

        "rows": 3,

        "statistics":
        {
                "elapsed": 0.000070241,
                "rows_read": 3,
                "bytes_read": 24
        }
}
```

## format_csv_delimiter {#settings-format_csv_delimiter}

The character is interpreted as a delimiter in the CSV data. By default, the delimiter is `,`.

## input_format_csv_unquoted_null_literal_as_null {#settings-input_format_csv_unquoted_null_literal_as_null}

For CSV input format enables or disables parsing of unquoted `NULL` as literal (synonym for `\N`).

## input_format_csv_enum_as_number {#settings-input_format_csv_enum_as_number}

Enables or disables parsing enum values as enum ids for CSV input format.

Possible values:

-   0 — Enum values are parsed as values.
-   1 — Enum values are parsed as enum IDs.

Default value: 0.

**Examples**

Consider the table:

```sql
CREATE TABLE table_with_enum_column_for_csv_insert (Id Int32,Value Enum('first' = 1, 'second' = 2)) ENGINE=Memory();
```

When the `input_format_csv_enum_as_number` setting is enabled:  

```sql
SET input_format_csv_enum_as_number = 1;
INSERT INTO table_with_enum_column_for_csv_insert FORMAT CSV 102,2;
SELECT * FROM table_with_enum_column_for_csv_insert;
```

Result:

```text
┌──Id─┬─Value─────┐
│ 102 │ second    │
└─────┴───────────┘
```

When the `input_format_csv_enum_as_number` setting is disabled, the `INSERT` query:

```sql
SET input_format_csv_enum_as_number = 0;
INSERT INTO table_with_enum_column_for_csv_insert FORMAT CSV 102,2;
```

throws an exception.

## output_format_csv_crlf_end_of_line {#settings-output-format-csv-crlf-end-of-line}

Use DOS/Windows-style line separator (CRLF) in CSV instead of Unix style (LF).

## output_format_tsv_crlf_end_of_line {#settings-output-format-tsv-crlf-end-of-line}

Use DOC/Windows-style line separator (CRLF) in TSV instead of Unix style (LF).

## insert_quorum {#settings-insert_quorum}

Enables the quorum writes.

-   If `insert_quorum < 2`, the quorum writes are disabled.
-   If `insert_quorum >= 2`, the quorum writes are enabled.

Default value: 0.

Quorum writes

`INSERT` succeeds only when ClickHouse manages to correctly write data to the `insert_quorum` of replicas during the `insert_quorum_timeout`. If for any reason the number of replicas with successful writes does not reach the `insert_quorum`, the write is considered failed and ClickHouse will delete the inserted block from all the replicas where data has already been written.

All the replicas in the quorum are consistent, i.e., they contain data from all previous `INSERT` queries. The `INSERT` sequence is linearized.

When reading the data written from the `insert_quorum`, you can use the [select_sequential_consistency](#settings-select_sequential_consistency) option.

ClickHouse generates an exception

-   If the number of available replicas at the time of the query is less than the `insert_quorum`.
-   At an attempt to write data when the previous block has not yet been inserted in the `insert_quorum` of replicas. This situation may occur if the user tries to perform an `INSERT` before the previous one with the `insert_quorum` is completed.

See also:

-   [insert_quorum_timeout](#settings-insert_quorum_timeout)
-   [select_sequential_consistency](#settings-select_sequential_consistency)

## insert_quorum_timeout {#settings-insert_quorum_timeout}

Write to a quorum timeout in milliseconds. If the timeout has passed and no write has taken place yet, ClickHouse will generate an exception and the client must repeat the query to write the same block to the same or any other replica.

Default value: 600000 milliseconds (ten minutes).

See also:

-   [insert_quorum](#settings-insert_quorum)
-   [select_sequential_consistency](#settings-select_sequential_consistency)

## select_sequential_consistency {#settings-select_sequential_consistency}

Enables or disables sequential consistency for `SELECT` queries:

Possible values:

-   0 — Disabled.
-   1 — Enabled.

Default value: 0.

Usage

When sequential consistency is enabled, ClickHouse allows the client to execute the `SELECT` query only for those replicas that contain data from all previous `INSERT` queries executed with `insert_quorum`. If the client refers to a partial replica, ClickHouse will generate an exception. The SELECT query will not include data that has not yet been written to the quorum of replicas.

See also:

-   [insert_quorum](#settings-insert_quorum)
-   [insert_quorum_timeout](#settings-insert_quorum_timeout)

## insert_deduplicate {#settings-insert-deduplicate}

Enables or disables block deduplication of `INSERT` (for Replicated\* tables).

Possible values:

-   0 — Disabled.
-   1 — Enabled.

Default value: 1.

By default, blocks inserted into replicated tables by the `INSERT` statement are deduplicated (see [Data Replication](../../engines/table-engines/mergetree-family/replication.md)).

## deduplicate_blocks_in_dependent_materialized_views {#settings-deduplicate-blocks-in-dependent-materialized-views}

Enables or disables the deduplication check for materialized views that receive data from Replicated\* tables.

Possible values:

      0 — Disabled.
      1 — Enabled.

Default value: 0.

Usage

By default, deduplication is not performed for materialized views but is done upstream, in the source table.
If an INSERTed block is skipped due to deduplication in the source table, there will be no insertion into attached materialized views. This behaviour exists to enable the insertion of highly aggregated data into materialized views, for cases where inserted blocks are the same after materialized view aggregation but derived from different INSERTs into the source table.
At the same time, this behaviour “breaks” `INSERT` idempotency. If an `INSERT` into the main table was successful and `INSERT` into a materialized view failed (e.g. because of communication failure with Zookeeper) a client will get an error and can retry the operation. However, the materialized view won’t receive the second insert because it will be discarded by deduplication in the main (source) table. The setting `deduplicate_blocks_in_dependent_materialized_views` allows for changing this behaviour. On retry, a materialized view will receive the repeat insert and will perform a deduplication check by itself,
ignoring check result for the source table, and will insert rows lost because of the first failure.

## max_network_bytes {#settings-max-network-bytes}

Limits the data volume (in bytes) that is received or transmitted over the network when executing a query. This setting applies to every individual query.

Possible values:

-   Positive integer.
-   0 — Data volume control is disabled.

Default value: 0.

## max_network_bandwidth {#settings-max-network-bandwidth}

Limits the speed of the data exchange over the network in bytes per second. This setting applies to every query.

Possible values:

-   Positive integer.
-   0 — Bandwidth control is disabled.

Default value: 0.

## max_network_bandwidth_for_user {#settings-max-network-bandwidth-for-user}

Limits the speed of the data exchange over the network in bytes per second. This setting applies to all concurrently running queries performed by a single user.

Possible values:

-   Positive integer.
-   0 — Control of the data speed is disabled.

Default value: 0.

## max_network_bandwidth_for_all_users {#settings-max-network-bandwidth-for-all-users}

Limits the speed that data is exchanged at over the network in bytes per second. This setting applies to all concurrently running queries on the server.

Possible values:

-   Positive integer.
-   0 — Control of the data speed is disabled.

Default value: 0.

## count_distinct_implementation {#settings-count_distinct_implementation}

Specifies which of the `uniq*` functions should be used to perform the [COUNT(DISTINCT …)](../../sql-reference/aggregate-functions/reference/count.md#agg_function-count) construction.

Possible values:

-   [uniq](../../sql-reference/aggregate-functions/reference/uniq.md#agg_function-uniq)
-   [uniqCombined](../../sql-reference/aggregate-functions/reference/uniqcombined.md#agg_function-uniqcombined)
-   [uniqCombined64](../../sql-reference/aggregate-functions/reference/uniqcombined64.md#agg_function-uniqcombined64)
-   [uniqHLL12](../../sql-reference/aggregate-functions/reference/uniqhll12.md#agg_function-uniqhll12)
-   [uniqExact](../../sql-reference/aggregate-functions/reference/uniqexact.md#agg_function-uniqexact)

Default value: `uniqExact`.

## skip_unavailable_shards {#settings-skip_unavailable_shards}

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

## distributed_group_by_no_merge {#distributed-group-by-no-merge}

Do not merge aggregation states from different servers for distributed query processing, you can use this in case it is for certain that there are different keys on different shards

Possible values:

-   0 — Disabled (final query processing is done on the initiator node).
-   1 - Do not merge aggregation states from different servers for distributed query processing (query completelly processed on the shard, initiator only proxy the data).
-   2 - Same as 1 but apply `ORDER BY` and `LIMIT` on the initiator (can be used for queries with `ORDER BY` and/or `LIMIT`).

**Example**

```sql
SELECT *
FROM remote('127.0.0.{2,3}', system.one)
GROUP BY dummy
LIMIT 1
SETTINGS distributed_group_by_no_merge = 1
FORMAT PrettyCompactMonoBlock

┌─dummy─┐
│     0 │
│     0 │
└───────┘
```

```sql
SELECT *
FROM remote('127.0.0.{2,3}', system.one)
GROUP BY dummy
LIMIT 1
SETTINGS distributed_group_by_no_merge = 2
FORMAT PrettyCompactMonoBlock

┌─dummy─┐
│     0 │
└───────┘
```

Default value: 0

## optimize_skip_unused_shards {#optimize-skip-unused-shards}

Enables or disables skipping of unused shards for [SELECT](../../sql-reference/statements/select/index.md) queries that have sharding key condition in `WHERE/PREWHERE` (assuming that the data is distributed by sharding key, otherwise does nothing).

Possible values:

-   0 — Disabled.
-   1 — Enabled.

Default value: 0

## allow_nondeterministic_optimize_skip_unused_shards {#allow-nondeterministic-optimize-skip-unused-shards}

Allow nondeterministic (like `rand` or `dictGet`, since later has some caveats with updates) functions in sharding key.

Possible values:

-   0 — Disallowed.
-   1 — Allowed.

Default value: 0

## optimize_skip_unused_shards_nesting {#optimize-skip-unused-shards-nesting}

Controls [`optimize_skip_unused_shards`](#optimize-skip-unused-shards) (hence still requires [`optimize_skip_unused_shards`](#optimize-skip-unused-shards)) depends on the nesting level of the distributed query (case when you have `Distributed` table that look into another `Distributed` table).

Possible values:

-   0 — Disabled, `optimize_skip_unused_shards` works always.
-   1 — Enables `optimize_skip_unused_shards` only for the first level.
-   2 — Enables `optimize_skip_unused_shards` up to the second level.

Default value: 0

## force_optimize_skip_unused_shards {#force-optimize-skip-unused-shards}

Enables or disables query execution if [optimize_skip_unused_shards](#optimize-skip-unused-shards) is enabled and skipping of unused shards is not possible. If the skipping is not possible and the setting is enabled, an exception will be thrown.

Possible values:

-   0 — Disabled. ClickHouse doesn’t throw an exception.
-   1 — Enabled. Query execution is disabled only if the table has a sharding key.
-   2 — Enabled. Query execution is disabled regardless of whether a sharding key is defined for the table.

Default value: 0

## force_optimize_skip_unused_shards_nesting {#settings-force_optimize_skip_unused_shards_nesting}

Controls [`force_optimize_skip_unused_shards`](#force-optimize-skip-unused-shards) (hence still requires [`force_optimize_skip_unused_shards`](#force-optimize-skip-unused-shards)) depends on the nesting level of the distributed query (case when you have `Distributed` table that look into another `Distributed` table).

Possible values:

-   0 - Disabled, `force_optimize_skip_unused_shards` works always.
-   1 — Enables `force_optimize_skip_unused_shards` only for the first level.
-   2 — Enables `force_optimize_skip_unused_shards` up to the second level.

Default value: 0

## optimize_distributed_group_by_sharding_key {#optimize-distributed-group-by-sharding-key}

Optimize `GROUP BY sharding_key` queries, by avoiding costly aggregation on the initiator server (which will reduce memory usage for the query on the initiator server).

The following types of queries are supported (and all combinations of them):

- `SELECT DISTINCT [..., ]sharding_key[, ...] FROM dist`
- `SELECT ... FROM dist GROUP BY sharding_key[, ...]`
- `SELECT ... FROM dist GROUP BY sharding_key[, ...] ORDER BY x`
- `SELECT ... FROM dist GROUP BY sharding_key[, ...] LIMIT 1`
- `SELECT ... FROM dist GROUP BY sharding_key[, ...] LIMIT 1 BY x`

The following types of queries are not supported (support for some of them may be added later):

- `SELECT ... GROUP BY sharding_key[, ...] WITH TOTALS`
- `SELECT ... GROUP BY sharding_key[, ...] WITH ROLLUP`
- `SELECT ... GROUP BY sharding_key[, ...] WITH CUBE`
- `SELECT ... GROUP BY sharding_key[, ...] SETTINGS extremes=1`

Possible values:

-   0 — Disabled.
-   1 — Enabled.

Default value: 0

See also:

-   [distributed_group_by_no_merge](#distributed-group-by-no-merge)
-   [optimize_skip_unused_shards](#optimize-skip-unused-shards)

!!! note "Note"
    Right now it requires `optimize_skip_unused_shards` (the reason behind this is that one day it may be enabled by default, and it will work correctly only if data was inserted via Distributed table, i.e. data is distributed according to sharding_key).

## optimize_throw_if_noop {#setting-optimize_throw_if_noop}

Enables or disables throwing an exception if an [OPTIMIZE](../../sql-reference/statements/misc.md#misc_operations-optimize) query didn’t perform a merge.

By default, `OPTIMIZE` returns successfully even if it didn’t do anything. This setting lets you differentiate these situations and get the reason in an exception message.

Possible values:

-   1 — Throwing an exception is enabled.
-   0 — Throwing an exception is disabled.

Default value: 0.

## distributed_replica_error_half_life {#settings-distributed_replica_error_half_life}

-   Type: seconds
-   Default value: 60 seconds

Controls how fast errors in distributed tables are zeroed. If a replica is unavailable for some time, accumulates 5 errors, and distributed_replica_error_half_life is set to 1 second, then the replica is considered normal 3 seconds after the last error.

See also:

-   [load_balancing](#load_balancing-round_robin)
-   [Table engine Distributed](../../engines/table-engines/special/distributed.md)
-   [distributed_replica_error_cap](#settings-distributed_replica_error_cap)
-   [distributed_replica_max_ignored_errors](#settings-distributed_replica_max_ignored_errors)

## distributed_replica_error_cap {#settings-distributed_replica_error_cap}

-   Type: unsigned int
-   Default value: 1000

The error count of each replica is capped at this value, preventing a single replica from accumulating too many errors.

See also:

-   [load_balancing](#load_balancing-round_robin)
-   [Table engine Distributed](../../engines/table-engines/special/distributed.md)
-   [distributed_replica_error_half_life](#settings-distributed_replica_error_half_life)
-   [distributed_replica_max_ignored_errors](#settings-distributed_replica_max_ignored_errors)

## distributed_replica_max_ignored_errors {#settings-distributed_replica_max_ignored_errors}

-   Type: unsigned int
-   Default value: 0

The number of errors that will be ignored while choosing replicas (according to `load_balancing` algorithm).

See also:

-   [load_balancing](#load_balancing-round_robin)
-   [Table engine Distributed](../../engines/table-engines/special/distributed.md)
-   [distributed_replica_error_cap](#settings-distributed_replica_error_cap)
-   [distributed_replica_error_half_life](#settings-distributed_replica_error_half_life)

## distributed_directory_monitor_sleep_time_ms {#distributed_directory_monitor_sleep_time_ms}

Base interval for the [Distributed](../../engines/table-engines/special/distributed.md) table engine to send data. The actual interval grows exponentially in the event of errors.

Possible values:

-   A positive integer number of milliseconds.

Default value: 100 milliseconds.

## distributed_directory_monitor_max_sleep_time_ms {#distributed_directory_monitor_max_sleep_time_ms}

Maximum interval for the [Distributed](../../engines/table-engines/special/distributed.md) table engine to send data. Limits exponential growth of the interval set in the [distributed_directory_monitor_sleep_time_ms](#distributed_directory_monitor_sleep_time_ms) setting.

Possible values:

-   A positive integer number of milliseconds.

Default value: 30000 milliseconds (30 seconds).

## distributed_directory_monitor_batch_inserts {#distributed_directory_monitor_batch_inserts}

Enables/disables inserted data sending in batches.

When batch sending is enabled, the [Distributed](../../engines/table-engines/special/distributed.md) table engine tries to send multiple files of inserted data in one operation instead of sending them separately. Batch sending improves cluster performance by better-utilizing server and network resources.

Possible values:

-   1 — Enabled.
-   0 — Disabled.

Default value: 0.

## os_thread_priority {#setting-os-thread-priority}

Sets the priority ([nice](https://en.wikipedia.org/wiki/Nice_(Unix))) for threads that execute queries. The OS scheduler considers this priority when choosing the next thread to run on each available CPU core.

!!! warning "Warning"
    To use this setting, you need to set the `CAP_SYS_NICE` capability. The `clickhouse-server` package sets it up during installation. Some virtual environments don’t allow you to set the `CAP_SYS_NICE` capability. In this case, `clickhouse-server` shows a message about it at the start.

Possible values:

-   You can set values in the range `[-20, 19]`.

Lower values mean higher priority. Threads with low `nice` priority values are executed more frequently than threads with high values. High values are preferable for long-running non-interactive queries because it allows them to quickly give up resources in favour of short interactive queries when they arrive.

Default value: 0.

## query_profiler_real_time_period_ns {#query_profiler_real_time_period_ns}

Sets the period for a real clock timer of the [query profiler](../../operations/optimizing-performance/sampling-query-profiler.md). Real clock timer counts wall-clock time.

Possible values:

-   Positive integer number, in nanoseconds.

    Recommended values:

            - 10000000 (100 times a second) nanoseconds and less for single queries.
            - 1000000000 (once a second) for cluster-wide profiling.

-   0 for turning off the timer.

Type: [UInt64](../../sql-reference/data-types/int-uint.md).

Default value: 1000000000 nanoseconds (once a second).

See also:

-   System table [trace_log](../../operations/system-tables/trace_log.md#system_tables-trace_log)

## query_profiler_cpu_time_period_ns {#query_profiler_cpu_time_period_ns}

Sets the period for a CPU clock timer of the [query profiler](../../operations/optimizing-performance/sampling-query-profiler.md). This timer counts only CPU time.

Possible values:

-   A positive integer number of nanoseconds.

    Recommended values:

            - 10000000 (100 times a second) nanoseconds and more for single queries.
            - 1000000000 (once a second) for cluster-wide profiling.

-   0 for turning off the timer.

Type: [UInt64](../../sql-reference/data-types/int-uint.md).

Default value: 1000000000 nanoseconds.

See also:

-   System table [trace_log](../../operations/system-tables/trace_log.md#system_tables-trace_log)

## allow_introspection_functions {#settings-allow_introspection_functions}

Enables or disables [introspections functions](../../sql-reference/functions/introspection.md) for query profiling.

Possible values:

-   1 — Introspection functions enabled.
-   0 — Introspection functions disabled.

Default value: 0.

**See Also**

-   [Sampling Query Profiler](../../operations/optimizing-performance/sampling-query-profiler.md)
-   System table [trace_log](../../operations/system-tables/trace_log.md#system_tables-trace_log)

## input_format_parallel_parsing {#input-format-parallel-parsing}

-   Type: bool
-   Default value: True

Enable order-preserving parallel parsing of data formats. Supported only for TSV, TKSV, CSV, and JSONEachRow formats.

## min_chunk_bytes_for_parallel_parsing {#min-chunk-bytes-for-parallel-parsing}

-   Type: unsigned int
-   Default value: 1 MiB

The minimum chunk size in bytes, which each thread will parse in parallel.

## output_format_avro_codec {#settings-output_format_avro_codec}

Sets the compression codec used for output Avro file.

Type: string

Possible values:

-   `null` — No compression
-   `deflate` — Compress with Deflate (zlib)
-   `snappy` — Compress with [Snappy](https://google.github.io/snappy/)

Default value: `snappy` (if available) or `deflate`.

## output_format_avro_sync_interval {#settings-output_format_avro_sync_interval}

Sets minimum data size (in bytes) between synchronization markers for output Avro file.

Type: unsigned int

Possible values: 32 (32 bytes) - 1073741824 (1 GiB)

Default value: 32768 (32 KiB)

## format_avro_schema_registry_url {#format_avro_schema_registry_url}

Sets [Confluent Schema Registry](https://docs.confluent.io/current/schema-registry/index.html) URL to use with [AvroConfluent](../../interfaces/formats.md#data-format-avro-confluent) format.

Default value: `Empty`.

## input_format_avro_allow_missing_fields {#input_format_avro_allow_missing_fields}

Enables using fields that are not specified in [Avro](../../interfaces/formats.md#data-format-avro) or [AvroConfluent](../../interfaces/formats.md#data-format-avro-confluent) format schema. When a field is not found in the schema, ClickHouse uses the default value instead of throwing an exception.

Possible values:

-   0 — Disabled.
-   1 — Enabled.

Default value: 0.

## background_pool_size {#background_pool_size}

Sets the number of threads performing background operations in table engines (for example, merges in [MergeTree engine](../../engines/table-engines/mergetree-family/index.md) tables). This setting is applied from the `default` profile at the ClickHouse server start and can’t be changed in a user session. By adjusting this setting, you manage CPU and disk load. Smaller pool size utilizes less CPU and disk resources, but background processes advance slower which might eventually impact query performance.

Before changing it, please also take a look at related [MergeTree settings](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-merge_tree), such as `number_of_free_entries_in_pool_to_lower_max_size_of_merge` and `number_of_free_entries_in_pool_to_execute_mutation`.

Possible values:

-   Any positive integer.

Default value: 16.

## parallel_distributed_insert_select {#parallel_distributed_insert_select}

Enables parallel distributed `INSERT ... SELECT` query.

If we execute `INSERT INTO distributed_table_a SELECT ... FROM distributed_table_b` queries and both tables use the same cluster, and both tables are either [replicated](../../engines/table-engines/mergetree-family/replication.md) or non-replicated, then this query is processed locally on every shard.

Possible values:

-   0 — Disabled.
-   1 — `SELECT` will be executed on each shard from the underlying table of the distributed engine.
-   2 — `SELECT` and `INSERT` will be executed on each shard from/to the underlying table of the distributed engine.

Default value: 0.

## insert_distributed_sync {#insert_distributed_sync}

Enables or disables synchronous data insertion into a [Distributed](../../engines/table-engines/special/distributed.md#distributed) table.

By default, when inserting data into a `Distributed` table, the ClickHouse server sends data to cluster nodes in asynchronous mode. When `insert_distributed_sync=1`, the data is processed synchronously, and the `INSERT` operation succeeds only after all the data is saved on all shards (at least one replica for each shard if `internal_replication` is true). 

Possible values:

-   0 — Data is inserted in asynchronous mode.
-   1 — Data is inserted in synchronous mode.

Default value: `0`.

**See Also**

-   [Distributed Table Engine](../../engines/table-engines/special/distributed.md#distributed)
-   [Managing Distributed Tables](../../sql-reference/statements/system.md#query-language-system-distributed)


## use_compact_format_in_distributed_parts_names {#use_compact_format_in_distributed_parts_names}

Uses compact format for storing blocks for async (`insert_distributed_sync`) INSERT into tables with `Distributed` engine.

Possible values:

-   0 — Uses `user[:password]@host:port#default_database` directory format.
-   1 — Uses `[shard{shard_index}[_replica{replica_index}]]` directory format.

Default value: `1`.

!!! note "Note"
    - with `use_compact_format_in_distributed_parts_names=0` changes from cluster definition will not be applied for async INSERT.
    - with `use_compact_format_in_distributed_parts_names=1` changing the order of the nodes in the cluster definition, will change the `shard_index`/`replica_index` so be aware.

## background_buffer_flush_schedule_pool_size {#background_buffer_flush_schedule_pool_size}

Sets the number of threads performing background flush in [Buffer](../../engines/table-engines/special/buffer.md)-engine tables. This setting is applied at the ClickHouse server start and can’t be changed in a user session.

Possible values:

-   Any positive integer.

Default value: 16.

## background_move_pool_size {#background_move_pool_size}

Sets the number of threads performing background moves of data parts for [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes)-engine tables. This setting is applied at the ClickHouse server start and can’t be changed in a user session.

Possible values:

-   Any positive integer.

Default value: 8.

## background_schedule_pool_size {#background_schedule_pool_size}

Sets the number of threads performing background tasks for [replicated](../../engines/table-engines/mergetree-family/replication.md) tables, [Kafka](../../engines/table-engines/integrations/kafka.md) streaming, [DNS cache updates](../../operations/server-configuration-parameters/settings.md#server-settings-dns-cache-update-period). This setting is applied at ClickHouse server start and can’t be changed in a user session.

Possible values:

-   Any positive integer.

Default value: 16.

## always_fetch_merged_part {#always_fetch_merged_part}

Prohibits data parts merging in [Replicated\*MergeTree](../../engines/table-engines/mergetree-family/replication.md)-engine tables.

When merging is prohibited, the replica never merges parts and always downloads merged parts from other replicas. If there is no required data yet, the replica waits for it. CPU and disk load on the replica server decreases, but the network load on the cluster increases. This setting can be useful on servers with relatively weak CPUs or slow disks, such as servers for backups storage.

Possible values:

-   0 — `Replicated*MergeTree`-engine tables merge data parts at the replica.
-   1 — `Replicated*MergeTree`-engine tables don’t merge data parts at the replica. The tables download merged data parts from other replicas.

Default value: 0.

**See Also**

-   [Data Replication](../../engines/table-engines/mergetree-family/replication.md)

## background_distributed_schedule_pool_size {#background_distributed_schedule_pool_size}

Sets the number of threads performing background tasks for [distributed](../../engines/table-engines/special/distributed.md) sends. This setting is applied at the ClickHouse server start and can’t be changed in a user session.

Possible values:

-   Any positive integer.

Default value: 16.

## validate_polygons {#validate_polygons}

Enables or disables throwing an exception in the [pointInPolygon](../../sql-reference/functions/geo/index.md#pointinpolygon) function, if the polygon is self-intersecting or self-tangent.

Possible values:

- 0 — Throwing an exception is disabled. `pointInPolygon` accepts invalid polygons and returns possibly incorrect results for them.
- 1 — Throwing an exception is enabled.

Default value: 1.

## transform_null_in {#transform_null_in}

Enables equality of [NULL](../../sql-reference/syntax.md#null-literal) values for [IN](../../sql-reference/operators/in.md) operator.

By default, `NULL` values can’t be compared because `NULL` means undefined value. Thus, comparison `expr = NULL` must always return `false`. With this setting `NULL = NULL` returns `true` for `IN` operator.

Possible values:

-   0 — Comparison of `NULL` values in `IN` operator returns `false`.
-   1 — Comparison of `NULL` values in `IN` operator returns `true`.

Default value: 0.

**Example**

Consider the `null_in` table:

``` text
┌──idx─┬─────i─┐
│    1 │     1 │
│    2 │  NULL │
│    3 │     3 │
└──────┴───────┘
```

Query:

``` sql
SELECT idx, i FROM null_in WHERE i IN (1, NULL) SETTINGS transform_null_in = 0;
```

Result:

``` text
┌──idx─┬────i─┐
│    1 │    1 │
└──────┴──────┘
```

Query:

``` sql
SELECT idx, i FROM null_in WHERE i IN (1, NULL) SETTINGS transform_null_in = 1;
```

Result:

``` text
┌──idx─┬─────i─┐
│    1 │     1 │
│    2 │  NULL │
└──────┴───────┘
```

**See Also**

-   [NULL Processing in IN Operators](../../sql-reference/operators/in.md#in-null-processing)

## low_cardinality_max_dictionary_size {#low_cardinality_max_dictionary_size}

Sets a maximum size in rows of a shared global dictionary for the [LowCardinality](../../sql-reference/data-types/lowcardinality.md) data type that can be written to a storage file system. This setting prevents issues with RAM in case of unlimited dictionary growth. All the data that can’t be encoded due to maximum dictionary size limitation ClickHouse writes in an ordinary method.

Possible values:

-   Any positive integer.

Default value: 8192.

## low_cardinality_use_single_dictionary_for_part {#low_cardinality_use_single_dictionary_for_part}

Turns on or turns off using of single dictionary for the data part.

By default, the ClickHouse server monitors the size of dictionaries and if a dictionary overflows then the server starts to write the next one. To prohibit creating several dictionaries set `low_cardinality_use_single_dictionary_for_part = 1`.

Possible values:

-   1 — Creating several dictionaries for the data part is prohibited.
-   0 — Creating several dictionaries for the data part is not prohibited.

Default value: 0.

## low_cardinality_allow_in_native_format {#low_cardinality_allow_in_native_format}

Allows or restricts using the [LowCardinality](../../sql-reference/data-types/lowcardinality.md) data type with the [Native](../../interfaces/formats.md#native) format.

If usage of `LowCardinality` is restricted, ClickHouse server converts `LowCardinality`-columns to ordinary ones for `SELECT` queries, and convert ordinary columns to `LowCardinality`-columns for `INSERT` queries.

This setting is required mainly for third-party clients which don’t support `LowCardinality` data type.

Possible values:

-   1 — Usage of `LowCardinality` is not restricted.
-   0 — Usage of `LowCardinality` is restricted.

Default value: 1.

## allow_suspicious_low_cardinality_types {#allow_suspicious_low_cardinality_types}

Allows or restricts using [LowCardinality](../../sql-reference/data-types/lowcardinality.md) with data types with fixed size of 8 bytes or less: numeric data types and `FixedString(8_bytes_or_less)`.

For small fixed values using of `LowCardinality` is usually inefficient, because ClickHouse stores a numeric index for each row. As a result:

-   Disk space usage can rise.
-   RAM consumption can be higher, depending on a dictionary size.
-   Some functions can work slower due to extra coding/encoding operations.

Merge times in [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md)-engine tables can grow due to all the reasons described above.

Possible values:

-   1 — Usage of `LowCardinality` is not restricted.
-   0 — Usage of `LowCardinality` is restricted.

Default value: 0.

## min_insert_block_size_rows_for_materialized_views {#min-insert-block-size-rows-for-materialized-views}

Sets the minimum number of rows in the block which can be inserted into a table by an `INSERT` query. Smaller-sized blocks are squashed into bigger ones. This setting is applied only for blocks inserted into [materialized view](../../sql-reference/statements/create/view.md). By adjusting this setting, you control blocks squashing while pushing to materialized view and avoid excessive memory usage.

Possible values:

-   Any positive integer.
-   0 — Squashing disabled.

Default value: 1048576.

**See Also**

-   [min_insert_block_size_rows](#min-insert-block-size-rows)

## min_insert_block_size_bytes_for_materialized_views {#min-insert-block-size-bytes-for-materialized-views}

Sets the minimum number of bytes in the block which can be inserted into a table by an `INSERT` query. Smaller-sized blocks are squashed into bigger ones. This setting is applied only for blocks inserted into [materialized view](../../sql-reference/statements/create/view.md). By adjusting this setting, you control blocks squashing while pushing to materialized view and avoid excessive memory usage.

Possible values:

-   Any positive integer.
-   0 — Squashing disabled.

Default value: 268435456.

**See also**

-   [min_insert_block_size_bytes](#min-insert-block-size-bytes)

## output_format_pretty_grid_charset {#output-format-pretty-grid-charset}

Allows changing a charset which is used for printing grids borders. Available charsets are UTF-8, ASCII.

**Example**

``` text
SET output_format_pretty_grid_charset = 'UTF-8';
SELECT * FROM a;
┌─a─┐
│ 1 │
└───┘

SET output_format_pretty_grid_charset = 'ASCII';
SELECT * FROM a;
+-a-+
| 1 |
+---+
```
## optimize_read_in_order {#optimize_read_in_order}

Enables [ORDER BY](../../sql-reference/statements/select/order-by.md#optimize_read_in_order) optimization in [SELECT](../../sql-reference/statements/select/index.md) queries for reading data from [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) tables.

Possible values:

-   0 — `ORDER BY` optimization is disabled. 
-   1 — `ORDER BY` optimization is enabled. 

Default value: `1`.

**See Also**

-   [ORDER BY Clause](../../sql-reference/statements/select/order-by.md#optimize_read_in_order)

## mutations_sync {#mutations_sync}

Allows to execute `ALTER TABLE ... UPDATE|DELETE` queries ([mutations](../../sql-reference/statements/alter/index.md#mutations)) synchronously.

Possible values:

-   0 - Mutations execute asynchronously. 
-   1 - The query waits for all mutations to complete on the current server. 
-   2 - The query waits for all mutations to complete on all replicas (if they exist).

Default value: `0`.

**See Also**

-   [Synchronicity of ALTER Queries](../../sql-reference/statements/alter/index.md#synchronicity-of-alter-queries)
-   [Mutations](../../sql-reference/statements/alter/index.md#mutations)

## ttl_only_drop_parts {#ttl_only_drop_parts}

Enables or disables complete dropping of data parts where all rows are expired in [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) tables. 

When `ttl_only_drop_parts` is disabled (by default), the ClickHouse server only deletes expired rows according to their TTL. 

When `ttl_only_drop_parts` is enabled, the ClickHouse server drops a whole part when all rows in it are expired. 

Dropping whole parts instead of partial cleaning TTL-d rows allows having shorter `merge_with_ttl_timeout` times and lower impact on system performance.

Possible values:

-   0 — The complete dropping of data parts is disabled.
-   1 — The complete dropping of data parts is enabled.

Default value: `0`.

**See Also** 

-   [CREATE TABLE query clauses and settings](../../engines/table-engines/mergetree-family/mergetree.md#mergetree-query-clauses) (`merge_with_ttl_timeout` setting)
-   [Table TTL](../../engines/table-engines/mergetree-family/mergetree.md#mergetree-table-ttl)

## lock_acquire_timeout {#lock_acquire_timeout}

Defines how many seconds a locking request waits before failing. 

Locking timeout is used to protect from deadlocks while executing read/write operations with tables. When the timeout expires and the locking request fails, the ClickHouse server throws an exception "Locking attempt timed out! Possible deadlock avoided. Client should retry." with error code `DEADLOCK_AVOIDED`.

Possible values:

-   Positive integer (in seconds).
-   0 — No locking timeout.

Default value: `120` seconds.

## cast_keep_nullable {#cast_keep_nullable}

Enables or disables keeping of the `Nullable` data type in [CAST](../../sql-reference/functions/type-conversion-functions.md#type_conversion_function-cast) operations.

When the setting is enabled and the argument of `CAST` function is `Nullable`, the result is also transformed to `Nullable` type. When the setting is disabled, the result always has the destination type exactly.

Possible values:

-  0 — The `CAST` result has exactly the destination type specified.
-  1 — If the argument type is `Nullable`, the `CAST` result is transformed to `Nullable(DestinationDataType)`. 

Default value: `0`.

**Examples** 

The following query results in the destination data type exactly:

```sql
SET cast_keep_nullable = 0;
SELECT CAST(toNullable(toInt32(0)) AS Int32) as x, toTypeName(x);
```

Result:

```text
┌─x─┬─toTypeName(CAST(toNullable(toInt32(0)), 'Int32'))─┐
│ 0 │ Int32                                             │
└───┴───────────────────────────────────────────────────┘
```

The following query results in the `Nullable` modification on the destination data type:

```sql
SET cast_keep_nullable = 1;
SELECT CAST(toNullable(toInt32(0)) AS Int32) as x, toTypeName(x);
```

Result:

```text
┌─x─┬─toTypeName(CAST(toNullable(toInt32(0)), 'Int32'))─┐
│ 0 │ Nullable(Int32)                                   │
└───┴───────────────────────────────────────────────────┘
```

**See Also** 

-   [CAST](../../sql-reference/functions/type-conversion-functions.md#type_conversion_function-cast) function

## output_format_pretty_max_value_width {#output_format_pretty_max_value_width}

Limits the width of value displayed in [Pretty](../../interfaces/formats.md#pretty) formats. If the value width exceeds the limit, the value is cut. 

Possible values:

-   Positive integer. 
-   0 — The value is cut completely.

Default value: `10000` symbols.

**Examples**

Query:
```sql
SET output_format_pretty_max_value_width = 10;
SELECT range(number) FROM system.numbers LIMIT 10 FORMAT PrettyCompactNoEscapes;
```
Result:
```text
┌─range(number)─┐
│ []            │
│ [0]           │
│ [0,1]         │
│ [0,1,2]       │
│ [0,1,2,3]     │
│ [0,1,2,3,4⋯   │
│ [0,1,2,3,4⋯   │
│ [0,1,2,3,4⋯   │
│ [0,1,2,3,4⋯   │
│ [0,1,2,3,4⋯   │
└───────────────┘
```

Query with zero width:
```sql
SET output_format_pretty_max_value_width = 0;
SELECT range(number) FROM system.numbers LIMIT 5 FORMAT PrettyCompactNoEscapes;
```
Result:
```text
┌─range(number)─┐
│ ⋯             │
│ ⋯             │
│ ⋯             │
│ ⋯             │
│ ⋯             │
└───────────────┘
```

## output_format_pretty_row_numbers {#output_format_pretty_row_numbers}

Adds row numbers to output in the [Pretty](../../interfaces/formats.md#pretty) format.

Possible values:

-   0 — Output without row numbers.
-   1 — Output with row numbers.

Default value: `0`.

**Example**

Query:

```sql
SET output_format_pretty_row_numbers = 1;
SELECT TOP 3 name, value FROM system.settings;
```

Result:
```text
   ┌─name────────────────────┬─value───┐
1. │ min_compress_block_size │ 65536   │
2. │ max_compress_block_size │ 1048576 │
3. │ max_block_size          │ 65505   │
   └─────────────────────────┴─────────┘
```

## system_events_show_zero_values {#system_events_show_zero_values}

Allows to select zero-valued events from [`system.events`](../../operations/system-tables/events.md).

Some monitoring systems require passing all the metrics values to them for each checkpoint, even if the metric value is zero.

Possible values:

-   0 — Disabled.
-   1 — Enabled.

Default value: `0`.

**Examples**

Query

```sql
SELECT * FROM system.events WHERE event='QueryMemoryLimitExceeded';
```

Result

```text
Ok.
```

Query
```sql
SET system_events_show_zero_values = 1;
SELECT * FROM system.events WHERE event='QueryMemoryLimitExceeded';
```

Result

```text
┌─event────────────────────┬─value─┬─description───────────────────────────────────────────┐
│ QueryMemoryLimitExceeded │     0 │ Number of times when memory limit exceeded for query. │
└──────────────────────────┴───────┴───────────────────────────────────────────────────────┘
```

## allow_experimental_bigint_types {#allow_experimental_bigint_types}

Enables or disables integer values exceeding the range that is supported by the int data type.

Possible values:

-   1 — The bigint data type is enabled.
-   0 — The bigint data type is disabled.

Default value: `0`.

## persistent {#persistent}

Disables persistency for the [Set](../../engines/table-engines/special/set.md#set) and [Join](../../engines/table-engines/special/join.md#join) table engines. 

Reduces the I/O overhead. Suitable for scenarios that pursue performance and do not require persistence.

Possible values:

- 1 — Enabled.
- 0 — Disabled.

Default value: `1`.

## output_format_tsv_null_representation {#output_format_tsv_null_representation}

Allows configurable `NULL` representation for [TSV](../../interfaces/formats.md#tabseparated) output format. The setting only controls output format and `\N` is the only supported `NULL` representation for TSV input format.

Default value: `\N`.

## output_format_json_array_of_rows {#output-format-json-array-of-rows}

Enables the ability to output all rows as a JSON array in the [JSONEachRow](../../interfaces/formats.md#jsoneachrow) format.

Possible values:

-   1 — ClickHouse outputs all rows as an array, each row in the `JSONEachRow` format.
-   0 — ClickHouse outputs each row separately in the `JSONEachRow` format.

Default value: `0`.

**Example of a query with the enabled setting**

Query:

```sql
SET output_format_json_array_of_rows = 1;
SELECT number FROM numbers(3) FORMAT JSONEachRow;
```

Result:

```text
[
{"number":"0"},
{"number":"1"},
{"number":"2"}                                                                                                                                                                                  
]
```

**Example of a query with the disabled setting**

Query:

```sql
SET output_format_json_array_of_rows = 0;
SELECT number FROM numbers(3) FORMAT JSONEachRow;
```

Result:

```text
{"number":"0"}
{"number":"1"}
{"number":"2"}
```

=======
## allow_nullable_key {#allow-nullable-key}

Allows using of the [Nullable](../../sql-reference/data-types/nullable.md#data_type-nullable)-typed values in a sorting and a primary key for [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md#table_engines-mergetree) tables.

Possible values:

- 1 — `Nullable`-type expressions are allowed in keys.
- 0 — `Nullable`-type expressions are not allowed in keys.

Default value: `0`.

[Original article](https://clickhouse.tech/docs/en/operations/settings/settings/) <!-- hide -->
