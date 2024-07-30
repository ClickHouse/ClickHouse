---
sidebar_label: Core Settings
sidebar_position: 2
slug: /en/operations/settings/settings
toc_max_heading_level: 2
---import DeprecatedBadge from '@theme/badges/DeprecatedBadge';



# Core Settings

All below settings are also available in table [system.settings](/docs/en/operations/system-tables/settings).

## additional_table_filters

An additional filter expression that is applied after reading
from the specified table.

Default value: 0.

**Example**

``` sql
INSERT INTO table_1 VALUES (1, 'a'), (2, 'bb'), (3, 'ccc'), (4, 'dddd');
SELECT * FROM table_1;
```
```response
┌─x─┬─y────┐
│ 1 │ a    │
│ 2 │ bb   │
│ 3 │ ccc  │
│ 4 │ dddd │
└───┴──────┘
```
```sql
SELECT *
FROM table_1
SETTINGS additional_table_filters = {'table_1': 'x != 2'}
```
```response
┌─x─┬─y────┐
│ 1 │ a    │
│ 3 │ ccc  │
│ 4 │ dddd │
└───┴──────┘
```

## additional_result_filter

An additional filter expression to apply to the result of `SELECT` query.
This setting is not applied to any subquery.

Default value: `''`.

**Example**

``` sql
INSERT INTO table_1 VALUES (1, 'a'), (2, 'bb'), (3, 'ccc'), (4, 'dddd');
SElECT * FROM table_1;
```
```response
┌─x─┬─y────┐
│ 1 │ a    │
│ 2 │ bb   │
│ 3 │ ccc  │
│ 4 │ dddd │
└───┴──────┘
```
```sql
SELECT *
FROM table_1
SETTINGS additional_result_filter = 'x != 2'
```
```response
┌─x─┬─y────┐
│ 1 │ a    │
│ 3 │ ccc  │
│ 4 │ dddd │
└───┴──────┘
```

## allow_nondeterministic_mutations {#allow_nondeterministic_mutations}

User-level setting that allows mutations on replicated tables to make use of non-deterministic functions such as `dictGet`.

Given that, for example, dictionaries, can be out of sync across nodes, mutations that pull values from them are disallowed on replicated tables by default. Enabling this setting allows this behavior, making it the user's responsibility to ensure that the data used is in sync across all nodes.

Default value: 0.

**Example**

``` xml
<profiles>
    <default>
        <allow_nondeterministic_mutations>1</allow_nondeterministic_mutations>

        <!-- ... -->
    </default>

    <!-- ... -->

</profiles>
```

## mutations_execute_nondeterministic_on_initiator {#mutations_execute_nondeterministic_on_initiator}

If true constant nondeterministic functions (e.g. function `now()`) are executed on initiator and replaced to literals in `UPDATE` and `DELETE` queries. It helps to keep data in sync on replicas while executing mutations with constant nondeterministic functions. Default value: `false`.

## mutations_execute_subqueries_on_initiator {#mutations_execute_subqueries_on_initiator}

If true scalar subqueries are executed on initiator and replaced to literals in `UPDATE` and `DELETE` queries. Default value: `false`.

## mutations_max_literal_size_to_replace {#mutations_max_literal_size_to_replace}

The maximum size of serialized literal in bytes to replace in `UPDATE` and `DELETE` queries. Takes effect only if at least one the two settings above is enabled. Default value: 16384 (16 KiB).

## distributed_product_mode {#distributed-product-mode}

Changes the behaviour of [distributed subqueries](../../sql-reference/operators/in.md).

ClickHouse applies this setting when the query contains the product of distributed tables, i.e. when the query for a distributed table contains a non-GLOBAL subquery for the distributed table.

Restrictions:

- Only applied for IN and JOIN subqueries.
- Only if the FROM section uses a distributed table containing more than one shard.
- If the subquery concerns a distributed table containing more than one shard.
- Not used for a table-valued [remote](../../sql-reference/table-functions/remote.md) function.

Possible values:

- `deny` — Default value. Prohibits using these types of subqueries (returns the “Double-distributed in/JOIN subqueries is denied” exception).
- `local` — Replaces the database and table in the subquery with local ones for the destination server (shard), leaving the normal `IN`/`JOIN.`
- `global` — Replaces the `IN`/`JOIN` query with `GLOBAL IN`/`GLOBAL JOIN.`
- `allow` — Allows the use of these types of subqueries.

## prefer_global_in_and_join {#prefer-global-in-and-join}

Enables the replacement of `IN`/`JOIN` operators with `GLOBAL IN`/`GLOBAL JOIN`.

Possible values:

- 0 — Disabled. `IN`/`JOIN` operators are not replaced with `GLOBAL IN`/`GLOBAL JOIN`.
- 1 — Enabled. `IN`/`JOIN` operators are replaced with `GLOBAL IN`/`GLOBAL JOIN`.

Default value: `0`.

**Usage**

Although `SET distributed_product_mode=global` can change the queries behavior for the distributed tables, it's not suitable for local tables or tables from external resources. Here is when the `prefer_global_in_and_join` setting comes into play.

For example, we have query serving nodes that contain local tables, which are not suitable for distribution. We need to scatter their data on the fly during distributed processing with the `GLOBAL` keyword — `GLOBAL IN`/`GLOBAL JOIN`.

Another use case of `prefer_global_in_and_join` is accessing tables created by external engines. This setting helps to reduce the number of calls to external sources while joining such tables: only one call per query.

**See also:**

- [Distributed subqueries](../../sql-reference/operators/in.md/#select-distributed-subqueries) for more information on how to use `GLOBAL IN`/`GLOBAL JOIN`

## enable_optimize_predicate_expression {#enable-optimize-predicate-expression}

Turns on predicate pushdown in `SELECT` queries.

Predicate pushdown may significantly reduce network traffic for distributed queries.

Possible values:

- 0 — Disabled.
- 1 — Enabled.

Default value: 1.

Usage

Consider the following queries:

1.  `SELECT count() FROM test_table WHERE date = '2018-10-10'`
2.  `SELECT count() FROM (SELECT * FROM test_table) WHERE date = '2018-10-10'`

If `enable_optimize_predicate_expression = 1`, then the execution time of these queries is equal because ClickHouse applies `WHERE` to the subquery when processing it.

If `enable_optimize_predicate_expression = 0`, then the execution time of the second query is much longer because the `WHERE` clause applies to all the data after the subquery finishes.

## fallback_to_stale_replicas_for_distributed_queries {#fallback_to_stale_replicas_for_distributed_queries}

Forces a query to an out-of-date replica if updated data is not available. See [Replication](../../engines/table-engines/mergetree-family/replication.md).

ClickHouse selects the most relevant from the outdated replicas of the table.

Used when performing `SELECT` from a distributed table that points to replicated tables.

By default, 1 (enabled).

## force_index_by_date {#force_index_by_date}

Disables query execution if the index can’t be used by date.

Works with tables in the MergeTree family.

If `force_index_by_date=1`, ClickHouse checks whether the query has a date key condition that can be used for restricting data ranges. If there is no suitable condition, it throws an exception. However, it does not check whether the condition reduces the amount of data to read. For example, the condition `Date != ' 2000-01-01 '` is acceptable even when it matches all the data in the table (i.e., running the query requires a full scan). For more information about ranges of data in MergeTree tables, see [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md).

## force_primary_key {#force-primary-key}

Disables query execution if indexing by the primary key is not possible.

Works with tables in the MergeTree family.

If `force_primary_key=1`, ClickHouse checks to see if the query has a primary key condition that can be used for restricting data ranges. If there is no suitable condition, it throws an exception. However, it does not check whether the condition reduces the amount of data to read. For more information about data ranges in MergeTree tables, see [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md).

## use_skip_indexes {#use_skip_indexes}

Use data skipping indexes during query execution.

Possible values:

- 0 — Disabled.
- 1 — Enabled.

Default value: 1.

## force_data_skipping_indices {#force_data_skipping_indices}

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

## ignore_data_skipping_indices {#ignore_data_skipping_indices}

Ignores the skipping indexes specified if used by the query.

Consider the following example:

```sql
CREATE TABLE data
(
    key Int,
    x Int,
    y Int,
    INDEX x_idx x TYPE minmax GRANULARITY 1,
    INDEX y_idx y TYPE minmax GRANULARITY 1,
    INDEX xy_idx (x,y) TYPE minmax GRANULARITY 1
)
Engine=MergeTree()
ORDER BY key;

INSERT INTO data VALUES (1, 2, 3);

SELECT * FROM data;
SELECT * FROM data SETTINGS ignore_data_skipping_indices=''; -- query will produce CANNOT_PARSE_TEXT error.
SELECT * FROM data SETTINGS ignore_data_skipping_indices='x_idx'; -- Ok.
SELECT * FROM data SETTINGS ignore_data_skipping_indices='na_idx'; -- Ok.

SELECT * FROM data WHERE x = 1 AND y = 1 SETTINGS ignore_data_skipping_indices='xy_idx',force_data_skipping_indices='xy_idx' ; -- query will produce INDEX_NOT_USED error, since xy_idx is explictly ignored.
SELECT * FROM data WHERE x = 1 AND y = 2 SETTINGS ignore_data_skipping_indices='xy_idx';
```

The query without ignoring any indexes:
```sql
EXPLAIN indexes = 1 SELECT * FROM data WHERE x = 1 AND y = 2;

Expression ((Projection + Before ORDER BY))
  Filter (WHERE)
    ReadFromMergeTree (default.data)
    Indexes:
      PrimaryKey
        Condition: true
        Parts: 1/1
        Granules: 1/1
      Skip
        Name: x_idx
        Description: minmax GRANULARITY 1
        Parts: 0/1
        Granules: 0/1
      Skip
        Name: y_idx
        Description: minmax GRANULARITY 1
        Parts: 0/0
        Granules: 0/0
      Skip
        Name: xy_idx
        Description: minmax GRANULARITY 1
        Parts: 0/0
        Granules: 0/0
```

Ignoring the `xy_idx` index:
```sql
EXPLAIN indexes = 1 SELECT * FROM data WHERE x = 1 AND y = 2 SETTINGS ignore_data_skipping_indices='xy_idx';

Expression ((Projection + Before ORDER BY))
  Filter (WHERE)
    ReadFromMergeTree (default.data)
    Indexes:
      PrimaryKey
        Condition: true
        Parts: 1/1
        Granules: 1/1
      Skip
        Name: x_idx
        Description: minmax GRANULARITY 1
        Parts: 0/1
        Granules: 0/1
      Skip
        Name: y_idx
        Description: minmax GRANULARITY 1
        Parts: 0/0
        Granules: 0/0
```

Works with tables in the MergeTree family.

## convert_query_to_cnf {#convert_query_to_cnf}

When set to `true`, a `SELECT` query will be converted to conjuctive normal form (CNF). There are scenarios where rewriting a query in CNF may execute faster (view this [Github issue](https://github.com/ClickHouse/ClickHouse/issues/11749) for an explanation).

For example, notice how the following `SELECT` query is not modified (the default behavior):

```sql
EXPLAIN SYNTAX
SELECT *
FROM
(
    SELECT number AS x
    FROM numbers(20)
) AS a
WHERE ((x >= 1) AND (x <= 5)) OR ((x >= 10) AND (x <= 15))
SETTINGS convert_query_to_cnf = false;
```

The result is:

```response
┌─explain────────────────────────────────────────────────────────┐
│ SELECT x                                                       │
│ FROM                                                           │
│ (                                                              │
│     SELECT number AS x                                         │
│     FROM numbers(20)                                           │
│     WHERE ((x >= 1) AND (x <= 5)) OR ((x >= 10) AND (x <= 15)) │
│ ) AS a                                                         │
│ WHERE ((x >= 1) AND (x <= 5)) OR ((x >= 10) AND (x <= 15))     │
│ SETTINGS convert_query_to_cnf = 0                              │
└────────────────────────────────────────────────────────────────┘
```

Let's set `convert_query_to_cnf` to `true` and see what changes:

```sql
EXPLAIN SYNTAX
SELECT *
FROM
(
    SELECT number AS x
    FROM numbers(20)
) AS a
WHERE ((x >= 1) AND (x <= 5)) OR ((x >= 10) AND (x <= 15))
SETTINGS convert_query_to_cnf = true;
```

Notice the `WHERE` clause is rewritten in CNF, but the result set is the identical - the Boolean logic is unchanged:

```response
┌─explain───────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ SELECT x                                                                                                              │
│ FROM                                                                                                                  │
│ (                                                                                                                     │
│     SELECT number AS x                                                                                                │
│     FROM numbers(20)                                                                                                  │
│     WHERE ((x <= 15) OR (x <= 5)) AND ((x <= 15) OR (x >= 1)) AND ((x >= 10) OR (x <= 5)) AND ((x >= 10) OR (x >= 1)) │
│ ) AS a                                                                                                                │
│ WHERE ((x >= 10) OR (x >= 1)) AND ((x >= 10) OR (x <= 5)) AND ((x <= 15) OR (x >= 1)) AND ((x <= 15) OR (x <= 5))     │
│ SETTINGS convert_query_to_cnf = 1                                                                                     │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

Possible values: true, false

Default value: false


## fsync_metadata {#fsync-metadata}

Enables or disables [fsync](http://pubs.opengroup.org/onlinepubs/9699919799/functions/fsync.html) when writing `.sql` files. Enabled by default.

It makes sense to disable it if the server has millions of tiny tables that are constantly being created and destroyed.

## function_range_max_elements_in_block {#function_range_max_elements_in_block}

Sets the safety threshold for data volume generated by function [range](../../sql-reference/functions/array-functions.md/#range). Defines the maximum number of values generated by function per block of data (sum of array sizes for every row in a block).

Possible values:

- Positive integer.

Default value: `500,000,000`.

**See Also**

- [max_block_size](#setting-max_block_size)
- [min_insert_block_size_rows](#min-insert-block-size-rows)

## enable_http_compression {#enable_http_compression}

Enables or disables data compression in the response to an HTTP request.

For more information, read the [HTTP interface description](../../interfaces/http.md).

Possible values:

- 0 — Disabled.
- 1 — Enabled.

Default value: 0.

## http_zlib_compression_level {#http_zlib_compression_level}

Sets the level of data compression in the response to an HTTP request if [enable_http_compression = 1](#enable_http_compression).

Possible values: Numbers from 1 to 9.

Default value: 3.

## http_native_compression_disable_checksumming_on_decompress {#http_native_compression_disable_checksumming_on_decompress}

Enables or disables checksum verification when decompressing the HTTP POST data from the client. Used only for ClickHouse native compression format (not used with `gzip` or `deflate`).

For more information, read the [HTTP interface description](../../interfaces/http.md).

Possible values:

- 0 — Disabled.
- 1 — Enabled.

Default value: 0.

## http_max_uri_size {#http-max-uri-size}

Sets the maximum URI length of an HTTP request.

Possible values:

- Positive integer.

Default value: 1048576.

## http_make_head_request {#http-make-head-request}

The `http_make_head_request` setting allows the execution of a `HEAD` request while reading data from HTTP to retrieve information about the file to be read, such as its size. Since it's enabled by default, it may be desirable to disable this setting in cases where the server does not support `HEAD` requests.

Default value: `true`.

## table_function_remote_max_addresses {#table_function_remote_max_addresses}

Sets the maximum number of addresses generated from patterns for the [remote](../../sql-reference/table-functions/remote.md) function.

Possible values:

- Positive integer.

Default value: `1000`.

##  glob_expansion_max_elements  {#glob_expansion_max_elements}

Sets the maximum number of addresses generated from patterns for external storages and table functions (like [url](../../sql-reference/table-functions/url.md)) except the `remote` function.

Possible values:

- Positive integer.

Default value: `1000`.

## send_progress_in_http_headers {#send_progress_in_http_headers}

Enables or disables `X-ClickHouse-Progress` HTTP response headers in `clickhouse-server` responses.

For more information, read the [HTTP interface description](../../interfaces/http.md).

Possible values:

- 0 — Disabled.
- 1 — Enabled.

Default value: 0.

## max_http_get_redirects {#setting-max_http_get_redirects}

Limits the maximum number of HTTP GET redirect hops for [URL](../../engines/table-engines/special/url.md)-engine tables. The setting applies to both types of tables: those created by the [CREATE TABLE](../../sql-reference/statements/create/table.md) query and by the [url](../../sql-reference/table-functions/url.md) table function.

Possible values:

- Any positive integer number of hops.
- 0 — No hops allowed.

Default value: `0`.

Cloud default value: `10`.

## insert_null_as_default {#insert_null_as_default}

Enables or disables the insertion of [default values](../../sql-reference/statements/create/table.md/#create-default-values) instead of [NULL](../../sql-reference/syntax.md/#null-literal) into columns with not [nullable](../../sql-reference/data-types/nullable.md/#data_type-nullable) data type.
If column type is not nullable and this setting is disabled, then inserting `NULL` causes an exception. If column type is nullable, then `NULL` values are inserted as is, regardless of this setting.

This setting is applicable to [INSERT ... SELECT](../../sql-reference/statements/insert-into.md/#inserting-the-results-of-select) queries. Note that `SELECT` subqueries may be concatenated with `UNION ALL` clause.

Possible values:

- 0 — Inserting `NULL` into a not nullable column causes an exception.
- 1 — Default column value is inserted instead of `NULL`.

Default value: `1`.

## join_default_strictness {#join_default_strictness}

Sets default strictness for [JOIN clauses](../../sql-reference/statements/select/join.md/#select-join).

Possible values:

- `ALL` — If the right table has several matching rows, ClickHouse creates a [Cartesian product](https://en.wikipedia.org/wiki/Cartesian_product) from matching rows. This is the normal `JOIN` behaviour from standard SQL.
- `ANY` — If the right table has several matching rows, only the first one found is joined. If the right table has only one matching row, the results of `ANY` and `ALL` are the same.
- `ASOF` — For joining sequences with an uncertain match.
- `Empty string` — If `ALL` or `ANY` is not specified in the query, ClickHouse throws an exception.

Default value: `ALL`.

## join_algorithm {#join_algorithm}

Specifies which [JOIN](../../sql-reference/statements/select/join.md) algorithm is used.

Several algorithms can be specified, and an available one would be chosen for a particular query based on kind/strictness and table engine.

Possible values:

- default

 This is the equivalent of `hash` or `direct`, if possible (same as `direct,hash`)

- grace_hash

 [Grace hash join](https://en.wikipedia.org/wiki/Hash_join#Grace_hash_join) is used.  Grace hash provides an algorithm option that provides performant complex joins while limiting memory use.

 The first phase of a grace join reads the right table and splits it into N buckets depending on the hash value of key columns (initially, N is `grace_hash_join_initial_buckets`). This is done in a way to ensure that each bucket can be processed independently. Rows from the first bucket are added to an in-memory hash table while the others are saved to disk. If the hash table grows beyond the memory limit (e.g., as set by [`max_bytes_in_join`](/docs/en/operations/settings/query-complexity.md/#max_bytes_in_join)), the number of buckets is increased and the assigned bucket for each row. Any rows which don’t belong to the current bucket are flushed and reassigned.

 Supports `INNER/LEFT/RIGHT/FULL ALL/ANY JOIN`.

- hash

 [Hash join algorithm](https://en.wikipedia.org/wiki/Hash_join) is used. The most generic implementation that supports all combinations of kind and strictness and multiple join keys that are combined with `OR` in the `JOIN ON` section.

- parallel_hash

 A variation of `hash` join that splits the data into buckets and builds several hashtables instead of one concurrently to speed up this process.

 When using the `hash` algorithm, the right part of `JOIN` is uploaded into RAM.

- partial_merge

 A variation of the [sort-merge algorithm](https://en.wikipedia.org/wiki/Sort-merge_join), where only the right table is fully sorted.

 The `RIGHT JOIN` and `FULL JOIN` are supported only with `ALL` strictness (`SEMI`, `ANTI`, `ANY`, and `ASOF` are not supported).

 When using the `partial_merge` algorithm, ClickHouse sorts the data and dumps it to the disk. The `partial_merge` algorithm in ClickHouse differs slightly from the classic realization. First, ClickHouse sorts the right table by joining keys in blocks and creates a min-max index for sorted blocks. Then it sorts parts of the left table by the `join key` and joins them over the right table. The min-max index is also used to skip unneeded right table blocks.

- direct

 This algorithm can be applied when the storage for the right table supports key-value requests.

 The `direct` algorithm performs a lookup in the right table using rows from the left table as keys. It's supported only by special storage such as [Dictionary](../../engines/table-engines/special/dictionary.md/#dictionary) or [EmbeddedRocksDB](../../engines/table-engines/integrations/embedded-rocksdb.md) and only the `LEFT` and `INNER` JOINs.

- auto

 When set to `auto`, `hash` join is tried first, and the algorithm is switched on the fly to another algorithm if the memory limit is violated.

- full_sorting_merge

 [Sort-merge algorithm](https://en.wikipedia.org/wiki/Sort-merge_join) with full sorting joined tables before joining.

- prefer_partial_merge

 ClickHouse always tries to use `partial_merge` join if possible, otherwise, it uses `hash`. *Deprecated*, same as `partial_merge,hash`.


## join_any_take_last_row {#join_any_take_last_row}

Changes the behaviour of join operations with `ANY` strictness.

:::note
This setting applies only for `JOIN` operations with [Join](../../engines/table-engines/special/join.md) engine tables.
:::

Possible values:

- 0 — If the right table has more than one matching row, only the first one found is joined.
- 1 — If the right table has more than one matching row, only the last one found is joined.

Default value: 0.

See also:

- [JOIN clause](../../sql-reference/statements/select/join.md/#select-join)
- [Join table engine](../../engines/table-engines/special/join.md)
- [join_default_strictness](#join_default_strictness)

## join_use_nulls {#join_use_nulls}

Sets the type of [JOIN](../../sql-reference/statements/select/join.md) behaviour. When merging tables, empty cells may appear. ClickHouse fills them differently based on this setting.

Possible values:

- 0 — The empty cells are filled with the default value of the corresponding field type.
- 1 — `JOIN` behaves the same way as in standard SQL. The type of the corresponding field is converted to [Nullable](../../sql-reference/data-types/nullable.md/#data_type-nullable), and empty cells are filled with [NULL](../../sql-reference/syntax.md).

Default value: 0.

## group_by_use_nulls {#group_by_use_nulls}

Changes the way the [GROUP BY clause](/docs/en/sql-reference/statements/select/group-by.md) treats the types of aggregation keys.
When the `ROLLUP`, `CUBE`, or `GROUPING SETS` specifiers are used, some aggregation keys may not be used to produce some result rows.
Columns for these keys are filled with either default value or `NULL` in corresponding rows depending on this setting.

Possible values:

- 0 — The default value for the aggregation key type is used to produce missing values.
- 1 — ClickHouse executes `GROUP BY` the same way as the SQL standard says. The types of aggregation keys are converted to [Nullable](/docs/en/sql-reference/data-types/nullable.md/#data_type-nullable). Columns for corresponding aggregation keys are filled with [NULL](/docs/en/sql-reference/syntax.md) for rows that didn't use it.

Default value: 0.

See also:

- [GROUP BY clause](/docs/en/sql-reference/statements/select/group-by.md)

## partial_merge_join_optimizations {#partial_merge_join_optimizations}

Disables optimizations in partial merge join algorithm for [JOIN](../../sql-reference/statements/select/join.md) queries.

By default, this setting enables improvements that could lead to wrong results. If you see suspicious results in your queries, disable optimizations by this setting. Optimizations can be different in different versions of the ClickHouse server.

Possible values:

- 0 — Optimizations disabled.
- 1 — Optimizations enabled.

Default value: 1.

## partial_merge_join_rows_in_right_blocks {#partial_merge_join_rows_in_right_blocks}

Limits sizes of right-hand join data blocks in partial merge join algorithm for [JOIN](../../sql-reference/statements/select/join.md) queries.

ClickHouse server:

1.  Splits right-hand join data into blocks with up to the specified number of rows.
2.  Indexes each block with its minimum and maximum values.
3.  Unloads prepared blocks to disk if it is possible.

Possible values:

- Any positive integer. Recommended range of values: \[1000, 100000\].

Default value: 65536.

## join_on_disk_max_files_to_merge {#join_on_disk_max_files_to_merge}

Limits the number of files allowed for parallel sorting in MergeJoin operations when they are executed on disk.

The bigger the value of the setting, the more RAM is used and the less disk I/O is needed.

Possible values:

- Any positive integer, starting from 2.

Default value: 64.

## any_join_distinct_right_table_keys {#any_join_distinct_right_table_keys}

Enables legacy ClickHouse server behaviour in `ANY INNER|LEFT JOIN` operations.

:::note
Use this setting only for backward compatibility if your use cases depend on legacy `JOIN` behaviour.
:::

When the legacy behaviour is enabled:

- Results of `t1 ANY LEFT JOIN t2` and `t2 ANY RIGHT JOIN t1` operations are not equal because ClickHouse uses the logic with many-to-one left-to-right table keys mapping.
- Results of `ANY INNER JOIN` operations contain all rows from the left table like the `SEMI LEFT JOIN` operations do.

When the legacy behaviour is disabled:

- Results of `t1 ANY LEFT JOIN t2` and `t2 ANY RIGHT JOIN t1` operations are equal because ClickHouse uses the logic which provides one-to-many keys mapping in `ANY RIGHT JOIN` operations.
- Results of `ANY INNER JOIN` operations contain one row per key from both the left and right tables.

Possible values:

- 0 — Legacy behaviour is disabled.
- 1 — Legacy behaviour is enabled.

Default value: 0.

See also:

- [JOIN strictness](../../sql-reference/statements/select/join.md/#join-settings)

## max_rows_in_set_to_optimize_join

Maximal size of the set to filter joined tables by each other's row sets before joining.

Possible values:

- 0 — Disable.
- Any positive integer.

Default value: 100000.

## temporary_files_codec {#temporary_files_codec}

Sets compression codec for temporary files used in sorting and joining operations on disk.

Possible values:

- LZ4 — [LZ4](https://en.wikipedia.org/wiki/LZ4_(compression_algorithm)) compression is applied.
- NONE — No compression is applied.

Default value: LZ4.

## max_block_size {#setting-max_block_size}

In ClickHouse, data is processed by blocks, which are sets of column parts. The internal processing cycles for a single block are efficient but there are noticeable costs when processing each block.

The `max_block_size` setting indicates the recommended maximum number of rows to include in a single block when loading data from tables. Blocks the size of `max_block_size` are not always loaded from the table: if ClickHouse determines that less data needs to be retrieved, a smaller block is processed.

The block size should not be too small to avoid noticeable costs when processing each block. It should also not be too large to ensure that queries with a LIMIT clause execute quickly after processing the first block. When setting `max_block_size`, the goal should be to avoid consuming too much memory when extracting a large number of columns in multiple threads and to preserve at least some cache locality.

Default value: `65,409`

## preferred_block_size_bytes {#preferred-block-size-bytes}

Used for the same purpose as `max_block_size`, but it sets the recommended block size in bytes by adapting it to the number of rows in the block.
However, the block size cannot be more than `max_block_size` rows.
By default: 1,000,000. It only works when reading from MergeTree engines.

## max_concurrent_queries_for_user {#max-concurrent-queries-for-user}

The maximum number of simultaneously processed queries per user.

Possible values:

- Positive integer.
- 0 — No limit.

Default value: `0`.

**Example**

``` xml
<max_concurrent_queries_for_user>5</max_concurrent_queries_for_user>
```

## max_concurrent_queries_for_all_users {#max-concurrent-queries-for-all-users}

Throw exception if the value of this setting is less or equal than the current number of simultaneously processed queries.

Example: `max_concurrent_queries_for_all_users` can be set to 99 for all users and database administrator can set it to 100 for itself to run queries for investigation even when the server is overloaded.

Modifying the setting for one query or user does not affect other queries.

Possible values:

- Positive integer.
- 0 — No limit.

Default value: `0`.

**Example**

``` xml
<max_concurrent_queries_for_all_users>99</max_concurrent_queries_for_all_users>
```

**See Also**

- [max_concurrent_queries](/docs/en/operations/server-configuration-parameters/settings.md/#max_concurrent_queries)

## merge_tree_min_rows_for_concurrent_read {#setting-merge-tree-min-rows-for-concurrent-read}

If the number of rows to be read from a file of a [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) table exceeds `merge_tree_min_rows_for_concurrent_read` then ClickHouse tries to perform a concurrent reading from this file on several threads.

Possible values:

- Positive integer.

Default value: `163840`.

## merge_tree_min_rows_for_concurrent_read_for_remote_filesystem {#merge-tree-min-rows-for-concurrent-read-for-remote-filesystem}

The minimum number of lines to read from one file before the [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) engine can parallelize reading, when reading from remote filesystem.

Possible values:

- Positive integer.

Default value: `163840`.

## merge_tree_min_bytes_for_concurrent_read {#setting-merge-tree-min-bytes-for-concurrent-read}

If the number of bytes to read from one file of a [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md)-engine table exceeds `merge_tree_min_bytes_for_concurrent_read`, then ClickHouse tries to concurrently read from this file in several threads.

Possible value:

- Positive integer.

Default value: `251658240`.

## merge_tree_min_bytes_for_concurrent_read_for_remote_filesystem {#merge-tree-min-bytes-for-concurrent-read-for-remote-filesystem}

The minimum number of bytes to read from one file before [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) engine can parallelize reading, when reading from remote filesystem.

Possible values:

- Positive integer.

Default value: `251658240`.

## merge_tree_min_rows_for_seek {#setting-merge-tree-min-rows-for-seek}

If the distance between two data blocks to be read in one file is less than `merge_tree_min_rows_for_seek` rows, then ClickHouse does not seek through the file but reads the data sequentially.

Possible values:

- Any positive integer.

Default value: 0.

## merge_tree_min_bytes_for_seek {#setting-merge-tree-min-bytes-for-seek}

If the distance between two data blocks to be read in one file is less than `merge_tree_min_bytes_for_seek` bytes, then ClickHouse sequentially reads a range of file that contains both blocks, thus avoiding extra seek.

Possible values:

- Any positive integer.

Default value: 0.

## merge_tree_coarse_index_granularity {#setting-merge-tree-coarse-index-granularity}

When searching for data, ClickHouse checks the data marks in the index file. If ClickHouse finds that required keys are in some range, it divides this range into `merge_tree_coarse_index_granularity` subranges and searches the required keys there recursively.

Possible values:

- Any positive even integer.

Default value: 8.

## merge_tree_max_rows_to_use_cache {#setting-merge-tree-max-rows-to-use-cache}

If ClickHouse should read more than `merge_tree_max_rows_to_use_cache` rows in one query, it does not use the cache of uncompressed blocks.

The cache of uncompressed blocks stores data extracted for queries. ClickHouse uses this cache to speed up responses to repeated small queries. This setting protects the cache from trashing by queries that read a large amount of data. The [uncompressed_cache_size](../../operations/server-configuration-parameters/settings.md/#server-settings-uncompressed_cache_size) server setting defines the size of the cache of uncompressed blocks.

Possible values:

- Any positive integer.

Default value: 128 ✕ 8192.

## merge_tree_max_bytes_to_use_cache {#setting-merge-tree-max-bytes-to-use-cache}

If ClickHouse should read more than `merge_tree_max_bytes_to_use_cache` bytes in one query, it does not use the cache of uncompressed blocks.

The cache of uncompressed blocks stores data extracted for queries. ClickHouse uses this cache to speed up responses to repeated small queries. This setting protects the cache from trashing by queries that read a large amount of data. The [uncompressed_cache_size](../../operations/server-configuration-parameters/settings.md/#server-settings-uncompressed_cache_size) server setting defines the size of the cache of uncompressed blocks.

Possible values:

- Any positive integer.

Default value: 2013265920.

## min_bytes_to_use_direct_io {#min-bytes-to-use-direct-io}

The minimum data volume required for using direct I/O access to the storage disk.

ClickHouse uses this setting when reading data from tables. If the total storage volume of all the data to be read exceeds `min_bytes_to_use_direct_io` bytes, then ClickHouse reads the data from the storage disk with the `O_DIRECT` option.

Possible values:

- 0 — Direct I/O is disabled.
- Positive integer.

Default value: 0.

## network_compression_method {#network_compression_method}

Sets the method of data compression that is used for communication between servers and between server and [clickhouse-client](../../interfaces/cli.md).

Possible values:

- `LZ4` — sets LZ4 compression method.
- `ZSTD` — sets ZSTD compression method.

Default value: `LZ4`.

**See Also**

- [network_zstd_compression_level](#network_zstd_compression_level)

## network_zstd_compression_level {#network_zstd_compression_level}

Adjusts the level of ZSTD compression. Used only when [network_compression_method](#network_compression_method) is set to `ZSTD`.

Possible values:

- Positive integer from 1 to 15.

Default value: `1`.

## log_queries {#log-queries}

Setting up query logging.

Queries sent to ClickHouse with this setup are logged according to the rules in the [query_log](../../operations/server-configuration-parameters/settings.md/#server_configuration_parameters-query-log) server configuration parameter.

Example:

``` text
log_queries=1
```

## log_queries_min_query_duration_ms {#log-queries-min-query-duration-ms}

If enabled (non-zero), queries faster than the value of this setting will not be logged (you can think about this as a `long_query_time` for [MySQL Slow Query Log](https://dev.mysql.com/doc/refman/5.7/en/slow-query-log.html)), and this basically means that you will not find them in the following tables:

- `system.query_log`
- `system.query_thread_log`

Only the queries with the following type will get to the log:

- `QUERY_FINISH`
- `EXCEPTION_WHILE_PROCESSING`

- Type: milliseconds
- Default value: 0 (any query)

## log_queries_min_type {#log-queries-min-type}

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

## log_query_threads {#log-query-threads}

Setting up query threads logging.

Query threads log into the [system.query_thread_log](../../operations/system-tables/query_thread_log.md) table. This setting has effect only when [log_queries](#log-queries) is true. Queries’ threads run by ClickHouse with this setup are logged according to the rules in the [query_thread_log](../../operations/server-configuration-parameters/settings.md/#server_configuration_parameters-query_thread_log) server configuration parameter.

Possible values:

- 0 — Disabled.
- 1 — Enabled.

Default value: `1`.

**Example**

``` text
log_query_threads=1
```

## log_query_views {#log-query-views}

Setting up query views logging.

When a query run by ClickHouse with this setting enabled has associated views (materialized or live views), they are logged in the [query_views_log](../../operations/server-configuration-parameters/settings.md/#server_configuration_parameters-query_views_log) server configuration parameter.

Example:

``` text
log_query_views=1
```

## log_formatted_queries {#log-formatted-queries}

Allows to log formatted queries to the [system.query_log](../../operations/system-tables/query_log.md) system table (populates `formatted_query` column in the [system.query_log](../../operations/system-tables/query_log.md)).

Possible values:

- 0 — Formatted queries are not logged in the system table.
- 1 — Formatted queries are logged in the system table.

Default value: `0`.

## log_comment {#log-comment}

Specifies the value for the `log_comment` field of the [system.query_log](../system-tables/query_log.md) table and comment text for the server log.

It can be used to improve the readability of server logs. Additionally, it helps to select queries related to the test from the `system.query_log` after running [clickhouse-test](../../development/tests.md).

Possible values:

- Any string no longer than [max_query_size](#max_query_size). If the max_query_size is exceeded, the server throws an exception.

Default value: empty string.

**Example**

Query:

``` sql
SET log_comment = 'log_comment test', log_queries = 1;
SELECT 1;
SYSTEM FLUSH LOGS;
SELECT type, query FROM system.query_log WHERE log_comment = 'log_comment test' AND event_date >= yesterday() ORDER BY event_time DESC LIMIT 2;
```

Result:

``` text
┌─type────────┬─query─────┐
│ QueryStart  │ SELECT 1; │
│ QueryFinish │ SELECT 1; │
└─────────────┴───────────┘
```

## log_processors_profiles {#log_processors_profiles}

Write time that processor spent during execution/waiting for data to `system.processors_profile_log` table.

See also:

- [`system.processors_profile_log`](../../operations/system-tables/processors_profile_log.md)
- [`EXPLAIN PIPELINE`](../../sql-reference/statements/explain.md#explain-pipeline)

## max_insert_block_size {#max_insert_block_size}

The size of blocks (in a count of rows) to form for insertion into a table.
This setting only applies in cases when the server forms the blocks.
For example, for an INSERT via the HTTP interface, the server parses the data format and forms blocks of the specified size.
But when using clickhouse-client, the client parses the data itself, and the ‘max_insert_block_size’ setting on the server does not affect the size of the inserted blocks.
The setting also does not have a purpose when using INSERT SELECT, since data is inserted using the same blocks that are formed after SELECT.

Default value: 1,048,576.

The default is slightly more than `max_block_size`. The reason for this is that certain table engines (`*MergeTree`) form a data part on the disk for each inserted block, which is a fairly large entity. Similarly, `*MergeTree` tables sort data during insertion, and a large enough block size allow sorting more data in RAM.

## min_insert_block_size_rows {#min-insert-block-size-rows}

Sets the minimum number of rows in the block that can be inserted into a table by an `INSERT` query. Smaller-sized blocks are squashed into bigger ones.

Possible values:

- Positive integer.
- 0 — Squashing disabled.

Default value: 1048576.

## min_insert_block_size_bytes {#min-insert-block-size-bytes}

Sets the minimum number of bytes in the block which can be inserted into a table by an `INSERT` query. Smaller-sized blocks are squashed into bigger ones.

Possible values:

- Positive integer.
- 0 — Squashing disabled.

Default value: 268435456.

## max_replica_delay_for_distributed_queries {#max_replica_delay_for_distributed_queries}

Disables lagging replicas for distributed queries. See [Replication](../../engines/table-engines/mergetree-family/replication.md).

Sets the time in seconds. If a replica's lag is greater than or equal to the set value, this replica is not used.

Possible values:

- Positive integer.
- 0 — Replica lags are not checked.

To prevent the use of any replica with a non-zero lag, set this parameter to 1.

Default value: 300.

Used when performing `SELECT` from a distributed table that points to replicated tables.

## max_threads {#max_threads}

The maximum number of query processing threads, excluding threads for retrieving data from remote servers (see the ‘max_distributed_connections’ parameter).

This parameter applies to threads that perform the same stages of the query processing pipeline in parallel.
For example, when reading from a table, if it is possible to evaluate expressions with functions, filter with WHERE and pre-aggregate for GROUP BY in parallel using at least ‘max_threads’ number of threads, then ‘max_threads’ are used.

Default value: the number of physical CPU cores.

For queries that are completed quickly because of a LIMIT, you can set a lower ‘max_threads’. For example, if the necessary number of entries are located in every block and max_threads = 8, then 8 blocks are retrieved, although it would have been enough to read just one.

The smaller the `max_threads` value, the less memory is consumed.

## max_insert_threads {#max-insert-threads}

The maximum number of threads to execute the `INSERT SELECT` query.

Possible values:

- 0 (or 1) — `INSERT SELECT` no parallel execution.
- Positive integer. Bigger than 1.

Default value: `0`.

Cloud default value: from `2` to `4`, depending on the service size.

Parallel `INSERT SELECT` has effect only if the `SELECT` part is executed in parallel, see [max_threads](#max_threads) setting.
Higher values will lead to higher memory usage.

## max_compress_block_size {#max-compress-block-size}

The maximum size of blocks of uncompressed data before compressing for writing to a table. By default, 1,048,576 (1 MiB). Specifying a smaller block size generally leads to slightly reduced compression ratio, the compression and decompression speed increases slightly due to cache locality, and memory consumption is reduced.

:::note
This is an expert-level setting, and you shouldn't change it if you're just getting started with ClickHouse.
:::

Don’t confuse blocks for compression (a chunk of memory consisting of bytes) with blocks for query processing (a set of rows from a table).

## min_compress_block_size {#min-compress-block-size}

For [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) tables. In order to reduce latency when processing queries, a block is compressed when writing the next mark if its size is at least `min_compress_block_size`. By default, 65,536.

The actual size of the block, if the uncompressed data is less than `max_compress_block_size`, is no less than this value and no less than the volume of data for one mark.

Let’s look at an example. Assume that `index_granularity` was set to 8192 during table creation.

We are writing a UInt32-type column (4 bytes per value). When writing 8192 rows, the total will be 32 KB of data. Since min_compress_block_size = 65,536, a compressed block will be formed for every two marks.

We are writing a URL column with the String type (average size of 60 bytes per value). When writing 8192 rows, the average will be slightly less than 500 KB of data. Since this is more than 65,536, a compressed block will be formed for each mark. In this case, when reading data from the disk in the range of a single mark, extra data won’t be decompressed.

:::note
This is an expert-level setting, and you shouldn't change it if you're just getting started with ClickHouse.
:::

## max_query_size {#max_query_size}

The maximum number of bytes of a query string parsed by the SQL parser.
Data in the VALUES clause of INSERT queries is processed by a separate stream parser (that consumes O(1) RAM) and not affected by this restriction.

Default value: 262144 (= 256 KiB).

:::note
`max_query_size` cannot be set within an SQL query (e.g., `SELECT now() SETTINGS max_query_size=10000`) because ClickHouse needs to allocate a buffer to parse the query, and this buffer size is determined by the `max_query_size` setting, which must be configured before the query is executed.
:::

## max_parser_depth {#max_parser_depth}

Limits maximum recursion depth in the recursive descent parser. Allows controlling the stack size.

Possible values:

- Positive integer.
- 0 — Recursion depth is unlimited.

Default value: 1000.

## interactive_delay {#interactive-delay}

The interval in microseconds for checking whether request execution has been canceled and sending the progress.

Default value: 100,000 (checks for cancelling and sends the progress ten times per second).

## idle_connection_timeout {#idle_connection_timeout}

Timeout to close idle TCP connections after specified number of seconds.

Possible values:

- Positive integer (0 - close immediately, after 0 seconds).

Default value: 3600.

## connect_timeout, receive_timeout, send_timeout {#connect-timeout-receive-timeout-send-timeout}

Timeouts in seconds on the socket used for communicating with the client.

Default value: 10, 300, 300.

## handshake_timeout_ms {#handshake-timeout-ms}

Timeout in milliseconds for receiving Hello packet from replicas during handshake.

Default value: 10000.

## cancel_http_readonly_queries_on_client_close {#cancel-http-readonly-queries-on-client-close}

Cancels HTTP read-only queries (e.g. SELECT) when a client closes the connection without waiting for the response.

Default value: `0`.

Cloud default value: `1`.

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

## max_distributed_depth {#max-distributed-depth}

Limits the maximum depth of recursive queries for [Distributed](../../engines/table-engines/special/distributed.md) tables.

If the value is exceeded, the server throws an exception.

Possible values:

- Positive integer.
- 0 — Unlimited depth.

Default value: `5`.

## max_replicated_fetches_network_bandwidth_for_server {#max_replicated_fetches_network_bandwidth_for_server}

Limits the maximum speed of data exchange over the network in bytes per second for [replicated](../../engines/table-engines/mergetree-family/replication.md) fetches for the server. Only has meaning at server startup. You can also limit the speed for a particular table with [max_replicated_fetches_network_bandwidth](../../operations/settings/merge-tree-settings.md/#max_replicated_fetches_network_bandwidth) setting.

The setting isn't followed perfectly accurately.

Possible values:

- Positive integer.
- 0 — Unlimited.

Default value: `0`.

**Usage**

Could be used for throttling speed when replicating the data to add or replace new nodes.

:::note
60000000 bytes/s approximately corresponds to 457 Mbps (60000000 / 1024 / 1024 * 8).
:::

## max_replicated_sends_network_bandwidth_for_server {#max_replicated_sends_network_bandwidth_for_server}

Limits the maximum speed of data exchange over the network in bytes per second for [replicated](../../engines/table-engines/mergetree-family/replication.md) sends for the server. Only has meaning at server startup.  You can also limit the speed for a particular table with [max_replicated_sends_network_bandwidth](../../operations/settings/merge-tree-settings.md/#max_replicated_sends_network_bandwidth) setting.

The setting isn't followed perfectly accurately.

Possible values:

- Positive integer.
- 0 — Unlimited.

Default value: `0`.

**Usage**

Could be used for throttling speed when replicating the data to add or replace new nodes.

:::note
60000000 bytes/s approximately corresponds to 457 Mbps (60000000 / 1024 / 1024 * 8).
:::

## connect_timeout_with_failover_ms {#connect-timeout-with-failover-ms}

The timeout in milliseconds for connecting to a remote server for a Distributed table engine, if the ‘shard’ and ‘replica’ sections are used in the cluster definition.
If unsuccessful, several attempts are made to connect to various replicas.

Default value: 1000.

## connect_timeout_with_failover_secure_ms

Connection timeout for selecting first healthy replica (for secure connections)

Default value: 1000.

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

The wait time in milliseconds for reading messages from [Kafka](../../engines/table-engines/integrations/kafka.md/#kafka) before retry.

Possible values:

- Positive integer.
- 0 — Infinite timeout.

Default value: 5000.

See also:

- [Apache Kafka](https://kafka.apache.org/)

## kafka_disable_num_consumers_limit {#kafka-disable-num-consumers-limit}

Disable limit on kafka_num_consumers that depends on the number of available CPU cores.

Default value: false.

## postgresql_connection_pool_size {#postgresql-connection-pool-size}

Connection pool size for PostgreSQL table engine and database engine.

Default value: 16

## postgresql_connection_attempt_timeout {#postgresql-connection-attempt-timeout}

Connection timeout in seconds of a single attempt to connect PostgreSQL end-point.
The value is passed as a `connect_timeout` parameter of the connection URL.

Default value: `2`.

## postgresql_connection_pool_wait_timeout {#postgresql-connection-pool-wait-timeout}

Connection pool push/pop timeout on empty pool for PostgreSQL table engine and database engine. By default it will block on empty pool.

Default value: 5000

## postgresql_connection_pool_retries {#postgresql-connection-pool-retries}

The maximum number of retries to establish a connection with the PostgreSQL end-point.

Default value: `2`.

## postgresql_connection_pool_auto_close_connection {#postgresql-connection-pool-auto-close-connection}

Close connection before returning connection to the pool.

Default value: true.

## odbc_bridge_connection_pool_size {#odbc-bridge-connection-pool-size}

Connection pool size for each connection settings string in ODBC bridge.

Default value: 16

## odbc_bridge_use_connection_pooling {#odbc-bridge-use-connection-pooling}

Use connection pooling in ODBC bridge. If set to false, a new connection is created every time.

Default value: true

## use_uncompressed_cache {#setting-use_uncompressed_cache}

Whether to use a cache of uncompressed blocks. Accepts 0 or 1. By default, 0 (disabled).
Using the uncompressed cache (only for tables in the MergeTree family) can significantly reduce latency and increase throughput when working with a large number of short queries. Enable this setting for users who send frequent short requests. Also pay attention to the [uncompressed_cache_size](../../operations/server-configuration-parameters/settings.md/#server-settings-uncompressed_cache_size) configuration parameter (only set in the config file) – the size of uncompressed cache blocks. By default, it is 8 GiB. The uncompressed cache is filled in as needed and the least-used data is automatically deleted.

For queries that read at least a somewhat large volume of data (one million rows or more), the uncompressed cache is disabled automatically to save space for truly small queries. This means that you can keep the ‘use_uncompressed_cache’ setting always set to 1.

## replace_running_query {#replace-running-query}

When using the HTTP interface, the ‘query_id’ parameter can be passed. This is any string that serves as the query identifier.
If a query from the same user with the same ‘query_id’ already exists at this time, the behaviour depends on the ‘replace_running_query’ parameter.

`0` (default) – Throw an exception (do not allow the query to run if a query with the same ‘query_id’ is already running).

`1` – Cancel the old query and start running the new one.

Set this parameter to 1 for implementing suggestions for segmentation conditions. After entering the next character, if the old query hasn’t finished yet, it should be cancelled.

## replace_running_query_max_wait_ms {#replace-running-query-max-wait-ms}

The wait time for running the query with the same `query_id` to finish, when the [replace_running_query](#replace-running-query) setting is active.

Possible values:

- Positive integer.
- 0 — Throwing an exception that does not allow to run a new query if the server already executes a query with the same `query_id`.

Default value: 5000.

## stream_flush_interval_ms {#stream-flush-interval-ms}

Works for tables with streaming in the case of a timeout, or when a thread generates [max_insert_block_size](#max_insert_block_size) rows.

The default value is 7500.

The smaller the value, the more often data is flushed into the table. Setting the value too low leads to poor performance.

## stream_poll_timeout_ms {#stream_poll_timeout_ms}

Timeout for polling data from/to streaming storages.

Default value: 500.

## load_balancing {#load_balancing}

Specifies the algorithm of replicas selection that is used for distributed query processing.

ClickHouse supports the following algorithms of choosing replicas:

- [Random](#load_balancing-random) (by default)
- [Nearest hostname](#load_balancing-nearest_hostname)
- [Hostname levenshtein distance](#load_balancing-hostname_levenshtein_distance)
- [In order](#load_balancing-in_order)
- [First or random](#load_balancing-first_or_random)
- [Round robin](#load_balancing-round_robin)

See also:

- [distributed_replica_max_ignored_errors](#distributed_replica_max_ignored_errors)

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

The number of errors is counted for each replica. Every 5 minutes, the number of errors is integrally divided by 2. Thus, the number of errors is calculated for a recent time with exponential smoothing. If there is one replica with a minimal number of errors (i.e. errors occurred recently on the other replicas), the query is sent to it. If there are multiple replicas with the same minimal number of errors, the query is sent to the replica with a hostname that is most similar to the server’s hostname in the config file (for the number of different characters in identical positions, up to the minimum length of both hostnames).

For instance, example01-01-1 and example01-01-2 are different in one position, while example01-01-1 and example01-02-2 differ in two places.
This method might seem primitive, but it does not require external data about network topology, and it does not compare IP addresses, which would be complicated for our IPv6 addresses.

Thus, if there are equivalent replicas, the closest one by name is preferred.
We can also assume that when sending a query to the same server, in the absence of failures, a distributed query will also go to the same servers. So even if different data is placed on the replicas, the query will return mostly the same results.

### Hostname levenshtein distance {#load_balancing-hostname_levenshtein_distance}

``` sql
load_balancing = hostname_levenshtein_distance
```

Just like `nearest_hostname`, but it compares hostname in a [levenshtein distance](https://en.wikipedia.org/wiki/Levenshtein_distance) manner. For example:

``` text
example-clickhouse-0-0 ample-clickhouse-0-0
1

example-clickhouse-0-0 example-clickhouse-1-10
2

example-clickhouse-0-0 example-clickhouse-12-0
3
```

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

## prefer_localhost_replica {#prefer-localhost-replica}

Enables/disables preferable using the localhost replica when processing distributed queries.

Possible values:

- 1 — ClickHouse always sends a query to the localhost replica if it exists.
- 0 — ClickHouse uses the balancing strategy specified by the [load_balancing](#load_balancing) setting.

Default value: 1.

:::note
Disable this setting if you use [max_parallel_replicas](#max_parallel_replicas) without [parallel_replicas_custom_key](#parallel_replicas_custom_key).
If [parallel_replicas_custom_key](#parallel_replicas_custom_key) is set, disable this setting only if it's used on a cluster with multiple shards containing multiple replicas.
If it's used on a cluster with a single shard and multiple replicas, disabling this setting will have negative effects.
:::

## totals_mode {#totals-mode}

How to calculate TOTALS when HAVING is present, as well as when max_rows_to_group_by and group_by_overflow_mode = ‘any’ are present.
See the section “WITH TOTALS modifier”.

## totals_auto_threshold {#totals-auto-threshold}

The threshold for `totals_mode = 'auto'`.
See the section “WITH TOTALS modifier”.

## max_parallel_replicas {#max_parallel_replicas}

The maximum number of replicas for each shard when executing a query.

Possible values:

- Positive integer.

Default value: `1`.

**Additional Info**

This options will produce different results depending on the settings used.

:::note
This setting will produce incorrect results when joins or subqueries are involved, and all tables don't meet certain requirements. See [Distributed Subqueries and max_parallel_replicas](../../sql-reference/operators/in.md/#max_parallel_replica-subqueries) for more details.
:::

### Parallel processing using `SAMPLE` key

A query may be processed faster if it is executed on several servers in parallel. But the query performance may degrade in the following cases:

- The position of the sampling key in the partitioning key does not allow efficient range scans.
- Adding a sampling key to the table makes filtering by other columns less efficient.
- The sampling key is an expression that is expensive to calculate.
- The cluster latency distribution has a long tail, so that querying more servers increases the query overall latency.

### Parallel processing using [parallel_replicas_custom_key](#parallel_replicas_custom_key)

This setting is useful for any replicated table.

## parallel_replicas_custom_key {#parallel_replicas_custom_key}

An arbitrary integer expression that can be used to split work between replicas for a specific table.
The value can be any integer expression.
A query may be processed faster if it is executed on several servers in parallel but it depends on the used [parallel_replicas_custom_key](#parallel_replicas_custom_key)
and [parallel_replicas_custom_key_filter_type](#parallel_replicas_custom_key_filter_type).

Simple expressions using primary keys are preferred.

If the setting is used on a cluster that consists of a single shard with multiple replicas, those replicas will be converted into virtual shards.
Otherwise, it will behave same as for `SAMPLE` key, it will use multiple replicas of each shard.

## parallel_replicas_custom_key_filter_type {#parallel_replicas_custom_key_filter_type}

How to use `parallel_replicas_custom_key` expression for splitting work between replicas.

Possible values:

- `default` — Use the default implementation using modulo operation on the `parallel_replicas_custom_key`.
- `range` — Split the entire value space of the expression in the ranges. This type of filtering is useful if values of `parallel_replicas_custom_key` are uniformly spread across the entire integer space, e.g. hash values.

Default value: `default`.

## parallel_replicas_custom_key_range_lower {#parallel_replicas_custom_key_range_lower}

Allows the filter type `range` to split the work evenly between replicas based on the custom range `[parallel_replicas_custom_key_range_lower, INT_MAX]`.

When used in conjuction with [parallel_replicas_custom_key_range_upper](#parallel_replicas_custom_key_range_upper), it lets the filter evenly split the work over replicas for the range `[parallel_replicas_custom_key_range_lower, parallel_replicas_custom_key_range_upper]`.

Note: This setting will not cause any additional data to be filtered during query processing, rather it changes the points at which the range filter breaks up the range `[0, INT_MAX]` for parallel processing.

## parallel_replicas_custom_key_range_upper {#parallel_replicas_custom_key_range_upper}

Allows the filter type `range` to split the work evenly between replicas based on the custom range `[0, parallel_replicas_custom_key_range_upper]`. A value of 0 disables the upper bound, setting it the max value of the custom key expression.

When used in conjuction with [parallel_replicas_custom_key_range_lower](#parallel_replicas_custom_key_range_lower), it lets the filter evenly split the work over replicas for the range `[parallel_replicas_custom_key_range_lower, parallel_replicas_custom_key_range_upper]`.

Note: This setting will not cause any additional data to be filtered during query processing, rather it changes the points at which the range filter breaks up the range `[0, INT_MAX]` for parallel processing.

## allow_experimental_parallel_reading_from_replicas

Enables or disables sending SELECT queries to all replicas of a table (up to `max_parallel_replicas`). Reading is parallelized and coordinated dynamically. It will work for any kind of MergeTree table.

Possible values:

- 0 - Disabled.
- 1 - Enabled, silently disabled in case of failure.
- 2 - Enabled, throws an exception in case of failure.

Default value: `0`.

## compile_expressions {#compile-expressions}

Enables or disables compilation of frequently used simple functions and operators to native code with LLVM at runtime.

Possible values:

- 0 — Disabled.
- 1 — Enabled.

Default value: `1`.

## min_count_to_compile_expression {#min-count-to-compile-expression}

Minimum count of executing same expression before it is get compiled.

Default value: `3`.

## compile_aggregate_expressions {#compile_aggregate_expressions}

Enables or disables JIT-compilation of aggregate functions to native code. Enabling this setting can improve the performance.

Possible values:

- 0 — Aggregation is done without JIT compilation.
- 1 — Aggregation is done using JIT compilation.

Default value: `1`.

**See Also**

- [min_count_to_compile_aggregate_expression](#min_count_to_compile_aggregate_expression)

## min_count_to_compile_aggregate_expression {#min_count_to_compile_aggregate_expression}

The minimum number of identical aggregate expressions to start JIT-compilation. Works only if the [compile_aggregate_expressions](#compile_aggregate_expressions) setting is enabled.

Possible values:

- Positive integer.
- 0 — Identical aggregate expressions are always JIT-compiled.

Default value: `3`.

## use_query_cache {#use-query-cache}

If turned on, `SELECT` queries may utilize the [query cache](../query-cache.md). Parameters [enable_reads_from_query_cache](#enable-reads-from-query-cache)
and [enable_writes_to_query_cache](#enable-writes-to-query-cache) control in more detail how the cache is used.

Possible values:

- 0 - Disabled
- 1 - Enabled

Default value: `0`.

## enable_reads_from_query_cache {#enable-reads-from-query-cache}

If turned on, results of `SELECT` queries are retrieved from the [query cache](../query-cache.md).

Possible values:

- 0 - Disabled
- 1 - Enabled

Default value: `1`.

## enable_writes_to_query_cache {#enable-writes-to-query-cache}

If turned on, results of `SELECT` queries are stored in the [query cache](../query-cache.md).

Possible values:

- 0 - Disabled
- 1 - Enabled

Default value: `1`.

## query_cache_nondeterministic_function_handling {#query-cache-nondeterministic-function-handling}

Controls how the [query cache](../query-cache.md) handles `SELECT` queries with non-deterministic functions like `rand()` or `now()`.

Possible values:

- `'throw'` - Throw an exception and don't cache the query result.
- `'save'` - Cache the query result.
- `'ignore'` - Don't cache the query result and don't throw an exception.

Default value: `throw`.

## query_cache_system_table_handling {#query-cache-system-table-handling}

Controls how the [query cache](../query-cache.md) handles `SELECT` queries against system tables, i.e. tables in databases `system.*` and `information_schema.*`.

Possible values:

- `'throw'` - Throw an exception and don't cache the query result.
- `'save'` - Cache the query result.
- `'ignore'` - Don't cache the query result and don't throw an exception.

Default value: `throw`.

## query_cache_min_query_runs {#query-cache-min-query-runs}

Minimum number of times a `SELECT` query must run before its result is stored in the [query cache](../query-cache.md).

Possible values:

- Positive integer >= 0.

Default value: `0`

## query_cache_min_query_duration {#query-cache-min-query-duration}

Minimum duration in milliseconds a query needs to run for its result to be stored in the [query cache](../query-cache.md).

Possible values:

- Positive integer >= 0.

Default value: `0`

## query_cache_compress_entries {#query-cache-compress-entries}

Compress entries in the [query cache](../query-cache.md). Lessens the memory consumption of the query cache at the cost of slower inserts into / reads from it.

Possible values:

- 0 - Disabled
- 1 - Enabled

Default value: `1`

## query_cache_squash_partial_results {#query-cache-squash-partial-results}

Squash partial result blocks to blocks of size [max_block_size](#setting-max_block_size). Reduces performance of inserts into the [query cache](../query-cache.md) but improves the compressability of cache entries (see [query_cache_compress-entries](#query-cache-compress-entries)).

Possible values:

- 0 - Disabled
- 1 - Enabled

Default value: `1`

## query_cache_ttl {#query-cache-ttl}

After this time in seconds entries in the [query cache](../query-cache.md) become stale.

Possible values:

- Positive integer >= 0.

Default value: `60`

## query_cache_share_between_users {#query-cache-share-between-users}

If turned on, the result of `SELECT` queries cached in the [query cache](../query-cache.md) can be read by other users.
It is not recommended to enable this setting due to security reasons.

Possible values:

- 0 - Disabled
- 1 - Enabled

Default value: `0`.

## query_cache_max_size_in_bytes {#query-cache-max-size-in-bytes}

The maximum amount of memory (in bytes) the current user may allocate in the [query cache](../query-cache.md). 0 means unlimited.

Possible values:

- Positive integer >= 0.

Default value: 0 (no restriction).

## query_cache_max_entries {#query-cache-max-entries}

The maximum number of query results the current user may store in the [query cache](../query-cache.md). 0 means unlimited.

Possible values:

- Positive integer >= 0.

Default value: 0 (no restriction).

## insert_quorum {#insert_quorum}

:::note
This setting is not applicable to SharedMergeTree, see [SharedMergeTree consistency](/docs/en/cloud/reference/shared-merge-tree/#consistency) for more information.
:::

Enables the quorum writes.

- If `insert_quorum < 2`, the quorum writes are disabled.
- If `insert_quorum >= 2`, the quorum writes are enabled.
- If `insert_quorum = 'auto'`, use majority number (`number_of_replicas / 2 + 1`) as quorum number.

Default value: 0 - disabled.

Quorum writes

`INSERT` succeeds only when ClickHouse manages to correctly write data to the `insert_quorum` of replicas during the `insert_quorum_timeout`. If for any reason the number of replicas with successful writes does not reach the `insert_quorum`, the write is considered failed and ClickHouse will delete the inserted block from all the replicas where data has already been written.

When `insert_quorum_parallel` is disabled, all replicas in the quorum are consistent, i.e. they contain data from all previous `INSERT` queries (the `INSERT` sequence is linearized). When reading data written using `insert_quorum` and `insert_quorum_parallel` is disabled, you can turn on sequential consistency for `SELECT` queries using [select_sequential_consistency](#select_sequential_consistency).

ClickHouse generates an exception:

- If the number of available replicas at the time of the query is less than the `insert_quorum`.
- When `insert_quorum_parallel` is disabled and an attempt to write data is made when the previous block has not yet been inserted in `insert_quorum` of replicas. This situation may occur if the user tries to perform another `INSERT` query to the same table before the previous one with `insert_quorum` is completed.

See also:

- [insert_quorum_timeout](#insert_quorum_timeout)
- [insert_quorum_parallel](#insert_quorum_parallel)
- [select_sequential_consistency](#select_sequential_consistency)

## insert_quorum_timeout {#insert_quorum_timeout}

Write to a quorum timeout in milliseconds. If the timeout has passed and no write has taken place yet, ClickHouse will generate an exception and the client must repeat the query to write the same block to the same or any other replica.

Default value: 600 000 milliseconds (ten minutes).

See also:

- [insert_quorum](#insert_quorum)
- [insert_quorum_parallel](#insert_quorum_parallel)
- [select_sequential_consistency](#select_sequential_consistency)

## insert_quorum_parallel {#insert_quorum_parallel}

:::note
This setting is not applicable to SharedMergeTree, see [SharedMergeTree consistency](/docs/en/cloud/reference/shared-merge-tree/#consistency) for more information.
:::

Enables or disables parallelism for quorum `INSERT` queries. If enabled, additional `INSERT` queries can be sent while previous queries have not yet finished. If disabled, additional writes to the same table will be rejected.

Possible values:

- 0 — Disabled.
- 1 — Enabled.

Default value: 1.

See also:

- [insert_quorum](#insert_quorum)
- [insert_quorum_timeout](#insert_quorum_timeout)
- [select_sequential_consistency](#select_sequential_consistency)

## select_sequential_consistency {#select_sequential_consistency}

:::note
This setting differ in behavior between SharedMergeTree and ReplicatedMergeTree, see [SharedMergeTree consistency](/docs/en/cloud/reference/shared-merge-tree/#consistency) for more information about the behavior of `select_sequential_consistency` in SharedMergeTree.
:::

Enables or disables sequential consistency for `SELECT` queries. Requires `insert_quorum_parallel` to be disabled (enabled by default).

Possible values:

- 0 — Disabled.
- 1 — Enabled.

Default value: 0.

Usage

When sequential consistency is enabled, ClickHouse allows the client to execute the `SELECT` query only for those replicas that contain data from all previous `INSERT` queries executed with `insert_quorum`. If the client refers to a partial replica, ClickHouse will generate an exception. The SELECT query will not include data that has not yet been written to the quorum of replicas.

When `insert_quorum_parallel` is enabled (the default), then `select_sequential_consistency` does not work. This is because parallel `INSERT` queries can be written to different sets of quorum replicas so there is no guarantee a single replica will have received all writes.

See also:

- [insert_quorum](#insert_quorum)
- [insert_quorum_timeout](#insert_quorum_timeout)
- [insert_quorum_parallel](#insert_quorum_parallel)

## insert_deduplicate {#insert-deduplicate}

Enables or disables block deduplication of `INSERT` (for Replicated\* tables).

Possible values:

- 0 — Disabled.
- 1 — Enabled.

Default value: 1.

By default, blocks inserted into replicated tables by the `INSERT` statement are deduplicated (see [Data Replication](../../engines/table-engines/mergetree-family/replication.md)).
For the replicated tables by default the only 100 of the most recent blocks for each partition are deduplicated (see [replicated_deduplication_window](merge-tree-settings.md/#replicated-deduplication-window), [replicated_deduplication_window_seconds](merge-tree-settings.md/#replicated-deduplication-window-seconds)).
For not replicated tables see [non_replicated_deduplication_window](merge-tree-settings.md/#non-replicated-deduplication-window).

## Asynchronous Insert settings

### async_insert {#async-insert}

Enables or disables asynchronous inserts. Note that deduplication is disabled by default, see [async_insert_deduplicate](#async-insert-deduplicate).

If enabled, the data is combined into batches before the insertion into tables, so it is possible to do small and frequent insertions into ClickHouse (up to 15000 queries per second) without buffer tables.

The data is inserted either after the [async_insert_max_data_size](#async-insert-max-data-size) is exceeded or after [async_insert_busy_timeout_ms](#async-insert-busy-timeout-ms) milliseconds since the first `INSERT` query. If the [async_insert_stale_timeout_ms](#async-insert-stale-timeout-ms) is set to a non-zero value, the data is inserted after `async_insert_stale_timeout_ms` milliseconds since the last query. Also the buffer will be flushed to disk if at least [async_insert_max_query_number](#async-insert-max-query-number) async insert queries per block were received. This last setting takes effect only if [async_insert_deduplicate](#async-insert-deduplicate) is enabled.

If [wait_for_async_insert](#wait-for-async-insert) is enabled, every client will wait for the data to be processed and flushed to the table. Otherwise, the query would be processed almost instantly, even if the data is not inserted.

Possible values:

- 0 — Insertions are made synchronously, one after another.
- 1 — Multiple asynchronous insertions enabled.

Default value: `0`.

### async_insert_threads {#async-insert-threads}

The maximum number of threads for background data parsing and insertion.

Possible values:

- Positive integer.
- 0 — Asynchronous insertions are disabled.

Default value: `16`.

### wait_for_async_insert {#wait-for-async-insert}

Enables or disables waiting for processing of asynchronous insertion. If enabled, server will return `OK` only after the data is inserted. Otherwise, it will return `OK` as soon it has received the data, but it might still fail to parse or insert it later (You can check in system.asynchronous_insert_log)

If you want to use asynchronous inserts, we need to also enable [`async_insert`](#async-insert).

Possible values:

- 0 — Server returns `OK` even if the data is not yet inserted.
- 1 — Server returns `OK` only after the data is inserted.

Default value: `1`.

### wait_for_async_insert_timeout {#wait-for-async-insert-timeout}

The timeout in seconds for waiting for processing of asynchronous insertion.

Possible values:

- Positive integer.
- 0 — Disabled.

Default value: [lock_acquire_timeout](#lock_acquire_timeout).

### async_insert_max_data_size {#async-insert-max-data-size}

The maximum size of the unparsed data in bytes collected per query before being inserted.

Possible values:

- Positive integer.
- 0 — Asynchronous insertions are disabled.

Default value: `10485760`.

### async_insert_max_query_number {#async-insert-max-query-number}

The maximum number of insert queries per block before being inserted. This setting takes effect only if [async_insert_deduplicate](#async-insert-deduplicate) is enabled.

Possible values:

- Positive integer.
- 0 — Asynchronous insertions are disabled.

Default value: `450`.

### async_insert_busy_timeout_max_ms {#async-insert-busy-timeout-max-ms}

The maximum timeout in milliseconds since the first `INSERT` query before inserting collected data.

Possible values:

- Positive integer.
- 0 — Timeout disabled.

Default value: `200`.

Cloud default value: `1000`.

### async_insert_poll_timeout_ms {#async-insert-poll-timeout-ms}

Timeout in milliseconds for polling data from asynchronous insert queue.

Possible values:

- Positive integer.

Default value: `10`.

### async_insert_use_adaptive_busy_timeout {#allow-experimental-async-insert-adaptive-busy-timeout}

Use adaptive asynchronous insert timeout.

Possible values:

- 0 - Disabled.
- 1 - Enabled.

Default value: `0`.

### async_insert_busy_timeout_min_ms {#async-insert-busy-timeout-min-ms}

If adaptive asynchronous insert timeout is allowed through [async_insert_use_adaptive_busy_timeout](#allow-experimental-async-insert-adaptive-busy-timeout), the setting specifies the minimum value of the asynchronous insert timeout in milliseconds. It also serves as the initial value, which may be increased later by the adaptive algorithm, up to the [async_insert_busy_timeout_ms](#async_insert_busy_timeout_ms).

Possible values:

- Positive integer.

Default value: `50`.

### async_insert_busy_timeout_ms {#async-insert-busy-timeout-ms}

Alias for [`async_insert_busy_timeout_max_ms`](#async_insert_busy_timeout_max_ms).

### async_insert_busy_timeout_increase_rate {#async-insert-busy-timeout-increase-rate}

If adaptive asynchronous insert timeout is allowed through [async_insert_use_adaptive_busy_timeout](#allow-experimental-async-insert-adaptive-busy-timeout), the setting specifies the exponential growth rate at which the adaptive asynchronous insert timeout increases.

Possible values:

- A positive floating-point number.

Default value: `0.2`.

### async_insert_busy_timeout_decrease_rate {#async-insert-busy-timeout-decrease-rate}

If adaptive asynchronous insert timeout is allowed through [async_insert_use_adaptive_busy_timeout](#allow-experimental-async-insert-adaptive-busy-timeout), the setting specifies the exponential growth rate at which the adaptive asynchronous insert timeout decreases.

Possible values:

- A positive floating-point number.

Default value: `0.2`.

### async_insert_stale_timeout_ms {#async-insert-stale-timeout-ms}

The maximum timeout in milliseconds since the last `INSERT` query before dumping collected data. If enabled, the settings prolongs the [async_insert_busy_timeout_ms](#async-insert-busy-timeout-ms) with every `INSERT` query as long as [async_insert_max_data_size](#async-insert-max-data-size) is not exceeded.

Possible values:

- Positive integer.
- 0 — Timeout disabled.

Default value: `0`.

### async_insert_deduplicate {#async-insert-deduplicate}

Enables or disables insert deduplication of `ASYNC INSERT` (for Replicated\* tables).

Possible values:

- 0 — Disabled.
- 1 — Enabled.

Default value: 0.

By default, async inserts are inserted into replicated tables by the `INSERT` statement enabling [async_insert](#async-insert) are deduplicated (see [Data Replication](../../engines/table-engines/mergetree-family/replication.md)).
For the replicated tables, by default, only 10000 of the most recent inserts for each partition are deduplicated (see [replicated_deduplication_window_for_async_inserts](merge-tree-settings.md/#replicated-deduplication-window-async-inserts), [replicated_deduplication_window_seconds_for_async_inserts](merge-tree-settings.md/#replicated-deduplication-window-seconds-async-inserts)).
We recommend enabling the [async_block_ids_cache](merge-tree-settings.md/#use-async-block-ids-cache) to increase the efficiency of deduplication.
This function does not work for non-replicated tables.

## deduplicate_blocks_in_dependent_materialized_views {#deduplicate-blocks-in-dependent-materialized-views}

Enables or disables the deduplication check for materialized views that receive data from Replicated\* tables.

Possible values:

      0 — Disabled.
      1 — Enabled.

Default value: 0.

Usage

By default, deduplication is not performed for materialized views but is done upstream, in the source table.
If an INSERTed block is skipped due to deduplication in the source table, there will be no insertion into attached materialized views. This behaviour exists to enable the insertion of highly aggregated data into materialized views, for cases where inserted blocks are the same after materialized view aggregation but derived from different INSERTs into the source table.
At the same time, this behaviour “breaks” `INSERT` idempotency. If an `INSERT` into the main table was successful and `INSERT` into a materialized view failed (e.g. because of communication failure with ClickHouse Keeper) a client will get an error and can retry the operation. However, the materialized view won’t receive the second insert because it will be discarded by deduplication in the main (source) table. The setting `deduplicate_blocks_in_dependent_materialized_views` allows for changing this behaviour. On retry, a materialized view will receive the repeat insert and will perform a deduplication check by itself,
ignoring check result for the source table, and will insert rows lost because of the first failure.

## insert_deduplication_token {#insert_deduplication_token}

The setting allows a user to provide own deduplication semantic in MergeTree/ReplicatedMergeTree
For example, by providing a unique value for the setting in each INSERT statement,
user can avoid the same inserted data being deduplicated.

Possible values:

- Any string

Default value: empty string (disabled)

`insert_deduplication_token` is used for deduplication _only_ when not empty.

For the replicated tables by default the only 100 of the most recent inserts for each partition are deduplicated (see [replicated_deduplication_window](merge-tree-settings.md/#replicated-deduplication-window), [replicated_deduplication_window_seconds](merge-tree-settings.md/#replicated-deduplication-window-seconds)).
For not replicated tables see [non_replicated_deduplication_window](merge-tree-settings.md/#non-replicated-deduplication-window).

:::note
`insert_deduplication_token` works on a partition level (the same as `insert_deduplication` checksum). Multiple partitions can have the same `insert_deduplication_token`.
:::

Example:

```sql
CREATE TABLE test_table
( A Int64 )
ENGINE = MergeTree
ORDER BY A
SETTINGS non_replicated_deduplication_window = 100;

INSERT INTO test_table SETTINGS insert_deduplication_token = 'test' VALUES (1);

-- the next insert won't be deduplicated because insert_deduplication_token is different
INSERT INTO test_table SETTINGS insert_deduplication_token = 'test1' VALUES (1);

-- the next insert will be deduplicated because insert_deduplication_token
-- is the same as one of the previous
INSERT INTO test_table SETTINGS insert_deduplication_token = 'test' VALUES (2);

SELECT * FROM test_table

┌─A─┐
│ 1 │
└───┘
┌─A─┐
│ 1 │
└───┘
```

## update_insert_deduplication_token_in_dependent_materialized_views {#update-insert-deduplication-token-in-dependent-materialized-views}

Allows to update `insert_deduplication_token` with view identifier during insert in dependent materialized views, if setting `deduplicate_blocks_in_dependent_materialized_views` is enabled and `insert_deduplication_token` is set.

Possible values:

      0 — Disabled.
      1 — Enabled.

Default value: 0.

Usage:

If setting `deduplicate_blocks_in_dependent_materialized_views` is enabled, `insert_deduplication_token` is passed to dependent materialized views. But in complex INSERT flows it is possible that we want to avoid deduplication for dependent materialized views.

Example:
```
landing -┬--> mv_1_1 ---> ds_1_1 ---> mv_2_1 --┬-> ds_2_1 ---> mv_3_1 ---> ds_3_1
         |                                     |
         └--> mv_1_2 ---> ds_1_2 ---> mv_2_2 --┘
```

In this example we want to avoid deduplication for two different blocks generated from `mv_2_1` and `mv_2_2` that will be inserted into `ds_2_1`. Without `update_insert_deduplication_token_in_dependent_materialized_views` setting enabled, those two different blocks will be deduplicated, because different blocks from `mv_2_1` and `mv_2_2` will have the same `insert_deduplication_token`.

If setting `update_insert_deduplication_token_in_dependent_materialized_views` is enabled, during each insert into dependent materialized views `insert_deduplication_token` is updated with table identifier, so block from `mv_2_1` and block from `mv_2_2` will have different `insert_deduplication_token` and will not be deduplicated.

## insert_keeper_max_retries

The setting sets the maximum number of retries for ClickHouse Keeper (or ZooKeeper) requests during insert into replicated MergeTree. Only Keeper requests which failed due to network error, Keeper session timeout, or request timeout are considered for retries.

Possible values:

- Positive integer.
- 0 — Retries are disabled

Default value: 20

Cloud default value: `20`.

Keeper request retries are done after some timeout. The timeout is controlled by the following settings: `insert_keeper_retry_initial_backoff_ms`, `insert_keeper_retry_max_backoff_ms`.
The first retry is done after `insert_keeper_retry_initial_backoff_ms` timeout. The consequent timeouts will be calculated as follows:
```
timeout = min(insert_keeper_retry_max_backoff_ms, latest_timeout * 2)
```

For example, if `insert_keeper_retry_initial_backoff_ms=100`, `insert_keeper_retry_max_backoff_ms=10000` and `insert_keeper_max_retries=8` then timeouts will be `100, 200, 400, 800, 1600, 3200, 6400, 10000`.

Apart from fault tolerance, the retries aim to provide a better user experience - they allow to avoid returning an error during INSERT execution if Keeper is restarted, for example, due to an upgrade.

## insert_keeper_retry_initial_backoff_ms {#insert_keeper_retry_initial_backoff_ms}

Initial timeout(in milliseconds) to retry a failed Keeper request during INSERT query execution

Possible values:

- Positive integer.
- 0 — No timeout

Default value: 100

## insert_keeper_retry_max_backoff_ms {#insert_keeper_retry_max_backoff_ms}

Maximum timeout (in milliseconds) to retry a failed Keeper request during INSERT query execution

Possible values:

- Positive integer.
- 0 — Maximum timeout is not limited

Default value: 10000

## max_network_bytes {#max-network-bytes}

Limits the data volume (in bytes) that is received or transmitted over the network when executing a query. This setting applies to every individual query.

Possible values:

- Positive integer.
- 0 — Data volume control is disabled.

Default value: 0.

## max_network_bandwidth {#max-network-bandwidth}

Limits the speed of the data exchange over the network in bytes per second. This setting applies to every query.

Possible values:

- Positive integer.
- 0 — Bandwidth control is disabled.

Default value: 0.

## max_network_bandwidth_for_user {#max-network-bandwidth-for-user}

Limits the speed of the data exchange over the network in bytes per second. This setting applies to all concurrently running queries performed by a single user.

Possible values:

- Positive integer.
- 0 — Control of the data speed is disabled.

Default value: 0.

## max_network_bandwidth_for_all_users {#max-network-bandwidth-for-all-users}

Limits the speed that data is exchanged at over the network in bytes per second. This setting applies to all concurrently running queries on the server.

Possible values:

- Positive integer.
- 0 — Control of the data speed is disabled.

Default value: 0.

## count_distinct_implementation {#count_distinct_implementation}

Specifies which of the `uniq*` functions should be used to perform the [COUNT(DISTINCT ...)](../../sql-reference/aggregate-functions/reference/count.md/#agg_function-count) construction.

Possible values:

- [uniq](../../sql-reference/aggregate-functions/reference/uniq.md/#agg_function-uniq)
- [uniqCombined](../../sql-reference/aggregate-functions/reference/uniqcombined.md/#agg_function-uniqcombined)
- [uniqCombined64](../../sql-reference/aggregate-functions/reference/uniqcombined64.md/#agg_function-uniqcombined64)
- [uniqHLL12](../../sql-reference/aggregate-functions/reference/uniqhll12.md/#agg_function-uniqhll12)
- [uniqExact](../../sql-reference/aggregate-functions/reference/uniqexact.md/#agg_function-uniqexact)

Default value: `uniqExact`.

## skip_unavailable_shards {#skip_unavailable_shards}

Enables or disables silently skipping of unavailable shards.

Shard is considered unavailable if all its replicas are unavailable. A replica is unavailable in the following cases:

- ClickHouse can’t connect to replica for any reason.

    When connecting to a replica, ClickHouse performs several attempts. If all these attempts fail, the replica is considered unavailable.

- Replica can’t be resolved through DNS.

    If replica’s hostname can’t be resolved through DNS, it can indicate the following situations:

    - Replica’s host has no DNS record. It can occur in systems with dynamic DNS, for example, [Kubernetes](https://kubernetes.io), where nodes can be unresolvable during downtime, and this is not an error.

    - Configuration error. ClickHouse configuration file contains a wrong hostname.

Possible values:

- 1 — skipping enabled.

    If a shard is unavailable, ClickHouse returns a result based on partial data and does not report node availability issues.

- 0 — skipping disabled.

    If a shard is unavailable, ClickHouse throws an exception.

Default value: 0.

## distributed_group_by_no_merge {#distributed-group-by-no-merge}

Do not merge aggregation states from different servers for distributed query processing, you can use this in case it is for certain that there are different keys on different shards

Possible values:

- `0` — Disabled (final query processing is done on the initiator node).
- `1` - Do not merge aggregation states from different servers for distributed query processing (query completely processed on the shard, initiator only proxy the data), can be used in case it is for certain that there are different keys on different shards.
- `2` - Same as `1` but applies `ORDER BY` and `LIMIT` (it is not possible when the query processed completely on the remote node, like for `distributed_group_by_no_merge=1`) on the initiator (can be used for queries with `ORDER BY` and/or `LIMIT`).

Default value: `0`

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

## distributed_push_down_limit {#distributed-push-down-limit}

Enables or disables [LIMIT](#limit) applying on each shard separately.

This will allow to avoid:
- Sending extra rows over network;
- Processing rows behind the limit on the initiator.

Starting from 21.9 version you cannot get inaccurate results anymore, since `distributed_push_down_limit` changes query execution only if at least one of the conditions met:
- [distributed_group_by_no_merge](#distributed-group-by-no-merge) > 0.
- Query **does not have** `GROUP BY`/`DISTINCT`/`LIMIT BY`, but it has `ORDER BY`/`LIMIT`.
- Query **has** `GROUP BY`/`DISTINCT`/`LIMIT BY` with `ORDER BY`/`LIMIT` and:
    - [optimize_skip_unused_shards](#optimize-skip-unused-shards) is enabled.
    - [optimize_distributed_group_by_sharding_key](#optimize-distributed-group-by-sharding-key) is enabled.

Possible values:

- 0 — Disabled.
- 1 — Enabled.

Default value: `1`.

See also:

- [distributed_group_by_no_merge](#distributed-group-by-no-merge)
- [optimize_skip_unused_shards](#optimize-skip-unused-shards)
- [optimize_distributed_group_by_sharding_key](#optimize-distributed-group-by-sharding-key)

## optimize_skip_unused_shards_limit {#optimize-skip-unused-shards-limit}

Limit for number of sharding key values, turns off `optimize_skip_unused_shards` if the limit is reached.

Too many values may require significant amount for processing, while the benefit is doubtful, since if you have huge number of values in `IN (...)`, then most likely the query will be sent to all shards anyway.

Default value: 1000

## optimize_skip_unused_shards {#optimize-skip-unused-shards}

Enables or disables skipping of unused shards for [SELECT](../../sql-reference/statements/select/index.md) queries that have sharding key condition in `WHERE/PREWHERE` (assuming that the data is distributed by sharding key, otherwise a query yields incorrect result).

Possible values:

- 0 — Disabled.
- 1 — Enabled.

Default value: 0

## optimize_skip_unused_shards_rewrite_in {#optimize-skip-unused-shards-rewrite-in}

Rewrite IN in query for remote shards to exclude values that does not belong to the shard (requires optimize_skip_unused_shards).

Possible values:

- 0 — Disabled.
- 1 — Enabled.

Default value: 1 (since it requires `optimize_skip_unused_shards` anyway, which `0` by default)

## allow_nondeterministic_optimize_skip_unused_shards {#allow-nondeterministic-optimize-skip-unused-shards}

Allow nondeterministic (like `rand` or `dictGet`, since later has some caveats with updates) functions in sharding key.

Possible values:

- 0 — Disallowed.
- 1 — Allowed.

Default value: 0

## optimize_skip_unused_shards_nesting {#optimize-skip-unused-shards-nesting}

Controls [`optimize_skip_unused_shards`](#optimize-skip-unused-shards) (hence still requires [`optimize_skip_unused_shards`](#optimize-skip-unused-shards)) depends on the nesting level of the distributed query (case when you have `Distributed` table that look into another `Distributed` table).

Possible values:

- 0 — Disabled, `optimize_skip_unused_shards` works always.
- 1 — Enables `optimize_skip_unused_shards` only for the first level.
- 2 — Enables `optimize_skip_unused_shards` up to the second level.

Default value: 0

## force_optimize_skip_unused_shards {#force-optimize-skip-unused-shards}

Enables or disables query execution if [optimize_skip_unused_shards](#optimize-skip-unused-shards) is enabled and skipping of unused shards is not possible. If the skipping is not possible and the setting is enabled, an exception will be thrown.

Possible values:

- 0 — Disabled. ClickHouse does not throw an exception.
- 1 — Enabled. Query execution is disabled only if the table has a sharding key.
- 2 — Enabled. Query execution is disabled regardless of whether a sharding key is defined for the table.

Default value: 0

## force_optimize_skip_unused_shards_nesting {#force_optimize_skip_unused_shards_nesting}

Controls [`force_optimize_skip_unused_shards`](#force-optimize-skip-unused-shards) (hence still requires [`force_optimize_skip_unused_shards`](#force-optimize-skip-unused-shards)) depends on the nesting level of the distributed query (case when you have `Distributed` table that look into another `Distributed` table).

Possible values:

- 0 - Disabled, `force_optimize_skip_unused_shards` works always.
- 1 — Enables `force_optimize_skip_unused_shards` only for the first level.
- 2 — Enables `force_optimize_skip_unused_shards` up to the second level.

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

- 0 — Disabled.
- 1 — Enabled.

Default value: 0

See also:

- [distributed_group_by_no_merge](#distributed-group-by-no-merge)
- [distributed_push_down_limit](#distributed-push-down-limit)
- [optimize_skip_unused_shards](#optimize-skip-unused-shards)

:::note
Right now it requires `optimize_skip_unused_shards` (the reason behind this is that one day it may be enabled by default, and it will work correctly only if data was inserted via Distributed table, i.e. data is distributed according to sharding_key).
:::

## optimize_throw_if_noop {#setting-optimize_throw_if_noop}

Enables or disables throwing an exception if an [OPTIMIZE](../../sql-reference/statements/optimize.md) query didn’t perform a merge.

By default, `OPTIMIZE` returns successfully even if it didn’t do anything. This setting lets you differentiate these situations and get the reason in an exception message.

Possible values:

- 1 — Throwing an exception is enabled.
- 0 — Throwing an exception is disabled.

Default value: 0.

## optimize_skip_merged_partitions {#optimize-skip-merged-partitions}

Enables or disables optimization for [OPTIMIZE TABLE ... FINAL](../../sql-reference/statements/optimize.md) query if there is only one part with level > 0 and it doesn't have expired TTL.

- `OPTIMIZE TABLE ... FINAL SETTINGS optimize_skip_merged_partitions=1`

By default, `OPTIMIZE TABLE ... FINAL` query rewrites the one part even if there is only a single part.

Possible values:

- 1 - Enable optimization.
- 0 - Disable optimization.

Default value: 0.

## optimize_functions_to_subcolumns {#optimize-functions-to-subcolumns}

Enables or disables optimization by transforming some functions to reading subcolumns. This reduces the amount of data to read.

These functions can be transformed:

- [length](../../sql-reference/functions/array-functions.md/#array_functions-length) to read the [size0](../../sql-reference/data-types/array.md/#array-size) subcolumn.
- [empty](../../sql-reference/functions/array-functions.md/#function-empty) to read the [size0](../../sql-reference/data-types/array.md/#array-size) subcolumn.
- [notEmpty](../../sql-reference/functions/array-functions.md/#function-notempty) to read the [size0](../../sql-reference/data-types/array.md/#array-size) subcolumn.
- [isNull](../../sql-reference/operators/index.md#operator-is-null) to read the [null](../../sql-reference/data-types/nullable.md/#finding-null) subcolumn.
- [isNotNull](../../sql-reference/operators/index.md#is-not-null) to read the [null](../../sql-reference/data-types/nullable.md/#finding-null) subcolumn.
- [count](../../sql-reference/aggregate-functions/reference/count.md) to read the [null](../../sql-reference/data-types/nullable.md/#finding-null) subcolumn.
- [mapKeys](../../sql-reference/functions/tuple-map-functions.md/#mapkeys) to read the [keys](../../sql-reference/data-types/map.md/#map-subcolumns) subcolumn.
- [mapValues](../../sql-reference/functions/tuple-map-functions.md/#mapvalues) to read the [values](../../sql-reference/data-types/map.md/#map-subcolumns) subcolumn.

Possible values:

- 0 — Optimization disabled.
- 1 — Optimization enabled.

Default value: `1`.

## optimize_trivial_count_query {#optimize-trivial-count-query}

Enables or disables the optimization to trivial query `SELECT count() FROM table` using metadata from MergeTree. If you need to use row-level security, disable this setting.

Possible values:

   - 0 — Optimization disabled.
   - 1 — Optimization enabled.

Default value: `1`.

See also:

- [optimize_functions_to_subcolumns](#optimize-functions-to-subcolumns)

## optimize_trivial_approximate_count_query {#optimize_trivial_approximate_count_query}

Use an approximate value for trivial count optimization of storages that support such estimation, for example, EmbeddedRocksDB.

Possible values:

   - 0 — Optimization disabled.
   - 1 — Optimization enabled.

Default value: `0`.

## optimize_count_from_files {#optimize_count_from_files}

Enables or disables the optimization of counting number of rows from files in different input formats. It applies to table functions/engines `file`/`s3`/`url`/`hdfs`/`azureBlobStorage`.

Possible values:

- 0 — Optimization disabled.
- 1 — Optimization enabled.

Default value: `1`.

## use_cache_for_count_from_files {#use_cache_for_count_from_files}

Enables caching of rows number during count from files in table functions `file`/`s3`/`url`/`hdfs`/`azureBlobStorage`.

Enabled by default.

## distributed_replica_error_half_life {#distributed_replica_error_half_life}

- Type: seconds
- Default value: 60 seconds

Controls how fast errors in distributed tables are zeroed. If a replica is unavailable for some time, accumulates 5 errors, and distributed_replica_error_half_life is set to 1 second, then the replica is considered normal 3 seconds after the last error.

See also:

- [load_balancing](#load_balancing-round_robin)
- [Table engine Distributed](../../engines/table-engines/special/distributed.md)
- [distributed_replica_error_cap](#distributed_replica_error_cap)
- [distributed_replica_max_ignored_errors](#distributed_replica_max_ignored_errors)

## distributed_replica_error_cap {#distributed_replica_error_cap}

- Type: unsigned int
- Default value: 1000

The error count of each replica is capped at this value, preventing a single replica from accumulating too many errors.

See also:

- [load_balancing](#load_balancing-round_robin)
- [Table engine Distributed](../../engines/table-engines/special/distributed.md)
- [distributed_replica_error_half_life](#distributed_replica_error_half_life)
- [distributed_replica_max_ignored_errors](#distributed_replica_max_ignored_errors)

## distributed_replica_max_ignored_errors {#distributed_replica_max_ignored_errors}

- Type: unsigned int
- Default value: 0

The number of errors that will be ignored while choosing replicas (according to `load_balancing` algorithm).

See also:

- [load_balancing](#load_balancing-round_robin)
- [Table engine Distributed](../../engines/table-engines/special/distributed.md)
- [distributed_replica_error_cap](#distributed_replica_error_cap)
- [distributed_replica_error_half_life](#distributed_replica_error_half_life)

## distributed_background_insert_sleep_time_ms {#distributed_background_insert_sleep_time_ms}

Base interval for the [Distributed](../../engines/table-engines/special/distributed.md) table engine to send data. The actual interval grows exponentially in the event of errors.

Possible values:

- A positive integer number of milliseconds.

Default value: 100 milliseconds.

## distributed_background_insert_max_sleep_time_ms {#distributed_background_insert_max_sleep_time_ms}

Maximum interval for the [Distributed](../../engines/table-engines/special/distributed.md) table engine to send data. Limits exponential growth of the interval set in the [distributed_background_insert_sleep_time_ms](#distributed_background_insert_sleep_time_ms) setting.

Possible values:

- A positive integer number of milliseconds.

Default value: 30000 milliseconds (30 seconds).

## distributed_background_insert_batch {#distributed_background_insert_batch}

Enables/disables inserted data sending in batches.

When batch sending is enabled, the [Distributed](../../engines/table-engines/special/distributed.md) table engine tries to send multiple files of inserted data in one operation instead of sending them separately. Batch sending improves cluster performance by better-utilizing server and network resources.

Possible values:

- 1 — Enabled.
- 0 — Disabled.

Default value: 0.

## distributed_background_insert_split_batch_on_failure {#distributed_background_insert_split_batch_on_failure}

Enables/disables splitting batches on failures.

Sometimes sending particular batch to the remote shard may fail, because of some complex pipeline after (i.e. `MATERIALIZED VIEW` with `GROUP BY`) due to `Memory limit exceeded` or similar errors. In this case, retrying will not help (and this will stuck distributed sends for the table) but sending files from that batch one by one may succeed INSERT.

So installing this setting to `1` will disable batching for such batches (i.e. temporary disables `distributed_background_insert_batch` for failed batches).

Possible values:

- 1 — Enabled.
- 0 — Disabled.

Default value: 0.

:::note
This setting also affects broken batches (that may appears because of abnormal server (machine) termination and no `fsync_after_insert`/`fsync_directories` for [Distributed](../../engines/table-engines/special/distributed.md) table engine).
:::

:::note
You should not rely on automatic batch splitting, since this may hurt performance.
:::

## os_thread_priority {#setting-os-thread-priority}

Sets the priority ([nice](https://en.wikipedia.org/wiki/Nice_(Unix))) for threads that execute queries. The OS scheduler considers this priority when choosing the next thread to run on each available CPU core.

:::note
To use this setting, you need to set the `CAP_SYS_NICE` capability. The `clickhouse-server` package sets it up during installation. Some virtual environments do not allow you to set the `CAP_SYS_NICE` capability. In this case, `clickhouse-server` shows a message about it at the start.
:::

Possible values:

- You can set values in the range `[-20, 19]`.

Lower values mean higher priority. Threads with low `nice` priority values are executed more frequently than threads with high values. High values are preferable for long-running non-interactive queries because it allows them to quickly give up resources in favour of short interactive queries when they arrive.

Default value: 0.

## query_profiler_real_time_period_ns {#query_profiler_real_time_period_ns}

Sets the period for a real clock timer of the [query profiler](../../operations/optimizing-performance/sampling-query-profiler.md). Real clock timer counts wall-clock time.

Possible values:

- Positive integer number, in nanoseconds.

    Recommended values:

            - 10000000 (100 times a second) nanoseconds and less for single queries.
            - 1000000000 (once a second) for cluster-wide profiling.

- 0 for turning off the timer.

Type: [UInt64](../../sql-reference/data-types/int-uint.md).

Default value: 1000000000 nanoseconds (once a second).

**Temporarily disabled in ClickHouse Cloud.**

See also:

- System table [trace_log](../../operations/system-tables/trace_log.md/#system_tables-trace_log)

## query_profiler_cpu_time_period_ns {#query_profiler_cpu_time_period_ns}

Sets the period for a CPU clock timer of the [query profiler](../../operations/optimizing-performance/sampling-query-profiler.md). This timer counts only CPU time.

Possible values:

- A positive integer number of nanoseconds.

    Recommended values:

            - 10000000 (100 times a second) nanoseconds and more for single queries.
            - 1000000000 (once a second) for cluster-wide profiling.

- 0 for turning off the timer.

Type: [UInt64](../../sql-reference/data-types/int-uint.md).

Default value: 1000000000 nanoseconds.

**Temporarily disabled in ClickHouse Cloud.**

See also:

- System table [trace_log](../../operations/system-tables/trace_log.md/#system_tables-trace_log)

## memory_profiler_step {#memory_profiler_step}

Sets the step of memory profiler. Whenever query memory usage becomes larger than every next step in number of bytes the memory profiler will collect the allocating stacktrace and will write it into [trace_log](../../operations/system-tables/trace_log.md#system_tables-trace_log).

Possible values:

- A positive integer number of bytes.

- 0 for turning off the memory profiler.

Default value: 4,194,304 bytes (4 MiB).

## memory_profiler_sample_probability {#memory_profiler_sample_probability}

Sets the probability of collecting stacktraces at random allocations and deallocations and writing them into [trace_log](../../operations/system-tables/trace_log.md#system_tables-trace_log).

Possible values:

- A positive floating-point number in the range [0..1].

- 0.0 for turning off the memory sampling.

Default value: 0.0.

## trace_profile_events {#trace_profile_events}

Enables or disables collecting stacktraces on each update of profile events along with the name of profile event and the value of increment and sending them into [trace_log](../../operations/system-tables/trace_log.md#system_tables-trace_log).

Possible values:

- 1 — Tracing of profile events enabled.
- 0 — Tracing of profile events disabled.

Default value: 0.

## allow_introspection_functions {#allow_introspection_functions}

Enables or disables [introspection functions](../../sql-reference/functions/introspection.md) for query profiling.

Possible values:

- 1 — Introspection functions enabled.
- 0 — Introspection functions disabled.

Default value: 0.

**See Also**

- [Sampling Query Profiler](../../operations/optimizing-performance/sampling-query-profiler.md)
- System table [trace_log](../../operations/system-tables/trace_log.md/#system_tables-trace_log)

## input_format_parallel_parsing {#input-format-parallel-parsing}

Enables or disables order-preserving parallel parsing of data formats. Supported only for [TSV](../../interfaces/formats.md/#tabseparated), [TSKV](../../interfaces/formats.md/#tskv), [CSV](../../interfaces/formats.md/#csv) and [JSONEachRow](../../interfaces/formats.md/#jsoneachrow) formats.

Possible values:

- 1 — Enabled.
- 0 — Disabled.

Default value: `1`.

## output_format_parallel_formatting {#output-format-parallel-formatting}

Enables or disables parallel formatting of data formats. Supported only for [TSV](../../interfaces/formats.md/#tabseparated), [TSKV](../../interfaces/formats.md/#tskv), [CSV](../../interfaces/formats.md/#csv) and [JSONEachRow](../../interfaces/formats.md/#jsoneachrow) formats.

Possible values:

- 1 — Enabled.
- 0 — Disabled.

Default value: `1`.

## min_chunk_bytes_for_parallel_parsing {#min-chunk-bytes-for-parallel-parsing}

- Type: unsigned int
- Default value: 1 MiB

The minimum chunk size in bytes, which each thread will parse in parallel.

## merge_selecting_sleep_ms {#merge_selecting_sleep_ms}

Sleep time for merge selecting when no part is selected. A lower setting triggers selecting tasks in `background_schedule_pool` frequently, which results in a large number of requests to ClickHouse Keeper in large-scale clusters.

Possible values:

- Any positive integer.

Default value: `5000`.

## parallel_distributed_insert_select {#parallel_distributed_insert_select}

Enables parallel distributed `INSERT ... SELECT` query.

If we execute `INSERT INTO distributed_table_a SELECT ... FROM distributed_table_b` queries and both tables use the same cluster, and both tables are either [replicated](../../engines/table-engines/mergetree-family/replication.md) or non-replicated, then this query is processed locally on every shard.

Possible values:

- 0 — Disabled.
- 1 — `SELECT` will be executed on each shard from the underlying table of the distributed engine.
- 2 — `SELECT` and `INSERT` will be executed on each shard from/to the underlying table of the distributed engine.

Default value: 0.

## distributed_insert_skip_read_only_replicas {#distributed_insert_skip_read_only_replicas}

Enables skipping read-only replicas for INSERT queries into Distributed.

Possible values:

- 0 — INSERT was as usual, if it will go to read-only replica it will fail
- 1 — Initiator will skip read-only replicas before sending data to shards.

Default value: `0`

## distributed_foreground_insert {#distributed_foreground_insert}

Enables or disables synchronous data insertion into a [Distributed](../../engines/table-engines/special/distributed.md/#distributed) table.

By default, when inserting data into a `Distributed` table, the ClickHouse server sends data to cluster nodes in background mode. When `distributed_foreground_insert=1`, the data is processed synchronously, and the `INSERT` operation succeeds only after all the data is saved on all shards (at least one replica for each shard if `internal_replication` is true).

Possible values:

- 0 — Data is inserted in background mode.
- 1 — Data is inserted in synchronous mode.

Default value: `0`.

Cloud default value: `1`.

**See Also**

- [Distributed Table Engine](../../engines/table-engines/special/distributed.md/#distributed)
- [Managing Distributed Tables](../../sql-reference/statements/system.md/#query-language-system-distributed)

## insert_distributed_sync {#insert_distributed_sync}

Alias for [`distributed_foreground_insert`](#distributed_foreground_insert).

## insert_shard_id {#insert_shard_id}

If not `0`, specifies the shard of [Distributed](../../engines/table-engines/special/distributed.md/#distributed) table into which the data will be inserted synchronously.

If `insert_shard_id` value is incorrect, the server will throw an exception.

To get the number of shards on `requested_cluster`, you can check server config or use this query:

``` sql
SELECT uniq(shard_num) FROM system.clusters WHERE cluster = 'requested_cluster';
```

Possible values:

- 0 — Disabled.
- Any number from `1` to `shards_num` of corresponding [Distributed](../../engines/table-engines/special/distributed.md/#distributed) table.

Default value: `0`.

**Example**

Query:

```sql
CREATE TABLE x AS system.numbers ENGINE = MergeTree ORDER BY number;
CREATE TABLE x_dist AS x ENGINE = Distributed('test_cluster_two_shards_localhost', currentDatabase(), x);
INSERT INTO x_dist SELECT * FROM numbers(5) SETTINGS insert_shard_id = 1;
SELECT * FROM x_dist ORDER BY number ASC;
```

Result:

``` text
┌─number─┐
│      0 │
│      0 │
│      1 │
│      1 │
│      2 │
│      2 │
│      3 │
│      3 │
│      4 │
│      4 │
└────────┘
```

## use_compact_format_in_distributed_parts_names {#use_compact_format_in_distributed_parts_names}

Uses compact format for storing blocks for background (`distributed_foreground_insert`) INSERT into tables with `Distributed` engine.

Possible values:

- 0 — Uses `user[:password]@host:port#default_database` directory format.
- 1 — Uses `[shard{shard_index}[_replica{replica_index}]]` directory format.

Default value: `1`.

:::note
- with `use_compact_format_in_distributed_parts_names=0` changes from cluster definition will not be applied for background INSERT.
- with `use_compact_format_in_distributed_parts_names=1` changing the order of the nodes in the cluster definition, will change the `shard_index`/`replica_index` so be aware.
:::

## background_buffer_flush_schedule_pool_size {#background_buffer_flush_schedule_pool_size}

That setting was moved to the [server configuration parameters](../../operations/server-configuration-parameters/settings.md/#background_buffer_flush_schedule_pool_size).

## background_move_pool_size {#background_move_pool_size}

That setting was moved to the [server configuration parameters](../../operations/server-configuration-parameters/settings.md/#background_move_pool_size).

## background_schedule_pool_size {#background_schedule_pool_size}

That setting was moved to the [server configuration parameters](../../operations/server-configuration-parameters/settings.md/#background_schedule_pool_size).

## background_fetches_pool_size {#background_fetches_pool_size}

That setting was moved to the [server configuration parameters](../../operations/server-configuration-parameters/settings.md/#background_fetches_pool_size).

## always_fetch_merged_part {#always_fetch_merged_part}

Prohibits data parts merging in [Replicated\*MergeTree](../../engines/table-engines/mergetree-family/replication.md)-engine tables.

When merging is prohibited, the replica never merges parts and always downloads merged parts from other replicas. If there is no required data yet, the replica waits for it. CPU and disk load on the replica server decreases, but the network load on the cluster increases. This setting can be useful on servers with relatively weak CPUs or slow disks, such as servers for backups storage.

Possible values:

- 0 — `Replicated*MergeTree`-engine tables merge data parts at the replica.
- 1 — `Replicated*MergeTree`-engine tables do not merge data parts at the replica. The tables download merged data parts from other replicas.

Default value: 0.

**See Also**

- [Data Replication](../../engines/table-engines/mergetree-family/replication.md)

## background_distributed_schedule_pool_size {#background_distributed_schedule_pool_size}

That setting was moved to the [server configuration parameters](../../operations/server-configuration-parameters/settings.md/#background_distributed_schedule_pool_size).

## background_message_broker_schedule_pool_size {#background_message_broker_schedule_pool_size}

That setting was moved to the [server configuration parameters](../../operations/server-configuration-parameters/settings.md/#background_message_broker_schedule_pool_size).

## validate_polygons {#validate_polygons}

Enables or disables throwing an exception in the [pointInPolygon](../../sql-reference/functions/geo/index.md#pointinpolygon) function, if the polygon is self-intersecting or self-tangent.

Possible values:

- 0 — Throwing an exception is disabled. `pointInPolygon` accepts invalid polygons and returns possibly incorrect results for them.
- 1 — Throwing an exception is enabled.

Default value: 1.

## transform_null_in {#transform_null_in}

Enables equality of [NULL](../../sql-reference/syntax.md/#null-literal) values for [IN](../../sql-reference/operators/in.md) operator.

By default, `NULL` values can’t be compared because `NULL` means undefined value. Thus, comparison `expr = NULL` must always return `false`. With this setting `NULL = NULL` returns `true` for `IN` operator.

Possible values:

- 0 — Comparison of `NULL` values in `IN` operator returns `false`.
- 1 — Comparison of `NULL` values in `IN` operator returns `true`.

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

- [NULL Processing in IN Operators](../../sql-reference/operators/in.md/#in-null-processing)

## low_cardinality_max_dictionary_size {#low_cardinality_max_dictionary_size}

Sets a maximum size in rows of a shared global dictionary for the [LowCardinality](../../sql-reference/data-types/lowcardinality.md) data type that can be written to a storage file system. This setting prevents issues with RAM in case of unlimited dictionary growth. All the data that can’t be encoded due to maximum dictionary size limitation ClickHouse writes in an ordinary method.

Possible values:

- Any positive integer.

Default value: 8192.

## low_cardinality_use_single_dictionary_for_part {#low_cardinality_use_single_dictionary_for_part}

Turns on or turns off using of single dictionary for the data part.

By default, the ClickHouse server monitors the size of dictionaries and if a dictionary overflows then the server starts to write the next one. To prohibit creating several dictionaries set `low_cardinality_use_single_dictionary_for_part = 1`.

Possible values:

- 1 — Creating several dictionaries for the data part is prohibited.
- 0 — Creating several dictionaries for the data part is not prohibited.

Default value: 0.

## low_cardinality_allow_in_native_format {#low_cardinality_allow_in_native_format}

Allows or restricts using the [LowCardinality](../../sql-reference/data-types/lowcardinality.md) data type with the [Native](../../interfaces/formats.md/#native) format.

If usage of `LowCardinality` is restricted, ClickHouse server converts `LowCardinality`-columns to ordinary ones for `SELECT` queries, and convert ordinary columns to `LowCardinality`-columns for `INSERT` queries.

This setting is required mainly for third-party clients which do not support `LowCardinality` data type.

Possible values:

- 1 — Usage of `LowCardinality` is not restricted.
- 0 — Usage of `LowCardinality` is restricted.

Default value: 1.

## allow_suspicious_low_cardinality_types {#allow_suspicious_low_cardinality_types}

Allows or restricts using [LowCardinality](../../sql-reference/data-types/lowcardinality.md) with data types with fixed size of 8 bytes or less: numeric data types and `FixedString(8_bytes_or_less)`.

For small fixed values using of `LowCardinality` is usually inefficient, because ClickHouse stores a numeric index for each row. As a result:

- Disk space usage can rise.
- RAM consumption can be higher, depending on a dictionary size.
- Some functions can work slower due to extra coding/encoding operations.

Merge times in [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md)-engine tables can grow due to all the reasons described above.

Possible values:

- 1 — Usage of `LowCardinality` is not restricted.
- 0 — Usage of `LowCardinality` is restricted.

Default value: 0.

## min_insert_block_size_rows_for_materialized_views {#min-insert-block-size-rows-for-materialized-views}

Sets the minimum number of rows in the block which can be inserted into a table by an `INSERT` query. Smaller-sized blocks are squashed into bigger ones. This setting is applied only for blocks inserted into [materialized view](../../sql-reference/statements/create/view.md). By adjusting this setting, you control blocks squashing while pushing to materialized view and avoid excessive memory usage.

Possible values:

- Any positive integer.
- 0 — Squashing disabled.

Default value: 1048576.

**See Also**

- [min_insert_block_size_rows](#min-insert-block-size-rows)

## min_insert_block_size_bytes_for_materialized_views {#min-insert-block-size-bytes-for-materialized-views}

Sets the minimum number of bytes in the block which can be inserted into a table by an `INSERT` query. Smaller-sized blocks are squashed into bigger ones. This setting is applied only for blocks inserted into [materialized view](../../sql-reference/statements/create/view.md). By adjusting this setting, you control blocks squashing while pushing to materialized view and avoid excessive memory usage.

Possible values:

- Any positive integer.
- 0 — Squashing disabled.

Default value: 268435456.

**See also**

- [min_insert_block_size_bytes](#min-insert-block-size-bytes)

## optimize_read_in_order {#optimize_read_in_order}

Enables [ORDER BY](../../sql-reference/statements/select/order-by.md/#optimize_read_in_order) optimization in [SELECT](../../sql-reference/statements/select/index.md) queries for reading data from [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) tables.

Possible values:

- 0 — `ORDER BY` optimization is disabled.
- 1 — `ORDER BY` optimization is enabled.

Default value: `1`.

**See Also**

- [ORDER BY Clause](../../sql-reference/statements/select/order-by.md/#optimize_read_in_order)

## optimize_aggregation_in_order {#optimize_aggregation_in_order}

Enables [GROUP BY](../../sql-reference/statements/select/group-by.md) optimization in [SELECT](../../sql-reference/statements/select/index.md) queries for aggregating data in corresponding order in [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) tables.

Possible values:

- 0 — `GROUP BY` optimization is disabled.
- 1 — `GROUP BY` optimization is enabled.

Default value: `0`.

**See Also**

- [GROUP BY optimization](../../sql-reference/statements/select/group-by.md/#aggregation-in-order)

## mutations_sync {#mutations_sync}

Allows to execute `ALTER TABLE ... UPDATE|DELETE|MATERIALIZE INDEX|MATERIALIZE PROJECTION|MATERIALIZE COLUMN` queries ([mutations](../../sql-reference/statements/alter/index.md#mutations)) synchronously.

Possible values:

- 0 - Mutations execute asynchronously.
- 1 - The query waits for all mutations to complete on the current server.
- 2 - The query waits for all mutations to complete on all replicas (if they exist).

Default value: `0`.

## lightweight_deletes_sync {#lightweight_deletes_sync}

The same as 'mutation_sync', but controls only execution of lightweight deletes.

Possible values:

- 0 - Mutations execute asynchronously.
- 1 - The query waits for the lightweight deletes to complete on the current server.
- 2 - The query waits for the lightweight deletes to complete on all replicas (if they exist).

Default value: `2`.

**See Also**

- [Synchronicity of ALTER Queries](../../sql-reference/statements/alter/index.md#synchronicity-of-alter-queries)
- [Mutations](../../sql-reference/statements/alter/index.md#mutations)

## ttl_only_drop_parts {#ttl_only_drop_parts}

Enables or disables complete dropping of data parts where all rows are expired in [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) tables.

When `ttl_only_drop_parts` is disabled (by default), the ClickHouse server only deletes expired rows according to their TTL.

When `ttl_only_drop_parts` is enabled, the ClickHouse server drops a whole part when all rows in it are expired.

Dropping whole parts instead of partial cleaning TTL-d rows allows having shorter `merge_with_ttl_timeout` times and lower impact on system performance.

Possible values:

- 0 — The complete dropping of data parts is disabled.
- 1 — The complete dropping of data parts is enabled.

Default value: `0`.

**See Also**

- [CREATE TABLE query clauses and settings](../../engines/table-engines/mergetree-family/mergetree.md/#mergetree-query-clauses) (`merge_with_ttl_timeout` setting)
- [Table TTL](../../engines/table-engines/mergetree-family/mergetree.md/#mergetree-table-ttl)

## lock_acquire_timeout {#lock_acquire_timeout}

Defines how many seconds a locking request waits before failing.

Locking timeout is used to protect from deadlocks while executing read/write operations with tables. When the timeout expires and the locking request fails, the ClickHouse server throws an exception "Locking attempt timed out! Possible deadlock avoided. Client should retry." with error code `DEADLOCK_AVOIDED`.

Possible values:

- Positive integer (in seconds).
- 0 — No locking timeout.

Default value: `120` seconds.

## cast_keep_nullable {#cast_keep_nullable}

Enables or disables keeping of the `Nullable` data type in [CAST](../../sql-reference/functions/type-conversion-functions.md/#castx-t) operations.

When the setting is enabled and the argument of `CAST` function is `Nullable`, the result is also transformed to `Nullable` type. When the setting is disabled, the result always has the destination type exactly.

Possible values:

- 0 — The `CAST` result has exactly the destination type specified.
- 1 — If the argument type is `Nullable`, the `CAST` result is transformed to `Nullable(DestinationDataType)`.

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

- [CAST](../../sql-reference/functions/type-conversion-functions.md/#type_conversion_function-cast) function

## system_events_show_zero_values {#system_events_show_zero_values}

Allows to select zero-valued events from [`system.events`](../../operations/system-tables/events.md).

Some monitoring systems require passing all the metrics values to them for each checkpoint, even if the metric value is zero.

Possible values:

- 0 — Disabled.
- 1 — Enabled.

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

## allow_nullable_key {#allow-nullable-key}

Allows using of the [Nullable](../../sql-reference/data-types/nullable.md/#data_type-nullable)-typed values in a sorting and a primary key for [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md/#table_engines-mergetree) tables.

Possible values:

- 1 — `Nullable`-type expressions are allowed in keys.
- 0 — `Nullable`-type expressions are not allowed in keys.

Default value: `0`.

:::note
Nullable primary key usually indicates bad design. It is forbidden in almost all main stream DBMS. The feature is mainly for [AggregatingMergeTree](../../engines/table-engines/mergetree-family/aggregatingmergetree.md) and is not heavily tested. Use with care.
:::

:::note
Do not enable this feature in version `<= 21.8`. It's not properly implemented and may lead to server crash.
:::

## aggregate_functions_null_for_empty {#aggregate_functions_null_for_empty}

Enables or disables rewriting all aggregate functions in a query, adding [-OrNull](../../sql-reference/aggregate-functions/combinators.md/#agg-functions-combinator-ornull) suffix to them. Enable it for SQL standard compatibility.
It is implemented via query rewrite (similar to [count_distinct_implementation](#count_distinct_implementation) setting) to get consistent results for distributed queries.

Possible values:

- 0 — Disabled.
- 1 — Enabled.

Default value: 0.

**Example**

Consider the following query with aggregate functions:
```sql
SELECT SUM(-1), MAX(0) FROM system.one WHERE 0;
```

With `aggregate_functions_null_for_empty = 0` it would produce:
```text
┌─SUM(-1)─┬─MAX(0)─┐
│       0 │      0 │
└─────────┴────────┘
```

With `aggregate_functions_null_for_empty = 1` the result would be:
```text
┌─SUMOrNull(-1)─┬─MAXOrNull(0)─┐
│          NULL │         NULL │
└───────────────┴──────────────┘
```

## union_default_mode {#union-default-mode}

Sets a mode for combining `SELECT` query results. The setting is only used when shared with [UNION](../../sql-reference/statements/select/union.md) without explicitly specifying the `UNION ALL` or `UNION DISTINCT`.

Possible values:

- `'DISTINCT'` — ClickHouse outputs rows as a result of combining queries removing duplicate rows.
- `'ALL'` — ClickHouse outputs all rows as a result of combining queries including duplicate rows.
- `''` — ClickHouse generates an exception when used with `UNION`.

Default value: `''`.

See examples in [UNION](../../sql-reference/statements/select/union.md).

## default_table_engine {#default_table_engine}

Default table engine to use when `ENGINE` is not set in a `CREATE` statement.

Possible values:

- a string representing any valid table engine name

Default value: `MergeTree`.

Cloud default value: `SharedMergeTree`.

**Example**

Query:

```sql
SET default_table_engine = 'Log';

SELECT name, value, changed FROM system.settings WHERE name = 'default_table_engine';
```

Result:

```response
┌─name─────────────────┬─value─┬─changed─┐
│ default_table_engine │ Log   │       1 │
└──────────────────────┴───────┴─────────┘
```

In this example, any new table that does not specify an `Engine` will use the `Log` table engine:

Query:

```sql
CREATE TABLE my_table (
    x UInt32,
    y UInt32
);

SHOW CREATE TABLE my_table;
```

Result:

```response
┌─statement────────────────────────────────────────────────────────────────┐
│ CREATE TABLE default.my_table
(
    `x` UInt32,
    `y` UInt32
)
ENGINE = Log
└──────────────────────────────────────────────────────────────────────────┘
```

## default_temporary_table_engine {#default_temporary_table_engine}

Same as [default_table_engine](#default_table_engine) but for temporary tables.

Default value: `Memory`.

In this example, any new temporary table that does not specify an `Engine` will use the `Log` table engine:

Query:

```sql
SET default_temporary_table_engine = 'Log';

CREATE TEMPORARY TABLE my_table (
    x UInt32,
    y UInt32
);

SHOW CREATE TEMPORARY TABLE my_table;
```

Result:

```response
┌─statement────────────────────────────────────────────────────────────────┐
│ CREATE TEMPORARY TABLE default.my_table
(
    `x` UInt32,
    `y` UInt32
)
ENGINE = Log
└──────────────────────────────────────────────────────────────────────────┘
```

## data_type_default_nullable {#data_type_default_nullable}

Allows data types without explicit modifiers [NULL or NOT NULL](../../sql-reference/statements/create/table.md/#null-modifiers) in column definition will be [Nullable](../../sql-reference/data-types/nullable.md/#data_type-nullable).

Possible values:

- 1 — The data types in column definitions are set to `Nullable` by default.
- 0 — The data types in column definitions are set to not `Nullable` by default.

Default value: `0`.

## mysql_map_string_to_text_in_show_columns {#mysql_map_string_to_text_in_show_columns}

When enabled, [String](../../sql-reference/data-types/string.md) ClickHouse data type will be displayed as `TEXT` in [SHOW COLUMNS](../../sql-reference/statements/show.md#show_columns).

Has an effect only when the connection is made through the MySQL wire protocol.

- 0 - Use `BLOB`.
- 1 - Use `TEXT`.

Default value: `1`.

## mysql_map_fixed_string_to_text_in_show_columns {#mysql_map_fixed_string_to_text_in_show_columns}

When enabled, [FixedString](../../sql-reference/data-types/fixedstring.md) ClickHouse data type will be displayed as `TEXT` in [SHOW COLUMNS](../../sql-reference/statements/show.md#show_columns).

Has an effect only when the connection is made through the MySQL wire protocol.

- 0 - Use `BLOB`.
- 1 - Use `TEXT`.

Default value: `1`.

## execute_merges_on_single_replica_time_threshold {#execute-merges-on-single-replica-time-threshold}

Enables special logic to perform merges on replicas.

Possible values:

- Positive integer (in seconds).
- 0 — Special merges logic is not used. Merges happen in the usual way on all the replicas.

Default value: `0`.

**Usage**

Selects one replica to perform the merge on. Sets the time threshold from the start of the merge. Other replicas wait for the merge to finish, then download the result. If the time threshold passes and the selected replica does not perform the merge, then the merge is performed on other replicas as usual.

High values for that threshold may lead to replication delays.

It can be useful when merges are CPU bounded not IO bounded (performing heavy data compression, calculating aggregate functions or default expressions that require a large amount of calculations, or just very high number of tiny merges).

## max_final_threads {#max-final-threads}

Sets the maximum number of parallel threads for the `SELECT` query data read phase with the [FINAL](../../sql-reference/statements/select/from.md#select-from-final) modifier.

Possible values:

- Positive integer.
- 0 or 1 — Disabled. `SELECT` queries are executed in a single thread.

Default value: `max_threads`.

## opentelemetry_start_trace_probability {#opentelemetry-start-trace-probability}

Sets the probability that the ClickHouse can start a trace for executed queries (if no parent [trace context](https://www.w3.org/TR/trace-context/) is supplied).

Possible values:

- 0 — The trace for all executed queries is disabled (if no parent trace context is supplied).
- Positive floating-point number in the range [0..1]. For example, if the setting value is `0,5`, ClickHouse can start a trace on average for half of the queries.
- 1 — The trace for all executed queries is enabled.

Default value: `0`.

## optimize_on_insert {#optimize-on-insert}

Enables or disables data transformation before the insertion, as if merge was done on this block (according to table engine).

Possible values:

- 0 — Disabled.
- 1 — Enabled.

Default value: 1.

**Example**

The difference between enabled and disabled:

Query:

```sql
SET optimize_on_insert = 1;

CREATE TABLE test1 (`FirstTable` UInt32) ENGINE = ReplacingMergeTree ORDER BY FirstTable;

INSERT INTO test1 SELECT number % 2 FROM numbers(5);

SELECT * FROM test1;

SET optimize_on_insert = 0;

CREATE TABLE test2 (`SecondTable` UInt32) ENGINE = ReplacingMergeTree ORDER BY SecondTable;

INSERT INTO test2 SELECT number % 2 FROM numbers(5);

SELECT * FROM test2;
```

Result:

``` text
┌─FirstTable─┐
│          0 │
│          1 │
└────────────┘

┌─SecondTable─┐
│           0 │
│           0 │
│           0 │
│           1 │
│           1 │
└─────────────┘
```

Note that this setting influences [Materialized view](../../sql-reference/statements/create/view.md/#materialized) and [MaterializedMySQL](../../engines/database-engines/materialized-mysql.md) behaviour.

## engine_file_empty_if_not_exists {#engine-file-empty_if-not-exists}

Allows to select data from a file engine table without file.

Possible values:
- 0 — `SELECT` throws exception.
- 1 — `SELECT` returns empty result.

Default value: `0`.

## engine_file_truncate_on_insert {#engine-file-truncate-on-insert}

Enables or disables truncate before insert in [File](../../engines/table-engines/special/file.md) engine tables.

Possible values:
- 0 — `INSERT` query appends new data to the end of the file.
- 1 — `INSERT` query replaces existing content of the file with the new data.

Default value: `0`.

## engine_file_allow_create_multiple_files {#engine_file_allow_create_multiple_files}

Enables or disables creating a new file on each insert in file engine tables if the format has the suffix (`JSON`, `ORC`, `Parquet`, etc.). If enabled, on each insert a new file will be created with a name following this pattern:

`data.Parquet` -> `data.1.Parquet` -> `data.2.Parquet`, etc.

Possible values:
- 0 — `INSERT` query appends new data to the end of the file.
- 1 — `INSERT` query creates a new file.

Default value: `0`.

## engine_file_skip_empty_files {#engine_file_skip_empty_files}

Enables or disables skipping empty files in [File](../../engines/table-engines/special/file.md) engine tables.

Possible values:
- 0 — `SELECT` throws an exception if empty file is not compatible with requested format.
- 1 — `SELECT` returns empty result for empty file.

Default value: `0`.

## storage_file_read_method {#storage_file_read_method}

Method of reading data from storage file, one of: `read`, `pread`, `mmap`. The mmap method does not apply to clickhouse-server (it's intended for clickhouse-local).

Default value: `pread` for clickhouse-server, `mmap` for clickhouse-local.

## s3_truncate_on_insert {#s3_truncate_on_insert}

Enables or disables truncate before inserts in s3 engine tables. If disabled, an exception will be thrown on insert attempts if an S3 object already exists.

Possible values:
- 0 — `INSERT` query appends new data to the end of the file.
- 1 — `INSERT` query replaces existing content of the file with the new data.

Default value: `0`.

## s3_create_new_file_on_insert {#s3_create_new_file_on_insert}

Enables or disables creating a new file on each insert in s3 engine tables. If enabled, on each insert a new S3 object will be created with the key, similar to this pattern:

initial: `data.Parquet.gz` -> `data.1.Parquet.gz` -> `data.2.Parquet.gz`, etc.

Possible values:
- 0 — `INSERT` query appends new data to the end of the file.
- 1 — `INSERT` query creates a new file.

Default value: `0`.

## s3_skip_empty_files {#s3_skip_empty_files}

Enables or disables skipping empty files in [S3](../../engines/table-engines/integrations/s3.md) engine tables.

Possible values:
- 0 — `SELECT` throws an exception if empty file is not compatible with requested format.
- 1 — `SELECT` returns empty result for empty file.

Default value: `0`.

## s3_ignore_file_doesnt_exist {#s3_ignore_file_doesnt_exist}

Ignore absence of file if it does not exist when reading certain keys.

Possible values:
- 1 — `SELECT` returns empty result.
- 0 — `SELECT` throws an exception.

Default value: `0`.

## s3_validate_request_settings {#s3_validate_request_settings}

Enables s3 request settings validation.

Possible values:
- 1 — validate settings.
- 0 — do not validate settings.

Default value: `1`.

## hdfs_truncate_on_insert {#hdfs_truncate_on_insert}

Enables or disables truncation before an insert in hdfs engine tables. If disabled, an exception will be thrown on an attempt to insert if a file in HDFS already exists.

Possible values:
- 0 — `INSERT` query appends new data to the end of the file.
- 1 — `INSERT` query replaces existing content of the file with the new data.

Default value: `0`.

## hdfs_create_new_file_on_insert {#hdfs_create_new_file_on_insert

Enables or disables creating a new file on each insert in HDFS engine tables. If enabled, on each insert a new HDFS file will be created with the name, similar to this pattern:

initial: `data.Parquet.gz` -> `data.1.Parquet.gz` -> `data.2.Parquet.gz`, etc.

Possible values:
- 0 — `INSERT` query appends new data to the end of the file.
- 1 — `INSERT` query creates a new file.

Default value: `0`.

## hdfs_skip_empty_files {#hdfs_skip_empty_files}

Enables or disables skipping empty files in [HDFS](../../engines/table-engines/integrations/hdfs.md) engine tables.

Possible values:
- 0 — `SELECT` throws an exception if empty file is not compatible with requested format.
- 1 — `SELECT` returns empty result for empty file.

Default value: `0`.

## hdfs_throw_on_zero_files_match {#hdfs_throw_on_zero_files_match}

Throw an error if matched zero files according to glob expansion rules.

Possible values:
- 1 — `SELECT` throws an exception.
- 0 — `SELECT` returns empty result.

Default value: `0`.

## hdfs_ignore_file_doesnt_exist {#hdfs_ignore_file_doesnt_exist}

Ignore absence of file if it does not exist when reading certain keys.

Possible values:
- 1 — `SELECT` returns empty result.
- 0 — `SELECT` throws an exception.

Default value: `0`.

## azure_throw_on_zero_files_match {#azure_throw_on_zero_files_match}

Throw an error if matched zero files according to glob expansion rules.

Possible values:
- 1 — `SELECT` throws an exception.
- 0 — `SELECT` returns empty result.

Default value: `0`.

## azure_ignore_file_doesnt_exist {#azure_ignore_file_doesnt_exist}

Ignore absence of file if it does not exist when reading certain keys.

Possible values:
- 1 — `SELECT` returns empty result.
- 0 — `SELECT` throws an exception.

Default value: `0`.

## azure_skip_empty_files {#azure_skip_empty_files}

Enables or disables skipping empty files in S3 engine.

Possible values:
- 0 — `SELECT` throws an exception if empty file is not compatible with requested format.
- 1 — `SELECT` returns empty result for empty file.

Default value: `0`.

## engine_url_skip_empty_files {#engine_url_skip_empty_files}

Enables or disables skipping empty files in [URL](../../engines/table-engines/special/url.md) engine tables.

Possible values:
- 0 — `SELECT` throws an exception if empty file is not compatible with requested format.
- 1 — `SELECT` returns empty result for empty file.

Default value: `0`.

## enable_url_encoding {#enable_url_encoding}

Allows to enable/disable decoding/encoding path in uri in [URL](../../engines/table-engines/special/url.md) engine tables.

Enabled by default.

## database_atomic_wait_for_drop_and_detach_synchronously {#database_atomic_wait_for_drop_and_detach_synchronously}

Adds a modifier `SYNC` to all `DROP` and `DETACH` queries.

Possible values:

- 0 — Queries will be executed with delay.
- 1 — Queries will be executed without delay.

Default value: `0`.

## show_table_uuid_in_table_create_query_if_not_nil {#show_table_uuid_in_table_create_query_if_not_nil}

Sets the `SHOW TABLE` query display.

Possible values:

- 0 — The query will be displayed without table UUID.
- 1 — The query will be displayed with table UUID.

Default value: `0`.

## allow_experimental_live_view {#allow-experimental-live-view}

Allows creation of a deprecated LIVE VIEW.

Possible values:

- 0 — Working with live views is disabled.
- 1 — Working with live views is enabled.

Default value: `0`.

## live_view_heartbeat_interval {#live-view-heartbeat-interval}

Deprecated.

## max_live_view_insert_blocks_before_refresh {#max-live-view-insert-blocks-before-refresh}

Deprecated.

## periodic_live_view_refresh {#periodic-live-view-refresh}

Deprecated.

## http_connection_timeout {#http_connection_timeout}

HTTP connection timeout (in seconds).

Possible values:

- Any positive integer.
- 0 - Disabled (infinite timeout).

Default value: 1.

## http_send_timeout {#http_send_timeout}

HTTP send timeout (in seconds).

Possible values:

- Any positive integer.
- 0 - Disabled (infinite timeout).

Default value: 30.

:::note
It's applicable only to the default profile. A server reboot is required for the changes to take effect.
:::

## http_receive_timeout {#http_receive_timeout}

HTTP receive timeout (in seconds).

Possible values:

- Any positive integer.
- 0 - Disabled (infinite timeout).

Default value: 30.

## check_query_single_value_result {#check_query_single_value_result}

Defines the level of detail for the [CHECK TABLE](../../sql-reference/statements/check-table.md/#checking-mergetree-tables) query result for `MergeTree` family engines .

Possible values:

- 0 — the query shows a check status for every individual data part of a table.
- 1 — the query shows the general table check status.

Default value: `0`.

## prefer_column_name_to_alias {#prefer-column-name-to-alias}

Enables or disables using the original column names instead of aliases in query expressions and clauses. It especially matters when alias is the same as the column name, see [Expression Aliases](../../sql-reference/syntax.md/#notes-on-usage). Enable this setting to make aliases syntax rules in ClickHouse more compatible with most other database engines.

Possible values:

- 0 — The column name is substituted with the alias.
- 1 — The column name is not substituted with the alias.

Default value: `0`.

**Example**

The difference between enabled and disabled:

Query:

```sql
SET prefer_column_name_to_alias = 0;
SELECT avg(number) AS number, max(number) FROM numbers(10);
```

Result:

```text
Received exception from server (version 21.5.1):
Code: 184. DB::Exception: Received from localhost:9000. DB::Exception: Aggregate function avg(number) is found inside another aggregate function in query: While processing avg(number) AS number.
```

Query:

```sql
SET prefer_column_name_to_alias = 1;
SELECT avg(number) AS number, max(number) FROM numbers(10);
```

Result:

```text
┌─number─┬─max(number)─┐
│    4.5 │           9 │
└────────┴─────────────┘
```

## limit {#limit}

Sets the maximum number of rows to get from the query result. It adjusts the value set by the [LIMIT](../../sql-reference/statements/select/limit.md/#limit-clause) clause, so that the limit, specified in the query, cannot exceed the limit, set by this setting.

Possible values:

- 0 — The number of rows is not limited.
- Positive integer.

Default value: `0`.

## offset {#offset}

Sets the number of rows to skip before starting to return rows from the query. It adjusts the offset set by the [OFFSET](../../sql-reference/statements/select/offset.md/#offset-fetch) clause, so that these two values are summarized.

Possible values:

- 0 — No rows are skipped .
- Positive integer.

Default value: `0`.

**Example**

Input table:

``` sql
CREATE TABLE test (i UInt64) ENGINE = MergeTree() ORDER BY i;
INSERT INTO test SELECT number FROM numbers(500);
```

Query:

``` sql
SET limit = 5;
SET offset = 7;
SELECT * FROM test LIMIT 10 OFFSET 100;
```
Result:

``` text
┌───i─┐
│ 107 │
│ 108 │
│ 109 │
└─────┘
```

## optimize_syntax_fuse_functions {#optimize_syntax_fuse_functions}

Enables to fuse aggregate functions with identical argument. It rewrites query contains at least two aggregate functions from [sum](../../sql-reference/aggregate-functions/reference/sum.md/#agg_function-sum), [count](../../sql-reference/aggregate-functions/reference/count.md/#agg_function-count) or [avg](../../sql-reference/aggregate-functions/reference/avg.md/#agg_function-avg) with identical argument to [sumCount](../../sql-reference/aggregate-functions/reference/sumcount.md/#agg_function-sumCount).

Possible values:

- 0 — Functions with identical argument are not fused.
- 1 — Functions with identical argument are fused.

Default value: `0`.

**Example**

Query:

``` sql
CREATE TABLE fuse_tbl(a Int8, b Int8) Engine = Log;
SET optimize_syntax_fuse_functions = 1;
EXPLAIN SYNTAX SELECT sum(a), sum(b), count(b), avg(b) from fuse_tbl FORMAT TSV;
```

Result:

``` text
SELECT
    sum(a),
    sumCount(b).1,
    sumCount(b).2,
    (sumCount(b).1) / (sumCount(b).2)
FROM fuse_tbl
```

## optimize_rewrite_aggregate_function_with_if

Rewrite aggregate functions with if expression as argument when logically equivalent.
For example, `avg(if(cond, col, null))` can be rewritten to `avgOrNullIf(cond, col)`. It may improve performance.

:::note
Supported only with experimental analyzer (`allow_experimental_analyzer = 1`).
:::

## database_replicated_initial_query_timeout_sec {#database_replicated_initial_query_timeout_sec}

Sets how long initial DDL query should wait for Replicated database to process previous DDL queue entries in seconds.

Possible values:

- Positive integer.
- 0 — Unlimited.

Default value: `300`.

## distributed_ddl_task_timeout {#distributed_ddl_task_timeout}

Sets timeout for DDL query responses from all hosts in cluster. If a DDL request has not been performed on all hosts, a response will contain a timeout error and a request will be executed in an async mode. Negative value means infinite.

Possible values:

- Positive integer.
- 0 — Async mode.
- Negative integer — infinite timeout.

Default value: `180`.

## distributed_ddl_output_mode {#distributed_ddl_output_mode}

Sets format of distributed DDL query result.

Possible values:

- `throw` — Returns result set with query execution status for all hosts where query is finished. If query has failed on some hosts, then it will rethrow the first exception. If query is not finished yet on some hosts and [distributed_ddl_task_timeout](#distributed_ddl_task_timeout) exceeded, then it throws `TIMEOUT_EXCEEDED` exception.
- `none` — Is similar to throw, but distributed DDL query returns no result set.
- `null_status_on_timeout` — Returns `NULL` as execution status in some rows of result set instead of throwing `TIMEOUT_EXCEEDED` if query is not finished on the corresponding hosts.
- `never_throw` — Do not throw `TIMEOUT_EXCEEDED` and do not rethrow exceptions if query has failed on some hosts.
- `none_only_active` - similar to `none`, but doesn't wait for inactive replicas of the `Replicated` database. Note: with this mode it's impossible to figure out that the query was not executed on some replica and will be executed in background.
- `null_status_on_timeout_only_active` — similar to `null_status_on_timeout`, but doesn't wait for inactive replicas of the `Replicated` database
- `throw_only_active` — similar to `throw`, but doesn't wait for inactive replicas of the `Replicated` database

Default value: `throw`.

Cloud default value: `none`.

## flatten_nested {#flatten-nested}

Sets the data format of a [nested](../../sql-reference/data-types/nested-data-structures/index.md) columns.

Possible values:

- 1 — Nested column is flattened to separate arrays.
- 0 — Nested column stays a single array of tuples.

Default value: `1`.

**Usage**

If the setting is set to `0`, it is possible to use an arbitrary level of nesting.

**Examples**

Query:

``` sql
SET flatten_nested = 1;
CREATE TABLE t_nest (`n` Nested(a UInt32, b UInt32)) ENGINE = MergeTree ORDER BY tuple();

SHOW CREATE TABLE t_nest;
```

Result:

``` text
┌─statement───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ CREATE TABLE default.t_nest
(
    `n.a` Array(UInt32),
    `n.b` Array(UInt32)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 8192 │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

Query:

``` sql
SET flatten_nested = 0;

CREATE TABLE t_nest (`n` Nested(a UInt32, b UInt32)) ENGINE = MergeTree ORDER BY tuple();

SHOW CREATE TABLE t_nest;
```

Result:

``` text
┌─statement──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ CREATE TABLE default.t_nest
(
    `n` Nested(a UInt32, b UInt32)
)
ENGINE = MergeTree
ORDER BY tuple()
SETTINGS index_granularity = 8192 │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## external_table_functions_use_nulls {#external-table-functions-use-nulls}

Defines how [mysql](../../sql-reference/table-functions/mysql.md), [postgresql](../../sql-reference/table-functions/postgresql.md) and [odbc](../../sql-reference/table-functions/odbc.md) table functions use Nullable columns.

Possible values:

- 0 — The table function explicitly uses Nullable columns.
- 1 — The table function implicitly uses Nullable columns.

Default value: `1`.

**Usage**

If the setting is set to `0`, the table function does not make Nullable columns and inserts default values instead of NULL. This is also applicable for NULL values inside arrays.

## optimize_use_projections {#optimize_use_projections}

Enables or disables [projection](../../engines/table-engines/mergetree-family/mergetree.md/#projections) optimization when processing `SELECT` queries.

Possible values:

- 0 — Projection optimization disabled.
- 1 — Projection optimization enabled.

Default value: `1`.

## force_optimize_projection {#force-optimize-projection}

Enables or disables the obligatory use of [projections](../../engines/table-engines/mergetree-family/mergetree.md/#projections) in `SELECT` queries, when projection optimization is enabled (see [optimize_use_projections](#optimize_use_projections) setting).

Possible values:

- 0 — Projection optimization is not obligatory.
- 1 — Projection optimization is obligatory.

Default value: `0`.

## force_optimize_projection_name {#force-optimize-projection_name}

If it is set to a non-empty string, check that this projection is used in the query at least once.

Possible values:

- string: name of projection that used in a query

Default value: `''`.

## preferred_optimize_projection_name {#preferred_optimize_projection_name}

If it is set to a non-empty string, ClickHouse will try to apply specified projection in query.


Possible values:

- string: name of preferred projection

Default value: `''`.

## alter_sync {#alter-sync}

Allows to set up waiting for actions to be executed on replicas by [ALTER](../../sql-reference/statements/alter/index.md), [OPTIMIZE](../../sql-reference/statements/optimize.md) or [TRUNCATE](../../sql-reference/statements/truncate.md) queries.

Possible values:

- 0 — Do not wait.
- 1 — Wait for own execution.
- 2 — Wait for everyone.

Default value: `1`.

Cloud default value: `0`.

:::note
`alter_sync` is applicable to `Replicated` tables only, it does nothing to alters of not `Replicated` tables.
:::

## replication_wait_for_inactive_replica_timeout {#replication-wait-for-inactive-replica-timeout}

Specifies how long (in seconds) to wait for inactive replicas to execute [ALTER](../../sql-reference/statements/alter/index.md), [OPTIMIZE](../../sql-reference/statements/optimize.md) or [TRUNCATE](../../sql-reference/statements/truncate.md) queries.

Possible values:

- 0 — Do not wait.
- Negative integer — Wait for unlimited time.
- Positive integer — The number of seconds to wait.

Default value: `120` seconds.

## regexp_max_matches_per_row {#regexp-max-matches-per-row}

Sets the maximum number of matches for a single regular expression per row. Use it to protect against memory overload when using greedy regular expression in the [extractAllGroupsHorizontal](../../sql-reference/functions/string-search-functions.md/#extractallgroups-horizontal) function.

Possible values:

- Positive integer.

Default value: `1000`.

## http_max_single_read_retries {#http-max-single-read-retries}

Sets the maximum number of retries during a single HTTP read.

Possible values:

- Positive integer.

Default value: `1024`.

## log_queries_probability {#log-queries-probability}

Allows a user to write to [query_log](../../operations/system-tables/query_log.md), [query_thread_log](../../operations/system-tables/query_thread_log.md), and [query_views_log](../../operations/system-tables/query_views_log.md) system tables only a sample of queries selected randomly with the specified probability. It helps to reduce the load with a large volume of queries in a second.

Possible values:

- 0 — Queries are not logged in the system tables.
- Positive floating-point number in the range [0..1]. For example, if the setting value is `0.5`, about half of the queries are logged in the system tables.
- 1 — All queries are logged in the system tables.

Default value: `1`.

## short_circuit_function_evaluation {#short-circuit-function-evaluation}

Allows calculating the [if](../../sql-reference/functions/conditional-functions.md/#if), [multiIf](../../sql-reference/functions/conditional-functions.md/#multiif), [and](../../sql-reference/functions/logical-functions.md/#logical-and-function), and [or](../../sql-reference/functions/logical-functions.md/#logical-or-function) functions according to a [short scheme](https://en.wikipedia.org/wiki/Short-circuit_evaluation). This helps optimize the execution of complex expressions in these functions and prevent possible exceptions (such as division by zero when it is not expected).

Possible values:

- `enable` — Enables short-circuit function evaluation for functions that are suitable for it (can throw an exception or computationally heavy).
- `force_enable` — Enables short-circuit function evaluation for all functions.
- `disable` — Disables short-circuit function evaluation.

Default value: `enable`.

## max_hyperscan_regexp_length {#max-hyperscan-regexp-length}

Defines the maximum length for each regular expression in the [hyperscan multi-match functions](../../sql-reference/functions/string-search-functions.md/#multimatchanyhaystack-pattern1-pattern2-patternn).

Possible values:

- Positive integer.
- 0 - The length is not limited.

Default value: `0`.

**Example**

Query:

```sql
SELECT multiMatchAny('abcd', ['ab','bcd','c','d']) SETTINGS max_hyperscan_regexp_length = 3;
```

Result:

```text
┌─multiMatchAny('abcd', ['ab', 'bcd', 'c', 'd'])─┐
│                                              1 │
└────────────────────────────────────────────────┘
```

Query:

```sql
SELECT multiMatchAny('abcd', ['ab','bcd','c','d']) SETTINGS max_hyperscan_regexp_length = 2;
```

Result:

```text
Exception: Regexp length too large.
```

**See Also**

- [max_hyperscan_regexp_total_length](#max-hyperscan-regexp-total-length)

## max_hyperscan_regexp_total_length {#max-hyperscan-regexp-total-length}

Sets the maximum length total of all regular expressions in each [hyperscan multi-match function](../../sql-reference/functions/string-search-functions.md/#multimatchanyhaystack-pattern1-pattern2-patternn).

Possible values:

- Positive integer.
- 0 - The length is not limited.

Default value: `0`.

**Example**

Query:

```sql
SELECT multiMatchAny('abcd', ['a','b','c','d']) SETTINGS max_hyperscan_regexp_total_length = 5;
```

Result:

```text
┌─multiMatchAny('abcd', ['a', 'b', 'c', 'd'])─┐
│                                           1 │
└─────────────────────────────────────────────┘
```

Query:

```sql
SELECT multiMatchAny('abcd', ['ab','bc','c','d']) SETTINGS max_hyperscan_regexp_total_length = 5;
```

Result:

```text
Exception: Total regexp lengths too large.
```

**See Also**

- [max_hyperscan_regexp_length](#max-hyperscan-regexp-length)

## enable_positional_arguments {#enable-positional-arguments}

Enables or disables supporting positional arguments for [GROUP BY](../../sql-reference/statements/select/group-by.md), [LIMIT BY](../../sql-reference/statements/select/limit-by.md), [ORDER BY](../../sql-reference/statements/select/order-by.md) statements.

Possible values:

- 0 — Positional arguments aren't supported.
- 1 — Positional arguments are supported: column numbers can use instead of column names.

Default value: `1`.

**Example**

Query:

```sql
CREATE TABLE positional_arguments(one Int, two Int, three Int) ENGINE=Memory();

INSERT INTO positional_arguments VALUES (10, 20, 30), (20, 20, 10), (30, 10, 20);

SELECT * FROM positional_arguments ORDER BY 2,3;
```

Result:

```text
┌─one─┬─two─┬─three─┐
│  30 │  10 │   20  │
│  20 │  20 │   10  │
│  10 │  20 │   30  │
└─────┴─────┴───────┘
```

## enable_order_by_all {#enable-order-by-all}

Enables or disables sorting with `ORDER BY ALL` syntax, see [ORDER BY](../../sql-reference/statements/select/order-by.md).

Possible values:

- 0 — Disable ORDER BY ALL.
- 1 — Enable ORDER BY ALL.

Default value: `1`.

**Example**

Query:

```sql
CREATE TABLE TAB(C1 Int, C2 Int, ALL Int) ENGINE=Memory();

INSERT INTO TAB VALUES (10, 20, 30), (20, 20, 10), (30, 10, 20);

SELECT * FROM TAB ORDER BY ALL; -- returns an error that ALL is ambiguous

SELECT * FROM TAB ORDER BY ALL SETTINGS enable_order_by_all = 0;
```

Result:

```text
┌─C1─┬─C2─┬─ALL─┐
│ 20 │ 20 │  10 │
│ 30 │ 10 │  20 │
│ 10 │ 20 │  30 │
└────┴────┴─────┘
```

## splitby_max_substrings_includes_remaining_string {#splitby_max_substrings_includes_remaining_string}

Controls whether function [splitBy*()](../../sql-reference/functions/splitting-merging-functions.md) with argument `max_substrings` > 0 will include the remaining string in the last element of the result array.

Possible values:

- `0` - The remaining string will not be included in the last element of the result array.
- `1` - The remaining string will be included in the last element of the result array. This is the behavior of Spark's [`split()`](https://spark.apache.org/docs/3.1.2/api/python/reference/api/pyspark.sql.functions.split.html) function and Python's ['string.split()'](https://docs.python.org/3/library/stdtypes.html#str.split) method.

Default value: `0`

## enable_extended_results_for_datetime_functions {#enable-extended-results-for-datetime-functions}

Enables or disables returning results of type:
- `Date32` with extended range (compared to type `Date`) for functions [toStartOfYear](../../sql-reference/functions/date-time-functions.md#tostartofyear), [toStartOfISOYear](../../sql-reference/functions/date-time-functions.md#tostartofisoyear), [toStartOfQuarter](../../sql-reference/functions/date-time-functions.md#tostartofquarter), [toStartOfMonth](../../sql-reference/functions/date-time-functions.md#tostartofmonth), [toLastDayOfMonth](../../sql-reference/functions/date-time-functions.md#tolastdayofmonth), [toStartOfWeek](../../sql-reference/functions/date-time-functions.md#tostartofweek), [toLastDayOfWeek](../../sql-reference/functions/date-time-functions.md#tolastdayofweek) and [toMonday](../../sql-reference/functions/date-time-functions.md#tomonday).
- `DateTime64` with extended range (compared to type `DateTime`) for functions [toStartOfDay](../../sql-reference/functions/date-time-functions.md#tostartofday), [toStartOfHour](../../sql-reference/functions/date-time-functions.md#tostartofhour), [toStartOfMinute](../../sql-reference/functions/date-time-functions.md#tostartofminute), [toStartOfFiveMinutes](../../sql-reference/functions/date-time-functions.md#tostartoffiveminutes), [toStartOfTenMinutes](../../sql-reference/functions/date-time-functions.md#tostartoftenminutes), [toStartOfFifteenMinutes](../../sql-reference/functions/date-time-functions.md#tostartoffifteenminutes) and [timeSlot](../../sql-reference/functions/date-time-functions.md#timeslot).

Possible values:

- 0 — Functions return `Date` or `DateTime` for all types of arguments.
- 1 — Functions return `Date32` or `DateTime64` for `Date32` or `DateTime64` arguments and `Date` or `DateTime` otherwise.

Default value: `0`.


## function_locate_has_mysql_compatible_argument_order {#function-locate-has-mysql-compatible-argument-order}

Controls the order of arguments in function [locate](../../sql-reference/functions/string-search-functions.md#locate).

Possible values:

- 0 — Function `locate` accepts arguments `(haystack, needle[, start_pos])`.
- 1 — Function `locate` accepts arguments `(needle, haystack, [, start_pos])` (MySQL-compatible behavior)

Default value: `1`.

## date_time_overflow_behavior {#date_time_overflow_behavior}

Defines the behavior when [Date](../../sql-reference/data-types/date.md), [Date32](../../sql-reference/data-types/date32.md), [DateTime](../../sql-reference/data-types/datetime.md), [DateTime64](../../sql-reference/data-types/datetime64.md) or integers are converted into Date, Date32, DateTime or DateTime64 but the value cannot be represented in the result type.

Possible values:

- `ignore` — Silently ignore overflows. The result is random.
- `throw` — Throw an exception in case of conversion overflow.
- `saturate` — Silently saturate the result. If the value is smaller than the smallest value that can be represented by the target type, the result is chosen as the smallest representable value. If the value is bigger than the largest value that can be represented by the target type, the result is chosen as the largest representable value.

Default value: `ignore`.

## optimize_move_to_prewhere {#optimize_move_to_prewhere}

Enables or disables automatic [PREWHERE](../../sql-reference/statements/select/prewhere.md) optimization in [SELECT](../../sql-reference/statements/select/index.md) queries.

Works only for [*MergeTree](../../engines/table-engines/mergetree-family/index.md) tables.

Possible values:

- 0 — Automatic `PREWHERE` optimization is disabled.
- 1 — Automatic `PREWHERE` optimization is enabled.

Default value: `1`.

## optimize_move_to_prewhere_if_final {#optimize_move_to_prewhere_if_final}

Enables or disables automatic [PREWHERE](../../sql-reference/statements/select/prewhere.md) optimization in [SELECT](../../sql-reference/statements/select/index.md) queries with [FINAL](../../sql-reference/statements/select/from.md#select-from-final) modifier.

Works only for [*MergeTree](../../engines/table-engines/mergetree-family/index.md) tables.

Possible values:

- 0 — Automatic `PREWHERE` optimization in `SELECT` queries with `FINAL` modifier is disabled.
- 1 — Automatic `PREWHERE` optimization in `SELECT` queries with `FINAL` modifier is enabled.

Default value: `0`.

**See Also**

- [optimize_move_to_prewhere](#optimize_move_to_prewhere) setting

## optimize_using_constraints

Use [constraints](../../sql-reference/statements/create/table.md#constraints) for query optimization. The default is `false`.

Possible values:

- true, false

## optimize_append_index

Use [constraints](../../sql-reference/statements/create/table.md#constraints) in order to append index condition. The default is `false`.

Possible values:

- true, false

## optimize_substitute_columns

Use [constraints](../../sql-reference/statements/create/table.md#constraints) for column substitution. The default is `false`.

Possible values:

- true, false

## describe_include_subcolumns {#describe_include_subcolumns}

Enables describing subcolumns for a [DESCRIBE](../../sql-reference/statements/describe-table.md) query. For example, members of a [Tuple](../../sql-reference/data-types/tuple.md) or subcolumns of a [Map](../../sql-reference/data-types/map.md/#map-subcolumns), [Nullable](../../sql-reference/data-types/nullable.md/#finding-null) or an [Array](../../sql-reference/data-types/array.md/#array-size) data type.

Possible values:

- 0 — Subcolumns are not included in `DESCRIBE` queries.
- 1 — Subcolumns are included in `DESCRIBE` queries.

Default value: `0`.

**Example**

See an example for the [DESCRIBE](../../sql-reference/statements/describe-table.md) statement.


## alter_partition_verbose_result {#alter-partition-verbose-result}

Enables or disables the display of information about the parts to which the manipulation operations with partitions and parts have been successfully applied.
Applicable to [ATTACH PARTITION|PART](../../sql-reference/statements/alter/partition.md/#alter_attach-partition) and to [FREEZE PARTITION](../../sql-reference/statements/alter/partition.md/#alter_freeze-partition).

Possible values:

- 0 — disable verbosity.
- 1 — enable verbosity.

Default value: `0`.

**Example**

```sql
CREATE TABLE test(a Int64, d Date, s String) ENGINE = MergeTree PARTITION BY toYYYYMM(d) ORDER BY a;
INSERT INTO test VALUES(1, '2021-01-01', '');
INSERT INTO test VALUES(1, '2021-01-01', '');
ALTER TABLE test DETACH PARTITION ID '202101';

ALTER TABLE test ATTACH PARTITION ID '202101' SETTINGS alter_partition_verbose_result = 1;

┌─command_type─────┬─partition_id─┬─part_name────┬─old_part_name─┐
│ ATTACH PARTITION │ 202101       │ 202101_7_7_0 │ 202101_5_5_0  │
│ ATTACH PARTITION │ 202101       │ 202101_8_8_0 │ 202101_6_6_0  │
└──────────────────┴──────────────┴──────────────┴───────────────┘

ALTER TABLE test FREEZE SETTINGS alter_partition_verbose_result = 1;

┌─command_type─┬─partition_id─┬─part_name────┬─backup_name─┬─backup_path───────────────────┬─part_backup_path────────────────────────────────────────────┐
│ FREEZE ALL   │ 202101       │ 202101_7_7_0 │ 8           │ /var/lib/clickhouse/shadow/8/ │ /var/lib/clickhouse/shadow/8/data/default/test/202101_7_7_0 │
│ FREEZE ALL   │ 202101       │ 202101_8_8_0 │ 8           │ /var/lib/clickhouse/shadow/8/ │ /var/lib/clickhouse/shadow/8/data/default/test/202101_8_8_0 │
└──────────────┴──────────────┴──────────────┴─────────────┴───────────────────────────────┴─────────────────────────────────────────────────────────────┘
```

## min_bytes_to_use_mmap_io {#min-bytes-to-use-mmap-io}

This is an experimental setting. Sets the minimum amount of memory for reading large files without copying data from the kernel to userspace. Recommended threshold is about 64 MB, because [mmap/munmap](https://en.wikipedia.org/wiki/Mmap) is slow. It makes sense only for large files and helps only if data reside in the page cache.

Possible values:

- Positive integer.
- 0 — Big files read with only copying data from kernel to userspace.

Default value: `0`.

## shutdown_wait_unfinished_queries {#shutdown_wait_unfinished_queries}

Enables or disables waiting unfinished queries when shutdown server.

Possible values:

- 0 — Disabled.
- 1 — Enabled. The wait time equal shutdown_wait_unfinished config.

Default value: 0.

## shutdown_wait_unfinished {#shutdown_wait_unfinished}

The waiting time in seconds for currently handled connections when shutdown server.

Default Value: 5.

## memory_overcommit_ratio_denominator {#memory_overcommit_ratio_denominator}

It represents soft memory limit in case when hard limit is reached on user level.
This value is used to compute overcommit ratio for the query.
Zero means skip the query.
Read more about [memory overcommit](memory-overcommit.md).

Default value: `1GiB`.

## memory_usage_overcommit_max_wait_microseconds {#memory_usage_overcommit_max_wait_microseconds}

Maximum time thread will wait for memory to be freed in the case of memory overcommit on a user level.
If the timeout is reached and memory is not freed, an exception is thrown.
Read more about [memory overcommit](memory-overcommit.md).

Default value: `5000000`.

## memory_overcommit_ratio_denominator_for_user {#memory_overcommit_ratio_denominator_for_user}

It represents soft memory limit in case when hard limit is reached on global level.
This value is used to compute overcommit ratio for the query.
Zero means skip the query.
Read more about [memory overcommit](memory-overcommit.md).

Default value: `1GiB`.

## Schema Inference settings

See [schema inference](../../interfaces/schema-inference.md#schema-inference-modes) documentation for more details.

### schema_inference_use_cache_for_file {schema_inference_use_cache_for_file}

Enable schemas cache for schema inference in `file` table function.

Default value: `true`.

### schema_inference_use_cache_for_s3 {schema_inference_use_cache_for_s3}

Enable schemas cache for schema inference in `s3` table function.

Default value: `true`.

### schema_inference_use_cache_for_url {schema_inference_use_cache_for_url}

Enable schemas cache for schema inference in `url` table function.

Default value: `true`.

### schema_inference_use_cache_for_hdfs {schema_inference_use_cache_for_hdfs}

Enable schemas cache for schema inference in `hdfs` table function.

Default value: `true`.

### schema_inference_cache_require_modification_time_for_url {#schema_inference_cache_require_modification_time_for_url}

Use schema from cache for URL with last modification time validation (for urls with Last-Modified header). If this setting is enabled and URL doesn't have Last-Modified header, schema from cache won't be used.

Default value: `true`.

### use_structure_from_insertion_table_in_table_functions {use_structure_from_insertion_table_in_table_functions}

Use structure from insertion table instead of schema inference from data.

Possible values:
- 0 - disabled
- 1 - enabled
- 2 - auto

Default value: 2.

### schema_inference_mode {schema_inference_mode}

The mode of schema inference. Possible values: `default` and `union`.
See [schema inference modes](../../interfaces/schema-inference.md#schema-inference-modes) section for more details.

Default value: `default`.

## compatibility {#compatibility}

The `compatibility` setting causes ClickHouse to use the default settings of a previous version of ClickHouse, where the previous version is provided as the setting.

If settings are set to non-default values, then those settings are honored (only settings that have not been modified are affected by the `compatibility` setting).

This setting takes a ClickHouse version number as a string, like `22.3`, `22.8`. An empty value means that this setting is disabled.

Disabled by default.

:::note
In ClickHouse Cloud the compatibility setting must be set by ClickHouse Cloud support.  Please [open a case](https://clickhouse.cloud/support) to have it set.
:::

## allow_settings_after_format_in_insert {#allow_settings_after_format_in_insert}

Control whether `SETTINGS` after `FORMAT` in `INSERT` queries is allowed or not. It is not recommended to use this, since this may interpret part of `SETTINGS` as values.

Example:

```sql
INSERT INTO FUNCTION null('foo String') SETTINGS max_threads=1 VALUES ('bar');
```

But the following query will work only with `allow_settings_after_format_in_insert`:

```sql
SET allow_settings_after_format_in_insert=1;
INSERT INTO FUNCTION null('foo String') VALUES ('bar') SETTINGS max_threads=1;
```

Possible values:

- 0 — Disallow.
- 1 — Allow.

Default value: `0`.

:::note
Use this setting only for backward compatibility if your use cases depend on old syntax.
:::

## session_timezone {#session_timezone}

Sets the implicit time zone of the current session or query.
The implicit time zone is the time zone applied to values of type DateTime/DateTime64 which have no explicitly specified time zone.
The setting takes precedence over the globally configured (server-level) implicit time zone.
A value of '' (empty string) means that the implicit time zone of the current session or query is equal to the [server time zone](../server-configuration-parameters/settings.md#server_configuration_parameters-timezone).

You can use functions `timeZone()` and `serverTimeZone()` to get the session time zone and server time zone.

Possible values:

-    Any time zone name from `system.time_zones`, e.g. `Europe/Berlin`, `UTC` or `Zulu`

Default value: `''`.

Examples:

```sql
SELECT timeZone(), serverTimeZone() FORMAT TSV

Europe/Berlin	Europe/Berlin
```

```sql
SELECT timeZone(), serverTimeZone() SETTINGS session_timezone = 'Asia/Novosibirsk' FORMAT TSV

Asia/Novosibirsk	Europe/Berlin
```

Assign session time zone 'America/Denver' to the inner DateTime without explicitly specified time zone:

```sql
SELECT toDateTime64(toDateTime64('1999-12-12 23:23:23.123', 3), 3, 'Europe/Zurich') SETTINGS session_timezone = 'America/Denver' FORMAT TSV

1999-12-13 07:23:23.123
```

:::warning
Not all functions that parse DateTime/DateTime64 respect `session_timezone`. This can lead to subtle errors.
See the following example and explanation.
:::

```sql
CREATE TABLE test_tz (`d` DateTime('UTC')) ENGINE = Memory AS SELECT toDateTime('2000-01-01 00:00:00', 'UTC');

SELECT *, timeZone() FROM test_tz WHERE d = toDateTime('2000-01-01 00:00:00') SETTINGS session_timezone = 'Asia/Novosibirsk'
0 rows in set.

SELECT *, timeZone() FROM test_tz WHERE d = '2000-01-01 00:00:00' SETTINGS session_timezone = 'Asia/Novosibirsk'
┌───────────────────d─┬─timeZone()───────┐
│ 2000-01-01 00:00:00 │ Asia/Novosibirsk │
└─────────────────────┴──────────────────┘
```

This happens due to different parsing pipelines:

- `toDateTime()` without explicitly given time zone used in the first `SELECT` query honors setting `session_timezone` and the global time zone.
- In the second query, a DateTime is parsed from a String, and inherits the type and time zone of the existing column`d`. Thus, setting `session_timezone` and the global time zone are not honored.

**See also**

- [timezone](../server-configuration-parameters/settings.md#server_configuration_parameters-timezone)

## final {#final}

Automatically applies [FINAL](../../sql-reference/statements/select/from.md#final-modifier) modifier to all tables in a query, to tables where [FINAL](../../sql-reference/statements/select/from.md#final-modifier) is applicable, including joined tables and tables in sub-queries, and
distributed tables.

Possible values:

- 0 - disabled
- 1 - enabled

Default value: `0`.

Example:

```sql
CREATE TABLE test
(
    key Int64,
    some String
)
ENGINE = ReplacingMergeTree
ORDER BY key;

INSERT INTO test FORMAT Values (1, 'first');
INSERT INTO test FORMAT Values (1, 'second');

SELECT * FROM test;
┌─key─┬─some───┐
│   1 │ second │
└─────┴────────┘
┌─key─┬─some──┐
│   1 │ first │
└─────┴───────┘

SELECT * FROM test SETTINGS final = 1;
┌─key─┬─some───┐
│   1 │ second │
└─────┴────────┘

SET final = 1;
SELECT * FROM test;
┌─key─┬─some───┐
│   1 │ second │
└─────┴────────┘
```

## asterisk_include_materialized_columns {#asterisk_include_materialized_columns}

Include [MATERIALIZED](../../sql-reference/statements/create/table.md#materialized) columns for wildcard query (`SELECT *`).

Possible values:

- 0 - disabled
- 1 - enabled

Default value: `0`.

## asterisk_include_alias_columns {#asterisk_include_alias_columns}

Include [ALIAS](../../sql-reference/statements/create/table.md#alias) columns for wildcard query (`SELECT *`).

Possible values:

- 0 - disabled
- 1 - enabled

Default value: `0`.

## async_socket_for_remote {#async_socket_for_remote}

Enables asynchronous read from socket while executing remote query.

Enabled by default.

## async_query_sending_for_remote {#async_query_sending_for_remote}

Enables asynchronous connection creation and query sending while executing remote query.

Enabled by default.

## use_hedged_requests {#use_hedged_requests}

Enables hedged requests logic for remote queries. It allows to establish many connections with different replicas for query.
New connection is enabled in case existent connection(s) with replica(s) were not established within `hedged_connection_timeout`
or no data was received within `receive_data_timeout`. Query uses the first connection which send non empty progress packet (or data packet, if `allow_changing_replica_until_first_data_packet`);
other connections are cancelled. Queries with `max_parallel_replicas > 1` are supported.

Enabled by default.

Disabled by default on Cloud.

## hedged_connection_timeout {#hedged_connection_timeout}

If we can't establish connection with replica after this timeout in hedged requests, we start working with the next replica without cancelling connection to the previous.
Timeout value is in milliseconds.

Default value: `50`.

## receive_data_timeout {#receive_data_timeout}

This timeout is set when the query is sent to the replica in hedged requests, if we don't receive first packet of data and we don't make any progress in query execution after this timeout,
we start working with the next replica, without cancelling connection to the previous.
Timeout value is in milliseconds.

Default value: `2000`

## allow_changing_replica_until_first_data_packet {#allow_changing_replica_until_first_data_packet}

If it's enabled, in hedged requests we can start new connection until receiving first data packet even if we have already made some progress
(but progress haven't updated for `receive_data_timeout` timeout), otherwise we disable changing replica after the first time we made progress.

## parallel_view_processing

Enables pushing to attached views concurrently instead of sequentially.

Default value: `false`.

## partial_result_on_first_cancel {#partial_result_on_first_cancel}
When set to `true` and the user wants to interrupt a query (for example using `Ctrl+C` on the client), then the query continues execution only on data that was already read from the table. Afterwards, it will return a partial result of the query for the part of the table that was read. To fully stop the execution of a query without a partial result, the user should send 2 cancel requests.

**Example without setting on Ctrl+C**
```sql
SELECT sum(number) FROM numbers(10000000000)

Cancelling query.
Ok.
Query was cancelled.

0 rows in set. Elapsed: 1.334 sec. Processed 52.65 million rows, 421.23 MB (39.48 million rows/s., 315.85 MB/s.)
```

**Example with setting on Ctrl+C**
```sql
SELECT sum(number) FROM numbers(10000000000) SETTINGS partial_result_on_first_cancel=true

┌──────sum(number)─┐
│ 1355411451286266 │
└──────────────────┘

1 row in set. Elapsed: 1.331 sec. Processed 52.13 million rows, 417.05 MB (39.17 million rows/s., 313.33 MB/s.)
```

Possible values: `true`, `false`

Default value: `false`
## function_json_value_return_type_allow_nullable

Control whether allow to return `NULL` when value is not exist for JSON_VALUE function.

```sql
SELECT JSON_VALUE('{"hello":"world"}', '$.b') settings function_json_value_return_type_allow_nullable=true;

┌─JSON_VALUE('{"hello":"world"}', '$.b')─┐
│ ᴺᵁᴸᴸ                                   │
└────────────────────────────────────────┘

1 row in set. Elapsed: 0.001 sec.
```

Possible values:

- true — Allow.
- false — Disallow.

Default value: `false`.

## rename_files_after_processing {#rename_files_after_processing}

- **Type:** String

- **Default value:** Empty string

This setting allows to specify renaming pattern for files processed by `file` table function. When option is set, all files read by `file` table function will be renamed according to specified pattern with placeholders, only if files processing was successful.

### Placeholders

- `%a` — Full original filename (e.g., "sample.csv").
- `%f` — Original filename without extension (e.g., "sample").
- `%e` — Original file extension with dot (e.g., ".csv").
- `%t` — Timestamp (in microseconds).
- `%%` — Percentage sign ("%").

### Example
- Option: `--rename_files_after_processing="processed_%f_%t%e"`

- Query: `SELECT * FROM file('sample.csv')`


If reading `sample.csv` is successful, file will be renamed to `processed_sample_1683473210851438.csv`




## function_json_value_return_type_allow_complex

Control whether allow to return complex type (such as: struct, array, map) for json_value function.

```sql
SELECT JSON_VALUE('{"hello":{"world":"!"}}', '$.hello') settings function_json_value_return_type_allow_complex=true

┌─JSON_VALUE('{"hello":{"world":"!"}}', '$.hello')─┐
│ {"world":"!"}                                    │
└──────────────────────────────────────────────────┘

1 row in set. Elapsed: 0.001 sec.
```

Possible values:

- true — Allow.
- false — Disallow.

Default value: `false`.

## zstd_window_log_max

Allows you to select the max window log of ZSTD (it will not be used for MergeTree family)

Type: Int64

Default: 0

## enable_deflate_qpl_codec {#enable_deflate_qpl_codec}

If turned on, the DEFLATE_QPL codec may be used to compress columns.

Possible values:

- 0 - Disabled
- 1 - Enabled

Type: Bool

## enable_zstd_qat_codec {#enable_zstd_qat_codec}

If turned on, the ZSTD_QAT codec may be used to compress columns.

Possible values:

- 0 - Disabled
- 1 - Enabled

Type: Bool

## output_format_compression_level

Default compression level if query output is compressed. The setting is applied when `SELECT` query has `INTO OUTFILE` or when writing to table functions `file`, `url`, `hdfs`, `s3`, or `azureBlobStorage`.

Possible values: from `1` to `22`

Default: `3`


## output_format_compression_zstd_window_log

Can be used when the output compression method is `zstd`. If greater than `0`, this setting explicitly sets compression window size (power of `2`) and enables a long-range mode for zstd compression. This can help to achieve a better compression ratio.

Possible values: non-negative numbers. Note that if the value is too small or too big, `zstdlib` will throw an exception. Typical values are from `20` (window size = `1MB`) to `30` (window size = `1GB`).

Default: `0`

## rewrite_count_distinct_if_with_count_distinct_implementation

Allows you to rewrite `countDistcintIf` with [count_distinct_implementation](#count_distinct_implementation) setting.

Possible values:

- true — Allow.
- false — Disallow.

Default value: `false`.

## precise_float_parsing {#precise_float_parsing}

Switches [Float32/Float64](../../sql-reference/data-types/float.md) parsing algorithms:
* If the value is `1`, then precise method is used. It is slower than fast method, but it always returns a number that is the closest machine representable number to the input.
* Otherwise, fast method is used (default). It usually returns the same value as precise, but in rare cases result may differ by one or two least significant digits.

Possible values: `0`, `1`.

Default value: `0`.

Example:

```sql
SELECT toFloat64('1.7091'), toFloat64('1.5008753E7') SETTINGS precise_float_parsing = 0;

┌─toFloat64('1.7091')─┬─toFloat64('1.5008753E7')─┐
│  1.7090999999999998 │       15008753.000000002 │
└─────────────────────┴──────────────────────────┘

SELECT toFloat64('1.7091'), toFloat64('1.5008753E7') SETTINGS precise_float_parsing = 1;

┌─toFloat64('1.7091')─┬─toFloat64('1.5008753E7')─┐
│              1.7091 │                 15008753 │
└─────────────────────┴──────────────────────────┘
```

## validate_tcp_client_information {#validate-tcp-client-information}

Determines whether validation of client information enabled when query packet is received from a client using a TCP connection.

If `true`, an exception will be thrown on invalid client information from the TCP client.

If `false`, the data will not be validated. The server will work with clients of all versions.

The default value is `false`.

**Example**

``` xml
<validate_tcp_client_information>true</validate_tcp_client_information>
```

## print_pretty_type_names {#print_pretty_type_names}

Allows to print deep-nested type names in a pretty way with indents in `DESCRIBE` query and in `toTypeName()` function.

Example:

```sql
CREATE TABLE test (a Tuple(b String, c Tuple(d Nullable(UInt64), e Array(UInt32), f Array(Tuple(g String, h Map(String, Array(Tuple(i String, j UInt64))))), k Date), l Nullable(String))) ENGINE=Memory;
DESCRIBE TABLE test FORMAT TSVRaw SETTINGS print_pretty_type_names=1;
```

```
a	Tuple(
    b String,
    c Tuple(
        d Nullable(UInt64),
        e Array(UInt32),
        f Array(Tuple(
            g String,
            h Map(
                String,
                Array(Tuple(
                    i String,
                    j UInt64
                ))
            )
        )),
        k Date
    ),
    l Nullable(String)
)
```

## allow_experimental_statistics {#allow_experimental_statistics}

Allows defining columns with [statistics](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table) and [manipulate statistics](../../engines/table-engines/mergetree-family/mergetree.md#column-statistics).

## allow_statistic_optimize {#allow_statistic_optimize}

Allows using statistic to optimize the order of [prewhere conditions](../../sql-reference/statements/select/prewhere.md).

## analyze_index_with_space_filling_curves

If a table has a space-filling curve in its index, e.g. `ORDER BY mortonEncode(x, y)` or `ORDER BY hilbertEncode(x, y)`, and the query has conditions on its arguments, e.g. `x >= 10 AND x <= 20 AND y >= 20 AND y <= 30`, use the space-filling curve for index analysis.

## query_plan_enable_optimizations {#query_plan_enable_optimizations}

Toggles query optimization at the query plan level.

:::note
This is an expert-level setting which should only be used for debugging by developers. The setting may change in future in backward-incompatible ways or be removed.
:::

Possible values:

- 0 - Disable all optimizations at the query plan level
- 1 - Enable optimizations at the query plan level (but individual optimizations may still be disabled via their individual settings)

Default value: `1`.

## query_plan_max_optimizations_to_apply

Limits the total number of optimizations applied to query plan, see setting [query_plan_enable_optimizations](#query_plan_enable_optimizations).
Useful to avoid long optimization times for complex queries.
If the actual number of optimizations exceeds this setting, an exception is thrown.

:::note
This is an expert-level setting which should only be used for debugging by developers. The setting may change in future in backward-incompatible ways or be removed.
:::

Type: [UInt64](../../sql-reference/data-types/int-uint.md).

Default value: '10000'

## query_plan_lift_up_array_join

Toggles a query-plan-level optimization which moves ARRAY JOINs up in the execution plan.
Only takes effect if setting [query_plan_enable_optimizations](#query_plan_enable_optimizations) is 1.

:::note
This is an expert-level setting which should only be used for debugging by developers. The setting may change in future in backward-incompatible ways or be removed.
:::

Possible values:

- 0 - Disable
- 1 - Enable

Default value: `1`.

## query_plan_push_down_limit

Toggles a query-plan-level optimization which moves LIMITs down in the execution plan.
Only takes effect if setting [query_plan_enable_optimizations](#query_plan_enable_optimizations) is 1.

:::note
This is an expert-level setting which should only be used for debugging by developers. The setting may change in future in backward-incompatible ways or be removed.
:::

Possible values:

- 0 - Disable
- 1 - Enable

Default value: `1`.

## query_plan_split_filter

:::note
This is an expert-level setting which should only be used for debugging by developers. The setting may change in future in backward-incompatible ways or be removed.
:::

Toggles a query-plan-level optimization which splits filters into expressions.
Only takes effect if setting [query_plan_enable_optimizations](#query_plan_enable_optimizations) is 1.

Possible values:

- 0 - Disable
- 1 - Enable

Default value: `1`.

## query_plan_merge_expressions

Toggles a query-plan-level optimization which merges consecutive filters.
Only takes effect if setting [query_plan_enable_optimizations](#query_plan_enable_optimizations) is 1.

:::note
This is an expert-level setting which should only be used for debugging by developers. The setting may change in future in backward-incompatible ways or be removed.
:::

Possible values:

- 0 - Disable
- 1 - Enable

Default value: `1`.

## query_plan_filter_push_down

Toggles a query-plan-level optimization which moves filters down in the execution plan.
Only takes effect if setting [query_plan_enable_optimizations](#query_plan_enable_optimizations) is 1.

:::note
This is an expert-level setting which should only be used for debugging by developers. The setting may change in future in backward-incompatible ways or be removed.
:::

Possible values:

- 0 - Disable
- 1 - Enable

Default value: `1`.

## query_plan_execute_functions_after_sorting

Toggles a query-plan-level optimization which moves expressions after sorting steps.
Only takes effect if setting [query_plan_enable_optimizations](#query_plan_enable_optimizations) is 1.

:::note
This is an expert-level setting which should only be used for debugging by developers. The setting may change in future in backward-incompatible ways or be removed.
:::

Possible values:

- 0 - Disable
- 1 - Enable

Default value: `1`.

## query_plan_reuse_storage_ordering_for_window_functions

Toggles a query-plan-level optimization which uses storage sorting when sorting for window functions.
Only takes effect if setting [query_plan_enable_optimizations](#query_plan_enable_optimizations) is 1.

:::note
This is an expert-level setting which should only be used for debugging by developers. The setting may change in future in backward-incompatible ways or be removed.
:::

Possible values:

- 0 - Disable
- 1 - Enable

Default value: `1`.

## query_plan_lift_up_union

Toggles a query-plan-level optimization which moves larger subtrees of the query plan into union to enable further optimizations.
Only takes effect if setting [query_plan_enable_optimizations](#query_plan_enable_optimizations) is 1.

:::note
This is an expert-level setting which should only be used for debugging by developers. The setting may change in future in backward-incompatible ways or be removed.
:::

Possible values:

- 0 - Disable
- 1 - Enable

Default value: `1`.

## query_plan_distinct_in_order

Toggles the distinct in-order optimization query-plan-level optimization.
Only takes effect if setting [query_plan_enable_optimizations](#query_plan_enable_optimizations) is 1.

:::note
This is an expert-level setting which should only be used for debugging by developers. The setting may change in future in backward-incompatible ways or be removed.
:::

Possible values:

- 0 - Disable
- 1 - Enable

Default value: `1`.

## query_plan_read_in_order

Toggles the read in-order optimization query-plan-level optimization.
Only takes effect if setting [query_plan_enable_optimizations](#query_plan_enable_optimizations) is 1.

:::note
This is an expert-level setting which should only be used for debugging by developers. The setting may change in future in backward-incompatible ways or be removed.
:::

Possible values:

- 0 - Disable
- 1 - Enable

Default value: `1`.

## query_plan_aggregation_in_order

Toggles the aggregation in-order query-plan-level optimization.
Only takes effect if setting [query_plan_enable_optimizations](#query_plan_enable_optimizations) is 1.

:::note
This is an expert-level setting which should only be used for debugging by developers. The setting may change in future in backward-incompatible ways or be removed.
:::

Possible values:

- 0 - Disable
- 1 - Enable

Default value: `0`.

## query_plan_remove_redundant_sorting

Toggles a query-plan-level optimization which removes redundant sorting steps, e.g. in subqueries.
Only takes effect if setting [query_plan_enable_optimizations](#query_plan_enable_optimizations) is 1.

:::note
This is an expert-level setting which should only be used for debugging by developers. The setting may change in future in backward-incompatible ways or be removed.
:::

Possible values:

- 0 - Disable
- 1 - Enable

Default value: `1`.

## query_plan_remove_redundant_distinct

Toggles a query-plan-level optimization which removes redundant DISTINCT steps.
Only takes effect if setting [query_plan_enable_optimizations](#query_plan_enable_optimizations) is 1.

:::note
This is an expert-level setting which should only be used for debugging by developers. The setting may change in future in backward-incompatible ways or be removed.
:::

Possible values:

- 0 - Disable
- 1 - Enable

Default value: `1`.

## dictionary_use_async_executor {#dictionary_use_async_executor}

Execute a pipeline for reading dictionary source in several threads. It's supported only by dictionaries with local CLICKHOUSE source.

You may specify it in `SETTINGS` section of dictionary definition:

```sql
CREATE DICTIONARY t1_dict ( key String, attr UInt64 )
PRIMARY KEY key
SOURCE(CLICKHOUSE(QUERY `SELECT key, attr FROM t1 GROUP BY key`))
LIFETIME(MIN 0 MAX 3600)
LAYOUT(COMPLEX_KEY_HASHED_ARRAY())
SETTINGS(dictionary_use_async_executor=1, max_threads=8);
```

## storage_metadata_write_full_object_key {#storage_metadata_write_full_object_key}

When set to `true` the metadata files are written with `VERSION_FULL_OBJECT_KEY` format version. With that format full object storage key names are written to the metadata files.
When set to `false` the metadata files are written with the previous format version, `VERSION_INLINE_DATA`. With that format only suffixes of object storage key names are written to the metadata files. The prefix for all of object storage key names is set in configurations files at `storage_configuration.disks` section.

Default value: `false`.

## s3_use_adaptive_timeouts {#s3_use_adaptive_timeouts}

When set to `true` than for all s3 requests first two attempts are made with low send and receive timeouts.
When set to `false` than all attempts are made with identical timeouts.

Default value: `true`.

## allow_deprecated_snowflake_conversion_functions {#allow_deprecated_snowflake_conversion_functions}

Functions `snowflakeToDateTime`, `snowflakeToDateTime64`, `dateTimeToSnowflake`, and `dateTime64ToSnowflake` are deprecated and disabled by default.
Please use functions `snowflakeIDToDateTime`, `snowflakeIDToDateTime64`, `dateTimeToSnowflakeID`, and `dateTime64ToSnowflakeID` instead.

To re-enable the deprecated functions (e.g., during a transition period), please set this setting to `true`.

Default value: `false`

## allow_experimental_variant_type {#allow_experimental_variant_type}

Allows creation of experimental [Variant](../../sql-reference/data-types/variant.md).

Default value: `false`.

## use_variant_as_common_type {#use_variant_as_common_type}

Allows to use `Variant` type as a result type for [if](../../sql-reference/functions/conditional-functions.md/#if)/[multiIf](../../sql-reference/functions/conditional-functions.md/#multiif)/[array](../../sql-reference/functions/array-functions.md)/[map](../../sql-reference/functions/tuple-map-functions.md) functions when there is no common type for argument types.

Example:

```sql
SET use_variant_as_common_type = 1;
SELECT toTypeName(if(number % 2, number, range(number))) as variant_type FROM numbers(1);
SELECT if(number % 2, number, range(number)) as variant FROM numbers(5);
```

```text
┌─variant_type───────────────────┐
│ Variant(Array(UInt64), UInt64) │
└────────────────────────────────┘
┌─variant───┐
│ []        │
│ 1         │
│ [0,1]     │
│ 3         │
│ [0,1,2,3] │
└───────────┘
```

```sql
SET use_variant_as_common_type = 1;
SELECT toTypeName(multiIf((number % 4) = 0, 42, (number % 4) = 1, [1, 2, 3], (number % 4) = 2, 'Hello, World!', NULL)) AS variant_type FROM numbers(1);
SELECT multiIf((number % 4) = 0, 42, (number % 4) = 1, [1, 2, 3], (number % 4) = 2, 'Hello, World!', NULL) AS variant FROM numbers(4);
```

```text
─variant_type─────────────────────────┐
│ Variant(Array(UInt8), String, UInt8) │
└──────────────────────────────────────┘

┌─variant───────┐
│ 42            │
│ [1,2,3]       │
│ Hello, World! │
│ ᴺᵁᴸᴸ          │
└───────────────┘
```

```sql
SET use_variant_as_common_type = 1;
SELECT toTypeName(array(range(number), number, 'str_' || toString(number))) as array_of_variants_type from numbers(1);
SELECT array(range(number), number, 'str_' || toString(number)) as array_of_variants FROM numbers(3);
```

```text
┌─array_of_variants_type────────────────────────┐
│ Array(Variant(Array(UInt64), String, UInt64)) │
└───────────────────────────────────────────────┘

┌─array_of_variants─┐
│ [[],0,'str_0']    │
│ [[0],1,'str_1']   │
│ [[0,1],2,'str_2'] │
└───────────────────┘
```

```sql
SET use_variant_as_common_type = 1;
SELECT toTypeName(map('a', range(number), 'b', number, 'c', 'str_' || toString(number))) as map_of_variants_type from numbers(1);
SELECT map('a', range(number), 'b', number, 'c', 'str_' || toString(number)) as map_of_variants FROM numbers(3);
```

```text
┌─map_of_variants_type────────────────────────────────┐
│ Map(String, Variant(Array(UInt64), String, UInt64)) │
└─────────────────────────────────────────────────────┘

┌─map_of_variants───────────────┐
│ {'a':[],'b':0,'c':'str_0'}    │
│ {'a':[0],'b':1,'c':'str_1'}   │
│ {'a':[0,1],'b':2,'c':'str_2'} │
└───────────────────────────────┘
```


Default value: `false`.

## default_normal_view_sql_security {#default_normal_view_sql_security}

Allows to set default `SQL SECURITY` option while creating a normal view. [More about SQL security](../../sql-reference/statements/create/view.md#sql_security).

The default value is `INVOKER`.

## default_materialized_view_sql_security {#default_materialized_view_sql_security}

Allows to set a default value for SQL SECURITY option when creating a materialized view. [More about SQL security](../../sql-reference/statements/create/view.md#sql_security).

The default value is `DEFINER`.

## default_view_definer {#default_view_definer}

Allows to set default `DEFINER` option while creating a view. [More about SQL security](../../sql-reference/statements/create/view.md#sql_security).

The default value is `CURRENT_USER`.

## max_partition_size_to_drop

Restriction on dropping partitions in query time. The value 0 means that you can drop partitions without any restrictions.

Default value: 50 GB.

Cloud default value: 1 TB.

:::note
This query setting overwrites its server setting equivalent, see [max_partition_size_to_drop](/docs/en/operations/server-configuration-parameters/settings.md/#max-partition-size-to-drop)
:::

## max_table_size_to_drop

Restriction on deleting tables in query time. The value 0 means that you can delete all tables without any restrictions.

Default value: 50 GB.

Cloud default value: 1 TB.

:::note
This query setting overwrites its server setting equivalent, see [max_table_size_to_drop](/docs/en/operations/server-configuration-parameters/settings.md/#max-table-size-to-drop)
:::

## iceberg_engine_ignore_schema_evolution {#iceberg_engine_ignore_schema_evolution}

Allow to ignore schema evolution in Iceberg table engine and read all data using schema specified by the user on table creation or latest schema parsed from metadata on table creation.

:::note
Enabling this setting can lead to incorrect result as in case of evolved schema all data files will be read using the same schema.
:::

Default value: 'false'.

## allow_suspicious_primary_key {#allow_suspicious_primary_key}

Allow suspicious `PRIMARY KEY`/`ORDER BY` for MergeTree (i.e. SimpleAggregateFunction).

## mysql_datatypes_support_level

Defines how MySQL types are converted to corresponding ClickHouse types. A comma separated list in any combination of `decimal`, `datetime64`, `date2Date32` or `date2String`.
- `decimal`: convert `NUMERIC` and `DECIMAL` types to `Decimal` when precision allows it.
- `datetime64`: convert `DATETIME` and `TIMESTAMP` types to `DateTime64` instead of `DateTime` when precision is not `0`.
- `date2Date32`: convert `DATE` to `Date32` instead of `Date`. Takes precedence over `date2String`.
- `date2String`: convert `DATE` to `String` instead of `Date`. Overridden by `datetime64`.

## cross_join_min_rows_to_compress

Minimal count of rows to compress block in CROSS JOIN. Zero value means - disable this threshold. This block is compressed when any of the two thresholds (by rows or by bytes) are reached.

Default value: `10000000`.

## cross_join_min_bytes_to_compress

Minimal size of block to compress in CROSS JOIN. Zero value means - disable this threshold. This block is compressed when any of the two thresholds (by rows or by bytes) are reached.

Default value: `1GiB`.
## add_http_cors_header

Write add http CORS header.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## aggregation_in_order_max_block_bytes

Maximal size of block in bytes accumulated during aggregation in order of primary key. Lower block size allows to parallelize more final merge stage of aggregation.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>50000000</code></td></tr>
</table>


## aggregation_memory_efficient_merge_threads

Number of threads to use for merge intermediate aggregation results in memory efficient mode. When bigger, then more memory is consumed. 0 means - same as &#039;max_threads&#039;.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## allow_aggregate_partitions_independently

Enable independent aggregation of partitions on separate threads when partition key suits group by key. Beneficial when number of partitions close to number of cores and partitions have roughly the same size

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## allow_asynchronous_read_from_io_pool_for_merge_tree

Use background I/O pool to read from MergeTree tables. This setting may increase performance for I/O bound queries

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## allow_create_index_without_type

Allow CREATE INDEX query without TYPE. Query will be ignored. Made for SQL compatibility tests.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## allow_custom_error_code_in_throwif

Enable custom error code in function throwIf(). If true, thrown exceptions may have unexpected error codes.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## allow_ddl

If it is set to true, then a user is allowed to executed DDL queries.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## allow_deprecated_database_ordinary

Allow to create databases with deprecated Ordinary engine

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## allow_deprecated_syntax_for_merge_tree

Allow to create *MergeTree tables with deprecated engine definition syntax

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## allow_distributed_ddl

If it is set to true, then a user is allowed to executed distributed DDL queries.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## allow_drop_detached

Allow ALTER TABLE ... DROP DETACHED PART[ITION] ... queries

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## allow_execute_multiif_columnar

Allow execute multiIf function columnar

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## allow_experimental_alter_materialized_view_structure

<DeprecatedBadge />

Obsolete setting, does nothing.

<table>
  <tr><td>Readonly</td><td><code>1</code></td></tr>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## allow_experimental_analyzer

Allow experimental analyzer

<table>
  <tr><td>Readonly</td><td><code>1</code></td></tr>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## allow_experimental_annoy_index

Allows to use Annoy index. Disabled by default because this feature is experimental

<table>
  <tr><td>Readonly</td><td><code>1</code></td></tr>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## allow_experimental_bigint_types

<DeprecatedBadge />

Obsolete setting, does nothing.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## allow_experimental_codecs

If it is set to true, allow to specify experimental compression codecs (but we don&#039;t have those yet and this option does nothing).

<table>
  <tr><td>Readonly</td><td><code>1</code></td></tr>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## allow_experimental_database_atomic

<DeprecatedBadge />

Obsolete setting, does nothing.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## allow_experimental_database_materialized_mysql

Allow to create database with Engine=MaterializedMySQL(...).

<table>
  <tr><td>Readonly</td><td><code>1</code></td></tr>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## allow_experimental_database_materialized_postgresql

Allow to create database with Engine=MaterializedPostgreSQL(...).

<table>
  <tr><td>Readonly</td><td><code>1</code></td></tr>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## allow_experimental_database_replicated

Allow to create databases with Replicated engine

<table>
  <tr><td>Readonly</td><td><code>1</code></td></tr>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## allow_experimental_funnel_functions

Enable experimental functions for funnel analysis.

<table>
  <tr><td>Readonly</td><td><code>1</code></td></tr>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## allow_experimental_geo_types

<DeprecatedBadge />

Obsolete setting, does nothing.

<table>
  <tr><td>Readonly</td><td><code>1</code></td></tr>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## allow_experimental_hash_functions

Enable experimental hash functions

<table>
  <tr><td>Readonly</td><td><code>1</code></td></tr>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## allow_experimental_inverted_index

If it is set to true, allow to use experimental inverted index.

<table>
  <tr><td>Readonly</td><td><code>1</code></td></tr>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## allow_experimental_lightweight_delete

Enable lightweight DELETE mutations for mergetree tables.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
  <tr><td>Alias for</td><td><code>enable_lightweight_delete</code></td></tr>
</table>


## allow_experimental_map_type

<DeprecatedBadge />

Obsolete setting, does nothing.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## allow_experimental_materialized_postgresql_table

Allows to use the MaterializedPostgreSQL table engine. Disabled by default, because this feature is experimental

<table>
  <tr><td>Readonly</td><td><code>1</code></td></tr>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## allow_experimental_nlp_functions

Enable experimental functions for natural language processing.

<table>
  <tr><td>Readonly</td><td><code>1</code></td></tr>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## allow_experimental_object_type

Allow Object and JSON data types

<table>
  <tr><td>Readonly</td><td><code>1</code></td></tr>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## allow_experimental_projection_optimization

Automatically choose projections to perform SELECT query

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
  <tr><td>Alias for</td><td><code>optimize_use_projections</code></td></tr>
</table>


## allow_experimental_query_cache

<DeprecatedBadge />

Obsolete setting, does nothing.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## allow_experimental_query_deduplication

Experimental data deduplication for SELECT queries based on part UUIDs

<table>
  <tr><td>Readonly</td><td><code>1</code></td></tr>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## allow_experimental_refreshable_materialized_view

Allow refreshable materialized views (CREATE MATERIALIZED VIEW &lt;name&gt; REFRESH ...).

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## allow_experimental_s3queue

<DeprecatedBadge />

Obsolete setting, does nothing.

<table>
  <tr><td>Readonly</td><td><code>1</code></td></tr>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## allow_experimental_shared_merge_tree

<DeprecatedBadge />

Obsolete setting, does nothing.

<table>
  <tr><td>Readonly</td><td><code>1</code></td></tr>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## allow_experimental_statistic

Allows using statistic

<table>
  <tr><td>Readonly</td><td><code>1</code></td></tr>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## allow_experimental_undrop_table_query

<DeprecatedBadge />

Obsolete setting, does nothing.

<table>
  <tr><td>Readonly</td><td><code>1</code></td></tr>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## allow_experimental_usearch_index

Allows to use USearch index. Disabled by default because this feature is experimental

<table>
  <tr><td>Readonly</td><td><code>1</code></td></tr>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## allow_experimental_window_functions

<DeprecatedBadge />

Obsolete setting, does nothing.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## allow_experimental_window_view

Enable WINDOW VIEW. Not mature enough.

<table>
  <tr><td>Readonly</td><td><code>1</code></td></tr>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## allow_hyperscan

Allow functions that use Hyperscan library. Disable to avoid potentially long compilation times and excessive resource usage.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## allow_named_collection_override_by_default

Allow named collections&#039; fields override by default.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## allow_non_metadata_alters

Allow to execute alters which affects not only tables metadata, but also data on disk

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## allow_nonconst_timezone_arguments

Allow non-const timezone arguments in certain time-related functions like toTimeZone(), fromUnixTimestamp*(), snowflakeToDateTime*()

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## allow_prefetched_read_pool_for_local_filesystem

Prefer prefetched threadpool if all parts are on local filesystem

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## allow_prefetched_read_pool_for_remote_filesystem

Prefer prefetched threadpool if all parts are on remote filesystem

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## allow_push_predicate_when_subquery_contains_with

Allows push predicate when subquery contains WITH clause

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## allow_simdjson

Allow using simdjson library in &#039;JSON*&#039; functions if AVX2 instructions are available. If disabled rapidjson will be used.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## allow_suspicious_codecs

If it is set to true, allow to specify meaningless compression codecs.

<table>
  <tr><td>Readonly</td><td><code>1</code></td></tr>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## allow_suspicious_fixed_string_types

In CREATE TABLE statement allows creating columns of type FixedString(n) with n &gt; 256. FixedString with length &gt;= 256 is suspicious and most likely indicates misuse

<table>
  <tr><td>Readonly</td><td><code>1</code></td></tr>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## allow_suspicious_indices

Reject primary/secondary indexes and sorting keys with identical expressions

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## allow_suspicious_ttl_expressions

Reject TTL expressions that don&#039;t depend on any of table&#039;s columns. It indicates a user error most of the time.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## allow_suspicious_variant_types

In CREATE TABLE statement allows specifying Variant type with similar variant types (for example, with different numeric or date types). Enabling this setting may introduce some ambiguity when working with values with similar types.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## allow_unrestricted_reads_from_keeper

Allow unrestricted (without condition on path) reads from system.zookeeper table, can be handy, but is not safe for zookeeper

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## alter_move_to_space_execute_async

Execute ALTER TABLE MOVE ... TO [DISK|VOLUME] asynchronously

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## analyzer_compatibility_join_using_top_level_identifier

Force to resolve identifier in JOIN USING from projection (for example, in `SELECT a + 1 AS b FROM t1 JOIN t2 USING (b)` join will be performed by `t1.a + 1 = t2.b`, rather then `t1.b = t2.b`).

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## annoy_index_search_k_nodes

SELECT queries search up to this many nodes in Annoy indexes.

<table>
  <tr><td>Type</td><td><code>Int64</code></td></tr>
  <tr><td>Default</td><td><code>-1</code></td></tr>
</table>


## apply_deleted_mask

Enables filtering out rows deleted with lightweight DELETE. If disabled, a query will be able to read those rows. This is useful for debugging and &quot;undelete&quot; scenarios

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## apply_mutations_on_fly

If true, mutations (UPDATEs and DELETEs) which are not materialized in data part will be applied on SELECTs

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## async_insert_cleanup_timeout_ms

<DeprecatedBadge />

Obsolete setting, does nothing.

<table>
  <tr><td>Type</td><td><code>Milliseconds</code></td></tr>
  <tr><td>Default</td><td><code>1000</code></td></tr>
</table>


## azure_create_new_file_on_insert

Enables or disables creating a new file on each insert in azure engine tables

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## azure_list_object_keys_size

Maximum number of files that could be returned in batch by ListObject request

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>1000</code></td></tr>
</table>


## azure_max_single_part_copy_size

The maximum size of object to copy using single part copy to Azure blob storage.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>268435456</code></td></tr>
</table>


## azure_max_single_part_upload_size

The maximum size of object to upload using singlepart upload to Azure blob storage.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>104857600</code></td></tr>
</table>


## azure_max_single_read_retries

The maximum number of retries during single Azure blob storage read.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>4</code></td></tr>
</table>


## azure_max_unexpected_write_error_retries

The maximum number of retries in case of unexpected errors during Azure blob storage write

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>4</code></td></tr>
</table>


## azure_truncate_on_insert

Enables or disables truncate before insert in azure engine tables.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## background_common_pool_size

<DeprecatedBadge />

User-level setting is deprecated, and it must be defined in the server configuration instead.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>8</code></td></tr>
</table>


## background_merges_mutations_concurrency_ratio

<DeprecatedBadge />

User-level setting is deprecated, and it must be defined in the server configuration instead.

<table>
  <tr><td>Type</td><td><code>Float</code></td></tr>
  <tr><td>Default</td><td><code>2</code></td></tr>
</table>


## background_pool_size

<DeprecatedBadge />

User-level setting is deprecated, and it must be defined in the server configuration instead.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>16</code></td></tr>
</table>


## backup_restore_batch_size_for_keeper_multi

Maximum size of batch for multi request to [Zoo]Keeper during backup or restore

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>1000</code></td></tr>
</table>


## backup_restore_batch_size_for_keeper_multiread

Maximum size of batch for multiread request to [Zoo]Keeper during backup or restore

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>10000</code></td></tr>
</table>


## backup_restore_keeper_fault_injection_probability

Approximate probability of failure for a keeper request during backup or restore. Valid value is in interval [0.0f, 1.0f]

<table>
  <tr><td>Type</td><td><code>Float</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## backup_restore_keeper_fault_injection_seed

0 - random seed, otherwise the setting value

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## backup_restore_keeper_max_retries

Max retries for keeper operations during backup or restore

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>20</code></td></tr>
</table>


## backup_restore_keeper_retry_initial_backoff_ms

Initial backoff timeout for [Zoo]Keeper operations during backup or restore

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>100</code></td></tr>
</table>


## backup_restore_keeper_retry_max_backoff_ms

Max backoff timeout for [Zoo]Keeper operations during backup or restore

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>5000</code></td></tr>
</table>


## backup_restore_keeper_value_max_size

Maximum size of data of a [Zoo]Keeper&#039;s node during backup

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>1048576</code></td></tr>
</table>


## backup_threads

<DeprecatedBadge />

Obsolete setting, does nothing.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>16</code></td></tr>
</table>


## bool_false_representation

Text to represent bool value in TSV/CSV formats.

<table>
  <tr><td>Type</td><td><code>String</code></td></tr>
  <tr><td>Default</td><td><code>false</code></td></tr>
</table>


## bool_true_representation

Text to represent bool value in TSV/CSV formats.

<table>
  <tr><td>Type</td><td><code>String</code></td></tr>
  <tr><td>Default</td><td><code>true</code></td></tr>
</table>


## cache_warmer_threads

Only available in ClickHouse Cloud. Number of background threads for speculatively downloading new data parts into file cache, when cache_populated_by_fetch is enabled. Zero to disable.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>4</code></td></tr>
</table>


## calculate_text_stack_trace

Calculate text stack trace in case of exceptions during query execution. This is the default. It requires symbol lookups that may slow down fuzzing tests when huge amount of wrong queries are executed. In normal cases you should not disable this option.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## cast_ipv4_ipv6_default_on_conversion_error

CAST operator into IPv4, CAST operator into IPV6 type, toIPv4, toIPv6 functions will return default value instead of throwing exception on conversion error.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## check_referential_table_dependencies

Check that DDL query (such as DROP TABLE or RENAME) will not break referential dependencies

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## check_table_dependencies

Check that DDL query (such as DROP TABLE or RENAME) will not break dependencies

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## checksum_on_read

Validate checksums on reading. It is enabled by default and should be always enabled in production. Please do not expect any benefits in disabling this setting. It may only be used for experiments and benchmarks. The setting only applicable for tables of MergeTree family. Checksums are always validated for other table engines and when receiving data over network.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## cloud_mode

Cloud mode

<table>
  <tr><td>Readonly</td><td><code>1</code></td></tr>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## cloud_mode_engine

The engine family allowed in Cloud. 0 - allow everything, 1 - rewrite DDLs to use *ReplicatedMergeTree, 2 - rewrite DDLs to use SharedMergeTree. UInt64 to minimize public part

<table>
  <tr><td>Readonly</td><td><code>1</code></td></tr>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## cluster_for_parallel_replicas

Cluster for a shard in which current server is located

<table>
  <tr><td>Type</td><td><code>String</code></td></tr>
</table>


## collect_hash_table_stats_during_aggregation

Enable collecting hash table statistics to optimize memory allocation

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## column_names_for_schema_inference

The list of column names to use in schema inference for formats without column names. The format: &#039;column1,column2,column3,...&#039;

<table>
  <tr><td>Type</td><td><code>String</code></td></tr>
</table>


## compatibility_ignore_auto_increment_in_create_table

Ignore AUTO_INCREMENT keyword in column declaration if true, otherwise return error. It simplifies migration from MySQL

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## compatibility_ignore_collation_in_create_table

Compatibility ignore collation in create table

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## compile_sort_description

Compile sort description to native code.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## count_distinct_optimization

Rewrite count distinct to subquery of group by

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## create_index_ignore_unique

Ignore UNIQUE keyword in CREATE UNIQUE INDEX. Made for SQL compatibility tests.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## create_replicated_merge_tree_fault_injection_probability

The probability of a fault injection during table creation after creating metadata in ZooKeeper

<table>
  <tr><td>Type</td><td><code>Float</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## create_table_empty_primary_key_by_default

Allow to create *MergeTree tables with empty primary key when ORDER BY and PRIMARY KEY not specified

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## cross_to_inner_join_rewrite

Use inner join instead of comma/cross join if there are joining expressions in the WHERE section. Values: 0 - no rewrite, 1 - apply if possible for comma/cross, 2 - force rewrite all comma joins, cross - if possible

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## database_replicated_allow_only_replicated_engine

Allow to create only Replicated tables in database with engine Replicated

<table>
  <tr><td>Readonly</td><td><code>1</code></td></tr>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## database_replicated_allow_replicated_engine_arguments

Allow to create only Replicated tables in database with engine Replicated with explicit arguments

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## database_replicated_always_detach_permanently

Execute DETACH TABLE as DETACH TABLE PERMANENTLY if database engine is Replicated

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## database_replicated_ddl_output

<DeprecatedBadge />

Obsolete setting, does nothing.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## database_replicated_enforce_synchronous_settings

Enforces synchronous waiting for some queries (see also database_atomic_wait_for_drop_and_detach_synchronously, mutation_sync, alter_sync). Not recommended to enable these settings.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## date_time_input_format

Method to read DateTime from text input formats. Possible values: &#039;basic&#039;, &#039;best_effort&#039; and &#039;best_effort_us&#039;.

<table>
  <tr><td>Type</td><td><code>DateTimeInputFormat</code></td></tr>
  <tr><td>Default</td><td><code>basic</code></td></tr>
</table>


## date_time_output_format

Method to write DateTime to text output. Possible values: &#039;simple&#039;, &#039;iso&#039;, &#039;unix_timestamp&#039;.

<table>
  <tr><td>Type</td><td><code>DateTimeOutputFormat</code></td></tr>
  <tr><td>Default</td><td><code>simple</code></td></tr>
</table>


## decimal_check_overflow

Check overflow of decimal arithmetic/comparison operations

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## default_database_engine

<DeprecatedBadge />

Obsolete setting, does nothing.

<table>
  <tr><td>Type</td><td><code>DefaultDatabaseEngine</code></td></tr>
  <tr><td>Default</td><td><code>Atomic</code></td></tr>
</table>


## default_max_bytes_in_join

Maximum size of right-side table if limit is required but max_bytes_in_join is not set.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>1000000000</code></td></tr>
</table>


## describe_compact_output

If true, include only column names and types into result of DESCRIBE query

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## describe_extend_object_types

Deduce concrete type of columns of type Object in DESCRIBE query

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## describe_include_virtual_columns

If true, virtual columns of table will be included into result of DESCRIBE query

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## dialect

Which dialect will be used to parse query

<table>
  <tr><td>Type</td><td><code>Dialect</code></td></tr>
  <tr><td>Default</td><td><code>clickhouse</code></td></tr>
</table>


## distinct_overflow_mode

What to do when the limit is exceeded.

<table>
  <tr><td>Type</td><td><code>OverflowMode</code></td></tr>
  <tr><td>Default</td><td><code>throw</code></td></tr>
</table>


## distributed_aggregation_memory_efficient

Is the memory-saving mode of distributed aggregation enabled.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## distributed_background_insert_timeout

Timeout for insert query into distributed. Setting is used only with insert_distributed_sync enabled. Zero value means no timeout.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## distributed_cache_bypass_connection_pool

Allow to bypass distributed cache connection pool

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## distributed_cache_connect_max_tries

Number of tries to connect to distributed cache if unsuccessful

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>100</code></td></tr>
</table>


## distributed_cache_fetch_metrics_only_from_current_az

Fetch metrics only from current availability zone in system.distributed_cache_metrics, system.distributed_cache_events

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## distributed_cache_log_mode

Mode for writing to system.distributed_cache_log

<table>
  <tr><td>Type</td><td><code>DistributedCacheLogMode</code></td></tr>
  <tr><td>Default</td><td><code>on_error</code></td></tr>
</table>


## distributed_cache_pool_behaviour_on_limit

Identifies behaviour of distributed cache connection on pool limit reached

<table>
  <tr><td>Type</td><td><code>DistributedCachePoolBehaviourOnLimit</code></td></tr>
  <tr><td>Default</td><td><code>allocate_bypassing_pool</code></td></tr>
</table>


## distributed_cache_receive_response_wait_milliseconds

Wait time in milliseconds to receive response from distributed cache

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>1000</code></td></tr>
</table>


## distributed_cache_send_profile_events

Send incremental profile events from distributed cache. They will be written to system.query log

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## distributed_cache_throw_on_error

Rethrow exception happened during communication with distributed cache or exception received from distributed cache. Otherwise fallback to skipping distributed cache on error

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## distributed_cache_wait_connection_from_pool_milliseconds

Wait time in milliseconds to receive connection from connection pool if distributed_cache_pool_behaviour_on_limit is wait

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>100</code></td></tr>
</table>


## distributed_ddl_entry_format_version

Compatibility version of distributed DDL (ON CLUSTER) queries

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>5</code></td></tr>
</table>


## distributed_directory_monitor_batch_inserts

Should background INSERTs into Distributed be batched into bigger blocks.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
  <tr><td>Alias for</td><td><code>distributed_background_insert_batch</code></td></tr>
</table>


## distributed_directory_monitor_max_sleep_time_ms

Maximum sleep time for background INSERTs into Distributed, it limits exponential growth too.

<table>
  <tr><td>Type</td><td><code>Milliseconds</code></td></tr>
  <tr><td>Default</td><td><code>30000</code></td></tr>
  <tr><td>Alias for</td><td><code>distributed_background_insert_max_sleep_time_ms</code></td></tr>
</table>


## distributed_directory_monitor_sleep_time_ms

Sleep time for background INSERTs into Distributed, in case of any errors delay grows exponentially.

<table>
  <tr><td>Type</td><td><code>Milliseconds</code></td></tr>
  <tr><td>Default</td><td><code>100</code></td></tr>
  <tr><td>Alias for</td><td><code>distributed_background_insert_sleep_time_ms</code></td></tr>
</table>


## distributed_directory_monitor_split_batch_on_failure

Should batches of the background INSERT into Distributed be split into smaller batches in case of failures.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
  <tr><td>Alias for</td><td><code>distributed_background_insert_split_batch_on_failure</code></td></tr>
</table>


## do_not_merge_across_partitions_select_final

Merge parts only in one partition in select final

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## drain_timeout

<DeprecatedBadge />

Obsolete setting, does nothing.

<table>
  <tr><td>Type</td><td><code>Seconds</code></td></tr>
  <tr><td>Default</td><td><code>3</code></td></tr>
</table>


## empty_result_for_aggregation_by_constant_keys_on_empty_set

Return empty result when aggregating by constant keys on empty set.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## empty_result_for_aggregation_by_empty_set

Return empty result when aggregating without keys on empty set.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## enable_debug_queries

<DeprecatedBadge />

Obsolete setting, does nothing.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## enable_early_constant_folding

Enable query optimization where we analyze function and subqueries results and rewrite query if there are constants there

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## enable_filesystem_cache

Use cache for remote filesystem. This setting does not turn on/off cache for disks (must be done via disk config), but allows to bypass cache for some queries if intended

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## enable_filesystem_cache_log

Allows to record the filesystem caching log for each query

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## enable_filesystem_cache_on_write_operations

Write into cache on write operations. To actually work this setting requires be added to disk config too

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## enable_filesystem_read_prefetches_log

Log to system.filesystem prefetch_log during query. Should be used only for testing or debugging, not recommended to be turned on by default

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## enable_global_with_statement

Propagate WITH statements to UNION queries and all subqueries

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## enable_job_stack_trace

Output stack trace of a job creator when job results in exception

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## enable_lightweight_delete

Enable lightweight DELETE mutations for mergetree tables.

<table>
  <tr><td>Readonly</td><td><code>1</code></td></tr>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## enable_memory_bound_merging_of_aggregation_results

Enable memory bound merging strategy for aggregation.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## enable_multiple_prewhere_read_steps

Move more conditions from WHERE to PREWHERE and do reads from disk and filtering in multiple steps if there are multiple conditions combined with AND

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## enable_optimize_predicate_expression_to_final_subquery

Allow push predicate to final subquery.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## enable_s3_requests_logging

Enable very explicit logging of S3 requests. Makes sense for debug only.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## enable_scalar_subquery_optimization

If it is set to true, prevent scalar subqueries from (de)serializing large scalar values and possibly avoid running the same subquery more than once.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## enable_sharing_sets_for_mutations

Allow sharing set objects build for IN subqueries between different tasks of the same mutation. This reduces memory usage and CPU consumption

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## enable_software_prefetch_in_aggregation

Enable use of software prefetch in aggregation

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## enable_unaligned_array_join

Allow ARRAY JOIN with multiple arrays that have different sizes. When this settings is enabled, arrays will be resized to the longest one.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## enable_vertical_final

Not recommended. If enable, remove duplicated rows during FINAL by marking rows as deleted and filtering them later instead of merging rows

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## errors_output_format

Method to write Errors to text output.

<table>
  <tr><td>Type</td><td><code>String</code></td></tr>
  <tr><td>Default</td><td><code>CSV</code></td></tr>
</table>


## exact_rows_before_limit

When enabled, ClickHouse will provide exact value for rows_before_limit_at_least statistic, but with the cost that the data before limit will have to be read completely

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## except_default_mode

Set default mode in EXCEPT query. Possible values: empty string, &#039;ALL&#039;, &#039;DISTINCT&#039;. If empty, query without mode will throw exception.

<table>
  <tr><td>Type</td><td><code>SetOperationMode</code></td></tr>
  <tr><td>Default</td><td><code>ALL</code></td></tr>
</table>


## external_storage_connect_timeout_sec

Connect timeout in seconds. Now supported only for MySQL

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>10</code></td></tr>
</table>


## external_storage_max_read_bytes

Limit maximum number of bytes when table with external engine should flush history data. Now supported only for MySQL table engine, database engine, dictionary and MaterializedMySQL. If equal to 0, this setting is disabled

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## external_storage_max_read_rows

Limit maximum number of rows when table with external engine should flush history data. Now supported only for MySQL table engine, database engine, dictionary and MaterializedMySQL. If equal to 0, this setting is disabled

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## external_storage_rw_timeout_sec

Read/write timeout in seconds. Now supported only for MySQL

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>300</code></td></tr>
</table>


## external_table_strict_query

If it is set to true, transforming expression to local filter is forbidden for queries to external tables.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## extract_key_value_pairs_max_pairs_per_row

Max number of pairs that can be produced by the `extractKeyValuePairs` function. Used as a safeguard against consuming too much memory.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>1000</code></td></tr>
</table>


## extract_kvp_max_pairs_per_row

Max number of pairs that can be produced by the `extractKeyValuePairs` function. Used as a safeguard against consuming too much memory.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>1000</code></td></tr>
  <tr><td>Alias for</td><td><code>extract_key_value_pairs_max_pairs_per_row</code></td></tr>
</table>


## filesystem_cache_max_download_size

Max remote filesystem cache size that can be downloaded by a single query

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>137438953472</code></td></tr>
</table>


## filesystem_cache_reserve_space_wait_lock_timeout_milliseconds

Wait time to lock cache for sapce reservation in filesystem cache

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>1000</code></td></tr>
</table>


## filesystem_cache_segments_batch_size

Limit on size of a single batch of file segments that a read buffer can request from cache. Too low value will lead to excessive requests to cache, too large may slow down eviction from cache

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>20</code></td></tr>
</table>


## filesystem_prefetch_max_memory_usage

Maximum memory usage for prefetches.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>1073741824</code></td></tr>
</table>


## filesystem_prefetch_min_bytes_for_single_read_task

Do not parallelize within one file read less than this amount of bytes. E.g. one reader will not receive a read task of size less than this amount. This setting is recommended to avoid spikes of time for aws getObject requests to aws

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>2097152</code></td></tr>
</table>


## filesystem_prefetch_step_bytes

Prefetch step in bytes. Zero means `auto` - approximately the best prefetch step will be auto deduced, but might not be 100% the best. The actual value might be different because of setting filesystem_prefetch_min_bytes_for_single_read_task

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## filesystem_prefetch_step_marks

Prefetch step in marks. Zero means `auto` - approximately the best prefetch step will be auto deduced, but might not be 100% the best. The actual value might be different because of setting filesystem_prefetch_min_bytes_for_single_read_task

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## filesystem_prefetches_limit

Maximum number of prefetches. Zero means unlimited. A setting `filesystem_prefetches_max_memory_usage` is more recommended if you want to limit the number of prefetches

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>200</code></td></tr>
</table>


## force_aggregate_partitions_independently

Force the use of optimization when it is applicable, but heuristics decided not to use it

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## force_aggregation_in_order

The setting is used by the server itself to support distributed queries. Do not change it manually, because it will break normal operations. (Forces use of aggregation in order on remote nodes during distributed aggregation).

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## force_grouping_standard_compatibility

Make GROUPING function to return 1 when argument is not used as an aggregation key

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## force_remove_data_recursively_on_drop

Recursively remove data on DROP query. Avoids &#039;Directory not empty&#039; error, but may silently remove detached data

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## format_avro_schema_registry_url

For AvroConfluent format: Confluent Schema Registry URL.

<table>
  <tr><td>Type</td><td><code>URI</code></td></tr>
</table>


## format_binary_max_array_size

The maximum allowed size for Array in RowBinary format. It prevents allocating large amount of memory in case of corrupted data. 0 means there is no limit

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>1073741824</code></td></tr>
</table>


## format_binary_max_string_size

The maximum allowed size for String in RowBinary format. It prevents allocating large amount of memory in case of corrupted data. 0 means there is no limit

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>1073741824</code></td></tr>
</table>


## format_capn_proto_enum_comparising_mode

How to map ClickHouse Enum and CapnProto Enum

<table>
  <tr><td>Type</td><td><code>CapnProtoEnumComparingMode</code></td></tr>
  <tr><td>Default</td><td><code>by_values</code></td></tr>
</table>


## format_capn_proto_use_autogenerated_schema

Use autogenerated CapnProto schema when format_schema is not set

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## format_csv_allow_double_quotes

If it is set to true, allow strings in double quotes.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## format_csv_allow_single_quotes

If it is set to true, allow strings in single quotes.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## format_csv_delimiter

The character to be considered as a delimiter in CSV data. If setting with a string, a string has to have a length of 1.

<table>
  <tr><td>Type</td><td><code>Char</code></td></tr>
  <tr><td>Default</td><td><code>,</code></td></tr>
</table>


## format_csv_null_representation

Custom NULL representation in CSV format

<table>
  <tr><td>Type</td><td><code>String</code></td></tr>
  <tr><td>Default</td><td><code>\N</code></td></tr>
</table>


## format_custom_escaping_rule

Field escaping rule (for CustomSeparated format)

<table>
  <tr><td>Type</td><td><code>EscapingRule</code></td></tr>
  <tr><td>Default</td><td><code>Escaped</code></td></tr>
</table>


## format_custom_field_delimiter

Delimiter between fields (for CustomSeparated format)

<table>
  <tr><td>Type</td><td><code>String</code></td></tr>
</table>


## format_custom_result_after_delimiter

Suffix after result set (for CustomSeparated format)

<table>
  <tr><td>Type</td><td><code>String</code></td></tr>
</table>


## format_custom_result_before_delimiter

Prefix before result set (for CustomSeparated format)

<table>
  <tr><td>Type</td><td><code>String</code></td></tr>
</table>


## format_custom_row_after_delimiter

Delimiter after field of the last column (for CustomSeparated format)

<table>
  <tr><td>Type</td><td><code>String</code></td></tr>
</table>


## format_custom_row_before_delimiter

Delimiter before field of the first column (for CustomSeparated format)

<table>
  <tr><td>Type</td><td><code>String</code></td></tr>
</table>


## format_custom_row_between_delimiter

Delimiter between rows (for CustomSeparated format)

<table>
  <tr><td>Type</td><td><code>String</code></td></tr>
</table>


## format_display_secrets_in_show_and_select

Do not hide secrets in SHOW and SELECT queries.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## format_json_object_each_row_column_for_object_name

The name of column that will be used as object names in JSONObjectEachRow format. Column type should be String

<table>
  <tr><td>Type</td><td><code>String</code></td></tr>
</table>


## format_protobuf_use_autogenerated_schema

Use autogenerated Protobuf when format_schema is not set

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## format_regexp

Regular expression (for Regexp format)

<table>
  <tr><td>Type</td><td><code>String</code></td></tr>
</table>


## format_regexp_escaping_rule

Field escaping rule (for Regexp format)

<table>
  <tr><td>Type</td><td><code>EscapingRule</code></td></tr>
  <tr><td>Default</td><td><code>Raw</code></td></tr>
</table>


## format_regexp_skip_unmatched

Skip lines unmatched by regular expression (for Regexp format)

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## format_schema

Schema identifier (used by schema-based formats)

<table>
  <tr><td>Type</td><td><code>String</code></td></tr>
</table>


## format_template_resultset

Path to file which contains format string for result set (for Template format)

<table>
  <tr><td>Type</td><td><code>String</code></td></tr>
</table>


## format_template_resultset_format

Format string for result set (for Template format)

<table>
  <tr><td>Type</td><td><code>String</code></td></tr>
</table>


## format_template_row

Path to file which contains format string for rows (for Template format)

<table>
  <tr><td>Type</td><td><code>String</code></td></tr>
</table>


## format_template_row_format

Format string for rows (for Template format)

<table>
  <tr><td>Type</td><td><code>String</code></td></tr>
</table>


## format_template_rows_between_delimiter

Delimiter between rows (for Template format)

<table>
  <tr><td>Type</td><td><code>String</code></td></tr>
</table>


## format_tsv_null_representation

Custom NULL representation in TSV format

<table>
  <tr><td>Type</td><td><code>String</code></td></tr>
  <tr><td>Default</td><td><code>\N</code></td></tr>
</table>


## formatdatetime_f_prints_single_zero

Formatter &#039;%f&#039; in function &#039;formatDateTime()&#039; prints a single zero instead of six zeros if the formatted value has no fractional seconds.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## formatdatetime_format_without_leading_zeros

Formatters &#039;%c&#039;, &#039;%l&#039; and &#039;%k&#039; in function &#039;formatDateTime()&#039; print months and hours without leading zeros.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## formatdatetime_parsedatetime_m_is_month_name

Formatter &#039;%M&#039; in functions &#039;formatDateTime()&#039; and &#039;parseDateTime()&#039; print/parse the month name instead of minutes.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## function_implementation

Choose function implementation for specific target or variant (experimental). If empty enable all of them.

<table>
  <tr><td>Type</td><td><code>String</code></td></tr>
</table>


## function_sleep_max_microseconds_per_block

Maximum number of microseconds the function `sleep` is allowed to sleep for each block. If a user called it with a larger value, it throws an exception. It is a safety threshold.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>3000000</code></td></tr>
</table>


## function_visible_width_behavior

The version of `visibleWidth` behavior. 0 - only count the number of code points; 1 - correctly count zero-width and combining characters, count full-width characters as two, estimate the tab width, count delete characters.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## glob_expansion_max_elements

Maximum number of allowed addresses (For external storages, table functions, etc).

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>1000</code></td></tr>
</table>


## grace_hash_join_initial_buckets

Initial number of grace hash join buckets

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## grace_hash_join_max_buckets

Limit on the number of grace hash join buckets

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>1024</code></td></tr>
</table>


## group_by_overflow_mode

What to do when the limit is exceeded.

<table>
  <tr><td>Type</td><td><code>OverflowModeGroupBy</code></td></tr>
  <tr><td>Default</td><td><code>throw</code></td></tr>
</table>


## group_by_two_level_threshold

From what number of keys, a two-level aggregation starts. 0 - the threshold is not set.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>100000</code></td></tr>
</table>


## group_by_two_level_threshold_bytes

From what size of the aggregation state in bytes, a two-level aggregation begins to be used. 0 - the threshold is not set. Two-level aggregation is used when at least one of the thresholds is triggered.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>50000000</code></td></tr>
</table>


## handle_kafka_error_mode

<DeprecatedBadge />

Obsolete setting, does nothing.

<table>
  <tr><td>Type</td><td><code>StreamingHandleErrorMode</code></td></tr>
  <tr><td>Default</td><td><code>default</code></td></tr>
</table>


## hdfs_replication

The actual number of replications can be specified when the hdfs file is created.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## hedged_connection_timeout_ms

Connection timeout for establishing connection with replica for Hedged requests

<table>
  <tr><td>Type</td><td><code>Milliseconds</code></td></tr>
  <tr><td>Default</td><td><code>50</code></td></tr>
</table>


## hsts_max_age

Expired time for hsts. 0 means disable HSTS.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## http_headers_progress_interval_ms

Do not send HTTP headers X-ClickHouse-Progress more frequently than at each specified interval.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>100</code></td></tr>
</table>


## http_max_chunk_size

Maximum value of a chunk size in HTTP chunked transfer encoding

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>107374182400</code></td></tr>
</table>


## http_max_field_name_size

Maximum length of field name in HTTP header

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>131072</code></td></tr>
</table>


## http_max_field_value_size

Maximum length of field value in HTTP header

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>131072</code></td></tr>
</table>


## http_max_fields

Maximum number of fields in HTTP header

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>1000000</code></td></tr>
</table>


## http_max_multipart_form_data_size

Limit on size of multipart/form-data content. This setting cannot be parsed from URL parameters and should be set in user profile. Note that content is parsed and external tables are created in memory before start of query execution. And this is the only limit that has effect on that stage (limits on max memory usage and max execution time have no effect while reading HTTP form data).

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>1073741824</code></td></tr>
</table>


## http_max_request_param_data_size

Limit on size of request data used as a query parameter in predefined HTTP requests.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>10485760</code></td></tr>
</table>


## http_max_tries

Max attempts to read via http.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>10</code></td></tr>
</table>


## http_response_buffer_size

The number of bytes to buffer in the server memory before sending a HTTP response to the client or flushing to disk (when http_wait_end_of_query is enabled).

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## http_retry_initial_backoff_ms

Min milliseconds for backoff, when retrying read via http

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>100</code></td></tr>
</table>


## http_retry_max_backoff_ms

Max milliseconds for backoff, when retrying read via http

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>10000</code></td></tr>
</table>


## http_skip_not_found_url_for_globs

Skip url&#039;s for globs with HTTP_NOT_FOUND error

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## http_wait_end_of_query

Enable HTTP response buffering on the server-side.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## http_write_exception_in_output_format

Write exception in output format to produce valid output. Works with JSON and XML formats.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## ignore_cold_parts_seconds

Only available in ClickHouse Cloud. Exclude new data parts from SELECT queries until they&#039;re either pre-warmed (see cache_populated_by_fetch) or this many seconds old. Only for Replicated-/SharedMergeTree.

<table>
  <tr><td>Type</td><td><code>Int64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## ignore_materialized_views_with_dropped_target_table

Ignore MVs with dropped target table during pushing to views

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## ignore_on_cluster_for_replicated_access_entities_queries

Ignore ON CLUSTER clause for replicated access entities management queries.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## ignore_on_cluster_for_replicated_udf_queries

Ignore ON CLUSTER clause for replicated UDF management queries.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## implicit_transaction

If enabled and not already inside a transaction, wraps the query inside a full transaction (begin + commit or rollback)

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## input_format_allow_errors_num

Maximum absolute amount of errors while reading text formats (like CSV, TSV). In case of error, if at least absolute or relative amount of errors is lower than corresponding value, will skip until next line and continue.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## input_format_allow_errors_ratio

Maximum relative amount of errors while reading text formats (like CSV, TSV). In case of error, if at least absolute or relative amount of errors is lower than corresponding value, will skip until next line and continue.

<table>
  <tr><td>Type</td><td><code>Float</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## input_format_allow_seeks

Allow seeks while reading in ORC/Parquet/Arrow input formats

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## input_format_arrow_allow_missing_columns

Allow missing columns while reading Arrow input formats

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## input_format_arrow_case_insensitive_column_matching

Ignore case when matching Arrow columns with CH columns.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## input_format_arrow_import_nested

<DeprecatedBadge />

Obsolete setting, does nothing.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## input_format_arrow_skip_columns_with_unsupported_types_in_schema_inference

Skip columns with unsupported types while schema inference for format Arrow

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## input_format_avro_allow_missing_fields

For Avro/AvroConfluent format: when field is not found in schema use default value instead of error

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## input_format_avro_null_as_default

For Avro/AvroConfluent format: insert default in case of null and non Nullable column

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## input_format_bson_skip_fields_with_unsupported_types_in_schema_inference

Skip fields with unsupported types while schema inference for format BSON.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## input_format_capn_proto_skip_fields_with_unsupported_types_in_schema_inference

Skip columns with unsupported types while schema inference for format CapnProto

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## input_format_csv_allow_cr_end_of_line

If it is set true, \r will be allowed at end of line not followed by \n

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## input_format_csv_allow_variable_number_of_columns

Ignore extra columns in CSV input (if file has more columns than expected) and treat missing fields in CSV input as default values

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## input_format_csv_allow_whitespace_or_tab_as_delimiter

Allow to use spaces and tabs(\t) as field delimiter in the CSV strings

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## input_format_csv_arrays_as_nested_csv

When reading Array from CSV, expect that its elements were serialized in nested CSV and then put into string. Example: &quot;[&quot;&quot;Hello&quot;&quot;, &quot;&quot;world&quot;&quot;, &quot;&quot;42&quot;&quot;&quot;&quot; TV&quot;&quot;]&quot;. Braces around array can be omitted.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## input_format_csv_detect_header

Automatically detect header with names and types in CSV format

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## input_format_csv_empty_as_default

Treat empty fields in CSV input as default values.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## input_format_csv_enum_as_number

Treat inserted enum values in CSV formats as enum indices

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## input_format_csv_skip_first_lines

Skip specified number of lines at the beginning of data in CSV format

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## input_format_csv_skip_trailing_empty_lines

Skip trailing empty lines in CSV format

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## input_format_csv_trim_whitespaces

Trims spaces and tabs (\t) characters at the beginning and end in CSV strings

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## input_format_csv_try_infer_numbers_from_strings

Try to infer numbers from string fields while schema inference in CSV format

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## input_format_csv_use_best_effort_in_schema_inference

Use some tweaks and heuristics to infer schema in CSV format

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## input_format_csv_use_default_on_bad_values

Allow to set default value to column when CSV field deserialization failed on bad value

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## input_format_custom_allow_variable_number_of_columns

Ignore extra columns in CustomSeparated input (if file has more columns than expected) and treat missing fields in CustomSeparated input as default values

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## input_format_custom_detect_header

Automatically detect header with names and types in CustomSeparated format

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## input_format_custom_skip_trailing_empty_lines

Skip trailing empty lines in CustomSeparated format

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## input_format_defaults_for_omitted_fields

For input data calculate default expressions for omitted fields (it works for JSONEachRow, -WithNames, -WithNamesAndTypes formats).

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## input_format_hive_text_collection_items_delimiter

Delimiter between collection(array or map) items in Hive Text File

<table>
  <tr><td>Type</td><td><code>Char</code></td></tr>
  <tr><td>Default</td><td><code></code></td></tr>
</table>


## input_format_hive_text_fields_delimiter

Delimiter between fields in Hive Text File

<table>
  <tr><td>Type</td><td><code>Char</code></td></tr>
  <tr><td>Default</td><td><code></code></td></tr>
</table>


## input_format_hive_text_map_keys_delimiter

Delimiter between a pair of map key/values in Hive Text File

<table>
  <tr><td>Type</td><td><code>Char</code></td></tr>
  <tr><td>Default</td><td><code></code></td></tr>
</table>


## input_format_import_nested_json

Map nested JSON data to nested tables (it works for JSONEachRow format).

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## input_format_ipv4_default_on_conversion_error

Deserialization of IPv4 will use default values instead of throwing exception on conversion error.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## input_format_ipv6_default_on_conversion_error

Deserialization of IPV6 will use default values instead of throwing exception on conversion error.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## input_format_json_compact_allow_variable_number_of_columns

Ignore extra columns in JSONCompact(EachRow) input (if file has more columns than expected) and treat missing fields in JSONCompact(EachRow) input as default values

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## input_format_json_defaults_for_missing_elements_in_named_tuple

Insert default value in named tuple element if it&#039;s missing in json object

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## input_format_json_ignore_unknown_keys_in_named_tuple

Ignore unknown keys in json object for named tuples

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## input_format_json_infer_incomplete_types_as_strings

Use type String for keys that contains only Nulls or empty objects/arrays during schema inference in JSON input formats

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## input_format_json_named_tuples_as_objects

Deserialize named tuple columns as JSON objects

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## input_format_json_read_arrays_as_strings

Allow to parse JSON arrays as strings in JSON input formats

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## input_format_json_read_bools_as_numbers

Allow to parse bools as numbers in JSON input formats

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## input_format_json_read_bools_as_strings

Allow to parse bools as strings in JSON input formats

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## input_format_json_read_numbers_as_strings

Allow to parse numbers as strings in JSON input formats

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## input_format_json_read_objects_as_strings

Allow to parse JSON objects as strings in JSON input formats

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## input_format_json_try_infer_named_tuples_from_objects

Try to infer named tuples from JSON objects in JSON input formats

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## input_format_json_try_infer_numbers_from_strings

Try to infer numbers from string fields while schema inference

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## input_format_json_use_string_type_for_ambiguous_paths_in_named_tuples_inference_from_objects

Use String type instead of an exception in case of ambiguous paths in JSON objects during named tuples inference

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## input_format_json_validate_types_from_metadata

For JSON/JSONCompact/JSONColumnsWithMetadata input formats this controls whether format parser should check if data types from input metadata match data types of the corresponding columns from the table

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## input_format_max_bytes_to_read_for_schema_inference

The maximum bytes of data to read for automatic schema inference

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>33554432</code></td></tr>
</table>


## input_format_max_rows_to_read_for_schema_inference

The maximum rows of data to read for automatic schema inference

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>25000</code></td></tr>
</table>


## input_format_msgpack_number_of_columns

The number of columns in inserted MsgPack data. Used for automatic schema inference from data.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## input_format_mysql_dump_map_column_names

Match columns from table in MySQL dump and columns from ClickHouse table by names

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## input_format_mysql_dump_table_name

Name of the table in MySQL dump from which to read data

<table>
  <tr><td>Type</td><td><code>String</code></td></tr>
</table>


## input_format_native_allow_types_conversion

Allow data types conversion in Native input format

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## input_format_null_as_default

Initialize null fields with default values if the data type of this field is not nullable and it is supported by the input format

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## input_format_orc_allow_missing_columns

Allow missing columns while reading ORC input formats

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## input_format_orc_case_insensitive_column_matching

Ignore case when matching ORC columns with CH columns.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## input_format_orc_filter_push_down

When reading ORC files, skip whole stripes or row groups based on the WHERE/PREWHERE expressions, min/max statistics or bloom filter in the ORC metadata.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## input_format_orc_import_nested

<DeprecatedBadge />

Obsolete setting, does nothing.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## input_format_orc_row_batch_size

Batch size when reading ORC stripes.

<table>
  <tr><td>Type</td><td><code>Int64</code></td></tr>
  <tr><td>Default</td><td><code>100000</code></td></tr>
</table>


## input_format_orc_skip_columns_with_unsupported_types_in_schema_inference

Skip columns with unsupported types while schema inference for format ORC

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## input_format_orc_use_fast_decoder

Use a faster ORC decoder implementation.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## input_format_parquet_allow_missing_columns

Allow missing columns while reading Parquet input formats

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## input_format_parquet_case_insensitive_column_matching

Ignore case when matching Parquet columns with CH columns.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## input_format_parquet_filter_push_down

When reading Parquet files, skip whole row groups based on the WHERE/PREWHERE expressions and min/max statistics in the Parquet metadata.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## input_format_parquet_import_nested

<DeprecatedBadge />

Obsolete setting, does nothing.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## input_format_parquet_local_file_min_bytes_for_seek

Min bytes required for local read (file) to do seek, instead of read with ignore in Parquet input format

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>8192</code></td></tr>
</table>


## input_format_parquet_max_block_size

Max block size for parquet reader.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>8192</code></td></tr>
</table>


## input_format_parquet_preserve_order

Avoid reordering rows when reading from Parquet files. Usually makes it much slower.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## input_format_parquet_skip_columns_with_unsupported_types_in_schema_inference

Skip columns with unsupported types while schema inference for format Parquet

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## input_format_protobuf_flatten_google_wrappers

Enable Google wrappers for regular non-nested columns, e.g. google.protobuf.StringValue &#039;str&#039; for String column &#039;str&#039;. For Nullable columns empty wrappers are recognized as defaults, and missing as nulls

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## input_format_protobuf_skip_fields_with_unsupported_types_in_schema_inference

Skip fields with unsupported types while schema inference for format Protobuf

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## input_format_record_errors_file_path

Path of the file used to record errors while reading text formats (CSV, TSV).

<table>
  <tr><td>Type</td><td><code>String</code></td></tr>
</table>


## input_format_skip_unknown_fields

Skip columns with unknown names from input data (it works for JSONEachRow, -WithNames, -WithNamesAndTypes and TSKV formats).

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## input_format_try_infer_dates

Try to infer dates from string fields while schema inference in text formats

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## input_format_try_infer_datetimes

Try to infer datetimes from string fields while schema inference in text formats

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## input_format_try_infer_exponent_floats

Try to infer floats in exponential notation while schema inference in text formats (except JSON, where exponent numbers are always inferred)

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## input_format_try_infer_integers

Try to infer integers instead of floats while schema inference in text formats

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## input_format_tsv_allow_variable_number_of_columns

Ignore extra columns in TSV input (if file has more columns than expected) and treat missing fields in TSV input as default values

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## input_format_tsv_detect_header

Automatically detect header with names and types in TSV format

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## input_format_tsv_empty_as_default

Treat empty fields in TSV input as default values.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## input_format_tsv_enum_as_number

Treat inserted enum values in TSV formats as enum indices.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## input_format_tsv_skip_first_lines

Skip specified number of lines at the beginning of data in TSV format

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## input_format_tsv_skip_trailing_empty_lines

Skip trailing empty lines in TSV format

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## input_format_tsv_use_best_effort_in_schema_inference

Use some tweaks and heuristics to infer schema in TSV format

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## input_format_values_accurate_types_of_literals

For Values format: when parsing and interpreting expressions using template, check actual type of literal to avoid possible overflow and precision issues.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## input_format_values_allow_data_after_semicolon

For Values format: allow extra data after semicolon (used by client to interpret comments).

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## input_format_values_deduce_templates_of_expressions

For Values format: if the field could not be parsed by streaming parser, run SQL parser, deduce template of the SQL expression, try to parse all rows using template and then interpret expression for all rows.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## input_format_values_interpret_expressions

For Values format: if the field could not be parsed by streaming parser, run SQL parser and try to interpret it as SQL expression.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## input_format_with_names_use_header

For -WithNames input formats this controls whether format parser is to assume that column data appear in the input exactly as they are specified in the header.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## input_format_with_types_use_header

For -WithNamesAndTypes input formats this controls whether format parser should check if data types from the input match data types from the header.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## insert_allow_materialized_columns

If setting is enabled, Allow materialized columns in INSERT.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## insert_distributed_one_random_shard

If setting is enabled, inserting into distributed table will choose a random shard to write when there is no sharding key

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## insert_distributed_timeout

Timeout for insert query into distributed. Setting is used only with insert_distributed_sync enabled. Zero value means no timeout.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
  <tr><td>Alias for</td><td><code>distributed_background_insert_timeout</code></td></tr>
</table>


## insert_keeper_fault_injection_probability

Approximate probability of failure for a keeper request during insert. Valid value is in interval [0.0f, 1.0f]

<table>
  <tr><td>Type</td><td><code>Float</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## insert_keeper_fault_injection_seed

0 - random seed, otherwise the setting value

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## intersect_default_mode

Set default mode in INTERSECT query. Possible values: empty string, &#039;ALL&#039;, &#039;DISTINCT&#039;. If empty, query without mode will throw exception.

<table>
  <tr><td>Type</td><td><code>SetOperationMode</code></td></tr>
  <tr><td>Default</td><td><code>ALL</code></td></tr>
</table>


## interval_output_format

Textual representation of Interval. Possible values: &#039;kusto&#039;, &#039;numeric&#039;.

<table>
  <tr><td>Type</td><td><code>IntervalOutputFormat</code></td></tr>
  <tr><td>Default</td><td><code>numeric</code></td></tr>
</table>


## join_overflow_mode

What to do when the limit is exceeded.

<table>
  <tr><td>Type</td><td><code>OverflowMode</code></td></tr>
  <tr><td>Default</td><td><code>throw</code></td></tr>
</table>


## joined_subquery_requires_alias

Force joined subqueries and table functions to have aliases for correct name qualification.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## keeper_map_strict_mode

Enforce additional checks during operations on KeeperMap. E.g. throw an exception on an insert for already existing key

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## keeper_max_retries

Max retries for general keeper operations

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>10</code></td></tr>
</table>


## keeper_retry_initial_backoff_ms

Initial backoff timeout for general keeper operations

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>100</code></td></tr>
</table>


## keeper_retry_max_backoff_ms

Max backoff timeout for general keeper operations

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>5000</code></td></tr>
</table>


## legacy_column_name_of_tuple_literal

List all names of element of large tuple literals in their column names instead of hash. This settings exists only for compatibility reasons. It makes sense to set to &#039;true&#039;, while doing rolling update of cluster from version lower than 21.7 to higher.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## load_balancing_first_offset

Which replica to preferably send a query when FIRST_OR_RANDOM load balancing strategy is used.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## load_marks_asynchronously

Load MergeTree marks asynchronously

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## local_filesystem_read_method

Method of reading data from local filesystem, one of: read, pread, mmap, io_uring, pread_threadpool. The &#039;io_uring&#039; method is experimental and does not work for Log, TinyLog, StripeLog, File, Set and Join, and other tables with append-able files in presence of concurrent reads and writes.

<table>
  <tr><td>Type</td><td><code>String</code></td></tr>
  <tr><td>Default</td><td><code>pread_threadpool</code></td></tr>
</table>


## local_filesystem_read_prefetch

Should use prefetching when reading data from local filesystem.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## log_profile_events

Log query performance statistics into the query_log, query_thread_log and query_views_log.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## log_queries_cut_to_length

If query length is greater than specified threshold (in bytes), then cut query when writing to query log. Also limit length of printed query in ordinary text log.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>100000</code></td></tr>
</table>


## log_query_settings

Log query settings into the query_log.

<table>
  <tr><td>Readonly</td><td><code>1</code></td></tr>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## materialize_ttl_after_modify

Apply TTL for old data, after ALTER MODIFY TTL query

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## materialized_views_ignore_errors

Allows to ignore errors for MATERIALIZED VIEW, and deliver original block to the table regardless of MVs

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## max_alter_threads

<DeprecatedBadge />

Obsolete setting, does nothing.

<table>
  <tr><td>Type</td><td><code>MaxThreads</code></td></tr>
  <tr><td>Default</td><td><code>&#039;auto(5)&#039;</code></td></tr>
</table>


## max_analyze_depth

Maximum number of analyses performed by interpreter.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>5000</code></td></tr>
</table>


## max_ast_depth

Maximum depth of query syntax tree. Checked after parsing.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>1000</code></td></tr>
</table>


## max_ast_elements

Maximum size of query syntax tree in number of nodes. Checked after parsing.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>50000</code></td></tr>
</table>


## max_backup_bandwidth

The maximum read speed in bytes per second for particular backup on server. Zero means unlimited.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## max_bytes_before_external_group_by

If memory usage during GROUP BY operation is exceeding this threshold in bytes, activate the &#039;external aggregation&#039; mode (spill data to disk). Recommended value is half of available system memory.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## max_bytes_before_external_sort

If memory usage during ORDER BY operation is exceeding this threshold in bytes, activate the &#039;external sorting&#039; mode (spill data to disk). Recommended value is half of available system memory.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## max_bytes_before_remerge_sort

In case of ORDER BY with LIMIT, when memory usage is higher than specified threshold, perform additional steps of merging blocks before final merge to keep just top LIMIT rows.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>1000000000</code></td></tr>
</table>


## max_bytes_in_distinct

Maximum total size of state (in uncompressed bytes) in memory for the execution of DISTINCT.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## max_bytes_in_join

Maximum size of the hash table for JOIN (in number of bytes in memory).

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## max_bytes_in_set

Maximum size of the set (in bytes in memory) resulting from the execution of the IN section.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## max_bytes_to_read

Limit on read bytes (after decompression) from the most &#039;deep&#039; sources. That is, only in the deepest subquery. When reading from a remote server, it is only checked on a remote server.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## max_bytes_to_read_leaf

Limit on read bytes (after decompression) on the leaf nodes for distributed queries. Limit is applied for local reads only, excluding the final merge stage on the root node. Note, the setting is unstable with prefer_localhost_replica=1.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## max_bytes_to_sort

If more than the specified amount of (uncompressed) bytes have to be processed for ORDER BY operation, the behavior will be determined by the &#039;sort_overflow_mode&#039; which by default is - throw an exception

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## max_bytes_to_transfer

Maximum size (in uncompressed bytes) of the transmitted external table obtained when the GLOBAL IN/JOIN section is executed.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## max_columns_to_read

If a query requires reading more than specified number of columns, exception is thrown. Zero value means unlimited. This setting is useful to prevent too complex queries.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## max_download_buffer_size

The maximal size of buffer for parallel downloading (e.g. for URL engine) per each thread.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>10485760</code></td></tr>
</table>


## max_download_threads

The maximum number of threads to download data (e.g. for URL engine).

<table>
  <tr><td>Type</td><td><code>MaxThreads</code></td></tr>
  <tr><td>Default</td><td><code>4</code></td></tr>
</table>


## max_entries_for_hash_table_stats

How many entries hash table statistics collected during aggregation is allowed to have

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>10000</code></td></tr>
</table>


## max_estimated_execution_time

Maximum query estimate execution time in seconds.

<table>
  <tr><td>Type</td><td><code>Seconds</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## max_execution_speed

Maximum number of execution rows per second.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## max_execution_speed_bytes

Maximum number of execution bytes per second.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## max_execution_time

If query runtime exceeds the specified number of seconds, the behavior will be determined by the &#039;timeout_overflow_mode&#039;, which by default is - throw an exception. Note that the timeout is checked and query can stop only in designated places during data processing. It currently cannot stop during merging of aggregation states or during query analysis, and the actual run time will be higher than the value of this setting.

<table>
  <tr><td>Type</td><td><code>Seconds</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## max_execution_time_leaf

Similar semantic to max_execution_time but only apply on leaf node for distributed queries, the time out behavior will be determined by &#039;timeout_overflow_mode_leaf&#039; which by default is - throw an exception

<table>
  <tr><td>Type</td><td><code>Seconds</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## max_expanded_ast_elements

Maximum size of query syntax tree in number of nodes after expansion of aliases and the asterisk.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>500000</code></td></tr>
</table>


## max_fetch_partition_retries_count

Amount of retries while fetching partition from another host.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>5</code></td></tr>
</table>


## max_insert_delayed_streams_for_parallel_write

The maximum number of streams (columns) to delay final part flush. Default - auto (1000 in case of underlying storage supports parallel write, for example S3 and disabled otherwise)

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## max_joined_block_size_rows

Maximum block size for JOIN result (if join algorithm supports it). 0 means unlimited.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>65409</code></td></tr>
</table>


## max_limit_for_ann_queries

SELECT queries with LIMIT bigger than this setting cannot use ANN indexes. Helps to prevent memory overflows in ANN search indexes.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>1000000</code></td></tr>
</table>


## max_local_read_bandwidth

The maximum speed of local reads in bytes per second.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## max_local_write_bandwidth

The maximum speed of local writes in bytes per second.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## max_memory_usage

Maximum memory usage for processing of single query. Zero means unlimited.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## max_memory_usage_for_all_queries

<DeprecatedBadge />

Obsolete setting, does nothing.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## max_memory_usage_for_user

Maximum memory usage for processing all concurrently running queries for the user. Zero means unlimited.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## max_number_of_partitions_for_independent_aggregation

Maximal number of partitions in table to apply optimization

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>128</code></td></tr>
</table>


## max_parser_backtracks

Maximum parser backtracking (how many times it tries different alternatives in the recursive descend parsing process).

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>1000000</code></td></tr>
</table>


## max_partitions_per_insert_block

Limit maximum number of partitions in single INSERTed block. Zero means unlimited. Throw exception if the block contains too many partitions. This setting is a safety threshold, because using large number of partitions is a common misconception.

<table>
  <tr><td>Readonly</td><td><code>1</code></td></tr>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>100</code></td></tr>
</table>


## max_partitions_to_read

Limit the max number of partitions that can be accessed in one query. &lt;= 0 means unlimited.

<table>
  <tr><td>Type</td><td><code>Int64</code></td></tr>
  <tr><td>Default</td><td><code>-1</code></td></tr>
</table>


## max_pipeline_depth

<DeprecatedBadge />

Obsolete setting, does nothing.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## max_read_buffer_size

The maximum size of the buffer to read from the filesystem.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>1048576</code></td></tr>
</table>


## max_read_buffer_size_local_fs

The maximum size of the buffer to read from local filesystem. If set to 0 then max_read_buffer_size will be used.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>131072</code></td></tr>
</table>


## max_read_buffer_size_remote_fs

The maximum size of the buffer to read from remote filesystem. If set to 0 then max_read_buffer_size will be used.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## max_remote_read_network_bandwidth

The maximum speed of data exchange over the network in bytes per second for read.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## max_remote_read_network_bandwidth_for_server

<DeprecatedBadge />

User-level setting is deprecated, and it must be defined in the server configuration instead.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## max_remote_write_network_bandwidth

The maximum speed of data exchange over the network in bytes per second for write.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## max_remote_write_network_bandwidth_for_server

<DeprecatedBadge />

User-level setting is deprecated, and it must be defined in the server configuration instead.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## max_result_bytes

Limit on result size in bytes (uncompressed).  The query will stop after processing a block of data if the threshold is met, but it will not cut the last block of the result, therefore the result size can be larger than the threshold. Caveats: the result size in memory is taken into account for this threshold. Even if the result size is small, it can reference larger data structures in memory, representing dictionaries of LowCardinality columns, and Arenas of AggregateFunction columns, so the threshold can be exceeded despite the small result size. The setting is fairly low level and should be used with caution.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## max_result_rows

Limit on result size in rows. The query will stop after processing a block of data if the threshold is met, but it will not cut the last block of the result, therefore the result size can be larger than the threshold.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## max_rows_in_distinct

Maximum number of elements during execution of DISTINCT.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## max_rows_in_join

Maximum size of the hash table for JOIN (in number of rows).

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## max_rows_in_set

Maximum size of the set (in number of elements) resulting from the execution of the IN section.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## max_rows_to_group_by

If aggregation during GROUP BY is generating more than the specified number of rows (unique GROUP BY keys), the behavior will be determined by the &#039;group_by_overflow_mode&#039; which by default is - throw an exception, but can be also switched to an approximate GROUP BY mode.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## max_rows_to_read

Limit on read rows from the most &#039;deep&#039; sources. That is, only in the deepest subquery. When reading from a remote server, it is only checked on a remote server.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## max_rows_to_read_leaf

Limit on read rows on the leaf nodes for distributed queries. Limit is applied for local reads only, excluding the final merge stage on the root node. Note, the setting is unstable with prefer_localhost_replica=1.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## max_rows_to_sort

If more than the specified amount of records have to be processed for ORDER BY operation, the behavior will be determined by the &#039;sort_overflow_mode&#039; which by default is - throw an exception

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## max_rows_to_transfer

Maximum size (in rows) of the transmitted external table obtained when the GLOBAL IN/JOIN section is executed.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## max_sessions_for_user

Maximum number of simultaneous sessions for a user.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## max_size_to_preallocate_for_aggregation

For how many elements it is allowed to preallocate space in all hash tables in total before aggregation

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>100000000</code></td></tr>
</table>


## max_streams_for_merge_tree_reading

If is not zero, limit the number of reading streams for MergeTree table.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## max_streams_multiplier_for_merge_tables

Ask more streams when reading from Merge table. Streams will be spread across tables that Merge table will use. This allows more even distribution of work across threads and especially helpful when merged tables differ in size.

<table>
  <tr><td>Type</td><td><code>Float</code></td></tr>
  <tr><td>Default</td><td><code>5</code></td></tr>
</table>


## max_streams_to_max_threads_ratio

Allows you to use more sources than the number of threads - to more evenly distribute work across threads. It is assumed that this is a temporary solution, since it will be possible in the future to make the number of sources equal to the number of threads, but for each source to dynamically select available work for itself.

<table>
  <tr><td>Type</td><td><code>Float</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## max_subquery_depth

If a query has more than the specified number of nested subqueries, throw an exception. This allows you to have a sanity check to protect the users of your cluster from going insane with their queries.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>100</code></td></tr>
</table>


## max_temporary_columns

If a query generates more than the specified number of temporary columns in memory as a result of intermediate calculation, exception is thrown. Zero value means unlimited. This setting is useful to prevent too complex queries.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## max_temporary_data_on_disk_size_for_query

The maximum amount of data consumed by temporary files on disk in bytes for all concurrently running queries. Zero means unlimited.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## max_temporary_data_on_disk_size_for_user

The maximum amount of data consumed by temporary files on disk in bytes for all concurrently running user queries. Zero means unlimited.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## max_temporary_non_const_columns

Similar to the &#039;max_temporary_columns&#039; setting but applies only to non-constant columns. This makes sense, because constant columns are cheap and it is reasonable to allow more of them.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## max_threads_for_annoy_index_creation

Number of threads used to build Annoy indexes (0 means all cores, not recommended)

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>4</code></td></tr>
</table>


## max_threads_for_indexes

The maximum number of threads process indices.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## max_untracked_memory

Small allocations and deallocations are grouped in thread local variable and tracked or profiled only when amount (in absolute value) becomes larger than specified value. If the value is higher than &#039;memory_profiler_step&#039; it will be effectively lowered to &#039;memory_profiler_step&#039;.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>4194304</code></td></tr>
</table>


## memory_profiler_sample_max_allocation_size

Collect random allocations of size less or equal than specified value with probability equal to `memory_profiler_sample_probability`. 0 means disabled. You may want to set &#039;max_untracked_memory&#039; to 0 to make this threshold to work as expected.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## memory_profiler_sample_min_allocation_size

Collect random allocations of size greater or equal than specified value with probability equal to `memory_profiler_sample_probability`. 0 means disabled. You may want to set &#039;max_untracked_memory&#039; to 0 to make this threshold to work as expected.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## memory_tracker_fault_probability

For testing of `exception safety` - throw an exception every time you allocate memory with the specified probability.

<table>
  <tr><td>Type</td><td><code>Float</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## merge_tree_clear_old_parts_interval_seconds

<DeprecatedBadge />

Obsolete setting, does nothing.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## merge_tree_clear_old_temporary_directories_interval_seconds

<DeprecatedBadge />

Obsolete setting, does nothing.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>60</code></td></tr>
</table>


## merge_tree_compact_parts_min_granules_to_multibuffer_read

Number of granules in stripe of compact part of MergeTree tables to use multibuffer reader, which supports parallel reading and prefetch. In case of reading from remote fs using of multibuffer reader increases number of read request.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>16</code></td></tr>
</table>


## merge_tree_determine_task_size_by_prewhere_columns

Whether to use only prewhere columns size to determine reading task size.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## merge_tree_min_bytes_per_task_for_remote_reading

Min bytes to read per task.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>4194304</code></td></tr>
</table>


## merge_tree_read_split_ranges_into_intersecting_and_non_intersecting_injection_probability

For testing of `PartsSplitter` - split read ranges into intersecting and non intersecting every time you read from MergeTree with the specified probability.

<table>
  <tr><td>Type</td><td><code>Float</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## merge_tree_use_const_size_tasks_for_remote_reading

Whether to use constant size tasks for reading from a remote table.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## metrics_perf_events_enabled

If enabled, some of the perf events will be measured throughout queries&#039; execution.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## metrics_perf_events_list

Comma separated list of perf metrics that will be measured throughout queries&#039; execution. Empty means all events. See PerfEventInfo in sources for the available events.

<table>
  <tr><td>Type</td><td><code>String</code></td></tr>
</table>


## min_count_to_compile_sort_description

The number of identical sort descriptions before they are JIT-compiled

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>3</code></td></tr>
</table>


## min_execution_speed

Minimum number of execution rows per second.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## min_execution_speed_bytes

Minimum number of execution bytes per second.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## min_external_table_block_size_bytes

Squash blocks passed to external table to specified size in bytes, if blocks are not big enough.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>268402944</code></td></tr>
</table>


## min_external_table_block_size_rows

Squash blocks passed to external table to specified size in rows, if blocks are not big enough.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>1048449</code></td></tr>
</table>


## min_free_disk_space_for_temporary_data

The minimum disk space to keep while writing temporary data used in external sorting and aggregation.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## min_hit_rate_to_use_consecutive_keys_optimization

Minimal hit rate of a cache which is used for consecutive keys optimization in aggregation to keep it enabled

<table>
  <tr><td>Type</td><td><code>Float</code></td></tr>
  <tr><td>Default</td><td><code>0.5</code></td></tr>
</table>


## move_all_conditions_to_prewhere

Move all viable conditions from WHERE to PREWHERE

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## move_primary_key_columns_to_end_of_prewhere

Move PREWHERE conditions containing primary key columns to the end of AND chain. It is likely that these conditions are taken into account during primary key analysis and thus will not contribute a lot to PREWHERE filtering.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## multiple_joins_rewriter_version

<DeprecatedBadge />

Obsolete setting, does nothing.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## multiple_joins_try_to_keep_original_names

Do not add aliases to top level expression list on multiple joins rewrite

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## mysql_max_rows_to_insert

The maximum number of rows in MySQL batch insertion of the MySQL storage engine

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>65536</code></td></tr>
</table>


## normalize_function_names

Normalize function names to their canonical names

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## number_of_mutations_to_delay

If the mutated table contains at least that many unfinished mutations, artificially slow down mutations of table. 0 - disabled

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## number_of_mutations_to_throw

If the mutated table contains at least that many unfinished mutations, throw &#039;Too many mutations ...&#039; exception. 0 - disabled

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## odbc_max_field_size

<DeprecatedBadge />

Obsolete setting, does nothing.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## opentelemetry_trace_processors

Collect OpenTelemetry spans for processors.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## optimize_aggregators_of_group_by_keys

Eliminates min/max/any/anyLast aggregators of GROUP BY keys in SELECT section

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## optimize_arithmetic_operations_in_aggregate_functions

Move arithmetic operations out of aggregation functions

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## optimize_distinct_in_order

Enable DISTINCT optimization if some columns in DISTINCT form a prefix of sorting. For example, prefix of sorting key in merge tree or ORDER BY statement

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## optimize_duplicate_order_by_and_distinct

<DeprecatedBadge />

Obsolete setting, does nothing.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## optimize_fuse_sum_count_avg

<DeprecatedBadge />

Obsolete setting, does nothing.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## optimize_group_by_constant_keys

Optimize GROUP BY when all keys in block are constant

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## optimize_group_by_function_keys

Eliminates functions of other keys in GROUP BY section

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## optimize_if_chain_to_multiif

Replace if(cond1, then1, if(cond2, ...)) chains to multiIf. Currently it&#039;s not beneficial for numeric types.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## optimize_if_transform_strings_to_enum

Replaces string-type arguments in If and Transform to enum. Disabled by default cause it could make inconsistent change in distributed query that would lead to its fail.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## optimize_injective_functions_in_group_by

Replaces injective functions by it&#039;s arguments in GROUP BY section

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## optimize_injective_functions_inside_uniq

Delete injective functions of one argument inside uniq*() functions.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## optimize_min_equality_disjunction_chain_length

The minimum length of the expression `expr = x1 OR ... expr = xN` for optimization

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>3</code></td></tr>
</table>


## optimize_min_inequality_conjunction_chain_length

The minimum length of the expression `expr &lt;&gt; x1 AND ... expr &lt;&gt; xN` for optimization

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>3</code></td></tr>
</table>


## optimize_monotonous_functions_in_order_by

Replace monotonous function with its argument in ORDER BY

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## optimize_move_functions_out_of_any

<DeprecatedBadge />

Obsolete setting, does nothing.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## optimize_multiif_to_if

Replace &#039;multiIf&#039; with only one condition to &#039;if&#039;.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## optimize_normalize_count_variants

Rewrite aggregate functions that semantically equals to count() as count().

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## optimize_or_like_chain

Optimize multiple OR LIKE into multiMatchAny. This optimization should not be enabled by default, because it defies index analysis in some cases.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## optimize_read_in_window_order

Enable ORDER BY optimization in window clause for reading data in corresponding order in MergeTree tables.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## optimize_redundant_functions_in_order_by

Remove functions from ORDER BY if its argument is also in ORDER BY

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## optimize_respect_aliases

If it is set to true, it will respect aliases in WHERE/GROUP BY/ORDER BY, that will help with partition pruning/secondary indexes/optimize_aggregation_in_order/optimize_read_in_order/optimize_trivial_count

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## optimize_rewrite_array_exists_to_has

Rewrite arrayExists() functions to has() when logically equivalent. For example, arrayExists(x -&gt; x = 1, arr) can be rewritten to has(arr, 1)

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## optimize_rewrite_sum_if_to_count_if

Rewrite sumIf() and sum(if()) function countIf() function when logically equivalent

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## optimize_sorting_by_input_stream_properties

Optimize sorting by sorting properties of input stream

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## optimize_time_filter_with_preimage

Optimize Date and DateTime predicates by converting functions into equivalent comparisons without conversions (e.g. toYear(col) = 2023 -&gt; col &gt;= &#039;2023-01-01&#039; AND col &lt;= &#039;2023-12-31&#039;)

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## optimize_trivial_insert_select

Optimize trivial &#039;INSERT INTO table SELECT ... FROM TABLES&#039; query

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## optimize_uniq_to_count

Rewrite uniq and its variants(except uniqUpTo) to count if subquery has distinct or group by clause.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## optimize_use_implicit_projections

Automatically choose implicit projections to perform SELECT query

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## output_format_arrow_compression_method

Compression method for Arrow output format. Supported codecs: lz4_frame, zstd, none (uncompressed)

<table>
  <tr><td>Type</td><td><code>ArrowCompression</code></td></tr>
  <tr><td>Default</td><td><code>lz4_frame</code></td></tr>
</table>


## output_format_arrow_fixed_string_as_fixed_byte_array

Use Arrow FIXED_SIZE_BINARY type instead of Binary for FixedString columns.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## output_format_arrow_low_cardinality_as_dictionary

Enable output LowCardinality type as Dictionary Arrow type

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## output_format_arrow_string_as_string

Use Arrow String type instead of Binary for String columns

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## output_format_arrow_use_64_bit_indexes_for_dictionary

Always use 64 bit integers for dictionary indexes in Arrow format

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## output_format_arrow_use_signed_indexes_for_dictionary

Use signed integers for dictionary indexes in Arrow format

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## output_format_avro_codec

Compression codec used for output. Possible values: &#039;null&#039;, &#039;deflate&#039;, &#039;snappy&#039;, &#039;zstd&#039;.

<table>
  <tr><td>Type</td><td><code>String</code></td></tr>
</table>


## output_format_avro_rows_in_file

Max rows in a file (if permitted by storage)

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## output_format_avro_string_column_pattern

For Avro format: regexp of String columns to select as AVRO string.

<table>
  <tr><td>Type</td><td><code>String</code></td></tr>
</table>


## output_format_avro_sync_interval

Sync interval in bytes.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>16384</code></td></tr>
</table>


## output_format_bson_string_as_string

Use BSON String type instead of Binary for String columns.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## output_format_csv_crlf_end_of_line

If it is set true, end of line in CSV format will be \r\n instead of \n.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## output_format_decimal_trailing_zeros

Output trailing zeros when printing Decimal values. E.g. 1.230000 instead of 1.23.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## output_format_enable_streaming

Enable streaming in output formats that support it.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## output_format_json_array_of_rows

Output a JSON array of all rows in JSONEachRow(Compact) format.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## output_format_json_escape_forward_slashes

Controls escaping forward slashes for string outputs in JSON output format. This is intended for compatibility with JavaScript. Don&#039;t confuse with backslashes that are always escaped.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## output_format_json_named_tuples_as_objects

Serialize named tuple columns as JSON objects.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## output_format_json_quote_64bit_floats

Controls quoting of 64-bit float numbers in JSON output format.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## output_format_json_quote_64bit_integers

Controls quoting of 64-bit integers in JSON output format.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## output_format_json_quote_decimals

Controls quoting of decimals in JSON output format.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## output_format_json_quote_denormals

Enables &#039;+nan&#039;, &#039;-nan&#039;, &#039;+inf&#039;, &#039;-inf&#039; outputs in JSON output format.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## output_format_json_skip_null_value_in_named_tuples

Skip key value pairs with null value when serialize named tuple columns as JSON objects. It is only valid when output_format_json_named_tuples_as_objects is true.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## output_format_json_validate_utf8

Validate UTF-8 sequences in JSON output formats, doesn&#039;t impact formats JSON/JSONCompact/JSONColumnsWithMetadata, they always validate utf8

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## output_format_markdown_escape_special_characters

Escape special characters in Markdown

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## output_format_msgpack_uuid_representation

The way how to output UUID in MsgPack format.

<table>
  <tr><td>Type</td><td><code>MsgPackUUIDRepresentation</code></td></tr>
  <tr><td>Default</td><td><code>ext</code></td></tr>
</table>


## output_format_orc_compression_method

Compression method for ORC output format. Supported codecs: lz4, snappy, zlib, zstd, none (uncompressed)

<table>
  <tr><td>Type</td><td><code>ORCCompression</code></td></tr>
  <tr><td>Default</td><td><code>zstd</code></td></tr>
</table>


## output_format_orc_row_index_stride

Target row index stride in ORC output format

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>10000</code></td></tr>
</table>


## output_format_orc_string_as_string

Use ORC String type instead of Binary for String columns

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## output_format_parquet_batch_size

Check page size every this many rows. Consider decreasing if you have columns with average values size above a few KBs.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>1024</code></td></tr>
</table>


## output_format_parquet_compliant_nested_types

In parquet file schema, use name &#039;element&#039; instead of &#039;item&#039; for list elements. This is a historical artifact of Arrow library implementation. Generally increases compatibility, except perhaps with some old versions of Arrow.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## output_format_parquet_compression_method

Compression method for Parquet output format. Supported codecs: snappy, lz4, brotli, zstd, gzip, none (uncompressed)

<table>
  <tr><td>Type</td><td><code>ParquetCompression</code></td></tr>
  <tr><td>Default</td><td><code>zstd</code></td></tr>
</table>


## output_format_parquet_data_page_size

Target page size in bytes, before compression.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>1048576</code></td></tr>
</table>


## output_format_parquet_fixed_string_as_fixed_byte_array

Use Parquet FIXED_LENGTH_BYTE_ARRAY type instead of Binary for FixedString columns.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## output_format_parquet_parallel_encoding

Do Parquet encoding in multiple threads. Requires output_format_parquet_use_custom_encoder.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## output_format_parquet_row_group_size

Target row group size in rows.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>1000000</code></td></tr>
</table>


## output_format_parquet_row_group_size_bytes

Target row group size in bytes, before compression.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>536870912</code></td></tr>
</table>


## output_format_parquet_string_as_string

Use Parquet String type instead of Binary for String columns.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## output_format_parquet_use_custom_encoder

Use a faster Parquet encoder implementation.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## output_format_parquet_version

Parquet format version for output format. Supported versions: 1.0, 2.4, 2.6 and 2.latest (default)

<table>
  <tr><td>Type</td><td><code>ParquetVersion</code></td></tr>
  <tr><td>Default</td><td><code>2.latest</code></td></tr>
</table>


## output_format_pretty_color

Use ANSI escape sequences in Pretty formats. 0 - disabled, 1 - enabled, &#039;auto&#039; - enabled if a terminal.

<table>
  <tr><td>Type</td><td><code>UInt64Auto</code></td></tr>
  <tr><td>Default</td><td><code>auto</code></td></tr>
</table>


## output_format_pretty_grid_charset

Charset for printing grid borders. Available charsets: ASCII, UTF-8 (default one).

<table>
  <tr><td>Type</td><td><code>String</code></td></tr>
  <tr><td>Default</td><td><code>UTF-8</code></td></tr>
</table>


## output_format_pretty_highlight_digit_groups

If enabled and if output is a terminal, highlight every digit corresponding to the number of thousands, millions, etc. with underline.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## output_format_pretty_max_column_pad_width

Maximum width to pad all values in a column in Pretty formats.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>250</code></td></tr>
</table>


## output_format_pretty_max_rows

Rows limit for Pretty formats.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>10000</code></td></tr>
</table>


## output_format_pretty_max_value_width

Maximum width of value to display in Pretty formats. If greater - it will be cut.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>10000</code></td></tr>
</table>


## output_format_pretty_max_value_width_apply_for_single_value

Only cut values (see the `output_format_pretty_max_value_width` setting) when it is not a single value in a block. Otherwise output it entirely, which is useful for the `SHOW CREATE TABLE` query.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## output_format_pretty_row_numbers

Add row numbers before each row for pretty output format

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## output_format_pretty_single_large_number_tip_threshold

Print a readable number tip on the right side of the table if the block consists of a single number which exceeds this value (except 0)

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>1000000</code></td></tr>
</table>


## output_format_protobuf_nullables_with_google_wrappers

When serializing Nullable columns with Google wrappers, serialize default values as empty wrappers. If turned off, default and null values are not serialized

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## output_format_schema

The path to the file where the automatically generated schema will be saved

<table>
  <tr><td>Type</td><td><code>String</code></td></tr>
</table>


## output_format_sql_insert_include_column_names

Include column names in INSERT query

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## output_format_sql_insert_max_batch_size

The maximum number  of rows in one INSERT statement.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>65409</code></td></tr>
</table>


## output_format_sql_insert_quote_names

Quote column names with &#039;`&#039; characters

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## output_format_sql_insert_table_name

The name of table in the output INSERT query

<table>
  <tr><td>Type</td><td><code>String</code></td></tr>
  <tr><td>Default</td><td><code>table</code></td></tr>
</table>


## output_format_sql_insert_use_replace

Use REPLACE statement instead of INSERT

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## output_format_tsv_crlf_end_of_line

If it is set true, end of line in TSV format will be \r\n instead of \n.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## output_format_values_escape_quote_with_quote

If true escape &#039; with &#039;&#039;, otherwise quoted with \&#039;

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## output_format_write_statistics

Write statistics about read rows, bytes, time elapsed in suitable output formats.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## page_cache_inject_eviction

Userspace page cache will sometimes invalidate some pages at random. Intended for testing.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## parallel_replica_offset

This is internal setting that should not be used directly and represents an implementation detail of the &#039;parallel replicas&#039; mode. This setting will be automatically set up by the initiator server for distributed queries to the index of the replica participating in query processing among parallel replicas.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## parallel_replicas_allow_in_with_subquery

If true, subquery for IN will be executed on every follower replica.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## parallel_replicas_count

This is internal setting that should not be used directly and represents an implementation detail of the &#039;parallel replicas&#039; mode. This setting will be automatically set up by the initiator server for distributed queries to the number of parallel replicas participating in query processing.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## parallel_replicas_for_non_replicated_merge_tree

If true, ClickHouse will use parallel replicas algorithm also for non-replicated MergeTree tables

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## parallel_replicas_mark_segment_size

Parts virtually divided into segments to be distributed between replicas for parallel reading. This setting controls the size of these segments. Not recommended to change until you&#039;re absolutely sure in what you&#039;re doing

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>128</code></td></tr>
</table>


## parallel_replicas_min_number_of_granules_to_enable

<DeprecatedBadge />

Obsolete setting, does nothing.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## parallel_replicas_min_number_of_rows_per_replica

Limit the number of replicas used in a query to (estimated rows to read / min_number_of_rows_per_replica). The max is still limited by &#039;max_parallel_replicas&#039;

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## parallel_replicas_prefer_local_join

If true, and JOIN can be executed with parallel replicas algorithm, and all storages of right JOIN part are *MergeTree, local JOIN will be used instead of GLOBAL JOIN.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## parallel_replicas_single_task_marks_count_multiplier

A multiplier which will be added during calculation for minimal number of marks to retrieve from coordinator. This will be applied only for remote replicas.

<table>
  <tr><td>Type</td><td><code>Float</code></td></tr>
  <tr><td>Default</td><td><code>2</code></td></tr>
</table>


## parallelize_output_from_storages

Parallelize output for reading step from storage. It allows parallelization of  query processing right after reading from storage if possible

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## parsedatetime_parse_without_leading_zeros

Formatters &#039;%c&#039;, &#039;%l&#039; and &#039;%k&#039; in function &#039;parseDateTime()&#039; parse months and hours without leading zeros.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## partial_merge_join_left_table_buffer_bytes

If not 0 group left table blocks in bigger ones for left-side table in partial merge join. It uses up to 2x of specified memory per joining thread.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## parts_to_delay_insert

If the destination table contains at least that many active parts in a single partition, artificially slow down insert into table.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## parts_to_throw_insert

If more than this number active parts in a single partition of the destination table, throw &#039;Too many parts ...&#039; exception.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## prefer_warmed_unmerged_parts_seconds

Only available in ClickHouse Cloud. If a merged part is less than this many seconds old and is not pre-warmed (see cache_populated_by_fetch), but all its source parts are available and pre-warmed, SELECT queries will read from those parts instead. Only for ReplicatedMergeTree. Note that this only checks whether CacheWarmer processed the part; if the part was fetched into cache by something else, it&#039;ll still be considered cold until CacheWarmer gets to it; if it was warmed, then evicted from cache, it&#039;ll still be considered warm.

<table>
  <tr><td>Type</td><td><code>Int64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## preferred_max_column_in_block_size_bytes

Limit on max column size in block while reading. Helps to decrease cache misses count. Should be close to L2 cache size.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## prefetch_buffer_size

The maximum size of the prefetch buffer to read from the filesystem.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>1048576</code></td></tr>
</table>


## priority

Priority of the query. 1 - the highest, higher value - lower priority; 0 - do not use priorities.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## query_cache_store_results_of_queries_with_nondeterministic_functions

<DeprecatedBadge />

Obsolete setting, does nothing.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## query_plan_enable_multithreading_after_window_functions

Enable multithreading after evaluating window functions to allow parallel stream processing

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## query_plan_optimize_prewhere

Allow to push down filter to PREWHERE expression for supported storages

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## query_plan_optimize_primary_key

<DeprecatedBadge />

Obsolete setting, does nothing.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## query_plan_optimize_projection

<DeprecatedBadge />

Obsolete setting, does nothing.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## queue_max_wait_ms

The wait time in the request queue, if the number of concurrent requests exceeds the maximum.

<table>
  <tr><td>Type</td><td><code>Milliseconds</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## rabbitmq_max_wait_ms

The wait time for reading from RabbitMQ before retry.

<table>
  <tr><td>Type</td><td><code>Milliseconds</code></td></tr>
  <tr><td>Default</td><td><code>5000</code></td></tr>
</table>


## read_backoff_max_throughput

Settings to reduce the number of threads in case of slow reads. Count events when the read bandwidth is less than that many bytes per second.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>1048576</code></td></tr>
</table>


## read_backoff_min_concurrency

Settings to try keeping the minimal number of threads in case of slow reads.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## read_backoff_min_events

Settings to reduce the number of threads in case of slow reads. The number of events after which the number of threads will be reduced.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>2</code></td></tr>
</table>


## read_backoff_min_interval_between_events_ms

Settings to reduce the number of threads in case of slow reads. Do not pay attention to the event, if the previous one has passed less than a certain amount of time.

<table>
  <tr><td>Type</td><td><code>Milliseconds</code></td></tr>
  <tr><td>Default</td><td><code>1000</code></td></tr>
</table>


## read_backoff_min_latency_ms

Setting to reduce the number of threads in case of slow reads. Pay attention only to reads that took at least that much time.

<table>
  <tr><td>Type</td><td><code>Milliseconds</code></td></tr>
  <tr><td>Default</td><td><code>1000</code></td></tr>
</table>


## read_from_filesystem_cache_if_exists_otherwise_bypass_cache

Allow to use the filesystem cache in passive mode - benefit from the existing cache entries, but don&#039;t put more entries into the cache. If you set this setting for heavy ad-hoc queries and leave it disabled for short real-time queries, this will allows to avoid cache threshing by too heavy queries and to improve the overall system efficiency.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## read_from_page_cache_if_exists_otherwise_bypass_cache

Use userspace page cache in passive mode, similar to read_from_filesystem_cache_if_exists_otherwise_bypass_cache.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## read_in_order_two_level_merge_threshold

Minimal number of parts to read to run preliminary merge step during multithread reading in order of primary key.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>100</code></td></tr>
</table>


## read_overflow_mode

What to do when the limit is exceeded.

<table>
  <tr><td>Type</td><td><code>OverflowMode</code></td></tr>
  <tr><td>Default</td><td><code>throw</code></td></tr>
</table>


## read_overflow_mode_leaf

What to do when the leaf limit is exceeded.

<table>
  <tr><td>Type</td><td><code>OverflowMode</code></td></tr>
  <tr><td>Default</td><td><code>throw</code></td></tr>
</table>


## read_priority

Priority to read data from local filesystem or remote filesystem. Only supported for &#039;pread_threadpool&#039; method for local filesystem and for `threadpool` method for remote filesystem.

<table>
  <tr><td>Type</td><td><code>Int64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## read_through_distributed_cache

Allow reading from distributed cache

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## readonly

0 - no read-only restrictions. 1 - only read requests, as well as changing explicitly allowed settings. 2 - only read requests, as well as changing settings, except for the &#039;readonly&#039; setting.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## receive_data_timeout_ms

Connection timeout for receiving first packet of data or packet with positive progress from replica

<table>
  <tr><td>Type</td><td><code>Milliseconds</code></td></tr>
  <tr><td>Default</td><td><code>2000</code></td></tr>
</table>


## receive_timeout

Timeout for receiving data from network, in seconds. If no bytes were received in this interval, exception is thrown. If you set this setting on client, the &#039;send_timeout&#039; for the socket will be also set on the corresponding connection end on the server.

<table>
  <tr><td>Type</td><td><code>Seconds</code></td></tr>
  <tr><td>Default</td><td><code>300</code></td></tr>
</table>


## regexp_dict_allow_hyperscan

Allow regexp_tree dictionary using Hyperscan library.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## regexp_dict_flag_case_insensitive

Use case-insensitive matching for a regexp_tree dictionary. Can be overridden in individual expressions with (?i) and (?-i).

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## regexp_dict_flag_dotall

Allow &#039;.&#039; to match newline characters for a regexp_tree dictionary.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## reject_expensive_hyperscan_regexps

Reject patterns which will likely be expensive to evaluate with hyperscan (due to NFA state explosion)

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## remerge_sort_lowered_memory_bytes_ratio

If memory usage after remerge does not reduced by this ratio, remerge will be disabled.

<table>
  <tr><td>Type</td><td><code>Float</code></td></tr>
  <tr><td>Default</td><td><code>2</code></td></tr>
</table>


## remote_filesystem_read_method

Method of reading data from remote filesystem, one of: read, threadpool.

<table>
  <tr><td>Type</td><td><code>String</code></td></tr>
  <tr><td>Default</td><td><code>threadpool</code></td></tr>
</table>


## remote_filesystem_read_prefetch

Should use prefetching when reading data from remote filesystem.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## remote_fs_read_backoff_max_tries

Max attempts to read with backoff

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>5</code></td></tr>
</table>


## remote_fs_read_max_backoff_ms

Max wait time when trying to read data for remote disk

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>10000</code></td></tr>
</table>


## remote_read_min_bytes_for_seek

Min bytes required for remote read (url, s3) to do seek, instead of read with ignore.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>4194304</code></td></tr>
</table>


## replication_alter_columns_timeout

<DeprecatedBadge />

Obsolete setting, does nothing.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>60</code></td></tr>
</table>


## replication_alter_partitions_sync

Wait for actions to manipulate the partitions. 0 - do not wait, 1 - wait for execution only of itself, 2 - wait for everyone.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
  <tr><td>Alias for</td><td><code>alter_sync</code></td></tr>
</table>


## restore_threads

<DeprecatedBadge />

Obsolete setting, does nothing.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>16</code></td></tr>
</table>


## result_overflow_mode

What to do when the limit is exceeded.

<table>
  <tr><td>Type</td><td><code>OverflowMode</code></td></tr>
  <tr><td>Default</td><td><code>throw</code></td></tr>
</table>


## s3_allow_parallel_part_upload

Use multiple threads for s3 multipart upload. It may lead to slightly higher memory usage

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## s3_check_objects_after_upload

Check each uploaded object to s3 with head request to be sure that upload was successful

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## s3_connect_timeout_ms

Connection timeout for host from s3 disks.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>1000</code></td></tr>
</table>


## s3_disable_checksum

Do not calculate a checksum when sending a file to S3. This speeds up writes by avoiding excessive processing passes on a file. It is mostly safe as the data of MergeTree tables is checksummed by ClickHouse anyway, and when S3 is accessed with HTTPS, the TLS layer already provides integrity while transferring through the network. While additional checksums on S3 give defense in depth.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## s3_list_object_keys_size

Maximum number of files that could be returned in batch by ListObject request

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>1000</code></td></tr>
</table>


## s3_max_connections

The maximum number of connections per server.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>1024</code></td></tr>
</table>


## s3_max_get_burst

Max number of requests that can be issued simultaneously before hitting request per second limit. By default (0) equals to `s3_max_get_rps`

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## s3_max_get_rps

Limit on S3 GET request per second rate before throttling. Zero means unlimited.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## s3_max_inflight_parts_for_one_file

The maximum number of a concurrent loaded parts in multipart upload request. 0 means unlimited. You

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>20</code></td></tr>
</table>


## s3_max_put_burst

Max number of requests that can be issued simultaneously before hitting request per second limit. By default (0) equals to `s3_max_put_rps`

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## s3_max_put_rps

Limit on S3 PUT request per second rate before throttling. Zero means unlimited.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## s3_max_redirects

Max number of S3 redirects hops allowed.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>10</code></td></tr>
</table>


## s3_max_single_part_upload_size

The maximum size of object to upload using singlepart upload to S3.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>33554432</code></td></tr>
</table>


## s3_max_single_read_retries

The maximum number of retries during single S3 read.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>4</code></td></tr>
</table>


## s3_max_unexpected_write_error_retries

The maximum number of retries in case of unexpected errors during S3 write.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>4</code></td></tr>
</table>


## s3_max_upload_part_size

The maximum size of part to upload during multipart upload to S3.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>5368709120</code></td></tr>
</table>


## s3_min_upload_part_size

The minimum size of part to upload during multipart upload to S3.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>16777216</code></td></tr>
</table>


## s3_request_timeout_ms

Idleness timeout for sending and receiving data to/from S3. Fail if a single TCP read or write call blocks for this long.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>30000</code></td></tr>
</table>


## s3_retry_attempts

Setting for Aws::Client::RetryStrategy, Aws::Client does retries itself, 0 means no retries

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>100</code></td></tr>
</table>


## s3_strict_upload_part_size

The exact size of part to upload during multipart upload to S3 (some implementations does not supports variable size parts).

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## s3_throw_on_zero_files_match

Throw an error, when ListObjects request cannot match any files

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## s3_upload_part_size_multiply_factor

Multiply s3_min_upload_part_size by this factor each time s3_multiply_parts_count_threshold parts were uploaded from a single write to S3.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>2</code></td></tr>
</table>


## s3_upload_part_size_multiply_parts_count_threshold

Each time this number of parts was uploaded to S3, s3_min_upload_part_size is multiplied by s3_upload_part_size_multiply_factor.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>500</code></td></tr>
</table>


## s3queue_allow_experimental_sharded_mode

Enable experimental sharded mode of S3Queue table engine. It is experimental because it will be rewritten

<table>
  <tr><td>Readonly</td><td><code>1</code></td></tr>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## s3queue_default_zookeeper_path

Default zookeeper path prefix for S3Queue engine

<table>
  <tr><td>Type</td><td><code>String</code></td></tr>
  <tr><td>Default</td><td><code>/clickhouse/s3queue/</code></td></tr>
</table>


## s3queue_enable_logging_to_s3queue_log

Enable writing to system.s3queue_log. The value can be overwritten per table with table settings

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## schema_inference_hints

The list of column names and types to use in schema inference for formats without column names. The format: &#039;column_name1 column_type1, column_name2 column_type2, ...&#039;

<table>
  <tr><td>Type</td><td><code>String</code></td></tr>
</table>


## schema_inference_make_columns_nullable

If set to true, all inferred types will be Nullable in schema inference for formats without information about nullability.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## schema_inference_use_cache_for_azure

Use cache in schema inference while using azure table function

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## send_logs_level

Send server text logs with specified minimum level to client. Valid values: &#039;trace&#039;, &#039;debug&#039;, &#039;information&#039;, &#039;warning&#039;, &#039;error&#039;, &#039;fatal&#039;, &#039;none&#039;

<table>
  <tr><td>Type</td><td><code>LogsLevel</code></td></tr>
  <tr><td>Default</td><td><code>fatal</code></td></tr>
</table>


## send_logs_source_regexp

Send server text logs with specified regexp to match log source name. Empty means all sources.

<table>
  <tr><td>Type</td><td><code>String</code></td></tr>
</table>


## send_timeout

Timeout for sending data to network, in seconds. If client needs to sent some data, but it did not able to send any bytes in this interval, exception is thrown. If you set this setting on client, the &#039;receive_timeout&#039; for the socket will be also set on the corresponding connection end on the server.

<table>
  <tr><td>Type</td><td><code>Seconds</code></td></tr>
  <tr><td>Default</td><td><code>300</code></td></tr>
</table>


## set_overflow_mode

What to do when the limit is exceeded.

<table>
  <tr><td>Type</td><td><code>OverflowMode</code></td></tr>
  <tr><td>Default</td><td><code>throw</code></td></tr>
</table>


## single_join_prefer_left_table

For single JOIN in case of identifier ambiguity prefer left table

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## skip_download_if_exceeds_query_cache

Skip download from remote filesystem if exceeds query cache size

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## sleep_after_receiving_query_ms

Time to sleep after receiving query in TCPHandler

<table>
  <tr><td>Type</td><td><code>Milliseconds</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## sleep_in_send_data_ms

Time to sleep in sending data in TCPHandler

<table>
  <tr><td>Type</td><td><code>Milliseconds</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## sleep_in_send_tables_status_ms

Time to sleep in sending tables status response in TCPHandler

<table>
  <tr><td>Type</td><td><code>Milliseconds</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## sort_overflow_mode

What to do when the limit is exceeded.

<table>
  <tr><td>Type</td><td><code>OverflowMode</code></td></tr>
  <tr><td>Default</td><td><code>throw</code></td></tr>
</table>


## split_intersecting_parts_ranges_into_layers_final

Split intersecting parts ranges into layers during FINAL optimization

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## split_parts_ranges_into_intersecting_and_non_intersecting_final

Split parts ranges into intersecting and non intersecting during FINAL optimization

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## stop_refreshable_materialized_views_on_startup

On server startup, prevent scheduling of refreshable materialized views, as if with SYSTEM STOP VIEWS. You can manually start them with SYSTEM START VIEWS or SYSTEM START VIEW &lt;name&gt; afterwards. Also applies to newly created views. Has no effect on non-refreshable materialized views.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## storage_system_stack_trace_pipe_read_timeout_ms

Maximum time to read from a pipe for receiving information from the threads when querying the `system.stack_trace` table. This setting is used for testing purposes and not meant to be changed by users.

<table>
  <tr><td>Type</td><td><code>Milliseconds</code></td></tr>
  <tr><td>Default</td><td><code>100</code></td></tr>
</table>


## stream_like_engine_allow_direct_select

Allow direct SELECT query for Kafka, RabbitMQ, FileLog, Redis Streams, and NATS engines. In case there are attached materialized views, SELECT query is not allowed even if this setting is enabled.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## stream_like_engine_insert_queue

When stream like engine reads from multiple queues, user will need to select one queue to insert into when writing. Used by Redis Streams and NATS.

<table>
  <tr><td>Type</td><td><code>String</code></td></tr>
</table>


## tcp_keep_alive_timeout

The time in seconds the connection needs to remain idle before TCP starts sending keepalive probes

<table>
  <tr><td>Type</td><td><code>Seconds</code></td></tr>
  <tr><td>Default</td><td><code>290</code></td></tr>
</table>


## temporary_data_in_cache_reserve_space_wait_lock_timeout_milliseconds

Wait time to lock cache for sapce reservation for temporary data in filesystem cache

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>600000</code></td></tr>
</table>


## temporary_live_view_timeout

<DeprecatedBadge />

Obsolete setting, does nothing.

<table>
  <tr><td>Type</td><td><code>Seconds</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## throw_if_deduplication_in_dependent_materialized_views_enabled_with_async_insert

Throw exception on INSERT query when the setting `deduplicate_blocks_in_dependent_materialized_views` is enabled along with `async_insert`. It guarantees correctness, because these features can&#039;t work together.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## throw_if_no_data_to_insert

Allows or forbids empty INSERTs, enabled by default (throws an error on an empty insert)

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## throw_on_error_from_cache_on_write_operations

Ignore error from cache when caching on write operations (INSERT, merges)

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## throw_on_max_partitions_per_insert_block

Used with max_partitions_per_insert_block. If true (default), an exception will be thrown when max_partitions_per_insert_block is reached. If false, details of the insert query reaching this limit with the number of partitions will be logged. This can be useful if you&#039;re trying to understand the impact on users when changing max_partitions_per_insert_block.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## throw_on_unsupported_query_inside_transaction

Throw exception if unsupported query is used inside transaction

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## timeout_before_checking_execution_speed

Check that the speed is not too low after the specified time has elapsed.

<table>
  <tr><td>Type</td><td><code>Seconds</code></td></tr>
  <tr><td>Default</td><td><code>10</code></td></tr>
</table>


## timeout_overflow_mode

What to do when the limit is exceeded.

<table>
  <tr><td>Type</td><td><code>OverflowMode</code></td></tr>
  <tr><td>Default</td><td><code>throw</code></td></tr>
</table>


## timeout_overflow_mode_leaf

What to do when the leaf limit is exceeded.

<table>
  <tr><td>Type</td><td><code>OverflowMode</code></td></tr>
  <tr><td>Default</td><td><code>throw</code></td></tr>
</table>


## transfer_overflow_mode

What to do when the limit is exceeded.

<table>
  <tr><td>Type</td><td><code>OverflowMode</code></td></tr>
  <tr><td>Default</td><td><code>throw</code></td></tr>
</table>


## traverse_shadow_remote_data_paths

Traverse shadow directory when query system.remote_data_paths

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## unknown_packet_in_send_data

Send unknown packet instead of data Nth data packet

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## use_client_time_zone

Use client timezone for interpreting DateTime string values, instead of adopting server timezone.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## use_concurrency_control

Respect the server&#039;s concurrency control (see the `concurrent_threads_soft_limit_num` and `concurrent_threads_soft_limit_ratio_to_cores` global server settings). If disabled, it allows using a larger number of threads even if the server is overloaded (not recommended for normal usage, and needed mostly for tests).

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## use_index_for_in_with_subqueries

Try using an index if there is a subquery or a table expression on the right side of the IN operator.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## use_index_for_in_with_subqueries_max_values

The maximum size of set in the right hand side of the IN operator to use table index for filtering. It allows to avoid performance degradation and higher memory usage due to preparation of additional data structures for large queries. Zero means no limit.

<table>
  <tr><td>Type</td><td><code>UInt64</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## use_local_cache_for_remote_storage

Use local cache for remote storage like HDFS or S3, it&#039;s used for remote table engine only

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## use_mysql_types_in_show_columns

<DeprecatedBadge />

Obsolete setting, does nothing.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## use_page_cache_for_disks_without_file_cache

Use userspace page cache for remote disks that don&#039;t have filesystem cache enabled.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## use_skip_indexes_if_final

If query has FINAL, then skipping data based on indexes may produce incorrect result, hence disabled by default.

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>


## use_with_fill_by_sorting_prefix

Columns preceding WITH FILL columns in ORDER BY clause form sorting prefix. Rows with different values in sorting prefix are filled independently

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## validate_experimental_and_suspicious_types_inside_nested_types

Validate usage of experimental and suspicious types inside nested types like Array/Map/Tuple

<table>
  <tr><td>Readonly</td><td><code>1</code></td></tr>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>1</code></td></tr>
</table>


## wait_changes_become_visible_after_commit_mode

Wait for committed changes to become actually visible in the latest snapshot

<table>
  <tr><td>Type</td><td><code>TransactionsWaitCSNMode</code></td></tr>
  <tr><td>Default</td><td><code>wait_unknown</code></td></tr>
</table>


## wait_for_window_view_fire_signal_timeout

Timeout for waiting for window view fire signal in event time processing

<table>
  <tr><td>Type</td><td><code>Seconds</code></td></tr>
  <tr><td>Default</td><td><code>10</code></td></tr>
</table>


## window_view_clean_interval

The clean interval of window view in seconds to free outdated data.

<table>
  <tr><td>Type</td><td><code>Seconds</code></td></tr>
  <tr><td>Default</td><td><code>60</code></td></tr>
</table>


## window_view_heartbeat_interval

The heartbeat interval in seconds to indicate watch query is alive.

<table>
  <tr><td>Type</td><td><code>Seconds</code></td></tr>
  <tr><td>Default</td><td><code>15</code></td></tr>
</table>


## workload

Name of workload to be used to access resources

<table>
  <tr><td>Type</td><td><code>String</code></td></tr>
  <tr><td>Default</td><td><code>default</code></td></tr>
</table>


## write_through_distributed_cache

Allow writing to distributed cache (writing to s3 will also be done by distributed cache)

<table>
  <tr><td>Type</td><td><code>Bool</code></td></tr>
  <tr><td>Default</td><td><code>0</code></td></tr>
</table>

