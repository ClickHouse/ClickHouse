---
sidebar_position: 11
sidebar_label:  MergeTree
---

# MergeTree

The `MergeTree` engine and other engines of this family (`*MergeTree`) are the most robust ClickHouse table engines.

Engines in the `MergeTree` family are designed for inserting a very large amount of data into a table. The data is quickly written to the table part by part, then rules are applied for merging the parts in the background. This method is much more efficient than continually rewriting the data in storage during insert.

Main features:

-   Stores data sorted by primary key.

    This allows you to create a small sparse index that helps find data faster.

-   Partitions can be used if the [partitioning key](../../../engines/table-engines/mergetree-family/custom-partitioning-key.md) is specified.

    ClickHouse supports certain operations with partitions that are more efficient than general operations on the same data with the same result. ClickHouse also automatically cuts off the partition data where the partitioning key is specified in the query.

-   Data replication support.

    The family of `ReplicatedMergeTree` tables provides data replication. For more information, see [Data replication](../../../engines/table-engines/mergetree-family/replication.md).

-   Data sampling support.

    If necessary, you can set the data sampling method in the table.

:::info
The [Merge](../../../engines/table-engines/special/merge.md#merge) engine does not belong to the `*MergeTree` family.
:::

## Creating a Table {#table_engine-mergetree-creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [TTL expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2] [TTL expr2],
    ...
    INDEX index_name1 expr1 TYPE type1(...) GRANULARITY value1,
    INDEX index_name2 expr2 TYPE type2(...) GRANULARITY value2,
    ...
    PROJECTION projection_name_1 (SELECT <COLUMN LIST EXPR> [GROUP BY] [ORDER BY]),
    PROJECTION projection_name_2 (SELECT <COLUMN LIST EXPR> [GROUP BY] [ORDER BY])
) ENGINE = MergeTree()
ORDER BY expr
[PARTITION BY expr]
[PRIMARY KEY expr]
[SAMPLE BY expr]
[TTL expr
    [DELETE|TO DISK 'xxx'|TO VOLUME 'xxx' [, ...] ]
    [WHERE conditions]
    [GROUP BY key_expr [SET v1 = aggr_func(v1) [, v2 = aggr_func(v2) ...]] ] ]
[SETTINGS name=value, ...]
```

For a description of parameters, see the [CREATE query description](../../../sql-reference/statements/create/table.md).

### Query Clauses {#mergetree-query-clauses}

#### ENGINE

`ENGINE` — Name and parameters of the engine. `ENGINE = MergeTree()`. The `MergeTree` engine does not have parameters.

#### ORDER_BY

`ORDER BY` — The sorting key.

A tuple of column names or arbitrary expressions. Example: `ORDER BY (CounterID, EventDate)`.

ClickHouse uses the sorting key as a primary key if the primary key is not defined explicitly by the `PRIMARY KEY` clause.

Use the `ORDER BY tuple()` syntax, if you do not need sorting. See [Selecting the Primary Key](#selecting-the-primary-key).

#### PARTITION BY

`PARTITION BY` — The [partitioning key](../../../engines/table-engines/mergetree-family/custom-partitioning-key.md). Optional. In most cases you don't need partition key, and in most other cases you don't need partition key more granular than by months. Partitioning does not speed up queries (in contrast to the ORDER BY expression). You should never use too granular partitioning. Don't partition your data by client identifiers or names (instead make client identifier or name the first column in the ORDER BY expression).

For partitioning by month, use the `toYYYYMM(date_column)` expression, where `date_column` is a column with a date of the type [Date](../../../sql-reference/data-types/date.md). The partition names here have the `"YYYYMM"` format.

#### PRIMARY KEY

`PRIMARY KEY` — The primary key if it [differs from the sorting key](#choosing-a-primary-key-that-differs-from-the-sorting-key). Optional.

By default the primary key is the same as the sorting key (which is specified by the `ORDER BY` clause). Thus in most cases it is unnecessary to specify a separate `PRIMARY KEY` clause.

#### SAMPLE BY

`SAMPLE BY` — An expression for sampling. Optional.

If a sampling expression is used, the primary key must contain it. The result of a sampling expression must be an unsigned integer. Example: `SAMPLE BY intHash32(UserID) ORDER BY (CounterID, EventDate, intHash32(UserID))`.

####  TTL

`TTL` — A list of rules specifying storage duration of rows and defining logic of automatic parts movement [between disks and volumes](#table_engine-mergetree-multiple-volumes). Optional.

Expression must have one `Date` or `DateTime` column as a result. Example:
```
TTL date + INTERVAL 1 DAY
```

Type of the rule `DELETE|TO DISK 'xxx'|TO VOLUME 'xxx'|GROUP BY` specifies an action to be done with the part if the expression is satisfied (reaches current time): removal of expired rows, moving a part (if expression is satisfied for all rows in a part) to specified disk (`TO DISK 'xxx'`) or to volume (`TO VOLUME 'xxx'`), or aggregating values in expired rows. Default type of the rule is removal (`DELETE`). List of multiple rules can be specified, but there should be no more than one `DELETE` rule.

For more details, see [TTL for columns and tables](#table_engine-mergetree-ttl)

### SETTINGS
Additional parameters that control the behavior of the `MergeTree` (optional):

#### index_granularity

`index_granularity` — Maximum number of data rows between the marks of an index. Default value: 8192. See [Data Storage](#mergetree-data-storage).

#### index_granularity_bytes

`index_granularity_bytes` — Maximum size of data granules in bytes. Default value: 10Mb. To restrict the granule size only by number of rows, set to 0 (not recommended). See [Data Storage](#mergetree-data-storage).

#### min_index_granularity_bytes

`min_index_granularity_bytes` — Min allowed size of data granules in bytes. Default value: 1024b. To provide a safeguard against accidentally creating tables with very low index_granularity_bytes. See [Data Storage](#mergetree-data-storage).

#### enable_mixed_granularity_parts

`enable_mixed_granularity_parts` — Enables or disables transitioning to control the granule size with the `index_granularity_bytes` setting. Before version 19.11, there was only the `index_granularity` setting for restricting granule size. The `index_granularity_bytes` setting improves ClickHouse performance when selecting data from tables with big rows (tens and hundreds of megabytes). If you have tables with big rows, you can enable this setting for the tables to improve the efficiency of `SELECT` queries.

#### use_minimalistic_part_header_in_zookeeper

`use_minimalistic_part_header_in_zookeeper` — Storage method of the data parts headers in ZooKeeper. If `use_minimalistic_part_header_in_zookeeper=1`, then ZooKeeper stores less data. For more information, see the [setting description](../../../operations/server-configuration-parameters/settings.md#server-settings-use_minimalistic_part_header_in_zookeeper) in “Server configuration parameters”.

#### min_merge_bytes_to_use_direct_io

`min_merge_bytes_to_use_direct_io` — The minimum data volume for merge operation that is required for using direct I/O access to the storage disk. When merging data parts, ClickHouse calculates the total storage volume of all the data to be merged. If the volume exceeds `min_merge_bytes_to_use_direct_io` bytes, ClickHouse reads and writes the data to the storage disk using the direct I/O interface (`O_DIRECT` option). If `min_merge_bytes_to_use_direct_io = 0`, then direct I/O is disabled. Default value: `10 * 1024 * 1024 * 1024` bytes.

#### merge_with_ttl_timeout

`merge_with_ttl_timeout` — Minimum delay in seconds before repeating a merge with delete TTL. Default value: `14400` seconds (4 hours).
#### merge_with_recompression_ttl_timeout

`merge_with_recompression_ttl_timeout` — Minimum delay in seconds before repeating a merge with recompression TTL. Default value: `14400` seconds (4 hours).

#### try_fetch_recompressed_part_timeout

`try_fetch_recompressed_part_timeout` — Timeout (in seconds) before starting merge with recompression. During this time ClickHouse tries to fetch recompressed part from replica which assigned this merge with recompression. Default value: `7200` seconds (2 hours).

#### write_final_mark

`write_final_mark` — Enables or disables writing the final index mark at the end of data part (after the last byte). Default value: 1. Don’t turn it off.

#### merge_max_block_size

`merge_max_block_size` — Maximum number of rows in block for merge operations. Default value: 8192.

#### storage_policy

`storage_policy` — Storage policy. See [Using Multiple Block Devices for Data Storage](#table_engine-mergetree-multiple-volumes).

#### min_bytes_for_wide_part

`min_bytes_for_wide_part`, `min_rows_for_wide_part` — Minimum number of bytes/rows in a data part that can be stored in `Wide` format. You can set one, both or none of these settings. See [Data Storage](#mergetree-data-storage).

#### max_parts_in_total

`max_parts_in_total` — Maximum number of parts in all partitions.

#### max_compress_block_size

`max_compress_block_size` — Maximum size of blocks of uncompressed data before compressing for writing to a table. You can also specify this setting in the global settings (see [max_compress_block_size](../../../operations/settings/settings.md#max-compress-block-size) setting). The value specified when table is created overrides the global value for this setting.

#### min_compress_block_size

`min_compress_block_size` — Minimum size of blocks of uncompressed data required for compression when writing the next mark. You can also specify this setting in the global settings (see [min_compress_block_size](../../../operations/settings/settings.md#min-compress-block-size) setting). The value specified when table is created overrides the global value for this setting.

#### max_partitions_to_read

`max_partitions_to_read` — Limits the maximum number of partitions that can be accessed in one query. You can also specify setting [max_partitions_to_read](../../../operations/settings/merge-tree-settings.md#max-partitions-to-read) in the global setting.

**Example of Sections Setting**

``` sql
ENGINE MergeTree() PARTITION BY toYYYYMM(EventDate) ORDER BY (CounterID, EventDate, intHash32(UserID)) SAMPLE BY intHash32(UserID) SETTINGS index_granularity=8192
```

In the example, we set partitioning by month.

We also set an expression for sampling as a hash by the user ID. This allows you to pseudorandomize the data in the table for each `CounterID` and `EventDate`. If you define a [SAMPLE](../../../sql-reference/statements/select/sample.md#select-sample-clause) clause when selecting the data, ClickHouse will return an evenly pseudorandom data sample for a subset of users.

The `index_granularity` setting can be omitted because 8192 is the default value.

<details markdown="1">

<summary>Deprecated Method for Creating a Table</summary>

:::warning
Do not use this method in new projects. If possible, switch old projects to the method described above.
:::

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE [=] MergeTree(date-column [, sampling_expression], (primary, key), index_granularity)
```

**MergeTree() Parameters**

-   `date-column` — The name of a column of the [Date](../../../sql-reference/data-types/date.md) type. ClickHouse automatically creates partitions by month based on this column. The partition names are in the `"YYYYMM"` format.
-   `sampling_expression` — An expression for sampling.
-   `(primary, key)` — Primary key. Type: [Tuple()](../../../sql-reference/data-types/tuple.md)
-   `index_granularity` — The granularity of an index. The number of data rows between the “marks” of an index. The value 8192 is appropriate for most tasks.

**Example**

``` sql
MergeTree(EventDate, intHash32(UserID), (CounterID, EventDate, intHash32(UserID)), 8192)
```

The `MergeTree` engine is configured in the same way as in the example above for the main engine configuration method.
</details>

## Data Storage {#mergetree-data-storage}

A table consists of data parts sorted by primary key.

When data is inserted in a table, separate data parts are created and each of them is lexicographically sorted by primary key. For example, if the primary key is `(CounterID, Date)`, the data in the part is sorted by `CounterID`, and within each `CounterID`, it is ordered by `Date`.

Data belonging to different partitions are separated into different parts. In the background, ClickHouse merges data parts for more efficient storage. Parts belonging to different partitions are not merged. The merge mechanism does not guarantee that all rows with the same primary key will be in the same data part.

Data parts can be stored in `Wide` or `Compact` format. In `Wide` format each column is stored in a separate file in a filesystem, in `Compact` format all columns are stored in one file. `Compact` format can be used to increase performance of small and frequent inserts.

Data storing format is controlled by the `min_bytes_for_wide_part` and `min_rows_for_wide_part` settings of the table engine. If the number of bytes or rows in a data part is less then the corresponding setting's value, the part is stored in `Compact` format. Otherwise it is stored in `Wide` format. If none of these settings is set, data parts are stored in `Wide` format.

Each data part is logically divided into granules. A granule is the smallest indivisible data set that ClickHouse reads when selecting data. ClickHouse does not split rows or values, so each granule always contains an integer number of rows. The first row of a granule is marked with the value of the primary key for the row. For each data part, ClickHouse creates an index file that stores the marks. For each column, whether it’s in the primary key or not, ClickHouse also stores the same marks. These marks let you find data directly in column files.

The granule size is restricted by the `index_granularity` and `index_granularity_bytes` settings of the table engine. The number of rows in a granule lays in the `[1, index_granularity]` range, depending on the size of the rows. The size of a granule can exceed `index_granularity_bytes` if the size of a single row is greater than the value of the setting. In this case, the size of the granule equals the size of the row.

## Primary Keys and Indexes in Queries {#primary-keys-and-indexes-in-queries}

Take the `(CounterID, Date)` primary key as an example. In this case, the sorting and index can be illustrated as follows:

      Whole data:     [---------------------------------------------]
      CounterID:      [aaaaaaaaaaaaaaaaaabbbbcdeeeeeeeeeeeeefgggggggghhhhhhhhhiiiiiiiiikllllllll]
      Date:           [1111111222222233331233211111222222333211111112122222223111112223311122333]
      Marks:           |      |      |      |      |      |      |      |      |      |      |
                      a,1    a,2    a,3    b,3    e,2    e,3    g,1    h,2    i,1    i,3    l,3
      Marks numbers:   0      1      2      3      4      5      6      7      8      9      10

If the data query specifies:

-   `CounterID in ('a', 'h')`, the server reads the data in the ranges of marks `[0, 3)` and `[6, 8)`.
-   `CounterID IN ('a', 'h') AND Date = 3`, the server reads the data in the ranges of marks `[1, 3)` and `[7, 8)`.
-   `Date = 3`, the server reads the data in the range of marks `[1, 10]`.

The examples above show that it is always more effective to use an index than a full scan.

A sparse index allows extra data to be read. When reading a single range of the primary key, up to `index_granularity * 2` extra rows in each data block can be read.

Sparse indexes allow you to work with a very large number of table rows, because in most cases, such indexes fit in the computer’s RAM.

ClickHouse does not require a unique primary key. You can insert multiple rows with the same primary key.

You can use `Nullable`-typed expressions in the `PRIMARY KEY` and `ORDER BY` clauses but it is strongly discouraged. To allow this feature, turn on the [allow_nullable_key](../../../operations/settings/settings.md#allow-nullable-key) setting. The [NULLS_LAST](../../../sql-reference/statements/select/order-by.md#sorting-of-special-values) principle applies for `NULL` values in the `ORDER BY` clause.

### Selecting the Primary Key {#selecting-the-primary-key}

The number of columns in the primary key is not explicitly limited. Depending on the data structure, you can include more or fewer columns in the primary key. This may:

-   Improve the performance of an index.

    If the primary key is `(a, b)`, then adding another column `c` will improve the performance if the following conditions are met:

    -   There are queries with a condition on column `c`.
    -   Long data ranges (several times longer than the `index_granularity`) with identical values for `(a, b)` are common. In other words, when adding another column allows you to skip quite long data ranges.

-   Improve data compression.

    ClickHouse sorts data by primary key, so the higher the consistency, the better the compression.

-   Provide additional logic when merging data parts in the [CollapsingMergeTree](../../../engines/table-engines/mergetree-family/collapsingmergetree.md#table_engine-collapsingmergetree) and [SummingMergeTree](../../../engines/table-engines/mergetree-family/summingmergetree.md) engines.

    In this case it makes sense to specify the *sorting key* that is different from the primary key.

A long primary key will negatively affect the insert performance and memory consumption, but extra columns in the primary key do not affect ClickHouse performance during `SELECT` queries.

You can create a table without a primary key using the `ORDER BY tuple()` syntax. In this case, ClickHouse stores data in the order of inserting. If you want to save data order when inserting data by `INSERT ... SELECT` queries, set [max_insert_threads = 1](../../../operations/settings/settings.md#settings-max-insert-threads).

To select data in the initial order, use [single-threaded](../../../operations/settings/settings.md#settings-max_threads) `SELECT` queries.

### Choosing a Primary Key that Differs from the Sorting Key {#choosing-a-primary-key-that-differs-from-the-sorting-key}

It is possible to specify a primary key (an expression with values that are written in the index file for each mark) that is different from the sorting key (an expression for sorting the rows in data parts). In this case the primary key expression tuple must be a prefix of the sorting key expression tuple.

This feature is helpful when using the [SummingMergeTree](../../../engines/table-engines/mergetree-family/summingmergetree.md) and
[AggregatingMergeTree](../../../engines/table-engines/mergetree-family/aggregatingmergetree.md) table engines. In a common case when using these engines, the table has two types of columns: *dimensions* and *measures*. Typical queries aggregate values of measure columns with arbitrary `GROUP BY` and filtering by dimensions. Because SummingMergeTree and AggregatingMergeTree aggregate rows with the same value of the sorting key, it is natural to add all dimensions to it. As a result, the key expression consists of a long list of columns and this list must be frequently updated with newly added dimensions.

In this case it makes sense to leave only a few columns in the primary key that will provide efficient range scans and add the remaining dimension columns to the sorting key tuple.

[ALTER](../../../sql-reference/statements/alter/index.md) of the sorting key is a lightweight operation because when a new column is simultaneously added to the table and to the sorting key, existing data parts do not need to be changed. Since the old sorting key is a prefix of the new sorting key and there is no data in the newly added column, the data is sorted by both the old and new sorting keys at the moment of table modification.

### Use of Indexes and Partitions in Queries {#use-of-indexes-and-partitions-in-queries}

For `SELECT` queries, ClickHouse analyzes whether an index can be used. An index can be used if the `WHERE/PREWHERE` clause has an expression (as one of the conjunction elements, or entirely) that represents an equality or inequality comparison operation, or if it has `IN` or `LIKE` with a fixed prefix on columns or expressions that are in the primary key or partitioning key, or on certain partially repetitive functions of these columns, or logical relationships of these expressions.

Thus, it is possible to quickly run queries on one or many ranges of the primary key. In this example, queries will be fast when run for a specific tracking tag, for a specific tag and date range, for a specific tag and date, for multiple tags with a date range, and so on.

Let’s look at the engine configured as follows:
```sql
ENGINE MergeTree()
PARTITION BY toYYYYMM(EventDate)
ORDER BY (CounterID, EventDate)
SETTINGS index_granularity=8192
```

In this case, in queries:

``` sql
SELECT count() FROM table
WHERE EventDate = toDate(now())
AND CounterID = 34

SELECT count() FROM table
WHERE EventDate = toDate(now())
AND (CounterID = 34 OR CounterID = 42)

SELECT count() FROM table
WHERE ((EventDate >= toDate('2014-01-01')
AND EventDate <= toDate('2014-01-31')) OR EventDate = toDate('2014-05-01'))
AND CounterID IN (101500, 731962, 160656)
AND (CounterID = 101500 OR EventDate != toDate('2014-05-01'))
```

ClickHouse will use the primary key index to trim improper data and the monthly partitioning key to trim partitions that are in improper date ranges.

The queries above show that the index is used even for complex expressions. Reading from the table is organized so that using the index can’t be slower than a full scan.

In the example below, the index can’t be used.

``` sql
SELECT count() FROM table WHERE CounterID = 34 OR URL LIKE '%upyachka%'
```

To check whether ClickHouse can use the index when running a query, use the settings [force_index_by_date](../../../operations/settings/settings.md#settings-force_index_by_date) and [force_primary_key](../../../operations/settings/settings.md#force-primary-key).

The key for partitioning by month allows reading only those data blocks which contain dates from the proper range. In this case, the data block may contain data for many dates (up to an entire month). Within a block, data is sorted by primary key, which might not contain the date as the first column. Because of this, using a query with only a date condition that does not specify the primary key prefix will cause more data to be read than for a single date.

### Use of Index for Partially-monotonic Primary Keys {#use-of-index-for-partially-monotonic-primary-keys}

Consider, for example, the days of the month. They form a [monotonic sequence](https://en.wikipedia.org/wiki/Monotonic_function) for one month, but not monotonic for more extended periods. This is a partially-monotonic sequence. If a user creates the table with partially-monotonic primary key, ClickHouse creates a sparse index as usual. When a user selects data from this kind of table, ClickHouse analyzes the query conditions. If the user wants to get data between two marks of the index and both these marks fall within one month, ClickHouse can use the index in this particular case because it can calculate the distance between the parameters of a query and index marks.

ClickHouse cannot use an index if the values of the primary key in the query parameter range do not represent a monotonic sequence. In this case, ClickHouse uses the full scan method.

ClickHouse uses this logic not only for days of the month sequences, but for any primary key that represents a partially-monotonic sequence.

### Data Skipping Indexes {#table_engine-mergetree-data_skipping-indexes}

The index declaration is in the columns section of the `CREATE` query.

``` sql
INDEX index_name expr TYPE type(...) GRANULARITY granularity_value
```

For tables from the `*MergeTree` family, data skipping indices can be specified.

These indices aggregate some information about the specified expression on blocks, which consist of `granularity_value` granules (the size of the granule is specified using the `index_granularity` setting in the table engine). Then these aggregates are used in `SELECT` queries for reducing the amount of data to read from the disk by skipping big blocks of data where the `where` query cannot be satisfied.

**Example**

``` sql
CREATE TABLE table_name
(
    u64 UInt64,
    i32 Int32,
    s String,
    ...
    INDEX a (u64 * i32, s) TYPE minmax GRANULARITY 3,
    INDEX b (u64 * length(s)) TYPE set(1000) GRANULARITY 4
) ENGINE = MergeTree()
...
```

Indices from the example can be used by ClickHouse to reduce the amount of data to read from disk in the following queries:

``` sql
SELECT count() FROM table WHERE s < 'z'
SELECT count() FROM table WHERE u64 * i32 == 10 AND u64 * length(s) >= 1234
```

### Available Types of Indices {#available-types-of-indices}

####   `minmax`

Stores extremes of the specified expression (if the expression is `tuple`, then it stores extremes for each element of `tuple`), uses stored info for skipping blocks of data like the primary key.

####   `set(max_rows)`

Stores unique values of the specified expression (no more than `max_rows` rows, `max_rows=0` means “no limits”). Uses the values to check if the `WHERE` expression is not satisfiable on a block of data.

####   `ngrambf_v1(n, size_of_bloom_filter_in_bytes, number_of_hash_functions, random_seed)`

Stores a [Bloom filter](https://en.wikipedia.org/wiki/Bloom_filter) that contains all ngrams from a block of data. Works only with datatypes: [String](../../../sql-reference/data-types/string.md), [FixedString](../../../sql-reference/data-types/fixedstring.md) and [Map](../../../sql-reference/data-types/map.md). Can be used for optimization of `EQUALS`, `LIKE` and `IN` expressions.

- `n` — ngram size,
- `size_of_bloom_filter_in_bytes` — Bloom filter size in bytes (you can use large values here, for example, 256 or 512, because it can be compressed well).
- `number_of_hash_functions` — The number of hash functions used in the Bloom filter.
- `random_seed` — The seed for Bloom filter hash functions.

####   `tokenbf_v1(size_of_bloom_filter_in_bytes, number_of_hash_functions, random_seed)`

The same as `ngrambf_v1`, but stores tokens instead of ngrams. Tokens are sequences separated by non-alphanumeric characters.

####   `bloom_filter([false_positive])` — Stores a [Bloom filter](https://en.wikipedia.org/wiki/Bloom_filter) for the specified columns.

The optional `false_positive` parameter is the probability of receiving a false positive response from the filter. Possible values: (0, 1). Default value: 0.025.

Supported data types: `Int*`, `UInt*`, `Float*`, `Enum`, `Date`, `DateTime`, `String`, `FixedString`, `Array`, `LowCardinality`, `Nullable`, `UUID`, `Map`.

For `Map` data type client can specify if index should be created for keys or values using [mapKeys](../../../sql-reference/functions/tuple-map-functions.md#mapkeys) or [mapValues](../../../sql-reference/functions/tuple-map-functions.md#mapvalues) function.

The following functions can use the filter: [equals](../../../sql-reference/functions/comparison-functions.md), [notEquals](../../../sql-reference/functions/comparison-functions.md), [in](../../../sql-reference/functions/in-functions), [notIn](../../../sql-reference/functions/in-functions), [has](../../../sql-reference/functions/array-functions#hasarr-elem), [hasAny](../../../sql-reference/functions/array-functions#hasany), [hasAll](../../../sql-reference/functions/array-functions#hasall).

Example of index creation for `Map` data type

```
INDEX map_key_index mapKeys(map_column) TYPE bloom_filter GRANULARITY 1
INDEX map_key_index mapValues(map_column) TYPE bloom_filter GRANULARITY 1
```


``` sql
INDEX sample_index (u64 * length(s)) TYPE minmax GRANULARITY 4
INDEX sample_index2 (u64 * length(str), i32 + f64 * 100, date, str) TYPE set(100) GRANULARITY 4
INDEX sample_index3 (lower(str), str) TYPE ngrambf_v1(3, 256, 2, 0) GRANULARITY 4
```

### Functions Support {#functions-support}

Conditions in the `WHERE` clause contains calls of the functions that operate with columns. If the column is a part of an index, ClickHouse tries to use this index when performing the functions. ClickHouse supports different subsets of functions for using indexes.

The `set` index can be used with all functions. Function subsets for other indexes are shown in the table below.

| Function (operator) / Index                                                                                | primary key | minmax | ngrambf_v1 | tokenbf_v1 | bloom_filter |
|------------------------------------------------------------------------------------------------------------|-------------|--------|-------------|-------------|---------------|
| [equals (=, ==)](../../../sql-reference/functions/comparison-functions.md#function-equals)                 | ✔           | ✔      | ✔           | ✔           | ✔             |
| [notEquals(!=, &lt;&gt;)](../../../sql-reference/functions/comparison-functions.md#function-notequals)         | ✔           | ✔      | ✔           | ✔           | ✔             |
| [like](../../../sql-reference/functions/string-search-functions.md#function-like)                          | ✔           | ✔      | ✔           | ✔           | ✗             |
| [notLike](../../../sql-reference/functions/string-search-functions.md#function-notlike)                    | ✔           | ✔      | ✔           | ✔           | ✗             |
| [startsWith](../../../sql-reference/functions/string-functions.md#startswith)                              | ✔           | ✔      | ✔           | ✔           | ✗             |
| [endsWith](../../../sql-reference/functions/string-functions.md#endswith)                                  | ✗           | ✗      | ✔           | ✔           | ✗             |
| [multiSearchAny](../../../sql-reference/functions/string-search-functions.md#function-multisearchany)      | ✗           | ✗      | ✔           | ✗           | ✗             |
| [in](../../../sql-reference/functions/in-functions#in-functions)                                        | ✔           | ✔      | ✔           | ✔           | ✔             |
| [notIn](../../../sql-reference/functions/in-functions#in-functions)                                     | ✔           | ✔      | ✔           | ✔           | ✔             |
| [less (<)](../../../sql-reference/functions/comparison-functions.md#function-less)                        | ✔           | ✔      | ✗           | ✗           | ✗             |
| [greater (>)](../../../sql-reference/functions/comparison-functions.md#function-greater)                  | ✔           | ✔      | ✗           | ✗           | ✗             |
| [lessOrEquals (<=)](../../../sql-reference/functions/comparison-functions.md#function-lessorequals)       | ✔           | ✔      | ✗           | ✗           | ✗             |
| [greaterOrEquals (>=)](../../../sql-reference/functions/comparison-functions.md#function-greaterorequals) | ✔           | ✔      | ✗           | ✗           | ✗             |
| [empty](../../../sql-reference/functions/array-functions#function-empty)                                | ✔           | ✔      | ✗           | ✗           | ✗             |
| [notEmpty](../../../sql-reference/functions/array-functions#function-notempty)                          | ✔           | ✔      | ✗           | ✗           | ✗             |
| hasToken                                                                                                   | ✗           | ✗      | ✗           | ✔           | ✗             |

Functions with a constant argument that is less than ngram size can’t be used by `ngrambf_v1` for query optimization.

:::note
Bloom filters can have false positive matches, so the `ngrambf_v1`, `tokenbf_v1`, and `bloom_filter` indexes can not be used for optimizing queries where the result of a function is expected to be false.

For example:

-   Can be optimized:
    -   `s LIKE '%test%'`
    -   `NOT s NOT LIKE '%test%'`
    -   `s = 1`
    -   `NOT s != 1`
    -   `startsWith(s, 'test')`
-   Can not be optimized:
    -   `NOT s LIKE '%test%'`
    -   `s NOT LIKE '%test%'`
    -   `NOT s = 1`
    -   `s != 1`
    -   `NOT startsWith(s, 'test')`
:::

## Projections {#projections}
Projections are like [materialized views](../../../sql-reference/statements/create/view.md#materialized) but defined in part-level. It provides consistency guarantees along with automatic usage in queries.

Projections are an experimental feature. To enable them you must set the [allow_experimental_projection_optimization](../../../operations/settings/settings.md#allow-experimental-projection-optimization) to `1`. See also the [force_optimize_projection](../../../operations/settings/settings.md#force-optimize-projection) setting.

Projections are not supported in the `SELECT` statements with the [FINAL](../../../sql-reference/statements/select/from.md#select-from-final) modifier.

### Projection Query {#projection-query}
A projection query is what defines a projection. It implicitly selects data from the parent table.
**Syntax**

```sql
SELECT <column list expr> [GROUP BY] <group keys expr> [ORDER BY] <expr>
```

Projections can be modified or dropped with the [ALTER](../../../sql-reference/statements/alter/projection.md) statement.

### Projection Storage {#projection-storage}
Projections are stored inside the part directory. It's similar to an index but contains a subdirectory that stores an anonymous `MergeTree` table's part. The table is induced by the definition query of the projection. If there is a `GROUP BY` clause, the underlying storage engine becomes [AggregatingMergeTree](aggregatingmergetree.md), and all aggregate functions are converted to `AggregateFunction`. If there is an `ORDER BY` clause, the `MergeTree` table uses it as its primary key expression. During the merge process the projection part is merged via its storage's merge routine. The checksum of the parent table's part is combined with the projection's part. Other maintenance jobs are similar to skip indices.

### Query Analysis {#projection-query-analysis}
1. Check if the projection can be used to answer the given query, that is, it generates the same answer as querying the base table.
2. Select the best feasible match, which contains the least granules to read.
3. The query pipeline which uses projections will be different from the one that uses the original parts. If the projection is absent in some parts, we can add the pipeline to "project" it on the fly.

## Concurrent Data Access {#concurrent-data-access}

For concurrent table access, we use multi-versioning. In other words, when a table is simultaneously read and updated, data is read from a set of parts that is current at the time of the query. There are no lengthy locks. Inserts do not get in the way of read operations.

Reading from a table is automatically parallelized.

## TTL for Columns and Tables {#table_engine-mergetree-ttl}

Determines the lifetime of values.

The `TTL` clause can be set for the whole table and for each individual column. Table-level `TTL` can also specify the logic of automatic moving data between disks and volumes, or recompressing parts where all the data has been expired.

Expressions must evaluate to [Date](../../../sql-reference/data-types/date.md) or [DateTime](../../../sql-reference/data-types/datetime.md) data type.

**Syntax**

Setting time-to-live for a column:

``` sql
TTL time_column
TTL time_column + interval
```

To define `interval`, use [time interval](../../../sql-reference/operators/index.md#operators-datetime) operators, for example:

``` sql
TTL date_time + INTERVAL 1 MONTH
TTL date_time + INTERVAL 15 HOUR
```

### Column TTL {#mergetree-column-ttl}

When the values in the column expire, ClickHouse replaces them with the default values for the column data type. If all the column values in the data part expire, ClickHouse deletes this column from the data part in a filesystem.

The `TTL` clause can’t be used for key columns.

**Examples**

#### Creating a table with `TTL`:

``` sql
CREATE TABLE example_table
(
    d DateTime,
    a Int TTL d + INTERVAL 1 MONTH,
    b Int TTL d + INTERVAL 1 MONTH,
    c String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(d)
ORDER BY d;
```

#### Adding TTL to a column of an existing table

``` sql
ALTER TABLE example_table
    MODIFY COLUMN
    c String TTL d + INTERVAL 1 DAY;
```

#### Altering TTL of the column

``` sql
ALTER TABLE example_table
    MODIFY COLUMN
    c String TTL d + INTERVAL 1 MONTH;
```

### Table TTL {#mergetree-table-ttl}

Table can have an expression for removal of expired rows, and multiple expressions for automatic move of parts between [disks or volumes](#table_engine-mergetree-multiple-volumes). When rows in the table expire, ClickHouse deletes all corresponding rows. For parts moving or recompressing, all rows of a part must satisfy the `TTL` expression criteria.

``` sql
TTL expr
    [DELETE|RECOMPRESS codec_name1|TO DISK 'xxx'|TO VOLUME 'xxx'][, DELETE|RECOMPRESS codec_name2|TO DISK 'aaa'|TO VOLUME 'bbb'] ...
    [WHERE conditions]
    [GROUP BY key_expr [SET v1 = aggr_func(v1) [, v2 = aggr_func(v2) ...]] ]
```

Type of TTL rule may follow each TTL expression. It affects an action which is to be done once the expression is satisfied (reaches current time):

-   `DELETE` - delete expired rows (default action);
-   `RECOMPRESS codec_name` - recompress data part with the `codec_name`;
-   `TO DISK 'aaa'` - move part to the disk `aaa`;
-   `TO VOLUME 'bbb'` - move part to the disk `bbb`;
-   `GROUP BY` - aggregate expired rows.

With `WHERE` clause you may specify which of the expired rows to delete or aggregate (it cannot be applied to moves or recompression).

`GROUP BY` expression must be a prefix of the table primary key.

If a column is not part of the `GROUP BY` expression and is not set explicitly in the `SET` clause, in result row it contains an occasional value from the grouped rows (as if aggregate function `any` is applied to it).

**Examples**

#### Creating a table with `TTL`:

``` sql
CREATE TABLE example_table
(
    d DateTime,
    a Int
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(d)
ORDER BY d
TTL d + INTERVAL 1 MONTH [DELETE],
    d + INTERVAL 1 WEEK TO VOLUME 'aaa',
    d + INTERVAL 2 WEEK TO DISK 'bbb';
```

#### Altering `TTL` of the table:

``` sql
ALTER TABLE example_table
    MODIFY TTL d + INTERVAL 1 DAY;
```

Creating a table, where the rows are expired after one month. The expired rows where dates are Mondays are deleted:

``` sql
CREATE TABLE table_with_where
(
    d DateTime,
    a Int
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(d)
ORDER BY d
TTL d + INTERVAL 1 MONTH DELETE WHERE toDayOfWeek(d) = 1;
```

#### Creating a table, where expired rows are recompressed:

```sql
CREATE TABLE table_for_recompression
(
    d DateTime,
    key UInt64,
    value String
) ENGINE MergeTree()
ORDER BY tuple()
PARTITION BY key
TTL d + INTERVAL 1 MONTH RECOMPRESS CODEC(ZSTD(17)), d + INTERVAL 1 YEAR RECOMPRESS CODEC(LZ4HC(10))
SETTINGS min_rows_for_wide_part = 0, min_bytes_for_wide_part = 0;
```

Creating a table, where expired rows are aggregated. In result rows `x` contains the maximum value accross the grouped rows, `y` — the minimum value, and `d` — any occasional value from grouped rows.

``` sql
CREATE TABLE table_for_aggregation
(
    d DateTime,
    k1 Int,
    k2 Int,
    x Int,
    y Int
)
ENGINE = MergeTree
ORDER BY (k1, k2)
TTL d + INTERVAL 1 MONTH GROUP BY k1, k2 SET x = max(x), y = min(y);
```

### Removing Expired Data {#mergetree-removing-expired-data}

Data with an expired `TTL` is removed when ClickHouse merges data parts.

When ClickHouse detects that data is expired, it performs an off-schedule merge. To control the frequency of such merges, you can set `merge_with_ttl_timeout`. If the value is too low, it will perform many off-schedule merges that may consume a lot of resources.

If you perform the `SELECT` query between merges, you may get expired data. To avoid it, use the [OPTIMIZE](../../../sql-reference/statements/optimize.md) query before `SELECT`.

**See Also**

- [ttl_only_drop_parts](../../../operations/settings/settings.md#ttl_only_drop_parts) setting


## Using Multiple Block Devices for Data Storage {#table_engine-mergetree-multiple-volumes}

### Introduction {#introduction}

`MergeTree` family table engines can store data on multiple block devices. For example, it can be useful when the data of a certain table are implicitly split into “hot” and “cold”. The most recent data is regularly requested but requires only a small amount of space. On the contrary, the fat-tailed historical data is requested rarely. If several disks are available, the “hot” data may be located on fast disks (for example, NVMe SSDs or in memory), while the “cold” data - on relatively slow ones (for example, HDD).

Data part is the minimum movable unit for `MergeTree`-engine tables. The data belonging to one part are stored on one disk. Data parts can be moved between disks in the background (according to user settings) as well as by means of the [ALTER](../../../sql-reference/statements/alter/partition.md#alter_move-partition) queries.

### Terms {#terms}

-   Disk — Block device mounted to the filesystem.
-   Default disk — Disk that stores the path specified in the [path](../../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-path) server setting.
-   Volume — Ordered set of equal disks (similar to [JBOD](https://en.wikipedia.org/wiki/Non-RAID_drive_architectures)).
-   Storage policy — Set of volumes and the rules for moving data between them.

The names given to the described entities can be found in the system tables, [system.storage_policies](../../../operations/system-tables/storage_policies.md#system_tables-storage_policies) and [system.disks](../../../operations/system-tables/disks.md#system_tables-disks). To apply one of the configured storage policies for a table, use the `storage_policy` setting of `MergeTree`-engine family tables.

### Configuration {#table_engine-mergetree-multiple-volumes_configure}

Disks, volumes and storage policies should be declared inside the `<storage_configuration>` tag either in the main file `config.xml` or in a distinct file in the `config.d` directory.

Configuration structure:

``` xml
<storage_configuration>
    <disks>
        <disk_name_1> <!-- disk name -->
            <path>/mnt/fast_ssd/clickhouse/</path>
        </disk_name_1>
        <disk_name_2>
            <path>/mnt/hdd1/clickhouse/</path>
            <keep_free_space_bytes>10485760</keep_free_space_bytes>
        </disk_name_2>
        <disk_name_3>
            <path>/mnt/hdd2/clickhouse/</path>
            <keep_free_space_bytes>10485760</keep_free_space_bytes>
        </disk_name_3>

        ...
    </disks>

    ...
</storage_configuration>
```

Tags:

-   `<disk_name_N>` — Disk name. Names must be different for all disks.
-   `path` — path under which a server will store data (`data` and `shadow` folders), should be terminated with ‘/’.
-   `keep_free_space_bytes` — the amount of free disk space to be reserved.

The order of the disk definition is not important.

Storage policies configuration markup:

``` xml
<storage_configuration>
    ...
    <policies>
        <policy_name_1>
            <volumes>
                <volume_name_1>
                    <disk>disk_name_from_disks_configuration</disk>
                    <max_data_part_size_bytes>1073741824</max_data_part_size_bytes>
                    <load_balancing>round_robin</load_balancing>
                </volume_name_1>
                <volume_name_2>
                    <!-- configuration -->
                </volume_name_2>
                <!-- more volumes -->
            </volumes>
            <move_factor>0.2</move_factor>
        </policy_name_1>
        <policy_name_2>
            <!-- configuration -->
        </policy_name_2>

        <!-- more policies -->
    </policies>
    ...
</storage_configuration>
```

Tags:

-   `policy_name_N` — Policy name. Policy names must be unique.
-   `volume_name_N` — Volume name. Volume names must be unique.
-   `disk` — a disk within a volume.
-   `max_data_part_size_bytes` — the maximum size of a part that can be stored on any of the volume’s disks. If the a size of a merged part estimated to be bigger than `max_data_part_size_bytes` then this part will be written to a next volume. Basically this feature allows to keep new/small parts on a hot (SSD) volume and move them to a cold (HDD) volume when they reach large size. Do not use this setting if your policy has only one volume.
-   `move_factor` — when the amount of available space gets lower than this factor, data automatically starts to move on the next volume if any (by default, 0.1). ClickHouse sorts existing parts by size from largest to smallest (in descending order) and selects parts with the total size that is sufficient to meet the `move_factor` condition. If the total size of all parts is insufficient, all parts will be moved.
-   `prefer_not_to_merge` — Disables merging of data parts on this volume. When this setting is enabled, merging data on this volume is not allowed. This allows controlling how ClickHouse works with slow disks.
-   `perform_ttl_move_on_insert` — Disables TTL move on data part INSERT. By default if we insert a data part that already expired by the TTL move rule it immediately goes to a volume/disk declared in move rule. This can significantly slowdown insert in case if destination volume/disk is slow (e.g. S3).
-   `load_balancing` - Policy for disk balancing, `round_robin` or `least_used`.

Cofiguration examples:

``` xml
<storage_configuration>
    ...
    <policies>
        <hdd_in_order> <!-- policy name -->
            <volumes>
                <single> <!-- volume name -->
                    <disk>disk1</disk>
                    <disk>disk2</disk>
                </single>
            </volumes>
        </hdd_in_order>

        <moving_from_ssd_to_hdd>
            <volumes>
                <hot>
                    <disk>fast_ssd</disk>
                    <max_data_part_size_bytes>1073741824</max_data_part_size_bytes>
                </hot>
                <cold>
                    <disk>disk1</disk>
                </cold>
            </volumes>
            <move_factor>0.2</move_factor>
        </moving_from_ssd_to_hdd>

        <small_jbod_with_external_no_merges>
            <volumes>
                <main>
                    <disk>jbod1</disk>
                </main>
                <external>
                    <disk>external</disk>
                    <prefer_not_to_merge>true</prefer_not_to_merge>
                </external>
            </volumes>
        </small_jbod_with_external_no_merges>
    </policies>
    ...
</storage_configuration>
```

In given example, the `hdd_in_order` policy implements the [round-robin](https://en.wikipedia.org/wiki/Round-robin_scheduling) approach. Thus this policy defines only one volume (`single`), the data parts are stored on all its disks in circular order. Such policy can be quite useful if there are several similar disks are mounted to the system, but RAID is not configured. Keep in mind that each individual disk drive is not reliable and you might want to compensate it with replication factor of 3 or more.

If there are different kinds of disks available in the system, `moving_from_ssd_to_hdd` policy can be used instead. The volume `hot` consists of an SSD disk (`fast_ssd`), and the maximum size of a part that can be stored on this volume is 1GB. All the parts with the size larger than 1GB will be stored directly on the `cold` volume, which contains an HDD disk `disk1`.
Also, once the disk `fast_ssd` gets filled by more than 80%, data will be transferred to the `disk1` by a background process.

The order of volume enumeration within a storage policy is important. Once a volume is overfilled, data are moved to the next one. The order of disk enumeration is important as well because data are stored on them in turns.

When creating a table, one can apply one of the configured storage policies to it:

``` sql
CREATE TABLE table_with_non_default_policy (
    EventDate Date,
    OrderID UInt64,
    BannerID UInt64,
    SearchPhrase String
) ENGINE = MergeTree
ORDER BY (OrderID, BannerID)
PARTITION BY toYYYYMM(EventDate)
SETTINGS storage_policy = 'moving_from_ssd_to_hdd'
```

The `default` storage policy implies using only one volume, which consists of only one disk given in `<path>`.
You could change storage policy after table creation with [ALTER TABLE ... MODIFY SETTING] query, new policy should include all old disks and volumes with same names.

The number of threads performing background moves of data parts can be changed by [background_move_pool_size](../../../operations/settings/settings.md#background_move_pool_size) setting.

### Details {#details}

In the case of `MergeTree` tables, data is getting to disk in different ways:

-   As a result of an insert (`INSERT` query).
-   During background merges and [mutations](../../../sql-reference/statements/alter/index.md#alter-mutations).
-   When downloading from another replica.
-   As a result of partition freezing [ALTER TABLE … FREEZE PARTITION](../../../sql-reference/statements/alter/partition.md#alter_freeze-partition).

In all these cases except for mutations and partition freezing, a part is stored on a volume and a disk according to the given storage policy:

1.  The first volume (in the order of definition) that has enough disk space for storing a part (`unreserved_space > current_part_size`) and allows for storing parts of a given size (`max_data_part_size_bytes > current_part_size`) is chosen.
2.  Within this volume, that disk is chosen that follows the one, which was used for storing the previous chunk of data, and that has free space more than the part size (`unreserved_space - keep_free_space_bytes > current_part_size`).

Under the hood, mutations and partition freezing make use of [hard links](https://en.wikipedia.org/wiki/Hard_link). Hard links between different disks are not supported, therefore in such cases the resulting parts are stored on the same disks as the initial ones.

In the background, parts are moved between volumes on the basis of the amount of free space (`move_factor` parameter) according to the order the volumes are declared in the configuration file.
Data is never transferred from the last one and into the first one. One may use system tables [system.part_log](../../../operations/system-tables/part_log.md#system_tables-part-log) (field `type = MOVE_PART`) and [system.parts](../../../operations/system-tables/parts.md#system_tables-parts) (fields `path` and `disk`) to monitor background moves. Also, the detailed information can be found in server logs.

User can force moving a part or a partition from one volume to another using the query [ALTER TABLE … MOVE PART\|PARTITION … TO VOLUME\|DISK …](../../../sql-reference/statements/alter/partition.md#alter_move-partition), all the restrictions for background operations are taken into account. The query initiates a move on its own and does not wait for background operations to be completed. User will get an error message if not enough free space is available or if any of the required conditions are not met.

Moving data does not interfere with data replication. Therefore, different storage policies can be specified for the same table on different replicas.

After the completion of background merges and mutations, old parts are removed only after a certain amount of time (`old_parts_lifetime`).
During this time, they are not moved to other volumes or disks. Therefore, until the parts are finally removed, they are still taken into account for evaluation of the occupied disk space.

User can assign new big parts to different disks of a [JBOD](https://en.wikipedia.org/wiki/Non-RAID_drive_architectures) volume in a balanced way using the [min_bytes_to_rebalance_partition_over_jbod](../../../operations/settings/merge-tree-settings.md#min-bytes-to-rebalance-partition-over-jbod) setting.

## Using S3 for Data Storage {#table_engine-mergetree-s3}

`MergeTree` family table engines can store data to [S3](https://aws.amazon.com/s3/) using a disk with type `s3`.

This feature is under development and not ready for production. There are known drawbacks such as very low performance.

Configuration markup:
``` xml
<storage_configuration>
    ...
    <disks>
        <s3>
            <type>s3</type>
            <endpoint>https://clickhouse-public-datasets.s3.amazonaws.com/my-bucket/root-path/</endpoint>
            <access_key_id>your_access_key_id</access_key_id>
            <secret_access_key>your_secret_access_key</secret_access_key>
            <region></region>
            <server_side_encryption_customer_key_base64>your_base64_encoded_customer_key</server_side_encryption_customer_key_base64>
            <proxy>
                <uri>http://proxy1</uri>
                <uri>http://proxy2</uri>
            </proxy>
            <connect_timeout_ms>10000</connect_timeout_ms>
            <request_timeout_ms>5000</request_timeout_ms>
            <retry_attempts>10</retry_attempts>
            <single_read_retries>4</single_read_retries>
            <min_bytes_for_seek>1000</min_bytes_for_seek>
            <metadata_path>/var/lib/clickhouse/disks/s3/</metadata_path>
            <cache_enabled>true</cache_enabled>
            <cache_path>/var/lib/clickhouse/disks/s3/cache/</cache_path>
            <skip_access_check>false</skip_access_check>
        </s3>
    </disks>
    ...
</storage_configuration>
```

Required parameters:

-   `endpoint` — S3 endpoint URL in `path` or `virtual hosted` [styles](https://docs.aws.amazon.com/AmazonS3/latest/dev/VirtualHosting.html). Endpoint URL should contain a bucket and root path to store data.
-   `access_key_id` — S3 access key id.
-   `secret_access_key` — S3 secret access key.

Optional parameters:

-   `region` — S3 region name.
-   `use_environment_credentials` — Reads AWS credentials from the Environment variables AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY and AWS_SESSION_TOKEN if they exist. Default value is `false`.
-   `use_insecure_imds_request` — If set to `true`, S3 client will use insecure IMDS request while obtaining credentials from Amazon EC2 metadata. Default value is `false`.
-   `proxy` — Proxy configuration for S3 endpoint. Each `uri` element inside `proxy` block should contain a proxy URL.
-   `connect_timeout_ms` — Socket connect timeout in milliseconds. Default value is `10 seconds`.
-   `request_timeout_ms` — Request timeout in milliseconds. Default value is `5 seconds`.
-   `retry_attempts` — Number of retry attempts in case of failed request. Default value is `10`.
-   `single_read_retries` — Number of retry attempts in case of connection drop during read. Default value is `4`.
-   `min_bytes_for_seek` — Minimal number of bytes to use seek operation instead of sequential read. Default value is `1 Mb`.
-   `metadata_path` — Path on local FS to store metadata files for S3. Default value is `/var/lib/clickhouse/disks/<disk_name>/`.
-   `cache_enabled` — Allows to cache mark and index files on local FS. Default value is `true`.
-   `cache_path` — Path on local FS where to store cached mark and index files. Default value is `/var/lib/clickhouse/disks/<disk_name>/cache/`.
-   `skip_access_check` — If true, disk access checks will not be performed on disk start-up. Default value is `false`.
-   `server_side_encryption_customer_key_base64` — If specified, required headers for accessing S3 objects with SSE-C encryption will be set.

S3 disk can be configured as `main` or `cold` storage:
``` xml
<storage_configuration>
    ...
    <disks>
        <s3>
            <type>s3</type>
            <endpoint>https://clickhouse-public-datasets.s3.amazonaws.com/my-bucket/root-path/</endpoint>
            <access_key_id>your_access_key_id</access_key_id>
            <secret_access_key>your_secret_access_key</secret_access_key>
        </s3>
    </disks>
    <policies>
        <s3_main>
            <volumes>
                <main>
                    <disk>s3</disk>
                </main>
            </volumes>
        </s3_main>
        <s3_cold>
            <volumes>
                <main>
                    <disk>default</disk>
                </main>
                <external>
                    <disk>s3</disk>
                </external>
            </volumes>
            <move_factor>0.2</move_factor>
        </s3_cold>
    </policies>
    ...
</storage_configuration>
```

In case of `cold` option a data can be moved to S3 if local disk free size will be smaller than `move_factor * disk_size` or by TTL move rule.

## Using Azure Blob Storage for Data Storage {#table_engine-mergetree-azure-blob-storage}

`MergeTree` family table engines can store data to [Azure Blob Storage](https://azure.microsoft.com/en-us/services/storage/blobs/) using a disk with type `azure_blob_storage`.

As of February 2022, this feature is still a fresh addition, so expect that some Azure Blob Storage functionalities might be unimplemented.

Configuration markup:
``` xml
<storage_configuration>
    ...
    <disks>
        <blob_storage_disk>
            <type>azure_blob_storage</type>
            <storage_account_url>http://account.blob.core.windows.net</storage_account_url>
            <container_name>container</container_name>
            <account_name>account</account_name>
            <account_key>pass123</account_key>
            <metadata_path>/var/lib/clickhouse/disks/blob_storage_disk/</metadata_path>
            <cache_enabled>true</cache_enabled>
            <cache_path>/var/lib/clickhouse/disks/blob_storage_disk/cache/</cache_path>
            <skip_access_check>false</skip_access_check>
        </blob_storage_disk>
    </disks>
    ...
</storage_configuration>
```

Connection parameters:
* `storage_account_url` - **Required**, Azure Blob Storage account URL, like `http://account.blob.core.windows.net` or `http://azurite1:10000/devstoreaccount1`.
* `container_name` - Target container name, defaults to `default-container`.
* `container_already_exists` - If set to `false`, a new container `container_name` is created in the storage account, if set to `true`, disk connects to the container directly, and if left unset, disk connects to the account, checks if the container `container_name` exists, and creates it if it doesn't exist yet.

Authentication parameters (the disk will try all available methods **and** Managed Identity Credential):
* `connection_string` - For authentication using a connection string.
* `account_name` and `account_key` - For authentication using Shared Key.

Limit parameters (mainly for internal usage):
* `max_single_part_upload_size` - Limits the size of a single block upload to Blob Storage.
* `min_bytes_for_seek` - Limits the size of a seekable region.
* `max_single_read_retries` - Limits the number of attempts to read a chunk of data from Blob Storage.
* `max_single_download_retries` - Limits the number of attempts to download a readable buffer from Blob Storage.
* `thread_pool_size` - Limits the number of threads with which `IDiskRemote` is instantiated.

Other parameters:
* `metadata_path` - Path on local FS to store metadata files for Blob Storage. Default value is `/var/lib/clickhouse/disks/<disk_name>/`.
* `cache_enabled` - Allows to cache mark and index files on local FS. Default value is `true`.
* `cache_path` - Path on local FS where to store cached mark and index files. Default value is `/var/lib/clickhouse/disks/<disk_name>/cache/`.
* `skip_access_check` - If true, disk access checks will not be performed on disk start-up. Default value is `false`.

Examples of working configurations can be found in integration tests directory (see e.g. [test_merge_tree_azure_blob_storage](https://github.com/ClickHouse/ClickHouse/blob/master/tests/integration/test_merge_tree_azure_blob_storage/configs/config.d/storage_conf.xml) or [test_azure_blob_storage_zero_copy_replication](https://github.com/ClickHouse/ClickHouse/blob/master/tests/integration/test_azure_blob_storage_zero_copy_replication/configs/config.d/storage_conf.xml)).

## Virtual Columns {#virtual-columns}

-   `_part` — Name of a part.
-   `_part_index` — Sequential index of the part in the query result.
-   `_partition_id` — Name of a partition.
-   `_part_uuid` — Unique part identifier (if enabled MergeTree setting `assign_part_uuids`).
-   `_partition_value` — Values (a tuple) of a `partition by` expression.
-   `_sample_factor` — Sample factor (from the query).
