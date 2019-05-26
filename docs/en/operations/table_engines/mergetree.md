# MergeTree {#table_engines-mergetree}

The `MergeTree` engine and other engines of this family (`*MergeTree`) are the most robust ClickHousе table engines.

The basic idea for `MergeTree` engines family is the following. When you have tremendous amount of a data that should be inserted into the table, you should write them quickly part by part and then merge parts by some rules in background. This method is much more efficient than constantly rewriting data in the storage at the insert.

Main features:

- Stores data sorted by primary key.

    This allows you to create a small sparse index that helps find data faster.

- This allows you to use partitions if the [partitioning key](custom_partitioning_key.md) is specified.

    ClickHouse supports certain operations with partitions that are more effective than general operations on the same data with the same result. ClickHouse also automatically cuts off the partition data where the partitioning key is specified in the query. This also increases the query performance.

- Data replication support.

    The family of `ReplicatedMergeTree` tables is used for this. For more information, see the [Data replication](replication.md) section.

- Data sampling support.

    If necessary, you can set the data sampling method in the table.

!!! info
    The [Merge](merge.md) engine does not belong to the `*MergeTree` family.


## Creating a Table  {#table_engine-mergetree-creating-a-table}

```
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [TTL expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2] [TTL expr2],
    ...
    INDEX index_name1 expr1 TYPE type1(...) GRANULARITY value1,
    INDEX index_name2 expr2 TYPE type2(...) GRANULARITY value2
) ENGINE = MergeTree()
[PARTITION BY expr]
[ORDER BY expr]
[PRIMARY KEY expr]
[SAMPLE BY expr]
[TTL expr]
[SETTINGS name=value, ...]
```

For a description of request parameters, see [request description](../../query_language/create.md).

**Query clauses**

- `ENGINE` — Name and parameters of the engine. `ENGINE = MergeTree()`. `MergeTree` engine does not have parameters.

- `PARTITION BY` — The [partitioning key](custom_partitioning_key.md).

    For partitioning by month, use the `toYYYYMM(date_column)` expression, where `date_column` is a column with a date of the type [Date](../../data_types/date.md). The partition names here have the `"YYYYMM"` format.

- `ORDER BY` — The sorting key.

    A tuple of columns or arbitrary expressions. Example: `ORDER BY (CounterID, EventDate)`.

- `PRIMARY KEY` — The primary key if it [differs from the sorting key](mergetree.md).

    By default the primary key is the same as the sorting key (which is specified by the `ORDER BY` clause). Thus in most cases it is unnecessary to specify a separate `PRIMARY KEY` clause.

- `SAMPLE BY` — An expression for sampling.

    If a sampling expression is used, the primary key must contain it. Example: `SAMPLE BY intHash32(UserID) ORDER BY (CounterID, EventDate, intHash32(UserID))`.

- `TTL` — An expression for setting storage time for rows.

    It must depends on `Date` or `DateTime` column and has one `Date` or `DateTime` column as a result. Example:
    `TTL date + INTERVAL 1 DAY`

    For more details, see [TTL for columns and tables](mergetree.md)

- `SETTINGS` — Additional parameters that control the behavior of the `MergeTree`:
    - `index_granularity` — The granularity of an index. The number of data rows between the "marks" of an index. By default, 8192. The list of all available parameters you can see in [MergeTreeSettings.h](https://github.com/yandex/ClickHouse/blob/master/dbms/src/Storages/MergeTree/MergeTreeSettings.h).
    - `use_minimalistic_part_header_in_zookeeper` — Storage method of the data parts headers in ZooKeeper. If  `use_minimalistic_part_header_in_zookeeper=1`, then ZooKeeper stores less data. For more information refer the [setting description](../server_settings/settings.md#server-settings-use_minimalistic_part_header_in_zookeeper) in the "Server configuration parameters" chapter.
    - `min_merge_bytes_to_use_direct_io` — The minimum data volume for merge operation required for using of the direct I/O access to the storage disk. During the merging of the data parts, ClickHouse calculates summary storage volume of all the data to be merged. If the volume exceeds `min_merge_bytes_to_use_direct_io` bytes, then ClickHouse reads and writes the data using direct I/O interface (`O_DIRECT` option) to the storage disk. If `min_merge_bytes_to_use_direct_io = 0`, then the direct I/O is disabled. Default value: `10 * 1024 * 1024 * 1024` bytes.
    - `merge_with_ttl_timeout` — Minimal time in seconds, when merge with TTL can be repeated. Default value: 86400 (1 day).

**Example of sections setting**

```
ENGINE MergeTree() PARTITION BY toYYYYMM(EventDate) ORDER BY (CounterID, EventDate, intHash32(UserID)) SAMPLE BY intHash32(UserID) SETTINGS index_granularity=8192
```

In the example, we set partitioning by month.

We also set an expression for sampling as a hash by the user ID. This allows you to pseudorandomize the data in the table for each `CounterID` and `EventDate`. If, when selecting the data, you define a [SAMPLE](../../query_language/select.md#select-sample-clause) clause, ClickHouse will return an evenly pseudorandom data sample for a subset of users.

`index_granularity` could be omitted because 8192 is the default value.

<details markdown="1"><summary>Deprecated Method for Creating a Table</summary>

!!! attention
    Do not use this method in new projects and, if possible, switch the old projects to the method described above.

```
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE [=] MergeTree(date-column [, sampling_expression], (primary, key), index_granularity)
```

**MergeTree() parameters**

- `date-column` — The name of a column of the type [Date](../../data_types/date.md). ClickHouse automatically creates partitions by month on the basis of this column. The partition names are in the `"YYYYMM"` format.
- `sampling_expression` — an expression for sampling.
- `(primary, key)` — primary key. Type — [Tuple()](../../data_types/tuple.md)
- `index_granularity` — The granularity of an index. The number of data rows between the "marks" of an index. The value 8192 is appropriate for most tasks.

**Example**

```
MergeTree(EventDate, intHash32(UserID), (CounterID, EventDate, intHash32(UserID)), 8192)
```

The `MergeTree` engine is configured in the same way as in the example above for the main engine configuration method.
</details>

## Data Storage

A table consists of data *parts* sorted by primary key.

When data is inserted in a table, separate data parts are created and each of them is lexicographically sorted by primary key. For example, if the primary key is `(CounterID, Date)`, the data in the part is sorted by `CounterID`, and within each `CounterID`, it is ordered by `Date`.

Data belonging to different partitions are separated into different parts. In the background, ClickHouse merges data parts for more efficient storage. Parts belonging to different partitions are not merged. The merge mechanism does not guarantee that all rows with the same primary key will be in the same data part.

For each data part, ClickHouse creates an index file that contains the primary key value for each index row ("mark"). Index row numbers are defined as `n * index_granularity`. The maximum value `n` is equal to the integer part of dividing the total number of rows by the `index_granularity`. For each column, the "marks" are also written for the same index rows as the primary key. These "marks" allow you to find the data directly in the columns.

You can use a single large table and continually add data to it in small chunks – this is what the `MergeTree` engine is intended for.

## Primary Keys and Indexes in Queries {#primary-keys-and-indexes-in-queries}

Let's take the `(CounterID, Date)` primary key. In this case, the sorting and index can be illustrated as follows:

```
Whole data:     [-------------------------------------------------------------------------]
CounterID:      [aaaaaaaaaaaaaaaaaabbbbcdeeeeeeeeeeeeefgggggggghhhhhhhhhiiiiiiiiikllllllll]
Date:           [1111111222222233331233211111222222333211111112122222223111112223311122333]
Marks:           |      |      |      |      |      |      |      |      |      |      |
                a,1    a,2    a,3    b,3    e,2    e,3    g,1    h,2    i,1    i,3    l,3
Marks numbers:   0      1      2      3      4      5      6      7      8      9      10
```

If the data query specifies:

- `CounterID in ('a', 'h')`, the server reads the data in the ranges of marks `[0, 3)` and `[6, 8)`.
- `CounterID IN ('a', 'h') AND Date = 3`, the server reads the data in the ranges of marks `[1, 3)` and `[7, 8)`.
- `Date = 3`, the server reads the data in the range of marks `[1, 10]`.

The examples above show that it is always more effective to use an index than a full scan.

A sparse index allows extra data to be read. When reading a single range of the primary key, up to `index_granularity * 2` extra rows in each data block can be read. In most cases, ClickHouse performance does not degrade when `index_granularity = 8192`.

Sparse indexes allow you to work with a very large number of table rows, because such indexes are always stored in the computer's RAM.

ClickHouse does not require a unique primary key. You can insert multiple rows with the same primary key.

### Selecting the Primary Key

The number of columns in the primary key is not explicitly limited. Depending on the data structure, you can include more or fewer columns in the primary key. This may:

- Improve the performance of an index.

    If the primary key is `(a, b)`, then adding another column `c` will improve the performance if the following conditions are met:
    - There are queries with a condition on column `c`.
    - Long data ranges (several times longer than the `index_granularity`) with identical values for `(a, b)` are common. In other words, when adding another column allows you to skip quite long data ranges.

- Improve data compression.

    ClickHouse sorts data by primary key, so the higher the consistency, the better the compression.

- Provide additional logic when data parts merging in the [CollapsingMergeTree](collapsingmergetree.md#table_engine-collapsingmergetree) and [SummingMergeTree](summingmergetree.md) engines.

    In this case it makes sense to specify the *sorting key* that is different from the primary key.

A long primary key will negatively affect the insert performance and memory consumption, but extra columns in the primary key do not affect ClickHouse performance during `SELECT` queries.


### Choosing the Primary Key that differs from the Sorting Key

It is possible to specify the primary key (the expression, values of which are written into the index file
for each mark) that is different from the sorting key (the expression for sorting the rows in data parts).
In this case the primary key expression tuple must be a prefix of the sorting key expression tuple.

This feature is helpful when using the [SummingMergeTree](summingmergetree.md) and
[AggregatingMergeTree](aggregatingmergetree.md) table engines. In a common case when using these engines the
table has two types of columns: *dimensions* and *measures*. Typical queries aggregate values of measure
columns with arbitrary `GROUP BY` and filtering by dimensions. As SummingMergeTree and AggregatingMergeTree
aggregate rows with the same value of the sorting key, it is natural to add all dimensions to it. As a result
the key expression consists of a long list of columns and this list must be frequently updated with newly
added dimensions.

In this case it makes sense to leave only a few columns in the primary key that will provide efficient
range scans and add the remaining dimension columns to the sorting key tuple.

[ALTER of the sorting key](../../query_language/alter.md) is a lightweight operation because when a new column is simultaneously added to the table and to the sorting key, existing data parts don't need to be changed. Since the old sorting key is a prefix of the new sorting key and there is no data in the just added column, the data at the moment of table modification is sorted by both the old and the new sorting key.

### Use of Indexes and Partitions in Queries

For`SELECT` queries, ClickHouse analyzes whether an index can be used. An index can be used if the `WHERE/PREWHERE` clause has an expression (as one of the conjunction elements, or entirely) that represents an equality or inequality comparison operation, or if it has `IN` or `LIKE` with a fixed prefix on columns or expressions that are in the primary key or partitioning key, or on certain partially repetitive functions of these columns, or logical relationships of these expressions.

Thus, it is possible to quickly run queries on one or many ranges of the primary key. In this example, queries will be fast when run for a specific tracking tag; for a specific tag and date range; for a specific tag and date; for multiple tags with a date range, and so on.

Let's look at the engine configured as follows:

```
ENGINE MergeTree() PARTITION BY toYYYYMM(EventDate) ORDER BY (CounterID, EventDate) SETTINGS index_granularity=8192
```

In this case, in queries:

``` sql
SELECT count() FROM table WHERE EventDate = toDate(now()) AND CounterID = 34
SELECT count() FROM table WHERE EventDate = toDate(now()) AND (CounterID = 34 OR CounterID = 42)
SELECT count() FROM table WHERE ((EventDate >= toDate('2014-01-01') AND EventDate <= toDate('2014-01-31')) OR EventDate = toDate('2014-05-01')) AND CounterID IN (101500, 731962, 160656) AND (CounterID = 101500 OR EventDate != toDate('2014-05-01'))
```

ClickHouse will use the primary key index to trim improper data and the monthly partitioning key to trim partitions that are in improper date ranges.

The queries above show that the index is used even for complex expressions. Reading from the table is organized so that using the index can't be slower than a full scan.

In the example below, the index can't be used.

``` sql
SELECT count() FROM table WHERE CounterID = 34 OR URL LIKE '%upyachka%'
```

To check whether ClickHouse can use the index when running a query, use the settings [force_index_by_date](../settings/settings.md#settings-force_index_by_date) and [force_primary_key](../settings/settings.md).

The key for partitioning by month allows reading only those data blocks which contain dates from the proper range. In this case, the data block may contain data for many dates (up to an entire month). Within a block, data is sorted by primary key, which might not contain the date as the first column. Because of this, using a query with only a date condition that does not specify the primary key prefix will cause more data to be read than for a single date.


### Data Skipping Indices (Experimental)

You need to set `allow_experimental_data_skipping_indices` to 1 to use indices.  (run `SET allow_experimental_data_skipping_indices = 1`).

Index declaration in the columns section of the `CREATE` query.
```sql
INDEX index_name expr TYPE type(...) GRANULARITY granularity_value
```

For tables from the `*MergeTree` family data skipping indices can be specified.

These indices aggregate some information about the specified expression on blocks, which consist of `granularity_value` granules (size of the granule is specified using `index_granularity` setting in the table engine), then these aggregates are used in `SELECT` queries for reducing the amount of data to read from the disk by skipping big blocks of data where `where` query cannot be satisfied.


**Example**

```sql
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

Indices from the example can be used by ClickHouse to reduce the amount of data to read from disk in following queries.

```sql
SELECT count() FROM table WHERE s < 'z'
SELECT count() FROM table WHERE u64 * i32 == 10 AND u64 * length(s) >= 1234
```

#### Available Types of Indices

- `minmax`

    Stores extremes of the specified expression (if the expression is `tuple`, then it stores extremes for each element of `tuple`), uses stored info for skipping blocks of the data like the primary key.

- `set(max_rows)`

    Stores unique values of the specified expression (no more than `max_rows` rows, `max_rows=0` means "no limits"), use them to check if the `WHERE` expression is not satisfiable on a block of the data.  

- `ngrambf_v1(n, size_of_bloom_filter_in_bytes, number_of_hash_functions, random_seed)`

    Stores [bloom filter](https://en.wikipedia.org/wiki/Bloom_filter) that contains all ngrams from block of data. Works only with strings. Can be used for optimization of `equals`, `like` and `in` expressions.

    - `n` — ngram size,
    - `size_of_bloom_filter_in_bytes` — bloom filter size in bytes (you can use big values here, for example, 256 or 512, because it can be compressed well),
    - `number_of_hash_functions` — number of hash functions used in bloom filter,
    - `random_seed` — seed for bloom filter hash functions.

- `tokenbf_v1(size_of_bloom_filter_in_bytes, number_of_hash_functions, random_seed)`

    The same as `ngrambf_v1`, but instead of ngrams stores tokens, which are sequences separated by non-alphanumeric characters.

```sql
INDEX sample_index (u64 * length(s)) TYPE minmax GRANULARITY 4
INDEX sample_index2 (u64 * length(str), i32 + f64 * 100, date, str) TYPE set(100) GRANULARITY 4
INDEX sample_index3 (lower(str), str) TYPE ngrambf_v1(3, 256, 2, 0) GRANULARITY 4
```

## Concurrent Data Access

For concurrent table access, we use multi-versioning. In other words, when a table is simultaneously read and updated, data is read from a set of parts that is current at the time of the query. There are no lengthy locks. Inserts do not get in the way of read operations.

Reading from a table is automatically parallelized.


## TTL for columns and tables

Data with expired TTL is removed while executing merges.

If TTL is set for column, when it expires, value will be replaced by default. If all values in columns were zeroed in part, data for this column will be deleted from disk for part. You are not allowed to set TTL for all key columns. If TTL is set for table, when it expires, row will be deleted.

When TTL expires on some value or row in part, extra merge will be executed. To control frequency of merges with TTL you can set `merge_with_ttl_timeout`. If it is too low, many extra merges and lack of regular merges can reduce the perfomance.  

[Original article](https://clickhouse.yandex/docs/en/operations/table_engines/mergetree/) <!--hide-->
