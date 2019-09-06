# MergeTree {#table_engines-mergetree}

The `MergeTree` engine and other engines of this family (`*MergeTree`) are the most robust ClickHousе table engines.

Engines in the `MergeTree` family are designed for inserting a very large amount of data into a table. The data is quickly written to the table part by part, then rules are applied for merging the parts in the background. This method is much more efficient than continually rewriting the data in storage during insert.

Main features:

- Stores data sorted by primary key.

    This allows you to create a small sparse index that helps find data faster.

- Partitions can be used if the [partitioning key](custom_partitioning_key.md) is specified.

    ClickHouse supports certain operations with partitions that are more effective than general operations on the same data with the same result. ClickHouse also automatically cuts off the partition data where the partitioning key is specified in the query. This also improves query performance.

- Data replication support.

    The family of `ReplicatedMergeTree` tables provides data replication. For more information, see [Data replication](replication.md).

- Data sampling support.

    If necessary, you can set the data sampling method in the table.

!!! info
    The [Merge](merge.md) engine does not belong to the `*MergeTree` family.


## Creating a Table  {#table_engine-mergetree-creating-a-table}

```sql
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

For a description of parameters, see the [CREATE query description](../../query_language/create.md).

!!! note "Note"
    `INDEX` is an experimental feature, see [Data Skipping Indexes](#table_engine-mergetree-data_skipping-indexes).

### Query Clauses

- `ENGINE` — Name and parameters of the engine. `ENGINE = MergeTree()`. The `MergeTree` engine does not have parameters.

- `PARTITION BY` — The [partitioning key](custom_partitioning_key.md).

    For partitioning by month, use the `toYYYYMM(date_column)` expression, where `date_column` is a column with a date of the type [Date](../../data_types/date.md). The partition names here have the `"YYYYMM"` format.

- `ORDER BY` — The sorting key.

    A tuple of columns or arbitrary expressions. Example: `ORDER BY (CounterID, EventDate)`.

- `PRIMARY KEY` — The primary key if it [differs from the sorting key](mergetree.md).

    By default the primary key is the same as the sorting key (which is specified by the `ORDER BY` clause). Thus in most cases it is unnecessary to specify a separate `PRIMARY KEY` clause.

- `SAMPLE BY` — An expression for sampling.

    If a sampling expression is used, the primary key must contain it. Example: `SAMPLE BY intHash32(UserID) ORDER BY (CounterID, EventDate, intHash32(UserID))`.

- `TTL` — An expression for setting storage time for rows.

    It must depend on the `Date` or `DateTime` column and have one `Date` or `DateTime` column as a result. Example:
    `TTL date + INTERVAL 1 DAY`

    For more details, see [TTL for columns and tables](#table_engine-mergetree-ttl)

- `SETTINGS` — Additional parameters that control the behavior of the `MergeTree`:
    - `index_granularity` — The granularity of an index. The number of data rows between the "marks" of an index. By default, 8192. For the list of available parameters, see [MergeTreeSettings.h](https://github.com/yandex/ClickHouse/blob/master/dbms/src/Storages/MergeTree/MergeTreeSettings.h).
    - `use_minimalistic_part_header_in_zookeeper` — Storage method of the data parts headers in ZooKeeper. If  `use_minimalistic_part_header_in_zookeeper=1`, then ZooKeeper stores less data. For more information, see the [setting description](../server_settings/settings.md#server-settings-use_minimalistic_part_header_in_zookeeper) in "Server configuration parameters".
    - `min_merge_bytes_to_use_direct_io` — The minimum data volume for merge operation that is required for using direct I/O access to the storage disk. When merging data parts, ClickHouse calculates the total storage volume of all the data to be merged. If the volume exceeds `min_merge_bytes_to_use_direct_io` bytes, ClickHouse reads and writes the data to the storage disk using the direct I/O interface (`O_DIRECT` option). If `min_merge_bytes_to_use_direct_io = 0`, then direct I/O is disabled. Default value: `10 * 1024 * 1024 * 1024` bytes.
    <a name="mergetree_setting-merge_with_ttl_timeout"></a>
    - `merge_with_ttl_timeout` — Minimum delay in seconds before repeating a merge with TTL. Default value: 86400 (1 day).

**Example of Sections Setting**

```sql
ENGINE MergeTree() PARTITION BY toYYYYMM(EventDate) ORDER BY (CounterID, EventDate, intHash32(UserID)) SAMPLE BY intHash32(UserID) SETTINGS index_granularity=8192
```

In the example, we set partitioning by month.

We also set an expression for sampling as a hash by the user ID. This allows you to pseudorandomize the data in the table for each `CounterID` and `EventDate`. If you define a [SAMPLE](../../query_language/select.md#select-sample-clause) clause when selecting the data, ClickHouse will return an evenly pseudorandom data sample for a subset of users.

The `index_granularity` setting can be omitted because 8192 is the default value.

<details markdown="1"><summary>Deprecated Method for Creating a Table</summary>

!!! attention
    Do not use this method in new projects. If possible, switch old projects to the method described above.

```
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE [=] MergeTree(date-column [, sampling_expression], (primary, key), index_granularity)
```

**MergeTree() Parameters**

- `date-column` — The name of a column of the [Date](../../data_types/date.md) type. ClickHouse automatically creates partitions by month based on this column. The partition names are in the `"YYYYMM"` format.
- `sampling_expression` — An expression for sampling.
- `(primary, key)` — Primary key. Type: [Tuple()](../../data_types/tuple.md)
- `index_granularity` — The granularity of an index. The number of data rows between the "marks" of an index. The value 8192 is appropriate for most tasks.

**Example**

```
MergeTree(EventDate, intHash32(UserID), (CounterID, EventDate, intHash32(UserID)), 8192)
```

The `MergeTree` engine is configured in the same way as in the example above for the main engine configuration method.
</details>

## Data Storage

A table consists of data parts sorted by primary key.

When data is inserted in a table, separate data parts are created and each of them is lexicographically sorted by primary key. For example, if the primary key is `(CounterID, Date)`, the data in the part is sorted by `CounterID`, and within each `CounterID`, it is ordered by `Date`.

Data belonging to different partitions are separated into different parts. In the background, ClickHouse merges data parts for more efficient storage. Parts belonging to different partitions are not merged. The merge mechanism does not guarantee that all rows with the same primary key will be in the same data part.

For each data part, ClickHouse creates an index file that contains the primary key value for each index row ("mark"). Index row numbers are defined as `n * index_granularity`. The maximum value `n` is equal to the integer part of dividing the total number of rows by the `index_granularity`. For each column, the "marks" are also written for the same index rows as the primary key. These "marks" allow you to find the data directly in the columns.

You can use a single large table and continually add data to it in small chunks – this is what the `MergeTree` engine is intended for.

## Primary Keys and Indexes in Queries {#primary-keys-and-indexes-in-queries}

Take the `(CounterID, Date)` primary key as an example. In this case, the sorting and index can be illustrated as follows:

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

- Provide additional logic when merging data parts in the [CollapsingMergeTree](collapsingmergetree.md#table_engine-collapsingmergetree) and [SummingMergeTree](summingmergetree.md) engines.

    In this case it makes sense to specify the *sorting key* that is different from the primary key.

A long primary key will negatively affect the insert performance and memory consumption, but extra columns in the primary key do not affect ClickHouse performance during `SELECT` queries.


### Choosing a Primary Key that Differs from the Sorting Key

It is possible to specify a primary key (an expression with values that are written in the index file for each mark) that is different from the sorting key (an expression for sorting the rows in data parts). In this case the primary key expression tuple must be a prefix of the sorting key expression tuple.

This feature is helpful when using the [SummingMergeTree](summingmergetree.md) and
[AggregatingMergeTree](aggregatingmergetree.md) table engines. In a common case when using these engines, the table has two types of columns: *dimensions* and *measures*. Typical queries aggregate values of measure columns with arbitrary `GROUP BY` and filtering by dimensions. Because SummingMergeTree and AggregatingMergeTree aggregate rows with the same value of the sorting key, it is natural to add all dimensions to it. As a result, the key expression consists of a long list of columns and this list must be frequently updated with newly added dimensions.

In this case it makes sense to leave only a few columns in the primary key that will provide efficient range scans and add the remaining dimension columns to the sorting key tuple.

[ALTER](../../query_language/alter.md) of the sorting key is a lightweight operation because when a new column is simultaneously added to the table and to the sorting key, existing data parts don't need to be changed. Since the old sorting key is a prefix of the new sorting key and there is no data in the newly added column, the data is sorted by both the old and new sorting keys at the moment of table modification.

### Use of Indexes and Partitions in Queries

For `SELECT` queries, ClickHouse analyzes whether an index can be used. An index can be used if the `WHERE/PREWHERE` clause has an expression (as one of the conjunction elements, or entirely) that represents an equality or inequality comparison operation, or if it has `IN` or `LIKE` with a fixed prefix on columns or expressions that are in the primary key or partitioning key, or on certain partially repetitive functions of these columns, or logical relationships of these expressions.

Thus, it is possible to quickly run queries on one or many ranges of the primary key. In this example, queries will be fast when run for a specific tracking tag, for a specific tag and date range, for a specific tag and date, for multiple tags with a date range, and so on.

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

### Use of Index for Partially-Monotonic Primary Keys

Consider, for example, the days of the month. They form a [monotonic sequence](https://en.wikipedia.org/wiki/Monotonic_function) for one month, but not monotonic for more extended periods. This is a partially-monotonic sequence. If a user creates the table with partially-monotonic primary key, ClickHouse creates a sparse index as usual. When a user selects data from this kind of table, ClickHouse analyzes the query conditions. If the user wants to get data between two marks of the index and both these marks fall within one month, ClickHouse can use the index in this particular case because it can calculate the distance between the parameters of a query and index marks.

ClickHouse cannot use an index if the values of the primary key in the query parameter range don't represent a monotonic sequence. In this case, ClickHouse uses the full scan method.

ClickHouse uses this logic not only for days of the month sequences, but for any primary key that represents a partially-monotonic sequence.

### Data Skipping Indexes (Experimental) {#table_engine-mergetree-data_skipping-indexes}

You need to set `allow_experimental_data_skipping_indices` to 1 to use indices.  (run `SET allow_experimental_data_skipping_indices = 1`).

The index declaration is in the columns section of the `CREATE` query.
```sql
INDEX index_name expr TYPE type(...) GRANULARITY granularity_value
```

For tables from the `*MergeTree` family, data skipping indices can be specified.

These indices aggregate some information about the specified expression on blocks, which consist of `granularity_value` granules (the size of the granule is specified using the `index_granularity` setting in the table engine). Then these aggregates are used in `SELECT` queries for reducing the amount of data to read from the disk by skipping big blocks of data where the `where` query cannot be satisfied.


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

Indices from the example can be used by ClickHouse to reduce the amount of data to read from disk in the following queries:

```sql
SELECT count() FROM table WHERE s < 'z'
SELECT count() FROM table WHERE u64 * i32 == 10 AND u64 * length(s) >= 1234
```

#### Available Types of Indices

- `minmax`

    Stores extremes of the specified expression (if the expression is `tuple`, then it stores extremes for each element of `tuple`), uses stored info for skipping blocks of data like the primary key.

- `set(max_rows)`

    Stores unique values of the specified expression (no more than `max_rows` rows, `max_rows=0` means "no limits"). Uses the values to check if the `WHERE` expression is not satisfiable on a block of data.  

- `ngrambf_v1(n, size_of_bloom_filter_in_bytes, number_of_hash_functions, random_seed)`

    Stores a [bloom filter](https://en.wikipedia.org/wiki/Bloom_filter) that contains all ngrams from a block of data. Works only with strings. Can be used for optimization of `equals`, `like` and `in` expressions.

    - `n` — ngram size,
    - `size_of_bloom_filter_in_bytes` — Bloom filter size in bytes (you can use large values here, for example, 256 or 512, because it can be compressed well).
    - `number_of_hash_functions` — The number of hash functions used in the bloom filter.
    - `random_seed` — The seed for bloom filter hash functions.

- `tokenbf_v1(size_of_bloom_filter_in_bytes, number_of_hash_functions, random_seed)`

    The same as `ngrambf_v1`, but stores tokens instead of ngrams. Tokens are sequences separated by non-alphanumeric characters.

- `bloom_filter([false_positive])` — Stores [bloom filter](https://en.wikipedia.org/wiki/Bloom_filter) for the specified columns.

    The `false_positive` optional parameter is the probability of false positive response from the filter. Possible values: (0, 1). Default value: 0.025.

    Supported data types: `Int*`, `UInt*`, `Float*`, `Enum`, `Date`, `DateTime`, `String`, `FixedString`.

    Supported for the following functions: [equals](../../query_language/functions/comparison_functions.md), [notEquals](../../query_language/functions/comparison_functions.md), [in](../../query_language/functions/in_functions.md), [notIn](../../query_language/functions/in_functions.md).

```sql
INDEX sample_index (u64 * length(s)) TYPE minmax GRANULARITY 4
INDEX sample_index2 (u64 * length(str), i32 + f64 * 100, date, str) TYPE set(100) GRANULARITY 4
INDEX sample_index3 (lower(str), str) TYPE ngrambf_v1(3, 256, 2, 0) GRANULARITY 4
```

#### Functions Support

Conditions in the `WHERE` clause contain calls of functions over the columns. If the column is a part of some index, ClickHouse tries to use this index when performing the functions. ClickHouse supports different subset of functions for using indexes.

The `set` index can be used with all functions. Functions subsets for other indexes are in the table below.

Function (operator) / Index | primary key | minmax | ngrambf_v1 | tokenbf_v1 | bloom_filter
----------------------------|-------------|--------|------------|------------|---------------
[equals (=, ==)](../../query_language/functions/comparison_functions.md#function-equals) | ✔ | ✔ | ✔ | ✔ | ✔ 
[notEquals(!=, <>)](../../query_language/functions/comparison_functions.md#function-notequals) | ✔ | ✔ | ✔ | ✔ | ✔
[like](../../query_language/functions/string_search_functions.md#function-like) | ✔ | ✔ | ✔ | ✗ |  ✗
[notLike](../../query_language/functions/string_search_functions.md#function-notlike) | ✔ | ✔ | ✔ | ✔ | ✗
[startsWith](../../query_language/functions/string_functions.md#function-startswith) | ✔ | ✔ | ✔ | ✔ | ✗
[endsWith](../../query_language/functions/string_functions.md#function-endswith) | ✗ | ✗ | ✔ | ✔ |
[multiSearchAny](../../query_language/functions/string_search_functions.md#function-multisearchany) | ✗ | ✗ | ✔ | ✔ | ✗
[in](../../query_language/functions/in_functions.md#in-functions) | ✔ | ✔ | ✔ | ✔ | ✔ 
[notIn](../../query_language/functions/in_functions.md#in-functions) | ✔ | ✔ | ✔ | ✔ | ✔ 
[less (<)](../../query_language/functions/comparison_functions.md#function-less) | ✔ | ✔ | ✗ | ✗ | ✗
[greater (>)](../../query_language/functions/comparison_functions.md#function-greater) | ✔ | ✔ | ✗ | ✗ | ✗
[lessOrEquals (<=)](../../query_language/functions/comparison_functions.md#function-lessorequals) | ✔ | ✔ | ✗ | ✗ | ✗
[greaterOrEquals (>=)](../../query_language/functions/comparison_functions.md#function-greaterorequals) | ✔ | ✔ | ✗ | ✗ | ✗
[empty](../../query_language/functions/array_functions.md#function-empty)  | ✔ | ✔ | ✗ | ✗ | ✗
[notEmpty](../../query_language/functions/array_functions.md#function-notempty)  | ✔ | ✔ | ✗ | ✗ | ✗
hasToken  | ✗ | ✗ | ✗ | ✔ | ✗ 

Functions with a constant argument less than ngram size couldn't be used by `ngrambf_v1` for the query optimization.

Bloom filters can have false positive matches, so the `ngrambf_v1`, `tokenbf_v1`, `bloom_filter` indexes couldn't be used for optimizing queries where the result of a function is expected to be false, for example:

- Can be optimized:
    - `s LIKE '%test%'`
    - `NOT s NOT LIKE '%test%'`
    - `s = 1`
    - `NOT s != 1`
    - `startsWith(s, 'test')`
- Can't be optimized:
    - `NOT s LIKE '%test%'`
    - `s NOT LIKE '%test%'`
    - `NOT s = 1`
    - `s != 1`
    - `NOT startsWith(s, 'test')`

## Concurrent Data Access

For concurrent table access, we use multi-versioning. In other words, when a table is simultaneously read and updated, data is read from a set of parts that is current at the time of the query. There are no lengthy locks. Inserts do not get in the way of read operations.

Reading from a table is automatically parallelized.


## TTL for Columns and Tables {#table_engine-mergetree-ttl}

Determines the lifetime of values.

The `TTL` clause can be set for the whole table and for each individual column. If both `TTL` are set, ClickHouse uses that `TTL` which expires earlier.

The table must have the column in the [Date](../../data_types/date.md) or [DateTime](../../data_types/datetime.md) data type. To define the lifetime of data, use operations on this time column, for example:

```
TTL time_column
TTL time_column + interval
```

To define `interval`, use [time interval](../../query_language/operators.md#operators-datetime) operators.

```
TTL date_time + INTERVAL 1 MONTH
TTL date_time + INTERVAL 15 HOUR
```

**Column TTL**

When the values in the column expire, ClickHouse replaces them with the default values for the column data type. If all the column values in the data part expire, ClickHouse deletes this column from the data part in a filesystem.

The `TTL` clause can't be used for key columns.

Examples:

Creating a table with TTL

```sql
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

Adding TTL to a column of an existing table

```sql
ALTER TABLE example_table
    MODIFY COLUMN
    c String TTL d + INTERVAL 1 DAY;
```

Altering TTL of the column

```sql
ALTER TABLE example_table
    MODIFY COLUMN
    c String TTL d + INTERVAL 1 MONTH;
```

**Table TTL**

When data in a table expires, ClickHouse deletes all corresponding rows.

Examples:

Creating a table with TTL

```sql
CREATE TABLE example_table 
(
    d DateTime,
    a Int
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(d)
ORDER BY d
TTL d + INTERVAL 1 MONTH;
```

Altering TTL of the table

```sql
ALTER TABLE example_table
    MODIFY TTL d + INTERVAL 1 DAY;
```

**Removing Data**

Data with an expired TTL is removed when ClickHouse merges data parts.

When ClickHouse see that data is expired, it performs an off-schedule merge. To control the frequency of such merges, you can set [merge_with_ttl_timeout](#mergetree_setting-merge_with_ttl_timeout). If the value is too low, it will perform many off-schedule merges that may consume a lot of resources.

If you perform the `SELECT` query between merges, you may get expired data. To avoid it, use the [OPTIMIZE](../../query_language/misc.md#misc_operations-optimize) query before `SELECT`.

[Original article](https://clickhouse.yandex/docs/en/operations/table_engines/mergetree/) <!--hide-->
