---
description: '`MergeTree`-family table engines are designed for high data ingest rates
  and huge data volumes.'
sidebar_label: 'MergeTree'
sidebar_position: 11
slug: /engines/table-engines/mergetree-family/mergetree
title: 'MergeTree'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

# MergeTree

The `MergeTree` engine and other engines of the `MergeTree` family (e.g. `ReplacingMergeTree`, `AggregatingMergeTree` ) are the most commonly used and most robust table engines in ClickHouse.

`MergeTree`-family table engines are designed for high data ingest rates and huge data volumes.
Insert operations create table parts which are merged by a background process with other table parts.

Main features of `MergeTree`-family table engines.

- The table's primary key determines the sort order within each table part (clustered index). The primary key also does not reference individual rows but blocks of 8192 rows called granules. This makes primary keys of huge data sets small enough to remain loaded in main memory, while still providing fast access to on-disk data.

- Tables can be partitioned using an arbitrary partition expression. Partition pruning ensures partitions are omitted from reading when the query allows it.

- Data can be replicated across multiple cluster nodes for high availability, failover, and zero downtime upgrades. See [Data replication](/engines/table-engines/mergetree-family/replication.md).

- `MergeTree` table engines support various statistics kinds and sampling methods to help query optimization.

:::note
Despite a similar name, the [Merge](/engines/table-engines/special/merge) engine is different from `*MergeTree` engines.
:::

## Creating Tables {#table_engine-mergetree-creating-a-table}

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [[NOT] NULL] [DEFAULT|MATERIALIZED|ALIAS|EPHEMERAL expr1] [COMMENT ...] [CODEC(codec1)] [STATISTICS(stat1)] [TTL expr1] [PRIMARY KEY] [SETTINGS (name = value, ...)],
    name2 [type2] [[NOT] NULL] [DEFAULT|MATERIALIZED|ALIAS|EPHEMERAL expr2] [COMMENT ...] [CODEC(codec2)] [STATISTICS(stat2)] [TTL expr2] [PRIMARY KEY] [SETTINGS (name = value, ...)],
    ...
    INDEX index_name1 expr1 TYPE type1(...) [GRANULARITY value1],
    INDEX index_name2 expr2 TYPE type2(...) [GRANULARITY value2],
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
[SETTINGS name = value, ...]
```

For a detailed description of the parameters, see the [CREATE TABLE](/sql-reference/statements/create/table.md) statement

### Query Clauses {#mergetree-query-clauses}

#### ENGINE {#engine}

`ENGINE` — Name and parameters of the engine. `ENGINE = MergeTree()`. The `MergeTree` engine has no parameters.

#### ORDER_BY {#order_by}

`ORDER BY` — The sorting key.

A tuple of column names or arbitrary expressions. Example: `ORDER BY (CounterID + 1, EventDate)`.

If no primary key is defined (i.e. `PRIMARY KEY` was not specified), ClickHouse uses the the sorting key as primary key.

If no sorting is required, you can use syntax `ORDER BY tuple()`.
Alternatively, if setting `create_table_empty_primary_key_by_default` is enabled, `ORDER BY tuple()` is implicitly added to `CREATE TABLE` statements. See [Selecting a Primary Key](#selecting-a-primary-key).

#### PARTITION BY {#partition-by}

`PARTITION BY` — The [partitioning key](/engines/table-engines/mergetree-family/custom-partitioning-key.md). Optional. In most cases, you don't need a partition key, and if you do need to partition, generally you do not need a partition key more granular than by month. Partitioning does not speed up queries (in contrast to the ORDER BY expression). You should never use too granular partitioning. Don't partition your data by client identifiers or names (instead, make client identifier or name the first column in the ORDER BY expression).

For partitioning by month, use the `toYYYYMM(date_column)` expression, where `date_column` is a column with a date of the type [Date](/sql-reference/data-types/date.md). The partition names here have the `"YYYYMM"` format.

#### PRIMARY KEY {#primary-key}

`PRIMARY KEY` — The primary key if it [differs from the sorting key](#choosing-a-primary-key-that-differs-from-the-sorting-key). Optional.

Specifying a sorting key (using `ORDER BY` clause) implicitly specifies a primary key.
It is usually not necessary to specify the primary key in addition to the sorting key.

#### SAMPLE BY {#sample-by}

`SAMPLE BY` — A sampling expression. Optional.

If specified, it must be contained in the primary key.
The sampling expression must result in an unsigned integer.

Example: `SAMPLE BY intHash32(UserID) ORDER BY (CounterID, EventDate, intHash32(UserID))`.

####  TTL {#ttl}

`TTL` — A list of rules that specify the storage duration of rows and the logic of automatic parts movement [between disks and volumes](#table_engine-mergetree-multiple-volumes). Optional.

Expression must result in a `Date` or `DateTime`, e.g. `TTL date + INTERVAL 1 DAY`.

Type of the rule `DELETE|TO DISK 'xxx'|TO VOLUME 'xxx'|GROUP BY` specifies an action to be done with the part if the expression is satisfied (reaches current time): removal of expired rows, moving a part (if expression is satisfied for all rows in a part) to specified disk (`TO DISK 'xxx'`) or to volume (`TO VOLUME 'xxx'`), or aggregating values in expired rows. Default type of the rule is removal (`DELETE`). List of multiple rules can be specified, but there should be no more than one `DELETE` rule.


For more details, see [TTL for columns and tables](#table_engine-mergetree-ttl)

#### SETTINGS {#settings}

See [MergeTree Settings](../../../operations/settings/merge-tree-settings.md).

**Example of Sections Setting**

```sql
ENGINE MergeTree() PARTITION BY toYYYYMM(EventDate) ORDER BY (CounterID, EventDate, intHash32(UserID)) SAMPLE BY intHash32(UserID) SETTINGS index_granularity=8192
```

In the example, we set partitioning by month.

We also set an expression for sampling as a hash by the user ID. This allows you to pseudorandomize the data in the table for each `CounterID` and `EventDate`. If you define a [SAMPLE](/sql-reference/statements/select/sample) clause when selecting the data, ClickHouse will return an evenly pseudorandom data sample for a subset of users.

The `index_granularity` setting can be omitted because 8192 is the default value.

<details markdown="1">

<summary>Deprecated Method for Creating a Table</summary>

:::note
Do not use this method in new projects. If possible, switch old projects to the method described above.
:::

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE [=] MergeTree(date-column [, sampling_expression], (primary, key), index_granularity)
```

**MergeTree() Parameters**

- `date-column` — The name of a column of the [Date](/sql-reference/data-types/date.md) type. ClickHouse automatically creates partitions by month based on this column. The partition names are in the `"YYYYMM"` format.
- `sampling_expression` — An expression for sampling.
- `(primary, key)` — Primary key. Type: [Tuple()](/sql-reference/data-types/tuple.md)
- `index_granularity` — The granularity of an index. The number of data rows between the "marks" of an index. The value 8192 is appropriate for most tasks.

**Example**

```sql
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

Each data part is logically divided into granules. A granule is the smallest indivisible data set that ClickHouse reads when selecting data. ClickHouse does not split rows or values, so each granule always contains an integer number of rows. The first row of a granule is marked with the value of the primary key for the row. For each data part, ClickHouse creates an index file that stores the marks. For each column, whether it's in the primary key or not, ClickHouse also stores the same marks. These marks let you find data directly in column files.

The granule size is restricted by the `index_granularity` and `index_granularity_bytes` settings of the table engine. The number of rows in a granule lays in the `[1, index_granularity]` range, depending on the size of the rows. The size of a granule can exceed `index_granularity_bytes` if the size of a single row is greater than the value of the setting. In this case, the size of the granule equals the size of the row.

## Primary Keys and Indexes in Queries {#primary-keys-and-indexes-in-queries}

Take the `(CounterID, Date)` primary key as an example. In this case, the sorting and index can be illustrated as follows:

```text
Whole data:     [---------------------------------------------]
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

A sparse index allows extra data to be read. When reading a single range of the primary key, up to `index_granularity * 2` extra rows in each data block can be read.

Sparse indexes allow you to work with a very large number of table rows, because in most cases, such indexes fit in the computer's RAM.

ClickHouse does not require a unique primary key. You can insert multiple rows with the same primary key.

You can use `Nullable`-typed expressions in the `PRIMARY KEY` and `ORDER BY` clauses but it is strongly discouraged. To allow this feature, turn on the [allow_nullable_key](/operations/settings/merge-tree-settings/#allow_nullable_key) setting. The [NULLS_LAST](/sql-reference/statements/select/order-by.md/#sorting-of-special-values) principle applies for `NULL` values in the `ORDER BY` clause.

### Selecting a Primary Key {#selecting-a-primary-key}

The number of columns in the primary key is not explicitly limited. Depending on the data structure, you can include more or fewer columns in the primary key. This may:

- Improve the performance of an index.

    If the primary key is `(a, b)`, then adding another column `c` will improve the performance if the following conditions are met:

    - There are queries with a condition on column `c`.
    - Long data ranges (several times longer than the `index_granularity`) with identical values for `(a, b)` are common. In other words, when adding another column allows you to skip quite long data ranges.

- Improve data compression.

    ClickHouse sorts data by primary key, so the higher the consistency, the better the compression.

- Provide additional logic when merging data parts in the [CollapsingMergeTree](/engines/table-engines/mergetree-family/collapsingmergetree) and [SummingMergeTree](/engines/table-engines/mergetree-family/summingmergetree.md) engines.

    In this case it makes sense to specify the *sorting key* that is different from the primary key.

A long primary key will negatively affect the insert performance and memory consumption, but extra columns in the primary key do not affect ClickHouse performance during `SELECT` queries.

You can create a table without a primary key using the `ORDER BY tuple()` syntax. In this case, ClickHouse stores data in the order of inserting. If you want to save data order when inserting data by `INSERT ... SELECT` queries, set [max_insert_threads = 1](/operations/settings/settings#max_insert_threads).

To select data in the initial order, use [single-threaded](/operations/settings/settings.md/#max_threads) `SELECT` queries.

### Choosing a Primary Key that Differs from the Sorting Key {#choosing-a-primary-key-that-differs-from-the-sorting-key}

It is possible to specify a primary key (an expression with values that are written in the index file for each mark) that is different from the sorting key (an expression for sorting the rows in data parts). In this case the primary key expression tuple must be a prefix of the sorting key expression tuple.

This feature is helpful when using the [SummingMergeTree](/engines/table-engines/mergetree-family/summingmergetree.md) and
[AggregatingMergeTree](/engines/table-engines/mergetree-family/aggregatingmergetree.md) table engines. In a common case when using these engines, the table has two types of columns: *dimensions* and *measures*. Typical queries aggregate values of measure columns with arbitrary `GROUP BY` and filtering by dimensions. Because SummingMergeTree and AggregatingMergeTree aggregate rows with the same value of the sorting key, it is natural to add all dimensions to it. As a result, the key expression consists of a long list of columns and this list must be frequently updated with newly added dimensions.

In this case it makes sense to leave only a few columns in the primary key that will provide efficient range scans and add the remaining dimension columns to the sorting key tuple.

[ALTER](/sql-reference/statements/alter/index.md) of the sorting key is a lightweight operation because when a new column is simultaneously added to the table and to the sorting key, existing data parts do not need to be changed. Since the old sorting key is a prefix of the new sorting key and there is no data in the newly added column, the data is sorted by both the old and new sorting keys at the moment of table modification.

### Use of Indexes and Partitions in Queries {#use-of-indexes-and-partitions-in-queries}

For `SELECT` queries, ClickHouse analyzes whether an index can be used. An index can be used if the `WHERE/PREWHERE` clause has an expression (as one of the conjunction elements, or entirely) that represents an equality or inequality comparison operation, or if it has `IN` or `LIKE` with a fixed prefix on columns or expressions that are in the primary key or partitioning key, or on certain partially repetitive functions of these columns, or logical relationships of these expressions.

Thus, it is possible to quickly run queries on one or many ranges of the primary key. In this example, queries will be fast when run for a specific tracking tag, for a specific tag and date range, for a specific tag and date, for multiple tags with a date range, and so on.

Let's look at the engine configured as follows:
```sql
ENGINE MergeTree()
PARTITION BY toYYYYMM(EventDate)
ORDER BY (CounterID, EventDate)
SETTINGS index_granularity=8192
```

In this case, in queries:

```sql
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

The queries above show that the index is used even for complex expressions. Reading from the table is organized so that using the index can't be slower than a full scan.

In the example below, the index can't be used.

```sql
SELECT count() FROM table WHERE CounterID = 34 OR URL LIKE '%upyachka%'
```

To check whether ClickHouse can use the index when running a query, use the settings [force_index_by_date](/operations/settings/settings.md/#force_index_by_date) and [force_primary_key](/operations/settings/settings#force_primary_key).

The key for partitioning by month allows reading only those data blocks which contain dates from the proper range. In this case, the data block may contain data for many dates (up to an entire month). Within a block, data is sorted by primary key, which might not contain the date as the first column. Because of this, using a query with only a date condition that does not specify the primary key prefix will cause more data to be read than for a single date.

### Use of Index for Partially-monotonic Primary Keys {#use-of-index-for-partially-monotonic-primary-keys}

Consider, for example, the days of the month. They form a [monotonic sequence](https://en.wikipedia.org/wiki/Monotonic_function) for one month, but not monotonic for more extended periods. This is a partially-monotonic sequence. If a user creates the table with partially-monotonic primary key, ClickHouse creates a sparse index as usual. When a user selects data from this kind of table, ClickHouse analyzes the query conditions. If the user wants to get data between two marks of the index and both these marks fall within one month, ClickHouse can use the index in this particular case because it can calculate the distance between the parameters of a query and index marks.

ClickHouse cannot use an index if the values of the primary key in the query parameter range do not represent a monotonic sequence. In this case, ClickHouse uses the full scan method.

ClickHouse uses this logic not only for days of the month sequences, but for any primary key that represents a partially-monotonic sequence.

### Data Skipping Indexes {#table_engine-mergetree-data_skipping-indexes}

The index declaration is in the columns section of the `CREATE` query.

```sql
INDEX index_name expr TYPE type(...) [GRANULARITY granularity_value]
```

For tables from the `*MergeTree` family, data skipping indices can be specified.

These indices aggregate some information about the specified expression on blocks, which consist of `granularity_value` granules (the size of the granule is specified using the `index_granularity` setting in the table engine). Then these aggregates are used in `SELECT` queries for reducing the amount of data to read from the disk by skipping big blocks of data where the `where` query cannot be satisfied.

The `GRANULARITY` clause can be omitted, the default value of `granularity_value` is 1.

**Example**

```sql
CREATE TABLE table_name
(
    u64 UInt64,
    i32 Int32,
    s String,
    ...
    INDEX idx1 u64 TYPE bloom_filter GRANULARITY 3,
    INDEX idx2 u64 * i32 TYPE minmax GRANULARITY 3,
    INDEX idx3 u64 * length(s) TYPE set(1000) GRANULARITY 4
) ENGINE = MergeTree()
...
```

Indices from the example can be used by ClickHouse to reduce the amount of data to read from disk in the following queries:

```sql
SELECT count() FROM table WHERE u64 == 10;
SELECT count() FROM table WHERE u64 * i32 >= 1234
SELECT count() FROM table WHERE u64 * length(s) == 1234
```

Data skipping indexes can also be created on composite columns:

```sql
-- on columns of type Map:
INDEX map_key_index mapKeys(map_column) TYPE bloom_filter
INDEX map_value_index mapValues(map_column) TYPE bloom_filter

-- on columns of type Tuple:
INDEX tuple_1_index tuple_column.1 TYPE bloom_filter
INDEX tuple_2_index tuple_column.2 TYPE bloom_filter

-- on columns of type Nested:
INDEX nested_1_index col.nested_col1 TYPE bloom_filter
INDEX nested_2_index col.nested_col2 TYPE bloom_filter
```

### Available Types of Indices {#available-types-of-indices}

#### MinMax {#minmax}

Stores extremes of the specified expression (if the expression is `tuple`, then it stores extremes for each element of `tuple`), uses stored info for skipping blocks of data like the primary key.

Syntax: `minmax`

#### Set {#set}

Stores unique values of the specified expression (no more than `max_rows` rows, `max_rows=0` means "no limits"). Uses the values to check if the `WHERE` expression is not satisfiable on a block of data.

Syntax: `set(max_rows)`

#### Bloom Filter {#bloom-filter}

Stores a [Bloom filter](https://en.wikipedia.org/wiki/Bloom_filter) for the specified columns. An optional `false_positive` parameter with possible values between 0 and 1 specifies the probability of receiving a false positive response from the filter. Default value: 0.025. Supported data types: `Int*`, `UInt*`, `Float*`, `Enum`, `Date`, `DateTime`, `String`, `FixedString`, `Array`, `LowCardinality`, `Nullable`, `UUID` and `Map`. For the `Map` data type, the client can specify if the index should be created for keys or values using [mapKeys](/sql-reference/functions/tuple-map-functions.md/#mapkeys) or [mapValues](/sql-reference/functions/tuple-map-functions.md/#mapvalues) function.

Syntax: `bloom_filter([false_positive])`

#### N-gram Bloom Filter {#n-gram-bloom-filter}

Stores a [Bloom filter](https://en.wikipedia.org/wiki/Bloom_filter) that contains all n-grams from a block of data. Only works with datatypes: [String](/sql-reference/data-types/string.md), [FixedString](/sql-reference/data-types/fixedstring.md) and [Map](/sql-reference/data-types/map.md). Can be used for optimization of `EQUALS`, `LIKE` and `IN` expressions.

Syntax: `ngrambf_v1(n, size_of_bloom_filter_in_bytes, number_of_hash_functions, random_seed)`

- `n` — ngram size,
- `size_of_bloom_filter_in_bytes` — Bloom filter size in bytes (you can use large values here, for example, 256 or 512, because it can be compressed well).
- `number_of_hash_functions` — The number of hash functions used in the Bloom filter.
- `random_seed` — The seed for Bloom filter hash functions.

Users can create [UDF](/sql-reference/statements/create/function.md) to estimate the parameters set of `ngrambf_v1`. Query statements are as follows:

```sql
CREATE FUNCTION bfEstimateFunctions [ON CLUSTER cluster]
AS
(total_number_of_all_grams, size_of_bloom_filter_in_bits) -> round((size_of_bloom_filter_in_bits / total_number_of_all_grams) * log(2));

CREATE FUNCTION bfEstimateBmSize [ON CLUSTER cluster]
AS
(total_number_of_all_grams,  probability_of_false_positives) -> ceil((total_number_of_all_grams * log(probability_of_false_positives)) / log(1 / pow(2, log(2))));

CREATE FUNCTION bfEstimateFalsePositive [ON CLUSTER cluster]
AS
(total_number_of_all_grams, number_of_hash_functions, size_of_bloom_filter_in_bytes) -> pow(1 - exp(-number_of_hash_functions/ (size_of_bloom_filter_in_bytes / total_number_of_all_grams)), number_of_hash_functions);

CREATE FUNCTION bfEstimateGramNumber [ON CLUSTER cluster]
AS
(number_of_hash_functions, probability_of_false_positives, size_of_bloom_filter_in_bytes) -> ceil(size_of_bloom_filter_in_bytes / (-number_of_hash_functions / log(1 - exp(log(probability_of_false_positives) / number_of_hash_functions))))

```
To use those functions,we need to specify two parameter at least.
For example, if there 4300 ngrams in the granule and we expect false positives to be less than 0.0001. The other parameters can be estimated by executing following queries:


```sql
--- estimate number of bits in the filter
SELECT bfEstimateBmSize(4300, 0.0001) / 8 as size_of_bloom_filter_in_bytes;

┌─size_of_bloom_filter_in_bytes─┐
│                         10304 │
└───────────────────────────────┘

--- estimate number of hash functions
SELECT bfEstimateFunctions(4300, bfEstimateBmSize(4300, 0.0001)) as number_of_hash_functions

┌─number_of_hash_functions─┐
│                       13 │
└──────────────────────────┘

```
Of course, you can also use those functions to estimate parameters by other conditions.
The functions refer to the content [here](https://hur.st/bloomfilter).


#### Token Bloom Filter {#token-bloom-filter}

The same as `ngrambf_v1`, but stores tokens instead of ngrams. Tokens are sequences separated by non-alphanumeric characters.

Syntax: `tokenbf_v1(size_of_bloom_filter_in_bytes, number_of_hash_functions, random_seed)`

#### Special-purpose {#special-purpose}

- An experimental index to support approximate nearest neighbor search. See [here](annindexes.md) for details.
- An experimental text index to support full-text search. See [here](invertedindexes.md) for details.

### Functions Support {#functions-support}

Conditions in the `WHERE` clause contains calls of the functions that operate with columns. If the column is a part of an index, ClickHouse tries to use this index when performing the functions. ClickHouse supports different subsets of functions for using indexes.

Indexes of type `set` can be utilized by all functions. The other index types are supported as follows:

| Function (operator) / Index                                                                                | primary key | minmax | ngrambf_v1 | tokenbf_v1 | bloom_filter | text |
|------------------------------------------------------------------------------------------------------------|-------------|--------|------------|------------|--------------|------|
| [equals (=, ==)](/sql-reference/functions/comparison-functions.md/#equals)                                 | ✔           | ✔      | ✔          | ✔          | ✔            | ✔    |
| [notEquals(!=, &lt;&gt;)](/sql-reference/functions/comparison-functions.md/#notEquals)                     | ✔           | ✔      | ✔          | ✔          | ✔            | ✔    |
| [like](/sql-reference/functions/string-search-functions.md/#like)                                          | ✔           | ✔      | ✔          | ✔          | ✗            | ✔    |
| [notLike](/sql-reference/functions/string-search-functions.md/#notlike)                                    | ✔           | ✔      | ✔          | ✔          | ✗            | ✔    |
| [match](/sql-reference/functions/string-search-functions.md/#match)                                        | ✗           | ✗      | ✔          | ✔          | ✗            | ✔    |
| [startsWith](/sql-reference/functions/string-functions.md/#startswith)                                     | ✔           | ✔      | ✔          | ✔          | ✗            | ✔    |
| [endsWith](/sql-reference/functions/string-functions.md/#endswith)                                         | ✗           | ✗      | ✔          | ✔          | ✗            | ✔    |
| [multiSearchAny](/sql-reference/functions/string-search-functions.md/#multisearchany)                      | ✗           | ✗      | ✔          | ✗          | ✗            | ✔    |
| [in](/sql-reference/functions/in-functions)                                                                | ✔           | ✔      | ✔          | ✔          | ✔            | ✔    |
| [notIn](/sql-reference/functions/in-functions)                                                             | ✔           | ✔      | ✔          | ✔          | ✔            | ✔    |
| [less (`<`)](/sql-reference/functions/comparison-functions.md/#less)                                       | ✔           | ✔      | ✗          | ✗          | ✗            | ✗    |
| [greater (`>`)](/sql-reference/functions/comparison-functions.md/#greater)                                 | ✔           | ✔      | ✗          | ✗          | ✗            | ✗    |
| [lessOrEquals (`<=`)](/sql-reference/functions/comparison-functions.md/#lessOrEquals)                      | ✔           | ✔      | ✗          | ✗          | ✗            | ✗    |
| [greaterOrEquals (`>=`)](/sql-reference/functions/comparison-functions.md/#greaterOrEquals)                | ✔           | ✔      | ✗          | ✗          | ✗            | ✗    |
| [empty](/sql-reference/functions/array-functions/#empty)                                                   | ✔           | ✔      | ✗          | ✗          | ✗            | ✗    |
| [notEmpty](/sql-reference/functions/array-functions/#notEmpty)                                             | ✔           | ✔      | ✗          | ✗          | ✗            | ✗    |
| [has](/sql-reference/functions/array-functions#has)                                                        | ✗           | ✗      | ✔          | ✔          | ✔            | ✔    |
| [hasAny](/sql-reference/functions/array-functions#hasAny)                                                  | ✗           | ✗      | ✔          | ✔          | ✔            | ✗    |
| [hasAll](/sql-reference/functions/array-functions#hasAll)                                                  | ✗           | ✗      | ✔          | ✔          | ✔            | ✗    |
| hasToken                                                                                                   | ✗           | ✗      | ✗          | ✔          | ✗            | ✔    |
| hasTokenOrNull                                                                                             | ✗           | ✗      | ✗          | ✔          | ✗            | ✔    |
| hasTokenCaseInsensitive (*)                                                                                | ✗           | ✗      | ✗          | ✔          | ✗            | ✗    |
| hasTokenCaseInsensitiveOrNull (*)                                                                          | ✗           | ✗      | ✗          | ✔          | ✗            | ✗    |

Functions with a constant argument that is less than ngram size can't be used by `ngrambf_v1` for query optimization.

(*) For `hasTokenCaseInsensitive` and `hasTokenCaseInsensitiveOrNull` to be effective, the `tokenbf_v1` index must be created on lowercased data, for example `INDEX idx (lower(str_col)) TYPE tokenbf_v1(512, 3, 0)`.

:::note
Bloom filters can have false positive matches, so the `ngrambf_v1`, `tokenbf_v1`, and `bloom_filter` indexes can not be used for optimizing queries where the result of a function is expected to be false.

For example:

- Can be optimized:
    - `s LIKE '%test%'`
    - `NOT s NOT LIKE '%test%'`
    - `s = 1`
    - `NOT s != 1`
    - `startsWith(s, 'test')`
- Can not be optimized:
    - `NOT s LIKE '%test%'`
    - `s NOT LIKE '%test%'`
    - `NOT s = 1`
    - `s != 1`
    - `NOT startsWith(s, 'test')`
:::


## Projections {#projections}
Projections are like [materialized views](/sql-reference/statements/create/view) but defined in part-level. It provides consistency guarantees along with automatic usage in queries.

:::note
When you are implementing projections you should also consider the [force_optimize_projection](/operations/settings/settings#force_optimize_projection) setting.
:::

Projections are not supported in the `SELECT` statements with the [FINAL](/sql-reference/statements/select/from#final-modifier) modifier.

### Projection Query {#projection-query}
A projection query is what defines a projection. It implicitly selects data from the parent table.
**Syntax**

```sql
SELECT <column list expr> [GROUP BY] <group keys expr> [ORDER BY] <expr>
```

Projections can be modified or dropped with the [ALTER](/sql-reference/statements/alter/projection.md) statement.

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

Expressions must evaluate to [Date](/sql-reference/data-types/date.md), [Date32](/sql-reference/data-types/date32.md), [DateTime](/sql-reference/data-types/datetime.md) or [DateTime64](/sql-reference/data-types/datetime64.md) data type.

**Syntax**

Setting time-to-live for a column:

```sql
TTL time_column
TTL time_column + interval
```

To define `interval`, use [time interval](/sql-reference/operators#operators-for-working-with-dates-and-times) operators, for example:

```sql
TTL date_time + INTERVAL 1 MONTH
TTL date_time + INTERVAL 15 HOUR
```

### Column TTL {#mergetree-column-ttl}

When the values in the column expire, ClickHouse replaces them with the default values for the column data type. If all the column values in the data part expire, ClickHouse deletes this column from the data part in a filesystem.

The `TTL` clause can't be used for key columns.

**Examples**

#### Creating a table with `TTL`: {#creating-a-table-with-ttl}

```sql
CREATE TABLE tab
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

#### Adding TTL to a column of an existing table {#adding-ttl-to-a-column-of-an-existing-table}

```sql
ALTER TABLE tab
    MODIFY COLUMN
    c String TTL d + INTERVAL 1 DAY;
```

#### Altering TTL of the column {#altering-ttl-of-the-column}

```sql
ALTER TABLE tab
    MODIFY COLUMN
    c String TTL d + INTERVAL 1 MONTH;
```

### Table TTL {#mergetree-table-ttl}

Table can have an expression for removal of expired rows, and multiple expressions for automatic move of parts between [disks or volumes](#table_engine-mergetree-multiple-volumes). When rows in the table expire, ClickHouse deletes all corresponding rows. For parts moving or recompressing, all rows of a part must satisfy the `TTL` expression criteria.

```sql
TTL expr
    [DELETE|RECOMPRESS codec_name1|TO DISK 'xxx'|TO VOLUME 'xxx'][, DELETE|RECOMPRESS codec_name2|TO DISK 'aaa'|TO VOLUME 'bbb'] ...
    [WHERE conditions]
    [GROUP BY key_expr [SET v1 = aggr_func(v1) [, v2 = aggr_func(v2) ...]] ]
```

Type of TTL rule may follow each TTL expression. It affects an action which is to be done once the expression is satisfied (reaches current time):

- `DELETE` - delete expired rows (default action);
- `RECOMPRESS codec_name` - recompress data part with the `codec_name`;
- `TO DISK 'aaa'` - move part to the disk `aaa`;
- `TO VOLUME 'bbb'` - move part to the disk `bbb`;
- `GROUP BY` - aggregate expired rows.

`DELETE` action can be used together with `WHERE` clause to delete only some of the expired rows based on a filtering condition:
```sql
TTL time_column + INTERVAL 1 MONTH DELETE WHERE column = 'value'
```

`GROUP BY` expression must be a prefix of the table primary key.

If a column is not part of the `GROUP BY` expression and is not set explicitly in the `SET` clause, in result row it contains an occasional value from the grouped rows (as if aggregate function `any` is applied to it).

**Examples**

#### Creating a table with `TTL`: {#creating-a-table-with-ttl-1}

```sql
CREATE TABLE tab
(
    d DateTime,
    a Int
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(d)
ORDER BY d
TTL d + INTERVAL 1 MONTH DELETE,
    d + INTERVAL 1 WEEK TO VOLUME 'aaa',
    d + INTERVAL 2 WEEK TO DISK 'bbb';
```

#### Altering `TTL` of the table: {#altering-ttl-of-the-table}

```sql
ALTER TABLE tab
    MODIFY TTL d + INTERVAL 1 DAY;
```

Creating a table, where the rows are expired after one month. The expired rows where dates are Mondays are deleted:

```sql
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

#### Creating a table, where expired rows are recompressed: {#creating-a-table-where-expired-rows-are-recompressed}

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

Creating a table, where expired rows are aggregated. In result rows `x` contains the maximum value across the grouped rows, `y` — the minimum value, and `d` — any occasional value from grouped rows.

```sql
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

If you perform the `SELECT` query between merges, you may get expired data. To avoid it, use the [OPTIMIZE](/sql-reference/statements/optimize.md) query before `SELECT`.

**See Also**

- [ttl_only_drop_parts](/operations/settings/merge-tree-settings#ttl_only_drop_parts) setting

## Disk types {#disk-types}

In addition to local block devices, ClickHouse supports these storage types:
- [`s3` for S3 and MinIO](#table_engine-mergetree-s3)
- [`gcs` for GCS](/integrations/data-ingestion/gcs/index.md/#creating-a-disk)
- [`blob_storage_disk` for Azure Blob Storage](/operations/storing-data#azure-blob-storage)
- [`hdfs` for HDFS](/engines/table-engines/integrations/hdfs)
- [`web` for read-only from web](/operations/storing-data#web-storage)
- [`cache` for local caching](/operations/storing-data#using-local-cache)
- [`s3_plain` for backups to S3](/operations/backup#backuprestore-using-an-s3-disk)
- [`s3_plain_rewritable` for immutable, non-replicated tables in S3](/operations/storing-data.md#s3-plain-rewritable-storage)

## Using Multiple Block Devices for Data Storage {#table_engine-mergetree-multiple-volumes}

### Introduction {#introduction}

`MergeTree` family table engines can store data on multiple block devices. For example, it can be useful when the data of a certain table are implicitly split into "hot" and "cold". The most recent data is regularly requested but requires only a small amount of space. On the contrary, the fat-tailed historical data is requested rarely. If several disks are available, the "hot" data may be located on fast disks (for example, NVMe SSDs or in memory), while the "cold" data - on relatively slow ones (for example, HDD).

Data part is the minimum movable unit for `MergeTree`-engine tables. The data belonging to one part are stored on one disk. Data parts can be moved between disks in the background (according to user settings) as well as by means of the [ALTER](/sql-reference/statements/alter/partition) queries.

### Terms {#terms}

- Disk — Block device mounted to the filesystem.
- Default disk — Disk that stores the path specified in the [path](/operations/server-configuration-parameters/settings.md/#path) server setting.
- Volume — Ordered set of equal disks (similar to [JBOD](https://en.wikipedia.org/wiki/Non-RAID_drive_architectures)).
- Storage policy — Set of volumes and the rules for moving data between them.

The names given to the described entities can be found in the system tables, [system.storage_policies](/operations/system-tables/storage_policies) and [system.disks](/operations/system-tables/disks). To apply one of the configured storage policies for a table, use the `storage_policy` setting of `MergeTree`-engine family tables.

### Configuration {#table_engine-mergetree-multiple-volumes_configure}

Disks, volumes and storage policies should be declared inside the `<storage_configuration>` tag either in a file in the `config.d` directory.

:::tip
Disks can also be declared in the `SETTINGS` section of a query.  This is useful
for ad-hoc analysis to temporarily attach a disk that is, for example, hosted at a URL.
See [dynamic storage](/operations/storing-data#dynamic-configuration) for more details.
:::

Configuration structure:

```xml
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

- `<disk_name_N>` — Disk name. Names must be different for all disks.
- `path` — path under which a server will store data (`data` and `shadow` folders), should be terminated with '/'.
- `keep_free_space_bytes` — the amount of free disk space to be reserved.

The order of the disk definition is not important.

Storage policies configuration markup:

```xml
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

- `policy_name_N` — Policy name. Policy names must be unique.
- `volume_name_N` — Volume name. Volume names must be unique.
- `disk` — a disk within a volume.
- `max_data_part_size_bytes` — the maximum size of a part that can be stored on any of the volume's disks. If the a size of a merged part estimated to be bigger than `max_data_part_size_bytes` then this part will be written to a next volume. Basically this feature allows to keep new/small parts on a hot (SSD) volume and move them to a cold (HDD) volume when they reach large size. Do not use this setting if your policy has only one volume.
- `move_factor` — when the amount of available space gets lower than this factor, data automatically starts to move on the next volume if any (by default, 0.1). ClickHouse sorts existing parts by size from largest to smallest (in descending order) and selects parts with the total size that is sufficient to meet the `move_factor` condition. If the total size of all parts is insufficient, all parts will be moved.
- `perform_ttl_move_on_insert` — Disables TTL move on data part INSERT. By default (if enabled) if we insert a data part that already expired by the TTL move rule it immediately goes to a volume/disk declared in move rule. This can significantly slowdown insert in case if destination volume/disk is slow (e.g. S3). If disabled then already expired data part is written into a default volume and then right after moved to TTL volume.
- `load_balancing` - Policy for disk balancing, `round_robin` or `least_used`.
- `least_used_ttl_ms` - Configure timeout (in milliseconds) for the updating available space on all disks (`0` - update always, `-1` - never update, default is `60000`). Note, if the disk can be used by ClickHouse only and is not subject to a online filesystem resize/shrink you can use `-1`, in all other cases it is not recommended, since eventually it will lead to incorrect space distribution.
- `prefer_not_to_merge` — You should not use this setting. Disables merging of data parts on this volume (this is harmful and leads to performance degradation). When this setting is enabled (don't do it), merging data on this volume is not allowed (which is bad). This allows (but you don't need it) controlling (if you want to control something, you're making a mistake) how ClickHouse works with slow disks (but ClickHouse knows better, so please don't use this setting).
- `volume_priority` — Defines the priority (order) in which volumes are filled. Lower value means higher priority. The parameter values should be natural numbers and collectively cover the range from 1 to N (lowest priority given) without skipping any numbers.
  * If _all_ volumes are tagged, they are prioritized in given order.
  * If only _some_ volumes are tagged, those without the tag have the lowest priority, and they are prioritized in the order they are defined in config.
  * If _no_ volumes are tagged, their priority is set correspondingly to their order they are declared in configuration.
  * Two volumes cannot have the same priority value.

Configuration examples:

```xml
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

The order of volume enumeration within a storage policy is important in case at least one of the volumes listed has no explicit `volume_priority` parameter.
Once a volume is overfilled, data are moved to the next one. The order of disk enumeration is important as well because data are stored on them in turns.

When creating a table, one can apply one of the configured storage policies to it:

```sql
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

The number of threads performing background moves of data parts can be changed by [background_move_pool_size](/operations/server-configuration-parameters/settings.md/#background_move_pool_size) setting.

### Details {#details}

In the case of `MergeTree` tables, data is getting to disk in different ways:

- As a result of an insert (`INSERT` query).
- During background merges and [mutations](/sql-reference/statements/alter#mutations).
- When downloading from another replica.
- As a result of partition freezing [ALTER TABLE ... FREEZE PARTITION](/sql-reference/statements/alter/partition#freeze-partition).

In all these cases except for mutations and partition freezing, a part is stored on a volume and a disk according to the given storage policy:

1.  The first volume (in the order of definition) that has enough disk space for storing a part (`unreserved_space > current_part_size`) and allows for storing parts of a given size (`max_data_part_size_bytes > current_part_size`) is chosen.
2.  Within this volume, that disk is chosen that follows the one, which was used for storing the previous chunk of data, and that has free space more than the part size (`unreserved_space - keep_free_space_bytes > current_part_size`).

Under the hood, mutations and partition freezing make use of [hard links](https://en.wikipedia.org/wiki/Hard_link). Hard links between different disks are not supported, therefore in such cases the resulting parts are stored on the same disks as the initial ones.

In the background, parts are moved between volumes on the basis of the amount of free space (`move_factor` parameter) according to the order the volumes are declared in the configuration file.
Data is never transferred from the last one and into the first one. One may use system tables [system.part_log](/operations/system-tables/part_log) (field `type = MOVE_PART`) and [system.parts](/operations/system-tables/parts.md) (fields `path` and `disk`) to monitor background moves. Also, the detailed information can be found in server logs.

User can force moving a part or a partition from one volume to another using the query [ALTER TABLE ... MOVE PART\|PARTITION ... TO VOLUME\|DISK ...](/sql-reference/statements/alter/partition), all the restrictions for background operations are taken into account. The query initiates a move on its own and does not wait for background operations to be completed. User will get an error message if not enough free space is available or if any of the required conditions are not met.

Moving data does not interfere with data replication. Therefore, different storage policies can be specified for the same table on different replicas.

After the completion of background merges and mutations, old parts are removed only after a certain amount of time (`old_parts_lifetime`).
During this time, they are not moved to other volumes or disks. Therefore, until the parts are finally removed, they are still taken into account for evaluation of the occupied disk space.

User can assign new big parts to different disks of a [JBOD](https://en.wikipedia.org/wiki/Non-RAID_drive_architectures) volume in a balanced way using the [min_bytes_to_rebalance_partition_over_jbod](/operations/settings/merge-tree-settings.md/#min_bytes_to_rebalance_partition_over_jbod) setting.

## Using External Storage for Data Storage {#table_engine-mergetree-s3}

[MergeTree](/engines/table-engines/mergetree-family/mergetree.md) family table engines can store data to `S3`, `AzureBlobStorage`, `HDFS` using a disk with types `s3`, `azure_blob_storage`, `hdfs` accordingly. See [configuring external storage options](/operations/storing-data.md/#configuring-external-storage) for more details.

Example for [S3](https://aws.amazon.com/s3/) as external storage using a disk with type `s3`.

Configuration markup:
```xml
<storage_configuration>
    ...
    <disks>
        <s3>
            <type>s3</type>
            <support_batch_delete>true</support_batch_delete>
            <endpoint>https://clickhouse-public-datasets.s3.amazonaws.com/my-bucket/root-path/</endpoint>
            <access_key_id>your_access_key_id</access_key_id>
            <secret_access_key>your_secret_access_key</secret_access_key>
            <region></region>
            <header>Authorization: Bearer SOME-TOKEN</header>
            <server_side_encryption_customer_key_base64>your_base64_encoded_customer_key</server_side_encryption_customer_key_base64>
            <server_side_encryption_kms_key_id>your_kms_key_id</server_side_encryption_kms_key_id>
            <server_side_encryption_kms_encryption_context>your_kms_encryption_context</server_side_encryption_kms_encryption_context>
            <server_side_encryption_kms_bucket_key_enabled>true</server_side_encryption_kms_bucket_key_enabled>
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
            <skip_access_check>false</skip_access_check>
        </s3>
        <s3_cache>
            <type>cache</type>
            <disk>s3</disk>
            <path>/var/lib/clickhouse/disks/s3_cache/</path>
            <max_size>10Gi</max_size>
        </s3_cache>
    </disks>
    ...
</storage_configuration>
```

Also see [configuring external storage options](/operations/storing-data.md/#configuring-external-storage).

:::note cache configuration
ClickHouse versions 22.3 through 22.7 use a different cache configuration, see [using local cache](/operations/storing-data.md/#using-local-cache) if you are using one of those versions.
:::

## Virtual Columns {#virtual-columns}

- `_part` — Name of a part.
- `_part_index` — Sequential index of the part in the query result.
- `_part_starting_offset` — Cumulative starting row of the part in the query result.
- `_part_offset` — Number of row in the part.
- `_part_granule_offset` — Number of granule in the part.
- `_partition_id` — Name of a partition.
- `_part_uuid` — Unique part identifier (if enabled MergeTree setting `assign_part_uuids`).
- `_part_data_version` — Data version of part (either min block number or mutation version).
- `_partition_value` — Values (a tuple) of a `partition by` expression.
- `_sample_factor` — Sample factor (from the query).
- `_block_number` — Block number of the row, it is persisted on merges when `allow_experimental_block_number_column` is set to true.
- `_disk_name` — Disk name used for the storage.

## Column Statistics {#column-statistics}

<ExperimentalBadge/>
<CloudNotSupportedBadge/>

The statistics declaration is in the columns section of the `CREATE` query for tables from the `*MergeTree*` Family when we enable `set allow_experimental_statistics = 1`.

```sql
CREATE TABLE tab
(
    a Int64 STATISTICS(TDigest, Uniq),
    b Float64
)
ENGINE = MergeTree
ORDER BY a
```

We can also manipulate statistics with `ALTER` statements.

```sql
ALTER TABLE tab ADD STATISTICS b TYPE TDigest, Uniq;
ALTER TABLE tab DROP STATISTICS a;
```

These lightweight statistics aggregate information about distribution of values in columns. Statistics are stored in every part and updated when every insert comes.
They can be used for prewhere optimization only if we enable `set allow_statistics_optimize = 1`.

### Available Types of Column Statistics {#available-types-of-column-statistics}

- `MinMax`

    The minimum and maximum column value which allows to estimate the selectivity of range filters on numeric columns.

    Syntax: `minmax`

- `TDigest`

    [TDigest](https://github.com/tdunning/t-digest) sketches which allow to compute approximate percentiles (e.g. the 90th percentile) for numeric columns.

    Syntax: `tdigest`

- `Uniq`

    [HyperLogLog](https://en.wikipedia.org/wiki/HyperLogLog) sketches which provide an estimation how many distinct values a column contains.

    Syntax: `uniq`

- `CountMin`

    [CountMin](https://en.wikipedia.org/wiki/Count%E2%80%93min_sketch) sketches which provide an approximate count of the frequency of each value in a column.

    Syntax `countmin`


### Supported Data Types {#supported-data-types}

|           | (U)Int*, Float*, Decimal(*), Date*, Boolean, Enum* | String or FixedString |
|-----------|----------------------------------------------------|-----------------------|
| CountMin  | ✔                                                  | ✔                     |
| MinMax    | ✔                                                  | ✗                     |
| TDigest   | ✔                                                  | ✗                     |
| Uniq      | ✔                                                  | ✔                     |


### Supported Operations {#supported-operations}

|           | Equality filters (==) | Range filters (`>, >=, <, <=`) |
|-----------|-----------------------|------------------------------|
| CountMin  | ✔                     | ✗                            |
| MinMax    | ✗                     | ✔                            |
| TDigest   | ✗                     | ✔                            |
| Uniq      | ✔                     | ✗                            |


## Column-level Settings {#column-level-settings}

Certain MergeTree settings can be overridden at column level:

- `max_compress_block_size` — Maximum size of blocks of uncompressed data before compressing for writing to a table.
- `min_compress_block_size` — Minimum size of blocks of uncompressed data required for compression when writing the next mark.

Example:

```sql
CREATE TABLE tab
(
    id Int64,
    document String SETTINGS (min_compress_block_size = 16777216, max_compress_block_size = 16777216)
)
ENGINE = MergeTree
ORDER BY id
```

Column-level settings can be modified or removed using [ALTER MODIFY COLUMN](/sql-reference/statements/alter/column.md), for example:

- Remove `SETTINGS` from column declaration:

```sql
ALTER TABLE tab MODIFY COLUMN document REMOVE SETTINGS;
```

- Modify a setting:

```sql
ALTER TABLE tab MODIFY COLUMN document MODIFY SETTING min_compress_block_size = 8192;
```

- Reset one or more settings, also removes the setting declaration in the column expression of the table's CREATE query.

```sql
ALTER TABLE tab MODIFY COLUMN document RESET SETTING min_compress_block_size;
```
