
# SummingMergeTree

The engine inherits from [MergeTree](mergetree.md#table_engines-mergetree). The difference is that when merging data parts for `SummingMergeTree` tables ClickHouse replaces all the rows with the same primary key (or more accurately, with the same [sorting key](mergetree.md)) with one row which contains summarized values for the columns with the numeric data type. If the sorting key is composed in a way that a single key value corresponds to large number of rows, this significantly reduces storage volume and speeds up data selection.

We recommend to use the engine together with `MergeTree`. Store complete data in `MergeTree` table, and use `SummingMergeTree` for aggregated data storing, for example, when preparing reports. Such an approach will prevent you from losing valuable data due to an incorrectly composed primary key.

## Creating a Table

```
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = SummingMergeTree([columns])
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

For a description of request parameters, see [request description](../../query_language/create.md).

**Parameters of SummingMergeTree**

- `columns` - a tuple with the names of columns where values will be summarized. Optional parameter.
The columns must be of a numeric type and must not be in the primary key.

    If `columns` not specified, ClickHouse summarizes the values in all columns with a numeric data type that are not in the primary key.

**Query clauses**

When creating a `SummingMergeTree` table the same [clauses](mergetree.md) are required, as when creating a `MergeTree` table.

<details markdown="1"><summary>Deprecated Method for Creating a Table</summary>

!!! attention
    Do not use this method in new projects and, if possible, switch the old projects to the method described above.

```
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE [=] SummingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, [columns])
```

All of the parameters excepting `columns` have the same meaning as in `MergeTree`.

- `columns` — tuple with names of columns values of which will be summarized. Optional parameter. For a description, see the text above.
</details>

## Usage Example

Consider the following table:

```sql
CREATE TABLE summtt
(
    key UInt32,
    value UInt32
)
ENGINE = SummingMergeTree()
ORDER BY key
```

Insert data to it:

```
:) INSERT INTO summtt Values(1,1),(1,2),(2,1)
```

ClickHouse may sum all the rows not completely ([see below](#data-processing)), so we use an aggregate function `sum` and `GROUP BY` clause in the query.

```sql
SELECT key, sum(value) FROM summtt GROUP BY key
```

```
┌─key─┬─sum(value)─┐
│   2 │          1 │
│   1 │          3 │
└─────┴────────────┘
```


## Data Processing {#data-processing}

When data are inserted into a table, they are saved as-is. Clickhouse merges the inserted parts of data periodically and this is when rows with the same primary key are summed and replaced with one for each resulting part of data.

ClickHouse can merge the data parts so that different resulting parts of data cat consist rows with the same primary key, i.e. the summation will be incomplete. Therefore (`SELECT`) an aggregate function [sum()](../../query_language/agg_functions/reference.md#agg_function-sum) and `GROUP BY` clause should be used in a query as described in the example above.

### Common rules for summation

The values in the columns with the numeric data type are summarized. The set of columns is defined by the parameter `columns`.

If the values were 0 in all of the columns for summation, the row is deleted.

If column is not in the primary key and is not summarized, an arbitrary value is selected from the existing ones.

The values are not summarized for columns in the primary key.

### The Summation in the AggregateFunction Columns

For columns of [AggregateFunction type](../../data_types/nested_data_structures/aggregatefunction.md) ClickHouse behaves as [AggregatingMergeTree](aggregatingmergetree.md) engine aggregating according to the function.

### Nested Structures

Table can have nested data structures that are processed in a special way.

If the name of a nested table ends with `Map` and it contains at least two columns that meet the following criteria:

- the first column is numeric `(*Int*, Date, DateTime)`, let's call it `key`,
- the other columns are arithmetic `(*Int*, Float32/64)`, let's call it `(values...)`,

then this nested table is interpreted as a mapping of `key => (values...)`, and when merging its rows, the elements of two data sets are merged by `key` with a summation of the corresponding `(values...)`.

Examples:

```
[(1, 100)] + [(2, 150)] -> [(1, 100), (2, 150)]
[(1, 100)] + [(1, 150)] -> [(1, 250)]
[(1, 100)] + [(1, 150), (2, 150)] -> [(1, 250), (2, 150)]
[(1, 100), (2, 150)] + [(1, -100)] -> [(2, 150)]
```

When requesting data, use the [sumMap(key, value)](../../query_language/agg_functions/reference.md) function for aggregation of `Map`.

For nested data structure, you do not need to specify its columns in the tuple of columns for summation.

[Original article](https://clickhouse.yandex/docs/en/operations/table_engines/summingmergetree/) <!--hide-->
