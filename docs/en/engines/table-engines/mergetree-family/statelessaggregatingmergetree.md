---
slug: /en/engines/table-engines/mergetree-family/statelessaggregatingmergetree
sidebar_position: 48
sidebar_label:  StatelessAggregatingMergeTree
---

# StatelessAggregatingMergeTree {#statelessaggregatingmergetree}

The engine inherits from [MergeTree](./mergetree.md#table_engines-mergetree). 
The key difference is merging strategy: `StatelessAggregatingMergeTree` replaces all specified rows with the same primary key (or more accurately, with the same [sorting key](../../../engines/table-engines/mergetree-family/mergetree.md)) with one row consisting of aggregated values for selected columns and arbitrary values for others. 
If the sorting key is composed in a way that a single key value corresponds to large number of rows, this significantly reduces storage volume and speeds up data selection.

We recommend using the engine together with `MergeTree`. Store complete data in `MergeTree` table, and use `StatelessAggregatingMergeTree` for aggregated data storing, for example, when preparing reports. Such an approach will prevent you from losing valuable data due to an incorrectly composed primary key.

## Creating a Table {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = StatelessAggregatingMergeTree(aggregate_function(s), [columns])
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```
The only difference from creating a plain [MergeTree](../../../engines/table-engines/mergetree-family/mergetree.md) table is two additional engine parameters (see below).

For detailed information on creating tables, see [CREATE TABLE](../../../sql-reference/statements/create/table.md).

### Parameters of StatelessAggregatingMergeTree

#### aggregate_function(s)

`[aggr|(aggr1, aggr2, ...)]` - You can specify either a single aggregate function or a tuple containing names of individual aggregate functions. Each aggregate function listed in the tuple will be applied to its corresponding column.
If the number of aggregate functions listed in the tuple is less than the number of aggregated columns, the last aggregate function specified will be used for all subsequent columns.
If you only need one aggregate function for all columns, you can simply specify it without using a tuple.  

Mandatory parameter.

#### columns

`columns` - a tuple with the names of columns where values will be aggregated.  
Optional parameter.
The columns must not be in the primary key, and they must have a data type that can be passed to the aggregate function.
By default, all columns (except for the primary key column) are aggregated. For columns that are not aggregated, any of values in the column within the same primary key is taken.

:::note
The type of value returned by an aggregate function used to aggregate data in a column needs to be the same as the type of data in the column itself.
Otherwise, it will be impossible to insert data in the column.
:::

:::warning
If an aggregate function cannot be applied to the data type contained in its corresponding column, attempts to insert data into that column will result in an exception being thrown.
:::

## Usage Example {#usage-example}

Create a table:

``` sql
CREATE TABLE samt
(
    `k` UInt32,
    `uint64_val` UInt64,
    `int32_val` Int32,
    `nullable_str` Nullable(String)
)
ENGINE = StatelessAggregatingMergeTree((sum, anyLast))
ORDER BY k
```

Insert some data to it:

``` sql
INSERT INTO samt VALUES (1,1,1,'qwe'),(1,2,2,'rty'),(2,1,1,NULL);
INSERT INTO samt VALUES (1,1,1,'newl'),(2,1,1,'NonNull');
INSERT INTO samt VALUES (2,2,2,NULL);
```

Query data from the table:
``` sql
SELECT k, sum(uint64_val), anyLast(int32_val), anyLast(nullable_str) FROM samt GROUP BY k ORDER BY k;
```

``` text
┌─k─┬─sum(uint64_val)─┬─anyLast(int32_val)─┬─anyLast(nullable_str)─┐
│ 1 │               4 │                  1 │ newl                  │
│ 2 │               4 │                  2 │ NonNull               │
└───┴─────────────────┴────────────────────┴───────────────────────┘
```

## Data Processing {#data-processing}

When data is inserted into a table, it is initially saved as-is. The inserted data parts are then merged in the background. Only at that point rows with the same primary key are actually aggregated and replaced by the aggregation result.

At some point, data parts can be merged in such a way that different resulting data parts may contain different rows with the same primary key. This means that the aggregation process may be incomplete. Thus, it is recommended to use a `SELECT` query with the corresponding aggregate function for each aggregated column and `GROUP BY`.
E.g.in the example above, a simple `SELECT *` query may return a different result:

```sql
SELECT * FROM samt ORDER BY k;
```

```text
┌─k─┬─uint64_val─┬─int32_val─┬─nullable_str─┐
│ 2 │          2 │         2 │ ᴺᵁᴸᴸ         │
└───┴────────────┴───────────┴──────────────┘
┌─k─┬─uint64_val─┬─int32_val─┬─nullable_str─┐
│ 1 │          3 │         2 │ rty          │
│ 2 │          1 │         1 │ ᴺᵁᴸᴸ         │
└───┴────────────┴───────────┴──────────────┘
┌─k─┬─uint64_val─┬─int32_val─┬─nullable_str─┐
│ 1 │          1 │         1 │ newl         │
│ 2 │          1 │         1 │ NonNull      │
└───┴────────────┴───────────┴──────────────┘
```

### Aggregation of Aggregatefunction Columns {#aggregation-of-aggregatefunction-columns}

For columns of [AggregateFunction type](../../../sql-reference/data-types/aggregatefunction.md) ClickHouse behaves as [AggregatingMergeTree](../../../engines/table-engines/mergetree-family/aggregatingmergetree.md) engine aggregating according to the function.