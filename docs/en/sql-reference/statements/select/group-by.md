---
description: 'Documentation for GROUP BY Clause'
sidebar_label: 'GROUP BY'
slug: /sql-reference/statements/select/group-by
title: 'GROUP BY Clause'
---

# GROUP BY Clause

`GROUP BY` clause switches the `SELECT` query into an aggregation mode, which works as follows:

- `GROUP BY` clause contains a list of expressions (or a single expression, which is considered to be the list of length one). This list acts as a "grouping key", while each individual expression will be referred to as a "key expression".
- All the expressions in the [SELECT](/sql-reference/statements/select/index.md), [HAVING](/sql-reference/statements/select/having.md), and [ORDER BY](/sql-reference/statements/select/order-by.md) clauses **must** be calculated based on key expressions **or** on [aggregate functions](../../../sql-reference/aggregate-functions/index.md) over non-key expressions (including plain columns). In other words, each column selected from the table must be used either in a key expression or inside an aggregate function, but not both.
- Result of aggregating `SELECT` query will contain as many rows as there were unique values of "grouping key" in source table. Usually, this significantly reduces the row count, often by orders of magnitude, but not necessarily: row count stays the same if all "grouping key" values were distinct.

When you want to group data in the table by column numbers instead of column names, enable the setting [enable_positional_arguments](/operations/settings/settings#enable_positional_arguments).

:::note
There's an additional way to run aggregation over a table. If a query contains table columns only inside aggregate functions, the `GROUP BY clause` can be omitted, and aggregation by an empty set of keys is assumed. Such queries always return exactly one row.
:::

## NULL Processing {#null-processing}

For grouping, ClickHouse interprets [NULL](/sql-reference/syntax#null) as a value, and `NULL==NULL`. It differs from `NULL` processing in most other contexts.

Here's an example to show what this means.

Assume you have this table:

```text
┌─x─┬────y─┐
│ 1 │    2 │
│ 2 │ ᴺᵁᴸᴸ │
│ 3 │    2 │
│ 3 │    3 │
│ 3 │ ᴺᵁᴸᴸ │
└───┴──────┘
```

The query `SELECT sum(x), y FROM t_null_big GROUP BY y` results in:

```text
┌─sum(x)─┬────y─┐
│      4 │    2 │
│      3 │    3 │
│      5 │ ᴺᵁᴸᴸ │
└────────┴──────┘
```

You can see that `GROUP BY` for `y = NULL` summed up `x`, as if `NULL` is this value.

If you pass several keys to `GROUP BY`, the result will give you all the combinations of the selection, as if `NULL` were a specific value.

## ROLLUP Modifier {#rollup-modifier}

`ROLLUP` modifier is used to calculate subtotals for the key expressions, based on their order in the `GROUP BY` list. The subtotals rows are added after the result table.

The subtotals are calculated in the reverse order: at first subtotals are calculated for the last key expression in the list, then for the previous one, and so on up to the first key expression.

In the subtotals rows the values of already "grouped" key expressions are set to `0` or empty line.

:::note
Mind that [HAVING](/sql-reference/statements/select/having.md) clause can affect the subtotals results.
:::

**Example**

Consider the table t:

```text
┌─year─┬─month─┬─day─┐
│ 2019 │     1 │   5 │
│ 2019 │     1 │  15 │
│ 2020 │     1 │   5 │
│ 2020 │     1 │  15 │
│ 2020 │    10 │   5 │
│ 2020 │    10 │  15 │
└──────┴───────┴─────┘
```

Query:

```sql
SELECT year, month, day, count(*) FROM t GROUP BY ROLLUP(year, month, day);
```
As `GROUP BY` section has three key expressions, the result contains four tables with subtotals "rolled up" from right to left:

- `GROUP BY year, month, day`;
- `GROUP BY year, month` (and `day` column is filled with zeros);
- `GROUP BY year` (now `month, day` columns are both filled with zeros);
- and totals (and all three key expression columns are zeros).

```text
┌─year─┬─month─┬─day─┬─count()─┐
│ 2020 │    10 │  15 │       1 │
│ 2020 │     1 │   5 │       1 │
│ 2019 │     1 │   5 │       1 │
│ 2020 │     1 │  15 │       1 │
│ 2019 │     1 │  15 │       1 │
│ 2020 │    10 │   5 │       1 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│ 2019 │     1 │   0 │       2 │
│ 2020 │     1 │   0 │       2 │
│ 2020 │    10 │   0 │       2 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│ 2019 │     0 │   0 │       2 │
│ 2020 │     0 │   0 │       4 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│    0 │     0 │   0 │       6 │
└──────┴───────┴─────┴─────────┘
```
The same query also can be written using `WITH` keyword.
```sql
SELECT year, month, day, count(*) FROM t GROUP BY year, month, day WITH ROLLUP;
```

**See also**

- [group_by_use_nulls](/operations/settings/settings.md#group_by_use_nulls) setting for SQL standard compatibility.

## CUBE Modifier {#cube-modifier}

`CUBE` modifier is used to calculate subtotals for every combination of the key expressions in the `GROUP BY` list. The subtotals rows are added after the result table.

In the subtotals rows the values of all "grouped" key expressions are set to `0` or empty line.

:::note
Mind that [HAVING](/sql-reference/statements/select/having.md) clause can affect the subtotals results.
:::

**Example**

Consider the table t:

```text
┌─year─┬─month─┬─day─┐
│ 2019 │     1 │   5 │
│ 2019 │     1 │  15 │
│ 2020 │     1 │   5 │
│ 2020 │     1 │  15 │
│ 2020 │    10 │   5 │
│ 2020 │    10 │  15 │
└──────┴───────┴─────┘
```

Query:

```sql
SELECT year, month, day, count(*) FROM t GROUP BY CUBE(year, month, day);
```

As `GROUP BY` section has three key expressions, the result contains eight tables with subtotals for all key expression combinations:

- `GROUP BY year, month, day`
- `GROUP BY year, month`
- `GROUP BY year, day`
- `GROUP BY year`
- `GROUP BY month, day`
- `GROUP BY month`
- `GROUP BY day`
- and totals.

Columns, excluded from `GROUP BY`, are filled with zeros.

```text
┌─year─┬─month─┬─day─┬─count()─┐
│ 2020 │    10 │  15 │       1 │
│ 2020 │     1 │   5 │       1 │
│ 2019 │     1 │   5 │       1 │
│ 2020 │     1 │  15 │       1 │
│ 2019 │     1 │  15 │       1 │
│ 2020 │    10 │   5 │       1 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│ 2019 │     1 │   0 │       2 │
│ 2020 │     1 │   0 │       2 │
│ 2020 │    10 │   0 │       2 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│ 2020 │     0 │   5 │       2 │
│ 2019 │     0 │   5 │       1 │
│ 2020 │     0 │  15 │       2 │
│ 2019 │     0 │  15 │       1 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│ 2019 │     0 │   0 │       2 │
│ 2020 │     0 │   0 │       4 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│    0 │     1 │   5 │       2 │
│    0 │    10 │  15 │       1 │
│    0 │    10 │   5 │       1 │
│    0 │     1 │  15 │       2 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│    0 │     1 │   0 │       4 │
│    0 │    10 │   0 │       2 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│    0 │     0 │   5 │       3 │
│    0 │     0 │  15 │       3 │
└──────┴───────┴─────┴─────────┘
┌─year─┬─month─┬─day─┬─count()─┐
│    0 │     0 │   0 │       6 │
└──────┴───────┴─────┴─────────┘
```
The same query also can be written using `WITH` keyword.
```sql
SELECT year, month, day, count(*) FROM t GROUP BY year, month, day WITH CUBE;
```

**See also**

- [group_by_use_nulls](/operations/settings/settings.md#group_by_use_nulls) setting for SQL standard compatibility.

## WITH TOTALS Modifier {#with-totals-modifier}

If the `WITH TOTALS` modifier is specified, another row will be calculated. This row will have key columns containing default values (zeros or empty lines), and columns of aggregate functions with the values calculated across all the rows (the "total" values).

This extra row is only produced in `JSON*`, `TabSeparated*`, and `Pretty*` formats, separately from the other rows:

- In `XML` and `JSON*` formats, this row is output as a separate 'totals' field.
- In `TabSeparated*`, `CSV*` and `Vertical` formats, the row comes after the main result, preceded by an empty row (after the other data).
- In `Pretty*` formats, the row is output as a separate table after the main result.
- In `Template` format, the row is output according to specified template.
- In the other formats it is not available.

:::note
totals is output in the results of `SELECT` queries, and is not output in `INSERT INTO ... SELECT`.
:::

`WITH TOTALS` can be run in different ways when [HAVING](/sql-reference/statements/select/having.md) is present. The behavior depends on the `totals_mode` setting.

### Configuring Totals Processing {#configuring-totals-processing}

By default, `totals_mode = 'before_having'`. In this case, 'totals' is calculated across all rows, including the ones that do not pass through HAVING and `max_rows_to_group_by`.

The other alternatives include only the rows that pass through HAVING in 'totals', and behave differently with the setting `max_rows_to_group_by` and `group_by_overflow_mode = 'any'`.

`after_having_exclusive` – Don't include rows that didn't pass through `max_rows_to_group_by`. In other words, 'totals' will have less than or the same number of rows as it would if `max_rows_to_group_by` were omitted.

`after_having_inclusive` – Include all the rows that didn't pass through 'max_rows_to_group_by' in 'totals'. In other words, 'totals' will have more than or the same number of rows as it would if `max_rows_to_group_by` were omitted.

`after_having_auto` – Count the number of rows that passed through HAVING. If it is more than a certain amount (by default, 50%), include all the rows that didn't pass through 'max_rows_to_group_by' in 'totals'. Otherwise, do not include them.

`totals_auto_threshold` – By default, 0.5. The coefficient for `after_having_auto`.

If `max_rows_to_group_by` and `group_by_overflow_mode = 'any'` are not used, all variations of `after_having` are the same, and you can use any of them (for example, `after_having_auto`).

You can use `WITH TOTALS` in subqueries, including subqueries in the [JOIN](/sql-reference/statements/select/join.md) clause (in this case, the respective total values are combined).

## GROUP BY ALL {#group-by-all}

`GROUP BY ALL` is equivalent to listing all the SELECT-ed expressions that are not aggregate functions.

For example:

```sql
SELECT
    a * 2,
    b,
    count(c),
FROM t
GROUP BY ALL
```

is the same as

```sql
SELECT
    a * 2,
    b,
    count(c),
FROM t
GROUP BY a * 2, b
```

For a special case that if there is a function having both aggregate functions and other fields as its arguments, the `GROUP BY` keys will contain the maximum non-aggregate fields we can extract from it.

For example:

```sql
SELECT
    substring(a, 4, 2),
    substring(substring(a, 1, 2), 1, count(b))
FROM t
GROUP BY ALL
```

is the same as

```sql
SELECT
    substring(a, 4, 2),
    substring(substring(a, 1, 2), 1, count(b))
FROM t
GROUP BY substring(a, 4, 2), substring(a, 1, 2)
```

## Examples {#examples}

Example:

```sql
SELECT
    count(),
    median(FetchTiming > 60 ? 60 : FetchTiming),
    count() - sum(Refresh)
FROM hits
```

As opposed to MySQL (and conforming to standard SQL), you can't get some value of some column that is not in a key or aggregate function (except constant expressions). To work around this, you can use the 'any' aggregate function (get the first encountered value) or 'min/max'.

Example:

```sql
SELECT
    domainWithoutWWW(URL) AS domain,
    count(),
    any(Title) AS title -- getting the first occurred page header for each domain.
FROM hits
GROUP BY domain
```

For every different key value encountered, `GROUP BY` calculates a set of aggregate function values.

## GROUPING SETS modifier {#grouping-sets-modifier}

This is the most general modifier.
This modifier allows manually specifying several aggregation key sets (grouping sets).
Aggregation is performed separately for each grouping set, and after that, all results are combined.
If a column is not presented in a grouping set, it's filled with a default value.

In other words, modifiers described above can be represented via `GROUPING SETS`.
Despite the fact that queries with `ROLLUP`, `CUBE` and `GROUPING SETS` modifiers are syntactically equal, they may perform differently.
When `GROUPING SETS` try to execute everything in parallel, `ROLLUP` and `CUBE` are executing the final merging of the aggregates in a single thread.

In the situation when source columns contain default values, it might be hard to distinguish if a row is a part of the aggregation which uses those columns as keys or not.
To solve this problem `GROUPING` function must be used.

**Example**

The following two queries are equivalent.

```sql
-- Query 1
SELECT year, month, day, count(*) FROM t GROUP BY year, month, day WITH ROLLUP;

-- Query 2
SELECT year, month, day, count(*) FROM t GROUP BY
GROUPING SETS
(
    (year, month, day),
    (year, month),
    (year),
    ()
);
```

**See also**

- [group_by_use_nulls](/operations/settings/settings.md#group_by_use_nulls) setting for SQL standard compatibility.

## Implementation Details {#implementation-details}

Aggregation is one of the most important features of a column-oriented DBMS, and thus it's implementation is one of the most heavily optimized parts of ClickHouse. By default, aggregation is done in memory using a hash-table. It has 40+ specializations that are chosen automatically depending on "grouping key" data types.

### GROUP BY Optimization Depending on Table Sorting Key {#group-by-optimization-depending-on-table-sorting-key}

The aggregation can be performed more effectively, if a table is sorted by some key, and `GROUP BY` expression contains at least prefix of sorting key or injective functions. In this case when a new key is read from table, the in-between result of aggregation can be finalized and sent to client. This behaviour is switched on by the [optimize_aggregation_in_order](../../../operations/settings/settings.md#optimize_aggregation_in_order) setting. Such optimization reduces memory usage during aggregation, but in some cases may slow down the query execution.

### GROUP BY in External Memory {#group-by-in-external-memory}

You can enable dumping temporary data to the disk to restrict memory usage during `GROUP BY`.
The [max_bytes_before_external_group_by](/operations/settings/settings#max_bytes_before_external_group_by) setting determines the threshold RAM consumption for dumping `GROUP BY` temporary data to the file system. If set to 0 (the default), it is disabled.
Alternatively, you can set [max_bytes_ratio_before_external_group_by](/operations/settings/settings#max_bytes_ratio_before_external_group_by), which allows to use `GROUP BY` in external memory only once the query reaches certain threshold of used memory.

When using `max_bytes_before_external_group_by`, we recommend that you set `max_memory_usage` about twice as high (or `max_bytes_ratio_before_external_group_by=0.5`). This is necessary because there are two stages to aggregation: reading the data and forming intermediate data (1) and merging the intermediate data (2). Dumping data to the file system can only occur during stage 1. If the temporary data wasn't dumped, then stage 2 might require up to the same amount of memory as in stage 1.

For example, if [max_memory_usage](/operations/settings/settings#max_memory_usage) was set to 10000000000 and you want to use external aggregation, it makes sense to set `max_bytes_before_external_group_by` to 10000000000, and `max_memory_usage` to 20000000000. When external aggregation is triggered (if there was at least one dump of temporary data), maximum consumption of RAM is only slightly more than `max_bytes_before_external_group_by`.

With distributed query processing, external aggregation is performed on remote servers. In order for the requester server to use only a small amount of RAM, set `distributed_aggregation_memory_efficient` to 1.

When merging data flushed to the disk, as well as when merging results from remote servers when the `distributed_aggregation_memory_efficient` setting is enabled, consumes up to `1/256 * the_number_of_threads` from the total amount of RAM.

When external aggregation is enabled, if there was less than `max_bytes_before_external_group_by` of data (i.e. data was not flushed), the query runs just as fast as without external aggregation. If any temporary data was flushed, the run time will be several times longer (approximately three times).

If you have an [ORDER BY](/sql-reference/statements/select/order-by.md) with a [LIMIT](/sql-reference/statements/select/limit.md) after `GROUP BY`, then the amount of used RAM depends on the amount of data in `LIMIT`, not in the whole table. But if the `ORDER BY` does not have `LIMIT`, do not forget to enable external sorting (`max_bytes_before_external_sort`).
