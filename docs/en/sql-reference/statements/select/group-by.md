---
toc_title: GROUP BY
---

# GROUP BY Clause {#select-group-by-clause}

`GROUP BY` clause switches the `SELECT` query into an aggregation mode, which works as follows:

-   `GROUP BY` clause contains a list of expressions (or a single expression, which is considered to be the list of length one). This list acts as a “grouping key”, while each individual expression will be referred to as a “key expressions”.
-   All the expressions in the [SELECT](../../../sql-reference/statements/select/index.md), [HAVING](../../../sql-reference/statements/select/having.md), and [ORDER BY](../../../sql-reference/statements/select/order-by.md) clauses **must** be calculated based on key expressions **or** on [aggregate functions](../../../sql-reference/aggregate-functions/index.md) over non-key expressions (including plain columns). In other words, each column selected from the table must be used either in a key expression or inside an aggregate function, but not both.
-   Result of aggregating `SELECT` query will contain as many rows as there were unique values of “grouping key” in source table. Usually this signficantly reduces the row count, often by orders of magnitude, but not necessarily: row count stays the same if all “grouping key” values were distinct.

!!! note "Note"
    There’s an additional way to run aggregation over a table. If a query contains table columns only inside aggregate functions, the `GROUP BY clause` can be omitted, and aggregation by an empty set of keys is assumed. Such queries always return exactly one row.

## NULL Processing {#null-processing}

For grouping, ClickHouse interprets [NULL](../../../sql-reference/syntax.md#null-literal) as a value, and `NULL==NULL`. It differs from `NULL` processing in most other contexts.

Here’s an example to show what this means.

Assume you have this table:

``` text
┌─x─┬────y─┐
│ 1 │    2 │
│ 2 │ ᴺᵁᴸᴸ │
│ 3 │    2 │
│ 3 │    3 │
│ 3 │ ᴺᵁᴸᴸ │
└───┴──────┘
```

The query `SELECT sum(x), y FROM t_null_big GROUP BY y` results in:

``` text
┌─sum(x)─┬────y─┐
│      4 │    2 │
│      3 │    3 │
│      5 │ ᴺᵁᴸᴸ │
└────────┴──────┘
```

You can see that `GROUP BY` for `y = NULL` summed up `x`, as if `NULL` is this value.

If you pass several keys to `GROUP BY`, the result will give you all the combinations of the selection, as if `NULL` were a specific value.

## WITH TOTALS Modifier {#with-totals-modifier}

If the `WITH TOTALS` modifier is specified, another row will be calculated. This row will have key columns containing default values (zeros or empty lines), and columns of aggregate functions with the values calculated across all the rows (the “total” values).

This extra row is only produced in `JSON*`, `TabSeparated*`, and `Pretty*` formats, separately from the other rows:

-   In `JSON*` formats, this row is output as a separate ‘totals’ field.
-   In `TabSeparated*` formats, the row comes after the main result, preceded by an empty row (after the other data).
-   In `Pretty*` formats, the row is output as a separate table after the main result.
-   In the other formats it is not available.

`WITH TOTALS` can be run in different ways when [HAVING](../../../sql-reference/statements/select/having.md) is present. The behavior depends on the `totals_mode` setting.

### Configuring Totals Processing {#configuring-totals-processing}

By default, `totals_mode = 'before_having'`. In this case, ‘totals’ is calculated across all rows, including the ones that don’t pass through HAVING and `max_rows_to_group_by`.

The other alternatives include only the rows that pass through HAVING in ‘totals’, and behave differently with the setting `max_rows_to_group_by` and `group_by_overflow_mode = 'any'`.

`after_having_exclusive` – Don’t include rows that didn’t pass through `max_rows_to_group_by`. In other words, ‘totals’ will have less than or the same number of rows as it would if `max_rows_to_group_by` were omitted.

`after_having_inclusive` – Include all the rows that didn’t pass through ‘max\_rows\_to\_group\_by’ in ‘totals’. In other words, ‘totals’ will have more than or the same number of rows as it would if `max_rows_to_group_by` were omitted.

`after_having_auto` – Count the number of rows that passed through HAVING. If it is more than a certain amount (by default, 50%), include all the rows that didn’t pass through ‘max\_rows\_to\_group\_by’ in ‘totals’. Otherwise, do not include them.

`totals_auto_threshold` – By default, 0.5. The coefficient for `after_having_auto`.

If `max_rows_to_group_by` and `group_by_overflow_mode = 'any'` are not used, all variations of `after_having` are the same, and you can use any of them (for example, `after_having_auto`).

You can use `WITH TOTALS` in subqueries, including subqueries in the [JOIN](../../../sql-reference/statements/select/join.md) clause (in this case, the respective total values are combined).

## Examples {#examples}

Example:

``` sql
SELECT
    count(),
    median(FetchTiming > 60 ? 60 : FetchTiming),
    count() - sum(Refresh)
FROM hits
```

However, in contrast to standard SQL, if the table doesn’t have any rows (either there aren’t any at all, or there aren’t any after using WHERE to filter), an empty result is returned, and not the result from one of the rows containing the initial values of aggregate functions.

As opposed to MySQL (and conforming to standard SQL), you can’t get some value of some column that is not in a key or aggregate function (except constant expressions). To work around this, you can use the ‘any’ aggregate function (get the first encountered value) or ‘min/max’.

Example:

``` sql
SELECT
    domainWithoutWWW(URL) AS domain,
    count(),
    any(Title) AS title -- getting the first occurred page header for each domain.
FROM hits
GROUP BY domain
```

For every different key value encountered, `GROUP BY` calculates a set of aggregate function values.

`GROUP BY` is not supported for array columns.

A constant can’t be specified as arguments for aggregate functions. Example: `sum(1)`. Instead of this, you can get rid of the constant. Example: `count()`.

## Implementation Details {#implementation-details}

Aggregation is one of the most important features of a column-oriented DBMS, and thus it’s implementation is one of the most heavily optimized parts of ClickHouse. By default, aggregation is done in memory using a hash-table. It has 40+ specializations that are chosen automatically depending on “grouping key” data types.

### GROUP BY in External Memory {#select-group-by-in-external-memory}

You can enable dumping temporary data to the disk to restrict memory usage during `GROUP BY`.
The [max\_bytes\_before\_external\_group\_by](../../../operations/settings/settings.md#settings-max_bytes_before_external_group_by) setting determines the threshold RAM consumption for dumping `GROUP BY` temporary data to the file system. If set to 0 (the default), it is disabled.

When using `max_bytes_before_external_group_by`, we recommend that you set `max_memory_usage` about twice as high. This is necessary because there are two stages to aggregation: reading the data and forming intermediate data (1) and merging the intermediate data (2). Dumping data to the file system can only occur during stage 1. If the temporary data wasn’t dumped, then stage 2 might require up to the same amount of memory as in stage 1.

For example, if [max\_memory\_usage](../../../operations/settings/settings.md#settings_max_memory_usage) was set to 10000000000 and you want to use external aggregation, it makes sense to set `max_bytes_before_external_group_by` to 10000000000, and `max_memory_usage` to 20000000000. When external aggregation is triggered (if there was at least one dump of temporary data), maximum consumption of RAM is only slightly more than `max_bytes_before_external_group_by`.

With distributed query processing, external aggregation is performed on remote servers. In order for the requester server to use only a small amount of RAM, set `distributed_aggregation_memory_efficient` to 1.

When merging data flushed to the disk, as well as when merging results from remote servers when the `distributed_aggregation_memory_efficient` setting is enabled, consumes up to `1/256 * the_number_of_threads` from the total amount of RAM.

When external aggregation is enabled, if there was less than `max_bytes_before_external_group_by` of data (i.e. data was not flushed), the query runs just as fast as without external aggregation. If any temporary data was flushed, the run time will be several times longer (approximately three times).

If you have an [ORDER BY](../../../sql-reference/statements/select/order-by.md) with a [LIMIT](../../../sql-reference/statements/select/limit.md) after `GROUP BY`, then the amount of used RAM depends on the amount of data in `LIMIT`, not in the whole table. But if the `ORDER BY` doesn’t have `LIMIT`, don’t forget to enable external sorting (`max_bytes_before_external_sort`).
