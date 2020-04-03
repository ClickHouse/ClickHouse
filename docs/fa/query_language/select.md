---
en_copy: true
---

# SELECT Queries Syntax {#select-queries-syntax}

`SELECT` performs data retrieval.

``` sql
[WITH expr_list|(subquery)]
SELECT [DISTINCT] expr_list
[FROM [db.]table | (subquery) | table_function] [FINAL]
[SAMPLE sample_coeff]
[ARRAY JOIN ...]
[GLOBAL] [ANY|ALL] [INNER|LEFT|RIGHT|FULL|CROSS] [OUTER] JOIN (subquery)|table USING columns_list
[PREWHERE expr]
[WHERE expr]
[GROUP BY expr_list] [WITH TOTALS]
[HAVING expr]
[ORDER BY expr_list]
[LIMIT [offset_value, ]n BY columns]
[LIMIT [n, ]m]
[UNION ALL ...]
[INTO OUTFILE filename]
[FORMAT format]
```

All the clauses are optional, except for the required list of expressions immediately after SELECT.
The clauses below are described in almost the same order as in the query execution conveyor.

If the query omits the `DISTINCT`, `GROUP BY` and `ORDER BY` clauses and the `IN` and `JOIN` subqueries, the query will be completely stream processed, using O(1) amount of RAM.
Otherwise, the query might consume a lot of RAM if the appropriate restrictions are not specified: `max_memory_usage`, `max_rows_to_group_by`, `max_rows_to_sort`, `max_rows_in_distinct`, `max_bytes_in_distinct`, `max_rows_in_set`, `max_bytes_in_set`, `max_rows_in_join`, `max_bytes_in_join`, `max_bytes_before_external_sort`, `max_bytes_before_external_group_by`. For more information, see the section “Settings”. It is possible to use external sorting (saving temporary tables to a disk) and external aggregation. `The system does not have "merge join"`.

### WITH Clause {#with-clause}

This section provides support for Common Table Expressions ([CTE](https://en.wikipedia.org/wiki/Hierarchical_and_recursive_queries_in_SQL)), with some limitations:
1. Recursive queries are not supported
2. When subquery is used inside WITH section, it’s result should be scalar with exactly one row
3. Expression’s results are not available in subqueries
Results of WITH clause expressions can be used inside SELECT clause.

Example 1: Using constant expression as “variable”

``` sql
WITH '2019-08-01 15:23:00' as ts_upper_bound
SELECT *
FROM hits
WHERE
    EventDate = toDate(ts_upper_bound) AND
    EventTime <= ts_upper_bound
```

Example 2: Evicting sum(bytes) expression result from SELECT clause column list

``` sql
WITH sum(bytes) as s
SELECT
    formatReadableSize(s),
    table
FROM system.parts
GROUP BY table
ORDER BY s
```

Example 3: Using results of scalar subquery

``` sql
/* this example would return TOP 10 of most huge tables */
WITH
    (
        SELECT sum(bytes)
        FROM system.parts
        WHERE active
    ) AS total_disk_usage
SELECT
    (sum(bytes) / total_disk_usage) * 100 AS table_disk_usage,
    table
FROM system.parts
GROUP BY table
ORDER BY table_disk_usage DESC
LIMIT 10
```

Example 4: Re-using expression in subquery
As a workaround for current limitation for expression usage in subqueries, you may duplicate it.

``` sql
WITH ['hello'] AS hello
SELECT
    hello,
    *
FROM
(
    WITH ['hello'] AS hello
    SELECT hello
)
```

``` text
┌─hello─────┬─hello─────┐
│ ['hello'] │ ['hello'] │
└───────────┴───────────┘
```

### FROM Clause {#select-from}

If the FROM clause is omitted, data will be read from the `system.one` table.
The `system.one` table contains exactly one row (this table fulfills the same purpose as the DUAL table found in other DBMSs).

The `FROM` clause specifies the source to read data from:

-   Table
-   Subquery
-   [Table function](table_functions/index.md)

`ARRAY JOIN` and the regular `JOIN` may also be included (see below).

Instead of a table, the `SELECT` subquery may be specified in parenthesis.
In contrast to standard SQL, a synonym does not need to be specified after a subquery.

To execute a query, all the columns listed in the query are extracted from the appropriate table. Any columns not needed for the external query are thrown out of the subqueries.
If a query does not list any columns (for example, `SELECT count() FROM t`), some column is extracted from the table anyway (the smallest one is preferred), in order to calculate the number of rows.

#### FINAL Modifier {#select-from-final}

Applicable when selecting data from tables from the [MergeTree](../operations/table_engines/mergetree.md)-engine family other than `GraphiteMergeTree`. When `FINAL` is specified, ClickHouse fully merges the data before returning the result and thus performs all data transformations that happen during merges for the given table engine.

Also supported for:
- [Replicated](../operations/table_engines/replication.md) versions of `MergeTree` engines.
- [View](../operations/table_engines/view.md), [Buffer](../operations/table_engines/buffer.md), [Distributed](../operations/table_engines/distributed.md), and [MaterializedView](../operations/table_engines/materializedview.md) engines that operate over other engines, provided they were created over `MergeTree`-engine tables.

Queries that use `FINAL` are executed not as fast as similar queries that don’t, because:

-   Query is executed in a single thread and data is merged during query execution.
-   Queries with `FINAL` read primary key columns in addition to the columns specified in the query.

In most cases, avoid using `FINAL`.

### SAMPLE Clause {#select-sample-clause}

The `SAMPLE` clause allows for approximated query processing.

When data sampling is enabled, the query is not performed on all the data, but only on a certain fraction of data (sample). For example, if you need to calculate statistics for all the visits, it is enough to execute the query on the 1/10 fraction of all the visits and then multiply the result by 10.

Approximated query processing can be useful in the following cases:

-   When you have strict timing requirements (like \<100ms) but you can’t justify the cost of additional hardware resources to meet them.
-   When your raw data is not accurate, so approximation doesn’t noticeably degrade the quality.
-   Business requirements target approximate results (for cost-effectiveness, or in order to market exact results to premium users).

!!! note "Note"
    You can only use sampling with the tables in the [MergeTree](../operations/table_engines/mergetree.md) family, and only if the sampling expression was specified during table creation (see [MergeTree engine](../operations/table_engines/mergetree.md#table_engine-mergetree-creating-a-table)).

The features of data sampling are listed below:

-   Data sampling is a deterministic mechanism. The result of the same `SELECT .. SAMPLE` query is always the same.
-   Sampling works consistently for different tables. For tables with a single sampling key, a sample with the same coefficient always selects the same subset of possible data. For example, a sample of user IDs takes rows with the same subset of all the possible user IDs from different tables. This means that you can use the sample in subqueries in the [IN](#select-in-operators) clause. Also, you can join samples using the [JOIN](#select-join) clause.
-   Sampling allows reading less data from a disk. Note that you must specify the sampling key correctly. For more information, see [Creating a MergeTree Table](../operations/table_engines/mergetree.md#table_engine-mergetree-creating-a-table).

For the `SAMPLE` clause the following syntax is supported:

| SAMPLE Clause Syntax | Description                                                                                                                                                                                                                                               |
|----------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `SAMPLE k`           | Here `k` is the number from 0 to 1.</br>The query is executed on `k` fraction of data. For example, `SAMPLE 0.1` runs the query on 10% of data. [Read more](#select-sample-k)                                                                             |
| `SAMPLE n`           | Here `n` is a sufficiently large integer.</br>The query is executed on a sample of at least `n` rows (but not significantly more than this). For example, `SAMPLE 10000000` runs the query on a minimum of 10,000,000 rows. [Read more](#select-sample-n) |
| `SAMPLE k OFFSET m`  | Here `k` and `m` are the numbers from 0 to 1.</br>The query is executed on a sample of `k` fraction of the data. The data used for the sample is offset by `m` fraction. [Read more](#select-sample-offset)                                               |

#### SAMPLE k {#select-sample-k}

Here `k` is the number from 0 to 1 (both fractional and decimal notations are supported). For example, `SAMPLE 1/2` or `SAMPLE 0.5`.

In a `SAMPLE k` clause, the sample is taken from the `k` fraction of data. The example is shown below:

``` sql
SELECT
    Title,
    count() * 10 AS PageViews
FROM hits_distributed
SAMPLE 0.1
WHERE
    CounterID = 34
GROUP BY Title
ORDER BY PageViews DESC LIMIT 1000
```

In this example, the query is executed on a sample from 0.1 (10%) of data. Values of aggregate functions are not corrected automatically, so to get an approximate result, the value `count()` is manually multiplied by 10.

#### SAMPLE n {#select-sample-n}

Here `n` is a sufficiently large integer. For example, `SAMPLE 10000000`.

In this case, the query is executed on a sample of at least `n` rows (but not significantly more than this). For example, `SAMPLE 10000000` runs the query on a minimum of 10,000,000 rows.

Since the minimum unit for data reading is one granule (its size is set by the `index_granularity` setting), it makes sense to set a sample that is much larger than the size of the granule.

When using the `SAMPLE n` clause, you don’t know which relative percent of data was processed. So you don’t know the coefficient the aggregate functions should be multiplied by. Use the `_sample_factor` virtual column to get the approximate result.

The `_sample_factor` column contains relative coefficients that are calculated dynamically. This column is created automatically when you [create](../operations/table_engines/mergetree.md#table_engine-mergetree-creating-a-table) a table with the specified sampling key. The usage examples of the `_sample_factor` column are shown below.

Let’s consider the table `visits`, which contains the statistics about site visits. The first example shows how to calculate the number of page views:

``` sql
SELECT sum(PageViews * _sample_factor)
FROM visits
SAMPLE 10000000
```

The next example shows how to calculate the total number of visits:

``` sql
SELECT sum(_sample_factor)
FROM visits
SAMPLE 10000000
```

The example below shows how to calculate the average session duration. Note that you don’t need to use the relative coefficient to calculate the average values.

``` sql
SELECT avg(Duration)
FROM visits
SAMPLE 10000000
```

#### SAMPLE k OFFSET m {#select-sample-offset}

Here `k` and `m` are numbers from 0 to 1. Examples are shown below.

**Example 1**

``` sql
SAMPLE 1/10
```

In this example, the sample is 1/10th of all data:

`[++------------------]`

**Example 2**

``` sql
SAMPLE 1/10 OFFSET 1/2
```

Here, a sample of 10% is taken from the second half of the data.

`[----------++--------]`

### ARRAY JOIN Clause {#select-array-join-clause}

Allows executing `JOIN` with an array or nested data structure. The intent is similar to the [arrayJoin](functions/array_join.md#functions_arrayjoin) function, but its functionality is broader.

``` sql
SELECT <expr_list>
FROM <left_subquery>
[LEFT] ARRAY JOIN <array>
[WHERE|PREWHERE <expr>]
...
```

You can specify only a single `ARRAY JOIN` clause in a query.

The query execution order is optimized when running `ARRAY JOIN`. Although `ARRAY JOIN` must always be specified before the `WHERE/PREWHERE` clause, it can be performed either before `WHERE/PREWHERE` (if the result is needed in this clause), or after completing it (to reduce the volume of calculations). The processing order is controlled by the query optimizer.

Supported types of `ARRAY JOIN` are listed below:

-   `ARRAY JOIN` - In this case, empty arrays are not included in the result of `JOIN`.
-   `LEFT ARRAY JOIN` - The result of `JOIN` contains rows with empty arrays. The value for an empty array is set to the default value for the array element type (usually 0, empty string or NULL).

The examples below demonstrate the usage of the `ARRAY JOIN` and `LEFT ARRAY JOIN` clauses. Let’s create a table with an [Array](../data_types/array.md) type column and insert values into it:

``` sql
CREATE TABLE arrays_test
(
    s String,
    arr Array(UInt8)
) ENGINE = Memory;

INSERT INTO arrays_test
VALUES ('Hello', [1,2]), ('World', [3,4,5]), ('Goodbye', []);
```

``` text
┌─s───────────┬─arr─────┐
│ Hello       │ [1,2]   │
│ World       │ [3,4,5] │
│ Goodbye     │ []      │
└─────────────┴─────────┘
```

The example below uses the `ARRAY JOIN` clause:

``` sql
SELECT s, arr
FROM arrays_test
ARRAY JOIN arr;
```

``` text
┌─s─────┬─arr─┐
│ Hello │   1 │
│ Hello │   2 │
│ World │   3 │
│ World │   4 │
│ World │   5 │
└───────┴─────┘
```

The next example uses the `LEFT ARRAY JOIN` clause:

``` sql
SELECT s, arr
FROM arrays_test
LEFT ARRAY JOIN arr;
```

``` text
┌─s───────────┬─arr─┐
│ Hello       │   1 │
│ Hello       │   2 │
│ World       │   3 │
│ World       │   4 │
│ World       │   5 │
│ Goodbye     │   0 │
└─────────────┴─────┘
```

#### Using Aliases {#using-aliases}

An alias can be specified for an array in the `ARRAY JOIN` clause. In this case, an array item can be accessed by this alias, but the array itself is accessed by the original name. Example:

``` sql
SELECT s, arr, a
FROM arrays_test
ARRAY JOIN arr AS a;
```

``` text
┌─s─────┬─arr─────┬─a─┐
│ Hello │ [1,2]   │ 1 │
│ Hello │ [1,2]   │ 2 │
│ World │ [3,4,5] │ 3 │
│ World │ [3,4,5] │ 4 │
│ World │ [3,4,5] │ 5 │
└───────┴─────────┴───┘
```

Using aliases, you can perform `ARRAY JOIN` with an external array. For example:

``` sql
SELECT s, arr_external
FROM arrays_test
ARRAY JOIN [1, 2, 3] AS arr_external;
```

``` text
┌─s───────────┬─arr_external─┐
│ Hello       │            1 │
│ Hello       │            2 │
│ Hello       │            3 │
│ World       │            1 │
│ World       │            2 │
│ World       │            3 │
│ Goodbye     │            1 │
│ Goodbye     │            2 │
│ Goodbye     │            3 │
└─────────────┴──────────────┘
```

Multiple arrays can be comma-separated in the `ARRAY JOIN` clause. In this case, `JOIN` is performed with them simultaneously (the direct sum, not the cartesian product). Note that all the arrays must have the same size. Example:

``` sql
SELECT s, arr, a, num, mapped
FROM arrays_test
ARRAY JOIN arr AS a, arrayEnumerate(arr) AS num, arrayMap(x -> x + 1, arr) AS mapped;
```

``` text
┌─s─────┬─arr─────┬─a─┬─num─┬─mapped─┐
│ Hello │ [1,2]   │ 1 │   1 │      2 │
│ Hello │ [1,2]   │ 2 │   2 │      3 │
│ World │ [3,4,5] │ 3 │   1 │      4 │
│ World │ [3,4,5] │ 4 │   2 │      5 │
│ World │ [3,4,5] │ 5 │   3 │      6 │
└───────┴─────────┴───┴─────┴────────┘
```

The example below uses the [arrayEnumerate](functions/array_functions.md#array_functions-arrayenumerate) function:

``` sql
SELECT s, arr, a, num, arrayEnumerate(arr)
FROM arrays_test
ARRAY JOIN arr AS a, arrayEnumerate(arr) AS num;
```

``` text
┌─s─────┬─arr─────┬─a─┬─num─┬─arrayEnumerate(arr)─┐
│ Hello │ [1,2]   │ 1 │   1 │ [1,2]               │
│ Hello │ [1,2]   │ 2 │   2 │ [1,2]               │
│ World │ [3,4,5] │ 3 │   1 │ [1,2,3]             │
│ World │ [3,4,5] │ 4 │   2 │ [1,2,3]             │
│ World │ [3,4,5] │ 5 │   3 │ [1,2,3]             │
└───────┴─────────┴───┴─────┴─────────────────────┘
```

#### ARRAY JOIN With Nested Data Structure {#array-join-with-nested-data-structure}

`ARRAY`JOIN\`\` also works with [nested data structures](../data_types/nested_data_structures/nested.md). Example:

``` sql
CREATE TABLE nested_test
(
    s String,
    nest Nested(
    x UInt8,
    y UInt32)
) ENGINE = Memory;

INSERT INTO nested_test
VALUES ('Hello', [1,2], [10,20]), ('World', [3,4,5], [30,40,50]), ('Goodbye', [], []);
```

``` text
┌─s───────┬─nest.x──┬─nest.y─────┐
│ Hello   │ [1,2]   │ [10,20]    │
│ World   │ [3,4,5] │ [30,40,50] │
│ Goodbye │ []      │ []         │
└─────────┴─────────┴────────────┘
```

``` sql
SELECT s, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN nest;
```

``` text
┌─s─────┬─nest.x─┬─nest.y─┐
│ Hello │      1 │     10 │
│ Hello │      2 │     20 │
│ World │      3 │     30 │
│ World │      4 │     40 │
│ World │      5 │     50 │
└───────┴────────┴────────┘
```

When specifying names of nested data structures in `ARRAY JOIN`, the meaning is the same as `ARRAY JOIN` with all the array elements that it consists of. Examples are listed below:

``` sql
SELECT s, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN `nest.x`, `nest.y`;
```

``` text
┌─s─────┬─nest.x─┬─nest.y─┐
│ Hello │      1 │     10 │
│ Hello │      2 │     20 │
│ World │      3 │     30 │
│ World │      4 │     40 │
│ World │      5 │     50 │
└───────┴────────┴────────┘
```

This variation also makes sense:

``` sql
SELECT s, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN `nest.x`;
```

``` text
┌─s─────┬─nest.x─┬─nest.y─────┐
│ Hello │      1 │ [10,20]    │
│ Hello │      2 │ [10,20]    │
│ World │      3 │ [30,40,50] │
│ World │      4 │ [30,40,50] │
│ World │      5 │ [30,40,50] │
└───────┴────────┴────────────┘
```

An alias may be used for a nested data structure, in order to select either the `JOIN` result or the source array. Example:

``` sql
SELECT s, `n.x`, `n.y`, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN nest AS n;
```

``` text
┌─s─────┬─n.x─┬─n.y─┬─nest.x──┬─nest.y─────┐
│ Hello │   1 │  10 │ [1,2]   │ [10,20]    │
│ Hello │   2 │  20 │ [1,2]   │ [10,20]    │
│ World │   3 │  30 │ [3,4,5] │ [30,40,50] │
│ World │   4 │  40 │ [3,4,5] │ [30,40,50] │
│ World │   5 │  50 │ [3,4,5] │ [30,40,50] │
└───────┴─────┴─────┴─────────┴────────────┘
```

Example of using the [arrayEnumerate](functions/array_functions.md#array_functions-arrayenumerate) function:

``` sql
SELECT s, `n.x`, `n.y`, `nest.x`, `nest.y`, num
FROM nested_test
ARRAY JOIN nest AS n, arrayEnumerate(`nest.x`) AS num;
```

``` text
┌─s─────┬─n.x─┬─n.y─┬─nest.x──┬─nest.y─────┬─num─┐
│ Hello │   1 │  10 │ [1,2]   │ [10,20]    │   1 │
│ Hello │   2 │  20 │ [1,2]   │ [10,20]    │   2 │
│ World │   3 │  30 │ [3,4,5] │ [30,40,50] │   1 │
│ World │   4 │  40 │ [3,4,5] │ [30,40,50] │   2 │
│ World │   5 │  50 │ [3,4,5] │ [30,40,50] │   3 │
└───────┴─────┴─────┴─────────┴────────────┴─────┘
```

### JOIN Clause {#select-join}

Joins the data in the normal [SQL JOIN](https://en.wikipedia.org/wiki/Join_(SQL)) sense.

!!! info "Note"
    Not related to [ARRAY JOIN](#select-array-join-clause).

``` sql
SELECT <expr_list>
FROM <left_subquery>
[GLOBAL] [ANY|ALL] [INNER|LEFT|RIGHT|FULL|CROSS] [OUTER] JOIN <right_subquery>
(ON <expr_list>)|(USING <column_list>) ...
```

The table names can be specified instead of `<left_subquery>` and `<right_subquery>`. This is equivalent to the `SELECT * FROM table` subquery, except in a special case when the table has the [Join](../operations/table_engines/join.md) engine – an array prepared for joining.

#### Supported Types of `JOIN` {#select-join-types}

-   `INNER JOIN` (or `JOIN`)
-   `LEFT JOIN` (or `LEFT OUTER JOIN`)
-   `RIGHT JOIN` (or `RIGHT OUTER JOIN`)
-   `FULL JOIN` (or `FULL OUTER JOIN`)
-   `CROSS JOIN` (or `,` )

See the standard [SQL JOIN](https://en.wikipedia.org/wiki/Join_(SQL)) description.

#### Multiple JOIN {#multiple-join}

Performing queries, ClickHouse rewrites multi-table joins into the sequence of two-table joins. For example, if there are four tables for join ClickHouse joins the first and the second, then joins the result with the third table, and at the last step, it joins the fourth one.

If a query contains the `WHERE` clause, ClickHouse tries to pushdown filters from this clause through the intermediate join. If it cannot apply the filter to each intermediate join, ClickHouse applies the filters after all joins are completed.

We recommend the `JOIN ON` or `JOIN USING` syntax for creating queries. For example:

``` sql
SELECT * FROM t1 JOIN t2 ON t1.a = t2.a JOIN t3 ON t1.a = t3.a
```

You can use comma-separated lists of tables in the `FROM` clause. For example:

``` sql
SELECT * FROM t1, t2, t3 WHERE t1.a = t2.a AND t1.a = t3.a
```

Don’t mix these syntaxes.

ClickHouse doesn’t directly support syntax with commas, so we don’t recommend using them. The algorithm tries to rewrite the query in terms of `CROSS JOIN` and `INNER JOIN` clauses and then proceeds to query processing. When rewriting the query, ClickHouse tries to optimize performance and memory consumption. By default, ClickHouse treats commas as an `INNER JOIN` clause and converts `INNER JOIN` to `CROSS JOIN` when the algorithm cannot guarantee that `INNER JOIN` returns the required data.

#### Strictness {#select-join-strictness}

-   `ALL` — If the right table has several matching rows, ClickHouse creates a [Cartesian product](https://en.wikipedia.org/wiki/Cartesian_product) from matching rows. This is the standard `JOIN` behavior in SQL.
-   `ANY` — If the right table has several matching rows, only the first one found is joined. If the right table has only one matching row, the results of queries with `ANY` and `ALL` keywords are the same.
-   `ASOF` — For joining sequences with a non-exact match. `ASOF JOIN` usage is described below.

**ASOF JOIN Usage**

`ASOF JOIN` is useful when you need to join records that have no exact match.

Tables for `ASOF JOIN` must have an ordered sequence column. This column cannot be alone in a table, and should be one of the data types: `UInt32`, `UInt64`, `Float32`, `Float64`, `Date`, and `DateTime`.

Syntax `ASOF JOIN ... ON`:

``` sql
SELECT expressions_list
FROM table_1
ASOF LEFT JOIN table_2
ON equi_cond AND closest_match_cond
```

You can use any number of equality conditions and exactly one closest match condition. For example, `SELECT count() FROM table_1 ASOF LEFT JOIN table_2 ON table_1.a == table_2.b AND table_2.t <= table_1.t`.

Conditions supported for the closest match: `>`, `>=`, `<`, `<=`.

Syntax `ASOF JOIN ... USING`:

``` sql
SELECT expressions_list
FROM table_1
ASOF JOIN table_2
USING (equi_column1, ... equi_columnN, asof_column)
```

`ASOF JOIN` uses `equi_columnX` for joining on equality and `asof_column` for joining on the closest match with the `table_1.asof_column >= table_2.asof_column` condition. The `asof_column` column always the last one in the `USING` clause.

For example, consider the following tables:

``` text
     table_1                           table_2

  event   | ev_time | user_id       event   | ev_time | user_id
