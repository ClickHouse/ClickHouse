# SELECT Queries Syntax

`SELECT` performs data retrieval.

```sql
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
[LIMIT [n, ]m]
[UNION ALL ...]
[INTO OUTFILE filename]
[FORMAT format]
[LIMIT [offset_value, ]n BY columns]
```

All the clauses are optional, except for the required list of expressions immediately after SELECT.
The clauses below are described in almost the same order as in the query execution conveyor.

If the query omits the `DISTINCT`, `GROUP BY` and `ORDER BY` clauses and the `IN` and `JOIN` subqueries, the query will be completely stream processed, using O(1) amount of RAM.
Otherwise, the query might consume a lot of RAM if the appropriate restrictions are not specified: `max_memory_usage`, `max_rows_to_group_by`, `max_rows_to_sort`, `max_rows_in_distinct`, `max_bytes_in_distinct`, `max_rows_in_set`, `max_bytes_in_set`, `max_rows_in_join`, `max_bytes_in_join`, `max_bytes_before_external_sort`, `max_bytes_before_external_group_by`. For more information, see the section "Settings". It is possible to use external sorting (saving temporary tables to a disk) and external aggregation. `The system does not have "merge join"`.

### WITH Clause
This section provides support for Common Table Expressions ([CTE](https://en.wikipedia.org/wiki/Hierarchical_and_recursive_queries_in_SQL)), with some limitations:
1. Recursive queries are not supported
2. When subquery is used inside WITH section, it's result should be scalar with exactly one row
3. Expression's results are not available in subqueries
Results of WITH clause expressions can be used inside SELECT clause.

Example 1: Using constant expression as "variable"
```sql
WITH '2019-08-01 15:23:00' as ts_upper_bound
SELECT *
FROM hits
WHERE
    EventDate = toDate(ts_upper_bound) AND
    EventTime <= ts_upper_bound
```

Example 2: Evicting sum(bytes) expression result from SELECT clause column list
```sql
WITH sum(bytes) as s
SELECT
    formatReadableSize(s),
    table
FROM system.parts
GROUP BY table
ORDER BY s
```

Example 3: Using results of scalar subquery
```sql
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
```sql
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
```text
┌─hello─────┬─hello─────┐
│ ['hello'] │ ['hello'] │
└───────────┴───────────┘
```


### FROM Clause {#select-from}

If the FROM clause is omitted, data will be read from the `system.one` table.
The `system.one` table contains exactly one row (this table fulfills the same purpose as the DUAL table found in other DBMSs).

The `FROM` clause specifies the source to read data from:

- Table
- Subquery
- [Table function](table_functions/index.md)

`ARRAY JOIN` and the regular `JOIN` may also be included (see below).

Instead of a table, the `SELECT` subquery may be specified in parenthesis.
In contrast to standard SQL, a synonym does not need to be specified after a subquery. For compatibility, it is possible to write `AS name` after a subquery, but the specified name isn't used anywhere.

To execute a query, all the columns listed in the query are extracted from the appropriate table. Any columns not needed for the external query are thrown out of the subqueries.
If a query does not list any columns (for example, `SELECT count() FROM t`), some column is extracted from the table anyway (the smallest one is preferred), in order to calculate the number of rows.

The `FINAL` modifier can be used in the `SELECT` select query for engines from the [MergeTree](../operations/table_engines/mergetree.md) family. When you specify `FINAL`, data is selected fully "merged". Keep in mind that using `FINAL` leads to reading columns related to the primary key, in addition to the columns specified in the query. Additionally, the query will be executed in a single thread, and data will be merged during query execution. This means that when using `FINAL`, the query is processed slowly. In the most cases, avoid using `FINAL`.
The `FINAL` modifier can be applied for all engines of MergeTree family that do data transformations in background merges (except GraphiteMergeTree).

### SAMPLE Clause {#select-sample-clause}

The `SAMPLE` clause allows for approximated query processing.

When data sampling is enabled, the query is not performed on all the data, but only on a certain fraction of data (sample). For example, if you need to calculate statistics for all the visits, it is enough to execute the query on the 1/10 fraction of all the visits and then multiply the result by 10.

Approximated query processing can be useful in the following cases:

- When you have strict timing requirements (like <100ms) but you can't justify the cost of additional hardware resources to meet them.
- When your raw data is not accurate, so approximation doesn't noticeably degrade the quality.
- Business requirements target approximate results (for cost-effectiveness, or in order to market exact results to premium users).

!!! note
    You can only use sampling with the tables in the [MergeTree](../operations/table_engines/mergetree.md) family, and only if the sampling expression was specified during table creation (see [MergeTree engine](../operations/table_engines/mergetree.md#table_engine-mergetree-creating-a-table)).

The features of data sampling are listed below:

- Data sampling is a deterministic mechanism. The result of the same `SELECT .. SAMPLE` query is always the same.
- Sampling works consistently for different tables. For tables with a single sampling key, a sample with the same coefficient always selects the same subset of possible data. For example, a sample of user IDs takes rows with the same subset of all the possible user IDs from different tables. This means that you can use the sample in subqueries in the [IN](#select-in-operators) clause. Also, you can join samples using the [JOIN](#select-join) clause.
- Sampling allows reading less data from a disk. Note that you must specify the sampling key correctly. For more information, see [Creating a MergeTree Table](../operations/table_engines/mergetree.md#table_engine-mergetree-creating-a-table).

For the `SAMPLE` clause the following syntax is supported:

| SAMPLE&#160;Clause&#160;Syntax | Description |
| ---------------- | --------- |
| `SAMPLE k` | Here `k` is the number from 0 to 1.</br>The query is executed on `k` fraction of data. For example, `SAMPLE 0.1` runs the query on 10% of data. [Read more](#select-sample-k)|
| `SAMPLE n` | Here `n` is a sufficiently large integer.</br>The query is executed on a sample of at least `n` rows (but not significantly more than this). For example, `SAMPLE 10000000` runs the query on a minimum of 10,000,000 rows. [Read more](#select-sample-n) |
| `SAMPLE k OFFSET m` | Here `k` and `m` are the numbers from 0 to 1.</br>The query is executed on a sample of `k` fraction of the data. The data used for the sample is offset by `m` fraction. [Read more](#select-sample-offset) |


#### SAMPLE k {#select-sample-k}

Here `k` is the number from 0 to 1 (both fractional and decimal notations are supported). For example, `SAMPLE 1/2` or `SAMPLE 0.5`.

In a `SAMPLE k` clause, the sample is taken from the `k` fraction of data. The example is shown below:

```sql
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

When using the `SAMPLE n` clause, you don't know which relative percent of data was processed. So you don't know the coefficient the aggregate functions should be multiplied by. Use the `_sample_factor` virtual column to get the approximate result.

The `_sample_factor` column contains relative coefficients that are calculated dynamically. This column is created automatically when you [create](../operations/table_engines/mergetree.md#table_engine-mergetree-creating-a-table) a table with the specified sampling key. The usage examples of the `_sample_factor` column are shown below.

Let's consider the table `visits`, which contains the statistics about site visits. The first example shows how to calculate the number of page views:

```sql
SELECT sum(PageViews * _sample_factor)
FROM visits
SAMPLE 10000000
```  

The next example shows how to calculate the total number of visits:

```sql
SELECT sum(_sample_factor)
FROM visits
SAMPLE 10000000
``` 

The example below shows how to calculate the average session duration. Note that you don't need to use the relative coefficient to calculate the average values.

```sql
SELECT avg(Duration)
FROM visits
SAMPLE 10000000
``` 

#### SAMPLE k OFFSET m {#select-sample-offset}

Here `k` and `m` are numbers from 0 to 1. Examples are shown below.

**Example 1**

```sql
SAMPLE 1/10
```

In this example, the sample is 1/10th of all data:

`[++------------------]`

**Example 2**

```sql
SAMPLE 1/10 OFFSET 1/2
```

Here, a sample of 10% is taken from the second half of the data.

`[----------++--------]`

### ARRAY JOIN Clause {#select-array-join-clause}

Allows executing `JOIN` with an array or nested data structure. The intent is similar to the [arrayJoin](functions/array_join.md#functions_arrayjoin) function, but its functionality is broader.

```sql
SELECT <expr_list>
FROM <left_subquery>
[LEFT] ARRAY JOIN <array>
[WHERE|PREWHERE <expr>]
...
```

You can specify only a single `ARRAY JOIN` clause in a query.

The query execution order is optimized when running `ARRAY JOIN`. Although `ARRAY JOIN` must always be specified before the `WHERE/PREWHERE` clause, it can be performed either before `WHERE/PREWHERE` (if the result is needed in this clause), or after completing it (to reduce the volume of calculations). The processing order is controlled by the query optimizer.

Supported types of `ARRAY JOIN` are listed below:

- `ARRAY JOIN` - In this case, empty arrays are not included in the result of `JOIN`.
- `LEFT ARRAY JOIN` - The result of `JOIN` contains rows with empty arrays. The value for an empty array is set to the default value for the array element type (usually 0, empty string or NULL).

The examples below demonstrate the usage of the `ARRAY JOIN` and `LEFT ARRAY JOIN` clauses. Let's create a table with an [Array](../data_types/array.md) type column and insert values into it:

```sql
CREATE TABLE arrays_test
(
    s String,
    arr Array(UInt8)
) ENGINE = Memory;

INSERT INTO arrays_test
VALUES ('Hello', [1,2]), ('World', [3,4,5]), ('Goodbye', []);
```
```text
┌─s───────────┬─arr─────┐
│ Hello       │ [1,2]   │
│ World       │ [3,4,5] │
│ Goodbye     │ []      │
└─────────────┴─────────┘
```

The example below uses the `ARRAY JOIN` clause:

```sql
SELECT s, arr
FROM arrays_test
ARRAY JOIN arr;
```
```text
┌─s─────┬─arr─┐
│ Hello │   1 │
│ Hello │   2 │
│ World │   3 │
│ World │   4 │
│ World │   5 │
└───────┴─────┘
```

The next example uses the `LEFT ARRAY JOIN` clause:

```sql
SELECT s, arr
FROM arrays_test
LEFT ARRAY JOIN arr;
```
```text
┌─s───────────┬─arr─┐
│ Hello       │   1 │
│ Hello       │   2 │
│ World       │   3 │
│ World       │   4 │
│ World       │   5 │
│ Goodbye     │   0 │
└─────────────┴─────┘
```

#### Using Aliases

An alias can be specified for an array in the `ARRAY JOIN` clause. In this case, an array item can be accessed by this alias, but the array itself is accessed by the original name. Example:

```sql
SELECT s, arr, a
FROM arrays_test
ARRAY JOIN arr AS a;
```

```text
┌─s─────┬─arr─────┬─a─┐
│ Hello │ [1,2]   │ 1 │
│ Hello │ [1,2]   │ 2 │
│ World │ [3,4,5] │ 3 │
│ World │ [3,4,5] │ 4 │
│ World │ [3,4,5] │ 5 │
└───────┴─────────┴───┘
```

Using aliases, you can perform `ARRAY JOIN` with an external array. For example:

```sql
SELECT s, arr_external
FROM arrays_test
ARRAY JOIN [1, 2, 3] AS arr_external;
```

```text
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

```sql
SELECT s, arr, a, num, mapped
FROM arrays_test
ARRAY JOIN arr AS a, arrayEnumerate(arr) AS num, arrayMap(x -> x + 1, arr) AS mapped;
```

```text
┌─s─────┬─arr─────┬─a─┬─num─┬─mapped─┐
│ Hello │ [1,2]   │ 1 │   1 │      2 │
│ Hello │ [1,2]   │ 2 │   2 │      3 │
│ World │ [3,4,5] │ 3 │   1 │      4 │
│ World │ [3,4,5] │ 4 │   2 │      5 │
│ World │ [3,4,5] │ 5 │   3 │      6 │
└───────┴─────────┴───┴─────┴────────┘
```

The example below uses the [arrayEnumerate](functions/array_functions.md#array_functions-arrayenumerate) function:

```sql
SELECT s, arr, a, num, arrayEnumerate(arr)
FROM arrays_test
ARRAY JOIN arr AS a, arrayEnumerate(arr) AS num;
```

```text
┌─s─────┬─arr─────┬─a─┬─num─┬─arrayEnumerate(arr)─┐
│ Hello │ [1,2]   │ 1 │   1 │ [1,2]               │
│ Hello │ [1,2]   │ 2 │   2 │ [1,2]               │
│ World │ [3,4,5] │ 3 │   1 │ [1,2,3]             │
│ World │ [3,4,5] │ 4 │   2 │ [1,2,3]             │
│ World │ [3,4,5] │ 5 │   3 │ [1,2,3]             │
└───────┴─────────┴───┴─────┴─────────────────────┘
```

#### ARRAY JOIN With Nested Data Structure

`ARRAY `JOIN`` also works with [nested data structures](../data_types/nested_data_structures/nested.md). Example:

```sql
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

```text
┌─s───────┬─nest.x──┬─nest.y─────┐
│ Hello   │ [1,2]   │ [10,20]    │
│ World   │ [3,4,5] │ [30,40,50] │
│ Goodbye │ []      │ []         │
└─────────┴─────────┴────────────┘
```

```sql
SELECT s, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN nest;
```

```text
┌─s─────┬─nest.x─┬─nest.y─┐
│ Hello │      1 │     10 │
│ Hello │      2 │     20 │
│ World │      3 │     30 │
│ World │      4 │     40 │
│ World │      5 │     50 │
└───────┴────────┴────────┘
```

When specifying names of nested data structures in `ARRAY JOIN`, the meaning is the same as `ARRAY JOIN` with all the array elements that it consists of. Examples are listed below:

```sql
SELECT s, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN `nest.x`, `nest.y`;
```

```text
┌─s─────┬─nest.x─┬─nest.y─┐
│ Hello │      1 │     10 │
│ Hello │      2 │     20 │
│ World │      3 │     30 │
│ World │      4 │     40 │
│ World │      5 │     50 │
└───────┴────────┴────────┘
```

This variation also makes sense:

```sql
SELECT s, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN `nest.x`;
```

```text
┌─s─────┬─nest.x─┬─nest.y─────┐
│ Hello │      1 │ [10,20]    │
│ Hello │      2 │ [10,20]    │
│ World │      3 │ [30,40,50] │
│ World │      4 │ [30,40,50] │
│ World │      5 │ [30,40,50] │
└───────┴────────┴────────────┘
```

An alias may be used for a nested data structure, in order to select either the `JOIN` result or the source array. Example:

```sql
SELECT s, `n.x`, `n.y`, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN nest AS n;
```

```text
┌─s─────┬─n.x─┬─n.y─┬─nest.x──┬─nest.y─────┐
│ Hello │   1 │  10 │ [1,2]   │ [10,20]    │
│ Hello │   2 │  20 │ [1,2]   │ [10,20]    │
│ World │   3 │  30 │ [3,4,5] │ [30,40,50] │
│ World │   4 │  40 │ [3,4,5] │ [30,40,50] │
│ World │   5 │  50 │ [3,4,5] │ [30,40,50] │
└───────┴─────┴─────┴─────────┴────────────┘
```

Example of using the [arrayEnumerate](functions/array_functions.md#array_functions-arrayenumerate) function:

```sql
SELECT s, `n.x`, `n.y`, `nest.x`, `nest.y`, num
FROM nested_test
ARRAY JOIN nest AS n, arrayEnumerate(`nest.x`) AS num;
```

```text
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

```sql
SELECT <expr_list>
FROM <left_subquery>
[GLOBAL] [ANY|ALL] [INNER|LEFT|RIGHT|FULL|CROSS] [OUTER] JOIN <right_subquery>
(ON <expr_list>)|(USING <column_list>) ...
```

The table names can be specified instead of `<left_subquery>` and `<right_subquery>`. This is equivalent to the `SELECT * FROM table` subquery, except in a special case when the table has the [Join](../operations/table_engines/join.md) engine – an array prepared for joining.

#### Supported Types of `JOIN` {#select-join-types}

- `INNER JOIN` (or `JOIN`)
- `LEFT JOIN` (or `LEFT OUTER JOIN`)
- `RIGHT JOIN` (or `RIGHT OUTER JOIN`)
- `FULL JOIN` (or `FULL OUTER JOIN`)
- `CROSS JOIN` (or `,` )

See the standard [SQL JOIN](https://en.wikipedia.org/wiki/Join_(SQL)) description.

#### Multiple JOIN

Performing queries, ClickHouse rewrites multi-table joins into the sequence of two-table joins. For example, if there are four tables for join ClickHouse joins the first and the second, then joins the result with the third table, and at the last step, it joins the fourth one.

If a query contains the `WHERE` clause, ClickHouse tries to pushdown filters from this clause through the intermediate join. If it cannot apply the filter to each intermediate join, ClickHouse applies the filters after all joins are completed.

We recommend the `JOIN ON` or `JOIN USING` syntax for creating queries. For example:

```sql
SELECT * FROM t1 JOIN t2 ON t1.a = t2.a JOIN t3 ON t1.a = t3.a
```

You can use comma-separated lists of tables in the `FROM` clause. This works only with the [allow_experimental_cross_to_join_conversion = 1](../operations/settings/settings.md#settings-allow_experimental_cross_to_join_conversion) setting. For example:

```sql
SELECT * FROM t1, t2, t3 WHERE t1.a = t2.a AND t1.a = t3.a
```

Don't mix these syntaxes.

ClickHouse doesn't directly support syntax with commas, so we don't recommend using them. The algorithm tries to rewrite the query in terms of `CROSS JOIN` and `INNER JOIN` clauses and then proceeds to query processing. When rewriting the query, ClickHouse tries to optimize performance and memory consumption. By default, ClickHouse treats commas as an `INNER JOIN` clause and converts `INNER JOIN` to `CROSS JOIN` when the algorithm cannot guarantee that `INNER JOIN` returns the required data.

#### Strictness {#select-join-strictness}

- `ALL` — If the right table has several matching rows, ClickHouse creates a [Cartesian product](https://en.wikipedia.org/wiki/Cartesian_product) from matching rows. This is the standard `JOIN` behavior in SQL.
- `ANY` — If the right table has several matching rows, only the first one found is joined. If the right table has only one matching row, the results of queries with `ANY` and `ALL` keywords are the same.
- `ASOF` — For joining sequences with a non-exact match. `ASOF JOIN` usage is described below.

**ASOF JOIN Usage**

`ASOF JOIN` is useful when you need to join records that have no exact match.

Tables for `ASOF JOIN` must have an ordered sequence column. This column cannot be alone in a table, and should be one of the data types: `UInt32`, `UInt64`, `Float32`, `Float64`, `Date`, and `DateTime`.

You can use the following types of syntax:

- `ASOF JOIN ... ON`

    ```sql
    SELECT expressions_list
    FROM table_1
    ASOF LEFT JOIN table_2
    ON equi_cond AND closest_match_cond
    ```

    You can use any number of equality conditions and exactly one closest match condition. For example, `SELECT count() FROM A ASOF LEFT JOIN B ON A.a == B.b AND B.t <= A.t`. Only `table_2.some_col <= table_1.some_col` and `table_1.some_col >= table2.some_col` condition types are available. You can't apply other conditions like `>` or `!=`.

- `ASOF JOIN ... USING`

    ```sql
    SELECT expressions_list
    FROM table_1
    ASOF JOIN table_2
    USING (equi_column1, ... equi_columnN, asof_column)
    ```

    `ASOF JOIN` uses `equi_columnX` for joining on equality and `asof_column` for joining on the closest match with the `table_1.asof_column >= table2.asof_column` condition. The `asof_column` column must be the last in the `USING` clause.

For example, consider the following tables:

```text
     table_1                           table_2

  event   | ev_time | user_id       event   | ev_time | user_id
----------|---------|----------   ----------|---------|----------             
              ...                               ...
event_1_1 |  12:00  |  42         event_2_1 |  11:59  |   42
              ...                 event_2_2 |  12:30  |   42
event_1_2 |  13:00  |  42         event_2_3 |  13:00  |   42
              ...                               ...
```

`ASOF JOIN` can take the timestamp of a user event from `table_1` and find an event in `table_2` where the timestamp is closest (equal to or less) to the timestamp of the event from `table_1`. Here, the `user_id` column can be used for joining on equality and the `ev_time` column can be used for joining on the closest match. In our example, `event_1_1` can be joined with `event_2_1` and `event_1_2` can be joined with `event_2_3`, but `event_2_2` can't be joined.


!!! note "Note"
    `ASOF` join is **not** supported in the [Join](../operations/table_engines/join.md) table engine.

To set the default strictness value, use the session configuration parameter [join_default_strictness](../operations/settings/settings.md#settings-join_default_strictness).

#### GLOBAL JOIN

When using a normal `JOIN`, the query is sent to remote servers. Subqueries are run on each of them in order to make the right table, and the join is performed with this table. In other words, the right table is formed on each server separately.

When using `GLOBAL ... JOIN`, first the requestor server runs a subquery to calculate the right table. This temporary table is passed to each remote server, and queries are run on them using the temporary data that was transmitted.

Be careful when using `GLOBAL`. For more information, see the section [Distributed subqueries](#select-distributed-subqueries).

#### Usage Recommendations

When running a `JOIN`, there is no optimization of the order of execution in relation to other stages of the query. The join (a search in the right table) is run before filtering in `WHERE` and before aggregation. In order to explicitly set the processing order, we recommend running a `JOIN` subquery with a subquery.

Example:

```sql
SELECT
    CounterID,
    hits,
    visits
FROM
(
    SELECT
        CounterID,
        count() AS hits
    FROM test.hits
    GROUP BY CounterID
) ANY LEFT JOIN
(
    SELECT
        CounterID,
        sum(Sign) AS visits
    FROM test.visits
    GROUP BY CounterID
) USING CounterID
ORDER BY hits DESC
LIMIT 10
```

```text
┌─CounterID─┬───hits─┬─visits─┐
│   1143050 │ 523264 │  13665 │
│    731962 │ 475698 │ 102716 │
│    722545 │ 337212 │ 108187 │
│    722889 │ 252197 │  10547 │
│   2237260 │ 196036 │   9522 │
│  23057320 │ 147211 │   7689 │
│    722818 │  90109 │  17847 │
│     48221 │  85379 │   4652 │
│  19762435 │  77807 │   7026 │
│    722884 │  77492 │  11056 │
└───────────┴────────┴────────┘
```

Subqueries don't allow you to set names or use them for referencing a column from a specific subquery.
The columns specified in `USING` must have the same names in both subqueries, and the other columns must be named differently. You can use aliases to change the names of columns in subqueries (the example uses the aliases `hits` and `visits`).

The `USING` clause specifies one or more columns to join, which establishes the equality of these columns. The list of columns is set without brackets. More complex join conditions are not supported.

The right table (the subquery result) resides in RAM. If there isn't enough memory, you can't run a `JOIN`.

Each time a query is run with the same `JOIN`, the subquery is run again because the result is not cached. To avoid this, use the special [Join](../operations/table_engines/join.md) table engine, which is a prepared array for joining that is always in RAM.

In some cases, it is more efficient to use `IN` instead of `JOIN`.
Among the various types of `JOIN`, the most efficient is `ANY LEFT JOIN`, then `ANY INNER JOIN`. The least efficient are `ALL LEFT JOIN` and `ALL INNER JOIN`.

If you need a `JOIN` for joining with dimension tables (these are relatively small tables that contain dimension properties, such as names for advertising campaigns), a `JOIN` might not be very convenient due to the fact that the right table is re-accessed for every query. For such cases, there is an "external dictionaries" feature that you should use instead of `JOIN`. For more information, see the section [External dictionaries](dicts/external_dicts.md).

**Memory Limitations**

ClickHouse uses the [hash join](https://en.wikipedia.org/wiki/Hash_join) algorithm. ClickHouse takes the `<right_subquery>` and creates a hash table for it in RAM. If you need to restrict join operation memory consumption use the following settings:

- [max_rows_in_join](../operations/settings/query_complexity.md#settings-max_rows_in_join) — Limits number of rows in the hash table.
- [max_bytes_in_join](../operations/settings/query_complexity.md#settings-max_bytes_in_join) — Limits size of the hash table.

When any of these limits is reached, ClickHouse acts as the [join_overflow_mode](../operations/settings/query_complexity.md#settings-join_overflow_mode) setting instructs.

#### Processing of Empty or NULL Cells

While joining tables, the empty cells may appear. The setting [join_use_nulls](../operations/settings/settings.md#settings-join_use_nulls) define how ClickHouse fills these cells.

If the `JOIN` keys are [Nullable](../data_types/nullable.md) fields, the rows where at least one of the keys has the value [NULL](syntax.md#null-literal) are not joined.

#### Syntax Limitations

For multiple `JOIN` clauses in a single `SELECT` query:

- Taking all the columns via `*` is available only if tables are joined, not subqueries.
- The `PREWHERE` clause is not available.

For `ON`, `WHERE`, and `GROUP BY` clauses:

- Arbitrary expressions cannot be used in `ON`, `WHERE`, and `GROUP BY` clauses, but you can define an expression in a `SELECT` clause and then use it in these clauses via an alias.


### WHERE Clause

If there is a WHERE clause, it must contain an expression with the UInt8 type. This is usually an expression with comparison and logical operators.
This expression will be used for filtering data before all other transformations.

If indexes are supported by the database table engine, the expression is evaluated on the ability to use indexes.


### PREWHERE Clause

This clause has the same meaning as the WHERE clause. The difference is in which data is read from the table.
When using PREWHERE, first only the columns necessary for executing PREWHERE are read. Then the other columns are read that are needed for running the query, but only those blocks where the PREWHERE expression is true.

It makes sense to use PREWHERE if there are filtration conditions that are used by a minority of the columns in the query, but that provide strong data filtration. This reduces the volume of data to read.

For example, it is useful to write PREWHERE for queries that extract a large number of columns, but that only have filtration for a few columns.

PREWHERE is only supported by tables from the `*MergeTree` family.

A query may simultaneously specify PREWHERE and WHERE. In this case, PREWHERE precedes WHERE.

If the 'optimize_move_to_prewhere' setting is set to 1 and PREWHERE is omitted, the system uses heuristics to automatically move parts of expressions from WHERE to PREWHERE.

### GROUP BY Clause {#select-group-by-clause}

This is one of the most important parts of a column-oriented DBMS.

If there is a GROUP BY clause, it must contain a list of expressions. Each expression will be referred to here as a "key".
All the expressions in the SELECT, HAVING, and ORDER BY clauses must be calculated from keys or from aggregate functions. In other words, each column selected from the table must be used either in keys or inside aggregate functions.

If a query contains only table columns inside aggregate functions, the GROUP BY clause can be omitted, and aggregation by an empty set of keys is assumed.

Example:

```sql
SELECT
    count(),
    median(FetchTiming > 60 ? 60 : FetchTiming),
    count() - sum(Refresh)
FROM hits
```

However, in contrast to standard SQL, if the table doesn't have any rows (either there aren't any at all, or there aren't any after using WHERE to filter), an empty result is returned, and not the result from one of the rows containing the initial values of aggregate functions.

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

For every different key value encountered, GROUP BY calculates a set of aggregate function values.

GROUP BY is not supported for array columns.

A constant can't be specified as arguments for aggregate functions. Example: sum(1). Instead of this, you can get rid of the constant. Example: `count()`.

#### NULL processing

For grouping, ClickHouse interprets [NULL](syntax.md) as a value, and `NULL=NULL`.

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

#### WITH TOTALS Modifier

If the WITH TOTALS modifier is specified, another row will be calculated. This row will have key columns containing default values (zeros or empty lines), and columns of aggregate functions with the values calculated across all the rows (the "total" values).

This extra row is output in JSON\*, TabSeparated\*, and Pretty\* formats, separately from the other rows. In the other formats, this row is not output.

In JSON\* formats, this row is output as a separate 'totals' field. In TabSeparated\* formats, the row comes after the main result, preceded by an empty row (after the other data). In Pretty\* formats, the row is output as a separate table after the main result.

`WITH TOTALS` can be run in different ways when HAVING is present. The behavior depends on the 'totals_mode' setting.
By default, `totals_mode = 'before_having'`. In this case, 'totals' is calculated across all rows, including the ones that don't pass through HAVING and 'max_rows_to_group_by'.

The other alternatives include only the rows that pass through HAVING in 'totals', and behave differently with the setting `max_rows_to_group_by` and `group_by_overflow_mode = 'any'`.

`after_having_exclusive` – Don't include rows that didn't pass through `max_rows_to_group_by`. In other words, 'totals' will have less than or the same number of rows as it would if `max_rows_to_group_by` were omitted.

`after_having_inclusive` – Include all the rows that didn't pass through 'max_rows_to_group_by' in 'totals'. In other words, 'totals' will have more than or the same number of rows as it would if `max_rows_to_group_by` were omitted.

`after_having_auto` – Count the number of rows that passed through HAVING. If it is more than a certain amount (by default, 50%), include all the rows that didn't pass through 'max_rows_to_group_by' in 'totals'. Otherwise, do not include them.

`totals_auto_threshold` – By default, 0.5. The coefficient for `after_having_auto`.

If `max_rows_to_group_by` and `group_by_overflow_mode = 'any'` are not used, all variations of `after_having` are the same, and you can use any of them (for example, `after_having_auto`).

You can use WITH TOTALS in subqueries, including subqueries in the JOIN clause (in this case, the respective total values are combined).

#### GROUP BY in External Memory {#select-group-by-in-external-memory}

You can enable dumping temporary data to the disk to restrict memory usage during `GROUP BY`.
The [max_bytes_before_external_group_by](../operations/settings/settings.md#settings-max_bytes_before_external_group_by) setting determines the threshold RAM consumption for dumping `GROUP BY` temporary data to the file system. If set to 0 (the default), it is disabled.

When using `max_bytes_before_external_group_by`, we recommend that you set `max_memory_usage` about twice as high. This is necessary because there are two stages to aggregation: reading the date and forming intermediate data (1) and merging the intermediate data (2). Dumping data to the file system can only occur during stage 1. If the temporary data wasn't dumped, then stage 2 might require up to the same amount of memory as in stage 1.

For example, if [max_memory_usage](../operations/settings/settings.md#settings_max_memory_usage) was set to 10000000000 and you want to use external aggregation, it makes sense to set `max_bytes_before_external_group_by` to 10000000000, and max_memory_usage to 20000000000. When external aggregation is triggered (if there was at least one dump of temporary data), maximum consumption of RAM is only slightly more than `max_bytes_before_external_group_by`.

With distributed query processing, external aggregation is performed on remote servers. In order for the requester server to use only a small amount of RAM, set `distributed_aggregation_memory_efficient` to 1.

When merging data flushed to the disk, as well as when merging results from remote servers when the `distributed_aggregation_memory_efficient` setting is enabled, consumes up to `1/256 * the_number_of_threads` from the total amount of RAM.

When external aggregation is enabled, if there was less than `max_bytes_before_external_group_by` of data (i.e. data was not flushed), the query runs just as fast as without external aggregation. If any temporary data was flushed, the run time will be several times longer (approximately three times).

If you have an `ORDER BY` with a `LIMIT` after `GROUP BY`, then the amount of used RAM depends on the amount of data in `LIMIT`, not in the whole table. But if the `ORDER BY` doesn't have `LIMIT`, don't forget to enable external sorting (`max_bytes_before_external_sort`).

### LIMIT BY Clause

A query with the `LIMIT n BY expressions` clause selects the first `n` rows for each distinct value of `expressions`. The key for `LIMIT BY` can contain any number of [expressions](syntax.md#syntax-expressions).

ClickHouse supports the following syntax:

- `LIMIT [offset_value, ]n BY expressions`
- `LIMIT n OFFSET offset_value BY expressions`

During query processing, ClickHouse selects data ordered by sorting key. The sorting key is set explicitly using an [ORDER BY](#select-order-by) clause or implicitly as a property of the table engine. Then ClickHouse applies `LIMIT n BY expressions` and returns the first `n` rows for each distinct combination of `expressions`. If `OFFSET` is specified, then for each data block that belongs to a distinct combination of `expressions`, ClickHouse skips `offset_value` number of rows from the beginning of the block and returns a maximum of `n` rows as a result. If `offset_value` is bigger than the number of rows in the data block, ClickHouse returns zero rows from the block.

`LIMIT BY` is not related to `LIMIT`. They can both be used in the same query.

**Examples**

Sample table:

```sql
CREATE TABLE limit_by(id Int, val Int) ENGINE = Memory;
INSERT INTO limit_by values(1, 10), (1, 11), (1, 12), (2, 20), (2, 21);
```

Queries:

```sql
SELECT * FROM limit_by ORDER BY id, val LIMIT 2 BY id
```
```text
┌─id─┬─val─┐
│  1 │  10 │
│  1 │  11 │
│  2 │  20 │
│  2 │  21 │
└────┴─────┘
```
```sql
SELECT * FROM limit_by ORDER BY id, val LIMIT 1, 2 BY id
```
```text
┌─id─┬─val─┐
│  1 │  11 │
│  1 │  12 │
│  2 │  21 │
└────┴─────┘
```

The `SELECT * FROM limit_by ORDER BY id, val LIMIT 2 OFFSET 1 BY id` query returns the same result.

The following query returns the top 5 referrers for each `domain, device_type` pair with a maximum of 100 rows in total (`LIMIT n BY + LIMIT`).

```sql
SELECT
    domainWithoutWWW(URL) AS domain,
    domainWithoutWWW(REFERRER_URL) AS referrer,
    device_type,
    count() cnt
FROM hits
GROUP BY domain, referrer, device_type
ORDER BY cnt DESC
LIMIT 5 BY domain, device_type
LIMIT 100
```

### HAVING Clause

Allows filtering the result received after GROUP BY, similar to the WHERE clause.
WHERE and HAVING differ in that WHERE is performed before aggregation (GROUP BY), while HAVING is performed after it.
If aggregation is not performed, HAVING can't be used.


### ORDER BY Clause {#select-order-by}

The ORDER BY clause contains a list of expressions, which can each be assigned DESC or ASC (the sorting direction). If the direction is not specified, ASC is assumed. ASC is sorted in ascending order, and DESC in descending order. The sorting direction applies to a single expression, not to the entire list. Example: `ORDER BY Visits DESC, SearchPhrase`

For sorting by String values, you can specify collation (comparison). Example: `ORDER BY SearchPhrase COLLATE 'tr'` - for sorting by keyword in ascending order, using the Turkish alphabet, case insensitive, assuming that strings are UTF-8 encoded. COLLATE can be specified or not for each expression in ORDER BY independently. If ASC or DESC is specified, COLLATE is specified after it. When using COLLATE, sorting is always case-insensitive.

We only recommend using COLLATE for final sorting of a small number of rows, since sorting with COLLATE is less efficient than normal sorting by bytes.

Rows that have identical values for the list of sorting expressions are output in an arbitrary order, which can also be nondeterministic (different each time).
If the ORDER BY clause is omitted, the order of the rows is also undefined, and may be nondeterministic as well.

`NaN` and `NULL` sorting order:

- With the modifier `NULLS FIRST` — First `NULL`, then `NaN`, then other values.
- With the modifier `NULLS LAST` — First the values, then `NaN`, then `NULL`.
- Default — The same as with the `NULLS LAST` modifier.

Example:

For the table

```text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │    2 │
│ 1 │  nan │
│ 2 │    2 │
│ 3 │    4 │
│ 5 │    6 │
│ 6 │  nan │
│ 7 │ ᴺᵁᴸᴸ │
│ 6 │    7 │
│ 8 │    9 │
└───┴──────┘
```

Run the query `SELECT * FROM t_null_nan ORDER BY y NULLS FIRST` to get:

```text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 7 │ ᴺᵁᴸᴸ │
│ 1 │  nan │
│ 6 │  nan │
│ 2 │    2 │
│ 2 │    2 │
│ 3 │    4 │
│ 5 │    6 │
│ 6 │    7 │
│ 8 │    9 │
└───┴──────┘
```

When floating point numbers are sorted, NaNs are separate from the other values. Regardless of the sorting order, NaNs come at the end. In other words, for ascending sorting they are placed as if they are larger than all the other numbers, while for descending sorting they are placed as if they are smaller than the rest.

Less RAM is used if a small enough LIMIT is specified in addition to ORDER BY. Otherwise, the amount of memory spent is proportional to the volume of data for sorting. For distributed query processing, if GROUP BY is omitted, sorting is partially done on remote servers, and the results are merged on the requestor server. This means that for distributed sorting, the volume of data to sort can be greater than the amount of memory on a single server.

If there is not enough RAM, it is possible to perform sorting in external memory (creating temporary files on a disk). Use the setting `max_bytes_before_external_sort` for this purpose. If it is set to 0 (the default), external sorting is disabled. If it is enabled, when the volume of data to sort reaches the specified number of bytes, the collected data is sorted and dumped into a temporary file. After all data is read, all the sorted files are merged and the results are output. Files are written to the /var/lib/clickhouse/tmp/ directory in the config (by default, but you can use the 'tmp_path' parameter to change this setting).

Running a query may use more memory than 'max_bytes_before_external_sort'. For this reason, this setting must have a value significantly smaller than 'max_memory_usage'. As an example, if your server has 128 GB of RAM and you need to run a single query, set 'max_memory_usage' to 100 GB, and 'max_bytes_before_external_sort' to 80 GB.

External sorting works much less effectively than sorting in RAM.

### SELECT Clause {#select-select}

[Expressions](syntax.md#syntax-expressions) specified in the `SELECT` clause are calculated after all the operations in the clauses described above are finished. These expressions work as if they apply to separate rows in the result. If expressions in the `SELECT` clause contain aggregate functions, then ClickHouse processes aggregate functions and expressions used as their arguments during the [GROUP BY](#select-group-by-clause) aggregation.

If you want to include all columns in the result, use the asterisk (`*`) symbol. For example, `SELECT * FROM ...`.

To match some columns in the result with a [re2](https://en.wikipedia.org/wiki/RE2_(software)) regular expression, you can use the `COLUMNS` expression.

```sql
COLUMNS('regexp')
```

For example, consider the table:

```sql
CREATE TABLE default.col_names (aa Int8, ab Int8, bc Int8) ENGINE = TinyLog
```

The following query selects data from all the columns containing the `a` symbol in their name.

```sql
SELECT COLUMNS('a') FROM col_names
```
```text
┌─aa─┬─ab─┐
│  1 │  1 │
└────┴────┘
```

The selected columns are returned not in the alphabetical order.

You can use multiple `COLUMNS` expressions in a query and apply functions to them.

For example:

```sql
SELECT COLUMNS('a'), COLUMNS('c'), toTypeName(COLUMNS('c')) FROM col_names
```
```text
┌─aa─┬─ab─┬─bc─┬─toTypeName(bc)─┐
│  1 │  1 │  1 │ Int8           │
└────┴────┴────┴────────────────┘
```

Each column returned by the `COLUMNS` expression is passed to the function as a separate argument. Also you can pass other arguments to the function if it supports them. Be careful when using functions. If a function doesn't support the number of arguments you have passed to it, ClickHouse throws an exception.

For example:

```sql
SELECT COLUMNS('a') + COLUMNS('c') FROM col_names
```
```text
Received exception from server (version 19.14.1):
Code: 42. DB::Exception: Received from localhost:9000. DB::Exception: Number of arguments for function plus doesn't match: passed 3, should be 2. 
```

In this example, `COLUMNS('a')` returns two columns: `aa` and `ab`. `COLUMNS('c')` returns the `bc` column. The `+` operator can't apply to 3 arguments, so ClickHouse throws an exception with the relevant message.

Columns that matched the `COLUMNS` expression can have different data types. If `COLUMNS` doesn't match any columns and is the only expression in `SELECT`, ClickHouse throws an exception.


### DISTINCT Clause {#select-distinct}

If DISTINCT is specified, only a single row will remain out of all the sets of fully matching rows in the result.
The result will be the same as if GROUP BY were specified across all the fields specified in SELECT without aggregate functions. But there are several differences from GROUP BY:

- DISTINCT can be applied together with GROUP BY.
- When ORDER BY is omitted and LIMIT is defined, the query stops running immediately after the required number of different rows has been read.
- Data blocks are output as they are processed, without waiting for the entire query to finish running.

DISTINCT is not supported if SELECT has at least one array column.

`DISTINCT` works with [NULL](syntax.md) as if `NULL` were a specific value, and `NULL=NULL`. In other words, in the `DISTINCT` results, different combinations with `NULL` only occur once.

ClickHouse supports using the `DISTINCT` and `ORDER BY` clauses for different columns in one query. The `DISTINCT` clause is executed before the `ORDER BY` clause.

Example table:

```text
┌─a─┬─b─┐
│ 2 │ 1 │
│ 1 │ 2 │
│ 3 │ 3 │
│ 2 │ 4 │
└───┴───┘
```

When selecting data with the `SELECT DISTINCT a FROM t1 ORDER BY b ASC` query, we get the following result:

```text
┌─a─┐
│ 2 │
│ 1 │
│ 3 │
└───┘
```

If we change the sorting direction `SELECT DISTINCT a FROM t1 ORDER BY b DESC`, we get the following result:

```text
┌─a─┐
│ 3 │
│ 1 │
│ 2 │
└───┘
```

Row `2, 4` was cut before sorting.

Take this implementation specificity into account when programming queries.

### LIMIT Clause

`LIMIT m` allows you to select the first `m` rows from the result.

`LIMIT n, m` allows you to select the first `m` rows from the result after skipping the first `n` rows. The `LIMIT m OFFSET n` syntax is also supported.

`n` and `m` must be non-negative integers.

If there isn't an `ORDER BY` clause that explicitly sorts results, the result may be arbitrary and nondeterministic.

### UNION ALL Clause

You can use UNION ALL to combine any number of queries. Example:

```sql
SELECT CounterID, 1 AS table, toInt64(count()) AS c
    FROM test.hits
    GROUP BY CounterID

UNION ALL

SELECT CounterID, 2 AS table, sum(Sign) AS c
    FROM test.visits
    GROUP BY CounterID
    HAVING c > 0
```

Only UNION ALL is supported. The regular UNION (UNION DISTINCT) is not supported. If you need UNION DISTINCT, you can write SELECT DISTINCT from a subquery containing UNION ALL.

Queries that are parts of UNION ALL can be run simultaneously, and their results can be mixed together.

The structure of results (the number and type of columns) must match for the queries. But the column names can differ. In this case, the column names for the final result will be taken from the first query. Type casting is performed for unions. For example, if two queries being combined have the same field with non-`Nullable` and `Nullable` types from a compatible type, the resulting `UNION ALL` has a `Nullable` type field.

Queries that are parts of UNION ALL can't be enclosed in brackets. ORDER BY and LIMIT are applied to separate queries, not to the final result. If you need to apply a conversion to the final result, you can put all the queries with UNION ALL in a subquery in the FROM clause.

### INTO OUTFILE Clause

Add the `INTO OUTFILE filename` clause (where filename is a string literal) to redirect query output to the specified file.
In contrast to MySQL, the file is created on the client side. The query will fail if a file with the same filename already exists.
This functionality is available in the command-line client and clickhouse-local (a query sent via HTTP interface will fail).

The default output format is TabSeparated (the same as in the command-line client batch mode).

### FORMAT Clause

Specify 'FORMAT format' to get data in any specified format.
You can use this for convenience, or for creating dumps.
For more information, see the section "Formats".
If the FORMAT clause is omitted, the default format is used, which depends on both the settings and the interface used for accessing the DB. For the HTTP interface and the command-line client in batch mode, the default format is TabSeparated. For the command-line client in interactive mode, the default format is PrettyCompact (it has attractive and compact tables).

When using the command-line client, data is passed to the client in an internal efficient format. The client independently interprets the FORMAT clause of the query and formats the data itself (thus relieving the network and the server from the load).


### IN Operators {#select-in-operators}

The `IN`, `NOT IN`, `GLOBAL IN`, and `GLOBAL NOT IN` operators are covered separately, since their functionality is quite rich.

The left side of the operator is either a single column or a tuple.

Examples:

```sql
SELECT UserID IN (123, 456) FROM ...
SELECT (CounterID, UserID) IN ((34, 123), (101500, 456)) FROM ...
```

If the left side is a single column that is in the index, and the right side is a set of constants, the system uses the index for processing the query.

Don't list too many values explicitly (i.e. millions). If a data set is large, put it in a temporary table (for example, see the section "External data for query processing"), then use a subquery.

The right side of the operator can be a set of constant expressions, a set of tuples with constant expressions (shown in the examples above), or the name of a database table or SELECT subquery in brackets.

If the right side of the operator is the name of a table (for example, `UserID IN users`), this is equivalent to the subquery `UserID IN (SELECT * FROM users)`. Use this when working with external data that is sent along with the query. For example, the query can be sent together with a set of user IDs loaded to the 'users' temporary table, which should be filtered.

If the right side of the operator is a table name that has the Set engine (a prepared data set that is always in RAM), the data set will not be created over again for each query.

The subquery may specify more than one column for filtering tuples.
Example:

```sql
SELECT (CounterID, UserID) IN (SELECT CounterID, UserID FROM ...) FROM ...
```

The columns to the left and right of the IN operator should have the same type.

The IN operator and subquery may occur in any part of the query, including in aggregate functions and lambda functions.
Example:

```sql
SELECT
    EventDate,
    avg(UserID IN
    (
        SELECT UserID
        FROM test.hits
        WHERE EventDate = toDate('2014-03-17')
    )) AS ratio
FROM test.hits
GROUP BY EventDate
ORDER BY EventDate ASC
```

```text
┌──EventDate─┬────ratio─┐
│ 2014-03-17 │        1 │
│ 2014-03-18 │ 0.807696 │
│ 2014-03-19 │ 0.755406 │
│ 2014-03-20 │ 0.723218 │
│ 2014-03-21 │ 0.697021 │
│ 2014-03-22 │ 0.647851 │
│ 2014-03-23 │ 0.648416 │
└────────────┴──────────┘
```

For each day after March 17th, count the percentage of pageviews made by users who visited the site on March 17th.
A subquery in the IN clause is always run just one time on a single server. There are no dependent subqueries.

#### NULL processing

During request processing, the IN operator assumes that the result of an operation with [NULL](syntax.md) is always equal to `0`, regardless of whether `NULL` is on the right or left side of the operator. `NULL` values are not included in any dataset, do not correspond to each other and cannot be compared.

Here is an example with the `t_null` table:

```text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │    3 │
└───┴──────┘
```

Running the query `SELECT x FROM t_null WHERE y IN (NULL,3)` gives you the following result:

```text
┌─x─┐
│ 2 │
└───┘
```

You can see that the row in which `y = NULL` is thrown out of the query results. This is because ClickHouse can't decide whether `NULL` is included in the `(NULL,3)` set, returns `0` as the result of the operation, and `SELECT` excludes this row from the final output.

```sql
SELECT y IN (NULL, 3)
FROM t_null
```
```text
┌─in(y, tuple(NULL, 3))─┐
│                     0 │
│                     1 │
└───────────────────────┘
```


#### Distributed Subqueries {#select-distributed-subqueries}

There are two options for IN-s with subqueries (similar to JOINs): normal `IN` / `JOIN` and `GLOBAL IN` / `GLOBAL JOIN`. They differ in how they are run for distributed query processing.

!!! attention
    Remember that the algorithms described below may work differently depending on the [settings](../operations/settings/settings.md) `distributed_product_mode` setting.

When using the regular IN, the query is sent to remote servers, and each of them runs the subqueries in the `IN` or `JOIN` clause.

When using `GLOBAL IN` / `GLOBAL JOINs`, first all the subqueries are run for `GLOBAL IN` / `GLOBAL JOINs`, and the results are collected in temporary tables. Then the temporary tables are sent to each remote server, where the queries are run using this temporary data.

For a non-distributed query, use the regular `IN` / `JOIN`.

Be careful when using subqueries in the `IN` / `JOIN` clauses for distributed query processing.

Let's look at some examples. Assume that each server in the cluster has a normal **local_table**. Each server also has a **distributed_table** table with the **Distributed** type, which looks at all the servers in the cluster.

For a query to the **distributed_table**, the query will be sent to all the remote servers and run on them using the **local_table**.

For example, the query

```sql
SELECT uniq(UserID) FROM distributed_table
```

will be sent to all remote servers as

```sql
SELECT uniq(UserID) FROM local_table
```

and run on each of them in parallel, until it reaches the stage where intermediate results can be combined. Then the intermediate results will be returned to the requestor server and merged on it, and the final result will be sent to the client.

Now let's examine a query with IN:

```sql
SELECT uniq(UserID) FROM distributed_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM local_table WHERE CounterID = 34)
```

- Calculation of the intersection of audiences of two sites.

This query will be sent to all remote servers as

```sql
SELECT uniq(UserID) FROM local_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM local_table WHERE CounterID = 34)
```

In other words, the data set in the IN clause will be collected on each server independently, only across the data that is stored locally on each of the servers.

This will work correctly and optimally if you are prepared for this case and have spread data across the cluster servers such that the data for a single UserID resides entirely on a single server. In this case, all the necessary data will be available locally on each server. Otherwise, the result will be inaccurate. We refer to this variation of the query as "local IN".

To correct how the query works when data is spread randomly across the cluster servers, you could specify **distributed_table** inside a subquery. The query would look like this:

```sql
SELECT uniq(UserID) FROM distributed_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM distributed_table WHERE CounterID = 34)
```

This query will be sent to all remote servers as

```sql
SELECT uniq(UserID) FROM local_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM distributed_table WHERE CounterID = 34)
```

The subquery will begin running on each remote server. Since the subquery uses a distributed table, the subquery that is on each remote server will be resent to every remote server as

```sql
SELECT UserID FROM local_table WHERE CounterID = 34
```

For example, if you have a cluster of 100 servers, executing the entire query will require 10,000 elementary requests, which is generally considered unacceptable.

In such cases, you should always use GLOBAL IN instead of IN. Let's look at how it works for the query

```sql
SELECT uniq(UserID) FROM distributed_table WHERE CounterID = 101500 AND UserID GLOBAL IN (SELECT UserID FROM distributed_table WHERE CounterID = 34)
```

The requestor server will run the subquery

```sql
SELECT UserID FROM distributed_table WHERE CounterID = 34
```

and the result will be put in a temporary table in RAM. Then the request will be sent to each remote server as

```sql
SELECT uniq(UserID) FROM local_table WHERE CounterID = 101500 AND UserID GLOBAL IN _data1
```

and the temporary table `_data1` will be sent to every remote server with the query (the name of the temporary table is implementation-defined).

This is more optimal than using the normal IN. However, keep the following points in mind:

1. When creating a temporary table, data is not made unique. To reduce the volume of data transmitted over the network, specify DISTINCT in the subquery. (You don't need to do this for a normal IN.)
2. The temporary table will be sent to all the remote servers. Transmission does not account for network topology. For example, if 10 remote servers reside in a datacenter that is very remote in relation to the requestor server, the data will be sent 10 times over the channel to the remote datacenter. Try to avoid large data sets when using GLOBAL IN.
3. When transmitting data to remote servers, restrictions on network bandwidth are not configurable. You might overload the network.
4. Try to distribute data across servers so that you don't need to use GLOBAL IN on a regular basis.
5. If you need to use GLOBAL IN often, plan the location of the ClickHouse cluster so that a single group of replicas resides in no more than one data center with a fast network between them, so that a query can be processed entirely within a single data center.

It also makes sense to specify a local table in the `GLOBAL IN` clause, in case this local table is only available on the requestor server and you want to use data from it on remote servers.

### Extreme Values

In addition to results, you can also get minimum and maximum values for the results columns. To do this, set the **extremes** setting to 1. Minimums and maximums are calculated for numeric types, dates, and dates with times. For other columns, the default values are output.

An extra two rows are calculated – the minimums and maximums, respectively. These extra two rows are output in `JSON*`, `TabSeparated*`, and `Pretty*` [formats](../interfaces/formats.md), separate from the other rows. They are not output for other formats.

In `JSON*` formats, the extreme values are output in a separate 'extremes' field. In `TabSeparated*` formats, the row comes after the main result, and after 'totals' if present. It is preceded by an empty row (after the other data). In `Pretty*` formats, the row is output as a separate table after the main result, and after `totals` if present.

Extreme values are calculated for rows before `LIMIT`, but after `LIMIT BY`. However, when using `LIMIT offset, size`, the rows before `offset` are included in `extremes`. In stream requests, the result may also include a small number of rows that passed through `LIMIT`.

### Notes

The `GROUP BY` and `ORDER BY` clauses do not support positional arguments. This contradicts MySQL, but conforms to standard SQL.
For example, `GROUP BY 1, 2` will be interpreted as grouping by constants (i.e. aggregation of all rows into one).

You can use synonyms (`AS` aliases) in any part of a query.

You can put an asterisk in any part of a query instead of an expression. When the query is analyzed, the asterisk is expanded to a list of all table columns (excluding the `MATERIALIZED` and `ALIAS` columns). There are only a few cases when using an asterisk is justified:

- When creating a table dump.
- For tables containing just a few columns, such as system tables.
- For getting information about what columns are in a table. In this case, set `LIMIT 1`. But it is better to use the `DESC TABLE` query.
- When there is strong filtration on a small number of columns using `PREWHERE`.
- In subqueries (since columns that aren't needed for the external query are excluded from subqueries).

In all other cases, we don't recommend using the asterisk, since it only gives you the drawbacks of a columnar DBMS instead of the advantages. In other words using the asterisk is not recommended.

[Original article](https://clickhouse.yandex/docs/en/query_language/select/) <!--hide-->
