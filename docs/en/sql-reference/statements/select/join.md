# JOIN Clause {#select-join}

Joins the data in the normal [SQL JOIN](https://en.wikipedia.org/wiki/Join_(SQL)) sense.

!!! info "Note"
    Not related to [ARRAY JOIN](array-join.md).

``` sql
SELECT <expr_list>
FROM <left_subquery>
[GLOBAL] [ANY|ALL] [INNER|LEFT|RIGHT|FULL|CROSS] [OUTER] JOIN <right_subquery>
(ON <expr_list>)|(USING <column_list>) ...
```

The table names can be specified instead of `<left_subquery>` and `<right_subquery>`. This is equivalent to the `SELECT * FROM table` subquery, except in a special case when the table has the [Join](../../../engines/table-engines/special/join.md) engine – an array prepared for joining.

## Supported Types of JOIN {#select-join-types}

-   `INNER JOIN` (or `JOIN`)
-   `LEFT JOIN` (or `LEFT OUTER JOIN`)
-   `RIGHT JOIN` (or `RIGHT OUTER JOIN`)
-   `FULL JOIN` (or `FULL OUTER JOIN`)
-   `CROSS JOIN` (or `,` )

See the standard [SQL JOIN](https://en.wikipedia.org/wiki/Join_(SQL)) description.

## Multiple JOIN {#multiple-join}

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

## Strictness {#select-join-strictness}

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

         table_1                           table_2
      event   | ev_time | user_id       event   | ev_time | user_id
    ----------|---------|----------   ----------|---------|----------
                  ...                               ...
    event_1_1 |  12:00  |  42         event_2_1 |  11:59  |   42
                  ...                 event_2_2 |  12:30  |   42
    event_1_2 |  13:00  |  42         event_2_3 |  13:00  |   42
                  ...                               ...

`ASOF JOIN` can take the timestamp of a user event from `table_1` and find an event in `table_2` where the timestamp is closest to the timestamp of the event from `table_1` corresponding to the closest match condition. Equal timestamp values are the closest if available. Here, the `user_id` column can be used for joining on equality and the `ev_time` column can be used for joining on the closest match. In our example, `event_1_1` can be joined with `event_2_1` and `event_1_2` can be joined with `event_2_3`, but `event_2_2` can’t be joined.

!!! note "Note"
    `ASOF` join is **not** supported in the [Join](../../../engines/table-engines/special/join.md) table engine.

To set the default strictness value, use the session configuration parameter [join\_default\_strictness](../../../operations/settings/settings.md#settings-join_default_strictness).

## GLOBAL JOIN {#global-join}

When using a normal `JOIN`, the query is sent to remote servers. Subqueries are run on each of them in order to make the right table, and the join is performed with this table. In other words, the right table is formed on each server separately.

When using `GLOBAL ... JOIN`, first the requestor server runs a subquery to calculate the right table. This temporary table is passed to each remote server, and queries are run on them using the temporary data that was transmitted.

Be careful when using `GLOBAL`. For more information, see the section [Distributed subqueries](#select-distributed-subqueries).

## Usage Recommendations {#usage-recommendations}

When running a `JOIN`, there is no optimization of the order of execution in relation to other stages of the query. The join (a search in the right table) is run before filtering in `WHERE` and before aggregation. In order to explicitly set the processing order, we recommend running a `JOIN` subquery with a subquery.

Example:

``` sql
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

``` text
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

Subqueries don’t allow you to set names or use them for referencing a column from a specific subquery.
The columns specified in `USING` must have the same names in both subqueries, and the other columns must be named differently. You can use aliases to change the names of columns in subqueries (the example uses the aliases `hits` and `visits`).

The `USING` clause specifies one or more columns to join, which establishes the equality of these columns. The list of columns is set without brackets. More complex join conditions are not supported.

The right table (the subquery result) resides in RAM. If there isn’t enough memory, you can’t run a `JOIN`.

Each time a query is run with the same `JOIN`, the subquery is run again because the result is not cached. To avoid this, use the special [Join](../../../engines/table-engines/special/join.md) table engine, which is a prepared array for joining that is always in RAM.

In some cases, it is more efficient to use `IN` instead of `JOIN`.
Among the various types of `JOIN`, the most efficient is `ANY LEFT JOIN`, then `ANY INNER JOIN`. The least efficient are `ALL LEFT JOIN` and `ALL INNER JOIN`.

If you need a `JOIN` for joining with dimension tables (these are relatively small tables that contain dimension properties, such as names for advertising campaigns), a `JOIN` might not be very convenient due to the fact that the right table is re-accessed for every query. For such cases, there is an “external dictionaries” feature that you should use instead of `JOIN`. For more information, see the section [External dictionaries](../../dictionaries/external-dictionaries/external-dicts.md).

**Memory Limitations**

ClickHouse uses the [hash join](https://en.wikipedia.org/wiki/Hash_join) algorithm. ClickHouse takes the `<right_subquery>` and creates a hash table for it in RAM. If you need to restrict join operation memory consumption use the following settings:

-   [max\_rows\_in\_join](../../../operations/settings/query-complexity.md#settings-max_rows_in_join) — Limits number of rows in the hash table.
-   [max\_bytes\_in\_join](../../../operations/settings/query-complexity.md#settings-max_bytes_in_join) — Limits size of the hash table.

When any of these limits is reached, ClickHouse acts as the [join\_overflow\_mode](../../../operations/settings/query-complexity.md#settings-join_overflow_mode) setting instructs.

## Processing of Empty or NULL Cells {#processing-of-empty-or-null-cells}

While joining tables, the empty cells may appear. The setting [join\_use\_nulls](../../../operations/settings/settings.md#join_use_nulls) define how ClickHouse fills these cells.

If the `JOIN` keys are [Nullable](../../data-types/nullable.md) fields, the rows where at least one of the keys has the value [NULL](../../../sql-reference/syntax.md#null-literal) are not joined.

## Syntax Limitations {#syntax-limitations}

For multiple `JOIN` clauses in a single `SELECT` query:

-   Taking all the columns via `*` is available only if tables are joined, not subqueries.
-   The `PREWHERE` clause is not available.

For `ON`, `WHERE`, and `GROUP BY` clauses:

-   Arbitrary expressions cannot be used in `ON`, `WHERE`, and `GROUP BY` clauses, but you can define an expression in a `SELECT` clause and then use it in these clauses via an alias.
