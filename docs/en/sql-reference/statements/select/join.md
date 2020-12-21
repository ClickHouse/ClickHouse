---
toc_title: JOIN
---

# JOIN Clause {#select-join}

Join produces a new table by combining columns from one or multiple tables by using values common to each. It is a common operation in databases with SQL support, which corresponds to [relational algebra](https://en.wikipedia.org/wiki/Relational_algebra#Joins_and_join-like_operators) join. The special case of one table join is often referred to as “self-join”.

Syntax:

``` sql
SELECT <expr_list>
FROM <left_table>
[GLOBAL] [INNER|LEFT|RIGHT|FULL|CROSS] [OUTER|SEMI|ANTI|ANY|ASOF] JOIN <right_table>
(ON <expr_list>)|(USING <column_list>) ...
```

Expressions from `ON` clause and columns from `USING` clause are called “join keys”. Unless otherwise stated, join produces a [Cartesian product](https://en.wikipedia.org/wiki/Cartesian_product) from rows with matching “join keys”, which might produce results with much more rows than the source tables.

## Supported Types of JOIN {#select-join-types}

All standard [SQL JOIN](https://en.wikipedia.org/wiki/Join_(SQL)) types are supported:

-   `INNER JOIN`, only matching rows are returned.
-   `LEFT OUTER JOIN`, non-matching rows from left table are returned in addition to matching rows.
-   `RIGHT OUTER JOIN`, non-matching rows from right table are returned in addition to matching rows.
-   `FULL OUTER JOIN`, non-matching rows from both tables are returned in addition to matching rows.
-   `CROSS JOIN`, produces cartesian product of whole tables, “join keys” are **not** specified.

`JOIN` without specified type implies `INNER`. Keyword `OUTER` can be safely omitted. Alternative syntax for `CROSS JOIN` is specifying multiple tables in [FROM clause](../../../sql-reference/statements/select/from.md) separated by commas.

Additional join types available in ClickHouse:

-   `LEFT SEMI JOIN` and `RIGHT SEMI JOIN`, a whitelist on “join keys”, without producing a cartesian product.
-   `LEFT ANTI JOIN` and `RIGHT ANTI JOIN`, a blacklist on “join keys”, without producing a cartesian product.
-   `LEFT ANY JOIN`, `RIGHT ANY JOIN` and `INNER ANY JOIN`, partially (for opposite side of `LEFT` and `RIGHT`) or completely (for `INNER` and `FULL`) disables the cartesian product for standard `JOIN` types.
-   `ASOF JOIN` and `LEFT ASOF JOIN`, joining sequences with a non-exact match. `ASOF JOIN` usage is described below.

## Setting {#join-settings}

!!! note "Note"
    The default join type can be overriden using [join\_default\_strictness](../../../operations/settings/settings.md#settings-join_default_strictness) setting.

    Also the behavior of ClickHouse server for `ANY JOIN` operations depends on the [any_join_distinct_right_table_keys](../../../operations/settings/settings.md#any_join_distinct_right_table_keys) setting.

### ASOF JOIN Usage {#asof-join-usage}

`ASOF JOIN` is useful when you need to join records that have no exact match.

Algorithm requires the special column in tables. This column:

-   Must contain an ordered sequence.
-   Can be one of the following types: [Int*, UInt*](../../../sql-reference/data-types/int-uint.md), [Float\*](../../../sql-reference/data-types/float.md), [Date](../../../sql-reference/data-types/date.md), [DateTime](../../../sql-reference/data-types/datetime.md), [Decimal\*](../../../sql-reference/data-types/decimal.md).
-   Can’t be the only column in the `JOIN` clause.

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

## Distributed Join {#global-join}

There are two ways to execute join involving distributed tables:

-   When using a normal `JOIN`, the query is sent to remote servers. Subqueries are run on each of them in order to make the right table, and the join is performed with this table. In other words, the right table is formed on each server separately.
-   When using `GLOBAL ... JOIN`, first the requestor server runs a subquery to calculate the right table. This temporary table is passed to each remote server, and queries are run on them using the temporary data that was transmitted.

Be careful when using `GLOBAL`. For more information, see the [Distributed subqueries](../../../sql-reference/operators/in.md#select-distributed-subqueries) section.

## Usage Recommendations {#usage-recommendations}

### Processing of Empty or NULL Cells {#processing-of-empty-or-null-cells}

While joining tables, the empty cells may appear. The setting [join\_use\_nulls](../../../operations/settings/settings.md#join_use_nulls) define how ClickHouse fills these cells.

If the `JOIN` keys are [Nullable](../../../sql-reference/data-types/nullable.md) fields, the rows where at least one of the keys has the value [NULL](../../../sql-reference/syntax.md#null-literal) are not joined.

### Syntax {#syntax}

The columns specified in `USING` must have the same names in both subqueries, and the other columns must be named differently. You can use aliases to change the names of columns in subqueries.

The `USING` clause specifies one or more columns to join, which establishes the equality of these columns. The list of columns is set without brackets. More complex join conditions are not supported.

### Syntax Limitations {#syntax-limitations}

For multiple `JOIN` clauses in a single `SELECT` query:

-   Taking all the columns via `*` is available only if tables are joined, not subqueries.
-   The `PREWHERE` clause is not available.

For `ON`, `WHERE`, and `GROUP BY` clauses:

-   Arbitrary expressions cannot be used in `ON`, `WHERE`, and `GROUP BY` clauses, but you can define an expression in a `SELECT` clause and then use it in these clauses via an alias.

### Performance {#performance}

When running a `JOIN`, there is no optimization of the order of execution in relation to other stages of the query. The join (a search in the right table) is run before filtering in `WHERE` and before aggregation.

Each time a query is run with the same `JOIN`, the subquery is run again because the result is not cached. To avoid this, use the special [Join](../../../engines/table-engines/special/join.md) table engine, which is a prepared array for joining that is always in RAM.

In some cases, it is more efficient to use [IN](../../../sql-reference/operators/in.md) instead of `JOIN`.

If you need a `JOIN` for joining with dimension tables (these are relatively small tables that contain dimension properties, such as names for advertising campaigns), a `JOIN` might not be very convenient due to the fact that the right table is re-accessed for every query. For such cases, there is an “external dictionaries” feature that you should use instead of `JOIN`. For more information, see the [External dictionaries](../../../sql-reference/dictionaries/external-dictionaries/external-dicts.md) section.

### Memory Limitations {#memory-limitations}

By default, ClickHouse uses the [hash join](https://en.wikipedia.org/wiki/Hash_join) algorithm. ClickHouse takes the `<right_table>` and creates a hash table for it in RAM. After some threshold of memory consumption, ClickHouse falls back to merge join algorithm.

If you need to restrict join operation memory consumption use the following settings:

-   [max\_rows\_in\_join](../../../operations/settings/query-complexity.md#settings-max_rows_in_join) — Limits number of rows in the hash table.
-   [max\_bytes\_in\_join](../../../operations/settings/query-complexity.md#settings-max_bytes_in_join) — Limits size of the hash table.

When any of these limits is reached, ClickHouse acts as the [join\_overflow\_mode](../../../operations/settings/query-complexity.md#settings-join_overflow_mode) setting instructs.

## Examples {#examples}

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
