---
description: 'Documentation for JOIN Clause'
sidebar_label: 'JOIN'
slug: /sql-reference/statements/select/join
title: 'JOIN Clause'
keywords: ['INNER JOIN', 'LEFT JOIN', 'LEFT OUTER JOIN', 'RIGHT JOIN', 'RIGHT OUTER JOIN', 'FULL OUTER JOIN', 'CROSS JOIN', 'LEFT SEMI JOIN', 'RIGHT SEMI JOIN', 'LEFT ANTI JOIN', 'RIGHT ANTI JOIN', 'LEFT ANY JOIN', 'RIGHT ANY JOIN', 'INNER ANY JOIN', 'ASOF JOIN', 'LEFT ASOF JOIN', 'PASTE JOIN']
---

# JOIN clause

The `JOIN` clause produces a new table by combining columns from one or multiple tables by using values common to each. It is a common operation in databases with SQL support, which corresponds to [relational algebra](https://en.wikipedia.org/wiki/Relational_algebra#Joins_and_join-like_operators) join. The special case of one table join is often referred to as a "self-join".

**Syntax**

```sql
SELECT <expr_list>
FROM <left_table>
[GLOBAL] [INNER|LEFT|RIGHT|FULL|CROSS] [OUTER|SEMI|ANTI|ANY|ALL|ASOF] JOIN <right_table>
(ON <expr_list>)|(USING <column_list>) ...
```

Expressions from the `ON` clause and columns from the `USING` clause are called "join keys". Unless otherwise stated, a `JOIN` produces a [Cartesian product](https://en.wikipedia.org/wiki/Cartesian_product) from rows with matching "join keys", which might produce results with many more rows than the source tables.

## Supported types of JOIN {#supported-types-of-join}

All standard [SQL JOIN](https://en.wikipedia.org/wiki/Join_(SQL)) types are supported:

| Type              | Description                                                                   |
|-------------------|-------------------------------------------------------------------------------|
| `INNER JOIN`      | only matching rows are returned.                                              |
| `LEFT OUTER JOIN` | non-matching rows from left table are returned in addition to matching rows.  |
| `RIGHT OUTER JOIN`| non-matching rows from right table are returned in addition to matching rows. |
| `FULL OUTER JOIN` | non-matching rows from both tables are returned in addition to matching rows. |
| `CROSS JOIN`      | produces cartesian product of whole tables, "join keys" are **not** specified.|

- `JOIN` without a type specified implies `INNER`.
- The keyword `OUTER` can be safely omitted.
- An alternative syntax for `CROSS JOIN` is specifying multiple tables in the [`FROM` clause](../../../sql-reference/statements/select/from.md) separated by commas.

Additional join types available in ClickHouse are:

| Type                                        | Description                                                                                                                               |
|---------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------|
| `LEFT SEMI JOIN`, `RIGHT SEMI JOIN`         | An allowlist on "join keys", without producing a cartesian product.                                                                        |
| `LEFT ANTI JOIN`, `RIGHT ANTI JOIN`         | A denylist on "join keys", without producing a cartesian product.                                                                        |
| `LEFT ANY JOIN`, `RIGHT ANY JOIN`, `INNER ANY JOIN` | Partially (for opposite side of `LEFT` and `RIGHT`) or completely (for `INNER` and `FULL`) disables the cartesian product for standard `JOIN` types. |
| `ASOF JOIN`, `LEFT ASOF JOIN`               | Joining sequences with a non-exact match. `ASOF JOIN` usage is described below.                                                           |
| `PASTE JOIN`                                | Performs a horizontal concatenation of two tables.                                                                                          |

:::note
When [join_algorithm](../../../operations/settings/settings.md#join_algorithm) is set to `partial_merge`, `RIGHT JOIN` and `FULL JOIN` are supported only with `ALL` strictness (`SEMI`, `ANTI`, `ANY`, and `ASOF` are not supported).
:::

## Settings {#settings}

The default join type can be overridden using [`join_default_strictness`](../../../operations/settings/settings.md#join_default_strictness) setting.

The behavior of the ClickHouse server for `ANY JOIN` operations depends on the [`any_join_distinct_right_table_keys`](../../../operations/settings/settings.md#any_join_distinct_right_table_keys) setting.

**See also**

- [`join_algorithm`](../../../operations/settings/settings.md#join_algorithm)
- [`join_any_take_last_row`](../../../operations/settings/settings.md#join_any_take_last_row)
- [`join_use_nulls`](../../../operations/settings/settings.md#join_use_nulls)
- [`partial_merge_join_rows_in_right_blocks`](../../../operations/settings/settings.md#partial_merge_join_rows_in_right_blocks)
- [`join_on_disk_max_files_to_merge`](../../../operations/settings/settings.md#join_on_disk_max_files_to_merge)
- [`any_join_distinct_right_table_keys`](../../../operations/settings/settings.md#any_join_distinct_right_table_keys)

Use the `cross_to_inner_join_rewrite` setting to define the behavior when ClickHouse fails to rewrite a `CROSS JOIN` as an `INNER JOIN`. The default value is `1`, which  allows the join to continue but it will be slower. Set `cross_to_inner_join_rewrite` to `0` if you want an error to be thrown, and set it to `2` to not run the cross joins but instead force a rewrite of all comma/cross joins. If the rewriting fails when the value is `2`, you will receive an error message stating "Please, try to simplify `WHERE` section".

## ON section conditions {#on-section-conditions}

An `ON` section can contain several conditions combined using the `AND` and `OR` operators. Conditions specifying join keys must:
- reference both left and right tables
- use the equality operator

Other conditions may use other logical operators but they must reference either the left or the right table of a query.

Rows are joined if the whole complex condition is met. If the conditions are not met, rows may still be included in the result depending on the `JOIN` type. Note that if the same conditions are placed in a `WHERE` section and they are not met, then rows are always filtered out from the result.

The `OR` operator inside the `ON` clause works using the hash join algorithm — for each `OR` argument with join keys for `JOIN`, a separate hash table is created, so memory consumption and query execution time grow linearly with an increase in the number of expressions `OR` of the `ON` clause.

:::note
If a condition references columns from different tables, then only the equality operator (`=`) is supported so far.
:::

**Example**

Consider `table_1` and `table_2`:

```response
┌─Id─┬─name─┐     ┌─Id─┬─text───────────┬─scores─┐
│  1 │ A    │     │  1 │ Text A         │     10 │
│  2 │ B    │     │  1 │ Another text A │     12 │
│  3 │ C    │     │  2 │ Text B         │     15 │
└────┴──────┘     └────┴────────────────┴────────┘
```

Query with one join key condition and an additional condition for `table_2`:

```sql
SELECT name, text FROM table_1 LEFT OUTER JOIN table_2
    ON table_1.Id = table_2.Id AND startsWith(table_2.text, 'Text');
```

Note that the result contains the row with the name `C` and the empty text column. It is included into the result because an `OUTER` type of a join is used.

```response
┌─name─┬─text───┐
│ A    │ Text A │
│ B    │ Text B │
│ C    │        │
└──────┴────────┘
```

Query with `INNER` type of a join and multiple conditions:

```sql
SELECT name, text, scores FROM table_1 INNER JOIN table_2
    ON table_1.Id = table_2.Id AND table_2.scores > 10 AND startsWith(table_2.text, 'Text');
```

Result:

```sql
┌─name─┬─text───┬─scores─┐
│ B    │ Text B │     15 │
└──────┴────────┴────────┘
```
Query with `INNER` type of a join and condition with `OR`:

```sql
CREATE TABLE t1 (`a` Int64, `b` Int64) ENGINE = MergeTree() ORDER BY a;

CREATE TABLE t2 (`key` Int32, `val` Int64) ENGINE = MergeTree() ORDER BY key;

INSERT INTO t1 SELECT number as a, -a as b from numbers(5);

INSERT INTO t2 SELECT if(number % 2 == 0, toInt64(number), -number) as key, number as val from numbers(5);

SELECT a, b, val FROM t1 INNER JOIN t2 ON t1.a = t2.key OR t1.b = t2.key;
```

Result:

```response
┌─a─┬──b─┬─val─┐
│ 0 │  0 │   0 │
│ 1 │ -1 │   1 │
│ 2 │ -2 │   2 │
│ 3 │ -3 │   3 │
│ 4 │ -4 │   4 │
└───┴────┴─────┘
```

Query with `INNER` type of a join and conditions with `OR` and `AND`:

:::note

By default, non-equal conditions are supported as long as they use columns from the same table.
For example, `t1.a = t2.key AND t1.b > 0 AND t2.b > t2.c`, because `t1.b > 0` uses columns only from `t1` and `t2.b > t2.c` uses columns only from `t2`.
However, you can try experimental support for conditions like `t1.a = t2.key AND t1.b > t2.key`, check out the section below for more details.

:::

```sql
SELECT a, b, val FROM t1 INNER JOIN t2 ON t1.a = t2.key OR t1.b = t2.key AND t2.val > 3;
```

Result:

```response
┌─a─┬──b─┬─val─┐
│ 0 │  0 │   0 │
│ 2 │ -2 │   2 │
│ 4 │ -4 │   4 │
└───┴────┴─────┘
```

## JOIN with inequality conditions for columns from different tables {#join-with-inequality-conditions-for-columns-from-different-tables}

Clickhouse currently supports `ALL/ANY/SEMI/ANTI INNER/LEFT/RIGHT/FULL JOIN` with inequality conditions in addition to equality conditions. The inequality conditions are supported only for `hash` and `grace_hash` join algorithms. The inequality conditions are not supported with `join_use_nulls`.

**Example**

Table `t1`:

```response
┌─key──┬─attr─┬─a─┬─b─┬─c─┐
│ key1 │ a    │ 1 │ 1 │ 2 │
│ key1 │ b    │ 2 │ 3 │ 2 │
│ key1 │ c    │ 3 │ 2 │ 1 │
│ key1 │ d    │ 4 │ 7 │ 2 │
│ key1 │ e    │ 5 │ 5 │ 5 │
│ key2 │ a2   │ 1 │ 1 │ 1 │
│ key4 │ f    │ 2 │ 3 │ 4 │
└──────┴──────┴───┴───┴───┘
```

Table `t2`

```response
┌─key──┬─attr─┬─a─┬─b─┬─c─┐
│ key1 │ A    │ 1 │ 2 │ 1 │
│ key1 │ B    │ 2 │ 1 │ 2 │
│ key1 │ C    │ 3 │ 4 │ 5 │
│ key1 │ D    │ 4 │ 1 │ 6 │
│ key3 │ a3   │ 1 │ 1 │ 1 │
│ key4 │ F    │ 1 │ 1 │ 1 │
└──────┴──────┴───┴───┴───┘
```

```sql
SELECT t1.*, t2.* FROM t1 LEFT JOIN t2 ON t1.key = t2.key AND (t1.a < t2.a) ORDER BY (t1.key, t1.attr, t2.key, t2.attr);
```

```response
key1    a    1    1    2    key1    B    2    1    2
key1    a    1    1    2    key1    C    3    4    5
key1    a    1    1    2    key1    D    4    1    6
key1    b    2    3    2    key1    C    3    4    5
key1    b    2    3    2    key1    D    4    1    6
key1    c    3    2    1    key1    D    4    1    6
key1    d    4    7    2            0    0    \N
key1    e    5    5    5            0    0    \N
key2    a2    1    1    1            0    0    \N
key4    f    2    3    4            0    0    \N
```


## NULL values in JOIN keys {#null-values-in-join-keys}

`NULL` is not equal to any value, including itself. This means that if a `JOIN` key has a `NULL` value in one table, it won't match a `NULL` value in the other table.

**Example**

Table `A`:

```response
┌───id─┬─name────┐
│    1 │ Alice   │
│    2 │ Bob     │
│ ᴺᵁᴸᴸ │ Charlie │
└──────┴─────────┘
```

Table `B`:

```response
┌───id─┬─score─┐
│    1 │    90 │
│    3 │    85 │
│ ᴺᵁᴸᴸ │    88 │
└──────┴───────┘
```

```sql
SELECT A.name, B.score FROM A LEFT JOIN B ON A.id = B.id
```

```response
┌─name────┬─score─┐
│ Alice   │    90 │
│ Bob     │     0 │
│ Charlie │     0 │
└─────────┴───────┘
```

Notice that the row with `Charlie` from table `A` and the row with score 88 from table `B` are not in the result because of the `NULL` value in the `JOIN` key.

In case you want to match `NULL` values, use the `isNotDistinctFrom` function to compare the `JOIN` keys.

```sql
SELECT A.name, B.score FROM A LEFT JOIN B ON isNotDistinctFrom(A.id, B.id)
```

```markdown
┌─name────┬─score─┐
│ Alice   │    90 │
│ Bob     │     0 │
│ Charlie │    88 │
└─────────┴───────┘
```

## ASOF JOIN usage {#asof-join-usage}

`ASOF JOIN` is useful when you need to join records that have no exact match.

This JOIN algorithm requires a special column in tables. This column:

- Must contain an ordered sequence.
- Can be one of the following types: [Int, UInt](../../../sql-reference/data-types/int-uint.md), [Float](../../../sql-reference/data-types/float.md), [Date](../../../sql-reference/data-types/date.md), [DateTime](../../../sql-reference/data-types/datetime.md), [Decimal](../../../sql-reference/data-types/decimal.md).
- For the `hash` join algorithm it can't be the only column in the `JOIN` clause.

Syntax `ASOF JOIN ... ON`:

```sql
SELECT expressions_list
FROM table_1
ASOF LEFT JOIN table_2
ON equi_cond AND closest_match_cond
```

You can use any number of equality conditions and exactly one closest match condition. For example, `SELECT count() FROM table_1 ASOF LEFT JOIN table_2 ON table_1.a == table_2.b AND table_2.t <= table_1.t`.

Conditions supported for the closest match: `>`, `>=`, `<`, `<=`.

Syntax `ASOF JOIN ... USING`:

```sql
SELECT expressions_list
FROM table_1
ASOF JOIN table_2
USING (equi_column1, ... equi_columnN, asof_column)
```

`ASOF JOIN` uses `equi_columnX` for joining on equality and `asof_column` for joining on the closest match with the `table_1.asof_column >= table_2.asof_column` condition. The `asof_column` column is always the last one in the `USING` clause.

For example, consider the following tables:

```text
         table_1                           table_2
      event   | ev_time | user_id       event   | ev_time | user_id
    ----------|---------|---------- ----------|---------|----------
                  ...                               ...
    event_1_1 |  12:00  |  42         event_2_1 |  11:59  |   42
                  ...                 event_2_2 |  12:30  |   42
    event_1_2 |  13:00  |  42         event_2_3 |  13:00  |   42
                  ...                               ...
```

`ASOF JOIN` can take the timestamp of a user event from `table_1` and find an event in `table_2` where the timestamp is closest to the timestamp of the event from `table_1` corresponding to the closest match condition. Equal timestamp values are the closest if available. Here, the `user_id` column can be used for joining on equality and the `ev_time` column can be used for joining on the closest match. In our example, `event_1_1` can be joined with `event_2_1` and `event_1_2` can be joined with `event_2_3`, but `event_2_2` can't be joined.

:::note
`ASOF JOIN` is supported only by `hash` and `full_sorting_merge` join algorithms.
It's **not** supported in the [Join](../../../engines/table-engines/special/join.md) table engine.
:::

## PASTE JOIN usage {#paste-join-usage}

The result of `PASTE JOIN` is a table that contains all columns from left subquery followed by all columns from the right subquery.
The rows are matched based on their positions in the original tables (the order of rows should be defined).
If the subqueries return a different number of rows, extra rows will be cut.

Example:
```sql
SELECT *
FROM
(
    SELECT number AS a
    FROM numbers(2)
) AS t1
PASTE JOIN
(
    SELECT number AS a
    FROM numbers(2)
    ORDER BY a DESC
) AS t2

┌─a─┬─t2.a─┐
│ 0 │    1 │
│ 1 │    0 │
└───┴──────┘
```

Note: in this case result can be nondeterministic if the reading is parallel. For example:

```sql
SELECT *
FROM
(
    SELECT number AS a
    FROM numbers_mt(5)
) AS t1
PASTE JOIN
(
    SELECT number AS a
    FROM numbers(10)
    ORDER BY a DESC
) AS t2
SETTINGS max_block_size = 2;

┌─a─┬─t2.a─┐
│ 2 │    9 │
│ 3 │    8 │
└───┴──────┘
┌─a─┬─t2.a─┐
│ 0 │    7 │
│ 1 │    6 │
└───┴──────┘
┌─a─┬─t2.a─┐
│ 4 │    5 │
└───┴──────┘
```

## Distributed JOIN {#distributed-join}

There are two ways to execute a JOIN involving distributed tables:

- When using a normal `JOIN`, the query is sent to remote servers. Subqueries are run on each of them in order to make the right table, and the join is performed with this table. In other words, the right table is formed on each server separately.
- When using `GLOBAL ... JOIN`, first the requestor server runs a subquery to calculate the right table. This temporary table is passed to each remote server, and queries are run on them using the temporary data that was transmitted.

Be careful when using `GLOBAL`. For more information, see the [Distributed subqueries](/sql-reference/operators/in#distributed-subqueries) section.

## Implicit type conversion {#implicit-type-conversion}

`INNER JOIN`, `LEFT JOIN`, `RIGHT JOIN`, and `FULL JOIN` queries support the implicit type conversion for "join keys". However the query can not be executed, if join keys from the left and the right tables cannot be converted to a single type (for example, there is no data type that can hold all values from both `UInt64` and `Int64`, or `String` and `Int32`).

**Example**

Consider the table `t_1`:
```response
┌─a─┬─b─┬─toTypeName(a)─┬─toTypeName(b)─┐
│ 1 │ 1 │ UInt16        │ UInt8         │
│ 2 │ 2 │ UInt16        │ UInt8         │
└───┴───┴───────────────┴───────────────┘
```
and the table `t_2`:
```response
┌──a─┬────b─┬─toTypeName(a)─┬─toTypeName(b)───┐
│ -1 │    1 │ Int16         │ Nullable(Int64) │
│  1 │   -1 │ Int16         │ Nullable(Int64) │
│  1 │    1 │ Int16         │ Nullable(Int64) │
└────┴──────┴───────────────┴─────────────────┘
```

The query
```sql
SELECT a, b, toTypeName(a), toTypeName(b) FROM t_1 FULL JOIN t_2 USING (a, b);
```
returns the set:
```response
┌──a─┬────b─┬─toTypeName(a)─┬─toTypeName(b)───┐
│  1 │    1 │ Int32         │ Nullable(Int64) │
│  2 │    2 │ Int32         │ Nullable(Int64) │
│ -1 │    1 │ Int32         │ Nullable(Int64) │
│  1 │   -1 │ Int32         │ Nullable(Int64) │
└────┴──────┴───────────────┴─────────────────┘
```

## Usage recommendations {#usage-recommendations}

### Processing of empty or NULL cells {#processing-of-empty-or-null-cells}

While joining tables, the empty cells may appear. The setting [join_use_nulls](../../../operations/settings/settings.md#join_use_nulls) define how ClickHouse fills these cells.

If the `JOIN` keys are [Nullable](../../../sql-reference/data-types/nullable.md) fields, the rows where at least one of the keys has the value [NULL](/sql-reference/syntax#null) are not joined.

### Syntax {#syntax}

The columns specified in `USING` must have the same names in both subqueries, and the other columns must be named differently. You can use aliases to change the names of columns in subqueries.

The `USING` clause specifies one or more columns to join, which establishes the equality of these columns. The list of columns is set without brackets. More complex join conditions are not supported.

### Syntax Limitations {#syntax-limitations}

For multiple `JOIN` clauses in a single `SELECT` query:

- Taking all the columns via `*` is available only if tables are joined, not subqueries.
- The `PREWHERE` clause is not available.
- The `USING` clause is not available.

For `ON`, `WHERE`, and `GROUP BY` clauses:

- Arbitrary expressions cannot be used in `ON`, `WHERE`, and `GROUP BY` clauses, but you can define an expression in a `SELECT` clause and then use it in these clauses via an alias.

### Performance {#performance}

When running a `JOIN`, there is no optimization of the order of execution in relation to other stages of the query. The join (a search in the right table) is run before filtering in `WHERE` and before aggregation.

Each time a query is run with the same `JOIN`, the subquery is run again because the result is not cached. To avoid this, use the special [Join](../../../engines/table-engines/special/join.md) table engine, which is a prepared array for joining that is always in RAM.

In some cases, it is more efficient to use [IN](../../../sql-reference/operators/in.md) instead of `JOIN`.

If you need a `JOIN` for joining with dimension tables (these are relatively small tables that contain dimension properties, such as names for advertising campaigns), a `JOIN` might not be very convenient due to the fact that the right table is re-accessed for every query. For such cases, there is a "dictionaries" feature that you should use instead of `JOIN`. For more information, see the [Dictionaries](../../../sql-reference/dictionaries/index.md) section.

### Memory limitations {#memory-limitations}

By default, ClickHouse uses the [hash join](https://en.wikipedia.org/wiki/Hash_join) algorithm. ClickHouse takes the right_table and creates a hash table for it in RAM. If `join_algorithm = 'auto'` is enabled, then after some threshold of memory consumption, ClickHouse falls back to [merge](https://en.wikipedia.org/wiki/Sort-merge_join) join algorithm. For `JOIN` algorithms description see the [join_algorithm](../../../operations/settings/settings.md#join_algorithm) setting.

If you need to restrict `JOIN` operation memory consumption use the following settings:

- [max_rows_in_join](/operations/settings/settings#max_rows_in_join) — Limits number of rows in the hash table.
- [max_bytes_in_join](/operations/settings/settings#max_bytes_in_join) — Limits size of the hash table.

When any of these limits is reached, ClickHouse acts as the [join_overflow_mode](/operations/settings/settings#join_overflow_mode) 
setting instructs.

## Examples {#examples}

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

## Related content {#related-content}

- Blog: [ClickHouse: A Blazingly Fast DBMS with Full SQL Join Support - Part 1](https://clickhouse.com/blog/clickhouse-fully-supports-joins)
- Blog: [ClickHouse: A Blazingly Fast DBMS with Full SQL Join Support - Under the Hood - Part 2](https://clickhouse.com/blog/clickhouse-fully-supports-joins-hash-joins-part2)
- Blog: [ClickHouse: A Blazingly Fast DBMS with Full SQL Join Support - Under the Hood - Part 3](https://clickhouse.com/blog/clickhouse-fully-supports-joins-full-sort-partial-merge-part3)
- Blog: [ClickHouse: A Blazingly Fast DBMS with Full SQL Join Support - Under the Hood - Part 4](https://clickhouse.com/blog/clickhouse-fully-supports-joins-direct-join-part4)
