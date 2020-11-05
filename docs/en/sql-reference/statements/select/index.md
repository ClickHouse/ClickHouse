---
title: SELECT Query
toc_folder_title: SELECT
toc_priority: 32
toc_title: Overview
---

# SELECT Query {#select-queries-syntax}

`SELECT` queries perform data retrieval. By default, the requested data is returned to the client, while in conjunction with [INSERT INTO](../../../sql-reference/statements/insert-into.md) it can be forwarded to a different table.

## Syntax {#syntax}

``` sql
[WITH expr_list|(subquery)]
SELECT [DISTINCT] expr_list
[FROM [db.]table | (subquery) | table_function] [FINAL]
[SAMPLE sample_coeff]
[ARRAY JOIN ...]
[GLOBAL] [ANY|ALL|ASOF] [INNER|LEFT|RIGHT|FULL|CROSS] [OUTER|SEMI|ANTI] JOIN (subquery)|table (ON <expr_list>)|(USING <column_list>)
[PREWHERE expr]
[WHERE expr]
[GROUP BY expr_list] [WITH TOTALS]
[HAVING expr]
[ORDER BY expr_list] [WITH FILL] [FROM expr] [TO expr] [STEP expr]
[LIMIT [offset_value, ]n BY columns]
[LIMIT [n, ]m] [WITH TIES]
[UNION ALL ...]
[INTO OUTFILE filename]
[FORMAT format]
```

All clauses are optional, except for the required list of expressions immediately after `SELECT` which is covered in more detail [below](#select-clause).

Specifics of each optional clause are covered in separate sections, which are listed in the same order as they are executed:

-   [WITH clause](../../../sql-reference/statements/select/with.md)
-   [FROM clause](../../../sql-reference/statements/select/from.md)
-   [SAMPLE clause](../../../sql-reference/statements/select/sample.md)
-   [JOIN clause](../../../sql-reference/statements/select/join.md)
-   [PREWHERE clause](../../../sql-reference/statements/select/prewhere.md)
-   [WHERE clause](../../../sql-reference/statements/select/where.md)
-   [GROUP BY clause](../../../sql-reference/statements/select/group-by.md)
-   [LIMIT BY clause](../../../sql-reference/statements/select/limit-by.md)
-   [HAVING clause](../../../sql-reference/statements/select/having.md)
-   [SELECT clause](#select-clause)
-   [DISTINCT clause](../../../sql-reference/statements/select/distinct.md)
-   [LIMIT clause](../../../sql-reference/statements/select/limit.md)
-   [UNION ALL clause](../../../sql-reference/statements/select/union-all.md)
-   [INTO OUTFILE clause](../../../sql-reference/statements/select/into-outfile.md)
-   [FORMAT clause](../../../sql-reference/statements/select/format.md)

## SELECT Clause {#select-clause}

[Expressions](../../../sql-reference/syntax.md#syntax-expressions) specified in the `SELECT` clause are calculated after all the operations in the clauses described above are finished. These expressions work as if they apply to separate rows in the result. If expressions in the `SELECT` clause contain aggregate functions, then ClickHouse processes aggregate functions and expressions used as their arguments during the [GROUP BY](../../../sql-reference/statements/select/group-by.md) aggregation.

If you want to include all columns in the result, use the asterisk (`*`) symbol. For example, `SELECT * FROM ...`.

To match some columns in the result with a [re2](https://en.wikipedia.org/wiki/RE2_(software)) regular expression, you can use the `COLUMNS` expression.

``` sql
COLUMNS('regexp')
```

For example, consider the table:

``` sql
CREATE TABLE default.col_names (aa Int8, ab Int8, bc Int8) ENGINE = TinyLog
```

The following query selects data from all the columns containing the `a` symbol in their name.

``` sql
SELECT COLUMNS('a') FROM col_names
```

``` text
┌─aa─┬─ab─┐
│  1 │  1 │
└────┴────┘
```

The selected columns are returned not in the alphabetical order.

You can use multiple `COLUMNS` expressions in a query and apply functions to them.

For example:

``` sql
SELECT COLUMNS('a'), COLUMNS('c'), toTypeName(COLUMNS('c')) FROM col_names
```

``` text
┌─aa─┬─ab─┬─bc─┬─toTypeName(bc)─┐
│  1 │  1 │  1 │ Int8           │
└────┴────┴────┴────────────────┘
```

Each column returned by the `COLUMNS` expression is passed to the function as a separate argument. Also you can pass other arguments to the function if it supports them. Be careful when using functions. If a function doesn’t support the number of arguments you have passed to it, ClickHouse throws an exception.

For example:

``` sql
SELECT COLUMNS('a') + COLUMNS('c') FROM col_names
```

``` text
Received exception from server (version 19.14.1):
Code: 42. DB::Exception: Received from localhost:9000. DB::Exception: Number of arguments for function plus doesn't match: passed 3, should be 2.
```

In this example, `COLUMNS('a')` returns two columns: `aa` and `ab`. `COLUMNS('c')` returns the `bc` column. The `+` operator can’t apply to 3 arguments, so ClickHouse throws an exception with the relevant message.

Columns that matched the `COLUMNS` expression can have different data types. If `COLUMNS` doesn’t match any columns and is the only expression in `SELECT`, ClickHouse throws an exception.

### Asterisk {#asterisk}

You can put an asterisk in any part of a query instead of an expression. When the query is analyzed, the asterisk is expanded to a list of all table columns (excluding the `MATERIALIZED` and `ALIAS` columns). There are only a few cases when using an asterisk is justified:

-   When creating a table dump.
-   For tables containing just a few columns, such as system tables.
-   For getting information about what columns are in a table. In this case, set `LIMIT 1`. But it is better to use the `DESC TABLE` query.
-   When there is strong filtration on a small number of columns using `PREWHERE`.
-   In subqueries (since columns that aren’t needed for the external query are excluded from subqueries).

In all other cases, we don’t recommend using the asterisk, since it only gives you the drawbacks of a columnar DBMS instead of the advantages. In other words using the asterisk is not recommended.

### Extreme Values {#extreme-values}

In addition to results, you can also get minimum and maximum values for the results columns. To do this, set the **extremes** setting to 1. Minimums and maximums are calculated for numeric types, dates, and dates with times. For other columns, the default values are output.

An extra two rows are calculated – the minimums and maximums, respectively. These extra two rows are output in `JSON*`, `TabSeparated*`, and `Pretty*` [formats](../../../interfaces/formats.md), separate from the other rows. They are not output for other formats.

In `JSON*` formats, the extreme values are output in a separate ‘extremes’ field. In `TabSeparated*` formats, the row comes after the main result, and after ‘totals’ if present. It is preceded by an empty row (after the other data). In `Pretty*` formats, the row is output as a separate table after the main result, and after `totals` if present.

Extreme values are calculated for rows before `LIMIT`, but after `LIMIT BY`. However, when using `LIMIT offset, size`, the rows before `offset` are included in `extremes`. In stream requests, the result may also include a small number of rows that passed through `LIMIT`.

### Notes {#notes}

You can use synonyms (`AS` aliases) in any part of a query.

The `GROUP BY` and `ORDER BY` clauses do not support positional arguments. This contradicts MySQL, but conforms to standard SQL. For example, `GROUP BY 1, 2` will be interpreted as grouping by constants (i.e. aggregation of all rows into one).

## Select modifiers {select-modifiers}

You can modify the results returned from a `SELECT` query, as follows.

### Apply {select-apply}

The `APPLY` function allows you to use aggregate functions on multiple columns. 

Syntax:

``` sql
SELECT * APPLY from [db.]table;
```

You can use the `APPLY` function to sum the results of each row across columns.

Syntax:

``` sql
SELECT * APPLY(sum) from [db.]table;
```

For example, consider the table:

``` sql
SHOW CREATE TABLE test_apply;
```

```
┌─statement───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ CREATE TABLE default.test_apply
(
    `i` Int64,
    `j` Int16,
    `k` Int64
)
ENGINE = MergeTree
ORDER BY i
SETTINGS index_granularity = 8192 │
└─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

``` sql
INSERT INTO test_apply VALUES (100, 10, 324), (120, 8, 23);

SELECT * APPLY(sum) from test_apply;
```

```
┌─sum(i)─┬─sum(j)─┬─sum(k)─┐
│    220 │     18 │    347 │
└────────┴────────┴────────┘
```

You can also use `APPLY` to find average values of each row across columns.

Syntax:

``` sql
SELECT [db.]table.* APPLY(avg) from [db.]table;
```

For example:

``` sql
SELECT test_apply.* APPLY(avg) from test_apply;
```

```
┌─avg(i)─┬─avg(j)─┬─avg(k)─┐
│    110 │      9 │  173.5 │
└────────┴────────┴────────┘
```

The `APPLY` function can also help to find out the maxium length of all string columns:

Syntax:

``` sql
SELECT COLUMNS('[name1name2]') APPLY(toString) APPLY(length) APPLY(max) from [db.]table;
```

For example:

``` sql
SELECT COLUMNS('[jk]') APPLY(toString) APPLY(length) APPLY(max) from test_apply;
```

```
┌─max(length(toString(j)))─┬─max(length(toString(k)))─┐
│                        2 │                        3 │
└──────────────────────────┴──────────────────────────┘
```

### Except {select_except}

The `SELECT * EXCEPT` operator specifies the names of one or more columns to exclude from the result. All matching column names are omitted from the output.

Syntax:

``` sql
SELECT * EXCEPT (name1) from [db.]table;
```

For example, consider the table:

``` sql
SHOW CREATE TABLE test_except;
```

```
┌─statement────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ CREATE TABLE default.test_except
(
    `i` Int64,
    `j` Int16,
    `k` Int64
)
ENGINE = MergeTree
ORDER BY i
SETTINGS index_granularity = 8192 │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

``` sql
INSERT INTO test_except VALUES (100, 10, 324), (120, 8, 23);

SELECT * EXCEPT (i) from test_except;
```

```
┌──j─┬───k─┐
│ 10 │ 324 │
│  8 │  23 │
└────┴─────┘
```

`SELECT * EXCEPT` can be also used with `APPLY`:

Syntax:

``` sql
SELECT * EXCEPT(name1) APPLY(sum) from [db.]table;
```

For example:

``` sql
SELECT * EXCEPT(i) APPLY(sum) from test_except;
```

```
┌─sum(j)─┬─sum(k)─┐
│     18 │    347 │
└────────┴────────┘
```

ClickHouse database also supports `SELECT * EXCEPT (col1, col2, ...)` syntax. This would select all columns except for those declared within the `EXCEPT` keyword. This is specially useful for tables with many columns.

Syntax:

``` sql
SELECT * EXCEPT(name1, name2) from [db.]table;
```

For example:

``` sql
SELECT * EXCEPT(i, j) from test_except;
```

```
┌───k─┐
│ 324 │
│  23 │
└─────┘
```

### Replace {select_replace}

You can use `SELECT * REPLACE` to specify one or more `expression AS identifier` clauses.  
Each identifier must match a column name from the `SELECT * statement`. In the output column list, the column that matches the identifier in a `REPLACE` operator is replaced by the expression in that `REPLACE` operator.
A `SELECT * REPLACE` operator does not change the names or order of columns. However, it can change the value and the value type.

Syntax:

``` sql
SELECT * REPLACE(name1 + 1 AS name1) from [db.]table;
```

For example, consider the table:

``` sql
SHOW CREATE TABLE test_replace;
```

```
┌─statement─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ CREATE TABLE default.test_replace
(
    `i` Int64,
    `j` Int16,
    `k` Int64
)
ENGINE = MergeTree
ORDER BY i
SETTINGS index_granularity = 8192 │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

``` sql
INSERT INTO test_replace VALUES (100, 10, 324), (120, 8, 23);

SELECT * REPLACE(i + 1 AS i) from test_replace;
```

```
┌───i─┬──j─┬───k─┐
│ 101 │ 10 │ 324 │
│ 121 │  8 │  23 │
└─────┴────┴─────┘
```

`SELECT * REPLACE` can also be used with `APPLY`:

Syntax:

``` sql
SELECT * REPLACE(name1 + 1 AS name1) APPLY(sum) from [db.]table;
```

For exapmle:

``` sql
SELECT * REPLACE(i + 1 AS i) APPLY(sum) from test_replace;
```

```
┌─sum(plus(i, 1))─┬─sum(j)─┬─sum(k)─┐
│             222 │     18 │    347 │
└─────────────────┴────────┴────────┘
```

ClickHouse database also supports multiple `REPLACE` in a row. You can choose as many columns as you need and change its values or value types.

Syntax:

``` sql
SELECT [db]table.* REPLACE(name1 + 2 AS name1, name2 + 1 AS name2) APPLY(avg) from [db.]table;
```

For example:

``` sql
SELECT test_replace.* REPLACE(j + 2 AS j, i + 1 AS i) APPLY(avg) from test_replace;
```

```
┌─avg(plus(i, 1))─┬─avg(plus(j, 2))─┬─avg(k)─┐
│             111 │              11 │  173.5 │
└─────────────────┴─────────────────┴────────┘
```

## Implementation Details {#implementation-details}

If the query omits the `DISTINCT`, `GROUP BY` and `ORDER BY` clauses and the `IN` and `JOIN` subqueries, the query will be completely stream processed, using O(1) amount of RAM. Otherwise, the query might consume a lot of RAM if the appropriate restrictions are not specified:

-   `max_memory_usage`
-   `max_rows_to_group_by`
-   `max_rows_to_sort`
-   `max_rows_in_distinct`
-   `max_bytes_in_distinct`
-   `max_rows_in_set`
-   `max_bytes_in_set`
-   `max_rows_in_join`
-   `max_bytes_in_join`
-   `max_bytes_before_external_sort`
-   `max_bytes_before_external_group_by`

For more information, see the section “Settings”. It is possible to use external sorting (saving temporary tables to a disk) and external aggregation.

{## [Original article](https://clickhouse.tech/docs/en/sql-reference/statements/select/) ##}
