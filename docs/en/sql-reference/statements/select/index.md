---
description: 'Documentation for SELECT Query'
sidebar_label: 'SELECT'
sidebar_position: 32
slug: /sql-reference/statements/select/
title: 'SELECT Query'
doc_type: 'reference'
---

# SELECT Query

`SELECT` queries perform data retrieval. By default, the requested data is returned to the client, while in conjunction with [INSERT INTO](../../../sql-reference/statements/insert-into.md) it can be forwarded to a different table.

## Syntax {#syntax}

```sql
[WITH expr_list(subquery)]
SELECT [DISTINCT [ON (column1, column2, ...)]] expr_list
[FROM [db.]table | (subquery) | table_function] [FINAL]
[SAMPLE sample_coeff]
[ARRAY JOIN ...]
[GLOBAL] [ANY|ALL|ASOF] [INNER|LEFT|RIGHT|FULL|CROSS] [OUTER|SEMI|ANTI] JOIN (subquery)|table [(alias1 [, alias2 ...])] (ON <expr_list>)|(USING <column_list>)
[PREWHERE expr]
[WHERE expr]
[GROUP BY expr_list] [WITH ROLLUP|WITH CUBE] [WITH TOTALS]
[HAVING expr]
[WINDOW window_expr_list]
[QUALIFY expr]
[ORDER BY expr_list] [WITH FILL] [FROM expr] [TO expr] [STEP expr] [INTERPOLATE [(expr_list)]]
[LIMIT [offset_value, ]n BY columns]
[LIMIT [n, ]m] [WITH TIES]
[SETTINGS ...]
[UNION  ...]
[INTO OUTFILE filename [TRUNCATE] [COMPRESSION type [LEVEL level]] ]
[FORMAT format]
```

All clauses are optional, except for the required list of expressions immediately after `SELECT` which is covered in more detail [below](#select-clause).

Specifics of each optional clause are covered in separate sections, which are listed in the same order as they are executed:

- [WITH clause](../../../sql-reference/statements/select/with.md)
- [SELECT clause](#select-clause)
- [DISTINCT clause](../../../sql-reference/statements/select/distinct.md)
- [FROM clause](../../../sql-reference/statements/select/from.md)
- [SAMPLE clause](../../../sql-reference/statements/select/sample.md)
- [JOIN clause](../../../sql-reference/statements/select/join.md)
- [PREWHERE clause](../../../sql-reference/statements/select/prewhere.md)
- [WHERE clause](../../../sql-reference/statements/select/where.md)
- [WINDOW clause](../../../sql-reference/window-functions/index.md)
- [GROUP BY clause](/sql-reference/statements/select/group-by)
- [LIMIT BY clause](../../../sql-reference/statements/select/limit-by.md)
- [HAVING clause](../../../sql-reference/statements/select/having.md)
- [QUALIFY clause](../../../sql-reference/statements/select/qualify.md)
- [LIMIT clause](../../../sql-reference/statements/select/limit.md)
- [OFFSET clause](../../../sql-reference/statements/select/offset.md)
- [UNION clause](../../../sql-reference/statements/select/union.md)
- [INTERSECT clause](../../../sql-reference/statements/select/intersect.md)
- [EXCEPT clause](../../../sql-reference/statements/select/except.md)
- [INTO OUTFILE clause](../../../sql-reference/statements/select/into-outfile.md)
- [FORMAT clause](../../../sql-reference/statements/select/format.md)

## SELECT Clause {#select-clause}

[Expressions](/sql-reference/syntax#expressions) specified in the `SELECT` clause are calculated after all the operations in the clauses described above are finished. These expressions work as if they apply to separate rows in the result. If expressions in the `SELECT` clause contain aggregate functions, then ClickHouse processes aggregate functions and expressions used as their arguments during the [GROUP BY](/sql-reference/statements/select/group-by) aggregation.

If you want to include all columns in the result, use the asterisk (`*`) symbol. For example, `SELECT * FROM ...`.

### Dynamic column selection {#dynamic-column-selection}

Dynamic column selection (also known as a COLUMNS expression) allows you to match some columns in a result with a [re2](https://en.wikipedia.org/wiki/RE2_(software)) regular expression.

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

Each column returned by the `COLUMNS` expression is passed to the function as a separate argument. Also you can pass other arguments to the function if it supports them. Be careful when using functions. If a function does not support the number of arguments you have passed to it, ClickHouse throws an exception.

For example:

```sql
SELECT COLUMNS('a') + COLUMNS('c') FROM col_names
```

```text
Received exception from server (version 19.14.1):
Code: 42. DB::Exception: Received from localhost:9000. DB::Exception: Number of arguments for function plus does not match: passed 3, should be 2.
```

In this example, `COLUMNS('a')` returns two columns: `aa` and `ab`. `COLUMNS('c')` returns the `bc` column. The `+` operator can't apply to 3 arguments, so ClickHouse throws an exception with the relevant message.

Columns that matched the `COLUMNS` expression can have different data types. If `COLUMNS` does not match any columns and is the only expression in `SELECT`, ClickHouse throws an exception.

#### Select columns with `LIKE` or `ILIKE` {#select-columns-with-like-or-ilike}

You can also select columns by matching their names against a pattern after `*`, using a case-sensitive `LIKE` or a case-insensitive `ILIKE`:

```sql
SELECT * ILIKE 'a%' FROM col_names
```

```text
┌─aa─┬─ab─┐
│  1 │  1 │
└────┴────┘
```

The `LIKE` and `ILIKE` patterns follow `LIKE` semantics, not regular expression semantics. The `%` character matches any sequence of characters, the `_` character matches any single character, and `\` escapes `%`, `_`, and `\`. The only difference between the two is that `LIKE` matches column names case-sensitively, while `ILIKE` is case-insensitive. For example:

```sql
SELECT * ILIKE 'a_' FROM col_names
```

The query selects columns with two-character names that start with `a`, such as `aa` and `ab`.

`* LIKE` and `* ILIKE` also support qualified asterisks and column transformers:

```sql
SELECT t.* ILIKE 'a%' EXCEPT (ab) FROM col_names AS t
```

```text
┌─aa─┐
│  1 │
└────┘
```

### Asterisk {#asterisk}

You can put an asterisk in any part of a query instead of an expression. When the query is analyzed, the asterisk is expanded to a list of all table columns (excluding the `MATERIALIZED` and `ALIAS` columns). There are only a few cases when using an asterisk is justified:

- When creating a table dump.
- For tables containing just a few columns, such as system tables.
- For getting information about what columns are in a table. In this case, set `LIMIT 1`. But it is better to use the `DESC TABLE` query.
- When there is strong filtration on a small number of columns using `PREWHERE`.
- In subqueries (since columns that aren't needed for the external query are excluded from subqueries).

In all other cases, we do not recommend using the asterisk, since it only gives you the drawbacks of a columnar DBMS instead of the advantages. In other words using the asterisk is not recommended.

### Extreme Values {#extreme-values}

In addition to results, you can also get minimum and maximum values for the results columns. To do this, set the **extremes** setting to 1. Minimums and maximums are calculated for numeric types, dates, and dates with times. For other columns, the default values are output.

An extra two rows are calculated – the minimums and maximums, respectively. These extra two rows are output in `XML`, `JSON*`, `TabSeparated*`, `CSV*`, `Vertical`, `Template` and `Pretty*` [formats](../../../interfaces/formats.md), separate from the other rows. They are not output for other formats.

In `JSON*` and `XML` formats, the extreme values are output in a separate 'extremes' field. In `TabSeparated*`, `CSV*` and `Vertical` formats, the row comes after the main result, and after 'totals' if present. It is preceded by an empty row (after the other data). In `Pretty*` formats, the row is output as a separate table after the main result, and after `totals` if present. In `Template` format the extreme values are output according to specified template.

Extreme values are calculated for rows before `LIMIT`, but after `LIMIT BY`. However, when using `LIMIT offset, size`, the rows before `offset` are included in `extremes`. In stream requests, the result may also include a small number of rows that passed through `LIMIT`.

### Notes {#notes}

You can use synonyms (`AS` aliases) in any part of a query.

The `GROUP BY`, `ORDER BY`, and `LIMIT BY` clauses can support positional arguments. To enable this, switch on the [enable_positional_arguments](/operations/settings/settings#enable_positional_arguments) setting. Then, for example, `ORDER BY 1,2` will be sorting rows in the table on the first and then the second column.

## Implementation Details {#implementation-details}

If the query omits the `DISTINCT`, `GROUP BY` and `ORDER BY` clauses and the `IN` and `JOIN` subqueries, the query will be completely stream processed, using O(1) amount of RAM. Otherwise, the query might consume a lot of RAM if the appropriate restrictions are not specified:

- `max_memory_usage`
- `max_rows_to_group_by`
- `max_rows_to_sort`
- `max_rows_in_distinct`
- `max_bytes_in_distinct`
- `max_rows_in_set`
- `max_bytes_in_set`
- `max_rows_in_join`
- `max_bytes_in_join`
- `max_bytes_before_external_sort`
- `max_bytes_ratio_before_external_sort`
- `max_bytes_before_external_group_by`
- `max_bytes_ratio_before_external_group_by`

For more information, see the section "Settings". It is possible to use external sorting (saving temporary tables to a disk) and external aggregation.

## SELECT modifiers {#select-modifiers}

You can use the following modifiers in `SELECT` queries.

| Modifier                            | Description                                                                                                                                                                                                                                                                                                                                                                              |
|-------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [`APPLY`](./apply_modifier.md)     | Allows you to invoke some function for each row returned by an outer table expression of a query.                                                                                                                                                                                                                                                                                        |
| [`EXCEPT`](./except_modifier.md)   | Specifies the names of one or more columns to exclude from the result. All matching column names are omitted from the output.                                                                                                                                                                                                                                                            |
| [`REPLACE`](./replace_modifier.md) | Specifies one or more [expression aliases](/sql-reference/syntax#expression-aliases). Each alias must match a column name from the `SELECT *` statement. In the output column list, the column that matches the alias is replaced by the expression in that `REPLACE`. This modifier does not change the names or order of columns. However, it can change the value and the value type. |

### Modifier Combinations {#modifier-combinations}

You can use each modifier separately or combine them.

**Examples:**

Using the same modifier multiple times.

```sql
SELECT COLUMNS('[jk]') APPLY(toString) APPLY(length) APPLY(max) FROM columns_transformers;
```

```response
┌─max(length(toString(j)))─┬─max(length(toString(k)))─┐
│                        2 │                        3 │
└──────────────────────────┴──────────────────────────┘
```

Using multiple modifiers in a single query.

```sql
SELECT * REPLACE(i + 1 AS i) EXCEPT (j) APPLY(sum) from columns_transformers;
```

```response
┌─sum(plus(i, 1))─┬─sum(k)─┐
│             222 │    347 │
└─────────────────┴────────┘
```

## SETTINGS in SELECT Query {#settings-in-select-query}

You can specify the necessary settings right in the `SELECT` query. The setting value is applied only to this query and is reset to default or previous value after the query is executed.

Other ways to make settings see [here](/operations/settings/overview).

For boolean settings set to true, you can use a shorthand syntax by omitting the value assignment. When only the setting name is specified, it is automatically set to `1` (true).

**Example**

```sql
SELECT * FROM some_table SETTINGS optimize_read_in_order=1, cast_keep_nullable=1;
```
