---
description: 'Documentation describing the RENAME modifier which changes output column names after a SELECT star or COLUMNS expression is expanded.'
sidebar_label: 'RENAME'
sidebar_position: 4
slug: /sql-reference/statements/select/rename-modifier
title: 'RENAME modifier'
keywords: ['RENAME', 'modifier']
doc_type: 'reference'
---

> Allows you to change output column names returned by a `SELECT *` or `COLUMNS` expression.

The `RENAME` modifier changes column names only. It does not change column values, column types, or column order.

## Syntax {#syntax}

```sql
SELECT <expr> RENAME <column_name> AS <new_column_name> FROM [db.]table_name
SELECT <expr> RENAME (<column_name> AS <new_column_name> [, ...]) FROM [db.]table_name
SELECT <expr> RENAME (<lambda>) FROM [db.]table_name
```

`RENAME` can be used after other column transformers. It is a terminal transformer: no other `APPLY`, `EXCEPT`, `REPLACE`, or `RENAME` modifier can follow it for the same `SELECT` expression.

In the explicit form, each `<column_name>` must match a column name selected by the preceding `SELECT *` or `COLUMNS` expression. If a listed column is not selected, ClickHouse throws an exception.

In the lambda form, ClickHouse passes each selected column name to the lambda as a `String` value. The lambda must return a `String`, and the result is used as the output column name.

## Explicit column rename {#explicit-column-rename}

```sql
CREATE TABLE columns_transformers
(
    i Int64,
    j Int16,
    k Int64
)
ENGINE = MergeTree
ORDER BY i;

INSERT INTO columns_transformers VALUES (100, 10, 324), (120, 8, 23);

SELECT * RENAME i AS total_i
FROM columns_transformers;
```

```response
┌─total_i─┬──j─┬───k─┐
│     100 │ 10 │ 324 │
│     120 │  8 │  23 │
└─────────┴────┴─────┘
```

## Multiple column rename {#multiple-column-rename}

```sql
SELECT * RENAME (i AS total_i, k AS total_k)
FROM columns_transformers;
```

```response
┌─total_i─┬──j─┬─total_k─┐
│     100 │ 10 │     324 │
│     120 │  8 │      23 │
└─────────┴────┴─────────┘
```

## Lambda rename {#lambda-rename}

```sql
SELECT COLUMNS('[jk]') RENAME (col -> concat(col, '_value'))
FROM columns_transformers;
```

```response
┌─j_value─┬─k_value─┐
│      10 │     324 │
│       8 │      23 │
└─────────┴─────────┘
```

## Using `RENAME` with other modifiers {#using-rename-with-other-modifiers}

`RENAME` is applied after the previous modifiers. For example, `REPLACE` changes the value and type, and `RENAME` changes the output name:

```sql
SELECT * REPLACE(i + 1 AS i) RENAME i AS next_i
FROM columns_transformers;
```

```response
┌─next_i─┬──j─┬───k─┐
│    101 │ 10 │ 324 │
│    121 │  8 │  23 │
└────────┴────┴─────┘
```

When `RENAME` is used after `APPLY`, explicit `RENAME` still matches the original selected column names:

```sql
SELECT * APPLY(sum) RENAME (i AS i_sum, j AS j_sum, k AS k_sum)
FROM columns_transformers;
```

```response
┌─i_sum─┬─j_sum─┬─k_sum─┐
│   220 │    18 │   347 │
└───────┴───────┴───────┘
```
