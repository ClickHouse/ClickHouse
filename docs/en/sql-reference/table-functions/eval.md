---
description: 'The `eval` table function evaluates a constant expression to a query
  string and executes the resulting `SELECT` query.'
sidebar_label: 'eval'
sidebar_position: 49
slug: /sql-reference/table-functions/eval
title: 'eval'
doc_type: 'reference'
---

# eval Table Function {#eval-table-function}

The `eval` table function evaluates its argument to a query string, then executes that string as a single `SELECT` query.

This table function is experimental and disabled by default. Enable it with the [`allow_experimental_eval_table_function`](/operations/settings/settings#allow_experimental_eval_table_function) setting:

```sql
SET allow_experimental_eval_table_function = 1;
```

It also requires the analyzer ([`enable_analyzer`](/operations/settings/settings#enable_analyzer), enabled by default).

## Syntax {#syntax}

```sql
eval(expression)
eval(SELECT ...)
```

## Arguments {#arguments}

- `expression` — A constant expression that returns a query string. Query parameters and scalar subqueries are allowed.
- `SELECT ...` — A `SELECT` query that returns a query string. This form is syntactic sugar for a scalar subquery: `eval(SELECT ...)` is equivalent to `eval((SELECT ...))`, so the query must return exactly one row and one column.

The value of the expression must have one of these types:

- `String`
- `Nullable(String)`
- `LowCardinality(String)`
- `LowCardinality(Nullable(String))`

If the value is `NULL`, `eval` throws an exception.

## Returned Value {#returned-value}

Returns the result of the generated `SELECT` query as a table.

The output schema is determined when the `eval` table function is analyzed, so outer queries can refer to the generated query's real column names and types.

## Examples {#examples}

Evaluate a constant expression:

```sql
SELECT * FROM eval('SEL' || 'ECT 1 AS x');
```

Result:

```text
┌─x─┐
│ 1 │
└───┘
```

Use a query parameter:

```sql
SET param_q = 'SELECT 2 AS y';
SELECT * FROM eval({q:String});
```

Result:

```text
┌─y─┐
│ 2 │
└───┘
```

Evaluate an input `SELECT` query that returns query text:

```sql
SELECT * FROM eval(SELECT 'SELECT 3 AS z');
```

Result:

```text
┌─z─┐
│ 3 │
└───┘
```

Use the generated schema in the outer query:

```sql
SELECT x + 1 FROM eval('SELECT 4 AS x');
```

Result:

```text
┌─plus(x, 1)─┐
│          5 │
└────────────┘
```

## Restrictions {#restrictions}

- `eval` works only with query analyzer (`enable_analyzer = 1`).
- The generated query must be a single `SELECT` query without output options such as `INTO OUTFILE` or `FORMAT`. `eval` does not execute multiple statements.
- The generated query must be self-contained: it is resolved in its own scope and cannot reference `WITH` aliases or columns of the outer query.
- The generated query cannot use the `eval` table function again.
- `eval` cannot be used as an argument of another table function, such as `remote('host', eval(...))`. To execute the generated query on a remote server, wrap it into `view`: `remote('host', view(SELECT * FROM eval(...)))`.
- The argument is evaluated once while the query is analyzed, not once per row or block.
- The outer query log records the original query that contains `eval`; the generated `SELECT` query is not logged as a separate user query.

## Related {#related}

- [`view` table function](/sql-reference/table-functions/view)
- [`allow_experimental_eval_table_function` setting](/operations/settings/settings#allow_experimental_eval_table_function)
