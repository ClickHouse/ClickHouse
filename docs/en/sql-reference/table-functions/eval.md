---
description: 'The `eval` table function evaluates a constant expression or a query to a query string, then executes that string as a single `SELECT` query.'
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

## Syntax {#syntax}

```sql
eval(expression)
eval(SELECT ...)
```

## Arguments {#arguments}

- `expression` — A constant expression that returns a query string.
- `SELECT ...` — A `SELECT` query that returns a query string.

For `expression`, the argument must be a constant expression. Query parameters are allowed if the substituted expression is constant.

For `SELECT ...`, the input query is executed once while `eval` is analyzed. It must return exactly one row and exactly one column. The resulting value is used as the query text for the query that `eval` exposes as a table.

The input value must have one of these types:

- `String`
- `Nullable(String)`
- `LowCardinality(String)`
- `LowCardinality(Nullable(String))`

If the input value is `NULL`, `eval` throws an exception.

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

- The generated query must be a single `SELECT` query. `eval` does not execute multiple statements.
- The generated query cannot contain another `eval` table function.
- The input `SELECT` query is evaluated once during query analysis, not once per row or block.
- In distributed queries, `eval` is usually analyzed on the initiator. If `eval` is used as a nested table function argument of `remote` or `cluster`, the generated `SELECT` query is resolved on the remote shard instead, so it can reference objects available only on that shard.
- The outer query log records the original query that contains `eval`; the generated `SELECT` query is not logged as a separate user query.

## Related {#related}

- [`view` table function](/sql-reference/table-functions/view)
- [`allow_experimental_eval_table_function` setting](/operations/settings/settings#allow_experimental_eval_table_function)
