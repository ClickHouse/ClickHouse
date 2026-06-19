---
description: 'Documentation for the experimental pipe (`|>`) SQL syntax in ClickHouse'
sidebar_label: 'Pipe syntax'
sidebar_position: 50
slug: /sql-reference/statements/select/pipe-syntax
title: 'Pipe syntax'
doc_type: 'reference'
keywords: ['pipe syntax', 'pipelined query', 'AGGREGATE']
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

<ExperimentalBadge/>

:::note
This is an experimental feature. To use it, set [`allow_experimental_pipe_syntax`](/operations/settings/settings#allow_experimental_pipe_syntax) to `1`. It is disabled by default.
:::

Pipe syntax lets you build a query top-to-bottom instead of the inside-out flow of a regular `SELECT`. A query starts with a standalone [`FROM`](/sql-reference/statements/select/from) clause and chains operators with the pipe operator `|>`. Each operator transforms the output of the previous stage, so the scope of every clause is limited to the rows produced by the stage before it.

The syntax is introduced by Google [[1]](#references). The parser produces the same AST as the regular `SELECT` parser, so the optimizer and executor are unchanged: a pipe query and its equivalent `SELECT` run identically.

## Syntax {#syntax}

```sql
FROM <table_expression>
[|> WHERE <expr>]
[|> AGGREGATE <agg_expr>, ... [GROUP BY <grouping_expr>, ...]]
[|> [<join_type>] JOIN <table_expression> [ON <condition>]]
[|> ORDER BY <expr>, ...]
[|> LIMIT <n> [OFFSET <m>]]
[|> OFFSET <m>]
```

Operators may be repeated and combined freely. When an operator would conflict with the stage already built (for example a second `WHERE` after an `ORDER BY`, or an operator that must apply to the result of an earlier one), the current stage is wrapped into a subquery automatically and the new operator applies to that subquery's output.

## Supported operators {#supported-operators}

- `|> WHERE <expr>` — filters rows, like the [`WHERE`](/sql-reference/statements/select/where) clause.
- `|> AGGREGATE <agg_expr>, ...` — full-table aggregation (no grouping).
- `|> AGGREGATE <agg_expr>, ... GROUP BY <grouping_expr>, ...` — aggregation with grouping, like [`GROUP BY`](/sql-reference/statements/select/group-by).
- `|> [LEFT|RIGHT|FULL|INNER|CROSS|...] JOIN <table_expression> [ON <condition>]` — joins the current stage with another table expression, like the [`JOIN`](/sql-reference/statements/select/join) clause.
- `|> ORDER BY <expr>, ...` — sorts the output, like [`ORDER BY`](/sql-reference/statements/select/order-by).
- `|> LIMIT <n> [OFFSET <m>]` — truncates the output, like [`LIMIT`](/sql-reference/statements/select/limit).
- `|> OFFSET <m>` — skips the first `m` rows, like [`OFFSET`](/sql-reference/statements/select/offset).

### `AGGREGATE` output columns {#aggregate-output-columns}

The output of `AGGREGATE` is the grouping columns followed by the aggregate expressions. A grouping expression listed in `GROUP BY` is prepended to the projection automatically, unless the projection already produces it (matched by the grouping expression itself, or by an alias referring to it). This keeps the documented example below from emitting a duplicate leading column.

## Examples {#examples}

A regular `SELECT`:

```sql
SELECT *
FROM
(
    SELECT number % 2 AS k, count() AS c
    FROM numbers(6)
    GROUP BY number % 2
)
WHERE k = 1;
```

The pipe equivalent:

```sql
SET allow_experimental_pipe_syntax = 1;

FROM numbers(6)
|> AGGREGATE number % 2 AS k, count() AS c GROUP BY number % 2
|> WHERE k = 1;
```

A translation of TPC-H query 13:

```sql
SET allow_experimental_pipe_syntax = 1;

FROM customer
|> LEFT OUTER JOIN orders ON c_custkey = o_custkey AND o_comment NOT LIKE '%special%requests%'
|> AGGREGATE COUNT(o_orderkey) AS c_count GROUP BY c_custkey
|> AGGREGATE COUNT(*) AS custdist GROUP BY c_count
|> ORDER BY custdist DESC, c_count DESC;
```

is equivalent to:

```sql
SELECT
    c_count,
    COUNT(*) AS custdist
FROM
(
    SELECT
        c_custkey,
        COUNT(o_orderkey) AS c_count
    FROM customer
    LEFT JOIN orders ON (c_custkey = o_custkey) AND (o_comment NOT LIKE '%special%requests%')
    GROUP BY c_custkey
) AS _pipe_subquery_0
GROUP BY c_count
ORDER BY custdist DESC, c_count DESC;
```

## Limitations {#limitations}

As an experimental feature, pipe syntax currently has the following limitations:

- Only the operators listed [above](#supported-operators) are supported. There is no pipe equivalent yet for projection-only operators (such as a standalone `SELECT`/`EXTEND`), `DISTINCT`, `WINDOW`, `HAVING`, `QUALIFY`, `WITH`, `UNION`, set operations, `SAMPLE`, `PREWHERE`, `ARRAY JOIN`, or `WITH ROLLUP`/`CUBE`/`TOTALS` written in pipe form.
- A query must start with a standalone `FROM`; a bare `FROM <table>` without any `|>` operator is only accepted when nothing else follows the table expression (otherwise it is handled by the regular `SELECT` parser, so existing `FROM ... SELECT ...` queries are not shadowed).
- Pipe syntax is only accepted at the top level of a query. A pipe query cannot appear where a regular `SELECT` is expected in a nested position — inside a subquery (for example `SELECT * FROM (FROM t |> WHERE c)`), a CTE (`WITH x AS (FROM t |> WHERE c)`), an `INSERT ... FROM` source, or a `CREATE VIEW ... AS` definition. Such uses are rejected with a syntax error. Wrap the pipe query at the top level instead, or use the equivalent regular `SELECT` in the nested position.
- Listing a grouping key in `AGGREGATE` under an alias that collides with an aggregate (for example `|> AGGREGATE count() AS k GROUP BY k`) is ambiguous in the same way as the equivalent `SELECT count() AS k ... GROUP BY k`, and behaves the same way (the alias resolves to the aggregate). Use distinct names for grouping keys and aggregates to avoid the collision.

## References {#references}

[1] [SQL Has Problems. We Can Fix Them: Pipe Syntax in SQL](https://research.google/pubs/pub1005959/)
