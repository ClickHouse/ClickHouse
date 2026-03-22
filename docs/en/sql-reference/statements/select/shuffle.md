---
description: 'Documentation for SHUFFLE Clause'
sidebar_label: 'SHUFFLE'
slug: /sql-reference/statements/select/shuffle
title: 'SHUFFLE Clause'
doc_type: 'reference'
---

# SHUFFLE Clause

The `SHUFFLE` clause randomizes the order of rows in a `SELECT` query.

It is a query clause, not a table alias. Both `FROM numbers(10) AS SHUFFLE` and `FROM numbers(10) SHUFFLE` (without a preceding `WHERE`/`GROUP BY`) assign the alias `SHUFFLE` to the table expression and do not randomize rows.

`SHUFFLE` is experimental. It requires both the `allow_experimental_shuffle_query` setting and the query analyzer (`allow_experimental_analyzer = 1`). Using `SHUFFLE` without the analyzer throws a `NOT_IMPLEMENTED` exception.

## Syntax {#syntax}

```sql
SELECT ...
FROM ...
WHERE ...   -- at least one WHERE or GROUP BY clause is required before SHUFFLE;
SHUFFLE     -- without it, the parser treats SHUFFLE as a table alias
[LIMIT n]
-- ORDER BY cannot be combined with SHUFFLE
[SETTINGS allow_experimental_shuffle_query = 1, allow_experimental_analyzer = 1]
```

`SHUFFLE` appears before [`LIMIT`](./limit.md) in the query syntax. It cannot be combined with `ORDER BY`.

:::note
A `WHERE`, `PREWHERE`, `GROUP BY`, or similar clause must appear between `FROM` and `SHUFFLE`. Without it, the parser treats bare `SHUFFLE` as a table alias (e.g. `FROM t SHUFFLE` is equivalent to `FROM t AS SHUFFLE`). Adding `WHERE 1` is the minimal way to force the clause interpretation.
:::

## Examples {#examples}

Randomize all rows:

```sql
SELECT number
FROM numbers(10)
WHERE 1
SHUFFLE
SETTINGS allow_experimental_shuffle_query = 1, allow_experimental_analyzer = 1;
```

Randomize rows and return only `n` rows:

```sql
SELECT number
FROM numbers(100)
WHERE 1
SHUFFLE
LIMIT 5
SETTINGS allow_experimental_shuffle_query = 1, allow_experimental_analyzer = 1;
```

This form is more efficient than `ORDER BY rand() LIMIT 5`, because `SHUFFLE LIMIT` uses bounded-memory reservoir sampling and does not require a full sort of all input rows.

## Notes {#notes}

- `SHUFFLE LIMIT n` is intended for fast random sampling of rows.
- Set `allow_experimental_shuffle_query = 1` and `allow_experimental_analyzer = 1` to enable the clause.
- `FROM table AS SHUFFLE` keeps the usual alias semantics because `AS` makes the alias explicit.
- If you need deterministic ordering, use [`ORDER BY`](./order-by.md) instead.
