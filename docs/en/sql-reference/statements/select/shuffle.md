---
description: 'Documentation for SHUFFLE Clause'
sidebar_label: 'SHUFFLE'
slug: /sql-reference/statements/select/shuffle
title: 'SHUFFLE Clause'
doc_type: 'reference'
---

# SHUFFLE Clause

The `SHUFFLE` clause randomizes the order of rows in a `SELECT` query.

It is a query clause, not a table alias. Use `AS` to keep `SHUFFLE` as an alias, for example `FROM numbers(10) AS SHUFFLE`.

`SHUFFLE` is experimental. It requires both the `allow_experimental_shuffle_query` setting and the query analyzer (`enable_analyzer = 1`). Using `SHUFFLE` without the analyzer throws a `NOT_IMPLEMENTED` exception.

## Syntax {#syntax}

```sql
SELECT ...
FROM ...
SHUFFLE
[LIMIT n]
-- ORDER BY cannot be combined with SHUFFLE
[SETTINGS allow_experimental_shuffle_query = 1, enable_analyzer = 1]
```

`SHUFFLE` appears before [`LIMIT`](./limit.md) in the query syntax. It cannot be combined with `ORDER BY`.

## Examples {#examples}

Randomize all rows:

```sql
SELECT number
FROM numbers(10)
SHUFFLE
SETTINGS allow_experimental_shuffle_query = 1, enable_analyzer = 1;
```

Randomize rows and return only `n` rows:

```sql
SELECT number
FROM numbers(100)
SHUFFLE
LIMIT 5
SETTINGS allow_experimental_shuffle_query = 1, enable_analyzer = 1;
```

This form is more efficient than `ORDER BY rand() LIMIT 5`, because `SHUFFLE LIMIT` uses bounded-memory reservoir sampling and does not require a full sorting of all input rows.

## Notes {#notes}

- `SHUFFLE LIMIT n` is intended for fast random sampling of rows.
- Set `allow_experimental_shuffle_query = 1` and `enable_analyzer = 1` to enable the clause.
- `FROM table AS SHUFFLE` keeps the usual alias semantics because `AS` makes the alias explicit.
- If you need deterministic ordering, use [`ORDER BY`](./order-by.md) instead.
