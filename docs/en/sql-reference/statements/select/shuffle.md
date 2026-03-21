---
description: 'Documentation for SHUFFLE Clause'
sidebar_label: 'SHUFFLE'
slug: /sql-reference/statements/select/shuffle
title: 'SHUFFLE Clause'
doc_type: 'reference'
---

# SHUFFLE Clause

The `SHUFFLE` clause randomizes the order of rows in a `SELECT` query.

It is a query clause, not a table alias. For example, `FROM numbers(10) AS SHUFFLE` assigns the alias `SHUFFLE` to the table expression and does not randomize rows.

## Syntax {#syntax}

```sql
SELECT ...
FROM ...
[WHERE ...]
SHUFFLE
[LIMIT n]
```

`SHUFFLE` appears before [`ORDER BY`](./order-by.md) and [`LIMIT`](./limit.md) in the query syntax.

## Examples {#examples}

Randomize all rows:

```sql
SELECT number
FROM numbers(10)
SHUFFLE;
```

Randomize rows and return only `n` rows:

```sql
SELECT number
FROM numbers(100)
SHUFFLE
LIMIT 5;
```

This form is more efficient than `ORDER BY rand() LIMIT 5`, because it does not require a full sort of all input rows.

## Notes {#notes}

- `SHUFFLE LIMIT n` is intended for fast random sampling of rows.
- `FROM table AS SHUFFLE` keeps the usual alias semantics because `AS` makes the alias explicit.
- If you need deterministic ordering, use [`ORDER BY`](./order-by.md) instead.
