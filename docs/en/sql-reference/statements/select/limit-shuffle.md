---
description: 'Documentation for LIMIT SHUFFLE'
sidebar_label: 'LIMIT SHUFFLE'
sidebar_position: 39
slug: /sql-reference/statements/select/limit-shuffle
title: 'LIMIT SHUFFLE'
doc_type: 'reference'
---

# LIMIT SHUFFLE {#limit-shuffle}

The `LIMIT ... SHUFFLE` clause returns a random sample from a `SELECT` query.

It is experimental. To use it, enable both `allow_experimental_shuffle_query` and the query analyzer with `enable_analyzer = 1`.

## Syntax {#syntax}

```sql
SELECT ...
LIMIT n SHUFFLE
```

`LIMIT ... SHUFFLE` cannot be used together with `ORDER BY`, `OFFSET`, or `WITH TIES`.

## Behavior {#behavior}

`LIMIT n SHUFFLE` returns up to `n` random rows from the query result. The returned rows are a random sample, but their output order is not guaranteed to be random.

Internally, ClickHouse plans `LIMIT n SHUFFLE` as `ORDER BY rand() LIMIT n`. Where possible, the query plan may avoid preserving the final random-value order to improve performance.

## Example {#example}

```sql
SELECT *
FROM numbers(100)
LIMIT 5 SHUFFLE
SETTINGS allow_experimental_shuffle_query = 1, enable_analyzer = 1;
```

Example result:

```text
┌─number─┐
│     42 │
│      7 │
│     81 │
│     14 │
│     63 │
└────────┘
```
