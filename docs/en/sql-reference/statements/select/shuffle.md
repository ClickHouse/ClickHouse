---
description: 'Documentation for SHUFFLE Clause'
sidebar_label: 'SHUFFLE'
slug: /sql-reference/statements/select/shuffle
title: 'SHUFFLE Clause'
doc_type: 'reference'
---

# SHUFFLE Clause

The `SHUFFLE` clause randomizes the order of rows in a `SELECT` query.

`SHUFFLE` is intended as an explicit, user-facing SQL clause for randomizing rows. The main use case is random row sampling:

- small exact samples, such as a few thousand rows
- large exact samples, such as millions of rows from a very large table
- other explicit randomization tasks such as cohort generation or demo/test data generation

`SHUFFLE` randomizes the rows of the query result, not the physical read order inside the storage engine. For example, a `MergeTree` table still uses its normal scan strategy; `SHUFFLE` is applied to the rows flowing through the query pipeline before they are returned to the user.

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

## Behavior {#behavior}

- `SHUFFLE` randomizes the final result rows produced by the `SELECT`.
- `SHUFFLE LIMIT n` returns exactly `n` uniformly random rows from the query result, or all rows if the input has fewer than `n` rows.
- `SHUFFLE LIMIT n` is intended for exact random-row sampling with bounded sample memory.

For large exact samples, `SHUFFLE LIMIT n` can be more suitable than `ORDER BY rand() LIMIT n`, because it does not need to sort the entire result set. However, `ORDER BY rand() LIMIT n` can still be competitive for very small `n`, especially when other optimizations such as lazy materialization apply.

If you need approximate percentage-based sampling instead of an exact row count, use storage-aware sampling such as [`SAMPLE`](./sample.md) when available, or Bernoulli-style predicates such as `WHERE randCanonical() < p`.

## Comparison With Test-Only Random Order Injection {#comparison-with-test-only-random-order-injection}

`SHUFFLE` is different from the test-only setting `inject_random_order_for_select_without_order_by`.

- `SHUFFLE` is explicit SQL syntax written intentionally by the user.
- `inject_random_order_for_select_without_order_by` is an internal test setting that silently injects `ORDER BY rand()` into top-level `SELECT` queries without `ORDER BY` to expose tests that accidentally depend on implicit row order.
- `SHUFFLE` is intended for user-facing query semantics and exact random-row sampling.
- The test-only setting is intended for test flakiness detection, not as a user-facing sampling feature.

## Examples {#examples}

Create a table and insert a few rows:

```sql
CREATE TABLE shuffle_example
(
    id UInt32,
    label String
)
ENGINE = Memory;

INSERT INTO shuffle_example VALUES
    (1, 'alpha'),
    (2, 'beta'),
    (3, 'gamma'),
    (4, 'delta'),
    (5, 'epsilon');
```

Randomize all rows:

```sql
SELECT *
FROM shuffle_example
SHUFFLE
SETTINGS allow_experimental_shuffle_query = 1, enable_analyzer = 1;
```

Example result:

```text
┌─id─┬─label───┐
│  4 │ delta   │
│  1 │ alpha   │
│  5 │ epsilon │
│  2 │ beta    │
│  3 │ gamma   │
└────┴─────────┘
```

Randomize rows and return only `n` rows:

```sql
SELECT *
FROM shuffle_example
SHUFFLE
LIMIT 2
SETTINGS allow_experimental_shuffle_query = 1, enable_analyzer = 1;
```

Example result:

```text
┌─id─┬─label───┐
│  2 │ beta    │
│  5 │ epsilon │
└────┴─────────┘
```

This form can be more efficient than `ORDER BY rand() LIMIT 2` for large exact samples, because `SHUFFLE LIMIT` keeps only the current sample in memory and does not require a full sorting of all input rows.

## Notes {#notes}

- `SHUFFLE LIMIT n` is intended for exact random sampling of rows.
- Set `allow_experimental_shuffle_query = 1` and `enable_analyzer = 1` to enable the clause.
- `FROM table AS SHUFFLE` keeps the usual alias semantics because `AS` makes the alias explicit.
- If you need deterministic ordering, use [`ORDER BY`](./order-by.md) instead.
