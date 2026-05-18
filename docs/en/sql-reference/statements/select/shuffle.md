---
description: 'Documentation for SHUFFLE Clause'
sidebar_label: 'SHUFFLE'
slug: /sql-reference/statements/select/shuffle
title: 'SHUFFLE Clause'
doc_type: 'reference'
---

# SHUFFLE Clause {#shuffle-clause}

The `SHUFFLE` clause returns rows from a `SELECT` query in randomized order.

`SHUFFLE` is intended as an explicit, user-facing SQL clause for randomizing result row order. A common use case is returning a random sample with `SHUFFLE LIMIT n`.

`SHUFFLE` randomizes the rows of the query result, not the physical read order inside the storage engine. For example, a `MergeTree` table still uses its normal scan strategy; `SHUFFLE` is applied to the rows flowing through the query pipeline before they are returned to the user.

`SHUFFLE` is a query clause, not a table alias. If you want to use `SHUFFLE` as a table alias, you must write `AS SHUFFLE`, for example `FROM numbers(10) AS SHUFFLE`. By contrast, `FROM numbers(10) SHUFFLE` uses `SHUFFLE` as the clause.

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

- `SHUFFLE` returns the final result rows produced by the `SELECT` in randomized order.
- `SHUFFLE LIMIT n` returns exactly `n` random rows from the query result, or all rows if the input has fewer than `n` rows. The returned sample is random, but its output order is not guaranteed to be random.

`ORDER BY rand() LIMIT n` also returns `n` random rows from the result and additionally orders them by the generated random value. `SHUFFLE LIMIT n` can avoid preserving that final random order and use lazy materialization where possible.

If you need approximate percentage-based sampling instead of an exact row count, use storage-aware sampling such as [`SAMPLE`](./sample.md) when available, or Bernoulli-style predicates such as `WHERE randCanonical() < p`.

## Comparison With Test-Only Random Order Injection {#comparison-with-test-only-random-order-injection}

`SHUFFLE` is different from the test-only setting `inject_random_order_for_select_without_order_by`.

- `SHUFFLE` is explicit SQL syntax written intentionally by the user.
- `inject_random_order_for_select_without_order_by` is an internal test setting that silently injects `ORDER BY rand()` into top-level `SELECT` queries without `ORDER BY` to expose tests that accidentally depend on implicit row order.
- `SHUFFLE` is intended for user-facing query semantics and randomized result row order.
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

This form returns a random sample like `ORDER BY rand() LIMIT 2`, but it does not guarantee random output order.

## Notes {#notes}

- `SHUFFLE LIMIT n` returns `n` random rows from the final query result. The output order of those rows is unspecified.
- Set `allow_experimental_shuffle_query = 1` and `enable_analyzer = 1` to enable the clause.
- If you want to use `SHUFFLE` as an alias, write `AS SHUFFLE`; otherwise `SHUFFLE` is parsed as the clause.
- If you need deterministic ordering, use [`ORDER BY`](./order-by.md) instead.
