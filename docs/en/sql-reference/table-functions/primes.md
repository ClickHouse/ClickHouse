---
slug: /sql-reference/table-functions/primes
sidebar_position: 145
sidebar_label: 'primes'
title: 'primes'
description: 'Returns a table with a single `prime` column that contains prime numbers.'
doc_type: 'reference'
---

# primes Table Function

- `primes()` – Returns an infinite table with a single `prime` column (UInt64) that contains prime numbers in ascending order, starting from 2. Use `LIMIT` (and optionally `OFFSET`) to restrict the number of rows.

- `primes(N)` – Returns a table with the single `prime` column (UInt64) that contains the first `N` prime numbers, starting from 2.

- `primes(N, M)` – Returns a table with the single `prime` column (UInt64) that contains `M` prime numbers starting at the `N`-th prime (0-based).

- `primes(N, M, S)` – Returns a table with the single `prime` column (UInt64) that contains `M` prime numbers starting from the `N`-th prime (0-based) with step `S` by prime index. The returned primes correspond to indices `N, N + S, N + 2S, ..., N + (M - 1)S`. `S` must be `>= 1`.

This is similar to the [`system.primes`](/operations/system-tables/primes) system table.

The following queries are equivalent:

```sql
SELECT * FROM primes(10);
SELECT * FROM primes(0, 10);
SELECT * FROM primes() LIMIT 10;
SELECT * FROM system.primes LIMIT 10;
SELECT * FROM system.primes WHERE prime IN (2, 3, 5, 7, 11, 13, 17, 19, 23, 29);
```

The following queries are also equivalent:

```sql
SELECT * FROM primes(10, 10);
SELECT * FROM primes() LIMIT 10 OFFSET 10;
SELECT * FROM system.primes LIMIT 10 OFFSET 10;
```

### Examples {#examples}

The first 10 primes.
```sql
SELECT * FROM primes(10);
```

```response
  ┌─prime─┐
  │     2 │
  │     3 │
  │     5 │
  │     7 │
  │    11 │
  │    13 │
  │    17 │
  │    19 │
  │    23 │
  │    29 │
  └───────┘
```

The first prime greater than 1e15.
```sql
SELECT prime FROM primes() WHERE prime > toUInt64(1e15) LIMIT 1;
```

```response
  ┌────────────prime─┐
  │ 1000000000000037 │ -- 1.00 quadrillion
  └──────────────────┘
```

Solve a modular constraint over primes in a very large range: find the first prime `p >= 10^15` such that `p` modulo `65537` equals `1`.
```sql
SELECT prime
FROM primes()
WHERE prime >= toUInt64(1e15)
  AND prime % 65537 = 1
LIMIT 1;
```

```response
 ┌────────────prime─┐
 │ 1000000001218399 │ -- 1.00 quadrillion
 └──────────────────┘
```

The first 7 Mersenne primes.
```sql
SELECT prime
FROM primes()
WHERE bitAnd(prime, prime + 1) = 0
LIMIT 7;
```

```response
  ┌──prime─┐
  │      3 │
  │      7 │
  │     31 │
  │    127 │
  │   8191 │
  │ 131071 │
  │ 524287 │
  └────────┘
```

### Notes {#notes}
- The fastest forms are the plain range and point-filter queries that use the default step (`1`), for example, `primes(N)` or `primes() LIMIT N`. These forms use an optimized prime generator to compute very large primes efficiently.
- For unbounded sources (`primes()` / `system.primes`), simple value filters such as `prime BETWEEN ...`, `prime IN (...)`, or `prime = ...` can be applied during generation to restrict the searched value ranges. For example, the following query executes almost instantly:
```sql
SELECT sum(prime)
FROM primes()
WHERE prime BETWEEN toUInt64(1e6) AND toUInt64(1e6) + 100
   OR prime BETWEEN toUInt64(1e12) AND toUInt64(1e12) + 100
   OR prime BETWEEN toUInt64(1e15) AND toUInt64(1e15) + 100
   OR prime IN (9999999967, 9999999971, 9999999973)
   OR prime = 1000000000000037;
```

```response
  ┌───────sum(prime)─┐
  │ 2004010006000641 │ -- 2.00 quadrillion
  └──────────────────┘

1 row in set. Elapsed: 0.090 sec. 
```
- This value-range optimization does not apply to bounded table functions (`primes(N)`, `primes(offset, count[, step])`) with `WHERE`, because those variants define a finite table by prime index and the filter must be evaluated after generating that table to preserve semantics.
- Using a non-zero offset and/or step greater than 1 (`primes(offset, count)` / `primes(offset, count, step)`) may be slower because additional primes may need to be generated and skipped internally. If you don't need an offset or step, omit them.
