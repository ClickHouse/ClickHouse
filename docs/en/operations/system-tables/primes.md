---
description: 'System table containing a single UInt64 column named `prime` that contains prime numbers in ascending order, starting from 2.'
keywords: ['system table', 'primes']
slug: /operations/system-tables/primes
title: 'system.primes'
doc_type: 'reference'
---

# system.primes

This table contains a single UInt64 column named `prime` that contains prime numbers in ascending order, starting from 2.

You can use this table for tests, or if you need to do a brute force search over prime numbers.

Reads from this table are not parallelized.

This is similar to the [`primes`](/sql-reference/table-functions/primes) table function.

You can also limit the output by predicates.

**Example**

The first 10 primes.
```sql
SELECT * FROM system.primes LIMIT 10;
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
SELECT prime FROM system.primes WHERE prime > toUInt64(1e15) LIMIT 1;
```

```response
  ┌────────────prime─┐
  │ 1000000000000037 │ -- 1.00 quadrillion
  └──────────────────┘
```
