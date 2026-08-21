---
slug: /sql-reference/table-functions/numbers
sidebar_position: 145
sidebar_label: 'numbers'
title: 'numbers'
description: 'Returns a table with a single `number` column that contains a sequence of integers.'
doc_type: 'reference'
---

# numbers Table Function

- `numbers()` – Returns an infinite table with a single `number` column (UInt64) that contains integers in ascending order, starting from 0. Use `LIMIT` (and optionally `OFFSET`) to restrict the number of rows.

- `numbers(N)` – Returns a table with a single `number` column (UInt64) that contains integers from 0 to `N - 1`.

- `numbers(N, M)` – Returns a table with a single `number` column (UInt64) that contains `M` integers from `N` to `N + M - 1`.

- `numbers(N, M, S)` – Returns a table with a single `number` column (UInt64) that contains values in `[N, N + M)` with step `S` (about `M / S` rows, rounded up). `S` must be `>= 1`.

This is similar to the [`system.numbers`](/operations/system-tables/numbers) system table. It can be used for testing and generating successive values.

The following queries are equivalent:

```sql
SELECT * FROM numbers(10);
SELECT * FROM numbers(0, 10);
SELECT * FROM numbers() LIMIT 10;
SELECT * FROM system.numbers LIMIT 10;
SELECT * FROM system.numbers WHERE number BETWEEN 0 AND 9;
SELECT * FROM system.numbers WHERE number IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
```

The following queries are also equivalent:

```sql
SELECT * FROM numbers(10, 10);
SELECT * FROM numbers() LIMIT 10 OFFSET 10;
SELECT * FROM system.numbers LIMIT 10 OFFSET 10;
```

The following queries are also equivalent:

```sql
SELECT number * 2 FROM numbers(10);
SELECT (number - 10) * 2 FROM numbers(10, 10);
SELECT * FROM numbers(0, 20, 2);
```

### Examples {#examples}

The first 10 numbers.
```sql
SELECT * FROM numbers(10);
```

```response
 ┌─number─┐
 │      0 │
 │      1 │
 │      2 │
 │      3 │
 │      4 │
 │      5 │
 │      6 │
 │      7 │
 │      8 │
 │      9 │
 └────────┘
```

Generate a sequence of dates from 2010-01-01 to 2010-12-31.

```sql
SELECT toDate('2010-01-01') + number AS d FROM numbers(365);
```

Find the first `UInt64` `>= 10^15` whose `sipHash64(number)` has 20 trailing zero bits.

```sql
SELECT number
FROM numbers()
WHERE number >= toUInt64(1e15)
  AND bitAnd(sipHash64(number), 0xFFFFF) = 0
LIMIT 1;
```

```response
 ┌───────────number─┐
 │ 1000000000056095 │ -- 1.00 quadrillion
 └──────────────────┘
```

### Notes {#notes}
- For performance reasons, if you know how many rows you need, prefer bounded forms (`numbers(N)`, `numbers(N, M[, S])`) over unbounded `numbers()` / `system.numbers`.
- For parallel generation, use `numbers_mt(...)` or the [`system.numbers_mt`](/operations/system-tables/numbers_mt) table. Note that results may be returned in any order.
