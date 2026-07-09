---
slug: /sql-reference/table-functions/primes
sidebar_position: 145
sidebar_label: 'primes'
title: 'primes'
description: '返回一个仅含单个 `prime` 列的表，其中包含质数。'
doc_type: 'reference'
---

* `primes()` – 返回一个仅含单个 `prime` 列 (UInt64) 的无限表，该列按升序包含从 2 开始的质数。使用 `LIMIT` (以及可选的 `OFFSET`) 来限制返回的行数。

* `primes(N)` – 返回一个仅含单个 `prime` 列 (UInt64) 的表，该列包含从 2 开始的前 `N` 个质数。

* `primes(N, M)` – 返回一个仅含单个 `prime` 列 (UInt64) 的表，该列包含从第 `N` 个质数开始的 `M` 个质数 (从 0 开始计数) 。

* `primes(N, M, S)` – 返回一个仅含单个 `prime` 列 (UInt64) 的表，该列包含从第 `N` 个质数开始的 `M` 个质数 (从 0 开始计数) ，并按质数序号以步长 `S` 选取。返回的质数对应的序号为 `N, N + S, N + 2S, ..., N + (M - 1)S`。`S` 必须 `>= 1`。

这与 [`system.primes`](/zh/operations/system-tables/primes) 系统表类似。

以下查询是等价的：

```sql
SELECT * FROM primes(10);
SELECT * FROM primes(0, 10);
SELECT * FROM primes() LIMIT 10;
SELECT * FROM system.primes LIMIT 10;
SELECT * FROM system.primes WHERE prime IN (2, 3, 5, 7, 11, 13, 17, 19, 23, 29);
```

以下查询也等价：

```sql
SELECT * FROM primes(10, 10);
SELECT * FROM primes() LIMIT 10 OFFSET 10;
SELECT * FROM system.primes LIMIT 10 OFFSET 10;
```

<div id="examples">
  ### 示例
</div>

前 10 个质数。

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

大于 1e15 的第一个质数。

```sql
SELECT prime FROM primes() WHERE prime > 1e15 LIMIT 1;
```

```response
  ┌────────────prime─┐
  │ 1000000000000037 │ -- 1.00 quadrillion
  └──────────────────┘
```

在极大的范围内求解一个关于质数的模约束：找到第一个满足 `p >= 10^15` 且 `p` 对 `65537` 取模等于 `1` 的质数 `p`。

```sql
SELECT prime
FROM primes()
WHERE prime >= 1e15
  AND prime % 65537 = 1
LIMIT 1;
```

```response
 ┌────────────prime─┐
 │ 1000000001218399 │ -- 1.00 quadrillion
 └──────────────────┘
```

前 7 个梅森质数。

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

<div id="notes">
  ### 注意事项
</div>

* 最快的形式是使用默认 step (`1`) 的普通范围查询和点过滤查询，例如 `primes(N)` 或 `primes() LIMIT N`。这些形式使用优化过的质数生成器，能够高效地计算非常大的质数。
* 对于无界 source (`primes()` / `system.primes`) ，可以在生成过程中应用简单的值过滤器，例如 `prime BETWEEN ...`、`prime IN (...)` 或 `prime = ...`，以将搜索范围限制在特定的值区间内。例如，以下查询几乎会立即执行：

```sql
SELECT sum(prime)
FROM primes()
WHERE prime BETWEEN 1e6 AND 1e6 + 100
   OR prime BETWEEN 1e12 AND 1e12 + 100
   OR prime BETWEEN 1e15 AND 1e15 + 100
   OR prime IN (9999999967, 9999999971, 9999999973)
   OR prime = 1000000000000037;
```

```response
  ┌───────sum(prime)─┐
  │ 2004010006000641 │ -- 2.00 quadrillion
  └──────────────────┘

1 row in set. Elapsed: 0.090 sec. 
```

* 此值域优化不适用于带有 `WHERE` 的有界表函数 (`primes(N)`、`primes(offset, count[, step])`) ，因为这些变体按质数索引定义了一个有限表。为保留语义，必须在生成该表后再计算过滤条件。
* 使用非零偏移量和/或大于 1 的步长 (`primes(offset, count)` / `primes(offset, count, step)`) 可能会更慢，因为内部可能需要额外生成并跳过更多质数。如果不需要偏移量或步长，请省略它们。