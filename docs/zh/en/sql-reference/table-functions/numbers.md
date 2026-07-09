---
slug: /sql-reference/table-functions/numbers
sidebar_position: 145
sidebar_label: 'numbers'
title: 'numbers'
description: '返回一个仅包含单个 `number` 列的表，其中包含一系列整数。'
doc_type: 'reference'
---

* `numbers()` – 返回一个仅包含单个 `number` 列 (UInt64) 的无限表，其中的整数按升序排列，从 0 开始。使用 `LIMIT` (以及可选的 `OFFSET`) 可限制返回的行数。

* `numbers(N)` – 返回一个仅包含单个 `number` 列 (UInt64) 的表，其中包含从 0 到 `N - 1` 的整数。

* `numbers(N, M)` – 返回一个仅包含单个 `number` 列 (UInt64) 的表，其中包含从 `N` 到 `N + M - 1` 的 `M` 个整数。

* `numbers(N, M, S)` – 返回一个仅包含单个 `number` 列 (UInt64) 的表，其中包含区间 `[N, N + M)` 内按步长 `S` 递增的值 (约为 `M / S` 行，向上取整) 。`S` 必须 `>= 1`。

这与 [`system.numbers`](/zh/operations/system-tables/numbers) 系统表类似。它可用于测试以及生成连续的值。

以下查询是等价的：

```sql
SELECT * FROM numbers(10);
SELECT * FROM numbers(0, 10);
SELECT * FROM numbers() LIMIT 10;
SELECT * FROM system.numbers LIMIT 10;
SELECT * FROM system.numbers WHERE number BETWEEN 0 AND 9;
SELECT * FROM system.numbers WHERE number IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
```

以下查询也等价：

```sql
SELECT * FROM numbers(10, 10);
SELECT * FROM numbers() LIMIT 10 OFFSET 10;
SELECT * FROM system.numbers LIMIT 10 OFFSET 10;
```

以下查询也同样等价：

```sql
SELECT number * 2 FROM numbers(10);
SELECT (number - 10) * 2 FROM numbers(10, 10);
SELECT * FROM numbers(0, 20, 2);
```

<div id="examples">
  ### 示例
</div>

前 10 个数。

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

生成 2010-01-01 到 2010-12-31 的日期序列。

```sql
SELECT toDate('2010-01-01') + number AS d FROM numbers(365);
```

找出第一个 `>= 10^15` 的 `UInt64`，其 `sipHash64(number)` 的末尾有 20 个零位。

```sql
SELECT number
FROM numbers()
WHERE number >= 1e15
  AND bitAnd(sipHash64(number), 0xFFFFF) = 0
LIMIT 1;
```

```response
 ┌───────────number─┐
 │ 1000000000056095 │ -- 1.00 quadrillion
 └──────────────────┘
```

<div id="notes">
  ### 注意事项
</div>

* 出于性能考虑，如果你知道需要多少行，建议优先使用有界形式 (`numbers(N)`、`numbers(N, M[, S])`) ，而不是无界的 `numbers()` / `system.numbers`。
* 如需并行生成，请使用 `numbers_mt(...)` 或 [`system.numbers_mt`](/zh/operations/system-tables/numbers_mt) 表。请注意，结果可能以任意顺序返回。