---
description: '用于测试，是生成大量行的最快方法。
  类似于 `system.zeros` 和 `system.zeros_mt` 系统表。'
sidebar_label: 'zeros'
sidebar_position: 145
slug: /sql-reference/table-functions/zeros
title: 'zeros'
doc_type: 'reference'
---

* `zeros(N)` – 返回一个表，该表仅包含一个名为 &#39;zero&#39; 的列 (UInt8) ，其中包含 `N` 个整数 0
* `zeros_mt(N)` – 与 `zeros` 相同，但会使用多个线程。

此函数用于测试，是生成大量行的最快方法。类似于 `system.zeros` 和 `system.zeros_mt` 系统表。

以下查询是等价的：

```sql
SELECT * FROM zeros(10);
SELECT * FROM system.zeros LIMIT 10;
SELECT * FROM zeros_mt(10);
SELECT * FROM system.zeros_mt LIMIT 10;
```

```response
┌─zero─┐
│    0 │
│    0 │
│    0 │
│    0 │
│    0 │
│    0 │
│    0 │
│    0 │
│    0 │
│    0 │
└──────┘
```