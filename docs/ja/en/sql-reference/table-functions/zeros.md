---
description: 'テスト用途で多数の行を生成する最速の方法として使用されます。`system.zeros` および `system.zeros_mt` システムテーブルに似ています。'
sidebar_label: 'zeros'
sidebar_position: 145
slug: /sql-reference/table-functions/zeros
title: 'zeros'
doc_type: 'reference'
---

* `zeros(N)` – 整数 0 を `N` 回含む、単一の &#39;zero&#39; カラム (UInt8) を持つテーブルを返します
* `zeros_mt(N)` – `zeros` と同じですが、複数のスレッドを使用します。

この関数は、テスト用途で多数の行を生成する最速の方法として使用されます。`system.zeros` および `system.zeros_mt` システムテーブルに似ています。

以下のクエリは同等です。

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