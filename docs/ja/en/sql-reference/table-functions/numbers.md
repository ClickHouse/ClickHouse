---
slug: /sql-reference/table-functions/numbers
sidebar_position: 145
sidebar_label: 'numbers'
title: 'numbers'
description: '整数の数列を含む単一の `number` カラムを持つテーブルを返します。'
doc_type: 'reference'
---

* `numbers()` – 0 から始まる整数を昇順で格納した、単一の `number` カラム (UInt64) を持つ無限テーブルを返します。行数を制限するには `LIMIT` (必要に応じて `OFFSET`) を使用します。

* `numbers(N)` – 0 から `N - 1` までの整数を格納した、単一の `number` カラム (UInt64) を持つテーブルを返します。

* `numbers(N, M)` – `N` から `N + M - 1` までの `M` 個の整数を格納した、単一の `number` カラム (UInt64) を持つテーブルを返します。

* `numbers(N, M, S)` – `[N, N + M)` の範囲の値をステップ `S` で格納した、単一の `number` カラム (UInt64) を持つテーブルを返します (約 `M / S` 行、端数切り上げ) 。`S` は `>= 1` である必要があります。

これは [`system.numbers`](/ja/operations/system-tables/numbers) system table に似ています。テストや連番の生成に使用できます。

次のクエリは等価です。

```sql
SELECT * FROM numbers(10);
SELECT * FROM numbers(0, 10);
SELECT * FROM numbers() LIMIT 10;
SELECT * FROM system.numbers LIMIT 10;
SELECT * FROM system.numbers WHERE number BETWEEN 0 AND 9;
SELECT * FROM system.numbers WHERE number IN (0, 1, 2, 3, 4, 5, 6, 7, 8, 9);
```

次のクエリも同等です：

```sql
SELECT * FROM numbers(10, 10);
SELECT * FROM numbers() LIMIT 10 OFFSET 10;
SELECT * FROM system.numbers LIMIT 10 OFFSET 10;
```

以下のクエリも等価です。

```sql
SELECT number * 2 FROM numbers(10);
SELECT (number - 10) * 2 FROM numbers(10, 10);
SELECT * FROM numbers(0, 20, 2);
```

<div id="examples">
  ### 例
</div>

最初の10個の数。

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

2010-01-01 から 2010-12-31 までの日付の数列を生成します。

```sql
SELECT toDate('2010-01-01') + number AS d FROM numbers(365);
```

`sipHash64(number)` の下位 20 ビットが 0 になる、`>= 10^15` の最初の `UInt64` を見つけてください。

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
  ### 注意事項
</div>

* パフォーマンス上の理由から、必要な行数がわかっている場合は、制限のない `numbers()` / `system.numbers` ではなく、上限のある形式 (`numbers(N)`、`numbers(N, M[, S])`) を優先してください。
* 並列生成には、`numbers_mt(...)` または [`system.numbers_mt`](/ja/operations/system-tables/numbers_mt) テーブルを使用してください。結果は任意の順序で返される可能性がある点に注意してください。