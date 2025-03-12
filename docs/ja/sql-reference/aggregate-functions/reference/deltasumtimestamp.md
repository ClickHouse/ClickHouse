---
slug: /ja/sql-reference/aggregate-functions/reference/deltasumtimestamp
sidebar_position: 130
title: deltaSumTimestamp
---

連続する行間の差を加算します。差が負の場合は無視されます。

この関数は主に、`toStartOfMinute` バケットのような時間バケットに揃ったタイムスタンプでデータを順序付けて保存する[マテリアライズドビュー](../../../sql-reference/statements/create/view.md#materialized)用です。このようなマテリアライズドビュー内の行は全て同じタイムスタンプを持つため、元の丸められていないタイムスタンプ値を保存せずに正しい順序でマージすることは不可能です。`deltaSumTimestamp` 関数は、見た値の元の `timestamp` を追跡し、そのため関数の値（状態）はパーツのマージ中に正しく計算されます。

順序付けられたコレクション全体のデルタサムを計算するには、単に [deltaSum](../../../sql-reference/aggregate-functions/reference/deltasum.md#agg_functions-deltasum) 関数を使うことができます。

**構文**

``` sql
deltaSumTimestamp(value, timestamp)
```

**引数**

- `value` — 入力値。いくつかの [Integer](../../data-types/int-uint.md) 型または [Float](../../data-types/float.md) 型、または [Date](../../data-types/date.md) もしくは [DateTime](../../data-types/datetime.md) 型でなければなりません。
- `timestamp` — 値を順序付けるためのパラメータ。いくつかの [Integer](../../data-types/int-uint.md) 型または [Float](../../data-types/float.md) 型、または [Date](../../data-types/date.md) もしくは [DateTime](../../data-types/datetime.md) 型でなければなりません。

**返される値**

- `timestamp` パラメータで順序付けられた、連続する値間の累積差。

型: [Integer](../../data-types/int-uint.md) または [Float](../../data-types/float.md) または [Date](../../data-types/date.md) または [DateTime](../../data-types/datetime.md)。

**例**

クエリ:

```sql
SELECT deltaSumTimestamp(value, timestamp)
FROM (SELECT number AS timestamp, [0, 4, 8, 3, 0, 0, 0, 1, 3, 5][number] AS value FROM numbers(1, 10));
```

結果:

``` text
┌─deltaSumTimestamp(value, timestamp)─┐
│                                  13 │
└─────────────────────────────────────┘
```
