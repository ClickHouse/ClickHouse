---
slug: /ja/sql-reference/aggregate-functions/reference/intervalLengthSum
sidebar_position: 155
sidebar_label: intervalLengthSum
title: intervalLengthSum
---

すべての範囲（数軸上のセグメント）の合併の総長を計算します。

**構文**

``` sql
intervalLengthSum(start, end)
```

**引数**

- `start` — インターバルの開始値。 [Int32](../../../sql-reference/data-types/int-uint.md#uint8-uint16-uint32-uint64-int8-int16-int32-int64), [Int64](../../../sql-reference/data-types/int-uint.md#uint8-uint16-uint32-uint64-int8-int16-int32-int64), [UInt32](../../../sql-reference/data-types/int-uint.md#uint8-uint16-uint32-uint64-int8-int16-int32-int64), [UInt64](../../../sql-reference/data-types/int-uint.md#uint8-uint16-uint32-uint64-int8-int16-int32-int64), [Float32](../../../sql-reference/data-types/float.md#float32-float64), [Float64](../../../sql-reference/data-types/float.md#float32-float64), [DateTime](../../../sql-reference/data-types/datetime.md#data_type-datetime) または [Date](../../../sql-reference/data-types/date.md#data_type-date)。
- `end` — インターバルの終了値。[Int32](../../../sql-reference/data-types/int-uint.md#uint8-uint16-uint32-uint64-int8-int16-int32-int64), [Int64](../../../sql-reference/data-types/int-uint.md#uint8-uint16-uint32-uint64-int8-int16-int32-int64), [UInt32](../../../sql-reference/data-types/int-uint.md#uint8-uint16-uint32-uint64-int8-int16-int32-int64), [UInt64](../../../sql-reference/data-types/int-uint.md#uint8-uint16-uint32-uint64-int8-int16-int32-int64), [Float32](../../../sql-reference/data-types/float.md#float32-float64), [Float64](../../../sql-reference/data-types/float.md#float32-float64), [DateTime](../../../sql-reference/data-types/datetime.md#data_type-datetime) または [Date](../../../sql-reference/data-types/date.md#data_type-date)。

:::note
引数は同じデータ型である必要があります。そうでない場合、例外がスローされます。
:::

**戻り値**

- すべての範囲（数軸上のセグメント）の合併の総長。引数の型に応じて、戻り値は [UInt64](../../../sql-reference/data-types/int-uint.md#uint8-uint16-uint32-uint64-int8-int16-int32-int64) または [Float64](../../../sql-reference/data-types/float.md#float32-float64) 型になります。

**例**

1. 入力テーブル：

``` text
┌─id─┬─start─┬─end─┐
│ a  │   1.1 │ 2.9 │
│ a  │   2.5 │ 3.2 │
│ a  │     4 │   5 │
└────┴───────┴─────┘
```

この例では、Float32 型の引数が使用されています。関数は Float64 型の値を返します。

結果は、インターバル `[1.1, 3.2]` （`[1.1, 2.9]` と `[2.5, 3.2]` の合併）と `[4, 5]` の長さの合計です。

クエリ：

``` sql
SELECT id, intervalLengthSum(start, end), toTypeName(intervalLengthSum(start, end)) FROM fl_interval GROUP BY id ORDER BY id;
```

結果：

``` text
┌─id─┬─intervalLengthSum(start, end)─┬─toTypeName(intervalLengthSum(start, end))─┐
│ a  │                           3.1 │ Float64                                   │
└────┴───────────────────────────────┴───────────────────────────────────────────┘
```

2. 入力テーブル：

``` text
┌─id─┬───────────────start─┬─────────────────end─┐
│ a  │ 2020-01-01 01:12:30 │ 2020-01-01 02:10:10 │
│ a  │ 2020-01-01 02:05:30 │ 2020-01-01 02:50:31 │
│ a  │ 2020-01-01 03:11:22 │ 2020-01-01 03:23:31 │
└────┴─────────────────────┴─────────────────────┘
```

この例では、DateTime 型の引数が使用されています。関数は秒単位の値を返します。

クエリ：

``` sql
SELECT id, intervalLengthSum(start, end), toTypeName(intervalLengthSum(start, end)) FROM dt_interval GROUP BY id ORDER BY id;
```

結果：

``` text
┌─id─┬─intervalLengthSum(start, end)─┬─toTypeName(intervalLengthSum(start, end))─┐
│ a  │                          6610 │ UInt64                                    │
└────┴───────────────────────────────┴───────────────────────────────────────────┘
```

3. 入力テーブル：

``` text
┌─id─┬──────start─┬────────end─┐
│ a  │ 2020-01-01 │ 2020-01-04 │
│ a  │ 2020-01-12 │ 2020-01-18 │
└────┴────────────┴────────────┘
```

この例では、Date 型の引数が使用されています。関数は日数単位の値を返します。

クエリ：

``` sql
SELECT id, intervalLengthSum(start, end), toTypeName(intervalLengthSum(start, end)) FROM date_interval GROUP BY id ORDER BY id;
```

結果：

``` text
┌─id─┬─intervalLengthSum(start, end)─┬─toTypeName(intervalLengthSum(start, end))─┐
│ a  │                             9 │ UInt64                                    │
└────┴───────────────────────────────┴───────────────────────────────────────────┘
```
