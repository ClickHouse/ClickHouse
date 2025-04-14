---
slug: /ja/sql-reference/data-types/special-data-types/interval
sidebar_position: 61
sidebar_label: Interval
---

# Interval

時間および日付の間隔を表すデータ型のファミリーです。[INTERVAL](../../../sql-reference/operators/index.md#operator-interval) 演算子の結果として得られる型です。

構造:

- 符号なし整数値としての時間間隔。
- 間隔の種類。

サポートされている間隔の種類:

- `NANOSECOND`
- `MICROSECOND`
- `MILLISECOND`
- `SECOND`
- `MINUTE`
- `HOUR`
- `DAY`
- `WEEK`
- `MONTH`
- `QUARTER`
- `YEAR`

各間隔の種類に対して、個別のデータ型が存在します。例えば、`DAY` 間隔は `IntervalDay` データ型に対応しています。

``` sql
SELECT toTypeName(INTERVAL 4 DAY)
```

``` text
┌─toTypeName(toIntervalDay(4))─┐
│ IntervalDay                  │
└──────────────────────────────┘
```

## 使用上の注意

`Interval` 型の値は、[Date](../../../sql-reference/data-types/date.md) 型および [DateTime](../../../sql-reference/data-types/datetime.md) 型の値との算術演算で使用できます。例えば、現在の時刻に4日を加算することができます。

``` sql
SELECT now() as current_date_time, current_date_time + INTERVAL 4 DAY
```

``` text
┌───current_date_time─┬─plus(now(), toIntervalDay(4))─┐
│ 2019-10-23 10:58:45 │           2019-10-27 10:58:45 │
└─────────────────────┴───────────────────────────────┘
```

また、複数の間隔を同時に使用することも可能です。

``` sql
SELECT now() AS current_date_time, current_date_time + (INTERVAL 4 DAY + INTERVAL 3 HOUR)
```

``` text
┌───current_date_time─┬─plus(current_date_time, plus(toIntervalDay(4), toIntervalHour(3)))─┐
│ 2024-08-08 18:31:39 │                                                2024-08-12 21:31:39 │
└─────────────────────┴────────────────────────────────────────────────────────────────────┘
```

さらに、異なる間隔との比較も可能です。

``` sql
SELECT toIntervalMicrosecond(3600000000) = toIntervalHour(1);
```

``` text
┌─less(toIntervalMicrosecond(179999999), toIntervalMinute(3))─┐
│                                                           1 │
└─────────────────────────────────────────────────────────────┘
```

## 参照

- [INTERVAL](../../../sql-reference/operators/index.md#operator-interval) 演算子
- [toInterval](../../../sql-reference/functions/type-conversion-functions.md#function-tointerval) 型変換関数

