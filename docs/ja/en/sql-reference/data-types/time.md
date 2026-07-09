---
description: '秒精度の時刻を格納する ClickHouse の Time データ型に関するドキュメント'
slug: /sql-reference/data-types/time
sidebar_position: 15
sidebar_label: 'Time'
title: 'Time'
doc_type: 'reference'
---

データ型 `Time` は、時・分・秒の部分を持つ時刻を表します。
特定の暦日に依存せず、日・月・年の部分を必要としない値に適しています。

構文:

```sql
Time
```

文字列表現の範囲: [-999:59:59, 999:59:59].

分解能: 1秒.

<div id="implementation-details">
  ## 実装の詳細
</div>

**表現とパフォーマンス**。
データ型 `Time` は内部的に、秒を表す符号付き 32 ビット整数として格納されます。
`Time` 型と `DateTime` 型の値は同じバイト数で格納されるため、パフォーマンスも同程度です。

**正規化**。
文字列を `Time` にパースする際、時刻の各部分は正規化されますが、妥当性の検証は行われません。
たとえば、`25:70:70` は `26:11:10` として解釈されます。

**負の値**。
先頭のマイナス記号はサポートされ、そのまま保持されます。
負の値は通常、`Time` 値に対する算術演算によって生じます。
`Time` 型では、テキスト入力 (例: `'-01:02:03'`) と数値入力 (例: `-3723`) の両方で、負の入力が保持されます。

**飽和**。
時刻部分は [-999:59:59, 999:59:59] の範囲に制限されます。
時間が 999 を超える値 (または -999 未満の値) は、テキストでは `999:59:59` (または `-999:59:59`) として表現され、その形式で往復変換されます。

**タイムゾーン**。
`Time` はタイムゾーンをサポートしません。つまり、`Time` 値は地域ごとの文脈を持たずに解釈されます。
型パラメータとして、または値の作成時に `Time` にタイムゾーンを指定すると、エラーになります。
同様に、`Time` カラムにタイムゾーンを適用または変更しようとすることはサポートされておらず、エラーになります。
`Time` 値が異なるタイムゾーンのもとで暗黙的に再解釈されることはありません。

<div id="examples">
  ## 例
</div>

**1.** `Time`型のカラムを持つテーブルを作成し、データを挿入する例:

```sql
CREATE TABLE tab
(
    `event_id` UInt8,
    `time` Time
)
ENGINE = TinyLog;
```

```sql
-- Parse Time
-- - from string,
-- - from integer interpreted as number of seconds since 00:00:00.
INSERT INTO tab VALUES (1, '14:30:25'), (2, 52225);

SELECT * FROM tab ORDER BY event_id;
```

```text
   ┌─event_id─┬──────time─┐
1. │        1 │ 14:30:25 │
2. │        2 │ 14:30:25 │
   └──────────┴───────────┘
```

**2.** `Time` の値での絞り込み

```sql
SET use_legacy_to_time = 0;
SELECT * FROM tab WHERE time = toTime('14:30:25')
```

```text
   ┌─event_id─┬──────time─┐
1. │        1 │ 14:30:25 │
2. │        2 │ 14:30:25 │
   └──────────┴───────────┘
```

`Time` カラムの値は、`WHERE` 述語内で文字列値を使ってフィルタできます。文字列値は自動的に `Time` に変換されます。

```sql
SELECT * FROM tab WHERE time = '14:30:25'
```

```text
   ┌─event_id─┬──────time─┐
1. │        1 │ 14:30:25 │
2. │        2 │ 14:30:25 │
   └──────────┴───────────┘
```

**3.** 結果の型を確認します:

```sql
SELECT CAST('14:30:25' AS Time) AS column, toTypeName(column) AS type
```

```text
   ┌────column─┬─type─┐
1. │ 14:30:25 │ Time │
   └───────────┴──────┘
```

<div id="addition-with-date">
  ## Date への加算
</div>

[Time](time.md) の値は [Date](date.md) または [Date32](date32.md) の値に加算でき、その結果 [DateTime](datetime.md) または [DateTime64](datetime64.md) になります。

```sql
SET use_legacy_to_time = 0;
SELECT toDate('2024-07-15') + toTime('14:30:25') as datetime;
```

```text
   ┌────────────datetime─┐
1. │ 2024-07-15 14:30:25 │
   └─────────────────────┘
```

サポートされているすべての組み合わせと結果型の詳細については、[日付と時刻の加算](../operators/index.md#date-time-addition)を参照してください。

<div id="see-also">
  ## 関連項目
</div>

* [型変換関数](../functions/type-conversion-functions.md)
* [日付と時刻を扱う関数](../functions/date-time-functions.md)
* [配列を扱う関数](../functions/array-functions.md)
* [`date_time_input_format` 設定](../../operations/settings/settings-formats.md#date_time_input_format)
* [`date_time_output_format` 設定](../../operations/settings/settings-formats.md#date_time_output_format)
* [`timezone` サーバー構成パラメータ](../../operations/server-configuration-parameters/settings.md#timezone)
* [`session_timezone` 設定](../../operations/settings/settings.md#session_timezone)
* [`DateTime` データ型](datetime.md)
* [`Date` データ型](date.md)