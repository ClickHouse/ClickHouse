---
description: 'ClickHouse の DateTime64 データ型に関するドキュメント。サブ秒精度のタイムスタンプを格納します'
sidebar_label: 'DateTime64'
sidebar_position: 18
slug: /sql-reference/data-types/datetime64
title: 'DateTime64'
doc_type: 'reference'
---

定義されたサブ秒精度で、暦日と時刻として表現できる時点を格納できます。

ティックサイズ (精度) : 10<sup>-precision</sup> Seconds。有効範囲: [ 0 : 9 ]。
通常は 3 (ミリ秒) 、6 (マイクロ秒) 、9 (ナノ秒) が使用されます。

デフォルト値: 3 (ミリ秒) 。

**構文:**

```sql
DateTime64(precision, [timezone])
```

内部的には、データはエポック開始時点 (1970-01-01 00:00:00 UTC) からの&#39;ticks&#39;数として `Int64` で格納されます。ティックの分解能は精度パラメータによって決まります。さらに、`DateTime64` 型は、カラム全体で共通のタイムゾーンを保持できます。これは、`DateTime64` 型の値がテキストフォーマットでどのように表示されるか、および文字列として指定された値 (&#39;2020-01-01 05:00:01.000&#39;) がどのようにパースされるかに影響します。タイムゾーンはテーブルの行 (または結果セット) には格納されず、カラムのメタデータに格納されます。詳細は [DateTime](../../sql-reference/data-types/datetime.md) を参照してください。

サポートされる値の範囲: [1900-01-01 00:00:00, 2299-12-31 23:59:59.999999999]

小数点以下の桁数は精度パラメータによって異なります。

注: 最大値の精度は 8 です。最大精度である 9 桁 (ナノ秒) を使用する場合、サポートされる最大値は UTC で `2262-04-11 23:47:16` です。

<div id="examples">
  ## 例
</div>

1. `DateTime64` 型のカラムを持つテーブルを作成し、そこにデータを挿入します:

```sql
CREATE TABLE dt64
(
    `timestamp` DateTime64(3, 'Asia/Istanbul'),
    `event_id` UInt8
)
ENGINE = MergeTree;
```

```sql
-- Parse DateTime
-- - from an integer interpreted as the number of milliseconds (because of precision 3) since 1970-01-01,
-- - from a decimal interpreted as the number of seconds before the decimal part, and based on the precision after the decimal point,
-- - from a string.

INSERT INTO dt64
VALUES
(1546300800123, 1),
(1546300800.123, 2),
('2019-01-01 00:00:00', 3);

SELECT * FROM dt64;
```

```text
┌───────────────timestamp─┬─event_id─┐
│ 2019-01-01 03:00:00.123 │        1 │
│ 2019-01-01 03:00:00.123 │        2 │
│ 2019-01-01 00:00:00.000 │        3 │
└─────────────────────────┴──────────┘
```

* datetime を整数として挿入する場合、適切なスケールの Unixタイムスタンプ (UTC) として扱われます。`1546300800000` は (精度 3 の場合) UTC の `'2019-01-01 00:00:00'` を表します。ただし、`timestamp` カラムには `Asia/Istanbul` (UTC+3) のタイムゾーンが指定されているため、文字列として出力すると、この値は `'2019-01-01 03:00:00'` と表示されます。datetime を小数として挿入する場合も整数の場合と同様に扱われますが、小数点より前の値は秒までを含む Unixタイムスタンプとして、小数点より後の値は精度として扱われます。
* 文字列値を datetime として挿入する場合は、カラムのタイムゾーンの時刻として扱われます。`'2019-01-01 00:00:00'` は `Asia/Istanbul` タイムゾーンの時刻として解釈され、`1546290000000` として保存されます。

2. `DateTime64` 値のフィルタリング

```sql
SELECT * FROM dt64 WHERE timestamp = toDateTime64('2019-01-01 00:00:00', 3, 'Asia/Istanbul');
```

```text
┌───────────────timestamp─┬─event_id─┐
│ 2019-01-01 00:00:00.000 │        3 │
└─────────────────────────┴──────────┘
```

`DateTime` とは異なり、`DateTime64` の値は `String` から自動変換されません。

```sql
SELECT * FROM dt64 WHERE timestamp = toDateTime64(1546300800.123, 3);
```

```text
┌───────────────timestamp─┬─event_id─┐
│ 2019-01-01 03:00:00.123 │        1 │
│ 2019-01-01 03:00:00.123 │        2 │
└─────────────────────────┴──────────┘
```

データの挿入時とは異なり、`toDateTime64` 関数はすべての値を小数として扱うため、精度は
小数点以下の桁数として指定する必要があります。

3. `DateTime64` 型の値のタイムゾーンを取得する:

```sql
SELECT toDateTime64(now(), 3, 'Asia/Istanbul') AS column, toTypeName(column) AS x;
```

```text
┌──────────────────column─┬─x──────────────────────────────┐
│ 2023-06-05 00:09:52.000 │ DateTime64(3, 'Asia/Istanbul') │
└─────────────────────────┴────────────────────────────────┘
```

4. タイムゾーン変換

```sql
SELECT
toDateTime64(timestamp, 3, 'Europe/London') AS lon_time,
toDateTime64(timestamp, 3, 'Asia/Istanbul') AS istanbul_time
FROM dt64;
```

```text
┌────────────────lon_time─┬───────────istanbul_time─┐
│ 2019-01-01 00:00:00.123 │ 2019-01-01 03:00:00.123 │
│ 2019-01-01 00:00:00.123 │ 2019-01-01 03:00:00.123 │
│ 2018-12-31 21:00:00.000 │ 2019-01-01 00:00:00.000 │
└─────────────────────────┴─────────────────────────┘
```

**関連項目**

* [型変換関数](../../sql-reference/functions/type-conversion-functions.md)
* [日付と時刻を扱う関数](../../sql-reference/functions/date-time-functions.md)
* [`date_time_input_format` 設定](../../operations/settings/settings-formats.md#date_time_input_format)
* [`date_time_output_format` 設定](../../operations/settings/settings-formats.md#date_time_output_format)
* [`timezone` サーバー設定パラメータ](../../operations/server-configuration-parameters/settings.md#timezone)
* [`session_timezone` 設定](../../operations/settings/settings.md#session_timezone)
* [日付と時刻を扱う演算子](../../sql-reference/operators/index.md#operators-for-working-with-dates-and-times)
* [`Date` データ型](../../sql-reference/data-types/date.md)
* [`DateTime` データ型](../../sql-reference/data-types/datetime.md)