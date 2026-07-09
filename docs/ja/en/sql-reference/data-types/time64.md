---
description: '秒未満の精度を持つ時刻を格納する ClickHouse の Time64 データ型に関するドキュメント'
slug: /sql-reference/data-types/time64
sidebar_position: 17
sidebar_label: 'Time64'
title: 'Time64'
doc_type: 'reference'
---

データ型 `Time64` は、小数秒を含む時刻を表します。
カレンダー日付の部分 (日、月、年) は含まれません。
`precision` パラメータは小数点以下の桁数を定義し、それによってティックサイズが決まります。

ティックサイズ (精度) : 10<sup>-precision</sup> 秒。有効範囲: 0..9。一般的には 3 (ミリ秒) 、6 (マイクロ秒) 、9 (ナノ秒) が使われます。

**構文:**

```sql
Time64(precision)
```

内部的には、`Time64` は秒の小数部分を符号付き 64 ビット十進数 (Decimal64) として格納します。
tick の精度は `precision` パラメーターによって決まります。
タイムゾーンはサポートされていません。`Time64` でタイムゾーンを指定するとエラーがスローされます。

`DateTime64` とは異なり、`Time64` は日付の部分を格納しません。
関連項目 [`Time`](../../sql-reference/data-types/time.md)。

テキスト表現の範囲: `precision = 3` の場合は [-999:59:59.000, 999:59:59.999] です。一般に、最小値は `-999:59:59`、最大値は `999:59:59` で、小数点以下は最大 `precision` 桁です (`precision = 9` の場合、最小値は `-999:59:59.999999999`) 。

<div id="implementation-details">
  ## 実装の詳細
</div>

**表現**。
小数秒を `precision` 桁で表す、符号付き `Decimal64` 値です。

**正規化**。
文字列を `Time64` にパースする際、時刻の各部分は正規化されますが、妥当性は検証されません。
たとえば、`25:70:70` は `26:11:10` と解釈されます。

**負の値**。
先頭のマイナス記号はサポートされ、そのまま保持されます。
負の値は通常、`Time64` 値に対する算術演算によって生じます。
`Time64` では、テキスト入力 (例: `'-01:02:03.123'`) と数値入力 (例: `-3723.123`) の両方で負の入力が保持されます。

**飽和**。
時刻部分は、各部分に変換する際やテキストにシリアライズする際に、範囲 [-999:59:59.xxx, 999:59:59.xxx] に制限されます。
格納される数値はこの範囲を超える場合がありますが、各部分の抽出 (時、分、秒) とテキスト表現には、飽和後の値が使用されます。

**タイムゾーン**。
`Time64` はタイムゾーンをサポートしていません。
`Time64` 型または値の作成時にタイムゾーンを指定すると、エラーが発生します。
同様に、`Time64` カラムへのタイムゾーンの適用や変更もサポートされておらず、エラーになります。

<div id="examples">
  ## 例
</div>

1. `Time64`型のカラムを持つテーブルを作成し、データを挿入します:

```sql
CREATE TABLE tab64
(
    `event_id` UInt8,
    `time` Time64(3)
)
ENGINE = TinyLog;
```

```sql
-- Parse Time64
-- - from string,
-- - from a number of seconds since 00:00:00 (fractional part according to precision).
INSERT INTO tab64 VALUES (1, '14:30:25'), (2, 52225.123), (3, '14:30:25');

SELECT * FROM tab64 ORDER BY event_id;
```

```text
   ┌─event_id─┬────────time─┐
1. │        1 │ 14:30:25.000 │
2. │        2 │ 14:30:25.123 │
3. │        3 │ 14:30:25.000 │
   └──────────┴──────────────┘
```

2. `Time64` の値で絞り込む

```sql
SELECT * FROM tab64 WHERE time = toTime64('14:30:25', 3);
```

```text
   ┌─event_id─┬────────time─┐
1. │        1 │ 14:30:25.000 │
2. │        3 │ 14:30:25.000 │
   └──────────┴──────────────┘
```

```sql
SELECT * FROM tab64 WHERE time = toTime64(52225.123, 3);
```

```text
   ┌─event_id─┬────────time─┐
1. │        2 │ 14:30:25.123 │
   └──────────┴──────────────┘
```

注: `toTime64` は数値リテラルを、指定した精度に応じた小数部を含む秒数として解析するため、意図した小数点以下の桁数を明示的に指定してください。

3. 結果の型を確認します:

```sql
SELECT CAST('14:30:25.250' AS Time64(3)) AS column, toTypeName(column) AS type;
```

```text
   ┌────────column─┬─type──────┐
1. │ 14:30:25.250 │ Time64(3) │
   └───────────────┴───────────┘
```

<div id="addition-with-date">
  ## Date との加算
</div>

[Time64](time64.md) の値は [Date](date.md) または [Date32](date32.md) の値に加算でき、`Time64` と同じスケールの [DateTime64](datetime64.md) を生成できます:

```sql
SET use_legacy_to_time = 0;
SELECT toDate('2024-07-15') + toTime64('14:30:25.123456', 6) AS dt, toTypeName(dt);
```

```text
   ┌─────────────────────────dt─┬─toTypeName(dt)─┐
1. │ 2024-07-15 14:30:25.123456 │ DateTime64(6)  │
   └────────────────────────────┴────────────────┘
```

サポートされているすべての組み合わせと、結果のデータ型の詳細については、[日付と時刻の加算](../operators/index.md#date-time-addition)を参照してください。

**関連項目**

* [型変換関数](../../sql-reference/functions/type-conversion-functions.md)
* [日付と時刻を扱う関数](../../sql-reference/functions/date-time-functions.md)
* [`date_time_input_format` 設定](../../operations/settings/settings-formats.md#date_time_input_format)
* [`date_time_output_format` 設定](../../operations/settings/settings-formats.md#date_time_output_format)
* [`timezone` サーバー設定パラメータ](../../operations/server-configuration-parameters/settings.md#timezone)
* [`session_timezone` 設定](../../operations/settings/settings.md#session_timezone)
* [日付と時刻を扱う演算子](../../sql-reference/operators/index.md#operators-for-working-with-dates-and-times)
* [`Date` データ型](../../sql-reference/data-types/date.md)
* [`Time` データ型](../../sql-reference/data-types/time.md)
* [`DateTime` データ型](../../sql-reference/data-types/datetime.md)