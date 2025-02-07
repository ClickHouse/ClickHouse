---
slug: /ja/sql-reference/data-types/datetime64
sidebar_position: 18
sidebar_label: DateTime64
---

# DateTime64

日付と時刻、および定義されたサブ秒精度で表現できる瞬間を格納することができます。

チックサイズ（精度）：10<sup>-precision</sup> 秒。 有効範囲: [ 0 : 9 ]。
通常は、3（ミリ秒）、6（マイクロ秒）、9（ナノ秒）が使用されます。

**構文:**

``` sql
DateTime64(precision, [timezone])
```

内部的には、1970-01-01 00:00:00 UTCからエポックの開始までの「チック」数としてデータを Int64 として格納します。 チックの解像度は精度パラメータによって決定されます。さらに、`DateTime64` 型はカラム全体に対して同じタイムゾーンを格納することができ、これによって `DateTime64` 型の値がテキスト形式で表示される方法や、文字列として指定された値が解析される方法に影響します（‘2020-01-01 05:00:01.000’）。タイムゾーンはテーブルの行（または結果セット）には格納されませんが、カラムのメタデータに格納されます。詳細は[DateTime](../../sql-reference/data-types/datetime.md)を参照してください。

サポートされている値の範囲: \[1900-01-01 00:00:00, 2299-12-31 23:59:59.99999999\]

注意: 最大値の精度は8です。最大精度の9桁（ナノ秒）が使用された場合、最大でサポートされる値は UTC において `2262-04-11 23:47:16` です。

## 例

1. `DateTime64`型のカラムを持つテーブルを作成し、データを挿入する例:

``` sql
CREATE TABLE dt64
(
    `timestamp` DateTime64(3, 'Asia/Istanbul'),
    `event_id` UInt8
)
ENGINE = TinyLog;
```

``` sql
-- DateTime を解析する
-- - 1970-01-01からの秒数としての整数から。
-- - 文字列から。
INSERT INTO dt64 VALUES (1546300800123, 1), (1546300800.123, 2), ('2019-01-01 00:00:00', 3);

SELECT * FROM dt64;
```

``` text
┌───────────────timestamp─┬─event_id─┐
│ 2019-01-01 03:00:00.123 │        1 │
│ 2019-01-01 03:00:00.123 │        2 │
│ 2019-01-01 00:00:00.000 │        3 │
└─────────────────────────┴──────────┘
```

- datetimeが整数として挿入される場合、それは適切にスケーリングされた Unix タイムスタンプ (UTC) として扱われます。`1546300800000` (精度3) は `'2019-01-01 00:00:00'` UTC を表します。ただし、`timestamp` カラムには `Asia/Istanbul` (UTC+3) タイムゾーンが指定されているため、文字列として出力されると `'2019-01-01 03:00:00'` と表示されます。datetimeを小数として挿入すると、整数と同様に扱われますが、小数点前の値が Unix タイムスタンプの秒までを表し、小数点後の値は精度として扱われます。
- 文字列値がdatetimeとして挿入される場合、それはカラムのタイムゾーンにあるものとして扱われます。`'2019-01-01 00:00:00'` は `Asia/Istanbul` タイムゾーンにあるものとして扱われ、`1546290000000` として格納されます。

2. `DateTime64` 値のフィルタリング

``` sql
SELECT * FROM dt64 WHERE timestamp = toDateTime64('2019-01-01 00:00:00', 3, 'Asia/Istanbul');
```

``` text
┌───────────────timestamp─┬─event_id─┐
│ 2019-01-01 00:00:00.000 │        3 │
└─────────────────────────┴──────────┘
```

`DateTime` とは異なり、`DateTime64` の値は `String` から自動的に変換されません。

``` sql
SELECT * FROM dt64 WHERE timestamp = toDateTime64(1546300800.123, 3);
```

``` text
┌───────────────timestamp─┬─event_id─┐
│ 2019-01-01 03:00:00.123 │        1 │
│ 2019-01-01 03:00:00.123 │        2 │
└─────────────────────────┴──────────┘
```

挿入とは逆に、`toDateTime64` 関数はすべての値を小数バリアントとして処理するため、精度は小数点の後に与える必要があります。

3. `DateTime64`型の値のタイムゾーンを取得する:

``` sql
SELECT toDateTime64(now(), 3, 'Asia/Istanbul') AS column, toTypeName(column) AS x;
```

``` text
┌──────────────────column─┬─x──────────────────────────────┐
│ 2023-06-05 00:09:52.000 │ DateTime64(3, 'Asia/Istanbul') │
└─────────────────────────┴────────────────────────────────┘
```

4. タイムゾーン変換

``` sql
SELECT
toDateTime64(timestamp, 3, 'Europe/London') as lon_time,
toDateTime64(timestamp, 3, 'Asia/Istanbul') as istanbul_time
FROM dt64;
```

``` text
┌────────────────lon_time─┬───────────istanbul_time─┐
│ 2019-01-01 00:00:00.123 │ 2019-01-01 03:00:00.123 │
│ 2019-01-01 00:00:00.123 │ 2019-01-01 03:00:00.123 │
│ 2018-12-31 21:00:00.000 │ 2019-01-01 00:00:00.000 │
└─────────────────────────┴─────────────────────────┘
```

**参照**

- [型変換関数](../../sql-reference/functions/type-conversion-functions.md)
- [日付と時刻を扱う関数](../../sql-reference/functions/date-time-functions.md)
- [`date_time_input_format` 設定](../../operations/settings/settings-formats.md#date_time_input_format)
- [`date_time_output_format` 設定](../../operations/settings/settings-formats.md#date_time_output_format)
- [`timezone` サーバー構成パラメータ](../../operations/server-configuration-parameters/settings.md#timezone)
- [`session_timezone` 設定](../../operations/settings/settings.md#session_timezone)
- [日付と時刻を扱うための演算子](../../sql-reference/operators/index.md#operators-for-working-with-dates-and-times)
- [`Date` データ型](../../sql-reference/data-types/date.md)
- [`DateTime` データ型](../../sql-reference/data-types/datetime.md)
