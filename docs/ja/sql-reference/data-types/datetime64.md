---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 49
toc_title: DateTime64
---

# Datetime64 {#data_type-datetime64}

定義されたサブ秒の精度で、カレンダーの日付と時刻として表現することができる時刻にインスタントを格納することができます

目盛りサイズ(精度):10<sup>-精密</sup> 秒

構文:

``` sql
DateTime64(precision, [timezone])
```

内部的には、データを次の数として格納します ‘ticks’ エポックスタート（1970-01-01 00:00:00UTC）はInt64です。 目盛りの分解能は、precisionパラメーターによって決まります。 さらに、 `DateTime64` タイプは、列全体で同じタイムゾーンを格納することができます。 `DateTime64` 型の値は、テキスト形式で表示され、文字列として指定された値がどのように解析されますか (‘2020-01-01 05:00:01.000’). タイムゾーンはテーブルの行(またはresultset)に格納されず、列のメタデータに格納されます。 詳細はこちら [DateTime](datetime.md).

## 例 {#examples}

**1.** テーブルの作成 `DateTime64`-列を入力してデータを挿入する:

``` sql
CREATE TABLE dt
(
    `timestamp` DateTime64(3, 'Europe/Moscow'),
    `event_id` UInt8
)
ENGINE = TinyLog
```

``` sql
INSERT INTO dt Values (1546300800000, 1), ('2019-01-01 00:00:00', 2)
```

``` sql
SELECT * FROM dt
```

``` text
┌───────────────timestamp─┬─event_id─┐
│ 2019-01-01 03:00:00.000 │        1 │
│ 2019-01-01 00:00:00.000 │        2 │
└─────────────────────────┴──────────┘
```

-   Datetimeを整数として挿入すると、適切にスケーリングされたUnixタイムスタンプ(UTC)として扱われます。 `1546300800000` (精度3)を表します `'2019-01-01 00:00:00'` UTC。 しかし、として `timestamp` 列は `Europe/Moscow` （UTC+3）タイムゾーン指定、文字列として出力すると、値は次のように表示されます `'2019-01-01 03:00:00'`
-   文字列値をdatetimeとして挿入すると、列timezoneにあるものとして扱われます。 `'2019-01-01 00:00:00'` れる。 `Europe/Moscow` タイムゾーンとして保存 `1546290000000`.

**2.** フィルタリング `DateTime64` 値

``` sql
SELECT * FROM dt WHERE timestamp = toDateTime64('2019-01-01 00:00:00', 3, 'Europe/Moscow')
```

``` text
┌───────────────timestamp─┬─event_id─┐
│ 2019-01-01 00:00:00.000 │        2 │
└─────────────────────────┴──────────┘
```

とは異なり `DateTime`, `DateTime64` 値は変換されません `String` 自動的に

**3.** Aのタイムゾーンを取得する `DateTime64`-タイプ値:

``` sql
SELECT toDateTime64(now(), 3, 'Europe/Moscow') AS column, toTypeName(column) AS x
```

``` text
┌──────────────────column─┬─x──────────────────────────────┐
│ 2019-10-16 04:12:04.000 │ DateTime64(3, 'Europe/Moscow') │
└─────────────────────────┴────────────────────────────────┘
```

**4.** タイムゾーン変換

``` sql
SELECT
toDateTime64(timestamp, 3, 'Europe/London') as lon_time,
toDateTime64(timestamp, 3, 'Europe/Moscow') as mos_time
FROM dt
```

``` text
┌───────────────lon_time──┬────────────────mos_time─┐
│ 2019-01-01 00:00:00.000 │ 2019-01-01 03:00:00.000 │
│ 2018-12-31 21:00:00.000 │ 2019-01-01 00:00:00.000 │
└─────────────────────────┴─────────────────────────┘
```

## また見なさい {#see-also}

-   [タイプ変換関数](../../sql-reference/functions/type-conversion-functions.md)
-   [日付と時刻を操作するための関数](../../sql-reference/functions/date-time-functions.md)
-   [配列を操作するための関数](../../sql-reference/functions/array-functions.md)
-   [その `date_time_input_format` 設定](../../operations/settings/settings.md#settings-date_time_input_format)
-   [その `timezone` サーバ設定パラメータ](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone)
-   [日付と時刻を操作する演算子](../../sql-reference/operators.md#operators-datetime)
-   [`Date` データ型](date.md)
-   [`DateTime` データ型](datetime.md)
