---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 49
toc_title: DateTime64
---

# Datetime64 {#data_type-datetime64}

定義された秒以下の精度で、カレンダーの日付と時刻として表すことができるインスタントを時間内に格納することができます

目盛りのサイズ（精密）:10<sup>-精密</sup> 秒

構文:

``` sql
DateTime64(precision, [timezone])
```

内部的には、データを ‘ticks’ エポック開始（1970-01-01 00:00:00UTC）以来、Int64として。 目盛りの解像度は、精度パラメータによって決定されます。 さらに、 `DateTime64` 型は、列全体で同じタイムゾーンを格納することができます。 `DateTime64` 型の値はテキスト形式で表示され、文字列として指定された値がどのように解析されるか (‘2020-01-01 05:00:01.000’). タイムゾーンは、テーブルの行(またはresultset)には格納されませんが、列メタデータに格納されます。 詳細はを参照。 [DateTime](datetime.md).

サポートされる値の範囲: \[1925-01-01 00:00:00, 2283-11-11 23:59:59.99999999\] (注）最大値の精度は、8).

## 例 {#examples}

**1.** テーブルの作成 `DateTime64`-列を入力し、そこにデータを挿入する:

``` sql
CREATE TABLE dt
(
    `timestamp` DateTime64(3, 'Asia/Istanbul'),
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

-   Datetimeを整数として挿入する場合、適切にスケーリングされたUnixタイムスタンプ(UTC)として扱われます。 `1546300800000` （精度3で）を表します `'2019-01-01 00:00:00'` UTC しかし、 `timestamp` 列は `Asia/Istanbul` (UTC+3)タイムゾーンが指定されている場合、文字列として出力すると、値は次のように表示されます `'2019-01-01 03:00:00'`
-   文字列値をdatetimeとして挿入すると、列タイムゾーンにあるものとして扱われます。 `'2019-01-01 00:00:00'` であるとして扱われます `Asia/Istanbul` タイムゾーンとして保存 `1546290000000`.

**2.** フィルタリング `DateTime64` 値

``` sql
SELECT * FROM dt WHERE timestamp = toDateTime64('2019-01-01 00:00:00', 3, 'Asia/Istanbul')
```

``` text
┌───────────────timestamp─┬─event_id─┐
│ 2019-01-01 00:00:00.000 │        2 │
└─────────────────────────┴──────────┘
```

異なり `DateTime`, `DateTime64` 値は変換されません `String` 自動的に

**3.** Aのタイムゾーンの取得 `DateTime64`-タイプ値:

``` sql
SELECT toDateTime64(now(), 3, 'Asia/Istanbul') AS column, toTypeName(column) AS x
```

``` text
┌──────────────────column─┬─x──────────────────────────────┐
│ 2019-10-16 04:12:04.000 │ DateTime64(3, 'Asia/Istanbul') │
└─────────────────────────┴────────────────────────────────┘
```

**4.** タイムゾーン変換

``` sql
SELECT
toDateTime64(timestamp, 3, 'Europe/London') as lon_time,
toDateTime64(timestamp, 3, 'Asia/Istanbul') as mos_time
FROM dt
```

``` text
┌───────────────lon_time──┬────────────────mos_time─┐
│ 2019-01-01 00:00:00.000 │ 2019-01-01 03:00:00.000 │
│ 2018-12-31 21:00:00.000 │ 2019-01-01 00:00:00.000 │
└─────────────────────────┴─────────────────────────┘
```

## も参照。 {#see-also}

-   [型変換関数](../../sql-reference/functions/type-conversion-functions.md)
-   [日付と時刻を操作するための関数](../../sql-reference/functions/date-time-functions.md)
-   [配列を操作するための関数](../../sql-reference/functions/array-functions.md)
-   [その `date_time_input_format` 設定](../../operations/settings/settings.md#settings-date_time_input_format)
-   [その `timezone` サーバ構成パラメータ](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone)
-   [日付と時刻を操作する演算子](../../sql-reference/operators/index.md#operators-datetime)
-   [`Date` データ型](date.md)
-   [`DateTime` データ型](datetime.md)
