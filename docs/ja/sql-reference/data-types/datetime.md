---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 48
toc_title: DateTime
---

# Datetime {#data_type-datetime}

カレンダーの日付と時刻として表現することができ、時間内にインスタントを格納することができます。

構文:

``` sql
DateTime([timezone])
```

サポートされる値の範囲: \[1970-01-01 00:00:00, 2105-12-31 23:59:59\].

解像度:1秒。

## 使用上の注意 {#usage-remarks}

のとして保存すると、 [Unix timestamp](https://en.wikipedia.org/wiki/Unix_time) タイムゾーンまたは夏時間に関係なく。 さらに、 `DateTime` タイプは、列全体で同じタイムゾーンを格納することができます。 `DateTime` 型の値は、テキスト形式で表示され、文字列として指定された値がどのように解析されますか (‘2020-01-01 05:00:01’). タイムゾーンはテーブルの行(またはresultset)に格納されず、列のメタデータに格納されます。
リストの対応時間帯の [IANA時間帯のデータベース](https://www.iana.org/time-zones).
その `tzdata` パッケージ、含む [IANA時間帯のデータベース](https://www.iana.org/time-zones)、システムに取付けられているべきです。 を使用 `timedatectl list-timezones` ローカルシステ

タイムゾーンを明示的に設定することができます `DateTime`-テーブルを作成するときに列を入力します。 タイムゾーンが設定されていない場合、ClickHouseはタイムゾーンの値を使用します。 [タイムゾーン](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone) クリックハウスサーバーの起動時にサーバー設定またはオペレーティングシステム設定のパラメータ。

その [クリックハウス-顧客](../../interfaces/cli.md) データ型の初期化時にタイムゾーンが明示的に設定されていない場合は、既定でサーバーのタイムゾーンを適用します。 クライアントのタイムゾーンを使用するには `clickhouse-client` と `--use_client_time_zone` パラメータ。

ClickHouse出力値で `YYYY-MM-DD hh:mm:ss` デフォルトではテキスト形式。 出力を変更するには、次のようにします [formatDateTime](../../sql-reference/functions/date-time-functions.md#formatdatetime) 機能。

ClickHouseにデータを挿入するときは、日付と時刻の文字列の異なる形式を使用することができます。 [date\_time\_input\_format](../../operations/settings/settings.md#settings-date_time_input_format) 設定。

## 例 {#examples}

**1.** テーブルを作成する `DateTime`-列を入力してデータを挿入する:

``` sql
CREATE TABLE dt
(
    `timestamp` DateTime('Europe/Moscow'),
    `event_id` UInt8
)
ENGINE = TinyLog;
```

``` sql
INSERT INTO dt Values (1546300800, 1), ('2019-01-01 00:00:00', 2);
```

``` sql
SELECT * FROM dt;
```

``` text
┌───────────timestamp─┬─event_id─┐
│ 2019-01-01 03:00:00 │        1 │
│ 2019-01-01 00:00:00 │        2 │
└─────────────────────┴──────────┘
```

-   Datetimeを整数として挿入すると、Unixタイムスタンプ（UTC）として扱われます。 `1546300800` を表します `'2019-01-01 00:00:00'` UTC。 しかし、として `timestamp` 列は `Europe/Moscow` （UTC+3）タイムゾーン指定、文字列として出力すると、値は次のように表示されます `'2019-01-01 03:00:00'`
-   文字列値をdatetimeとして挿入すると、列timezoneにあるものとして扱われます。 `'2019-01-01 00:00:00'` するものとして扱われる `Europe/Moscow` タイムゾーンとして保存 `1546290000`.

**2.** フィルタリング `DateTime` 値

``` sql
SELECT * FROM dt WHERE timestamp = toDateTime('2019-01-01 00:00:00', 'Europe/Moscow')
```

``` text
┌───────────timestamp─┬─event_id─┐
│ 2019-01-01 00:00:00 │        2 │
└─────────────────────┴──────────┘
```

`DateTime` 列の値は、文字列値を使用してフィルター処理できます。 `WHERE` 述語。 それはに変換されます `DateTime` 自動的に:

``` sql
SELECT * FROM dt WHERE timestamp = '2019-01-01 00:00:00'
```

``` text
┌───────────timestamp─┬─event_id─┐
│ 2019-01-01 03:00:00 │        1 │
└─────────────────────┴──────────┘
```

**3.** Aのタイムゾーンを取得する `DateTime`-タイプ列:

``` sql
SELECT toDateTime(now(), 'Europe/Moscow') AS column, toTypeName(column) AS x
```

``` text
┌──────────────column─┬─x─────────────────────────┐
│ 2019-10-16 04:12:04 │ DateTime('Europe/Moscow') │
└─────────────────────┴───────────────────────────┘
```

**4.** タイムゾーン変換

``` sql
SELECT
toDateTime(timestamp, 'Europe/London') as lon_time,
toDateTime(timestamp, 'Europe/Moscow') as mos_time
FROM dt
```

``` text
┌───────────lon_time──┬────────────mos_time─┐
│ 2019-01-01 00:00:00 │ 2019-01-01 03:00:00 │
│ 2018-12-31 21:00:00 │ 2019-01-01 00:00:00 │
└─────────────────────┴─────────────────────┘
```

## また見なさい {#see-also}

-   [型変換機能](../../sql-reference/functions/type-conversion-functions.md)
-   [日付と時刻を操作するための関数](../../sql-reference/functions/date-time-functions.md)
-   [配列を操作するための関数](../../sql-reference/functions/array-functions.md)
-   [その `date_time_input_format` 設定](../../operations/settings/settings.md#settings-date_time_input_format)
-   [その `timezone` サーバ設定パラメータ](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone)
-   [日付と時刻を操作する演算子](../../sql-reference/operators.md#operators-datetime)
-   [その `Date` データ型](date.md)

[元の記事](https://clickhouse.tech/docs/en/data_types/datetime/) <!--hide-->
