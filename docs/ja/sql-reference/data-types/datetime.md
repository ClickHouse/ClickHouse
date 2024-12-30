---
slug: /ja/sql-reference/data-types/datetime
sidebar_position: 16
sidebar_label: DateTime
---

# DateTime

日付と時間として表現できる瞬間の時間を保存することができます。

構文:

``` sql
DateTime([timezone])
```

対応する値の範囲: \[1970-01-01 00:00:00, 2106-02-07 06:28:15\]。

解像度: 1秒。

## スピード

ほとんどの条件下で、`Date`データ型は`DateTime`より高速です。

`Date`型は2バイトのストレージを必要とし、`DateTime`は4バイトを必要とします。しかし、データベースを圧縮すると、この違いは増幅されます。この増幅は、`DateTime`の分と秒が圧縮しにくいためです。`DateTime`よりも`Date`をフィルタリングおよび集計する方が高速です。

## 使用上の注意

時刻は、タイムゾーンや夏時間に関係なく、[Unixタイムスタンプ](https://en.wikipedia.org/wiki/Unix_time)として保存されます。タイムゾーンは、テキスト形式での`DateTime`型の値の表示方法と、文字列として指定された値（‘2020-01-01 05:00:01’）の解析方法に影響を与えます。

タイムゾーンに依存しないUnixタイムスタンプはテーブルに保存され、データのインポート/エクスポート時や値に対するカレンダー計算を行う際にテキスト形式に変換されます（例: `toDate`, `toHour`関数など）。タイムゾーンはテーブルの行（または結果セット）には保存されませんが、カラムのメタデータに保存されます。

対応するタイムゾーンのリストは、[IANA Time Zone Database](https://www.iana.org/time-zones)で見つかり、`SELECT * FROM system.time_zones`でクエリできます。[このリスト](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones)はWikipediaにもあります。

テーブルを作成するときに`DateTime`型のカラムにタイムゾーンを明示的に設定できます。例: `DateTime('UTC')`。タイムゾーンが設定されていない場合、ClickHouseはサーバー設定の[timezone](../../operations/server-configuration-parameters/settings.md#timezone)パラメータの値またはClickHouseサーバー開始時のオペレーティングシステム設定を使用します。

[clickhouse-client](../../interfaces/cli.md)はデフォルトでこのデータ型を初期化する際、明示的にタイムゾーンが設定されていない場合、サーバーのタイムゾーンを適用します。クライアントタイムゾーンを使用するには、`--use_client_time_zone`パラメータを指定して`clickhouse-client`を実行します。

ClickHouseは、[date_time_output_format](../../operations/settings/settings-formats.md#date_time_output_format)設定の値に依存して値を出力します。デフォルトでは `YYYY-MM-DD hh:mm:ss` 形式で表示されます。さらに、[formatDateTime](../../sql-reference/functions/date-time-functions.md#formatdatetime)関数を使用して出力を変更できます。

ClickHouseにデータを挿入する際は、[date_time_input_format](../../operations/settings/settings-formats.md#date_time_input_format)設定の値に応じて、様々な形式の日時文字列を使用できます。

## 例

**1.** `DateTime`型のカラムを持つテーブルを作成し、データを挿入する:

``` sql
CREATE TABLE dt
(
    `timestamp` DateTime('Asia/Istanbul'),
    `event_id` UInt8
)
ENGINE = TinyLog;
```

``` sql
-- DateTimeを解析する
-- - 文字列から、
-- - 1970-01-01からの秒数として整数から。
INSERT INTO dt VALUES ('2019-01-01 00:00:00', 1), (1546300800, 3);

SELECT * FROM dt;
```

``` text
┌───────────timestamp─┬─event_id─┐
│ 2019-01-01 00:00:00 │        2 │
│ 2019-01-01 03:00:00 │        1 │
└─────────────────────┴──────────┘
```

- 日時を整数として挿入する場合、それはUnix Timestamp (UTC) として扱われます。`1546300800`はUTCで`'2019-01-01 00:00:00'`を表します。しかし、`timestamp`カラムには`Asia/Istanbul` (UTC+3) タイムゾーンが指定されているため、文字列として出力する際には`'2019-01-01 03:00:00'`として表示されます。
- 文字列の値を日時として挿入する際には、カラムのタイムゾーンとして扱われます。`'2019-01-01 00:00:00'`は`Asia/Istanbul`タイムゾーンとして扱われ、`1546290000`として保存されます。

**2.** `DateTime`値のフィルタリング

``` sql
SELECT * FROM dt WHERE timestamp = toDateTime('2019-01-01 00:00:00', 'Asia/Istanbul')
```

``` text
┌───────────timestamp─┬─event_id─┐
│ 2019-01-01 00:00:00 │        1 │
└─────────────────────┴──────────┘
```

`DateTime`カラムの値は、`WHERE`述語内の文字列値を使用してフィルタリングできます。自動的に`DateTime`に変換されます:

``` sql
SELECT * FROM dt WHERE timestamp = '2019-01-01 00:00:00'
```

``` text
┌───────────timestamp─┬─event_id─┐
│ 2019-01-01 00:00:00 │        1 │
└─────────────────────┴──────────┘
```

**3.** `DateTime`型カラムのタイムゾーンを取得する:

``` sql
SELECT toDateTime(now(), 'Asia/Istanbul') AS column, toTypeName(column) AS x
```

``` text
┌──────────────column─┬─x─────────────────────────┐
│ 2019-10-16 04:12:04 │ DateTime('Asia/Istanbul') │
└─────────────────────┴───────────────────────────┘
```

**4.** タイムゾーン変換

``` sql
SELECT
toDateTime(timestamp, 'Europe/London') as lon_time,
toDateTime(timestamp, 'Asia/Istanbul') as mos_time
FROM dt
```

``` text
┌───────────lon_time──┬────────────mos_time─┐
│ 2019-01-01 00:00:00 │ 2019-01-01 03:00:00 │
│ 2018-12-31 21:00:00 │ 2019-01-01 00:00:00 │
└─────────────────────┴─────────────────────┘
```

タイムゾーン変換はメタデータのみを変更するため、この操作には計算コストはありません。

## タイムゾーンサポートの制限

一部のタイムゾーンは完全にはサポートされていない場合があります。いくつかのケースについて説明します。

UTCからのオフセットが15分の倍数でない場合、時と分の計算が正確でない場合があります。例えば、1972年1月7日以前のリベリアのモンロビアのタイムゾーンは、UTC -0:44:30のオフセットがあります。モンロビアのタイムゾーンで歴史的な時間の計算を行う場合、時間処理関数が正確でない結果を返すことがありますしかし、1972年1月7日以降の結果は正確です。

時間の転換（夏時間などの理由で）が15分の倍数でない時点で行われた場合、その特定の日に誤った結果が出る可能性があります。

単調増加でないカレンダー日付。たとえば、ハッピーバレー - グースベイでは、2010年11月7日00:01:00（真夜中から1分後）に1時間後ろに移す時間がありました。そのため、11月6日が終わった後、人々は11月7日の1分間を全部観測し、その後11月6日23:01に戻り、59分後に11月7日が再び始まりました。ClickHouseではまだこのような現象はサポートされていません。これらの日には時間処理関数の結果が若干不正確となる可能性があります。

同様の問題が2010年のケーシー南極基地にも存在します。2010年3月5日02:00に3時間後ろに移されました。もしあなたが南極基地にいるなら、ClickHouseを使うことを恐れないでください。タイムゾーンをUTCに設定するか、誤差を意識してください。

複数日の時間シフト。一部の太平洋の島々はUTC+14からUTC-12にタイムゾーンオフセットを変更しました。それは問題ありませんが、過去の日時で計算を行う場合、変換日の場合に不正確さが存在することがあります。

## 関連項目

- [型変換関数](../../sql-reference/functions/type-conversion-functions.md)
- [日時を操作するための関数](../../sql-reference/functions/date-time-functions.md)
- [配列を操作するための関数](../../sql-reference/functions/array-functions.md)
- [`date_time_input_format`設定](../../operations/settings/settings-formats.md#date_time_input_format)
- [`date_time_output_format`設定](../../operations/settings/settings-formats.md#date_time_output_format)
- [`timezone`サーバー構成パラメータ](../../operations/server-configuration-parameters/settings.md#timezone)
- [`session_timezone`設定](../../operations/settings/settings.md#session_timezone)
- [日時を操作するための演算子](../../sql-reference/operators/index.md#operators-datetime)
- [`Date`データ型](../../sql-reference/data-types/date.md)
