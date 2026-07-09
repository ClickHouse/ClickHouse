---
description: 'カレンダー日付と時刻で表されるタイムスタンプを秒精度で格納する、ClickHouse の DateTime データ型のドキュメント'
sidebar_label: 'DateTime'
sidebar_position: 16
slug: /sql-reference/data-types/datetime
title: 'DateTime'
doc_type: 'reference'
---

カレンダー上の日付と時刻で表せる時点を格納できます。

構文:

```sql
DateTime([timezone])
```

サポートされる値の範囲: [1970-01-01 00:00:00, 2106-02-07 06:28:15].

分解能: 1秒.

<div id="speed">
  ## 速度
</div>

`Date` データ型は、*ほとんど* の場合で `DateTime` より高速です。

`Date` 型に必要なストレージは 2 バイトですが、`DateTime` 型では 4 バイト必要です。ただし、圧縮時には `Date` と `DateTime` のサイズ差はさらに大きくなります。これは、`DateTime` に含まれる分や秒の値が圧縮されにくいためです。また、`DateTime` ではなく `Date` に対して Filtering や集約を行うほうが高速です。

<div id="usage-remarks">
  ## 使用上の注意
</div>

時点は、タイムゾーンや夏時間に関係なく、[Unix timestamp](https://en.wikipedia.org/wiki/Unix_time) として保存されます。タイムゾーンは、`DateTime` 型の値がテキストフォーマットでどのように表示されるか、および文字列として指定された値 (`'2020-01-01 05:00:01'`) がどのように解析されるかに影響します。

タイムゾーンに依存しない Unix timestamp がテーブルに保存され、タイムゾーンは、データのインポート/エクスポート時にそれをテキストフォーマットへ変換したり、その逆変換を行ったり、値に対してカレンダー計算を行ったりするために使用されます (例: `toDate`、`toHour` 関数など) 。タイムゾーンはテーブルの行 (または resultset) には保存されず、カラムのメタデータに保存されます。

サポートされているタイムゾーンの一覧は、[IANA Time Zone Database](https://www.iana.org/time-zones) で確認できるほか、`SELECT * FROM system.time_zones` で問い合わせることもできます。[この一覧](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones)は Wikipedia でも利用できます。

テーブルの作成時に、`DateTime` 型のカラムに対してタイムゾーンを明示的に設定できます。Example: `DateTime('UTC')`。タイムゾーンが設定されていない場合、ClickHouse は server settings の [timezone](../../operations/server-configuration-parameters/settings.md#timezone) parameter の値、または ClickHouse server の起動時点のオペレーティングシステム設定を使用します。

[clickhouse-client](../../interfaces/client.md) は、データ型の初期化時にタイムゾーンが明示的に設定されていない場合、デフォルトで server のタイムゾーンを適用します。client のタイムゾーンを使用するには、`clickhouse-client` を `--use_client_time_zone` parameter を付けて実行します。

ClickHouse は、[date&#95;time&#95;output&#95;format](../../operations/settings/settings-formats.md#date_time_output_format) setting の値に応じて値を出力します。デフォルトのテキストフォーマットは `YYYY-MM-DD hh:mm:ss` です。さらに、[formatDateTime](../../sql-reference/functions/date-time-functions.md#formatDateTime) 関数を使用して出力を変更できます。

ClickHouse にデータを insert する際には、[date&#95;time&#95;input&#95;format](../../operations/settings/settings-formats.md#date_time_input_format) setting の値に応じて、さまざまな日付・時刻文字列のフォーマットを使用できます。

<div id="examples">
  ## 例
</div>

**1.** `DateTime` 型のカラムを持つテーブルを作成し、データを挿入する例:

```sql
CREATE TABLE dt
(
    `timestamp` DateTime('Asia/Istanbul'),
    `event_id` UInt8
)
ENGINE = TinyLog;
```

```sql
-- Parse DateTime
-- - from string,
-- - from integer interpreted as number of seconds since 1970-01-01.
INSERT INTO dt VALUES ('2019-01-01 00:00:00', 1), (1546300800, 2);

SELECT * FROM dt;
```

```text
┌───────────timestamp─┬─event_id─┐
│ 2019-01-01 00:00:00 │        1 │
│ 2019-01-01 03:00:00 │        2 │
└─────────────────────┴──────────┘
```

* datetime を整数として挿入すると、Unix timestamp (UTC) として扱われます。`1546300800` は UTC の `'2019-01-01 00:00:00'` を表します。ただし、`timestamp` カラムには `Asia/Istanbul` (UTC+3) のタイムゾーンが指定されているため、文字列として出力すると、この値は `'2019-01-01 03:00:00'` と表示されます。
* 文字列の値を datetime として挿入すると、カラムのタイムゾーンの時刻として扱われます。`'2019-01-01 00:00:00'` は `Asia/Istanbul` タイムゾーンの時刻として解釈され、`1546290000` として保存されます。

**2.** `DateTime` 値のフィルタリング

```sql
SELECT * FROM dt WHERE timestamp = toDateTime('2019-01-01 00:00:00', 'Asia/Istanbul')
```

```text
┌───────────timestamp─┬─event_id─┐
│ 2019-01-01 00:00:00 │        1 │
└─────────────────────┴──────────┘
```

`DateTime` カラムの値は、`WHERE` 句の条件で文字列値を使ってフィルタリングできます。文字列値は自動的に `DateTime` に変換されます:

```sql
SELECT * FROM dt WHERE timestamp = '2019-01-01 00:00:00'
```

```text
┌───────────timestamp─┬─event_id─┐
│ 2019-01-01 00:00:00 │        1 │
└─────────────────────┴──────────┘
```

**3.** `DateTime`型カラムのタイムゾーンを取得する:

```sql
SELECT toDateTime(now(), 'Asia/Istanbul') AS column, toTypeName(column) AS x
```

```text
┌──────────────column─┬─x─────────────────────────┐
│ 2019-10-16 04:12:04 │ DateTime('Asia/Istanbul') │
└─────────────────────┴───────────────────────────┘
```

**4.** タイムゾーン変換

```sql
SELECT
toDateTime(timestamp, 'Europe/London') AS lon_time,
toDateTime(timestamp, 'Asia/Istanbul') AS istanbul_time
FROM dt
```

```text
┌───────────lon_time──┬───────istanbul_time─┐
│ 2019-01-01 00:00:00 │ 2019-01-01 03:00:00 │
│ 2018-12-31 21:00:00 │ 2019-01-01 00:00:00 │
└─────────────────────┴─────────────────────┘
```

タイムゾーン変換ではメタデータが変更されるだけなので、この操作に計算コストはかかりません。

<div id="limitations-on-time-zones-support">
  ## タイムゾーンのサポートに関する制限事項
</div>

一部のタイムゾーンは、完全にはサポートされていない場合があります。主なケースは次のとおりです。

UTC からのオフセットが 15 分単位でない場合、時間や分の計算が不正確になることがあります。たとえば、リベリアのモンロビアのタイムゾーンは、1972 年 1 月 7 日より前は UTC -0:44:30 のオフセットでした。Monrovia タイムゾーン の過去の時刻に対して計算を行うと、時刻処理関数が誤った結果を返すことがあります。ただし、1972 年 1 月 7 日以降の結果は正しくなります。

時刻の切り替え (夏時間やその他の理由によるもの) が 15 分単位でない時点で行われた場合も、その特定の日に不正確な結果が生じることがあります。

暦日が単調に進まないケース。たとえば、Happy Valley - Goose Bay では、2010 年 11 月 7 日 00:01:00 (深夜 1 分後) に時刻が 1 時間戻されました。そのため、11 月 6 日が終わった後、11 月 7 日が 1 分だけ経過した時点で時刻が 11 月 6 日 23:01 に戻され、さらに 59 分後に再び 11 月 7 日が始まりました。ClickHouse は、この種のやっかいなケースには (まだ) 対応していません。この期間中は、時刻処理関数の結果がわずかに不正確になることがあります。

同様の問題は、2010 年の Casey Antarctic station にもあります。ここでは 3 月 5 日の 02:00 に時刻が 3 時間戻されました。南極の観測所で作業している場合でも、安心して ClickHouse を使ってください。ただし、タイムゾーン を UTC に設定するか、不正確さがあり得ることを認識しておいてください。

複数日にまたがる時刻のシフト。一部の太平洋の島々では、タイムゾーン の UTC オフセットが UTC+14 から UTC-12 に変更されました。これ自体は問題ありませんが、切り替えが行われた日の過去の時点についてその タイムゾーン で計算を行うと、多少の不正確さが生じることがあります。

<div id="handling-daylight-saving-time-dst">
  ## 夏時間 (DST) の扱い
</div>

タイムゾーン付きの ClickHouse の DateTime 型は、夏時間 (DST) の切り替え時に予期しない動作をすることがあります。特に次のような場合です。

* [`date_time_output_format`](../../operations/settings/settings-formats.md#date_time_output_format) が `simple` に設定されている場合。
* 時計が後ろに戻る (&quot;Fall Back&quot;) ことで、1 時間の重複が発生する場合。
* 時計が前に進む (&quot;Spring Forward&quot;) ことで、1 時間のギャップが発生する場合。

デフォルトでは、ClickHouse は重複した時刻では常に前の時刻を選択し、時計が進む切り替え時には存在しない時刻をそのまま解釈することがあります。

たとえば、夏時間 (DST) から標準時への次の切り替えを考えてみましょう。

* 2023 年 10 月 29 日の 02:00:00 に、時計は 01:00:00 に戻ります (BST → GMT) 。
* 01:00:00 – 01:59:59 の 1 時間は 2 回現れます (1 回は BST、もう 1 回は GMT) 。
* ClickHouse は常に最初の時刻 (BST) を選択するため、時間インターバルを加算すると予期しない結果になることがあります。

```sql
SELECT '2023-10-29 01:30:00'::DateTime('Europe/London') AS time, time + toIntervalHour(1) AS one_hour_later

┌────────────────time─┬──────one_hour_later─┐
│ 2023-10-29 01:30:00 │ 2023-10-29 01:30:00 │
└─────────────────────┴─────────────────────┘
```

同様に、標準時から夏時間に切り替わる際には、1時間が飛ばされたように見えることがあります。

たとえば:

* 2023年3月26日の `00:59:59` に、時計は 02:00:00 に進みます (GMT → BST) 。
* `01:00:00` ～ `01:59:59` の1時間は存在しません。

```sql
SELECT '2023-03-26 01:30:00'::DateTime('Europe/London') AS time, time + toIntervalHour(1) AS one_hour_later

┌────────────────time─┬──────one_hour_later─┐
│ 2023-03-26 00:30:00 │ 2023-03-26 02:30:00 │
└─────────────────────┴─────────────────────┘
```

この場合、ClickHouse は存在しない時刻 `2023-03-26 01:30:00` を `2023-03-26 00:30:00` に戻して扱います。

<div id="see-also">
  ## 関連項目
</div>

* [型変換関数](../../sql-reference/functions/type-conversion-functions.md)
* [日付と時刻を扱う関数](../../sql-reference/functions/date-time-functions.md)
* [Array を扱う関数](../../sql-reference/functions/array-functions.md)
* [`date_time_input_format` 設定](../../operations/settings/settings-formats.md#date_time_input_format)
* [`date_time_output_format` 設定](../../operations/settings/settings-formats.md#date_time_output_format)
* [`timezone` サーバー設定パラメータ](../../operations/server-configuration-parameters/settings.md#timezone)
* [`session_timezone` 設定](../../operations/settings/settings.md#session_timezone)
* [日付と時刻を扱う演算子](../../sql-reference/operators#operators-for-working-with-dates-and-times)
* [`Date` データ型](../../sql-reference/data-types/date.md)