---
slug: /ja/sql-reference/functions/date-time-functions
sidebar_position: 45
sidebar_label: 日付と時刻
---

# 日付と時刻を操作するための関数

このセクションのほとんどの関数は、オプションのタイムゾーン引数を受け入れます。例: `Europe/Amsterdam`。この場合、タイムゾーンは指定されたものであり、ローカル（デフォルト）のものではありません。

**例**

```sql
SELECT
    toDateTime('2016-06-15 23:00:00') AS time,
    toDate(time) AS date_local,
    toDate(time, 'Asia/Yekaterinburg') AS date_yekat,
    toString(time, 'US/Samoa') AS time_samoa
```

```text
┌────────────────time─┬─date_local─┬─date_yekat─┬─time_samoa──────────┐
│ 2016-06-15 23:00:00 │ 2016-06-15 │ 2016-06-16 │ 2016-06-15 09:00:00 │
└─────────────────────┴────────────┴────────────┴─────────────────────┘
```

## makeDate

[Date](../data-types/date.md)を作成します。
- 年、月、日を引数として指定する場合、または
- 年と年内の日を引数として指定する場合。

**構文**

```sql
makeDate(year, month, day);
makeDate(year, day_of_year);
```

エイリアス:
- `MAKEDATE(year, month, day);`
- `MAKEDATE(year, day_of_year);`

**引数**

- `year` — 年。[Integer](../data-types/int-uint.md)、[Float](../data-types/float.md) または [Decimal](../data-types/decimal.md)。
- `month` — 月。[Integer](../data-types/int-uint.md)、[Float](../data-types/float.md) または [Decimal](../data-types/decimal.md)。
- `day` — 日。[Integer](../data-types/int-uint.md)、[Float](../data-types/float.md) または [Decimal](../data-types/decimal.md)。
- `day_of_year` — 年内の日。[Integer](../data-types/int-uint.md)、[Float](../data-types/float.md) または [Decimal](../data-types/decimal.md)。

**返される値**

- 引数から作成された日付。[Date](../data-types/date.md)。

**例**

年、月、日からDateを作成します：

```sql
SELECT makeDate(2023, 2, 28) AS Date;
```

結果：

```text
┌───────date─┐
│ 2023-02-28 │
└────────────┘
```

年と年内の日を引数としてDateを作成します：

```sql
SELECT makeDate(2023, 42) AS Date;
```

結果：

```text
┌───────date─┐
│ 2023-02-11 │
└────────────┘
```

## makeDate32

年、月、日（またはオプションとして年と日）から[Date32](../../sql-reference/data-types/date32.md)型の日付を作成します。

**構文**

```sql
makeDate32(year, [month,] day)
```

**引数**

- `year` — 年。[Integer](../../sql-reference/data-types/int-uint.md)、[Float](../../sql-reference/data-types/float.md) または [Decimal](../../sql-reference/data-types/decimal.md)。
- `month` — 月（オプション）。[Integer](../../sql-reference/data-types/int-uint.md)、[Float](../../sql-reference/data-types/float.md) または [Decimal](../../sql-reference/data-types/decimal.md)。
- `day` — 日。[Integer](../../sql-reference/data-types/int-uint.md)、[Float](../../sql-reference/data-types/float.md) または [Decimal](../../sql-reference/data-types/decimal.md)。

:::note
`month`が省略された場合、`day`は`1`から`365`の値を取る必要があります。そうでなければ、`1`から`31`の値を取る必要があります。
:::

**返される値**

- 引数から作成された日付。[Date32](../../sql-reference/data-types/date32.md)。

**例**

年、月、日から日付を作成します：

クエリ：

```sql
SELECT makeDate32(2024, 1, 1);
```

結果：

```response
2024-01-01
```

年と年内の日からDateを作成します：

クエリ：

```sql
SELECT makeDate32(2024, 100);
```

結果：

```response
2024-04-09
```

## makeDateTime

年、月、日、時、分、秒の引数から[DateTime](../data-types/datetime.md)を作成します。

**構文**

```sql
makeDateTime(year, month, day, hour, minute, second[, timezone])
```

**引数**

- `year` — 年。[Integer](../data-types/int-uint.md)、[Float](../data-types/float.md) または [Decimal](../data-types/decimal.md)。
- `month` — 月。[Integer](../data-types/int-uint.md)、[Float](../data-types/float.md) または [Decimal](../data-types/decimal.md)。
- `day` — 日。[Integer](../data-types/int-uint.md)、[Float](../data-types/float.md) または [Decimal](../data-types/decimal.md)。
- `hour` — 時。[Integer](../data-types/int-uint.md)、[Float](../data-types/float.md) または [Decimal](../data-types/decimal.md)。
- `minute` — 分。[Integer](../data-types/int-uint.md)、[Float](../data-types/float.md) または [Decimal](../data-types/decimal.md)。
- `second` — 秒。[Integer](../data-types/int-uint.md)、[Float](../data-types/float.md) または [Decimal](../data-types/decimal.md)。
- `timezone` — 返される値の[タイムゾーン](../../operations/server-configuration-parameters/settings.md#timezone)（オプション）。

**返される値**

- 引数から作成された日付と時刻。[DateTime](../data-types/datetime.md)。

**例**

```sql
SELECT makeDateTime(2023, 2, 28, 17, 12, 33) AS DateTime;
```

結果：

```text
┌────────────DateTime─┐
│ 2023-02-28 17:12:33 │
└─────────────────────┘
```

## makeDateTime64

コンポーネントから[DateTime64](../../sql-reference/data-types/datetime64.md)のデータ型の値を作成します：年、月、日、時、分、秒。オプションで小数点以下の精度を持ちます。

**構文**

```sql
makeDateTime64(year, month, day, hour, minute, second[, precision])
```

**引数**

- `year` — 年（0-9999）。[Integer](../../sql-reference/data-types/int-uint.md)、[Float](../../sql-reference/data-types/float.md) または [Decimal](../../sql-reference/data-types/decimal.md)。
- `month` — 月（1-12）。[Integer](../../sql-reference/data-types/int-uint.md)、[Float](../../sql-reference/data-types/float.md) または [Decimal](../../sql-reference/data-types/decimal.md)。
- `day` — 日（1-31）。[Integer](../../sql-reference/data-types/int-uint.md)、[Float](../../sql-reference/data-types/float.md) または [Decimal](../../sql-reference/data-types/decimal.md)。
- `hour` — 時（0-23）。[Integer](../../sql-reference/data-types/int-uint.md)、[Float](../../sql-reference/data-types/float.md) または [Decimal](../../sql-reference/data-types/decimal.md)。
- `minute` — 分（0-59）。[Integer](../../sql-reference/data-types/int-uint.md)、[Float](../../sql-reference/data-types/float.md) または [Decimal](../../sql-reference/data-types/decimal.md)。
- `second` — 秒（0-59）。[Integer](../../sql-reference/data-types/int-uint.md)、[Float](../../sql-reference/data-types/float.md) または [Decimal](../../sql-reference/data-types/decimal.md)。
- `precision` — 小数点以下のコンポーネントのオプションの精度（0-9）。[Integer](../../sql-reference/data-types/int-uint.md)。

**返される値**

- 引数から作成された日付と時刻。[DateTime64](../../sql-reference/data-types/datetime64.md)。  

**例**

```sql
SELECT makeDateTime64(2023, 5, 15, 10, 30, 45, 779, 5);
```

```response
┌─makeDateTime64(2023, 5, 15, 10, 30, 45, 779, 5)─┐
│                       2023-05-15 10:30:45.00779 │
└─────────────────────────────────────────────────┘
```

## timestamp

最初の引数'expr'を[DateTime64(6)](../data-types/datetime64.md)型に変換します。
2番目の引数'expr_time'が指定されている場合、変換された値に指定された時間を加えます。

**構文**

```sql
timestamp(expr[, expr_time])
```

エイリアス: `TIMESTAMP`

**引数**

- `expr` - 日付または日時。[String](../data-types/string.md)。
- `expr_time` - オプションのパラメータ。加える時間。[String](../data-types/string.md)。

**例**

```sql
SELECT timestamp('2023-12-31') as ts;
```

結果：

```text
┌─────────────────────────ts─┐
│ 2023-12-31 00:00:00.000000 │
└────────────────────────────┘
```

```sql
SELECT timestamp('2023-12-31 12:00:00', '12:00:00.11') as ts;
```

結果：

```text
┌─────────────────────────ts─┐
│ 2024-01-01 00:00:00.110000 │
└────────────────────────────┘
```

**返される値**

- [DateTime64](../data-types/datetime64.md)(6)

## timeZone

現在のセッションのタイムゾーン、すなわち設定の値[session_timezone](../../operations/settings/settings.md#session_timezone)を返します。
分散テーブルのコンテキストで関数が実行されると、各シャードに関連する値を持つ通常のカラムが生成されます。そうでなければ定数値が生成されます。

**構文**

```sql
timeZone()
```

エイリアス: `timezone`.

**返される値**

- タイムゾーン。[String](../data-types/string.md)。

**例**

```sql
SELECT timezone()
```

結果：

```response
┌─timezone()─────┐
│ America/Denver │
└────────────────┘
```

**参照**

- [serverTimeZone](#servertimezone)

## serverTimeZone

サーバのタイムゾーン、すなわち設定の値[timezone](../../operations/server-configuration-parameters/settings.md#timezone)を返します。
分散テーブルのコンテキストで関数が実行されると、各シャードに関連する値を持つ通常のカラムが生成されます。そうでなければ定数値が生成されます。

**構文**

```sql
serverTimeZone()
```

エイリアス: `serverTimezone`.

**返される値**

- タイムゾーン。[String](../data-types/string.md)。

**例**

```sql
SELECT serverTimeZone()
```

結果：

```response
┌─serverTimeZone()─┐
│ UTC              │
└──────────────────┘
```

**参照**

- [timeZone](#timezone)

## toTimeZone

日付または日時を指定されたタイムゾーンに変換します。データの内部値（Unix秒の数）は変更せず、値のタイムゾーン属性と値の文字列表現のみが変更されます。

**構文**

```sql
toTimezone(value, timezone)
```

エイリアス: `toTimezone`.

**引数**

- `value` — 時刻または日付と時刻。[DateTime64](../data-types/datetime64.md)。
- `timezone` — 返される値のタイムゾーン。[String](../data-types/string.md)。この引数は定数です。`toTimezone`はカラムのタイムゾーンを変更するため（タイムゾーンは`DateTime*`型の属性です）。

**返される値**

- 日付と時刻。[DateTime](../data-types/datetime.md)。

**例**

```sql
SELECT toDateTime('2019-01-01 00:00:00', 'UTC') AS time_utc,
    toTypeName(time_utc) AS type_utc,
    toInt32(time_utc) AS int32utc,
    toTimeZone(time_utc, 'Asia/Yekaterinburg') AS time_yekat,
    toTypeName(time_yekat) AS type_yekat,
    toInt32(time_yekat) AS int32yekat,
    toTimeZone(time_utc, 'US/Samoa') AS time_samoa,
    toTypeName(time_samoa) AS type_samoa,
    toInt32(time_samoa) AS int32samoa
FORMAT Vertical;
```

結果：

```text
Row 1:
──────
time_utc:   2019-01-01 00:00:00
type_utc:   DateTime('UTC')
int32utc:   1546300800
time_yekat: 2019-01-01 05:00:00
type_yekat: DateTime('Asia/Yekaterinburg')
int32yekat: 1546300800
time_samoa: 2018-12-31 13:00:00
type_samoa: DateTime('US/Samoa')
int32samoa: 1546300800
```

**参照**

- [formatDateTime](#formatdatetime) - 非定数のタイムゾーンをサポートします。
- [toString](type-conversion-functions.md#tostring) - 非定数のタイムゾーンをサポートします。

## timeZoneOf

[DateTime](../data-types/datetime.md)または[DateTime64](../data-types/datetime64.md)データ型のタイムゾーン名を返します。

**構文**

```sql
timeZoneOf(value)
```

エイリアス: `timezoneOf`.

**引数**

- `value` — 日付と時刻。[DateTime](../data-types/datetime.md)または[DateTime64](../data-types/datetime64.md)。

**返される値**

- タイムゾーン名。[String](../data-types/string.md)。

**例**

```sql
SELECT timezoneOf(now());
```

結果：
```text
┌─timezoneOf(now())─┐
│ Etc/UTC           │
└───────────────────┘
```

## timeZoneOffset

[UTC](https://en.wikipedia.org/wiki/Coordinated_Universal_Time)からの秒単位のタイムゾーンオフセットを返します。
関数は指定された日時における[夏時間](https://en.wikipedia.org/wiki/Daylight_saving_time)および過去のタイムゾーンの変化を考慮します。
オフセットを計算するために[IANAタイムゾーンデータベース](https://www.iana.org/time-zones)が使用されます。

**構文**

```sql
timeZoneOffset(value)
```

エイリアス: `timezoneOffset`.

**引数**

- `value` — 日付と時刻。[DateTime](../data-types/datetime.md)または[DateTime64](../data-types/datetime64.md)。

**返される値**

- UTCからの秒単位のオフセット。[Int32](../data-types/int-uint.md)。

**例**

```sql
SELECT toDateTime('2021-04-21 10:20:30', 'America/New_York') AS Time, toTypeName(Time) AS Type,
       timeZoneOffset(Time) AS Offset_in_seconds, (Offset_in_seconds / 3600) AS Offset_in_hours;
```

結果：

```text
┌────────────────Time─┬─Type─────────────────────────┬─Offset_in_seconds─┬─Offset_in_hours─┐
│ 2021-04-21 10:20:30 │ DateTime('America/New_York') │            -14400 │              -4 │
└─────────────────────┴──────────────────────────────┴───────────────────┴─────────────────┘
```

## toYear

日付または日時の年成分（西暦）を返します。

**構文**

```sql
toYear(value)
```

エイリアス: `YEAR`

**引数**

- `value` - [Date](../data-types/date.md)、[Date32](../data-types/date32.md)、[DateTime](../data-types/datetime.md)または[DateTime64](../data-types/datetime64.md)。

**返される値**

- 指定された日付/時刻の年。[UInt16](../data-types/int-uint.md)。

**例**

```sql
SELECT toYear(toDateTime('2023-04-21 10:20:30'))
```

結果：

```response
┌─toYear(toDateTime('2023-04-21 10:20:30'))─┐
│                                      2023 │
└───────────────────────────────────────────┘
```

## toQuarter

日付または日時の四半期（1-4）を返します。

**構文**

```sql
toQuarter(value)
```

エイリアス: `QUARTER`

**引数**

- `value` - [Date](../data-types/date.md)、[Date32](../data-types/date32.md)、[DateTime](../data-types/datetime.md)または[DateTime64](../data-types/datetime64.md)。

**返される値**

- 指定された日付/時刻の四半期（1、2、3または4）。[UInt8](../data-types/int-uint.md)。

**例**

```sql
SELECT toQuarter(toDateTime('2023-04-21 10:20:30'))
```

結果：

```response
┌─toQuarter(toDateTime('2023-04-21 10:20:30'))─┐
│                                            2 │
└──────────────────────────────────────────────┘
```

## toMonth

日付または日時の月成分（1-12）を返します。

**構文**

```sql
toMonth(value)
```

エイリアス: `MONTH`

**引数**

- `value` - [Date](../data-types/date.md)、[Date32](../data-types/date32.md)、[DateTime](../data-types/datetime.md)または[DateTime64](../data-types/datetime64.md)。

**返される値**

- 指定された日付/時刻の月（1-12）。[UInt8](../data-types/int-uint.md)。

**例**

```sql
SELECT toMonth(toDateTime('2023-04-21 10:20:30'))
```

結果：

```response
┌─toMonth(toDateTime('2023-04-21 10:20:30'))─┐
│                                          4 │
└────────────────────────────────────────────┘
```

## toDayOfYear

日付または日時の年内の日の数（1-366）を返します。

**構文**

```sql
toDayOfYear(value)
```

エイリアス: `DAYOFYEAR`

**引数**

- `value` - [Date](../data-types/date.md)、[Date32](../data-types/date32.md)、[DateTime](../data-types/datetime.md)または[DateTime64](../data-types/datetime64.md)。

**返される値**

- 指定された日付/時刻の年内の日（1-366）。[UInt16](../data-types/int-uint.md)。

**例**

```sql
SELECT toDayOfYear(toDateTime('2023-04-21 10:20:30'))
```

結果：

```response
┌─toDayOfYear(toDateTime('2023-04-21 10:20:30'))─┐
│                                            111 │
└────────────────────────────────────────────────┘
```

## toDayOfMonth

日付または日時の月内の日の数（1-31）を返します。

**構文**

```sql
toDayOfMonth(value)
```

エイリアス: `DAYOFMONTH`, `DAY`

**引数**

- `value` - [Date](../data-types/date.md)、[Date32](../data-types/date32.md)、[DateTime](../data-types/datetime.md)または[DateTime64](../data-types/datetime64.md)。

**返される値**

- 指定された日付/時刻の月内の日（1-31）。[UInt8](../data-types/int-uint.md)。

**例**

```sql
SELECT toDayOfMonth(toDateTime('2023-04-21 10:20:30'))
```

結果：

```response
┌─toDayOfMonth(toDateTime('2023-04-21 10:20:30'))─┐
│                                              21 │
└─────────────────────────────────────────────────┘
```

## toDayOfWeek

日付または日時の週内の日の数を返します。

`toDayOfWeek()`の2引数形式により、週の始まりが月曜日か日曜日か、返される値が0から6の範囲か1から7の範囲かを指定できます。モード引数が省略された場合、デフォルトモードは0です。日付のタイムゾーンは、3番目の引数として指定できます。

| モード | 週の始まり | 範囲                                          |
|------|-------------------|------------------------------------------------|
| 0    | 月曜日            | 1-7: 月曜日 = 1, 火曜日 = 2, ..., 日曜日 = 7  |
| 1    | 月曜日            | 0-6: 月曜日 = 0, 火曜日 = 1, ..., 日曜日 = 6  |
| 2    | 日曜日            | 0-6: 日曜日 = 0, 月曜日 = 1, ..., 土曜日 = 6 |
| 3    | 日曜日            | 1-7: 日曜日 = 1, 月曜日 = 2, ..., 土曜日 = 7 |

**構文**

```sql
toDayOfWeek(t[, mode[, timezone]])
```

エイリアス: `DAYOFWEEK`.

**引数**

- `t` - [Date](../data-types/date.md)、[Date32](../data-types/date32.md)、[DateTime](../data-types/datetime.md)または[DateTime64](../data-types/datetime64.md)。
- `mode` - 週の最初の日を決定します。可能な値は0、1、2または3です。上記の表を参照してください。
- `timezone` - オプションのパラメータであり、他の変換関数と同様に機能します。

最初の引数は、[parseDateTime64BestEffort()](type-conversion-functions.md#parsedatetime64besteffort)によってサポートされる形式の[String](../data-types/string.md)としても指定できます。文字列引数のサポートは、特定のサードパーティツールが期待するMySQLとの互換性のために存在します。将来的に文字列引数のサポートが新しいMySQL互換設定に依存する可能性があり、文字列の解析は一般的に遅いため、使用しないことを推奨します。

**返される値**

- 指定された日付/時刻の週の曜日（1-7）、選択したモードに応じて。

**例**

以下の日付は2023年4月21日で、金曜日でした：

```sql
SELECT
    toDayOfWeek(toDateTime('2023-04-21')),
    toDayOfWeek(toDateTime('2023-04-21'), 1)
```

結果：

```response
┌─toDayOfWeek(toDateTime('2023-04-21'))─┬─toDayOfWeek(toDateTime('2023-04-21'), 1)─┐
│                                     5 │                                        4 │
└───────────────────────────────────────┴──────────────────────────────────────────┘
```

## toHour

日付時刻の時間成分（0-24）を返します。

時計が進められる場合、1時間進むと2時に発生し、時計が戻される場合、1時間戻ると3時に発生することが前提とされます（これは常に正確になるわけではなく、タイムゾーンによって異なります）。

**構文**

```sql
toHour(value)
```

エイリアス: `HOUR`

**引数**

- `value` - [DateTime](../data-types/datetime.md)または[DateTime64](../data-types/datetime64.md)。

**返される値**

- 指定された日付/時刻の時間（0-23）。[UInt8](../data-types/int-uint.md)。

**例**

```sql
SELECT toHour(toDateTime('2023-04-21 10:20:30'))
```

結果：

```response
┌─toHour(toDateTime('2023-04-21 10:20:30'))─┐
│                                        10 │
└───────────────────────────────────────────┘
```

## toMinute

日時の分成分（0-59）を返します。

**構文**

```sql
toMinute(value)
```

エイリアス: `MINUTE`

**引数**

- `value` - [DateTime](../data-types/datetime.md)または[DateTime64](../data-types/datetime64.md)。

**返される値**

- 指定された日付/時刻の時間の分（0-59）。[UInt8](../data-types/int-uint.md)。

**例**

```sql
SELECT toMinute(toDateTime('2023-04-21 10:20:30'))
```

結果：

```response
┌─toMinute(toDateTime('2023-04-21 10:20:30'))─┐
│                                          20 │
└─────────────────────────────────────────────┘
```

## toSecond

日時の秒成分（0-59）を返します。うるう秒は考慮されません。

**構文**

```sql
toSecond(value)
```

エイリアス: `SECOND`

**引数**

- `value` - [DateTime](../data-types/datetime.md)または[DateTime64](../data-types/datetime64.md)。

**返される値**

- 指定された日付/時刻の分の秒（0-59）。[UInt8](../data-types/int-uint.md)。

**例**

```sql
SELECT toSecond(toDateTime('2023-04-21 10:20:30'))
```

結果：

```response
┌─toSecond(toDateTime('2023-04-21 10:20:30'))─┐
│                                          30 │
└─────────────────────────────────────────────┘
```

## toMillisecond

日時のミリ秒成分（0-999）を返します。

**構文**

```sql
toMillisecond(value)
```

**引数**

- `value` - [DateTime](../data-types/datetime.md)または[DateTime64](../data-types/datetime64.md)。

エイリアス: `MILLISECOND`

```sql
SELECT toMillisecond(toDateTime64('2023-04-21 10:20:30.456', 3))
```

結果：

```response
┌──toMillisecond(toDateTime64('2023-04-21 10:20:30.456', 3))─┐
│                                                        456 │
└────────────────────────────────────────────────────────────┘
```

**返される値**

- 指定された日付/時刻の分のミリ秒（0-999）。[UInt16](../data-types/int-uint.md)。

## toUnixTimestamp

文字列、日付、または日時を[Unixタイムスタンプ](https://en.wikipedia.org/wiki/Unix_time)に変換し、`UInt32`表現で返します。

関数が文字列として呼び出されると、オプションのタイムゾーン引数を受け入れます。

**構文**

```sql
toUnixTimestamp(date)
toUnixTimestamp(str, [timezone])
```

**返される値**

- Unixタイムスタンプを返します。[UInt32](../data-types/int-uint.md)。

**例**

```sql
SELECT
    '2017-11-05 08:07:47' AS dt_str,
    toUnixTimestamp(dt_str) AS from_str,
    toUnixTimestamp(dt_str, 'Asia/Tokyo') AS from_str_tokyo,
    toUnixTimestamp(toDateTime(dt_str)) AS from_datetime,
    toUnixTimestamp(toDateTime64(dt_str, 0)) AS from_datetime64,
    toUnixTimestamp(toDate(dt_str)) AS from_date,
    toUnixTimestamp(toDate32(dt_str)) AS from_date32
FORMAT Vertical;
```

結果：

```text
Row 1:
──────
dt_str:          2017-11-05 08:07:47
from_str:        1509869267
from_str_tokyo:  1509836867
from_datetime:   1509869267
from_datetime64: 1509869267
from_date:       1509840000
from_date32:     1509840000
```

:::note
`toStartOf*`、`toLastDayOf*`、`toMonday`、`timeSlot`関数の返り値の型は、構成パラメータ[enable_extended_results_for_datetime_functions](../../operations/settings/settings.md#enable-extended-results-for-datetime-functions)によって決まります。このパラメータはデフォルトで`0`です。

動作は以下の通りです：
* `enable_extended_results_for_datetime_functions = 0`の場合：
  * `toStartOfYear`、`toStartOfISOYear`、`toStartOfQuarter`、`toStartOfMonth`、`toStartOfWeek`、`toLastDayOfWeek`、`toLastDayOfMonth`、`toMonday`は、引数が`Date`または`DateTime`であれば`Date`または`DateTime`を返し、引数が`Date32`または`DateTime64`であれば`Date32`または`DateTime64`を返します。
  * `toStartOfDay`、`toStartOfHour`、`toStartOfFifteenMinutes`、`toStartOfTenMinutes`、`toStartOfFiveMinutes`、`toStartOfMinute`、`timeSlot`は、引数が`Date`または`DateTime`であれば`DateTime`を返し、引数が`Date32`または`DateTime64`であれば`DateTime64`を返します。
::: 

## toStartOfYear

日付または日時を年の最初の日に切り捨てます。日付を`Date`オブジェクトとして返します。

**構文**

```sql
toStartOfYear(value)
```

**引数**

- `value` - [Date](../data-types/date.md)、[Date32](../data-types/date32.md)、[DateTime](../data-types/datetime.md)または[DateTime64](../data-types/datetime64.md)。

**返される値**

- 入力された日付/時刻の年の最初の日。[Date](../data-types/date.md)。

**例**

```sql
SELECT toStartOfYear(toDateTime('2023-04-21 10:20:30'))
```

結果：

```response
┌─toStartOfYear(toDateTime('2023-04-21 10:20:30'))─┐
│                                       2023-01-01 │
└──────────────────────────────────────────────────┘
```

## toStartOfISOYear

日付または日時をISO年の最初の日に切り捨てます。これは「通常」の年とは異なる場合があります。（参照: [https://en.wikipedia.org/wiki/ISO_week_date](https://en.wikipedia.org/wiki/ISO_week_date)）。

**構文**

```sql
toStartOfISOYear(value)
```

**引数**

- `value` - [Date](../data-types/date.md)、[Date32](../data-types/date32.md)、[DateTime](../data-types/datetime.md)または[DateTime64](../data-types/datetime64.md)。

**返される値**

- 入力された日付/時刻の年の最初の日。[Date](../data-types/date.md)。

**例**

```sql
SELECT toStartOfISOYear(toDateTime('2023-04-21 10:20:30'))
```

結果：

```response
┌─toStartOfISOYear(toDateTime('2023-04-21 10:20:30'))─┐
│                                          2023-01-02 │
└─────────────────────────────────────────────────────┘
```

## toStartOfQuarter

日付または日時を四半期の最初の日に切り捨てます。四半期の最初の日は、1月1日、4月1日、7月1日、または10月1日です。
日付を返します。

**構文**

```sql
toStartOfQuarter(value)
```

**引数**

- `value` - [Date](../data-types/date.md)、[Date32](../data-types/date32.md)、[DateTime](../data-types/datetime.md)または[DateTime64](../data-types/datetime64.md)。

**返される値**

- 指定された日付/時刻の四半期の最初の日。[Date](../data-types/date.md)。

**例**

```sql
SELECT toStartOfQuarter(toDateTime('2023-04-21 10:20:30'))
```

結果：

```response
┌─toStartOfQuarter(toDateTime('2023-04-21 10:20:30'))─┐
│                                          2023-04-01 │
└─────────────────────────────────────────────────────┘
```

## toStartOfMonth

日付または日時を月の最初の日に切り捨てます。日付を返します。

**構文**

```sql
toStartOfMonth(value)
```

**引数**

- `value` - [Date](../data-types/date.md)、[Date32](../data-types/date32.md)、[DateTime](../data-types/datetime.md)または[DateTime64](../data-types/datetime64.md)。

**返される値**

- 指定された日付/時刻の月の最初の日。[Date](../data-types/date.md)。

**例**

```sql
SELECT toStartOfMonth(toDateTime('2023-04-21 10:20:30'))
```

結果：

```response
┌─toStartOfMonth(toDateTime('2023-04-21 10:20:30'))─┐
│                                        2023-04-01 │
└───────────────────────────────────────────────────┘
```

:::note
不正な日付を解析する際の動作は実装に特有です。ClickHouseはゼロの日付を返すか、例外をスローするか、「自然」オーバーフローを行う可能性があります。
:::

## toLastDayOfMonth

日付または日時を月の最終日に切り上げます。日付を返します。

**構文**

```sql
toLastDayOfMonth(value)
```

エイリアス: `LAST_DAY`

**引数**

- `value` - [Date](../data-types/date.md)、[Date32](../data-types/date32.md)、[DateTime](../data-types/datetime.md)または[DateTime64](../data-types/datetime64.md)。

**返される値**

- 指定された日付/時刻の月の最終日。[Date](../data-types/date.md)。

**例**

```sql
SELECT toLastDayOfMonth(toDateTime('2023-04-21 10:20:30'))
```

結果：

```response
┌─toLastDayOfMonth(toDateTime('2023-04-21 10:20:30'))─┐
│                                          2023-04-30 │
└─────────────────────────────────────────────────────┘
```

## toMonday

日付または日時を最寄りの月曜日に切り捨てます。日付を返します。

**構文**

```sql
toMonday(value)
```

**引数**

- `value` - [Date](../data-types/date.md)、[Date32](../data-types/date32.md)、[DateTime](../data-types/datetime.md)または[DateTime64](../data-types/datetime64.md)。

**返される値**

- 指定された日付/時刻の最寄りの日付（またはそれ以前）の月曜日。[Date](../data-types/date.md)。

**例**

```sql
SELECT
    toMonday(toDateTime('2023-04-21 10:20:30')), /* 金曜日 */
    toMonday(toDate('2023-04-24')), /* すでに月曜日 */
```

結果：

```response
┌─toMonday(toDateTime('2023-04-21 10:20:30'))─┬─toMonday(toDate('2023-04-24'))─┐
│                                  2023-04-17 │                     2023-04-24 │
└─────────────────────────────────────────────┴────────────────────────────────┘
```

## toStartOfWeek

日付または日時を最寄りの日曜日または月曜日に切り捨てます。日付を返します。モード引数は`toWeek()`関数のモード引数と同じように機能します。モードが指定されていない場合、デフォルトは0になります。

**構文**

```sql
toStartOfWeek(t[, mode[, timezone]])
```

**引数**

- `t` - [Date](../data-types/date.md)、[Date32](../data-types/date32.md)、[DateTime](../data-types/datetime.md)または[DateTime64](../data-types/datetime64.md)。
- `mode` - 最初の日を決定します。`toWeek()`関数の説明を参照してください。
- `timezone` - オプションのパラメータであり、他の変換関数と同様に機能します。

**返される値**

- 指定された日付に対する日曜日または月曜日の日付（またはそれ以前の日付）。[Date](../data-types/date.md)。

**例**

```sql
SELECT
    toStartOfWeek(toDateTime('2023-04-21 10:20:30')), /* 金曜日 */
    toStartOfWeek(toDateTime('2023-04-21 10:20:30'), 1), /* 金曜日 */
    toStartOfWeek(toDate('2023-04-24')), /* 月曜日 */
    toStartOfWeek(toDate('2023-04-24'), 1) /* 月曜日 */
FORMAT Vertical
```

結果：

```response
Row 1:
──────
toStartOfWeek(toDateTime('2023-04-21 10:20:30')):    2023-04-16
toStartOfWeek(toDateTime('2023-04-21 10:20:30'), 1): 2023-04-17
toStartOfWeek(toDate('2023-04-24')):                 2023-04-23
toStartOfWeek(toDate('2023-04-24'), 1):              2023-04-24
```

**引数**

- `t` - [Date](../data-types/date.md)、[Date32](../data-types/date32.md)、[DateTime](../data-types/datetime.md) または [DateTime64](../data-types/datetime64.md)
- `mode` - [toWeek](#toweek) 関数で説明されている通り、週の最後の日を決定します
- `timezone` - オプションのパラメータで、他の変換関数と同様に動作します

**返される値**

- 指定された日付に基づく、最も近い日曜日または月曜日の日付。 [Date](../data-types/date.md)。

**例**

```sql
SELECT
    toLastDayOfWeek(toDateTime('2023-04-21 10:20:30')), /* 金曜日 */
    toLastDayOfWeek(toDateTime('2023-04-21 10:20:30'), 1), /* 金曜日 */
    toLastDayOfWeek(toDate('2023-04-22')), /* 土曜日 */
    toLastDayOfWeek(toDate('2023-04-22'), 1) /* 土曜日 */
FORMAT Vertical
```

結果:

```response
行 1:
──────
toLastDayOfWeek(toDateTime('2023-04-21 10:20:30')):    2023-04-22
toLastDayOfWeek(toDateTime('2023-04-21 10:20:30'), 1): 2023-04-23
toLastDayOfWeek(toDate('2023-04-22')):                 2023-04-22
toLastDayOfWeek(toDate('2023-04-22'), 1):              2023-04-23
```

## toStartOfDay

時間を含む日付を、日付の開始時刻に切り下げます。

**構文**

```sql
toStartOfDay(value)
```

**引数**

- `value` - [Date](../data-types/date.md)、[Date32](../data-types/date32.md)、[DateTime](../data-types/datetime.md) または [DateTime64](../data-types/datetime64.md)

**返される値**

- 指定された日付/時間の開始時刻。 [DateTime](../data-types/datetime.md)。

**例**

```sql
SELECT toStartOfDay(toDateTime('2023-04-21 10:20:30'))
```

結果:

```response
┌─toStartOfDay(toDateTime('2023-04-21 10:20:30'))─┐
│                             2023-04-21 00:00:00 │
└─────────────────────────────────────────────────┘
```

## toStartOfHour

時間を含む日付を、時刻の開始に切り下げます。

**構文**

```sql
toStartOfHour(value)
```

**引数**

- `value` - [DateTime](../data-types/datetime.md) または [DateTime64](../data-types/datetime64.md)

**返される値**

- 指定された日付/時間の開始時刻。 [DateTime](../data-types/datetime.md)。

**例**

```sql
SELECT
    toStartOfHour(toDateTime('2023-04-21 10:20:30')),
    toStartOfHour(toDateTime64('2023-04-21', 6))
```

結果:

```response
┌─toStartOfHour(toDateTime('2023-04-21 10:20:30'))─┬─toStartOfHour(toDateTime64('2023-04-21', 6))─┐
│                              2023-04-21 10:00:00 │                          2023-04-21 00:00:00 │
└──────────────────────────────────────────────────┴──────────────────────────────────────────────┘
```

## toStartOfMinute

時間を含む日付を、分の開始に切り下げます。

**構文**

```sql
toStartOfMinute(value)
```

**引数**

- `value` - [DateTime](../data-types/datetime.md) または [DateTime64](../data-types/datetime64.md)

**返される値**

- 指定された日付/時間の開始時刻。 [DateTime](../data-types/datetime.md)。

**例**

```sql
SELECT
    toStartOfMinute(toDateTime('2023-04-21 10:20:30')),
    toStartOfMinute(toDateTime64('2023-04-21 10:20:30.5300', 8))
FORMAT Vertical
```

結果:

```response
行 1:
──────
toStartOfMinute(toDateTime('2023-04-21 10:20:30')):           2023-04-21 10:20:00
toStartOfMinute(toDateTime64('2023-04-21 10:20:30.5300', 8)): 2023-04-21 10:20:00
```

## toStartOfSecond

サブ秒を切り捨てます。

**構文**

``` sql
toStartOfSecond(value, [timezone])
```

**引数**

- `value` — 日付と時刻。[DateTime64](../data-types/datetime64.md)。
- `timezone` — 返される値の [Timezone](../../operations/server-configuration-parameters/settings.md#timezone)（オプション）。指定されていない場合、この関数は `value` パラメータのタイムゾーンを使用します。[String](../data-types/string.md)。

**返される値**

- サブ秒を持たない入力値。[DateTime64](../data-types/datetime64.md)。

**例**

タイムゾーンなしのクエリ:

``` sql
WITH toDateTime64('2020-01-01 10:20:30.999', 3) AS dt64
SELECT toStartOfSecond(dt64);
```

結果:

``` text
┌───toStartOfSecond(dt64)─┐
│ 2020-01-01 10:20:30.000 │
└─────────────────────────┘
```

タイムゾーンありのクエリ:

``` sql
WITH toDateTime64('2020-01-01 10:20:30.999', 3) AS dt64
SELECT toStartOfSecond(dt64, 'Asia/Istanbul');
```

結果:

``` text
┌─toStartOfSecond(dt64, 'Asia/Istanbul')─┐
│                2020-01-01 13:20:30.000 │
└────────────────────────────────────────┘
```

**参照**

- [Timezone](../../operations/server-configuration-parameters/settings.md#timezone) サーバー構成パラメータ。

## toStartOfMillisecond

時間を含む日付を、ミリ秒の開始に切り下げます。

**構文**

``` sql
toStartOfMillisecond(value, [timezone])
```

**引数**

- `value` — 日付と時刻。[DateTime64](../../sql-reference/data-types/datetime64.md)。
- `timezone` — 返される値の [Timezone](../../operations/server-configuration-parameters/settings.md#timezone)（オプション）。指定されていない場合、この関数は `value` パラメータのタイムゾーンを使用します。[String](../../sql-reference/data-types/string.md)。

**返される値**

- サブミリ秒を持つ入力値。[DateTime64](../../sql-reference/data-types/datetime64.md)。

**例**

タイムゾーンなしのクエリ:

``` sql
WITH toDateTime64('2020-01-01 10:20:30.999999999', 9) AS dt64
SELECT toStartOfMillisecond(dt64);
```

結果:

``` text
┌────toStartOfMillisecond(dt64)─┐
│ 2020-01-01 10:20:30.999000000 │
└───────────────────────────────┘
```

タイムゾーンありのクエリ:

``` sql
WITH toDateTime64('2020-01-01 10:20:30.999999999', 9) AS dt64
SELECT toStartOfMillisecond(dt64, 'Asia/Istanbul');
```

結果:

``` text
┌─toStartOfMillisecond(dt64, 'Asia/Istanbul')─┐
│               2020-01-01 12:20:30.999000000 │
└─────────────────────────────────────────────┘
```

**参照**

- [Timezone](../../operations/server-configuration-parameters/settings.md#timezone) サーバー構成パラメータ。

## toStartOfMicrosecond

時間を含む日付を、マイクロ秒の開始に切り下げます。

**構文**

``` sql
toStartOfMicrosecond(value, [timezone])
```

**引数**

- `value` — 日付と時刻。[DateTime64](../../sql-reference/data-types/datetime64.md)。
- `timezone` — 返される値の [Timezone](../../operations/server-configuration-parameters/settings.md#timezone)（オプション）。指定されていない場合、この関数は `value` パラメータのタイムゾーンを使用します。[String](../../sql-reference/data-types/string.md)。

**返される値**

- サブマイクロ秒を持つ入力値。[DateTime64](../../sql-reference/data-types/datetime64.md)。

**例**

タイムゾーンなしのクエリ:

``` sql
WITH toDateTime64('2020-01-01 10:20:30.999999999', 9) AS dt64
SELECT toStartOfMicrosecond(dt64);
```

結果:

``` text
┌────toStartOfMicrosecond(dt64)─┐
│ 2020-01-01 10:20:30.999999000 │
└───────────────────────────────┘
```

タイムゾーンありのクエリ:

``` sql
WITH toDateTime64('2020-01-01 10:20:30.999999999', 9) AS dt64
SELECT toStartOfMicrosecond(dt64, 'Asia/Istanbul');
```

結果:

``` text
┌─toStartOfMicrosecond(dt64, 'Asia/Istanbul')─┐
│               2020-01-01 12:20:30.999999000 │
└─────────────────────────────────────────────┘
```

**参照**

- [Timezone](../../operations/server-configuration-parameters/settings.md#timezone) サーバー構成パラメータ。

## toStartOfNanosecond

時間を含む日付を、ナノ秒の開始に切り下げます。

**構文**

``` sql
toStartOfNanosecond(value, [timezone])
```

**引数**

- `value` — 日付と時刻。[DateTime64](../../sql-reference/data-types/datetime64.md)。
- `timezone` — 返される値の [Timezone](../../operations/server-configuration-parameters/settings.md#timezone)（オプション）。指定されていない場合、この関数は `value` パラメータのタイムゾーンを使用します。[String](../../sql-reference/data-types/string.md)。

**返される値**

- ナノ秒を持つ入力値。[DateTime64](../../sql-reference/data-types/datetime64.md)。

**例**

タイムゾーンなしのクエリ:

``` sql
WITH toDateTime64('2020-01-01 10:20:30.999999999', 9) AS dt64
SELECT toStartOfNanosecond(dt64);
```

結果:

``` text
┌─────toStartOfNanosecond(dt64)─┐
│ 2020-01-01 10:20:30.999999999 │
└───────────────────────────────┘
```

タイムゾーンありのクエリ:

``` sql
WITH toDateTime64('2020-01-01 10:20:30.999999999', 9) AS dt64
SELECT toStartOfNanosecond(dt64, 'Asia/Istanbul');
```

結果:

``` text
┌─toStartOfNanosecond(dt64, 'Asia/Istanbul')─┐
│              2020-01-01 12:20:30.999999999 │
└────────────────────────────────────────────┘
```

**参照**

- [Timezone](../../operations/server-configuration-parameters/settings.md#timezone) サーバー構成パラメータ。

## toStartOfFiveMinutes

時間を含む日付を、5分の開始に切り下げます。

**構文**

```sql
toStartOfFiveMinutes(value)
```

**引数**

- `value` - [DateTime](../data-types/datetime.md) または [DateTime64](../data-types/datetime64.md)

**返される値**

- 指定された日付/時間の5分の開始時刻。 [DateTime](../data-types/datetime.md)。

**例**

```sql
SELECT
    toStartOfFiveMinutes(toDateTime('2023-04-21 10:17:00')),
    toStartOfFiveMinutes(toDateTime('2023-04-21 10:20:00')),
    toStartOfFiveMinutes(toDateTime('2023-04-21 10:23:00'))
FORMAT Vertical
```

結果:

```response
行 1:
──────
toStartOfFiveMinutes(toDateTime('2023-04-21 10:17:00')): 2023-04-21 10:15:00
toStartOfFiveMinutes(toDateTime('2023-04-21 10:20:00')): 2023-04-21 10:20:00
toStartOfFiveMinutes(toDateTime('2023-04-21 10:23:00')): 2023-04-21 10:20:00
```

## toStartOfTenMinutes

時間を含む日付を、10分の開始に切り下げます。

**構文**

```sql
toStartOfTenMinutes(value)
```

**引数**

- `value` - [DateTime](../data-types/datetime.md) または [DateTime64](../data-types/datetime64.md)

**返される値**

- 指定された日付/時間の10分の開始時刻。 [DateTime](../data-types/datetime.md)。

**例**

```sql
SELECT
    toStartOfTenMinutes(toDateTime('2023-04-21 10:17:00')),
    toStartOfTenMinutes(toDateTime('2023-04-21 10:20:00')),
    toStartOfTenMinutes(toDateTime('2023-04-21 10:23:00'))
FORMAT Vertical
```

結果:

```response
行 1:
──────
toStartOfTenMinutes(toDateTime('2023-04-21 10:17:00')): 2023-04-21 10:10:00
toStartOfTenMinutes(toDateTime('2023-04-21 10:20:00')): 2023-04-21 10:20:00
toStartOfTenMinutes(toDateTime('2023-04-21 10:23:00')): 2023-04-21 10:20:00
```

## toStartOfFifteenMinutes

時間を含む日付を、15分の開始に切り下げます。

**構文**

```sql
toStartOfFifteenMinutes(value)
```

**引数**

- `value` - [DateTime](../data-types/datetime.md) または [DateTime64](../data-types/datetime64.md)

**返される値**

- 指定された日付/時間の15分の開始時刻。 [DateTime](../data-types/datetime.md)。

**例**

```sql
SELECT
    toStartOfFifteenMinutes(toDateTime('2023-04-21 10:17:00')),
    toStartOfFifteenMinutes(toDateTime('2023-04-21 10:20:00')),
    toStartOfFifteenMinutes(toDateTime('2023-04-21 10:23:00'))
FORMAT Vertical
```

結果:

```response
行 1:
──────
toStartOfFifteenMinutes(toDateTime('2023-04-21 10:17:00')): 2023-04-21 10:15:00
toStartOfFifteenMinutes(toDateTime('2023-04-21 10:20:00')): 2023-04-21 10:15:00
toStartOfFifteenMinutes(toDateTime('2023-04-21 10:23:00')): 2023-04-21 10:15:00
```

## toStartOfInterval

この関数は `toStartOf*()` 関数を一般化し、 `toStartOfInterval(date_or_date_with_time, INTERVAL x unit [, time_zone])` 構文を使用します。
例えば、
- `toStartOfInterval(t, INTERVAL 1 YEAR)` は `toStartOfYear(t)` と同じ結果を返します
- `toStartOfInterval(t, INTERVAL 1 MONTH)` は `toStartOfMonth(t)` と同じ結果を返します
- `toStartOfInterval(t, INTERVAL 1 DAY)` は `toStartOfDay(t)` と同じ結果を返します
- `toStartOfInterval(t, INTERVAL 15 MINUTE)` は `toStartOfFifteenMinutes(t)` と同じ結果を返します

計算は特定の時点に対して行われます。

| インターバル    | 開始                     |
|-------------|--------------------------|
| YEAR        | 年 0                     |
| QUARTER     | 1900年第1四半期          |
| MONTH       | 1900年1月                |
| WEEK        | 1970年第1週（01-05）     |
| DAY         | 1970-01-01               |
| HOUR        | (*)                      |
| MINUTE      | 1970-01-01 00:00:00      |
| SECOND      | 1970-01-01 00:00:00      |
| MILLISECOND | 1970-01-01 00:00:00      |
| MICROSECOND | 1970-01-01 00:00:00      |
| NANOSECOND  | 1970-01-01 00:00:00      |

(*) 時間インターバルは特別です: 計算は常に現在の日の00:00:00（真夜中）を基準に行われます。そのため、1時から23時の間の時間値のみが有用です。

もし `WEEK` 単位が指定された場合、`toStartOfInterval` は週の始まりを月曜日と仮定します。この動作は、デフォルトで日曜日から始まる `toStartOfWeek` 関数とは異なります。

**構文**

```sql
toStartOfInterval(value, INTERVAL x unit[, time_zone])
toStartOfInterval(value, INTERVAL x unit[, origin[, time_zone]])
```
エイリアス: `time_bucket`, `date_bin`.

2番目のオーバーロードは、TimescaleDBの `time_bucket()` および PostgreSQLの `date_bin()` 関数をエミュレートします。例えば、

``` SQL
SELECT toStartOfInterval(toDateTime('2023-01-01 14:45:00'), INTERVAL 1 MINUTE, toDateTime('2023-01-01 14:35:30'));
```

結果:

``` reference
┌───toStartOfInterval(...)─┐
│      2023-01-01 14:44:30 │
└──────────────────────────┘
```

**参照**
- [date_trunc](#date_trunc)

## toTime

時間を保持しながら、日時を特定の固定日付に変換します。

**構文**

```sql
toTime(date[,timezone])
```

**引数**

- `date` — 時間に変換する日付。 [Date](../data-types/date.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md)。
- `timezone`（オプション） — 返される値のタイムゾーン。[String](../data-types/string.md)。

**返される値**

- 日付を `1970-01-02` に等しくしながら、時間を保持した DateTime。[DateTime](../data-types/datetime.md)。

:::note
`date` 入力引数がサブ秒の成分を含んでいた場合、それらは返される `DateTime` 値において秒精度で削除されます。
:::

**例**

クエリ:

```sql
SELECT toTime(toDateTime64('1970-12-10 01:20:30.3000',3)) AS result, toTypeName(result);
```

結果:

```response
┌──────────────result─┬─toTypeName(result)─┐
│ 1970-01-02 01:20:30 │ DateTime           │
└─────────────────────┴────────────────────┘
```

## toRelativeYearNum

日時を、過去の特定の固定点から経過した年数に変換します。

**構文**

```sql
toRelativeYearNum(date)
```

**引数**

- `date` — 日付または日時。[Date](../data-types/date.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md)。

**返される値**

- 過去の固定参照点からの年数。 [UInt16](../data-types/int-uint.md)。

**例**

クエリ:

```sql
SELECT
    toRelativeYearNum(toDate('2002-12-08')) AS y1,
    toRelativeYearNum(toDate('2010-10-26')) AS y2
```

結果:

```response
┌───y1─┬───y2─┐
│ 2002 │ 2010 │
└──────┴──────┘
```

## toRelativeQuarterNum

日時を、過去の特定の固定点から経過した四半期数に変換します。

**構文**

```sql
toRelativeQuarterNum(date)
```

**引数**

- `date` — 日付または日時。[Date](../data-types/date.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md)。

**返される値**

- 過去の固定参照点からの四半期数。 [UInt32](../data-types/int-uint.md)。

**例**

クエリ:

```sql
SELECT
  toRelativeQuarterNum(toDate('1993-11-25')) AS q1,
  toRelativeQuarterNum(toDate('2005-01-05')) AS q2
```

結果:

```response
┌───q1─┬───q2─┐
│ 7975 │ 8020 │
└──────┴──────┘
```

## toRelativeMonthNum

日時を、過去の特定の固定点から経過した月数に変換します。

**構文**

```sql
toRelativeMonthNum(date)
```

**引数**

- `date` — 日付または日時。[Date](../data-types/date.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md)。

**返される値**

- 過去の固定参照点からの月数。 [UInt32](../data-types/int-uint.md)。

**例**

クエリ:

```sql
SELECT
  toRelativeMonthNum(toDate('2001-04-25')) AS m1,
  toRelativeMonthNum(toDate('2009-07-08')) AS m2
```

結果:

```response
┌────m1─┬────m2─┐
│ 24016 │ 24115 │
└───────┴───────┘
```

## toRelativeWeekNum

日時を、過去の特定の固定点から経過した週数に変換します。

**構文**

```sql
toRelativeWeekNum(date)
```

**引数**

- `date` — 日付または日時。[Date](../data-types/date.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md)。

**返される値**

- 過去の固定参照点からの週数。 [UInt32](../data-types/int-uint.md)。

**例**

クエリ:

```sql
SELECT
  toRelativeWeekNum(toDate('2000-02-29')) AS w1,
  toRelativeWeekNum(toDate('2001-01-12')) AS w2
```

結果:

```response
┌───w1─┬───w2─┐
│ 1574 │ 1619 │
└──────┴──────┘
```

## toRelativeDayNum

日時を、過去の特定の固定点から経過した日数に変換します。

**構文**

```sql
toRelativeDayNum(date)
```

**引数**

- `date` — 日付または日時。[Date](../data-types/date.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md)。

**返される値**

- 過去の固定参照点からの日数。 [UInt32](../data-types/int-uint.md)。

**例**

クエリ:

```sql
SELECT
  toRelativeDayNum(toDate('1993-10-05')) AS d1,
  toRelativeDayNum(toDate('2000-09-20')) AS d2
```

結果:

```response
┌───d1─┬────d2─┐
│ 8678 │ 11220 │
└──────┴───────┘
```

## toRelativeHourNum

日時を、過去の特定の固定点から経過した時間数に変換します。

**構文**

```sql
toRelativeHourNum(date)
```

**引数**

- `date` — 日付または日時。[Date](../data-types/date.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md)。

**返される値**

- 過去の固定参照点からの時間数。 [UInt32](../data-types/int-uint.md)。

**例**

クエリ:

```sql
SELECT
  toRelativeHourNum(toDateTime('1993-10-05 05:20:36')) AS h1,
  toRelativeHourNum(toDateTime('2000-09-20 14:11:29')) AS h2
```

結果:

```response
┌─────h1─┬─────h2─┐
│ 208276 │ 269292 │
└────────┴────────┘
```

## toRelativeMinuteNum

日時を、過去の特定の固定点から経過した分数に変換します。

**構文**

```sql
toRelativeMinuteNum(date)
```

**引数**

- `date` — 日付または日時。[Date](../data-types/date.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md)。

**返される値**

- 過去の固定参照点からの分数。 [UInt32](../data-types/int-uint.md)。

**例**

クエリ:

```sql
SELECT
  toRelativeMinuteNum(toDateTime('1993-10-05 05:20:36')) AS m1,
  toRelativeMinuteNum(toDateTime('2000-09-20 14:11:29')) AS m2
```

結果:

```response
┌───────m1─┬───────m2─┐
│ 12496580 │ 16157531 │
└──────────┴──────────┘
```

## toRelativeSecondNum

日時を、過去の特定の固定点から経過した秒数に変換します。

**構文**

```sql
toRelativeSecondNum(date)
```

**引数**

- `date` — 日付または日時。[Date](../data-types/date.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md)。

**返される値**

- 過去の固定参照点からの秒数。 [UInt32](../data-types/int-uint.md)。

**例**

クエリ:

```sql
SELECT
  toRelativeSecondNum(toDateTime('1993-10-05 05:20:36')) AS s1,
  toRelativeSecondNum(toDateTime('2000-09-20 14:11:29')) AS s2
```

結果:

```response
┌────────s1─┬────────s2─┐
│ 749794836 │ 969451889 │
└───────────┴───────────┘
```

## toISOYear

日時を、ISO年を UInt16 数値として変換します。

**構文**

```sql
toISOYear(value)
```

**引数**

- `value` — 日付または日時。 [Date](../data-types/date.md)、[Date32](../data-types/date32.md)、[DateTime](../data-types/datetime.md) または [DateTime64](../data-types/datetime64.md)

**返される値**

- 入力値をISO年番号に変換したもの。 [UInt16](../data-types/int-uint.md)。

**例**

クエリ:

```sql
SELECT
  toISOYear(toDate('2024/10/02')) as year1,
  toISOYear(toDateTime('2024-10-02 01:30:00')) as year2
```

結果:

```response
┌─year1─┬─year2─┐
│  2024 │  2024 │
└───────┴───────┘
```

## toISOWeek

日時を、ISO週番号を含む UInt8 数値に変換します。

**構文**

```sql
toISOWeek(value)
```

**引数**

- `value` — 日付または日時に関連する値。

**返される値**

- `value`を現在のISO週番号に変換したもの。 [UInt8](../data-types/int-uint.md)。

**例**

クエリ:

```sql
SELECT
  toISOWeek(toDate('2024/10/02')) AS week1,
  toISOWeek(toDateTime('2024/10/02 01:30:00')) AS week2
```

結果:

```response
┌─week1─┬─week2─┐
│    40 │    40 │
└───────┴───────┘
```

## toWeek

この関数は日時または日付の週番号を返します。 `toWeek()` の2引数形式では、週の始まりを日曜日または月曜日に指定でき、返される値が0から53の範囲か1から53の範囲にするかを指定します。mode引数が省略された場合、デフォルトのmodeは0です。

`toISOWeek()` は、`toWeek(date,3)` に相当する互換性関数です。

以下の表はmode引数の動作を説明します。

| モード | 週の初日 | 範囲 | 週1は最初の週 ...             |
|------|-------|-------|------------------------|
| 0    | 日曜日   | 0-53  | 当年に日曜日を含む週       |
| 1    | 月曜日   | 0-53  | 当年に4日以上を含む週     |
| 2    | 日曜日   | 1-53  | 当年に日曜日を含む週       |
| 3    | 月曜日   | 1-53  | 当年に4日以上を含む週     |
| 4    | 日曜日   | 0-53  | 当年に4日以上を含む週     |
| 5    | 月曜日   | 0-53  | 当年に月曜日を含む週      |
| 6    | 日曜日   | 1-53  | 当年に4日以上を含む週     |
| 7    | 月曜日   | 1-53  | 当年に月曜日を含む週      |
| 8    | 日曜日   | 1-53  | 1月1日を含む週            |
| 9    | 月曜日   | 1-53  | 1月1日を含む週            |

「当年に4日以上を含む」と意味するmode値の場合、週はISO 8601:1988に従って番号が付けられます：

- 1月1日を含む週が4日以上ある場合、それは週1です。

- そうでない場合、前の年の最後の週になり、次の週が週1です。

「1月1日を含む」と意味するmode値の場合、1月1日を含む週が週1です。
新年に何日あったかは関係ありません。一日だけでも構いません。
つまり、12月の最後の週が翌年の1月1日を含む場合、それは翌年の週1になります。

**構文**

``` sql
toWeek(t[, mode[, time_zone]])
```

エイリアス: `WEEK`

**引数**

- `t` – 日付または日時。
- `mode` – オプションのパラメータ。値の範囲は \[0,9\]、デフォルトは0です。
- `timezone` – オプションのパラメータで、他の変換関数と同様に動作します。

最初の引数は、[parseDateTime64BestEffort()](type-conversion-functions.md#parsedatetime64besteffort)でサポートされたフォーマットの[文字列](../data-types/string.md)として指定することもできます。文字列引数のサポートは、特定の3rdパーティツールによって期待されるMySQLとの互換性を理由に存在しています。文字列引数のサポートは、将来的には新しいMySQL互換性設定に依存する可能性があり、文字列解析は一般的に遅いため使用しないことを推奨します。

**例**

``` sql
SELECT toDate('2016-12-27') AS date, toWeek(date) AS week0, toWeek(date,1) AS week1, toWeek(date,9) AS week9;
```

``` text
┌───────date─┬─week0─┬─week1─┬─week9─┐
│ 2016-12-27 │    52 │    52 │     1 │
└────────────┴───────┴───────┴───────┘
```

## toYearWeek

日付の年と週を返します。結果の年は、年の最初の週と最後の週で日付引数の年とは異なる場合があります。

mode引数は `toWeek()` のmode引数と同様に機能します。単一引数の構文の場合、mode値0が使用されます。

`toISOYear()` は、`intDiv(toYearWeek(date,3),100)` に相当する互換性関数です。

:::warning
`toYearWeek()` によって返される週番号は、`toWeek()` が返すものと異なる場合があります。`toWeek()` は常に指定された年のコンテキストで週番号を返し、`toWeek()` が `0` を返す場合、`toYearWeek()` は前年度の最後の週に対応する値を返します。以下の例の `prev_yearWeek` を参照してください。
:::

**構文**

``` sql
toYearWeek(t[, mode[, timezone]])
```

エイリアス: `YEARWEEK`

最初の引数は、[parseDateTime64BestEffort()](type-conversion-functions.md#parsedatetime64besteffort) でサポートされたフォーマットの[文字列](../data-types/string.md)として指定することもできます。文字列引数のサポートは、特定の3rdパーティツールによって期待されるMySQLとの互換性を理由に存在しています。文字列引数のサポートは、将来的には新しいMySQL互換性設定に依存する可能性があり、文字列解析は一般的に遅いため使用しないことを推奨します。

**例**

``` sql
SELECT toDate('2016-12-27') AS date, toYearWeek(date) AS yearWeek0, toYearWeek(date,1) AS yearWeek1, toYearWeek(date,9) AS yearWeek9, toYearWeek(toDate('2022-01-01')) AS prev_yearWeek;
```

``` text
┌───────date─┬─yearWeek0─┬─yearWeek1─┬─yearWeek9─┬─prev_yearWeek─┐
│ 2016-12-27 │    201652 │    201652 │    201701 │        202152 │
└────────────┴───────────┴───────────┴───────────┴───────────────┘
```

## toDaysSinceYearZero

指定された日付から[0000年1月1日](https://en.wikipedia.org/wiki/Year_zero)までの経過日数を返します。[ISO 8601](https://en.wikipedia.org/wiki/Gregorian_calendar#Proleptic_Gregorian_calendar)で定義される先取りグレゴリオ暦に基づきます。計算はMySQLの[`TO_DAYS()`](https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_to-days)関数と同様です。

**構文**

``` sql
toDaysSinceYearZero(date[, time_zone])
```

エイリアス: `TO_DAYS`

**引数**

- `date` — 年ゼロから経過した日数を計算する日付。[Date](../data-types/date.md)、[Date32](../data-types/date32.md)、[DateTime](../data-types/datetime.md) または [DateTime64](../data-types/datetime64.md)。
- `time_zone` — タイムゾーンを表す文字列型の定数値または式。[String types](../data-types/string.md)

**返される値**

0000-01-01からの日数。[UInt32](../data-types/int-uint.md)。

**例**

``` sql
SELECT toDaysSinceYearZero(toDate('2023-09-08'));
```

結果:

``` text
┌─toDaysSinceYearZero(toDate('2023-09-08')))─┐
│                                     713569 │
└────────────────────────────────────────────┘
```

**参照**

- [fromDaysSinceYearZero](#fromdayssinceyearzero)

## fromDaysSinceYearZero

指定された日数から[0000年1月1日](https://en.wikipedia.org/wiki/Year_zero)を過ぎた対応する日付を返します。[ISO 8601](https://en.wikipedia.org/wiki/Gregorian_calendar#Proleptic_Gregorian_calendar)で定義される先取りグレゴリオ暦に基づきます。計算はMySQLの[`FROM_DAYS()`](https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_from-days)関数と同様です。

結果が[Date](../data-types/date.md)型の範囲内に収まらない場合、未定義です。

**構文**

``` sql
fromDaysSinceYearZero(days)
```

エイリアス: `FROM_DAYS`

**引数**

- `days` — 年ゼロから経過した日数。

**返される値**

年ゼロから経過した日数に対応する日付。[Date](../data-types/date.md)。

**例**

``` sql
SELECT fromDaysSinceYearZero(739136), fromDaysSinceYearZero(toDaysSinceYearZero(toDate('2023-09-08')));
```

結果:

``` text
┌─fromDaysSinceYearZero(739136)─┬─fromDaysSinceYearZero(toDaysSinceYearZero(toDate('2023-09-08')))─┐
│                    2023-09-08 │                                                       2023-09-08 │
└───────────────────────────────┴──────────────────────────────────────────────────────────────────┘
```

**参照**

- [toDaysSinceYearZero](#todayssinceyearzero)

## fromDaysSinceYearZero32

[FromDaysSinceYearZero](#fromdayssinceyearzero) と同様ですが、[Date32](../data-types/date32.md)を返します。

## age

`startdate` と `enddate` の間の `unit` コンポーネントの差を返します。差は1ナノ秒の精度で計算されます。
例えば、`2021-12-29` と `2022-01-01` の間の差は、`day` 単位で3日、`month` 単位で0ヶ月、`year` 単位で0年です。

`age` の代替として、`date_diff` 関数があります。

**構文**

``` sql
age('unit', startdate, enddate, [timezone])
```

**引数**

- `unit` — 結果のためのインターバルタイプ。[String](../data-types/string.md)。
    可能な値:

    - `nanosecond`, `nanoseconds`, `ns`
    - `microsecond`, `microseconds`, `us`, `u`
    - `millisecond`, `milliseconds`, `ms`
    - `second`, `seconds`, `ss`, `s`
    - `minute`, `minutes`, `mi`, `n`
    - `hour`, `hours`, `hh`, `h`
    - `day`, `days`, `dd`, `d`
    - `week`, `weeks`, `wk`, `ww`
    - `month`, `months`, `mm`, `m`
    - `quarter`, `quarters`, `qq`, `q`
    - `year`, `years`, `yyyy`, `yy`

- `startdate` — 引き算の最初の時間値（被減数）。[Date](../data-types/date.md)、[Date32](../data-types/date32.md)、[DateTime](../data-types/datetime.md) または [DateTime64](../data-types/datetime64.md)。

- `enddate` — 引き算の2番目の時間値（減数）。[Date](../data-types/date.md)、[Date32](../data-types/date32.md)、[DateTime](../data-types/datetime.md) または [DateTime64](../data-types/datetime64.md)。

- `timezone` — [タイムゾーン名](../../operations/server-configuration-parameters/settings.md#timezone) (オプション)。指定された場合、`startdate` と `enddate` の両方に適用されます。指定されていない場合は、`startdate` と `enddate` のタイムゾーンが使用されます。それらが異なる場合、結果は未定義となります。[文字列](../data-types/string.md)。

**戻り値**

`enddate` と `startdate` の間の差を `unit` で表現したもの。[整数](../data-types/int-uint.md)。

**例**

``` sql
SELECT age('hour', toDateTime('2018-01-01 22:30:00'), toDateTime('2018-01-02 23:00:00'));
```

結果:

``` text
┌─age('hour', toDateTime('2018-01-01 22:30:00'), toDateTime('2018-01-02 23:00:00'))─┐
│                                                                                24 │
└───────────────────────────────────────────────────────────────────────────────────┘
```

``` sql
SELECT
    toDate('2022-01-01') AS e,
    toDate('2021-12-29') AS s,
    age('day', s, e) AS day_age,
    age('month', s, e) AS month__age,
    age('year', s, e) AS year_age;
```

結果:

``` text
┌──────────e─┬──────────s─┬─day_age─┬─month__age─┬─year_age─┐
│ 2022-01-01 │ 2021-12-29 │       3 │          0 │        0 │
└────────────┴────────────┴─────────┴────────────┴──────────┘
```


## date_diff

`startdate` と `enddate` の間で交差した特定の `unit` 境界の数を返します。
差は相対単位を使用して計算されます。例えば、`2021-12-29` と `2022-01-01` の差は単位 `day` で 3 日（[toRelativeDayNum](#torelativedaynum) を参照）、単位 `month` で 1 ヶ月（[toRelativeMonthNum](#torelativemonthnum) を参照）、単位 `year` で 1 年（[toRelativeYearNum](#torelativeyearnum) を参照）です。

単位 `week` が指定された場合、`date_diff` は週が月曜日から始まると仮定します。この挙動は、週がデフォルトで日曜日から始まる関数 `toWeek()` とは異なります。

`date_diff` に代わる関数については、関数 `age` を参照してください。

**構文**

``` sql
date_diff('unit', startdate, enddate, [timezone])
```

別名: `dateDiff`, `DATE_DIFF`, `timestampDiff`, `timestamp_diff`, `TIMESTAMP_DIFF`.

**引数**

- `unit` — 結果の間隔の種類。[文字列](../data-types/string.md)。
    可能な値:

    - `nanosecond`, `nanoseconds`, `ns`
    - `microsecond`, `microseconds`, `us`, `u`
    - `millisecond`, `milliseconds`, `ms`
    - `second`, `seconds`, `ss`, `s`
    - `minute`, `minutes`, `mi`, `n`
    - `hour`, `hours`, `hh`, `h`
    - `day`, `days`, `dd`, `d`
    - `week`, `weeks`, `wk`, `ww`
    - `month`, `months`, `mm`, `m`
    - `quarter`, `quarters`, `qq`, `q`
    - `year`, `years`, `yyyy`, `yy`

- `startdate` — 引き算する最初の日時（被減数）。[日付](../data-types/date.md)、[Date32](../data-types/date32.md)、[日時](../data-types/datetime.md) または [DateTime64](../data-types/datetime64.md)。

- `enddate` — 引き算される第二の日時（減数）。[日付](../data-types/date.md)、[Date32](../data-types/date32.md)、[日時](../data-types/datetime.md) または [DateTime64](../data-types/datetime64.md)。

- `timezone` — [タイムゾーン名](../../operations/server-configuration-parameters/settings.md#timezone) (オプション)。指定された場合、`startdate` と `enddate` の両方に適用されます。指定されていない場合は、`startdate` と `enddate` のタイムゾーンが使用されます。それらが異なる場合、結果は未定義となります。[文字列](../data-types/string.md)。

**戻り値**

`enddate` と `startdate` の間の差を `unit` で表現したもの。[整数](../data-types/int-uint.md)。

**例**

``` sql
SELECT dateDiff('hour', toDateTime('2018-01-01 22:00:00'), toDateTime('2018-01-02 23:00:00'));
```

結果:

``` text
┌─dateDiff('hour', toDateTime('2018-01-01 22:00:00'), toDateTime('2018-01-02 23:00:00'))─┐
│                                                                                     25 │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

``` sql
SELECT
    toDate('2022-01-01') AS e,
    toDate('2021-12-29') AS s,
    dateDiff('day', s, e) AS day_diff,
    dateDiff('month', s, e) AS month__diff,
    dateDiff('year', s, e) AS year_diff;
```

結果:

``` text
┌──────────e─┬──────────s─┬─day_diff─┬─month__diff─┬─year_diff─┐
│ 2022-01-01 │ 2021-12-29 │        3 │           1 │         1 │
└────────────┴────────────┴──────────┴─────────────┴───────────┘
```

## date\_trunc

日付と時刻のデータを指定された日付の部分に切り捨てます。

**構文**

``` sql
date_trunc(unit, value[, timezone])
```

別名: `dateTrunc`.

**引数**

- `unit` — 結果を切り捨てる間隔の種類。[文字列リテラル](../syntax.md#syntax-string-literal)。
    可能な値:

    - `nanosecond` - DateTime64 のみ互換性があります
    - `microsecond` - DateTime64 のみ互換性があります
    - `milisecond` - DateTime64 のみ互換性があります
    - `second`
    - `minute`
    - `hour`
    - `day`
    - `week`
    - `month`
    - `quarter`
    - `year`

    `unit` 引数は大文字小文字を区別しません。

- `value` — 日付と時刻。[日付](../data-types/date.md)、[Date32](../data-types/date32.md)、[日時](../data-types/datetime.md) または [DateTime64](../data-types/datetime64.md)。
- `timezone` — 戻り値の [タイムゾーン名](../../operations/server-configuration-parameters/settings.md#timezone) (オプション)。指定されていない場合、関数は `value` パラメータのタイムゾーンを使用します。[文字列](../data-types/string.md)。

**戻り値**

- 指定された日付の部分に切り捨てられた値。[日時](../data-types/datetime.md)。

**例**

タイムゾーンなしのクエリ:

``` sql
SELECT now(), date_trunc('hour', now());
```

結果:

``` text
┌───────────────now()─┬─date_trunc('hour', now())─┐
│ 2020-09-28 10:40:45 │       2020-09-28 10:00:00 │
└─────────────────────┴───────────────────────────┘
```

指定されたタイムゾーンでのクエリ:

```sql
SELECT now(), date_trunc('hour', now(), 'Asia/Istanbul');
```

結果:

```text
┌───────────────now()─┬─date_trunc('hour', now(), 'Asia/Istanbul')─┐
│ 2020-09-28 10:46:26 │                        2020-09-28 13:00:00 │
└─────────────────────┴────────────────────────────────────────────┘
```

**参照してください**

- [toStartOfInterval](#tostartofinterval)

## date\_add

指定された日数または日時に時間間隔または日付間隔を追加します。

加算の結果がデータ型の範囲外の値になる場合、結果は未定義になります。

**構文**

``` sql
date_add(unit, value, date)
```

代替構文:

``` sql
date_add(date, INTERVAL value unit)
```

別名: `dateAdd`, `DATE_ADD`.

**引数**

- `unit` — 追加する間隔の種類。注: これは [文字列](../data-types/string.md) ではなく、引用されない必要があります。
    可能な値:

    - `second`
    - `minute`
    - `hour`
    - `day`
    - `week`
    - `month`
    - `quarter`
    - `year`

- `value` — 追加する間隔の値。[整数](../data-types/int-uint.md)。
- `date` — `value` が追加される日付または日付と時刻。[日付](../data-types/date.md)、[Date32](../data-types/date32.md)、[日時](../data-types/datetime.md) または [DateTime64](../data-types/datetime64.md)。

**戻り値**

`date` に `value` を追加した結果の日付または日付と時刻。 [日付](../data-types/date.md)、[Date32](../data-types/date32.md)、[日時](../data-types/datetime.md) または [DateTime64](../data-types/datetime64.md)。

**例**

```sql
SELECT date_add(YEAR, 3, toDate('2018-01-01'));
```

結果:

```text
┌─plus(toDate('2018-01-01'), toIntervalYear(3))─┐
│                                    2021-01-01 │
└───────────────────────────────────────────────┘
```

```sql
SELECT date_add(toDate('2018-01-01'), INTERVAL 3 YEAR);
```

結果:

```text
┌─plus(toDate('2018-01-01'), toIntervalYear(3))─┐
│                                    2021-01-01 │
└───────────────────────────────────────────────┘
```



**参照してください**

- [addDate](#adddate)

## date\_sub

指定された日付または日時から時間間隔または日付間隔を引き算します。

減算の結果がデータ型の範囲外の値になる場合、結果は未定義になります。

**構文**

``` sql
date_sub(unit, value, date)
```

代替構文:

``` sql
date_sub(date, INTERVAL value unit)
```


別名: `dateSub`, `DATE_SUB`.

**引数**

- `unit` — 減算する間隔の種類。注: これは [文字列](../data-types/string.md) ではなく、引用されない必要があります。

    可能な値:

    - `second`
    - `minute`
    - `hour`
    - `day`
    - `week`
    - `month`
    - `quarter`
    - `year`

- `value` — 減算する間隔の値。[整数](../data-types/int-uint.md)。
- `date` — `value` が引かれる日付または日時。[日付](../data-types/date.md)、[Date32](../data-types/date32.md)、[日時](../data-types/datetime.md) または [DateTime64](../data-types/datetime64.md)。

**戻り値**

`date` から `value` を引いた結果の日付または日付と時刻。[日付](../data-types/date.md)、[Date32](../data-types/date32.md)、[日時](../data-types/datetime.md) または [DateTime64](../data-types/datetime64.md)。

**例**

``` sql
SELECT date_sub(YEAR, 3, toDate('2018-01-01'));
```

結果:

``` text
┌─minus(toDate('2018-01-01'), toIntervalYear(3))─┐
│                                     2015-01-01 │
└────────────────────────────────────────────────┘
```

``` sql
SELECT date_sub(toDate('2018-01-01'), INTERVAL 3 YEAR);
```

結果:

``` text
┌─minus(toDate('2018-01-01'), toIntervalYear(3))─┐
│                                     2015-01-01 │
└────────────────────────────────────────────────┘
```


**参照してください**

- [subDate](#subdate)

## timestamp\_add

指定された日時と提供された日付または日時を加算します。

加算の結果がデータ型の範囲外の値になる場合、結果は未定義になります。

**構文**

``` sql
timestamp_add(date, INTERVAL value unit)
```

別名: `timeStampAdd`, `TIMESTAMP_ADD`.

**引数**

- `date` — 日付または日時。[日付](../data-types/date.md)、[Date32](../data-types/date32.md)、[日時](../data-types/datetime.md) または [DateTime64](../data-types/datetime64.md)。
- `value` — 追加する間隔の値。[整数](../data-types/int-uint.md)。
- `unit` — 追加する間隔の種類。[文字列](../data-types/string.md)。
    可能な値:

    - `second`
    - `minute`
    - `hour`
    - `day`
    - `week`
    - `month`
    - `quarter`
    - `year`

**戻り値**

指定された `value` を `unit` で日付に追加した結果の日付または日時。[日付](../data-types/date.md)、[Date32](../data-types/date32.md)、[日時](../data-types/datetime.md) または [DateTime64](../data-types/datetime64.md)。

**例**

```sql
select timestamp_add(toDate('2018-01-01'), INTERVAL 3 MONTH);
```

結果:

```text
┌─plus(toDate('2018-01-01'), toIntervalMonth(3))─┐
│                                     2018-04-01 │
└────────────────────────────────────────────────┘
```

## timestamp\_sub

提供された日付または日時から時間間隔を引き算します。

引き算の結果がデータ型の範囲外の値になる場合、結果は未定義になります。

**構文**

``` sql
timestamp_sub(unit, value, date)
```

別名: `timeStampSub`, `TIMESTAMP_SUB`.

**引数**

- `unit` — 減算する間隔の種類。[文字列](../data-types/string.md)。
    可能な値:

    - `second`
    - `minute`
    - `hour`
    - `day`
    - `week`
    - `month`
    - `quarter`
    - `year`

- `value` — 減算する間隔の値。[整数](../data-types/int-uint.md)。
- `date` — 日付または日時。[日付](../data-types/date.md)、[Date32](../data-types/date32.md)、[日時](../data-types/datetime.md) または [DateTime64](../data-types/datetime64.md)。

**戻り値**

指定された `value` を `unit` で日付から引いた結果の日付または日時。[日付](../data-types/date.md)、[Date32](../data-types/date32.md)、[日時](../data-types/datetime.md) または [DateTime64](../data-types/datetime64.md)。

**例**

```sql
select timestamp_sub(MONTH, 5, toDateTime('2018-12-18 01:02:03'));
```

結果:

```text
┌─minus(toDateTime('2018-12-18 01:02:03'), toIntervalMonth(5))─┐
│                                          2018-07-18 01:02:03 │
└──────────────────────────────────────────────────────────────┘
```

## addDate

指定された日付、日時、または文字列形式の日付/日時に時間間隔を追加します。

加算の結果がデータ型の範囲外の値になる場合、結果は未定義になります。

**構文**

```sql
addDate(date, interval)
```

**引数**

- `date` — 指定された間隔が追加される日付または日時。[日付](../data-types/date.md)、[Date32](../data-types/date32.md)、[日時](../data-types/datetime.md)、[DateTime64](../data-types/datetime64.md)、または [文字列](../data-types/string.md)
- `interval` — 加える間隔。[間隔](../data-types/special-data-types/interval.md)。

**戻り値**

`date` に `interval` を追加した結果の日付または日時。[日付](../data-types/date.md)、[Date32](../data-types/date32.md)、[日時](../data-types/datetime.md) または [DateTime64](../data-types/datetime64.md)。

**例**

```sql
SELECT addDate(toDate('2018-01-01'), INTERVAL 3 YEAR);
```

結果:

```text
┌─addDate(toDate('2018-01-01'), toIntervalYear(3))─┐
│                                       2021-01-01 │
└──────────────────────────────────────────────────┘
```

別名: `ADDDATE`

**参照してください**

- [date_add](#date_add)

## subDate

指定された日付、日時、または文字列形式の日付/日時から時間間隔を引き算します。

減算の結果がデータ型の範囲外の値になる場合、結果は未定義になります。

**構文**

```sql
subDate(date, interval)
```

**引数**

- `date` — `interval` が引かれる日付または日時。[日付](../data-types/date.md)、[Date32](../data-types/date32.md)、[日時](../data-types/datetime.md)、[DateTime64](../data-types/datetime64.md)、または [文字列](../data-types/string.md)
- `interval` — 引く間隔。[間隔](../data-types/special-data-types/interval.md)。

**戻り値**

`date` から `interval` を引いた結果の日付または日時。[日付](../data-types/date.md)、[Date32](../data-types/date32.md)、[日時](../data-types/datetime.md) または [DateTime64](../data-types/datetime64.md)。

**例**

```sql
SELECT subDate(toDate('2018-01-01'), INTERVAL 3 YEAR);
```

結果:

```text
┌─subDate(toDate('2018-01-01'), toIntervalYear(3))─┐
│                                       2015-01-01 │
└──────────────────────────────────────────────────┘
```

別名: `SUBDATE`

**参照してください**

- [date_sub](#date_sub)

## now

クエリ解析の瞬間の現在の日付と時刻を返します。この関数は定数式です。

別名: `current_timestamp`.

**構文**

``` sql
now([timezone])
```

**引数**

- `timezone` — 戻り値の [タイムゾーン名](../../operations/server-configuration-parameters/settings.md#timezone) (オプション)。 [文字列](../data-types/string.md)。

**戻り値**

- 現在の日付と時刻。[日時](../data-types/datetime.md)。

**例**

タイムゾーンなしのクエリ:

``` sql
SELECT now();
```

結果:

``` text
┌───────────────now()─┐
│ 2020-10-17 07:42:09 │
└─────────────────────┘
```

指定されたタイムゾーンでのクエリ:

``` sql
SELECT now('Asia/Istanbul');
```

結果:

``` text
┌─now('Asia/Istanbul')─┐
│  2020-10-17 10:42:23 │
└──────────────────────┘
```

## now64

クエリ解析の瞬間の現在の日付と時刻をサブ秒精度で返します。この関数は定数式です。

**構文**

``` sql
now64([scale], [timezone])
```

**引数**

- `scale` - チックサイズ（精度）：10<sup>-精度</sup> 秒。妥当な範囲: [0 : 9]。典型的には - 3 (デフォルト) (ミリ秒)、6 (マイクロ秒)、9 (ナノ秒) が使用されます。
- `timezone` — 戻り値の [タイムゾーン名](../../operations/server-configuration-parameters/settings.md#timezone) (オプション)。 [文字列](../data-types/string.md)。

**戻り値**

- サブ秒精度での現在の日付と時刻。[DateTime64](../data-types/datetime64.md)。

**例**

``` sql
SELECT now64(), now64(9, 'Asia/Istanbul');
```

結果:

``` text
┌─────────────────now64()─┬─────now64(9, 'Asia/Istanbul')─┐
│ 2022-08-21 19:34:26.196 │ 2022-08-21 22:34:26.196542766 │
└─────────────────────────┴───────────────────────────────┘
```

## nowInBlock {#nowInBlock}

データの各ブロックを処理する瞬間の現在の日付と時刻を返します。[now](#now) の関数とは異なり、これは定数式ではなく、長時間実行されるクエリの場合、異なるブロックで返される値が異なります。

長時間実行される INSERT SELECT クエリで現在の時刻を生成するためにこの関数を使用することが意味があります。

**構文**

``` sql
nowInBlock([timezone])
```

**引数**

- `timezone` — 戻り値の [タイムゾーン名](../../operations/server-configuration-parameters/settings.md#timezone) (オプション)。 [文字列](../data-types/string.md)。

**戻り値**

- 各データブロックの処理の瞬間の現在の日付と時刻。[日時](../data-types/datetime.md)。

**例**

``` sql
SELECT
    now(),
    nowInBlock(),
    sleep(1)
FROM numbers(3)
SETTINGS max_block_size = 1
FORMAT PrettyCompactMonoBlock
```

結果:

``` text
┌───────────────now()─┬────────nowInBlock()─┬─sleep(1)─┐
│ 2022-08-21 19:41:19 │ 2022-08-21 19:41:19 │        0 │
│ 2022-08-21 19:41:19 │ 2022-08-21 19:41:20 │        0 │
│ 2022-08-21 19:41:19 │ 2022-08-21 19:41:21 │        0 │
└─────────────────────┴─────────────────────┴──────────┘
```

## today {#today}

クエリ解析の瞬間の現在の日付を返します。これは `toDate(now())` と同じであり、エイリアスとして `curdate`, `current_date` があります。

**構文**

```sql
today()
```

**引数**

- なし

**戻り値**

- 現在の日付。[日付](../data-types/date.md)。

**例**

クエリ:

```sql
SELECT today() AS today, curdate() AS curdate, current_date() AS current_date FORMAT Pretty
```

**結果**:

2024年3月3日に上記のクエリを実行すると、次の応答が返されます。

```response
┏━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━━━┓
┃      today ┃    curdate ┃ current_date ┃
┡━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━━━┩
│ 2024-03-03 │ 2024-03-03 │   2024-03-03 │
└────────────┴────────────┴──────────────┘
```

## yesterday {#yesterday}

引数を受け付けず、クエリ解析のいずれかの瞬間で昨日の日付を返します。
`today() - 1` と同じです。

## timeSlot

時間を30分間隔の開始時刻に丸めます。

**構文**

```sql
timeSlot(time[, time_zone])
```

**引数**

- `time` — 30分間隔の開始時刻に丸める時間。[日時](../data-types/datetime.md)/[Date32](../data-types/date32.md)/[DateTime64](../data-types/datetime64.md)。
- `time_zone` — タイムゾーンを表す文字列型の定数値または式。[文字列](../data-types/string.md)。

:::note
この関数は拡張されたタイプ `Date32` と `DateTime64` の値を引数として受け取ることができますが、通常の範囲（年 1970 から 2149 までの `Date` / 2106 までの `DateTime`）を超える時間を渡すと、誤った結果が生成されます。
:::

**戻り値の型**

- 30分間隔の開始時刻に丸められた時間を返します。[日時](../data-types/datetime.md)。

**例**

クエリ:

```sql
SELECT timeSlot(toDateTime('2000-01-02 03:04:05', 'UTC'));
```

結果:

```response
┌─timeSlot(toDateTime('2000-01-02 03:04:05', 'UTC'))─┐
│                                2000-01-02 03:00:00 │
└────────────────────────────────────────────────────┘
```

## toYYYYMM

日付または日付と時刻を年と月の数（YYYY \* 100 + MM）を含む UInt32 数に変換します。第二のオプションのタイムゾーン引数を受け付けます。提供された場合、タイムゾーンは文字列定数でなければなりません。

この関数は関数 `YYYYMMDDToDate()` の逆です。

**例**

``` sql
SELECT
    toYYYYMM(now(), 'US/Eastern')
```

結果:

``` text
┌─toYYYYMM(now(), 'US/Eastern')─┐
│                        202303 │
└───────────────────────────────┘
```

## toYYYYMMDD

日付または日付と時刻を年、月、日を含む UInt32 数（YYYY \* 10000 + MM \* 100 + DD）に変換します。第二のオプションのタイムゾーン引数を受け付けます。提供された場合、タイムゾーンは文字列定数でなければなりません。

**例**

```sql
SELECT toYYYYMMDD(now(), 'US/Eastern')
```

結果:

```response
┌─toYYYYMMDD(now(), 'US/Eastern')─┐
│                        20230302 │
└─────────────────────────────────┘
```

## toYYYYMMDDhhmmss

日付または日付と時刻を年、月、日、時、分、秒を含む UInt64 数（YYYY \* 10000000000 + MM \* 100000000 + DD \* 1000000 + hh \* 10000 + mm \* 100 + ss）に変換します。第二のオプションのタイムゾーン引数を受け付けます。提供された場合、タイムゾーンは文字列定数でなければなりません。

**例**

```sql
SELECT toYYYYMMDDhhmmss(now(), 'US/Eastern')
```

結果:

```response
┌─toYYYYMMDDhhmmss(now(), 'US/Eastern')─┐
│                        20230302112209 │
└───────────────────────────────────────┘
```

## YYYYMMDDToDate

年、月、日を含む数値を [日付](../data-types/date.md) に変換します。

この関数は関数 `toYYYYMMDD()` の逆です。

入力が有効な日付値をエンコードしない場合、出力は未定義です。

**構文**

```sql
YYYYMMDDToDate(yyyymmdd);
```

**引数**

- `yyyymmdd` - 年、月、および日を表す数値。[整数](../data-types/int-uint.md)、[浮動小数点](../data-types/float.md)または [小数](../data-types/decimal.md)。

**戻り値**

- 引数から作成された日付。[日付](../data-types/date.md)。

**例**

```sql
SELECT YYYYMMDDToDate(20230911);
```

結果:

```response
┌─toYYYYMMDD(20230911)─┐
│           2023-09-11 │
└──────────────────────┘
```

## YYYYMMDDToDate32

関数 `YYYYMMDDToDate()` と同様ですが、[Date32](../data-types/date32.md) を生成します。

## YYYYMMDDhhmmssToDateTime

年、月、日、時、分、秒を含む数値を [日時](../data-types/datetime.md) に変換します。

入力が有効な日時値をエンコードしない場合、出力は未定義です。

この関数は関数 `toYYYYMMDDhhmmss()` の逆です。

**構文**

```sql
YYYYMMDDhhmmssToDateTime(yyyymmddhhmmss[, timezone]);
```

**引数**

- `yyyymmddhhmmss` - 年、月、日を表す数値。[整数](../data-types/int-uint.md)、[浮動小数点](../data-types/float.md)または [小数](../data-types/decimal.md)。
- `timezone` - 戻り値の [タイムゾーン](../../operations/server-configuration-parameters/settings.md#timezone) (オプション)。

**戻り値**

- 引数から作成された日時。[日時](../data-types/datetime.md)。

**例**

```sql
SELECT YYYYMMDDToDateTime(20230911131415);
```

結果:

```response
┌──────YYYYMMDDhhmmssToDateTime(20230911131415)─┐
│                           2023-09-11 13:14:15 │
└───────────────────────────────────────────────┘
```

## YYYYMMDDhhmmssToDateTime64

関数 `YYYYMMDDhhmmssToDate()` と同様ですが、[DateTime64](../data-types/datetime64.md) を生成します。

必要に応じて `timezone` パラメータの後に追加のオプションの `precision` パラメータを受け取ります。

## changeYear

日付または日時の年の部分を変更します。

**構文**
``` sql

changeYear(date_or_datetime, value)
```

**引数**

- `date_or_datetime` - [日付](../data-types/date.md)、[Date32](../data-types/date32.md)、[日時](../data-types/datetime.md) または [DateTime64](../data-types/datetime64.md)
- `value` - 年の新しい値。[整数](../../sql-reference/data-types/int-uint.md)。

**戻り値**

- `date_or_datetime` と同じ型。

**例**

``` sql
SELECT changeYear(toDate('1999-01-01'), 2000), changeYear(toDateTime64('1999-01-01 00:00:00.000', 3), 2000);
```

結果:

```
┌─changeYear(toDate('1999-01-01'), 2000)─┬─changeYear(toDateTime64('1999-01-01 00:00:00.000', 3), 2000)─┐
│                             2000-01-01 │                                      2000-01-01 00:00:00.000 │
└────────────────────────────────────────┴──────────────────────────────────────────────────────────────┘
```

## changeMonth

日付または日時の月の部分を変更します。

**構文**

``` sql
changeMonth(date_or_datetime, value)
```

**引数**

- `date_or_datetime` - [日付](../data-types/date.md)、[Date32](../data-types/date32.md)、[日時](../data-types/datetime.md) または [DateTime64](../data-types/datetime64.md)
- `value` - 月の新しい値。[整数](../../sql-reference/data-types/int-uint.md)。

**戻り値**

- `date_or_datetime` と同じ型の値を返します。

**例**

``` sql
SELECT changeMonth(toDate('1999-01-01'), 2), changeMonth(toDateTime64('1999-01-01 00:00:00.000', 3), 2);
```

結果:

```
┌─changeMonth(toDate('1999-01-01'), 2)─┬─changeMonth(toDateTime64('1999-01-01 00:00:00.000', 3), 2)─┐
│                           1999-02-01 │                                    1999-02-01 00:00:00.000 │
└──────────────────────────────────────┴────────────────────────────────────────────────────────────┘
```

## changeDay

日付または日時の日の部分を変更します。

**構文**

``` sql
changeDay(date_or_datetime, value)
```

**引数**

- `date_or_datetime` - [日付](../data-types/date.md)、[Date32](../data-types/date32.md)、[日時](../data-types/datetime.md) または [DateTime64](../data-types/datetime64.md)
- `value` - 日の新しい値。[整数](../../sql-reference/data-types/int-uint.md)。

**戻り値**

- `date_or_datetime` と同じ型の値を返します。

**例**

``` sql
SELECT changeDay(toDate('1999-01-01'), 5), changeDay(toDateTime64('1999-01-01 00:00:00.000', 3), 5);
```

結果:

```
┌─changeDay(toDate('1999-01-01'), 5)─┬─changeDay(toDateTime64('1999-01-01 00:00:00.000', 3), 5)─┐
│                         1999-01-05 │                                  1999-01-05 00:00:00.000 │
└────────────────────────────────────┴──────────────────────────────────────────────────────────┘
```

## changeHour

日付または日時の時間の部分を変更します。

**構文**

``` sql
changeHour(date_or_datetime, value)
```

**引数**

- `date_or_datetime` - [日付](../data-types/date.md)、[Date32](../data-types/date32.md)、[日時](../data-types/datetime.md) または [DateTime64](../data-types/datetime64.md)
- `value` - 時間の新しい値。[整数](../../sql-reference/data-types/int-uint.md)。

**戻り値**

- `date_or_datetime` と同じ型の値を返します。入力が [日付](../data-types/date.md) の場合、[日時](../data-types/datetime.md) が返されます。入力が [Date32](../data-types/date32.md) の場合、[DateTime64](../data-types/datetime64.md) が返されます。

**例**

``` sql
SELECT changeHour(toDate('1999-01-01'), 14), changeHour(toDateTime64('1999-01-01 00:00:00.000', 3), 14);
```

結果:

```
┌─changeHour(toDate('1999-01-01'), 14)─┬─changeHour(toDateTime64('1999-01-01 00:00:00.000', 3), 14)─┐
│                  1999-01-01 14:00:00 │                                    1999-01-01 14:00:00.000 │
└──────────────────────────────────────┴────────────────────────────────────────────────────────────┘
```

## changeMinute

日付または日時の分の部分を変更します。

**構文**

``` sql
changeMinute(date_or_datetime, value)
```

**引数**

- `date_or_datetime` - [日付](../data-types/date.md)、[Date32](../data-types/date32.md)、[日時](../data-types/datetime.md) または [DateTime64](../data-types/datetime64.md)
- `value` - 分の新しい値。[整数](../../sql-reference/data-types/int-uint.md)。

**戻り値**

- `date_or_datetime` と同じ型の値を返します。入力が [日付](../data-types/date.md) の場合、[日時](../data-types/datetime.md) が返されます。入力が [Date32](../data-types/date32.md) の場合、[DateTime64](../data-types/datetime64.md) が返されます。

**例**

``` sql
    SELECT changeMinute(toDate('1999-01-01'), 15), changeMinute(toDateTime64('1999-01-01 00:00:00.000', 3), 15);
```

結果:

```
┌─changeMinute(toDate('1999-01-01'), 15)─┬─changeMinute(toDateTime64('1999-01-01 00:00:00.000', 3), 15)─┐
│                    1999-01-01 00:15:00 │                                      1999-01-01 00:15:00.000 │
└────────────────────────────────────────┴──────────────────────────────────────────────────────────────┘
```

## changeSecond

日付または日時の秒の部分を変更します。

**構文**

``` sql
changeSecond(date_or_datetime, value)
```

**引数**

- `date_or_datetime` - [日付](../data-types/date.md)、[Date32](../data-types/date32.md)、[日時](../data-types/datetime.md) または [DateTime64](../data-types/datetime64.md)
- `value` - 秒の新しい値。[整数](../../sql-reference/data-types/int-uint.md)。

**戻り値**

- `date_or_datetime` と同じ型の値を返します。入力が [日付](../data-types/date.md) の場合、[日時](../data-types/datetime.md) が返されます。入力が [Date32](../data-types/date32.md) の場合、[DateTime64](../data-types/datetime64.md) が返されます。

**例**

``` sql
SELECT changeSecond(toDate('1999-01-01'), 15), changeSecond(toDateTime64('1999-01-01 00:00:00.000', 3), 15);
```

結果:

```
┌─changeSecond(toDate('1999-01-01'), 15)─┬─changeSecond(toDateTime64('1999-01-01 00:00:00.000', 3), 15)─┐
│                    1999-01-01 00:00:15 │                                      1999-01-01 00:00:15.000 │
└────────────────────────────────────────┴──────────────────────────────────────────────────────────────┘
```

## addYears

日付、日時、または文字列形式の日付/日時に指定された年の数を追加します。

**構文**

```sql
addYears(date, num)
```

**引数**

- `date`: 指定された年の数を追加する日付/日時。[日付](../data-types/date.md) / [Date32](../data-types/date32.md) / [日時](../data-types/datetime.md) / [DateTime64](../data-types/datetime64.md)、[文字列](../data-types/string.md)。
- `num`: 追加する年の数。[(U)Int*](../data-types/int-uint.md)、[Float*](../data-types/float.md)。

**戻り値**

- `date` に `num` 年を加えます。 [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md)。

**例**

```sql
WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    addYears(date, 1) AS add_years_with_date,
    addYears(date_time, 1) AS add_years_with_date_time,
    addYears(date_time_string, 1) AS add_years_with_date_time_string
```

```response
┌─add_years_with_date─┬─add_years_with_date_time─┬─add_years_with_date_time_string─┐
│          2025-01-01 │      2025-01-01 00:00:00 │         2025-01-01 00:00:00.000 │
└─────────────────────┴──────────────────────────┴─────────────────────────────────┘
```

## addQuarters

指定された数の四半期を日付、時刻付きの日付、または文字列エンコードされた日付 / 時刻付きの日付に加えます。

**構文**

```sql
addQuarters(date, num)
```

**パラメーター**

- `date`: 指定された数の四半期を加える日付 / 時刻付き日付。 [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md)、[String](../data-types/string.md)。
- `num`: 加える四半期の数。 [(U)Int*](../data-types/int-uint.md)、[Float*](../data-types/float.md)。

**返却値**

- `date` に `num` 四半期を加えます。 [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md)。

**例**

```sql
WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    addQuarters(date, 1) AS add_quarters_with_date,
    addQuarters(date_time, 1) AS add_quarters_with_date_time,
    addQuarters(date_time_string, 1) AS add_quarters_with_date_time_string
```

```response
┌─add_quarters_with_date─┬─add_quarters_with_date_time─┬─add_quarters_with_date_time_string─┐
│             2024-04-01 │         2024-04-01 00:00:00 │            2024-04-01 00:00:00.000 │
└────────────────────────┴─────────────────────────────┴────────────────────────────────────┘
```

## addMonths

指定された数の月を日付、時刻付きの日付、または文字列エンコードされた日付 / 時刻付きの日付に加えます。

**構文**

```sql
addMonths(date, num)
```

**パラメーター**

- `date`: 指定された数の月を加える日付 / 時刻付き日付。 [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md)、[String](../data-types/string.md)。
- `num`: 加える月の数。 [(U)Int*](../data-types/int-uint.md)、[Float*](../data-types/float.md)。

**返却値**

- `date` に `num` 月を加えます。 [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md)。

**例**

```sql
WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    addMonths(date, 6) AS add_months_with_date,
    addMonths(date_time, 6) AS add_months_with_date_time,
    addMonths(date_time_string, 6) AS add_months_with_date_time_string
```

```response
┌─add_months_with_date─┬─add_months_with_date_time─┬─add_months_with_date_time_string─┐
│           2024-07-01 │       2024-07-01 00:00:00 │          2024-07-01 00:00:00.000 │
└──────────────────────┴───────────────────────────┴──────────────────────────────────┘
```

## addWeeks

指定された数の週を日付、時刻付きの日付、または文字列エンコードされた日付 / 時刻付きの日付に加えます。

**構文**

```sql
addWeeks(date, num)
```

**パラメーター**

- `date`: 指定された数の週を加える日付 / 時刻付き日付。 [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md)、[String](../data-types/string.md)。
- `num`: 加える週の数。 [(U)Int*](../data-types/int-uint.md)、[Float*](../data-types/float.md)。

**返却値**

- `date` に `num` 週間を加えます。 [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md)。

**例**

```sql
WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    addWeeks(date, 5) AS add_weeks_with_date,
    addWeeks(date_time, 5) AS add_weeks_with_date_time,
    addWeeks(date_time_string, 5) AS add_weeks_with_date_time_string
```

```response
┌─add_weeks_with_date─┬─add_weeks_with_date_time─┬─add_weeks_with_date_time_string─┐
│          2024-02-05 │      2024-02-05 00:00:00 │         2024-02-05 00:00:00.000 │
└─────────────────────┴──────────────────────────┴─────────────────────────────────┘
```

## addDays

指定された数の日を日付、時刻付きの日付、または文字列エンコードされた日付 / 時刻付きの日付に加えます。

**構文**

```sql
addDays(date, num)
```

**パラメーター**

- `date`: 指定された数の日を加える日付 / 時刻付き日付。 [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md)、[String](../data-types/string.md)。
- `num`: 加える日の数。 [(U)Int*](../data-types/int-uint.md)、[Float*](../data-types/float.md)。

**返却値**

- `date` に `num` 日を加えます。 [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md)。

**例**

```sql
WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    addDays(date, 5) AS add_days_with_date,
    addDays(date_time, 5) AS add_days_with_date_time,
    addDays(date_time_string, 5) AS add_days_with_date_time_string
```

```response
┌─add_days_with_date─┬─add_days_with_date_time─┬─add_days_with_date_time_string─┐
│         2024-01-06 │     2024-01-06 00:00:00 │        2024-01-06 00:00:00.000 │
└────────────────────┴─────────────────────────┴────────────────────────────────┘
```

## addHours

指定された数の時間を日付、時刻付きの日付、または文字列エンコードされた日付 / 時刻付きの日付に加えます。

**構文**

```sql
addHours(date, num)
```

**パラメーター**

- `date`: 指定された数の時間を加える日付 / 時刻付き日付。 [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md)、[String](../data-types/string.md)。
- `num`: 加える時間の数。 [(U)Int*](../data-types/int-uint.md)、[Float*](../data-types/float.md)。

**返却値**

- `date` に `num` 時間を加えます。 [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md)。

**例**

```sql
WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    addHours(date, 12) AS add_hours_with_date,
    addHours(date_time, 12) AS add_hours_with_date_time,
    addHours(date_time_string, 12) AS add_hours_with_date_time_string
```

```response
┌─add_hours_with_date─┬─add_hours_with_date_time─┬─add_hours_with_date_time_string─┐
│ 2024-01-01 12:00:00 │      2024-01-01 12:00:00 │         2024-01-01 12:00:00.000 │
└─────────────────────┴──────────────────────────┴─────────────────────────────────┘
```

## addMinutes

指定された数の分を日付、時刻付きの日付、または文字列エンコードされた日付 / 時刻付きの日付に加えます。

**構文**

```sql
addMinutes(date, num)
```

**パラメーター**

- `date`: 指定された数の分を加える日付 / 時刻付き日付。 [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md)、[String](../data-types/string.md)。
- `num`: 加える分の数。 [(U)Int*](../data-types/int-uint.md)、[Float*](../data-types/float.md)。

**返却値**

- `date` に `num` 分を加えます。 [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md)。

**例**

```sql
WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    addMinutes(date, 20) AS add_minutes_with_date,
    addMinutes(date_time, 20) AS add_minutes_with_date_time,
    addMinutes(date_time_string, 20) AS add_minutes_with_date_time_string
```

```response
┌─add_minutes_with_date─┬─add_minutes_with_date_time─┬─add_minutes_with_date_time_string─┐
│   2024-01-01 00:20:00 │        2024-01-01 00:20:00 │           2024-01-01 00:20:00.000 │
└───────────────────────┴────────────────────────────┴───────────────────────────────────┘
```

## addSeconds

指定された数の秒を日付、時刻付きの日付、または文字列エンコードされた日付 / 時刻付きの日付に加えます。

**構文**

```sql
addSeconds(date, num)
```

**パラメーター**

- `date`: 指定された数の秒を加える日付 / 時刻付き日付。 [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md)、[String](../data-types/string.md)。
- `num`: 加える秒の数。 [(U)Int*](../data-types/int-uint.md)、[Float*](../data-types/float.md)。

**返却値**

- `date` に `num` 秒を加えます。 [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md)。

**例**

```sql
WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    addSeconds(date, 30) AS add_seconds_with_date,
    addSeconds(date_time, 30) AS add_seconds_with_date_time,
    addSeconds(date_time_string, 30) AS add_seconds_with_date_time_string
```

```response
┌─add_seconds_with_date─┬─add_seconds_with_date_time─┬─add_seconds_with_date_time_string─┐
│   2024-01-01 00:00:30 │        2024-01-01 00:00:30 │           2024-01-01 00:00:30.000 │
└───────────────────────┴────────────────────────────┴───────────────────────────────────┘
```

## addMilliseconds

指定された数のミリ秒を時刻付きの日付または文字列エンコードされた時刻付きの日付に加えます。

**構文**

```sql
addMilliseconds(date_time, num)
```

**パラメーター**

- `date_time`: 指定された数のミリ秒を加える時刻付き日付。 [DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md)、[String](../data-types/string.md)。
- `num`: 加えるミリ秒の数。 [(U)Int*](../data-types/int-uint.md)、[Float*](../data-types/float.md)。

**返却値**

- `date_time` に `num` ミリ秒を加えます。 [DateTime64](../data-types/datetime64.md)。

**例**

```sql
WITH
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    addMilliseconds(date_time, 1000) AS add_milliseconds_with_date_time,
    addMilliseconds(date_time_string, 1000) AS add_milliseconds_with_date_time_string
```

```response
┌─add_milliseconds_with_date_time─┬─add_milliseconds_with_date_time_string─┐
│         2024-01-01 00:00:01.000 │                2024-01-01 00:00:01.000 │
└─────────────────────────────────┴────────────────────────────────────────┘
```

## addMicroseconds

指定された数のマイクロ秒を時刻付きの日付または文字列エンコードされた時刻付きの日付に加えます。

**構文**

```sql
addMicroseconds(date_time, num)
```

**パラメーター**

- `date_time`: 指定された数のマイクロ秒を加える時刻付き日付。 [DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md)、[String](../data-types/string.md)。
- `num`: 加えるマイクロ秒の数。 [(U)Int*](../data-types/int-uint.md)、[Float*](../data-types/float.md)。

**返却値**

- `date_time` に `num` マイクロ秒を加えます。 [DateTime64](../data-types/datetime64.md)。

**例**

```sql
WITH
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    addMicroseconds(date_time, 1000000) AS add_microseconds_with_date_time,
    addMicroseconds(date_time_string, 1000000) AS add_microseconds_with_date_time_string
```

```response
┌─add_microseconds_with_date_time─┬─add_microseconds_with_date_time_string─┐
│      2024-01-01 00:00:01.000000 │             2024-01-01 00:00:01.000000 │
└─────────────────────────────────┴────────────────────────────────────────┘
```

## addNanoseconds

指定された数のナノ秒を時刻付きの日付または文字列エンコードされた時刻付きの日付に加えます。

**構文**

```sql
addNanoseconds(date_time, num)
```

**パラメーター**

- `date_time`: 指定された数のナノ秒を加える時刻付き日付。 [DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md)、[String](../data-types/string.md)。
- `num`: 加えるナノ秒の数。 [(U)Int*](../data-types/int-uint.md)、[Float*](../data-types/float.md)。

**返却値**

- `date_time` に `num` ナノ秒を加えます。 [DateTime64](../data-types/datetime64.md)。

**例**

```sql
WITH
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    addNanoseconds(date_time, 1000) AS add_nanoseconds_with_date_time,
    addNanoseconds(date_time_string, 1000) AS add_nanoseconds_with_date_time_string
```

```response
┌─add_nanoseconds_with_date_time─┬─add_nanoseconds_with_date_time_string─┐
│  2024-01-01 00:00:00.000001000 │         2024-01-01 00:00:00.000001000 │
└────────────────────────────────┴───────────────────────────────────────┘
```

## addInterval

別の間隔または間隔のタプルを追加します。

**構文**

```sql
addInterval(interval_1, interval_2)
```

**パラメーター**

- `interval_1`: 最初の間隔または間隔のタプル。 [interval](../data-types/special-data-types/interval.md)、[tuple](../data-types/tuple.md)([interval](../data-types/special-data-types/interval.md))。
- `interval_2`: 追加される2番目の間隔。 [interval](../data-types/special-data-types/interval.md)。

**返却値**

- 間隔のタプルを返します。 [tuple](../data-types/tuple.md)([interval](../data-types/special-data-types/interval.md))。

:::note
同じタイプの間隔は、単一の間隔にまとめられます。たとえば、`toIntervalDay(1)` と `toIntervalDay(2)` が渡されると、結果は `(3)` になります。
:::

**例**

クエリ：

```sql
SELECT addInterval(INTERVAL 1 DAY, INTERVAL 1 MONTH);
SELECT addInterval((INTERVAL 1 DAY, INTERVAL 1 YEAR), INTERVAL 1 MONTH);
SELECT addInterval(INTERVAL 2 DAY, INTERVAL 1 DAY);
```

結果：

```response
┌─addInterval(toIntervalDay(1), toIntervalMonth(1))─┐
│ (1,1)                                             │
└───────────────────────────────────────────────────┘
┌─addInterval((toIntervalDay(1), toIntervalYear(1)), toIntervalMonth(1))─┐
│ (1,1,1)                                                                │
└────────────────────────────────────────────────────────────────────────┘
┌─addInterval(toIntervalDay(2), toIntervalDay(1))─┐
│ (3)                                             │
└─────────────────────────────────────────────────┘
```

## addTupleOfIntervals

間隔のタプルを連続して日付または日時に加えます。

**構文**

```sql
addTupleOfIntervals(interval_1, interval_2)
```

**パラメーター**

- `date`: 最初の間隔または間隔のタプル。 [date](../data-types/date.md)/[date32](../data-types/date32.md)/[datetime](../data-types/datetime.md)/[datetime64](../data-types/datetime64.md)。
- `intervals`: `date` に加える間隔のタプル。 [tuple](../data-types/tuple.md)([interval](../data-types/special-data-types/interval.md))。

**返却値**

- `intervals` を加えた `date` を返します。 [date](../data-types/date.md)/[date32](../data-types/date32.md)/[datetime](../data-types/datetime.md)/[datetime64](../data-types/datetime64.md)。

**例**

クエリ：

```sql
WITH toDate('2018-01-01') AS date
SELECT addTupleOfIntervals(date, (INTERVAL 1 DAY, INTERVAL 1 MONTH, INTERVAL 1 YEAR))
```

結果：

```response
┌─addTupleOfIntervals(date, (toIntervalDay(1), toIntervalMonth(1), toIntervalYear(1)))─┐
│                                                                           2019-02-02 │
└──────────────────────────────────────────────────────────────────────────────────────┘
```

## subtractYears

指定された数の年を日付、時刻付きの日付、または文字列エンコードされた日付 / 時刻付きの日付から減算します。

**構文**

```sql
subtractYears(date, num)
```

**パラメーター**

- `date`: 指定された数の年を減算する日付 / 時刻付き日付。 [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md)、[String](../data-types/string.md)。
- `num`: 減算する年の数。 [(U)Int*](../data-types/int-uint.md)、[Float*](../data-types/float.md)。

**返却値**

- `date` から `num` 年を減算します。 [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md)。

**例**

```sql
WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    subtractYears(date, 1) AS subtract_years_with_date,
    subtractYears(date_time, 1) AS subtract_years_with_date_time,
    subtractYears(date_time_string, 1) AS subtract_years_with_date_time_string
```

```response
┌─subtract_years_with_date─┬─subtract_years_with_date_time─┬─subtract_years_with_date_time_string─┐
│               2023-01-01 │           2023-01-01 00:00:00 │              2023-01-01 00:00:00.000 │
└──────────────────────────┴───────────────────────────────┴──────────────────────────────────────┘
```

## subtractQuarters

指定された数の四半期を日付、時刻付きの日付、または文字列エンコードされた日付 / 時刻付きの日付から減算します。

**構文**

```sql
subtractQuarters(date, num)
```

**パラメーター**

- `date`: 指定された数の四半期を減算する日付 / 時刻付き日付。 [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md)、[String](../data-types/string.md)。
- `num`: 減算する四半期の数。 [(U)Int*](../data-types/int-uint.md)、[Float*](../data-types/float.md)。

**返却値**

- `date` から `num` 四半期を減算します。 [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md)。

**例**

```sql
WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    subtractQuarters(date, 1) AS subtract_quarters_with_date,
    subtractQuarters(date_time, 1) AS subtract_quarters_with_date_time,
    subtractQuarters(date_time_string, 1) AS subtract_quarters_with_date_time_string
```

```response
┌─subtract_quarters_with_date─┬─subtract_quarters_with_date_time─┬─subtract_quarters_with_date_time_string─┐
│                  2023-10-01 │              2023-10-01 00:00:00 │                 2023-10-01 00:00:00.000 │
└─────────────────────────────┴──────────────────────────────────┴─────────────────────────────────────────┘
```

## subtractMonths

指定された数の月を日付、時刻付きの日付、または文字列エンコードされた日付 / 時刻付きの日付から減算します。

**構文**

```sql
subtractMonths(date, num)
```

**パラメーター**

- `date`: 指定された数の月を減算する日付 / 時刻付き日付。 [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md)、[String](../data-types/string.md)。
- `num`: 減算する月の数。 [(U)Int*](../data-types/int-uint.md)、[Float*](../data-types/float.md)。

**返却値**

- `date` から `num` 月を減算します。 [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md)。

**例**

```sql
WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    subtractMonths(date, 1) AS subtract_months_with_date,
    subtractMonths(date_time, 1) AS subtract_months_with_date_time,
    subtractMonths(date_time_string, 1) AS subtract_months_with_date_time_string
```

```response
┌─subtract_months_with_date─┬─subtract_months_with_date_time─┬─subtract_months_with_date_time_string─┐
│                2023-12-01 │            2023-12-01 00:00:00 │               2023-12-01 00:00:00.000 │
└───────────────────────────┴────────────────────────────────┴───────────────────────────────────────┘
```

## subtractWeeks

指定された数の週を日付、時刻付きの日付、または文字列エンコードされた日付 / 時刻付きの日付から減算します。

**構文**

```sql
subtractWeeks(date, num)
```

**パラメーター**

- `date`: 指定された数の週を減算する日付 / 時刻付き日付。 [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md)、[String](../data-types/string.md)。
- `num`: 減算する週の数。 [(U)Int*](../data-types/int-uint.md)、[Float*](../data-types/float.md)。

**返却値**

- `date` から `num` 週間を減算します。 [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md)。

**例**

```sql
WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    subtractWeeks(date, 1) AS subtract_weeks_with_date,
    subtractWeeks(date_time, 1) AS subtract_weeks_with_date_time,
    subtractWeeks(date_time_string, 1) AS subtract_weeks_with_date_time_string
```

```response
 ┌─subtract_weeks_with_date─┬─subtract_weeks_with_date_time─┬─subtract_weeks_with_date_time_string─┐
 │               2023-12-25 │           2023-12-25 00:00:00 │              2023-12-25 00:00:00.000 │
 └──────────────────────────┴───────────────────────────────┴──────────────────────────────────────┘
```

## subtractDays

指定された数の日を日付、時刻付きの日付、または文字列エンコードされた日付 / 時刻付きの日付から減算します。

**構文**

```sql
subtractDays(date, num)
```

**パラメーター**

- `date`: 指定された数の日を減算する日付 / 時刻付き日付。 [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md)、[String](../data-types/string.md)。
- `num`: 減算する日の数。 [(U)Int*](../data-types/int-uint.md)、[Float*](../data-types/float.md)。

**返却値**

- `date` から `num` 日を減算します。 [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md)。

**例**

```sql
WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    subtractDays(date, 31) AS subtract_days_with_date,
    subtractDays(date_time, 31) AS subtract_days_with_date_time,
    subtractDays(date_time_string, 31) AS subtract_days_with_date_time_string
```

```response
┌─subtract_days_with_date─┬─subtract_days_with_date_time─┬─subtract_days_with_date_time_string─┐
│              2023-12-01 │          2023-12-01 00:00:00 │             2023-12-01 00:00:00.000 │
└─────────────────────────┴──────────────────────────────┴─────────────────────────────────────┘
```

## subtractHours

指定された数の時間を日付、時刻付きの日付、または文字列エンコードされた日付 / 時刻付きの日付から減算します。

**構文**

```sql
subtractHours(date, num)
```

**パラメーター**

- `date`: 指定された数の時間を減算する日付 / 時刻付き日付。 [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[Datetime](../data-types/datetime.md)/[Datetime64](../data-types/datetime64.md)、[String](../data-types/string.md)。
- `num`: 減算する時間の数。 [(U)Int*](../data-types/int-uint.md)、[Float*](../data-types/float.md)。

**返却値**

- `date` から `num` 時間を減算します。 [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[Datetime](../data-types/datetime.md)/[Datetime64](../data-types/datetime64.md)。

**例**

```sql
WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    subtractHours(date, 12) AS subtract_hours_with_date,
    subtractHours(date_time, 12) AS subtract_hours_with_date_time,
    subtractHours(date_time_string, 12) AS subtract_hours_with_date_time_string
```

```response
┌─subtract_hours_with_date─┬─subtract_hours_with_date_time─┬─subtract_hours_with_date_time_string─┐
│      2023-12-31 12:00:00 │           2023-12-31 12:00:00 │              2023-12-31 12:00:00.000 │
└──────────────────────────┴───────────────────────────────┴──────────────────────────────────────┘
```

## subtractMinutes

指定された数の分を日付、時刻付きの日付、または文字列エンコードされた日付 / 時刻付きの日付から減算します。

**構文**

```sql
subtractMinutes(date, num)
```

**パラメーター**

- `date`: 指定された数の分を減算する日付 / 時刻付き日付。 [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md)、[String](../data-types/string.md)。
- `num`: 減算する分の数。 [(U)Int*](../data-types/int-uint.md)、[Float*](../data-types/float.md)。

**返却値**

- `date` から `num` 分を減算します。 [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md)。

**例**

```sql
WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    subtractMinutes(date, 30) AS subtract_minutes_with_date,
    subtractMinutes(date_time, 30) AS subtract_minutes_with_date_time,
    subtractMinutes(date_time_string, 30) AS subtract_minutes_with_date_time_string
```

```response
┌─subtract_minutes_with_date─┬─subtract_minutes_with_date_time─┬─subtract_minutes_with_date_time_string─┐
│        2023-12-31 23:30:00 │             2023-12-31 23:30:00 │                2023-12-31 23:30:00.000 │
└────────────────────────────┴─────────────────────────────────┴────────────────────────────────────────┘
```

## subtractSeconds

指定された数の秒を日付、時刻付きの日付、または文字列エンコードされた日付 / 時刻付きの日付から減算します。

**構文**

```sql
subtractSeconds(date, num)
```

**パラメーター**

- `date`: 指定された数の秒を減算する日付 / 時刻付き日付。 [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md)、[String](../data-types/string.md)。
- `num`: 減算する秒の数。 [(U)Int*](../data-types/int-uint.md)、[Float*](../data-types/float.md)。

**返却値**

- `date` から `num` 秒を減算します。 [Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md)。

**例**

```sql
WITH
    toDate('2024-01-01') AS date,
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    subtractSeconds(date, 60) AS subtract_seconds_with_date,
    subtractSeconds(date_time, 60) AS subtract_seconds_with_date_time,
    subtractSeconds(date_time_string, 60) AS subtract_seconds_with_date_time_string
```

```response
┌─subtract_seconds_with_date─┬─subtract_seconds_with_date_time─┬─subtract_seconds_with_date_time_string─┐
│        2023-12-31 23:59:00 │             2023-12-31 23:59:00 │                2023-12-31 23:59:00.000 │
└────────────────────────────┴─────────────────────────────────┴────────────────────────────────────────┘
```

## subtractMilliseconds

指定された数のミリ秒を時刻付きの日付または文字列エンコードされた時刻付きの日付から減算します。

**構文**

```sql
subtractMilliseconds(date_time, num)
```

**パラメーター**

- `date_time`: 指定された数のミリ秒を減算する時刻付き日付。 [DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md)、[String](../data-types/string.md)。
- `num`: 減算するミリ秒の数。 [(U)Int*](../data-types/int-uint.md)、[Float*](../data-types/float.md)。

**返却値**

- `date_time` から `num` ミリ秒を減算します。 [DateTime64](../data-types/datetime64.md)。

**例**

```sql
WITH
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    subtractMilliseconds(date_time, 1000) AS subtract_milliseconds_with_date_time,
    subtractMilliseconds(date_time_string, 1000) AS subtract_milliseconds_with_date_time_string
```

```response
┌─subtract_milliseconds_with_date_time─┬─subtract_milliseconds_with_date_time_string─┐
│              2023-12-31 23:59:59.000 │                     2023-12-31 23:59:59.000 │
└──────────────────────────────────────┴─────────────────────────────────────────────┘
```
- `num`: 引き算するマイクロ秒の数。[(U)Int*](../data-types/int-uint.md)、[Float*](../data-types/float.md)。

**返される値**

- `date_time` から `num` マイクロ秒を引いた結果を返します。 [DateTime64](../data-types/datetime64.md)。

**例**

```sql
WITH
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    subtractMicroseconds(date_time, 1000000) AS subtract_microseconds_with_date_time,
    subtractMicroseconds(date_time_string, 1000000) AS subtract_microseconds_with_date_time_string
```

```response
┌─subtract_microseconds_with_date_time─┬─subtract_microseconds_with_date_time_string─┐
│           2023-12-31 23:59:59.000000 │                  2023-12-31 23:59:59.000000 │
└──────────────────────────────────────┴─────────────────────────────────────────────┘
```

## subtractNanoseconds

指定したナノ秒数を日付と時刻、または文字列で表現された日付と時刻から引き算します。

**構文**

```sql
subtractNanoseconds(date_time, num)
```

**パラメータ**

- `date_time`: ナノ秒数を引き算する対象の日付と時刻。[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md)、[String](../data-types/string.md)。
- `num`: 引き算するナノ秒の数。[(U)Int*](../data-types/int-uint.md)、[Float*](../data-types/float.md)。

**返される値**

- `date_time` から `num` ナノ秒を引いた結果を返します。 [DateTime64](../data-types/datetime64.md)。

**例**

```sql
WITH
    toDateTime('2024-01-01 00:00:00') AS date_time,
    '2024-01-01 00:00:00' AS date_time_string
SELECT
    subtractNanoseconds(date_time, 1000) AS subtract_nanoseconds_with_date_time,
    subtractNanoseconds(date_time_string, 1000) AS subtract_nanoseconds_with_date_time_string
```

```response
┌─subtract_nanoseconds_with_date_time─┬─subtract_nanoseconds_with_date_time_string─┐
│       2023-12-31 23:59:59.999999000 │              2023-12-31 23:59:59.999999000 │
└─────────────────────────────────────┴────────────────────────────────────────────┘
```

## subtractInterval

別の間隔や間隔のタプルに対して、ネガティブな間隔を追加します。

**構文**

```sql
subtractInterval(interval_1, interval_2)
```

**パラメータ**

- `interval_1`: 最初の間隔またはタプルの間隔。[interval](../data-types/special-data-types/interval.md)、[tuple](../data-types/tuple.md)([interval](../data-types/special-data-types/interval.md))。
- `interval_2`: ネガティブにする第2の間隔。[interval](../data-types/special-data-types/interval.md)。

**返される値**

- 間隔のタプルを返します。[tuple](../data-types/tuple.md)([interval](../data-types/special-data-types/interval.md))。

:::note
同じタイプの間隔は、単一の間隔に結合されます。たとえば、 `toIntervalDay(2)` と `toIntervalDay(1)` が渡された場合、結果は `(1)` になります。
:::

**例**

クエリ:

```sql
SELECT subtractInterval(INTERVAL 1 DAY, INTERVAL 1 MONTH);
SELECT subtractInterval((INTERVAL 1 DAY, INTERVAL 1 YEAR), INTERVAL 1 MONTH);
SELECT subtractInterval(INTERVAL 2 DAY, INTERVAL 1 DAY);
```

結果:

```response
┌─subtractInterval(toIntervalDay(1), toIntervalMonth(1))─┐
│ (1,-1)                                                 │
└────────────────────────────────────────────────────────┘
┌─subtractInterval((toIntervalDay(1), toIntervalYear(1)), toIntervalMonth(1))─┐
│ (1,1,-1)                                                                    │
└─────────────────────────────────────────────────────────────────────────────┘
┌─subtractInterval(toIntervalDay(2), toIntervalDay(1))─┐
│ (1)                                                  │
└──────────────────────────────────────────────────────┘
```

## subtractTupleOfIntervals

日付または日付時刻からタプルの間隔を順次引き算します。

**構文**

```sql
subtractTupleOfIntervals(interval_1, interval_2)
```

**パラメータ**

- `date`: 最初の間隔またはタプルの間隔。[Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md)。
- `intervals`: `date` から引く間隔のタプル。[tuple](../data-types/tuple.md)([interval](../data-types/special-data-types/interval.md))。

**返される値**

- 引かれた `intervals` を持つ `date` を返します。[Date](../data-types/date.md)/[Date32](../data-types/date32.md)/[DateTime](../data-types/datetime.md)/[DateTime64](../data-types/datetime64.md)。

**例**

クエリ:

```sql
WITH toDate('2018-01-01') AS date SELECT subtractTupleOfIntervals(date, (INTERVAL 1 DAY, INTERVAL 1 YEAR))
```

結果:

```response
┌─subtractTupleOfIntervals(date, (toIntervalDay(1), toIntervalYear(1)))─┐
│                                                            2016-12-31 │
└───────────────────────────────────────────────────────────────────────┘
```

## timeSlots

‘StartTime’ から始まり ‘Duration’ 秒続く時間間隔において、この間隔内の時間の瞬間を、’Size’ 秒単位に切り捨てた点の配列を返します。’Size’ はオプションのパラメータで、デフォルトは 1800（30分）です。
これは、対応するセッション内のページビューを検索する際に必要です。
‘StartTime’ 引数には DateTime および DateTime64 を受け付けます。DateTime では、’Duration’ と ’Size’ 引数は `UInt32` でなければなりません。’DateTime64’ では、これらは `Decimal64` でなければなりません。
DateTime/DateTime64 の配列を返します（戻り値の型は ’StartTime’ の型に一致します）。DateTime64 の場合、戻り値のスケールは ’StartTime’ のスケールと異なる場合があります --- すべての指定された引数の中で最も高いスケールが適用されます。

**構文**

```sql
timeSlots(StartTime, Duration,\[, Size\])
```

**例**

```sql
SELECT timeSlots(toDateTime('2012-01-01 12:20:00'), toUInt32(600));
SELECT timeSlots(toDateTime('1980-12-12 21:01:02', 'UTC'), toUInt32(600), 299);
SELECT timeSlots(toDateTime64('1980-12-12 21:01:02.1234', 4, 'UTC'), toDecimal64(600.1, 1), toDecimal64(299, 0));
```

結果:

``` text
┌─timeSlots(toDateTime('2012-01-01 12:20:00'), toUInt32(600))─┐
│ ['2012-01-01 12:00:00','2012-01-01 12:30:00']               │
└─────────────────────────────────────────────────────────────┘
┌─timeSlots(toDateTime('1980-12-12 21:01:02', 'UTC'), toUInt32(600), 299)─┐
│ ['1980-12-12 20:56:13','1980-12-12 21:01:12','1980-12-12 21:06:11']     │
└─────────────────────────────────────────────────────────────────────────┘
┌─timeSlots(toDateTime64('1980-12-12 21:01:02.1234', 4, 'UTC'), toDecimal64(600.1, 1), toDecimal64(299, 0))─┐
│ ['1980-12-12 20:56:13.0000','1980-12-12 21:01:12.0000','1980-12-12 21:06:11.0000']                        │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## formatDateTime

指定された形式の文字列に従って時刻をフォーマットします。形式は定数式なので、単一の結果列に対して複数の形式を使用することはできません。

formatDateTime は MySQL の日付と時刻のフォーマットスタイルを使用します。詳細は https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_date-format を参照してください。

この関数の逆操作は [parseDateTime](../functions/type-conversion-functions.md#type_conversion_functions-parseDateTime) です。

エイリアス: `DATE_FORMAT`。

**構文**

``` sql
formatDateTime(Time, Format[, Timezone])
```

**返される値**

指定された形式に従い、日時の値を返します。

**置換フィールド**

置換フィールドを使用して、結果の文字列のパターンを定義できます。“例” 列は `2018-01-02 22:33:44` に対するフォーマッティング結果を示しています。

| プレースホルダー | 説明                                          | 例         |
|----------|-------------------------------------------------|------------|
| %a       | 短縮形の曜日名 (月-日)                          | Mon        |
| %b       | 短縮形の月名 (1月-12月)                        | Jan        |
| %c       | 月を整数番号 (01-12) で表す。’ノート3’を参照      | 01         |
| %C       | 100年で割った後、整数に切り捨て（00-99）        | 20         |
| %d       | 月の日を0詰め（01-31）                          | 02         |
| %D       | 短縮形のMM/DD/YYの日付（%m/%d/%yと同等）        | 01/02/18   |
| %e       | 月の日を空白詰め（1-31）                        | &nbsp; 2   |
| %f       | 小数点以下の秒、’ノート1’を参照                   | 1234560    |
| %F       | 短縮形のYYYY-MM-DDの日付（%Y-%m-%dと同等）      | 2018-01-02 |
| %g       | ISO 8601に整列した2桁の年形式、4桁の表記から短縮化 | 18         |
| %G       | ISO 週番号用の4桁の年形式、週ベースの年から計算 | 2018       |
| %h       | 12時間形式の時（01-12）                        | 09         |
| %H       | 24時間形式の時（00-23）                        | 22         |
| %i       | 分（00-59）                                   | 33         |
| %I       | 12時間形式の時（01-12）                        | 10         |
| %j       | 年の日（001-366）                             | 002        |
| %k       | 24時間形式の時（00-23）、’ノート3’を参照       | 14         |
| %l       | 12時間形式の時（01-12）、’ノート3’を参照       | 09         |
| %m       | 月を整数番号 (01-12) で表す                    | 01         |
| %M       | 完全な月名 (1月-12月)、’ノート2’を参照         | January    |
| %n       | 改行文字 (‘’)                                 |            |
| %p       | AM または PM の指定                            | PM         |
| %Q       | 四半期（1-4）                                 | 1          |
| %r       | 12時間HH:MM AM/PM形式、%h:%i %pと同等          | 10:30 PM   |
| %R       | 24時間HH:MM形式、%H:%iと同等                   | 22:33      |
| %s       | 秒（00-59）                                   | 44         |
| %S       | 秒（00-59）                                   | 44         |
| %t       | 横タブ文字（’）                               |            |
| %T       | ISO 8601形式の時刻 (HH:MM:SS)、%H:%i:%Sと同等  | 22:33:44   |
| %u       | 月曜日を1とするISO 8601の曜日番号（1-7）       | 2          |
| %V       | ISO 8601週番号（01-53）                         | 01         |
| %w       | 日曜日を0とする整数形式の曜日（0-6）            | 2          |
| %W       | 完全な曜日名（月曜日-日曜日）                   | Monday     |
| %y       | 年の最終2桁（00-99）                            | 18         |
| %Y       | 年                                            | 2018       |
| %z       | UTCとの時間オフセットを+HHMMまたは-HHMM形式で表示 | -0500      |
| %%       | % 記号                                       | %          |

ノート1: ClickHouse のバージョンが v23.4 より以前のものである場合、`%f` は日付、Date32、または日付時刻（これらは小数点以下の秒を持たない）や DateTime64 の精度が 0 の場合、単一のゼロ (0) を印刷します。以前の動作は、設定 `formatdatetime_f_prints_single_zero = 1` を使用することで復元できます。

ノート2: ClickHouse のバージョンが v23.4 より以前のものである場合、`%M` は完全な月名（1月-12月）ではなく、分を印刷します（00-59）。以前の動作は、設定 `formatdatetime_parsedatetime_m_is_month_name = 0` を使用することで復元できます。

ノート3: ClickHouse のバージョンが v23.11 より以前のものである場合、関数 `parseDateTime()` はフォーマッタ `%c`（月）および `%l` / `%k`（時）に先頭のゼロを必要としていました。例: `07`。後のバージョンでは、先頭のゼロは省略できます、例: `7`。以前の動作は、設定 `parsedatetime_parse_without_leading_zeros = 0` を使用することで復元できます。関数 `formatDateTime()` はデフォルトでは依然として `%c` および `%l` / `%k` に先頭のゼロを印刷し、既存の使用例を壊さないようにします。この動作は、設定 `formatdatetime_format_without_leading_zeros = 1` によって変更できます。

**例**

``` sql
SELECT formatDateTime(toDate('2010-01-04'), '%g')
```

結果:

```
┌─formatDateTime(toDate('2010-01-04'), '%g')─┐
│ 10                                         │
└────────────────────────────────────────────┘
```

``` sql
SELECT formatDateTime(toDateTime64('2010-01-04 12:34:56.123456', 7), '%f')
```

結果:

```
┌─formatDateTime(toDateTime64('2010-01-04 12:34:56.123456', 7), '%f')─┐
│ 1234560                                                             │
└─────────────────────────────────────────────────────────────────────┘
```

さらに、`formatDateTime` 関数は、タイムゾーンの名前を含む第3の文字列引数を取ることができます。例: `Asia/Istanbul`。この場合、指定されたタイムゾーンに従って時刻がフォーマットされます。

**例**

```sql
SELECT
    now() AS ts,
    time_zone,
    formatDateTime(ts, '%T', time_zone) AS str_tz_time
FROM system.time_zones
WHERE time_zone LIKE 'Europe%'
LIMIT 10

┌──────────────────ts─┬─time_zone─────────┬─str_tz_time─┐
│ 2023-09-08 19:13:40 │ Europe/Amsterdam  │ 21:13:40    │
│ 2023-09-08 19:13:40 │ Europe/Andorra    │ 21:13:40    │
│ 2023-09-08 19:13:40 │ Europe/Astrakhan  │ 23:13:40    │
│ 2023-09-08 19:13:40 │ Europe/Athens     │ 22:13:40    │
│ 2023-09-08 19:13:40 │ Europe/Belfast    │ 20:13:40    │
│ 2023-09-08 19:13:40 │ Europe/Belgrade   │ 21:13:40    │
│ 2023-09-08 19:13:40 │ Europe/Berlin     │ 21:13:40    │
│ 2023-09-08 19:13:40 │ Europe/Bratislava │ 21:13:40    │
│ 2023-09-08 19:13:40 │ Europe/Brussels   │ 21:13:40    │
│ 2023-09-08 19:13:40 │ Europe/Bucharest  │ 22:13:40    │
└─────────────────────┴───────────────────┴─────────────┘
```

**関連情報**

- [formatDateTimeInJodaSyntax](#formatdatetimeinjodasyntax)

## formatDateTimeInJodaSyntax

formatDateTime と似ていますが、MySQL スタイルの代わりに Joda スタイルで日付時刻をフォーマットします。詳細は https://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat.html を参照してください。

この関数の逆操作は [parseDateTimeInJodaSyntax](../functions/type-conversion-functions.md#type_conversion_functions-parseDateTimeInJodaSyntax) です。

**置換フィールド**

置換フィールドを使用して、結果の文字列のパターンを定義できます。

| プレースホルダー | 説明                              | プレゼンテーション  | 例                             |
| ----------- | ---------------------------------- | ------------- | ------------------------------ |
| G           | 年代                              | テキスト      | AD                             |
| C           | 年代の世紀 (>=0)                 | 数字          | 20                             |
| Y           | 年代の年 (>=0)                   | 年            | 1996                           |
| x           | 週年（未対応）                   | 年            | 1996                           |
| w           | 週年の週（未対応）               | 数字          | 27                             |
| e           | 曜日                             | 数字          | 2                              |
| E           | 曜日                             | テキスト      | Tuesday; Tue                   |
| y           | 年                               | 年            | 1996                           |
| D           | 年の日                           | 数字          | 189                            |
| M           | 年の月                           | 月            | July; Jul; 07                 |
| d           | 月の日                           | 数字          | 10                             |
| a           | 日の半分                       | テキスト      | PM                             |
| K           | 半日の時刻 (0〜11)              | 数字          | 0                              |
| h           | 半日の時刻 (1〜12)              | 数字          | 12                             |
| H           | 一日の時刻 (0〜23)              | 数字          | 0                              |
| k           | 一日の時刻 (1〜24)              | 数字          | 24                             |
| m           | 時間の分                       | 数字          | 30                             |
| s           | 分の秒                       | 数字          | 55                             |
| S           | 秒の小数点以下（未対応）         | 数字          | 978                            |
| z           | タイムゾーン（短縮名は未対応）  | テキスト      | 太平洋標準時; PST              |
| Z           | タイムゾーンオフセット/ID（未対応） | ゾーン        | -0800; -08:00; America/Los_Angeles |
| '           | テキストのエスケープ              | 区切り       |                                |
| ''          | 単一引用符                     | リテラル      | '                              |

**例**

``` sql
SELECT formatDateTimeInJodaSyntax(toDateTime('2010-01-04 12:34:56'), 'yyyy-MM-dd HH:mm:ss')
```

結果:

```
┌─formatDateTimeInJodaSyntax(toDateTime('2010-01-04 12:34:56'), 'yyyy-MM-dd HH:mm:ss')─┐
│ 2010-01-04 12:34:56                                                                     │
└─────────────────────────────────────────────────────────────────────────────────────────┘
```

## dateName

指定された日付の部分を返します。

**構文**

``` sql
dateName(date_part, date)
```

**引数**

- `date_part` — 日付の部分。可能な値: 'year', 'quarter', 'month', 'week', 'dayofyear', 'day', 'weekday', 'hour', 'minute', 'second'。[String](../data-types/string.md)。
- `date` — 日付。[Date](../data-types/date.md)、[Date32](../data-types/date32.md)、[DateTime](../data-types/datetime.md) または [DateTime64](../data-types/datetime64.md)。
- `timezone` — タイムゾーン。オプション。[String](../data-types/string.md)。

**返される値**

- 指定された日付の部分。[String](../data-types/string.md#string)

**例**

```sql
WITH toDateTime('2021-04-14 11:22:33') AS date_value
SELECT
    dateName('year', date_value),
    dateName('month', date_value),
    dateName('day', date_value);
```

結果:

```text
┌─dateName('year', date_value)─┬─dateName('month', date_value)─┬─dateName('day', date_value)─┐
│ 2021                         │ April                         │ 14                          │
└──────────────────────────────┴───────────────────────────────┴─────────────────────────────┘
```

## monthName

月の名前を返します。

**構文**

``` sql
monthName(date)
```

**引数**

- `date` — 日付または時刻付きの日付。[Date](../data-types/date.md)、[DateTime](../data-types/datetime.md) または [DateTime64](../data-types/datetime64.md)。

**返される値**

- 月の名前。[String](../data-types/string.md#string)

**例**

```sql
WITH toDateTime('2021-04-14 11:22:33') AS date_value
SELECT monthName(date_value);
```

結果:

```text
┌─monthName(date_value)─┐
│ April                 │
└───────────────────────┘
```

## fromUnixTimestamp

この関数はUnixタイムスタンプをカレンダー日付と一日中の時間に変換します。

二つの方法で呼び出すことができます：

単一の引数として [Integer](../data-types/int-uint.md) 型を渡すと、[DateTime](../data-types/datetime.md) 型の値を返します。すなわち、[toDateTime](../../sql-reference/functions/type-conversion-functions.md#todatetime) のように振舞います。

エイリアス: `FROM_UNIXTIME`。

**例:**

```sql
SELECT fromUnixTimestamp(423543535);
```

結果:

```text
┌─fromUnixTimestamp(423543535)─┐
│          1983-06-04 10:58:55 │
└──────────────────────────────┘
```

二つまたは三つの引数を与え、最初の引数に [Integer](../data-types/int-uint.md)、[Date](../data-types/date.md)、[Date32](../data-types/date32.md)、[DateTime](../data-types/datetime.md) または [DateTime64](../data-types/datetime64.md) の値を渡すと、第二の引数が定数形式の文字列、第三の引数がオプションの定数タイムゾーンの文字列になる場合、この関数は[String](../data-types/string.md#string) 型の値を返します。すなわち、[formatDateTime](#formatdatetime) のように振舞います。この場合、[MySQL の日付と時刻のフォーマットスタイル](https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html#function_date-format) が使用されます。

**例:**

```sql
SELECT fromUnixTimestamp(1234334543, '%Y-%m-%d %R:%S') AS DateTime;
```

結果:

```text
┌─DateTime────────────┐
│ 2009-02-11 14:42:23 │
└─────────────────────┘
```

**関連情報**

- [fromUnixTimestampInJodaSyntax](#fromunixtimestampinjodasyntax)

## fromUnixTimestampInJodaSyntax

[fromUnixTimestamp](#fromunixtimestamp)と同様ですが、二つまたは三つの引数で呼び出す際には、日付フォーマットが[MySQLスタイル](https://joda-time.sourceforge.net/apidocs/org/joda/time/format/DateTimeFormat.html)ではなくJodaスタイルで実行されます。

**例:**

``` sql
SELECT fromUnixTimestampInJodaSyntax(1234334543, 'yyyy-MM-dd HH:mm:ss', 'UTC') AS DateTime;
```

結果:

```
┌─DateTime────────────┐
│ 2009-02-11 06:42:23 │
└─────────────────────┘
```

## toModifiedJulianDay

[プロレプティックグレゴリオ暦](https://en.wikipedia.org/wiki/Proleptic_Gregorian_calendar)の日付をテキスト形式 `YYYY-MM-DD`から[修正ユリウス日](https://en.wikipedia.org/wiki/Julian_day#Variants)番号に変換します。この関数は `0000-01-01` から `9999-12-31` までの日付をサポートします。引数が日付として解析できない場合や無効な日付の場合は例外を発生させます。

**構文**

``` sql
toModifiedJulianDay(date)
```

**引数**

- `date` — テキスト形式の日付。[String](../data-types/string.md) または [FixedString](../data-types/fixedstring.md)。

**返される値**

- 修正ユリウス日番号。[Int32](../data-types/int-uint.md)。

**例**

``` sql
SELECT toModifiedJulianDay('2020-01-01');
```

結果:

``` text
┌─toModifiedJulianDay('2020-01-01')─┐
│                             58849 │
└───────────────────────────────────┘
```

## toModifiedJulianDayOrNull

[toModifiedJulianDay()](#tomodifiedjulianday) よりも似ていますが、例外を発生させる代わりに `NULL` を返します。

**構文**

``` sql
toModifiedJulianDayOrNull(date)
```

**引数**

- `date` — テキスト形式の日付。[String](../data-types/string.md) または [FixedString](../data-types/fixedstring.md)。

**返される値**

- 修正ユリウス日番号。[Nullable(Int32)](../data-types/int-uint.md)。

**例**

``` sql
SELECT toModifiedJulianDayOrNull('2020-01-01');
```

結果:

``` text
┌─toModifiedJulianDayOrNull('2020-01-01')─┐
│                                   58849 │
└─────────────────────────────────────────┘
```

## fromModifiedJulianDay

修正ユリウス日番号を[プロレプティックグレゴリオ暦](https://en.wikipedia.org/wiki/Proleptic_Gregorian_calendar)の日付形式 `YYYY-MM-DD` に変換します。この関数は、`-678941` から `2973483` までの整数をサポートします（これにより `0000-01-01` と `9999-12-31` を表します）。日数がサポートされた範囲外の場合は例外を発生させます。

**構文**

``` sql
fromModifiedJulianDay(day)
```

**引数**

- `day` — 修正ユリウス日番号。[任意の整数型](../data-types/int-uint.md)。

**返される値**

- テキスト形式の日付。[String](../data-types/string.md)

**例**

``` sql
SELECT fromModifiedJulianDay(58849);
```

結果:

``` text
┌─fromModifiedJulianDay(58849)─┐
│ 2020-01-01                   │
└──────────────────────────────┘
```

## fromModifiedJulianDayOrNull

[fromModifiedJulianDayOrNull()](#frommodifiedjuliandayornull) と似ていますが、例外を発生させる代わりに `NULL` を返します。

**構文**

``` sql
fromModifiedJulianDayOrNull(day)
```

**引数**

- `day` — 修正ユリウス日番号。[任意の整数型](../data-types/int-uint.md)。

**返される値**

- テキスト形式の日付。[Nullable(String)](../data-types/string.md)

**例**

``` sql
SELECT fromModifiedJulianDayOrNull(58849);
```

結果:

``` text
┌─fromModifiedJulianDayOrNull(58849)─┐
│ 2020-01-01                         │
└────────────────────────────────────┘
```

## toUTCTimestamp

他のタイムゾーンからUTCタイムスタンプにDateTime/DateTime64型の値を変換します。

**構文**

``` sql
toUTCTimestamp(time_val, time_zone)
```

**引数**

- `time_val` — DateTime/DateTime64型の定数値または式。[DateTime/DateTime64型](../data-types/datetime.md)
- `time_zone` — タイムゾーンを表す定数の文字列値または式。[String型](../data-types/string.md)

**返される値**

- テキスト形式のDateTime/DateTime64

**例**

``` sql
SELECT toUTCTimestamp(toDateTime('2023-03-16'), 'Asia/Shanghai');
```

結果:

``` text
┌─toUTCTimestamp(toDateTime('2023-03-16'),'Asia/Shanghai')┐
│                                     2023-03-15 16:00:00 │
└─────────────────────────────────────────────────────────┘
```

## fromUTCTimestamp

UTCタイムゾーンから他のタイムゾーンのタイムスタンプにDateTime/DateTime64型の値を変換します。

**構文**

``` sql
fromUTCTimestamp(time_val, time_zone)
```

**引数**

- `time_val` — DateTime/DateTime64型の定数値または式。[DateTime/DateTime64型](../data-types/datetime.md)
- `time_zone` — タイムゾーンを表す定数の文字列値または式。[String型](../data-types/string.md)

**返される値**

- テキスト形式のDateTime/DateTime64

**例**

``` sql
SELECT fromUTCTimestamp(toDateTime64('2023-03-16 10:00:00', 3), 'Asia/Shanghai');
```

結果:

``` text
┌─fromUTCTimestamp(toDateTime64('2023-03-16 10:00:00',3),'Asia/Shanghai')─┐
│                                                 2023-03-16 18:00:00.000 │
└─────────────────────────────────────────────────────────────────────────┘
```

## UTCTimestamp

クエリ分析の瞬間における現在の日付と時刻を返します。この関数は定数式です。

:::note
この関数は `now('UTC')` と同じ結果を返します。MySQLサポートのために追加されたものであり、[`now`](#now) が推奨される使用法です。
:::

**構文**

```sql
UTCTimestamp()
```

エイリアス: `UTC_timestamp`。

**返される値**

- クエリ分析の瞬間における現在の日付と時刻を返します。[DateTime](../data-types/datetime.md)。

**例**

クエリ:

```sql
SELECT UTCTimestamp();
```

結果:

```response
┌──────UTCTimestamp()─┐
│ 2024-05-28 08:32:09 │
└─────────────────────┘
```

## timeDiff

二つの日付または日付時刻値の差を返します。差は秒単位で計算されます。これは `dateDiff` と同じであり、MySQLサポートのために追加されました。`dateDiff` が推奨されます。

**構文**

```sql
timeDiff(first_datetime, second_datetime)
```

**引数**

- `first_datetime` — DateTime/DateTime64型の定数値または式。[DateTime/DateTime64型](../data-types/datetime.md)
- `second_datetime` — DateTime/DateTime64型の定数値または式。[DateTime/DateTime64型](../data-types/datetime.md)

**返される値**

二つの日付または日付時刻の値の秒数の差。

**例**

クエリ:

```sql
timeDiff(toDateTime64('1927-01-01 00:00:00', 3), toDate32('1927-01-02'));
```

**結果**:

```response
┌─timeDiff(toDateTime64('1927-01-01 00:00:00', 3), toDate32('1927-01-02'))─┐
│                                                                    86400 │
└──────────────────────────────────────────────────────────────────────────┘
```

## 関連コンテンツ

- ブログ: [ClickHouseでの時系列データの操作](https://clickhouse.com/blog/working-with-time-series-data-and-functions-ClickHouse)
