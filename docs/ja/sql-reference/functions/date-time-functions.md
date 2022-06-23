---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 39
toc_title: "\u65E5\u4ED8\u3068\u6642\u523B\u306E\u64CD\u4F5C"
---

# 日付と時刻を操作するための関数 {#functions-for-working-with-dates-and-times}

タイムゾーンのサポート

タイムゾーンに論理的に使用される日付と時刻を操作するためのすべての関数は、オプションのタイムゾーン引数を受け入れることができます。 例：アジア/エカテリンブルク。 この場合、ローカル(既定)のタイムゾーンではなく、指定されたタイムゾーンを使用します。

``` sql
SELECT
    toDateTime('2016-06-15 23:00:00') AS time,
    toDate(time) AS date_local,
    toDate(time, 'Asia/Yekaterinburg') AS date_yekat,
    toString(time, 'US/Samoa') AS time_samoa
```

``` text
┌────────────────time─┬─date_local─┬─date_yekat─┬─time_samoa──────────┐
│ 2016-06-15 23:00:00 │ 2016-06-15 │ 2016-06-16 │ 2016-06-15 09:00:00 │
└─────────────────────┴────────────┴────────────┴─────────────────────┘
```

UTCと時間数が異なるタイムゾーンのみがサポートされます。

## トティムゾーン {#totimezone}

時刻または日付と時刻を指定したタイムゾーンに変換します。

## toYear {#toyear}

日付または日付と時刻を、年番号(AD)を含むUInt16番号に変換します。

## トクアーター {#toquarter}

Dateまたはdate with timeを、四半期番号を含むUInt8番号に変換します。

## トモンス {#tomonth}

Dateまたはdate with timeを、月番号(1-12)を含むUInt8番号に変換します。

## 今日の年 {#todayofyear}

日付または日付と時刻を、年の日の番号を含むUInt16番号(1-366)に変換します。

## 今日の月 {#todayofmonth}

日付または日付と時刻を、月の日の番号を含むUInt8番号(1-31)に変換します。

## 今日の週 {#todayofweek}

Dateまたはdate with timeを、曜日の番号を含むUInt8番号に変換します(月曜日は1、日曜日は7)。

## toHour {#tohour}

時刻を持つ日付を、24時間(0-23)の時間数を含むUInt8数値に変換します。
This function assumes that if clocks are moved ahead, it is by one hour and occurs at 2 a.m., and if clocks are moved back, it is by one hour and occurs at 3 a.m. (which is not always true – even in Moscow the clocks were twice changed at a different time).

## トミヌテ {#tominute}

時刻を持つ日付を、時間の分の数を含むUInt8番号(0-59)に変換します。

## toSecond {#tosecond}

日付と時刻を、分(0-59)の秒数を含むUInt8数値に変換します。
うるう秒は考慮されません。

## toUnixTimestamp {#to-unix-timestamp}

DateTime引数の場合:値を内部の数値表現(Unixタイムスタンプ)に変換します。
文字列引数の場合：タイムゾーンに従って文字列からdatetimeを解析し（オプションの第二引数、サーバーのタイムゾーンがデフォルトで使用されます）、対応するunixタ
Date引数の場合：動作は指定されていません。

**構文**

``` sql
toUnixTimestamp(datetime)
toUnixTimestamp(str, [timezone])
```

**戻り値**

-   Unixタイムスタンプを返す。

タイプ: `UInt32`.

**例**

クエリ:

``` sql
SELECT toUnixTimestamp('2017-11-05 08:07:47', 'Asia/Tokyo') AS unix_timestamp
```

結果:

``` text
┌─unix_timestamp─┐
│     1509836867 │
└────────────────┘
```

## トスタート {#tostartofyear}

日付または日付を年の最初の日に切り捨てます。
日付を返します。

## トスタートフィソイヤー {#tostartofisoyear}

日付または日付をiso年の最初の日に切り捨てます。
日付を返します。

## toStartOfQuarter {#tostartofquarter}

日付または日付を四半期の最初の日に切り捨てます。
四半期の最初の日は、1月、1月、1月、または1月のいずれかです。
日付を返します。

## トスタートモンス {#tostartofmonth}

日付または日付を月の最初の日に切り捨てます。
日付を返します。

!!! attention "注意"
    不正な日付を解析する動作は、実装固有です。 ClickHouseはゼロの日付を返すか、例外をスローするか “natural” オーバーフロー

## トモンデイ {#tomonday}

日付または日付を時刻とともに最も近い月曜日に切り捨てます。
日付を返します。

## トスタートフィーク(t\[,モード\]) {#tostartofweektmode}

日付または時刻の日付を、最も近い日曜日または月曜日にモードで切り捨てます。
日付を返します。
Mode引数は、toWeek()のmode引数とまったく同じように動作します。 単一引数構文では、モード値0が使用されます。

## トスタートフデイ {#tostartofday}

日付を時刻とともに日の始まりまで切り捨てます。

## トスタートフール {#tostartofhour}

日付と時刻を時間の開始まで切り捨てます。

## トスタートフミニュート {#tostartofminute}

日付と時刻を分の開始まで切り捨てます。

## トスタートオフィブミニュート {#tostartoffiveminute}

日付と時刻を切り捨てます。

## toStartOfTenMinutes {#tostartoftenminutes}

日付と時刻を切り捨てて、十分間隔の開始まで切り捨てます。

## トスタートフィフテンミニュート {#tostartoffifteenminutes}

日付を時刻とともに切り捨てて、十分間隔の開始まで切り捨てます。

## トスタートオフインターバル(time_or_data,区間x単位\[,time_zone\]) {#tostartofintervaltime-or-data-interval-x-unit-time-zone}

これは、名前付きの他の関数の一般化です `toStartOf*`. 例えば,
`toStartOfInterval(t, INTERVAL 1 year)` と同じを返します `toStartOfYear(t)`,
`toStartOfInterval(t, INTERVAL 1 month)` と同じを返します `toStartOfMonth(t)`,
`toStartOfInterval(t, INTERVAL 1 day)` と同じを返します `toStartOfDay(t)`,
`toStartOfInterval(t, INTERVAL 15 minute)` と同じを返します `toStartOfFifteenMinutes(t)` 等。

## トータイム {#totime}

時刻を保持しながら、時刻を持つ日付を特定の固定日付に変換します。

## toRelativeYearNum {#torelativeyearnum}

時刻または日付を持つ日付を、過去の特定の固定点から始まる年の番号に変換します。

## toRelativeQuarterNum {#torelativequarternum}

時刻または日付を持つ日付を、過去の特定の固定点から始まる四半期の数値に変換します。

## トレラティブモンスヌム {#torelativemonthnum}

時刻または日付を持つ日付を、過去の特定の固定点から始まる月の番号に変換します。

## toRelativeWeekNum {#torelativeweeknum}

時刻または日付を持つ日付を、過去の特定の固定点から始まる週の数に変換します。

## toRelativeDayNum {#torelativedaynum}

時刻または日付を持つ日付を、過去の特定の固定点から始まる日付の数値に変換します。

## toRelativeHourNum {#torelativehournum}

時刻または日付を持つ日付を、過去の特定の固定点から始まる時間の数に変換します。

## トレラティブミノテン {#torelativeminutenum}

時刻または日付を持つ日付を、過去の特定の固定点から始まる分の数に変換します。

## toRelativeSecondNum {#torelativesecondnum}

時刻または日付を持つ日付を、過去の特定の固定点から始まる秒数に変換します。

## toISOYear {#toisoyear}

Dateまたはdate with timeをISO年番号を含むUInt16番号に変換します。

## toISOWeek {#toisoweek}

日付または日付と時刻をISO週番号を含むUInt8番号に変換します。

## トウィーク(日付\[,モード\]) {#toweekdatemode}

この関数は、dateまたはdatetimeの週番号を返します。 二引数形式のtoWeek()を使用すると、週が日曜日か月曜日か、戻り値が0から53または1から53の範囲であるかどうかを指定できます。 引数modeを省略すると、デフォルトのモードは0になります。
`toISOWeek()`と等価な互換性関数です `toWeek(date,3)`.
次の表は、mode引数の動作方法を示しています。

| モード | 週の最初の日 | 範囲 | Week 1 is the first week … |
|--------|--------------|------|----------------------------|
| 0      | 日曜日       | 0-53 | 今年の日曜日と             |
| 1      | 月曜日       | 0-53 | 今年は4日以上              |
| 2      | 日曜日       | 1-53 | 今年の日曜日と             |
| 3      | 月曜日       | 1-53 | 今年は4日以上              |
| 4      | 日曜日       | 0-53 | 今年は4日以上              |
| 5      | 月曜日       | 0-53 | 今年の月曜日と             |
| 6      | 日曜日       | 1-53 | 今年は4日以上              |
| 7      | 月曜日       | 1-53 | 今年の月曜日と             |
| 8      | 日曜日       | 1-53 | 1を含む                    |
| 9      | 月曜日       | 1-53 | 1を含む                    |

の意味を持つモード値の場合 “with 4 or more days this year,” 週はISO8601:1988に従って番号が付けられます:

-   1月を含む週が新年に4日以上ある場合、それは1週である。

-   それ以外の場合は、前年の最後の週であり、次の週は1週です。

の意味を持つモード値の場合 “contains January 1”、1月を含む週は1週である。 たとえそれが一日だけ含まれていても、その週が含まれている新年の日数は問題ではありません。

``` sql
toWeek(date, [, mode][, Timezone])
```

**パラメータ**

-   `date` – Date or DateTime.
-   `mode` – Optional parameter, Range of values is \[0,9\], default is 0.
-   `Timezone` – Optional parameter, it behaves like any other conversion function.

**例**

``` sql
SELECT toDate('2016-12-27') AS date, toWeek(date) AS week0, toWeek(date,1) AS week1, toWeek(date,9) AS week9;
```

``` text
┌───────date─┬─week0─┬─week1─┬─week9─┐
│ 2016-12-27 │    52 │    52 │     1 │
└────────────┴───────┴───────┴───────┘
```

## toYearWeek(日付\[,モード\]) {#toyearweekdatemode}

日付の年と週を返します。 結果の年は、年の最初と最後の週のdate引数の年とは異なる場合があります。

Mode引数は、toWeek()のmode引数とまったく同じように動作します。 単一引数構文では、モード値0が使用されます。

`toISOYear()`と等価な互換性関数です `intDiv(toYearWeek(date,3),100)`.

**例**

``` sql
SELECT toDate('2016-12-27') AS date, toYearWeek(date) AS yearWeek0, toYearWeek(date,1) AS yearWeek1, toYearWeek(date,9) AS yearWeek9;
```

``` text
┌───────date─┬─yearWeek0─┬─yearWeek1─┬─yearWeek9─┐
│ 2016-12-27 │    201652 │    201652 │    201701 │
└────────────┴───────────┴───────────┴───────────┘
```

## さて {#now}

ゼロの引数を受け取り、要求の実行のいずれかの瞬間に現在の時刻を返します。
この関数は、要求が完了するまでに時間がかかった場合でも、定数を返します。

## 今日 {#today}

ゼロの引数を受け取り、要求の実行のいずれかの瞬間に現在の日付を返します。
と同じ ‘toDate(now())’.

## 昨日 {#yesterday}

ゼロの引数を受け取り、要求の実行のいずれかの瞬間に昨日の日付を返します。
と同じ ‘today() - 1’.

## タイムスロット {#timeslot}

時間を半分の時間に丸めます。
この機能はYandexに固有のものです。Metricaは、トラッキングタグが単一のユーザーの連続したページビューを表示する場合、セッションを二つのセッションに分割するための最小時間であるため、こ つまり、タプル(タグID、ユーザー ID、およびタイムスロット)を使用して、対応するセッションに含まれるページビューを検索できます。

## トイヤイム {#toyyyymm}

日付または日付と時刻を、年と月の番号(YYYY\*100+MM)を含むUInt32番号に変換します。

## トイヤイム {#toyyyymmdd}

日付または日付と時刻を、年と月の番号(YYYY\*10000+MM\*100+DD)を含むUInt32番号に変換します。

## トイヤイム {#toyyyymmddhhmmss}

日付または日付と時刻を、年と月の番号(YYYY\*10000000000+MM\*100000000+DD\*1000000+hh\*10000+mm\*100+ss)を含むUInt64番号に変換します。

## addYears,addMonths,addWeeks,addDays,addHours,addMinutes,addSeconds,addQuarters {#addyears-addmonths-addweeks-adddays-addhours-addminutes-addseconds-addquarters}

関数は、日付/日付時刻に日付/日付時刻の間隔を追加し、日付/日付時刻を返します。 例えば:

``` sql
WITH
    toDate('2018-01-01') AS date,
    toDateTime('2018-01-01 00:00:00') AS date_time
SELECT
    addYears(date, 1) AS add_years_with_date,
    addYears(date_time, 1) AS add_years_with_date_time
```

``` text
┌─add_years_with_date─┬─add_years_with_date_time─┐
│          2019-01-01 │      2019-01-01 00:00:00 │
└─────────────────────┴──────────────────────────┘
```

## subtruttyears、subtrutmonths、subtrutweeks、subtrutdays、subtrutthours、subtrutminutes、subtrutseconds、subtrutquarters {#subtractyears-subtractmonths-subtractweeks-subtractdays-subtracthours-subtractminutes-subtractseconds-subtractquarters}

関数Date/DateTime間隔をDate/DateTimeに減算し、Date/DateTimeを返します。 例えば:

``` sql
WITH
    toDate('2019-01-01') AS date,
    toDateTime('2019-01-01 00:00:00') AS date_time
SELECT
    subtractYears(date, 1) AS subtract_years_with_date,
    subtractYears(date_time, 1) AS subtract_years_with_date_time
```

``` text
┌─subtract_years_with_date─┬─subtract_years_with_date_time─┐
│               2018-01-01 │           2018-01-01 00:00:00 │
└──────────────────────────┴───────────────────────────────┘
```

## dateDiff {#datediff}

Date値またはDateTime値の差を返します。

**構文**

``` sql
dateDiff('unit', startdate, enddate, [timezone])
```

**パラメータ**

-   `unit` — Time unit, in which the returned value is expressed. [文字列](../syntax.md#syntax-string-literal).

        Supported values:

        | unit   |
        | ---- |
        |second  |
        |minute  |
        |hour    |
        |day     |
        |week    |
        |month   |
        |quarter |
        |year    |

-   `startdate` — The first time value to compare. [日付](../../sql-reference/data-types/date.md) または [DateTime](../../sql-reference/data-types/datetime.md).

-   `enddate` — The second time value to compare. [日付](../../sql-reference/data-types/date.md) または [DateTime](../../sql-reference/data-types/datetime.md).

-   `timezone` — Optional parameter. If specified, it is applied to both `startdate` と `enddate`. 指定されていない場合、 `startdate` と `enddate` 使用されます。 それらが同じでない場合、結果は未指定です。

**戻り値**

の違い `startdate` と `enddate` で表される。 `unit`.

タイプ: `int`.

**例**

クエリ:

``` sql
SELECT dateDiff('hour', toDateTime('2018-01-01 22:00:00'), toDateTime('2018-01-02 23:00:00'));
```

結果:

``` text
┌─dateDiff('hour', toDateTime('2018-01-01 22:00:00'), toDateTime('2018-01-02 23:00:00'))─┐
│                                                                                     25 │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

## タイムスロット(StartTime,Duration,\[,Size\]) {#timeslotsstarttime-duration-size}

で始まる時間間隔の場合 ‘StartTime’ そして続けるために ‘Duration’ これは、この区間の点で構成される時間内の瞬間の配列を返します。 ‘Size’ 秒で。 ‘Size’ 定数UInt32で、既定では1800に設定されます。
例えば, `timeSlots(toDateTime('2012-01-01 12:20:00'), 600) = [toDateTime('2012-01-01 12:00:00'), toDateTime('2012-01-01 12:30:00')]`.
これは、対応するセッションでページビューを検索するために必要です。

## formatDateTime(時刻,Format\[,タイムゾーン\]) {#formatdatetime}

Function formats a Time according given Format string. N.B.: Format is a constant expression, e.g. you can not have multiple formats for single result column.

サポートされている形式の修飾子:
(“Example” 列に時間の書式設定の結果が表示されます `2018-01-02 22:33:44`)

| 修飾子 | 説明                                            | 例         |
|--------|-------------------------------------------------|------------|
| %C     | 年を100で割り、整数に切り捨てます(00-99)        | 20         |
| %d     | 月の日、ゼロパッド(01-31)                       | 02         |
| %D     | %M/%d/%yに相当する短いMM/DD/YYの日付            | 01/02/18   |
| %e     | 月の日,スペース埋め(1-31)                       | 2          |
| %F     | %Y-%m-%dに相当します                            | 2018-01-02 |
| %H     | 24時間形式（00-23時)                            | 22         |
| %I     | 12時間形式（01-12)                              | 10         |
| %j     | 年の日(001-366)                                 | 002        |
| %m     | 十進数としての月(01-12)                         | 01         |
| %M     | 分(00-59)                                       | 33         |
| %n     | 改行文字(")                                     |            |
| %p     | AMまたはPMの指定                                | PM         |
| %R     | 24時間HH:MM時間、%H:%Mに相当                    | 22:33      |
| %S     | 第二（00-59)                                    | 44         |
| %t     | 横タブ文字(')                                   |            |
| %T     | ISO8601時間形式(HH:MM:SS)、%H:%M:%Sに相当します | 22:33:44   |
| %u     | ISO8601平日as番号、月曜日as1(1-7)               | 2          |
| %V     | ISO8601週番号(01-53)                            | 01         |
| %w     | 曜日を十進数とし、日曜日を0(0-6)とします)       | 2          |
| %y     | 年,最後の二桁(00-99)                            | 18         |
| %Y     | 年                                              | 2018       |
| %%     | %記号                                           | %          |

[元の記事](https://clickhouse.tech/docs/en/query_language/functions/date_time_functions/) <!--hide-->
