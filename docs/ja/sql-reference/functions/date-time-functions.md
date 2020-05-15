---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 39
toc_title: "\u65E5\u4ED8\u3068\u6642\u523B\u306E\u64CD\u4F5C"
---

# 日付と時刻を操作するための関数 {#functions-for-working-with-dates-and-times}

タイムゾーンのサポート

タイムゾーンの論理的使用を持つ日付と時刻を操作するためのすべての関数は、二番目の省略可能なタイムゾーン引数を受け入れることができます。 例：アジア/エカテリンブルク。 この場合、ローカル（デフォルト）の代わりに指定されたタイムゾーンを使用します。

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

## トティメゾン {#totimezone}

時刻または日付と時刻を指定したタイムゾーンに変換します。

## toYear {#toyear}

時刻を含む日付または日付を年番号(ad)を含むuint16番号に変換します。

## toQuarter {#toquarter}

時刻を含む日付または日付を、四半期番号を含むuint8番号に変換します。

## トモント県france.kgm {#tomonth}

時刻を含む日付または日付を、月番号(1～12)を含むuint8番号に変換します。

## 今日の年 {#todayofyear}

時刻を含む日付または日付を、その年の日付の番号(1-366)を含むuint16番号に変換します。

## toDayOfMonth {#todayofmonth}

時刻を含む日付または日付を、その月の日の番号(1-31)を含むuint8番号に変換します。

## toDayOfWeek {#todayofweek}

時刻を含む日付または日付を、曜日の番号を含むuint8番号に変換します(月曜日は1、日曜日は7)。

## tohourgenericname {#tohour}

時刻を含む日付を、uint8の24時間(0-23)の時刻を含む数値に変換します。
This function assumes that if clocks are moved ahead, it is by one hour and occurs at 2 a.m., and if clocks are moved back, it is by one hour and occurs at 3 a.m. (which is not always true – even in Moscow the clocks were twice changed at a different time).

## toMinute {#tominute}

時刻を含む日付を、時刻の分(0～59)の数を含むuint8数値に変換します。

## ト秒 {#tosecond}

Timeを含む日付をUInt8の数値に変換します(0～59)。
うるう秒は説明されていません。

## toUnixTimestamp {#to-unix-timestamp}

For DateTime argument:値を内部の数値表現(Unixタイムスタンプ)に変換します。
文字列引数の場合：タイムゾーンに従って文字列からのdatetimeを解析します（オプションの第二引数、サーバーのタイムゾーンはデフォルトで使用されます）。
日付の引数の場合：この動作は指定されていません。

**構文**

``` sql
toUnixTimestamp(datetime)
toUnixTimestamp(str, [timezone])
```

**戻り値**

-   Unixタイムスタンプを返す。

タイプ: `UInt32`.

**例えば**

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

## toStartOfYear {#tostartofyear}

日付または日付のある時刻を年の最初の日に切り捨てます。
日付を返します。

## tostartofisoyearcomment {#tostartofisoyear}

日付または日付と時刻をiso暦年の最初の日に切り捨てます。
日付を返します。

## toStartOfQuarter {#tostartofquarter}

日付または日付のある時刻を四半期の最初の日に切り捨てます。
四半期の最初の日はどちらかです1月,1四月,1七月,若しくは1十月.
日付を返します。

## toStartOfMonth {#tostartofmonth}

日付または日付と時刻を月の最初の日に切り捨てます。
日付を返します。

!!! attention "注意"
    間違った日付を解析する動作は実装固有です。 clickhouseはゼロの日付を返したり、例外をスローしたりします “natural” オーバーフロー

## toMonday {#tomonday}

日付または日付と時刻を最も近い月曜日に切り捨てます。
日付を返します。

## toStartOfWeek(t\[,mode\]) {#tostartofweektmode}

日付または日付と時刻を、モード別に最も近い日曜日または月曜日に切り捨てます。
日付を返します。
Mode引数は、toWeek()のmode引数とまったく同じように動作します。 単一引数の構文では、モード値0が使用されます。

## toStartOfDay {#tostartofday}

時刻を含む日付をその日の始まりに切り捨てます。

## toStartOfHour {#tostartofhour}

時刻を含む日付を時間の開始位置に切り捨てます。

## toStartOfMinute {#tostartofminute}

日付と時刻が分の先頭に切り捨てられます。

## toStartOfFiveMinute {#tostartoffiveminute}

日付と時刻を切り捨てます。

## トスタートオフテンミニュート {#tostartoftenminutes}

日付と時刻を切り捨てます。

## トスタートオフィフテンミニュート {#tostartoffifteenminutes}

日付と時刻を切り捨てます。

## toStartOfInterval(time\_or\_data,間隔xユニット\[,time\_zone\]) {#tostartofintervaltime-or-data-interval-x-unit-time-zone}

これは、名前の付いた他の関数の一般化です `toStartOf*`. 例えば,
`toStartOfInterval(t, INTERVAL 1 year)` と同じを返します `toStartOfYear(t)`,
`toStartOfInterval(t, INTERVAL 1 month)` と同じを返します `toStartOfMonth(t)`,
`toStartOfInterval(t, INTERVAL 1 day)` と同じを返します `toStartOfDay(t)`,
`toStartOfInterval(t, INTERVAL 15 minute)` と同じを返します `toStartOfFifteenMinutes(t)` など。

## トタイム {#totime}

時刻を保持しながら、時刻を含む日付を特定の固定日付に変換します。

## torelativeyearnumcomment {#torelativeyearnum}

時刻または日付の日付を、過去の特定の固定小数点から始まる年の数に変換します。

## torelativequarternumcomment {#torelativequarternum}

時刻または日付の日付を、過去の特定の固定小数点から開始して、四半期の数に変換します。

## torelativemonthnumcomment {#torelativemonthnum}

時刻または日付を含む日付を、過去の特定の固定小数点から始まる月の数に変換します。

## torelativeweeknumcomment {#torelativeweeknum}

時刻または日付を含む日付を、過去の特定の固定小数点から始まる週の数に変換します。

## torrelativedaynumcomment {#torelativedaynum}

時刻または日付を含む日付を、過去の特定の固定小数点から始まる日の数に変換します。

## torrelativehournumgenericname {#torelativehournum}

時刻または日付の日付を、過去の特定の固定小数点から始まる時間の数値に変換します。

## toRelativeMinuteNum {#torelativeminutenum}

時刻または日付の日付を、過去の特定の固定小数点から始まる分の数値に変換します。

## torrelativesecondnumcomdnamescription {#torelativesecondnum}

時刻または日付の日付を、過去の特定の固定小数点から開始して秒の数値に変換します。

## toISOYear {#toisoyear}

時刻を含む日付または日付を、iso年番号を含むuint16番号に変換します。

## toISOWeek {#toisoweek}

時刻を含む日付または日付を、iso週番号を含むuint8番号に変換します。

## toWeek(日付\[,モード\]) {#toweekdatemode}

この関数は、dateまたはdatetimeの週番号を返します。 また、戻り値の範囲が0から53または1から53のどちらであるかを指定することができます。 引数modeを省略すると、デフォルトのモードは0になります。
`toISOWeek()`は、以下と同等の互換性関数です `toWeek(date,3)`.
次の表では、mode引数の動作について説明します。

| モード | 週の最初の日 | 範囲 | Week 1 is the first week … |
|--------|--------------|------|----------------------------|
| 0      | 日曜日       | 0-53 | 今年の日曜日に             |
| 1      | 月曜日       | 0-53 | 今年は4日以上              |
| 2      | 日曜日       | 1-53 | 今年の日曜日に             |
| 3      | 月曜日       | 1-53 | 今年は4日以上              |
| 4      | 日曜日       | 0-53 | 今年は4日以上              |
| 5      | 月曜日       | 0-53 | 今年の月曜日と             |
| 6      | 日曜日       | 1-53 | 今年は4日以上              |
| 7      | 月曜日       | 1-53 | 今年の月曜日と             |
| 8      | 日曜日       | 1-53 | 含まれ月1                  |
| 9      | 月曜日       | 1-53 | 含まれ月1                  |

意味のあるモード値の場合 “with 4 or more days this year,” 週はISO8601:1988に従って番号が付けられます:

-   1月を含む週がある場合4新年の日,それは週です1.

-   それ以外の場合は、前年の最後の週であり、次の週は1週です。

意味のあるモード値の場合 “contains January 1” の週の月には1週間に1. たとえそれが一日だけ含まれていても、その週に含まれている新年の日数は関係ありません。

``` sql
toWeek(date, [, mode][, Timezone])
```

**パラメータ**

-   `date` – Date or DateTime.
-   `mode` – Optional parameter, Range of values is \[0,9\], default is 0.
-   `Timezone` – Optional parameter, it behaves like any other conversion function.

**例えば**

``` sql
SELECT toDate('2016-12-27') AS date, toWeek(date) AS week0, toWeek(date,1) AS week1, toWeek(date,9) AS week9;
```

``` text
┌───────date─┬─week0─┬─week1─┬─week9─┐
│ 2016-12-27 │    52 │    52 │     1 │
└────────────┴───────┴───────┴───────┘
```

## toeearweek(日付\[,モード\]) {#toyearweekdatemode}

日付の年と週を返します。 結果の年は、その年の最初と最後の週の日付の引数の年とは異なる場合があります。

Mode引数は、toWeek()のmode引数とまったく同じように動作します。 単一引数の構文では、モード値0が使用されます。

`toISOYear()`は、以下と同等の互換性関数です `intDiv(toYearWeek(date,3),100)`.

**例えば**

``` sql
SELECT toDate('2016-12-27') AS date, toYearWeek(date) AS yearWeek0, toYearWeek(date,1) AS yearWeek1, toYearWeek(date,9) AS yearWeek9;
```

``` text
┌───────date─┬─yearWeek0─┬─yearWeek1─┬─yearWeek9─┐
│ 2016-12-27 │    201652 │    201652 │    201701 │
└────────────┴───────────┴───────────┴───────────┘
```

## さて {#now}

ゼロ引数を受け取り、リクエスト実行のいずれかの時点で現在の時刻を返します。
この関数は、要求が完了するまでに長い時間がかかった場合でも、定数を返します。

## 今日 {#today}

ゼロ引数を受け取り、リクエスト実行のいずれかの時点で現在の日付を返します。
同じように ‘toDate(now())’.

## 昨日 {#yesterday}

ゼロの引数を受け取り、リクエストの実行のいずれかの時点で、昨日の日付を返します。
同じように ‘today() - 1’.

## タイムスロット {#timeslot}

時間を半分時間に丸めます。
この機能はyandexに固有です。トラッキングタグがこの量よりも厳密に時間が異なる単一のユーザーの連続したページビューを表示する場合、セッションを二つのセッションに分割す つまり、タプル(タグid、ユーザー id、およびタイムスロット)を使用して、対応するセッションに含まれるページビューを検索できます。

## toYYYYMM {#toyyyymm}

時刻を含む日付または日付を、年と月の数値(yyyy\*100+mm)を含むuint32番号に変換します。

## toyyymmdd {#toyyyymmdd}

時刻を含む日付または日付を、年と月の数値(yyyy\*10000+mm\*100+dd)を含むuint32番号に変換します。

## toYYYYMMDDhhmmss {#toyyyymmddhhmmss}

時刻付きの日付または日付を、年と月の数値を含むuint64番号に変換します(yyyy\*10000000000+mm\*100000000+dd\*1000000+hh\*10000+mm\*100+ss)。

## addYears,addMonths,addweks,addDays,addHours,addMinutes,addSeconds,addQuarters {#addyears-addmonths-addweeks-adddays-addhours-addminutes-addseconds-addquarters}

関数は、日付/日付時刻の間隔を日付/日付時刻に追加してから、日付/日付時刻を返します。 例えば:

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

## subtractYears,subtractMonths,subtractWeeks,subtractDays,subtractHours,subtractMinutes,subtractSeconds,subtractQuarters {#subtractyears-subtractmonths-subtractweeks-subtractdays-subtracthours-subtractminutes-subtractseconds-subtractquarters}

関数date/datetime間隔をdate/datetimeに減算し、date/datetimeを返します。 例えば:

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

日付または日付時刻値の差を返します。

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

-   `timezone` — Optional parameter. If specified, it is applied to both `startdate` と `enddate`. 指定されていない場合は、 `startdate` と `enddate` 使用されます。 それらが同じでない場合、結果は不特定です。

**戻り値**

の違い `startdate` と `enddate` で表現 `unit`.

タイプ: `int`.

**例えば**

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

## timeSlots(StartTime,Duration,\[,Size\]) {#timeslotsstarttime-duration-size}

で始まる時間間隔のために ‘StartTime’ そしてのために継続 ‘Duration’ 秒、この間隔からのポイントからなる時間内のモーメントの配列を返します。 ‘Size’ 数秒で。 ‘Size’ オプションのパラメーターです:既定では定数UInt32を1800に設定します。
例えば, `timeSlots(toDateTime('2012-01-01 12:20:00'), 600) = [toDateTime('2012-01-01 12:00:00'), toDateTime('2012-01-01 12:30:00')]`.
これは、対応するセッションでページビューを検索するために必要です。

## formatDateTime(時間,フォーマット\[,タイムゾーン\]) {#formatdatetime}

Function formats a Time according given Format string. N.B.: Format is a constant expression, e.g. you can not have multiple formats for single result column.

形式のサポートされている修飾子:
(“Example” このページを正しく表示するフォーマット結果のための時間 `2018-01-02 22:33:44`)

| 修飾子 | 説明                                               | 例えば     |
|--------|----------------------------------------------------|------------|
| %C     | 年を100で除算し、整数(00から99)に切り捨てられます) | 20         |
| %d     | 月の日、ゼロ-パディング（01-31)                    | 02         |
| %D     | %m/%d/%yに相当する短いmm/dd/yy日付                 | 01/02/18   |
| %e     | 月の日、スペース埋め(1-31)                         | 2          |
| %F     | %Y-%m-%dに相当する短いYYYY-MM-DD日付               | 2018-01-02 |
| %H     | 24時間形式(00-23)の時間)                           | 22         |
| %I     | 時間で12h形式(01-12)                               | 10         |
| %j     | 年の日(001-366)                                    | 002        |
| %m     | 月を小数(01-12)として指定します)                   | 01         |
| %M     | 分(00-59)                                          | 33         |
| %n     | 改行文字(")                                        |            |
| %p     | AMまたはPMの指定                                   | PM         |
| %R     | 24時間HH:MM時間、%Hに相当する:%M                   | 22:33      |
| %S     | 二番目に（00-59)                                   | 44         |
| %t     | 水平タブ文字(’)                                    |            |
| %T     | ISO8601時刻フォーマット(HH:MM:SS)、%H:%M:%Sに相当  | 22:33:44   |
| %u     | ISO8601月曜日が1(1-7)の数値としての平日)           | 2          |
| %V     | ISO8601週番号(01-53)                               | 01         |
| %w     | weekday as a decimal number with Sunday as0(0-6)   | 2          |
| %y     | 年,最後の二つの数字(00-99)                         | 18         |
| %Y     | 年                                                 | 2018       |
| %%     | %記号                                              | %          |

[元の記事](https://clickhouse.tech/docs/en/query_language/functions/date_time_functions/) <!--hide-->
