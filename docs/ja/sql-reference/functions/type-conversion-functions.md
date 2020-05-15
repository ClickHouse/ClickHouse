---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 38
toc_title: "\u30BF\u30A4\u30D7\u5909\u63DB"
---

# タイプ変換関数 {#type-conversion-functions}

## 数値変換の一般的な問題 {#numeric-conversion-issues}

値をあるデータ型から別のデータ型に変換するときは、一般的なケースでは、データの損失につながる危険な操作であることを覚えておく必要があります。 大きいデータ型の値を小さいデータ型にフィットさせる場合、または異なるデータ型の間で値を変換する場合、データ損失が発生する可能性があります。

クリックハウスには [C++プログラムと同じ動作](https://en.cppreference.com/w/cpp/language/implicit_conversion).

## toInt(8/16/32/64) {#toint8163264}

入力値を次の値に変換します。 [Int](../../sql-reference/data-types/int-uint.md) データ型。 この関数ファミ:

-   `toInt8(expr)` — Results in the `Int8` データ型。
-   `toInt16(expr)` — Results in the `Int16` データ型。
-   `toInt32(expr)` — Results in the `Int32` データ型。
-   `toInt64(expr)` — Results in the `Int64` データ型。

**パラメータ**

-   `expr` — [式](../syntax.md#syntax-expressions) 数値または数値の小数表現を含む文字列を返します。 数値のBinary、octal、およびhexadecimal表現はサポートされていません。 先頭のゼロは除去されます。

**戻り値**

の整数値 `Int8`, `Int16`, `Int32`、または `Int64` データ型。

関数の使用 [ゼロに向かって丸め](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero) つまり、数字の小数桁を切り捨てます。

のための機能の動作 [NaNおよびInf](../../sql-reference/data-types/float.md#data_type-float-nan-inf) 引数は未定義です。 覚えておいて [数値変換の問題](#numeric-conversion-issues)、機能を使用する場合。

**例えば**

``` sql
SELECT toInt64(nan), toInt32(32), toInt16('16'), toInt8(8.8)
```

``` text
┌─────────toInt64(nan)─┬─toInt32(32)─┬─toInt16('16')─┬─toInt8(8.8)─┐
│ -9223372036854775808 │          32 │            16 │           8 │
└──────────────────────┴─────────────┴───────────────┴─────────────┘
```

## toInt(8/16/32/64)OrZero {#toint8163264orzero}

これは、string型の引数をとり、int型にそれを解析しようとします(8 \| 16 \| 32 \| 64). 失敗した場合は0を返します。

**例えば**

``` sql
select toInt64OrZero('123123'), toInt8OrZero('123qwe123')
```

``` text
┌─toInt64OrZero('123123')─┬─toInt8OrZero('123qwe123')─┐
│                  123123 │                         0 │
└─────────────────────────┴───────────────────────────┘
```

## toInt(8/16/32/64)OrNull {#toint8163264ornull}

これは、string型の引数をとり、int型にそれを解析しようとします(8 \| 16 \| 32 \| 64). 失敗した場合はnullを返します。

**例えば**

``` sql
select toInt64OrNull('123123'), toInt8OrNull('123qwe123')
```

``` text
┌─toInt64OrNull('123123')─┬─toInt8OrNull('123qwe123')─┐
│                  123123 │                      ᴺᵁᴸᴸ │
└─────────────────────────┴───────────────────────────┘
```

## toUInt(8/16/32/64) {#touint8163264}

入力値を次の値に変換します。 [UInt](../../sql-reference/data-types/int-uint.md) データ型。 この関数ファミ:

-   `toUInt8(expr)` — Results in the `UInt8` データ型。
-   `toUInt16(expr)` — Results in the `UInt16` データ型。
-   `toUInt32(expr)` — Results in the `UInt32` データ型。
-   `toUInt64(expr)` — Results in the `UInt64` データ型。

**パラメータ**

-   `expr` — [式](../syntax.md#syntax-expressions) 数値または数値の小数表現を含む文字列を返します。 数値のBinary、octal、およびhexadecimal表現はサポートされていません。 先頭のゼロは除去されます。

**戻り値**

の整数値 `UInt8`, `UInt16`, `UInt32`、または `UInt64` データ型。

関数の使用 [ゼロに向かって丸め](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero) つまり、数字の小数桁を切り捨てます。

負のagrumentsのための関数の動作と [NaNおよびInf](../../sql-reference/data-types/float.md#data_type-float-nan-inf) 引数は未定義です。 負の数の文字列を渡すと、次のようになります `'-32'`、ClickHouseは例外を発生させます。 覚えておいて [数値変換の問題](#numeric-conversion-issues)、機能を使用する場合。

**例えば**

``` sql
SELECT toUInt64(nan), toUInt32(-32), toUInt16('16'), toUInt8(8.8)
```

``` text
┌───────toUInt64(nan)─┬─toUInt32(-32)─┬─toUInt16('16')─┬─toUInt8(8.8)─┐
│ 9223372036854775808 │    4294967264 │             16 │            8 │
└─────────────────────┴───────────────┴────────────────┴──────────────┘
```

## toUInt(8/16/32/64)OrZero {#touint8163264orzero}

## toUInt(8/16/32/64)OrNull {#touint8163264ornull}

## toFloat(32/64) {#tofloat3264}

## toFloat(32/64)OrZero {#tofloat3264orzero}

## toFloat(32/64)OrNull {#tofloat3264ornull}

## toDate {#todate}

## toDateOrZero {#todateorzero}

## toDateOrNull {#todateornull}

## toDateTime {#todatetime}

## toDateTimeOrZero {#todatetimeorzero}

## toDateTimeOrNull {#todatetimeornull}

## toDecimal(32/64/128) {#todecimal3264128}

変換 `value` に [小数](../../sql-reference/data-types/decimal.md) 精度の高いデータ型 `S`. その `value` 数値または文字列を指定できます。 その `S` （スケール）パラメータ小数点以下の桁数を指定します。

-   `toDecimal32(value, S)`
-   `toDecimal64(value, S)`
-   `toDecimal128(value, S)`

## toDecimal(32/64/128)OrNull {#todecimal3264128ornull}

入力文字列をaに変換します [Nullable(小数点(P,S)))](../../sql-reference/data-types/decimal.md) データ型の値。 このファミリの機能など:

-   `toDecimal32OrNull(expr, S)` — Results in `Nullable(Decimal32(S))` データ型。
-   `toDecimal64OrNull(expr, S)` — Results in `Nullable(Decimal64(S))` データ型。
-   `toDecimal128OrNull(expr, S)` — Results in `Nullable(Decimal128(S))` データ型。

これらの関数は、次の代わりに使用します `toDecimal*()` を取得したい場合は、 `NULL` 入力値の解析エラーが発生した場合の例外の代わりに値を指定します。

**パラメータ**

-   `expr` — [式](../syntax.md#syntax-expressions)、値を返します [文字列](../../sql-reference/data-types/string.md) データ型。 ClickHouseは、小数のテキスト表現を想定しています。 例えば, `'1.111'`.
-   `S` — Scale, the number of decimal places in the resulting value.

**戻り値**

の値 `Nullable(Decimal(P,S))` データ型。 値は次のとおりです:

-   数との `S` ClickHouseが入力文字列を数値として解釈する場合、小数点以下の桁数。
-   `NULL` ClickHouseが入力文字列を数値として解釈できない場合、または入力番号に `S` 小数点以下の桁数。

**例**

``` sql
SELECT toDecimal32OrNull(toString(-1.111), 5) AS val, toTypeName(val)
```

``` text
┌──────val─┬─toTypeName(toDecimal32OrNull(toString(-1.111), 5))─┐
│ -1.11100 │ Nullable(Decimal(9, 5))                            │
└──────────┴────────────────────────────────────────────────────┘
```

``` sql
SELECT toDecimal32OrNull(toString(-1.111), 2) AS val, toTypeName(val)
```

``` text
┌──val─┬─toTypeName(toDecimal32OrNull(toString(-1.111), 2))─┐
│ ᴺᵁᴸᴸ │ Nullable(Decimal(9, 2))                            │
└──────┴────────────────────────────────────────────────────┘
```

## toDecimal(32/64/128)OrZero {#todecimal3264128orzero}

入力値を次の値に変換します。 [小数点(p,s))](../../sql-reference/data-types/decimal.md) データ型。 このファミリの機能など:

-   `toDecimal32OrZero( expr, S)` — Results in `Decimal32(S)` データ型。
-   `toDecimal64OrZero( expr, S)` — Results in `Decimal64(S)` データ型。
-   `toDecimal128OrZero( expr, S)` — Results in `Decimal128(S)` データ型。

これらの関数は、次の代わりに使用します `toDecimal*()` を取得したい場合は、 `0` 入力値の解析エラーが発生した場合の例外の代わりに値を指定します。

**パラメータ**

-   `expr` — [式](../syntax.md#syntax-expressions)、値を返します [文字列](../../sql-reference/data-types/string.md) データ型。 ClickHouseは、小数のテキスト表現を想定しています。 例えば, `'1.111'`.
-   `S` — Scale, the number of decimal places in the resulting value.

**戻り値**

の値 `Nullable(Decimal(P,S))` データ型。 値は次のとおりです:

-   数との `S` ClickHouseが入力文字列を数値として解釈する場合、小数点以下の桁数。
-   0とともに `S` ClickHouseが入力文字列を数値として解釈できない場合、または入力番号に `S` 小数点以下の桁数。

**例えば**

``` sql
SELECT toDecimal32OrZero(toString(-1.111), 5) AS val, toTypeName(val)
```

``` text
┌──────val─┬─toTypeName(toDecimal32OrZero(toString(-1.111), 5))─┐
│ -1.11100 │ Decimal(9, 5)                                      │
└──────────┴────────────────────────────────────────────────────┘
```

``` sql
SELECT toDecimal32OrZero(toString(-1.111), 2) AS val, toTypeName(val)
```

``` text
┌──val─┬─toTypeName(toDecimal32OrZero(toString(-1.111), 2))─┐
│ 0.00 │ Decimal(9, 2)                                      │
└──────┴────────────────────────────────────────────────────┘
```

## toString {#tostring}

数値、文字列（固定文字列ではない）、日付、および日付を時刻で変換するための関数。
これら全ての機能を受け入れを一つの引数。

文字列に変換するとき、または文字列から変換するとき、値はtabseparated形式（および他のほとんどすべてのテキスト形式）と同じ規則を使用して書式設定ま 文字列を解析できない場合は、例外がスローされ、要求はキャンセルされます。

日付を数値またはその逆に変換する場合、日付はunixエポックの開始からの日数に対応します。
時刻を含む日付を数値またはその逆に変換する場合、時刻を含む日付は、unixエポックの開始からの秒数に対応します。

ToDate/toDateTime関数の日時形式は、次のように定義されています:

``` text
YYYY-MM-DD
YYYY-MM-DD hh:mm:ss
```

例外として、uint32、int32、uint64、またはint64の数値型からdateに変換し、その数値が65536以上の場合、その数値はunixタイムスタンプとして(日数ではなく)解釈さ これにより、一般的な執筆のサポートが可能になります ‘toDate(unix\_timestamp)’ それ以外の場合はエラーになり、より面倒な書き込みが必要になります ‘toDate(toDateTime(unix\_timestamp))’.

時間を伴う日付と日付の間の変換は、ヌル時間を追加するか、時間を落とすことによって自然な方法で行われます。

数値型間の変換は、c++で異なる数値型間の代入と同じ規則を使用します。

さらに、datetime引数のtostring関数は、タイムゾーンの名前を含む第二の文字列引数を取ることができます。 例えば: `Asia/Yekaterinburg` この場合、時刻は指定されたタイムゾーンに従ってフォーマットされます。

``` sql
SELECT
    now() AS now_local,
    toString(now(), 'Asia/Yekaterinburg') AS now_yekat
```

``` text
┌───────────now_local─┬─now_yekat───────────┐
│ 2016-06-15 00:11:21 │ 2016-06-15 02:11:21 │
└─────────────────────┴─────────────────────┘
```

また、 `toUnixTimestamp` 機能。

## toFixedString(s,N) {#tofixedstrings-n}

文字列型引数をfixedstring(n)型(固定長nの文字列)に変換します。 nは定数でなければなりません。
文字列のバイト数がnより少ない場合は、右側にnullバイトが渡されます。 文字列のバイト数がnより多い場合は、例外がスローされます。

## tostringクットゼロ(s) {#tostringcuttozeros}

文字列またはfixedstring引数を受け取ります。 最初のゼロ-バイトで切り捨てられたコンテンツを持つ文字列を返します。

例えば:

``` sql
SELECT toFixedString('foo', 8) AS s, toStringCutToZero(s) AS s_cut
```

``` text
┌─s─────────────┬─s_cut─┐
│ foo\0\0\0\0\0 │ foo   │
└───────────────┴───────┘
```

``` sql
SELECT toFixedString('foo\0bar', 8) AS s, toStringCutToZero(s) AS s_cut
```

``` text
┌─s──────────┬─s_cut─┐
│ foo\0bar\0 │ foo   │
└────────────┴───────┘
```

## reinterpretAsUInt(8/16/32/64) {#reinterpretasuint8163264}

## 再解釈(8/16/32/64) {#reinterpretasint8163264}

## 再解釈(32/64) {#reinterpretasfloat3264}

## 再解釈アスデート {#reinterpretasdate}

## タスデータタイムの再解釈 {#reinterpretasdatetime}

これらの関数は文字列を受け取り、文字列の先頭に置かれたバイトをホスト順(リトルエンディアン)の数値として解釈します。 文字列が十分な長さでない場合、関数は、文字列が必要な数のヌルバイトで埋められているかのように機能します。 文字列が必要以上に長い場合、余分なバイトは無視されます。 日付はunixエポックの開始からの日数として解釈され、時刻付きの日付はunixエポックの開始からの秒数として解釈されます。

## 文字列の再解釈 {#type_conversion_functions-reinterpretAsString}

この関数は、時刻を含む数値または日付または日付を受け取り、対応する値をホスト順(リトルエンディアン)で表すバイトを含む文字列を返します。 nullバイトは、末尾から削除されます。 たとえば、uint32型の値255は、バイト長の文字列です。

## 再解釈された文字列 {#reinterpretasfixedstring}

この関数は、時刻を含む数値または日付または日付を受け取り、対応する値をホスト順（リトルエンディアン）で表すバイトを含むfixedstringを返します。 nullバイトは、末尾から削除されます。 たとえば、uint32型の値255は、バイト長のfixedstringです。

## キャスト(x,t) {#type_conversion_function-cast}

変換 ‘x’ に ‘t’ データ型。 構文CAST(x AS t)もサポートされています。

例えば:

``` sql
SELECT
    '2016-06-15 23:00:00' AS timestamp,
    CAST(timestamp AS DateTime) AS datetime,
    CAST(timestamp AS Date) AS date,
    CAST(timestamp, 'String') AS string,
    CAST(timestamp, 'FixedString(22)') AS fixed_string
```

``` text
┌─timestamp───────────┬────────────datetime─┬───────date─┬─string──────────────┬─fixed_string──────────────┐
│ 2016-06-15 23:00:00 │ 2016-06-15 23:00:00 │ 2016-06-15 │ 2016-06-15 23:00:00 │ 2016-06-15 23:00:00\0\0\0 │
└─────────────────────┴─────────────────────┴────────────┴─────────────────────┴───────────────────────────┘
```

FixedString(N)への変換は、String型またはFixedString(N)型の引数に対してのみ機能します。

タイプへの変換 [Nullable](../../sql-reference/data-types/nullable.md) そして背部は支えられます。 例えば:

``` sql
SELECT toTypeName(x) FROM t_null
```

``` text
┌─toTypeName(x)─┐
│ Int8          │
│ Int8          │
└───────────────┘
```

``` sql
SELECT toTypeName(CAST(x, 'Nullable(UInt16)')) FROM t_null
```

``` text
┌─toTypeName(CAST(x, 'Nullable(UInt16)'))─┐
│ Nullable(UInt16)                        │
│ Nullable(UInt16)                        │
└─────────────────────────────────────────┘
```

## toInterval(年/四半期\|月/週\|日/時/分/秒) {#function-tointerval}

数値型の引数を [間隔](../../sql-reference/data-types/special-data-types/interval.md) データ型。

**構文**

``` sql
toIntervalSecond(number)
toIntervalMinute(number)
toIntervalHour(number)
toIntervalDay(number)
toIntervalWeek(number)
toIntervalMonth(number)
toIntervalQuarter(number)
toIntervalYear(number)
```

**パラメータ**

-   `number` — Duration of interval. Positive integer number.

**戻り値**

-   の値 `Interval` データ型。

**例えば**

``` sql
WITH
    toDate('2019-01-01') AS date,
    INTERVAL 1 WEEK AS interval_week,
    toIntervalWeek(1) AS interval_to_week
SELECT
    date + interval_week,
    date + interval_to_week
```

``` text
┌─plus(date, interval_week)─┬─plus(date, interval_to_week)─┐
│                2019-01-08 │                   2019-01-08 │
└───────────────────────────┴──────────────────────────────┘
```

## parseDateTimeBestEffort {#parsedatetimebesteffort}

の日付と時刻を変換します。 [文字列](../../sql-reference/data-types/string.md) 表現する [DateTime](../../sql-reference/data-types/datetime.md#data_type-datetime) データ型。

関数は解析します [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601), [RFC1123-5.2.14RFC-822日付と時刻の指定](https://tools.ietf.org/html/rfc1123#page-55)、ClickHouseのと他のいくつかの日付と時刻の形式。

**構文**

``` sql
parseDateTimeBestEffort(time_string [, time_zone]);
```

**パラメータ**

-   `time_string` — String containing a date and time to convert. [文字列](../../sql-reference/data-types/string.md).
-   `time_zone` — Time zone. The function parses `time_string` タイムゾーンによると。 [文字列](../../sql-reference/data-types/string.md).

**サポートされている非標準形式**

-   9を含む文字列。.10桁 [unix timestamp](https://en.wikipedia.org/wiki/Unix_time).
-   日付と時刻コンポーネントを含む文字列: `YYYYMMDDhhmmss`, `DD/MM/YYYY hh:mm:ss`, `DD-MM-YY hh:mm`, `YYYY-MM-DD hh:mm:ss`、等。
-   日付を含む文字列で、時間の要素は含まれません: `YYYY`, `YYYYMM`, `YYYY*MM`, `DD/MM/YYYY`, `DD-MM-YY` など。
-   日と時間のある文字列: `DD`, `DD hh`, `DD hh:mm`. この場合 `YYYY-MM` として代入される。 `2000-01`.
-   タイムゾーンオフセット情報と共に日付と時刻を含む文字列: `YYYY-MM-DD hh:mm:ss ±h:mm`、等。 例えば, `2020-12-12 17:36:00 -5:00`.

Separatorを持つすべての形式について、この関数は、フルネームまたは月名の最初の三文字で表される月の名前を解析します。 例: `24/DEC/18`, `24-Dec-18`, `01-September-2018`.

**戻り値**

-   `time_string` に変換される。 `DateTime` データ型。

**例**

クエリ:

``` sql
SELECT parseDateTimeBestEffort('12/12/2020 12:12:57')
AS parseDateTimeBestEffort;
```

結果:

``` text
┌─parseDateTimeBestEffort─┐
│     2020-12-12 12:12:57 │
└─────────────────────────┘
```

クエリ:

``` sql
SELECT parseDateTimeBestEffort('Sat, 18 Aug 2018 07:22:16 GMT', 'Europe/Moscow')
AS parseDateTimeBestEffort
```

結果:

``` text
┌─parseDateTimeBestEffort─┐
│     2018-08-18 10:22:16 │
└─────────────────────────┘
```

クエリ:

``` sql
SELECT parseDateTimeBestEffort('1284101485')
AS parseDateTimeBestEffort
```

結果:

``` text
┌─parseDateTimeBestEffort─┐
│     2015-07-07 12:04:41 │
└─────────────────────────┘
```

クエリ:

``` sql
SELECT parseDateTimeBestEffort('2018-12-12 10:12:12')
AS parseDateTimeBestEffort
```

結果:

``` text
┌─parseDateTimeBestEffort─┐
│     2018-12-12 10:12:12 │
└─────────────────────────┘
```

クエリ:

``` sql
SELECT parseDateTimeBestEffort('10 20:19')
```

結果:

``` text
┌─parseDateTimeBestEffort('10 20:19')─┐
│                 2000-01-10 20:19:00 │
└─────────────────────────────────────┘
```

**また見なさい**

-   \[ISO 8601 announcement by @xkcd\](https://xkcd.com/1179/)
-   [RFC 1123](https://tools.ietf.org/html/rfc1123)
-   [toDate](#todate)
-   [toDateTime](#todatetime)

## parseDateTimeBestEffortOrNull {#parsedatetimebesteffortornull}

と同じ [parseDateTimeBestEffort](#parsedatetimebesteffort) ただし、処理できない日付形式が検出された場合はnullを返します。

## parseDateTimeBestEffortOrZero {#parsedatetimebesteffortorzero}

と同じ [parseDateTimeBestEffort](#parsedatetimebesteffort) ただし、処理できない日付形式に遭遇した場合は、日付またはゼロの日時が返されます。

[元の記事](https://clickhouse.tech/docs/en/query_language/functions/type_conversion_functions/) <!--hide-->
