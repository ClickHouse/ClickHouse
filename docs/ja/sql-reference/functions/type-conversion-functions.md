---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 38
toc_title: "\u30BF\u30A4\u30D7\u5909\u63DB"
---

# 型変換関数 {#type-conversion-functions}

## 数値変換の一般的な問題 {#numeric-conversion-issues}

あるデータ型から別のデータ型に値を変換する場合、一般的なケースでは、データ損失につながる危険な操作であることを覚えておく必要があります。 大きいデータ型から小さいデータ型に値を近似しようとする場合、または異なるデータ型間で値を変換する場合、データ損失が発生する可能性があります。

クリックハウスは [C++プログラムと同じ動作](https://en.cppreference.com/w/cpp/language/implicit_conversion).

## トイント（8/16/32/64) {#toint8163264}

入力値を [Int](../../sql-reference/data-types/int-uint.md) データ型。 この関数ファミ:

-   `toInt8(expr)` — Results in the `Int8` データ型。
-   `toInt16(expr)` — Results in the `Int16` データ型。
-   `toInt32(expr)` — Results in the `Int32` データ型。
-   `toInt64(expr)` — Results in the `Int64` データ型。

**パラメータ**

-   `expr` — [式](../syntax.md#syntax-expressions) 数値または数値の十進表現を持つ文字列を返します。 数値の二進表現、八進表現、進表現はサポートされていません。 先頭のゼロは削除されます。

**戻り値**

の整数値 `Int8`, `Int16`, `Int32`,または `Int64` データ型。

関数の使用 [ゼロへの丸め](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero) つまり、数字の小数部の数字を切り捨てます。

に対する関数の振る舞い [NaNおよびInf](../../sql-reference/data-types/float.md#data_type-float-nan-inf) 引数は未定義です。 覚えておいて [数値変換の問題](#numeric-conversion-issues)、関数を使用する場合。

**例**

``` sql
SELECT toInt64(nan), toInt32(32), toInt16('16'), toInt8(8.8)
```

``` text
┌─────────toInt64(nan)─┬─toInt32(32)─┬─toInt16('16')─┬─toInt8(8.8)─┐
│ -9223372036854775808 │          32 │            16 │           8 │
└──────────────────────┴─────────────┴───────────────┴─────────────┘
```

## toInt(8/16/32/64)OrZero {#toint8163264orzero}

String型の引数を取り、それをIntに解析しようとします(8 \| 16 \| 32 \| 64). 失敗した場合は0を返します。

**例**

``` sql
select toInt64OrZero('123123'), toInt8OrZero('123qwe123')
```

``` text
┌─toInt64OrZero('123123')─┬─toInt8OrZero('123qwe123')─┐
│                  123123 │                         0 │
└─────────────────────────┴───────────────────────────┘
```

## toInt(8/16/32/64)OrNull {#toint8163264ornull}

String型の引数を取り、それをIntに解析しようとします(8 \| 16 \| 32 \| 64). 失敗した場合はNULLを返します。

**例**

``` sql
select toInt64OrNull('123123'), toInt8OrNull('123qwe123')
```

``` text
┌─toInt64OrNull('123123')─┬─toInt8OrNull('123qwe123')─┐
│                  123123 │                      ᴺᵁᴸᴸ │
└─────────────────────────┴───────────────────────────┘
```

## トゥイント（8/16/32/64) {#touint8163264}

入力値を [UInt](../../sql-reference/data-types/int-uint.md) データ型。 この関数ファミ:

-   `toUInt8(expr)` — Results in the `UInt8` データ型。
-   `toUInt16(expr)` — Results in the `UInt16` データ型。
-   `toUInt32(expr)` — Results in the `UInt32` データ型。
-   `toUInt64(expr)` — Results in the `UInt64` データ型。

**パラメータ**

-   `expr` — [式](../syntax.md#syntax-expressions) 数値または数値の十進表現を持つ文字列を返します。 数値の二進表現、八進表現、進表現はサポートされていません。 先頭のゼロは削除されます。

**戻り値**

の整数値 `UInt8`, `UInt16`, `UInt32`,または `UInt64` データ型。

関数の使用 [ゼロへの丸め](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero) つまり、数字の小数部の数字を切り捨てます。

負の関数に対する関数の振る舞いと [NaNおよびInf](../../sql-reference/data-types/float.md#data_type-float-nan-inf) 引数は未定義です。 負の数の文字列を渡すと、次のようになります `'-32'`,ClickHouseは例外を発生させます。 覚えておいて [数値変換の問題](#numeric-conversion-issues)、関数を使用する場合。

**例**

``` sql
SELECT toUInt64(nan), toUInt32(-32), toUInt16('16'), toUInt8(8.8)
```

``` text
┌───────toUInt64(nan)─┬─toUInt32(-32)─┬─toUInt16('16')─┬─toUInt8(8.8)─┐
│ 9223372036854775808 │    4294967264 │             16 │            8 │
└─────────────────────┴───────────────┴────────────────┴──────────────┘
```

## トゥイント(8/16/32/64)オルゼロ {#touint8163264orzero}

## トゥイント(8/16/32/64)OrNull {#touint8163264ornull}

## トフロア(32/64) {#tofloat3264}

## toFloat(32/64)OrZero {#tofloat3264orzero}

## toFloat(32/64)OrNull {#tofloat3264ornull}

## 東立（とうだて {#todate}

## トダテオルゼロ {#todateorzero}

## toDateOrNull {#todateornull}

## toDateTime {#todatetime}

## トダティメオルゼロ {#todatetimeorzero}

## toDateTimeOrNull {#todatetimeornull}

## トデシマル(32/64/128) {#todecimal3264128}

変換 `value` に [小数点](../../sql-reference/data-types/decimal.md) 精度の高いデータ型 `S`. その `value` 数値または文字列を指定できます。 その `S` (scale)パラメータは、小数点以下の桁数を指定します。

-   `toDecimal32(value, S)`
-   `toDecimal64(value, S)`
-   `toDecimal128(value, S)`

## toDecimal(32/64/128)OrNull {#todecimal3264128ornull}

入力文字列をaに変換します [Nullable(Decimal(P,S)))](../../sql-reference/data-types/decimal.md) データ型の値。 このファミリの機能など:

-   `toDecimal32OrNull(expr, S)` — Results in `Nullable(Decimal32(S))` データ型。
-   `toDecimal64OrNull(expr, S)` — Results in `Nullable(Decimal64(S))` データ型。
-   `toDecimal128OrNull(expr, S)` — Results in `Nullable(Decimal128(S))` データ型。

これらの関数は、 `toDecimal*()` あなたが得ることを好むならば、関数 `NULL` 入力値の解析エラーが発生した場合の例外ではなく、値。

**パラメータ**

-   `expr` — [式](../syntax.md#syntax-expressions) の値を返します。 [文字列](../../sql-reference/data-types/string.md) データ型。 ClickHouseは、十進数のテキスト表現を想定しています。 例えば, `'1.111'`.
-   `S` — Scale, the number of decimal places in the resulting value.

**戻り値**

の値 `Nullable(Decimal(P,S))` データ型。 この値を含む:

-   との数 `S` ClickHouseが入力文字列を数値として解釈する場合は、小数点以下の桁数。
-   `NULL`、ClickHouseが入力文字列を数値として解釈できない場合、または入力番号に以下のものが含まれている場合 `S` 小数点以下の桁数。

**例**

``` sql
SELECT toDecimal32OrNull(toString(-1.111), 5) AS val, toTypeName(val)
```

``` text
┌────val─┬─toTypeName(toDecimal32OrNull(toString(-1.111), 5))─┐
│ -1.111 │ Nullable(Decimal(9, 5))                            │
└────────┴────────────────────────────────────────────────────┘
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

入力値を [小数(P,S)](../../sql-reference/data-types/decimal.md) データ型。 このファミリの機能など:

-   `toDecimal32OrZero( expr, S)` — Results in `Decimal32(S)` データ型。
-   `toDecimal64OrZero( expr, S)` — Results in `Decimal64(S)` データ型。
-   `toDecimal128OrZero( expr, S)` — Results in `Decimal128(S)` データ型。

これらの関数は、 `toDecimal*()` あなたが得ることを好むならば、関数 `0` 入力値の解析エラーが発生した場合の例外ではなく、値。

**パラメータ**

-   `expr` — [式](../syntax.md#syntax-expressions) の値を返します。 [文字列](../../sql-reference/data-types/string.md) データ型。 ClickHouseは、十進数のテキスト表現を想定しています。 例えば, `'1.111'`.
-   `S` — Scale, the number of decimal places in the resulting value.

**戻り値**

の値 `Nullable(Decimal(P,S))` データ型。 この値を含む:

-   との数 `S` ClickHouseが入力文字列を数値として解釈する場合は、小数点以下の桁数。
-   0とともに `S` ClickHouseが入力文字列を数値として解釈できない場合、または入力番号に以下のものが含まれている場合は、小数点以下の桁数 `S` 小数点以下の桁数。

**例**

``` sql
SELECT toDecimal32OrZero(toString(-1.111), 5) AS val, toTypeName(val)
```

``` text
┌────val─┬─toTypeName(toDecimal32OrZero(toString(-1.111), 5))─┐
│ -1.111 │ Decimal(9, 5)                                      │
└────────┴────────────────────────────────────────────────────┘
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

数値、文字列（ただし、固定文字列ではありません）、日付、および時刻を持つ日付の間で変換するための関数。
これら全ての機能を受け入れを一つの引数。

文字列との間で変換する場合、値はTabSeparated形式(およびほとんどすべての他のテキスト形式)と同じ規則を使用して書式設定または解析されます。 文字列を解析できない場合は、例外がスローされ、要求がキャンセルされます。

日付を数値に変換する場合、またはその逆の場合、日付はUnixエポックの開始からの日数に対応します。
時刻付きの日付を数値に変換する場合、またはその逆の場合、時刻付きの日付はUnixエポックの開始からの秒数に対応します。

ToDate/toDateTime関数の日付および日付と時刻の形式は、次のように定義されます:

``` text
YYYY-MM-DD
YYYY-MM-DD hh:mm:ss
```

例外として、uint32、Int32、UInt64、またはInt64の数値型からDateに変換する場合、数値が65536以上の場合、数値はUnixタイムスタンプとして解釈され(日数ではなく)、日付 この支援のための共通の発生を書く ‘toDate(unix_timestamp)’ それ以外の場合はエラーになり、より面倒な書き込みが必要になります ‘toDate(toDateTime(unix_timestamp))’.

日付と時刻の間の変換は、null時間を追加するか、時間を削除することによって、自然な方法で実行されます。

数値型間の変換では、C++の異なる数値型間の代入と同じ規則が使用されます。

さらに、DateTime引数のtoString関数は、タイムゾーンの名前を含む第二の文字列引数を取ることができます。 例: `Asia/Yekaterinburg` この場合、時刻は指定されたタイムゾーンに従って書式設定されます。

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

文字列型の引数をFixedString(N)型(固定長Nの文字列)に変換します。 Nは定数でなければなりません。
文字列のバイト数がNより少ない場合は、右側にnullバイトが埋め込まれます。 文字列のバイト数がNより多い場合は、例外がスローされます。

## トストリングカットゼロ(s) {#tostringcuttozeros}

文字列またはFixedString引数を受け取ります。 見つかった最初のゼロバイトで切り捨てられた内容の文字列を返します。

例:

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

## 再解釈（8/16/32/64) {#reinterpretasuint8163264}

## 再解釈(8/16/32/64) {#reinterpretasint8163264}

## reinterpretAsFloat(32/64) {#reinterpretasfloat3264}

## 再解釈日 {#reinterpretasdate}

## データの再解釈 {#reinterpretasdatetime}

これらの関数は、文字列を受け入れ、文字列の先頭に置かれたバイトをホスト順(リトルエンディアン)の数値として解釈します。 文字列が十分な長さでない場合、関数は文字列がnullバイトの必要な数で埋め込まれているかのように動作します。 文字列が必要以上に長い場合、余分なバイトは無視されます。 日付はUnixエポックの開始からの日数として解釈され、時刻を持つ日付はUnixエポックの開始からの秒数として解釈されます。

## 再解釈 {#type_conversion_functions-reinterpretAsString}

この関数は、数値または日付または日付と時刻を受け入れ、対応する値をホスト順(リトルエンディアン)で表すバイトを含む文字列を返します。 Nullバイトは末尾から削除されます。 たとえば、UInt32型の値255は、バイト長の文字列です。

## reinterpretAsFixedString {#reinterpretasfixedstring}

この関数は、数値または日付または日付と時刻を受け取り、ホスト順(リトルエンディアン)で対応する値を表すバイトを含むFixedStringを返します。 Nullバイトは末尾から削除されます。 たとえば、UInt32型の値255は、バイト長のFixedStringです。

## キャスト(x,T) {#type_conversion_function-cast}

変換 ‘x’ に ‘t’ データ型。 構文CAST(x AS t)もサポートされています。

例:

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

タイプ変換 [Null可能](../../sql-reference/data-types/nullable.md) そして背部は支えられる。 例:

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

## toInterval(年/四半期\|月/週\|日\|時/分/秒) {#function-tointerval}

数値型引数を [間隔](../../sql-reference/data-types/special-data-types/interval.md) データ型。

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

**例**

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

の日付と時刻を変換します。 [文字列](../../sql-reference/data-types/string.md) への表現 [DateTime](../../sql-reference/data-types/datetime.md#data_type-datetime) データ型。

関数は解析します [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601), [RFC1123-5.2.14RFC-822日付と時刻の仕様](https://tools.ietf.org/html/rfc1123#page-55)、ClickHouseのといくつかの他の日付と時刻の形式。

**構文**

``` sql
parseDateTimeBestEffort(time_string [, time_zone]);
```

**パラメータ**

-   `time_string` — String containing a date and time to convert. [文字列](../../sql-reference/data-types/string.md).
-   `time_zone` — Time zone. The function parses `time_string` タイムゾーンによると。 [文字列](../../sql-reference/data-types/string.md).

**サポートされている非標準形式**

-   9を含む文字列。.10桁 [unixタイムスタン](https://en.wikipedia.org/wiki/Unix_time).
-   日付と時刻コンポーネントを持つ文字列: `YYYYMMDDhhmmss`, `DD/MM/YYYY hh:mm:ss`, `DD-MM-YY hh:mm`, `YYYY-MM-DD hh:mm:ss` など。
-   日付を持つ文字列ですが、時刻コンポーネントはありません: `YYYY`, `YYYYMM`, `YYYY*MM`, `DD/MM/YYYY`, `DD-MM-YY` 等。
-   日付と時刻の文字列: `DD`, `DD hh`, `DD hh:mm`. この場合 `YYYY-MM` と置き換えられる。 `2000-01`.
-   日付と時刻とタイムゾーンのオフセット情報を含む文字列: `YYYY-MM-DD hh:mm:ss ±h:mm` など。 例えば, `2020-12-12 17:36:00 -5:00`.

区切り文字を持つすべての形式について、関数は月名をフルネームまたは月名の最初の三文字で表したものを解析します。 例: `24/DEC/18`, `24-Dec-18`, `01-September-2018`.

**戻り値**

-   `time_string` に変換されます。 `DateTime` データ型。

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
SELECT parseDateTimeBestEffort('Sat, 18 Aug 2018 07:22:16 GMT', 'Asia/Istanbul')
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

**も参照。**

-   \[ISO8601による発表 @xkcd\]（https://xkcd.com/1179/)
-   [RFC 1123](https://tools.ietf.org/html/rfc1123)
-   [東立（とうだて](#todate)
-   [toDateTime](#todatetime)

## parseDateTimeBestEffortOrNull {#parsedatetimebesteffortornull}

同じように [parseDateTimeBestEffort](#parsedatetimebesteffort) ただし、処理できない日付形式が検出されるとnullが返されます。

## parseDateTimeBestEffortOrZero {#parsedatetimebesteffortorzero}

同じように [parseDateTimeBestEffort](#parsedatetimebesteffort) ただし、処理できない日付形式が検出されたときにゼロの日付またはゼロの日付時刻を返します。

[元の記事](https://clickhouse.com/docs/en/query_language/functions/type_conversion_functions/) <!--hide-->
