---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 38
toc_title: "Tip D\xF6n\xFC\u015Ft\xFCrme"
---

# Tip Dönüştürme Fonksiyonları {#type-conversion-functions}

## Sayısal dönüşümlerin ortak sorunları {#numeric-conversion-issues}

Bir değeri birinden başka bir veri türüne dönüştürdüğünüzde, ortak durumda, veri kaybına neden olabilecek güvenli olmayan bir işlem olduğunu unutmamalısınız. Değeri daha büyük bir veri türünden daha küçük bir veri türüne sığdırmaya çalışırsanız veya değerleri farklı veri türleri arasında dönüştürürseniz, veri kaybı oluşabilir.

ClickHouse vardır [C++ programları ile aynı davranış](https://en.cppreference.com/w/cpp/language/implicit_conversion).

## toİnt(8/16/32/64) {#toint8163264}

Bir giriş değeri dönüştürür [Tamsayı](../../sql-reference/data-types/int-uint.md) veri türü. Bu işlev ailesi şunları içerir:

-   `toInt8(expr)` — Results in the `Int8` veri türü.
-   `toInt16(expr)` — Results in the `Int16` veri türü.
-   `toInt32(expr)` — Results in the `Int32` veri türü.
-   `toInt64(expr)` — Results in the `Int64` veri türü.

**Parametre**

-   `expr` — [İfade](../syntax.md#syntax-expressions) bir sayının ondalık gösterimiyle bir sayı veya dize döndürülmesi. Sayıların ikili, sekizli ve onaltılık gösterimleri desteklenmez. Önde gelen sıfırlar soyulur.

**Döndürülen değer**

Tamsayı değeri `Int8`, `Int16`, `Int32`, veya `Int64` veri türü.

Fonksiyonlar kullanımı [sıfıra doğru yuvarlama](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), yani sayıların kesirli rakamlarını keserler.

Fonksiyon behaviorların davranışı [N andan ve In andf](../../sql-reference/data-types/float.md#data_type-float-nan-inf) argümanlar tanımsızdır. Hakkında hatırla [sayısal convertions sorunları](#numeric-conversion-issues), fonksiyonları kullanırken.

**Örnek**

``` sql
SELECT toInt64(nan), toInt32(32), toInt16('16'), toInt8(8.8)
```

``` text
┌─────────toInt64(nan)─┬─toInt32(32)─┬─toInt16('16')─┬─toInt8(8.8)─┐
│ -9223372036854775808 │          32 │            16 │           8 │
└──────────────────────┴─────────────┴───────────────┴─────────────┘
```

## toİnt (8/16/32/64)OrZero {#toint8163264orzero}

String türünde bir argüman alır ve İnt içine ayrıştırmaya çalışır(8 \| 16 \| 32 \| 64). Başarısız olursa, 0 döndürür.

**Örnek**

``` sql
select toInt64OrZero('123123'), toInt8OrZero('123qwe123')
```

``` text
┌─toInt64OrZero('123123')─┬─toInt8OrZero('123qwe123')─┐
│                  123123 │                         0 │
└─────────────────────────┴───────────────────────────┘
```

## toİnt(8/16/32/64) OrNull {#toint8163264ornull}

String türünde bir argüman alır ve İnt içine ayrıştırmaya çalışır(8 \| 16 \| 32 \| 64). Başarısız olursa, NULL döndürür.

**Örnek**

``` sql
select toInt64OrNull('123123'), toInt8OrNull('123qwe123')
```

``` text
┌─toInt64OrNull('123123')─┬─toInt8OrNull('123qwe123')─┐
│                  123123 │                      ᴺᵁᴸᴸ │
└─────────────────────────┴───────────────────────────┘
```

## toUİnt(8/16/32/64) {#touint8163264}

Bir giriş değeri dönüştürür [Uİnt](../../sql-reference/data-types/int-uint.md) veri türü. Bu işlev ailesi şunları içerir:

-   `toUInt8(expr)` — Results in the `UInt8` veri türü.
-   `toUInt16(expr)` — Results in the `UInt16` veri türü.
-   `toUInt32(expr)` — Results in the `UInt32` veri türü.
-   `toUInt64(expr)` — Results in the `UInt64` veri türü.

**Parametre**

-   `expr` — [İfade](../syntax.md#syntax-expressions) bir sayının ondalık gösterimiyle bir sayı veya dize döndürülmesi. Sayıların ikili, sekizli ve onaltılık gösterimleri desteklenmez. Önde gelen sıfırlar soyulur.

**Döndürülen değer**

Tamsayı değeri `UInt8`, `UInt16`, `UInt32`, veya `UInt64` veri türü.

Fonksiyonlar kullanımı [sıfıra doğru yuvarlama](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero), yani sayıların kesirli rakamlarını keserler.

Olumsuz agruments için işlevlerin davranışı ve [N andan ve In andf](../../sql-reference/data-types/float.md#data_type-float-nan-inf) argümanlar tanımsızdır. Örneğin, negatif bir sayı ile bir dize geçirirseniz `'-32'`, ClickHouse bir özel durum yükseltir. Hakkında hatırla [sayısal convertions sorunları](#numeric-conversion-issues), fonksiyonları kullanırken.

**Örnek**

``` sql
SELECT toUInt64(nan), toUInt32(-32), toUInt16('16'), toUInt8(8.8)
```

``` text
┌───────toUInt64(nan)─┬─toUInt32(-32)─┬─toUInt16('16')─┬─toUInt8(8.8)─┐
│ 9223372036854775808 │    4294967264 │             16 │            8 │
└─────────────────────┴───────────────┴────────────────┴──────────────┘
```

## toUİnt (8/16/32/64)OrZero {#touint8163264orzero}

## toUİnt(8/16/32/64) OrNull {#touint8163264ornull}

## toFloat(32/64) {#tofloat3264}

## toFloat (32/64)OrZero {#tofloat3264orzero}

## toFloat(32/64) OrNull {#tofloat3264ornull}

## toDate {#todate}

## toDateOrZero {#todateorzero}

## toDateOrNull {#todateornull}

## toDateTime {#todatetime}

## toDateTimeOrZero {#todatetimeorzero}

## toDateTimeOrNull {#todatetimeornull}

## toDecimal(32/64/128) {#todecimal3264128}

Dönüşüyo `value` to the [Ondalık](../../sql-reference/data-types/decimal.md) hassas veri türü `S`. Bu `value` bir sayı veya bir dize olabilir. Bu `S` (scale) parametresi ondalık basamak sayısını belirtir.

-   `toDecimal32(value, S)`
-   `toDecimal64(value, S)`
-   `toDecimal128(value, S)`

## toDecimal(32/64/128) OrNull {#todecimal3264128ornull}

Bir giriş dizesini bir [Nullable (Ondalık (P, S))](../../sql-reference/data-types/decimal.md) veri türü değeri. Bu işlev ailesi şunları içerir:

-   `toDecimal32OrNull(expr, S)` — Results in `Nullable(Decimal32(S))` veri türü.
-   `toDecimal64OrNull(expr, S)` — Results in `Nullable(Decimal64(S))` veri türü.
-   `toDecimal128OrNull(expr, S)` — Results in `Nullable(Decimal128(S))` veri türü.

Bu işlevler yerine kullanılmalıdır `toDecimal*()` fonksiyonlar, eğer bir almak için tercih `NULL` bir giriş değeri ayrıştırma hatası durumunda bir özel durum yerine değer.

**Parametre**

-   `expr` — [İfade](../syntax.md#syntax-expressions) bir değeri döndürür [Dize](../../sql-reference/data-types/string.md) veri türü. ClickHouse ondalık sayının metinsel temsilini bekler. Mesela, `'1.111'`.
-   `S` — Scale, the number of decimal places in the resulting value.

**Döndürülen değer**

İçinde bir değer `Nullable(Decimal(P,S))` veri türü. Değeri içerir:

-   İle sayı `S` ondalık basamaklar, ClickHouse giriş dizesi bir sayı olarak yorumlar.
-   `NULL`, ClickHouse giriş dizesini bir sayı olarak yorumlayamazsa veya giriş numarası birden fazla içeriyorsa `S` ondalık basamaklar.

**Örnekler**

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

## toDecimal (32/64/128)OrZero {#todecimal3264128orzero}

Bir giriş değeri dönüştürür [Ondalık(P, S)](../../sql-reference/data-types/decimal.md) veri türü. Bu işlev ailesi şunları içerir:

-   `toDecimal32OrZero( expr, S)` — Results in `Decimal32(S)` veri türü.
-   `toDecimal64OrZero( expr, S)` — Results in `Decimal64(S)` veri türü.
-   `toDecimal128OrZero( expr, S)` — Results in `Decimal128(S)` veri türü.

Bu işlevler yerine kullanılmalıdır `toDecimal*()` fonksiyonlar, eğer bir almak için tercih `0` bir giriş değeri ayrıştırma hatası durumunda bir özel durum yerine değer.

**Parametre**

-   `expr` — [İfade](../syntax.md#syntax-expressions) bir değeri döndürür [Dize](../../sql-reference/data-types/string.md) veri türü. ClickHouse ondalık sayının metinsel temsilini bekler. Mesela, `'1.111'`.
-   `S` — Scale, the number of decimal places in the resulting value.

**Döndürülen değer**

İçinde bir değer `Nullable(Decimal(P,S))` veri türü. Değeri içerir:

-   İle sayı `S` ondalık basamaklar, ClickHouse giriş dizesi bir sayı olarak yorumlar.
-   0 ile `S` ondalık basamaklar, ClickHouse giriş dizesini bir sayı olarak yorumlayamazsa veya giriş numarası birden fazla içeriyorsa `S` ondalık basamaklar.

**Örnek**

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

Sayılar, dizeler (ancak sabit olmayan dizeler), tarihler ve tarihlerle saatler arasında dönüştürme işlevleri.
Tüm bu işlevler bir argümanı kabul eder.

Bir dizeye veya dizeye dönüştürürken, değer, sekmeyle aynı kuralları kullanarak biçimlendirilir veya ayrıştırılır. ayrı biçim (ve hemen hemen tüm diğer metin biçimleri). Dize ayrıştırılamazsa, bir istisna atılır ve istek iptal edilir.

Tarihleri sayılara dönüştürürken veya tam tersi, Tarih Unix döneminin başlangıcından bu yana geçen gün sayısına karşılık gelir.
Tarihleri zamanlarla sayılara dönüştürürken veya tam tersi olduğunda, zaman ile tarih, Unix döneminin başlangıcından bu yana geçen saniye sayısına karşılık gelir.

ToDate / toDateTime işlevleri için tarih ve saatli tarih biçimleri aşağıdaki gibi tanımlanır:

``` text
YYYY-MM-DD
YYYY-MM-DD hh:mm:ss
```

Özel durum olarak, uınt32, Int32, Uınt64 veya Int64 sayısal türlerinden bugüne dönüştürme ve sayı 65536'dan büyük veya eşitse, sayı Unıx zaman damgası (ve gün sayısı olarak değil) olarak yorumlanır ve tarihe yuvarlanır. Bu, yaygın yazı oluşumu için destek sağlar ‘toDate(unix\_timestamp)’, aksi takdirde bir hata olur ve daha hantal yazmayı gerektirir ‘toDate(toDateTime(unix\_timestamp))’.

Bir tarih ve tarih ile saat arasında dönüştürme doğal bir şekilde gerçekleştirilir: boş bir zaman ekleyerek veya saati bırakarak.

Sayısal türler arasındaki dönüştürme, C++ ' daki farklı sayısal türler arasındaki atamalarla aynı kuralları kullanır.

Ayrıca, Tostring işlevi DateTime bağımsız değişkeni, saat dilimi adını içeren ikinci bir dize bağımsız değişkeni alabilir. Örnek: `Asia/Yekaterinburg` Bu durumda, saat belirtilen saat dilimine göre biçimlendirilir.

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

Ayrıca bakınız `toUnixTimestamp` İşlev.

## toFixedString(s, N) {#tofixedstrings-n}

Bir dize türü bağımsız değişkeni dönüştürür bir FixedString(N) türü (sabit uzunlukta bir dize N). N sabit olmalıdır.
Dize n'den daha az bayt varsa, sağa boş bayt ile doldurulur. Dize n'den daha fazla bayt varsa, bir özel durum atılır.

## tostringcuttozero (s) {#tostringcuttozeros}

Bir dize veya fixedstring bağımsız değişkeni kabul eder. Bulunan ilk sıfır baytta kesilmiş içeriği olan dizeyi döndürür.

Örnek:

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

## reinterpretAsUİnt(8/16/32/64) {#reinterpretasuint8163264}

## reinterpretAsİnt(8/16/32/64) {#reinterpretasint8163264}

## reinterpretAsFloat (32/64) {#reinterpretasfloat3264}

## reinterpretAsDate {#reinterpretasdate}

## reinterpretAsDateTime {#reinterpretasdatetime}

Bu işlevler bir dizeyi kabul eder ve dizenin başına yerleştirilen baytları ana bilgisayar düzeninde (little endian) bir sayı olarak yorumlar. Dize yeterince uzun değilse, işlevler dize gerekli sayıda boş baytla doldurulmuş gibi çalışır. Dize gerekenden daha uzunsa, ek bayt yoksayılır. Bir tarih, Unix döneminin başlangıcından bu yana geçen gün sayısı olarak yorumlanır ve zamana sahip bir tarih, Unix döneminin başlangıcından bu yana geçen saniye sayısı olarak yorumlanır.

## reinterpretAsString {#type_conversion_functions-reinterpretAsString}

Bu işlev, bir sayı veya tarih veya tarih saat ile kabul eder ve ana bilgisayar düzeninde (little endian) karşılık gelen değeri temsil eden bayt içeren bir dize döndürür. Boş bayt sondan bırakılır. Örneğin, 255 uint32 türü değeri bir bayt uzunluğunda bir dizedir.

## reinterpretAsFixedString {#reinterpretasfixedstring}

Bu işlev, bir sayı veya tarih veya tarih saat ile kabul eder ve karşılık gelen değeri ana bilgisayar sırasına (little endian) temsil eden bayt içeren bir FixedString döndürür. Boş bayt sondan bırakılır. Örneğin, 255 uint32 türü değeri bir bayt uzunluğunda bir FixedString.

## CAS (t(x, T) {#type_conversion_function-cast}

Dönüşüyo ‘x’ to the ‘t’ veri türü. Sözdizimi CAST (x AS t) da desteklenmektedir.

Örnek:

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

Fixedstring(N) ' ye dönüştürme yalnızca String veya FixedString(N) türünde argümanlar için çalışır.

Type con conversionvers conversionion to [Nullable](../../sql-reference/data-types/nullable.md) ve geri desteklenmektedir. Örnek:

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

## toİnterval(yıl\|Çeyrek\|Ay\|hafta\|Gün\|Saat\|Dakika / Saniye) {#function-tointerval}

Bir sayı türü argümanını bir [Aralıklı](../../sql-reference/data-types/special-data-types/interval.md) veri türü.

**Sözdizimi**

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

**Parametre**

-   `number` — Duration of interval. Positive integer number.

**Döndürülen değerler**

-   Değeri `Interval` veri türü.

**Örnek**

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

Bir tarih ve saati dönüştürür [Dize](../../sql-reference/data-types/string.md) temsil etmek [DateTime](../../sql-reference/data-types/datetime.md#data_type-datetime) veri türü.

İşlev ayrıştırır [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601), [RFC 1123-5.2.14 RFC-822 Tarih ve Saat özellikleri](https://tools.ietf.org/html/rfc1123#page-55), ClickHouse ve diğer bazı tarih ve saat biçimleri.

**Sözdizimi**

``` sql
parseDateTimeBestEffort(time_string [, time_zone]);
```

**Parametre**

-   `time_string` — String containing a date and time to convert. [Dize](../../sql-reference/data-types/string.md).
-   `time_zone` — Time zone. The function parses `time_string` saat dilimine göre. [Dize](../../sql-reference/data-types/string.md).

**Desteklenen standart dışı formatlar**

-   9 içeren bir dize..10 haneli [unix zaman damgası](https://en.wikipedia.org/wiki/Unix_time).
-   Tarih ve saat bileşeni olan bir dize: `YYYYMMDDhhmmss`, `DD/MM/YYYY hh:mm:ss`, `DD-MM-YY hh:mm`, `YYYY-MM-DD hh:mm:ss` vb.
-   Bir tarih, ancak hiçbir zaman bileşeni ile bir dize: `YYYY`, `YYYYMM`, `YYYY*MM`, `DD/MM/YYYY`, `DD-MM-YY` vb.
-   Bir gün ve Saat ile bir dize: `DD`, `DD hh`, `DD hh:mm`. Bu durumda `YYYY-MM` olarak ikame edilir `2000-01`.
-   Tarih ve Saat Saat Dilimi uzaklık bilgileri ile birlikte içeren bir dize: `YYYY-MM-DD hh:mm:ss ±h:mm` vb. Mesela, `2020-12-12 17:36:00 -5:00`.

Ayırıcılı tüm formatlar için işlev, tam adlarıyla veya bir ay adının ilk üç harfiyle ifade edilen ay adlarını ayrıştırır. Örnekler: `24/DEC/18`, `24-Dec-18`, `01-September-2018`.

**Döndürülen değer**

-   `time_string` dönüştürül thedü `DateTime` veri türü.

**Örnekler**

Sorgu:

``` sql
SELECT parseDateTimeBestEffort('12/12/2020 12:12:57')
AS parseDateTimeBestEffort;
```

Sonuç:

``` text
┌─parseDateTimeBestEffort─┐
│     2020-12-12 12:12:57 │
└─────────────────────────┘
```

Sorgu:

``` sql
SELECT parseDateTimeBestEffort('Sat, 18 Aug 2018 07:22:16 GMT', 'Europe/Moscow')
AS parseDateTimeBestEffort
```

Sonuç:

``` text
┌─parseDateTimeBestEffort─┐
│     2018-08-18 10:22:16 │
└─────────────────────────┘
```

Sorgu:

``` sql
SELECT parseDateTimeBestEffort('1284101485')
AS parseDateTimeBestEffort
```

Sonuç:

``` text
┌─parseDateTimeBestEffort─┐
│     2015-07-07 12:04:41 │
└─────────────────────────┘
```

Sorgu:

``` sql
SELECT parseDateTimeBestEffort('2018-12-12 10:12:12')
AS parseDateTimeBestEffort
```

Sonuç:

``` text
┌─parseDateTimeBestEffort─┐
│     2018-12-12 10:12:12 │
└─────────────────────────┘
```

Sorgu:

``` sql
SELECT parseDateTimeBestEffort('10 20:19')
```

Sonuç:

``` text
┌─parseDateTimeBestEffort('10 20:19')─┐
│                 2000-01-10 20:19:00 │
└─────────────────────────────────────┘
```

**Ayrıca Bakınız**

-   \[ISO 8601 duyuru @xkcd\](https://xkcd.com/1179/)
-   [RFC 1123](https://tools.ietf.org/html/rfc1123)
-   [toDate](#todate)
-   [toDateTime](#todatetime)

## parseDateTimeBestEffortOrNull {#parsedatetimebesteffortornull}

İçin aynı [parseDateTimeBestEffort](#parsedatetimebesteffort) işlenemeyen bir tarih biçimiyle karşılaştığında null döndürmesi dışında.

## parseDateTimeBestEffortOrZero {#parsedatetimebesteffortorzero}

İçin aynı [parseDateTimeBestEffort](#parsedatetimebesteffort) bunun dışında, işlenemeyen bir tarih biçimiyle karşılaştığında sıfır tarih veya sıfır tarih saati döndürür.

[Orijinal makale](https://clickhouse.tech/docs/en/query_language/functions/type_conversion_functions/) <!--hide-->
