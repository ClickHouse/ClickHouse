---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 39
toc_title: "Tarih ve Saatlerle \xE7al\u0131\u015Fma"
---

# Tarih ve Saatlerle çalışmak için işlevler {#functions-for-working-with-dates-and-times}

Saat dilimleri için destek

Saat dilimi için mantıksal kullanımı olan tarih ve Saat ile çalışmak için tüm işlevler, ikinci bir isteğe bağlı saat dilimi bağımsız değişkeni kabul edebilir. Örnek: Asya / Yekaterinburg. Bu durumda, yerel (varsayılan) yerine belirtilen saat dilimini kullanırlar.

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

UTC'DEN saat sayısına göre farklı olan yalnızca saat dilimleri desteklenir.

## toTimeZone {#totimezone}

Saat veya tarih ve saati belirtilen saat dilimine dönüştürün.

## toYear {#toyear}

Bir tarihi veya tarihi zamanla yıl numarasını (AD) içeren bir Uınt16 numarasına dönüştürür.

## toQuarter {#toquarter}

Bir tarihi veya tarihi zaman ile çeyrek sayısını içeren bir Uİnt8 numarasına dönüştürür.

## toMonth {#tomonth}

Bir tarih veya tarih ile saati, ay numarasını (1-12) içeren bir Uİnt8 numarasına dönüştürür.

## bugünyıl {#todayofyear}

Bir tarih veya tarih ile saat, yılın gün sayısını (1-366) içeren bir Uınt16 numarasına dönüştürür.

## bugünay {#todayofmonth}

Bir tarih veya tarih ile saat, Ayın gün sayısını (1-31) içeren bir Uınt8 numarasına dönüştürür.

## bugünhafta {#todayofweek}

Bir tarih veya tarih ile saat, haftanın gününün sayısını içeren bir Uınt8 numarasına dönüştürür (Pazartesi 1 ve pazar 7'dir).

## toHour {#tohour}

Saatli bir tarihi, 24 saatlik süre (0-23) saat sayısını içeren bir Uınt8 numarasına dönüştürür.
This function assumes that if clocks are moved ahead, it is by one hour and occurs at 2 a.m., and if clocks are moved back, it is by one hour and occurs at 3 a.m. (which is not always true – even in Moscow the clocks were twice changed at a different time).

## toMinute {#tominute}

Saatli bir tarihi, saatin dakika sayısını (0-59) içeren bir Uınt8 numarasına dönüştürür.

## toSecond {#tosecond}

Dakika (0-59) ikinci sayısını içeren bir uınt8 numarasına zaman ile bir tarih dönüştürür.
Sıçrama saniye hesaba değildir.

## toUnixTimestamp {#to-unix-timestamp}

DateTime argümanı için: değeri dahili sayısal gösterimine dönüştürür (Unıx Zaman Damgası).
String argümanı için: datetime'ı dizeden saat dilimine göre ayrıştırın (isteğe bağlı ikinci argüman, sunucu zaman dilimi varsayılan olarak kullanılır) ve karşılık gelen unıx zaman damgasını döndürür.
Tarih argümanı için: davranış belirtilmemiş.

**Sözdizimi**

``` sql
toUnixTimestamp(datetime)
toUnixTimestamp(str, [timezone])
```

**Döndürülen değer**

-   Unix zaman damgasını döndürür.

Tür: `UInt32`.

**Örnek**

Sorgu:

``` sql
SELECT toUnixTimestamp('2017-11-05 08:07:47', 'Asia/Tokyo') AS unix_timestamp
```

Sonuç:

``` text
┌─unix_timestamp─┐
│     1509836867 │
└────────────────┘
```

## toStartOfYear {#tostartofyear}

Yılın ilk gününe kadar bir tarih veya tarih aşağı yuvarlar.
Tarihi döndürür.

## toStartOfİSOYear {#tostartofisoyear}

ISO yılın ilk gününe kadar bir tarih veya tarih aşağı yuvarlar.
Tarihi döndürür.

## toStartOfQuarter {#tostartofquarter}

Çeyrek ilk güne kadar bir tarih veya tarih aşağı yuvarlar.
Çeyreğin ilk günü 1 Ocak, 1 Nisan, 1 Temmuz veya 1 ekim'dir.
Tarihi döndürür.

## toStartOfMonth {#tostartofmonth}

Ayın ilk gününe kadar bir tarih veya tarih aşağı yuvarlar.
Tarihi döndürür.

!!! attention "Dikkat"
    Yanlış tarihleri ayrıştırma davranışı uygulamaya özeldir. ClickHouse sıfır tarihi döndürebilir, bir istisna atabilir veya yapabilir “natural” taşmak.

## toMonday {#tomonday}

En yakın Pazartesi günü bir tarih veya tarih aşağı yuvarlar.
Tarihi döndürür.

## toStartOfWeek(t \[, mod\]) {#tostartofweektmode}

Modu ile en yakın pazar veya Pazartesi zaman bir tarih veya tarih aşağı yuvarlar.
Tarihi döndürür.
Mod bağımsız değişkeni, toWeek () için mod bağımsız değişkeni gibi çalışır. Tek bağımsız değişken sözdizimi için 0 mod değeri kullanılır.

## toStartOfDay {#tostartofday}

Günün başlangıcına kadar bir tarih aşağı yuvarlar.

## toStartOfHour {#tostartofhour}

Saat başlangıcına kadar bir tarih aşağı yuvarlar.

## toStartOfMinute {#tostartofminute}

Dakikanın başlangıcına kadar bir tarih aşağı yuvarlar.

## toStartOfFiveMinute {#tostartoffiveminute}

Beş dakikalık aralığın başlangıcına kadar bir tarih aşağı yuvarlar.

## toStartOfTenMinutes {#tostartoftenminutes}

On dakikalık aralığın başlangıcına kadar bir tarih aşağı yuvarlar.

## toStartOfFifteenMinutes {#tostartoffifteenminutes}

On beş dakikalık aralığın başlangıcına kadar tarih aşağı yuvarlar.

## toStartOfİnterval (time_or_data, Aralık x birimi \[, time_zone\]) {#tostartofintervaltime-or-data-interval-x-unit-time-zone}

Bu, diğer işlevlerin bir genellemesidir `toStartOf*`. Mesela,
`toStartOfInterval(t, INTERVAL 1 year)` aynı döndürür `toStartOfYear(t)`,
`toStartOfInterval(t, INTERVAL 1 month)` aynı döndürür `toStartOfMonth(t)`,
`toStartOfInterval(t, INTERVAL 1 day)` aynı döndürür `toStartOfDay(t)`,
`toStartOfInterval(t, INTERVAL 15 minute)` aynı döndürür `toStartOfFifteenMinutes(t)` vb.

## toTime {#totime}

Belirli bir sabit tarihe zaman ile bir tarih dönüştürür, zaman korurken.

## toRelativeYearNum {#torelativeyearnum}

Geçmişte belirli bir sabit noktadan başlayarak, yıl sayısına saat veya tarih ile bir tarih dönüştürür.

## toRelativeQuarterNum {#torelativequarternum}

Geçmişte belirli bir sabit noktadan başlayarak, çeyrek sayısına saat veya tarih ile bir tarih dönüştürür.

## toRelativeMonthNum {#torelativemonthnum}

Geçmişte belirli bir sabit noktadan başlayarak, Ayın sayısına saat veya tarih ile bir tarih dönüştürür.

## toRelativeWeekNum {#torelativeweeknum}

Geçmişte belirli bir sabit noktadan başlayarak, haftanın sayısına saat veya tarih ile bir tarih dönüştürür.

## toRelativeDayNum {#torelativedaynum}

Geçmişte belirli bir sabit noktadan başlayarak, günün sayısına saat veya tarih ile bir tarih dönüştürür.

## toRelativeHourNum {#torelativehournum}

Geçmişte belirli bir sabit noktadan başlayarak, saat veya tarih ile bir tarih saat sayısına dönüştürür.

## toRelativeMinuteNum {#torelativeminutenum}

Geçmişte belirli bir sabit noktadan başlayarak, dakika sayısına saat veya tarih ile bir tarih dönüştürür.

## toRelativeSecondNum {#torelativesecondnum}

Geçmişte belirli bir sabit noktadan başlayarak, ikinci sayısına saat veya tarih ile bir tarih dönüştürür.

## toİSOYear {#toisoyear}

ISO yıl numarasını içeren bir uınt16 numarasına bir tarih veya tarih zaman dönüştürür.

## toİSOWeek {#toisoweek}

ISO hafta numarasını içeren bir uınt8 numarasına bir tarih veya tarih zaman dönüştürür.

## toWeek (tarih \[, mod\]) {#toweekdatemode}

Bu işlev, date veya datetime için hafta numarasını döndürür. ToWeek () ' in iki bağımsız değişkenli formu, haftanın pazar veya Pazartesi günü başlayıp başlamadığını ve dönüş değerinin 0 ile 53 arasında mı yoksa 1 ile 53 arasında mı olması gerektiğini belirlemenizi sağlar. Mod bağımsız değişkeni atlanırsa, varsayılan mod 0'dır.
`toISOWeek()`eşdeğer bir uyumluluk işlevidir `toWeek(date,3)`.
Aşağıdaki tabloda mod bağımsız değişkeni nasıl çalıştığını açıklar.

| Modu | Haftanın ilk günü | Aralık | Week 1 is the first week …       |
|------|-------------------|--------|----------------------------------|
| 0    | Pazar             | 0-53   | bu yıl bir pazar günü ile        |
| 1    | Pazartesi         | 0-53   | bu yıl 4 veya daha fazla gün ile |
| 2    | Pazar             | 1-53   | bu yıl bir pazar günü ile        |
| 3    | Pazartesi         | 1-53   | bu yıl 4 veya daha fazla gün ile |
| 4    | Pazar             | 0-53   | bu yıl 4 veya daha fazla gün ile |
| 5    | Pazartesi         | 0-53   | bu yıl bir Pazartesi ile         |
| 6    | Pazar             | 1-53   | bu yıl 4 veya daha fazla gün ile |
| 7    | Pazartesi         | 1-53   | bu yıl bir Pazartesi ile         |
| 8    | Pazar             | 1-53   | 1 Ocak içerir                    |
| 9    | Pazartesi         | 1-53   | 1 Ocak içerir                    |

Bir anlamı olan mod değerleri için “with 4 or more days this year,” haftalar ISO 8601: 1988'e göre numaralandırılmıştır:

-   1 Ocak içeren haftanın yeni yılda 4 veya daha fazla günü varsa, 1. haftadır.

-   Aksi takdirde, bir önceki yılın son haftasıdır ve bir sonraki hafta 1. haftadır.

Bir anlamı olan mod değerleri için “contains January 1”, 1 Ocak haftanın 1.haft .asıdır. Haftanın yeni yılda kaç gün içerdiği önemli değil, sadece bir gün içerse bile.

``` sql
toWeek(date, [, mode][, Timezone])
```

**Parametre**

-   `date` – Date or DateTime.
-   `mode` – Optional parameter, Range of values is \[0,9\], default is 0.
-   `Timezone` – Optional parameter, it behaves like any other conversion function.

**Örnek**

``` sql
SELECT toDate('2016-12-27') AS date, toWeek(date) AS week0, toWeek(date,1) AS week1, toWeek(date,9) AS week9;
```

``` text
┌───────date─┬─week0─┬─week1─┬─week9─┐
│ 2016-12-27 │    52 │    52 │     1 │
└────────────┴───────┴───────┴───────┘
```

## toYearWeek (tarih \[, mod\]) {#toyearweekdatemode}

Bir tarih için yıl ve hafta döndürür. Sonuçtaki yıl, yılın ilk ve son haftası için tarih argümanındaki yıldan farklı olabilir.

Mod bağımsız değişkeni, toWeek () için mod bağımsız değişkeni gibi çalışır. Tek bağımsız değişken sözdizimi için 0 mod değeri kullanılır.

`toISOYear()`eşdeğer bir uyumluluk işlevidir `intDiv(toYearWeek(date,3),100)`.

**Örnek**

``` sql
SELECT toDate('2016-12-27') AS date, toYearWeek(date) AS yearWeek0, toYearWeek(date,1) AS yearWeek1, toYearWeek(date,9) AS yearWeek9;
```

``` text
┌───────date─┬─yearWeek0─┬─yearWeek1─┬─yearWeek9─┐
│ 2016-12-27 │    201652 │    201652 │    201701 │
└────────────┴───────────┴───────────┴───────────┘
```

## şimdi {#now}

Sıfır bağımsız değişkeni kabul eder ve geçerli saati istek yürütme anlarından birinde döndürür.
Bu işlev, isteğin tamamlanması uzun zaman alsa bile bir sabit döndürür.

## bugünkü {#today}

Sıfır bağımsız değişkeni kabul eder ve geçerli tarihi, istek yürütme anlarından birinde döndürür.
Olarak aynı ‘toDate(now())’.

## dün {#yesterday}

Sıfır bağımsız değişkeni kabul eder ve istek yürütme anlarından birinde dünün tarihini döndürür.
Olarak aynı ‘today() - 1’.

## zaman dilimi {#timeslot}

Yarım saat için zaman yuvarlar.
Bu fonksiyon (kayıt olmak için özeldir.Metrica, yarım saat, bir izleme etiketi, tek bir kullanıcının ardışık sayfa görüntülemelerini, zaman içinde bu miktardan kesinlikle daha fazla farklılık gösteriyorsa, bir oturumu iki oturuma bölmek için minimum zaman miktarıdır. Bu, ilgili oturumda bulunan sayfa görüntülemelerini aramak için tuples (etiket kimliği, kullanıcı kimliği ve zaman dilimi) kullanılabileceği anlamına gelir.

## toYYYYMM {#toyyyymm}

Bir tarih veya tarih ile saat, yıl ve ay numarasını (YYYY \* 100 + MM) içeren bir Uınt32 numarasına dönüştürür.

## toYYYYMMDD {#toyyyymmdd}

Bir tarih veya tarih ile saat, yıl ve ay numarasını içeren bir Uınt32 numarasına dönüştürür (YYYY \* 10000 + MM \* 100 + DD).

## toYYYYMMDDhhmmss {#toyyyymmddhhmmss}

Bir tarihi veya tarihi, yıl ve ay numarasını içeren bir Uınt64 numarasına dönüştürür (YYYY \* 1000000 + MM \* 1000000 + DD \* 1000000 + hh \* 10000 + mm \* 100 + ss).

## addYears, addMonths, addWeeks, addDays, addHours, addMinutes, addSeconds, addQuarters {#addyears-addmonths-addweeks-adddays-addhours-addminutes-addseconds-addquarters}

İşlev, bir tarih/DateTime aralığına bir tarih/DateTime ekler ve ardından Tarih/Datetime'ı döndürür. Mesela:

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

## subtractYears, subtractMonths, subtractWeeks, subtractDays, subtractHours, subtractMinutes, subtractSeconds, subtractQuarters {#subtractyears-subtractmonths-subtractweeks-subtractdays-subtracthours-subtractminutes-subtractseconds-subtractquarters}

Fonksiyon bir tarih/DateTime aralığını bir tarih/DateTime olarak çıkarır ve ardından Tarih/Datetime'ı döndürür. Mesela:

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

İki Date veya DateTime değerleri arasındaki farkı döndürür.

**Sözdizimi**

``` sql
dateDiff('unit', startdate, enddate, [timezone])
```

**Parametre**

-   `unit` — Time unit, in which the returned value is expressed. [Dize](../syntax.md#syntax-string-literal).

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

-   `startdate` — The first time value to compare. [Tarihli](../../sql-reference/data-types/date.md) veya [DateTime](../../sql-reference/data-types/datetime.md).

-   `enddate` — The second time value to compare. [Tarihli](../../sql-reference/data-types/date.md) veya [DateTime](../../sql-reference/data-types/datetime.md).

-   `timezone` — Optional parameter. If specified, it is applied to both `startdate` ve `enddate`. Belirtilmemişse, saat dilimleri `startdate` ve `enddate` kullanılır. Aynı değilse, sonuç belirtilmemiştir.

**Döndürülen değer**

Arasındaki fark `startdate` ve `enddate` ifade edilen `unit`.

Tür: `int`.

**Örnek**

Sorgu:

``` sql
SELECT dateDiff('hour', toDateTime('2018-01-01 22:00:00'), toDateTime('2018-01-02 23:00:00'));
```

Sonuç:

``` text
┌─dateDiff('hour', toDateTime('2018-01-01 22:00:00'), toDateTime('2018-01-02 23:00:00'))─┐
│                                                                                     25 │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

## (StartTime, Süresi,\[, Boyutu zaman yuvasının\]) {#timeslotsstarttime-duration-size}

Başlayan bir zaman aralığı için ‘StartTime’ ve devam etmek için ‘Duration’ saniye, bu aralıktan aşağı yuvarlanan noktalardan oluşan zaman içinde bir dizi moment döndürür ‘Size’ saniyeler içinde. ‘Size’ isteğe bağlı bir parametredir: varsayılan olarak 1800 olarak ayarlanmış bir sabit Uİnt32.
Mesela, `timeSlots(toDateTime('2012-01-01 12:20:00'), 600) = [toDateTime('2012-01-01 12:00:00'), toDateTime('2012-01-01 12:30:00')]`.
Bu, ilgili oturumda sayfa görüntülemelerini aramak için gereklidir.

## formatDateTime (saat, Biçim \[, Saat Dilimi\]) {#formatdatetime}

Function formats a Time according given Format string. N.B.: Format is a constant expression, e.g. you can not have multiple formats for single result column.

Biçim için desteklenen değiştiriciler:
(“Example” sütun, zaman için biçimlendirme sonucunu gösterir `2018-01-02 22:33:44`)

| Değiştirici | Açıklama                                                 | Örnek      |
|-------------|----------------------------------------------------------|------------|
| %C          | yıl 100'e bölünür ve tamsayıya kesilir (00-99)           | 20         |
| %d          | Ayın günü, sıfır yastıklı (01-31)                        | 02         |
| %D          | Kısa MM/DD/YY tarih, eşdeğer %m / %d / % y               | 01/02/18   |
| %e          | Ayın günü, boşluk dolgulu (1-31)                         | 2          |
| %F          | kısa YYYY-AA-DD tarih, eşdeğer %Y-%m - %d                | 2018-01-02 |
| %H          | 24 saat formatında saat (00-23)                          | 22         |
| %I          | 12h formatında saat (01-12)                              | 10         |
| %j          | yılın günü (001-366)                                     | 002        |
| %metre      | ondalık sayı olarak ay (01-12)                           | 01         |
| %M          | dakika (00-59)                                           | 33         |
| %ve         | new-line char (ac (ter (")                               |            |
| %p          | AM veya PM atama                                         | PM         |
| %R          | 24-hour HH: MM Zaman, eşdeğer %H:%M                      | 22:33      |
| %S          | ikinci (00-59)                                           | 44         |
| %t          | yatay-sekme karakteri (')                                |            |
| %T          | ISO 8601 saat biçimi (HH:MM:SS), eşdeğer %H:%M: % S      | 22:33:44   |
| %u          | ISO 8601 hafta içi sayı olarak Pazartesi olarak 1 (1-7)  | 2          |
| %V          | ISO 8601 hafta numarası (01-53)                          | 01         |
| %g          | Pazar günü 0 (0-6) olarak ondalık sayı olarak hafta içi) | 2          |
| %y          | Yıl, son iki basamak (00-99)                             | 18         |
| %Y          | Yıllık                                                   | 2018       |
| %%          | im                                                       | %          |

[Orijinal makale](https://clickhouse.tech/docs/en/query_language/functions/date_time_functions/) <!--hide-->
