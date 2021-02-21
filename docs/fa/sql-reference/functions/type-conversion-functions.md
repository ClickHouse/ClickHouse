---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 38
toc_title: "\u062A\u0628\u062F\u06CC\u0644 \u0646\u0648\u0639"
---

# توابع تبدیل نوع {#type-conversion-functions}

## مشکلات متداول تبدیل عددی {#numeric-conversion-issues}

هنگامی که شما یک مقدار تبدیل از یک به نوع داده های دیگر, شما باید به یاد داشته باشید که در مورد مشترک, این یک عملیات نا امن است که می تواند به از دست دادن داده ها منجر شود. از دست دادن داده ها می تواند رخ دهد اگر شما سعی می کنید به جا ارزش از یک نوع داده بزرگتر به یک نوع داده کوچکتر, و یا اگر شما ارزش بین انواع داده های مختلف تبدیل.

کلیک کرده است [همان رفتار به عنوان ج++ برنامه](https://en.cppreference.com/w/cpp/language/implicit_conversion).

## toInt(8/16/32/64) {#toint8163264}

تبدیل یک مقدار ورودی به [Int](../../sql-reference/data-types/int-uint.md) نوع داده. این خانواده تابع شامل:

-   `toInt8(expr)` — Results in the `Int8` نوع داده.
-   `toInt16(expr)` — Results in the `Int16` نوع داده.
-   `toInt32(expr)` — Results in the `Int32` نوع داده.
-   `toInt64(expr)` — Results in the `Int64` نوع داده.

**پارامترها**

-   `expr` — [عبارت](../syntax.md#syntax-expressions) بازگشت یک عدد یا یک رشته با نمایش اعشاری یک عدد. دودویی, مبنای هشت, و بازنمایی هگزادسیمال از اعداد پشتیبانی نمی شوند. صفر منجر محروم هستند.

**مقدار بازگشتی**

مقدار صحیح در `Int8`, `Int16`, `Int32` یا `Int64` نوع داده.

توابع استفاده [گرد کردن به سمت صفر](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero) یعنی عدد کسری از اعداد را کوتاه می کنند.

رفتار توابع برای [هشدار داده می شود](../../sql-reference/data-types/float.md#data_type-float-nan-inf) استدلال تعریف نشده است. به یاد داشته باشید در مورد [مسایل همگرایی عددی](#numeric-conversion-issues), هنگام استفاده از توابع.

**مثال**

``` sql
SELECT toInt64(nan), toInt32(32), toInt16('16'), toInt8(8.8)
```

``` text
┌─────────toInt64(nan)─┬─toInt32(32)─┬─toInt16('16')─┬─toInt8(8.8)─┐
│ -9223372036854775808 │          32 │            16 │           8 │
└──────────────────────┴─────────────┴───────────────┴─────────────┘
```

## toInt(8/16/32/64)OrZero {#toint8163264orzero}

این استدلال از نوع رشته طول می کشد و تلاش می کند تا به اعضای هیات تجزیه (8 \| 16 \| 32 \| 64). اگر شکست خورده, بازده 0.

**مثال**

``` sql
select toInt64OrZero('123123'), toInt8OrZero('123qwe123')
```

``` text
┌─toInt64OrZero('123123')─┬─toInt8OrZero('123qwe123')─┐
│                  123123 │                         0 │
└─────────────────────────┴───────────────────────────┘
```

## toInt(8/16/32/64)OrNull {#toint8163264ornull}

این استدلال از نوع رشته طول می کشد و تلاش می کند تا به اعضای هیات تجزیه (8 \| 16 \| 32 \| 64). اگر شکست خورده, تهی گرداند.

**مثال**

``` sql
select toInt64OrNull('123123'), toInt8OrNull('123qwe123')
```

``` text
┌─toInt64OrNull('123123')─┬─toInt8OrNull('123qwe123')─┐
│                  123123 │                      ᴺᵁᴸᴸ │
└─────────────────────────┴───────────────────────────┘
```

## toUInt(8/16/32/64) {#touint8163264}

تبدیل یک مقدار ورودی به [اینترنت](../../sql-reference/data-types/int-uint.md) نوع داده. این خانواده تابع شامل:

-   `toUInt8(expr)` — Results in the `UInt8` نوع داده.
-   `toUInt16(expr)` — Results in the `UInt16` نوع داده.
-   `toUInt32(expr)` — Results in the `UInt32` نوع داده.
-   `toUInt64(expr)` — Results in the `UInt64` نوع داده.

**پارامترها**

-   `expr` — [عبارت](../syntax.md#syntax-expressions) بازگشت یک عدد یا یک رشته با نمایش اعشاری یک عدد. دودویی, مبنای هشت, و بازنمایی هگزادسیمال از اعداد پشتیبانی نمی شوند. صفر منجر محروم هستند.

**مقدار بازگشتی**

مقدار صحیح در `UInt8`, `UInt16`, `UInt32` یا `UInt64` نوع داده.

توابع استفاده [گرد کردن به سمت صفر](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero) یعنی عدد کسری از اعداد را کوتاه می کنند.

رفتار توابع برای کشاورزی منفی و برای [هشدار داده می شود](../../sql-reference/data-types/float.md#data_type-float-nan-inf) استدلال تعریف نشده است. اگر شما یک رشته عبور با تعداد منفی, مثلا `'-32'`, خانه را افزایش می دهد یک استثنا. به یاد داشته باشید در مورد [مسایل همگرایی عددی](#numeric-conversion-issues), هنگام استفاده از توابع.

**مثال**

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

## توفلوات (32/64) {#tofloat3264}

## toFloat(32/64)OrZero {#tofloat3264orzero}

## toFloat(32/64)OrNull {#tofloat3264ornull}

## toDate {#todate}

## تودارزرو {#todateorzero}

## طول عمر {#todateornull}

## toDateTime {#todatetime}

## به اندازه تو {#todatetimeorzero}

## طول عمر ترنول {#todatetimeornull}

## toDecimal(32/64/128) {#todecimal3264128}

تبدیل `value` به [دهدهی](../../sql-reference/data-types/decimal.md) نوع داده با دقت `S`. این `value` می تواند یک عدد یا یک رشته. این `S` (مقیاس) پارامتر تعداد رقم اعشار را مشخص می کند.

-   `toDecimal32(value, S)`
-   `toDecimal64(value, S)`
-   `toDecimal128(value, S)`

## toDecimal(32/64/128)OrNull {#todecimal3264128ornull}

تبدیل یک رشته ورودی به یک [Nullable(اعشاری(P,S))](../../sql-reference/data-types/decimal.md) مقدار نوع داده. این خانواده از توابع عبارتند از:

-   `toDecimal32OrNull(expr, S)` — Results in `Nullable(Decimal32(S))` نوع داده.
-   `toDecimal64OrNull(expr, S)` — Results in `Nullable(Decimal64(S))` نوع داده.
-   `toDecimal128OrNull(expr, S)` — Results in `Nullable(Decimal128(S))` نوع داده.

این توابع باید به جای استفاده `toDecimal*()` توابع, اگر شما ترجیح می دهید برای دریافت یک `NULL` ارزش به جای یک استثنا در صورت خطا تجزیه ارزش ورودی.

**پارامترها**

-   `expr` — [عبارت](../syntax.md#syntax-expressions), بازگرداندن یک مقدار در [رشته](../../sql-reference/data-types/string.md) نوع داده. تاتر انتظار نمایندگی متنی از عدد اعشاری. به عنوان مثال, `'1.111'`.
-   `S` — Scale, the number of decimal places in the resulting value.

**مقدار بازگشتی**

یک مقدار در `Nullable(Decimal(P,S))` نوع داده. مقدار شامل:

-   شماره با `S` اعشار, اگر تاتر تفسیر رشته ورودی به عنوان یک عدد.
-   `NULL`, اگر تاتر می توانید رشته ورودی به عنوان یک عدد تفسیر نمی کند و یا اگر تعداد ورودی شامل بیش از `S` رقم اعشار.

**مثالها**

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

تبدیل یک مقدار ورودی به [دهدهی)](../../sql-reference/data-types/decimal.md) نوع داده. این خانواده از توابع عبارتند از:

-   `toDecimal32OrZero( expr, S)` — Results in `Decimal32(S)` نوع داده.
-   `toDecimal64OrZero( expr, S)` — Results in `Decimal64(S)` نوع داده.
-   `toDecimal128OrZero( expr, S)` — Results in `Decimal128(S)` نوع داده.

این توابع باید به جای استفاده `toDecimal*()` توابع, اگر شما ترجیح می دهید برای دریافت یک `0` ارزش به جای یک استثنا در صورت خطا تجزیه ارزش ورودی.

**پارامترها**

-   `expr` — [عبارت](../syntax.md#syntax-expressions), بازگرداندن یک مقدار در [رشته](../../sql-reference/data-types/string.md) نوع داده. تاتر انتظار نمایندگی متنی از عدد اعشاری. به عنوان مثال, `'1.111'`.
-   `S` — Scale, the number of decimal places in the resulting value.

**مقدار بازگشتی**

یک مقدار در `Nullable(Decimal(P,S))` نوع داده. مقدار شامل:

-   شماره با `S` اعشار, اگر تاتر تفسیر رشته ورودی به عنوان یک عدد.
-   0 با `S` رقم اعشار, اگر تاتر می توانید رشته ورودی به عنوان یک عدد تفسیر نمی کند و یا اگر تعداد ورودی شامل بیش از `S` رقم اعشار.

**مثال**

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

توابع برای تبدیل بین اعداد, رشته ها (اما رشته ثابت نیست), تاریخ, و تاریخ با زمان.
همه این توابع قبول یک استدلال.

در هنگام تبدیل به و یا از یک رشته, ارزش فرمت شده و یا تجزیه با استفاده از قوانین مشابه برای فرمت جدولبندیشده (و تقریبا تمام فرمت های متنی دیگر). اگر رشته را نمی توان تجزیه, یک استثنا پرتاب می شود و درخواست لغو شده است.

هنگام تبدیل تاریخ به اعداد و یا بالعکس, تاریخ مربوط به تعداد روز از ابتدای عصر یونیکس.
هنگام تبدیل تاریخ با بار به اعداد و یا بالعکس, تاریخ با زمان مربوط به تعداد ثانیه از ابتدای عصر یونیکس.

تاریخ و تاریخ-با-فرمت زمان برای toDate/toDateTime توابع تعریف شده به شرح زیر است:

``` text
YYYY-MM-DD
YYYY-MM-DD hh:mm:ss
```

به عنوان یک استثنا اگر تبدیل از UInt32, Int32, UInt64 یا Int64 عددی انواع, به, تاریخ, و اگر عدد بزرگتر یا مساوی به 65536 تعدادی را به عنوان تفسیر یک زمان یونیکس (و نه به عنوان تعداد روز) و گرد است به تاریخ. این اجازه می دهد تا پشتیبانی از وقوع مشترک نوشتن ‘toDate(unix\_timestamp)’ که در غیر این صورت یک خطا خواهد بود و نیاز به نوشتن بیشتر دست و پا گیر ‘toDate(toDateTime(unix\_timestamp))’.

تبدیل بین تاریخ و تاریخ با زمان انجام شده است راه طبیعی: با اضافه کردن یک زمان خالی و یا حذف زمان.

تبدیل بین انواع عددی با استفاده از قوانین مشابه به عنوان تکالیف بین انواع مختلف عددی در ج++.

علاوه بر این, تابع حول از استدلال حسگر ناحیه رنگی می توانید یک استدلال رشته دوم حاوی نام منطقه زمانی را. مثال: `Asia/Yekaterinburg` در این مورد, زمان با توجه به منطقه زمانی مشخص فرمت.

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

همچنین `toUnixTimestamp` تابع.

## وضعیت زیستشناختی رکورد) {#tofixedstrings-n}

تبدیل یک استدلال نوع رشته به یک رشته (نفر) نوع (یک رشته با طول ثابت نفر). نفر باید ثابت باشد.
اگر رشته دارای بایت کمتر از نفر, این است که با بایت پوچ به سمت راست خالی. اگر رشته دارای بایت بیش از نفر, یک استثنا پرتاب می شود.

## در حال بارگذاری) {#tostringcuttozeros}

می پذیرد یک رشته یا رشته ثابت استدلال. بازگرداندن رشته با محتوای کوتاه در اولین صفر بایت پیدا شده است.

مثال:

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

## reinterpretAsInt(8/16/32/64) {#reinterpretasint8163264}

## تفسیر مجدد (32/64) {#reinterpretasfloat3264}

## بازخوانی مجدد {#reinterpretasdate}

## حسگر ناحیه رنگی {#reinterpretasdatetime}

این توابع یک رشته را قبول می کنند و بایت هایی را که در ابتدای رشته قرار می گیرند به عنوان یک عدد در دستور میزبان (اندی کوچک) تفسیر می کنند. اگر رشته به اندازه کافی بلند نیست, توابع کار به عنوان اگر رشته با تعداد لازم از بایت پوچ خالی. اگر رشته طولانی تر از مورد نیاز است, بایت اضافی نادیده گرفته می شوند. تاریخ به عنوان تعداد روز از ابتدای عصر یونیکس تفسیر و تاریخ با زمان به عنوان تعداد ثانیه از ابتدای عصر یونیکس تفسیر شده است.

## رشته مجدد {#type_conversion_functions-reinterpretAsString}

این تابع یک شماره یا تاریخ و یا تاریخ با زمان می پذیرد, و یک رشته حاوی بایت به نمایندگی از ارزش مربوطه را در سفارش میزبان را برمی گرداند (اندی کمی). بایت پوچ از پایان کاهش یافته است. مثلا, ارزش نوع اوینت32 255 یک رشته است که یک بایت طولانی است.

## رشته مجدد {#reinterpretasfixedstring}

این تابع یک شماره یا تاریخ و یا تاریخ با زمان می پذیرد, و یک رشته ثابت حاوی بایت به نمایندگی از ارزش مربوطه را در سفارش میزبان را برمی گرداند (اندی کمی). بایت پوچ از پایان کاهش یافته است. مثلا, ارزش نوع اوینت32 255 رشته ثابت است که یک بایت طولانی است.

## بازیگران(x, T) {#type_conversion_function-cast}

تبدیل ‘x’ به ‘t’ نوع داده. بازیگران نحو (ایکس به عنوان تی) نیز پشتیبانی می کند.

مثال:

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

تبدیل به FixedString(N) تنها با این نسخهها کار برای استدلال از نوع String یا FixedString(N).

تبدیل نوع به [Nullable](../../sql-reference/data-types/nullable.md) و پشت پشتیبانی می شود. مثال:

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

## توینتال (سال / سه ماهه / ماه / هفته / روز / ساعت / دقیقه / ثانیه) {#function-tointerval}

تبدیل یک استدلال نوع شماره به یک [فاصله](../../sql-reference/data-types/special-data-types/interval.md) نوع داده.

**نحو**

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

**پارامترها**

-   `number` — Duration of interval. Positive integer number.

**مقادیر بازگشتی**

-   مقدار در `Interval` نوع داده.

**مثال**

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

## پردازش زمان {#parsedatetimebesteffort}

تبدیل یک تاریخ و زمان در [رشته](../../sql-reference/data-types/string.md) نمایندگی به [DateTime](../../sql-reference/data-types/datetime.md#data_type-datetime) نوع داده.

تابع تجزیه می کند [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601), [1123-5.2.14 تومان-822 تاریخ و زمان مشخصات](https://tools.ietf.org/html/rfc1123#page-55), را کلیک کنید و برخی از فرمت های تاریخ و زمان دیگر.

**نحو**

``` sql
parseDateTimeBestEffort(time_string [, time_zone]);
```

**پارامترها**

-   `time_string` — String containing a date and time to convert. [رشته](../../sql-reference/data-types/string.md).
-   `time_zone` — Time zone. The function parses `time_string` با توجه به منطقه زمانی. [رشته](../../sql-reference/data-types/string.md).

**فرمت های غیر استاندارد پشتیبانی شده**

-   یک رشته حاوی 9..10 رقمی [برچسب زمان یونیکس](https://en.wikipedia.org/wiki/Unix_time).
-   یک رشته با یک تاریخ و یک مولفه زمان: `YYYYMMDDhhmmss`, `DD/MM/YYYY hh:mm:ss`, `DD-MM-YY hh:mm`, `YYYY-MM-DD hh:mm:ss`, و غیره.
-   یک رشته با یک تاریخ, اما هیچ مولفه زمان: `YYYY`, `YYYYMM`, `YYYY*MM`, `DD/MM/YYYY`, `DD-MM-YY` و غیره
-   یک رشته با یک روز و زمان: `DD`, `DD hh`, `DD hh:mm`. در این مورد `YYYY-MM` به عنوان جایگزین `2000-01`.
-   یک رشته است که شامل تاریخ و زمان همراه با منطقه زمانی اطلاعات افست: `YYYY-MM-DD hh:mm:ss ±h:mm`, و غیره. به عنوان مثال, `2020-12-12 17:36:00 -5:00`.

برای همه فرمت های با جدا تابع تجزیه نام ماه بیان شده توسط نام کامل خود و یا با سه حرف اول یک نام ماه. مثالها: `24/DEC/18`, `24-Dec-18`, `01-September-2018`.

**مقدار بازگشتی**

-   `time_string` تبدیل به `DateTime` نوع داده.

**مثالها**

پرسوجو:

``` sql
SELECT parseDateTimeBestEffort('12/12/2020 12:12:57')
AS parseDateTimeBestEffort;
```

نتیجه:

``` text
┌─parseDateTimeBestEffort─┐
│     2020-12-12 12:12:57 │
└─────────────────────────┘
```

پرسوجو:

``` sql
SELECT parseDateTimeBestEffort('Sat, 18 Aug 2018 07:22:16 GMT', 'Europe/Moscow')
AS parseDateTimeBestEffort
```

نتیجه:

``` text
┌─parseDateTimeBestEffort─┐
│     2018-08-18 10:22:16 │
└─────────────────────────┘
```

پرسوجو:

``` sql
SELECT parseDateTimeBestEffort('1284101485')
AS parseDateTimeBestEffort
```

نتیجه:

``` text
┌─parseDateTimeBestEffort─┐
│     2015-07-07 12:04:41 │
└─────────────────────────┘
```

پرسوجو:

``` sql
SELECT parseDateTimeBestEffort('2018-12-12 10:12:12')
AS parseDateTimeBestEffort
```

نتیجه:

``` text
┌─parseDateTimeBestEffort─┐
│     2018-12-12 10:12:12 │
└─────────────────────────┘
```

پرسوجو:

``` sql
SELECT parseDateTimeBestEffort('10 20:19')
```

نتیجه:

``` text
┌─parseDateTimeBestEffort('10 20:19')─┐
│                 2000-01-10 20:19:00 │
└─────────────────────────────────────┘
```

**همچنین نگاه کنید به**

-   \[ایزو 8601 اطلاعیه توسط @xkcd\] (https://xkcd.com/1179/)
-   [RFC 1123](https://tools.ietf.org/html/rfc1123)
-   [toDate](#todate)
-   [toDateTime](#todatetime)

## - ترجمه نشده - {#parsedatetimebesteffortornull}

همان است که برای [پردازش زمان](#parsedatetimebesteffort) با این تفاوت که وقتی با فرمت تاریخ مواجه می شود که نمی تواند پردازش شود تهی می شود.

## - ترجمه نشده - {#parsedatetimebesteffortorzero}

همان است که برای [پردازش زمان](#parsedatetimebesteffort) با این تفاوت که تاریخ صفر یا زمان صفر را باز می گرداند زمانی که یک فرمت تاریخ مواجه است که نمی تواند پردازش شود.

[مقاله اصلی](https://clickhouse.tech/docs/en/query_language/functions/type_conversion_functions/) <!--hide-->
