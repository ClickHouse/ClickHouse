---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 66
toc_title: "\u063A\u06CC\u0631\u0647"
---

# توابع دیگر {#other-functions}

## نام میزبان() {#hostname}

بازگرداندن یک رشته با نام میزبان که این تابع در انجام شد. برای پردازش توزیع شده, این نام میزبان سرور از راه دور است, اگر تابع بر روی یک سرور از راه دور انجام.

## گدماکرو {#getmacro}

می شود یک مقدار به نام از [& کلاندارها](../../operations/server-configuration-parameters/settings.md#macros) بخش پیکربندی سرور.

**نحو**

``` sql
getMacro(name);
```

**پارامترها**

-   `name` — Name to retrieve from the `macros` بخش. [رشته](../../sql-reference/data-types/string.md#string).

**مقدار بازگشتی**

-   ارزش ماکرو مشخص.

نوع: [رشته](../../sql-reference/data-types/string.md).

**مثال**

به عنوان مثال `macros` بخش در فایل پیکربندی سرور:

``` xml
<macros>
    <test>Value</test>
</macros>
```

پرسوجو:

``` sql
SELECT getMacro('test');
```

نتیجه:

``` text
┌─getMacro('test')─┐
│ Value            │
└──────────────────┘
```

یک راه جایگزین برای دریافت همان مقدار:

``` sql
SELECT * FROM system.macros
WHERE macro = 'test';
```

``` text
┌─macro─┬─substitution─┐
│ test  │ Value        │
└───────┴──────────────┘
```

## FQDN {#fqdn}

بازگرداندن نام دامنه به طور کامل واجد شرایط.

**نحو**

``` sql
fqdn();
```

این تابع غیر حساس است.

**مقدار بازگشتی**

-   رشته با نام دامنه به طور کامل واجد شرایط.

نوع: `String`.

**مثال**

پرسوجو:

``` sql
SELECT FQDN();
```

نتیجه:

``` text
┌─FQDN()──────────────────────────┐
│ clickhouse.ru-central1.internal │
└─────────────────────────────────┘
```

## basename {#basename}

عصاره قسمت انتهایی یک رشته پس از بریده بریده و یا ممیز گذشته. این تابع اگر اغلب مورد استفاده برای استخراج نام فایل از یک مسیر.

``` sql
basename( expr )
```

**پارامترها**

-   `expr` — Expression resulting in a [رشته](../../sql-reference/data-types/string.md) نوع ارزش. همه بک اسلش باید در ارزش حاصل فرار.

**مقدار بازگشتی**

یک رشته که شامل:

-   قسمت انتهایی یک رشته پس از بریده بریده و یا ممیز گذشته.

        If the input string contains a path ending with slash or backslash, for example, `/` or `c:\`, the function returns an empty string.

-   رشته اصلی اگر هیچ اسلش یا بک اسلش وجود دارد.

**مثال**

``` sql
SELECT 'some/long/path/to/file' AS a, basename(a)
```

``` text
┌─a──────────────────────┬─basename('some\\long\\path\\to\\file')─┐
│ some\long\path\to\file │ file                                   │
└────────────────────────┴────────────────────────────────────────┘
```

``` sql
SELECT 'some\\long\\path\\to\\file' AS a, basename(a)
```

``` text
┌─a──────────────────────┬─basename('some\\long\\path\\to\\file')─┐
│ some\long\path\to\file │ file                                   │
└────────────────────────┴────────────────────────────────────────┘
```

``` sql
SELECT 'some-file-name' AS a, basename(a)
```

``` text
┌─a──────────────┬─basename('some-file-name')─┐
│ some-file-name │ some-file-name             │
└────────────────┴────────────────────────────┘
```

## عریض) {#visiblewidthx}

محاسبه عرض تقریبی در هنگام خروجی ارزش به کنسول در قالب متن (تب از هم جدا).
این تابع توسط سیستم برای اجرای فرمت های زیبا استفاده می شود.

`NULL` به عنوان یک رشته مربوط به نمایندگی `NULL` داخل `Pretty` فرمتها.

``` sql
SELECT visibleWidth(NULL)
```

``` text
┌─visibleWidth(NULL)─┐
│                  4 │
└────────────────────┘
```

## نام کامل) {#totypenamex}

بازگرداندن یک رشته حاوی نام نوع استدلال گذشت.

اگر `NULL` به عنوان ورودی به عملکرد منتقل می شود و سپس باز می گردد `Nullable(Nothing)` نوع, که مربوط به داخلی `NULL` نمایندگی در فاحشه خانه.

## blockSize() {#function-blocksize}

می شود به اندازه بلوک.
در خانه, نمایش داده شد همیشه در بلوک های اجرا (مجموعه ای از قطعات ستون). این تابع اجازه می دهد تا اندازه بلوک را که شما برای نام برد دریافت کنید.

## تحقق (ایکس) {#materializex}

تبدیل ثابت به یک ستون کامل حاوی فقط یک مقدار.
در خانه, ستون کامل و ثابت متفاوت در حافظه نشان. توابع کار متفاوت برای استدلال ثابت و استدلال طبیعی (کد های مختلف اجرا شده است), اگر چه نتیجه این است که تقریبا همیشه همان. این تابع برای اشکال زدایی این رفتار.

## ignore(…) {#ignore}

می پذیرد هر استدلال, محتوی `NULL`. همیشه برمی گرداند 0.
با این حال, استدلال هنوز ارزیابی. این را می توان برای معیار استفاده می شود.

## خواب (ثانیه) {#sleepseconds}

خواب ‘seconds’ ثانیه در هر بلوک داده ها. شما می توانید یک عدد صحیح یا عدد ممیز شناور را مشخص کنید.

## خواب (ثانیه) {#sleepeachrowseconds}

خواب ‘seconds’ ثانیه در هر سطر. شما می توانید یک عدد صحیح یا عدد ممیز شناور را مشخص کنید.

## متن() {#currentdatabase}

بازگرداندن نام پایگاه داده فعلی.
شما می توانید این تابع در پارامترهای موتور جدول در یک پرس و جو جدول ایجاد جایی که شما نیاز به مشخص کردن پایگاه داده استفاده.

## currentUser() {#other-function-currentuser}

بازگرداندن ورود کاربر فعلی. ورود کاربر, که پرس و جو شروع, خواهد شد در پرس و جو مورد رقیق بازگشت.

``` sql
SELECT currentUser();
```

نام مستعار: `user()`, `USER()`.

**مقادیر بازگشتی**

-   ورود کاربر فعلی.
-   ورود کاربر که پرس و جو در صورت پرس و جو از کار افتاده است.

نوع: `String`.

**مثال**

پرسوجو:

``` sql
SELECT currentUser();
```

نتیجه:

``` text
┌─currentUser()─┐
│ default       │
└───────────────┘
```

## & ایستانت {#is-constant}

بررسی اینکه استدلال بیان ثابت است.

A constant expression means an expression whose resulting value is known at the query analysis (i.e. before execution). For example, expressions over [literals](../syntax.md#literals) عبارات ثابت هستند.

این تابع برای توسعه در نظر گرفته شده, اشکال زدایی و تظاهرات.

**نحو**

``` sql
isConstant(x)
```

**پارامترها**

-   `x` — Expression to check.

**مقادیر بازگشتی**

-   `1` — `x` ثابت است.
-   `0` — `x` غیر ثابت است.

نوع: [UInt8](../data-types/int-uint.md).

**مثالها**

پرسوجو:

``` sql
SELECT isConstant(x + 1) FROM (SELECT 43 AS x)
```

نتیجه:

``` text
┌─isConstant(plus(x, 1))─┐
│                      1 │
└────────────────────────┘
```

پرسوجو:

``` sql
WITH 3.14 AS pi SELECT isConstant(cos(pi))
```

نتیجه:

``` text
┌─isConstant(cos(pi))─┐
│                   1 │
└─────────────────────┘
```

پرسوجو:

``` sql
SELECT isConstant(number) FROM numbers(1)
```

نتیجه:

``` text
┌─isConstant(number)─┐
│                  0 │
└────────────────────┘
```

## اطلاعات دقیق) {#isfinitex}

قبول Float32 و Float64 و بازده UInt8 برابر با 1 اگر این استدلال بی نهایت است و نه یک نان در غیر این صورت 0 است.

## اطلاعات دقیق) {#isinfinitex}

قبول Float32 و Float64 و بازده UInt8 برابر با 1 اگر این استدلال بی نهایت است در غیر این صورت 0 است. توجه داشته باشید که 0 برای نان بازگشت.

## اطلاعات دقیق {#ifnotfinite}

بررسی اینکه مقدار ممیز شناور محدود است.

**نحو**

    ifNotFinite(x,y)

**پارامترها**

-   `x` — Value to be checked for infinity. Type: [شناور\*](../../sql-reference/data-types/float.md).
-   `y` — Fallback value. Type: [شناور\*](../../sql-reference/data-types/float.md).

**مقدار بازگشتی**

-   `x` اگر `x` محدود است.
-   `y` اگر `x` محدود نیست.

**مثال**

پرسوجو:

    SELECT 1/0 as infimum, ifNotFinite(infimum,42)

نتیجه:

    ┌─infimum─┬─ifNotFinite(divide(1, 0), 42)─┐
    │     inf │                            42 │
    └─────────┴───────────────────────────────┘

شما می توانید نتیجه مشابه با استفاده از [اپراتور سه تایی](conditional-functions.md#ternary-operator): `isFinite(x) ? x : y`.

## اطلاعات دقیق) {#isnanx}

قبول Float32 و Float64 و بازده UInt8 برابر با 1 اگر استدلال این است که یک نان در غیر این صورت 0 است.

## قابل تنظیم(\[‘hostname’\[, ‘username’\[, ‘password’\]\],\] ‘database’, ‘table’, ‘column’) {#hascolumnintablehostname-username-password-database-table-column}

می پذیرد رشته ثابت: نام پایگاه داده, نام جدول, و نام ستون. بازگرداندن یک بیان ثابت سنت8 برابر 1 اگر یک ستون وجود دارد, در غیر این صورت 0. اگر پارامتر نام میزبان تنظیم شده است, تست بر روی یک سرور از راه دور اجرا خواهد شد.
تابع می اندازد یک استثنا اگر جدول وجود ندارد.
برای عناصر در یک ساختار داده های تو در تو, تابع چک برای وجود یک ستون. برای ساختار داده های تو در تو خود, بازده تابع 0.

## بار {#function-bar}

اجازه می دهد تا ساخت یک نمودار یونیکد هنر.

`bar(x, min, max, width)` تساوی یک گروه با عرض متناسب با `(x - min)` و برابر با `width` شخصیت زمانی که `x = max`.

پارامترها:

-   `x` — Size to display.
-   `min, max` — Integer constants. The value must fit in `Int64`.
-   `width` — Constant, positive integer, can be fractional.

گروه با دقت به یک هشتم نماد کشیده شده است.

مثال:

``` sql
SELECT
    toHour(EventTime) AS h,
    count() AS c,
    bar(c, 0, 600000, 20) AS bar
FROM test.hits
GROUP BY h
ORDER BY h ASC
```

``` text
┌──h─┬──────c─┬─bar────────────────┐
│  0 │ 292907 │ █████████▋         │
│  1 │ 180563 │ ██████             │
│  2 │ 114861 │ ███▋               │
│  3 │  85069 │ ██▋                │
│  4 │  68543 │ ██▎                │
│  5 │  78116 │ ██▌                │
│  6 │ 113474 │ ███▋               │
│  7 │ 170678 │ █████▋             │
│  8 │ 278380 │ █████████▎         │
│  9 │ 391053 │ █████████████      │
│ 10 │ 457681 │ ███████████████▎   │
│ 11 │ 493667 │ ████████████████▍  │
│ 12 │ 509641 │ ████████████████▊  │
│ 13 │ 522947 │ █████████████████▍ │
│ 14 │ 539954 │ █████████████████▊ │
│ 15 │ 528460 │ █████████████████▌ │
│ 16 │ 539201 │ █████████████████▊ │
│ 17 │ 523539 │ █████████████████▍ │
│ 18 │ 506467 │ ████████████████▊  │
│ 19 │ 520915 │ █████████████████▎ │
│ 20 │ 521665 │ █████████████████▍ │
│ 21 │ 542078 │ ██████████████████ │
│ 22 │ 493642 │ ████████████████▍  │
│ 23 │ 400397 │ █████████████▎     │
└────┴────────┴────────────────────┘
```

## تبدیل {#transform}

تبدیل یک ارزش با توجه به نقشه برداری به صراحت تعریف شده از برخی از عناصر به دیگر.
دو نوع از این تابع وجود دارد:

### تبدیل(x array\_from, array\_to به طور پیش فرض) {#transformx-array-from-array-to-default}

`x` – What to transform.

`array_from` – Constant array of values for converting.

`array_to` – Constant array of values to convert the values in ‘from’ به.

`default` – Which value to use if ‘x’ برابر است با هر یک از مقادیر در ‘from’.

`array_from` و `array_to` – Arrays of the same size.

انواع:

`transform(T, Array(T), Array(U), U) -> U`

`T` و `U` می تواند عددی, رشته,یا تاریخ و یا انواع تاریخ ساعت.
از کجا همان نامه نشان داده شده است (تی یا تو), برای انواع عددی این ممکن است تطبیق انواع, اما انواع که یک نوع رایج.
برای مثال استدلال می توانید نوع Int64 در حالی که دوم آرایه(UInt16) نوع.

اگر ‘x’ ارزش به یکی از عناصر در برابر است ‘array\_from’ مجموعه, این بازگرداندن عنصر موجود (که شماره همان) از ‘array\_to’ صف کردن. در غیر این صورت, باز می گردد ‘default’. اگر عناصر تطبیق های متعدد در وجود دارد ‘array\_from’ این یکی از مسابقات را برمی گرداند.

مثال:

``` sql
SELECT
    transform(SearchEngineID, [2, 3], ['Yandex', 'Google'], 'Other') AS title,
    count() AS c
FROM test.hits
WHERE SearchEngineID != 0
GROUP BY title
ORDER BY c DESC
```

``` text
┌─title─────┬──────c─┐
│ Yandex    │ 498635 │
│ Google    │ 229872 │
│ Other     │ 104472 │
└───────────┴────────┘
```

### تبدیل) {#transformx-array-from-array-to}

متفاوت از تنوع برای اولین بار در که ‘default’ استدلال حذف شده است.
اگر ‘x’ ارزش به یکی از عناصر در برابر است ‘array\_from’ مجموعه, این بازگرداندن عنصر تطبیق (که شماره همان) از ‘array\_to’ صف کردن. در غیر این صورت, باز می گردد ‘x’.

انواع:

`transform(T, Array(T), Array(T)) -> T`

مثال:

``` sql
SELECT
    transform(domain(Referer), ['yandex.ru', 'google.ru', 'vk.com'], ['www.yandex', 'example.com']) AS s,
    count() AS c
FROM test.hits
GROUP BY domain(Referer)
ORDER BY count() DESC
LIMIT 10
```

``` text
┌─s──────────────┬───────c─┐
│                │ 2906259 │
│ www.yandex     │  867767 │
│ ███████.ru     │  313599 │
│ mail.yandex.ru │  107147 │
│ ██████.ru      │  100355 │
│ █████████.ru   │   65040 │
│ news.yandex.ru │   64515 │
│ ██████.net     │   59141 │
│ example.com    │   57316 │
└────────────────┴─────────┘
```

## قالببندی) ایکس() {#formatreadablesizex}

می پذیرد اندازه (تعداد بایت). بازگرداندن اندازه گرد با پسوند (کیلوبایت, مگابایت, و غیره.) به عنوان یک رشته .

مثال:

``` sql
SELECT
    arrayJoin([1, 1024, 1024*1024, 192851925]) AS filesize_bytes,
    formatReadableSize(filesize_bytes) AS filesize
```

``` text
┌─filesize_bytes─┬─filesize───┐
│              1 │ 1.00 B     │
│           1024 │ 1.00 KiB   │
│        1048576 │ 1.00 MiB   │
│      192851925 │ 183.92 MiB │
└────────────────┴────────────┘
```

## کمترین) {#leasta-b}

بازگرداندن کوچکترین ارزش از یک و ب.

## بزرگترین (و, ب) {#greatesta-b}

بازگرداندن بزرگترین ارزش یک و ب.

## زمان بالا() {#uptime}

بازگرداندن زمان انجام کار سرور در ثانیه.

## نسخه() {#version}

بازگرداندن نسخه از سرور به عنوان یک رشته.

## منطقهی زمانی() {#timezone}

بازگرداندن منطقه زمانی از سرور.

## blockNumber {#blocknumber}

بازگرداندن تعداد دنباله ای از بلوک داده که در ردیف واقع شده است.

## رفع موانع {#function-rownumberinblock}

بازگرداندن تعداد ترتیبی از ردیف در بلوک داده. بلوک های داده های مختلف همیشه محاسبه.

## بلوک های رونمبرینالیک() {#rownumberinallblocks}

بازگرداندن تعداد ترتیبی از ردیف در بلوک داده. این تابع تنها بلوک های داده تحت تاثیر قرار می گیرد.

## همسایه {#neighbor}

تابع پنجره که دسترسی به یک ردیف در یک افست مشخص شده است که قبل یا بعد از ردیف فعلی یک ستون داده می شود فراهم می کند.

**نحو**

``` sql
neighbor(column, offset[, default_value])
```

نتیجه عملکرد بستگی به بلوک های داده تحت تاثیر قرار و منظور از داده ها در بلوک.
اگر شما یک خرده فروشی با سفارش و پاسخ تابع از خارج از خرده فروشی, شما می توانید نتیجه مورد انتظار از.

**پارامترها**

-   `column` — A column name or scalar expression.
-   `offset` — The number of rows forwards or backwards from the current row of `column`. [Int64](../../sql-reference/data-types/int-uint.md).
-   `default_value` — Optional. The value to be returned if offset goes beyond the scope of the block. Type of data blocks affected.

**مقادیر بازگشتی**

-   مقدار برای `column` داخل `offset` فاصله از ردیف فعلی اگر `offset` ارزش خارج از مرزهای بلوک نیست.
-   مقدار پیشفرض برای `column` اگر `offset` ارزش مرزهای بلوک خارج است. اگر `default_value` داده می شود و سپس استفاده می شود.

نوع: نوع بلوک های داده را تحت تاثیر قرار و یا نوع مقدار پیش فرض.

**مثال**

پرسوجو:

``` sql
SELECT number, neighbor(number, 2) FROM system.numbers LIMIT 10;
```

نتیجه:

``` text
┌─number─┬─neighbor(number, 2)─┐
│      0 │                   2 │
│      1 │                   3 │
│      2 │                   4 │
│      3 │                   5 │
│      4 │                   6 │
│      5 │                   7 │
│      6 │                   8 │
│      7 │                   9 │
│      8 │                   0 │
│      9 │                   0 │
└────────┴─────────────────────┘
```

پرسوجو:

``` sql
SELECT number, neighbor(number, 2, 999) FROM system.numbers LIMIT 10;
```

نتیجه:

``` text
┌─number─┬─neighbor(number, 2, 999)─┐
│      0 │                        2 │
│      1 │                        3 │
│      2 │                        4 │
│      3 │                        5 │
│      4 │                        6 │
│      5 │                        7 │
│      6 │                        8 │
│      7 │                        9 │
│      8 │                      999 │
│      9 │                      999 │
└────────┴──────────────────────────┘
```

این تابع می تواند مورد استفاده قرار گیرد برای محاسبه ارزش متریک در سال بیش از سال:

پرسوجو:

``` sql
WITH toDate('2018-01-01') AS start_date
SELECT
    toStartOfMonth(start_date + (number * 32)) AS month,
    toInt32(month) % 100 AS money,
    neighbor(money, -12) AS prev_year,
    round(prev_year / money, 2) AS year_over_year
FROM numbers(16)
```

نتیجه:

``` text
┌──────month─┬─money─┬─prev_year─┬─year_over_year─┐
│ 2018-01-01 │    32 │         0 │              0 │
│ 2018-02-01 │    63 │         0 │              0 │
│ 2018-03-01 │    91 │         0 │              0 │
│ 2018-04-01 │    22 │         0 │              0 │
│ 2018-05-01 │    52 │         0 │              0 │
│ 2018-06-01 │    83 │         0 │              0 │
│ 2018-07-01 │    13 │         0 │              0 │
│ 2018-08-01 │    44 │         0 │              0 │
│ 2018-09-01 │    75 │         0 │              0 │
│ 2018-10-01 │     5 │         0 │              0 │
│ 2018-11-01 │    36 │         0 │              0 │
│ 2018-12-01 │    66 │         0 │              0 │
│ 2019-01-01 │    97 │        32 │           0.33 │
│ 2019-02-01 │    28 │        63 │           2.25 │
│ 2019-03-01 │    56 │        91 │           1.62 │
│ 2019-04-01 │    87 │        22 │           0.25 │
└────────────┴───────┴───────────┴────────────────┘
```

## تغییر تنظیمات صدا) {#other_functions-runningdifference}

Calculates the difference between successive row values ​​in the data block.
بازده 0 برای ردیف اول و تفاوت از ردیف قبلی برای هر سطر بعدی.

نتیجه عملکرد بستگی به بلوک های داده تحت تاثیر قرار و منظور از داده ها در بلوک.
اگر شما یک خرده فروشی با سفارش و پاسخ تابع از خارج از خرده فروشی, شما می توانید نتیجه مورد انتظار از.

مثال:

``` sql
SELECT
    EventID,
    EventTime,
    runningDifference(EventTime) AS delta
FROM
(
    SELECT
        EventID,
        EventTime
    FROM events
    WHERE EventDate = '2016-11-24'
    ORDER BY EventTime ASC
    LIMIT 5
)
```

``` text
┌─EventID─┬───────────EventTime─┬─delta─┐
│    1106 │ 2016-11-24 00:00:04 │     0 │
│    1107 │ 2016-11-24 00:00:05 │     1 │
│    1108 │ 2016-11-24 00:00:05 │     0 │
│    1109 │ 2016-11-24 00:00:09 │     4 │
│    1110 │ 2016-11-24 00:00:10 │     1 │
└─────────┴─────────────────────┴───────┘
```

لطفا توجه داشته باشید-اندازه بلوک بر نتیجه تاثیر می گذارد. با هر بلوک جدید `runningDifference` دولت تنظیم مجدد است.

``` sql
SELECT
    number,
    runningDifference(number + 1) AS diff
FROM numbers(100000)
WHERE diff != 1
```

``` text
┌─number─┬─diff─┐
│      0 │    0 │
└────────┴──────┘
┌─number─┬─diff─┐
│  65536 │    0 │
└────────┴──────┘
```

``` sql
set max_block_size=100000 -- default value is 65536!

SELECT
    number,
    runningDifference(number + 1) AS diff
FROM numbers(100000)
WHERE diff != 1
```

``` text
┌─number─┬─diff─┐
│      0 │    0 │
└────────┴──────┘
```

## در حال بارگذاری {#runningdifferencestartingwithfirstvalue}

همان است که برای [عدم پذیرش](./other-functions.md#other_functions-runningdifference), تفاوت ارزش ردیف اول است, ارزش سطر اول بازگشت, و هر سطر بعدی تفاوت از ردیف قبلی را برمی گرداند.

## هشدار داده می شود) {#macnumtostringnum}

قبول UInt64 شماره. تفسیر به عنوان نشانی مک در اندی بزرگ. بازگرداندن یک رشته حاوی نشانی مک مربوطه را در قالب قلمی: ب: ر. ن:دکتر: ف.ا: ف. ف. (تعداد کولون جدا شده در فرم هگزادسیمال).

## MACStringToNum(s) {#macstringtonums}

عملکرد معکوس مک نومتوسترینگ. اگر نشانی مک دارای یک فرمت نامعتبر, باز می گردد 0.

## درشتنمایی) {#macstringtoouis}

می پذیرد یک نشانی مک در فرمت قلمی:بیت:ر.ن:دی. دی:اف (تعداد روده بزرگ از هم جدا در فرم هگزادسیمال). بازگرداندن سه هشت تایی اول به عنوان یک عدد ظاهری64. اگر نشانی مک دارای یک فرمت نامعتبر, باز می گردد 0.

## نوع گیرنده {#getsizeofenumtype}

بازگرداندن تعدادی از زمینه های در [شمارشی](../../sql-reference/data-types/enum.md).

``` sql
getSizeOfEnumType(value)
```

**پارامترها:**

-   `value` — Value of type `Enum`.

**مقادیر بازگشتی**

-   تعدادی از زمینه های با `Enum` مقادیر ورودی.
-   یک استثنا پرتاب می شود اگر نوع نیست `Enum`.

**مثال**

``` sql
SELECT getSizeOfEnumType( CAST('a' AS Enum8('a' = 1, 'b' = 2) ) ) AS x
```

``` text
┌─x─┐
│ 2 │
└───┘
```

## بلوک سازی {#blockserializedsize}

بازده اندازه بر روی دیسک (بدون در نظر گرفتن فشرده سازی حساب).

``` sql
blockSerializedSize(value[, value[, ...]])
```

**پارامترها:**

-   `value` — Any value.

**مقادیر بازگشتی**

-   تعداد بایت خواهد شد که به دیسک برای بلوک از ارزش های نوشته شده (بدون فشرده سازی).

**مثال**

``` sql
SELECT blockSerializedSize(maxState(1)) as x
```

``` text
┌─x─┐
│ 2 │
└───┘
```

## بدون نام {#tocolumntypename}

بازگرداندن نام کلاس است که نشان دهنده نوع داده ها از ستون در رم.

``` sql
toColumnTypeName(value)
```

**پارامترها:**

-   `value` — Any type of value.

**مقادیر بازگشتی**

-   یک رشته با نام کلاس است که برای نمایندگی از استفاده `value` نوع داده در رم.

**نمونه ای از تفاوت بین`toTypeName ' and ' toColumnTypeName`**

``` sql
SELECT toTypeName(CAST('2018-01-01 01:02:03' AS DateTime))
```

``` text
┌─toTypeName(CAST('2018-01-01 01:02:03', 'DateTime'))─┐
│ DateTime                                            │
└─────────────────────────────────────────────────────┘
```

``` sql
SELECT toColumnTypeName(CAST('2018-01-01 01:02:03' AS DateTime))
```

``` text
┌─toColumnTypeName(CAST('2018-01-01 01:02:03', 'DateTime'))─┐
│ Const(UInt32)                                             │
└───────────────────────────────────────────────────────────┘
```

مثال نشان می دهد که `DateTime` نوع داده در حافظه ذخیره می شود به عنوان `Const(UInt32)`.

## روبنا دامپکول {#dumpcolumnstructure}

خروجی شرح مفصلی از ساختارهای داده در رم

``` sql
dumpColumnStructure(value)
```

**پارامترها:**

-   `value` — Any type of value.

**مقادیر بازگشتی**

-   یک رشته توصیف ساختار است که برای نمایندگی از استفاده `value` نوع داده در رم.

**مثال**

``` sql
SELECT dumpColumnStructure(CAST('2018-01-01 01:02:03', 'DateTime'))
```

``` text
┌─dumpColumnStructure(CAST('2018-01-01 01:02:03', 'DateTime'))─┐
│ DateTime, Const(size = 1, UInt32(size = 1))                  │
└──────────────────────────────────────────────────────────────┘
```

## نوع قراردادی {#defaultvalueofargumenttype}

خروجی مقدار پیش فرض برای نوع داده.

مقادیر پیش فرض برای ستون های سفارشی تعیین شده توسط کاربر را شامل نمی شود.

``` sql
defaultValueOfArgumentType(expression)
```

**پارامترها:**

-   `expression` — Arbitrary type of value or an expression that results in a value of an arbitrary type.

**مقادیر بازگشتی**

-   `0` برای اعداد.
-   رشته خالی برای رشته.
-   `ᴺᵁᴸᴸ` برای [Nullable](../../sql-reference/data-types/nullable.md).

**مثال**

``` sql
SELECT defaultValueOfArgumentType( CAST(1 AS Int8) )
```

``` text
┌─defaultValueOfArgumentType(CAST(1, 'Int8'))─┐
│                                           0 │
└─────────────────────────────────────────────┘
```

``` sql
SELECT defaultValueOfArgumentType( CAST(1 AS Nullable(Int8) ) )
```

``` text
┌─defaultValueOfArgumentType(CAST(1, 'Nullable(Int8)'))─┐
│                                                  ᴺᵁᴸᴸ │
└───────────────────────────────────────────────────────┘
```

## تکرار {#other-functions-replicate}

ایجاد مجموعه ای با یک مقدار واحد.

مورد استفاده برای اجرای داخلی [ارریجین](array-join.md#functions_arrayjoin).

``` sql
SELECT replicate(x, arr);
```

**پارامترها:**

-   `arr` — Original array. ClickHouse creates a new array of the same length as the original and fills it with the value `x`.
-   `x` — The value that the resulting array will be filled with.

**مقدار بازگشتی**

مجموعه ای پر از ارزش `x`.

نوع: `Array`.

**مثال**

پرسوجو:

``` sql
SELECT replicate(1, ['a', 'b', 'c'])
```

نتیجه:

``` text
┌─replicate(1, ['a', 'b', 'c'])─┐
│ [1,1,1]                       │
└───────────────────────────────┘
```

## رشته های قابل استفاده {#filesystemavailable}

بازگرداندن مقدار فضای باقی مانده بر روی سیستم فایل که فایل های پایگاه داده واقع. این است که همیشه کوچکتر از فضای کل رایگان ([بدون پرونده](#filesystemfree)) چرا که برخی از فضا برای سیستم عامل محفوظ می باشد.

**نحو**

``` sql
filesystemAvailable()
```

**مقدار بازگشتی**

-   مقدار فضای باقی مانده موجود در بایت.

نوع: [UInt64](../../sql-reference/data-types/int-uint.md).

**مثال**

پرسوجو:

``` sql
SELECT formatReadableSize(filesystemAvailable()) AS "Available space", toTypeName(filesystemAvailable()) AS "Type";
```

نتیجه:

``` text
┌─Available space─┬─Type───┐
│ 30.75 GiB       │ UInt64 │
└─────────────────┴────────┘
```

## بدون پرونده {#filesystemfree}

بازگرداندن مقدار کل فضای رایگان بر روی سیستم فایل که فایل های پایگاه داده واقع. همچنین نگاه کنید به `filesystemAvailable`

**نحو**

``` sql
filesystemFree()
```

**مقدار بازگشتی**

-   مقدار فضای رایگان در بایت.

نوع: [UInt64](../../sql-reference/data-types/int-uint.md).

**مثال**

پرسوجو:

``` sql
SELECT formatReadableSize(filesystemFree()) AS "Free space", toTypeName(filesystemFree()) AS "Type";
```

نتیجه:

``` text
┌─Free space─┬─Type───┐
│ 32.39 GiB  │ UInt64 │
└────────────┴────────┘
```

## سختی پرونده {#filesystemcapacity}

بازگرداندن ظرفیت فایل سیستم در بایت. برای ارزیابی [مسیر](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-path) به دایرکتوری داده ها باید پیکربندی شود.

**نحو**

``` sql
filesystemCapacity()
```

**مقدار بازگشتی**

-   اطلاعات ظرفیت سیستم فایل در بایت.

نوع: [UInt64](../../sql-reference/data-types/int-uint.md).

**مثال**

پرسوجو:

``` sql
SELECT formatReadableSize(filesystemCapacity()) AS "Capacity", toTypeName(filesystemCapacity()) AS "Type"
```

نتیجه:

``` text
┌─Capacity──┬─Type───┐
│ 39.32 GiB │ UInt64 │
└───────────┴────────┘
```

## پلاکتی {#function-finalizeaggregation}

طول می کشد دولت از تابع جمع. بازده نتیجه تجمع (دولت نهایی).

## خرابی اجرا {#function-runningaccumulate}

طول می کشد کشورهای تابع جمع و یک ستون با ارزش را برمی گرداند, در نتیجه تجمع این کشورها برای مجموعه ای از خطوط بلوک هستند, از اول به خط فعلی.
برای مثال طول می کشد state of aggregate function (به عنوان مثال runningAccumulate(uniqState(UserID))) و برای هر ردیف از بلوک بازگشت نتیجه از مجموع عملکرد در ادغام دولت قبلی تمام ردیف و ردیف جاری است.
بنابراین نتیجه عملکرد بستگی به پارتیشن داده ها به بلوک ها و به ترتیب داده ها در بلوک دارد.

## جوینت {#joinget}

تابع شما اجازه می دهد استخراج داده ها از جدول به همان شیوه به عنوان از یک [واژهنامه](../../sql-reference/dictionaries/index.md).

می شود داده ها از [پیوستن](../../engines/table-engines/special/join.md#creating-a-table) جداول با استفاده از کلید ملحق مشخص.

فقط پشتیبانی از جداول ایجاد شده با `ENGINE = Join(ANY, LEFT, <join_keys>)` بیانیه.

**نحو**

``` sql
joinGet(join_storage_table_name, `value_column`, join_keys)
```

**پارامترها**

-   `join_storage_table_name` — an [شناسه](../syntax.md#syntax-identifiers) نشان می دهد که جستجو انجام شده است. شناسه در پایگاه داده به طور پیش فرض جستجو (پارامتر را ببینید `default_database` در فایل پیکربندی). برای نادیده گرفتن پایگاه داده پیش فرض از `USE db_name` یا پایگاه داده و جدول را از طریق جداساز مشخص کنید `db_name.db_table`, مثال را ببینید.
-   `value_column` — name of the column of the table that contains required data.
-   `join_keys` — list of keys.

**مقدار بازگشتی**

را برمی گرداند لیستی از ارزش مطابقت دارد به لیست کلید.

اگر برخی در جدول منبع وجود ندارد و سپس `0` یا `null` خواهد شد بر اساس بازگشت [ارزشهای خبری عبارتند از:](../../operations/settings/settings.md#join_use_nulls) تنظیمات.

اطلاعات بیشتر در مورد `join_use_nulls` داخل [پیوستن به عملیات](../../engines/table-engines/special/join.md).

**مثال**

جدول ورودی:

``` sql
CREATE DATABASE db_test
CREATE TABLE db_test.id_val(`id` UInt32, `val` UInt32) ENGINE = Join(ANY, LEFT, id) SETTINGS join_use_nulls = 1
INSERT INTO db_test.id_val VALUES (1,11)(2,12)(4,13)
```

``` text
┌─id─┬─val─┐
│  4 │  13 │
│  2 │  12 │
│  1 │  11 │
└────┴─────┘
```

پرسوجو:

``` sql
SELECT joinGet(db_test.id_val,'val',toUInt32(number)) from numbers(4) SETTINGS join_use_nulls = 1
```

نتیجه:

``` text
┌─joinGet(db_test.id_val, 'val', toUInt32(number))─┐
│                                                0 │
│                                               11 │
│                                               12 │
│                                                0 │
└──────────────────────────────────────────────────┘
```

## modelEvaluate(model\_name, …) {#function-modelevaluate}

ارزیابی مدل خارجی.
می پذیرد نام مدل و استدلال مدل. را برمی گرداند شناور64.

## throwIf(x\[, custom\_message\]) {#throwifx-custom-message}

پرتاب یک استثنا اگر استدلال غیر صفر است.
\_پیغام سفارشی-پارامتر اختیاری است: یک رشته ثابت, فراهم می کند یک پیغام خطا

``` sql
SELECT throwIf(number = 3, 'Too many') FROM numbers(10);
```

``` text
↙ Progress: 0.00 rows, 0.00 B (0.00 rows/s., 0.00 B/s.) Received exception from server (version 19.14.1):
Code: 395. DB::Exception: Received from localhost:9000. DB::Exception: Too many.
```

## هویت {#identity}

بازگرداندن همان مقدار که به عنوان استدلال خود مورد استفاده قرار گرفت. مورد استفاده برای اشکال زدایی و تست, اجازه می دهد تا به لغو با استفاده از شاخص, و عملکرد پرس و جو از یک اسکن کامل. هنگامی که پرس و جو برای استفاده احتمالی از شاخص تجزیه و تحلیل, تجزیه و تحلیل می کند در داخل نگاه نمی `identity` توابع.

**نحو**

``` sql
identity(x)
```

**مثال**

پرسوجو:

``` sql
SELECT identity(42)
```

نتیجه:

``` text
┌─identity(42)─┐
│           42 │
└──────────────┘
```

## درباره ما {#randomascii}

تولید یک رشته با مجموعه ای تصادفی از [ASCII](https://en.wikipedia.org/wiki/ASCII#Printable_characters) شخصیت های قابل چاپ.

**نحو**

``` sql
randomPrintableASCII(length)
```

**پارامترها**

-   `length` — Resulting string length. Positive integer.

        If you pass `length < 0`, behavior of the function is undefined.

**مقدار بازگشتی**

-   رشته با مجموعه ای تصادفی از [ASCII](https://en.wikipedia.org/wiki/ASCII#Printable_characters) شخصیت های قابل چاپ.

نوع: [رشته](../../sql-reference/data-types/string.md)

**مثال**

``` sql
SELECT number, randomPrintableASCII(30) as str, length(str) FROM system.numbers LIMIT 3
```

``` text
┌─number─┬─str────────────────────────────┬─length(randomPrintableASCII(30))─┐
│      0 │ SuiCOSTvC0csfABSw=UcSzp2.`rv8x │                               30 │
│      1 │ 1Ag NlJ &RCN:*>HVPG;PE-nO"SUFD │                               30 │
│      2 │ /"+<"wUTh:=LjJ Vm!c&hI*m#XTfzz │                               30 │
└────────┴────────────────────────────────┴──────────────────────────────────┘
```

[مقاله اصلی](https://clickhouse.tech/docs/en/query_language/functions/other_functions/) <!--hide-->
