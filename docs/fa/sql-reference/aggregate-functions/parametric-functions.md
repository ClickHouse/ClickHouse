---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 38
toc_title: "\u067E\u0627\u0631\u0627\u0645\u062A\u0631\u06CC"
---

# توابع مجموع پارامتری {#aggregate_functions_parametric}

Some aggregate functions can accept not only argument columns (used for compression), but a set of parameters – constants for initialization. The syntax is two pairs of brackets instead of one. The first is for parameters, and the second is for arguments.

## سابقهنما {#histogram}

محاسبه هیستوگرام تطبیقی. این نتایج دقیق را تضمین نمی کند.

``` sql
histogram(number_of_bins)(values)
```

توابع استفاده می کند [جریان الگوریتم درخت تصمیم موازی](http://jmlr.org/papers/volume11/ben-haim10a/ben-haim10a.pdf). مرزهای سطل هیستوگرام تنظیم به عنوان داده های جدید وارد یک تابع. در مورد مشترک عرض سطل برابر نیست.

**پارامترها**

`number_of_bins` — Upper limit for the number of bins in the histogram. The function automatically calculates the number of bins. It tries to reach the specified number of bins, but if it fails, it uses fewer bins.
`values` — [عبارت](../syntax.md#syntax-expressions) در نتیجه مقادیر ورودی.

**مقادیر بازگشتی**

-   [& حذف](../../sql-reference/data-types/array.md) از [توپلس](../../sql-reference/data-types/tuple.md) از قالب زیر:

        ```
        [(lower_1, upper_1, height_1), ... (lower_N, upper_N, height_N)]
        ```

        - `lower` — Lower bound of the bin.
        - `upper` — Upper bound of the bin.
        - `height` — Calculated height of the bin.

**مثال**

``` sql
SELECT histogram(5)(number + 1)
FROM (
    SELECT *
    FROM system.numbers
    LIMIT 20
)
```

``` text
┌─histogram(5)(plus(number, 1))───────────────────────────────────────────┐
│ [(1,4.5,4),(4.5,8.5,4),(8.5,12.75,4.125),(12.75,17,4.625),(17,20,3.25)] │
└─────────────────────────────────────────────────────────────────────────┘
```

شما می توانید یک هیستوگرام با تجسم [بار](../../sql-reference/functions/other-functions.md#function-bar) تابع, مثلا:

``` sql
WITH histogram(5)(rand() % 100) AS hist
SELECT
    arrayJoin(hist).3 AS height,
    bar(height, 0, 6, 5) AS bar
FROM
(
    SELECT *
    FROM system.numbers
    LIMIT 20
)
```

``` text
┌─height─┬─bar───┐
│  2.125 │ █▋    │
│   3.25 │ ██▌   │
│  5.625 │ ████▏ │
│  5.625 │ ████▏ │
│  3.375 │ ██▌   │
└────────┴───────┘
```

در این مورد, شما باید به یاد داشته باشید که شما مرزهای هیستوگرام بن نمی دانند.

## sequenceMatch(pattern)(timestamp, cond1, cond2, …) {#function-sequencematch}

بررسی اینکه دنباله شامل یک زنجیره رویداد که منطبق بر الگوی.

``` sql
sequenceMatch(pattern)(timestamp, cond1, cond2, ...)
```

!!! warning "اخطار"
    رویدادهایی که در همان دوم رخ می دهد ممکن است در دنباله در سفارش تعریف نشده موثر بر نتیجه دراز.

**پارامترها**

-   `pattern` — Pattern string. See [نحو الگو](#sequence-function-pattern-syntax).

-   `timestamp` — Column considered to contain time data. Typical data types are `Date` و `DateTime`. شما همچنین می توانید هر یک از پشتیبانی استفاده کنید [اینترنت](../../sql-reference/data-types/int-uint.md) انواع داده ها.

-   `cond1`, `cond2` — Conditions that describe the chain of events. Data type: `UInt8`. شما می توانید به تصویب تا 32 استدلال شرط. تابع طول می کشد تنها حوادث شرح داده شده در این شرایط به حساب. اگر دنباله حاوی اطلاعاتی است که در شرایط توصیف نشده, تابع پرش.

**مقادیر بازگشتی**

-   1, اگر الگوی همسان است.
-   0, اگر الگوی همسان نیست.

نوع: `UInt8`.

<a name="sequence-function-pattern-syntax"></a>
**نحو الگو**

-   `(?N)` — Matches the condition argument at position `N`. شرایط در شماره `[1, 32]` محدوده. به عنوان مثال, `(?1)` با استدلال به تصویب رسید `cond1` پارامتر.

-   `.*` — Matches any number of events. You don't need conditional arguments to match this element of the pattern.

-   `(?t operator value)` — Sets the time in seconds that should separate two events. For example, pattern `(?1)(?t>1800)(?2)` مسابقات رویدادهایی که رخ می دهد بیش از 1800 ثانیه از یکدیگر. تعداد دلخواه از هر رویدادی می تواند بین این حوادث دراز. شما می توانید از `>=`, `>`, `<`, `<=` اپراتورها.

**مثالها**

داده ها را در نظر بگیرید `t` جدول:

``` text
┌─time─┬─number─┐
│    1 │      1 │
│    2 │      3 │
│    3 │      2 │
└──────┴────────┘
```

انجام پرس و جو:

``` sql
SELECT sequenceMatch('(?1)(?2)')(time, number = 1, number = 2) FROM t
```

``` text
┌─sequenceMatch('(?1)(?2)')(time, equals(number, 1), equals(number, 2))─┐
│                                                                     1 │
└───────────────────────────────────────────────────────────────────────┘
```

تابع زنجیره رویداد که تعداد پیدا شده است 2 زیر شماره 1. این قلم شماره 3 بین, چرا که تعداد به عنوان یک رویداد توصیف نشده. اگر ما می خواهیم این شماره را در نظر بگیریم هنگام جستجو برای زنجیره رویداد داده شده در مثال باید شرایط را ایجاد کنیم.

``` sql
SELECT sequenceMatch('(?1)(?2)')(time, number = 1, number = 2, number = 3) FROM t
```

``` text
┌─sequenceMatch('(?1)(?2)')(time, equals(number, 1), equals(number, 2), equals(number, 3))─┐
│                                                                                        0 │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

در این مورد, تابع می تواند زنجیره رویداد تطبیق الگوی پیدا کنید, چرا که این رویداد برای شماره 3 رخ داده است بین 1 و 2. اگر در همان مورد ما شرایط را برای شماره بررسی 4, دنباله الگوی مطابقت.

``` sql
SELECT sequenceMatch('(?1)(?2)')(time, number = 1, number = 2, number = 4) FROM t
```

``` text
┌─sequenceMatch('(?1)(?2)')(time, equals(number, 1), equals(number, 2), equals(number, 4))─┐
│                                                                                        1 │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

**همچنین نگاه کنید به**

-   [شمارش معکوس](#function-sequencecount)

## sequenceCount(pattern)(time, cond1, cond2, …) {#function-sequencecount}

شمارش تعداد زنجیره رویداد که الگوی همسان. تابع جستجو زنجیره رویداد که با هم همپوشانی دارند. این شروع به جستجو برای زنجیره بعدی پس از زنجیره فعلی همسان است.

!!! warning "اخطار"
    رویدادهایی که در همان دوم رخ می دهد ممکن است در دنباله در سفارش تعریف نشده موثر بر نتیجه دراز.

``` sql
sequenceCount(pattern)(timestamp, cond1, cond2, ...)
```

**پارامترها**

-   `pattern` — Pattern string. See [نحو الگو](#sequence-function-pattern-syntax).

-   `timestamp` — Column considered to contain time data. Typical data types are `Date` و `DateTime`. شما همچنین می توانید هر یک از پشتیبانی استفاده کنید [اینترنت](../../sql-reference/data-types/int-uint.md) انواع داده ها.

-   `cond1`, `cond2` — Conditions that describe the chain of events. Data type: `UInt8`. شما می توانید به تصویب تا 32 استدلال شرط. تابع طول می کشد تنها حوادث شرح داده شده در این شرایط به حساب. اگر دنباله حاوی اطلاعاتی است که در شرایط توصیف نشده, تابع پرش.

**مقادیر بازگشتی**

-   تعداد زنجیره رویداد غیر با هم تداخل دارند که همسان.

نوع: `UInt64`.

**مثال**

داده ها را در نظر بگیرید `t` جدول:

``` text
┌─time─┬─number─┐
│    1 │      1 │
│    2 │      3 │
│    3 │      2 │
│    4 │      1 │
│    5 │      3 │
│    6 │      2 │
└──────┴────────┘
```

تعداد چند بار تعداد 2 پس از شماره 1 با هر مقدار از شماره های دیگر بین رخ می دهد:

``` sql
SELECT sequenceCount('(?1).*(?2)')(time, number = 1, number = 2) FROM t
```

``` text
┌─sequenceCount('(?1).*(?2)')(time, equals(number, 1), equals(number, 2))─┐
│                                                                       2 │
└─────────────────────────────────────────────────────────────────────────┘
```

**همچنین نگاه کنید به**

-   [ترتیب سنج](#function-sequencematch)

## در پنجره {#windowfunnel}

جستجو برای زنجیره رویداد در یک پنجره زمان کشویی و محاسبه حداکثر تعداد رویدادهایی که از زنجیره رخ داده است.

تابع با توجه به الگوریتم کار می کند:

-   تابع جستجو برای داده هایی که باعث شرط اول در زنجیره و مجموعه ضد رویداد به 1. این لحظه ای است که پنجره کشویی شروع می شود.

-   اگر حوادث از زنجیره پی در پی در پنجره رخ می دهد, ضد افزایش است. اگر دنباله ای از حوادث مختل شده است, شمارنده است افزایش نمی.

-   اگر داده های زنجیره رویداد های متعدد در نقاط مختلف از اتمام, تابع تنها خروجی به اندازه طولانی ترین زنجیره ای.

**نحو**

``` sql
windowFunnel(window, [mode])(timestamp, cond1, cond2, ..., condN)
```

**پارامترها**

-   `window` — Length of the sliding window in seconds.
-   `mode` - این یک استدلال اختیاری است .
    -   `'strict'` - وقتی که `'strict'` تنظیم شده است, پنجره() اعمال شرایط تنها برای ارزش های منحصر به فرد.
-   `timestamp` — Name of the column containing the timestamp. Data types supported: [تاریخ](../../sql-reference/data-types/date.md), [DateTime](../../sql-reference/data-types/datetime.md#data_type-datetime) و دیگر انواع عدد صحیح بدون علامت (توجه داشته باشید که حتی اگر برچسب زمان پشتیبانی از `UInt64` نوع, این مقدار می تواند بین المللی تجاوز نمی64 بیشترین, که 2^63 - 1).
-   `cond` — Conditions or data describing the chain of events. [UInt8](../../sql-reference/data-types/int-uint.md).

**مقدار بازگشتی**

حداکثر تعداد متوالی باعث شرایط از زنجیره ای در پنجره زمان کشویی.
تمام زنجیره ها در انتخاب تجزیه و تحلیل می شوند.

نوع: `Integer`.

**مثال**

تعیین کنید که یک دوره زمانی معین برای کاربر کافی باشد تا گوشی را انتخاب کند و دو بار در فروشگاه اینترنتی خریداری کند.

زنجیره ای از وقایع زیر را تنظیم کنید:

1.  کاربر وارد شده به حساب خود را در فروشگاه (`eventID = 1003`).
2.  کاربر برای یک تلفن جستجو می کند (`eventID = 1007, product = 'phone'`).
3.  کاربر سفارش داده شده (`eventID = 1009`).
4.  کاربر دوباره سفارش داد (`eventID = 1010`).

جدول ورودی:

``` text
┌─event_date─┬─user_id─┬───────────timestamp─┬─eventID─┬─product─┐
│ 2019-01-28 │       1 │ 2019-01-29 10:00:00 │    1003 │ phone   │
└────────────┴─────────┴─────────────────────┴─────────┴─────────┘
┌─event_date─┬─user_id─┬───────────timestamp─┬─eventID─┬─product─┐
│ 2019-01-31 │       1 │ 2019-01-31 09:00:00 │    1007 │ phone   │
└────────────┴─────────┴─────────────────────┴─────────┴─────────┘
┌─event_date─┬─user_id─┬───────────timestamp─┬─eventID─┬─product─┐
│ 2019-01-30 │       1 │ 2019-01-30 08:00:00 │    1009 │ phone   │
└────────────┴─────────┴─────────────────────┴─────────┴─────────┘
┌─event_date─┬─user_id─┬───────────timestamp─┬─eventID─┬─product─┐
│ 2019-02-01 │       1 │ 2019-02-01 08:00:00 │    1010 │ phone   │
└────────────┴─────────┴─────────────────────┴─────────┴─────────┘
```

یافتن پست های تا چه حد کاربر `user_id` می تواند از طریق زنجیره ای در یک دوره در ژانویه و فوریه از 2019.

پرسوجو:

``` sql
SELECT
    level,
    count() AS c
FROM
(
    SELECT
        user_id,
        windowFunnel(6048000000000000)(timestamp, eventID = 1003, eventID = 1009, eventID = 1007, eventID = 1010) AS level
    FROM trend
    WHERE (event_date >= '2019-01-01') AND (event_date <= '2019-02-02')
    GROUP BY user_id
)
GROUP BY level
ORDER BY level ASC
```

نتیجه:

``` text
┌─level─┬─c─┐
│     4 │ 1 │
└───────┴───┘
```

## نگهداری {#retention}

تابع طول می کشد به عنوان استدلال مجموعه ای از شرایط از 1 به 32 استدلال از نوع `UInt8` که نشان می دهد که یک بیماری خاص برای این رویداد مواجه شد.
هر گونه شرایط را می توان به عنوان یک استدلال مشخص (همانطور که در [WHERE](../../sql-reference/statements/select/where.md#select-where)).

شرایط, به جز اولین, درخواست در جفت: نتیجه دوم درست خواهد بود اگر اول و دوم درست باشد, از سوم اگر اولین و فیرد درست باشد, و غیره.

**نحو**

``` sql
retention(cond1, cond2, ..., cond32);
```

**پارامترها**

-   `cond` — an expression that returns a `UInt8` نتیجه (1 یا 0).

**مقدار بازگشتی**

مجموعه ای از 1 یا 0.

-   1 — condition was met for the event.
-   0 — condition wasn't met for the event.

نوع: `UInt8`.

**مثال**

بیایید یک نمونه از محاسبه را در نظر بگیریم `retention` تابع برای تعیین ترافیک سایت.

**1.** Сreate a table to illustrate an example.

``` sql
CREATE TABLE retention_test(date Date, uid Int32) ENGINE = Memory;

INSERT INTO retention_test SELECT '2020-01-01', number FROM numbers(5);
INSERT INTO retention_test SELECT '2020-01-02', number FROM numbers(10);
INSERT INTO retention_test SELECT '2020-01-03', number FROM numbers(15);
```

جدول ورودی:

پرسوجو:

``` sql
SELECT * FROM retention_test
```

نتیجه:

``` text
┌───────date─┬─uid─┐
│ 2020-01-01 │   0 │
│ 2020-01-01 │   1 │
│ 2020-01-01 │   2 │
│ 2020-01-01 │   3 │
│ 2020-01-01 │   4 │
└────────────┴─────┘
┌───────date─┬─uid─┐
│ 2020-01-02 │   0 │
│ 2020-01-02 │   1 │
│ 2020-01-02 │   2 │
│ 2020-01-02 │   3 │
│ 2020-01-02 │   4 │
│ 2020-01-02 │   5 │
│ 2020-01-02 │   6 │
│ 2020-01-02 │   7 │
│ 2020-01-02 │   8 │
│ 2020-01-02 │   9 │
└────────────┴─────┘
┌───────date─┬─uid─┐
│ 2020-01-03 │   0 │
│ 2020-01-03 │   1 │
│ 2020-01-03 │   2 │
│ 2020-01-03 │   3 │
│ 2020-01-03 │   4 │
│ 2020-01-03 │   5 │
│ 2020-01-03 │   6 │
│ 2020-01-03 │   7 │
│ 2020-01-03 │   8 │
│ 2020-01-03 │   9 │
│ 2020-01-03 │  10 │
│ 2020-01-03 │  11 │
│ 2020-01-03 │  12 │
│ 2020-01-03 │  13 │
│ 2020-01-03 │  14 │
└────────────┴─────┘
```

**2.** کاربران گروه با شناسه منحصر به فرد `uid` با استفاده از `retention` تابع.

پرسوجو:

``` sql
SELECT
    uid,
    retention(date = '2020-01-01', date = '2020-01-02', date = '2020-01-03') AS r
FROM retention_test
WHERE date IN ('2020-01-01', '2020-01-02', '2020-01-03')
GROUP BY uid
ORDER BY uid ASC
```

نتیجه:

``` text
┌─uid─┬─r───────┐
│   0 │ [1,1,1] │
│   1 │ [1,1,1] │
│   2 │ [1,1,1] │
│   3 │ [1,1,1] │
│   4 │ [1,1,1] │
│   5 │ [0,0,0] │
│   6 │ [0,0,0] │
│   7 │ [0,0,0] │
│   8 │ [0,0,0] │
│   9 │ [0,0,0] │
│  10 │ [0,0,0] │
│  11 │ [0,0,0] │
│  12 │ [0,0,0] │
│  13 │ [0,0,0] │
│  14 │ [0,0,0] │
└─────┴─────────┘
```

**3.** محاسبه تعداد کل بازدیدکننده داشته است سایت در هر روز.

پرسوجو:

``` sql
SELECT
    sum(r[1]) AS r1,
    sum(r[2]) AS r2,
    sum(r[3]) AS r3
FROM
(
    SELECT
        uid,
        retention(date = '2020-01-01', date = '2020-01-02', date = '2020-01-03') AS r
    FROM retention_test
    WHERE date IN ('2020-01-01', '2020-01-02', '2020-01-03')
    GROUP BY uid
)
```

نتیجه:

``` text
┌─r1─┬─r2─┬─r3─┐
│  5 │  5 │  5 │
└────┴────┴────┘
```

کجا:

-   `r1`- تعداد بازدید کنندگان منحصر به فرد که در طول 2020-01-01 (بازدید `cond1` شرط).
-   `r2`- تعداد بازدید کنندگان منحصر به فرد که برای بازدید از سایت در طول یک دوره زمانی خاص بین 2020-01-01 و 2020-01-02 (`cond1` و `cond2` شرایط).
-   `r3`- تعداد بازدید کنندگان منحصر به فرد که برای بازدید از سایت در طول یک دوره زمانی خاص بین 2020-01-01 و 2020-01-03 (`cond1` و `cond3` شرایط).

## uniqUpTo(N)(x) {#uniquptonx}

Calculates the number of different argument values ​​if it is less than or equal to N. If the number of different argument values is greater than N, it returns N + 1.

توصیه می شود برای استفاده با شماره های کوچک, تا 10. حداکثر مقدار نفر است 100.

برای دولت از یک تابع جمع, با استفاده از مقدار حافظه برابر با 1 + نفر \* اندازه یک مقدار بایت.
برای رشته, این فروشگاه یک هش غیر رمزنگاری 8 بایت. به این معنا که محاسبه برای رشته ها تقریبی است.

این تابع همچنین برای چندین استدلال کار می کند.

این کار به همان سرعتی که ممکن است, به جز برای موارد زمانی که یک مقدار نفر بزرگ استفاده می شود و تعدادی از ارزش های منحصر به فرد است کمی کمتر از ان.

مثال طریقه استفاده:

``` text
Problem: Generate a report that shows only keywords that produced at least 5 unique users.
Solution: Write in the GROUP BY query SearchPhrase HAVING uniqUpTo(4)(UserID) >= 5
```

[مقاله اصلی](https://clickhouse.tech/docs/en/query_language/agg_functions/parametric_functions/) <!--hide-->

## sumMapFiltered(keys_to_keep)(کلید ارزش ها) {#summapfilteredkeys-to-keepkeys-values}

رفتار مشابه [& سواپ](reference.md#agg_functions-summap) جز این که مجموعه ای از کلید به عنوان یک پارامتر منتقل می شود. این می تواند مفید باشد به خصوص در هنگام کار با یک کارت از کلید های بالا.
