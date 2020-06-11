---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 36
toc_title: "\u0645\u0631\u062C\u0639"
---

# مرجع عملکرد کامل {#aggregate-functions-reference}

## شمارش {#agg_function-count}

شمارش تعداد ردیف یا نه تهی ارزش.

ClickHouse زیر پشتیبانی می کند syntaxes برای `count`:
- `count(expr)` یا `COUNT(DISTINCT expr)`.
- `count()` یا `COUNT(*)`. این `count()` نحو تاتر خاص است.

**پارامترها**

این تابع می تواند:

-   صفر پارامتر.
-   یک [عبارت](../syntax.md#syntax-expressions).

**مقدار بازگشتی**

-   اگر تابع بدون پارامتر نامیده می شود تعداد ردیف شمارش.
-   اگر [عبارت](../syntax.md#syntax-expressions) به تصویب می رسد, سپس تابع شمارش چند بار این عبارت بازگشت تهی نیست. اگر بیان می گرداند [Nullable](../../sql-reference/data-types/nullable.md)- نوع ارزش و سپس نتیجه `count` باقی نمی ماند `Nullable`. تابع بازده 0 اگر بیان بازگشت `NULL` برای تمام ردیف.

در هر دو مورد نوع مقدار بازگشتی است [UInt64](../../sql-reference/data-types/int-uint.md).

**اطلاعات دقیق**

تاتر از `COUNT(DISTINCT ...)` نحو. رفتار این ساخت و ساز بستگی به [ا\_فزونهها](../../operations/settings/settings.md#settings-count_distinct_implementation) تنظیمات. این تعریف می کند که کدام یک از [دانشگاه\*](#agg_function-uniq) توابع برای انجام عملیات استفاده می شود. به طور پیش فرض است [قرارداد اتحادیه](#agg_function-uniqexact) تابع.

این `SELECT count() FROM table` پرس و جو بهینه سازی شده نیست, چرا که تعداد ورودی در جدول به طور جداگانه ذخیره نمی. این ستون کوچک را از جدول انتخاب می کند و تعداد مقادیر موجود را شمارش می کند.

**مثالها**

مثال 1:

``` sql
SELECT count() FROM t
```

``` text
┌─count()─┐
│       5 │
└─────────┘
```

مثال 2:

``` sql
SELECT name, value FROM system.settings WHERE name = 'count_distinct_implementation'
```

``` text
┌─name──────────────────────────┬─value─────┐
│ count_distinct_implementation │ uniqExact │
└───────────────────────────────┴───────────┘
```

``` sql
SELECT count(DISTINCT num) FROM t
```

``` text
┌─uniqExact(num)─┐
│              3 │
└────────────────┘
```

این مثال نشان می دهد که `count(DISTINCT num)` توسط `uniqExact` عملکرد با توجه به `count_distinct_implementation` مقدار تنظیم.

## هر) {#agg_function-any}

انتخاب اولین مقدار مواجه می شوند.
پرس و جو را می توان در هر سفارش و حتی در جهت های مختلف در هر زمان اجرا, بنابراین نتیجه این تابع نامشخص است.
برای دریافت یک نتیجه معین, شما می توانید با استفاده از ‘min’ یا ‘max’ تابع به جای ‘any’.

در بعضی موارد, شما می توانید در جهت اعدام تکیه. این امر در مورد مواردی که انتخاب می شود از یک زیرخاکی است که از سفارش استفاده می کند.

هنگامی که یک `SELECT` پرسوجو دارد `GROUP BY` بند و یا حداقل یک مجموع عملکرد ClickHouse (در مقایسه با MySQL) مستلزم آن است که تمام عبارات در `SELECT`, `HAVING` و `ORDER BY` بند از کلید و یا از توابع کل محاسبه می شود. به عبارت دیگر, هر ستون انتخاب شده از جدول باید یا در کلید و یا در داخل توابع دانه استفاده می شود. برای دریافت رفتار مانند خروجی زیر, شما می توانید ستون های دیگر در قرار `any` تابع جمع.

## هشدار داده می شود) {#anyheavyx}

انتخاب یک مقدار اغلب اتفاق می افتد با استفاده از [بزرگان سنگین](http://www.cs.umd.edu/~samir/498/karp.pdf) الگوریتم. در صورتی که یک مقدار که بیش از در نیمی از موارد در هر یک از موضوعات اعدام پرس و جو رخ می دهد وجود دارد, این مقدار بازگشته است. به طور معمول نتیجه nondeterministic.

``` sql
anyHeavy(column)
```

**نشانوندها**

-   `column` – The column name.

**مثال**

نگاهی به [به موقع](../../getting-started/example-datasets/ontime.md) مجموعه داده ها و انتخاب هر مقدار اغلب اتفاق می افتد در `AirlineID` ستون.

``` sql
SELECT anyHeavy(AirlineID) AS res
FROM ontime
```

``` text
┌───res─┐
│ 19690 │
└───────┘
```

## حداقل صفحه نمایش:) {#anylastx}

ارزش گذشته مواجه می شوند را انتخاب می کند.
نتیجه این است که فقط به عنوان نامشخص به عنوان برای `any` تابع.

## گروه بیتاند {#groupbitand}

اعمال بیتی `AND` برای مجموعه ای از اعداد.

``` sql
groupBitAnd(expr)
```

**پارامترها**

`expr` – An expression that results in `UInt*` نوع.

**مقدار بازگشتی**

ارزش `UInt*` نوع.

**مثال**

داده های تست:

``` text
binary     decimal
00101100 = 44
00011100 = 28
00001101 = 13
01010101 = 85
```

پرسوجو:

``` sql
SELECT groupBitAnd(num) FROM t
```

کجا `num` ستون با داده های تست است.

نتیجه:

``` text
binary     decimal
00000100 = 4
```

## ویرایشگر گروه {#groupbitor}

اعمال بیتی `OR` برای مجموعه ای از اعداد.

``` sql
groupBitOr(expr)
```

**پارامترها**

`expr` – An expression that results in `UInt*` نوع.

**مقدار بازگشتی**

ارزش `UInt*` نوع.

**مثال**

داده های تست:

``` text
binary     decimal
00101100 = 44
00011100 = 28
00001101 = 13
01010101 = 85
```

پرسوجو:

``` sql
SELECT groupBitOr(num) FROM t
```

کجا `num` ستون با داده های تست است.

نتیجه:

``` text
binary     decimal
01111101 = 125
```

## گروهبیتکسور {#groupbitxor}

اعمال بیتی `XOR` برای مجموعه ای از اعداد.

``` sql
groupBitXor(expr)
```

**پارامترها**

`expr` – An expression that results in `UInt*` نوع.

**مقدار بازگشتی**

ارزش `UInt*` نوع.

**مثال**

داده های تست:

``` text
binary     decimal
00101100 = 44
00011100 = 28
00001101 = 13
01010101 = 85
```

پرسوجو:

``` sql
SELECT groupBitXor(num) FROM t
```

کجا `num` ستون با داده های تست است.

نتیجه:

``` text
binary     decimal
01101000 = 104
```

## نگاشت گروهی {#groupbitmap}

بیت مپ و یا کل محاسبات از یک unsigned integer ستون بازگشت cardinality از نوع UInt64 اگر اضافه کردن پسوند -دولت بازگشت [شی نگاشت بیت](../../sql-reference/functions/bitmap-functions.md).

``` sql
groupBitmap(expr)
```

**پارامترها**

`expr` – An expression that results in `UInt*` نوع.

**مقدار بازگشتی**

ارزش `UInt64` نوع.

**مثال**

داده های تست:

``` text
UserID
1
1
2
3
```

پرسوجو:

``` sql
SELECT groupBitmap(UserID) as num FROM t
```

نتیجه:

``` text
num
3
```

## کمینه) {#agg_function-min}

محاسبه حداقل.

## بیشینه) {#agg_function-max}

محاسبه حداکثر.

## هشدار داده می شود) {#agg-function-argmin}

محاسبه ‘arg’ ارزش برای حداقل ‘val’ ارزش. اگر چندین مقدار مختلف وجود دارد ‘arg’ برای مقادیر حداقل ‘val’ اولین بار از این مقادیر مواجه خروجی است.

**مثال:**

``` text
┌─user─────┬─salary─┐
│ director │   5000 │
│ manager  │   3000 │
│ worker   │   1000 │
└──────────┴────────┘
```

``` sql
SELECT argMin(user, salary) FROM salary
```

``` text
┌─argMin(user, salary)─┐
│ worker               │
└──────────────────────┘
```

## هشدار داده می شود) {#agg-function-argmax}

محاسبه ‘arg’ مقدار برای حداکثر ‘val’ ارزش. اگر چندین مقدار مختلف وجود دارد ‘arg’ برای حداکثر مقادیر ‘val’ اولین بار از این مقادیر مواجه خروجی است.

## جمع) {#agg_function-sum}

محاسبه مجموع.
فقط برای اعداد کار می کند.

## ورود به سیستم) {#sumwithoverflowx}

محاسبه مجموع اعداد, با استفاده از همان نوع داده برای نتیجه به عنوان پارامترهای ورودی. اگر مجموع بیش از حداکثر مقدار برای این نوع داده, تابع یک خطا می گرداند.

فقط برای اعداد کار می کند.

## sumMap(key, value), sumMap(تاپل(key, value)) {#agg_functions-summap}

مجموع ‘value’ تنظیم با توجه به کلید های مشخص شده در ‘key’ صف کردن.
عبور تاپل از کلید ها و ارزش های عرریس مترادف به عبور از دو مجموعه از کلید ها و ارزش است.
تعداد عناصر در ‘key’ و ‘value’ باید همین کار را برای هر سطر است که بالغ بر شود.
Returns a tuple of two arrays: keys in sorted order, and values ​​summed for the corresponding keys.

مثال:

``` sql
CREATE TABLE sum_map(
    date Date,
    timeslot DateTime,
    statusMap Nested(
        status UInt16,
        requests UInt64
    ),
    statusMapTuple Tuple(Array(Int32), Array(Int32))
) ENGINE = Log;
INSERT INTO sum_map VALUES
    ('2000-01-01', '2000-01-01 00:00:00', [1, 2, 3], [10, 10, 10], ([1, 2, 3], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:00:00', [3, 4, 5], [10, 10, 10], ([3, 4, 5], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:01:00', [4, 5, 6], [10, 10, 10], ([4, 5, 6], [10, 10, 10])),
    ('2000-01-01', '2000-01-01 00:01:00', [6, 7, 8], [10, 10, 10], ([6, 7, 8], [10, 10, 10]));

SELECT
    timeslot,
    sumMap(statusMap.status, statusMap.requests),
    sumMap(statusMapTuple)
FROM sum_map
GROUP BY timeslot
```

``` text
┌────────────timeslot─┬─sumMap(statusMap.status, statusMap.requests)─┬─sumMap(statusMapTuple)─────────┐
│ 2000-01-01 00:00:00 │ ([1,2,3,4,5],[10,10,20,10,10])               │ ([1,2,3,4,5],[10,10,20,10,10]) │
│ 2000-01-01 00:01:00 │ ([4,5,6,7,8],[10,10,20,10,10])               │ ([4,5,6,7,8],[10,10,20,10,10]) │
└─────────────────────┴──────────────────────────────────────────────┴────────────────────────────────┘
```

## سیخ کباب {#skewpop}

محاسبه [skewness](https://en.wikipedia.org/wiki/Skewness) از یک توالی.

``` sql
skewPop(expr)
```

**پارامترها**

`expr` — [عبارت](../syntax.md#syntax-expressions) بازگشت یک عدد.

**مقدار بازگشتی**

The skewness of the given distribution. Type — [جسم شناور64](../../sql-reference/data-types/float.md)

**مثال**

``` sql
SELECT skewPop(value) FROM series_with_value_column
```

## سیخ {#skewsamp}

محاسبه [نمونه skewness](https://en.wikipedia.org/wiki/Skewness) از یک توالی.

این نشان دهنده یک تخمین بی طرفانه از اریب یک متغیر تصادفی اگر ارزش گذشت نمونه خود را تشکیل می دهند.

``` sql
skewSamp(expr)
```

**پارامترها**

`expr` — [عبارت](../syntax.md#syntax-expressions) بازگشت یک عدد.

**مقدار بازگشتی**

The skewness of the given distribution. Type — [جسم شناور64](../../sql-reference/data-types/float.md). اگر `n <= 1` (`n` اندازه نمونه است), سپس بازده تابع `nan`.

**مثال**

``` sql
SELECT skewSamp(value) FROM series_with_value_column
```

## کورتپ {#kurtpop}

محاسبه [kurtosis](https://en.wikipedia.org/wiki/Kurtosis) از یک توالی.

``` sql
kurtPop(expr)
```

**پارامترها**

`expr` — [عبارت](../syntax.md#syntax-expressions) بازگشت یک عدد.

**مقدار بازگشتی**

The kurtosis of the given distribution. Type — [جسم شناور64](../../sql-reference/data-types/float.md)

**مثال**

``` sql
SELECT kurtPop(value) FROM series_with_value_column
```

## کردها {#kurtsamp}

محاسبه [نمونه kurtosis](https://en.wikipedia.org/wiki/Kurtosis) از یک توالی.

این نشان دهنده یک تخمین بی طرفانه از کورتوز یک متغیر تصادفی اگر ارزش گذشت نمونه خود را تشکیل می دهند.

``` sql
kurtSamp(expr)
```

**پارامترها**

`expr` — [عبارت](../syntax.md#syntax-expressions) بازگشت یک عدد.

**مقدار بازگشتی**

The kurtosis of the given distribution. Type — [جسم شناور64](../../sql-reference/data-types/float.md). اگر `n <= 1` (`n` اندازه نمونه است) و سپس تابع بازده `nan`.

**مثال**

``` sql
SELECT kurtSamp(value) FROM series_with_value_column
```

## هشدار داده می شود) {#agg-function-timeseriesgroupsum}

`timeSeriesGroupSum` می توانید سری های زمانی مختلف که برچسب زمان نمونه هم ترازی جمع نمی.
این برون یابی خطی بین دو برچسب زمان نمونه و سپس مجموع زمان سری با هم استفاده کنید.

-   `uid` سری زمان شناسه منحصر به فرد است, `UInt64`.
-   `timestamp` است نوع درون64 به منظور حمایت میلی ثانیه یا میکروثانیه.
-   `value` متریک است.

تابع گرداند مجموعه ای از تاپل با `(timestamp, aggregated_value)` جفت

قبل از استفاده از این تابع اطمینان حاصل کنید `timestamp` به ترتیب صعودی است.

مثال:

``` text
┌─uid─┬─timestamp─┬─value─┐
│ 1   │     2     │   0.2 │
│ 1   │     7     │   0.7 │
│ 1   │    12     │   1.2 │
│ 1   │    17     │   1.7 │
│ 1   │    25     │   2.5 │
│ 2   │     3     │   0.6 │
│ 2   │     8     │   1.6 │
│ 2   │    12     │   2.4 │
│ 2   │    18     │   3.6 │
│ 2   │    24     │   4.8 │
└─────┴───────────┴───────┘
```

``` sql
CREATE TABLE time_series(
    uid       UInt64,
    timestamp Int64,
    value     Float64
) ENGINE = Memory;
INSERT INTO time_series VALUES
    (1,2,0.2),(1,7,0.7),(1,12,1.2),(1,17,1.7),(1,25,2.5),
    (2,3,0.6),(2,8,1.6),(2,12,2.4),(2,18,3.6),(2,24,4.8);

SELECT timeSeriesGroupSum(uid, timestamp, value)
FROM (
    SELECT * FROM time_series order by timestamp ASC
);
```

و نتیجه خواهد بود:

``` text
[(2,0.2),(3,0.9),(7,2.1),(8,2.4),(12,3.6),(17,5.1),(18,5.4),(24,7.2),(25,2.5)]
```

## هشدار داده می شود) {#agg-function-timeseriesgroupratesum}

به طور مشابه به `timeSeriesGroupSum`, `timeSeriesGroupRateSum` محاسبه نرخ زمان سری و سپس مجموع نرخ با هم.
همچنین, برچسب زمان باید در جهت صعود قبل از استفاده از این تابع باشد.

استفاده از این تابع به داده ها از `timeSeriesGroupSum` مثال, شما نتیجه زیر را دریافت کنید:

``` text
[(2,0),(3,0.1),(7,0.3),(8,0.3),(12,0.3),(17,0.3),(18,0.3),(24,0.3),(25,0.1)]
```

## میانگین) {#agg_function-avg}

محاسبه متوسط.
فقط برای اعداد کار می کند.
نتیجه این است که همیشه شناور64.

## کشتی کج {#avgweighted}

محاسبه [میانگین ریاضی وزنی](https://en.wikipedia.org/wiki/Weighted_arithmetic_mean).

**نحو**

``` sql
avgWeighted(x, weight)
```

**پارامترها**

-   `x` — Values. [عدد صحیح](../data-types/int-uint.md) یا [شناور نقطه](../data-types/float.md).
-   `weight` — Weights of the values. [عدد صحیح](../data-types/int-uint.md) یا [شناور نقطه](../data-types/float.md).

نوع `x` و `weight` باید مثل قبل باشه

**مقدار بازگشتی**

-   وزن متوسط.
-   `NaN`. اگر تمام وزن به برابر هستند 0.

نوع: [جسم شناور64](../data-types/float.md).

**مثال**

پرسوجو:

``` sql
SELECT avgWeighted(x, w)
FROM values('x Int8, w Int8', (4, 1), (1, 0), (10, 2))
```

نتیجه:

``` text
┌─avgWeighted(x, weight)─┐
│                      8 │
└────────────────────────┘
```

## دانشگاه {#agg_function-uniq}

محاسبه تعداد تقریبی مقادیر مختلف استدلال.

``` sql
uniq(x[, ...])
```

**پارامترها**

تابع طول می کشد تعداد متغیر از پارامترهای. پارامترها می توانند باشند `Tuple`, `Array`, `Date`, `DateTime`, `String`, یا انواع عددی.

**مقدار بازگشتی**

-   A [UInt64](../../sql-reference/data-types/int-uint.md)- نوع شماره .

**پیاده سازی اطلاعات**

تابع:

-   هش را برای تمام پارامترها در مجموع محاسبه می کند و سپس در محاسبات استفاده می شود.

-   با استفاده از یک الگوریتم نمونه تطبیقی. برای محاسبه دولت تابع با استفاده از یک نمونه از عناصر هش ارزش تا 65536.

        This algorithm is very accurate and very efficient on the CPU. When the query contains several of these functions, using `uniq` is almost as fast as using other aggregate functions.

-   نتیجه را تعیین می کند (به سفارش پردازش پرس و جو بستگی ندارد).

ما توصیه می کنیم با استفاده از این تابع تقریبا در تمام حالات.

**همچنین نگاه کنید به**

-   [مخلوط نشده](#agg_function-uniqcombined)
-   [نیم قرن 64](#agg_function-uniqcombined64)
-   [یونقلل12](#agg_function-uniqhll12)
-   [قرارداد اتحادیه](#agg_function-uniqexact)

## مخلوط نشده {#agg_function-uniqcombined}

محاسبه تعداد تقریبی مقادیر استدلال های مختلف.

``` sql
uniqCombined(HLL_precision)(x[, ...])
```

این `uniqCombined` تابع یک انتخاب خوب برای محاسبه تعداد مقادیر مختلف است.

**پارامترها**

تابع طول می کشد تعداد متغیر از پارامترهای. پارامترها می توانند باشند `Tuple`, `Array`, `Date`, `DateTime`, `String`, یا انواع عددی.

`HLL_precision` پایه 2 لگاریتم تعداد سلول ها در است [جمع شدن](https://en.wikipedia.org/wiki/HyperLogLog). اختیاری, شما می توانید تابع به عنوان استفاده `uniqCombined(x[, ...])`. مقدار پیش فرض برای `HLL_precision` است 17, که به طور موثر 96 کیلوبایت فضا (2^17 سلول ها, 6 بیت در هر).

**مقدار بازگشتی**

-   یک عدد [UInt64](../../sql-reference/data-types/int-uint.md)- نوع شماره .

**پیاده سازی اطلاعات**

تابع:

-   محاسبه هش (هش 64 بیتی برای `String` و در غیر این صورت 32 بیتی) برای تمام پارامترها در مجموع و سپس در محاسبات استفاده می شود.

-   با استفاده از ترکیبی از سه الگوریتم: مجموعه, جدول هش, و جمع شدن با جدول تصحیح خطا.

        For a small number of distinct elements, an array is used. When the set size is larger, a hash table is used. For a larger number of elements, HyperLogLog is used, which will occupy a fixed amount of memory.

-   نتیجه را تعیین می کند (به سفارش پردازش پرس و جو بستگی ندارد).

!!! note "یادداشت"
    از هش 32 بیتی برای غیر استفاده می کند-`String` نوع, نتیجه خطا بسیار بالا برای کاریت به طور قابل توجهی بزرگتر از اند `UINT_MAX` (خطا به سرعت پس از چند ده میلیارد ارزش متمایز افزایش خواهد یافت), از این رو در این مورد شما باید استفاده کنید [نیم قرن 64](#agg_function-uniqcombined64)

در مقایسه با [دانشگاه](#agg_function-uniq) عملکرد `uniqCombined`:

-   مصرف چندین بار حافظه کمتر.
-   محاسبه با دقت چند بار بالاتر است.
-   معمولا عملکرد کمی پایین تر است. در برخی از حالات, `uniqCombined` می توانید بهتر از انجام `uniq` برای مثال با توزیع نمایش داده شد که انتقال تعداد زیادی از جمع متحده بر روی شبکه.

**همچنین نگاه کنید به**

-   [دانشگاه](#agg_function-uniq)
-   [نیم قرن 64](#agg_function-uniqcombined64)
-   [یونقلل12](#agg_function-uniqhll12)
-   [قرارداد اتحادیه](#agg_function-uniqexact)

## نیم قرن 64 {#agg_function-uniqcombined64}

مثل [مخلوط نشده](#agg_function-uniqcombined), اما با استفاده از هش 64 بیتی برای تمام انواع داده ها.

## یونقلل12 {#agg_function-uniqhll12}

محاسبه تعداد تقریبی مقادیر استدلال های مختلف, با استفاده از [جمع شدن](https://en.wikipedia.org/wiki/HyperLogLog) الگوریتم.

``` sql
uniqHLL12(x[, ...])
```

**پارامترها**

تابع طول می کشد تعداد متغیر از پارامترهای. پارامترها می توانند باشند `Tuple`, `Array`, `Date`, `DateTime`, `String`, یا انواع عددی.

**مقدار بازگشتی**

-   A [UInt64](../../sql-reference/data-types/int-uint.md)- نوع شماره .

**پیاده سازی اطلاعات**

تابع:

-   هش را برای تمام پارامترها در مجموع محاسبه می کند و سپس در محاسبات استفاده می شود.

-   با استفاده از الگوریتم جمع شدن تقریبی تعداد مقادیر استدلال های مختلف.

        212 5-bit cells are used. The size of the state is slightly more than 2.5 KB. The result is not very accurate (up to ~10% error) for small data sets (<10K elements). However, the result is fairly accurate for high-cardinality data sets (10K-100M), with a maximum error of ~1.6%. Starting from 100M, the estimation error increases, and the function will return very inaccurate results for data sets with extremely high cardinality (1B+ elements).

-   نتیجه تعیین شده را فراهم می کند (به سفارش پردازش پرس و جو بستگی ندارد).

ما توصیه نمی کنیم با استفاده از این تابع. در اغلب موارد از [دانشگاه](#agg_function-uniq) یا [مخلوط نشده](#agg_function-uniqcombined) تابع.

**همچنین نگاه کنید به**

-   [دانشگاه](#agg_function-uniq)
-   [مخلوط نشده](#agg_function-uniqcombined)
-   [قرارداد اتحادیه](#agg_function-uniqexact)

## قرارداد اتحادیه {#agg_function-uniqexact}

محاسبه تعداد دقیق ارزش استدلال های مختلف.

``` sql
uniqExact(x[, ...])
```

استفاده از `uniqExact` تابع اگر شما کاملا نیاز به یک نتیجه دقیق. در غیر این صورت استفاده از [دانشگاه](#agg_function-uniq) تابع.

این `uniqExact` تابع با استفاده از حافظه بیش از `uniq`, چرا که اندازه دولت رشد گشوده است به عنوان تعدادی از ارزش های مختلف را افزایش می دهد.

**پارامترها**

تابع طول می کشد تعداد متغیر از پارامترهای. پارامترها می توانند باشند `Tuple`, `Array`, `Date`, `DateTime`, `String`, یا انواع عددی.

**همچنین نگاه کنید به**

-   [دانشگاه](#agg_function-uniq)
-   [مخلوط نشده](#agg_function-uniqcombined)
-   [یونقلل12](#agg_function-uniqhll12)

## groupArray(x) groupArray(max\_size)(x) {#agg_function-grouparray}

مجموعه ای از مقادیر استدلال را ایجاد می کند.
مقادیر را می توان به ترتیب در هر (نامعین) اضافه کرد.

نسخه دوم (با `max_size` پارامتر) اندازه مجموعه حاصل را محدود می کند `max_size` عناصر.
به عنوان مثال, `groupArray (1) (x)` معادل است `[any (x)]`.

در بعضی موارد, شما هنوز هم می توانید در جهت اعدام تکیه. این امر در مورد مواردی که `SELECT` همراه از یک خرده فروشی که با استفاده از `ORDER BY`.

## هشدار داده می شود {#grouparrayinsertat}

مقدار را به مجموعه ای در موقعیت مشخص شده وارد می کند.

**نحو**

``` sql
groupArrayInsertAt(default_x, size)(x, pos);
```

اگر در یک پرس و جو چند مقدار به همان موقعیت قرار داده, تابع رفتار در روش های زیر:

-   اگر پرس و جو در یک موضوع اجرا, یکی از اولین از مقادیر درج شده استفاده شده است.
-   اگر یک پرس و جو در موضوعات مختلف اجرا, ارزش حاصل یکی نامشخص از مقادیر درج شده است.

**پارامترها**

-   `x` — Value to be inserted. [عبارت](../syntax.md#syntax-expressions) در نتیجه یکی از [انواع داده های پشتیبانی شده](../../sql-reference/data-types/index.md).
-   `pos` — Position at which the specified element `x` قرار داده می شود. شماره شاخص در مجموعه از صفر شروع می شود. [UInt32](../../sql-reference/data-types/int-uint.md#uint-ranges).
-   `default_x`— Default value for substituting in empty positions. Optional parameter. [عبارت](../syntax.md#syntax-expressions) در نتیجه نوع داده پیکربندی شده برای `x` پارامتر. اگر `default_x` تعریف نشده است [مقادیر پیشفرض](../../sql-reference/statements/create.md#create-default-values) استفاده می شود.
-   `size`— Length of the resulting array. Optional parameter. When using this parameter, the default value `default_x` باید مشخص شود. [UInt32](../../sql-reference/data-types/int-uint.md#uint-ranges).

**مقدار بازگشتی**

-   مجموعه ای با مقادیر درج شده.

نوع: [& حذف](../../sql-reference/data-types/array.md#data-type-array).

**مثال**

پرسوجو:

``` sql
SELECT groupArrayInsertAt(toString(number), number * 2) FROM numbers(5);
```

نتیجه:

``` text
┌─groupArrayInsertAt(toString(number), multiply(number, 2))─┐
│ ['0','','1','','2','','3','','4']                         │
└───────────────────────────────────────────────────────────┘
```

پرسوجو:

``` sql
SELECT groupArrayInsertAt('-')(toString(number), number * 2) FROM numbers(5);
```

نتیجه:

``` text
┌─groupArrayInsertAt('-')(toString(number), multiply(number, 2))─┐
│ ['0','-','1','-','2','-','3','-','4']                          │
└────────────────────────────────────────────────────────────────┘
```

پرسوجو:

``` sql
SELECT groupArrayInsertAt('-', 5)(toString(number), number * 2) FROM numbers(5);
```

نتیجه:

``` text
┌─groupArrayInsertAt('-', 5)(toString(number), multiply(number, 2))─┐
│ ['0','-','1','-','2']                                             │
└───────────────────────────────────────────────────────────────────┘
```

درج چند رشته ای از عناصر را به یک موقعیت.

پرسوجو:

``` sql
SELECT groupArrayInsertAt(number, 0) FROM numbers_mt(10) SETTINGS max_block_size = 1;
```

به عنوان یک نتیجه از این پرس و جو شما عدد صحیح تصادفی در `[0,9]` محدوده. به عنوان مثال:

``` text
┌─groupArrayInsertAt(number, 0)─┐
│ [7]                           │
└───────────────────────────────┘
```

## هشدار داده می شود {#agg_function-grouparraymovingsum}

محاسبه مجموع در حال حرکت از ارزش های ورودی.

``` sql
groupArrayMovingSum(numbers_for_summing)
groupArrayMovingSum(window_size)(numbers_for_summing)
```

این تابع می تواند اندازه پنجره به عنوان یک پارامتر را. اگر سمت چپ نامشخص, تابع طول می کشد اندازه پنجره به تعداد ردیف در ستون برابر.

**پارامترها**

-   `numbers_for_summing` — [عبارت](../syntax.md#syntax-expressions) در نتیجه یک مقدار نوع داده عددی.
-   `window_size` — Size of the calculation window.

**مقادیر بازگشتی**

-   مجموعه ای از همان اندازه و نوع به عنوان داده های ورودی.

**مثال**

جدول نمونه:

``` sql
CREATE TABLE t
(
    `int` UInt8,
    `float` Float32,
    `dec` Decimal32(2)
)
ENGINE = TinyLog
```

``` text
┌─int─┬─float─┬──dec─┐
│   1 │   1.1 │ 1.10 │
│   2 │   2.2 │ 2.20 │
│   4 │   4.4 │ 4.40 │
│   7 │  7.77 │ 7.77 │
└─────┴───────┴──────┘
```

نمایش داده شد:

``` sql
SELECT
    groupArrayMovingSum(int) AS I,
    groupArrayMovingSum(float) AS F,
    groupArrayMovingSum(dec) AS D
FROM t
```

``` text
┌─I──────────┬─F───────────────────────────────┬─D──────────────────────┐
│ [1,3,7,14] │ [1.1,3.3000002,7.7000003,15.47] │ [1.10,3.30,7.70,15.47] │
└────────────┴─────────────────────────────────┴────────────────────────┘
```

``` sql
SELECT
    groupArrayMovingSum(2)(int) AS I,
    groupArrayMovingSum(2)(float) AS F,
    groupArrayMovingSum(2)(dec) AS D
FROM t
```

``` text
┌─I──────────┬─F───────────────────────────────┬─D──────────────────────┐
│ [1,3,6,11] │ [1.1,3.3000002,6.6000004,12.17] │ [1.10,3.30,6.60,12.17] │
└────────────┴─────────────────────────────────┴────────────────────────┘
```

## گروهاریموینگاوگ {#agg_function-grouparraymovingavg}

محاسبه میانگین متحرک از ارزش های ورودی.

``` sql
groupArrayMovingAvg(numbers_for_summing)
groupArrayMovingAvg(window_size)(numbers_for_summing)
```

این تابع می تواند اندازه پنجره به عنوان یک پارامتر را. اگر سمت چپ نامشخص, تابع طول می کشد اندازه پنجره به تعداد ردیف در ستون برابر.

**پارامترها**

-   `numbers_for_summing` — [عبارت](../syntax.md#syntax-expressions) در نتیجه یک مقدار نوع داده عددی.
-   `window_size` — Size of the calculation window.

**مقادیر بازگشتی**

-   مجموعه ای از همان اندازه و نوع به عنوان داده های ورودی.

تابع استفاده می کند [گرد کردن به سمت صفر](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero). این کوتاه رقم اعشار ناچیز برای نوع داده و در نتیجه.

**مثال**

جدول نمونه `b`:

``` sql
CREATE TABLE t
(
    `int` UInt8,
    `float` Float32,
    `dec` Decimal32(2)
)
ENGINE = TinyLog
```

``` text
┌─int─┬─float─┬──dec─┐
│   1 │   1.1 │ 1.10 │
│   2 │   2.2 │ 2.20 │
│   4 │   4.4 │ 4.40 │
│   7 │  7.77 │ 7.77 │
└─────┴───────┴──────┘
```

نمایش داده شد:

``` sql
SELECT
    groupArrayMovingAvg(int) AS I,
    groupArrayMovingAvg(float) AS F,
    groupArrayMovingAvg(dec) AS D
FROM t
```

``` text
┌─I─────────┬─F───────────────────────────────────┬─D─────────────────────┐
│ [0,0,1,3] │ [0.275,0.82500005,1.9250001,3.8675] │ [0.27,0.82,1.92,3.86] │
└───────────┴─────────────────────────────────────┴───────────────────────┘
```

``` sql
SELECT
    groupArrayMovingAvg(2)(int) AS I,
    groupArrayMovingAvg(2)(float) AS F,
    groupArrayMovingAvg(2)(dec) AS D
FROM t
```

``` text
┌─I─────────┬─F────────────────────────────────┬─D─────────────────────┐
│ [0,1,3,5] │ [0.55,1.6500001,3.3000002,6.085] │ [0.55,1.65,3.30,6.08] │
└───────────┴──────────────────────────────────┴───────────────────────┘
```

## groupUniqArray(x) groupUniqArray(max\_size)(x) {#groupuniqarrayx-groupuniqarraymax-sizex}

مجموعه ای از مقادیر مختلف استدلال ایجاد می کند. مصرف حافظه همان است که برای `uniqExact` تابع.

نسخه دوم (با `max_size` پارامتر) اندازه مجموعه حاصل را محدود می کند `max_size` عناصر.
به عنوان مثال, `groupUniqArray(1)(x)` معادل است `[any(x)]`.

## quantile {#quantile}

محاسبه تقریبی [quantile](https://en.wikipedia.org/wiki/Quantile) از یک توالی داده های عددی.

این تابع اعمال می شود [نمونه برداری مخزن](https://en.wikipedia.org/wiki/Reservoir_sampling) با اندازه مخزن تا 8192 و یک مولد عدد تصادفی برای نمونه برداری. نتیجه غیر قطعی است. برای دریافت یک کمی دقیق, استفاده از [کوانتوم](#quantileexact) تابع.

هنگام استفاده از چندین `quantile*` توابع با سطوح مختلف در پرس و جو, کشورهای داخلی در ترکیب نیست (به این معنا که, پرس و جو کار می کند موثر کمتر از می تواند). در این مورد از [quantiles](#quantiles) تابع.

**نحو**

``` sql
quantile(level)(expr)
```

نام مستعار: `median`.

**پارامترها**

-   `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` مقدار در محدوده `[0.01, 0.99]`. مقدار پیش فرض: 0.5. در `level=0.5` تابع محاسبه می کند [میانه](https://en.wikipedia.org/wiki/Median).
-   `expr` — Expression over the column values resulting in numeric [انواع داده ها](../../sql-reference/data-types/index.md#data_types), [تاریخ](../../sql-reference/data-types/date.md) یا [DateTime](../../sql-reference/data-types/datetime.md).

**مقدار بازگشتی**

-   کمی تقریبی از سطح مشخص شده است.

نوع:

-   [جسم شناور64](../../sql-reference/data-types/float.md) برای ورودی نوع داده عددی.
-   [تاریخ](../../sql-reference/data-types/date.md) اگر مقادیر ورودی `Date` نوع.
-   [DateTime](../../sql-reference/data-types/datetime.md) اگر مقادیر ورودی `DateTime` نوع.

**مثال**

جدول ورودی:

``` text
┌─val─┐
│   1 │
│   1 │
│   2 │
│   3 │
└─────┘
```

پرسوجو:

``` sql
SELECT quantile(val) FROM t
```

نتیجه:

``` text
┌─quantile(val)─┐
│           1.5 │
└───────────────┘
```

**همچنین نگاه کنید به**

-   [میانه](#median)
-   [quantiles](#quantiles)

## نامعینیهای کوانتی {#quantiledeterministic}

محاسبه تقریبی [quantile](https://en.wikipedia.org/wiki/Quantile) از یک توالی داده های عددی.

این تابع اعمال می شود [نمونه برداری مخزن](https://en.wikipedia.org/wiki/Reservoir_sampling) با اندازه مخزن تا 8192 و الگوریتم قطعی نمونه گیری. نتیجه قطعی است. برای دریافت یک کمی دقیق, استفاده از [کوانتوم](#quantileexact) تابع.

هنگام استفاده از چندین `quantile*` توابع با سطوح مختلف در پرس و جو, کشورهای داخلی در ترکیب نیست (به این معنا که, پرس و جو کار می کند موثر کمتر از می تواند). در این مورد از [quantiles](#quantiles) تابع.

**نحو**

``` sql
quantileDeterministic(level)(expr, determinator)
```

نام مستعار: `medianDeterministic`.

**پارامترها**

-   `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` مقدار در محدوده `[0.01, 0.99]`. مقدار پیش فرض: 0.5. در `level=0.5` تابع محاسبه می کند [میانه](https://en.wikipedia.org/wiki/Median).
-   `expr` — Expression over the column values resulting in numeric [انواع داده ها](../../sql-reference/data-types/index.md#data_types), [تاریخ](../../sql-reference/data-types/date.md) یا [DateTime](../../sql-reference/data-types/datetime.md).
-   `determinator` — Number whose hash is used instead of a random number generator in the reservoir sampling algorithm to make the result of sampling deterministic. As a determinator you can use any deterministic positive number, for example, a user id or an event id. If the same determinator value occures too often, the function works incorrectly.

**مقدار بازگشتی**

-   کمی تقریبی از سطح مشخص شده است.

نوع:

-   [جسم شناور64](../../sql-reference/data-types/float.md) برای ورودی نوع داده عددی.
-   [تاریخ](../../sql-reference/data-types/date.md) اگر مقادیر ورودی `Date` نوع.
-   [DateTime](../../sql-reference/data-types/datetime.md) اگر مقادیر ورودی `DateTime` نوع.

**مثال**

جدول ورودی:

``` text
┌─val─┐
│   1 │
│   1 │
│   2 │
│   3 │
└─────┘
```

پرسوجو:

``` sql
SELECT quantileDeterministic(val, 1) FROM t
```

نتیجه:

``` text
┌─quantileDeterministic(val, 1)─┐
│                           1.5 │
└───────────────────────────────┘
```

**همچنین نگاه کنید به**

-   [میانه](#median)
-   [quantiles](#quantiles)

## کوانتوم {#quantileexact}

دقیقا محاسبه می کند [quantile](https://en.wikipedia.org/wiki/Quantile) از یک توالی داده های عددی.

To get exact value, all the passed values ​​are combined into an array, which is then partially sorted. Therefore, the function consumes `O(n)` حافظه, جایی که `n` تعدادی از ارزش هایی که تصویب شد. اما, برای تعداد کمی از ارزش, تابع بسیار موثر است.

هنگام استفاده از چندین `quantile*` توابع با سطوح مختلف در پرس و جو, کشورهای داخلی در ترکیب نیست (به این معنا که, پرس و جو کار می کند موثر کمتر از می تواند). در این مورد از [quantiles](#quantiles) تابع.

**نحو**

``` sql
quantileExact(level)(expr)
```

نام مستعار: `medianExact`.

**پارامترها**

-   `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` مقدار در محدوده `[0.01, 0.99]`. مقدار پیش فرض: 0.5. در `level=0.5` تابع محاسبه می کند [میانه](https://en.wikipedia.org/wiki/Median).
-   `expr` — Expression over the column values resulting in numeric [انواع داده ها](../../sql-reference/data-types/index.md#data_types), [تاریخ](../../sql-reference/data-types/date.md) یا [DateTime](../../sql-reference/data-types/datetime.md).

**مقدار بازگشتی**

-   Quantile از سطح مشخص شده.

نوع:

-   [جسم شناور64](../../sql-reference/data-types/float.md) برای ورودی نوع داده عددی.
-   [تاریخ](../../sql-reference/data-types/date.md) اگر مقادیر ورودی `Date` نوع.
-   [DateTime](../../sql-reference/data-types/datetime.md) اگر مقادیر ورودی `DateTime` نوع.

**مثال**

پرسوجو:

``` sql
SELECT quantileExact(number) FROM numbers(10)
```

نتیجه:

``` text
┌─quantileExact(number)─┐
│                     5 │
└───────────────────────┘
```

**همچنین نگاه کنید به**

-   [میانه](#median)
-   [quantiles](#quantiles)

## نمایش سایت {#quantileexactweighted}

دقیقا محاسبه می کند [quantile](https://en.wikipedia.org/wiki/Quantile) از یک توالی داده های عددی, با در نظر گرفتن وزن هر عنصر.

To get exact value, all the passed values ​​are combined into an array, which is then partially sorted. Each value is counted with its weight, as if it is present `weight` times. A hash table is used in the algorithm. Because of this, if the passed values ​​are frequently repeated, the function consumes less RAM than [کوانتوم](#quantileexact). شما می توانید این تابع به جای استفاده از `quantileExact` و وزن 1 را مشخص کنید.

هنگام استفاده از چندین `quantile*` توابع با سطوح مختلف در پرس و جو, کشورهای داخلی در ترکیب نیست (به این معنا که, پرس و جو کار می کند موثر کمتر از می تواند). در این مورد از [quantiles](#quantiles) تابع.

**نحو**

``` sql
quantileExactWeighted(level)(expr, weight)
```

نام مستعار: `medianExactWeighted`.

**پارامترها**

-   `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` مقدار در محدوده `[0.01, 0.99]`. مقدار پیش فرض: 0.5. در `level=0.5` تابع محاسبه می کند [میانه](https://en.wikipedia.org/wiki/Median).
-   `expr` — Expression over the column values resulting in numeric [انواع داده ها](../../sql-reference/data-types/index.md#data_types), [تاریخ](../../sql-reference/data-types/date.md) یا [DateTime](../../sql-reference/data-types/datetime.md).
-   `weight` — Column with weights of sequence members. Weight is a number of value occurrences.

**مقدار بازگشتی**

-   Quantile از سطح مشخص شده.

نوع:

-   [جسم شناور64](../../sql-reference/data-types/float.md) برای ورودی نوع داده عددی.
-   [تاریخ](../../sql-reference/data-types/date.md) اگر مقادیر ورودی `Date` نوع.
-   [DateTime](../../sql-reference/data-types/datetime.md) اگر مقادیر ورودی `DateTime` نوع.

**مثال**

جدول ورودی:

``` text
┌─n─┬─val─┐
│ 0 │   3 │
│ 1 │   2 │
│ 2 │   1 │
│ 5 │   4 │
└───┴─────┘
```

پرسوجو:

``` sql
SELECT quantileExactWeighted(n, val) FROM t
```

نتیجه:

``` text
┌─quantileExactWeighted(n, val)─┐
│                             1 │
└───────────────────────────────┘
```

**همچنین نگاه کنید به**

-   [میانه](#median)
-   [quantiles](#quantiles)

## زمان کمی {#quantiletiming}

با دقت تعیین شده محاسبه می شود [quantile](https://en.wikipedia.org/wiki/Quantile) از یک توالی داده های عددی.

نتیجه قطعی است(به سفارش پردازش پرس و جو بستگی ندارد). این تابع برای کار با توالی هایی که توزیع هایی مانند بارگذاری صفحات وب بار یا زمان پاسخ باطن را توصیف می کنند بهینه شده است.

هنگام استفاده از چندین `quantile*` توابع با سطوح مختلف در پرس و جو, کشورهای داخلی در ترکیب نیست (به این معنا که, پرس و جو کار می کند موثر کمتر از می تواند). در این مورد از [quantiles](#quantiles) تابع.

**نحو**

``` sql
quantileTiming(level)(expr)
```

نام مستعار: `medianTiming`.

**پارامترها**

-   `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` مقدار در محدوده `[0.01, 0.99]`. مقدار پیش فرض: 0.5. در `level=0.5` تابع محاسبه می کند [میانه](https://en.wikipedia.org/wiki/Median).

-   `expr` — [عبارت](../syntax.md#syntax-expressions) بیش از یک مقادیر ستون بازگشت [شناور\*](../../sql-reference/data-types/float.md)- نوع شماره .

        - If negative values are passed to the function, the behavior is undefined.
        - If the value is greater than 30,000 (a page loading time of more than 30 seconds), it is assumed to be 30,000.

**دقت**

محاسبه دقیق است اگر:

-   تعداد کل مقادیر 5670 تجاوز نمی کند.
-   تعداد کل مقادیر بیش از 5670, اما زمان بارگذاری صفحه کمتر از است 1024خانم.

در غیر این صورت, نتیجه محاسبه به نزدیکترین چند از گرد 16 خانم.

!!! note "یادداشت"
    برای محاسبه زمان بارگذاری صفحه quantiles این تابع این است که موثر تر و دقیق تر از [quantile](#quantile).

**مقدار بازگشتی**

-   Quantile از سطح مشخص شده.

نوع: `Float32`.

!!! note "یادداشت"
    اگر هیچ ارزش به تابع منتقل می شود (هنگام استفاده از `quantileTimingIf`), [نان](../../sql-reference/data-types/float.md#data_type-float-nan-inf) بازگشته است. هدف از این است که افتراق این موارد از مواردی که منجر به صفر. ببینید [ORDER BY](../statements/select/order-by.md#select-order-by) برای یادداشت ها در مرتب سازی `NaN` ارزشهای خبری عبارتند از:

**مثال**

جدول ورودی:

``` text
┌─response_time─┐
│            72 │
│           112 │
│           126 │
│           145 │
│           104 │
│           242 │
│           313 │
│           168 │
│           108 │
└───────────────┘
```

پرسوجو:

``` sql
SELECT quantileTiming(response_time) FROM t
```

نتیجه:

``` text
┌─quantileTiming(response_time)─┐
│                           126 │
└───────────────────────────────┘
```

**همچنین نگاه کنید به**

-   [میانه](#median)
-   [quantiles](#quantiles)

## زمان کمی {#quantiletimingweighted}

با دقت تعیین شده محاسبه می شود [quantile](https://en.wikipedia.org/wiki/Quantile) از یک توالی داده های عددی با توجه به وزن هر یک از اعضای دنباله.

نتیجه قطعی است(به سفارش پردازش پرس و جو بستگی ندارد). این تابع برای کار با توالی هایی که توزیع هایی مانند بارگذاری صفحات وب بار یا زمان پاسخ باطن را توصیف می کنند بهینه شده است.

هنگام استفاده از چندین `quantile*` توابع با سطوح مختلف در پرس و جو, کشورهای داخلی در ترکیب نیست (به این معنا که, پرس و جو کار می کند موثر کمتر از می تواند). در این مورد از [quantiles](#quantiles) تابع.

**نحو**

``` sql
quantileTimingWeighted(level)(expr, weight)
```

نام مستعار: `medianTimingWeighted`.

**پارامترها**

-   `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` مقدار در محدوده `[0.01, 0.99]`. مقدار پیش فرض: 0.5. در `level=0.5` تابع محاسبه می کند [میانه](https://en.wikipedia.org/wiki/Median).

-   `expr` — [عبارت](../syntax.md#syntax-expressions) بیش از یک مقادیر ستون بازگشت [شناور\*](../../sql-reference/data-types/float.md)- نوع شماره .

        - If negative values are passed to the function, the behavior is undefined.
        - If the value is greater than 30,000 (a page loading time of more than 30 seconds), it is assumed to be 30,000.

-   `weight` — Column with weights of sequence elements. Weight is a number of value occurrences.

**دقت**

محاسبه دقیق است اگر:

-   تعداد کل مقادیر 5670 تجاوز نمی کند.
-   تعداد کل مقادیر بیش از 5670, اما زمان بارگذاری صفحه کمتر از است 1024خانم.

در غیر این صورت, نتیجه محاسبه به نزدیکترین چند از گرد 16 خانم.

!!! note "یادداشت"
    برای محاسبه زمان بارگذاری صفحه quantiles این تابع این است که موثر تر و دقیق تر از [quantile](#quantile).

**مقدار بازگشتی**

-   Quantile از سطح مشخص شده.

نوع: `Float32`.

!!! note "یادداشت"
    اگر هیچ ارزش به تابع منتقل می شود (هنگام استفاده از `quantileTimingIf`), [نان](../../sql-reference/data-types/float.md#data_type-float-nan-inf) بازگشته است. هدف از این است که افتراق این موارد از مواردی که منجر به صفر. ببینید [ORDER BY](../statements/select/order-by.md#select-order-by) برای یادداشت ها در مرتب سازی `NaN` ارزشهای خبری عبارتند از:

**مثال**

جدول ورودی:

``` text
┌─response_time─┬─weight─┐
│            68 │      1 │
│           104 │      2 │
│           112 │      3 │
│           126 │      2 │
│           138 │      1 │
│           162 │      1 │
└───────────────┴────────┘
```

پرسوجو:

``` sql
SELECT quantileTimingWeighted(response_time, weight) FROM t
```

نتیجه:

``` text
┌─quantileTimingWeighted(response_time, weight)─┐
│                                           112 │
└───────────────────────────────────────────────┘
```

**همچنین نگاه کنید به**

-   [میانه](#median)
-   [quantiles](#quantiles)

## مقدار کمی {#quantiletdigest}

محاسبه تقریبی [quantile](https://en.wikipedia.org/wiki/Quantile) از یک توالی داده های عددی با استفاده از [خلاصه](https://github.com/tdunning/t-digest/blob/master/docs/t-digest-paper/histo.pdf) الگوریتم.

حداکثر خطا است 1%. مصرف حافظه است `log(n)` کجا `n` تعدادی از ارزش است. نتیجه بستگی دارد منظور از در حال اجرا پرس و جو و nondeterministic.

عملکرد تابع کمتر از عملکرد است [quantile](#quantile) یا [زمان کمی](#quantiletiming). از لحاظ نسبت اندازه دولت به دقت, این تابع بسیار بهتر از `quantile`.

هنگام استفاده از چندین `quantile*` توابع با سطوح مختلف در پرس و جو, کشورهای داخلی در ترکیب نیست (به این معنا که, پرس و جو کار می کند موثر کمتر از می تواند). در این مورد از [quantiles](#quantiles) تابع.

**نحو**

``` sql
quantileTDigest(level)(expr)
```

نام مستعار: `medianTDigest`.

**پارامترها**

-   `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` مقدار در محدوده `[0.01, 0.99]`. مقدار پیش فرض: 0.5. در `level=0.5` تابع محاسبه می کند [میانه](https://en.wikipedia.org/wiki/Median).
-   `expr` — Expression over the column values resulting in numeric [انواع داده ها](../../sql-reference/data-types/index.md#data_types), [تاریخ](../../sql-reference/data-types/date.md) یا [DateTime](../../sql-reference/data-types/datetime.md).

**مقدار بازگشتی**

-   کمی تقریبی از سطح مشخص شده است.

نوع:

-   [جسم شناور64](../../sql-reference/data-types/float.md) برای ورودی نوع داده عددی.
-   [تاریخ](../../sql-reference/data-types/date.md) اگر مقادیر ورودی `Date` نوع.
-   [DateTime](../../sql-reference/data-types/datetime.md) اگر مقادیر ورودی `DateTime` نوع.

**مثال**

پرسوجو:

``` sql
SELECT quantileTDigest(number) FROM numbers(10)
```

نتیجه:

``` text
┌─quantileTDigest(number)─┐
│                     4.5 │
└─────────────────────────┘
```

**همچنین نگاه کنید به**

-   [میانه](#median)
-   [quantiles](#quantiles)

## نمایش سایت {#quantiletdigestweighted}

محاسبه تقریبی [quantile](https://en.wikipedia.org/wiki/Quantile) از یک توالی داده های عددی با استفاده از [خلاصه](https://github.com/tdunning/t-digest/blob/master/docs/t-digest-paper/histo.pdf) الگوریتم. تابع طول می کشد را به حساب وزن هر یک از اعضای دنباله. حداکثر خطا است 1%. مصرف حافظه است `log(n)` کجا `n` تعدادی از ارزش است.

عملکرد تابع کمتر از عملکرد است [quantile](#quantile) یا [زمان کمی](#quantiletiming). از لحاظ نسبت اندازه دولت به دقت, این تابع بسیار بهتر از `quantile`.

نتیجه بستگی دارد منظور از در حال اجرا پرس و جو و nondeterministic.

هنگام استفاده از چندین `quantile*` توابع با سطوح مختلف در پرس و جو, کشورهای داخلی در ترکیب نیست (به این معنا که, پرس و جو کار می کند موثر کمتر از می تواند). در این مورد از [quantiles](#quantiles) تابع.

**نحو**

``` sql
quantileTDigest(level)(expr)
```

نام مستعار: `medianTDigest`.

**پارامترها**

-   `level` — Level of quantile. Optional parameter. Constant floating-point number from 0 to 1. We recommend using a `level` مقدار در محدوده `[0.01, 0.99]`. مقدار پیش فرض: 0.5. در `level=0.5` تابع محاسبه می کند [میانه](https://en.wikipedia.org/wiki/Median).
-   `expr` — Expression over the column values resulting in numeric [انواع داده ها](../../sql-reference/data-types/index.md#data_types), [تاریخ](../../sql-reference/data-types/date.md) یا [DateTime](../../sql-reference/data-types/datetime.md).
-   `weight` — Column with weights of sequence elements. Weight is a number of value occurrences.

**مقدار بازگشتی**

-   کمی تقریبی از سطح مشخص شده است.

نوع:

-   [جسم شناور64](../../sql-reference/data-types/float.md) برای ورودی نوع داده عددی.
-   [تاریخ](../../sql-reference/data-types/date.md) اگر مقادیر ورودی `Date` نوع.
-   [DateTime](../../sql-reference/data-types/datetime.md) اگر مقادیر ورودی `DateTime` نوع.

**مثال**

پرسوجو:

``` sql
SELECT quantileTDigestWeighted(number, 1) FROM numbers(10)
```

نتیجه:

``` text
┌─quantileTDigestWeighted(number, 1)─┐
│                                4.5 │
└────────────────────────────────────┘
```

**همچنین نگاه کنید به**

-   [میانه](#median)
-   [quantiles](#quantiles)

## میانه {#median}

این `median*` توابع نام مستعار برای مربوطه `quantile*` توابع. متوسط یک نمونه داده عددی را محاسبه می کنند.

توابع:

-   `median` — Alias for [quantile](#quantile).
-   `medianDeterministic` — Alias for [نامعینیهای کوانتی](#quantiledeterministic).
-   `medianExact` — Alias for [کوانتوم](#quantileexact).
-   `medianExactWeighted` — Alias for [نمایش سایت](#quantileexactweighted).
-   `medianTiming` — Alias for [زمان کمی](#quantiletiming).
-   `medianTimingWeighted` — Alias for [زمان کمی](#quantiletimingweighted).
-   `medianTDigest` — Alias for [مقدار کمی](#quantiletdigest).
-   `medianTDigestWeighted` — Alias for [نمایش سایت](#quantiletdigestweighted).

**مثال**

جدول ورودی:

``` text
┌─val─┐
│   1 │
│   1 │
│   2 │
│   3 │
└─────┘
```

پرسوجو:

``` sql
SELECT medianDeterministic(val, 1) FROM t
```

نتیجه:

``` text
┌─medianDeterministic(val, 1)─┐
│                         1.5 │
└─────────────────────────────┘
```

## quantiles(level1, level2, …)(x) {#quantiles}

تمام quantile توابع نیز مربوط quantiles توابع: `quantiles`, `quantilesDeterministic`, `quantilesTiming`, `quantilesTimingWeighted`, `quantilesExact`, `quantilesExactWeighted`, `quantilesTDigest`. این توابع محاسبه تمام کوانتوم از سطوح ذکر شده در یک پاس, و بازگشت مجموعه ای از مقادیر حاصل.

## اطلاعات دقیق) {#varsampx}

محاسبه مقدار `Σ((x - x̅)^2) / (n - 1)` کجا `n` اندازه نمونه است و `x̅`مقدار متوسط است `x`.

این نشان دهنده یک تخمین بی طرفانه از واریانس یک متغیر تصادفی اگر ارزش گذشت نمونه خود را تشکیل می دهند.

بازگشت `Float64`. چه زمانی `n <= 1`, بازگشت `+∞`.

!!! note "یادداشت"
    این تابع با استفاده از الگوریتم عددی ناپایدار. اگر شما نیاز دارید [پایداری عددی](https://en.wikipedia.org/wiki/Numerical_stability) در محاسبات, استفاده از `varSampStable` تابع. این کار کندتر, فراهم می کند اما خطای محاسباتی کمتر.

## هشدار داده می شود) {#varpopx}

محاسبه مقدار `Σ((x - x̅)^2) / n` کجا `n` اندازه نمونه است و `x̅`مقدار متوسط است `x`.

به عبارت دیگر, پراکندگی برای مجموعه ای از ارزش. بازگشت `Float64`.

!!! note "یادداشت"
    این تابع با استفاده از الگوریتم عددی ناپایدار. اگر شما نیاز دارید [پایداری عددی](https://en.wikipedia.org/wiki/Numerical_stability) در محاسبات, استفاده از `varPopStable` تابع. این کار کندتر, فراهم می کند اما خطای محاسباتی کمتر.

## اطلاعات دقیق) {#stddevsampx}

نتیجه برابر با ریشه مربع است `varSamp(x)`.

!!! note "یادداشت"
    این تابع با استفاده از الگوریتم عددی ناپایدار. اگر شما نیاز دارید [پایداری عددی](https://en.wikipedia.org/wiki/Numerical_stability) در محاسبات, استفاده از `stddevSampStable` تابع. این کار کندتر, فراهم می کند اما خطای محاسباتی کمتر.

## اطلاعات دقیق) {#stddevpopx}

نتیجه برابر با ریشه مربع است `varPop(x)`.

!!! note "یادداشت"
    این تابع با استفاده از الگوریتم عددی ناپایدار. اگر شما نیاز دارید [پایداری عددی](https://en.wikipedia.org/wiki/Numerical_stability) در محاسبات, استفاده از `stddevPopStable` تابع. این کار کندتر, فراهم می کند اما خطای محاسباتی کمتر.

## topK(N)(x) {#topknx}

بازگرداندن مجموعه ای از مقادیر تقریبا شایع ترین در ستون مشخص. مجموعه حاصل به ترتیب نزولی فرکانس تقریبی ارزش ها (نه با ارزش های خود) طبقه بندی شده اند.

پیاده سازی [فیلتر صرفه جویی در فضا](http://www.l2f.inesc-id.pt/~fmmb/wiki/uploads/Work/misnis.ref0a.pdf) الگوریتم برای تجزیه و تحلیل توپک, بر اساس الگوریتم کاهش و ترکیب از [صرفه جویی در فضای موازی](https://arxiv.org/pdf/1401.0702.pdf).

``` sql
topK(N)(column)
```

این تابع یک نتیجه تضمین شده را فراهم نمی کند. در شرایط خاص, اشتباهات ممکن است رخ دهد و ممکن است مقادیر مکرر که مقادیر شایع ترین نیست بازگشت.

ما توصیه می کنیم با استفاده از `N < 10` عملکرد با بزرگ کاهش می یابد `N` ارزشهای خبری عبارتند از: حداکثر مقدار `N = 65536`.

**پارامترها**

-   ‘N’ است تعدادی از عناصر به بازگشت.

اگر پارامتر حذف شده است, مقدار پیش فرض 10 استفاده شده است.

**نشانوندها**

-   ' x ' – The value to calculate frequency.

**مثال**

نگاهی به [به موقع](../../getting-started/example-datasets/ontime.md) مجموعه داده ها و انتخاب سه ارزش اغلب اتفاق می افتد در `AirlineID` ستون.

``` sql
SELECT topK(3)(AirlineID) AS res
FROM ontime
```

``` text
┌─res─────────────────┐
│ [19393,19790,19805] │
└─────────────────────┘
```

## کشتی کج {#topkweighted}

مشابه به `topK` اما طول می کشد یک استدلال اضافی از نوع صحیح - `weight`. هر مقدار به حساب `weight` بار برای محاسبه فرکانس.

**نحو**

``` sql
topKWeighted(N)(x, weight)
```

**پارامترها**

-   `N` — The number of elements to return.

**نشانوندها**

-   `x` – The value.
-   `weight` — The weight. [UInt8](../../sql-reference/data-types/int-uint.md).

**مقدار بازگشتی**

بازگرداندن مجموعه ای از مقادیر با حداکثر مجموع تقریبی وزن.

**مثال**

پرسوجو:

``` sql
SELECT topKWeighted(10)(number, number) FROM numbers(1000)
```

نتیجه:

``` text
┌─topKWeighted(10)(number, number)──────────┐
│ [999,998,997,996,995,994,993,992,991,990] │
└───────────────────────────────────────────┘
```

## هشدار داده می شود) {#covarsampx-y}

محاسبه ارزش `Σ((x - x̅)(y - y̅)) / (n - 1)`.

را برمی گرداند شناور64. چه زمانی `n <= 1`, returns +∞.

!!! note "یادداشت"
    این تابع با استفاده از الگوریتم عددی ناپایدار. اگر شما نیاز دارید [پایداری عددی](https://en.wikipedia.org/wiki/Numerical_stability) در محاسبات, استفاده از `covarSampStable` تابع. این کار کندتر, فراهم می کند اما خطای محاسباتی کمتر.

## نمایش سایت) {#covarpopx-y}

محاسبه ارزش `Σ((x - x̅)(y - y̅)) / n`.

!!! note "یادداشت"
    این تابع با استفاده از الگوریتم عددی ناپایدار. اگر شما نیاز دارید [پایداری عددی](https://en.wikipedia.org/wiki/Numerical_stability) در محاسبات, استفاده از `covarPopStable` تابع. این کار کندتر فراهم می کند اما یک خطای محاسباتی کمتر.

## هشدار داده می شود) {#corrx-y}

محاسبه ضریب همبستگی پیرسون: `Σ((x - x̅)(y - y̅)) / sqrt(Σ((x - x̅)^2) * Σ((y - y̅)^2))`.

!!! note "یادداشت"
    این تابع با استفاده از الگوریتم عددی ناپایدار. اگر شما نیاز دارید [پایداری عددی](https://en.wikipedia.org/wiki/Numerical_stability) در محاسبات, استفاده از `corrStable` تابع. این کار کندتر, فراهم می کند اما خطای محاسباتی کمتر.

## طبقه بندی فرمول بندی {#categoricalinformationvalue}

محاسبه ارزش `(P(tag = 1) - P(tag = 0))(log(P(tag = 1)) - log(P(tag = 0)))` برای هر دسته.

``` sql
categoricalInformationValue(category1, category2, ..., tag)
```

نتیجه نشان می دهد که چگونه یک ویژگی گسسته (قطعی) `[category1, category2, ...]` کمک به یک مدل یادگیری که پیش بینی ارزش `tag`.

## ساده سازی مقررات {#simplelinearregression}

انجام ساده (unidimensional) رگرسیون خطی.

``` sql
simpleLinearRegression(x, y)
```

پارامترها:

-   `x` — Column with dependent variable values.
-   `y` — Column with explanatory variable values.

مقادیر بازگشتی:

ثابتها `(a, b)` از خط نتیجه `y = a*x + b`.

**مثالها**

``` sql
SELECT arrayReduce('simpleLinearRegression', [0, 1, 2, 3], [0, 1, 2, 3])
```

``` text
┌─arrayReduce('simpleLinearRegression', [0, 1, 2, 3], [0, 1, 2, 3])─┐
│ (1,0)                                                             │
└───────────────────────────────────────────────────────────────────┘
```

``` sql
SELECT arrayReduce('simpleLinearRegression', [0, 1, 2, 3], [3, 4, 5, 6])
```

``` text
┌─arrayReduce('simpleLinearRegression', [0, 1, 2, 3], [3, 4, 5, 6])─┐
│ (1,3)                                                             │
└───────────────────────────────────────────────────────────────────┘
```

## تنظیم مقررات {#agg_functions-stochasticlinearregression}

این تابع پیاده سازی رگرسیون خطی تصادفی. این پشتیبانی از پارامترهای سفارشی برای نرخ یادگیری, ل2 ضریب منظم, اندازه مینی دسته ای و دارای چند روش برای به روز رسانی وزن ([ادام](https://en.wikipedia.org/wiki/Stochastic_gradient_descent#Adam) (به طور پیش فرض استفاده می شود), [اطلاعات دقیق](https://en.wikipedia.org/wiki/Stochastic_gradient_descent), [شتاب](https://en.wikipedia.org/wiki/Stochastic_gradient_descent#Momentum), [نستروف](https://mipt.ru/upload/medialibrary/d7e/41-91.pdf)).

### پارامترها {#agg_functions-stochasticlinearregression-parameters}

4 پارامتر قابل تنظیم وجود دارد. به ترتیب تابع منتقل می شود اما بدون نیاز به تصویب تمام مقادیر چهار پیش فرض استفاده می شود با این حال مدل خوب مورد نیاز برخی از تنظیم پارامتر وجود دارد.

``` text
stochasticLinearRegression(1.0, 1.0, 10, 'SGD')
```

1.  `learning rate` ضریب در طول گام است, زمانی که گام گرادیان تبار انجام شده است. نرخ یادگیری بیش از حد بزرگ ممکن است وزن بی نهایت از مدل شود. پیشفرض `0.00001`.
2.  `l2 regularization coefficient` که ممکن است کمک به جلوگیری از سوراخ سوراخ شدن بیش از حد. پیشفرض `0.1`.
3.  `mini-batch size` مجموعه تعدادی از عناصر که شیب محاسبه خواهد شد و خلاصه به انجام یک مرحله از گرادیان تبار. تبار تصادفی خالص با استفاده از یک عنصر, با این حال داشتن دسته های کوچک(در باره 10 عناصر) را گام شیب پایدار تر. پیشفرض `15`.
4.  `method for updating weights` اونا: `Adam` (به طور پیش فرض), `SGD`, `Momentum`, `Nesterov`. `Momentum` و `Nesterov` نیاز به کمی بیشتر محاسبات و حافظه, اما آنها به اتفاق مفید از نظر سرعت convergance و ثبات stochastic gradient روش.

### استفاده {#agg_functions-stochasticlinearregression-usage}

`stochasticLinearRegression` در دو مرحله استفاده می شود: اتصالات مدل و پیش بینی بر روی داده های جدید. به منظور متناسب با مدل و صرفه جویی در دولت خود را برای استفاده های بعدی استفاده می کنیم `-State` ترکیب کننده, که اساسا موجب صرفه جویی در دولت (وزن مدل, و غیره).
برای پیش بینی ما با استفاده از تابع [ارزیابی](../functions/machine-learning-functions.md#machine_learning_methods-evalmlmethod), که طول می کشد یک دولت به عنوان یک استدلال و همچنین ویژگی های به پیش بینی در.

<a name="stochasticlinearregression-usage-fitting"></a>

**1.** اتصالات

چنین پرس و جو ممکن است مورد استفاده قرار گیرد.

``` sql
CREATE TABLE IF NOT EXISTS train_data
(
    param1 Float64,
    param2 Float64,
    target Float64
) ENGINE = Memory;

CREATE TABLE your_model ENGINE = Memory AS SELECT
stochasticLinearRegressionState(0.1, 0.0, 5, 'SGD')(target, param1, param2)
AS state FROM train_data;
```

در اینجا ما همچنین نیاز به وارد کردن داده ها به `train_data` جدول تعداد پارامترهای ثابت نیست, این تنها در تعدادی از استدلال بستگی دارد, گذشت به `linearRegressionState`. همه باید مقادیر عددی باشد.
توجه داشته باشید که ستون با ارزش هدف(که ما می خواهم برای یادگیری به پیش بینی) به عنوان اولین استدلال قرار داده شده است.

**2.** پیش بینی

پس از ذخیره یک دولت به جدول, ما ممکن است چندین بار برای پیش بینی استفاده, و یا حتی با کشورهای دیگر ادغام و ایجاد مدل های جدید و حتی بهتر.

``` sql
WITH (SELECT state FROM your_model) AS model SELECT
evalMLMethod(model, param1, param2) FROM test_data
```

پرس و جو یک ستون از مقادیر پیش بینی شده بازگشت. توجه داشته باشید که استدلال اول `evalMLMethod` هست `AggregateFunctionState` هدف, بعدی ستون از ویژگی های هستند.

`test_data` یک جدول مانند `train_data` اما ممکن است حاوی ارزش هدف نیست.

### یادداشتها {#agg_functions-stochasticlinearregression-notes}

1.  برای ادغام دو مدل کاربر ممکن است چنین پرس و جو ایجاد کنید:
    `sql  SELECT state1 + state2 FROM your_models`
    کجا `your_models` جدول شامل هر دو مدل. این پرس و جو جدید باز خواهد گشت `AggregateFunctionState` اعتراض.

2.  کاربر ممکن است وزن مدل ایجاد شده برای اهداف خود را بدون صرفه جویی در مدل اگر هیچ واکشی `-State` ترکیب استفاده شده است.
    `sql  SELECT stochasticLinearRegression(0.01)(target, param1, param2) FROM train_data`
    چنین پرس و جو خواهد مدل مناسب و بازگشت وزن خود را - برای اولین بار وزن هستند, که به پارامترهای مدل مطابقت, یکی از گذشته تعصب است. بنابراین در مثال بالا پرس و جو یک ستون با 3 مقدار بازگشت.

**همچنین نگاه کنید به**

-   [سرکوب مقررات عمومی](#agg_functions-stochasticlogisticregression)
-   [تفاوت رگرسیون خطی و لجستیک](https://stackoverflow.com/questions/12146914/what-is-the-difference-between-linear-regression-and-logistic-regression)

## سرکوب مقررات عمومی {#agg_functions-stochasticlogisticregression}

این تابع پیاده سازی رگرسیون لجستیک تصادفی. این را می توان برای مشکل طبقه بندی دودویی استفاده, پشتیبانی از پارامترهای سفارشی به عنوان مقررات زدایی و کار به همان شیوه.

### پارامترها {#agg_functions-stochasticlogisticregression-parameters}

پارامترها دقیقا مشابه در تنظیم مقررات است:
`learning rate`, `l2 regularization coefficient`, `mini-batch size`, `method for updating weights`.
برای اطلاعات بیشتر نگاه کنید به [پارامترها](#agg_functions-stochasticlinearregression-parameters).

``` text
stochasticLogisticRegression(1.0, 1.0, 10, 'SGD')
```

1.  اتصالات

<!-- -->

    See the `Fitting` section in the [stochasticLinearRegression](#stochasticlinearregression-usage-fitting) description.

    Predicted labels have to be in \[-1, 1\].

1.  پیش بینی

<!-- -->

    Using saved state we can predict probability of object having label `1`.

    ``` sql
    WITH (SELECT state FROM your_model) AS model SELECT
    evalMLMethod(model, param1, param2) FROM test_data
    ```

    The query will return a column of probabilities. Note that first argument of `evalMLMethod` is `AggregateFunctionState` object, next are columns of features.

    We can also set a bound of probability, which assigns elements to different labels.

    ``` sql
    SELECT ans < 1.1 AND ans > 0.5 FROM
    (WITH (SELECT state FROM your_model) AS model SELECT
    evalMLMethod(model, param1, param2) AS ans FROM test_data)
    ```

    Then the result will be labels.

    `test_data` is a table like `train_data` but may not contain target value.

**همچنین نگاه کنید به**

-   [تنظیم مقررات](#agg_functions-stochasticlinearregression)
-   [تفاوت بین رگرسیون خطی و لجستیک.](https://stackoverflow.com/questions/12146914/what-is-the-difference-between-linear-regression-and-logistic-regression)

## گروهبیتمافند {#groupbitmapand}

محاسبات و یک بیت مپ ستون بازگشت cardinality از نوع UInt64 اگر اضافه کردن پسوند -دولت بازگشت [شی نگاشت بیت](../../sql-reference/functions/bitmap-functions.md).

``` sql
groupBitmapAnd(expr)
```

**پارامترها**

`expr` – An expression that results in `AggregateFunction(groupBitmap, UInt*)` نوع.

**مقدار بازگشتی**

ارزش `UInt64` نوع.

**مثال**

``` sql
DROP TABLE IF EXISTS bitmap_column_expr_test2;
CREATE TABLE bitmap_column_expr_test2
(
    tag_id String,
    z AggregateFunction(groupBitmap, UInt32)
)
ENGINE = MergeTree
ORDER BY tag_id;

INSERT INTO bitmap_column_expr_test2 VALUES ('tag1', bitmapBuild(cast([1,2,3,4,5,6,7,8,9,10] as Array(UInt32))));
INSERT INTO bitmap_column_expr_test2 VALUES ('tag2', bitmapBuild(cast([6,7,8,9,10,11,12,13,14,15] as Array(UInt32))));
INSERT INTO bitmap_column_expr_test2 VALUES ('tag3', bitmapBuild(cast([2,4,6,8,10,12] as Array(UInt32))));

SELECT groupBitmapAnd(z) FROM bitmap_column_expr_test2 WHERE like(tag_id, 'tag%');
┌─groupBitmapAnd(z)─┐
│               3   │
└───────────────────┘

SELECT arraySort(bitmapToArray(groupBitmapAndState(z))) FROM bitmap_column_expr_test2 WHERE like(tag_id, 'tag%');
┌─arraySort(bitmapToArray(groupBitmapAndState(z)))─┐
│ [6,8,10]                                         │
└──────────────────────────────────────────────────┘
```

## گروهبیتمافور {#groupbitmapor}

محاسبات و یا یک بیت مپ ستون بازگشت cardinality از نوع UInt64 اگر اضافه کردن پسوند -دولت بازگشت [شی نگاشت بیت](../../sql-reference/functions/bitmap-functions.md). این معادل است `groupBitmapMerge`.

``` sql
groupBitmapOr(expr)
```

**پارامترها**

`expr` – An expression that results in `AggregateFunction(groupBitmap, UInt*)` نوع.

**مقدار بازگشتی**

ارزش `UInt64` نوع.

**مثال**

``` sql
DROP TABLE IF EXISTS bitmap_column_expr_test2;
CREATE TABLE bitmap_column_expr_test2
(
    tag_id String,
    z AggregateFunction(groupBitmap, UInt32)
)
ENGINE = MergeTree
ORDER BY tag_id;

INSERT INTO bitmap_column_expr_test2 VALUES ('tag1', bitmapBuild(cast([1,2,3,4,5,6,7,8,9,10] as Array(UInt32))));
INSERT INTO bitmap_column_expr_test2 VALUES ('tag2', bitmapBuild(cast([6,7,8,9,10,11,12,13,14,15] as Array(UInt32))));
INSERT INTO bitmap_column_expr_test2 VALUES ('tag3', bitmapBuild(cast([2,4,6,8,10,12] as Array(UInt32))));

SELECT groupBitmapOr(z) FROM bitmap_column_expr_test2 WHERE like(tag_id, 'tag%');
┌─groupBitmapOr(z)─┐
│             15   │
└──────────────────┘

SELECT arraySort(bitmapToArray(groupBitmapOrState(z))) FROM bitmap_column_expr_test2 WHERE like(tag_id, 'tag%');
┌─arraySort(bitmapToArray(groupBitmapOrState(z)))─┐
│ [1,2,3,4,5,6,7,8,9,10,11,12,13,14,15]           │
└─────────────────────────────────────────────────┘
```

## گروهبیتمافکر {#groupbitmapxor}

محاسبات XOR یک بیت مپ ستون بازگشت cardinality از نوع UInt64 اگر اضافه کردن پسوند -دولت بازگشت [شی نگاشت بیت](../../sql-reference/functions/bitmap-functions.md).

``` sql
groupBitmapOr(expr)
```

**پارامترها**

`expr` – An expression that results in `AggregateFunction(groupBitmap, UInt*)` نوع.

**مقدار بازگشتی**

ارزش `UInt64` نوع.

**مثال**

``` sql
DROP TABLE IF EXISTS bitmap_column_expr_test2;
CREATE TABLE bitmap_column_expr_test2
(
    tag_id String,
    z AggregateFunction(groupBitmap, UInt32)
)
ENGINE = MergeTree
ORDER BY tag_id;

INSERT INTO bitmap_column_expr_test2 VALUES ('tag1', bitmapBuild(cast([1,2,3,4,5,6,7,8,9,10] as Array(UInt32))));
INSERT INTO bitmap_column_expr_test2 VALUES ('tag2', bitmapBuild(cast([6,7,8,9,10,11,12,13,14,15] as Array(UInt32))));
INSERT INTO bitmap_column_expr_test2 VALUES ('tag3', bitmapBuild(cast([2,4,6,8,10,12] as Array(UInt32))));

SELECT groupBitmapXor(z) FROM bitmap_column_expr_test2 WHERE like(tag_id, 'tag%');
┌─groupBitmapXor(z)─┐
│              10   │
└───────────────────┘

SELECT arraySort(bitmapToArray(groupBitmapXorState(z))) FROM bitmap_column_expr_test2 WHERE like(tag_id, 'tag%');
┌─arraySort(bitmapToArray(groupBitmapXorState(z)))─┐
│ [1,3,5,6,8,10,11,13,14,15]                       │
└──────────────────────────────────────────────────┘
```

[مقاله اصلی](https://clickhouse.tech/docs/en/query_language/agg_functions/reference/) <!--hide-->
