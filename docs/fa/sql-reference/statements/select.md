---
machine_translated: true
machine_translated_rev: 0f7ef7704d018700049223525bad4a63911b6e70
toc_priority: 33
toc_title: SELECT
---

# نحو نمایش داده شد را انتخاب کنید {#select-queries-syntax}

`SELECT` بازیابی داده ها را انجام می دهد.

``` sql
[WITH expr_list|(subquery)]
SELECT [DISTINCT] expr_list
[FROM [db.]table | (subquery) | table_function] [FINAL]
[SAMPLE sample_coeff]
[ARRAY JOIN ...]
[GLOBAL] [ANY|ALL] [INNER|LEFT|RIGHT|FULL|CROSS] [OUTER] JOIN (subquery)|table USING columns_list
[PREWHERE expr]
[WHERE expr]
[GROUP BY expr_list] [WITH TOTALS]
[HAVING expr]
[ORDER BY expr_list]
[LIMIT [offset_value, ]n BY columns]
[LIMIT [n, ]m]
[UNION ALL ...]
[INTO OUTFILE filename]
[FORMAT format]
```

همه بند اختیاری هستند, به جز برای لیست مورد نیاز از عبارات بلافاصله پس از انتخاب.
بند های زیر تقریبا به همان ترتیب در نوار نقاله اجرای پرس و جو توصیف می شوند.

اگر پرس و جو حذف `DISTINCT`, `GROUP BY` و `ORDER BY` بند و `IN` و `JOIN` کارخانه های فرعی, پرس و جو خواهد شد به طور کامل جریان پردازش, با استفاده از ای(1) مقدار رم.
در غیر این صورت, پرس و جو ممکن است مقدار زیادی از رم مصرف اگر محدودیت های مناسب مشخص نشده است: `max_memory_usage`, `max_rows_to_group_by`, `max_rows_to_sort`, `max_rows_in_distinct`, `max_bytes_in_distinct`, `max_rows_in_set`, `max_bytes_in_set`, `max_rows_in_join`, `max_bytes_in_join`, `max_bytes_before_external_sort`, `max_bytes_before_external_group_by`. برای کسب اطلاعات بیشتر به بخش مراجعه کنید “Settings”. ممکن است که به استفاده از مرتب سازی خارجی (صرفه جویی در جداول موقت به یک دیسک) و تجمع خارجی. `The system does not have "merge join"`.

### با بند {#with-clause}

در این بخش پشتیبانی از عبارات جدول مشترک فراهم می کند ([CTE](https://en.wikipedia.org/wiki/Hierarchical_and_recursive_queries_in_SQL)), با برخی از محدودیت:
1. نمایش داده شد بازگشتی پشتیبانی نمی شوند
2. هنگامی که زیرخاکری در داخل با بخش استفاده می شود, این نتیجه باید اسکالر با دقیقا یک ردیف باشد
3. بیان نتایج در دسترس نیست در subqueries
نتایج با عبارات بند را می توان در داخل بند را انتخاب کنید استفاده می شود.

مثال 1: با استفاده از عبارت ثابت به عنوان “variable”

``` sql
WITH '2019-08-01 15:23:00' as ts_upper_bound
SELECT *
FROM hits
WHERE
    EventDate = toDate(ts_upper_bound) AND
    EventTime <= ts_upper_bound
```

مثال 2: جمع تخلیه(بایت) نتیجه بیان از بند لیست ستون را انتخاب کنید

``` sql
WITH sum(bytes) as s
SELECT
    formatReadableSize(s),
    table
FROM system.parts
GROUP BY table
ORDER BY s
```

مثال 3: با استفاده از نتایج حاصل از زیرخاکری اسکالر

``` sql
/* this example would return TOP 10 of most huge tables */
WITH
    (
        SELECT sum(bytes)
        FROM system.parts
        WHERE active
    ) AS total_disk_usage
SELECT
    (sum(bytes) / total_disk_usage) * 100 AS table_disk_usage,
    table
FROM system.parts
GROUP BY table
ORDER BY table_disk_usage DESC
LIMIT 10
```

مثال 4: استفاده مجدد از بیان در زیرخاکری
به عنوان یک راه حل برای محدودیت فعلی برای استفاده بیان در زیر مجموعه, شما ممکن است تکراری.

``` sql
WITH ['hello'] AS hello
SELECT
    hello,
    *
FROM
(
    WITH ['hello'] AS hello
    SELECT hello
)
```

``` text
┌─hello─────┬─hello─────┐
│ ['hello'] │ ['hello'] │
└───────────┴───────────┘
```

### از بند {#select-from}

اگر از بند حذف شده است, داده خواهد شد از خواندن `system.one` جدول
این `system.one` جدول شامل دقیقا یک ردیف است (این جدول همان هدف را به عنوان جدول دوگانه موجود در سایر دساماسها انجام می دهد).

این `FROM` بند منبع برای خواندن داده ها از مشخص:

-   جدول
-   خرده فروشی
-   [تابع جدول](../table-functions/index.md#table-functions)

`ARRAY JOIN` و به طور منظم `JOIN` همچنین ممکن است شامل شود (پایین را ببینید).

به جای یک جدول `SELECT` خرده فروشی ممکن است در پرانتز مشخص.
در مقابل به گذاشتن استاندارد, مترادف نیازی به پس از یک خرده فروشی مشخص شود.

برای اجرای پرس و جو تمام ستون های ذکر شده در پرس و جو از جدول مناسب استخراج می شوند. هر ستون برای پرس و جو خارجی مورد نیاز نیست از کارخانه های فرعی پرتاب می شود.
اگر پرس و جو هیچ ستون لیست نیست (به عنوان مثال, `SELECT count() FROM t`), برخی از ستون از جدول استخراج به هر حال (کوچکترین ترجیح داده می شود), به منظور محاسبه تعداد ردیف.

#### تغییردهنده نهایی {#select-from-final}

قابل استفاده در هنگام انتخاب داده ها از جداول از [ادغام](../../engines/table-engines/mergetree-family/mergetree.md)- خانواده موتور غیر از `GraphiteMergeTree`. چه زمانی `FINAL` مشخص شده است, تاتر به طور کامل ادغام داده ها قبل از بازگشت به نتیجه و در نتیجه انجام تمام تحولات داده که در طول ادغام برای موتور جدول داده شده اتفاق می افتد.

همچنین برای پشتیبانی:
- [تکرار](../../engines/table-engines/mergetree-family/replication.md) نسخه های `MergeTree` موتورها.
- [نما](../../engines/table-engines/special/view.md), [بافر](../../engines/table-engines/special/buffer.md), [توزیع شده](../../engines/table-engines/special/distributed.md) و [ماده بینی](../../engines/table-engines/special/materializedview.md) موتورها که بیش از موتورهای دیگر کار می کنند به شرطی که بیش از ایجاد شده اند `MergeTree`- جدول موتور .

نمایش داده شد که با استفاده از `FINAL` اعدام به همان سرعتی که نمایش داده شد مشابه که نمی, زیرا:

-   پرس و جو در یک موضوع اجرا و داده ها در طول اجرای پرس و جو با هم ادغام شدند.
-   نمایش داده شد با `FINAL` خوانده شده ستون کلید اولیه در علاوه بر این به ستون مشخص شده در پرس و جو.

در بیشتر موارد, اجتناب از استفاده از `FINAL`.

### بند نمونه {#select-sample-clause}

این `SAMPLE` بند اجازه می دهد تا برای پردازش پرس و جو تقریبی.

هنگامی که نمونه گیری داده ها فعال است, پرس و جو بر روی تمام داده ها انجام نمی, اما تنها در بخش خاصی از داده ها (نمونه). مثلا, اگر شما نیاز به محاسبه ارقام برای تمام بازدیدکننده داشته است, کافی است برای اجرای پرس و جو در 1/10 کسری از تمام بازدیدکننده داشته است و سپس ضرب در نتیجه 10.

پردازش پرس و جو تقریبی می تواند در موارد زیر مفید باشد:

-   هنگامی که شما شرایط زمان بندی دقیق (مانند \<100 مگابایت) اما شما نمی توانید هزینه منابع سخت افزاری اضافی را برای دیدار با خود توجیه کنید.
-   هنگامی که داده های خام خود را دقیق نیست, بنابراین تقریب می کند به طرز محسوسی کاهش کیفیت.
-   کسب و کار مورد نیاز هدف قرار دادن نتایج تقریبی (برای مقرون به صرفه بودن, و یا به منظور بازار نتایج دقیق به کاربران حق بیمه).

!!! note "یادداشت"
    شما فقط می توانید نمونه برداری با استفاده از جداول در [ادغام](../../engines/table-engines/mergetree-family/mergetree.md) خانواده, و تنها در صورتی که بیان نمونه برداری در ایجاد جدول مشخص شد (دیدن [موتور ادغام](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table)).

ویژگی های نمونه گیری داده ها به شرح زیر است:

-   نمونهگیری دادهها یک مکانیسم قطعی است. نتیجه همان `SELECT .. SAMPLE` پرس و جو همیشه یکسان است.
-   نمونه گیری به طور مداوم برای جداول مختلف کار می کند. برای جداول با یک کلید نمونه برداری تک, یک نمونه با ضریب همان همیشه زیر مجموعه همان داده های ممکن را انتخاب. برای مثال یک نمونه از شناسه های کاربر طول می کشد ردیف با همان زیر مجموعه از همه ممکن است شناسه کاربر از جداول مختلف. این به این معنی است که شما می توانید نمونه در کارخانه های فرعی در استفاده از [IN](#select-in-operators) بند بند. همچنین شما می توانید نمونه ها را با استفاده از [JOIN](#select-join) بند بند.
-   نمونه گیری اجازه می دهد تا خواندن اطلاعات کمتر از یک دیسک. توجه داشته باشید که شما باید کلید نمونه برداری به درستی مشخص کنید. برای کسب اطلاعات بیشتر, دیدن [ایجاد یک جدول ادغام](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table).

برای `SAMPLE` بند نحو زیر پشتیبانی می شود:

| SAMPLE Clause Syntax | توصیف                                                                                                                                                                                                                                                         |
|----------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `SAMPLE k`           | اینجا `k` است تعداد از 0 به 1.</br>پرس و جو در اجرا `k` کسری از داده ها. به عنوان مثال, `SAMPLE 0.1` پرس و جو را در 10 درصد از داده ها اجرا می کند. [ادامه مطلب](#select-sample-k)                                                                            |
| `SAMPLE n`           | اینجا `n` عدد صحیح به اندازه کافی بزرگ است.</br>پرس و جو بر روی یک نمونه از حداقل اعدام `n` ردیف (اما نه به طور قابل توجهی بیشتر از این). به عنوان مثال, `SAMPLE 10000000` پرس و جو را در حداقل ردیف های 10000000 اجرا می کند. [ادامه مطلب](#select-sample-n) |
| `SAMPLE k OFFSET m`  | اینجا `k` و `m` اعداد از 0 به 1.</br>پرس و جو بر روی یک نمونه از اعدام `k` کسری از داده ها. داده های مورد استفاده برای نمونه توسط جبران `m` کسر کردن. [ادامه مطلب](#select-sample-offset)                                                                     |

#### SAMPLE K {#select-sample-k}

اینجا `k` است تعداد از 0 به 1 (هر دو نمادهای کسری و اعشاری پشتیبانی می شوند). به عنوان مثال, `SAMPLE 1/2` یا `SAMPLE 0.5`.

در یک `SAMPLE k` بند, نمونه از گرفته `k` کسری از داده ها. مثال زیر نشان داده شده است:

``` sql
SELECT
    Title,
    count() * 10 AS PageViews
FROM hits_distributed
SAMPLE 0.1
WHERE
    CounterID = 34
GROUP BY Title
ORDER BY PageViews DESC LIMIT 1000
```

در این مثال پرس و جو اجرا شده است در یک نمونه از 0.1 (10%) از داده ها. ارزش توابع دانه ها به طور خودکار اصلاح نمی, بنابراین برای دریافت یک نتیجه تقریبی, ارزش `count()` به صورت دستی توسط ضرب 10.

#### SAMPLE N {#select-sample-n}

اینجا `n` عدد صحیح به اندازه کافی بزرگ است. به عنوان مثال, `SAMPLE 10000000`.

در این مورد, پرس و جو بر روی یک نمونه از حداقل اعدام `n` ردیف (اما نه به طور قابل توجهی بیشتر از این). به عنوان مثال, `SAMPLE 10000000` پرس و جو را در حداقل ردیف های 10000000 اجرا می کند.

از حداقل واحد برای خواندن داده ها یک گرانول است (اندازه خود را توسط مجموعه `index_granularity` تنظیم), این را حس می کند به مجموعه ای از یک نمونه است که بسیار بزرگتر از اندازه گرانول.

هنگام استفاده از `SAMPLE n` بند, شما نمی دانید که درصد نسبی داده پردازش شد. بنابراین شما نمی دانید ضریب توابع کل باید توسط ضرب. استفاده از `_sample_factor` ستون مجازی برای دریافت نتیجه تقریبی.

این `_sample_factor` ستون شامل ضرایب نسبی است که به صورت پویا محاسبه می شود. این ستون به طور خودکار ایجاد زمانی که شما [ایجاد](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table) یک جدول با کلید نمونه گیری مشخص. نمونه های استفاده از `_sample_factor` ستون در زیر نشان داده شده.

بیایید جدول را در نظر بگیریم `visits`, که شامل ارقام در مورد بازدیدکننده داشته است سایت. مثال اول نشان می دهد که چگونه برای محاسبه تعداد بازدید از صفحه:

``` sql
SELECT sum(PageViews * _sample_factor)
FROM visits
SAMPLE 10000000
```

مثال بعدی نشان می دهد که چگونه برای محاسبه تعداد کل بازدیدکننده داشته است:

``` sql
SELECT sum(_sample_factor)
FROM visits
SAMPLE 10000000
```

مثال زیر نشان می دهد که چگونه برای محاسبه مدت زمان جلسه به طور متوسط. توجه داشته باشید که شما لازم نیست به استفاده از ضریب نسبی برای محاسبه مقادیر متوسط.

``` sql
SELECT avg(Duration)
FROM visits
SAMPLE 10000000
```

#### SAMPLE K OFFSET M {#select-sample-offset}

اینجا `k` و `m` اعداد از 0 به 1. نمونه های زیر نشان داده شده.

**مثال 1**

``` sql
SAMPLE 1/10
```

در این مثال نمونه 1 / 10 از تمام داده ها است:

`[++------------]`

**مثال 2**

``` sql
SAMPLE 1/10 OFFSET 1/2
```

در اینجا یک نمونه از 10 درصد گرفته شده از نیمه دوم از داده ها.

`[------++------]`

### مجموعه پیوستن بند {#select-array-join-clause}

اجازه می دهد تا اجرای `JOIN` با یک مجموعه یا ساختار داده های تو در تو. قصد شبیه به است [ارریجین](../functions/array-join.md#functions_arrayjoin) تابع, اما قابلیت های خود را گسترده تر است.

``` sql
SELECT <expr_list>
FROM <left_subquery>
[LEFT] ARRAY JOIN <array>
[WHERE|PREWHERE <expr>]
...
```

شما می توانید تنها یک مشخص `ARRAY JOIN` بند در یک پرس و جو.

سفارش اجرای پرس و جو در هنگام اجرا بهینه شده است `ARRAY JOIN`. اگرچه `ARRAY JOIN` همیشه باید قبل از مشخص شود `WHERE/PREWHERE` بند, این می تواند انجام شود یا قبل از `WHERE/PREWHERE` (اگر نتیجه در این بند مورد نیاز است), و یا پس از اتمام (برای کاهش حجم محاسبات). سفارش پردازش توسط بهینه ساز پرس و جو کنترل می شود.

انواع پشتیبانی شده از `ARRAY JOIN` به شرح زیر است:

-   `ARRAY JOIN` - در این مورد, بند خالی در نتیجه شامل نمی شود `JOIN`.
-   `LEFT ARRAY JOIN` - نتیجه `JOIN` شامل ردیف با ارریس خالی است. مقدار برای یک مجموعه خالی است به مقدار پیش فرض برای نوع عنصر مجموعه (معمولا 0, رشته خالی و یا تهی).

نمونه های زیر نشان می دهد استفاده از `ARRAY JOIN` و `LEFT ARRAY JOIN` بند. بیایید یک جدول با یک [& حذف](../../sql-reference/data-types/array.md) ستون را تایپ کنید و مقادیر را وارد کنید:

``` sql
CREATE TABLE arrays_test
(
    s String,
    arr Array(UInt8)
) ENGINE = Memory;

INSERT INTO arrays_test
VALUES ('Hello', [1,2]), ('World', [3,4,5]), ('Goodbye', []);
```

``` text
┌─s───────────┬─arr─────┐
│ Hello       │ [1,2]   │
│ World       │ [3,4,5] │
│ Goodbye     │ []      │
└─────────────┴─────────┘
```

مثال زیر از `ARRAY JOIN` بند:

``` sql
SELECT s, arr
FROM arrays_test
ARRAY JOIN arr;
```

``` text
┌─s─────┬─arr─┐
│ Hello │   1 │
│ Hello │   2 │
│ World │   3 │
│ World │   4 │
│ World │   5 │
└───────┴─────┘
```

مثال بعدی با استفاده از `LEFT ARRAY JOIN` بند:

``` sql
SELECT s, arr
FROM arrays_test
LEFT ARRAY JOIN arr;
```

``` text
┌─s───────────┬─arr─┐
│ Hello       │   1 │
│ Hello       │   2 │
│ World       │   3 │
│ World       │   4 │
│ World       │   5 │
│ Goodbye     │   0 │
└─────────────┴─────┘
```

#### استفاده از نام مستعار {#using-aliases}

یک نام مستعار می تواند برای مجموعه ای در `ARRAY JOIN` بند بند. در این مورد, یک مورد مجموعه ای را می توان با این نام مستعار دیده, اما مجموعه خود را با نام اصلی قابل دسترسی. مثال:

``` sql
SELECT s, arr, a
FROM arrays_test
ARRAY JOIN arr AS a;
```

``` text
┌─s─────┬─arr─────┬─a─┐
│ Hello │ [1,2]   │ 1 │
│ Hello │ [1,2]   │ 2 │
│ World │ [3,4,5] │ 3 │
│ World │ [3,4,5] │ 4 │
│ World │ [3,4,5] │ 5 │
└───────┴─────────┴───┘
```

با استفاده از نام مستعار, شما می توانید انجام `ARRAY JOIN` با یک مجموعه خارجی. به عنوان مثال:

``` sql
SELECT s, arr_external
FROM arrays_test
ARRAY JOIN [1, 2, 3] AS arr_external;
```

``` text
┌─s───────────┬─arr_external─┐
│ Hello       │            1 │
│ Hello       │            2 │
│ Hello       │            3 │
│ World       │            1 │
│ World       │            2 │
│ World       │            3 │
│ Goodbye     │            1 │
│ Goodbye     │            2 │
│ Goodbye     │            3 │
└─────────────┴──────────────┘
```

ارریس های متعدد را می توان با کاما از هم جدا در `ARRAY JOIN` بند بند. در این مورد, `JOIN` به طور همزمان (مجموع مستقیم و نه محصول دکارتی) انجام می شود. توجه داشته باشید که تمام ارریس باید به همان اندازه. مثال:

``` sql
SELECT s, arr, a, num, mapped
FROM arrays_test
ARRAY JOIN arr AS a, arrayEnumerate(arr) AS num, arrayMap(x -> x + 1, arr) AS mapped;
```

``` text
┌─s─────┬─arr─────┬─a─┬─num─┬─mapped─┐
│ Hello │ [1,2]   │ 1 │   1 │      2 │
│ Hello │ [1,2]   │ 2 │   2 │      3 │
│ World │ [3,4,5] │ 3 │   1 │      4 │
│ World │ [3,4,5] │ 4 │   2 │      5 │
│ World │ [3,4,5] │ 5 │   3 │      6 │
└───────┴─────────┴───┴─────┴────────┘
```

مثال زیر از [شناسه بسته:](../../sql-reference/functions/array-functions.md#array_functions-arrayenumerate) تابع:

``` sql
SELECT s, arr, a, num, arrayEnumerate(arr)
FROM arrays_test
ARRAY JOIN arr AS a, arrayEnumerate(arr) AS num;
```

``` text
┌─s─────┬─arr─────┬─a─┬─num─┬─arrayEnumerate(arr)─┐
│ Hello │ [1,2]   │ 1 │   1 │ [1,2]               │
│ Hello │ [1,2]   │ 2 │   2 │ [1,2]               │
│ World │ [3,4,5] │ 3 │   1 │ [1,2,3]             │
│ World │ [3,4,5] │ 4 │   2 │ [1,2,3]             │
│ World │ [3,4,5] │ 5 │   3 │ [1,2,3]             │
└───────┴─────────┴───┴─────┴─────────────────────┘
```

#### مجموعه پیوستن با ساختار داده های تو در تو {#array-join-with-nested-data-structure}

`ARRAY`پیوستن " همچنین با این نسخهها کار [ساختارهای داده تو در تو](../../sql-reference/data-types/nested-data-structures/nested.md). مثال:

``` sql
CREATE TABLE nested_test
(
    s String,
    nest Nested(
    x UInt8,
    y UInt32)
) ENGINE = Memory;

INSERT INTO nested_test
VALUES ('Hello', [1,2], [10,20]), ('World', [3,4,5], [30,40,50]), ('Goodbye', [], []);
```

``` text
┌─s───────┬─nest.x──┬─nest.y─────┐
│ Hello   │ [1,2]   │ [10,20]    │
│ World   │ [3,4,5] │ [30,40,50] │
│ Goodbye │ []      │ []         │
└─────────┴─────────┴────────────┘
```

``` sql
SELECT s, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN nest;
```

``` text
┌─s─────┬─nest.x─┬─nest.y─┐
│ Hello │      1 │     10 │
│ Hello │      2 │     20 │
│ World │      3 │     30 │
│ World │      4 │     40 │
│ World │      5 │     50 │
└───────┴────────┴────────┘
```

هنگام مشخص کردن نام ساختارهای داده های تو در تو در `ARRAY JOIN`, معنای همان است که `ARRAY JOIN` با تمام عناصر مجموعه ای که شامل. نمونه به شرح زیر است:

``` sql
SELECT s, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN `nest.x`, `nest.y`;
```

``` text
┌─s─────┬─nest.x─┬─nest.y─┐
│ Hello │      1 │     10 │
│ Hello │      2 │     20 │
│ World │      3 │     30 │
│ World │      4 │     40 │
│ World │      5 │     50 │
└───────┴────────┴────────┘
```

این تنوع نیز حس می کند:

``` sql
SELECT s, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN `nest.x`;
```

``` text
┌─s─────┬─nest.x─┬─nest.y─────┐
│ Hello │      1 │ [10,20]    │
│ Hello │      2 │ [10,20]    │
│ World │      3 │ [30,40,50] │
│ World │      4 │ [30,40,50] │
│ World │      5 │ [30,40,50] │
└───────┴────────┴────────────┘
```

نام مستعار ممکن است برای یک ساختار داده های تو در تو استفاده می شود, به منظور انتخاب هر دو `JOIN` نتیجه یا مجموعه منبع. مثال:

``` sql
SELECT s, `n.x`, `n.y`, `nest.x`, `nest.y`
FROM nested_test
ARRAY JOIN nest AS n;
```

``` text
┌─s─────┬─n.x─┬─n.y─┬─nest.x──┬─nest.y─────┐
│ Hello │   1 │  10 │ [1,2]   │ [10,20]    │
│ Hello │   2 │  20 │ [1,2]   │ [10,20]    │
│ World │   3 │  30 │ [3,4,5] │ [30,40,50] │
│ World │   4 │  40 │ [3,4,5] │ [30,40,50] │
│ World │   5 │  50 │ [3,4,5] │ [30,40,50] │
└───────┴─────┴─────┴─────────┴────────────┘
```

نمونه ای از استفاده از [شناسه بسته:](../../sql-reference/functions/array-functions.md#array_functions-arrayenumerate) تابع:

``` sql
SELECT s, `n.x`, `n.y`, `nest.x`, `nest.y`, num
FROM nested_test
ARRAY JOIN nest AS n, arrayEnumerate(`nest.x`) AS num;
```

``` text
┌─s─────┬─n.x─┬─n.y─┬─nest.x──┬─nest.y─────┬─num─┐
│ Hello │   1 │  10 │ [1,2]   │ [10,20]    │   1 │
│ Hello │   2 │  20 │ [1,2]   │ [10,20]    │   2 │
│ World │   3 │  30 │ [3,4,5] │ [30,40,50] │   1 │
│ World │   4 │  40 │ [3,4,5] │ [30,40,50] │   2 │
│ World │   5 │  50 │ [3,4,5] │ [30,40,50] │   3 │
└───────┴─────┴─────┴─────────┴────────────┴─────┘
```

### پیوستن بند {#select-join}

می پیوندد داده ها در عادی [SQL JOIN](https://en.wikipedia.org/wiki/Join_(SQL)) با عقل.

!!! info "یادداشت"
    نه مربوط به [ARRAY JOIN](#select-array-join-clause).

``` sql
SELECT <expr_list>
FROM <left_subquery>
[GLOBAL] [ANY|ALL] [INNER|LEFT|RIGHT|FULL|CROSS] [OUTER] JOIN <right_subquery>
(ON <expr_list>)|(USING <column_list>) ...
```

نام جدول را می توان به جای مشخص `<left_subquery>` و `<right_subquery>`. این معادل است `SELECT * FROM table` خرده فروشی, به جز در یک مورد خاص زمانی که جدول است [پیوستن](../../engines/table-engines/special/join.md) engine – an array prepared for joining.

#### انواع پشتیبانی شده از `JOIN` {#select-join-types}

-   `INNER JOIN` (یا `JOIN`)
-   `LEFT JOIN` (یا `LEFT OUTER JOIN`)
-   `RIGHT JOIN` (یا `RIGHT OUTER JOIN`)
-   `FULL JOIN` (یا `FULL OUTER JOIN`)
-   `CROSS JOIN` (یا `,` )

مشاهده استاندارد [SQL JOIN](https://en.wikipedia.org/wiki/Join_(SQL)) توصیف.

#### چند پیوستن {#multiple-join}

انجام نمایش داده شد, بازنویسی کلیک خانه چند جدول می پیوندد به دنباله ای از دو جدول می پیوندد. مثلا, اگر چهار جدول برای عضویت کلیک خانه می پیوندد اول و دوم وجود دارد, سپس نتیجه می پیوندد با جدول سوم, و در مرحله گذشته, می پیوندد یک چهارم.

اگر پرس و جو شامل `WHERE` بند ClickHouse تلاش می کند به pushdown فیلتر از این بند از طریق متوسط پیوستن به. اگر می تواند فیلتر به هر ملحق متوسط اعمال می شود, تاتر اعمال فیلتر بعد از همه می پیوندد به پایان رسید.

ما توصیه می کنیم `JOIN ON` یا `JOIN USING` نحو برای ایجاد نمایش داده شد. به عنوان مثال:

``` sql
SELECT * FROM t1 JOIN t2 ON t1.a = t2.a JOIN t3 ON t1.a = t3.a
```

شما می توانید لیست کاما از هم جدا از جداول در استفاده از `FROM` بند بند. به عنوان مثال:

``` sql
SELECT * FROM t1, t2, t3 WHERE t1.a = t2.a AND t1.a = t3.a
```

این سینتکس را مخلوط نکنید.

کلیکهاوس مستقیما از دستورات ارتباطی با کاما پشتیبانی نمی کند بنابراین توصیه نمی کنیم از انها استفاده کنید. الگوریتم تلاش می کند به بازنویسی پرس و جو از نظر `CROSS JOIN` و `INNER JOIN` بند و سپس به پرس و جو پردازش. هنگامی که بازنویسی پرس و جو, تاتر تلاش می کند برای بهینه سازی عملکرد و مصرف حافظه. به طور پیش فرض, تاتر رفتار کاما به عنوان یک `INNER JOIN` بند و تبدیل `INNER JOIN` به `CROSS JOIN` هنگامی که الگوریتم نمی تواند تضمین کند که `INNER JOIN` بازگرداندن اطلاعات مورد نیاز.

#### سختی {#select-join-strictness}

-   `ALL` — If the right table has several matching rows, ClickHouse creates a [محصول دکارتی](https://en.wikipedia.org/wiki/Cartesian_product) از تطبیق ردیف. این استاندارد است `JOIN` رفتار در گذاشتن.
-   `ANY` — If the right table has several matching rows, only the first one found is joined. If the right table has only one matching row, the results of queries with `ANY` و `ALL` کلمات کلیدی یکسان هستند.
-   `ASOF` — For joining sequences with a non-exact match. `ASOF JOIN` استفاده در زیر توضیح داده شده است.

**از این رو پیوستن به استفاده**

`ASOF JOIN` مفید است زمانی که شما نیاز به پیوستن به سوابق که هیچ بازی دقیق.

جداول برای `ASOF JOIN` باید یک ستون توالی دستور داده اند. این ستون نمی تواند به تنهایی در یک جدول باشد و باید یکی از انواع داده ها باشد: `UInt32`, `UInt64`, `Float32`, `Float64`, `Date` و `DateTime`.

نحو `ASOF JOIN ... ON`:

``` sql
SELECT expressions_list
FROM table_1
ASOF LEFT JOIN table_2
ON equi_cond AND closest_match_cond
```

شما می توانید هر تعداد از شرایط برابری و دقیقا یکی از نزدیک ترین شرایط بازی استفاده کنید. به عنوان مثال, `SELECT count() FROM table_1 ASOF LEFT JOIN table_2 ON table_1.a == table_2.b AND table_2.t <= table_1.t`.

شرایط پشتیبانی شده برای نزدیک ترین بازی: `>`, `>=`, `<`, `<=`.

نحو `ASOF JOIN ... USING`:

``` sql
SELECT expressions_list
FROM table_1
ASOF JOIN table_2
USING (equi_column1, ... equi_columnN, asof_column)
```

`ASOF JOIN` استفاده `equi_columnX` برای پیوستن به برابری و `asof_column` برای پیوستن به نزدیک ترین مسابقه با `table_1.asof_column >= table_2.asof_column` شرط. این `asof_column` ستون همیشه یکی از گذشته در `USING` بند بند.

مثلا, جداول زیر را در نظر بگیرید:

         table_1                           table_2
      event   | ev_time | user_id       event   | ev_time | user_id
    ----------|---------|----------   ----------|---------|----------
                  ...                               ...
    event_1_1 |  12:00  |  42         event_2_1 |  11:59  |   42
                  ...                 event_2_2 |  12:30  |   42
    event_1_2 |  13:00  |  42         event_2_3 |  13:00  |   42
                  ...                               ...

`ASOF JOIN` می توانید برچسب زمان از یک رویداد کاربر از را `table_1` و پیدا کردن یک رویداد در `table_2` جایی که برچسب زمان نزدیک به زمان این رویداد از `table_1` مربوط به نزدیک ترین شرایط بازی. مقادیر برچسب زمان برابر نزدیک ترین در صورت موجود بودن. اینجا `user_id` ستون را می توان برای پیوستن به برابری و `ev_time` ستون را می توان برای پیوستن به در نزدیک ترین بازی استفاده می شود. در مثال ما, `event_1_1` می توان با پیوست `event_2_1` و `event_1_2` می توان با پیوست `event_2_3` اما `event_2_2` نمیشه عضو شد

!!! note "یادداشت"
    `ASOF` پیوستن است **نه** پردازشگر پشتیبانی شده: [پیوستن](../../engines/table-engines/special/join.md) موتور جدول.

برای تنظیم مقدار سختگیرانه پیش فرض, استفاده از پارامتر پیکربندی جلسه [بررسی اجمالی](../../operations/settings/settings.md#settings-join_default_strictness).

#### GLOBAL JOIN {#global-join}

هنگام استفاده از نرمال `JOIN` پرس و جو به سرورهای راه دور ارسال می شود. زیرکریزها روی هر کدام اجرا می شوند تا میز مناسب را بسازند و پیوستن با این جدول انجام می شود. به عبارت دیگر, جدول سمت راست بر روی هر سرور تشکیل به طور جداگانه.

هنگام استفاده از `GLOBAL ... JOIN`, اول سرور درخواست کننده اجرا می شود یک خرده فروشی برای محاسبه جدول سمت راست. این جدول موقت به هر سرور از راه دور منتقل می شود و نمایش داده می شود با استفاده از داده های موقت منتقل می شود.

مراقب باشید در هنگام استفاده از `GLOBAL`. برای کسب اطلاعات بیشتر به بخش مراجعه کنید [توزیع subqueries](#select-distributed-subqueries).

#### توصیه های استفاده {#usage-recommendations}

هنگامی که در حال اجرا `JOIN` بهینه سازی سفارش اعدام در رابطه با سایر مراحل پرس و جو وجود ندارد. پیوستن (جستجو در جدول سمت راست) قبل از فیلتر کردن در اجرا می شود `WHERE` و قبل از تجمع. به منظور صراحت تنظیم سفارش پردازش, توصیه می کنیم در حال اجرا یک `JOIN` خرده فروشی با یک خرده فروشی.

مثال:

``` sql
SELECT
    CounterID,
    hits,
    visits
FROM
(
    SELECT
        CounterID,
        count() AS hits
    FROM test.hits
    GROUP BY CounterID
) ANY LEFT JOIN
(
    SELECT
        CounterID,
        sum(Sign) AS visits
    FROM test.visits
    GROUP BY CounterID
) USING CounterID
ORDER BY hits DESC
LIMIT 10
```

``` text
┌─CounterID─┬───hits─┬─visits─┐
│   1143050 │ 523264 │  13665 │
│    731962 │ 475698 │ 102716 │
│    722545 │ 337212 │ 108187 │
│    722889 │ 252197 │  10547 │
│   2237260 │ 196036 │   9522 │
│  23057320 │ 147211 │   7689 │
│    722818 │  90109 │  17847 │
│     48221 │  85379 │   4652 │
│  19762435 │  77807 │   7026 │
│    722884 │  77492 │  11056 │
└───────────┴────────┴────────┘
```

زیرمجموعه ها به شما اجازه نمی دهند نام ها را تنظیم کنید یا از یک ستون از یک زیر اندازه خاص استفاده کنید.
ستون های مشخص شده در `USING` باید نام های مشابه در هر دو کارخانه های فرعی دارند, و ستون های دیگر باید متفاوت به نام. شما می توانید نام مستعار برای تغییر نام ستون در زیرکریز استفاده (به عنوان مثال با استفاده از نام مستعار `hits` و `visits`).

این `USING` بند یک یا چند ستون برای پیوستن به مشخص, که ایجاد برابری این ستون. لیست ستون ها بدون براکت تنظیم شده است. شرایط پیوستن پیچیده تر پشتیبانی نمی شوند.

جدول سمت راست (نتیجه زیرخاکی) ساکن در رم. اگر حافظه کافی وجود ندارد, شما می توانید یک اجرا کنید `JOIN`.

هر بار که پرس و جو با همان اجرا شود `JOIN`, خرده فروشی است دوباره اجرا به دلیل نتیجه ذخیره سازی نیست. برای جلوگیری از این, استفاده از ویژه [پیوستن](../../engines/table-engines/special/join.md) موتور جدول, که مجموعه ای تهیه شده برای پیوستن است که همیشه در رم.

در بعضی موارد کارایی بیشتری برای استفاده دارد `IN` به جای `JOIN`.
در میان انواع مختلف `JOIN`, موثر ترین است `ANY LEFT JOIN` پس `ANY INNER JOIN`. کمترین کارایی عبارتند از `ALL LEFT JOIN` و `ALL INNER JOIN`.

اگر شما نیاز به یک `JOIN` برای پیوستن به جداول بعد (این جداول نسبتا کوچک است که شامل خواص ابعاد هستند, مانند نام برای کمپین های تبلیغاتی), یک `JOIN` ممکن است بسیار مناسب با توجه به این واقعیت است که جدول سمت راست برای هر پرس و جو دوباره قابل دسترسی است. برای چنین مواردی وجود دارد “external dictionaries” ویژگی است که شما باید به جای استفاده از `JOIN`. برای کسب اطلاعات بیشتر به بخش مراجعه کنید [واژهنامهها خارجی](../dictionaries/external-dictionaries/external-dicts.md).

**محدودیت حافظه**

تاتر با استفاده از [هش پیوستن](https://en.wikipedia.org/wiki/Hash_join) الگوریتم. تاتر طول می کشد `<right_subquery>` و یک جدول هش را در رم ایجاد می کند. اگر شما نیاز به محدود کردن پیوستن به مصرف حافظه عملیات استفاده از تنظیمات زیر:

-   [\_پاک کردن \_روشن گرافیک](../../operations/settings/query-complexity.md#settings-max_rows_in_join) — Limits number of rows in the hash table.
-   [\_پویش همیشگی](../../operations/settings/query-complexity.md#settings-max_bytes_in_join) — Limits size of the hash table.

هنگامی که هر یک از این محدودیت رسیده است, کلیک به عنوان عمل می کند [\_شروع مجدد](../../operations/settings/query-complexity.md#settings-join_overflow_mode) تنظیم دستور.

#### پردازش سلولهای خالی یا خالی {#processing-of-empty-or-null-cells}

در حالی که پیوستن به جداول سلول های خالی ظاهر می شود. تنظیمات [ارزشهای خبری عبارتند از:](../../operations/settings/settings.md#join_use_nulls) تعریف چگونه خانه را پر می کند این سلول ها.

اگر `JOIN` کلید ها [Nullable](../data-types/nullable.md) زمینه, ردیف که حداقل یکی از کلید های دارای ارزش [NULL](../syntax.md#null-literal) عضو نشده.

#### محدودیت نحو {#syntax-limitations}

برای چند `JOIN` بند در یک `SELECT` پرسوجو:

-   گرفتن تمام ستون ها از طریق `*` در دسترس است تنها اگر جداول پیوست, نمی فرعی.
-   این `PREWHERE` بند در دسترس نیست.

برای `ON`, `WHERE` و `GROUP BY` بند:

-   عبارات دلخواه را نمی توان در `ON`, `WHERE` و `GROUP BY` بند, اما شما می توانید یک عبارت در یک تعریف `SELECT` بند و سپس در این بند از طریق یک نام مستعار استفاده کنید.

### بند کجا {#select-where}

در صورتی که بند جایی وجود دارد, باید بیان با نوع زیرپوش شامل8. این است که معمولا بیان با مقایسه و اپراتورهای منطقی.
این عبارت خواهد شد برای فیلتر کردن داده ها قبل از همه تحولات دیگر استفاده می شود.

اگر شاخص توسط موتور جدول پایگاه داده پشتیبانی, بیان بر توانایی استفاده از شاخص ارزیابی.

### در بند {#prewhere-clause}

این بند همان معنی که بند است. تفاوت در این است که داده ها از جدول خوانده می شوند.
هنگامی که با استفاده از PREWHERE اول تنها ستون لازم برای اجرای PREWHERE در حال خواندن. سپس ستون های دیگر خوانده می شوند که برای اجرای پرس و جو مورد نیاز است اما تنها بلوک هایی که بیان پیشین درست است.

این را حس می کند به استفاده از همه جا اگر شرایط فیلتراسیون که توسط یک اقلیت از ستون ها در پرس و جو استفاده می شود وجود دارد, اما که فیلتراسیون داده های قوی. این باعث کاهش حجم داده ها به خواندن.

مثلا, مفید است برای نوشتن از کجا برای نمایش داده شد که استخراج تعداد زیادی از ستون, اما این تنها فیلتراسیون برای چند ستون دارند.

همه جا تنها با جداول از پشتیبانی `*MergeTree` خانواده

یک پرس و جو ممکن است به طور همزمان مشخص PREWHERE و در آن. در این مورد PREWHERE پیش می آید که در آن.

اگر ‘optimize\_move\_to\_prewhere’ تنظیم به 1 و PREWHERE حذف شده از سیستم با استفاده از ابتکارات به طور خودکار حرکت قطعات از عبارات از کجا PREWHERE.

### گروه بر اساس بند {#select-group-by-clause}

این یکی از مهم ترین بخش های سندرم تونل کارپ ستون گرا است.

در صورتی که یک گروه بند وجود دارد, باید یک لیست از عبارات حاوی. هر عبارت خواهد شد به اینجا به عنوان یک اشاره “key”.
همه عبارات در انتخاب, داشتن, و سفارش توسط بند باید از کلید و یا از توابع کل محاسبه می شود. به عبارت دیگر, هر ستون انتخاب شده از جدول باید یا در کلید و یا در داخل توابع دانه استفاده می شود.

اگر یک پرس و جو شامل تنها ستون جدول در داخل توابع کل, گروه بند را می توان حذف, و تجمع توسط یک مجموعه خالی از کلید فرض بر این است.

مثال:

``` sql
SELECT
    count(),
    median(FetchTiming > 60 ? 60 : FetchTiming),
    count() - sum(Refresh)
FROM hits
```

با این حال, در مقابل به استاندارد گذاشتن, اگر جدول هیچ ردیف ندارد (یا هیچ در همه وجود ندارد, و یا هر پس از استفاده از جایی که برای فیلتر کردن وجود ندارد), یک نتیجه خالی بازگشته است, و نه در نتیجه از یکی از ردیف های حاوی مقادیر اولیه از توابع کل.

همانطور که به خروجی زیر مخالف (و منطبق با استاندارد گذاشتن), شما می توانید برخی از ارزش برخی از ستون است که در یک کلید و یا کل تابع نیست (به جز عبارات ثابت). برای کار در اطراف این, شما می توانید با استفاده از ‘any’ تابع جمع (اولین مقدار مواجه می شوند) یا ‘min/max’.

مثال:

``` sql
SELECT
    domainWithoutWWW(URL) AS domain,
    count(),
    any(Title) AS title -- getting the first occurred page header for each domain.
FROM hits
GROUP BY domain
```

برای هر مقدار کلیدی مختلف مواجه می شوند, گروه با محاسبه مجموعه ای از مقادیر تابع کل.

گروه توسط ستون های مجموعه پشتیبانی نمی شود.

ثابت را نمی توان به عنوان استدلال برای توابع کل مشخص شده است. مثال: مجموع(1). به جای این, شما می توانید از ثابت خلاص. مثال: `count()`.

#### پردازش پوچ {#null-processing}

برای گروه بندی, تفسیر کلیک [NULL](../syntax.md#null-literal) به عنوان یک ارزش, و `NULL=NULL`.

در اینجا یک مثال برای نشان دادن این بدان معنی است.

فرض کنید شما باید این جدول:

``` text
┌─x─┬────y─┐
│ 1 │    2 │
│ 2 │ ᴺᵁᴸᴸ │
│ 3 │    2 │
│ 3 │    3 │
│ 3 │ ᴺᵁᴸᴸ │
└───┴──────┘
```

پرسوجو `SELECT sum(x), y FROM t_null_big GROUP BY y` نتایج در:

``` text
┌─sum(x)─┬────y─┐
│      4 │    2 │
│      3 │    3 │
│      5 │ ᴺᵁᴸᴸ │
└────────┴──────┘
```

شما می توانید ببینید که `GROUP BY` برای `y = NULL` خلاصه تا `x`, به عنوان اگر `NULL` این مقدار است.

اگر شما تصویب چند کلید به `GROUP BY`, نتیجه به شما تمام ترکیبی از انتخاب را, اگر `NULL` یک مقدار خاص بودند.

#### با اصلاح کننده بالغ {#with-totals-modifier}

اگر با اصلاح بالغ مشخص شده است, ردیف دیگر محاسبه خواهد شد. این ردیف ستون های کلیدی حاوی مقادیر پیش فرض (صفر یا خطوط خالی) و ستون های توابع جمع شده با مقادیر محاسبه شده در تمام ردیف ها ( “total” ارزش ها).

این ردیف اضافی خروجی در جانسون است\*, تابسپار\*, و زیبا \* فرمت, به طور جداگانه از ردیف های دیگر. در فرمت های دیگر, این ردیف خروجی نیست.

در جانسون \* فرمت های, این ردیف خروجی به عنوان یک جداگانه است ‘totals’ رشته. در جدولپار\* فرمت های, ردیف پس از نتیجه اصلی, قبل از یک ردیف خالی (پس از داده های دیگر). در زیبا \* فرمت های ردیف خروجی به عنوان یک جدول جداگانه پس از نتیجه اصلی است.

`WITH TOTALS` می توان در راه های مختلف اجرا زمانی که داشتن حاضر است. رفتار بستگی به ‘totals\_mode’ تنظیمات.
به طور پیش فرض, `totals_mode = 'before_having'`. در این مورد, ‘totals’ محاسبه شده است در تمام ردیف از جمله کسانی که عبور نمی کند از طریق داشتن و ‘max\_rows\_to\_group\_by’.

جایگزین های دیگر شامل تنها ردیف است که از طریق داشتن در عبور ‘totals’ و رفتار متفاوت با تنظیم `max_rows_to_group_by` و `group_by_overflow_mode = 'any'`.

`after_having_exclusive` – Don't include rows that didn't pass through `max_rows_to_group_by`. به عبارت دیگر, ‘totals’ کمتر از و یا به همان تعداد از ردیف به عنوان اگر داشته باشد `max_rows_to_group_by` حذف شد.

`after_having_inclusive` – Include all the rows that didn't pass through ‘max\_rows\_to\_group\_by’ داخل ‘totals’. به عبارت دیگر, ‘totals’ بیش از و یا به همان تعداد از ردیف به عنوان اگر داشته باشد `max_rows_to_group_by` حذف شد.

`after_having_auto` – Count the number of rows that passed through HAVING. If it is more than a certain amount (by default, 50%), include all the rows that didn't pass through ‘max\_rows\_to\_group\_by’ داخل ‘totals’. در غیر این صورت, را شامل نمی شود.

`totals_auto_threshold` – By default, 0.5. The coefficient for `after_having_auto`.

اگر `max_rows_to_group_by` و `group_by_overflow_mode = 'any'` استفاده نمی شود, تمام تغییرات از `after_having` یکسان هستند و شما می توانید از هر یک از این موارد استفاده کنید, `after_having_auto`).

شما می توانید با استفاده از مجموع در subqueries از جمله subqueries در پیوستن به بند (در این مورد مربوطه مجموع ارزش ترکیب می شوند).

#### گروه در حافظه خارجی {#select-group-by-in-external-memory}

شما می توانید اطلاعات موقت تخلیه به دیسک را قادر به محدود کردن استفاده از حافظه در طول `GROUP BY`.
این [ا\_فزون\_بر\_گونهی\_گونهی زیر\_گروهها](../../operations/settings/settings.md#settings-max_bytes_before_external_group_by) تنظیم تعیین کننده مصرف رم را برای تخلیه می کند `GROUP BY` اطلاعات موقت به سیستم فایل. اگر به 0 (به طور پیش فرض), غیر فعال است.

هنگام استفاده از `max_bytes_before_external_group_by`, توصیه می کنیم که به شما در تنظیم `max_memory_usage` در مورد دو برابر بالا. این لازم است زیرا دو مرحله برای تجمع وجود دارد: خواندن تاریخ و تشکیل داده های متوسط (1) و ادغام داده های متوسط (2). واژگون اطلاعات به سیستم فایل تنها می تواند در طول مرحله رخ می دهد 1. اگر داده های موقت ریخته نمی شد, سپس مرحله 2 ممکن است نیاز به همان مقدار از حافظه در مرحله 1.

برای مثال اگر [\_کاساژ بیشینه](../../operations/settings/settings.md#settings_max_memory_usage) به 1000000000 تنظیم شد و شما می خواهید به استفاده از تجمع خارجی, این را حس می کند به مجموعه `max_bytes_before_external_group_by` به 10000000000 و حداکثر\_موری\_اساژ به 20000000000. هنگامی که تجمع خارجی باعث شده است (اگر حداقل یک روگرفت از داده های موقت وجود دارد), حداکثر مصرف رم تنها کمی بیشتر از `max_bytes_before_external_group_by`.

با پردازش پرس و جو توزیع, تجمع خارجی بر روی سرور از راه دور انجام. به منظور سرور درخواست به استفاده از تنها مقدار کمی از رم, تنظیم `distributed_aggregation_memory_efficient` به 1.

هنگامی که ادغام داده ها به دیسک سرخ, و همچنین زمانی که ادغام نتایج حاصل از سرور از راه دور زمانی که `distributed_aggregation_memory_efficient` تنظیم فعال است, مصرف تا `1/256 * the_number_of_threads` از مقدار کل رم.

هنگامی که تجمع خارجی فعال است, اگر کمتر از وجود دارد `max_bytes_before_external_group_by` of data (i.e. data was not flushed), the query runs just as fast as without external aggregation. If any temporary data was flushed, the run time will be several times longer (approximately three times).

اگر شما یک `ORDER BY` با یک `LIMIT` پس از `GROUP BY`, سپس مقدار رم استفاده می شود بستگی به مقدار داده ها در `LIMIT` نه تو کل میز اما اگر `ORDER BY` ندارد `LIMIT` فراموش نکنید که مرتب سازی خارجی را فعال کنید (`max_bytes_before_external_sort`).

### محدود کردن بند {#limit-by-clause}

پرس و جو با `LIMIT n BY expressions` بند اول را انتخاب می کند `n` ردیف برای هر مقدار مجزا از `expressions`. کلید برای `LIMIT BY` می تواند شامل هر تعداد از [عبارتها](../syntax.md#syntax-expressions).

تاتر از نحو زیر پشتیبانی می کند:

-   `LIMIT [offset_value, ]n BY expressions`
-   `LIMIT n OFFSET offset_value BY expressions`

در طول پردازش پرس و جو, خانه را انتخاب داده دستور داد با مرتب سازی کلید. کلید مرتب سازی به صراحت با استفاده از یک مجموعه [ORDER BY](#select-order-by) بند یا به طور ضمنی به عنوان یک ویژگی از موتور جدول. سپس کلیک کنیداوس اعمال می شود `LIMIT n BY expressions` و اولین را برمی گرداند `n` ردیف برای هر ترکیب مجزا از `expressions`. اگر `OFFSET` مشخص شده است, سپس برای هر بلوک داده که متعلق به یک ترکیب متمایز از `expressions`. `offset_value` تعداد ردیف از ابتدای بلوک و حداکثر می گرداند `n` ردیف به عنوان یک نتیجه. اگر `offset_value` بزرگتر از تعدادی از ردیف در بلوک داده است, کلیک بازگشت صفر ردیف از بلوک.

`LIMIT BY` مربوط به `LIMIT`. هر دو را می توان در همان پرس و جو استفاده کرد.

**مثالها**

جدول نمونه:

``` sql
CREATE TABLE limit_by(id Int, val Int) ENGINE = Memory;
INSERT INTO limit_by values(1, 10), (1, 11), (1, 12), (2, 20), (2, 21);
```

نمایش داده شد:

``` sql
SELECT * FROM limit_by ORDER BY id, val LIMIT 2 BY id
```

``` text
┌─id─┬─val─┐
│  1 │  10 │
│  1 │  11 │
│  2 │  20 │
│  2 │  21 │
└────┴─────┘
```

``` sql
SELECT * FROM limit_by ORDER BY id, val LIMIT 1, 2 BY id
```

``` text
┌─id─┬─val─┐
│  1 │  11 │
│  1 │  12 │
│  2 │  21 │
└────┴─────┘
```

این `SELECT * FROM limit_by ORDER BY id, val LIMIT 2 OFFSET 1 BY id` پرس و جو همان نتیجه را برمی گرداند.

پرس و جو زیر را برمی گرداند بالا 5 ارجاع برای هر `domain, device_type` جفت با حداکثر 100 ردیف در مجموع (`LIMIT n BY + LIMIT`).

``` sql
SELECT
    domainWithoutWWW(URL) AS domain,
    domainWithoutWWW(REFERRER_URL) AS referrer,
    device_type,
    count() cnt
FROM hits
GROUP BY domain, referrer, device_type
ORDER BY cnt DESC
LIMIT 5 BY domain, device_type
LIMIT 100
```

### داشتن بند {#having-clause}

اجازه می دهد تا فیلتر نتیجه پس از گروه های دریافت, شبیه به بند جایی که.
در کجا و در جایی که قبل از تجمع انجام متفاوت است (گروه های), در حالی که پس از انجام.
اگر تجمع انجام نشده است, داشتن نمی توان استفاده کرد.

### ORDER BY {#select-order-by}

سفارش توسط بند شامل یک لیست از عبارات, که می تواند هر یک اختصاص داده شود مجموعه خارج از محدوده و یا صعودی (جهت مرتب سازی). اگر جهت مشخص نشده است, صعودی فرض بر این است. صعودی و نزولی در نزولی مرتب شده اند. جهت مرتب سازی شامل یک عبارت واحد, نه به کل لیست. مثال: `ORDER BY Visits DESC, SearchPhrase`

برای مرتب سازی بر اساس مقادیر رشته, شما می توانید میترا مشخص (مقایسه). مثال: `ORDER BY SearchPhrase COLLATE 'tr'` - برای مرتب سازی بر اساس کلمه کلیدی به ترتیب صعودی, با استفاده از الفبای ترکی, حساس به حروف, فرض کنید که رشته ها سخن گفتن-8 کد گذاری. تلفیق می تواند مشخص شود یا نه برای هر عبارت به منظور به طور مستقل. اگر مرکز کنترل و یا مرکز کنترل خارج رحمی مشخص شده است, تلفیقی بعد از مشخص. هنگام استفاده از برخورد, مرتب سازی است که همیشه غیر حساس به حروف.

ما فقط توصیه می کنیم با استفاده از تلفیق برای مرتب سازی نهایی تعداد کمی از ردیف, از مرتب سازی با تلفیقی کمتر موثر تر از مرتب سازی طبیعی با بایت است.

ردیف هایی که دارای مقادیر یکسان برای لیست عبارات مرتب سازی هستند خروجی در جهت دلخواه هستند که همچنین می توانند نامعین (هر بار متفاوت) باشند.
اگر ORDER BY حذف شده منظور از ردیف نیز تعریف نشده و ممکن است nondeterministic به عنوان به خوبی.

`NaN` و `NULL` مرتب سازی سفارش:

-   با اصلاح کننده `NULLS FIRST` — First `NULL` پس `NaN`, سپس ارزش های دیگر.
-   با اصلاح کننده `NULLS LAST` — First the values, then `NaN` پس `NULL`.
-   Default — The same as with the `NULLS LAST` تغییردهنده.

مثال:

برای جدول

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │    2 │
│ 1 │  nan │
│ 2 │    2 │
│ 3 │    4 │
│ 5 │    6 │
│ 6 │  nan │
│ 7 │ ᴺᵁᴸᴸ │
│ 6 │    7 │
│ 8 │    9 │
└───┴──────┘
```

اجرای پرس و جو `SELECT * FROM t_null_nan ORDER BY y NULLS FIRST` برای دریافت:

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 7 │ ᴺᵁᴸᴸ │
│ 1 │  nan │
│ 6 │  nan │
│ 2 │    2 │
│ 2 │    2 │
│ 3 │    4 │
│ 5 │    6 │
│ 6 │    7 │
│ 8 │    9 │
└───┴──────┘
```

هنگامی که اعداد ممیز شناور طبقه بندی شده اند, نان جدا از ارزش های دیگر هستند. صرف نظر از نظم مرتب سازی, نان در پایان. به عبارت دیگر برای مرتب سازی صعودی قرار می گیرند همانطور که بزرگتر از همه شماره های دیگر هستند در حالی که برای مرتب سازی کوچکتر از بقیه قرار می گیرند.

رم کمتر استفاده می شود اگر یک محدودیت به اندازه کافی کوچک است علاوه بر سفارش های مشخص. در غیر این صورت, مقدار حافظه صرف متناسب با حجم داده ها برای مرتب سازی است. برای پردازش پرس و جو توزیع, اگر گروه حذف شده است, مرتب سازی تا حدی بر روی سرور از راه دور انجام, و نتایج در سرور درخواست با هم ادغام شدند. این به این معنی است که برای مرتب سازی توزیع, حجم داده ها برای مرتب کردن می تواند بیشتر از مقدار حافظه بر روی یک سرور واحد.

در صورتی که رم به اندازه کافی وجود ندارد, ممکن است به انجام مرتب سازی در حافظه خارجی (ایجاد فایل های موقت بر روی یک دیسک). از تنظیمات استفاده کنید `max_bytes_before_external_sort` برای این منظور. اگر قرار است 0 (به طور پیش فرض), مرتب سازی خارجی غیر فعال است. اگر فعال باشد, زمانی که حجم داده ها برای مرتب کردن بر اساس تعداد مشخصی از بایت می رسد, اطلاعات جمع شده مرتب شده و ریخته را به یک فایل موقت. پس از همه داده ها خوانده شده است, تمام فایل های طبقه بندی شده اند با هم ادغام شدند و نتایج خروجی. فایل ها به نوشته /ور/معاونت/تاتر/تی ام پی/ دایرکتوری در پیکربندی (به طور پیش فرض, اما شما می توانید با استفاده از ‘tmp\_path’ پارامتر برای تغییر این تنظیم).

در حال اجرا یک پرس و جو ممکن است حافظه بیش از استفاده ‘max\_bytes\_before\_external\_sort’. به همین دلیل این تنظیم باید یک مقدار قابل توجهی کوچکتر از ‘max\_memory\_usage’. به عنوان مثال, اگر سرور شما 128 گیگابایت رم و شما نیاز به اجرای یک پرس و جو واحد, تنظیم ‘max\_memory\_usage’ به 100 گیگابایت, و ‘max\_bytes\_before\_external\_sort’ به 80 گیگابایت.

مرتب سازی خارجی کار می کند بسیار کمتر به طور موثر از مرتب سازی در رم.

### انتخاب بند {#select-select}

[عبارتها](../syntax.md#syntax-expressions) مشخص شده در `SELECT` بند بعد از تمام عملیات در بند بالا توضیح محاسبه به پایان رسید. این عبارات کار می کنند به عنوان اگر به ردیف جداگانه در نتیجه اعمال می شود. اگر عبارات در `SELECT` بند شامل مجموع توابع سپس ClickHouse فرآیندهای کل توابع و عبارات استفاده می شود به عنوان استدلال های خود را در طول [GROUP BY](#select-group-by-clause) تجمع.

اگر شما می خواهید که شامل تمام ستون ها در نتیجه, استفاده از ستاره (`*`) نماد. به عنوان مثال, `SELECT * FROM ...`.

برای مطابقت با برخی از ستون ها در نتیجه با یک [شماره 2](https://en.wikipedia.org/wiki/RE2_(software)) عبارت منظم می توانید از `COLUMNS` اصطلاح.

``` sql
COLUMNS('regexp')
```

برای مثال جدول را در نظر بگیرید:

``` sql
CREATE TABLE default.col_names (aa Int8, ab Int8, bc Int8) ENGINE = TinyLog
```

پرس و جو زیر داده ها را از تمام ستون های حاوی `a` نماد به نام خود.

``` sql
SELECT COLUMNS('a') FROM col_names
```

``` text
┌─aa─┬─ab─┐
│  1 │  1 │
└────┴────┘
```

ستون های انتخاب شده به ترتیب حروف الفبا بازگردانده نمی شوند.

شما می توانید چند استفاده کنید `COLUMNS` عبارات در پرس و جو و اعمال توابع را به خود.

به عنوان مثال:

``` sql
SELECT COLUMNS('a'), COLUMNS('c'), toTypeName(COLUMNS('c')) FROM col_names
```

``` text
┌─aa─┬─ab─┬─bc─┬─toTypeName(bc)─┐
│  1 │  1 │  1 │ Int8           │
└────┴────┴────┴────────────────┘
```

هر ستون توسط `COLUMNS` بیان به تابع به عنوان یک استدلال جداگانه منتقل می شود. همچنین شما می توانید استدلال های دیگر به تابع منتقل می کند اگر از. مراقب باشید در هنگام استفاده از توابع. اگر یک تابع تعداد استدلال شما را به تصویب را پشتیبانی نمی کند, خانه عروسکی می اندازد یک استثنا.

به عنوان مثال:

``` sql
SELECT COLUMNS('a') + COLUMNS('c') FROM col_names
```

``` text
Received exception from server (version 19.14.1):
Code: 42. DB::Exception: Received from localhost:9000. DB::Exception: Number of arguments for function plus doesn't match: passed 3, should be 2.
```

در این مثال, `COLUMNS('a')` بازگرداندن دو ستون: `aa` و `ab`. `COLUMNS('c')` بازگرداندن `bc` ستون. این `+` اپراتور نمی تواند به اعمال 3 استدلال, بنابراین تاتر می اندازد یک استثنا با پیام مربوطه.

ستون که همسان `COLUMNS` بیان می تواند انواع داده های مختلف داشته باشد. اگر `COLUMNS` هیچ ستون مطابقت ندارد و تنها بیان در است `SELECT`, فاحشه خانه می اندازد یک استثنا.

### بند مجزا {#select-distinct}

اگر مشخص شده است, تنها یک ردیف از تمام مجموعه از ردیف به طور کامل تطبیق در نتیجه باقی خواهد ماند.
نتیجه همان خواهد بود که اگر گروه توسط در تمام زمینه های مشخص شده در انتخاب بدون توابع دانه مشخص شد. اما تفاوت های مختلفی از گروه وجود دارد:

-   مجزا را می توان همراه با گروه توسط اعمال می شود.
-   هنگامی که سفارش های حذف شده است و حد تعریف شده است, پرس و جو متوقف می شود در حال اجرا بلافاصله پس از تعداد مورد نیاز از ردیف های مختلف خوانده شده است.
-   بلوک های داده خروجی به عنوان پردازش می شوند, بدون انتظار برای کل پرس و جو را به پایان برساند در حال اجرا.

متمایز پشتیبانی نمی شود اگر انتخاب حداقل یک ستون مجموعه ای دارد.

`DISTINCT` با این نسخهها کار میکند [NULL](../syntax.md#null-literal) همانطور که اگر `NULL` یک مقدار خاص بودند, و `NULL=NULL`. به عبارت دیگر در `DISTINCT` نتایج, ترکیب های مختلف با `NULL` فقط یک بار رخ می دهد.

کلیک پشتیبانی با استفاده از `DISTINCT` و `ORDER BY` بند برای ستون های مختلف در یک پرس و جو. این `DISTINCT` بند قبل از اجرا `ORDER BY` بند بند.

جدول نمونه:

``` text
┌─a─┬─b─┐
│ 2 │ 1 │
│ 1 │ 2 │
│ 3 │ 3 │
│ 2 │ 4 │
└───┴───┘
```

هنگام انتخاب داده ها با `SELECT DISTINCT a FROM t1 ORDER BY b ASC` پرس و جو, ما نتیجه زیر را دریافت کنید:

``` text
┌─a─┐
│ 2 │
│ 1 │
│ 3 │
└───┘
```

اگر ما جهت مرتب سازی را تغییر دهیم `SELECT DISTINCT a FROM t1 ORDER BY b DESC` ما نتیجه زیر را دریافت می کنیم:

``` text
┌─a─┐
│ 3 │
│ 1 │
│ 2 │
└───┘
```

سطر `2, 4` قبل از مرتب سازی قطع شد.

نگاهی به این ویژگی پیاده سازی به حساب زمانی که برنامه نویسی نمایش داده شد.

### بند محدود {#limit-clause}

`LIMIT m` اجازه می دهد تا شما را به انتخاب اولین `m` ردیف از نتیجه.

`LIMIT n, m` اجازه می دهد تا شما را به انتخاب اولین `m` ردیف از نتیجه پس از پرش برای اولین بار `n` ردیف این `LIMIT m OFFSET n` نحو نیز پشتیبانی می کند.

`n` و `m` باید اعداد صحیح غیر منفی باشد.

اگر وجود ندارد `ORDER BY` بند که به صراحت انواع نتایج, نتیجه ممکن است خودسرانه و نامعین.

### اتحادیه همه بند {#union-all-clause}

شما می توانید اتحادیه همه به ترکیب هر تعداد از نمایش داده شد استفاده کنید. مثال:

``` sql
SELECT CounterID, 1 AS table, toInt64(count()) AS c
    FROM test.hits
    GROUP BY CounterID

UNION ALL

SELECT CounterID, 2 AS table, sum(Sign) AS c
    FROM test.visits
    GROUP BY CounterID
    HAVING c > 0
```

فقط اتحادیه پشتیبانی می شود. اتحادیه به طور منظم (اتحادیه مجزا) پشتیبانی نمی شود. اگر شما نیاز به اتحادیه مجزا, شما می توانید ارسال انتخاب کنید متمایز از زیرخاکی حاوی اتحادیه همه.

نمایش داده شد که بخش هایی از اتحادیه همه را می توان به طور همزمان اجرا, و نتایج خود را می توان با هم مخلوط.

ساختار نتایج (تعداد و نوع ستون) باید برای نمایش داده شد مطابقت داشته باشد. اما نام ستون می تواند متفاوت باشد. در این مورد, نام ستون برای نتیجه نهایی خواهد شد از پرس و جو برای اولین بار گرفته شده. نوع ریخته گری برای اتحادیه انجام می شود. برای مثال اگر دو نمایش داده شد در حال ترکیب باید همین زمینه را با غیر-`Nullable` و `Nullable` انواع از یک نوع سازگار, در نتیجه `UNION ALL` دارای یک `Nullable` نوع درست.

نمایش داده شد که بخش هایی از اتحادیه همه را نمی توان در داخل پرانتز محصور شده است. سفارش و محدودیت برای نمایش داده شد جداگانه به نتیجه نهایی اعمال می شود. اگر شما نیاز به اعمال یک تبدیل به نتیجه نهایی, شما می توانید تمام نمایش داده شد با اتحادیه همه در یک خرده فروشی در بند از قرار.

### به OUTFILE بند {#into-outfile-clause}

افزودن `INTO OUTFILE filename` بند (جایی که نام فایل یک رشته تحت اللفظی است) برای تغییر مسیر خروجی پرس و جو به فایل مشخص شده است.
در مقابل خروجی زیر, فایل در سمت سرویس گیرنده ایجاد. پرس و جو شکست مواجه خواهد شد اگر یک فایل با نام فایل مشابه در حال حاضر وجود دارد.
این قابلیت در خط فرمان مشتری و فاحشه خانه در دسترس است-محلی (پرس و جو ارسال شده از طریق رابط اچ.تی. پی شکست خواهد خورد).

فرمت خروجی به طور پیش فرض جدول است (همان است که در خط فرمان حالت دسته ای مشتری).

### بند فرمت {#format-clause}

مشخص ‘FORMAT format’ برای دریافت اطلاعات در هر فرمت مشخص شده است.
شما می توانید این کار را برای راحتی و یا برای ایجاد افسردگی استفاده کنید.
برای کسب اطلاعات بیشتر به بخش مراجعه کنید “Formats”.
اگر بند فرمت حذف شده است, فرمت پیش فرض استفاده شده است, که بستگی به هر دو تنظیمات و رابط مورد استفاده برای دسترسی به دسی بل. برای رابط اچ تی پی و مشتری خط فرمان در حالت دسته ای, فرمت پیش فرض جدولبندی شده است. برای مشتری خط فرمان در حالت تعاملی فرمت پیش فرض است که پیش سازه (این جداول جذاب و جمع و جور).

هنگام استفاده از خط فرمان مشتری داده ها به مشتری در فرمت موثر داخلی منتقل می شود. مشتری به طور مستقل تفسیر بند فرمت پرس و جو و فرمت های داده های خود را (در نتیجه تسکین شبکه و سرور از بار).

### در اپراتورها {#select-in-operators}

این `IN`, `NOT IN`, `GLOBAL IN` و `GLOBAL NOT IN` اپراتورها به طور جداگانه تحت پوشش, از قابلیت های خود را کاملا غنی است.

سمت چپ اپراتور یا یک ستون یا یک تاپل است.

مثالها:

``` sql
SELECT UserID IN (123, 456) FROM ...
SELECT (CounterID, UserID) IN ((34, 123), (101500, 456)) FROM ...
```

اگر سمت چپ یک ستون است که در شاخص است, و در سمت راست مجموعه ای از ثابت است, سیستم با استفاده از شاخص برای پردازش پرس و جو.

Don't list too many values explicitly (i.e. millions). If a data set is large, put it in a temporary table (for example, see the section “External data for query processing”), سپس با استفاده از یک خرده فروشی.

سمت راست اپراتور می تواند مجموعه ای از عبارات ثابت, مجموعه ای از تاپل با عبارات ثابت (نشان داده شده در نمونه های بالا), و یا به نام یک جدول پایگاه داده و یا زیرخاکی در داخل پرانتز را انتخاب کنید.

اگر سمت راست اپراتور نام یک جدول است (مثلا, `UserID IN users`), این معادل به خرده فروشی است `UserID IN (SELECT * FROM users)`. با استفاده از این هنگام کار با داده های خارجی است که همراه با پرس و جو ارسال می شود. مثلا, پرس و جو را می توان همراه با مجموعه ای از شناسه کاربر لود شده به ارسال ‘users’ جدول موقت, که باید فیلتر شود.

اگر در سمت راست اپراتور یک نام جدول است که موتور مجموعه ای است (مجموعه داده های تهیه شده است که همیشه در رم), مجموعه داده خواهد شد بیش از دوباره برای هر پرس و جو ایجاد نمی.

زیرخاکری ممکن است بیش از یک ستون برای فیلتر کردن تاپل را مشخص کنید.
مثال:

``` sql
SELECT (CounterID, UserID) IN (SELECT CounterID, UserID FROM ...) FROM ...
```

ستون به سمت چپ و راست اپراتور در باید همان نوع.

اپراتور در و زیرخاکی ممکن است در هر بخشی از پرس و جو رخ می دهد, از جمله در توابع کل و توابع لامبدا.
مثال:

``` sql
SELECT
    EventDate,
    avg(UserID IN
    (
        SELECT UserID
        FROM test.hits
        WHERE EventDate = toDate('2014-03-17')
    )) AS ratio
FROM test.hits
GROUP BY EventDate
ORDER BY EventDate ASC
```

``` text
┌──EventDate─┬────ratio─┐
│ 2014-03-17 │        1 │
│ 2014-03-18 │ 0.807696 │
│ 2014-03-19 │ 0.755406 │
│ 2014-03-20 │ 0.723218 │
│ 2014-03-21 │ 0.697021 │
│ 2014-03-22 │ 0.647851 │
│ 2014-03-23 │ 0.648416 │
└────────────┴──────────┘
```

برای هر روز پس از مارس 17, تعداد درصد تعداد بازدید از صفحات ساخته شده توسط کاربران که سایت در مارس 17 بازدید.
یک خرده فروشی در بند در همیشه اجرا فقط یک بار بر روی یک سرور. هیچ زیرمجموعه وابسته وجود دارد.

#### پردازش پوچ {#null-processing-1}

در طول پردازش درخواست, در اپراتور فرض می شود که در نتیجه یک عملیات با [NULL](../syntax.md#null-literal) همیشه برابر است با `0`, صرف نظر از اینکه `NULL` است در سمت راست یا چپ اپراتور. `NULL` ارزش ها در هر مجموعه داده شامل نمی شود, به یکدیگر مربوط نیست و نمی توان در مقایسه.

در اینجا یک مثال با است `t_null` جدول:

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │    3 │
└───┴──────┘
```

در حال اجرا پرس و جو `SELECT x FROM t_null WHERE y IN (NULL,3)` به شما نتیجه زیر را می دهد:

``` text
┌─x─┐
│ 2 │
└───┘
```

شما می توانید ببینید که ردیف که `y = NULL` از نتایج پرس و جو پرتاب می شود. دلیل این است که تاتر نمی توانید تصمیم بگیرید که چه `NULL` در `(NULL,3)` تنظیم, بازگشت `0` به عنوان نتیجه عملیات و `SELECT` این سطر را از خروجی نهایی حذف می کند.

``` sql
SELECT y IN (NULL, 3)
FROM t_null
```

``` text
┌─in(y, tuple(NULL, 3))─┐
│                     0 │
│                     1 │
└───────────────────────┘
```

#### توزیع Subqueries {#select-distributed-subqueries}

دو گزینه برای در بازدید کنندگان با کارخانه های فرعی وجود دارد (شبیه به می پیوندد): طبیعی `IN` / `JOIN` و `GLOBAL IN` / `GLOBAL JOIN`. در نحوه اجرا برای پردازش پرس و جو توزیع شده متفاوت است.

!!! attention "توجه"
    به یاد داشته باشید که الگوریتم های زیر توضیح داده شده ممکن است متفاوت بسته به کار [تنظیمات](../../operations/settings/settings.md) `distributed_product_mode` تنظیمات.

هنگام استفاده از به طور منظم در, پرس و جو به سرور از راه دور ارسال, و هر یک از اجرا می شود کارخانه های فرعی در `IN` یا `JOIN` بند بند.

هنگام استفاده از `GLOBAL IN` / `GLOBAL JOINs`, اول همه زیرمجموعه ها برای اجرا `GLOBAL IN` / `GLOBAL JOINs` و نتایج در جداول موقت جمع می شوند. سپس جداول موقت به هر سرور از راه دور ارسال, جایی که نمایش داده شد با استفاده از این داده های موقت اجرا.

برای پرس و جو غیر توزیع, استفاده از به طور منظم `IN` / `JOIN`.

مراقب باشید در هنگام استفاده از کارخانه های فرعی در `IN` / `JOIN` بند برای پردازش پرس و جو توزیع.

بیایید نگاهی به برخی از نمونه. فرض کنید که هر سرور در خوشه طبیعی است **\_تمل**. هر سرور همچنین دارای یک **توزیع \_تماس** جدول با **توزیع شده** نوع, که به نظر می رسد در تمام سرور در خوشه.

برای پرس و جو به **توزیع \_تماس** پرس و جو به تمام سرورهای راه دور ارسال می شود و با استفاده از **\_تمل**.

برای مثال پرس و جو

``` sql
SELECT uniq(UserID) FROM distributed_table
```

به تمام سرورهای راه دور ارسال می شود

``` sql
SELECT uniq(UserID) FROM local_table
```

و به صورت موازی اجرا می شود تا زمانی که به مرحله ای برسد که نتایج متوسط می تواند ترکیب شود. سپس نتایج میانی به سرور درخواست کننده بازگردانده می شود و با هم ادغام می شوند و نتیجه نهایی به مشتری ارسال می شود.

حالا اجازه دهید به بررسی یک پرس و جو با در:

``` sql
SELECT uniq(UserID) FROM distributed_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM local_table WHERE CounterID = 34)
```

-   محاسبه تقاطع مخاطبان از دو سایت.

این پرس و جو خواهد شد به تمام سرور از راه دور به عنوان ارسال می شود

``` sql
SELECT uniq(UserID) FROM local_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM local_table WHERE CounterID = 34)
```

به عبارت دیگر, داده های تعیین شده در بند در خواهد شد بر روی هر سرور به طور مستقل جمع, تنها در سراسر داده است که به صورت محلی بر روی هر یک از سرور های ذخیره شده.

این به درستی و بهینه کار خواهد کرد اگر شما برای این مورد تهیه و داده ها در سراسر سرورهای خوشه گسترش یافته اند به طوری که داده ها را برای یک شناسه تنها ساکن به طور کامل بر روی یک سرور واحد. در این مورد, تمام اطلاعات لازم در دسترس خواهد بود به صورت محلی بر روی هر سرور. در غیر این صورت نتیجه نادرست خواهد بود. ما به این تنوع از پرس و جو به عنوان مراجعه کنید “local IN”.

برای اصلاح چگونه پرس و جو کار می کند زمانی که داده ها به طور تصادفی در سراسر سرور خوشه گسترش, شما می توانید مشخص **توزیع \_تماس** در داخل یک خرده فروشی. پرس و جو شبیه به این خواهد بود:

``` sql
SELECT uniq(UserID) FROM distributed_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM distributed_table WHERE CounterID = 34)
```

این پرس و جو خواهد شد به تمام سرور از راه دور به عنوان ارسال می شود

``` sql
SELECT uniq(UserID) FROM local_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM distributed_table WHERE CounterID = 34)
```

خرده فروشی شروع خواهد شد در حال اجرا بر روی هر سرور از راه دور. پس از زیرخاکری با استفاده از یک جدول توزیع, خرده فروشی است که در هر سرور از راه دور خواهد شد به هر سرور از راه دور به عنوان خشمگین

``` sql
SELECT UserID FROM local_table WHERE CounterID = 34
```

مثلا, اگر شما یک خوشه از 100 سرور, اجرای کل پرس و جو نیاز 10,000 درخواست ابتدایی, که به طور کلی در نظر گرفته غیر قابل قبول.

در چنین مواردی, شما همیشه باید جهانی به جای در استفاده از. بیایید نگاه کنیم که چگونه برای پرس و جو کار می کند

``` sql
SELECT uniq(UserID) FROM distributed_table WHERE CounterID = 101500 AND UserID GLOBAL IN (SELECT UserID FROM distributed_table WHERE CounterID = 34)
```

سرور درخواست کننده خرده فروشی را اجرا خواهد کرد

``` sql
SELECT UserID FROM distributed_table WHERE CounterID = 34
```

و در نتیجه خواهد شد در یک جدول موقت در رم قرار داده است. سپس درخواست خواهد شد به هر سرور از راه دور به عنوان ارسال

``` sql
SELECT uniq(UserID) FROM local_table WHERE CounterID = 101500 AND UserID GLOBAL IN _data1
```

و جدول موقت `_data1` خواهد شد به هر سرور از راه دور با پرس و جو ارسال (نام جدول موقت پیاده سازی تعریف شده است).

این مطلوب تر از استفاده از نرمال در است. با این حال, نگه داشتن نکات زیر را در ذهن:

1.  هنگام ایجاد یک جدول موقت داده های منحصر به فرد ساخته شده است. برای کاهش حجم داده های منتقل شده بر روی شبکه مشخص متمایز در زیرخاکری. (شما لازم نیست برای انجام این کار برای عادی در.)
2.  جدول موقت خواهد شد به تمام سرور از راه دور ارسال. انتقال برای توپولوژی شبکه به حساب نمی. مثلا, اگر 10 سرور از راه دور در یک مرکز داده است که در رابطه با سرور درخواست بسیار از راه دور اقامت, داده ها ارسال خواهد شد 10 بار بیش از کانال به مرکز داده از راه دور. سعی کنید برای جلوگیری از مجموعه داده های بزرگ در هنگام استفاده از جهانی در.
3.  هنگام انتقال داده ها به سرور از راه دور, محدودیت در پهنای باند شبکه قابل تنظیم نیست. شما ممکن است شبکه بیش از حد.
4.  سعی کنید برای توزیع داده ها در سراسر سرور به طوری که شما لازم نیست که به استفاده از جهانی را به صورت منظم.
5.  اگر شما نیاز به استفاده از جهانی در اغلب, برنامه ریزی محل خوشه خانه کلیک به طوری که یک گروه واحد از کپی ساکن در بیش از یک مرکز داده با یک شبکه سریع بین, به طوری که یک پرس و جو را می توان به طور کامل در یک مرکز داده واحد پردازش.

همچنین حس می کند برای مشخص کردن یک جدول محلی در `GLOBAL IN` بند, در صورتی که این جدول محلی تنها بر روی سرور درخواست در دسترس است و شما می خواهید به استفاده از داده ها را از روی سرور از راه دور.

### ارزش های شدید {#extreme-values}

علاوه بر نتایج, شما همچنین می توانید حداقل و حداکثر ارزش برای ستون نتایج از. برای انجام این کار, تنظیم **افراط** تنظیم به 1. حداقل و حداکثر برای انواع عددی محاسبه, تاریخ, و تاریخ با بار. برای ستون های دیگر مقادیر پیش فرض خروجی هستند.

An extra two rows are calculated – the minimums and maximums, respectively. These extra two rows are output in `JSON*`, `TabSeparated*` و `Pretty*` [فرشها](../../interfaces/formats.md), جدا از ردیف های دیگر. خروجی برای فرمت های دیگر نیستند.

داخل `JSON*` فرمت, ارزش شدید خروجی در یک جداگانه ‘extremes’ رشته. داخل `TabSeparated*` فرمت, ردیف پس از نتیجه اصلی, و بعد از ‘totals’ اگر در حال حاضر. این است که توسط یک ردیف خالی قبل (پس از داده های دیگر). داخل `Pretty*` فرمت, ردیف خروجی به عنوان یک جدول جداگانه پس از نتیجه اصلی است, و بعد از `totals` اگر در حال حاضر.

مقادیر شدید برای ردیف قبل محاسبه می شود `LIMIT` اما بعد از `LIMIT BY`. با این حال, هنگام استفاده از `LIMIT offset, size`, ردیف قبل از `offset` در `extremes`. در درخواست جریان, نتیجه نیز ممکن است شامل تعداد کمی از ردیف که از طریق تصویب `LIMIT`.

### یادداشتها {#notes}

این `GROUP BY` و `ORDER BY` بند انجام استدلال موضعی را پشتیبانی نمی کند. این در تضاد خروجی زیر, اما مطابق با استاندارد گذاشتن.
به عنوان مثال, `GROUP BY 1, 2` will be interpreted as grouping by constants (i.e. aggregation of all rows into one).

شما می توانید مترادف استفاده کنید (`AS` نام مستعار) در هر بخشی از یک پرس و جو.

شما می توانید یک ستاره در هر بخشی از یک پرس و جو به جای بیان قرار داده است. هنگامی که پرس و جو تجزیه و تحلیل, ستاره به یک لیست از تمام ستون جدول گسترش (به استثنای `MATERIALIZED` و `ALIAS` ستون). تنها چند مورد وجود دارد که با استفاده از ستاره توجیه می شود:

-   هنگام ایجاد یک روگرفت جدول.
-   برای جداول حاوی فقط چند ستون, مانند جداول سیستم.
-   برای گرفتن اطلاعات در مورد چه ستون در یک جدول هستند. در این مورد, تنظیم `LIMIT 1`. اما بهتر است از `DESC TABLE` پرس و جو.
-   هنگامی که فیلتراسیون قوی در تعداد کمی از ستون ها با استفاده از وجود دارد `PREWHERE`.
-   در subqueries (از ستون های که مورد نیاز نیست برای خارجی پرس و جو از مطالعه حذف شدند subqueries).

در تمام موارد دیگر, ما توصیه نمی با استفاده از ستاره, زیرا تنها به شما می دهد اشکالاتی از یک سندرم روده تحریک پذیر ستونی به جای مزایای. به عبارت دیگر با استفاده از ستاره توصیه نمی شود.

[مقاله اصلی](https://clickhouse.tech/docs/en/query_language/select/) <!--hide-->
