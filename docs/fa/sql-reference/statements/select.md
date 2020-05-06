---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
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

اگر پرس و جو حذف `DISTINCT`, `GROUP BY` و `ORDER BY` بند و `IN` و `JOIN` subqueries این پرس و جو خواهد شد به طور کامل جریان پردازش با استفاده از O(1) میزان رم.
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

مثال 3: استفاده از نتایج عددی پرس

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
به عنوان یک راهحل برای محدودیت کنونی برای بیان استفاده در subqueries شما ممکن است تکراری است.

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
-   نیازهای کسب و کار هدف تقریبی نتایج (برای مقرون به صرفه بودن و یا به منظور به بازار دقیق نتایج به کاربران حق بیمه).

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

اجازه می دهد تا اجرای `JOIN` با یک آرایه یا تو در تو ساختار داده ها. قصد این است که شبیه به [ارریجین](../../sql-reference/functions/array-join.md#functions_arrayjoin) تابع, اما قابلیت های خود را گسترده تر است.

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

"’متن
table\_1 table\_2

رویداد \| عصر / رویداد فراسوی / عصر / شناسه
