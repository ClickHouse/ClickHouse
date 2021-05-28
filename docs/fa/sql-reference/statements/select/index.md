---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
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
[GLOBAL] [ANY|ALL|ASOF] [INNER|LEFT|RIGHT|FULL|CROSS] [OUTER|SEMI|ANTI] JOIN (subquery)|table (ON <expr_list>)|(USING <column_list>)
[PREWHERE expr]
[WHERE expr]
[GROUP BY expr_list] [WITH TOTALS]
[HAVING expr]
[ORDER BY expr_list] [WITH FILL] [FROM expr] [TO expr] [STEP expr] 
[LIMIT [offset_value, ]n BY columns]
[LIMIT [n, ]m] [WITH TIES]
[UNION ALL ...]
[INTO OUTFILE filename]
[FORMAT format]
```

همه بند اختیاری هستند, به جز برای لیست مورد نیاز از عبارات بلافاصله پس از `SELECT` که بیشتر تحت پوشش است [در زیر](#select-clause).

ویژگی های هر بند اختیاری در بخش های جداگانه تحت پوشش, که در همان جهت ذکر شده که اجرا می شوند:

-   [با بند](with.md)
-   [بند مجزا](distinct.md)
-   [از بند](from.md)
-   [بند نمونه](sample.md)
-   [پیوستن بند](join.md)
-   [در بند](prewhere.md)
-   [بند کجا](where.md)
-   [گروه بر اساس بند](group-by.md)
-   [محدود کردن بند](limit-by.md)
-   [داشتن بند](having.md)
-   [انتخاب بند](#select-clause)
-   [بند محدود](limit.md)
-   [اتحادیه همه بند](union-all.md)

## انتخاب بند {#select-clause}

[عبارتها](../../syntax.md#syntax-expressions) مشخص شده در `SELECT` بند بعد از تمام عملیات در بند بالا توضیح محاسبه به پایان رسید. این عبارات کار می کنند به عنوان اگر به ردیف جداگانه در نتیجه اعمال می شود. اگر عبارات در `SELECT` بند شامل مجموع توابع سپس ClickHouse فرآیندهای کل توابع و عبارات استفاده می شود به عنوان استدلال های خود را در طول [GROUP BY](group-by.md) تجمع.

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

### ستاره {#asterisk}

شما می توانید یک ستاره در هر بخشی از یک پرس و جو به جای بیان قرار داده است. هنگامی که پرس و جو تجزیه و تحلیل, ستاره به یک لیست از تمام ستون جدول گسترش (به استثنای `MATERIALIZED` و `ALIAS` ستون). تنها چند مورد وجود دارد که با استفاده از ستاره توجیه می شود:

-   هنگام ایجاد یک روگرفت جدول.
-   برای جداول حاوی فقط چند ستون, مانند جداول سیستم.
-   برای گرفتن اطلاعات در مورد چه ستون در یک جدول هستند. در این مورد, تنظیم `LIMIT 1`. اما بهتر است از `DESC TABLE` پرس و جو.
-   هنگامی که فیلتراسیون قوی در تعداد کمی از ستون ها با استفاده از وجود دارد `PREWHERE`.
-   در subqueries (از ستون های که مورد نیاز نیست برای خارجی پرس و جو از مطالعه حذف شدند subqueries).

در تمام موارد دیگر, ما توصیه نمی با استفاده از ستاره, زیرا تنها به شما می دهد اشکالاتی از یک سندرم روده تحریک پذیر ستونی به جای مزایای. به عبارت دیگر با استفاده از ستاره توصیه نمی شود.

### ارزش های شدید {#extreme-values}

علاوه بر نتایج, شما همچنین می توانید حداقل و حداکثر ارزش برای ستون نتایج از. برای انجام این کار, تنظیم **افراط** تنظیم به 1. حداقل و حداکثر برای انواع عددی محاسبه, تاریخ, و تاریخ با بار. برای ستون های دیگر مقادیر پیش فرض خروجی هستند.

An extra two rows are calculated – the minimums and maximums, respectively. These extra two rows are output in `JSON*`, `TabSeparated*` و `Pretty*` [فرشها](../../../interfaces/formats.md), جدا از ردیف های دیگر. خروجی برای فرمت های دیگر نیستند.

داخل `JSON*` فرمت, ارزش شدید خروجی در یک جداگانه ‘extremes’ رشته. داخل `TabSeparated*` فرمت, ردیف پس از نتیجه اصلی, و بعد از ‘totals’ اگر در حال حاضر. این است که توسط یک ردیف خالی قبل (پس از داده های دیگر). داخل `Pretty*` فرمت, ردیف خروجی به عنوان یک جدول جداگانه پس از نتیجه اصلی است, و بعد از `totals` اگر در حال حاضر.

مقادیر شدید برای ردیف قبل محاسبه می شود `LIMIT` اما بعد از `LIMIT BY`. با این حال, هنگام استفاده از `LIMIT offset, size`, ردیف قبل از `offset` در `extremes`. در درخواست جریان, نتیجه نیز ممکن است شامل تعداد کمی از ردیف که از طریق تصویب `LIMIT`.

### یادداشتها {#notes}

شما می توانید مترادف استفاده کنید (`AS` نام مستعار) در هر بخشی از یک پرس و جو.

این `GROUP BY` و `ORDER BY` بند انجام استدلال موضعی را پشتیبانی نمی کند. این در تضاد خروجی زیر, اما مطابق با استاندارد گذاشتن. به عنوان مثال, `GROUP BY 1, 2` will be interpreted as grouping by constants (i.e. aggregation of all rows into one).

## پیاده سازی اطلاعات {#implementation-details}

اگر پرس و جو حذف `DISTINCT`, `GROUP BY` و `ORDER BY` بند و `IN` و `JOIN` کارخانه های فرعی, پرس و جو خواهد شد به طور کامل جریان پردازش, با استفاده از ای(1) مقدار رم. در غیر این صورت, پرس و جو ممکن است مقدار زیادی از رم مصرف اگر محدودیت های مناسب مشخص نشده است:

-   `max_memory_usage`
-   `max_rows_to_group_by`
-   `max_rows_to_sort`
-   `max_rows_in_distinct`
-   `max_bytes_in_distinct`
-   `max_rows_in_set`
-   `max_bytes_in_set`
-   `max_rows_in_join`
-   `max_bytes_in_join`
-   `max_bytes_before_external_sort`
-   `max_bytes_before_external_group_by`

برای کسب اطلاعات بیشتر به بخش مراجعه کنید “Settings”. ممکن است که به استفاده از مرتب سازی خارجی (صرفه جویی در جداول موقت به یک دیسک) و تجمع خارجی.

{## [مقاله اصلی](https://clickhouse.tech/docs/en/sql-reference/statements/select/) ##}
