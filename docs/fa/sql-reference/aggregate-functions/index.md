---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_folder_title: "\u062A\u0648\u0627\u0628\u0639 \u0645\u062C\u0645\u0648\u0639"
toc_priority: 33
toc_title: "\u0645\u0639\u0631\u0641\u06CC \u0634\u0631\u06A9\u062A"
---

# توابع مجموع {#aggregate-functions}

توابع مجموع در کار [عادی](http://www.sql-tutorial.com/sql-aggregate-functions-sql-tutorial) راه به عنوان کارشناسان پایگاه داده انتظار می رود.

فاحشه خانه نیز پشتیبانی می کند:

-   [توابع مجموع پارامتری](parametric-functions.md#aggregate_functions_parametric), که قبول پارامترهای دیگر علاوه بر ستون.
-   [ترکیب کنندهها](combinators.md#aggregate_functions_combinators) که تغییر رفتار مجموع توابع.

## پردازش پوچ {#null-processing}

در طول تجمع همه `NULL`بازدید کنندگان قلم می.

**مثالها:**

این جدول را در نظر بگیرید:

``` text
┌─x─┬────y─┐
│ 1 │    2 │
│ 2 │ ᴺᵁᴸᴸ │
│ 3 │    2 │
│ 3 │    3 │
│ 3 │ ᴺᵁᴸᴸ │
└───┴──────┘
```

بیایید می گویند شما نیاز به کل ارزش ها در `y` ستون:

``` sql
SELECT sum(y) FROM t_null_big
```

    ┌─sum(y)─┐
    │      7 │
    └────────┘

این `sum` تابع تفسیر می کند `NULL` به عنوان `0`. به خصوص, این بدان معنی است که اگر تابع ورودی از یک انتخاب که تمام مقادیر دریافت `NULL` سپس نتیجه خواهد بود `0` نه `NULL`.

حالا شما می توانید استفاده کنید `groupArray` تابع برای ایجاد مجموعه ای از `y` ستون:

``` sql
SELECT groupArray(y) FROM t_null_big
```

``` text
┌─groupArray(y)─┐
│ [2,2,3]       │
└───────────────┘
```

`groupArray` شامل نمی شود `NULL` در مجموعه ای نتیجه.

[مقاله اصلی](https://clickhouse.tech/docs/en/query_language/agg_functions/) <!--hide-->
