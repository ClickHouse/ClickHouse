---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
---

# با بند {#with-clause}

در این بخش پشتیبانی از عبارات جدول مشترک فراهم می کند ([CTE](https://en.wikipedia.org/wiki/Hierarchical_and_recursive_queries_in_SQL)), بنابراین نتایج حاصل از `WITH` بند را می توان در داخل استفاده کرد `SELECT` بند بند.

## محدودیت ها {#limitations}

1.  نمایش داده شد بازگشتی پشتیبانی نمی شوند.
2.  هنگامی که زیرخاکری در داخل با بخش استفاده می شود, این نتیجه باید اسکالر با دقیقا یک ردیف باشد.
3.  نتایج بیان در زیرمجموعه ها در دسترس نیست.

## مثالها {#examples}

**مثال 1:** استفاده از عبارت ثابت به عنوان “variable”

``` sql
WITH '2019-08-01 15:23:00' as ts_upper_bound
SELECT *
FROM hits
WHERE
    EventDate = toDate(ts_upper_bound) AND
    EventTime <= ts_upper_bound
```

**مثال 2:** جمع مبلغ (بایت) بیان نتیجه از بند لیست ستون را انتخاب کنید

``` sql
WITH sum(bytes) as s
SELECT
    formatReadableSize(s),
    table
FROM system.parts
GROUP BY table
ORDER BY s
```

**مثال 3:** استفاده از نتایج حاصل از زیرکواری اسکالر

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

**مثال 4:** استفاده مجدد از عبارت در زیرخاکری

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
