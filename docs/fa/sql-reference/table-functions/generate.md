---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 47
toc_title: "\u0698\u0646\u0631\u0627\u0644"
---

# ژنرال {#generaterandom}

تولید داده های تصادفی با طرح داده شده است.
اجازه می دهد تا به جمعیت جداول تست با داده ها.
پشتیبانی از تمام انواع داده است که می تواند در جدول به جز ذخیره می شود `LowCardinality` و `AggregateFunction`.

``` sql
generateRandom('name TypeName[, name TypeName]...', [, 'random_seed'[, 'max_string_length'[, 'max_array_length']]]);
```

**پارامترها**

-   `name` — Name of corresponding column.
-   `TypeName` — Type of corresponding column.
-   `max_array_length` — Maximum array length for all generated arrays. Defaults to `10`.
-   `max_string_length` — Maximum string length for all generated strings. Defaults to `10`.
-   `random_seed` — Specify random seed manually to produce stable results. If NULL — seed is randomly generated.

**مقدار بازگشتی**

یک شی جدول با طرح درخواست.

## مثال طریقه استفاده {#usage-example}

``` sql
SELECT * FROM generateRandom('a Array(Int8), d Decimal32(4), c Tuple(DateTime64(3), UUID)', 1, 10, 2) LIMIT 3;
```

``` text
┌─a────────┬────────────d─┬─c──────────────────────────────────────────────────────────────────┐
│ [77]     │ -124167.6723 │ ('2061-04-17 21:59:44.573','3f72f405-ec3e-13c8-44ca-66ef335f7835') │
│ [32,110] │ -141397.7312 │ ('1979-02-09 03:43:48.526','982486d1-5a5d-a308-e525-7bd8b80ffa73') │
│ [68]     │  -67417.0770 │ ('2080-03-12 14:17:31.269','110425e5-413f-10a6-05ba-fa6b3e929f15') │
└──────────┴──────────────┴────────────────────────────────────────────────────────────────────┘
```

[مقاله اصلی](https://clickhouse.tech/docs/en/query_language/table_functions/generate/) <!--hide-->
