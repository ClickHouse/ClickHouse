---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 46
toc_title: "\u0698\u0646\u0631\u0627\u0644"
---

# ژنرال {#table_engines-generate}

موتور جدول عمومی تولید داده های تصادفی برای طرح جدول داده شده است.

نمونه های استفاده:

-   استفاده در تست به جمعیت جدول بزرگ تجدید پذیر.
-   تولید ورودی تصادفی برای تست ریش ریش شدن.

## استفاده در سرور کلیک {#usage-in-clickhouse-server}

``` sql
ENGINE = GenerateRandom(random_seed, max_string_length, max_array_length)
```

این `max_array_length` و `max_string_length` پارامترها حداکثر طول همه را مشخص می کنند
ستون ها و رشته های متناوب در داده های تولید شده مطابقت دارند.

تولید موتور جدول پشتیبانی از تنها `SELECT` نمایش داده شد.

این پشتیبانی از تمام [انواع داده](../../../sql-reference/data-types/index.md) این را می توان در یک جدول ذخیره کرد به جز `LowCardinality` و `AggregateFunction`.

**مثال:**

**1.** تنظیم `generate_engine_table` جدول:

``` sql
CREATE TABLE generate_engine_table (name String, value UInt32) ENGINE = GenerateRandom(1, 5, 3)
```

**2.** پرسوجوی داده:

``` sql
SELECT * FROM generate_engine_table LIMIT 3
```

``` text
┌─name─┬──────value─┐
│ c4xJ │ 1412771199 │
│ r    │ 1791099446 │
│ 7#$  │  124312908 │
└──────┴────────────┘
```

## اطلاعات پیاده سازی {#details-of-implementation}

-   پشتیبانی نمیشود:
    -   `ALTER`
    -   `SELECT ... SAMPLE`
    -   `INSERT`
    -   شاخص ها
    -   تکرار

[مقاله اصلی](https://clickhouse.tech/docs/en/operations/table_engines/generate/) <!--hide-->
