---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 46
toc_title: UUID
---

# UUID {#uuid-data-type}

شناسه جهانی منحصر به فرد (شناسه) یک عدد 16 بایت مورد استفاده برای شناسایی سوابق است. برای کسب اطلاعات دقیق در مورد شناسه, دیدن [ویکیپدیا](https://en.wikipedia.org/wiki/Universally_unique_identifier).

نمونه ای از ارزش نوع شناسه در زیر نشان داده شده است:

``` text
61f0c404-5cb3-11e7-907b-a6006ad3dba0
```

اگر شما مقدار ستون شناسه مشخص نیست در هنگام قرار دادن یک رکورد جدید, ارزش شناسه با صفر پر:

``` text
00000000-0000-0000-0000-000000000000
```

## چگونه برای تولید {#how-to-generate}

برای تولید ارزش شناسه, خانه فراهم می کند [جنراتیدو4](../../sql_reference/functions/uuid_functions.md) تابع.

## مثال طریقه استفاده {#usage-example}

**مثال 1**

این مثال نشان می دهد ایجاد یک جدول با ستون نوع شناسه و قرار دادن یک مقدار به جدول.

``` sql
CREATE TABLE t_uuid (x UUID, y String) ENGINE=TinyLog
```

``` sql
INSERT INTO t_uuid SELECT generateUUIDv4(), 'Example 1'
```

``` sql
SELECT * FROM t_uuid
```

``` text
┌────────────────────────────────────x─┬─y─────────┐
│ 417ddc5d-e556-4d27-95dd-a34d84e46a50 │ Example 1 │
└──────────────────────────────────────┴───────────┘
```

**مثال 2**

در این مثال مقدار ستون یوید هنگام وارد کردن یک رکورد جدید مشخص نشده است.

``` sql
INSERT INTO t_uuid (y) VALUES ('Example 2')
```

``` sql
SELECT * FROM t_uuid
```

``` text
┌────────────────────────────────────x─┬─y─────────┐
│ 417ddc5d-e556-4d27-95dd-a34d84e46a50 │ Example 1 │
│ 00000000-0000-0000-0000-000000000000 │ Example 2 │
└──────────────────────────────────────┴───────────┘
```

## محدودیت ها {#restrictions}

نوع داده شناسه تنها پشتیبانی از توابع که [رشته](string.md) نوع داده نیز پشتیبانی می کند (به عنوان مثال, [کمینه](../../sql_reference/aggregate_functions/reference.md#agg_function-min), [حداکثر](../../sql_reference/aggregate_functions/reference.md#agg_function-max) و [شمارش](../../sql_reference/aggregate_functions/reference.md#agg_function-count)).

نوع داده یوید توسط عملیات ریاضی پشتیبانی نمی شود (به عنوان مثال, [شکم](../../sql_reference/functions/arithmetic_functions.md#arithm_func-abs)) و یا توابع دانه, مانند [جمع](../../sql_reference/aggregate_functions/reference.md#agg_function-sum) و [میانگین](../../sql_reference/aggregate_functions/reference.md#agg_function-avg).

[مقاله اصلی](https://clickhouse.tech/docs/en/data_types/uuid/) <!--hide-->
