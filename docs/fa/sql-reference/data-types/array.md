---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 51
toc_title: "& \u062A\u0648\u0631\u06CC)"
---

# & توری) {#data-type-array}

مجموعه ای از `T`- نوع اقلام است. `T` می تواند هر نوع داده, از جمله مجموعه.

## ایجاد یک مجموعه {#creating-an-array}

شما می توانید یک تابع برای ایجاد مجموعه ای استفاده کنید:

``` sql
array(T)
```

شما همچنین می توانید براکت مربع استفاده کنید.

``` sql
[]
```

نمونه ای از ایجاد یک مجموعه:

``` sql
SELECT array(1, 2) AS x, toTypeName(x)
```

``` text
┌─x─────┬─toTypeName(array(1, 2))─┐
│ [1,2] │ Array(UInt8)            │
└───────┴─────────────────────────┘
```

``` sql
SELECT [1, 2] AS x, toTypeName(x)
```

``` text
┌─x─────┬─toTypeName([1, 2])─┐
│ [1,2] │ Array(UInt8)       │
└───────┴────────────────────┘
```

## کار با انواع داده ها {#working-with-data-types}

در هنگام ایجاد مجموعه ای در پرواز, خانه رعیتی به طور خودکار نوع استدلال به عنوان باریک ترین نوع داده است که می تواند تمام استدلال ذکر شده ذخیره تعریف. اگر وجود دارد [Nullable](nullable.md#data_type-nullable) یا تحت اللفظی [NULL](../../sql-reference/syntax.md#null-literal) ارزش, نوع عنصر مجموعه ای نیز می شود [Nullable](nullable.md).

اگر فاحشه خانه می تواند نوع داده را تعیین نمی کند, این تولید یک استثنا. مثلا, این اتفاق می افتد زمانی که تلاش برای ایجاد مجموعه ای با رشته ها و اعداد به طور همزمان (`SELECT array(1, 'a')`).

نمونه هایی از تشخیص نوع داده ها به صورت خودکار:

``` sql
SELECT array(1, 2, NULL) AS x, toTypeName(x)
```

``` text
┌─x──────────┬─toTypeName(array(1, 2, NULL))─┐
│ [1,2,NULL] │ Array(Nullable(UInt8))        │
└────────────┴───────────────────────────────┘
```

اگر شما سعی می کنید برای ایجاد مجموعه ای از انواع داده های ناسازگار, تاتر می اندازد یک استثنا:

``` sql
SELECT array(1, 'a')
```

``` text
Received exception from server (version 1.1.54388):
Code: 386. DB::Exception: Received from localhost:9000, 127.0.0.1. DB::Exception: There is no supertype for types UInt8, String because some of them are String/FixedString and some of them are not.
```

[مقاله اصلی](https://clickhouse.tech/docs/en/data_types/array/) <!--hide-->
