---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 53
toc_title: "\u062A\u0627\u067E\u0644 (\u062A\u06CC1, \u062A\u06CC2,...)"
---

# Tuple(t1, T2, …) {#tuplet1-t2}

یک تاپل از عناصر, هر یک با داشتن یک فرد [نوع](index.md#data_types).

تاپل برای گروه بندی ستون موقت استفاده می شود. ستون ها را می توان گروه بندی کرد زمانی که یک عبارت در یک پرس و جو استفاده می شود, و برای مشخص کردن پارامترهای رسمی خاصی از توابع لامبدا. برای کسب اطلاعات بیشتر به بخش ها مراجعه کنید [در اپراتورها](../../sql-reference/operators/in.md) و [توابع سفارش بالاتر](../../sql-reference/functions/higher-order-functions.md).

تاپل می تواند در نتیجه یک پرس و جو. در این مورد, برای فرمت های متنی غیر از جانسون, ارزش کاما از هم جدا در براکت. در فرمت های جوسون, تاپل خروجی به عنوان ارریس هستند (در براکت مربع).

## ایجاد یک تاپل {#creating-a-tuple}

شما می توانید یک تابع برای ایجاد یک تاپل استفاده کنید:

``` sql
tuple(T1, T2, ...)
```

نمونه ای از ایجاد یک تاپل:

``` sql
SELECT tuple(1,'a') AS x, toTypeName(x)
```

``` text
┌─x───────┬─toTypeName(tuple(1, 'a'))─┐
│ (1,'a') │ Tuple(UInt8, String)      │
└─────────┴───────────────────────────┘
```

## کار با انواع داده ها {#working-with-data-types}

در هنگام ایجاد یک تاپل در پرواز, تاتر به طور خودکار نوع هر استدلال به عنوان حداقل از انواع که می تواند ارزش استدلال ذخیره تشخیص. اگر استدلال است [NULL](../../sql-reference/syntax.md#null-literal), نوع عنصر تاپل است [Nullable](nullable.md).

نمونه ای از تشخیص نوع داده ها به صورت خودکار:

``` sql
SELECT tuple(1, NULL) AS x, toTypeName(x)
```

``` text
┌─x────────┬─toTypeName(tuple(1, NULL))──────┐
│ (1,NULL) │ Tuple(UInt8, Nullable(Nothing)) │
└──────────┴─────────────────────────────────┘
```

[مقاله اصلی](https://clickhouse.tech/docs/en/data_types/tuple/) <!--hide-->
