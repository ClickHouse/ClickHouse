---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 54
toc_title: Nullable
---

# Nullable(typename) {#data_type-nullable}

اجازه می دهد تا برای ذخیره نشانگر ویژه ([NULL](../../sql-reference/syntax.md)) که نشان دهنده “missing value” در کنار مقادیر عادی مجاز `TypeName`. برای مثال یک `Nullable(Int8)` ستون نوع می تواند ذخیره شود `Int8` ارزش نوع و ردیف است که ارزش ذخیره خواهد شد `NULL`.

برای یک `TypeName` شما نمی توانید از انواع داده های کامپوزیت استفاده کنید [& حذف](array.md) و [تاپل](tuple.md). انواع داده های کامپوزیت می تواند شامل `Nullable` مقادیر نوع مانند `Array(Nullable(Int8))`.

A `Nullable` فیلد نوع را نمی توان در شاخص های جدول گنجانده شده است.

`NULL` مقدار پیش فرض برای هر `Nullable` نوع, مگر اینکه در غیر این صورت در پیکربندی سرور کلیک مشخص.

## ویژگی های ذخیره سازی {#storage-features}

برای ذخیره `Nullable` ارزش نوع در یک ستون جدول, تاتر با استفاده از یک فایل جداگانه با `NULL` ماسک علاوه بر فایل عادی با ارزش. مطالب در ماسک فایل اجازه می دهد خانه کلیک برای تشخیص بین `NULL` و یک مقدار پیش فرض از نوع داده مربوطه را برای هر سطر جدول. به دلیل یک فایل اضافی, `Nullable` ستون مصرف فضای ذخیره سازی اضافی در مقایسه با یک نرمال مشابه.

!!! info "یادداشت"
    با استفاده از `Nullable` تقریبا همیشه منفی تاثیر می گذارد عملکرد, نگه داشتن این در ذهن در هنگام طراحی پایگاه داده های خود را.

## مثال طریقه استفاده {#usage-example}

``` sql
CREATE TABLE t_null(x Int8, y Nullable(Int8)) ENGINE TinyLog
```

``` sql
INSERT INTO t_null VALUES (1, NULL), (2, 3)
```

``` sql
SELECT x + y FROM t_null
```

``` text
┌─plus(x, y)─┐
│       ᴺᵁᴸᴸ │
│          5 │
└────────────┘
```

[مقاله اصلی](https://clickhouse.tech/docs/en/data_types/nullable/) <!--hide-->
