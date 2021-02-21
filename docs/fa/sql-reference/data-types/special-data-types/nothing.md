---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 60
toc_title: "\u0647\u06CC\u0686\u06CC"
---

# هیچی {#nothing}

تنها هدف از این نوع داده ها نشان دهنده مواردی است که انتظار نمی رود ارزش باشد. بنابراین شما می توانید یک ایجاد کنید `Nothing` نوع ارزش.

مثلا, تحت اللفظی [NULL](../../../sql-reference/syntax.md#null-literal) دارای نوع `Nullable(Nothing)`. اطلاعات بیشتر در مورد [Nullable](../../../sql-reference/data-types/nullable.md).

این `Nothing` نوع نیز می تواند مورد استفاده برای نشان دادن خالی:

``` sql
SELECT toTypeName(array())
```

``` text
┌─toTypeName(array())─┐
│ Array(Nothing)      │
└─────────────────────┘
```

[مقاله اصلی](https://clickhouse.tech/docs/en/data_types/special_data_types/nothing/) <!--hide-->
