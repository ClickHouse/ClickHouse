---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 60
toc_title: "\u0647\u06CC\u0686 \u0686\u06CC\u0632"
---

# هیچی {#nothing}

تنها هدف از این نوع داده ها نشان دهنده مواردی است که انتظار نمی رود ارزش باشد. بنابراین شما می توانید یک ایجاد کنید `Nothing` نوع ارزش.

مثلا, تحت اللفظی [NULL](../../../sql_reference/syntax.md#null-literal) دارای نوع `Nullable(Nothing)`. اطلاعات بیشتر در مورد [Nullable](../../../sql_reference/data_types/nullable.md).

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
