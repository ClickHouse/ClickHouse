---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 41
toc_title: "\u0646\u0634\u0627\u0646\u06CC \u0648\u0628"
---

# نشانی وب {#url}

`url(URL, format, structure)` - بازگرداندن یک جدول ایجاد شده از `URL` با توجه به
`format` و `structure`.

نشانی وب-نشانی کارساز اچتیتیپ یا اچتیتیپس که میتواند بپذیرد `GET` و / یا `POST` درخواست.

قالب - [قالب](../../interfaces/formats.md#formats) از داده ها.

ساختار-ساختار جدول در `'UserID UInt64, Name String'` قالب. تعیین نام ستون و انواع.

**مثال**

``` sql
-- getting the first 3 lines of a table that contains columns of String and UInt32 type from HTTP-server which answers in CSV format.
SELECT * FROM url('http://127.0.0.1:12345/', CSV, 'column1 String, column2 UInt32') LIMIT 3
```

[مقاله اصلی](https://clickhouse.tech/docs/en/query_language/table_functions/url/) <!--hide-->
