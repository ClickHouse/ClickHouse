---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 59
toc_title: IPv4
---

## IPv4 {#ipv4}

`IPv4` یک دامنه بر اساس `UInt32` نوع و به عنوان یک جایگزین تایپ شده برای ذخیره سازی مقادیر ایپو4 عمل می کند. این فراهم می کند ذخیره سازی جمع و جور با فرمت ورودی خروجی انسان پسند و نوع ستون اطلاعات در بازرسی.

### استفاده عمومی {#basic-usage}

``` sql
CREATE TABLE hits (url String, from IPv4) ENGINE = MergeTree() ORDER BY url;

DESCRIBE TABLE hits;
```

``` text
┌─name─┬─type───┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┐
│ url  │ String │              │                    │         │                  │
│ from │ IPv4   │              │                    │         │                  │
└──────┴────────┴──────────────┴────────────────────┴─────────┴──────────────────┘
```

یا شما می توانید از دامنه ایپو4 به عنوان یک کلید استفاده کنید:

``` sql
CREATE TABLE hits (url String, from IPv4) ENGINE = MergeTree() ORDER BY from;
```

`IPv4` دامنه پشتیبانی از فرمت ورودی سفارشی به عنوان ایپو4 رشته:

``` sql
INSERT INTO hits (url, from) VALUES ('https://wikipedia.org', '116.253.40.133')('https://clickhouse.tech', '183.247.232.58')('https://clickhouse.tech/docs/en/', '116.106.34.242');

SELECT * FROM hits;
```

``` text
┌─url────────────────────────────────┬───────────from─┐
│ https://clickhouse.tech/docs/en/ │ 116.106.34.242 │
│ https://wikipedia.org              │ 116.253.40.133 │
│ https://clickhouse.tech          │ 183.247.232.58 │
└────────────────────────────────────┴────────────────┘
```

مقادیر به صورت باینری جمع و جور ذخیره می شود:

``` sql
SELECT toTypeName(from), hex(from) FROM hits LIMIT 1;
```

``` text
┌─toTypeName(from)─┬─hex(from)─┐
│ IPv4             │ B7F7E83A  │
└──────────────────┴───────────┘
```

ارزش دامنه به طور ضمنی قابل تبدیل به انواع دیگر از `UInt32`.
اگر شما می خواهید برای تبدیل `IPv4` ارزش به یک رشته, شما باید برای انجام این کار به صراحت با `IPv4NumToString()` تابع:

``` sql
SELECT toTypeName(s), IPv4NumToString(from) as s FROM hits LIMIT 1;
```

    ┌─toTypeName(IPv4NumToString(from))─┬─s──────────────┐
    │ String                            │ 183.247.232.58 │
    └───────────────────────────────────┴────────────────┘

یا بازیگران به یک `UInt32` مقدار:

``` sql
SELECT toTypeName(i), CAST(from as UInt32) as i FROM hits LIMIT 1;
```

``` text
┌─toTypeName(CAST(from, 'UInt32'))─┬──────────i─┐
│ UInt32                           │ 3086477370 │
└──────────────────────────────────┴────────────┘
```

[مقاله اصلی](https://clickhouse.tech/docs/en/data_types/domains/ipv4) <!--hide-->
