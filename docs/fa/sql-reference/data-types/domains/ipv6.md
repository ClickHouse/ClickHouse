---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 60
toc_title: IPv6
---

## IPv6 {#ipv6}

`IPv6` یک دامنه بر اساس `FixedString(16)` نوع و به عنوان یک جایگزین تایپ شده برای ذخیره سازی ارزش های ایپو6 عمل می کند. این فراهم می کند ذخیره سازی جمع و جور با فرمت ورودی خروجی انسان پسند و نوع ستون اطلاعات در بازرسی.

### استفاده عمومی {#basic-usage}

``` sql
CREATE TABLE hits (url String, from IPv6) ENGINE = MergeTree() ORDER BY url;

DESCRIBE TABLE hits;
```

``` text
┌─name─┬─type───┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┐
│ url  │ String │              │                    │         │                  │
│ from │ IPv6   │              │                    │         │                  │
└──────┴────────┴──────────────┴────────────────────┴─────────┴──────────────────┘
```

یا شما می توانید استفاده کنید `IPv6` دامنه به عنوان یک کلید:

``` sql
CREATE TABLE hits (url String, from IPv6) ENGINE = MergeTree() ORDER BY from;
```

`IPv6` دامنه پشتیبانی از ورودی های سفارشی به عنوان ایپو6 رشته:

``` sql
INSERT INTO hits (url, from) VALUES ('https://wikipedia.org', '2a02:aa08:e000:3100::2')('https://clickhouse.tech', '2001:44c8:129:2632:33:0:252:2')('https://clickhouse.tech/docs/en/', '2a02:e980:1e::1');

SELECT * FROM hits;
```

``` text
┌─url────────────────────────────────┬─from──────────────────────────┐
│ https://clickhouse.tech          │ 2001:44c8:129:2632:33:0:252:2 │
│ https://clickhouse.tech/docs/en/ │ 2a02:e980:1e::1               │
│ https://wikipedia.org              │ 2a02:aa08:e000:3100::2        │
└────────────────────────────────────┴───────────────────────────────┘
```

مقادیر به صورت باینری جمع و جور ذخیره می شود:

``` sql
SELECT toTypeName(from), hex(from) FROM hits LIMIT 1;
```

``` text
┌─toTypeName(from)─┬─hex(from)────────────────────────┐
│ IPv6             │ 200144C8012926320033000002520002 │
└──────────────────┴──────────────────────────────────┘
```

ارزش دامنه به طور ضمنی قابل تبدیل به انواع دیگر از `FixedString(16)`.
اگر شما می خواهید برای تبدیل `IPv6` ارزش به یک رشته, شما باید برای انجام این کار به صراحت با `IPv6NumToString()` تابع:

``` sql
SELECT toTypeName(s), IPv6NumToString(from) as s FROM hits LIMIT 1;
```

``` text
┌─toTypeName(IPv6NumToString(from))─┬─s─────────────────────────────┐
│ String                            │ 2001:44c8:129:2632:33:0:252:2 │
└───────────────────────────────────┴───────────────────────────────┘
```

یا بازیگران به یک `FixedString(16)` مقدار:

``` sql
SELECT toTypeName(i), CAST(from as FixedString(16)) as i FROM hits LIMIT 1;
```

``` text
┌─toTypeName(CAST(from, 'FixedString(16)'))─┬─i───────┐
│ FixedString(16)                           │  ��� │
└───────────────────────────────────────────┴─────────┘
```

[مقاله اصلی](https://clickhouse.tech/docs/en/data_types/domains/ipv6) <!--hide-->
