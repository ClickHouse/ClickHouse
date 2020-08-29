---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 53
toc_title: "\u06A9\u0627\u0631 \u0628\u0627 \u06CC\u0648\u06CC\u062F"
---

# توابع برای کار با یوید {#functions-for-working-with-uuid}

توابع برای کار با شناسه به شرح زیر است.

## جنراتیدو4 {#uuid-function-generate}

تولید [UUID](../../sql-reference/data-types/uuid.md) از [نسخه 4](https://tools.ietf.org/html/rfc4122#section-4.4).

``` sql
generateUUIDv4()
```

**مقدار بازگشتی**

مقدار نوع شناسه.

**مثال طریقه استفاده**

این مثال نشان می دهد ایجاد یک جدول با ستون نوع شناسه و قرار دادن یک مقدار به جدول.

``` sql
CREATE TABLE t_uuid (x UUID) ENGINE=TinyLog

INSERT INTO t_uuid SELECT generateUUIDv4()

SELECT * FROM t_uuid
```

``` text
┌────────────────────────────────────x─┐
│ f4bf890f-f9dc-4332-ad5c-0c18e73f28e9 │
└──────────────────────────────────────┘
```

## شناسه بسته:) {#touuid-x}

تبدیل مقدار نوع رشته به نوع شناسه.

``` sql
toUUID(String)
```

**مقدار بازگشتی**

مقدار نوع شناسه.

**مثال طریقه استفاده**

``` sql
SELECT toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0') AS uuid
```

``` text
┌─────────────────────────────────uuid─┐
│ 61f0c404-5cb3-11e7-907b-a6006ad3dba0 │
└──────────────────────────────────────┘
```

## وضعیت زیستشناختی رکورد {#uuidstringtonum}

می پذیرد یک رشته حاوی 36 کاراکتر در قالب `xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx` و به عنوان مجموعه ای از بایت ها در یک [رشته ثابت (16)](../../sql-reference/data-types/fixedstring.md).

``` sql
UUIDStringToNum(String)
```

**مقدار بازگشتی**

رشته ثابت (16)

**نمونه های استفاده**

``` sql
SELECT
    '612f3c40-5d3b-217e-707b-6a546a3d7b29' AS uuid,
    UUIDStringToNum(uuid) AS bytes
```

``` text
┌─uuid─────────────────────────────────┬─bytes────────────┐
│ 612f3c40-5d3b-217e-707b-6a546a3d7b29 │ a/<@];!~p{jTj={) │
└──────────────────────────────────────┴──────────────────┘
```

## هشدار داده می شود {#uuidnumtostring}

می پذیرد [رشته ثابت (16)](../../sql-reference/data-types/fixedstring.md) ارزش, و یک رشته حاوی گرداند 36 شخصیت در قالب متن.

``` sql
UUIDNumToString(FixedString(16))
```

**مقدار بازگشتی**

رشته.

**مثال طریقه استفاده**

``` sql
SELECT
    'a/<@];!~p{jTj={)' AS bytes,
    UUIDNumToString(toFixedString(bytes, 16)) AS uuid
```

``` text
┌─bytes────────────┬─uuid─────────────────────────────────┐
│ a/<@];!~p{jTj={) │ 612f3c40-5d3b-217e-707b-6a546a3d7b29 │
└──────────────────┴──────────────────────────────────────┘
```

## همچنین نگاه کنید به {#see-also}

-   [دیکتاتوری](ext-dict-functions.md#ext_dict_functions-other)

[مقاله اصلی](https://clickhouse.tech/docs/en/query_language/functions/uuid_function/) <!--hide-->
