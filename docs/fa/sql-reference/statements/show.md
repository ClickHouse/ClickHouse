---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 38
toc_title: SHOW
---

# نمایش & پرسوجو {#show-queries}

## SHOW CREATE TABLE {#show-create-table}

``` sql
SHOW CREATE [TEMPORARY] [TABLE|DICTIONARY] [db.]table [INTO OUTFILE filename] [FORMAT format]
```

بازگرداندن یک `String`- نوع ‘statement’ column, which contains a single value – the `CREATE` پرس و جو مورد استفاده برای ایجاد شی مشخص شده است.

## SHOW DATABASES {#show-databases}

``` sql
SHOW DATABASES [INTO OUTFILE filename] [FORMAT format]
```

چاپ یک لیست از تمام پایگاه های داده.
این پرس و جو یکسان است `SELECT name FROM system.databases [INTO OUTFILE filename] [FORMAT format]`.

## SHOW PROCESSLIST {#show-processlist}

``` sql
SHOW PROCESSLIST [INTO OUTFILE filename] [FORMAT format]
```

خروجی محتوای [سیستم.فرایندها](../../operations/system-tables.md#system_tables-processes) جدول, که شامل یک لیست از نمایش داده شد که در حال حاضر پردازش, به استثنای `SHOW PROCESSLIST` نمایش داده شد.

این `SELECT * FROM system.processes` پرس و جو داده ها در مورد تمام نمایش داده شد در حال حاضر را برمی گرداند.

نکته (اجرا در کنسول):

``` bash
$ watch -n1 "clickhouse-client --query='SHOW PROCESSLIST'"
```

## SHOW TABLES {#show-tables}

نمایش یک لیست از جداول.

``` sql
SHOW [TEMPORARY] TABLES [{FROM | IN} <db>] [LIKE '<pattern>' | WHERE expr] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

اگر `FROM` بند مشخص نشده است, پرس و جو لیستی از جداول گرداند از پایگاه داده فعلی.

شما می توانید نتایج مشابه را دریافت کنید `SHOW TABLES` پرسوجو به روش زیر:

``` sql
SELECT name FROM system.tables WHERE database = <db> [AND name LIKE <pattern>] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

**مثال**

پرس و جو زیر انتخاب دو ردیف اول از لیست جداول در `system` پایگاه داده, که نام حاوی `co`.

``` sql
SHOW TABLES FROM system LIKE '%co%' LIMIT 2
```

``` text
┌─name───────────────────────────┐
│ aggregate_function_combinators │
│ collations                     │
└────────────────────────────────┘
```

## SHOW DICTIONARIES {#show-dictionaries}

نمایش یک لیست از [واژهنامهها خارجی](../../sql-reference/dictionaries/external-dictionaries/external-dicts.md).

``` sql
SHOW DICTIONARIES [FROM <db>] [LIKE '<pattern>'] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

اگر `FROM` بند مشخص نشده است, پرس و جو لیستی از لغت نامه ها را برمی گرداند از پایگاه داده در حال حاضر.

شما می توانید نتایج مشابه را دریافت کنید `SHOW DICTIONARIES` پرسوجو به روش زیر:

``` sql
SELECT name FROM system.dictionaries WHERE database = <db> [AND name LIKE <pattern>] [LIMIT <N>] [INTO OUTFILE <filename>] [FORMAT <format>]
```

**مثال**

پرس و جو زیر انتخاب دو ردیف اول از لیست جداول در `system` پایگاه داده, که نام حاوی `reg`.

``` sql
SHOW DICTIONARIES FROM db LIKE '%reg%' LIMIT 2
```

``` text
┌─name─────────┐
│ regions      │
│ region_names │
└──────────────┘
```

## SHOW GRANTS {#show-grants-statement}

نشان می دهد امتیازات برای یک کاربر.

### نحو {#show-grants-syntax}

``` sql
SHOW GRANTS [FOR user]
```

اگر کاربر مشخص نشده است, پرس و جو امتیازات برای کاربر فعلی را برمی گرداند.

## SHOW CREATE USER {#show-create-user-statement}

پارامترهای که در یک مورد استفاده قرار گرفت را نشان می دهد [ایجاد کاربر](create.md#create-user-statement).

`SHOW CREATE USER` رمزهای عبور کاربر خروجی نیست.

### نحو {#show-create-user-syntax}

``` sql
SHOW CREATE USER [name | CURRENT_USER]
```

## SHOW CREATE ROLE {#show-create-role-statement}

پارامترهای که در یک مورد استفاده قرار گرفت را نشان می دهد [ایجاد نقش](create.md#create-role-statement)

### نحو {#show-create-role-syntax}

``` sql
SHOW CREATE ROLE name
```

## SHOW CREATE ROW POLICY {#show-create-row-policy-statement}

پارامترهای که در یک مورد استفاده قرار گرفت را نشان می دهد [ایجاد خط مشی سطر](create.md#create-row-policy-statement)

### نحو {#show-create-row-policy-syntax}

``` sql
SHOW CREATE [ROW] POLICY name ON [database.]table
```

## SHOW CREATE QUOTA {#show-create-quota-statement}

پارامترهای که در یک مورد استفاده قرار گرفت را نشان می دهد [ایجاد سهمیه](create.md#create-quota-statement)

### نحو {#show-create-row-policy-syntax}

``` sql
SHOW CREATE QUOTA [name | CURRENT]
```

## SHOW CREATE SETTINGS PROFILE {#show-create-settings-profile-statement}

پارامترهای که در یک مورد استفاده قرار گرفت را نشان می دهد [تنظیمات ایجاد پروفایل](create.md#create-settings-profile-statement)

### نحو {#show-create-row-policy-syntax}

``` sql
SHOW CREATE [SETTINGS] PROFILE name
```

[مقاله اصلی](https://clickhouse.tech/docs/en/query_language/show/) <!--hide-->
