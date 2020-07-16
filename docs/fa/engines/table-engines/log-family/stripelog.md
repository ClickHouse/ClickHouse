---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 32
toc_title: "\u062E\u0637 \u0632\u062F\u0646"
---

# خط زدن {#stripelog}

این موتور متعلق به خانواده از موتورهای ورود به سیستم. مشاهده خواص مشترک از موتورهای ورود به سیستم و تفاوت های خود را در [ورود خانواده موتور](index.md) مقاله.

با استفاده از این موتور در حالات زمانی که شما نیاز به نوشتن بسیاری از جداول با مقدار کمی از داده ها (کمتر از 1 میلیون ردیف).

## ایجاد یک جدول {#table_engines-stripelog-creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    column1_name [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    column2_name [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = StripeLog
```

شرح مفصلی از [CREATE TABLE](../../../sql-reference/statements/create.md#create-table-query) پرس و جو.

## نوشتن داده ها {#table_engines-stripelog-writing-the-data}

این `StripeLog` موتور فروشگاه تمام ستون ها در یک فایل. برای هر `INSERT` پرس و جو, خانه رعیتی بلوک داده ها به پایان یک فایل جدول, نوشتن ستون یک به یک.

برای هر کلیک جدول فایل ها را می نویسد:

-   `data.bin` — Data file.
-   `index.mrk` — File with marks. Marks contain offsets for each column of each data block inserted.

این `StripeLog` موتور را پشتیبانی نمی کند `ALTER UPDATE` و `ALTER DELETE` عملیات.

## خواندن داده ها {#table_engines-stripelog-reading-the-data}

فایل را با نشانه اجازه می دهد تا ClickHouse به parallelize خواندن داده ها. این به این معنی است که یک `SELECT` پرس و جو ردیف در جهت غیر قابل پیش بینی می گرداند. استفاده از `ORDER BY` بند برای مرتب کردن ردیف.

## مثال استفاده {#table_engines-stripelog-example-of-use}

ایجاد یک جدول:

``` sql
CREATE TABLE stripe_log_table
(
    timestamp DateTime,
    message_type String,
    message String
)
ENGINE = StripeLog
```

درج داده:

``` sql
INSERT INTO stripe_log_table VALUES (now(),'REGULAR','The first regular message')
INSERT INTO stripe_log_table VALUES (now(),'REGULAR','The second regular message'),(now(),'WARNING','The first warning message')
```

ما با استفاده از دو `INSERT` نمایش داده شد برای ایجاد دو بلوک داده ها در داخل `data.bin` پرونده.

خانه رعیتی با استفاده از موضوعات متعدد در هنگام انتخاب داده ها. هر موضوع یک بلوک داده جداگانه را می خواند و ردیف ها را به طور مستقل به پایان می رساند. در نتیجه, منظور از بلوک های ردیف در خروجی می کند منظور از بلوک های مشابه در ورودی در اکثر موارد مطابقت ندارد. به عنوان مثال:

``` sql
SELECT * FROM stripe_log_table
```

``` text
┌───────────timestamp─┬─message_type─┬─message────────────────────┐
│ 2019-01-18 14:27:32 │ REGULAR      │ The second regular message │
│ 2019-01-18 14:34:53 │ WARNING      │ The first warning message  │
└─────────────────────┴──────────────┴────────────────────────────┘
┌───────────timestamp─┬─message_type─┬─message───────────────────┐
│ 2019-01-18 14:23:43 │ REGULAR      │ The first regular message │
└─────────────────────┴──────────────┴───────────────────────────┘
```

مرتب سازی نتایج (صعودی با ترتیب به طور پیش فرض):

``` sql
SELECT * FROM stripe_log_table ORDER BY timestamp
```

``` text
┌───────────timestamp─┬─message_type─┬─message────────────────────┐
│ 2019-01-18 14:23:43 │ REGULAR      │ The first regular message  │
│ 2019-01-18 14:27:32 │ REGULAR      │ The second regular message │
│ 2019-01-18 14:34:53 │ WARNING      │ The first warning message  │
└─────────────────────┴──────────────┴────────────────────────────┘
```

[مقاله اصلی](https://clickhouse.tech/docs/en/operations/table_engines/stripelog/) <!--hide-->
