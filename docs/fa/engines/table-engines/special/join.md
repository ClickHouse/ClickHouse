---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 40
toc_title: "\u067E\u06CC\u0648\u0633\u062A\u0646"
---

# پیوستن {#join}

ساختار داده تهیه شده برای استفاده در [JOIN](../../../sql-reference/statements/select/join.md#select-join) عملیات.

## ایجاد یک جدول {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [TTL expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2] [TTL expr2],
) ENGINE = Join(join_strictness, join_type, k1[, k2, ...])
```

شرح مفصلی از [CREATE TABLE](../../../sql-reference/statements/create.md#create-table-query) پرس و جو.

**پارامترهای موتور**

-   `join_strictness` – [پیوستن به سختی](../../../sql-reference/statements/select/join.md#select-join-types).
-   `join_type` – [پیوستن به نوع](../../../sql-reference/statements/select/join.md#select-join-types).
-   `k1[, k2, ...]` – Key columns from the `USING` بند که `JOIN` عملیات با ساخته شده.

وارد کردن `join_strictness` و `join_type` پارامترهای بدون نقل قول, مثلا, `Join(ANY, LEFT, col1)`. اونا باید با `JOIN` عملیاتی که جدول خواهد شد برای استفاده. اگر پارامترها مطابقت ندارند, خانه عروسکی می کند یک استثنا پرتاب نمی کند و ممکن است داده های نادرست بازگشت.

## استفاده از جدول {#table-usage}

### مثال {#example}

ایجاد جدول سمت چپ:

``` sql
CREATE TABLE id_val(`id` UInt32, `val` UInt32) ENGINE = TinyLog
```

``` sql
INSERT INTO id_val VALUES (1,11)(2,12)(3,13)
```

ایجاد سمت راست `Join` جدول:

``` sql
CREATE TABLE id_val_join(`id` UInt32, `val` UInt8) ENGINE = Join(ANY, LEFT, id)
```

``` sql
INSERT INTO id_val_join VALUES (1,21)(1,22)(3,23)
```

پیوستن به جداول:

``` sql
SELECT * FROM id_val ANY LEFT JOIN id_val_join USING (id) SETTINGS join_use_nulls = 1
```

``` text
┌─id─┬─val─┬─id_val_join.val─┐
│  1 │  11 │              21 │
│  2 │  12 │            ᴺᵁᴸᴸ │
│  3 │  13 │              23 │
└────┴─────┴─────────────────┘
```

به عنوان یک جایگزین, شما می توانید داده ها را از بازیابی `Join` جدول مشخص کردن مقدار پیوستن کلید:

``` sql
SELECT joinGet('id_val_join', 'val', toUInt32(1))
```

``` text
┌─joinGet('id_val_join', 'val', toUInt32(1))─┐
│                                         21 │
└────────────────────────────────────────────┘
```

### انتخاب و قرار دادن داده ها {#selecting-and-inserting-data}

شما می توانید استفاده کنید `INSERT` نمایش داده شد برای اضافه کردن داده ها به `Join`- جدول موتور . اگر جدول با ایجاد شد `ANY` سخت, داده ها برای کلید های تکراری نادیده گرفته می شوند. با `ALL` سخت, تمام ردیف اضافه می شوند.

شما نمی توانید انجام دهید `SELECT` پرس و جو به طور مستقیم از جدول. بجای, استفاده از یکی از روش های زیر:

-   میز را به سمت راست قرار دهید `JOIN` بند بند.
-   تماس با [جوینت](../../../sql-reference/functions/other-functions.md#joinget) تابع, که به شما امکان استخراج داده ها از جدول به همان شیوه به عنوان از یک فرهنگ لغت.

### محدودیت ها و تنظیمات {#join-limitations-and-settings}

هنگام ایجاد یک جدول تنظیمات زیر اعمال می شود:

-   [ارزشهای خبری عبارتند از:](../../../operations/settings/settings.md#join_use_nulls)
-   [\_پاک کردن \_روشن گرافیک](../../../operations/settings/query-complexity.md#settings-max_rows_in_join)
-   [\_پویش همیشگی](../../../operations/settings/query-complexity.md#settings-max_bytes_in_join)
-   [\_شروع مجدد](../../../operations/settings/query-complexity.md#settings-join_overflow_mode)
-   [نمایش سایت](../../../operations/settings/settings.md#settings-join_any_take_last_row)

این `Join`- جداول موتور نمی تواند مورد استفاده قرار گیرد `GLOBAL JOIN` عملیات.

این `Join`- موتور اجازه می دهد تا استفاده کنید [ارزشهای خبری عبارتند از:](../../../operations/settings/settings.md#join_use_nulls) تنظیم در `CREATE TABLE` بیانیه. و [SELECT](../../../sql-reference/statements/select/index.md) پرسوجو به کار میرود `join_use_nulls` منم همینطور اگر شما متفاوت است `join_use_nulls` تنظیمات, شما می توانید یک خطا پیوستن به جدول از. این بستگی به نوع پیوستن دارد. هنگام استفاده [جوینت](../../../sql-reference/functions/other-functions.md#joinget) تابع, شما مجبور به استفاده از همان `join_use_nulls` تنظیم در `CRATE TABLE` و `SELECT` اظهارات.

## ذخیره سازی داده ها {#data-storage}

`Join` داده های جدول است که همیشه در رم واقع. در هنگام قرار دادن ردیف به یک جدول, کلیکهاوس می نویسد بلوک های داده را به دایرکتوری بر روی دیسک به طوری که می توان ترمیم زمانی که سرور راه اندازی مجدد.

اگر سرور نادرست راه اندازی مجدد بلوک داده ها بر روی دیسک از دست رفته یا صدمه دیده ممکن است. در این مورد ممکن است لازم باشد فایل را به صورت دستی با داده های خراب شده حذف کنید.

[مقاله اصلی](https://clickhouse.tech/docs/en/operations/table_engines/join/) <!--hide-->
