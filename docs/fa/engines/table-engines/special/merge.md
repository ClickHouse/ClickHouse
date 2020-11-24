---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 36
toc_title: "\u0627\u062F\u063A\u0627\u0645"
---

# ادغام {#merge}

این `Merge` موتور (با اشتباه گرفته شود `MergeTree`) اطلاعات خود را ذخیره نمی, اما اجازه می دهد تا خواندن از هر تعداد از جداول دیگر به طور همزمان.
خواندن به طور خودکار موازی. نوشتن به یک جدول پشتیبانی نمی شود. هنگام خواندن, شاخص جداول که در واقع در حال خواندن استفاده می شود, در صورتی که وجود داشته باشد.
این `Merge` موتور می پذیرد پارامترهای: نام پایگاه داده و یک عبارت منظم برای جداول.

مثال:

``` sql
Merge(hits, '^WatchLog')
```

داده خواهد شد از جداول در خواندن `hits` پایگاه داده است که نام هایی که مطابقت با عبارت منظم ‘`^WatchLog`’.

به جای نام پایگاه داده, شما می توانید یک عبارت ثابت است که یک رشته را برمی گرداند استفاده. به عنوان مثال, `currentDatabase()`.

Regular expressions — [شماره 2](https://github.com/google/re2) (پشتیبانی از یک زیر مجموعه از مدار چاپی), حساس به حروف.
یادداشت ها در مورد فرار نمادها در عبارات منظم در “match” بخش.

هنگام انتخاب جداول برای خواندن `Merge` جدول خود را انتخاب نخواهد شد, حتی اگر منطبق عبارت منظم. این است که برای جلوگیری از حلقه.
ممکن است که به ایجاد دو `Merge` جداول که بی وقفه سعی خواهد کرد به خواندن داده های هر یک از دیگران, اما این یک ایده خوب نیست.

راه معمولی برای استفاده از `Merge` موتور برای کار با تعداد زیادی از `TinyLog` جداول به عنوان اگر با یک جدول واحد.

مثال 2:

بیایید می گویند شما باید یک جدول (WatchLog_old) و تصمیم به تغییر پارتیشن بندی بدون حرکت داده ها به یک جدول جدید (WatchLog_new) و شما نیاز به مراجعه به داده ها از هر دو جدول.

``` sql
CREATE TABLE WatchLog_old(date Date, UserId Int64, EventType String, Cnt UInt64)
ENGINE=MergeTree(date, (UserId, EventType), 8192);
INSERT INTO WatchLog_old VALUES ('2018-01-01', 1, 'hit', 3);

CREATE TABLE WatchLog_new(date Date, UserId Int64, EventType String, Cnt UInt64)
ENGINE=MergeTree PARTITION BY date ORDER BY (UserId, EventType) SETTINGS index_granularity=8192;
INSERT INTO WatchLog_new VALUES ('2018-01-02', 2, 'hit', 3);

CREATE TABLE WatchLog as WatchLog_old ENGINE=Merge(currentDatabase(), '^WatchLog');

SELECT *
FROM WatchLog
```

``` text
┌───────date─┬─UserId─┬─EventType─┬─Cnt─┐
│ 2018-01-01 │      1 │ hit       │   3 │
└────────────┴────────┴───────────┴─────┘
┌───────date─┬─UserId─┬─EventType─┬─Cnt─┐
│ 2018-01-02 │      2 │ hit       │   3 │
└────────────┴────────┴───────────┴─────┘
```

## ستونهای مجازی {#virtual-columns}

-   `_table` — Contains the name of the table from which data was read. Type: [رشته](../../../sql-reference/data-types/string.md).

    شما می توانید شرایط ثابت را تنظیم کنید `_table` در `WHERE/PREWHERE` بند (به عنوان مثال, `WHERE _table='xyz'`). در این مورد عملیات خواندن فقط برای جداول انجام می شود که شرط است `_table` راضی است, به طوری که `_table` ستون به عنوان یک شاخص عمل می کند.

**همچنین نگاه کنید به**

-   [ستونهای مجازی](index.md#table_engines-virtual_columns)

[مقاله اصلی](https://clickhouse.tech/docs/en/operations/table_engines/merge/) <!--hide-->
