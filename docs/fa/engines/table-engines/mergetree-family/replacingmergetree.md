---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 33
toc_title: "\u062C\u0627\u06CC\u06AF\u0632\u06CC\u0646\u06CC"
---

# جایگزینی {#replacingmergetree}

موتور متفاوت از [ادغام](mergetree.md#table_engines-mergetree) در که حذف نوشته های تکراری با همان مقدار اصلی کلید (یا دقیق تر, با همان [کلید مرتب سازی](mergetree.md) ارزش).

تقسیم داده ها تنها در یک ادغام رخ می دهد. ادغام در پس زمینه در زمان ناشناخته رخ می دهد بنابراین شما نمی توانید برنامه ریزی کنید. برخی از داده ها ممکن است بدون پردازش باقی می ماند. اگر چه شما می توانید ادغام برنامه ریزی با استفاده از اجرا `OPTIMIZE` پرس و جو, در استفاده از این حساب نمی, به این دلیل که `OPTIMIZE` پرس و جو خواندن و نوشتن مقدار زیادی از داده ها.

بدین ترتیب, `ReplacingMergeTree` مناسب برای پاک کردن داده های تکراری در پس زمینه برای صرفه جویی در فضا است اما عدم وجود تکراری را تضمین نمی کند.

## ایجاد یک جدول {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = ReplacingMergeTree([ver])
[PARTITION BY expr]
[ORDER BY expr]
[PRIMARY KEY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

برای شرح پارامترهای درخواست را ببینید [درخواست توضیحات](../../../sql-reference/statements/create.md).

**پارامترهای جایگزین**

-   `ver` — column with version. Type `UInt*`, `Date` یا `DateTime`. پارامتر اختیاری.

    هنگام ادغام, `ReplacingMergeTree` از تمام ردیف ها با همان کلید اصلی تنها یک برگ دارد:

    -   گذشته در انتخاب, اگر `ver` تنظیم نشده است.
    -   با حداکثر نسخه, اگر `ver` مشخص.

**بندهای پرسوجو**

هنگام ایجاد یک `ReplacingMergeTree` جدول همان [بند](mergetree.md) در هنگام ایجاد یک مورد نیاز است `MergeTree` جدول

<details markdown="1">

<summary>روش منسوخ برای ایجاد یک جدول</summary>

!!! attention "توجه"
    هنوز این روش در پروژه های جدید استفاده کنید و, در صورت امکان, تغییر پروژه های قدیمی به روش بالا توضیح.

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE [=] ReplacingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, [ver])
```

همه پارامترها به استثنای `ver` همان معنی را در `MergeTree`.

-   `ver` - ستون با نسخه . پارامتر اختیاری. برای شرح, متن بالا را ببینید.

</details>

[مقاله اصلی](https://clickhouse.tech/docs/en/operations/table_engines/replacingmergetree/) <!--hide-->
