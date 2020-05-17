---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 35
toc_title: "\u0631\u06CC\u0632\u062F\u0627\u0646\u0647"
---

# ریزدانه {#aggregatingmergetree}

موتور به ارث می برد از [ادغام](mergetree.md#table_engines-mergetree), تغییر منطق برای ادغام قطعات داده. تاتر جایگزین تمام ردیف با کلید اصلی همان (یا با دقت بیشتر, با همان [کلید مرتب سازی](mergetree.md)) با یک ردیف (در یک بخش یک داده) که ترکیبی از ایالت های توابع کل را ذخیره می کند.

شما می توانید استفاده کنید `AggregatingMergeTree` جداول برای تجمع داده افزایشی, از جمله برای نمایش محقق جمع.

موتور تمام ستون ها را با انواع زیر پردازش می کند:

-   [کارکرد](../../../sql-reference/data-types/aggregatefunction.md)
-   [عملکرد پلاکتی](../../../sql-reference/data-types/simpleaggregatefunction.md)

مناسب برای استفاده است `AggregatingMergeTree` اگر تعداد ردیف ها را با دستور کاهش دهد.

## ایجاد یک جدول {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = AggregatingMergeTree()
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[TTL expr]
[SETTINGS name=value, ...]
```

برای شرح پارامترهای درخواست را ببینید [درخواست توضیحات](../../../sql-reference/statements/create.md).

**بندهای پرسوجو**

هنگام ایجاد یک `AggregatingMergeTree` جدول همان [بند](mergetree.md) در هنگام ایجاد یک مورد نیاز است `MergeTree` جدول

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
) ENGINE [=] AggregatingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity)
```

همه پارامترها همان معنی را دارند `MergeTree`.
</details>

## انتخاب و درج {#select-and-insert}

برای وارد کردن داده ها استفاده کنید [INSERT SELECT](../../../sql-reference/statements/insert-into.md) پرس و جو با کل دولت توابع.
هنگام انتخاب داده ها از `AggregatingMergeTree` جدول استفاده کنید `GROUP BY` بند و توابع کل همان هنگام قرار دادن داده, اما با استفاده از `-Merge` پسوند.

در نتایج `SELECT` پرس و جو, ارزش `AggregateFunction` نوع اجرای خاص نمایندگی دودویی برای همه فرمت های خروجی کلیک کنید. اگر کمپرسی داده ها به, مثلا, `TabSeparated` قالب با `SELECT` پرس و جو و سپس این روگرفت را می توان با استفاده از لود `INSERT` پرس و جو.

## به عنوان مثال از یک مشاهده محقق جمع {#example-of-an-aggregated-materialized-view}

`AggregatingMergeTree` مشاهده تحقق است که به تماشای `test.visits` جدول:

``` sql
CREATE MATERIALIZED VIEW test.basic
ENGINE = AggregatingMergeTree() PARTITION BY toYYYYMM(StartDate) ORDER BY (CounterID, StartDate)
AS SELECT
    CounterID,
    StartDate,
    sumState(Sign)    AS Visits,
    uniqState(UserID) AS Users
FROM test.visits
GROUP BY CounterID, StartDate;
```

درج داده به `test.visits` جدول

``` sql
INSERT INTO test.visits ...
```

داده ها در هر دو جدول و مشخصات قرار داده شده `test.basic` که تجمع انجام خواهد شد.

برای دریافت اطلاعات جمع, ما نیاز به اجرای یک پرس و جو مانند `SELECT ... GROUP BY ...` از نظر `test.basic`:

``` sql
SELECT
    StartDate,
    sumMerge(Visits) AS Visits,
    uniqMerge(Users) AS Users
FROM test.basic
GROUP BY StartDate
ORDER BY StartDate;
```

[مقاله اصلی](https://clickhouse.tech/docs/en/operations/table_engines/aggregatingmergetree/) <!--hide-->
