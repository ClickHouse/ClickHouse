---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 34
toc_title: "\u0633\u0627\u0645\u06CC\u0646\u06AF\u0645\u0631\u06AF\u062A\u0631\u06CC"
---

# سامینگمرگتری {#summingmergetree}

موتور به ارث می برد از [ادغام](mergetree.md#table_engines-mergetree). تفاوت در این است که هنگامی که ادغام قطعات داده برای `SummingMergeTree` جداول تاتر جایگزین تمام ردیف با کلید اصلی همان (یا با دقت بیشتر ,با همان [کلید مرتب سازی](mergetree.md)) با یک ردیف که حاوی مقادیر خلاصه شده برای ستون ها با نوع داده عددی است. اگر کلید مرتب سازی در راه است که یک مقدار کلید تنها مربوط به تعداد زیادی از ردیف تشکیل شده, این به طور قابل توجهی کاهش می دهد حجم ذخیره سازی و سرعت بخشیدن به انتخاب داده ها.

ما توصیه می کنیم به استفاده از موتور همراه با `MergeTree`. ذخیره اطلاعات کامل در `MergeTree` جدول و استفاده `SummingMergeTree` برای ذخیره سازی داده ها جمع, مثلا, هنگام تهیه گزارش. چنین رویکردی شما را از دست دادن اطلاعات با ارزش با توجه به کلید اولیه نادرست تشکیل شده جلوگیری می کند.

## ایجاد یک جدول {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = SummingMergeTree([columns])
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

برای شرح پارامترهای درخواست را ببینید [درخواست توضیحات](../../../sql-reference/statements/create.md).

**پارامترهای سامینگمرگتری**

-   `columns` - یک تاپل با نام ستون که ارزش خلاصه خواهد شد. پارامتر اختیاری.
    ستون باید از یک نوع عددی باشد و نباید در کلید اصلی باشد.

    اگر `columns` مشخص نشده, تاتر خلاصه مقادیر در تمام ستون ها با یک نوع داده عددی است که در کلید اصلی نیست.

**بندهای پرسوجو**

هنگام ایجاد یک `SummingMergeTree` جدول همان [بند](mergetree.md) در هنگام ایجاد یک مورد نیاز است `MergeTree` جدول

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
) ENGINE [=] SummingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, [columns])
```

همه پارامترها به استثنای `columns` همان معنی را در `MergeTree`.

-   `columns` — tuple with names of columns values of which will be summarized. Optional parameter. For a description, see the text above.

</details>

## مثال طریقه استفاده {#usage-example}

جدول زیر را در نظر بگیرید:

``` sql
CREATE TABLE summtt
(
    key UInt32,
    value UInt32
)
ENGINE = SummingMergeTree()
ORDER BY key
```

درج داده به این:

``` sql
INSERT INTO summtt Values(1,1),(1,2),(2,1)
```

تاتر ممکن است تمام ردیف نه به طور کامل جمع ([پایین را ببینید](#data-processing)), بنابراین ما با استفاده از یک تابع کلی `sum` و `GROUP BY` بند در پرس و جو.

``` sql
SELECT key, sum(value) FROM summtt GROUP BY key
```

``` text
┌─key─┬─sum(value)─┐
│   2 │          1 │
│   1 │          3 │
└─────┴────────────┘
```

## پردازش داده ها {#data-processing}

هنگامی که داده ها را به یک جدول قرار داده, ذخیره می شوند به عنوان است. خانه رعیتی ادغام بخش قرار داده شده از داده ها به صورت دوره ای و این زمانی است که ردیف با کلید اصلی همان خلاصه و جایگزین با یکی برای هر بخش حاصل از داده ها.

ClickHouse can merge the data parts so that different resulting parts of data cat consist rows with the same primary key, i.e. the summation will be incomplete. Therefore (`SELECT`) یک تابع جمع [جمع()](../../../sql-reference/aggregate-functions/reference.md#agg_function-sum) و `GROUP BY` بند باید در پرس و جو به عنوان مثال در بالا توضیح داده شده استفاده می شود.

### قوانین مشترک برای جمع {#common-rules-for-summation}

مقادیر در ستون با نوع داده عددی خلاصه شده است. مجموعه ای از ستون ها توسط پارامتر تعریف شده است `columns`.

اگر ارزش شد 0 در تمام ستون ها برای جمع, ردیف حذف شده است.

اگر ستون در کلید اصلی نیست و خلاصه نشده است, یک مقدار دلخواه از موجود انتخاب.

مقادیر برای ستون در کلید اصلی خلاصه نشده است.

### جمعبندی ستونها {#the-summation-in-the-aggregatefunction-columns}

برای ستون [نوع تابع](../../../sql-reference/data-types/aggregatefunction.md) عمل کلیک به عنوان [ریزدانه](aggregatingmergetree.md) جمع موتور با توجه به عملکرد.

### ساختارهای تو در تو {#nested-structures}

جدول می تواند ساختارهای داده تو در تو که در یک راه خاص پردازش کرده اند.

اگر نام یک جدول تو در تو با به پایان می رسد `Map` و این شامل حداقل دو ستون است که با معیارهای زیر مطابقت دارند:

-   ستون اول عددی است `(*Int*, Date, DateTime)` یا یک رشته `(String, FixedString)` بهش زنگ بزن `key`,
-   ستون های دیگر حساب `(*Int*, Float32/64)` بهش زنگ بزن `(values...)`,

سپس این جدول تو در تو به عنوان یک نقشه برداری از تفسیر `key => (values...)`, و هنگامی که ادغام ردیف خود, عناصر دو مجموعه داده ها با هم ادغام شدند `key` با جمع بندی مربوطه `(values...)`.

مثالها:

``` text
[(1, 100)] + [(2, 150)] -> [(1, 100), (2, 150)]
[(1, 100)] + [(1, 150)] -> [(1, 250)]
[(1, 100)] + [(1, 150), (2, 150)] -> [(1, 250), (2, 150)]
[(1, 100), (2, 150)] + [(1, -100)] -> [(2, 150)]
```

هنگام درخواست داده ها از [sumMap(key, value)](../../../sql-reference/aggregate-functions/reference.md) تابع برای تجمع `Map`.

برای ساختار داده های تو در تو, شما لازم نیست که برای مشخص ستون خود را در تاپل ستون برای جمع.

[مقاله اصلی](https://clickhouse.tech/docs/en/operations/table_engines/summingmergetree/) <!--hide-->
