---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 30
toc_title: "\u0627\u062F\u063A\u0627\u0645"
---

# ادغام {#table_engines-mergetree}

این `MergeTree` موتور و سایر موتورهای این خانواده (`*MergeTree`) موتورهای جدول کلیک قوی ترین.

موتور در `MergeTree` خانواده برای قرار دادن مقدار بسیار زیادی از داده ها را به یک جدول طراحی شده است. داده ها به سرعت به بخش جدول توسط بخش نوشته شده است, سپس قوانین برای ادغام قطعات در پس زمینه اعمال. این روش بسیار موثرتر از به طور مستمر بازنویسی داده ها در ذخیره سازی در درج است.

ویژگی های اصلی:

-   فروشگاه داده طبقه بندی شده اند توسط کلید اصلی.

    این اجازه می دهد تا به شما برای ایجاد یک شاخص پراکنده کوچک است که کمک می کند تا پیدا کردن اطلاعات سریع تر.

-   پارتیشن ها را می توان در صورت استفاده کرد [کلید پارتیشن بندی](custom-partitioning-key.md) مشخص شده است.

    تاتر پشتیبانی از عملیات خاص با پارتیشن که موثر تر از عملیات عمومی بر روی داده های مشابه با همان نتیجه. کلیک هاوس همچنین به طور خودکار داده های پارتیشن را که کلید پارتیشن بندی در پرس و جو مشخص شده است قطع می کند. این نیز باعث بهبود عملکرد پرس و جو.

-   پشتیبانی از تکرار داده ها.

    خانواده `ReplicatedMergeTree` جداول فراهم می کند تکرار داده ها. برای کسب اطلاعات بیشتر, دیدن [تکرار داده ها](replication.md).

-   پشتیبانی از نمونه برداری داده ها.

    در صورت لزوم می توانید روش نمونه گیری داده ها را در جدول تنظیم کنید.

!!! info "اطلاعات"
    این [ادغام](../special/merge.md#merge) موتور به تعلق ندارد `*MergeTree` خانواده

## ایجاد یک جدول {#table_engine-mergetree-creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [TTL expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2] [TTL expr2],
    ...
    INDEX index_name1 expr1 TYPE type1(...) GRANULARITY value1,
    INDEX index_name2 expr2 TYPE type2(...) GRANULARITY value2
) ENGINE = MergeTree()
[PARTITION BY expr]
[ORDER BY expr]
[PRIMARY KEY expr]
[SAMPLE BY expr]
[TTL expr [DELETE|TO DISK 'xxx'|TO VOLUME 'xxx'], ...]
[SETTINGS name=value, ...]
```

برای شرح پارامترها [ایجاد توصیف پرسوجو](../../../sql-reference/statements/create.md).

!!! note "یادداشت"
    `INDEX` یک ویژگی تجربی است [شاخص های داده پرش](#table_engine-mergetree-data_skipping-indexes).

### بندهای پرسوجو {#mergetree-query-clauses}

-   `ENGINE` — Name and parameters of the engine. `ENGINE = MergeTree()`. این `MergeTree` موتور پارامترهای ندارد.

-   `PARTITION BY` — The [کلید پارتیشن بندی](custom-partitioning-key.md).

    برای تقسیم ماه از `toYYYYMM(date_column)` عبارت, جایی که `date_column` یک ستون با تاریخ از نوع است [تاریخ](../../../sql-reference/data-types/date.md). نام پارتیشن در اینجا `"YYYYMM"` قالب.

-   `ORDER BY` — The sorting key.

    یک تاپل از ستون ها و یا عبارات دلخواه. مثال: `ORDER BY (CounterID, EventDate)`.

-   `PRIMARY KEY` — The primary key if it [متفاوت از کلید مرتب سازی](#choosing-a-primary-key-that-differs-from-the-sorting-key).

    به طور پیش فرض کلید اصلی همان کلید مرتب سازی است (که توسط مشخص شده است `ORDER BY` بند). بنابراین در اکثر موارد غیر ضروری است برای مشخص کردن یک جداگانه `PRIMARY KEY` بند بند.

-   `SAMPLE BY` — An expression for sampling.

    اگر یک عبارت نمونه برداری استفاده شده است, کلید اصلی باید باشد. مثال: `SAMPLE BY intHash32(UserID) ORDER BY (CounterID, EventDate, intHash32(UserID))`.

-   `TTL` — A list of rules specifying storage duration of rows and defining logic of automatic parts movement [بین دیسک و حجم](#table_engine-mergetree-multiple-volumes).

    بیان باید یکی داشته باشد `Date` یا `DateTime` ستون به عنوان یک نتیجه. مثال:
    `TTL date + INTERVAL 1 DAY`

    نوع قانون `DELETE|TO DISK 'xxx'|TO VOLUME 'xxx'` مشخص یک عمل با بخش انجام می شود در صورتی که بیان راضی است (می رسد زمان فعلی): حذف ردیف منقضی شده, در حال حرکت بخشی (اگر بیان برای تمام ردیف در یک بخش راضی است) به دیسک مشخص (`TO DISK 'xxx'`) یا به حجم (`TO VOLUME 'xxx'`). نوع پیش فرض قانون حذف است (`DELETE`). فهرست قوانین متعدد می توانید مشخص, اما باید بیش از یک وجود داشته باشد `DELETE` قانون.

    برای اطلاعات بیشتر, دیدن [ستون ها و جداول](#table_engine-mergetree-ttl)

-   `SETTINGS` — Additional parameters that control the behavior of the `MergeTree`:

    -   `index_granularity` — Maximum number of data rows between the marks of an index. Default value: 8192. See [ذخیره سازی داده ها](#mergetree-data-storage).
    -   `index_granularity_bytes` — Maximum size of data granules in bytes. Default value: 10Mb. To restrict the granule size only by number of rows, set to 0 (not recommended). See [ذخیره سازی داده ها](#mergetree-data-storage).
    -   `enable_mixed_granularity_parts` — Enables or disables transitioning to control the granule size with the `index_granularity_bytes` تنظیمات. قبل از نسخه 19.11, تنها وجود دارد `index_granularity` تنظیم برای محدود کردن اندازه گرانول. این `index_granularity_bytes` تنظیم را بهبود می بخشد عملکرد کلیک در هنگام انتخاب داده ها از جداول با ردیف بزرگ (ده ها و صدها مگابایت). اگر شما جداول با ردیف بزرگ, شما می توانید این تنظیمات را برای جداول را قادر به بهبود بهره وری از `SELECT` نمایش داده شد.
    -   `use_minimalistic_part_header_in_zookeeper` — Storage method of the data parts headers in ZooKeeper. If `use_minimalistic_part_header_in_zookeeper=1`, سپس باغ وحش ذخیره داده های کمتر. برای کسب اطلاعات بیشتر, دیدن [تنظیم توضیحات](../../../operations/server-configuration-parameters/settings.md#server-settings-use_minimalistic_part_header_in_zookeeper) داخل “Server configuration parameters”.
    -   `min_merge_bytes_to_use_direct_io` — The minimum data volume for merge operation that is required for using direct I/O access to the storage disk. When merging data parts, ClickHouse calculates the total storage volume of all the data to be merged. If the volume exceeds `min_merge_bytes_to_use_direct_io` بایت, تاتر می خواند و می نویسد داده ها به دیسک ذخیره سازی با استفاده از رابط من/ای مستقیم (`O_DIRECT` گزینه). اگر `min_merge_bytes_to_use_direct_io = 0`, سپس مستقیم من / ای غیر فعال است. مقدار پیشفرض: `10 * 1024 * 1024 * 1024` بایت
        <a name="mergetree_setting-merge_with_ttl_timeout"></a>
    -   `merge_with_ttl_timeout` — Minimum delay in seconds before repeating a merge with TTL. Default value: 86400 (1 day).
    -   `write_final_mark` — Enables or disables writing the final index mark at the end of data part (after the last byte). Default value: 1. Don't turn it off.
    -   `merge_max_block_size` — Maximum number of rows in block for merge operations. Default value: 8192.
    -   `storage_policy` — Storage policy. See [با استفاده از دستگاه های بلوک های متعدد برای ذخیره سازی داده ها](#table_engine-mergetree-multiple-volumes).

**مثال تنظیمات بخش**

``` sql
ENGINE MergeTree() PARTITION BY toYYYYMM(EventDate) ORDER BY (CounterID, EventDate, intHash32(UserID)) SAMPLE BY intHash32(UserID) SETTINGS index_granularity=8192
```

در مثال ما مجموعه پارتیشن بندی توسط ماه.

ما همچنین یک عبارت برای نمونه برداری به عنوان یک هش توسط شناسه کاربر تنظیم شده است. این اجازه می دهد تا شما را به نام مستعار داده ها در جدول برای هر `CounterID` و `EventDate`. اگر یک تعریف می کنید [SAMPLE](../../../sql-reference/statements/select/sample.md#select-sample-clause) بند هنگام انتخاب داده ClickHouse را یک به طور مساوی pseudorandom داده های نمونه به صورت زیر مجموعه ای از کاربران است.

این `index_granularity` تنظیم می تواند حذف شود زیرا 8192 مقدار پیش فرض است.

<details markdown="1">

<summary>روش منسوخ برای ایجاد یک جدول</summary>

!!! attention "توجه"
    از این روش در پروژه های جدید استفاده نکنید. در صورت امکان, تغییر پروژه های قدیمی به روش بالا توضیح.

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE [=] MergeTree(date-column [, sampling_expression], (primary, key), index_granularity)
```

**پارامترهای ادغام() **

-   `date-column` — The name of a column of the [تاریخ](../../../sql-reference/data-types/date.md) نوع. تاتر به طور خودکار ایجاد پارتیشن های ماه بر اساس این ستون. نام پارتیشن در `"YYYYMM"` قالب.
-   `sampling_expression` — An expression for sampling.
-   `(primary, key)` — Primary key. Type: [تاپل()](../../../sql-reference/data-types/tuple.md)
-   `index_granularity` — The granularity of an index. The number of data rows between the “marks” از یک شاخص. ارزش 8192 برای بسیاری از وظایف مناسب است.

**مثال**

``` sql
MergeTree(EventDate, intHash32(UserID), (CounterID, EventDate, intHash32(UserID)), 8192)
```

این `MergeTree` موتور در همان راه به عنوان مثال بالا برای روش پیکربندی موتور اصلی پیکربندی شده است.
</details>

## ذخیره سازی داده ها {#mergetree-data-storage}

جدول شامل قطعات داده مرتب شده بر اساس کلید اصلی.

هنگامی که داده ها در یک جدول قرار داده, قطعات داده های جداگانه ایجاد می شوند و هر یک از این واژه ها از لحاظ واژگان توسط کلید اصلی طبقه بندی شده اند. برای مثال اگر کلید اصلی است `(CounterID, Date)` داده ها در بخش طبقه بندی شده اند `CounterID` و در هر `CounterID`, این است که توسط دستور داد `Date`.

داده های متعلق به پارتیشن های مختلف به بخش های مختلف جدا می شوند. در پس زمینه, کلیکهاوس ادغام قطعات داده ها برای ذخیره سازی موثر تر. قطعات متعلق به پارتیشن های مختلف با هم ادغام شدند. مکانیزم ادغام تضمین نمی کند که تمام ردیف ها با همان کلید اصلی در بخش داده های مشابه باشد.

هر بخش داده منطقی به گرانول تقسیم شده است. گرانول کوچکترین مجموعه داده های تفکیک پذیر است که خانه می خواند در هنگام انتخاب داده ها است. خانه را کلیک می کند ردیف یا ارزش تقسیم نمی, بنابراین هر گرانول همیشه شامل یک عدد صحیح از ردیف. ردیف اول یک گرانول با ارزش کلید اصلی برای ردیف مشخص شده است. برای هر بخش داده, تاتر ایجاد یک فایل شاخص است که فروشگاه علامت. برای هر ستون, چه در کلید اصلی است یا نه, خانه رعیتی نیز علامت همان فروشگاه. این علامت به شما اجازه داده پیدا کردن به طور مستقیم در فایل های ستون.

اندازه گرانول توسط `index_granularity` و `index_granularity_bytes` تنظیمات موتور جدول. تعداد ردیف ها در یک گرانول در `[1, index_granularity]` محدوده, بسته به اندازه ردیف. اندازه گرانول می تواند بیش از `index_granularity_bytes` اگر اندازه یک ردیف بیشتر از ارزش تنظیم است. در این مورد, اندازه گرانول برابر اندازه ردیف.

## کلید های اولیه و شاخص ها در نمایش داده شد {#primary-keys-and-indexes-in-queries}

نگاهی به `(CounterID, Date)` کلید اصلی به عنوان مثال. در این مورد, مرتب سازی و شاخص را می توان به شرح زیر نشان داده شده:

      Whole data:     [---------------------------------------------]
      CounterID:      [aaaaaaaaaaaaaaaaaabbbbcdeeeeeeeeeeeeefgggggggghhhhhhhhhiiiiiiiiikllllllll]
      Date:           [1111111222222233331233211111222222333211111112122222223111112223311122333]
      Marks:           |      |      |      |      |      |      |      |      |      |      |
                      a,1    a,2    a,3    b,3    e,2    e,3    g,1    h,2    i,1    i,3    l,3
      Marks numbers:   0      1      2      3      4      5      6      7      8      9      10

اگر پرسوجوی داده مشخص شود:

-   `CounterID in ('a', 'h')`, سرور بار خوانده شده داده ها در محدوده علامت `[0, 3)` و `[6, 8)`.
-   `CounterID IN ('a', 'h') AND Date = 3`, سرور بار خوانده شده داده ها در محدوده علامت `[1, 3)` و `[7, 8)`.
-   `Date = 3`, سرور می خواند داده ها در طیف وسیعی از علامت `[1, 10]`.

نمونه های فوق نشان می دهد که همیشه بیشتر موثر برای استفاده از شاخص از اسکن کامل است.

شاخص پراکنده اجازه می دهد تا داده های اضافی به عنوان خوانده شده. هنگام خواندن یک طیف وسیعی از کلید اصلی, تا `index_granularity * 2` ردیف اضافی در هر بلوک داده را می توان به عنوان خوانده شده.

شاخص پراکنده اجازه می دهد شما را به کار با تعداد بسیار زیادی از ردیف جدول, چرا که در اکثر موارد, چنین شاخص در رم کامپیوتر مناسب.

کلیک یک کلید اصلی منحصر به فرد نیاز ندارد. شما می توانید ردیف های متعدد را با همان کلید اولیه وارد کنید.

### انتخاب کلید اصلی {#selecting-the-primary-key}

تعداد ستون ها در کلید اصلی به صراحت محدود نمی شود. بسته به ساختار داده ها, شما می توانید ستون های بیشتر یا کمتر در کلید اصلی شامل. این ممکن است:

-   بهبود عملکرد یک شاخص.

    اگر کلید اصلی است `(a, b)` سپس یک ستون دیگر اضافه کنید `c` عملکرد را بهبود می بخشد اگر شرایط زیر رعایت شود:

    -   نمایش داده شد با یک شرط در ستون وجود دارد `c`.
    -   محدوده داده های طولانی (چندین بار طولانی تر از `index_granularity`) با مقادیر یکسان برای `(a, b)` شایع هستند. به عبارت دیگر, در هنگام اضافه کردن یک ستون دیگر اجازه می دهد تا شما را به جست و خیز محدوده داده بسیار طولانی.

-   بهبود فشرده سازی داده ها.

    خانه را کلیک کنید انواع داده ها توسط کلید اصلی, بنابراین بالاتر از ثبات, بهتر فشرده سازی.

-   فراهم می کند که منطق اضافی در هنگام ادغام قطعات داده در [سقوط غذای اصلی](collapsingmergetree.md#table_engine-collapsingmergetree) و [سامینگمرگتری](summingmergetree.md) موتورها.

    در این مورد منطقی است که مشخص شود *کلید مرتب سازی* که متفاوت از کلید اصلی است.

یک کلید اولیه طولانی منفی عملکرد درج و مصرف حافظه تاثیر می گذارد, اما ستون های اضافی در کلید اصلی انجام عملکرد تاتر در طول تاثیر نمی گذارد `SELECT` نمایش داده شد.

### انتخاب کلید اصلی است که متفاوت از کلید مرتب سازی {#choosing-a-primary-key-that-differs-from-the-sorting-key}

ممکن است که به مشخص کردن یک کلید اولیه (بیان با ارزش هایی که در فایل شاخص برای هر علامت نوشته شده است) که متفاوت از کلید مرتب سازی (بیان برای مرتب سازی ردیف در بخش های داده). در این مورد تاپل عبارت کلیدی اولیه باید یک پیشوند از تاپل عبارت کلیدی مرتب سازی شود.

این ویژگی در هنگام استفاده از مفید است [سامینگمرگتری](summingmergetree.md) و
[ریزدانه](aggregatingmergetree.md) موتورهای جدول. در یک مورد مشترک در هنگام استفاده از این موتور جدول دو نوع ستون است: *ابعاد* و *اقدامات*. نمایش داده شد نمونه مقادیر کل ستون اندازه گیری با دلخواه `GROUP BY` و فیلتر بر اساس ابعاد. چون SummingMergeTree و AggregatingMergeTree جمع ردیف با همان مقدار از مرتب سازی کلیدی است برای اضافه کردن همه ابعاد آن است. در نتیجه, بیان کلیدی شامل یک لیست طولانی از ستون ها و این لیست باید اغلب با ابعاد تازه اضافه شده به روز.

در این مورد منطقی است که تنها چند ستون در کلید اصلی را ترک کنید که اسکن های محدوده ای موثر را فراهم می کند و ستون های بعد باقی مانده را به دسته کلید مرتب سازی اضافه می کند.

[ALTER](../../../sql-reference/statements/alter.md) از کلید مرتب سازی یک عملیات سبک وزن است چرا که زمانی که یک ستون جدید به طور همزمان به جدول و به کلید مرتب سازی اضافه, قطعات داده های موجود لازم نیست به تغییر. از کلید مرتب سازی قدیمی یک پیشوند از کلید مرتب سازی جدید است و هیچ داده در ستون به تازگی اضافه شده وجود دارد, داده ها توسط هر دو کلید مرتب سازی قدیمی و جدید در لحظه اصلاح جدول طبقه بندی شده اند.

### استفاده از شاخص ها و پارتیشن ها در نمایش داده شد {#use-of-indexes-and-partitions-in-queries}

برای `SELECT` نمایش داده شد, فاحشه خانه تجزیه و تحلیل اینکه یک شاخص می تواند مورد استفاده قرار گیرد. شاخص می تواند مورد استفاده قرار گیرد در صورتی که `WHERE/PREWHERE` بند بیان (به عنوان یکی از عناصر رابطه یا به طور کامل) است که نشان دهنده برابری یا نابرابری عملیات مقایسه و یا اگر `IN` یا `LIKE` با یک پیشوند ثابت در ستون ها و یا عبارات که در کلید اصلی و یا پارتیشن بندی هستند, و یا در برخی از توابع تا حدی تکراری از این ستون ها, و یا روابط منطقی از این عبارات.

بدین ترتیب, ممکن است به سرعت اجرا نمایش داده شد در یک یا بسیاری از محدوده کلید اصلی. در این مثال, نمایش داده شد سریع خواهد بود که برای یک تگ ردیابی خاص اجرا, برای یک برچسب خاص و محدوده تاریخ, برای یک تگ و تاریخ خاص, برای برچسب های متعدد با محدوده تاریخ, و غیره.

بیایید به موتور پیکربندی شده به شرح زیر نگاه کنیم:

      ENGINE MergeTree() PARTITION BY toYYYYMM(EventDate) ORDER BY (CounterID, EventDate) SETTINGS index_granularity=8192

در این مورد در نمایش داده شد:

``` sql
SELECT count() FROM table WHERE EventDate = toDate(now()) AND CounterID = 34
SELECT count() FROM table WHERE EventDate = toDate(now()) AND (CounterID = 34 OR CounterID = 42)
SELECT count() FROM table WHERE ((EventDate >= toDate('2014-01-01') AND EventDate <= toDate('2014-01-31')) OR EventDate = toDate('2014-05-01')) AND CounterID IN (101500, 731962, 160656) AND (CounterID = 101500 OR EventDate != toDate('2014-05-01'))
```

خانه رعیتی خواهد شاخص کلید اصلی به تر و تمیز داده های نامناسب و کلید پارتیشن بندی ماهانه به تر و تمیز پارتیشن که در محدوده تاریخ نامناسب هستند استفاده کنید.

نمایش داده شد بالا نشان می دهد که شاخص حتی برای عبارات پیچیده استفاده می شود. خواندن از جدول سازمان یافته است به طوری که با استفاده از شاخص نمی تواند کندتر از اسکن کامل.

در مثال زیر شاخص نمی تواند مورد استفاده قرار گیرد.

``` sql
SELECT count() FROM table WHERE CounterID = 34 OR URL LIKE '%upyachka%'
```

برای بررسی اینکه تاتر می توانید شاخص زمانی که در حال اجرا یک پرس و جو استفاده, استفاده از تنظیمات [اجبار](../../../operations/settings/settings.md#settings-force_index_by_date) و [اجبار](../../../operations/settings/settings.md).

کلید پارتیشن بندی توسط ماه اجازه می دهد تا خواندن تنها کسانی که بلوک های داده که حاوی تاریخ از محدوده مناسب. در این مورد, بلوک داده ها ممکن است حاوی داده ها برای بسیاری از تاریخ (تا یک ماه کامل). در یک بلوک, داده ها توسط کلید اصلی طبقه بندی شده اند, که ممکن است حاوی تاریخ به عنوان ستون اول نیست. به خاطر همین, با استفاده از یک پرس و جو تنها با یک وضعیت تاریخ که پیشوند کلید اصلی مشخص نیست باعث می شود اطلاعات بیشتر از یک تاریخ به عنوان خوانده شود.

### استفاده از شاخص برای کلید های اولیه تا حدی یکنواخت {#use-of-index-for-partially-monotonic-primary-keys}

در نظر بگیرید, مثلا, روز از ماه. یک فرم [توالی یکنواختی](https://en.wikipedia.org/wiki/Monotonic_function) برای یک ماه, اما برای مدت طولانی تر یکنواخت نیست. این یک توالی نیمه یکنواخت است. اگر یک کاربر ایجاد جدول با نیمه یکنواخت کلید اولیه, خانه را ایجاد یک شاخص پراکنده به طور معمول. هنگامی که یک کاربر داده ها را انتخاب از این نوع از جدول, تاتر تجزیه و تحلیل شرایط پرس و جو. اگر کاربر می خواهد برای دریافت اطلاعات بین دو علامت از شاخص و هر دو این علامت در عرض یک ماه سقوط, خانه رعیتی می توانید شاخص در این مورد خاص استفاده کنید زیرا می تواند فاصله بین پارامترهای یک پرس و جو و شاخص محاسبه.

کلیک خانه می تواند یک شاخص استفاده کنید اگر ارزش های کلید اصلی در محدوده پارامتر پرس و جو یک توالی یکنواخت نشان دهنده نیست. در این مورد, تاتر با استفاده از روش اسکن کامل.

تاتر با استفاده از این منطق نه تنها برای روز از توالی ماه, اما برای هر کلید اصلی است که نشان دهنده یک توالی نیمه یکنواخت.

### شاخص های پرش داده (تجربی) {#table_engine-mergetree-data_skipping-indexes}

اعلامیه شاخص در بخش ستون ها از `CREATE` پرس و جو.

``` sql
INDEX index_name expr TYPE type(...) GRANULARITY granularity_value
```

برای جداول از `*MergeTree` خانواده, شاخص پرش داده را می توان مشخص.

این شاخص جمع برخی از اطلاعات در مورد بیان مشخص شده بر روی بلوک, که شامل `granularity_value` گرانول (اندازه گرانول با استفاده از `index_granularity` تنظیم در موتور جدول). سپس این دانه ها در استفاده می شود `SELECT` نمایش داده شد برای کاهش مقدار داده ها به خواندن از روی دیسک با پرش بلوک های بزرگ از داده ها که `where` پرس و جو نمی تواند راضی باشد.

**مثال**

``` sql
CREATE TABLE table_name
(
    u64 UInt64,
    i32 Int32,
    s String,
    ...
    INDEX a (u64 * i32, s) TYPE minmax GRANULARITY 3,
    INDEX b (u64 * length(s)) TYPE set(1000) GRANULARITY 4
) ENGINE = MergeTree()
...
```

شاخص ها از مثال می توانند توسط کلیک خانه استفاده شوند تا میزان داده ها را برای خواندن از دیسک در موارد زیر کاهش دهند:

``` sql
SELECT count() FROM table WHERE s < 'z'
SELECT count() FROM table WHERE u64 * i32 == 10 AND u64 * length(s) >= 1234
```

#### انواع شاخص های موجود {#available-types-of-indices}

-   `minmax`

    فروشگاه افراط و بیان مشخص شده (در صورتی که بیان شده است `tuple` سپس افراط را برای هر عنصر ذخیره می کند `tuple`), با استفاده از اطلاعات ذخیره شده برای پرش بلوک از داده ها مانند کلید اصلی.

-   `set(max_rows)`

    ارزش های منحصر به فرد بیان مشخص شده را ذخیره می کند (بیش از `max_rows` سطرها, `max_rows=0` یعنی “no limits”). با استفاده از مقادیر برای بررسی در صورتی که `WHERE` بیان رضایت بخش در یک بلوک از داده ها نیست.

-   `ngrambf_v1(n, size_of_bloom_filter_in_bytes, number_of_hash_functions, random_seed)`

    فروشگاه ها [فیلتر بلوم](https://en.wikipedia.org/wiki/Bloom_filter) که شامل تمام نمگرام از یک بلوک از داده ها. این نسخهها کار میکند تنها با رشته. می توان برای بهینه سازی استفاده کرد `equals`, `like` و `in` عبارات.

    -   `n` — ngram size,
    -   `size_of_bloom_filter_in_bytes` — Bloom filter size in bytes (you can use large values here, for example, 256 or 512, because it can be compressed well).
    -   `number_of_hash_functions` — The number of hash functions used in the Bloom filter.
    -   `random_seed` — The seed for Bloom filter hash functions.

-   `tokenbf_v1(size_of_bloom_filter_in_bytes, number_of_hash_functions, random_seed)`

    همان `ngrambf_v1`, اما فروشگاه نشانه به جای نمرگرام. نشانه ها توالی هایی هستند که توسط شخصیت های غیر عددی جدا شده اند.

-   `bloom_filter([false_positive])` — Stores a [فیلتر بلوم](https://en.wikipedia.org/wiki/Bloom_filter) برای ستون مشخص.

    اختیاری `false_positive` پارامتر احتمال دریافت پاسخ مثبت کاذب از فیلتر است. مقادیر ممکن: (0, 1). مقدار پیش فرض: 0.025.

    انواع داده های پشتیبانی شده: `Int*`, `UInt*`, `Float*`, `Enum`, `Date`, `DateTime`, `String`, `FixedString`, `Array`, `LowCardinality`, `Nullable`.

    توابع زیر می توانند از این استفاده کنند: [برابر](../../../sql-reference/functions/comparison-functions.md), [نقلقولها](../../../sql-reference/functions/comparison-functions.md), [داخل](../../../sql-reference/functions/in-functions.md), [notIn](../../../sql-reference/functions/in-functions.md), [دارد](../../../sql-reference/functions/array-functions.md).

<!-- -->

``` sql
INDEX sample_index (u64 * length(s)) TYPE minmax GRANULARITY 4
INDEX sample_index2 (u64 * length(str), i32 + f64 * 100, date, str) TYPE set(100) GRANULARITY 4
INDEX sample_index3 (lower(str), str) TYPE ngrambf_v1(3, 256, 2, 0) GRANULARITY 4
```

#### توابع پشتیبانی {#functions-support}

شرایط در `WHERE` بند شامل تماس از توابع است که با ستون کار. اگر ستون بخشی از یک شاخص است, خانه رعیتی تلاش می کند تا استفاده از این شاخص در هنگام انجام توابع. تاتر از زیر مجموعه های مختلف از توابع برای استفاده از شاخص.

این `set` شاخص را می توان با تمام توابع استفاده می شود. زیر مجموعه های تابع برای شاخص های دیگر در جدول زیر نشان داده شده است.

| تابع (اپراتور) / شاخص                                                                                                | کلید اصلی | مینمکس | نمرمبف1 | توکنبف1 | ت_ضعیت |
|----------------------------------------------------------------------------------------------------------------------|-----------|--------|---------|---------|---------|
| [اطلاعات دقیق)](../../../sql-reference/functions/comparison-functions.md#function-equals)                            | ✔         | ✔      | ✔       | ✔       | ✔       |
| [نقلقولهای جدید از این نویسنده=, \<\>)](../../../sql-reference/functions/comparison-functions.md#function-notequals) | ✔         | ✔      | ✔       | ✔       | ✔       |
| [مانند](../../../sql-reference/functions/string-search-functions.md#function-like)                                   | ✔         | ✔      | ✔       | ✗       | ✗       |
| [notLike](../../../sql-reference/functions/string-search-functions.md#function-notlike)                              | ✔         | ✔      | ✔       | ✗       | ✗       |
| [startsWith](../../../sql-reference/functions/string-functions.md#startswith)                                        | ✔         | ✔      | ✔       | ✔       | ✗       |
| [endsWith](../../../sql-reference/functions/string-functions.md#endswith)                                            | ✗         | ✗      | ✔       | ✔       | ✗       |
| [چندزبانه](../../../sql-reference/functions/string-search-functions.md#function-multisearchany)                      | ✗         | ✗      | ✔       | ✗       | ✗       |
| [داخل](../../../sql-reference/functions/in-functions.md#in-functions)                                                | ✔         | ✔      | ✔       | ✔       | ✔       |
| [notIn](../../../sql-reference/functions/in-functions.md#in-functions)                                               | ✔         | ✔      | ✔       | ✔       | ✔       |
| [کمتر (\<)](../../../sql-reference/functions/comparison-functions.md#function-less)                                  | ✔         | ✔      | ✗       | ✗       | ✗       |
| [بیشتر (\>)](../../../sql-reference/functions/comparison-functions.md#function-greater)                              | ✔         | ✔      | ✗       | ✗       | ✗       |
| [در حال بارگذاری)](../../../sql-reference/functions/comparison-functions.md#function-lessorequals)                   | ✔         | ✔      | ✗       | ✗       | ✗       |
| [اطلاعات دقیق)](../../../sql-reference/functions/comparison-functions.md#function-greaterorequals)                   | ✔         | ✔      | ✗       | ✗       | ✗       |
| [خالی](../../../sql-reference/functions/array-functions.md#function-empty)                                           | ✔         | ✔      | ✗       | ✗       | ✗       |
| [notEmpty](../../../sql-reference/functions/array-functions.md#function-notempty)                                    | ✔         | ✔      | ✗       | ✗       | ✗       |
| شتابدهنده                                                                                                            | ✗         | ✗      | ✗       | ✔       | ✗       |

توابع با استدلال ثابت است که کمتر از اندازه نیگرام می تواند توسط استفاده نمی شود `ngrambf_v1` برای بهینه سازی پرس و جو.

فیلتر بلوم می توانید مسابقات مثبت کاذب دارند, به طوری که `ngrambf_v1`, `tokenbf_v1` و `bloom_filter` شاخص ها نمی توانند برای بهینه سازی پرس و جو هایی که انتظار می رود نتیجه عملکرد نادرست باشد استفاده شوند:

-   می توان بهینه سازی کرد:
    -   `s LIKE '%test%'`
    -   `NOT s NOT LIKE '%test%'`
    -   `s = 1`
    -   `NOT s != 1`
    -   `startsWith(s, 'test')`
-   نمی توان بهینه سازی کرد:
    -   `NOT s LIKE '%test%'`
    -   `s NOT LIKE '%test%'`
    -   `NOT s = 1`
    -   `s != 1`
    -   `NOT startsWith(s, 'test')`

## دسترسی همزمان داده ها {#concurrent-data-access}

برای دسترسی به جدول همزمان, ما با استفاده از چند نسخه. به عبارت دیگر, زمانی که یک جدول به طور همزمان خواندن و به روز, داده ها از مجموعه ای از قطعات است که در زمان پرس و جو در حال حاضر به عنوان خوانده شده. هیچ قفل طولانی وجود دارد. درج در راه عملیات خواندن نیست.

خواندن از یک جدول به طور خودکار موازی.

## ستون ها و جداول {#table_engine-mergetree-ttl}

تعیین طول عمر ارزش.

این `TTL` بند را می توان برای کل جدول و برای هر ستون فردی تنظیم شده است. همچنین منطق حرکت خودکار داده ها بین دیسک ها و حجم ها را مشخص می کند.

عبارات باید به ارزیابی [تاریخ](../../../sql-reference/data-types/date.md) یا [DateTime](../../../sql-reference/data-types/datetime.md) نوع داده.

مثال:

``` sql
TTL time_column
TTL time_column + interval
```

برای تعریف `interval` استفاده [فاصله زمانی](../../../sql-reference/operators/index.md#operators-datetime) اپراتورها.

``` sql
TTL date_time + INTERVAL 1 MONTH
TTL date_time + INTERVAL 15 HOUR
```

### ستون {#mergetree-column-ttl}

هنگامی که مقادیر در ستون منقضی, خانه را جایگزین با مقادیر پیش فرض برای نوع داده ستون. اگر تمام مقادیر ستون در بخش داده منقضی, تاتر حذف این ستون از بخش داده ها در یک سیستم فایل.

این `TTL` بند را نمی توان برای ستون های کلیدی استفاده کرد.

مثالها:

ایجاد یک جدول با تی ال

``` sql
CREATE TABLE example_table
(
    d DateTime,
    a Int TTL d + INTERVAL 1 MONTH,
    b Int TTL d + INTERVAL 1 MONTH,
    c String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(d)
ORDER BY d;
```

اضافه کردن تی ال به یک ستون از یک جدول موجود

``` sql
ALTER TABLE example_table
    MODIFY COLUMN
    c String TTL d + INTERVAL 1 DAY;
```

تغییر تعداد ستون

``` sql
ALTER TABLE example_table
    MODIFY COLUMN
    c String TTL d + INTERVAL 1 MONTH;
```

### جدول {#mergetree-table-ttl}

جدول می تواند بیان برای حذف ردیف منقضی شده و عبارات متعدد برای حرکت خودکار قطعات بین [دیسک یا حجم](#table_engine-mergetree-multiple-volumes). هنگامی که ردیف در جدول منقضی, تاتر حذف تمام ردیف مربوطه. برای قطعات در حال حرکت از ویژگی های, تمام ردیف از یک بخش باید معیارهای بیان جنبش را تامین کند.

``` sql
TTL expr [DELETE|TO DISK 'aaa'|TO VOLUME 'bbb'], ...
```

نوع قانون کنترل هوشمند ممکن است هر عبارت را دنبال کند. این تاثیر می گذارد یک عمل است که باید انجام شود یک بار بیان راضی است (زمان فعلی می رسد):

-   `DELETE` - حذف ردیف منقضی شده (اقدام پیش فرض);
-   `TO DISK 'aaa'` - انتقال بخشی به دیسک `aaa`;
-   `TO VOLUME 'bbb'` - انتقال بخشی به دیسک `bbb`.

مثالها:

ایجاد یک جدول با تی ال

``` sql
CREATE TABLE example_table
(
    d DateTime,
    a Int
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(d)
ORDER BY d
TTL d + INTERVAL 1 MONTH [DELETE],
    d + INTERVAL 1 WEEK TO VOLUME 'aaa',
    d + INTERVAL 2 WEEK TO DISK 'bbb';
```

تغییر تعداد جدول

``` sql
ALTER TABLE example_table
    MODIFY TTL d + INTERVAL 1 DAY;
```

**حذف داده ها**

داده ها با یک حذف شده است زمانی که محل انتخابی ادغام قطعات داده.

هنگامی که کلیک خانه را ببینید که داده تمام شده است, انجام یک ادغام خارج از برنامه. برای کنترل فرکانس چنین ادغام, شما می توانید مجموعه `merge_with_ttl_timeout`. اگر مقدار خیلی کم است, این بسیاری از ادغام خارج از برنامه است که ممکن است مقدار زیادی از منابع مصرف انجام.

اگر شما انجام `SELECT` پرس و جو بین ادغام, شما ممکن است داده های منقضی شده. برای جلوگیری از استفاده از [OPTIMIZE](../../../sql-reference/statements/misc.md#misc_operations-optimize) پرسوجو قبل از `SELECT`.

## با استفاده از دستگاه های بلوک های متعدد برای ذخیره سازی داده ها {#table_engine-mergetree-multiple-volumes}

### معرفی شرکت {#introduction}

`MergeTree` موتورهای جدول خانواده می تواند داده ها در دستگاه های بلوک های متعدد ذخیره کنید. مثلا, این می تواند مفید باشد زمانی که داده ها از یک جدول خاص به طور ضمنی به تقسیم “hot” و “cold”. داده های اخیر به طور منظم درخواست شده است اما نیاز به تنها مقدار کمی از فضای. برعکس, داده های تاریخی چربی دم به ندرت درخواست. اگر چندین دیسک در دسترس هستند “hot” داده ها ممکن است بر روی دیسک های سریع واقع (مثلا, اس اس اس اس بلوم و یا در حافظه), در حالی که “cold” داده ها بر روی موارد نسبتا کند (مثلا هارد).

بخش داده ها حداقل واحد متحرک برای `MergeTree`- جدول موتور . داده های متعلق به یک بخش بر روی یک دیسک ذخیره می شود. قطعات داده را می توان بین دیسک در پس زمینه (با توجه به تنظیمات کاربر) و همچنین با استفاده از نقل مکان کرد [ALTER](../../../sql-reference/statements/alter.md#alter_move-partition) نمایش داده شد.

### شرایط {#terms}

-   Disk — Block device mounted to the filesystem.
-   Default disk — Disk that stores the path specified in the [مسیر](../../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-path) تنظیم سرور.
-   Volume — Ordered set of equal disks (similar to [JBOD](https://en.wikipedia.org/wiki/Non-RAID_drive_architectures)).
-   Storage policy — Set of volumes and the rules for moving data between them.

اسامی داده شده به اشخاص توصیف شده را می توان در جداول سیستم یافت می شود, [سیستم.داستان_یابی](../../../operations/system-tables.md#system_tables-storage_policies) و [سیستم.دیسکها](../../../operations/system-tables.md#system_tables-disks). برای اعمال یکی از سیاست های ذخیره سازی پیکربندی شده برای یک جدول از `storage_policy` تنظیم از `MergeTree`- جداول خانواده موتور .

### پیکربندی {#table_engine-mergetree-multiple-volumes_configure}

دیسک, حجم و سیاست های ذخیره سازی باید در داخل اعلام `<storage_configuration>` برچسب یا در فایل اصلی `config.xml` یا در یک فایل مجزا در `config.d` فهرست راهنما.

ساختار پیکربندی:

``` xml
<storage_configuration>
    <disks>
        <disk_name_1> <!-- disk name -->
            <path>/mnt/fast_ssd/clickhouse/</path>
        </disk_name_1>
        <disk_name_2>
            <path>/mnt/hdd1/clickhouse/</path>
            <keep_free_space_bytes>10485760</keep_free_space_bytes>
        </disk_name_2>
        <disk_name_3>
            <path>/mnt/hdd2/clickhouse/</path>
            <keep_free_space_bytes>10485760</keep_free_space_bytes>
        </disk_name_3>

        ...
    </disks>

    ...
</storage_configuration>
```

برچسبها:

-   `<disk_name_N>` — Disk name. Names must be different for all disks.
-   `path` — path under which a server will store data (`data` و `shadow` پوشه ها) باید با پایان ‘/’.
-   `keep_free_space_bytes` — the amount of free disk space to be reserved.

منظور از تعریف دیسک مهم نیست.

نشانه گذاری پیکربندی سیاست های ذخیره سازی:

``` xml
<storage_configuration>
    ...
    <policies>
        <policy_name_1>
            <volumes>
                <volume_name_1>
                    <disk>disk_name_from_disks_configuration</disk>
                    <max_data_part_size_bytes>1073741824</max_data_part_size_bytes>
                </volume_name_1>
                <volume_name_2>
                    <!-- configuration -->
                </volume_name_2>
                <!-- more volumes -->
            </volumes>
            <move_factor>0.2</move_factor>
        </policy_name_1>
        <policy_name_2>
            <!-- configuration -->
        </policy_name_2>

        <!-- more policies -->
    </policies>
    ...
</storage_configuration>
```

برچسبها:

-   `policy_name_N` — Policy name. Policy names must be unique.
-   `volume_name_N` — Volume name. Volume names must be unique.
-   `disk` — a disk within a volume.
-   `max_data_part_size_bytes` — the maximum size of a part that can be stored on any of the volume's disks.
-   `move_factor` — when the amount of available space gets lower than this factor, data automatically start to move on the next volume if any (by default, 0.1).

Cofiguration نمونه:

``` xml
<storage_configuration>
    ...
    <policies>
        <hdd_in_order> <!-- policy name -->
            <volumes>
                <single> <!-- volume name -->
                    <disk>disk1</disk>
                    <disk>disk2</disk>
                </single>
            </volumes>
        </hdd_in_order>

        <moving_from_ssd_to_hdd>
            <volumes>
                <hot>
                    <disk>fast_ssd</disk>
                    <max_data_part_size_bytes>1073741824</max_data_part_size_bytes>
                </hot>
                <cold>
                    <disk>disk1</disk>
                </cold>
            </volumes>
            <move_factor>0.2</move_factor>
        </moving_from_ssd_to_hdd>
    </policies>
    ...
</storage_configuration>
```

در مثال داده شده `hdd_in_order` سیاست پیاده سازی [گرد رابین](https://en.wikipedia.org/wiki/Round-robin_scheduling) نزدیک شو بنابراین این سیاست تنها یک جلد را تعریف می کند (`single`), قطعات داده ها بر روی تمام دیسک های خود را به ترتیب دایره ای ذخیره می شود. چنین سیاستی می تواند بسیار مفید اگر چندین دیسک مشابه به سیستم نصب شده وجود دارد, اما حمله پیکربندی نشده است. به خاطر داشته باشید که هر درایو دیسک منحصر به فرد قابل اعتماد نیست و شما ممکن است بخواهید با عامل تکرار 3 یا بیشتر جبران.

اگر انواع مختلف دیسک های موجود در سیستم وجود دارد, `moving_from_ssd_to_hdd` سیاست را می توان به جای استفاده. حجم `hot` شامل یک دیسک اس اس دی (`fast_ssd`), و حداکثر اندازه یک بخش است که می تواند در این حجم ذخیره شده است 1گیگابایت. تمام قطعات با اندازه بزرگتر از 1 گیگابایت به طور مستقیم در `cold` حجم, که شامل یک دیسک هارد `disk1`.
همچنین هنگامی که دیسک `fast_ssd` می شود توسط بیش از پر 80%, داده خواهد شد به انتقال `disk1` توسط یک فرایند پس زمینه.

منظور شمارش حجم در یک سیاست ذخیره سازی مهم است. هنگامی که یک حجم پر شده است, داده ها به یک بعدی منتقل. ترتیب شمارش دیسک نیز مهم است زیرا داده ها در نوبت ذخیره می شوند.

هنگام ایجاد یک جدول می توان یکی از سیاست های ذخیره سازی پیکربندی شده را اعمال کرد:

``` sql
CREATE TABLE table_with_non_default_policy (
    EventDate Date,
    OrderID UInt64,
    BannerID UInt64,
    SearchPhrase String
) ENGINE = MergeTree
ORDER BY (OrderID, BannerID)
PARTITION BY toYYYYMM(EventDate)
SETTINGS storage_policy = 'moving_from_ssd_to_hdd'
```

این `default` سیاست ذخیره سازی نشان میدهد تنها با استفاده از یک حجم, که متشکل از تنها یک دیسک داده شده در `<path>`. هنگامی که یک جدول ایجاد شده است, سیاست ذخیره سازی خود را نمی توان تغییر داد.

### اطلاعات دقیق {#details}

در مورد `MergeTree` جداول داده ها به دیسک در راه های مختلف گرفتن است:

-   به عنوان یک نتیجه از درج (`INSERT` پرسوجو).
-   در طول پس زمینه ادغام و [جهشها](../../../sql-reference/statements/alter.md#alter-mutations).
-   هنگام دانلود از ماکت دیگر.
-   به عنوان یک نتیجه از انجماد پارتیشن [ALTER TABLE … FREEZE PARTITION](../../../sql-reference/statements/alter.md#alter_freeze-partition).

در تمام این موارد به جز جهش و پارتیشن انجماد بخش ذخیره شده در حجم و دیسک با توجه به سیاست ذخیره سازی داده شده است:

1.  جلد اول (به ترتیب تعریف) که فضای دیسک به اندازه کافی برای ذخیره سازی یک بخش (`unreserved_space > current_part_size`) و اجازه می دهد تا برای ذخیره سازی بخش هایی از اندازه داده شده (`max_data_part_size_bytes > current_part_size`) انتخاب شده است .
2.  در این حجم, که دیسک انتخاب شده است که به دنبال یکی, که برای ذخیره سازی تکه های قبلی از داده مورد استفاده قرار گرفت, و دارای فضای رایگان بیش از اندازه بخش (`unreserved_space - keep_free_space_bytes > current_part_size`).

تحت هود جهش و پارتیشن انجماد استفاده از [لینک های سخت](https://en.wikipedia.org/wiki/Hard_link). لینک های سخت بین دیسک های مختلف پشتیبانی نمی شوند بنابراین در چنین مواردی قطعات حاصل شده بر روی دیسک های مشابه به عنوان اولیه ذخیره می شوند.

در پس زمینه, قطعات بین حجم بر اساس مقدار فضای رایگان نقل مکان کرد (`move_factor` پارامتر) با توجه به سفارش حجم در فایل پیکربندی اعلام کرد.
داده ها هرگز از گذشته و به یکی از اولین منتقل شده است. ممکن است از جداول سیستم استفاده کنید [سیستم._خروج](../../../operations/system-tables.md#system_tables-part-log) (زمینه `type = MOVE_PART`) و [سیستم.قطعات](../../../operations/system-tables.md#system_tables-parts) (فیلدها `path` و `disk`) برای نظارت بر حرکت پس زمینه . همچنین, اطلاعات دقیق را می توان در سیاهههای مربوط به سرور پیدا شده است.

کاربر می تواند نیروی حرکت بخشی یا پارتیشن از یک حجم به دیگری با استفاده از پرس و جو [ALTER TABLE … MOVE PART\|PARTITION … TO VOLUME\|DISK …](../../../sql-reference/statements/alter.md#alter_move-partition), تمام محدودیت برای عملیات پس زمینه در نظر گرفته شود. پرس و جو شروع یک حرکت به خودی خود و منتظر نیست برای عملیات پس زمینه به پایان خواهد رسید. کاربر یک پیام خطا اگر فضای رایگان به اندازه کافی در دسترس است و یا اگر هر یک از شرایط مورد نیاز را ملاقات کرد.

داده های متحرک با تکرار داده ها دخالت نمی کنند. از این رو, سیاست های ذخیره سازی مختلف را می توان برای همان جدول در کپی های مختلف مشخص.

پس از اتمام ادغام پس زمینه و جهش, قطعات قدیمی تنها پس از یک مقدار مشخصی از زمان حذف (`old_parts_lifetime`).
در طول این زمان به حجم یا دیسک های دیگر منتقل نمی شوند. از این رو, تا زمانی که قطعات در نهایت حذف, هنوز هم به حساب برای ارزیابی فضای دیسک اشغال گرفته.

[مقاله اصلی](https://clickhouse.tech/docs/ru/operations/table_engines/mergetree/) <!--hide-->
