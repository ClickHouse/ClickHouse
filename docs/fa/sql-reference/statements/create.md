---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 35
toc_title: CREATE
---

# ایجاد پرس و جو {#create-queries}

## CREATE DATABASE {#query-language-create-database}

ایجاد پایگاه داده.

``` sql
CREATE DATABASE [IF NOT EXISTS] db_name [ON CLUSTER cluster] [ENGINE = engine(...)]
```

### بند {#clauses}

-   `IF NOT EXISTS`

        If the `db_name` database already exists, then ClickHouse doesn't create a new database and:

        - Doesn't throw an exception if clause is specified.
        - Throws an exception if clause isn't specified.

-   `ON CLUSTER`

        ClickHouse creates the `db_name` database on all the servers of a specified cluster.

-   `ENGINE`

        - [MySQL](../engines/database_engines/mysql.md)

            Allows you to retrieve data from the remote MySQL server.

        By default, ClickHouse uses its own [database engine](../engines/database_engines/index.md).

## CREATE TABLE {#create-table-query}

این `CREATE TABLE` پرس و جو می تواند اشکال مختلف داشته باشد.

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [compression_codec] [TTL expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2] [compression_codec] [TTL expr2],
    ...
) ENGINE = engine
```

ایجاد یک جدول به نام ‘name’ در ‘db’ پایگاه داده یا پایگاه داده فعلی اگر ‘db’ تنظیم نشده است, با ساختار مشخص شده در براکت و ‘engine’ موتور.
ساختار جدول یک لیست از توصیف ستون است. اگر شاخص ها توسط موتور پشتیبانی می شوند به عنوان پارامتر برای موتور جدول نشان داده می شوند.

شرح ستون است `name type` در ساده ترین حالت. مثال: `RegionID UInt32`.
عبارات همچنین می توانید برای مقادیر پیش فرض تعریف شود (پایین را ببینید).

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name AS [db2.]name2 [ENGINE = engine]
```

ایجاد یک جدول با همان ساختار به عنوان جدول دیگر. شما می توانید یک موتور مختلف برای جدول مشخص کنید. اگر موتور مشخص نشده است, همان موتور خواهد شد که برای استفاده `db2.name2` جدول

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name AS table_function()
```

ایجاد یک جدول با ساختار و داده های بازگردانده شده توسط یک [تابع جدول](../table-functions/index.md#table-functions).

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name ENGINE = engine AS SELECT ...
```

ایجاد یک جدول با یک ساختار مانند نتیجه `SELECT` پرس و جو, با ‘engine’ موتور, و پر با داده ها از را انتخاب کنید.

در همه موارد اگر `IF NOT EXISTS` مشخص شده است, پرس و جو یک خطا بازگشت نیست اگر جدول در حال حاضر وجود دارد. در این مورد پرس و جو هیچ کاری انجام نخواهد داد.

می تواند بند دیگر پس از وجود دارد `ENGINE` بند در پرس و جو. دیدن مستندات دقیق در مورد چگونگی ایجاد جدول در شرح [موتورهای جدول](../../engines/table-engines/index.md#table_engines).

### مقادیر پیشفرض {#create-default-values}

شرح ستون می تواند یک عبارت برای یک مقدار پیش فرض مشخص, در یکی از روش های زیر:`DEFAULT expr`, `MATERIALIZED expr`, `ALIAS expr`.
مثال: `URLDomain String DEFAULT domain(URL)`.

اگر یک عبارت برای مقدار پیش فرض تعریف نشده است, مقادیر پیش فرض خواهد شد به صفر برای اعداد تنظیم, رشته های خالی برای رشته, طعمه خالی برای ارریس, و `0000-00-00` برای تاریخ و یا `0000-00-00 00:00:00` برای تاریخ با زمان. نقاط صفر پشتیبانی نمی شوند.

اگر عبارت پیش فرض تعریف شده است, نوع ستون اختیاری است. در صورتی که یک نوع به صراحت تعریف شده وجود ندارد, نوع بیان پیش فرض استفاده شده است. مثال: `EventDate DEFAULT toDate(EventTime)` – the ‘Date’ نوع خواهد شد برای استفاده ‘EventDate’ ستون.

اگر نوع داده ها و پیش فرض بیان تعریف شده به صراحت این را بیان خواهد شد بازیگران به نوع مشخص شده با استفاده از نوع مصاحبه توابع. مثال: `Hits UInt32 DEFAULT 0` معنی همان چیزی که به عنوان `Hits UInt32 DEFAULT toUInt32(0)`.

Default expressions may be defined as an arbitrary expression from table constants and columns. When creating and changing the table structure, it checks that expressions don’t contain loops. For INSERT, it checks that expressions are resolvable – that all columns they can be calculated from have been passed.

`DEFAULT expr`

مقدار پیش فرض عادی. اگر پرس و جو درج می کند ستون مربوطه مشخص نیست, خواهد شد در محاسبات بیان مربوطه پر.

`MATERIALIZED expr`

بیان محقق. چنین ستون ای نمی تواند برای درج مشخص شود زیرا همیشه محاسبه می شود.
برای درج بدون یک لیست از ستون, این ستون ها در نظر گرفته نمی.
علاوه بر این, این ستون جایگزین نشده است که با استفاده از یک ستاره در یک پرس و جو را انتخاب کنید. این است که برای حفظ همواره که تخلیه با استفاده از `SELECT *` را می توان به جدول با استفاده از درج بدون مشخص کردن لیست ستون قرار داده شده.

`ALIAS expr`

دو واژه مترادف. چنین ستون در جدول ذخیره نمی شود و در همه.
ارزش های خود را نمی توان در یک جدول قرار داد و هنگام استفاده از ستاره در یک پرس و جو انتخاب جایگزین نمی شود.
این را می توان در انتخاب استفاده می شود اگر نام مستعار است که در طول تجزیه پرس و جو گسترش یافته است.

هنگام استفاده از پرس و جو را تغییر دهید برای اضافه کردن ستون جدید, داده های قدیمی برای این ستون نوشته نشده است. بجای, در هنگام خواندن داده های قدیمی که ارزش برای ستون جدید ندارد, عبارات در پرواز به طور پیش فرض محاسبه. با این حال, اگر در حال اجرا عبارات نیاز به ستون های مختلف است که در پرس و جو نشان داده نمی, این ستون علاوه بر خوانده خواهد شد, اما فقط برای بلوک های داده که نیاز.

اگر شما یک ستون جدید اضافه کردن به یک جدول اما بعد تغییر بیان پیش فرض خود, ارزش های مورد استفاده برای داده های قدیمی تغییر خواهد کرد (برای داده هایی که ارزش بر روی دیسک ذخیره نمی شد). توجه داشته باشید که هنگام اجرای ادغام پس زمینه, داده ها را برای ستون که در یکی از قطعات ادغام از دست رفته است به بخش ادغام شده نوشته شده.

این ممکن است به مجموعه مقادیر پیش فرض برای عناصر در ساختارهای داده تو در تو.

### قیدها {#constraints}

همراه با ستون توصیف محدودیت می تواند تعریف شود:

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [compression_codec] [TTL expr1],
    ...
    CONSTRAINT constraint_name_1 CHECK boolean_expr_1,
    ...
) ENGINE = engine
```

`boolean_expr_1` می تواند با هر عبارت بولی. اگر قیود برای جدول تعریف شود هر کدام برای هر سطر بررسی خواهند شد `INSERT` query. If any constraint is not satisfied — server will raise an exception with constraint name and checking expression.

اضافه کردن مقدار زیادی از محدودیت های منفی می تواند عملکرد بزرگ را تحت تاثیر قرار `INSERT` نمایش داده شد.

### عبارت دیگر {#ttl-expression}

تعریف می کند زمان ذخیره سازی برای ارزش. می توان تنها برای ادغام مشخص-جداول خانواده. برای توضیحات دقیق, دیدن [ستون ها و جداول](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-ttl).

### کدکهای فشردهسازی ستون {#codecs}

به طور پیش فرض, تاتر اعمال `lz4` روش فشرده سازی. برای `MergeTree`- خانواده موتور شما می توانید روش فشرده سازی به طور پیش فرض در تغییر [فشردهسازی](../../operations/server-configuration-parameters/settings.md#server-settings-compression) بخش پیکربندی سرور. شما همچنین می توانید روش فشرده سازی برای هر ستون فردی در تعریف `CREATE TABLE` پرس و جو.

``` sql
CREATE TABLE codec_example
(
    dt Date CODEC(ZSTD),
    ts DateTime CODEC(LZ4HC),
    float_value Float32 CODEC(NONE),
    double_value Float64 CODEC(LZ4HC(9))
    value Float32 CODEC(Delta, ZSTD)
)
ENGINE = <Engine>
...
```

اگر یک کدک مشخص شده است, کدک به طور پیش فرض صدق نمی کند. به عنوان مثال کدک ها را می توان در یک خط لوله ترکیب کرد, `CODEC(Delta, ZSTD)`. برای انتخاب بهترین ترکیب کدک برای شما پروژه معیار شبیه به شرح داده شده در التیت منتقل می کند [کدگذاری های جدید برای بهبود بهره وری کلیک](https://www.altinity.com/blog/2019/7/new-encodings-to-improve-clickhouse) مقاله.

!!! warning "اخطار"
    شما می توانید فایل های پایگاه داده کلیک از حالت فشرده خارج با تاسیسات خارجی مانند `lz4`. بجای, استفاده از ویژه [کلیک کمپرسور](https://github.com/ClickHouse/ClickHouse/tree/master/programs/compressor) سودمند.

فشرده سازی برای موتورهای جدول زیر پشتیبانی می شود:

-   [ادغام](../../engines/table-engines/mergetree-family/mergetree.md) خانواده پشتیبانی از کدک های فشرده سازی ستون و انتخاب روش فشرده سازی پیش فرض توسط [فشردهسازی](../../operations/server-configuration-parameters/settings.md#server-settings-compression) تنظیمات.
-   [ثبت](../../engines/table-engines/log-family/log-family.md) خانواده با استفاده از `lz4` روش فشرده سازی به طور پیش فرض و پشتیبانی از کدک های فشرده سازی ستون.
-   [تنظیم](../../engines/table-engines/special/set.md). فقط فشرده سازی پیش فرض پشتیبانی می کند.
-   [پیوستن](../../engines/table-engines/special/join.md). فقط فشرده سازی پیش فرض پشتیبانی می کند.

تاتر با پشتیبانی از کدک های هدف مشترک و کدک های تخصصی.

#### کدکهای تخصصی {#create-query-specialized-codecs}

این کدک ها طراحی شده اند تا فشرده سازی را با استفاده از ویژگی های خاص داده ها موثر تر کند. برخی از این کدک ها اطلاعات خود را فشرده سازی نمی کنند. در عوض داده ها را برای یک کدک هدف مشترک تهیه می کنند که بهتر از بدون این دارو را فشرده می کند.

کدکهای تخصصی:

-   `Delta(delta_bytes)` — Compression approach in which raw values are replaced by the difference of two neighboring values, except for the first value that stays unchanged. Up to `delta_bytes` برای ذخیره سازی مقادیر دلتا استفاده می شود, بنابراین `delta_bytes` حداکثر اندازه مقادیر خام است. ممکن است `delta_bytes` ارزش: 1, 2, 4, 8. مقدار پیش فرض برای `delta_bytes` هست `sizeof(type)` اگر به برابر 1, 2, 4, یا 8. در تمام موارد دیگر 1 است.
-   `DoubleDelta` — Calculates delta of deltas and writes it in compact binary form. Optimal compression rates are achieved for monotonic sequences with a constant stride, such as time series data. Can be used with any fixed-width type. Implements the algorithm used in Gorilla TSDB, extending it to support 64-bit types. Uses 1 extra bit for 32-byte deltas: 5-bit prefixes instead of 4-bit prefixes. For additional information, see Compressing Time Stamps in [گوریل: سریع, مقیاس پذیر, در حافظه پایگاه داده سری زمان](http://www.vldb.org/pvldb/vol8/p1816-teller.pdf).
-   `Gorilla` — Calculates XOR between current and previous value and writes it in compact binary form. Efficient when storing a series of floating point values that change slowly, because the best compression rate is achieved when neighboring values are binary equal. Implements the algorithm used in Gorilla TSDB, extending it to support 64-bit types. For additional information, see Compressing Values in [گوریل: سریع, مقیاس پذیر, در حافظه پایگاه داده سری زمان](http://www.vldb.org/pvldb/vol8/p1816-teller.pdf).
-   `T64` — Compression approach that crops unused high bits of values in integer data types (including `Enum`, `Date` و `DateTime`). در هر مرحله از الگوریتم کدک یک بلوک از ارزش های 64 را می گیرد و به ماتریس بیتی 6464 می رسد و بیت های استفاده نشده ارزش ها را تولید می کند و بقیه را به عنوان یک دنباله باز می گرداند. بیت های استفاده نشده بیت هایی هستند که بین مقادیر حداکثر و حداقل در کل بخش داده ای که فشرده سازی استفاده می شود متفاوت نیستند.

`DoubleDelta` و `Gorilla` کدک ها در گوریل تسدب به عنوان اجزای الگوریتم فشرده سازی خود استفاده می شود. رویکرد گوریل در سناریوها موثر است زمانی که یک دنباله از ارزش های کم زمان خود را با تغییر دادن زمان وجود دارد. مهر زمانی به طور موثر توسط فشرده `DoubleDelta` کدک و ارزش ها به طور موثر توسط فشرده `Gorilla` وابسته به کدک. مثلا برای دریافت جدول ذخیره شده به طور موثر می توانید در تنظیمات زیر ایجاد کنید:

``` sql
CREATE TABLE codec_example
(
    timestamp DateTime CODEC(DoubleDelta),
    slow_values Float32 CODEC(Gorilla)
)
ENGINE = MergeTree()
```

#### کدک های هدف مشترک {#create-query-common-purpose-codecs}

کدکها:

-   `NONE` — No compression.
-   `LZ4` — Lossless [الگوریتم فشرده سازی داده ها](https://github.com/lz4/lz4) به طور پیش فرض استفاده می شود. اعمال فشرده سازی سریع 4.
-   `LZ4HC[(level)]` — LZ4 HC (high compression) algorithm with configurable level. Default level: 9. Setting `level <= 0` سطح پیش فرض اعمال می شود. سطوح ممکن: \[1, 12\]. محدوده سطح توصیه شده: \[4, 9\].
-   `ZSTD[(level)]` — [الگوریتم فشرد فشاری](https://en.wikipedia.org/wiki/Zstandard) با قابلیت تنظیم `level`. سطوح ممکن: \[1, 22\]. مقدار پیش فرض: 1.

سطح فشرده سازی بالا برای حالات نامتقارن مفید هستند, مانند فشرده سازی یک بار, از حالت فشرده خارج بارها و بارها. سطوح بالاتر به معنای فشرده سازی بهتر و استفاده از پردازنده بالاتر است.

## جداول موقت {#temporary-tables}

تاتر از جداول موقت که دارای ویژگی های زیر:

-   جداول موقت ناپدید می شوند هنگامی که جلسه به پایان می رسد از جمله اگر اتصال از دست داده است.
-   جدول موقت با استفاده از موتور حافظه تنها.
-   دسی بل را نمی توان برای یک جدول موقت مشخص شده است. این است که در خارج از پایگاه داده ایجاد شده است.
-   غیر ممکن است برای ایجاد یک جدول موقت با پرس و جو توزیع دی ال در تمام سرورهای خوشه (با استفاده از `ON CLUSTER`): این جدول تنها در جلسه فعلی وجود دارد.
-   اگر یک جدول موقت است به همین نام به عنوان یکی دیگر و یک پرس و جو نام جدول بدون مشخص دسی بل مشخص, جدول موقت استفاده خواهد شد.
-   برای پردازش پرس و جو توزیع, جداول موقت مورد استفاده در یک پرس و جو به سرور از راه دور منتقل.

برای ایجاد یک جدول موقت از نحو زیر استفاده کنید:

``` sql
CREATE TEMPORARY TABLE [IF NOT EXISTS] table_name
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
)
```

در بیشتر موارد, جداول موقت به صورت دستی ایجاد نمی, اما در هنگام استفاده از داده های خارجی برای پرس و جو, و یا برای توزیع `(GLOBAL) IN`. برای کسب اطلاعات بیشتر به بخش های مناسب مراجعه کنید

امکان استفاده از جداول با [موتور = حافظه](../../engines/table-engines/special/memory.md) به جای جداول موقت.

## توزیع پرس و جو ددل (در بند خوشه) {#distributed-ddl-queries-on-cluster-clause}

این `CREATE`, `DROP`, `ALTER` و `RENAME` نمایش داده شد حمایت از اجرای توزیع در یک خوشه.
برای مثال پرس و جو زیر ایجاد می کند `all_hits` `Distributed` جدول در هر میزبان در `cluster`:

``` sql
CREATE TABLE IF NOT EXISTS all_hits ON CLUSTER cluster (p Date, i Int32) ENGINE = Distributed(cluster, default, hits)
```

به منظور اجرای این نمایش داده شد به درستی, هر یک از میزبان باید تعریف خوشه همان (برای ساده سازی تنظیمات همگام سازی, شما می توانید تعویض از باغ وحش استفاده). همچنین باید به سرورهای باغ وحش متصل شوند.
نسخه محلی از پرس و جو در نهایت بر روی هر یک از میزبان در خوشه اجرا می شود, حتی اگر برخی از میزبان در حال حاضر در دسترس نیست. سفارش برای اجرای نمایش داده شد در یک میزبان واحد تضمین شده است.

## CREATE VIEW {#create-view}

``` sql
CREATE [MATERIALIZED] VIEW [IF NOT EXISTS] [db.]table_name [TO[db.]name] [ENGINE = engine] [POPULATE] AS SELECT ...
```

ایجاد یک دیدگاه. دو نوع دیدگاه وجود دارد: طبیعی و تحقق.

نمایش عادی هیچ داده ذخیره نمی, اما فقط انجام خواندن از جدول دیگر. به عبارت دیگر, یک نمایش عادی چیزی بیش از یک پرس و جو ذخیره شده است. هنگام خواندن از نظر, این پرس و جو را نجات داد به عنوان یک خرده فروشی در بند از استفاده.

به عنوان مثال, فرض کنیم شما یک دیدگاه ایجاد کرده اید:

``` sql
CREATE VIEW view AS SELECT ...
```

و پرس و جو نوشته شده است:

``` sql
SELECT a, b, c FROM view
```

این پرس و جو به طور کامل به استفاده از خرده فروشی معادل است:

``` sql
SELECT a, b, c FROM (SELECT ...)
```

نمایش ها محقق داده فروشگاه تبدیل شده توسط پرس و جو مربوطه را انتخاب کنید.

هنگام ایجاد یک دیدگاه محقق بدون `TO [db].[table]`, you must specify ENGINE – the table engine for storing data.

هنگام ایجاد یک دیدگاه محقق با `TO [db].[table]`, شما باید استفاده کنید `POPULATE`.

مشاهده محقق به شرح زیر مرتب: هنگام قرار دادن داده ها به جدول مشخص شده در انتخاب, بخشی از داده های درج شده است این پرس و جو را انتخاب کنید تبدیل, و در نتیجه در نظر قرار داده.

اگر شما جمعیت مشخص, داده های جدول موجود در نظر قرار داده در هنگام ایجاد, اگر ساخت یک `CREATE TABLE ... AS SELECT ...` . در غیر این صورت پرس و جو شامل تنها داده های درج شده در جدول پس از ایجاد دیدگاه. ما توصیه نمی کنیم با استفاده از جمعیت, از داده ها در جدول در طول ایجاد مشاهده قرار داده نمی شود.

A `SELECT` پرسوجو می تواند شامل شود `DISTINCT`, `GROUP BY`, `ORDER BY`, `LIMIT`… Note that the corresponding conversions are performed independently on each block of inserted data. For example, if `GROUP BY` تنظیم شده است, داده ها در طول درج جمع, اما تنها در یک بسته واحد از داده های درج شده. داده ها بیشتر جمع نمی شود. استثنا است که با استفاده از یک موتور است که به طور مستقل انجام تجمع داده ها, مانند `SummingMergeTree`.

اعدام `ALTER` نمایش داده شد در نمایش محقق شده است به طور کامل توسعه یافته نیست, بنابراین ممکن است ناخوشایند. اگر مشاهده محقق با استفاده از ساخت و ساز `TO [db.]name` شما می توانید `DETACH` منظره, اجرا `ALTER` برای جدول هدف و سپس `ATTACH` قبلا جدا (`DETACH`) نظر .

نمایش ها نگاه همان جداول عادی. برای مثال در نتیجه ذکر شده است `SHOW TABLES` پرس و جو.

پرس و جو جداگانه برای حذف نمایش ها وجود ندارد. برای حذف یک نما, استفاده `DROP TABLE`.

## CREATE DICTIONARY {#create-dictionary-query}

``` sql
CREATE DICTIONARY [IF NOT EXISTS] [db.]dictionary_name [ON CLUSTER cluster]
(
    key1 type1  [DEFAULT|EXPRESSION expr1] [HIERARCHICAL|INJECTIVE|IS_OBJECT_ID],
    key2 type2  [DEFAULT|EXPRESSION expr2] [HIERARCHICAL|INJECTIVE|IS_OBJECT_ID],
    attr1 type2 [DEFAULT|EXPRESSION expr3],
    attr2 type2 [DEFAULT|EXPRESSION expr4]
)
PRIMARY KEY key1, key2
SOURCE(SOURCE_NAME([param1 value1 ... paramN valueN]))
LAYOUT(LAYOUT_NAME([param_name param_value]))
LIFETIME([MIN val1] MAX val2)
```

ایجاد [فرهنگ لغت خارجی](../../sql-reference/dictionaries/external-dictionaries/external-dicts.md) با توجه به [ساختار](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-structure.md), [متن](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md), [طرحبندی](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-layout.md) و [طول عمر](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-lifetime.md).

ساختار فرهنگ لغت خارجی شامل ویژگی های. ویژگی فرهنگ لغت به طور مشابه به ستون جدول مشخص شده است. تنها ویژگی مورد نیاز ویژگی نوع خود است, تمام خواص دیگر ممکن است مقادیر پیش فرض دارند.

بسته به فرهنگ لغت [طرحبندی](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-layout.md) یک یا چند ویژگی را می توان به عنوان کلید فرهنگ لغت مشخص شده است.

برای کسب اطلاعات بیشتر, دیدن [واژهنامهها خارجی](../../sql-reference/dictionaries/external-dictionaries/external-dicts.md) بخش.

[مقاله اصلی](https://clickhouse.tech/docs/en/query_language/create/) <!--hide-->
