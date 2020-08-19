---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
---

# تنظیمات {#settings}

## \_شماره توزیع شده {#distributed-product-mode}

تغییر رفتار [توزیع subqueries](../../sql-reference/operators/in.md).

ClickHouse applies this setting when the query contains the product of distributed tables, i.e. when the query for a distributed table contains a non-GLOBAL subquery for the distributed table.

محدودیت ها:

-   تنها برای اعمال در و پیوستن به subqueries.
-   فقط اگر از بخش با استفاده از یک جدول توزیع حاوی بیش از یک سفال.
-   اگر خرده فروشی مربوط به یک جدول توزیع حاوی بیش از یک سفال.
-   برای ارزش جدول استفاده نمی شود [دور](../../sql-reference/table-functions/remote.md) تابع.

مقادیر ممکن:

-   `deny` — Default value. Prohibits using these types of subqueries (returns the “Double-distributed in/JOIN subqueries is denied” استثنا).
-   `local` — Replaces the database and table in the subquery with local ones for the destination server (shard), leaving the normal `IN`/`JOIN.`
-   `global` — Replaces the `IN`/`JOIN` پرسوجو با `GLOBAL IN`/`GLOBAL JOIN.`
-   `allow` — Allows the use of these types of subqueries.

## تغییر در حسابهای کاربری دستگاه {#enable-optimize-predicate-expression}

تبدیل به پیش فرض فشار در `SELECT` نمایش داده شد.

افت فشار پیش فرض ممکن است به طور قابل توجهی کاهش ترافیک شبکه برای نمایش داده شد توزیع شده است.

مقادیر ممکن:

-   0 — Disabled.
-   1 — Enabled.

مقدار پیش فرض: 1.

استفاده

پرس و جو های زیر را در نظر بگیرید:

1.  `SELECT count() FROM test_table WHERE date = '2018-10-10'`
2.  `SELECT count() FROM (SELECT * FROM test_table) WHERE date = '2018-10-10'`

اگر `enable_optimize_predicate_expression = 1`, سپس زمان اجرای این نمایش داده شد برابر است چرا که کلیکاوس اعمال می شود `WHERE` به خرده فروشی در هنگام پردازش.

اگر `enable_optimize_predicate_expression = 0` سپس زمان اجرای پرس و جو دوم بسیار طولانی تر است زیرا `WHERE` بند مربوط به تمام داده ها پس از اتمام زیرخاکری.

## شناسه بسته: {#settings-fallback_to_stale_replicas_for_distributed_queries}

نیروهای پرس و جو به ماکت خارج از تاریخ اگر داده به روز شده در دسترس نیست. ببینید [تکرار](../../engines/table-engines/mergetree-family/replication.md).

تاتر انتخاب مناسب ترین از کپی منسوخ شده از جدول.

مورد استفاده در هنگام انجام `SELECT` از یک جدول توزیع شده است که اشاره به جداول تکرار.

به طور پیش فرض, 1 (فعال).

## اجبار {#settings-force_index_by_date}

غیرفعال اجرای پرس و جو در صورتی که شاخص می تواند بر اساس تاریخ استفاده نمی شود.

با جداول در خانواده ادغام کار می کند.

اگر `force_index_by_date=1` چک چه پرس و جو وضعیت کلید تاریخ است که می تواند مورد استفاده قرار گیرد برای محدود کردن محدوده داده. اگر هیچ شرایط مناسب وجود دارد, این یک استثنا می اندازد. با این حال, بررسی نمی کند که وضعیت مقدار داده ها به خواندن را کاهش می دهد. مثلا, شرایط `Date != ' 2000-01-01 '` قابل قبول است حتی زمانی که منطبق بر تمام داده ها در جدول (به عنوان مثال در حال اجرا پرس و جو نیاز به اسکن کامل). برای کسب اطلاعات بیشتر در مورد محدوده داده ها در جداول ادغام, دیدن [ادغام](../../engines/table-engines/mergetree-family/mergetree.md).

## اجبار {#force-primary-key}

غیر فعال اعدام پرس و جو اگر نمایه سازی توسط کلید اصلی امکان پذیر نیست.

با جداول در خانواده ادغام کار می کند.

اگر `force_primary_key=1` چک خانه را ببینید اگر پرس و جو شرایط کلیدی اولیه است که می تواند مورد استفاده قرار گیرد برای محدود کردن محدوده داده است. اگر هیچ شرایط مناسب وجود دارد, این یک استثنا می اندازد. با این حال, بررسی نمی کند که وضعیت مقدار داده ها به خواندن را کاهش می دهد. برای کسب اطلاعات بیشتر در مورد محدوده داده ها در جداول ادغام, دیدن [ادغام](../../engines/table-engines/mergetree-family/mergetree.md).

## قالب\_نما {#format-schema}

این پارامتر زمانی مفید است که شما با استفاده از فرمت های که نیاز به یک تعریف طرح, مانند [سروان نیا](https://capnproto.org/) یا [Protobuf](https://developers.google.com/protocol-buffers/). ارزش بستگی به فرمت.

## دادههای سایت {#fsync-metadata}

فعالسازی یا غیرفعال کردن [فوسینک](http://pubs.opengroup.org/onlinepubs/9699919799/functions/fsync.html) هنگام نوشتن `.sql` پرونده ها فعال به طور پیش فرض.

این را حس می کند به غیر فعال کردن اگر سرور دارای میلیون ها جدول کوچک است که به طور مداوم در حال ایجاد و نابود شده است.

## نصب و راه اندازی {#settings-enable_http_compression}

را قادر می سازد و یا غیر فعال فشرده سازی داده ها در پاسخ به درخواست قام.

برای کسب اطلاعات بیشتر, خواندن [توصیف واسط قام](../../interfaces/http.md).

مقادیر ممکن:

-   0 — Disabled.
-   1 — Enabled.

مقدار پیش فرض: 0.

## \_تنظیم مجدد به حالت اولیه {#settings-http_zlib_compression_level}

سطح فشرده سازی داده ها را در پاسخ به درخواست قام تنظیم می کند [قابلیت تنظیم صدا = 1](#settings-enable_http_compression).

مقادیر ممکن: اعداد از 1 تا 9.

مقدار پیش فرض: 3.

## تغییر در حسابهای کاربری دستگاه {#settings-http_native_compression_disable_checksumming_on_decompress}

را قادر می سازد و یا غیر فعال تایید کنترلی زمانی که از حالت فشرده خارج کردن اطلاعات ارسال قام از مشتری. فقط برای فرمت فشرده سازی بومی کلیک (با استفاده نمی شود `gzip` یا `deflate`).

برای کسب اطلاعات بیشتر, خواندن [توصیف واسط قام](../../interfaces/http.md).

مقادیر ممکن:

-   0 — Disabled.
-   1 — Enabled.

مقدار پیش فرض: 0.

## نمایش سایت {#settings-send_progress_in_http_headers}

فعالسازی یا غیرفعال کردن `X-ClickHouse-Progress` هدرهای پاسخ قام در `clickhouse-server` پاسخ.

برای کسب اطلاعات بیشتر, خواندن [توصیف واسط قام](../../interfaces/http.md).

مقادیر ممکن:

-   0 — Disabled.
-   1 — Enabled.

مقدار پیش فرض: 0.

## عناصر {#setting-max_http_get_redirects}

محدودیت حداکثر تعداد قام از رازک تغییر مسیر برای [URL](../../engines/table-engines/special/url.md)- جدول موتور . تنظیمات مربوط به هر دو نوع جداول: کسانی که ایجاد شده توسط [CREATE TABLE](../../sql-reference/statements/create.md#create-table-query) پرس و جو و توسط [نشانی وب](../../sql-reference/table-functions/url.md) تابع جدول.

مقادیر ممکن:

-   هر عدد صحیح مثبت رازک.
-   0 — No hops allowed.

مقدار پیش فرض: 0.

## وارد کردن \_فرست\_مرزیابی \_نمایش مجدد {#settings-input_format_allow_errors_num}

حداکثر تعداد خطاهای قابل قبول را در هنگام خواندن از فرمت های متنی تنظیم می کند).

مقدار پیش فرض 0 است.

همیشه با جفت `input_format_allow_errors_ratio`.

اگر یک خطا رخ داده است در حالی که خواندن ردیف اما ضد خطا است که هنوز هم کمتر از `input_format_allow_errors_num`, خانه را نادیده می گیرد ردیف و حرکت بر روی یک بعدی.

اگر هر دو `input_format_allow_errors_num` و `input_format_allow_errors_ratio` بیش از حد, فاحشه خانه می اندازد یک استثنا.

## ثبت نام {#settings-input_format_allow_errors_ratio}

حداکثر درصد خطاها را در هنگام خواندن از فرمت های متنی تنظیم می کند.).
درصد خطاها به عنوان یک عدد ممیز شناور بین 0 و 1 تنظیم شده است.

مقدار پیش فرض 0 است.

همیشه با جفت `input_format_allow_errors_num`.

اگر یک خطا رخ داده است در حالی که خواندن ردیف اما ضد خطا است که هنوز هم کمتر از `input_format_allow_errors_ratio`, خانه را نادیده می گیرد ردیف و حرکت بر روی یک بعدی.

اگر هر دو `input_format_allow_errors_num` و `input_format_allow_errors_ratio` بیش از حد, فاحشه خانه می اندازد یک استثنا.

## در حال خواندن: {#settings-input_format_values_interpret_expressions}

را قادر می سازد و یا غیر فعال تجزیه کننده کامل گذاشتن اگر تجزیه کننده جریان سریع نمی تواند تجزیه داده ها. این تنظیم فقط برای [مقادیر](../../interfaces/formats.md#data-format-values) فرمت در درج داده ها. برای کسب اطلاعات بیشتر در مورد تجزیه نحو, دیدن [نحو](../../sql-reference/syntax.md) بخش.

مقادیر ممکن:

-   0 — Disabled.

    در این مورد, شما باید داده های فرمت شده را فراهم. دیدن [فرشها](../../interfaces/formats.md) بخش.

-   1 — Enabled.

    در این مورد, شما می توانید یک عبارت گذاشتن به عنوان یک ارزش استفاده, اما درج داده است این راه بسیار کندتر. اگر شما وارد کردن داده های فرمت شده تنها, سپس کلیک کنیدهاوس رفتار به عنوان اگر مقدار تنظیم است 0.

مقدار پیش فرض: 1.

مثال استفاده

درج [DateTime](../../sql-reference/data-types/datetime.md) ارزش نوع با تنظیمات مختلف.

``` sql
SET input_format_values_interpret_expressions = 0;
INSERT INTO datetime_t VALUES (now())
```

``` text
Exception on client:
Code: 27. DB::Exception: Cannot parse input: expected ) before: now()): (at row 1)
```

``` sql
SET input_format_values_interpret_expressions = 1;
INSERT INTO datetime_t VALUES (now())
```

``` text
Ok.
```

پرس و جو گذشته معادل به شرح زیر است:

``` sql
SET input_format_values_interpret_expressions = 0;
INSERT INTO datetime_t SELECT now()
```

``` text
Ok.
```

## در حال خواندن: {#settings-input_format_values_deduce_templates_of_expressions}

را قادر می سازد و یا غیر فعال کسر الگو برای عبارات گذاشتن در [مقادیر](../../interfaces/formats.md#data-format-values) قالب. این اجازه می دهد تجزیه و تفسیر عبارات در `Values` بسیار سریع تر اگر عبارات در ردیف متوالی همان ساختار. تاتر تلاش می کند به استنباط قالب یک عبارت, تجزیه ردیف زیر با استفاده از این الگو و ارزیابی بیان در یک دسته از ردیف موفقیت تجزیه.

مقادیر ممکن:

-   0 — Disabled.
-   1 — Enabled.

مقدار پیش فرض: 1.

برای پرس و جو زیر:

``` sql
INSERT INTO test VALUES (lower('Hello')), (lower('world')), (lower('INSERT')), (upper('Values')), ...
```

-   اگر `input_format_values_interpret_expressions=1` و `format_values_deduce_templates_of_expressions=0`, عبارات به طور جداگانه برای هر سطر تفسیر (این برای تعداد زیادی از ردیف بسیار کند است).
-   اگر `input_format_values_interpret_expressions=0` و `format_values_deduce_templates_of_expressions=1`, عبارات در اولین, ردیف دوم و سوم با استفاده از الگو تجزیه `lower(String)` و با هم تفسیر, بیان در ردیف جلو با قالب دیگری تجزیه (`upper(String)`).
-   اگر `input_format_values_interpret_expressions=1` و `format_values_deduce_templates_of_expressions=1`, همان است که در مورد قبلی, بلکه اجازه می دهد تا عقب نشینی به تفسیر عبارات به طور جداگانه اگر این امکان وجود ندارد به استنباط الگو.

## وارد کردن \_تماس\_عول\_ایجاد \_شکلتهای \_شخصی {#settings-input-format-values-accurate-types-of-literals}

این تنظیم تنها زمانی استفاده می شود `input_format_values_deduce_templates_of_expressions = 1`. این می تواند رخ دهد, که عبارت برای برخی از ستون دارای ساختار مشابه, اما حاوی لیتر عددی از انواع مختلف, به عنوان مثال

``` sql
(..., abs(0), ...),             -- UInt64 literal
(..., abs(3.141592654), ...),   -- Float64 literal
(..., abs(-1), ...),            -- Int64 literal
```

مقادیر ممکن:

-   0 — Disabled.

    In this case, ClickHouse may use a more general type for some literals (e.g., `Float64` یا `Int64` به جای `UInt64` برای `42`), اما ممکن است مشکلات سرریز و دقت شود.

-   1 — Enabled.

    در این مورد, تاتر چک نوع واقعی تحت اللفظی و با استفاده از یک قالب بیان از نوع مربوطه. در بعضی موارد, ممکن است به طور قابل توجهی کاهش سرعت ارزیابی بیان در `Values`.

مقدار پیش فرض: 1.

## \_پوشه های ورودی و خروجی {#session_settings-input_format_defaults_for_omitted_fields}

هنگام انجام `INSERT` نمایش داده شد, جایگزین مقادیر ستون ورودی حذف شده با مقادیر پیش فرض از ستون مربوطه. این گزینه فقط برای اعمال [جیسانچرو](../../interfaces/formats.md#jsoneachrow), [CSV](../../interfaces/formats.md#csv) و [جدول دار](../../interfaces/formats.md#tabseparated) فرمتها.

!!! note "یادداشت"
    هنگامی که این گزینه فعال است, ابرداده جدول طولانی از سرور به مشتری ارسال. این مصرف منابع محاسباتی اضافی بر روی سرور و می تواند عملکرد را کاهش دهد.

مقادیر ممکن:

-   0 — Disabled.
-   1 — Enabled.

مقدار پیش فرض: 1.

## پیشسو {#settings-input-format-tsv-empty-as-default}

هنگامی که فعال, جایگزین زمینه های ورودی خالی در فیلم با مقادیر پیش فرض. برای عبارات پیش فرض پیچیده `input_format_defaults_for_omitted_fields` باید بیش از حد فعال شود.

غیر فعال به طور پیش فرض.

## خرابی در حذف گواهینامهها {#settings-input-format-null-as-default}

را قادر می سازد و یا غیر فعال با استفاده از مقادیر پیش فرض اگر داده های ورودی شامل `NULL`, اما نوع داده از ستون مربوطه در نمی `Nullable(T)` (برای فرمت های ورودی متن).

## \_دفتر\_صرفههای شناسنامهی ورودی {#settings-input-format-skip-unknown-fields}

را قادر می سازد و یا غیر فعال پرش درج داده های اضافی.

در هنگام نوشتن داده ها, تاتر می اندازد یک استثنا اگر داده های ورودی حاوی ستون که در جدول هدف وجود ندارد. اگر پرش فعال است, تاتر می کند داده های اضافی وارد کنید و یک استثنا پرتاب نمی.

فرمت های پشتیبانی شده:

-   [جیسانچرو](../../interfaces/formats.md#jsoneachrow)
-   [اطلاعات دقیق](../../interfaces/formats.md#csvwithnames)
-   [اطلاعات دقیق](../../interfaces/formats.md#tabseparatedwithnames)
-   [TSKV](../../interfaces/formats.md#tskv)

مقادیر ممکن:

-   0 — Disabled.
-   1 — Enabled.

مقدار پیش فرض: 0.

## تغییر \_کم\_تر\_تنظیم مجدد \_جنسان {#settings-input_format_import_nested_json}

درج دادههای جسون را با اشیای تو در تو فعال یا غیرفعال میکند.

فرمت های پشتیبانی شده:

-   [جیسانچرو](../../interfaces/formats.md#jsoneachrow)

مقادیر ممکن:

-   0 — Disabled.
-   1 — Enabled.

مقدار پیش فرض: 0.

همچنین نگاه کنید به:

-   [استفاده از ساختارهای تو در تو](../../interfaces/formats.md#jsoneachrow-nested) با `JSONEachRow` قالب.

## \_فرست\_ام\_امنمایش گذرواژه {#settings-input-format-with-names-use-header}

را قادر می سازد و یا غیر فعال چک کردن سفارش ستون در هنگام قرار دادن داده ها.

برای بهبود عملکرد درج, توصیه می کنیم غیر فعال کردن این چک اگر شما اطمینان حاصل کنید که سفارش ستون از داده های ورودی همان است که در جدول هدف است.

فرمت های پشتیبانی شده:

-   [اطلاعات دقیق](../../interfaces/formats.md#csvwithnames)
-   [اطلاعات دقیق](../../interfaces/formats.md#tabseparatedwithnames)

مقادیر ممکن:

-   0 — Disabled.
-   1 — Enabled.

مقدار پیش فرض: 1.

## تغییر \_شماره {#settings-date_time_input_format}

اجازه می دهد تا انتخاب تجزیه کننده از نمایش متن از تاریخ و زمان.

تنظیمات برای اعمال نمی شود [توابع تاریخ و زمان](../../sql-reference/functions/date-time-functions.md).

مقادیر ممکن:

-   `'best_effort'` — Enables extended parsing.

    تاتر می توانید پایه تجزیه `YYYY-MM-DD HH:MM:SS` فرمت و همه [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) فرمت های تاریخ و زمان. به عنوان مثال, `'2018-06-08T01:02:03.000Z'`.

-   `'basic'` — Use basic parser.

    تاتر می توانید تنها پایه تجزیه `YYYY-MM-DD HH:MM:SS` قالب. به عنوان مثال, `'2019-08-20 10:18:56'`.

مقدار پیشفرض: `'basic'`.

همچنین نگاه کنید به:

-   [نوع داده حسگر ناحیه رنگی.](../../sql-reference/data-types/datetime.md)
-   [توابع برای کار با تاریخ و زمان.](../../sql-reference/functions/date-time-functions.md)

## بررسی اجمالی {#settings-join_default_strictness}

مجموعه سختی پیش فرض برای [تاریخ بند](../../sql-reference/statements/select/join.md#select-join).

مقادیر ممکن:

-   `ALL` — If the right table has several matching rows, ClickHouse creates a [محصول دکارتی](https://en.wikipedia.org/wiki/Cartesian_product) از تطبیق ردیف. این طبیعی است `JOIN` رفتار از استاندارد گذاشتن.
-   `ANY` — If the right table has several matching rows, only the first one found is joined. If the right table has only one matching row, the results of `ANY` و `ALL` یکسان هستند.
-   `ASOF` — For joining sequences with an uncertain match.
-   `Empty string` — If `ALL` یا `ANY` در پرس و جو مشخص نشده است, خانه عروسکی می اندازد یک استثنا.

مقدار پیشفرض: `ALL`.

## نمایش سایت {#settings-join_any_take_last_row}

تغییرات رفتار پیوستن به عملیات با `ANY` سخت بودن.

!!! warning "توجه"
    این تنظیم فقط برای `JOIN` عملیات با [پیوستن](../../engines/table-engines/special/join.md) جداول موتور.

مقادیر ممکن:

-   0 — If the right table has more than one matching row, only the first one found is joined.
-   1 — If the right table has more than one matching row, only the last one found is joined.

مقدار پیش فرض: 0.

همچنین نگاه کنید به:

-   [پیوستن بند](../../sql-reference/statements/select/join.md#select-join)
-   [پیوستن به موتور جدول](../../engines/table-engines/special/join.md)
-   [بررسی اجمالی](#settings-join_default_strictness)

## ارزشهای خبری عبارتند از: {#join_use_nulls}

نوع را تنظیم می کند [JOIN](../../sql-reference/statements/select/join.md) رفتار هنگامی که ادغام جداول سلول های خالی ممکن است ظاهر شود. کلیک هاوس بر اساس این تنظیم متفاوت است.

مقادیر ممکن:

-   0 — The empty cells are filled with the default value of the corresponding field type.
-   1 — `JOIN` رفتار به همان شیوه به عنوان در گذاشتن استاندارد. نوع زمینه مربوطه به تبدیل [Nullable](../../sql-reference/data-types/nullable.md#data_type-nullable) سلول های خالی پر شده اند [NULL](../../sql-reference/syntax.md).

مقدار پیش فرض: 0.

## ت\_مایش بیشینه {#setting-max_block_size}

در خانه, داده ها توسط بلوک های پردازش (مجموعه ای از قطعات ستون). چرخه پردازش داخلی برای یک بلوک به اندازه کافی موثر هستند, اما هزینه های قابل توجه در هر بلوک وجود دارد. این `max_block_size` تنظیم یک توصیه برای چه اندازه بلوک (در تعداد ردیف) برای بارگذاری از جداول است. اندازه بلوک نباید بیش از حد کوچک, به طوری که هزینه در هر بلوک هنوز هم قابل توجه است, اما نه بیش از حد بزرگ به طوری که پرس و جو با محدودیت است که پس از اولین بلوک به سرعت پردازش تکمیل. هدف این است که برای جلوگیری از مصرف حافظه بیش از حد در هنگام استخراج تعداد زیادی از ستون ها در موضوعات مختلف و برای حفظ حداقل برخی از محل کش.

مقدار پیش فرض: 65,536.

بلوک اندازه `max_block_size` همیشه از جدول لود نمی. اگر واضح است که داده های کمتر نیاز به بازیابی یک بلوک کوچکتر پردازش می شود.

## ترجی\_حات {#preferred-block-size-bytes}

مورد استفاده برای همان هدف به عنوان `max_block_size` اما با تطبیق تعداد سطرها در بلوک اندازه بلوک توصیه شده را در بایت تنظیم می کند.
با این حال, اندازه بلوک نمی تواند بیش از `max_block_size` ردیف
به طور پیش فرض: 1,000,000. تنها در هنگام خواندن از موتورهای ادغام کار می کند.

## ادغام \_تر\_م\_را\_م\_مایش مجدد {#setting-merge-tree-min-rows-for-concurrent-read}

اگر تعداد ردیف از یک فایل از یک خوانده شود [ادغام](../../engines/table-engines/mergetree-family/mergetree.md) جدول بیش از `merge_tree_min_rows_for_concurrent_read` سپس کلیک کنیدهاوس تلاش می کند برای انجام خواندن همزمان از این فایل در موضوعات مختلف.

مقادیر ممکن:

-   هر عدد صحیح مثبت.

مقدار پیش فرض: 163840.

## \_انتقال به \_انتقال به \_شخصی {#setting-merge-tree-min-bytes-for-concurrent-read}

اگر تعداد بایت برای خواندن از یک فایل از یک [ادغام](../../engines/table-engines/mergetree-family/mergetree.md)- جدول موتور بیش از `merge_tree_min_bytes_for_concurrent_read` سپس کلیک کنیدهاوس تلاش می کند به صورت همزمان از این فایل در موضوعات مختلف به عنوان خوانده شده.

مقدار ممکن:

-   هر عدد صحیح مثبت.

مقدار پیش فرض: 251658240.

## ادغام \_تر\_م\_را\_م\_را\_مایش مجدد {#setting-merge-tree-min-rows-for-seek}

اگر فاصله بین دو بلوک داده در یک فایل خوانده شود کمتر از `merge_tree_min_rows_for_seek` ردیف, سپس کلیک می کند از طریق فایل به دنبال ندارد اما می خواند پی در پی داده ها.

مقادیر ممکن:

-   هر عدد صحیح مثبت.

مقدار پیش فرض: 0.

## ادغام \_تر\_حضربه \_ترکمال {#setting-merge-tree-min-bytes-for-seek}

اگر فاصله بین دو بلوک داده در یک فایل خوانده شود کمتر از `merge_tree_min_bytes_for_seek` بایت, سپس پی در پی تاتر می خواند طیف وسیعی از فایل است که شامل هر دو بلوک, در نتیجه اجتناب اضافی به دنبال.

مقادیر ممکن:

-   هر عدد صحیح مثبت.

مقدار پیش فرض: 0.

## ادغام \_تر\_کوارسی\_یندگرمانی {#setting-merge-tree-coarse-index-granularity}

هنگامی که جستجو برای داده ها, تاتر چک علامت داده ها در فایل شاخص. اگر فاحشه خانه می یابد که کلید های مورد نیاز در برخی از محدوده هستند, این تقسیم این محدوده به `merge_tree_coarse_index_granularity` موشک و جستجو کلید های مورد نیاز وجود دارد به صورت بازگشتی.

مقادیر ممکن:

-   هر عدد صحیح حتی مثبت.

مقدار پیش فرض: 8.

## \_انتقال به \_انتقال {#setting-merge-tree-max-rows-to-use-cache}

اگر کلیک خانه باید بیش از خواندن `merge_tree_max_rows_to_use_cache` ردیف ها در یک پرس و جو از کش بلوک های غیر فشرده استفاده نمی کنند.

ذخیره سازی داده های ذخیره شده بلوک های غیر فشرده برای نمایش داده شد. تاتر با استفاده از این کش برای سرعت بخشیدن به پاسخ به نمایش داده شد کوچک تکرار شده است. این تنظیم محافظت از کش از سطل زباله توسط نمایش داده شد که مقدار زیادی از داده ها به عنوان خوانده شده. این [\_بالا](../server-configuration-parameters/settings.md#server-settings-uncompressed_cache_size) تنظیم سرور اندازه کش از بلوک های غیر فشرده را تعریف می کند.

مقادیر ممکن:

-   هر عدد صحیح مثبت.

Default value: 128 ✕ 8192.

## \_انتقال به \_انتقال {#setting-merge-tree-max-bytes-to-use-cache}

اگر کلیک خانه باید بیش از خواندن `merge_tree_max_bytes_to_use_cache` بایت در یک پرس و جو, این کش از بلوک های غیر فشرده استفاده نمی.

ذخیره سازی داده های ذخیره شده بلوک های غیر فشرده برای نمایش داده شد. تاتر با استفاده از این کش برای سرعت بخشیدن به پاسخ به نمایش داده شد کوچک تکرار شده است. این تنظیم محافظت از کش از سطل زباله توسط نمایش داده شد که مقدار زیادی از داده ها به عنوان خوانده شده. این [\_بالا](../server-configuration-parameters/settings.md#server-settings-uncompressed_cache_size) تنظیم سرور اندازه کش از بلوک های غیر فشرده را تعریف می کند.

مقدار ممکن:

-   هر عدد صحیح مثبت.

مقدار پیش فرض: 2013265920.

## \_عنوان \_تو\_میشه {#settings-min-bytes-to-use-direct-io}

حداقل حجم داده های مورد نیاز برای استفاده از دسترسی مستقیم به دیسک ذخیره سازی.

تاتر با استفاده از این تنظیم در هنگام خواندن داده ها از جداول. اگر حجم کل ذخیره سازی از تمام داده ها به عنوان خوانده شده بیش از `min_bytes_to_use_direct_io` بایت, سپس کلیک هاوس می خواند داده ها از دیسک ذخیره سازی با `O_DIRECT` انتخاب

مقادیر ممکن:

-   0 — Direct I/O is disabled.
-   عدد صحیح مثبت.

مقدار پیش فرض: 0.

## \_خروج {#settings-log-queries}

راه اندازی ورود به سیستم پرس و جو.

نمایش داده شد با توجه به قوانین در به کلیک خانه فرستاده می شود [\_خروج](../server-configuration-parameters/settings.md#server_configuration_parameters-query-log) پارامتر پیکربندی سرور.

مثال:

``` text
log_queries=1
```

## \_قاب کردن \_نوع {#settings-log-queries-min-type}

`query_log` حداقل نوع برای ورود به سیستم.

مقادیر ممکن:
- `QUERY_START` (`=1`)
- `QUERY_FINISH` (`=2`)
- `EXCEPTION_BEFORE_START` (`=3`)
- `EXCEPTION_WHILE_PROCESSING` (`=4`)

مقدار پیشفرض: `QUERY_START`.

می توان برای محدود کردن که `query_log`, می گویند شما جالب تنها در اشتباهات هستند, سپس شما می توانید استفاده کنید `EXCEPTION_WHILE_PROCESSING`:

``` text
log_queries_min_type='EXCEPTION_WHILE_PROCESSING'
```

## باز کردن {#settings-log-query-threads}

راه اندازی موضوعات پرس و جو ورود به سیستم.

نمایش داده شد' موضوعات runned توسط ClickHouse با راه اندازی این سیستم هستند با توجه به قوانین در [\_ر\_خروج](../server-configuration-parameters/settings.md#server_configuration_parameters-query-thread-log) پارامتر پیکربندی سرور.

مثال:

``` text
log_query_threads=1
```

## ا\_فزونهها {#settings-max_insert_block_size}

اندازه بلوک به شکل برای درج به یک جدول.
این تنظیم فقط در مواردی که سرور بلوک را تشکیل می دهد اعمال می شود.
برای مثال برای درج از طریق رابط اچ.تی. تی. پی سرور تجزیه فرمت داده ها و اشکال بلوک از اندازه مشخص شده است.
اما هنگامی که با استفاده از کلیک-مشتری تجزیه داده های خود را و ‘max\_insert\_block\_size’ تنظیم بر روی سرور به اندازه بلوک قرار داده تاثیر نمی گذارد.
تنظیمات نیز یک هدف در هنگام استفاده از درج را انتخاب کنید ندارد, از داده ها با استفاده از بلوک های مشابه که پس از انتخاب تشکیل قرار داده.

مقدار پیش فرض: 1,048,576.

به طور پیش فرض کمی بیش از `max_block_size`. دلیل این کار این است زیرا موتورهای جدول خاص (`*MergeTree`) بخش داده ها بر روی دیسک برای هر بلوک قرار داده شده است که یک نهاد نسبتا بزرگ را تشکیل می دهند. به طور مشابه, `*MergeTree` جداول مرتب سازی بر داده ها در هنگام درج و اندازه بلوک به اندازه کافی بزرگ اجازه می دهد مرتب سازی داده های بیشتر در رم.

## \_معرض \_سبک\_ز\_وز {#min-insert-block-size-rows}

مجموعه حداقل تعداد ردیف در بلوک است که می تواند به یک جدول توسط یک قرار داده `INSERT` پرس و جو. بلوک های کوچکتر به اندازه به موارد بزرگتر له می شوند.

مقادیر ممکن:

-   عدد صحیح مثبت.
-   0 — Squashing disabled.

مقدار پیش فرض: 1048576.

## ا\_فزونهها {#min-insert-block-size-bytes}

مجموعه حداقل تعداد بایت در بلوک است که می تواند به یک جدول توسط یک قرار داده `INSERT` پرس و جو. بلوک های کوچکتر به اندازه به موارد بزرگتر له می شوند.

مقادیر ممکن:

-   عدد صحیح مثبت.
-   0 — Squashing disabled.

مقدار پیش فرض: 268435456.

## \_شروع مجدد \_شروع مجدد \_شروع مجدد \_کاربری {#settings-max_replica_delay_for_distributed_queries}

غیرفعال تاخیر کپی برای نمایش داده شد توزیع شده است. ببینید [تکرار](../../engines/table-engines/mergetree-family/replication.md).

زمان را در عرض چند ثانیه تنظیم می کند. اگر یک ماکت نشدم بیش از ارزش مجموعه, این ماکت استفاده نمی شود.

مقدار پیش فرض: 300.

مورد استفاده در هنگام انجام `SELECT` از یک جدول توزیع شده است که اشاره به جداول تکرار.

## \_مخفی کردن {#settings-max_threads}

حداکثر تعداد موضوعات پردازش پرس و جو, به جز موضوعات برای بازیابی داده ها از سرور از راه دور (دیدن ‘max\_distributed\_connections’ پارامتر).

این پارامتر شامل موضوعات است که انجام همان مراحل از خط لوله پردازش پرس و جو به صورت موازی.
مثلا, در هنگام خواندن از یک جدول, اگر ممکن است به ارزیابی عبارات با توابع, فیلتر با کجا و از پیش جمع شده برای گروه به صورت موازی با استفاده از حداقل ‘max\_threads’ تعداد موضوعات, سپس ‘max\_threads’ استفاده می شود.

مقدار پیش فرض: تعداد هسته های پردازنده فیزیکی.

اگر کمتر از یک پرس و جو را انتخاب کنید به طور معمول بر روی یک سرور در یک زمان اجرا, تنظیم این پارامتر به یک مقدار کمی کمتر از تعداد واقعی هسته پردازنده.

برای نمایش داده شد که به سرعت به دلیل محدودیت تکمیل, شما می توانید یک مجموعه پایین تر ‘max\_threads’. مثلا , اگر تعداد لازم از نوشته در هر بلوک و حداکثر \_سرخ واقع = 8, سپس 8 بلوک بازیابی می شوند, اگر چه این امر می توانست به اندازه کافی برای خواندن فقط یک بوده است.

کوچکتر `max_threads` ارزش حافظه کمتر مصرف می شود.

## ا\_فزونهها {#settings-max-insert-threads}

حداکثر تعداد موضوعات برای اجرای `INSERT SELECT` پرس و جو.

مقادیر ممکن:

-   0 (or 1) — `INSERT SELECT` بدون اجرای موازی.
-   عدد صحیح مثبت. بزرگتر از 1.

مقدار پیش فرض: 0.

موازی `INSERT SELECT` اثر تنها در صورتی که `SELECT` بخش به صورت موازی اجرا, دیدن [\_مخفی کردن](#settings-max_threads) تنظیمات.
مقادیر بالاتر به استفاده از حافظه بالاتر منجر شود.

## \_بزرگنمایی {#max-compress-block-size}

حداکثر اندازه بلوک از داده های غیر فشرده قبل از فشرده سازی برای نوشتن به یک جدول. به طور پیش فرض, 1,048,576 (1 مگابایت). اگر اندازه کاهش می یابد, میزان فشرده سازی به طور قابل توجهی کاهش می یابد, سرعت فشرده سازی و رفع فشار کمی با توجه به محل کش را افزایش می دهد, و مصرف حافظه کاهش می یابد. معمولا وجود دارد هر دلیلی برای تغییر این تنظیم نیست.

هنوز بلوک برای فشرده سازی اشتباه نیست (یک تکه از حافظه متشکل از بایت) با بلوک برای پردازش پرس و جو (مجموعه ای از ردیف از یک جدول).

## \_بزرگنمایی {#min-compress-block-size}

برای [ادغام](../../engines/table-engines/mergetree-family/mergetree.md)"جداول . به منظور کاهش زمان تاخیر در هنگام پردازش نمایش داده شد, یک بلوک فشرده شده است در هنگام نوشتن علامت بعدی اگر اندازه خود را حداقل ‘min\_compress\_block\_size’. به طور پیش فرض 65,536.

اندازه واقعی بلوک, اگر داده غیر فشرده کمتر از است ‘max\_compress\_block\_size’, کمتر از این مقدار و کمتر از حجم داده ها برای یک علامت.

بیایید نگاهی به عنوان مثال. فرض کنیم که ‘index\_granularity’ در طول ایجاد جدول به 8192 تنظیم شد.

ما در حال نوشتن یک ستون نوع 32 (4 بایت در هر مقدار). هنگام نوشتن 8192 ردیف, کل خواهد بود 32 کیلوبایت داده. پس min\_compress\_block\_size = 65,536 یک فشرده بلوک تشکیل خواهد شد برای هر دو نشانه است.

ما در حال نوشتن یک ستون نشانی اینترنتی با نوع رشته (اندازه متوسط 60 بایت در هر مقدار). هنگام نوشتن 8192 ردیف, متوسط خواهد بود کمی کمتر از 500 کیلوبایت داده. پس از این بیش از است 65,536, یک بلوک فشرده خواهد شد برای هر علامت تشکیل. در این مورد, در هنگام خواندن داده ها از دیسک در طیف وسیعی از یک علامت, اطلاعات اضافی نمی خواهد از حالت فشرده خارج شود.

معمولا وجود دارد هر دلیلی برای تغییر این تنظیم نیست.

## بیشینه\_کرکی\_سیز {#settings-max_query_size}

حداکثر بخشی از پرس و جو است که می تواند به رم برای تجزیه با تجزیه کننده گذاشتن گرفته شده است.
پرس و جو درج همچنین شامل داده ها برای درج است که توسط تجزیه کننده جریان جداگانه پردازش (که مصرف درجه(1) رم), است که در این محدودیت شامل نمی شود.

مقدار پیش فرض: 256 کیلوبایت.

## فعالسازی \_دلای {#interactive-delay}

فاصله در میکروثانیه برای بررسی اینکه اجرای درخواست لغو شده است و ارسال پیشرفت.

مقدار پیش فرض: 100,000 (چک برای لغو و پیشرفت می فرستد ده بار در ثانیه).

## connect\_timeout, receive\_timeout, send\_timeout {#connect-timeout-receive-timeout-send-timeout}

وقفه در ثانیه بر روی سوکت مورد استفاده برای برقراری ارتباط با مشتری.

مقدار پیش فرض: 10, 300, 300.

## \_انتقال به \_ار\_خروج {#cancel-http-readonly-queries-on-client-close}

Cancels HTTP read-only queries (e.g. SELECT) when a client closes the connection without waiting for the response.

مقدار پیشفرض: 0

## پول\_نتروال {#poll-interval}

قفل در یک حلقه انتظار برای تعداد مشخصی از ثانیه.

مقدار پیش فرض: 10.

## \_ادغام گیر {#max-distributed-connections}

حداکثر تعداد اتصالات همزمان با سرور از راه دور برای پردازش توزیع از یک پرس و جو تنها به یک جدول توزیع. ما توصیه می کنیم تنظیم یک مقدار کمتر از تعداد سرور در خوشه.

مقدار پیش فرض: 1024.

پارامترهای زیر فقط هنگام ایجاد جداول توزیع شده (و هنگام راه اندازی یک سرور) استفاده می شود بنابراین هیچ دلیلی برای تغییر در زمان اجرا وجود ندارد.

## نمایش سایت {#distributed-connections-pool-size}

حداکثر تعداد اتصالات همزمان با سرور از راه دور برای پردازش توزیع از همه نمایش داده شد به یک جدول توزیع شده است. ما توصیه می کنیم تنظیم یک مقدار کمتر از تعداد سرور در خوشه.

مقدار پیش فرض: 1024.

## \_انتقال به \_مزاح\_اف\_کننده {#connect-timeout-with-failover-ms}

فاصله در میلی ثانیه برای اتصال به یک سرور از راه دور برای یک موتور جدول توزیع, اگر ‘shard’ و ‘replica’ بخش ها در تعریف خوشه استفاده می شود.
اگر ناموفق, چندین تلاش برای اتصال به کپی های مختلف ساخته شده.

مقدار پیش فرض: 50.

## قابلیت اتصال به شبکه {#connections-with-failover-max-tries}

حداکثر تعداد تلاش اتصال با هر ماکت برای موتور جدول توزیع.

مقدار پیش فرض: 3.

## افراط {#extremes}

اینکه مقادیر شدید (حداقل و حداکثر در ستون یک نتیجه پرس و جو) شمارش شود. می پذیرد 0 یا 1. به طور پیش فرض, 0 (غیر فعال).
برای کسب اطلاعات بیشتر به بخش مراجعه کنید “Extreme values”.

## همترازی پایین {#setting-use_uncompressed_cache}

اینکه از یک کش از بلوکهای غیر فشرده استفاده شود یا خیر. می پذیرد 0 یا 1. به طور پیش فرض, 0 (غیر فعال).
با استفاده از کش غیر فشرده (فقط برای جداول در خانواده ادغام) می تواند به طور قابل توجهی کاهش زمان تاخیر و افزایش توان در هنگام کار با تعداد زیادی از نمایش داده شد کوتاه است. فعال کردن این تنظیم برای کاربرانی که ارسال درخواست کوتاه مکرر. همچنین با توجه به پرداخت [\_بالا](../server-configuration-parameters/settings.md#server-settings-uncompressed_cache_size) configuration parameter (only set in the config file) – the size of uncompressed cache blocks. By default, it is 8 GiB. The uncompressed cache is filled in as needed and the least-used data is automatically deleted.

برای نمایش داده شد که خواندن حداقل حجم تا حدودی زیادی از داده ها (یک میلیون ردیف یا بیشتر) غیر فشرده کش غیر فعال است به طور خودکار به صرفه جویی در فضا برای واقعا کوچک نمایش داده شد. این به این معنی است که شما می توانید نگه دارید ‘use\_uncompressed\_cache’ تنظیم همیشه به مجموعه 1.

## جایگزینی \_خروج {#replace-running-query}

هنگام استفاده از رابط قام ‘query\_id’ پارامتر را می توان گذشت. این هر رشته که به عنوان شناسه پرس و جو در خدمت است.
اگر پرس و جو از همان کاربر با همان ‘query\_id’ در حال حاضر در این زمان وجود دارد, رفتار بستگی به ‘replace\_running\_query’ پارامتر.

`0` (default) – Throw an exception (don't allow the query to run if a query with the same ‘query\_id’ در حال حاضر در حال اجرا).

`1` – Cancel the old query and start running the new one.

یاندکسمتریکا با استفاده از این پارامتر را به 1 برای اجرای پیشنهادات خود را برای شرایط تقسیم بندی. پس از ورود به شخصیت بعدی, اگر پرس و جو قدیمی هنوز تمام نشده است, باید لغو شود.

## \_خاله جریان {#stream-flush-interval-ms}

این نسخهها کار میکند برای جداول با جریان در مورد یک ایست, و یا زمانی که یک موضوع تولید [ا\_فزونهها](#settings-max_insert_block_size) ردیف

مقدار پیش فرض 7500 است.

کوچکتر ارزش, اطلاعات بیشتر به جدول سرخ. تنظیم مقدار خیلی کم منجر به عملکرد ضعیف می شود.

## \_تبالسازی {#settings-load_balancing}

تعیین الگوریتم انتخاب کپی است که برای پردازش پرس و جو توزیع استفاده.

کلیک هاوس از الگوریتم های زیر برای انتخاب کپی ها پشتیبانی می کند:

-   [تصادفی](#load_balancing-random) (به طور پیش فرض)
-   [نزدیکترین نام میزبان](#load_balancing-nearest_hostname)
-   [به ترتیب](#load_balancing-in_order)
-   [اول یا تصادفی](#load_balancing-first_or_random)

### تصادفی (به طور پیش فرض) {#load_balancing-random}

``` sql
load_balancing = random
```

تعداد خطاها برای هر ماکت شمارش. پرس و جو با کمترین خطاها به ماکت ارسال می شود و اگر چندین مورد از این موارد وجود داشته باشد به هر کسی.
معایب: نزدیکی سرور برای خود اختصاص نمی; اگر کپی داده های مختلف, شما همچنین می خواهد داده های مختلف.

### نزدیکترین نام میزبان {#load_balancing-nearest_hostname}

``` sql
load_balancing = nearest_hostname
```

The number of errors is counted for each replica. Every 5 minutes, the number of errors is integrally divided by 2. Thus, the number of errors is calculated for a recent time with exponential smoothing. If there is one replica with a minimal number of errors (i.e. errors occurred recently on the other replicas), the query is sent to it. If there are multiple replicas with the same minimal number of errors, the query is sent to the replica with a hostname that is most similar to the server's hostname in the config file (for the number of different characters in identical positions, up to the minimum length of both hostnames).

مثلا example01-01-1 و example01-01-2.yandex.ru متفاوت هستند در یک موقعیت در حالی که example01-01-1 و example01-02-2 متفاوت در دو مکان است.
این روش ممکن است ابتدایی به نظر برسد اما اطلاعات خارجی در مورد توپولوژی شبکه نیاز ندارد و نشانی های اینترنتی را مقایسه نمی کند که برای نشانیهای اینترنتی6 پیچیده خواهد بود.

بدین ترتیب, اگر کپی معادل وجود دارد, نزدیک ترین یک به نام ترجیح داده می شود.
ما همچنین می توانیم فرض کنیم که در هنگام ارسال یک پرس و جو به همان سرور در صورت عدم وجود شکست توزیع پرس و جو نیز به همان سرور. بنابراین حتی اگر داده های مختلف بر روی کپی قرار داده شده, پرس و جو بیشتر همان نتایج بازگشت.

### به ترتیب {#load_balancing-in_order}

``` sql
load_balancing = in_order
```

کپی با همان تعداد از اشتباهات در همان جهت قابل دسترسی هستند که در پیکربندی مشخص شده است.
این روش مناسب است که شما می دانید دقیقا همان است که ماکت ترجیح داده شده است.

### اول یا تصادفی {#load_balancing-first_or_random}

``` sql
load_balancing = first_or_random
```

این الگوریتم را انتخاب اولین ماکت در مجموعه و یا یک ماکت تصادفی اگر برای اولین بار در دسترس نیست. این در تنظیم توپولوژی متقابل تکرار موثر, اما بی فایده در تنظیمات دیگر.

این `first_or_random` الگوریتم حل مشکل از `in_order` الگوریتم. با `in_order`, اگر یک ماکت پایین می رود, یک بعدی می شود یک بار دو برابر در حالی که کپی باقی مانده رسیدگی به مقدار معمول از ترافیک. هنگام استفاده از `first_or_random` الگوریتم, بار به طور مساوی در میان کپی که هنوز هم در دسترس هستند توزیع.

## پیشفرض {#settings-prefer-localhost-replica}

را قادر می سازد / غیر فعال ترجیح با استفاده از ماکت مجنون زمانی که پردازش نمایش داده شد توزیع شده است.

مقادیر ممکن:

-   1 — ClickHouse always sends a query to the localhost replica if it exists.
-   0 — ClickHouse uses the balancing strategy specified by the [\_تبالسازی](#settings-load_balancing) تنظیمات.

مقدار پیش فرض: 1.

!!! warning "اخطار"
    غیر فعال کردن این تنظیم در صورت استفاده [بیشینه\_راپرال\_راپیکال](#settings-max_parallel_replicas).

## کد \_ورود {#totals-mode}

چگونه برای محاسبه مجموع زمانی که نیاز است در حال حاضر به عنوان زمانی که max\_rows\_to\_group\_by و group\_by\_overflow\_mode = ‘any’ حضور دارند.
بخش را ببینید “WITH TOTALS modifier”.

## در حال بارگذاری {#totals-auto-threshold}

نگهبان `totals_mode = 'auto'`.
بخش را ببینید “WITH TOTALS modifier”.

## بیشینه\_راپرال\_راپیکال {#settings-max_parallel_replicas}

حداکثر تعداد کپی برای هر سفال در هنگام اجرای یک پرس و جو.
برای سازگاری (برای دریافت بخش های مختلف از تقسیم داده های مشابه), این گزینه تنها کار می کند زمانی که کلید نمونه گیری قرار است.
تاخیر المثنی کنترل نمی شود.

## کامپایل {#compile}

فعال کردن مجموعه ای از نمایش داده شد. به طور پیش فرض, 0 (غیر فعال).

تدوین تنها برای بخشی از خط لوله پرس و جو پردازش استفاده می شود: برای مرحله اول تجمع (گروه های).
اگر این بخش از خط لوله وارد شده بود, پرس و جو ممکن است سریع تر با توجه به استقرار چرخه های کوتاه و محدود تماس تابع جمع اجرا. حداکثر بهبود عملکرد (تا چهار برابر سریعتر در موارد نادر) برای نمایش داده شد با توابع چند دانه ساده دیده می شود. معمولا افزایش عملکرد ناچیز است. در موارد بسیار نادر, ممکن است کم کردن سرعت اجرای پرس و جو.

## \_کوچکنمایی {#min-count-to-compile}

چند بار به طور بالقوه استفاده از یک تکه وارد شده از کد قبل از در حال اجرا تلفیقی. به طور پیش فرض, 3.
For testing, the value can be set to 0: compilation runs synchronously and the query waits for the end of the compilation process before continuing execution. For all other cases, use values ​​starting with 1. Compilation normally takes about 5-10 seconds.
اگر مقدار است 1 یا بیشتر, تلفیقی ناهمگام در یک موضوع جداگانه رخ می دهد. نتیجه به محض این که حاضر است از جمله نمایش داده شد که در حال حاضر در حال اجرا استفاده می شود.

کد کامپایل شده برای هر ترکیب های مختلف از توابع کل مورد استفاده در پرس و جو و نوع کلید در گروه بند مورد نیاز است.
The results of the compilation are saved in the build directory in the form of .so files. There is no restriction on the number of compilation results since they don't use very much space. Old results will be used after server restarts, except in the case of a server upgrade – in this case, the old results are deleted.

## خروجی \_فرمان\_جسون\_کوات\_64بیت\_تنظیمی {#session_settings-output_format_json_quote_64bit_integers}

اگر مقدار درست است صحیح به نظر می رسد در نقل قول ها در هنگام استفاده از JSON\* Int64 و UInt64 فرمت (برای سازگاری بیشتر با جاوا اسکریپت پیاده سازی); در غیر این صورت اعداد صحیح هستند و خروجی بدون نقل قول.

## \_مخفی کردن \_قابلیت \_جدید {#settings-format_csv_delimiter}

شخصیت به عنوان یک جداساز در داده های سی سی. وی تفسیر شده است. به طور پیش فرض, جداساز است `,`.

## \_فرستادن به \_کوچکنمایی {#settings-input_format_csv_unquoted_null_literal_as_null}

برای فرمت ورودی سی اس وی را قادر می سازد و یا غیر فعال تجزیه بدون نقل `NULL` به عنوان تحت اللفظی (مترادف برای `\N`).

## \_انتقال به \_شروع مجدد {#settings-output-format-csv-crlf-end-of-line}

استفاده از داس/ویندوز-سبک خط جدا کننده (CRLF) در CSV به جای یونیکس سبک (LF).

## \_فرستادن در\_م\_مایش از \_برخط {#settings-output-format-tsv-crlf-end-of-line}

استفاده از توضیحات / ویندوز به سبک خط جدا (سازمان تنظیم مقررات) در واحد پشتیبانی فنی فنی فنی مهندسی به جای سبک یونیکس.

## \_معامله {#settings-insert_quorum}

را قادر می سازد حد نصاب می نویسد.

-   اگر `insert_quorum < 2`, حد نصاب می نویسد غیر فعال هستند.
-   اگر `insert_quorum >= 2`, حد نصاب می نویسد فعال هستند.

مقدار پیش فرض: 0.

حد نصاب می نویسد

`INSERT` موفق تنها زمانی که تاتر موفق به درستی ارسال داده ها به `insert_quorum` از کپی در طول `insert_quorum_timeout`. اگر به هر دلیلی تعدادی از کپی با موفق می نویسد می کند از دسترس نیست `insert_quorum` نوشتن در نظر گرفته شده است شکست خورده و خانه را حذف بلوک قرار داده شده از تمام کپی که داده ها در حال حاضر نوشته شده است.

همه تکرار در حد نصاب سازگار هستند, به عنوان مثال, حاوی اطلاعات از همه قبلی `INSERT` نمایش داده شد. این `INSERT` دنباله خطی است.

هنگام خواندن داده های نوشته شده از `insert_quorum` شما می توانید از [مورد احترام](#settings-select_sequential_consistency) انتخاب

تاتر تولید یک استثنا

-   اگر تعداد کپی های موجود در زمان پرس و جو کمتر از `insert_quorum`.
-   در تلاش برای نوشتن داده ها زمانی که بلوک قبلی هنوز در وارد نشده است `insert_quorum` از کپی. این وضعیت ممکن است رخ دهد اگر کاربر تلاش می کند برای انجام یک `INSERT` قبل از قبلی با `insert_quorum` کامل شده است.

همچنین نگاه کنید به:

-   [\_بههنگامسازی](#settings-insert_quorum_timeout)
-   [مورد احترام](#settings-select_sequential_consistency)

## \_بههنگامسازی {#settings-insert_quorum_timeout}

ارسال به فاصله حد نصاب در ثانیه. اگر ایست را تصویب کرده است و بدون نوشتن صورت گرفته است و در عین حال, تاتر یک استثنا تولید و مشتری باید پرس و جو تکرار برای نوشتن همان بلوک به همان و یا هر ماکت دیگر.

مقدار پیش فرض: 60 ثانیه.

همچنین نگاه کنید به:

-   [\_معامله](#settings-insert_quorum)
-   [مورد احترام](#settings-select_sequential_consistency)

## مورد احترام {#settings-select_sequential_consistency}

قوام متوالی را فعال یا غیرفعال می کند `SELECT` نمایش داده شد:

مقادیر ممکن:

-   0 — Disabled.
-   1 — Enabled.

مقدار پیش فرض: 0.

استفاده

هنگامی که قوام پی در پی فعال است, تاتر اجازه می دهد تا مشتری برای اجرای `SELECT` پرس و جو فقط برای کسانی که کپی که حاوی داده ها از همه قبلی `INSERT` نمایش داده شد اجرا با `insert_quorum`. اگر مشتری اشاره به یک ماکت بخشی, تاتر یک استثنا تولید. پرس و جو را انتخاب کنید داده است که هنوز به حد نصاب کپی نوشته نشده است را شامل نمی شود.

همچنین نگاه کنید به:

-   [\_معامله](#settings-insert_quorum)
-   [\_بههنگامسازی](#settings-insert_quorum_timeout)

## \_تنظیم مجدد به حالت اولیه {#settings-insert-deduplicate}

امکان حذف یا غیرفعال کردن مسدود کردن تقسیم بندی `INSERT` (برای تکرار\* جداول).

مقادیر ممکن:

-   0 — Disabled.
-   1 — Enabled.

مقدار پیش فرض: 1.

به طور پیش فرض بلوک ها به جداول تکرار شده توسط `INSERT` بیانیه تقسیم شده است (نگاه کنید به [تکرار داده ها](../../engines/table-engines/mergetree-family/replication.md)).

## دریافت حسابهای کاربری دستگاه {#settings-deduplicate-blocks-in-dependent-materialized-views}

را قادر می سازد و یا غیر فعال بررسی تقسیم بندی برای نمایش محقق که دریافت داده ها از تکرار\* جداول.

مقادیر ممکن:

      0 — Disabled.
      1 — Enabled.

مقدار پیش فرض: 0.

استفاده

به طور پیش فرض, تقسیم بندی برای نمایش تحقق انجام نشده است اما بالادست انجام, در جدول منبع.
اگر یک بلوک قرار داده شده است به دلیل تقسیم بندی در جدول منبع قلم, وجود خواهد داشت بدون درج به نمایش مواد متصل. این رفتار وجود دارد برای فعال کردن درج داده ها بسیار جمع به نمایش محقق, برای مواردی که بلوک های قرار داده شده همان پس از تجمع مشاهده محقق اما مشتق شده از درج های مختلف را به جدول منبع.
همزمان, این رفتار “breaks” `INSERT` حق تقدم. اگر یک `INSERT` به جدول اصلی موفق بود و `INSERT` into a materialized view failed (e.g. because of communication failure with Zookeeper) a client will get an error and can retry the operation. However, the materialized view won't receive the second insert because it will be discarded by deduplication in the main (source) table. The setting `deduplicate_blocks_in_dependent_materialized_views` اجازه می دهد تا برای تغییر این رفتار. در تلاش مجدد, یک دیدگاه محقق درج تکرار دریافت خواهد کرد و بررسی تکرار به خودی خود انجام,
نادیده گرفتن نتیجه چک برای جدول منبع, و ردیف به دلیل شکست اول از دست داده وارد.

## ویژ\_گیها {#settings-max-network-bytes}

محدودیت حجم داده ها (به بایت) است که دریافت و یا انتقال بر روی شبکه در هنگام اجرای یک پرس و جو. این تنظیم در مورد هر پرس و جو فردی.

مقادیر ممکن:

-   عدد صحیح مثبت.
-   0 — Data volume control is disabled.

مقدار پیش فرض: 0.

## \_عرض {#settings-max-network-bandwidth}

محدودیت سرعت تبادل داده ها بر روی شبکه در بایت در هر ثانیه. این تنظیم در مورد هر پرس و جو.

مقادیر ممکن:

-   عدد صحیح مثبت.
-   0 — Bandwidth control is disabled.

مقدار پیش فرض: 0.

## \_شمارهگیر بیشینه {#settings-max-network-bandwidth-for-user}

محدودیت سرعت تبادل داده ها بر روی شبکه در بایت در هر ثانیه. این تنظیم به تمام نمایش داده شد همزمان در حال اجرا انجام شده توسط یک کاربر اعمال می شود.

مقادیر ممکن:

-   عدد صحیح مثبت.
-   0 — Control of the data speed is disabled.

مقدار پیش فرض: 0.

## \_شمارهگیرها {#settings-max-network-bandwidth-for-all-users}

محدودیت سرعت است که داده ها در بیش از شبکه در بایت در هر ثانیه رد و بدل. این تنظیم در مورد تمام نمایش داده شد به صورت همزمان در حال اجرا بر روی سرور.

مقادیر ممکن:

-   عدد صحیح مثبت.
-   0 — Control of the data speed is disabled.

مقدار پیش فرض: 0.

## ا\_فزونهها {#settings-count_distinct_implementation}

مشخص می کند که کدام یک از `uniq*` توابع باید برای انجام [COUNT(DISTINCT …)](../../sql-reference/aggregate-functions/reference.md#agg_function-count) ساخت و ساز.

مقادیر ممکن:

-   [دانشگاه](../../sql-reference/aggregate-functions/reference.md#agg_function-uniq)
-   [مخلوط نشده](../../sql-reference/aggregate-functions/reference.md#agg_function-uniqcombined)
-   [نیم قرن 64](../../sql-reference/aggregate-functions/reference.md#agg_function-uniqcombined64)
-   [یونقلل12](../../sql-reference/aggregate-functions/reference.md#agg_function-uniqhll12)
-   [قرارداد اتحادیه](../../sql-reference/aggregate-functions/reference.md#agg_function-uniqexact)

مقدار پیشفرض: `uniqExact`.

## در حال بارگذاری {#settings-skip_unavailable_shards}

را قادر می سازد و یا غیر فعال سکوت پرش از خرده ریز در دسترس نیست.

سفال در دسترس نیست در نظر گرفته اگر همه کپی خود را در دسترس نیست. ماکت در موارد زیر در دسترس نیست:

-   تاتر نمی تواند به هر دلیلی به ماکت متصل شود.

    هنگام اتصال به یک ماکت, تاتر انجام چندین تلاش. اگر تمام این تلاش شکست, ماکت در دسترس نیست در نظر گرفته شده است.

-   بدل نمی تواند از طریق دی ان اس حل شود.

    اگر نام میزبان ماکت را نمی توان از طریق دی ان اس حل و فصل, می تواند شرایط زیر نشان می دهد:

    -   میزبان ماکت هیچ سابقه دی ان اس. این می تواند در سیستم های با دی ان اس پویا رخ می دهد, مثلا, [کوبرنتس](https://kubernetes.io), جایی که گره می تواند در طول خرابی قابل حل, و این یک خطا نیست.

    -   خطای پیکربندی. فایل پیکربندی کلیک شامل یک نام میزبان اشتباه است.

مقادیر ممکن:

-   1 — skipping enabled.

    اگر یک سفال در دسترس نیست, خانه را برمی گرداند در نتیجه بر اساس داده های بخشی می کند و مشکلات در دسترس بودن گره گزارش نمی.

-   0 — skipping disabled.

    اگر یک سفال در دسترس نیست, تاتر می اندازد یک استثنا.

مقدار پیش فرض: 0.

## افراد زیر در این افزونه مشارکت کردهاند {#settings-optimize_skip_unused_shards}

فعال یا غیر فعال پرش استفاده نشده خرده ریز برای انتخاب نمایش داده شد که sharding شرط کلیدی در PREWHERE/که در آن (به فرض که داده ها توزیع شده است sharding کلیدی در غیر این صورت هیچ چیز).

مقدار پیشفرض: 0

## افراد زیر در این افزونه مشارکت کردهاند {#settings-force_optimize_skip_unused_shards}

فعال یا غیرفعال اجرای پرس و جو اگر [`optimize_skip_unused_shards`](#settings-optimize_skip_unused_shards) فعال و پرش از خرده ریز استفاده نشده امکان پذیر نیست. اگر پرش امکان پذیر نیست و تنظیمات را فعال کنید استثنا پرتاب خواهد شد.

مقادیر ممکن:

-   0-معلول (نمی اندازد)
-   1-غیر فعال کردن اجرای پرس و جو تنها در صورتی که جدول دارای کلید شارژ
-   2-غیر فعال کردن اجرای پرس و جو بدون در نظر گرفتن کلید شاردینگ برای جدول تعریف شده است

مقدار پیشفرض: 0

## ا\_فزون\_ف\_کوپ {#setting-optimize_throw_if_noop}

را قادر می سازد و یا غیر فعال پرتاب یک استثنا اگر یک [OPTIMIZE](../../sql-reference/statements/misc.md#misc_operations-optimize) پرس و جو یک ادغام انجام نمی.

به طور پیش فرض, `OPTIMIZE` حتی اگر هیچ کاری انجام نمی دهد با موفقیت باز می گردد. این تنظیم به شما اجازه می دهد تا این شرایط را متمایز کنید و دلیل را در یک پیام استثنا دریافت کنید.

مقادیر ممکن:

-   1 — Throwing an exception is enabled.
-   0 — Throwing an exception is disabled.

مقدار پیش فرض: 0.

## در حال بارگذاری {#settings-distributed_replica_error_half_life}

-   نوع: ثانیه
-   مقدار پیشفرض: 60 ثانیه

کنترل خطاهای چگونه سریع در جداول توزیع صفر. اگر یک ماکت برای برخی از زمان در دسترس نیست, تجمع می یابد 5 اشتباهات, و توزیع \_راپیرار\_لفا\_لایف تنظیم شده است 1 دوم, سپس ماکت در نظر گرفته شده است طبیعی 3 ثانیه پس از خطا گذشته.

همچنین نگاه کنید به:

-   [موتور جدول توزیع شده است](../../engines/table-engines/special/distributed.md)
-   [نمایش سایت](#settings-distributed_replica_error_cap)

## نمایش سایت {#settings-distributed_replica_error_cap}

-   نوع: امضا نشده
-   مقدار پیش فرض: 1000

تعداد خطا از هر ماکت است که در این مقدار پوش, جلوگیری از یک ماکت از تجمع بیش از حد بسیاری از اشتباهات.

همچنین نگاه کنید به:

-   [موتور جدول توزیع شده است](../../engines/table-engines/special/distributed.md)
-   [در حال بارگذاری](#settings-distributed_replica_error_half_life)

## در حال بارگذاری {#distributed_directory_monitor_sleep_time_ms}

فاصله پایه برای [توزیع شده](../../engines/table-engines/special/distributed.md) موتور جدول برای ارسال داده ها. فاصله واقعی نمایی رشد می کند در صورت خطا.

مقادیر ممکن:

-   عدد صحیح مثبت میلی ثانیه.

مقدار پیش فرض: 100 میلی ثانیه.

## در حال بارگذاری {#distributed_directory_monitor_max_sleep_time_ms}

حداکثر فاصله برای [توزیع شده](../../engines/table-engines/special/distributed.md) موتور جدول برای ارسال داده ها. محدودیت رشد نمایی از فاصله تعیین شده در [در حال بارگذاری](#distributed_directory_monitor_sleep_time_ms) تنظیمات.

مقادیر ممکن:

-   عدد صحیح مثبت میلی ثانیه.

مقدار پیش فرض: 30000 میلی ثانیه (30 ثانیه).

## نمایش سایت {#distributed_directory_monitor_batch_inserts}

را قادر می سازد/غیر فعال ارسال داده های درج شده در دسته.

هنگام ارسال دسته ای فعال است [توزیع شده](../../engines/table-engines/special/distributed.md) موتور جدول تلاش می کند چندین فایل داده های درج شده را در یک عملیات به جای ارسال جداگانه ارسال کند. دسته ای ارسال را بهبود می بخشد عملکرد خوشه با استفاده بهتر از سرور و شبکه منابع.

مقادیر ممکن:

-   1 — Enabled.
-   0 — Disabled.

مقدار پیش فرض: 0.

## \_شخصیت {#setting-os-thread-priority}

اولویت را تنظیم می کند ([خوبه](https://en.wikipedia.org/wiki/Nice_(Unix))) برای موضوعات که نمایش داده شد را اجرا کند . زمانبندی سیستم عامل این اولویت در نظر هنگام انتخاب موضوع بعدی به اجرا در هر هسته پردازنده در دسترس است.

!!! warning "اخطار"
    برای استفاده از این تنظیم, شما نیاز به تنظیم `CAP_SYS_NICE` توان. این `clickhouse-server` بسته بندی در هنگام نصب تنظیم می شود. برخی از محیط های مجازی اجازه نمی دهد که شما را به مجموعه `CAP_SYS_NICE` توان. در این مورد, `clickhouse-server` در ابتدا پیامی در موردش نشان می دهد.

مقادیر ممکن:

-   شما می توانید مقادیر را در محدوده تنظیم کنید `[-20, 19]`.

مقادیر پایین تر به معنای اولویت بالاتر است. رشتهها با کم `nice` مقادیر اولویت اغلب از موضوعات با ارزش های بالا اجرا می شود. مقادیر بالا برای پرس و جو های غیر تعاملی طولانی تر ترجیح داده می شود زیرا اجازه می دهد تا به سرعت منابع را به نفع نمایش های تعاملی کوتاه در هنگام ورود به دست بدهند.

مقدار پیش فرض: 0.

## جستجو {#query_profiler_real_time_period_ns}

دوره را برای یک تایمر ساعت واقعی تنظیم می کند [پروفیل پرس و جو](../../operations/optimizing-performance/sampling-query-profiler.md). تایمر ساعت واقعی شمارش زمان دیوار ساعت.

مقادیر ممکن:

-   عدد صحیح مثبت در nanoseconds.

    مقادیر توصیه شده:

            - 10000000 (100 times a second) nanoseconds and less for single queries.
            - 1000000000 (once a second) for cluster-wide profiling.

-   0 برای خاموش کردن تایمر.

نوع: [UInt64](../../sql-reference/data-types/int-uint.md).

مقدار پیش فرض: 1000000000 نانو ثانیه (یک بار در ثانیه).

همچنین نگاه کنید به:

-   جدول سیستم [\_قطع](../../operations/system-tables.md#system_tables-trace_log)

## ایران در تهران {#query_profiler_cpu_time_period_ns}

دوره را برای تایمر ساعت پردازنده تنظیم می کند [پروفیل پرس و جو](../../operations/optimizing-performance/sampling-query-profiler.md). این تایمر شمارش تنها زمان پردازنده.

مقادیر ممکن:

-   عدد صحیح مثبت نانو ثانیه.

    مقادیر توصیه شده:

            - 10000000 (100 times a second) nanoseconds and more for single queries.
            - 1000000000 (once a second) for cluster-wide profiling.

-   0 برای خاموش کردن تایمر.

نوع: [UInt64](../../sql-reference/data-types/int-uint.md).

مقدار پیش فرض: 1000000000 نانو ثانیه.

همچنین نگاه کنید به:

-   جدول سیستم [\_قطع](../../operations/system-tables.md#system_tables-trace_log)

## اجازه دادن به \_فعال کردن اختلال در عملکرد {#settings-allow_introspection_functions}

فعالسازی از کارانداختن [توابع درون گونه](../../sql-reference/functions/introspection.md) برای پروفایل پرس و جو.

مقادیر ممکن:

-   1 — Introspection functions enabled.
-   0 — Introspection functions disabled.

مقدار پیش فرض: 0.

**همچنین نگاه کنید به**

-   [پروفایل پرس و جو نمونه برداری](../optimizing-performance/sampling-query-profiler.md)
-   جدول سیستم [\_قطع](../../operations/system-tables.md#system_tables-trace_log)

## وارد\_فرمت\_پارلل\_درپارس {#input-format-parallel-parsing}

-   نوع: بولی
-   مقدار پیشفرض: درست

فعال کردن نظم حفظ تجزیه موازی از فرمت های داده. پشتیبانی تنها برای TSV TKSV CSV و JSONEachRow فرمت های.

## \_حداقل کردن \_بیتس\_برای\_پرال\_درپارس {#min-chunk-bytes-for-parallel-parsing}

-   نوع: امضا نشده
-   مقدار پیشفرض: 1 مگابایت

حداقل اندازه تکه در بایت, که هر موضوع به صورت موازی تجزیه خواهد شد.

## \_فرماندگی لبه بام {#settings-output_format_avro_codec}

مجموعه کدک فشرده سازی مورد استفاده برای خروجی فایل اورو.

نوع: رشته

مقادیر ممکن:

-   `null` — No compression
-   `deflate` — Compress with Deflate (zlib)
-   `snappy` — Compress with [روح](https://google.github.io/snappy/)

مقدار پیشفرض: `snappy` (در صورت موجود بودن) یا `deflate`.

## \_فرماندگی لبه چشم {#settings-output_format_avro_sync_interval}

مجموعه حداقل اندازه داده (در بایت) بین نشانگر هماهنگ سازی برای فایل خروجی هواپیما.

نوع: امضا نشده

مقادیر ممکن: 32 (32 بایت) - 1073741824 (1 دستگاه گوارش)

مقدار پیش فرض: 32768 (32 کیلوبایت)

## باز کردن \_نمایش مجدد {#settings-format_avro_schema_registry_url}

نشانی اینترنتی رجیستری طرحواره را برای استفاده تنظیم میکند [هشدار داده می شود](../../interfaces/formats.md#data-format-avro-confluent) قالب

نوع: نشانی وب

مقدار پیشفرض: خالی

## پس زمینه {#background_pool_size}

مجموعه تعدادی از موضوعات انجام عملیات پس زمینه در موتورهای جدول (مثلا, ادغام در [موتور ادغام](../../engines/table-engines/mergetree-family/index.md) جدول). این تنظیم در شروع سرور کلیک استفاده می شود و نمی تواند در یک جلسه کاربر تغییر کند. با تنظیم این تنظیم شما پردازنده و دیسک بار مدیریت. اندازه استخر کوچکتر با بهره گیری از پردازنده و دیسک منابع کمتر, اما فرایندهای پس زمینه پیشرفت کندتر که در نهایت ممکن است تاثیر عملکرد پرس و جو.

مقادیر ممکن:

-   هر عدد صحیح مثبت.

مقدار پیش فرض: 16.

[مقاله اصلی](https://clickhouse.tech/docs/en/operations/settings/settings/) <!-- hide -->
