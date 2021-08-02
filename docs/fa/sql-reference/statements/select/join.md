---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
---

# پیوستن بند {#select-join}

اضافه کردن تولید یک جدول جدید با ترکیب ستون از یک یا چند جدول با استفاده از مقادیر مشترک به هر. این یک عملیات مشترک در پایگاه داده با پشتیبانی از گذاشتن است, که مربوط به [جبر رابطه ای](https://en.wikipedia.org/wiki/Relational_algebra#Joins_and_join-like_operators) ملحق شو مورد خاص یک جدول پیوستن است که اغلب به عنوان اشاره “self-join”.

نحو:

``` sql
SELECT <expr_list>
FROM <left_table>
[GLOBAL] [INNER|LEFT|RIGHT|FULL|CROSS] [OUTER|SEMI|ANTI|ANY|ASOF] JOIN <right_table>
(ON <expr_list>)|(USING <column_list>) ...
```

عبارات از `ON` بند و ستون از `USING` بند نامیده می شوند “join keys”. مگر در مواردی که در غیر این صورت اعلام, پیوستن به تولید یک [محصول دکارتی](https://en.wikipedia.org/wiki/Cartesian_product) از ردیف با تطبیق “join keys”, که ممکن است نتایج را با ردیف خیلی بیشتر از جداول منبع تولید.

## انواع پشتیبانی شده از پیوستن {#select-join-types}

همه استاندارد [SQL JOIN](https://en.wikipedia.org/wiki/Join_(SQL)) انواع پشتیبانی می شوند:

-   `INNER JOIN`, تنها ردیف تطبیق برگردانده می شوند.
-   `LEFT OUTER JOIN`, ردیف غیر تطبیق از جدول سمت چپ علاوه بر تطبیق ردیف بازگشت.
-   `RIGHT OUTER JOIN`, ردیف غیر تطبیق از جدول سمت چپ علاوه بر تطبیق ردیف بازگشت.
-   `FULL OUTER JOIN`, ردیف غیر تطبیق از هر دو جدول علاوه بر تطبیق ردیف بازگشت.
-   `CROSS JOIN`, تولید محصول دکارتی از تمام جداول, “join keys” هستند **نه** مشخص.

`JOIN` بدون نوع مشخص نشان میدهد `INNER`. کلیدواژه `OUTER` می توان با خیال راحت حذف. نحو جایگزین برای `CROSS JOIN` مشخص جداول چندگانه در [از بند](from.md) جدا شده توسط کاما.

پیوستن به انواع اضافی موجود در کلیک:

-   `LEFT SEMI JOIN` و `RIGHT SEMI JOIN`, یک لیست سفید در “join keys”, بدون تولید محصول دکارتی.
-   `LEFT ANTI JOIN` و `RIGHT ANTI JOIN`, لیست سیاه در “join keys”, بدون تولید محصول دکارتی.
-   `LEFT ANY JOIN`, `RIGHT ANY JOIN` و `INNER ANY JOIN`, partially (for opposite side of `LEFT` and `RIGHT`) or completely (for `INNER` and `FULL`) disables the cartesian product for standard `JOIN` types.
-   `ASOF JOIN` و `LEFT ASOF JOIN`, joining sequences with a non-exact match. `ASOF JOIN` usage is described below.

## Setting {#join-settings}

تغییر چگونگی تطبیق توسط “join keys” انجام شده است
!!! note "یادداشت"
    مقدار سختگیرانه پیش فرض را می توان با استفاده از لغو [بررسی اجمالی](../../../operations/settings/settings.md#settings-join_default_strictness) تنظیمات.

### از این رو پیوستن به استفاده {#asof-join-usage}

`ASOF JOIN` مفید است زمانی که شما نیاز به پیوستن به سوابق که هیچ بازی دقیق.

جداول برای `ASOF JOIN` باید یک ستون توالی دستور داده اند. این ستون نمی تواند به تنهایی در یک جدول باشد و باید یکی از انواع داده ها باشد: `UInt32`, `UInt64`, `Float32`, `Float64`, `Date` و `DateTime`.

نحو `ASOF JOIN ... ON`:

``` sql
SELECT expressions_list
FROM table_1
ASOF LEFT JOIN table_2
ON equi_cond AND closest_match_cond
```

شما می توانید هر تعداد از شرایط برابری و دقیقا یکی از نزدیک ترین شرایط بازی استفاده کنید. به عنوان مثال, `SELECT count() FROM table_1 ASOF LEFT JOIN table_2 ON table_1.a == table_2.b AND table_2.t <= table_1.t`.

شرایط پشتیبانی شده برای نزدیک ترین بازی: `>`, `>=`, `<`, `<=`.

نحو `ASOF JOIN ... USING`:

``` sql
SELECT expressions_list
FROM table_1
ASOF JOIN table_2
USING (equi_column1, ... equi_columnN, asof_column)
```

`ASOF JOIN` استفاده `equi_columnX` برای پیوستن به برابری و `asof_column` برای پیوستن به نزدیک ترین مسابقه با `table_1.asof_column >= table_2.asof_column` شرط. این `asof_column` ستون همیشه یکی از گذشته در `USING` بند بند.

مثلا, جداول زیر را در نظر بگیرید:

         table_1                           table_2
      event   | ev_time | user_id       event   | ev_time | user_id
    ----------|---------|----------   ----------|---------|----------
                  ...                               ...
    event_1_1 |  12:00  |  42         event_2_1 |  11:59  |   42
                  ...                 event_2_2 |  12:30  |   42
    event_1_2 |  13:00  |  42         event_2_3 |  13:00  |   42
                  ...                               ...

`ASOF JOIN` می توانید برچسب زمان از یک رویداد کاربر از را `table_1` و پیدا کردن یک رویداد در `table_2` جایی که برچسب زمان نزدیک به زمان این رویداد از `table_1` مربوط به نزدیک ترین شرایط بازی. مقادیر برچسب زمان برابر نزدیک ترین در صورت موجود بودن. اینجا `user_id` ستون را می توان برای پیوستن به برابری و `ev_time` ستون را می توان برای پیوستن به در نزدیک ترین بازی استفاده می شود. در مثال ما, `event_1_1` می توان با پیوست `event_2_1` و `event_1_2` می توان با پیوست `event_2_3` اما `event_2_2` نمیشه عضو شد

!!! note "یادداشت"
    `ASOF` پیوستن است **نه** پردازشگر پشتیبانی شده: [پیوستن](../../../engines/table-engines/special/join.md) موتور جدول.

## توزیع پیوستن {#global-join}

دو راه برای اجرای پیوستن به جداول توزیع شده وجود دارد:

-   هنگام استفاده از نرمال `JOIN` پرس و جو به سرورهای راه دور ارسال می شود. زیرکریزها روی هر کدام اجرا می شوند تا میز مناسب را بسازند و پیوستن با این جدول انجام می شود. به عبارت دیگر, جدول سمت راست بر روی هر سرور تشکیل به طور جداگانه.
-   هنگام استفاده از `GLOBAL ... JOIN`, اول سرور درخواست کننده اجرا می شود یک خرده فروشی برای محاسبه جدول سمت راست. این جدول موقت به هر سرور از راه دور منتقل می شود و نمایش داده می شود با استفاده از داده های موقت منتقل می شود.

مراقب باشید در هنگام استفاده از `GLOBAL`. برای کسب اطلاعات بیشتر, دیدن [توزیع subqueries](../../operators/in.md#select-distributed-subqueries) بخش.

## توصیه های استفاده {#usage-recommendations}

### پردازش سلولهای خالی یا خالی {#processing-of-empty-or-null-cells}

در حالی که پیوستن به جداول سلول های خالی ظاهر می شود. تنظیمات [ارزشهای خبری عبارتند از:](../../../operations/settings/settings.md#join_use_nulls) تعریف چگونه خانه را پر می کند این سلول ها.

اگر `JOIN` کلید ها [Nullable](../../data-types/nullable.md) زمینه, ردیف که حداقل یکی از کلید های دارای ارزش [NULL](../../../sql-reference/syntax.md#null-literal) عضو نشده.

### نحو {#syntax}

ستون های مشخص شده در `USING` باید نام های مشابه در هر دو کارخانه های فرعی دارند, و ستون های دیگر باید متفاوت به نام. شما می توانید نام مستعار برای تغییر نام ستون ها در زیرکریز استفاده کنید.

این `USING` بند یک یا چند ستون برای پیوستن به مشخص, که ایجاد برابری این ستون. لیست ستون ها بدون براکت تنظیم شده است. شرایط پیوستن پیچیده تر پشتیبانی نمی شوند.

### محدودیت نحو {#syntax-limitations}

برای چند `JOIN` بند در یک `SELECT` پرسوجو:

-   گرفتن تمام ستون ها از طریق `*` در دسترس است تنها اگر جداول پیوست, نمی فرعی.
-   این `PREWHERE` بند در دسترس نیست.

برای `ON`, `WHERE` و `GROUP BY` بند:

-   عبارات دلخواه را نمی توان در `ON`, `WHERE` و `GROUP BY` بند, اما شما می توانید یک عبارت در یک تعریف `SELECT` بند و سپس در این بند از طریق یک نام مستعار استفاده کنید.

### عملکرد {#performance}

هنگامی که در حال اجرا `JOIN` بهینه سازی سفارش اعدام در رابطه با سایر مراحل پرس و جو وجود ندارد. پیوستن (جستجو در جدول سمت راست) قبل از فیلتر کردن در اجرا می شود `WHERE` و قبل از تجمع.

هر بار که پرس و جو با همان اجرا شود `JOIN`, خرده فروشی است دوباره اجرا به دلیل نتیجه ذخیره سازی نیست. برای جلوگیری از این, استفاده از ویژه [پیوستن](../../../engines/table-engines/special/join.md) موتور جدول, که مجموعه ای تهیه شده برای پیوستن است که همیشه در رم.

در بعضی موارد کارایی بیشتری برای استفاده دارد [IN](../../operators/in.md) به جای `JOIN`.

اگر شما نیاز به یک `JOIN` برای پیوستن به جداول بعد (این جداول نسبتا کوچک است که شامل خواص ابعاد هستند, مانند نام برای کمپین های تبلیغاتی), یک `JOIN` ممکن است بسیار مناسب با توجه به این واقعیت است که جدول سمت راست برای هر پرس و جو دوباره قابل دسترسی است. برای چنین مواردی وجود دارد “external dictionaries” ویژگی است که شما باید به جای استفاده از `JOIN`. برای کسب اطلاعات بیشتر, دیدن [واژهنامهها خارجی](../../dictionaries/external-dictionaries/external-dicts.md) بخش.

### محدودیت حافظه {#memory-limitations}

به طور پیش فرض, تاتر با استفاده از [هش پیوستن](https://en.wikipedia.org/wiki/Hash_join) الگوریتم. تاتر طول می کشد `<right_table>` و یک جدول هش را در رم ایجاد می کند. پس از برخی از حد مصرف حافظه, خانه رعیتی می افتد به ادغام پیوستن الگوریتم.

اگر شما نیاز به محدود کردن پیوستن به مصرف حافظه عملیات استفاده از تنظیمات زیر:

-   [\_پاک کردن \_روشن گرافیک](../../../operations/settings/query-complexity.md#settings-max_rows_in_join) — Limits number of rows in the hash table.
-   [\_پویش همیشگی](../../../operations/settings/query-complexity.md#settings-max_bytes_in_join) — Limits size of the hash table.

هنگامی که هر یک از این محدودیت رسیده است, کلیک به عنوان عمل می کند [\_شروع مجدد](../../../operations/settings/query-complexity.md#settings-join_overflow_mode) تنظیم دستور.

## مثالها {#examples}

مثال:

``` sql
SELECT
    CounterID,
    hits,
    visits
FROM
(
    SELECT
        CounterID,
        count() AS hits
    FROM test.hits
    GROUP BY CounterID
) ANY LEFT JOIN
(
    SELECT
        CounterID,
        sum(Sign) AS visits
    FROM test.visits
    GROUP BY CounterID
) USING CounterID
ORDER BY hits DESC
LIMIT 10
```

``` text
┌─CounterID─┬───hits─┬─visits─┐
│   1143050 │ 523264 │  13665 │
│    731962 │ 475698 │ 102716 │
│    722545 │ 337212 │ 108187 │
│    722889 │ 252197 │  10547 │
│   2237260 │ 196036 │   9522 │
│  23057320 │ 147211 │   7689 │
│    722818 │  90109 │  17847 │
│     48221 │  85379 │   4652 │
│  19762435 │  77807 │   7026 │
│    722884 │  77492 │  11056 │
└───────────┴────────┴────────┘
```
