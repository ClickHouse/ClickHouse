---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 48
toc_title: DateTime
---

# تاریخ ساعت {#data_type-datetime}

اجازه می دهد تا برای ذخیره یک لحظه در زمان, است که می تواند به عنوان یک تاریخ تقویم و یک زمان از یک روز بیان.

نحو:

``` sql
DateTime([timezone])
```

محدوده پشتیبانی شده از ارزش ها: \[1970-01-01 00:00:00, 2105-12-31 23:59:59\].

حل: 1 ثانیه.

## اظهارات طریقه استفاده {#usage-remarks}

نقطه در زمان به عنوان یک ذخیره می شود [برچسب زمان یونیکس](https://en.wikipedia.org/wiki/Unix_time), صرف نظر از منطقه زمانی و یا صرفه جویی در زمان نور روز. علاوه بر این `DateTime` نوع می توانید منطقه زمانی است که همین کار را برای کل ستون ذخیره, که تحت تاثیر قرار چگونه ارزش های `DateTime` مقادیر نوع در قالب متن نمایش داده می شود و چگونه مقادیر مشخص شده به عنوان رشته تجزیه می شوند (‘2020-01-01 05:00:01’). منطقه زمانی در ردیف جدول ذخیره نمی شود (و یا در نتیجه), اما در ابرداده ستون ذخیره می شود.
لیستی از مناطق زمانی پشتیبانی شده را می توان در [اانا پایگاه منطقه زمانی](https://www.iana.org/time-zones).
این `tzdata` بسته حاوی [اانا پایگاه منطقه زمانی](https://www.iana.org/time-zones), باید در سیستم نصب. استفاده از `timedatectl list-timezones` فرمان به لیست جغرافیایی شناخته شده توسط یک سیستم محلی.

شما به صراحت می توانید یک منطقه زمانی برای `DateTime`- ستون نوع در هنگام ایجاد یک جدول. اگر منطقه زمانی تنظیم نشده است, خانه رعیتی با استفاده از ارزش [منطقهی زمانی](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone) پارامتر در تنظیمات سرور و یا تنظیمات سیستم عامل در حال حاضر از شروع سرور کلیک.

این [کلیک مشتری](../../interfaces/cli.md) اعمال منطقه زمانی سرور به طور پیش فرض اگر یک منطقه زمانی است که به صراحت تنظیم نشده است که مقدار دهی اولیه نوع داده ها. برای استفاده از منطقه زمان مشتری اجرا کنید `clickhouse-client` با `--use_client_time_zone` پارامتر.

خروجی کلیک ارزش در `YYYY-MM-DD hh:mm:ss` قالب متن به طور پیش فرض. شما می توانید خروجی را با تغییر [formatDateTime](../../sql-reference/functions/date-time-functions.md#formatdatetime) تابع.

هنگام قرار دادن داده ها به تاتر, شما می توانید فرمت های مختلف تاریخ و زمان رشته استفاده, بسته به ارزش [تغییر \_شماره](../../operations/settings/settings.md#settings-date_time_input_format) تنظیمات.

## مثالها {#examples}

**1.** ایجاد یک جدول با یک `DateTime`- ستون را تایپ کنید و داده ها را وارد کنید:

``` sql
CREATE TABLE dt
(
    `timestamp` DateTime('Europe/Moscow'),
    `event_id` UInt8
)
ENGINE = TinyLog;
```

``` sql
INSERT INTO dt Values (1546300800, 1), ('2019-01-01 00:00:00', 2);
```

``` sql
SELECT * FROM dt;
```

``` text
┌───────────timestamp─┬─event_id─┐
│ 2019-01-01 03:00:00 │        1 │
│ 2019-01-01 00:00:00 │        2 │
└─────────────────────┴──────────┘
```

-   هنگام قرار دادن تاریخ ساعت به عنوان یک عدد صحیح, این است که به عنوان برچسب زمان یونیکس درمان (مجموعه مقالات). `1546300800` نشان دهنده `'2019-01-01 00:00:00'` ادا کردن. با این حال, مانند `timestamp` ستون دارد `Europe/Moscow` (مجموعه مقالات+3) منطقه زمانی مشخص, در هنگام خروجی به عنوان رشته ارزش خواهد شد به عنوان نشان داده شده است `'2019-01-01 03:00:00'`
-   هنگام قرار دادن مقدار رشته به عنوان تاریخ ساعت, این است که به عنوان بودن در منطقه زمانی ستون درمان. `'2019-01-01 00:00:00'` خواهد شد به عنوان در درمان `Europe/Moscow` منطقه زمانی و ذخیره به عنوان `1546290000`.

**2.** پالایش بر روی `DateTime` مقادیر

``` sql
SELECT * FROM dt WHERE timestamp = toDateTime('2019-01-01 00:00:00', 'Europe/Moscow')
```

``` text
┌───────────timestamp─┬─event_id─┐
│ 2019-01-01 00:00:00 │        2 │
└─────────────────────┴──────────┘
```

`DateTime` مقادیر ستون را می توان با استفاده از یک مقدار رشته در فیلتر `WHERE` مسندکردن. این تبدیل خواهد شد به `DateTime` به طور خودکار:

``` sql
SELECT * FROM dt WHERE timestamp = '2019-01-01 00:00:00'
```

``` text
┌───────────timestamp─┬─event_id─┐
│ 2019-01-01 03:00:00 │        1 │
└─────────────────────┴──────────┘
```

**3.** گرفتن یک منطقه زمانی برای یک `DateTime`- نوع ستون:

``` sql
SELECT toDateTime(now(), 'Europe/Moscow') AS column, toTypeName(column) AS x
```

``` text
┌──────────────column─┬─x─────────────────────────┐
│ 2019-10-16 04:12:04 │ DateTime('Europe/Moscow') │
└─────────────────────┴───────────────────────────┘
```

**4.** تبدیل منطقه زمانی

``` sql
SELECT
toDateTime(timestamp, 'Europe/London') as lon_time,
toDateTime(timestamp, 'Europe/Moscow') as mos_time
FROM dt
```

``` text
┌───────────lon_time──┬────────────mos_time─┐
│ 2019-01-01 00:00:00 │ 2019-01-01 03:00:00 │
│ 2018-12-31 21:00:00 │ 2019-01-01 00:00:00 │
└─────────────────────┴─────────────────────┘
```

## همچنین نگاه کنید به {#see-also}

-   [توابع تبدیل نوع](../../sql-reference/functions/type-conversion-functions.md)
-   [توابع برای کار با تاریخ و زمان](../../sql-reference/functions/date-time-functions.md)
-   [توابع برای کار با ارریس](../../sql-reference/functions/array-functions.md)
-   [این `date_time_input_format` تنظیم](../../operations/settings/settings.md#settings-date_time_input_format)
-   [این `timezone` پارامتر پیکربندی سرور](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone)
-   [اپراتورها برای کار با تاریخ و زمان](../../sql-reference/operators/index.md#operators-datetime)
-   [این `Date` نوع داده](date.md)

[مقاله اصلی](https://clickhouse.tech/docs/en/data_types/datetime/) <!--hide-->
