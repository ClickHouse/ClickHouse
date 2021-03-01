---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 49
toc_title: "\u0637\u0648\u0644 \u062A\u0627\u0631\u06CC\u062E 64"
---

# طول تاریخ 64 {#data_type-datetime64}

اجازه می دهد تا برای ذخیره یک لحظه در زمان, است که می تواند به عنوان یک تاریخ تقویم و یک زمان از یک روز بیان, با دقت زیر دوم تعریف شده

اندازه تیک (دقت): 10<sup>- دقت</sup> ثانیه

نحو:

``` sql
DateTime64(precision, [timezone])
```

داخلی, ذخیره داده ها به عنوان تعدادی از ‘ticks’ از عصر شروع (1970-01-01 00:00:00 یو تی سی) به عنوان اینترنشنال64. وضوح تیک توسط پارامتر دقیق تعیین می شود. علاوه بر این `DateTime64` نوع می توانید منطقه زمانی است که همین کار را برای کل ستون ذخیره, که تحت تاثیر قرار چگونه ارزش های `DateTime64` مقادیر نوع در قالب متن نمایش داده می شود و چگونه مقادیر مشخص شده به عنوان رشته تجزیه می شوند (‘2020-01-01 05:00:01.000’). منطقه زمانی در ردیف جدول ذخیره نمی شود (و یا در نتیجه), اما در ابرداده ستون ذخیره می شود. مشاهده اطلاعات در [DateTime](datetime.md).

## مثالها {#examples}

**1.** ایجاد یک جدول با `DateTime64`- ستون را تایپ کنید و داده ها را وارد کنید:

``` sql
CREATE TABLE dt
(
    `timestamp` DateTime64(3, 'Europe/Moscow'),
    `event_id` UInt8
)
ENGINE = TinyLog
```

``` sql
INSERT INTO dt Values (1546300800000, 1), ('2019-01-01 00:00:00', 2)
```

``` sql
SELECT * FROM dt
```

``` text
┌───────────────timestamp─┬─event_id─┐
│ 2019-01-01 03:00:00.000 │        1 │
│ 2019-01-01 00:00:00.000 │        2 │
└─────────────────────────┴──────────┘
```

-   هنگام قرار دادن تاریخ ساعت به عنوان یک عدد صحیح, این است که به عنوان یک مناسب کوچک برچسب زمان یونیکس درمان (مجموعه مقالات). `1546300800000` (با دقت 3) نشان دهنده `'2019-01-01 00:00:00'` ادا کردن. با این حال, مانند `timestamp` ستون دارد `Europe/Moscow` (مجموعه مقالات+3) منطقه زمانی مشخص, در هنگام خروجی به عنوان یک رشته ارزش خواهد شد به عنوان نشان داده شده است `'2019-01-01 03:00:00'`
-   هنگام قرار دادن مقدار رشته به عنوان تاریخ ساعت, این است که به عنوان بودن در منطقه زمانی ستون درمان. `'2019-01-01 00:00:00'` خواهد شد به عنوان در درمان `Europe/Moscow` منطقه زمانی و ذخیره شده به عنوان `1546290000000`.

**2.** پالایش بر روی `DateTime64` مقادیر

``` sql
SELECT * FROM dt WHERE timestamp = toDateTime64('2019-01-01 00:00:00', 3, 'Europe/Moscow')
```

``` text
┌───────────────timestamp─┬─event_id─┐
│ 2019-01-01 00:00:00.000 │        2 │
└─────────────────────────┴──────────┘
```

برخلاف `DateTime`, `DateTime64` ارزش ها از تبدیل نمی `String` به طور خودکار

**3.** گرفتن یک منطقه زمانی برای یک `DateTime64`- مقدار نوع:

``` sql
SELECT toDateTime64(now(), 3, 'Europe/Moscow') AS column, toTypeName(column) AS x
```

``` text
┌──────────────────column─┬─x──────────────────────────────┐
│ 2019-10-16 04:12:04.000 │ DateTime64(3, 'Europe/Moscow') │
└─────────────────────────┴────────────────────────────────┘
```

**4.** تبدیل منطقه زمانی

``` sql
SELECT
toDateTime64(timestamp, 3, 'Europe/London') as lon_time,
toDateTime64(timestamp, 3, 'Europe/Moscow') as mos_time
FROM dt
```

``` text
┌───────────────lon_time──┬────────────────mos_time─┐
│ 2019-01-01 00:00:00.000 │ 2019-01-01 03:00:00.000 │
│ 2018-12-31 21:00:00.000 │ 2019-01-01 00:00:00.000 │
└─────────────────────────┴─────────────────────────┘
```

## همچنین نگاه کنید به {#see-also}

-   [توابع تبدیل نوع](../../sql-reference/functions/type-conversion-functions.md)
-   [توابع برای کار با تاریخ و زمان](../../sql-reference/functions/date-time-functions.md)
-   [توابع برای کار با ارریس](../../sql-reference/functions/array-functions.md)
-   [این `date_time_input_format` تنظیم](../../operations/settings/settings.md#settings-date_time_input_format)
-   [این `timezone` پارامتر پیکربندی سرور](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone)
-   [اپراتورها برای کار با تاریخ و زمان](../../sql-reference/operators/index.md#operators-datetime)
-   [`Date` نوع داده](date.md)
-   [`DateTime` نوع داده](datetime.md)
