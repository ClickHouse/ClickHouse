---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 61
toc_title: "\u0641\u0627\u0635\u0644\u0647"
---

# فاصله {#data-type-interval}

خانواده از انواع داده ها به نمایندگی از فواصل زمان و تاریخ. انواع حاصل از [INTERVAL](../../../sql-reference/operators/index.md#operator-interval) اپراتور

!!! warning "اخطار"
    `Interval` مقادیر نوع داده را نمی توان در جداول ذخیره کرد.

ساختار:

-   فاصله زمانی به عنوان یک مقدار عدد صحیح بدون علامت.
-   نوع فاصله.

انواع فاصله پشتیبانی شده:

-   `SECOND`
-   `MINUTE`
-   `HOUR`
-   `DAY`
-   `WEEK`
-   `MONTH`
-   `QUARTER`
-   `YEAR`

برای هر نوع فاصله, یک نوع داده جداگانه وجود دارد. برای مثال `DAY` فاصله مربوط به `IntervalDay` نوع داده:

``` sql
SELECT toTypeName(INTERVAL 4 DAY)
```

``` text
┌─toTypeName(toIntervalDay(4))─┐
│ IntervalDay                  │
└──────────────────────────────┘
```

## اظهارات طریقه استفاده {#data-type-interval-usage-remarks}

شما می توانید استفاده کنید `Interval`- ارزش نوع در عملیات ریاضی با [تاریخ](../../../sql-reference/data-types/date.md) و [DateTime](../../../sql-reference/data-types/datetime.md)- ارزش نوع . مثلا, شما می توانید اضافه کنید 4 روز به زمان فعلی:

``` sql
SELECT now() as current_date_time, current_date_time + INTERVAL 4 DAY
```

``` text
┌───current_date_time─┬─plus(now(), toIntervalDay(4))─┐
│ 2019-10-23 10:58:45 │           2019-10-27 10:58:45 │
└─────────────────────┴───────────────────────────────┘
```

فواصل با انواع مختلف نمی تواند ترکیب شود. شما می توانید فواصل مانند استفاده کنید `4 DAY 1 HOUR`. مشخص فواصل در واحد است که کوچکتر یا برابر با کوچکترین واحد فاصله, مثلا, فاصله `1 day and an hour` فاصله را می توان به عنوان بیان شده است `25 HOUR` یا `90000 SECOND`.

شما می توانید عملیات ریاضی با انجام نمی `Interval`- ارزش نوع, اما شما می توانید فواصل از انواع مختلف در نتیجه به ارزش در اضافه `Date` یا `DateTime` انواع داده ها. به عنوان مثال:

``` sql
SELECT now() AS current_date_time, current_date_time + INTERVAL 4 DAY + INTERVAL 3 HOUR
```

``` text
┌───current_date_time─┬─plus(plus(now(), toIntervalDay(4)), toIntervalHour(3))─┐
│ 2019-10-23 11:16:28 │                                    2019-10-27 14:16:28 │
└─────────────────────┴────────────────────────────────────────────────────────┘
```

پرس و جو زیر باعث یک استثنا:

``` sql
select now() AS current_date_time, current_date_time + (INTERVAL 4 DAY + INTERVAL 3 HOUR)
```

``` text
Received exception from server (version 19.14.1):
Code: 43. DB::Exception: Received from localhost:9000. DB::Exception: Wrong argument types for function plus: if one argument is Interval, then another must be Date or DateTime..
```

## همچنین نگاه کنید به {#see-also}

-   [INTERVAL](../../../sql-reference/operators/index.md#operator-interval) اپراتور
-   [توینتروال](../../../sql-reference/functions/type-conversion-functions.md#function-tointerval) توابع تبدیل نوع
