---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 54
toc_title: "\u067E\u0631\u0648\u0641\u0627\u06CC\u0644 \u067E\u0631\u0633 \u0648 \u062C\
  \u0648"
---

# پروفایل پرس و جو نمونه برداری {#sampling-query-profiler}

فاحشه خانه اجرا می شود نمونه برداری پیشفیلتر که اجازه می دهد تجزیه و تحلیل اجرای پرس و جو. با استفاده از نیمرخ شما می توانید روال کد منبع که اغلب در طول اجرای پرس و جو استفاده پیدا. شما می توانید زمان پردازنده و دیوار ساعت زمان صرف شده از جمله زمان بیکار ردیابی.

برای استفاده از پروفیل:

-   برپایی [\_قطع](../server-configuration-parameters/settings.md#server_configuration_parameters-trace_log) بخش پیکربندی سرور.

    در این بخش پیکربندی [\_قطع](../../operations/system-tables.md#system_tables-trace_log) جدول سیستم حاوی نتایج حاصل از عملکرد پیشفیلتر. این است که به طور پیش فرض پیکربندی شده است. به یاد داشته باشید که داده ها در این جدول تنها برای یک سرور در حال اجرا معتبر است. پس از راه اندازی مجدد سرور, تاتر تمیز نمی کند تا جدول و تمام نشانی حافظه مجازی ذخیره شده ممکن است نامعتبر.

-   برپایی [ایران در تهران](../settings/settings.md#query_profiler_cpu_time_period_ns) یا [جستجو](../settings/settings.md#query_profiler_real_time_period_ns) تنظیمات. هر دو تنظیمات را می توان به طور همزمان استفاده کرد.

    این تنظیمات به شما اجازه پیکربندی تایمر پیشفیلتر. همانطور که این تنظیمات جلسه هستند, شما می توانید فرکانس نمونه برداری های مختلف برای کل سرور از, کاربران فردی و یا پروفایل های کاربر, برای جلسه تعاملی خود را, و برای هر پرس و جو فردی.

فرکانس نمونه گیری به طور پیش فرض یک نمونه در هر ثانیه است و هر دو پردازنده و تایمر واقعی را فعال کنید. این فرکانس اجازه می دهد تا اطلاعات کافی در مورد خوشه کلیک کنید. همزمان, کار با این فرکانس, پیشفیلتر می کند عملکرد سرور کلیک را تحت تاثیر قرار نمی. اگر شما نیاز به مشخصات هر پرس و جو فردی سعی کنید به استفاده از فرکانس نمونه برداری بالاتر است.

برای تجزیه و تحلیل `trace_log` جدول سیستم:

-   نصب `clickhouse-common-static-dbg` بسته ببینید [نصب از بسته های دب](../../getting-started/install.md#install-from-deb-packages).

-   اجازه توابع درون گرایی توسط [اجازه دادن به \_فعال کردن اختلال در عملکرد](../settings/settings.md#settings-allow_introspection_functions) تنظیمات.

    به دلایل امنیتی, توابع درون گرایی به طور پیش فرض غیر فعال.

-   استفاده از `addressToLine`, `addressToSymbol` و `demangle` [توابع درون گرایی](../../sql-reference/functions/introspection.md) برای گرفتن نام تابع و موقعیت خود را در کد کلیک کنید. برای دریافت یک پروفایل برای برخی از پرس و جو, شما نیاز به جمع داده ها از `trace_log` جدول شما می توانید داده ها را با توابع فردی یا کل ردیابی پشته جمع کنید.

اگر شما نیاز به تجسم `trace_log` اطلاعات را امتحان کنید [شق](../../interfaces/third-party/gui/#clickhouse-flamegraph) و [سرعت سنج](https://github.com/laplab/clickhouse-speedscope).

## مثال {#example}

در این مثال ما:

-   پالایش `trace_log` داده ها توسط یک شناسه پرس و جو و تاریخ جاری.

-   جمع توسط ردیابی پشته.

-   با استفاده از توابع درون گرایی, ما یک گزارش از دریافت:

    -   نام نمادها و توابع کد منبع مربوطه.
    -   محل کد منبع از این توابع.

<!-- -->

``` sql
SELECT
    count(),
    arrayStringConcat(arrayMap(x -> concat(demangle(addressToSymbol(x)), '\n    ', addressToLine(x)), trace), '\n') AS sym
FROM system.trace_log
WHERE (query_id = 'ebca3574-ad0a-400a-9cbc-dca382f5998c') AND (event_date = today())
GROUP BY trace
ORDER BY count() DESC
LIMIT 10
```

``` text
{% include "examples/sampling_query_profiler_result.txt" %}
```
