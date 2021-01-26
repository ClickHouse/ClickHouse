---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 45
toc_title: "\u0646\u0638\u0627\u0631\u062A"
---

# نظارت {#monitoring}

شما می توانید نظارت:

-   استفاده از منابع سخت افزاری.
-   معیارهای سرور کلیک.

## استفاده از منابع {#resource-utilization}

کلیک می کند دولت از منابع سخت افزاری به خودی خود نظارت نیست.

این است که به شدت توصیه می شود به راه اندازی نظارت برای:

-   بار و درجه حرارت در پردازنده.

    شما می توانید استفاده کنید [راهنمایی و رانندگی](https://en.wikipedia.org/wiki/Dmesg), [توربوستات](https://www.linux.org/docs/man8/turbostat.html) و یا ابزار های دیگر.

-   استفاده از سیستم ذخیره سازی, رم و شبکه.

## معیارهای سرور کلیک {#clickhouse-server-metrics}

سرور کلیک ابزار برای نظارت خود دولت تعبیه شده است.

برای پیگیری رویدادهای سرور استفاده از سیاهههای مربوط به سرور. دیدن [چوبگر](server-configuration-parameters/settings.md#server_configuration_parameters-logger) بخش از فایل پیکربندی.

جمعهای کلیک:

-   معیارهای مختلف چگونه سرور با استفاده از منابع محاسباتی.
-   ارقام مشترک در پردازش پرس و جو.

شما می توانید معیارهای موجود در [سیستم.متریک](../operations/system-tables.md#system_tables-metrics), [سیستم.رویدادها](../operations/system-tables.md#system_tables-events) و [سیستم._نامهنویسی ناهمزمان](../operations/system-tables.md#system_tables-asynchronous_metrics) میز

شما می توانید کلیک کنید هاوس به صادرات معیارهای به پیکربندی کنید [گرافیت](https://github.com/graphite-project). دیدن [بخش گرافیت](server-configuration-parameters/settings.md#server_configuration_parameters-graphite) در فایل پیکربندی سرور کلیک. قبل از پیکربندی صادرات معیارهای, شما باید راه اندازی گرافیت با پیروی از رسمی خود را [راهنما](https://graphite.readthedocs.io/en/latest/install.html).

شما می توانید کلیک کنید هاوس به صادرات معیارهای به پیکربندی کنید [پرومتیوس](https://prometheus.io). دیدن [بخش پرومته](server-configuration-parameters/settings.md#server_configuration_parameters-prometheus) در فایل پیکربندی سرور کلیک. قبل از پیکربندی صادرات معیارهای, شما باید راه اندازی پرومته با پیروی از رسمی خود [راهنما](https://prometheus.io/docs/prometheus/latest/installation/).

علاوه بر این, شما می توانید در دسترس بودن سرور از طریق صفحه اصلی نظارت. ارسال `HTTP GET` درخواست برای `/ping`. اگر سرور در دسترس است, با پاسخ `200 OK`.

برای نظارت بر سرور در یک پیکربندی خوشه, شما باید مجموعه ای از [_شروع مجدد _شروع مجدد _شروع مجدد _کاربری](settings/settings.md#settings-max_replica_delay_for_distributed_queries) پارامتر و استفاده از منبع قام `/replicas_status`. یک درخواست برای `/replicas_status` بازگشت `200 OK` اگر ماکت در دسترس است و در پشت کپی دیگر به تعویق افتاد. اگر یک ماکت به تاخیر افتاد, باز می گردد `503 HTTP_SERVICE_UNAVAILABLE` با اطلاعات در مورد شکاف.
