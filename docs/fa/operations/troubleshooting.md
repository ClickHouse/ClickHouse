---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 46
toc_title: "\u0639\u06CC\u0628 \u06CC\u0627\u0628\u06CC"
---

# عیب یابی {#troubleshooting}

-   [نصب و راه اندازی](#troubleshooting-installation-errors)
-   [اتصال به سرور](#troubleshooting-accepts-no-connections)
-   [پردازش پرس و جو](#troubleshooting-does-not-process-queries)
-   [کارایی پردازش پرس و جو](#troubleshooting-too-slow)

## نصب و راه اندازی {#troubleshooting-installation-errors}

### شما می توانید بسته های دب از مخزن کلیک با مناسب دریافت کنید {#you-cannot-get-deb-packages-from-clickhouse-repository-with-apt-get}

-   بررسی تنظیمات فایروال.
-   اگر شما می توانید مخزن به هر دلیلی دسترسی پیدا کنید, دانلود بسته همانطور که در توصیف [شروع کار](../getting-started/index.md) مقاله و نصب دستی با استفاده از `sudo dpkg -i <packages>` فرمان. همچنین شما می خواهد نیاز `tzdata` بسته

## اتصال به سرور {#troubleshooting-accepts-no-connections}

مشکلات احتمالی:

-   سرور در حال اجرا نیست.
-   پارامترهای پیکربندی غیر منتظره و یا اشتباه.

### کارساز در حال اجرا نیست {#server-is-not-running}

**بررسی کنید که کارگزار روننیگ باشد**

فرمان:

``` bash
$ sudo service clickhouse-server status
```

اگر سرور در حال اجرا نیست, شروع با فرمان:

``` bash
$ sudo service clickhouse-server start
```

**بررسی سیاههها**

ورود اصلی `clickhouse-server` در `/var/log/clickhouse-server/clickhouse-server.log` به طور پیش فرض.

اگر سرور با موفقیت شروع, شما باید رشته ها را ببینید:

-   `<Information> Application: starting up.` — Server started.
-   `<Information> Application: Ready for connections.` — Server is running and ready for connections.

اگر `clickhouse-server` شروع با یک خطای پیکربندی شکست خورده, شما باید ببینید `<Error>` رشته با شرح خطا. به عنوان مثال:

``` text
2019.01.11 15:23:25.549505 [ 45 ] {} <Error> ExternalDictionaries: Failed reloading 'event2id' external dictionary: Poco::Exception. Code: 1000, e.code() = 111, e.displayText() = Connection refused, e.what() = Connection refused
```

اگر شما یک خطا در انتهای فایل را نمی بینم, از طریق تمام فایل با شروع از رشته نگاه:

``` text
<Information> Application: starting up.
```

اگر شما سعی می کنید برای شروع یک نمونه دوم از `clickhouse-server` بر روی سرور, شما ورود به سیستم زیر را ببینید:

``` text
2019.01.11 15:25:11.151730 [ 1 ] {} <Information> : Starting ClickHouse 19.1.0 with revision 54413
2019.01.11 15:25:11.154578 [ 1 ] {} <Information> Application: starting up
2019.01.11 15:25:11.156361 [ 1 ] {} <Information> StatusFile: Status file ./status already exists - unclean restart. Contents:
PID: 8510
Started at: 2019-01-11 15:24:23
Revision: 54413

2019.01.11 15:25:11.156673 [ 1 ] {} <Error> Application: DB::Exception: Cannot lock file ./status. Another server instance in same directory is already running.
2019.01.11 15:25:11.156682 [ 1 ] {} <Information> Application: shutting down
2019.01.11 15:25:11.156686 [ 1 ] {} <Debug> Application: Uninitializing subsystem: Logging Subsystem
2019.01.11 15:25:11.156716 [ 2 ] {} <Information> BaseDaemon: Stop SignalListener thread
```

**مشاهده سیستم.د سیاهههای مربوط**

اگر شما هر گونه اطلاعات مفید در پیدا کنید `clickhouse-server` سیاهههای مربوط و یا هر گونه سیاهههای مربوط وجود ندارد, شما می توانید مشاهده `system.d` سیاهههای مربوط با استفاده از دستور:

``` bash
$ sudo journalctl -u clickhouse-server
```

**شروع کلیک-سرور در حالت تعاملی**

``` bash
$ sudo -u clickhouse /usr/bin/clickhouse-server --config-file /etc/clickhouse-server/config.xml
```

این دستور سرور را به عنوان یک برنامه تعاملی با پارامترهای استاندارد اسکریپت خودکار شروع می کند. در این حالت `clickhouse-server` چاپ تمام پیام های رویداد در کنسول.

### پارامترهای پیکربندی {#configuration-parameters}

بررسی:

-   تنظیمات کارگر بارانداز.

    اطمینان حاصل کنید که اگر شما اجرا خانه عروسکی در کارگر بارانداز در یک شبکه اینترنتی6 `network=host` قرار است.

-   تنظیمات نقطه پایانی.

    بررسی [\_نوست فهرست](server-configuration-parameters/settings.md#server_configuration_parameters-listen_host) و [\_صادر کردن](server-configuration-parameters/settings.md#server_configuration_parameters-tcp_port) تنظیمات.

    سرور کلیک می پذیرد اتصالات مجنون تنها به طور پیش فرض.

-   تنظیمات پروتکل قام.

    بررسی تنظیمات پروتکل برای صفحه اصلی.

-   تنظیمات اتصال امن.

    بررسی:

    -   این [\_شروع مجدد](server-configuration-parameters/settings.md#server_configuration_parameters-tcp_port_secure) تنظیمات.
    -   تنظیمات برای [SSL sertificates](server-configuration-parameters/settings.md#server_configuration_parameters-openssl).

    استفاده از پارامترهای مناسب در حالی که اتصال. برای مثال با استفاده از `port_secure` پارامتر با `clickhouse_client`.

-   تنظیمات کاربر.

    شما ممکن است با استفاده از نام کاربری اشتباه و یا رمز عبور.

## پردازش پرس و جو {#troubleshooting-does-not-process-queries}

اگر فاحشه خانه است که قادر به پردازش پرس و جو نمی, این شرح خطا به مشتری می فرستد. در `clickhouse-client` شما دریافت می کنید شرح خطا در کنسول. اگر شما با استفاده از HTTP رابط ClickHouse می فرستد خطا توضیحات در پاسخ بدن. به عنوان مثال:

``` bash
$ curl 'http://localhost:8123/' --data-binary "SELECT a"
Code: 47, e.displayText() = DB::Exception: Unknown identifier: a. Note that there are no tables (FROM clause) in your query, context: required_names: 'a' source_tables: table_aliases: private_aliases: column_aliases: public_columns: 'a' masked_columns: array_join_columns: source_columns: , e.what() = DB::Exception
```

اگر شما شروع `clickhouse-client` با `stack-trace` پارامتر, خانه را برمی گرداند ردیابی پشته سرور با شرح خطا.

شما ممکن است یک پیام در مورد یک اتصال شکسته را ببینید. در این مورد می توانید پرس و جو را تکرار کنید. اگر اتصال می شکند هر بار که شما انجام پرس و جو, بررسی سیاهههای مربوط به سرور برای اشتباهات.

## کارایی پردازش پرس و جو {#troubleshooting-too-slow}

اگر شما می بینید که تاتر در حال کار بیش از حد کند, شما نیاز به مشخصات بار بر روی منابع سرور و شبکه برای نمایش داده شد خود را.

شما می توانید ابزار کلیک معیار به نمایش داده شد مشخصات استفاده کنید. این نشان می دهد تعداد نمایش داده شد پردازش در هر ثانیه, تعداد ردیف پردازش در هر ثانیه, و صدک از زمان پردازش پرس و جو.
