---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 57
toc_title: "\u062A\u0646\u0638\u06CC\u0645\u0627\u062A \u06A9\u0627\u0631\u06AF\u0632\
  \u0627\u0631"
---

# تنظیمات کارگزار {#server-settings}

## ساختن و احراز هویت اکانتهای دستگاه {#builtin-dictionaries-reload-interval}

فاصله در ثانیه قبل از بارگذاری ساخته شده است در لغت نامه.

مخزن بارگذاری مجدد ساخته شده است در لغت نامه در هر ثانیه ایکس. این امکان ویرایش واژهنامهها را فراهم میکند “on the fly” بدون راه اندازی مجدد سرور.

مقدار پیش فرض: 3600.

**مثال**

``` xml
<builtin_dictionaries_reload_interval>3600</builtin_dictionaries_reload_interval>
```

## فشردهسازی {#server-settings-compression}

تنظیمات فشرده سازی داده ها برای [ادغام](../../engines/table-engines/mergetree-family/mergetree.md)- جدول موتور .

!!! warning "اخطار"
    اگر شما فقط شروع به استفاده از خانه کلیک استفاده نکنید.

قالب پیکربندی:

``` xml
<compression>
    <case>
      <min_part_size>...</min_part_size>
      <min_part_size_ratio>...</min_part_size_ratio>
      <method>...</method>
    </case>
    ...
</compression>
```

`<case>` زمینه:

-   `min_part_size` – The minimum size of a data part.
-   `min_part_size_ratio` – The ratio of the data part size to the table size.
-   `method` – Compression method. Acceptable values: `lz4` یا `zstd`.

شما می توانید چند پیکربندی کنید `<case>` بخش.

اقدامات زمانی که شرایط ملاقات می شوند:

-   اگر بخشی از داده ها منطبق یک مجموعه شرایط, تاتر با استفاده از روش فشرده سازی مشخص.
-   اگر یک بخش داده منطبق مجموعه شرایط متعدد, خانه رعیتی با استفاده از اولین مجموعه شرایط همسان.

اگر هیچ شرایطی برای یک بخش داده ملاقات, خانه عروسکی با استفاده از `lz4` فشردهسازی.

**مثال**

``` xml
<compression incl="clickhouse_compression">
    <case>
        <min_part_size>10000000000</min_part_size>
        <min_part_size_ratio>0.01</min_part_size_ratio>
        <method>zstd</method>
    </case>
</compression>
```

## & تنظیمات {#default-database}

پایگاه داده به طور پیش فرض.

برای دریافت یک لیست از پایگاه داده, استفاده از [SHOW DATABASES](../../sql-reference/statements/show.md#show-databases) پرس و جو.

**مثال**

``` xml
<default_database>default</default_database>
```

## قصور {#default-profile}

تنظیمات پیش فرض مشخصات.

پروفایل های تنظیمات در فایل مشخص شده در پارامتر واقع شده است `user_config`.

**مثال**

``` xml
<default_profile>default</default_profile>
```

## دیکشنامهای {#server_configuration_parameters-dictionaries_config}

مسیر به فایل پیکربندی برای لغت نامه های خارجی.

مسیر:

-   مشخص کردن مسیر مطلق و یا مسیر نسبت به فایل پیکربندی سرور.
-   مسیر می تواند حاوی نویسه عام \* و?.

همچنین نگاه کنید به “[واژهنامهها خارجی](../../sql-reference/dictionaries/external-dictionaries/external-dicts.md)”.

**مثال**

``` xml
<dictionaries_config>*_dictionary.xml</dictionaries_config>
```

## _بارگیری کامل {#server_configuration_parameters-dictionaries_lazy_load}

بارگذاری تنبل از لغت نامه.

اگر `true` سپس هر فرهنگ لغت در اولین استفاده ایجاد می شود. اگر ایجاد فرهنگ لغت شکست خورده, تابع بود که با استفاده از فرهنگ لغت می اندازد یک استثنا.

اگر `false`, تمام لغت نامه ها ایجاد می شوند زمانی که سرور شروع می شود, و اگر یک خطا وجود دارد, سرور خاموش.

به طور پیش فرض است `true`.

**مثال**

``` xml
<dictionaries_lazy_load>true</dictionaries_lazy_load>
```

## قالب_شکلمات شیمی {#server_configuration_parameters-format_schema_path}

مسیر به دایرکتوری با طرح برای داده های ورودی, مانند طرحواره برای [کاپپروتو](../../interfaces/formats.md#capnproto) قالب.

**مثال**

``` xml
  <!-- Directory containing schema files for various input formats. -->
  <format_schema_path>format_schemas/</format_schema_path>
```

## گرافیت {#server_configuration_parameters-graphite}

ارسال داده به [گرافیت](https://github.com/graphite-project).

تنظیمات:

-   host – The Graphite server.
-   port – The port on the Graphite server.
-   interval – The interval for sending, in seconds.
-   timeout – The timeout for sending data, in seconds.
-   root_path – Prefix for keys.
-   metrics – Sending data from the [سیستم.متریک](../../operations/system-tables.md#system_tables-metrics) جدول
-   events – Sending deltas data accumulated for the time period from the [سیستم.رویدادها](../../operations/system-tables.md#system_tables-events) جدول
-   events_cumulative – Sending cumulative data from the [سیستم.رویدادها](../../operations/system-tables.md#system_tables-events) جدول
-   asynchronous_metrics – Sending data from the [سیستم._نامهنویسی ناهمزمان](../../operations/system-tables.md#system_tables-asynchronous_metrics) جدول

شما می توانید چند پیکربندی کنید `<graphite>` بند. برای مثال شما می توانید از این برای ارسال داده های مختلف در فواصل مختلف استفاده کنید.

**مثال**

``` xml
<graphite>
    <host>localhost</host>
    <port>42000</port>
    <timeout>0.1</timeout>
    <interval>60</interval>
    <root_path>one_min</root_path>
    <metrics>true</metrics>
    <events>true</events>
    <events_cumulative>false</events_cumulative>
    <asynchronous_metrics>true</asynchronous_metrics>
</graphite>
```

## لغزش _ نمودار {#server_configuration_parameters-graphite-rollup}

تنظیمات برای نازک شدن داده ها برای گرافیت.

برای اطلاعات بیشتر, دیدن [نمودار](../../engines/table-engines/mergetree-family/graphitemergetree.md).

**مثال**

``` xml
<graphite_rollup_example>
    <default>
        <function>max</function>
        <retention>
            <age>0</age>
            <precision>60</precision>
        </retention>
        <retention>
            <age>3600</age>
            <precision>300</precision>
        </retention>
        <retention>
            <age>86400</age>
            <precision>3600</precision>
        </retention>
    </default>
</graphite_rollup_example>
```

## _وارد کردن/پشتیبانی {#http-porthttps-port}

درگاه برای اتصال به کارساز بالای صفحه) ها (.

اگر `https_port` مشخص شده است, [openSSL](#server_configuration_parameters-openssl) باید پیکربندی شود.

اگر `http_port` مشخص شده است, پیکربندی اپنسسل نادیده گرفته شده است حتی اگر قرار است.

**مثال**

``` xml
<https_port>9999</https_port>
```

## نقلقولهای جدید از این نویسنده {#server_configuration_parameters-http_server_default_response}

صفحه ای که به طور پیش فرض نشان داده شده است زمانی که شما دسترسی به سرور قام کلیک.
مقدار پیش فرض است “Ok.” (با خوراک خط در پایان)

**مثال**

باز می شود `https://tabix.io/` هنگام دسترسی `http://localhost: http_port`.

``` xml
<http_server_default_response>
  <![CDATA[<html ng-app="SMI2"><head><base href="http://ui.tabix.io/"></head><body><div ui-view="" class="content-ui"></div><script src="http://loader.tabix.io/master.js"></script></body></html>]]>
</http_server_default_response>
```

## شامل _فروم {#server_configuration_parameters-include_from}

مسیر به فایل با تعویض.

برای کسب اطلاعات بیشتر به بخش مراجعه کنید “[پروندههای پیکربندی](../configuration-files.md#configuration_files)”.

**مثال**

``` xml
<include_from>/etc/metrica.xml</include_from>
```

## _صادر کردن {#interserver-http-port}

پورت برای تبادل اطلاعات بین سرور های فاحشه خانه.

**مثال**

``` xml
<interserver_http_port>9009</interserver_http_port>
```

## حذف جستجو {#interserver-http-host}

نام میزبان است که می تواند توسط سرور های دیگر برای دسترسی به این سرور استفاده می شود.

اگر حذف, این است که در همان راه به عنوان تعریف `hostname-f` فرمان.

مفید برای شکستن دور از یک رابط شبکه خاص.

**مثال**

``` xml
<interserver_http_host>example.yandex.ru</interserver_http_host>
```

## پتانسیلهای متقابل {#server-settings-interserver-http-credentials}

نام کاربری و رمز عبور مورد استفاده برای تصدیق در طول [تکرار](../../engines/table-engines/mergetree-family/replication.md) با تکرار \* موتورهای. این اعتبار تنها برای ارتباط بین کپی استفاده می شود و ربطی به اعتبار برای مشتریان خانه عروسکی هستند. سرور چک کردن این اعتبار برای اتصال کپی و استفاده از اعتبار همان هنگام اتصال به دیگر کپی. بنابراین, این اعتبار باید همین کار را برای همه کپی در یک خوشه مجموعه.
به طور پیش فرض احراز هویت استفاده نمی شود.

این بخش شامل پارامترهای زیر است:

-   `user` — username.
-   `password` — password.

**مثال**

``` xml
<interserver_http_credentials>
    <user>admin</user>
    <password>222</password>
</interserver_http_credentials>
```

## حفاظت از حریم خصوصی {#keep-alive-timeout}

تعداد ثانیه که تاتر منتظر درخواست های دریافتی قبل از بستن اتصال. به طور پیش فرض به 3 ثانیه.

**مثال**

``` xml
<keep_alive_timeout>3</keep_alive_timeout>
```

## _نوست فهرست {#server_configuration_parameters-listen_host}

محدودیت در میزبان که درخواست می توانید از. اگر می خواهید سرور برای پاسخ به همه انها مشخص شود `::`.

مثالها:

``` xml
<listen_host>::1</listen_host>
<listen_host>127.0.0.1</listen_host>
```

## چوبگر {#server_configuration_parameters-logger}

تنظیمات ورود به سیستم.

کلید:

-   level – Logging level. Acceptable values: `trace`, `debug`, `information`, `warning`, `error`.
-   log – The log file. Contains all the entries according to `level`.
-   errorlog – Error log file.
-   size – Size of the file. Applies to `log`و`errorlog`. هنگامی که فایل می رسد `size`, بایگانی کلیک هوس و تغییر نام, و ایجاد یک فایل ورود به سیستم جدید را در خود جای.
-   count – The number of archived log files that ClickHouse stores.

**مثال**

``` xml
<logger>
    <level>trace</level>
    <log>/var/log/clickhouse-server/clickhouse-server.log</log>
    <errorlog>/var/log/clickhouse-server/clickhouse-server.err.log</errorlog>
    <size>1000M</size>
    <count>10</count>
</logger>
```

نوشتن به وبلاگ نیز پشتیبانی می کند. پیکربندی مثال:

``` xml
<logger>
    <use_syslog>1</use_syslog>
    <syslog>
        <address>syslog.remote:10514</address>
        <hostname>myhost.local</hostname>
        <facility>LOG_LOCAL6</facility>
        <format>syslog</format>
    </syslog>
</logger>
```

کلید:

-   use_syslog — Required setting if you want to write to the syslog.
-   address — The host\[:port\] of syslogd. If omitted, the local daemon is used.
-   hostname — Optional. The name of the host that logs are sent from.
-   facility — [کلمه کلیدی تسهیلات سیسلوگ](https://en.wikipedia.org/wiki/Syslog#Facility) در حروف بزرگ با “LOG_” پیشوند: (`LOG_USER`, `LOG_DAEMON`, `LOG_LOCAL3`, و به همین ترتیب).
    مقدار پیشفرض: `LOG_USER` اگر `address` مشخص شده است, `LOG_DAEMON otherwise.`
-   format – Message format. Possible values: `bsd` و `syslog.`

## & کلاندارها {#macros}

تعویض پارامتر برای جداول تکرار.

می توان حذف اگر جداول تکرار استفاده نمی شود.

برای کسب اطلاعات بیشتر به بخش مراجعه کنید “[ایجاد جداول تکرار شده](../../engines/table-engines/mergetree-family/replication.md)”.

**مثال**

``` xml
<macros incl="macros" optional="true" />
```

## نشاندار کردن _چ_سیز {#server-mark-cache-size}

اندازه تقریبی (به بایت) کش علامت های استفاده شده توسط موتورهای جدول [ادغام](../../engines/table-engines/mergetree-family/mergetree.md) خانواده

کش برای سرور به اشتراک گذاشته و حافظه به عنوان مورد نیاز اختصاص داده است. اندازه کش باید حداقل 5368709120 باشد.

**مثال**

``` xml
<mark_cache_size>5368709120</mark_cache_size>
```

## م_قیاس تصویر {#max-concurrent-queries}

حداکثر تعداد درخواست به طور همزمان پردازش.

**مثال**

``` xml
<max_concurrent_queries>100</max_concurrent_queries>
```

## _تنامههای بیشینه {#max-connections}

حداکثر تعداد اتصالات ورودی.

**مثال**

``` xml
<max_connections>4096</max_connections>
```

## _موضوعات بیشینه {#max-open-files}

حداکثر تعداد فایل های باز.

به طور پیش فرض: `maximum`.

ما توصیه می کنیم با استفاده از این گزینه در سیستم عامل مک ایکس از `getrlimit()` تابع یک مقدار نادرست می گرداند.

**مثال**

``` xml
<max_open_files>262144</max_open_files>
```

## حداکثر_طب_ضز_توقف {#max-table-size-to-drop}

محدودیت در حذف جداول.

اگر اندازه یک [ادغام](../../engines/table-engines/mergetree-family/mergetree.md) جدول بیش از `max_table_size_to_drop` با استفاده از پرس و جو قطره نمی توانید حذف کنید.

اگر شما هنوز هم نیاز به حذف جدول بدون راه اندازی مجدد سرور کلیک, ایجاد `<clickhouse-path>/flags/force_drop_table` فایل و اجرای پرس و جو قطره.

مقدار پیش فرض: 50 گیگابایت.

ارزش 0 بدان معنی است که شما می توانید تمام جداول بدون هیچ گونه محدودیت حذف.

**مثال**

``` xml
<max_table_size_to_drop>0</max_table_size_to_drop>
```

## ادغام {#server_configuration_parameters-merge_tree}

تنظیم زیبا برای جداول در [ادغام](../../engines/table-engines/mergetree-family/mergetree.md).

برای کسب اطلاعات بیشتر, دیدن ادغام.فایل هدر ساعت.

**مثال**

``` xml
<merge_tree>
    <max_suspicious_broken_parts>5</max_suspicious_broken_parts>
</merge_tree>
```

## openSSL {#server_configuration_parameters-openssl}

SSL client/server configuration.

پشتیبانی از اس اس ال توسط `libpoco` کتابخونه. رابط در فایل شرح داده شده است [سوسمنگر.ه](https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/include/Poco/Net/SSLManager.h)

کلید برای تنظیمات سرور / مشتری:

-   privateKeyFile – The path to the file with the secret key of the PEM certificate. The file may contain a key and certificate at the same time.
-   certificateFile – The path to the client/server certificate file in PEM format. You can omit it if `privateKeyFile` شامل گواهی.
-   caConfig – The path to the file or directory that contains trusted root certificates.
-   verificationMode – The method for checking the node's certificates. Details are in the description of the [متن](https://github.com/ClickHouse-Extras/poco/blob/master/NetSSL_OpenSSL/include/Poco/Net/Context.h) کلاس. مقادیر ممکن: `none`, `relaxed`, `strict`, `once`.
-   verificationDepth – The maximum length of the verification chain. Verification will fail if the certificate chain length exceeds the set value.
-   loadDefaultCAFile – Indicates that built-in CA certificates for OpenSSL will be used. Acceptable values: `true`, `false`. \|
-   cipherList – Supported OpenSSL encryptions. For example: `ALL:!ADH:!LOW:!EXP:!MD5:@STRENGTH`.
-   cacheSessions – Enables or disables caching sessions. Must be used in combination with `sessionIdContext`. مقادیر قابل قبول: `true`, `false`.
-   sessionIdContext – A unique set of random characters that the server appends to each generated identifier. The length of the string must not exceed `SSL_MAX_SSL_SESSION_ID_LENGTH`. این پارامتر همیشه توصیه می شود از این کمک می کند تا جلوگیری از مشکلات هر دو اگر سرور حافظه پنهان جلسه و اگر مشتری درخواست ذخیره. مقدار پیشفرض: `${application.name}`.
-   sessionCacheSize – The maximum number of sessions that the server caches. Default value: 1024\*20. 0 – Unlimited sessions.
-   sessionTimeout – Time for caching the session on the server.
-   extendedVerification – Automatically extended verification of certificates after the session ends. Acceptable values: `true`, `false`.
-   requireTLSv1 – Require a TLSv1 connection. Acceptable values: `true`, `false`.
-   requireTLSv1_1 – Require a TLSv1.1 connection. Acceptable values: `true`, `false`.
-   requireTLSv1 – Require a TLSv1.2 connection. Acceptable values: `true`, `false`.
-   fips – Activates OpenSSL FIPS mode. Supported if the library's OpenSSL version supports FIPS.
-   privateKeyPassphraseHandler – Class (PrivateKeyPassphraseHandler subclass) that requests the passphrase for accessing the private key. For example: `<privateKeyPassphraseHandler>`, `<name>KeyFileHandler</name>`, `<options><password>test</password></options>`, `</privateKeyPassphraseHandler>`.
-   invalidCertificateHandler – Class (a subclass of CertificateHandler) for verifying invalid certificates. For example: `<invalidCertificateHandler> <name>ConsoleCertificateHandler</name> </invalidCertificateHandler>` .
-   disableProtocols – Protocols that are not allowed to use.
-   preferServerCiphers – Preferred server ciphers on the client.

**مثال تنظیمات:**

``` xml
<openSSL>
    <server>
        <!-- openssl req -subj "/CN=localhost" -new -newkey rsa:2048 -days 365 -nodes -x509 -keyout /etc/clickhouse-server/server.key -out /etc/clickhouse-server/server.crt -->
        <certificateFile>/etc/clickhouse-server/server.crt</certificateFile>
        <privateKeyFile>/etc/clickhouse-server/server.key</privateKeyFile>
        <!-- openssl dhparam -out /etc/clickhouse-server/dhparam.pem 4096 -->
        <dhParamsFile>/etc/clickhouse-server/dhparam.pem</dhParamsFile>
        <verificationMode>none</verificationMode>
        <loadDefaultCAFile>true</loadDefaultCAFile>
        <cacheSessions>true</cacheSessions>
        <disableProtocols>sslv2,sslv3</disableProtocols>
        <preferServerCiphers>true</preferServerCiphers>
    </server>
    <client>
        <loadDefaultCAFile>true</loadDefaultCAFile>
        <cacheSessions>true</cacheSessions>
        <disableProtocols>sslv2,sslv3</disableProtocols>
        <preferServerCiphers>true</preferServerCiphers>
        <!-- Use for self-signed: <verificationMode>none</verificationMode> -->
        <invalidCertificateHandler>
            <!-- Use for self-signed: <name>AcceptCertificateHandler</name> -->
            <name>RejectCertificateHandler</name>
        </invalidCertificateHandler>
    </client>
</openSSL>
```

## _خروج {#server_configuration_parameters-part-log}

وقایع ورود به سیستم که با مرتبط [ادغام](../../engines/table-engines/mergetree-family/mergetree.md). برای مثال, اضافه کردن یا ادغام داده ها. شما می توانید ورود به سیستم برای شبیه سازی الگوریتم های ادغام و مقایسه ویژگی های خود استفاده کنید. شما می توانید روند ادغام تجسم.

نمایش داده شد در سیستم وارد [سیستم._خروج](../../operations/system-tables.md#system_tables-part-log) جدول, نه در یک فایل جداگانه. شما می توانید نام این جدول را در پیکربندی `table` پارامتر (پایین را ببینید).

از پارامترهای زیر برای پیکربندی ورود استفاده کنید:

-   `database` – Name of the database.
-   `table` – Name of the system table.
-   `partition_by` – Sets a [کلید پارتیشن بندی سفارشی](../../engines/table-engines/mergetree-family/custom-partitioning-key.md).
-   `flush_interval_milliseconds` – Interval for flushing data from the buffer in memory to the table.

**مثال**

``` xml
<part_log>
    <database>system</database>
    <table>part_log</table>
    <partition_by>toMonday(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
</part_log>
```

## مسیر {#server_configuration_parameters-path}

مسیر به دایرکتوری حاوی داده.

!!! note "یادداشت"
    اسلش الزامی است.

**مثال**

``` xml
<path>/var/lib/clickhouse/</path>
```

## پرومتیوس {#server_configuration_parameters-prometheus}

افشای معیارهای داده ها برای خراش دادن از [پرومتیوس](https://prometheus.io).

تنظیمات:

-   `endpoint` – HTTP endpoint for scraping metrics by prometheus server. Start from ‘/’.
-   `port` – Port for `endpoint`.
-   `metrics` – Flag that sets to expose metrics from the [سیستم.متریک](../system-tables.md#system_tables-metrics) جدول
-   `events` – Flag that sets to expose metrics from the [سیستم.رویدادها](../system-tables.md#system_tables-events) جدول
-   `asynchronous_metrics` – Flag that sets to expose current metrics values from the [سیستم._نامهنویسی ناهمزمان](../system-tables.md#system_tables-asynchronous_metrics) جدول

**مثال**

``` xml
 <prometheus>
        <endpoint>/metrics</endpoint>
        <port>8001</port>
        <metrics>true</metrics>
        <events>true</events>
        <asynchronous_metrics>true</asynchronous_metrics>
    </prometheus>
```

## _خروج {#server_configuration_parameters-query-log}

تنظیم برای ورود به سیستم نمایش داده شد با دریافت [_ترکیب = 1](../settings/settings.md) تنظیمات.

نمایش داده شد در سیستم وارد [سیستم._خروج](../../operations/system-tables.md#system_tables-query_log) جدول, نه در یک فایل جداگانه. شما می توانید نام جدول را در `table` پارامتر (پایین را ببینید).

از پارامترهای زیر برای پیکربندی ورود استفاده کنید:

-   `database` – Name of the database.
-   `table` – Name of the system table the queries will be logged in.
-   `partition_by` – Sets a [کلید پارتیشن بندی سفارشی](../../engines/table-engines/mergetree-family/custom-partitioning-key.md) برای یک جدول.
-   `flush_interval_milliseconds` – Interval for flushing data from the buffer in memory to the table.

اگه جدول وجود نداشته باشه. اگر ساختار ورود به سیستم پرس و جو تغییر زمانی که سرور فاحشه خانه به روز شد, جدول با ساختار قدیمی تغییر نام داد, و یک جدول جدید به طور خودکار ایجاد شده است.

**مثال**

``` xml
<query_log>
    <database>system</database>
    <table>query_log</table>
    <partition_by>toMonday(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
</query_log>
```

## _ر_خروج {#server_configuration_parameters-query-thread-log}

تنظیم برای ورود به سیستم موضوعات نمایش داده شد دریافت شده با [& پایین: 1](../settings/settings.md#settings-log-query-threads) تنظیمات.

نمایش داده شد در سیستم وارد [سیستم._ر_خروج](../../operations/system-tables.md#system_tables-query-thread-log) جدول, نه در یک فایل جداگانه. شما می توانید نام جدول را در `table` پارامتر (پایین را ببینید).

از پارامترهای زیر برای پیکربندی ورود استفاده کنید:

-   `database` – Name of the database.
-   `table` – Name of the system table the queries will be logged in.
-   `partition_by` – Sets a [کلید پارتیشن بندی سفارشی](../../engines/table-engines/mergetree-family/custom-partitioning-key.md) برای یک جدول سیستم.
-   `flush_interval_milliseconds` – Interval for flushing data from the buffer in memory to the table.

اگه جدول وجود نداشته باشه. اگر ساختار پرس و جو موضوع ورود به سیستم تغییر زمانی که سرور فاحشه خانه به روز شد, جدول با ساختار قدیمی تغییر نام داد, و یک جدول جدید به طور خودکار ایجاد شده است.

**مثال**

``` xml
<query_thread_log>
    <database>system</database>
    <table>query_thread_log</table>
    <partition_by>toMonday(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
</query_thread_log>
```

## _قطع {#server_configuration_parameters-trace_log}

تنظیمات برای [_قطع](../../operations/system-tables.md#system_tables-trace_log) عملیات جدول سیستم.

پارامترها:

-   `database` — Database for storing a table.
-   `table` — Table name.
-   `partition_by` — [کلید پارتیشن بندی سفارشی](../../engines/table-engines/mergetree-family/custom-partitioning-key.md) برای یک جدول سیستم.
-   `flush_interval_milliseconds` — Interval for flushing data from the buffer in memory to the table.

فایل پیکربندی پیش فرض سرور `config.xml` شامل بخش تنظیمات زیر است:

``` xml
<trace_log>
    <database>system</database>
    <table>trace_log</table>
    <partition_by>toYYYYMM(event_date)</partition_by>
    <flush_interval_milliseconds>7500</flush_interval_milliseconds>
</trace_log>
```

## _منبع {#query-masking-rules}

قوانین مبتنی بر عبارت منظم, خواهد شد که به نمایش داده شد و همچنین تمام پیام های ورود به سیستم قبل از ذخیره سازی در سیاهههای مربوط به سرور اعمال,
`system.query_log`, `system.text_log`, `system.processes` جدول, و در سیاهههای مربوط به مشتری ارسال. که اجازه می دهد تا جلوگیری از
نشت اطلاعات حساس از پرس و جو گذاشتن (مانند نام, ایمیل, شخصی
شناسه و یا شماره کارت اعتباری) به سیاهههای مربوط.

**مثال**

``` xml
<query_masking_rules>
    <rule>
        <name>hide SSN</name>
        <regexp>(^|\D)\d{3}-\d{2}-\d{4}($|\D)</regexp>
        <replace>000-00-0000</replace>
    </rule>
</query_masking_rules>
```

زمینه پیکربندی:
- `name` - نام قانون (اختیاری)
- `regexp` - تکرار 2 عبارت منظم سازگار (اجباری)
- `replace` - رشته جایگزینی برای داده های حساس (اختیاری به طور پیش فرض-شش ستاره)

قوانین پوشش به کل پرس و جو اعمال می شود (برای جلوگیری از نشت اطلاعات حساس از نمایش داده شد ناقص / غیر تجزیه).

`system.events` جدول شمارنده `QueryMaskingRulesMatch` که تعداد کلی از پرس و جو پوشش قوانین مسابقات.

برای نمایش داده شد توزیع هر سرور باید به طور جداگانه پیکربندی شود, در غیر این صورت, فرعی به دیگر منتقل
گره ها بدون پوشش ذخیره می شوند.

## دور دور {#server-settings-remote-servers}

پیکربندی خوشه های مورد استفاده توسط [توزیع شده](../../engines/table-engines/special/distributed.md) موتور جدول و توسط `cluster` تابع جدول.

**مثال**

``` xml
<remote_servers incl="clickhouse_remote_servers" />
```

برای ارزش `incl` ویژگی, بخش را ببینید “[پروندههای پیکربندی](../configuration-files.md#configuration_files)”.

**همچنین نگاه کنید به**

-   [در حال بارگذاری](../settings/settings.md#settings-skip_unavailable_shards)

## منطقهی زمانی {#server_configuration_parameters-timezone}

منطقه زمانی سرور.

مشخص شده به عنوان شناساگر ایانا برای منطقه زمانی یو تی سی یا موقعیت جغرافیایی (مثلا افریقا / ابیجان).

منطقه زمانی برای تبدیل بین فرمت های رشته و تاریخ ساعت لازم است که زمینه های تاریخ ساعت خروجی به فرمت متن (چاپ شده بر روی صفحه نمایش و یا در یک فایل), و هنگامی که گرفتن تاریخ ساعت از یک رشته. علاوه بر این, منطقه زمانی در توابع است که با زمان و تاریخ کار می کنند در صورتی که منطقه زمانی در پارامترهای ورودی دریافت نمی استفاده.

**مثال**

``` xml
<timezone>Europe/Moscow</timezone>
```

## _صادر کردن {#server_configuration_parameters-tcp_port}

پورت برای برقراری ارتباط با مشتریان بیش از پروتکل تی سی پی.

**مثال**

``` xml
<tcp_port>9000</tcp_port>
```

## _شروع مجدد {#server_configuration_parameters-tcp_port_secure}

پورت تی سی پی برای برقراری ارتباط امن با مشتریان. با استفاده از [OpenSSL](#server_configuration_parameters-openssl) تنظیمات.

**مقادیر ممکن**

عدد صحیح مثبت.

**مقدار پیشفرض**

``` xml
<tcp_port_secure>9440</tcp_port_secure>
```

## _وارد کردن {#server_configuration_parameters-mysql_port}

پورت برای برقراری ارتباط با مشتریان بیش از پروتکل خروجی زیر.

**مقادیر ممکن**

عدد صحیح مثبت.

مثال

``` xml
<mysql_port>9004</mysql_port>
```

## _مخفی کردن {#server-settings-tmp_path}

مسیر به داده های موقت برای پردازش نمایش داده شد بزرگ است.

!!! note "یادداشت"
    اسلش الزامی است.

**مثال**

``` xml
<tmp_path>/var/lib/clickhouse/tmp/</tmp_path>
```

## پیدا کردن موقعیت جغرافیایی از روی شبکه {#server-settings-tmp-policy}

سیاست از [`storage_configuration`](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes) برای ذخیره فایل های موقت.
اگر تنظیم نشود [`tmp_path`](#server-settings-tmp_path) استفاده شده است, در غیر این صورت نادیده گرفته شده است.

!!! note "یادداشت"
    - `move_factor` نادیده گرفته شده است
- `keep_free_space_bytes` نادیده گرفته شده است
- `max_data_part_size_bytes` نادیده گرفته شده است
- شما باید دقیقا یک جلد در این سیاست داشته باشید

## _بالا {#server-settings-uncompressed_cache_size}

اندازه کش (به بایت) برای داده های غیر فشرده استفاده شده توسط موتورهای جدول از [ادغام](../../engines/table-engines/mergetree-family/mergetree.md).

یک کش مشترک برای سرور وجود دارد. حافظه در تقاضا اختصاص داده. کش در صورتی که گزینه استفاده می شود [همترازی پایین](../settings/settings.md#setting-use_uncompressed_cache) فعال است.

کش غیر فشرده سودمند برای نمایش داده شد بسیار کوتاه در موارد فردی است.

**مثال**

``` xml
<uncompressed_cache_size>8589934592</uncompressed_cache_size>
```

## _مخفی کردن _صفحه {#server_configuration_parameters-user_files_path}

دایرکتوری با فایل های کاربر. مورد استفاده در تابع جدول [پرونده()](../../sql-reference/table-functions/file.md).

**مثال**

``` xml
<user_files_path>/var/lib/clickhouse/user_files/</user_files_path>
```

## _تنفورد {#users-config}

مسیر پروندهی شامل:

-   تنظیمات کاربر.
-   حقوق دسترسی.
-   پروفایل تنظیمات.
-   تنظیمات سهمیه.

**مثال**

``` xml
<users_config>users.xml</users_config>
```

## باغ وحش {#server-settings_zookeeper}

شامل تنظیماتی است که اجازه می دهد تا کلیک برای ارتباط برقرار کردن با یک [باغ وحش](http://zookeeper.apache.org/) خوشه خوشه.

کلیک هاوس با استفاده از باغ وحش برای ذخیره سازی ابرداده از کپی در هنگام استفاده از جداول تکرار. اگر جداول تکرار استفاده نمی شود, این بخش از پارامترها را می توان حذف.

این بخش شامل پارامترهای زیر است:

-   `node` — ZooKeeper endpoint. You can set multiple endpoints.

    به عنوان مثال:

<!-- -->

``` xml
    <node index="1">
        <host>example_host</host>
        <port>2181</port>
    </node>
```

      The `index` attribute specifies the node order when trying to connect to the ZooKeeper cluster.

-   `session_timeout` — Maximum timeout for the client session in milliseconds.
-   `root` — The [حالت](http://zookeeper.apache.org/doc/r3.5.5/zookeeperOver.html#Nodes+and+ephemeral+nodes) استفاده شده است که به عنوان ریشه برای znodes استفاده شده توسط ClickHouse سرور. اختیاری.
-   `identity` — User and password, that can be required by ZooKeeper to give access to requested znodes. Optional.

**پیکربندی نمونه**

``` xml
<zookeeper>
    <node>
        <host>example1</host>
        <port>2181</port>
    </node>
    <node>
        <host>example2</host>
        <port>2181</port>
    </node>
    <session_timeout_ms>30000</session_timeout_ms>
    <operation_timeout_ms>10000</operation_timeout_ms>
    <!-- Optional. Chroot suffix. Should exist. -->
    <root>/path/to/zookeeper/node</root>
    <!-- Optional. Zookeeper digest ACL string. -->
    <identity>user:password</identity>
</zookeeper>
```

**همچنین نگاه کنید به**

-   [تکرار](../../engines/table-engines/mergetree-family/replication.md)
-   [راهنمای برنامه نویس باغ وحش](http://zookeeper.apache.org/doc/current/zookeeperProgrammers.html)

## سرویس پرداخت درونبرنامهای پلی {#server-settings-use_minimalistic_part_header_in_zookeeper}

روش ذخیره سازی برای هدر بخش داده ها در باغ وحش.

این تنظیم فقط در مورد `MergeTree` خانواده این را می توان مشخص کرد:

-   در سطح جهانی در [ادغام](#server_configuration_parameters-merge_tree) بخش از `config.xml` پرونده.

    تاتر با استفاده از تنظیمات برای تمام جداول بر روی سرور. شما می توانید تنظیمات را در هر زمان تغییر دهید. جداول موجود رفتار خود را تغییر دهید زمانی که تنظیمات تغییر می کند.

-   برای هر جدول.

    هنگام ایجاد یک جدول مربوطه را مشخص کنید [تنظیم موتور](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-creating-a-table). رفتار یک جدول موجود با این تنظیم تغییر نمی کند, حتی اگر تغییرات تنظیم جهانی.

**مقادیر ممکن**

-   0 — Functionality is turned off.
-   1 — Functionality is turned on.

اگر `use_minimalistic_part_header_in_zookeeper = 1` پس [تکرار](../../engines/table-engines/mergetree-family/replication.md) جداول هدر قطعات داده را با استفاده از یک واحد ذخیره می کنند `znode`. اگر جدول شامل بسیاری از ستون, این روش ذخیره سازی به طور قابل توجهی کاهش می دهد حجم داده های ذخیره شده در باغ وحش.

!!! attention "توجه"
    پس از استفاده از `use_minimalistic_part_header_in_zookeeper = 1` شما نمیتوانید سرور کلیک را به نسخه ای که از این تنظیم پشتیبانی نمی کند ارتقا دهید. مراقب باشید در هنگام به روز رسانی تاتر بر روی سرور در یک خوشه. همه سرورها را در یک زمان ارتقا ندهید. این امن تر است برای تست نسخه های جدید از خانه رعیتی در یک محیط تست, و یا فقط در چند سرور از یک خوشه.

      Data part headers already stored with this setting can't be restored to their previous (non-compact) representation.

**مقدار پیشفرض:** 0.

## نمایش سایت {#server-settings-disable-internal-dns-cache}

غیر فعال کش دی ان اس داخلی. توصیه شده برای کارخانه کلیک در سیستم
با زیرساخت های اغلب در حال تغییر مانند کوبرنتس.

**مقدار پیشفرض:** 0.

## پیدا کردن موقعیت جغرافیایی از روی شبکه {#server-settings-dns-cache-update-period}

دوره به روز رسانی نشانی های اینترنتی ذخیره شده در کش دی ان اس داخلی خانه (در ثانیه).
به روز رسانی همزمان انجام, در یک موضوع سیستم جداگانه.

**مقدار پیشفرض**: 15.

## _پوشه دستیابی {#access_control_path}

مسیر را به یک پوشه که یک سرور کلیک ذخیره کاربر و نقش تنظیمات ایجاد شده توسط دستورات گذاشتن.

مقدار پیشفرض: `/var/lib/clickhouse/access/`.

**همچنین نگاه کنید به**

-   [کنترل دسترسی و مدیریت حساب](../access-rights.md#access-control)

[مقاله اصلی](https://clickhouse.tech/docs/en/operations/server_configuration_parameters/settings/) <!--hide-->
