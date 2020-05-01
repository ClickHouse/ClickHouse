---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 19
toc_title: "\u0631\u0627\u0628\u0637 \u0642\u0627\u0645"
---

# رابط قام {#http-interface}

رابط اچ تی پی به شما امکان استفاده از کلیک بر روی هر پلت فرم از هر زبان برنامه نویسی. ما برای کار از جاوا و پرل و همچنین اسکریپت های پوسته استفاده می کنیم. در بخش های دیگر, رابط قام است از پرل استفاده, پایتون, و رفتن. رابط قام محدود تر از رابط بومی است, اما سازگاری بهتر.

به طور پیش فرض, کلیک سرور گوش برای اچ تی پی در بندر 8123 (این را می توان در پیکربندی تغییر).

اگر شما یک دریافت / درخواست بدون پارامتر, باز می گردد 200 کد پاسخ و رشته که در تعریف [نقلقولهای جدید از این نویسنده](../operations/server-configuration-parameters/settings.md#server_configuration_parameters-http_server_default_response) مقدار پیشفرض “Ok.” (با خوراک خط در پایان)

``` bash
$ curl 'http://localhost:8123/'
Ok.
```

استفاده از دریافت / درخواست پینگ در بهداشت و درمان چک اسکریپت. این کنترل همیشه باز می گردد “Ok.” (با یک خوراک خط در پایان). موجود از نسخه 18.12.13.

``` bash
$ curl 'http://localhost:8123/ping'
Ok.
```

ارسال درخواست به عنوان نشانی وب ‘query’ پارامتر, و یا به عنوان یک پست. و یا ارسال ابتدای پرس و جو در ‘query’ پارامتر, و بقیه در پست (بعدا توضیح خواهیم داد که چرا این لازم است). اندازه نشانی اینترنتی محدود به 16 کیلوبایت است بنابراین این را در نظر داشته باشید در هنگام ارسال نمایش داده شد بزرگ.

اگر موفق, شما دریافت 200 کد پاسخ و در نتیجه در بدن پاسخ.
اگر یک خطا رخ می دهد, شما در دریافت 500 کد پاسخ و یک متن شرح خطا در بدن پاسخ.

هنگام استفاده از روش دریافت, ‘readonly’ قرار است. به عبارت دیگر برای نمایش داده شد که تغییر داده ها شما فقط می توانید با استفاده از روش پست. شما می توانید پرس و جو خود را در قسمت پست یا در پارامتر نشانی وب ارسال کنید.

مثالها:

``` bash
$ curl 'http://localhost:8123/?query=SELECT%201'
1

$ wget -O- -q 'http://localhost:8123/?query=SELECT 1'
1

$ echo -ne 'GET /?query=SELECT%201 HTTP/1.0\r\n\r\n' | nc localhost 8123
HTTP/1.0 200 OK
Date: Wed, 27 Nov 2019 10:30:18 GMT
Connection: Close
Content-Type: text/tab-separated-values; charset=UTF-8
X-ClickHouse-Server-Display-Name: clickhouse.ru-central1.internal
X-ClickHouse-Query-Id: 5abe861c-239c-467f-b955-8a201abb8b7f
X-ClickHouse-Summary: {"read_rows":"0","read_bytes":"0","written_rows":"0","written_bytes":"0","total_rows_to_read":"0"}

1
```

همانطور که می بینید, حلقه تا حدودی ناخوشایند است که در فضاهای باید نشانی اینترنتی فرار.
اگر چه سازمان تجارت جهانی از همه چیز خود فرار می کند ما توصیه نمی کنیم از این استفاده کنیم زیرا هنگام استفاده از زنده ماندن و انتقال رمزگذاری به خوبی کار نمی کند 1.1.

``` bash
$ echo 'SELECT 1' | curl 'http://localhost:8123/' --data-binary @-
1

$ echo 'SELECT 1' | curl 'http://localhost:8123/?query=' --data-binary @-
1

$ echo '1' | curl 'http://localhost:8123/?query=SELECT' --data-binary @-
1
```

اگر بخشی از پرس و جو در پارامتر ارسال, و بخشی در پست, خوراک خط بین این دو بخش داده قرار داده.
مثال (این کار نخواهد کرد):

``` bash
$ echo 'ECT 1' | curl 'http://localhost:8123/?query=SEL' --data-binary @-
Code: 59, e.displayText() = DB::Exception: Syntax error: failed at position 0: SEL
ECT 1
, expected One of: SHOW TABLES, SHOW DATABASES, SELECT, INSERT, CREATE, ATTACH, RENAME, DROP, DETACH, USE, SET, OPTIMIZE., e.what() = DB::Exception
```

به طور پیش فرض, داده ها در قالب جدولبندی بازگشت (برای اطلاعات بیشتر, دیدن “Formats” بخش).
شما با استفاده از بند فرمت پرس و جو به درخواست هر فرمت دیگر.

``` bash
$ echo 'SELECT 1 FORMAT Pretty' | curl 'http://localhost:8123/?' --data-binary @-
┏━━━┓
┃ 1 ┃
┡━━━┩
│ 1 │
└───┘
```

روش پست انتقال داده ها برای درج نمایش داده شد لازم است. در این مورد می توانید ابتدا پرس و جو را در پارامتر نشانی وب بنویسید و از پست برای انتقال داده ها برای وارد کردن استفاده کنید. داده ها برای وارد کردن می تواند, مثلا, تخلیه تب جدا از خروجی زیر. در این راه وارد کردن پرس و جو جایگزین بارگذاری داده های محلی infile از mysql.

نمونه: ایجاد یک جدول:

``` bash
$ echo 'CREATE TABLE t (a UInt8) ENGINE = Memory' | curl 'http://localhost:8123/' --data-binary @-
```

با استفاده از قرار دادن پرس و جو برای درج داده ها:

``` bash
$ echo 'INSERT INTO t VALUES (1),(2),(3)' | curl 'http://localhost:8123/' --data-binary @-
```

داده ها را می توان به طور جداگانه از پرس و جو ارسال می شود:

``` bash
$ echo '(4),(5),(6)' | curl 'http://localhost:8123/?query=INSERT%20INTO%20t%20VALUES' --data-binary @-
```

شما می توانید هر فرمت داده را مشخص کنید. این ‘Values’ فرمت همان چیزی است که هنگام نوشتن به مقادیر تی استفاده می شود:

``` bash
$ echo '(7),(8),(9)' | curl 'http://localhost:8123/?query=INSERT%20INTO%20t%20FORMAT%20Values' --data-binary @-
```

برای وارد کردن داده ها از تخلیه زبانه جدا شده فرمت مربوطه را مشخص کنید:

``` bash
$ echo -ne '10\n11\n12\n' | curl 'http://localhost:8123/?query=INSERT%20INTO%20t%20FORMAT%20TabSeparated' --data-binary @-
```

خواندن محتویات جدول. داده ها خروجی به صورت تصادفی به دلیل پردازش پرس و جو موازی است:

``` bash
$ curl 'http://localhost:8123/?query=SELECT%20a%20FROM%20t'
7
8
9
10
11
12
1
2
3
4
5
6
```

حذف جدول.

``` bash
$ echo 'DROP TABLE t' | curl 'http://localhost:8123/' --data-binary @-
```

برای درخواست موفق است که یک جدول داده ها بازگشت نیست, بدن پاسخ خالی بازگشته است.

شما می توانید فرمت فشرده سازی کلیک داخلی در هنگام انتقال داده ها استفاده کنید. داده های فشرده دارای فرمت غیر استاندارد است و شما باید از ویژه استفاده کنید `clickhouse-compressor` برنامه ای برای کار با ان (با ان نصب شده است `clickhouse-client` بسته). برای افزایش بهره وری از درج داده, شما می توانید سرور سمت تایید کنترلی با استفاده از غیر فعال کردن [تغییر در حسابهای کاربری دستگاه](../operations/settings/settings.md#settings-http_native_compression_disable_checksumming_on_decompress) تنظیمات.

اگر شما مشخص `compress=1` در نشانی وب سرور دادههای ارسالی شما را فشرده میکند.
اگر شما مشخص `decompress=1` در نشانی اینترنتی کارگزار دادههای مشابهی را که در `POST` روش.

شما همچنین می توانید استفاده کنید را انتخاب کنید [فشردهسازی قام](https://en.wikipedia.org/wiki/HTTP_compression). برای ارسال یک فشرده `POST` درخواست, اضافه هدر درخواست `Content-Encoding: compression_method`. به منظور کلیک برای فشرده سازی پاسخ, شما باید اضافه `Accept-Encoding: compression_method`. پشتیبانی از کلیک `gzip`, `br` و `deflate` [روش های فشرده سازی](https://en.wikipedia.org/wiki/HTTP_compression#Content-Encoding_tokens). برای فعال کردن فشرده سازی قام, شما باید از خانه کلیک استفاده [نصب و راه اندازی](../operations/settings/settings.md#settings-enable_http_compression) تنظیمات. شما می توانید سطح فشرده سازی داده ها در پیکربندی [\_تنظیم مجدد به حالت اولیه](#settings-http_zlib_compression_level) تنظیم برای تمام روش های فشرده سازی.

شما می توانید این برای کاهش ترافیک شبکه در هنگام انتقال مقدار زیادی از داده ها و یا برای ایجاد افسردگی است که بلافاصله فشرده استفاده کنید.

نمونه هایی از ارسال داده ها با فشرده سازی:

``` bash
#Sending data to the server:
$ curl -vsS "http://localhost:8123/?enable_http_compression=1" -d 'SELECT number FROM system.numbers LIMIT 10' -H 'Accept-Encoding: gzip'

#Sending data to the client:
$ echo "SELECT 1" | gzip -c | curl -sS --data-binary @- -H 'Content-Encoding: gzip' 'http://localhost:8123/'
```

!!! note "یادداشت"
    برخی از مشتریان اچ تی پی ممکن است داده ها را از حالت فشرده خارج از سرور به طور پیش فرض (با `gzip` و `deflate`) و شما ممکن است داده ها از حالت فشرده خارج حتی اگر شما با استفاده از تنظیمات فشرده سازی به درستی.

شما می توانید از ‘database’ پارامتر نشانی وب برای مشخص کردن پایگاه داده به طور پیش فرض.

``` bash
$ echo 'SELECT number FROM numbers LIMIT 10' | curl 'http://localhost:8123/?database=system' --data-binary @-
0
1
2
3
4
5
6
7
8
9
```

به طور پیش فرض, پایگاه داده است که در تنظیمات سرور ثبت نام به عنوان پایگاه داده به طور پیش فرض استفاده. به طور پیش فرض, این پایگاه داده به نام است ‘default’. متناوبا, شما همیشه می توانید مشخص کردن پایگاه داده با استفاده از یک نقطه قبل از نام جدول.

نام کاربری و رمز عبور را می توان در یکی از سه راه نشان داد:

1.  با استفاده از احراز هویت اولیه. مثال:

<!-- -->

``` bash
$ echo 'SELECT 1' | curl 'http://user:password@localhost:8123/' -d @-
```

1.  در ‘user’ و ‘password’ پارامترهای نشانی وب. مثال:

<!-- -->

``` bash
$ echo 'SELECT 1' | curl 'http://localhost:8123/?user=user&password=password' -d @-
```

1.  با استفاده از ‘X-ClickHouse-User’ و ‘X-ClickHouse-Key’ سرصفحهها. مثال:

<!-- -->

``` bash
$ echo 'SELECT 1' | curl -H 'X-ClickHouse-User: user' -H 'X-ClickHouse-Key: password' 'http://localhost:8123/' -d @-
```

اگر نام کاربر مشخص نشده است `default` نام استفاده شده است. اگر رمز عبور مشخص نشده است, رمز عبور خالی استفاده شده است.
شما همچنین می توانید از پارامترهای نشانی وب برای مشخص کردن هر گونه تنظیمات برای پردازش یک پرس و جو یا کل پروفایل های تنظیمات استفاده کنید. هشدار داده می شودمشخصات=وب و حداکثر\_نظیم = 1000000000 & پرس و جو = انتخاب+1

برای کسب اطلاعات بیشتر, دیدن [تنظیمات](../operations/settings/index.md) بخش.

``` bash
$ echo 'SELECT number FROM system.numbers LIMIT 10' | curl 'http://localhost:8123/?' --data-binary @-
0
1
2
3
4
5
6
7
8
9
```

برای اطلاعات در مورد پارامترهای دیگر, بخش را ببینید “SET”.

به طور مشابه, شما می توانید جلسات کلیک در پروتکل قام استفاده. برای انجام این کار, شما نیاز به اضافه کردن `session_id` دریافت پارامتر به درخواست. شما می توانید هر رشته به عنوان شناسه جلسه استفاده کنید. به طور پیش فرض جلسه پس از 60 ثانیه عدم فعالیت خاتمه می یابد. برای تغییر این فاصله, تغییر `default_session_timeout` تنظیم در پیکربندی سرور یا اضافه کردن `session_timeout` دریافت پارامتر به درخواست. برای بررسی وضعیت جلسه از `session_check=1` پارامتر. فقط یک پرس و جو در یک زمان می تواند در یک جلسه اجرا شود.

شما می توانید اطلاعات در مورد پیشرفت یک پرس و جو در دریافت `X-ClickHouse-Progress` هدر پاسخ. برای انجام این کار, فعال کردن [نمایش سایت](../operations/settings/settings.md#settings-send_progress_in_http_headers). مثال توالی هدر:

``` text
X-ClickHouse-Progress: {"read_rows":"2752512","read_bytes":"240570816","total_rows_to_read":"8880128"}
X-ClickHouse-Progress: {"read_rows":"5439488","read_bytes":"482285394","total_rows_to_read":"8880128"}
X-ClickHouse-Progress: {"read_rows":"8783786","read_bytes":"819092887","total_rows_to_read":"8880128"}
```

زمینه های سربرگ احتمالی:

-   `read_rows` — Number of rows read.
-   `read_bytes` — Volume of data read in bytes.
-   `total_rows_to_read` — Total number of rows to be read.
-   `written_rows` — Number of rows written.
-   `written_bytes` — Volume of data written in bytes.

درخواست های در حال اجرا به طور خودکار متوقف نمی شود اگر اتصال قام از دست داده است. تجزیه و قالب بندی داده ها در سمت سرور انجام, و با استفاده از شبکه ممکن است بی اثر.
اختیاری ‘query\_id’ پارامتر را می توان به عنوان شناسه پرس و جو (هر رشته) منتقل می شود. برای کسب اطلاعات بیشتر به بخش مراجعه کنید “Settings, replace\_running\_query”.

اختیاری ‘quota\_key’ پارامتر را می توان به عنوان کلید سهمیه (هر رشته) منتقل می شود. برای کسب اطلاعات بیشتر به بخش مراجعه کنید “Quotas”.

رابط اچ تی پی اجازه می دهد تا عبور داده های خارجی (جداول موقت خارجی) برای پرس و جو. برای کسب اطلاعات بیشتر به بخش مراجعه کنید “External data for query processing”.

## بافر پاسخ {#response-buffering}

شما می توانید پاسخ بافر در سمت سرور را فعال کنید. این `buffer_size` و `wait_end_of_query` پارامترهای نشانی وب برای این منظور فراهم شده است.

`buffer_size` تعیین تعداد بایت در نتیجه به بافر در حافظه سرور. اگر یک بدن نتیجه بزرگتر از این حد است, بافر به کانال قام نوشته شده, و داده های باقی مانده به طور مستقیم به کانال قام ارسال.

برای اطمینان از اینکه کل پاسخ بافر شده است, تنظیم `wait_end_of_query=1`. در این مورد داده هایی که در حافظه ذخیره نمی شوند در یک فایل سرور موقت بافر می شوند.

مثال:

``` bash
$ curl -sS 'http://localhost:8123/?max_result_bytes=4000000&buffer_size=3000000&wait_end_of_query=1' -d 'SELECT toUInt8(number) FROM system.numbers LIMIT 9000000 FORMAT RowBinary'
```

استفاده از بافر برای جلوگیری از شرایطی که یک خطای پردازش پرس و جو رخ داده است پس از کد پاسخ و هدر قام به مشتری ارسال شد. در این وضعیت یک پیغام خطا نوشته شده است در پایان پاسخ بدن و در سمت سرویس گیرنده خطا تنها می تواند تشخیص داده شده در مرحله تجزیه.

### نمایش داده شد با پارامترهای {#cli-queries-with-parameters}

شما می توانید پرس و جو را با پارامترها ایجاد کنید و مقادیر را از پارامترهای درخواست مربوط به اچ تی پی منتقل کنید. برای کسب اطلاعات بیشتر, دیدن [نمایش داده شد با پارامترهای کلی](cli.md#cli-queries-with-parameters).

### مثال {#example}

``` bash
$ curl -sS "<address>?param_id=2&param_phrase=test" -d "SELECT * FROM table WHERE int_column = {id:UInt8} and string_column = {phrase:String}"
```

## حذف میانبر در صفحه خانه {#predefined_http_interface}

تاتر پشتیبانی از نمایش داده شد خاص از طریق رابط قام. مثلا, شما می توانید داده ها را به یک جدول به شرح زیر ارسال:

``` bash
$ echo '(4),(5),(6)' | curl 'http://localhost:8123/?query=INSERT%20INTO%20t%20VALUES' --data-binary @-
```

کلیک هاوس همچنین از رابط اچ تی پی از پیش تعریف شده پشتیبانی می کند که می تواند به شما در ادغام راحت تر با ابزارهای شخص ثالث مانند کمک کند [پرومته صادر کننده](https://github.com/percona-lab/clickhouse_exporter).

مثال:

-   اول از همه, اضافه کردن این بخش به فایل پیکربندی سرور:

<!-- -->

``` xml
<http_handlers>
  <predefine_query_handler>
      <url>/metrics</url>
        <method>GET</method>
        <queries>
            <query>SELECT * FROM system.metrics LIMIT 5 FORMAT Template SETTINGS format_template_resultset = 'prometheus_template_output_format_resultset', format_template_row = 'prometheus_template_output_format_row', format_template_rows_between_delimiter = '\n'</query>
        </queries>
  </predefine_query_handler>
</http_handlers>
```

-   شما هم اکنون می توانید لینک را مستقیما برای داده ها در قالب پرومته درخواست کنید:

<!-- -->

``` bash
curl -vvv 'http://localhost:8123/metrics'
*   Trying ::1...
* Connected to localhost (::1) port 8123 (#0)
> GET /metrics HTTP/1.1
> Host: localhost:8123
> User-Agent: curl/7.47.0
> Accept: */*
>
< HTTP/1.1 200 OK
< Date: Wed, 27 Nov 2019 08:54:25 GMT
< Connection: Keep-Alive
< Content-Type: text/plain; charset=UTF-8
< X-ClickHouse-Server-Display-Name: i-tl62qd0o
< Transfer-Encoding: chunked
< X-ClickHouse-Query-Id: f39235f6-6ed7-488c-ae07-c7ceafb960f6
< Keep-Alive: timeout=3
< X-ClickHouse-Summary: {"read_rows":"0","read_bytes":"0","written_rows":"0","written_bytes":"0","total_rows_to_read":"0"}
<
# HELP "Query" "Number of executing queries"
# TYPE "Query" counter
"Query" 1

# HELP "Merge" "Number of executing background merges"
# TYPE "Merge" counter
"Merge" 0

# HELP "PartMutation" "Number of mutations (ALTER DELETE/UPDATE)"
# TYPE "PartMutation" counter
"PartMutation" 0

# HELP "ReplicatedFetch" "Number of data parts being fetched from replica"
# TYPE "ReplicatedFetch" counter
"ReplicatedFetch" 0

# HELP "ReplicatedSend" "Number of data parts being sent to replicas"
# TYPE "ReplicatedSend" counter
"ReplicatedSend" 0

* Connection #0 to host localhost left intact
```

همانطور که شما می توانید از مثال ببینید, اگر `<http_handlers>` در پیکربندی پیکربندی پیکربندی شده است.xml فایل ClickHouse را مطابقت با درخواست های HTTP به دریافت این نوع از پیش تعریف شده در `<http_handlers>`, سپس کلیک خانه پرس و جو مربوطه از پیش تعریف شده اجرا اگر بازی موفق است.

حالا `<http_handlers>` می توانید پیکربندی کنید `<root_handler>`, `<ping_handler>`, `<replicas_status_handler>`, `<dynamic_query_handler>` و `<no_handler_description>` .

## روی\_خندلر {#root_handler}

`<root_handler>` بازگرداندن محتوای مشخص شده برای درخواست مسیر ریشه. محتوای بازگشتی خاص توسط پیکربندی شده است `http_server_default_response` در پیکربندی.. اگر مشخص نشده, برگشت **باشه**

`http_server_default_response` تعریف نشده است و درخواست قام به کلیک ارسال می شود. نتیجه به شرح زیر است:

``` xml
<http_handlers>
    <root_handler/>
</http_handlers>
```

    $ curl 'http://localhost:8123'
    Ok.

`http_server_default_response` تعریف شده است و درخواست قام ارسال شده است به کلیک. نتیجه به شرح زیر است:

``` xml
<http_server_default_response><![CDATA[<html ng-app="SMI2"><head><base href="http://ui.tabix.io/"></head><body><div ui-view="" class="content-ui"></div><script src="http://loader.tabix.io/master.js"></script></body></html>]]></http_server_default_response>

<http_handlers>
    <root_handler/>
</http_handlers>
```

    $ curl 'http://localhost:8123'
    <html ng-app="SMI2"><head><base href="http://ui.tabix.io/"></head><body><div ui-view="" class="content-ui"></div><script src="http://loader.tabix.io/master.js"></script></body></html>%

## پینگ\_ هندلر {#ping_handler}

`<ping_handler>` می توان برای بررسی سلامت سرور فعلی کلیک استفاده کرد. زمانی که ClickHouse سرور HTTP طبیعی است دسترسی به ClickHouse از طریق `<ping_handler>` باز خواهد گشت **باشه**.

مثال:

``` xml
<http_handlers>
    <ping_handler>/ping</ping_handler>
</http_handlers>
```

``` bash
$ curl 'http://localhost:8123/ping'
Ok.
```

## تکرار\_ستاتوس\_هندلر {#replicas_status_handler}

`<replicas_status_handler>` برای تشخیص وضعیت گره ماکت و بازگشت استفاده می شود **باشه** اگر گره ماکت هیچ تاخیر. در صورتی که برای تاخیر وجود دارد, بازگشت تاخیر خاص. ارزش `<replicas_status_handler>` پشتیبانی از سفارشی سازی. اگر مشخص نکنید `<replicas_status_handler>` تنظیمات پیشفرض کلیک `<replicas_status_handler>` هست **/ بازتولیتوس**.

مثال:

``` xml
<http_handlers>
    <replicas_status_handler>/replicas_status</replicas_status_handler>
</http_handlers>
```

هیچ مورد تاخیر:

``` bash
$ curl 'http://localhost:8123/replicas_status'
Ok.
```

مورد تاخیر:

``` bash
$ curl 'http://localhost:8123/replicas_status'
db.stats:  Absolute delay: 22. Relative delay: 22.
```

## باز تعریف {#predefined_query_handler}

شما می توانید پیکربندی کنید `<method>`, `<headers>`, `<url>` و `<queries>` داخل `<predefined_query_handler>`.

`<method>` برای تطبیق روش بخشی از درخواست قام است. `<method>` به طور کامل مطابق با تعریف [روش](https://developer.mozilla.org/en-US/docs/Web/HTTP/Methods) در پروتکل قام. این پیکربندی اختیاری است. اگر در فایل پیکربندی تعریف نشده باشد با بخش روش درخواست قام مطابقت ندارد

`<url>` وظیفه تطبیق بخشی نشانی وب از درخواست قام است. این سازگار با است [RE2](https://github.com/google/re2)عبارات منظم است. این پیکربندی اختیاری است. اگر در فایل پیکربندی تعریف نشده باشد با بخش نشانی وب درخواست قام مطابقت ندارد

`<headers>` برای تطبیق بخش هدر درخواست قام است. این است که سازگار با عبارات منظم را دوباره2 است. این پیکربندی اختیاری است. اگر در فایل پیکربندی تعریف نشده باشد با بخش هدر درخواست قام مطابقت ندارد

`<queries>` مقدار پرس و جو از پیش تعریف شده است `<predefined_query_handler>`, است که توسط کلیکهاوس اجرا زمانی که یک درخواست قام همسان است و در نتیجه از پرس و جو بازگشته است. این پیکربندی باید است.

`<predefined_query_handler>` پشتیبانی از تنظیمات و مقادیر قوری\_پرم.

مثال زیر مقادیر را تعریف می کند `max_threads` و `max_alter_threads` تنظیمات, سپس نمایش داده شد جدول سیستم برای بررسی اینکه این تنظیمات با موفقیت تعیین شد.

مثال:

``` xml
<root_handlers>
    <predefined_query_handler>
        <method>GET</method>
        <headers>
            <XXX>TEST_HEADER_VALUE</XXX>
            <PARAMS_XXX><![CDATA[(?P<name_1>[^/]+)(/(?P<name_2>[^/]+))?]]></PARAMS_XXX>
        </headers>
        <url><![CDATA[/query_param_with_url/\w+/(?P<name_1>[^/]+)(/(?P<name_2>[^/]+))?]]></url>
        <queries>
            <query>SELECT value FROM system.settings WHERE name = {name_1:String}</query>
            <query>SELECT name, value FROM system.settings WHERE name = {name_2:String}</query>
        </queries>
    </predefined_query_handler>
</root_handlers>
```

``` bash
$ curl -H 'XXX:TEST_HEADER_VALUE' -H 'PARAMS_XXX:max_threads' 'http://localhost:8123/query_param_with_url/1/max_threads/max_alter_threads?max_threads=1&max_alter_threads=2'
1
max_alter_threads   2
```

!!! note "یادداشت"
    در یک `<predefined_query_handler>` یک `<queries>` تنها پشتیبانی از یک `<query>` از یک نوع درج.

## هشدار داده می شود {#dynamic_query_handler}

`<dynamic_query_handler>` از `<predefined_query_handler>` افزایش `<query_param_name>` .

عصاره کلیک و اجرا ارزش مربوط به `<query_param_name>` مقدار در نشانی وب درخواست قام.
تنظیم پیشفرض کلیک `<query_param_name>` هست `/query` . این پیکربندی اختیاری است. در صورتی که هیچ تعریف در فایل پیکربندی وجود دارد, پرم در تصویب نشده است.

برای آزمایش این قابلیت به عنوان مثال تعریف ارزش از max\_threads و max\_alter\_threads و پرس و جو که آیا تنظیمات راه اندازی شد با موفقیت.
تفاوت این است که در `<predefined_query_handler>`, پرس و جو در فایل پیکربندی نوشت. اما در `<dynamic_query_handler>`, پرس و جو در قالب پرام از درخواست قام نوشته شده است.

مثال:

``` xml
<root_handlers>
    <dynamic_query_handler>
        <headers>
            <XXX>TEST_HEADER_VALUE_DYNAMIC</XXX>
            <PARAMS_XXX><![CDATA[(?P<param_name_1>[^/]+)(/(?P<param_name_2>[^/]+))?]]></PARAMS_XXX>
        </headers>
        <query_param_name>query_param</query_param_name>
    </dynamic_query_handler>
</root_handlers>
```

``` bash
$ curl  -H 'XXX:TEST_HEADER_VALUE_DYNAMIC' -H 'PARAMS_XXX:max_threads' 'http://localhost:8123/?query_param=SELECT%20value%20FROM%20system.settings%20where%20name%20=%20%7Bname_1:String%7D%20OR%20name%20=%20%7Bname_2:String%7D&max_threads=1&max_alter_threads=2&param_name_2=max_alter_threads'
1
2
```

[مقاله اصلی](https://clickhouse.tech/docs/en/interfaces/http_interface/) <!--hide-->
