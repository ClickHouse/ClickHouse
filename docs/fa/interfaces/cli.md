---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 17
toc_title: "\u0645\u0634\u062A\u0631\u06CC \u062E\u0637 \u0641\u0631\u0645\u0627\u0646"
---

# مشتری خط فرمان {#command-line-client}

تاتر یک مشتری خط فرمان بومی فراهم می کند: `clickhouse-client`. مشتری پشتیبانی از گزینه های خط فرمان و فایل های پیکربندی. برای کسب اطلاعات بیشتر, دیدن [پیکربندی](#interfaces_cli_configuration).

[نصب](../getting-started/index.md) این از `clickhouse-client` بسته بندی و اجرا با فرمان `clickhouse-client`.

``` bash
$ clickhouse-client
ClickHouse client version 19.17.1.1579 (official build).
Connecting to localhost:9000 as user default.
Connected to ClickHouse server version 19.17.1 revision 54428.

:)
```

مشتری و سرور نسخه های مختلف سازگار با یکدیگر هستند, اما برخی از ویژگی های ممکن است در مشتریان قدیمی تر در دسترس باشد. ما توصیه می کنیم با استفاده از همان نسخه از مشتری به عنوان برنامه سرور. هنگامی که شما سعی می کنید به استفاده از یک مشتری از نسخه های قدیمی تر و سپس سرور, `clickhouse-client` پیام را نمایش میدهد:

      ClickHouse client version is older than ClickHouse server. It may lack support for new features.

## استفاده {#cli_usage}

مشتری را می توان در حالت تعاملی و غیر تعاملی (دسته ای) استفاده کرد. برای استفاده از حالت دسته ای ‘query’ پارامتر یا ارسال داده به ‘stdin’ (تایید می کند که ‘stdin’ یک ترمینال نیست), یا هر دو. هنگام استفاده از رابط اچ تی پی مشابه است ‘query’ پارامتر و ارسال داده به ‘stdin’, درخواست یک الحاق از است ‘query’ پارامتر, خوراک خط, و داده ها در ‘stdin’. این مناسب برای نمایش داده شد درج بزرگ است.

نمونه ای از استفاده از مشتری برای وارد کردن داده ها:

``` bash
$ echo -ne "1, 'some text', '2016-08-14 00:00:00'\n2, 'some more text', '2016-08-14 00:00:01'" | clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";

$ cat <<_EOF | clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";
3, 'some text', '2016-08-14 00:00:00'
4, 'some more text', '2016-08-14 00:00:01'
_EOF

$ cat file.csv | clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";
```

در حالت دسته ای, فرمت داده ها به طور پیش فرض جدول است. شما می توانید فرمت را در بند فرمت پرس و جو تنظیم کنید.

به طور پیش فرض, شما فقط می توانید پردازش یک پرس و جو تنها در حالت دسته ای. برای ایجاد چندین نمایش داده شد از یک “script,” استفاده از `--multiquery` پارامتر. این برای همه نمایش داده شد به جز درج کار می کند. نتایج پرس و جو خروجی متوالی بدون جداکننده های اضافی می باشد. به طور مشابه, برای پردازش تعداد زیادی از نمایش داده شد, شما می توانید اجرا ‘clickhouse-client’ برای هر پرس و جو. توجه داشته باشید که ممکن است دهها میلی ثانیه برای راه اندازی ‘clickhouse-client’ برنامه

در حالت تعاملی شما یک خط فرمان دریافت می کنید که می توانید نمایش داده شده را وارد کنید.

اگر ‘multiline’ مشخص نشده است (به طور پیش فرض): برای اجرای پرس و جو را فشار دهید را وارد کنید. نقطه و ویرگول در پایان پرس و جو لازم نیست. برای ورود به پرس و جو چند خطی یک بک اسلش را وارد کنید `\` قبل از خط تغذیه. بعد از اینکه شما فشار وارد, از شما خواسته خواهد شد که برای ورود به خط بعدی از پرس و جو.

اگر چند خطی مشخص شده است: برای اجرای یک پرس و جو, پایان با یک نقطه و ویرگول و مطبوعات را وارد کنید. اگر نقطه و ویرگول در پایان خط وارد شده حذف شد, از شما خواسته خواهد شد برای ورود به خط بعدی از پرس و جو.

فقط یک پرس و جو تنها اجرا می شود, بنابراین همه چیز پس از نقطه و ویرگول نادیده گرفته شده است.

شما می توانید مشخص کنید `\G` بجای یا بعد از نقطه و ویرگول. این نشان می دهد فرمت عمودی. در این قالب, هر مقدار بر روی یک خط جداگانه چاپ, مناسب است که برای جداول گسترده ای. این ویژگی غیر معمول برای سازگاری با خروجی زیر کلی اضافه شد.

خط فرمان بر اساس ‘replxx’ (شبیه به ‘readline’). به عبارت دیگر از میانبرهای صفحهکلید اشنایی استفاده میکند و تاریخ را حفظ میکند. تاریخ به نوشته شده است `~/.clickhouse-client-history`.

به طور پیش فرض, فرمت استفاده می شود قبل از شکست است. شما می توانید فرمت را در بند فرمت پرس و جو یا با مشخص کردن تغییر دهید `\G` در پایان پرس و جو, با استفاده از `--format` یا `--vertical` استدلال در خط فرمان, و یا با استفاده از فایل پیکربندی مشتری.

برای خروج از مشتری, کنترل مطبوعات+د (یا کنترل+ج), و یا یکی از موارد زیر را وارد کنید به جای یک پرس و جو: “exit”, “quit”, “logout”, “exit;”, “quit;”, “logout;”, “q”, “Q”, “:q”

هنگامی که پردازش یک پرس و جو مشتری نشان می دهد:

1.  پیشرفت, که به روز شده است بیش از 10 بار در ثانیه (به طور پیش فرض). برای نمایش داده شد سریع پیشرفت ممکن است زمان نمایش داده می شود.
2.  پرس و جو فرمت شده پس از تجزیه, برای اشکال زدایی.
3.  نتیجه در قالب مشخص شده است.
4.  تعداد خطوط در نتیجه زمان گذشت و سرعت متوسط پردازش پرس و جو.

شما می توانید یک پرس و جو طولانی با فشار دادن کنترل لغو+ج.با این حال, شما هنوز هم نیاز به کمی صبر کنید برای سرور به سقط درخواست. این ممکن است به لغو پرس و جو در مراحل خاص. اگر شما منتظر نیست و مطبوعات کنترل+ج بار دوم مشتری خروج خواهد شد.

مشتری خط فرمان اجازه می دهد تا عبور داده های خارجی (جداول موقت خارجی) برای پرس و جو. برای کسب اطلاعات بیشتر به بخش مراجعه کنید “External data for query processing”.

### نمایش داده شد با پارامترهای {#cli-queries-with-parameters}

شما می توانید پرس و جو را با پارامترها ایجاد کنید و مقادیر را از برنامه مشتری منتقل کنید. این اجازه می دهد تا برای جلوگیری از قالب بندی پرس و جو با ارزش های پویا خاص در سمت سرویس گیرنده. به عنوان مثال:

``` bash
$ clickhouse-client --param_parName="[1, 2]"  -q "SELECT * FROM table WHERE a = {parName:Array(UInt16)}"
```

#### نحو پرس و جو {#cli-queries-with-parameters-syntax}

یک پرس و جو را به طور معمول فرمت کنید و سپس مقادیر را که می خواهید از پارامترهای برنامه به پرس و جو در پرانتز در قالب زیر منتقل کنید قرار دهید:

``` sql
{<name>:<data type>}
```

-   `name` — Placeholder identifier. In the console client it should be used in app parameters as `--param_<name> = value`.
-   `data type` — [نوع داده](../sql-reference/data-types/index.md) از مقدار پارامتر برنامه. برای مثال یک ساختار داده مانند `(integer, ('string', integer))` می تواند داشته باشد `Tuple(UInt8, Tuple(String, UInt8))` نوع داده (شما همچنین می توانید از یکی دیگر استفاده کنید [عدد صحیح](../sql-reference/data-types/int-uint.md) انواع).

#### مثال {#example}

``` bash
$ clickhouse-client --param_tuple_in_tuple="(10, ('dt', 10))" -q "SELECT * FROM table WHERE val = {tuple_in_tuple:Tuple(UInt8, Tuple(String, UInt8))}"
```

## پیکربندی {#interfaces_cli_configuration}

شما می توانید پارامترها را به `clickhouse-client` (همه پارامترها یک مقدار پیش فرض) با استفاده از:

-   از خط فرمان

    گزینه های خط فرمان نادیده گرفتن مقادیر پیش فرض و تنظیمات در فایل های پیکربندی.

-   فایل های پیکربندی.

    تنظیمات در فایل های پیکربندی نادیده گرفتن مقادیر پیش فرض.

### گزینههای خط فرمان {#command-line-options}

-   `--host, -h` -– The server name, ‘localhost’ به طور پیش فرض. شما می توانید از نام یا نشانی اینترنتی4 یا ایپو6 استفاده کنید.
-   `--port` – The port to connect to. Default value: 9000. Note that the HTTP interface and the native interface use different ports.
-   `--user, -u` – The username. Default value: default.
-   `--password` – The password. Default value: empty string.
-   `--query, -q` – The query to process when using non-interactive mode.
-   `--database, -d` – Select the current default database. Default value: the current database from the server settings (‘default’ به طور پیش فرض).
-   `--multiline, -m` – If specified, allow multiline queries (do not send the query on Enter).
-   `--multiquery, -n` – If specified, allow processing multiple queries separated by semicolons.
-   `--format, -f` – Use the specified default format to output the result.
-   `--vertical, -E` – If specified, use the Vertical format by default to output the result. This is the same as ‘–format=Vertical’. در این قالب, هر مقدار بر روی یک خط جداگانه چاپ, مفید است که در هنگام نمایش جداول گسترده.
-   `--time, -t` – If specified, print the query execution time to ‘stderr’ در حالت غیر تعاملی.
-   `--stacktrace` – If specified, also print the stack trace if an exception occurs.
-   `--config-file` – The name of the configuration file.
-   `--secure` – If specified, will connect to server over secure connection.
-   `--param_<name>` — Value for a [پرسوجو با پارامترها](#cli-queries-with-parameters).

### پروندههای پیکربندی {#configuration_files}

`clickhouse-client` با استفاده از اولین فایل موجود در زیر:

-   تعریف شده در `--config-file` پارامتر.
-   `./clickhouse-client.xml`
-   `~/.clickhouse-client/config.xml`
-   `/etc/clickhouse-client/config.xml`

نمونه ای از یک فایل پیکربندی:

``` xml
<config>
    <user>username</user>
    <password>password</password>
    <secure>False</secure>
</config>
```

[مقاله اصلی](https://clickhouse.tech/docs/en/interfaces/cli/) <!--hide-->
