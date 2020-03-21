<div markdown="1" markdown="1" dir="rtl">

# کلاینت Command-line {#khlynt-command-line}

برای کار از طریق محیط ترمینال میتوانید از دستور `clickhouse-client` استفاده کنید

</div>

``` bash
$ clickhouse-client
ClickHouse client version 0.0.26176.
Connecting to localhost:9000.
Connected to ClickHouse server version 0.0.26176.

:)
```

<div markdown="1" markdown="1" dir="rtl">

کلاینت از آپشن های command-line و فایل های کانفیگ پشتیبانی می کند. برای اطلاعات بیشتر بخش «[پیکربندی](#interfaces_cli_configuration)» را مشاهده کنید.

## استفاده {#stfdh}

کلاینت می تواند به دو صورت interactive و non-intercative (batch) مورد استفاده قرار گیرد. برای استفاده از حالت batch، پارامتر `query` را مشخص کنید، و یا داده ها ره به `stdin` ارسال کنید (کلاینت تایید می کند که `stdin` ترمینال نیست) و یا از هر 2 استفاده کنید. مشابه HTTP interface، هنگامی که از از پارامتر `query` و ارسال داده ها به `stdin` استفاده می کنید، درخواست، ترکیبی از پارامتر `query`، line feed، و داده ها در `stdin` است. این کار برای query های بزرگ INSERT مناسب است.

مثالی از استفاده کلاینت برای اجرای دستور INSERT داده

</div>

``` bash
echo -ne "1, 'some text', '2016-08-14 00:00:00'\n2, 'some more text', '2016-08-14 00:00:01'" | clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";

cat <<_EOF | clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";
3, 'some text', '2016-08-14 00:00:00'
4, 'some more text', '2016-08-14 00:00:01'
_EOF

cat file.csv | clickhouse-client --database=test --query="INSERT INTO test FORMAT CSV";
```

<div markdown="1" markdown="1" dir="rtl">

در حالت Batch، فرمت داده ها به صورت پیش فرض به صورت TabSeparated می باشد. شما میتوانید فرمت داده ها رو در هنگام اجرای query و با استفاده از شرط FORMAT مشخص کنید.

به طور پیش فرض شما فقط می توانید یک query را در خالت batch اجرا کنید.برای ساخت چندین query از یک «اسکریپت»، از پارامتر –multiquery استفاده کنید. این روش برای تمام query ها به جز INSERT کار می کند. نتایج query ها به صورت متوالی و بدون seperator اضافه تولید می شوند. به طور مشابه برای پردازش تعداد زیادی از query ها شما می توانید از ‘clickhouse-client’ برای هر query استفاده کنید. دقت کنید که ممکن است حدود 10 میلی ثانیه تا زمان راه اندازی برنامه ‘clickhouse-client’ زمان گرفته شود.

در حالت intercative، شما یک command line برای درج query های خود دریافت می کنید.

اگر ‘multiline’ مشخص نشده باشد (به صورت پیش فرض): برای اجرای یک query، دکمه Enter را بزنید. سیمی کالن در انتهای query اجباری نیست. برای درج یک query چند خطی (multiline)، دکمه ی بک اسلش `\` را قبل از line feed فشار دهید. بعد از فشردن Enter، از شما برای درج خط بعدی query درخواست خواهد شد.

اگر چند خطی (multiline) مشخص شده باشد: برای اجرای query، در انتها سیمی کالن را وارد کنید و سپس Enter بزنید. اگر سیمی کالن از انتهای خط حذف می شد، از شما برای درج خط جدید query درخواست می شد.

تنها یک query اجرا می شود. پس همه چیز بعد از سیمی کالن ignore می شود.

شما میتوانید از `\G` به جای سیمی کالن یا بعد از سیمی کالن استفاده کنید. این علامت، فرمت Vertical را نشان می دهد. در این فرمت، هر مقدار در یک خط جدا چاپ می شود که برای جداول عریض مناسب است. این ویژگی غیرمعمول برای سازگاری با MySQL CLI اضافه شد.

command line برا پایه ‘replxx’ می باشد. به عبارت دیگر، این محیط از shortcut های آشنا استفاده می کند و history دستورات را نگه می دار. history ها در فایل ~/.clickhouse-client-history نوشته می شوند.

به صورت پیش فرض فرمت خروجی PrettyCompact می باشد. شما میتوانید از طریق دستور FORMAT در یک query، یا با مشخص کردن `\G` در انتهای query، استفاده از آرگومان های `--format` یا `--vertical` یا از کانفیگ فایل کلاینت، فرمت خروجی را مشخص کنید.

برای خروج از کلاینت، Ctrl-D (یا Ctrl+C) را فشار دهید؛ و یا یکی از دستورات زیر را به جای اجرای query اجرا کنید: «exit», «quit», «logout», «exit;», «quit;», «logout;», «q», «Q», «:q»

در هنگام اجرای یک query، کلاینت موارد زیر را نمایش می دهد:

1.  Progress، که بیش از 10 بار در ثانیه بروز نخواهد شد ( به صورت پیش فرض). برای query های سریع، progress ممکن است زمانی برای نمایش پیدا نکند.
2.  فرمت کردن query بعد از عملیات پارس کردن، به منظور دیباگ کردن query.
3.  نمایش خروجی با توجه به نوع فرمت.
4.  تعداد لاین های خروجی، زمان پاس شدن query، و میانگیم سرعت پردازش query.

شما میتوانید query های طولانی را با فشردن Ctrl-C کنسل کنید. هر چند، بعد از این کار همچنان نیاز به انتظار چند ثانیه ای برای قطع کردن درخواست توسط سرور می باشید. امکان کنسل کردن یک query در مراحل خاص وجود ندارد. اگر شما صبر نکنید و برای بار دوم Ctrl+C را وارد کنید از client خارج می شوید.

کلاینت commant-line اجازه ی پاس دادن داده های external (جداول موقت external) را برای query ها می دهد. برای اطلاعات بیشتر به بخش «داده های External برای پردازش query» مراجعه کنید.

## پیکربندی {#interfaces-cli-configuration}

شما میتوانید، پارامتر ها را به `clickhouse-client` (تمام پارامترها دارای مقدار پیش فرض هستند) از دو روش زیر پاس بدید:

-   از طریق Command Line

    گزینه های Command-line مقادیر پیش فرض در ستینگ و کانفیگ فایل را نادیده میگیرد.

-   کانفیگ فایل ها.

    ستینگ های داخل کانفیگ فایل، مقادیر پیش فرض را نادیده می گیرد.

### گزینه های Command line {#gzynh-hy-command-line}

-   `--host, -h` -– نام سرور، به صورت پیش فرض ‘localhost’ است. شما میتوانید یکی از موارد نام و یا IPv4 و یا IPv6 را در این گزینه مشخص کنید.
-   `--port` – پورت اتصال به ClickHouse. مقدار پیش فرض: 9000. دقت کنید که پرت اینترفیس HTTP و اینتفریس native متفاوت است.
-   `--user, -u` – نام کاربری جهت اتصال. پیش فرض: default.
-   `--password` – پسورد جهت اتصال. پیش فرض: خالی
-   `--query, -q` – مشخص کردن query برای پردازش در هنگام استفاده از حالت non-interactive.
-   `--database, -d` – انتخاب دیتابیس در بدو ورود به کلاینت. مقدار پیش فرض: دیتابیس مشخص شده در تنظیمات سرور (پیش فرض ‘default’)
-   `--multiline, -m` – اگر مشخص شود، یعنی اجازه ی نوشتن query های چند خطی را بده. (بعد از Enter، query را ارسال نکن).
-   `--multiquery, -n` – اگر مشخص شود، اجازه ی اجرای چندین query که از طریق جمع و حلقه ها جدا شده اند را می دهد. فقط در حالت non-interactive کار می کند.
-   `--format, -f` مشخص کردن نوع فرمت خروجی
-   `--vertical, -E` اگر مشخص شود، از فرمت Vertical برای نمایش خروجی استفاده می شود. این گزینه مشابه ‘–format=Vertical’ می باشد. در این فرمت، هر مقدار در یک خط جدید چاپ می شود، که در هنگام نمایش جداول عریض مفید است.
-   `--time, -t` اگر مشخص شود، در حالت non-interactive زمان اجرای query در ‘stderr’ جاپ می شود.
-   `--stacktrace` – اگر مشخص شود stack trase مربوط به اجرای query در هنگام رخ دادن یک exception چاپ می شود.
-   `--config-file` – نام فایل پیکربندی.

### فایل های پیکربندی {#fyl-hy-pykhrbndy}

`clickhouse-client` به ترتیب اولویت زیر از اولین فایل موجود برای ست کردن تنظیمات استفاده می کند:

-   مشخص شده در پارامتر `--config-file`
-   `./clickhouse-client.xml`
-   `\~/.clickhouse-client/config.xml`
-   `/etc/clickhouse-client/config.xml`

مثالی از یک کانفیگ فایل

</div>

``` xml
<config>
    <user>username</user>
    <password>password</password>
</config>
```

[مقاله اصلی](https://clickhouse.tech/docs/fa/interfaces/cli/) <!--hide-->
