<div dir="rtl" markdown="1">

# HTTP interface

HTTP interface به شما امکان استفاده از ClickHpuse در هر پلتفرم با هر زمان برنامه نویسی را می دهد. ما از این Interface برای زبان های Java و Perl به مانند shell استفاده می کنیم. در دیگر دپارتمان ها، HTTP interface در Perl، Python، و Go استفاده می شود. HTTP Interface محدود تر از native interface می باشد، اما سازگاری بهتری دارد.

به صورت پیش فرض، clickhouse-server به پرت 8123 در HTTP گوش می دهد. (میتونه در کانفیگ فایل تغییر پیدا کنه). اگر شما یک درخواست GET / بدون پارامتر بسازید، رشته ی "OK" رو دریافت می کنید (به همراه line feed در انتها). شما می توانید از این درخواست برای اسکریپت های health-check استفاده کنید.

</div>

```bash
$ curl 'http://localhost:8123/'
Ok.
```

<div dir="rtl" markdown="1">

درخواست های خود را پارامتر 'query'، یا با متد POST، یا ابتدای query را در پارامتر 'query' ارسال کنید، و بقیه را در POST (بعدا توضیح خواهیم داد که چرا این کار ضروری است). سایت URL محدود به 16 کیلوبایت است، پس هنگام ارسال query های بزرگ، اینو به خاطر داشته باشید

اگر درخواست موفق آمیز باشد، استاتوس کد 200 را دریافت می کنید و نتایج در بدنه response می باشد. اگر اروری رخ دهد، استاتوس کد 500 را دریافت می کنید و توضیحات ارور در بدنه ی reponse قرار می گیرد.

در هنگام استفاده از متد GET، 'readonly' ست می شود. به عبارت دیگر، برای query هایی که قصد تغییر دیتا را دارند، شما فقط از طریق متد POST می توانید این تغییرات را انجام دهید. شما میتونید query رو در بدنه ی POST یه یا به عنوان پارامتر های URL ارسال کنید.

مثال:

</div>

```bash
$ curl 'http://localhost:8123/?query=SELECT%201'
1

$ wget -O- -q 'http://localhost:8123/?query=SELECT 1'
1

$ echo -ne 'GET /?query=SELECT%201 HTTP/1.0\r\n\r\n' | nc localhost 8123
HTTP/1.0 200 OK
Connection: Close
Date: Fri, 16 Nov 2012 19:21:50 GMT

1
```

<div dir="rtl" markdown="1">

همانطور که می بینید،  curl is somewhat inconvenient in that spaces must be URL escaped. هر چند wget همه چیز را خودش escape می کنه، ما توصیه به استفاده از اون رو نمی کنیم، چون wget به خوبی با HTTP 1.1 در هنگام استفاده از هدر های keep-alive و Transfer-Encoding: chunked کار نمی کند.

</div>

```bash
$ echo 'SELECT 1' | curl 'http://localhost:8123/' --data-binary @-
1

$ echo 'SELECT 1' | curl 'http://localhost:8123/?query=' --data-binary @-
1

$ echo '1' | curl 'http://localhost:8123/?query=SELECT' --data-binary @-
1
```

<div dir="rtl" markdown="1">

اگر بخشی از query در پارامتر ارسال شود، و بخش دیگر در POST، یک line feed بین دو بخش وارد می شود. مثال (این کار نمی کند):

</div>

```bash
$ echo 'ECT 1' | curl 'http://localhost:8123/?query=SEL' --data-binary @-
Code: 59, e.displayText() = DB::Exception: Syntax error: failed at position 0: SEL
ECT 1
, expected One of: SHOW TABLES, SHOW DATABASES, SELECT, INSERT, CREATE, ATTACH, RENAME, DROP, DETACH, USE, SET, OPTIMIZE., e.what() = DB::Exception
```

<div dir="rtl" markdown="1">

به صورت پیش فرض، داده ها با فرمت TabSeparated بر میگردند. (برای اطلاعات بیشتر بخش "فرمت" را مشاهده کنید). شما میتوانید از دستور FORMAT در query خود برای ست کردن فرمتی دیگر استفاده کنید.

</div>

```bash
$ echo 'SELECT 1 FORMAT Pretty' | curl 'http://localhost:8123/?' --data-binary @-
┏━━━┓
┃ 1 ┃
┡━━━┩
│ 1 │
└───┘
```

<div dir="rtl" markdown="1">

برای query های INSERT متد POST ضروری است. در این مورد، شما می توانید ابتدای query خود را در URL parameter بنویسید، و از POST برای پاس داده داده ها برای درج استفاده کنید. داده ی برای درج می تواند، برای مثال یک دامپ tab-separated شده از MySQL باشد. به این ترتیب، query INSERT جایگزین  LOAD DATA LOCAL INFILE از MySQL می شود.

مثال: ساخت جدول

</div>

```bash
echo 'CREATE TABLE t (a UInt8) ENGINE = Memory' | curl 'http://localhost:8123/' --data-binary @-
```

<div dir="rtl" markdown="1">

استفاده از query INSERT برای درج داده:

</div>

```bash
echo 'INSERT INTO t VALUES (1),(2),(3)' | curl 'http://localhost:8123/' --data-binary @-
```

<div dir="rtl" markdown="1">

داده ها میتوانند جدا از پارامتر query ارسال شوند:

</div>

```bash
echo '(4),(5),(6)' | curl 'http://localhost:8123/?query=INSERT%20INTO%20t%20VALUES' --data-binary @-
```

<div dir="rtl" markdown="1">

شما می توانید هر نوع فرمت دیتایی مشخص کنید. فرمت 'Values' دقیقا مشابه زمانی است که شما INSERT INTO t VALUES را می نویسید:

</div>

```bash
echo '(7),(8),(9)' | curl 'http://localhost:8123/?query=INSERT%20INTO%20t%20FORMAT%20Values' --data-binary @-
```

<div dir="rtl" markdown="1">

برای درج داده ها از یک دامپ tab-separate، فرمت مشخص زیر را وارد کنید:

</div>

```bash
echo -ne '10\n11\n12\n' | curl 'http://localhost:8123/?query=INSERT%20INTO%20t%20FORMAT%20TabSeparated' --data-binary @-
```

<div dir="rtl" markdown="1">

به دلیل پردازش موازی، نتایج query با ترتیب رندوم چاپ می شود:

</div>

```bash
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

<div dir="rtl" markdown="1">

حدف جدول:

</div>

```bash
echo 'DROP TABLE t' | curl 'http://localhost:8123/' --data-binary @-
```

<div dir="rtl" markdown="1">

برای درخواست هایی موفقی که داده ای از جدول بر نمیگردد، بدنه response خالی است.

شما می توانید از فرمت فشرده سازی داخلی ClickHouse در هنگان انتقال داده ها استفاده کنید. این فشرده سازی داده، یک فرمت غیراستاندارد است، و شما باید از برنامه مخصوص فشرده سازی ClickHouse برای استفاده از آن استفاده کنید. (این برنامه در هنگام نصب پکیج clickhouse-client نصب شده است)

اگر شما در URL پارامتر  'compress=1' را قرار دهید، سرور داده های ارسالی به شما را فشرده سازی می کند. اگر شما پارامتر 'decompress=1' را در URL ست کنید، سرور داده های ارسالی توسط متد POST را decompress می کند.

همچنین استفاده از فشرده سازی استاندارد gzip در HTTP ممکن است. برای ارسال درخواست POST و فشرده سازی آن به صورت gzip، هدر `Content-Encoding: gzip` را به request خود اضافه کنید. برای اینکه ClickHouse، response فشرده شده به صورت gzip برای شما ارسال کند، ابتدا باید `enable_http_compression` را در تنظیمات ClickHouse فعال کنید و در ادامه هدر `Accept-Encoding: gzip` را به درخواست خود اضافه کنید.

شما می توانید از این کار برای کاهش ترافیک شبکه هنگام ارسال مقدار زیادی از داده ها یا برای ایجاد dump هایی که بلافاصله فشرده می شوند، استفاده کنید.

شما می توانید از پارامتر 'database' در URL برای مشخص کردن دیتابیس پیش فرض استفاده کنید.

</div>

```bash
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

<div dir="rtl" markdown="1">

به صورت پیش فرض، دیتابیس ثبت شده در تنظیمات سرور به عنوان دیتابیس پیش فرض مورد استفاده قرار می گیرد، این دیتابیس 'default' نامیده می شود. از سوی دیگر، شما می توانید همیشه نام دیتابیس را با دات و قبل از اسم جدول مشخص کنید.

نام کاربری و پسورد می توانند به یکی از دو روش زیر ست شوند:

1. استفاده از HTTP Basic Authentication. مثال:

</div>

```bash
echo 'SELECT 1' | curl 'http://user:password@localhost:8123/' -d @-
```

<div dir="rtl" markdown="1">

2. با دو پارامتر 'user' و 'password' در URL. مثال:

</div>

```bash
echo 'SELECT 1' | curl 'http://localhost:8123/?user=user&password=password' -d @-
```

<div dir="rtl" markdown="1">

اگر نام کاربری مشخص نشود، نام کاربری 'default' استفاده می شود. اگر پسورد مشخص نشود، پسورد خالی استفاده می شود. شما همچنین می توانید از پارامتر های URL برای مشخص کردن هر تنظیمی برای اجرای یک query استفاده کنید. مثال: http://localhost:8123/?profile=web&max_rows_to_read=1000000000&query=SELECT+1

برای اطلاعات بیشتر بخش "تنظیمات" را مشاهده کنید.

</div>

```bash
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

<div dir="rtl" markdown="1">

برای اطلاعات بیشتر در مورد دیگر پارامترها، بخش "SET" را ببینید.

به طور مشابه، شما می توانید از ClickHouse Session در HTTP استفاده کنید. برای این کار، شما نیاز به ست کردن پارامتر`session_id` برای درخواست را دارید. شما میتوانید از هر رشته ای برای ست کردن Session ID استفاده کنید. به صورت پیش فرض، یک session بعد از 60 ثانیه از اجرای آخرین درخواست نابود می شود. برای تغییر زمان timeout، باید `default_session_timeout` را در تنظیمات سرور تغییر دهید و یا پارامتر `session_timeout` در هنگام درخواست ست کنید. برای بررسی وضعیت status از پارامتر `session_check=1` استفاده کنید. با یک session تنها یک query در لحظه می توان اجرا کرد.

گزینه ای برای دریافت اطلاعات progress اجرای query وجود دارد. این گزینه هدر X-ClickHouse-Progress می باشد. برای این کار تنظیم send_progress_in_http_headers در کانفیگ فایل فعال کنید.

درخواست در حال اجرا، در صورت لاست شدن کانکشن HTTP به صورت اتوماتیک متوقف نمی شود. پارس کردن و فرمت کردن داده ها در سمت سرور صورت می گیرد، و استفاده از شبکه ممکن است ناکارآمد باشد. پارامتر 'query_id' می تونه به عنوان query id پاس داده شود (هر رشته ای قابل قبول است). برای اطلاعات بیشتر بخش "تنظیمات، replace_running_query" را مشاهده کنید.

پارامتر 'quoto_key' می تواند به عنوان quoto key پاس داده شود (هر رشته ای قابل قبول است). برای اطلاعات بیشتر بخش "Quotas" را مشاهده کنید.

HTTP interface اجازه ی پاس دادن داده های external (جداول موقت external) به query را می دهد. برای اطلاعات بیشتر، بخش "داده های External برای پردازش query" را مشاهده کنید.

## بافرینگ Response

شما می توانید در سمت سرور قابلی بافرینگ response را فعال کنید. پارامترهای `buffer_size` و `wait_end_of_query` در URL برای این هدف ارائه شده اند.

`buffer_size` تعیین کننده ی اندازه ی نتیجه (بایت) برای بافر شدن در حافظه سرور است. اگر بدنه ی نتیجه بیش بزرگتر از این threshold باشد، بافر بر روی HTTP channel نوشته می شود و باقی مانده ی دیتا به صورت مستقیم به HTTP Channel ارسال می شود.

برای اینکه مطمعن بشید که کل response بافر شده است، `wait_end_of_query=1` را ست کنید. در این مورد، داده های که در حافظه ذخیره نمی شوند، در یک فایل موقت سمت سرور بافر می شوند.

مثال:

</div>

```bash
curl -sS 'http://localhost:8123/?max_result_bytes=4000000&buffer_size=3000000&wait_end_of_query=1' -d 'SELECT toUInt8(number) FROM system.numbers LIMIT 9000000 FORMAT RowBinary'
```

<div dir="rtl" markdown="1">

از بافرینگ به منظور اجتناب از شرایطی که یک خطای پردازش query رخ داده بعد از response کد و هدر های ارسال شده به کلاینت استفاده کنید. در این شرایط، پیغام خطا در انتهای بنده response نوشته می شود، و در سمت کلاینت، پیغام خطا فقط از طریق مرحله پارس کردن قابل شناسایی است.

</div>
