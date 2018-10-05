<a name="formats"></a>

<div dir="rtl" markdown="1">

# فرمت های Input و Output

فرمت تعیین می کند که چگونه داده ها پس از اجرای SELECT (چگونه نوشته شده و چگونه توسط سرور فرمت شده) به شما بر می گردد، و چگونه آن برای INSERT ها پذیرفته شده (چگونه آن توسط سرور پارس و خوانده می شود).

جدول زیر لیست فرمت های پشتیبانی شده برای هر نوع از query ها را نمایش می دهد.

Format | INSERT | SELECT
-------|--------|--------
[TabSeparated](formats.md#tabseparated) | ✔ | ✔ |
[TabSeparatedRaw](formats.md#tabseparatedraw)  | ✗ | ✔ |
[TabSeparatedWithNames](formats.md#tabseparatedwithnames) | ✔ | ✔ |
[TabSeparatedWithNamesAndTypes](formats.md#tabseparatedwithnamesandtypes) | ✔ | ✔ |
[CSV](formats.md#csv) | ✔ | ✔ |
[CSVWithNames](formats.md#csvwithnames) | ✔ | ✔ |
[Values](formats.md#values) | ✔ | ✔ |
[Vertical](formats.md#vertical) | ✗ | ✔ |
[VerticalRaw](formats.md#verticalraw) | ✗ | ✔ |
[JSON](formats.md#json) | ✗ | ✔ |
[JSONCompact](formats.md#jsoncompact) | ✗ | ✔ |
[JSONEachRow](formats.md#jsoneachrow) | ✔ | ✔ |
[TSKV](formats.md#tskv) | ✔ | ✔ |
[Pretty](formats.md#pretty) | ✗ | ✔ |
[PrettyCompact](formats.md#prettycompact) | ✗ | ✔ |
[PrettyCompactMonoBlock](formats.md#prettycompactmonoblock) | ✗ | ✔ |
[PrettyNoEscapes](formats.md#prettynoescapes) | ✗ | ✔ |
[PrettySpace](formats.md#prettyspace) | ✗ | ✔ |
[RowBinary](formats.md#rowbinary) | ✔ | ✔ |
[Native](formats.md#native) | ✔ | ✔ |
[Null](formats.md#null) | ✗ | ✔ |
[XML](formats.md#xml) | ✗ | ✔ |
[CapnProto](formats.md#capnproto) | ✔ | ✔ |

<a name="format_capnproto"></a>

## CapnProto

Cap'n Proto یک فرمت پیام باینری شبیه به Protocol Buffer و Thrift می باشد، اما شبیه به JSON یا MessagePack نیست.

پیغام های Cap'n Proto به صورت self-describing نیستند، به این معنی که آنها نیاز دارند که به صورت external، schema آنها شرح داده شود. schema به صورت on the fly اضافه می شود و برای هر query، cache می شود.

</div>

```sql
SELECT SearchPhrase, count() AS c FROM test.hits
       GROUP BY SearchPhrase FORMAT CapnProto SETTINGS schema = 'schema:Message'
```

<div dir="rtl" markdown="1">

جایی که `schema.capnp` شبیه این است:

</div>

```
struct Message {
  SearchPhrase @0 :Text;
  c @1 :Uint64;
}
```

<div dir="rtl" markdown="1">

فایل های Schema در فایلی قرار دارند که این فایل در دایرکتوری مشخص شده کانفیگ [ format_schema_path](../operations/server_settings/settings.md#server_settings-format_schema_path) قرار گرفته اند.

عملیات Deserialization موثر است و معمولا لود سیستم را افزایش نمی دهد.

## CSV

Comma Separated Values format ([RFC](https://tools.ietf.org/html/rfc4180)).

زمانی که از این روش برای فرمت استفاده می شود، سطر ها با دابل کوتیشن enclosed می شوند. دابل کوتیشن داخل یک رشته خروجی آن به صورت دو دابل کوتیشن در  یک سطر است. قانون دیگری برای escape کردن کاراکترها وجود ندارد. تاریخ و تاریخ-ساعت در دابل کوتیشن ها enclosed می شوند. اعداد بدون دابل کوتیشن در خروجی می آیند. مقادیر با جدا کننده * مشخص می شوند. سطر ها با استفاده از line feed (LF) جدا می شوند. آرایه ها در csv به این صورت serialize می شوند: ابتدا آرایه به یک رشته با فرمت TabSeparate سریالایز می شوند، و سپس رشته ی حاصل در دابل کوتیشن برای csv ارسال می شود. Tuple ها در فرمت CSV در ستون های جدا سریالایز می شوند (به این ترتیب، nest ها در tuble از دست میروند)

</div>

```
clickhouse-client --format_csv_delimiter="|" --query="INSERT INTO test.csv FORMAT CSV" < data.csv
```

<div dir="rtl" markdown="1">

&ast;به صورت پیش فرض — `,`. برای اطلاعات بیشتر [format_csv_delimiter](/operations/settings/settings/#format_csv_delimiter) را ببینید.

در هنگام پارس کردن، تمامی مقادیر می توانند با کوتیشن یا بدون کوتیشن پارس شوند. تک کوتیشن و دابل کوتیشن پشتیبانی می شود. سطر ها می توانند بدون کوتیشن تنظیم شوند. در این مورد سطر ها، جدا کننده ها با (CR یا LF) پارس می شوند. در موارد نقض RFC، در هنگام پارس کردن سطر ها بدون کوتیشن، فضاها و tab های پیشین نادید گرفته می شوند. برای line feed، یونیکس از (LF)، ویدنوز از (CR LF) و Mac OS کلاسیک (CR LF) پشتیبانی می کند.

فرمت CSV خروجی total و extreme را همانند `TabSeparated` پشتیبانی می کنند.

## CSVWithNames

همچنین header سطر را چاپ می کند، شبیه به `TabSeparatedWithNames`.

## JSON

خروجی داده ها با فرمت JSON. در کنال داده های جداول، خروجی JSON اسم ستون ها و type آنها به همراه اطلاعات بیشتر تولید می کند: تعداد سطر های خروجی، و همچنین تعداد رکورد های کل بدون در نظر گرفتن دستور LIMIT. مثال:

</div>

```sql
SELECT SearchPhrase, count() AS c FROM test.hits GROUP BY SearchPhrase WITH TOTALS ORDER BY c DESC LIMIT 5 FORMAT JSON
```

```json
{
        "meta":
        [
                {
                        "name": "SearchPhrase",
                        "type": "String"
                },
                {
                        "name": "c",
                        "type": "UInt64"
                }
        ],

        "data":
        [
                {
                        "SearchPhrase": "",
                        "c": "8267016"
                },
                {
                        "SearchPhrase": "bathroom interior design",
                        "c": "2166"
                },
                {
                        "SearchPhrase": "yandex",
                        "c": "1655"
                },
                {
                        "SearchPhrase": "spring 2014 fashion",
                        "c": "1549"
                },
                {
                        "SearchPhrase": "freeform photos",
                        "c": "1480"
                }
        ],

        "totals":
        {
                "SearchPhrase": "",
                "c": "8873898"
        },

        "extremes":
        {
                "min":
                {
                        "SearchPhrase": "",
                        "c": "1480"
                },
                "max":
                {
                        "SearchPhrase": "",
                        "c": "8267016"
                }
        },

        "rows": 5,

        "rows_before_limit_at_least": 141137
}
```

<div dir="rtl" markdown="1">

JSON با جاوااسکریپت سازگار است. برای اطمینان از این، بعضی از کاراکتر ها ecape های اضافه دارند: اسلش `/` به صورت `\/` escape می شود؛ line break جایگزین یعنی `U+2028` و `U+2029` که باعث break در بعضی از مروگرها می شود، به شکل `\uXXXX` escape می شوند. کاراکتر های کنترلی ASCII هم escape می شوند: backspace، form feed، line feed، carriage return، و horizontal tab به ترتیب با `\b`، `\f`، `\n`، `\r`، `\t` جایگزین می شوند. همچنین بایت های باقی مانده در محدوده 00 تا 1F با استفاده از `\uXXXX` جایگزین می شوند. کاراکتر های بی اعتبار UTF-8 با � جایگزین می شوند، پس خروجی JSON شامل موارد معتبر UTF-8 می باشد. برای سازگاری با جاوااسکریپت، اعداد Int64 و Uint64 به صورت پیش فرض، با استفاده از دابل کوتیشن enclose می شوند. برای حذف کوتیشن، شما باید پارامتر output_format_json_quote_64bit_integers v رو برابر با 0 قرار دهید.

`rows` – تعداد سطر های خروجی

`rows_before_limit_at_least` حداقل تعداد سطر ها در هنگام عدم استفاده از LIMIT. فقط در هنگامی که query دارای LIMIT است خروجی دارد. اگر query شامل GROUP BY باشد، مقدار rows_before_limit_at_least دقیقا با زمانی که از LIMIT استفاده نمی شود یکی است.

`totals` – مقدار TOTAL (زمانی که از WITH TOTALS استفاده می شود).

`extremes` – مقدار Extreme (در هنگامی که extreme برابر با 1 است).

این فرمت فقط مناسب خروجی query های می باشد، به این معنی که برای عملیات پارس کردن (دریافت داده برای insert در جدول) نیست. همچنین فرمت JSONEachRow را ببینید.

## JSONCompact

فقط در جاهایی که داده ها به جای object در array هستند خروجی آنها متفاوت است.

مثال:

</div>

```json
{
        "meta":
        [
                {
                        "name": "SearchPhrase",
                        "type": "String"
                },
                {
                        "name": "c",
                        "type": "UInt64"
                }
        ],

        "data":
        [
                ["", "8267016"],
                ["bathroom interior design", "2166"],
                ["yandex", "1655"],
                ["spring 2014 fashion", "1549"],
                ["freeform photos", "1480"]
        ],

        "totals": ["","8873898"],

        "extremes":
        {
                "min": ["","1480"],
                "max": ["","8267016"]
        },

        "rows": 5,

        "rows_before_limit_at_least": 141137
}
```

<div dir="rtl" markdown="1">

این فرمت فقط مناسب خروجی query های می باشد، به این معنی که برای عملیات پارس کردن (دریافت داده برای insert در جدول) نیست. همچنین فرمت JSONEachRow را ببینید.

## JSONEachRow

هر سطر برای خود JSON Object جدا دارد. (با استفاده از newline، JSON تعریف می شوند.)

</div>

```json
{"SearchPhrase":"","count()":"8267016"}
{"SearchPhrase":"bathroom interior design","count()":"2166"}
{"SearchPhrase":"yandex","count()":"1655"}
{"SearchPhrase":"spring 2014 fashion","count()":"1549"}
{"SearchPhrase":"freeform photo","count()":"1480"}
{"SearchPhrase":"angelina jolie","count()":"1245"}
{"SearchPhrase":"omsk","count()":"1112"}
{"SearchPhrase":"photos of dog breeds","count()":"1091"}
{"SearchPhrase":"curtain design","count()":"1064"}
{"SearchPhrase":"baku","count()":"1000"}
```

<div dir="rtl" markdown="1">

بر خلاف فرمت JSON، هیچ جایگزینی برای کاراکتر های بی اعتبار UTF-8 وجود ندارد. هر مجموعه ای از بایت های می تواند داخل سطر در خروجی باشند. پس داده ها بدون از دست دادن هیچ اطلاعاتی فرمت می شوند. مقادیر شبیه به JSON، escape می شوند.

برای پارس کردن، هر ترتیبی برای مقادیر ستون های مختلف پشتیبانی می شود. حذف شدن بعضی مقادیر قابل قبول است، آنها با مقادیر پیش فرض خود برابر هستند. در این مورد، صفر و سطر های خالی به عنوان مقادیر پیش فرض قرار می گیرند. مقادیر پیچیده که می توانند در جدول مشخص شوند، به عنوان مقادیر پیش فرض پشتیبانی نمی شوند. Whitespace بین element ها نادیده گرفته می شوند. اگر کاما بعد از object ها قرار گیرند، نادیده گرفته می شوند. object ها نیازی به جداسازی با استفاده از new line را ندارند.

## Native

کارآمدترین فرمت. داده ها توسط بلاک ها و در فرمت باینری نوشته و خوانده می شوند. برای هر بلاک، تعداد سطرها، تعداد ستون ها، نام ستون ها و type آنها، و بخش هایی از ستون ها در این بلاک یکی پس از دیگری ثبت می شوند. به عبارت دیگر، این فرمت "columnar" است - این فرمت ستون ها را به سطر تبدیل نمی کند. این فرمت در حالت native interface و بین سرور و محیط ترمینال و همچنین کلاینت C++ استفاده می شود.

شما می توانید از این فرمت برای تهیه دامپ سریع که فقط توسط مدیریت دیتابیس ClickHouse قابل خواندن است استفاده کنید. برای استفاده از این فرمت برای خودتان منطقی نیست.

## Null

هیچی در خروجی نمایش داده نمی شود. با این حال، query پردازش می شود، و زمانی که از کلایت command-line استفاده می کنید، داده ها برای کلاینت ارسال می شوند. از این برای تست، شامل تست بهره وری استفاده می شود. به طور مشخص، این فرمت فقط برای خروجی مناسب است نه برای پارس کردن.

## Pretty

خروجی داده ها به صورت جداول Unicode-art، همچنین استفاده از ANSI-escape برای تنظیم رنگ های ترمینال. یک جدول کامل کشیده می شود، و هر سطر دو خط از ترمینال را اشغال می کند. هر بلاکِ نتیجه، به عنوان یک جدول جدا چاپ می شود.پس بلاک ها می توانند بدون بافر کردن نتایج چاپ شوند (بافرینگ برای pre-calculate تمام مقادیر قابل مشاهده  ضروری است). برای جلوگیری از دامپ زیاد داده ها در ترمینال، 10 هزار سطر اول چاپ می شوند. اگر تعداد سطر های بزرگتر مساوی 10 هزار باشد، پیغام " 10 هزار اول نمایش داده شد" چاپ می شود. این فرمت فقط مناسب خروجی نتایج query ها می باشد، نه برای پارس کردن (دریافت داده ها و درج آن در جدول).

فرمت Pretty از total values (هنگام استفاده از WITH TOTALS) و extreme (هنگام که 'extremes' برابر با 1 است) برای خروجی پشتیبانی می کند. در این موارد، total values و extreme values بعد از نمایش داده های اصلی در جداول جدا، چاپ می شوند. مثال (برای فرمت PrettyCompact نمایش داده شده است):

</div>

```sql
SELECT EventDate, count() AS c FROM test.hits GROUP BY EventDate WITH TOTALS ORDER BY EventDate FORMAT PrettyCompact
```

```text
┌──EventDate─┬───────c─┐
│ 2014-03-17 │ 1406958 │
│ 2014-03-18 │ 1383658 │
│ 2014-03-19 │ 1405797 │
│ 2014-03-20 │ 1353623 │
│ 2014-03-21 │ 1245779 │
│ 2014-03-22 │ 1031592 │
│ 2014-03-23 │ 1046491 │
└────────────┴─────────┘

Totals:
┌──EventDate─┬───────c─┐
│ 0000-00-00 │ 8873898 │
└────────────┴─────────┘

Extremes:
┌──EventDate─┬───────c─┐
│ 2014-03-17 │ 1031592 │
│ 2014-03-23 │ 1406958 │
└────────────┴─────────┘
```

<div dir="rtl" markdown="1">

## PrettyCompact

تفاوت آن با `Pretty` در این است که grid های کشیده شده بین سطر ها و خروجی فشرده تر است. این فرمت به صورت پیش فرض در محیط کلاینت در حالت interactive مورد استفاده قرار می گیرد.

## PrettyCompactMonoBlock

تفاوت آن با `PrettyCompact` در این است که 10 هزار سطر خروجی بافر می شوند، و سپس در یک جدول چاپ می شوند. نه به صورت بلاک

## PrettyNoEscapes

تفاوت آن با Pretty در این است که از ANSI-escape استفاده نمی کند. این برای نمایش این فرمت در مروگر ضروری است، و همچنین برای استفاده از دستور 'watch' ضروری است.

مثال:

</div>

```bash
watch -n1 "clickhouse-client --query='SELECT event, value FROM system.events FORMAT PrettyCompactNoEscapes'"
```

<div dir="rtl" markdown="1">

شما می توانید برای نمایش در مرورگر از interface HTTP استفاده کنید.



### PrettyCompactNoEscapes

همانند تنظیم قبلی می باشد.

### PrettySpaceNoEscapes

همانند تنظیم قبلی می باشد..

## PrettySpace

تفاوت آن با `PrettyCompact` در این است که از whitespace (کاراکتر های space) به جای grid استفاده می کند.

## RowBinary

فرمت ها و پارس کردن داده ها، براساس سطر در فرمت باینری است.سطرها و مقادیر به صورت پیوسته و بدون جدا کننده لیست می شوند.این فرمت کم کارآمد تر از فرمت native است، از آنجایی که ردیف گرا است.

اعداد Integers از fixed-length استفاده می کنند. برای مثال Uint64 از 8 بایت استفاده می کند. DateTime از UInt32 که شامل مقدار Unix Timestamp است استفاده می کند. Date از UInt16 که شامل تعداد روز از تاریخ 1-1-1970 است استفاده می کند. String به عنوان variant length نشان داده می شود (unsigned [LEB128](https://en.wikipedia.org/wiki/LEB128))، که دنباله ای از بایت های یک رشته هستند. FixedString به سادگی به عنوان توالی از بایت ها نمایش داده می شود.

آرایه به عنوان variant length نشان داده می شود (unsigned [LEB128](https://en.wikipedia.org/wiki/LEB128))، دنباله ای از عانصر پیوسته آرایه

## TabSeparated

در فرمت TabSeparated، داده ها به صورت سطر نوشته می شوند. هر سطر شامل مقادیر جدا شده با tab می باشد. هر مقدار با یک tab دنبال می شود، به جز آخرین مقدار یک سطر، که با line feed دنبال می شود. line feed unix در همه جا مورد تسافده قرار می گیرد. آخرین سطر از خروجی هم باید شامل line feed در انتها باشد. مقادیر در فرمت متنی بدون enclose با کوتیشون، و یا escape با کاراکترهای ویژه، نوشته می شوند.

اعداد Integer با فرم decimal نوشته می شوند. اعداد می توانند شامل کاراکتر اضافه "+" در ابتدای خود باشند. (در هنگام پارس کردن نادیده گرفته می شوند، و در هنگام فرمت کردن، ثبت نمی شوند). اعداد غیر منفی نمیتوانند شامل علامت منفی باشند. در هنگام خواندن، اجازه داده می شود که رشته خالی را به عنوان صفر، پارس کرد، یا (برای تایپ های sign) یک رشته که شامل فقط یک علامت منفی است به عنوان صفر پارس کرد. اعدادی که در data type مربوطه فیت نشوند ممکن است به عددی متفاوت تبدیل شوند و پیغام خطایی هم نمایش ندهند.

اعداد Floating-point به فرم decimal نوشته می شوند. از دات به عنوان جدا کننده decimal استفاده می شود. نوشته های نمایشی مثل 'inf'، '+inf'، '-inf' و 'nan' پشتیبانی می شوند. ورودی اعداد floating-point می تواند با یه نقطه اعشار شروع یا پایان یابد. در هنگام فرمت، دقت اعداد floating-point ممکن است گم شوند. در هنگام پارس کردن، دقیقا نیازی به خواندن نزدیکترین عدد machine-representable نیست.

Dates با فرمت YYY-MM-DD نوشته می شوند و به همین حالت پارس می شوند، اما با هر کاراکتری به عنوان جدا کننده. Dates به همراه زمان با فرمت YYYY-MM-DD hh:mm:ss نوشته می شوند و با همین فرمت پارس می شوند، اما با هر کاراکتری به عنوان جداکننده.  این در منطقه زمان سیستم در زمانی که کلاینت یا سرور شروع می شود (بسته به اینکه کدام یک از داده ها را تشکیل می دهد) رخ می دهد. برای تاریخ همراه با زمان DST مشخص نمی شود. پس اگر یک دامپ دارای زمان DST باشد، دامپ، داده ها را به طور غیرمستقیم مطابقت نمی دهد و پارسینگ، یکی از دو ساعت را انتخاب خواهد کرد. در طول عملیات خواندن، تاریخ ها و تاریخ و ساعت های نادرست می توانند به صورت null و یا natural overflow پارس شوند، بدون اینکه پیغام خطایی نمایش دهند.

به عنوان یک استثنا، پارس کردن تاریخ به همراه ساعت، اگر مقدار دقیقا شامل 10 عدد decimal باشد، به عنوان فرمت unix timestamp پشتیبانی خواهد کرد. خروجی وابسته به time-zone نمی باشد.  فرمت های YYYY-MM-DD hh: mm: ss و NNNNNNNNNN به صورت خودکار تمایز می یابند.

رشته های دارای کاراکتر های ویژه backslash-escaped چاپ می شوند. escape های در ادامه برای خروجی استفاده می شوند: `\b`، `\f`، `\r`، `\n`، `\t`، `\0`, `\'`، `\\`. پارسر همچنین از `\a`، `\v`، و `\xHH` (hex escape) و هر `\c`  پشتیبانی می کند. بدین ترتیب خواندن داده ها از فرمت line feed که می تواند به صورت `\n` یا `\`  نوشته شود پشتیبانی می کند. برای مثال، رشته ی `Hello world` به همراه line feed بین کلمات به جای space می تواند به هر یک از حالات زیر پارس شود::

</div>

```text
Hello\nworld

Hello\
world
```

<div dir="rtl" markdown="1">

نوع دوم به دلیل پشتیبانی MySQL در هنگام نوشتن دامپ به صورت tab-separate، پشتیبانی می شود.

حداقل مجموعه از کاراکترهایی که در هنگام پاس دادن داده در فرمت TabSeperate نیاز به escape آن دارید: tab، line feed (LF) بک اسلش.

فقط مجموعه ی کمی از نماد ها escape می شوند. شما به راحتی می توانید بر روی مقدار رشته که در ترمینال شما در خروجی نمایش داده می شود حرکت کنید.

آرایه ها به صورت لیستی از مقادیر که به comma از هم جدا شده اند و در داخل براکت قرار گرفته اند نوشته می شوند. آیتم های عددی در آرای به صورت نرمال فرمت می شوند، اما تاریخ و تاریخ با ساعت و رشته ها در داخل تک کوتیشن به همراه قوانین escape که بالا اشاره شد، نوشته می شوند.

فرمت TabSeparate برای پردازش داده ها با استفاده از برنامه های شخصی سازی شده و اسکریپت ها مناسب است. TabSeparate به صورت پیش فرض در HTTP interface و در حالت batch کلاینت command-line مورد استفاده قرار می گیرد. همچنین این فرمت اجازه ی انتقال داده ها بین DBMS های مختلف را می دهد. برای مثال، شما می توانید از MySQL با این روش دامپ بگیرید و آن را در ClickHouse یا vice versa آپلود کنید.

فرمت TabSeparated از خروحی total values (هنگام استفاده از WITH TOTALS) و extreme values (در هنگامی که 'extreme' برابر با 1 است) پشتیبانی می کند. در این موارد، total value و extreme بعد از داده های اصلی در خروجی می آیند. نتایج اصلی، total values و extreme همگی با یک empty line از هم جدا می شوند. مثال:

</div>

```sql
SELECT EventDate, count() AS c FROM test.hits GROUP BY EventDate WITH TOTALS ORDER BY EventDate FORMAT TabSeparated``
```

```text
2014-03-17      1406958
2014-03-18      1383658
2014-03-19      1405797
2014-03-20      1353623
2014-03-21      1245779
2014-03-22      1031592
2014-03-23      1046491

0000-00-00      8873898

2014-03-17      1031592
2014-03-23      1406958
```

<div dir="rtl" markdown="1">

این فرمت نیز تحت نام `TSV` موجود است.



## TabSeparatedRaw

تفاوت آن با `TabSeperated` در این است که در این فرمت سطرها بدون escape نوشته می شوند. این فرمت فقط مناسب خروجی نتایج query ها می باشد، نه برای پارس کردن (دریافت داده ها و درج آن در جدول).

همچنین این فرمت تحت عنوان ` TSVRaw`وجود دارد.

## TabSeparatedWithNames

تفاوت آن با فرمت `TabSeparated` در این است که، در این فرمت نام ستون ها در سطر اول قرار می گیرد. در طول پارس کردن، سطر اول به طور کامل نادیده گرفته می شود. شما نمی توانید نام ستون ها را برای تعیین موقعیت آنها یا بررسی صحت آنها استفاده کنید. (پشتیبانی از پارس کردن سطر header ممکن است در آینده اضافه شود.)

همچنین این فرمت تحت عنوان ` TSVWithNames`وجود دارد.

## TabSeparatedWithNamesAndTypes

تفاوت آن با `TabSeparated` در این است که در این فرمت نام ستون ها در سطر اول نوشته می شود، و type ستون ها در سطر دوم نوشته می شود. در طی پارسینگ، سطر اول و دوم به طور کامل نادیده گرفته می شوند.

همچنین این فرمت تحت عنوان ` TSVWithNamesAndTypes`وجود دارد.

## TSKV

مشابه فرمت TabSeparated، اما خروجی به صورت name=value می باشد. نام ها مشابه روش TabSeparated، escape می شوند، و همچنین = symbol هم escape می شود.

</div>

```text
SearchPhrase=   count()=8267016
SearchPhrase=bathroom interior design    count()=2166
SearchPhrase=yandex     count()=1655
SearchPhrase=spring 2014 fashion    count()=1549
SearchPhrase=freeform photos       count()=1480
SearchPhrase=angelina jolia    count()=1245
SearchPhrase=omsk       count()=1112
SearchPhrase=photos of dog breeds    count()=1091
SearchPhrase=curtain design        count()=1064
SearchPhrase=baku       count()=1000
```

<div dir="rtl" markdown="1">

وقتی تعداد زیادی از ستون ها وجود دارد، این فرمت بی فایده است، و در حالت کلی دلیلی بر استفاده از این فرمت در این مواقع وجود ندارد. این فرمت در بعضی از دپارتمان های Yandex استفاده می شد.

خروجی داده ها و پارس کردن هر دو در این فرمت پشتیبانی می شوند. برای پارس کردن، هر ترتیبی برای مقادیر ستون های مختلف پشتیبانی می شود. حذف بعضی از مقادیر قابل قبول است. این مقادیر با مقادیر پیش فرض خود برابر هستند. در این مورد، صفر و سطر خالی، توسط مقادیر پیش فرض پر می شوند. مقادیر پیچیده ای که می تواند در جدول مشخص شود به عنوان پیش فرض در این فرمت پشتیبانی نمیشوند.

پارس کردن، اجازه می دهد که فیلد اضافه ی `tskv` بدون علامت و مقدار وجود داشته باشد. این فیلد نادیده گرفته می شود.

## Values

هر سطر داخل براکت چاپ می شود. سطر ها توسط comma جدا می شوند. برای آخرین سطر comma وجود ندارد. مقادیر داخل براکت همچنین توسط comma جدا می شوند. اعداد با فرمت decimal و بدون کوتیشن چاپ می شوند. آرایه ها در براکت ها چاپ می شوند. رشته ها، تاریخ و تاریخ با ساعت داخل کوتیشن قرار می گیرند. قوانین escape و پارس کردن شبیه به فرمت TabSeparated انجام می شود. در طول فرمت، extra spaces درج نمی شوند، اما در هنگام پارس کردن، آنها مجاز و skip می شوند. (به جز space های داخل مقادیر آرایه، که مجاز نیستند).

حداقل کاراکترهای که شما در هنگام پاس دادن داده ها برای escape نیاز دارید: تک کوتیشن و بک اسلش.

این فرمت برای دستور `INSERT INTO t VALUES ...` مورد استفاده قرار می گیرد، اما همچنین شما می تونید برای فرمت نتایج query استفاده کنید.

## Vertical

مقدار هر ستون به همراه نام ستون در سطر جداگانه چاپ می شود. اگر هر سطر شامل تعداد زیادی ستون است، این فرمت جهت چاپ چند سطر مناسب است. این فرمت فقط مناسب خروجی نتایج query ها می باشد، نه برای پارس کردن (دریافت داده ها و درج آن در جدول).

## VerticalRaw

تفاوت آن با `Vertical` در این است که سطر ها escape نمی شوند. این فرمت فقط مناسب خروجی نتایج query ها می باشد، نه برای پارس کردن (دریافت داده ها و درج آن در جدول).

مثال:

</div>

```
:) SHOW CREATE TABLE geonames FORMAT VerticalRaw;
Row 1:
──────
statement: CREATE TABLE default.geonames ( geonameid UInt32, date Date DEFAULT CAST('2017-12-08' AS Date)) ENGINE = MergeTree(date, geonameid, 8192)

:) SELECT 'string with \'quotes\' and \t with some special \n characters' AS test FORMAT VerticalRaw;
Row 1:
──────
test: string with 'quotes' and   with some special
 characters
```

<div dir="rtl" markdown="1">

در مقایسه با فرمت Vertical:

</div>

```
:) SELECT 'string with \'quotes\' and \t with some special \n characters' AS test FORMAT Vertical;
Row 1:
──────
test: string with \'quotes\' and \t with some special \n characters
```
## XML

<div dir="rtl" markdown="1">

فرمت XML فقط برای خروجی مناسب است، نه برای پارس کردن. مثال:

</div>

```xml
<?xml version='1.0' encoding='UTF-8' ?>
<result>
        <meta>
                <columns>
                        <column>
                                <name>SearchPhrase</name>
                                <type>String</type>
                        </column>
                        <column>
                                <name>count()</name>
                                <type>UInt64</type>
                        </column>
                </columns>
        </meta>
        <data>
                <row>
                        <SearchPhrase></SearchPhrase>
                        <field>8267016</field>
                </row>
                <row>
                        <SearchPhrase>bathroom interior design</SearchPhrase>
                        <field>2166</field>
                </row>
                <row>
                        <SearchPhrase>yandex</SearchPhrase>
                        <field>1655</field>
                </row>
                <row>
                        <SearchPhrase>spring 2014 fashion</SearchPhrase>
                        <field>1549</field>
                </row>
                <row>
                        <SearchPhrase>freeform photos</SearchPhrase>
                        <field>1480</field>
                </row>
                <row>
                        <SearchPhrase>angelina jolie</SearchPhrase>
                        <field>1245</field>
                </row>
                <row>
                        <SearchPhrase>omsk</SearchPhrase>
                        <field>1112</field>
                </row>
                <row>
                        <SearchPhrase>photos of dog breeds</SearchPhrase>
                        <field>1091</field>
                </row>
                <row>
                        <SearchPhrase>curtain design</SearchPhrase>
                        <field>1064</field>
                </row>
                <row>
                        <SearchPhrase>baku</SearchPhrase>
                        <field>1000</field>
                </row>
        </data>
        <rows>10</rows>
        <rows_before_limit_at_least>141137</rows_before_limit_at_least>
</result>
```

<div dir="rtl" markdown="1">

اگر نام فیلد، فرمت قابل قبولی نداشته باشد، اسم 'field' به عنوان نام عنصر استفاده می شود. به طور کلی، ساختار XML مشابه ساختار JSON می باشد. فقط در JSON، موارد بی اعتبار UTF-8 تبدیل به کاراکتر � می شوند که منجر به خروجی معتبر UTF-8 می شود.

در مقادیر رشته ای، کاراکتر های `>` و `&` به صورت `<` و `&` escape می شوند.

آرایه ها به شکل `<array><elem>Hello</elem><elem>World</elem>...</array>` و tuple ها به صورت `<tuple><elem>Hello</elem><elem>World</elem>...</tuple>` در خروجی می آیند.

</div>
