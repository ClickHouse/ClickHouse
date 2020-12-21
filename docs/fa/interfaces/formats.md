---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 21
toc_title: "\u0641\u0631\u0645\u062A \u0647\u0627\u06CC \u0648\u0631\u0648\u062F\u06CC\
  \ \u0648 \u062E\u0631\u0648\u062C\u06CC"
---

# فرمت برای داده های ورودی و خروجی {#formats}

تاتر می توانید قبول و بازگشت داده ها در فرمت های مختلف. یک قالب پشتیبانی شده برای ورودی می تواند برای تجزیه داده های موجود در `INSERT`برای انجام `SELECT`بازدید کنندگان از یک جدول فایل حمایت مانند فایل, نشانی اینترنتی و یا اچ دی, و یا به خواندن یک فرهنگ لغت خارجی. فرمت پشتیبانی شده برای خروجی می تواند مورد استفاده قرار گیرد به ترتیب
نتایج یک `SELECT` و برای انجام `INSERT`ها را به یک جدول فایل حمایت.

فرمت های پشتیبانی شده عبارتند از:

| قالب                                                    | ورودی | خروجی |
|---------------------------------------------------------|-------|-------|
| [جدول دار](#tabseparated)                               | ✔     | ✔     |
| [اطلاعات دقیق](#tabseparatedraw)                        | ✗     | ✔     |
| [اطلاعات دقیق](#tabseparatedwithnames)                  | ✔     | ✔     |
| [اطلاعات دقیق](#tabseparatedwithnamesandtypes)          | ✔     | ✔     |
| [قالب](#format-template)                                | ✔     | ✔     |
| [پاسخ تمپلیتینی](#templateignorespaces)                 | ✔     | ✗     |
| [CSV](#csv)                                             | ✔     | ✔     |
| [اطلاعات دقیق](#csvwithnames)                           | ✔     | ✔     |
| [سفارشی](#format-customseparated)                       | ✔     | ✔     |
| [مقادیر](#data-format-values)                           | ✔     | ✔     |
| [عمودی](#vertical)                                      | ✗     | ✔     |
| [عمودی](#verticalraw)                                   | ✗     | ✔     |
| [JSON](#json)                                           | ✗     | ✔     |
| [فوق العاده](#jsoncompact)                              | ✗     | ✔     |
| [جیسانچرو](#jsoneachrow)                                | ✔     | ✔     |
| [TSKV](#tskv)                                           | ✔     | ✔     |
| [زیبا](#pretty)                                         | ✗     | ✔     |
| [پیش تیمار](#prettycompact)                             | ✗     | ✔     |
| [بلوک پیش ساخته](#prettycompactmonoblock)               | ✗     | ✔     |
| [کتاب های پیش بینی شده](#prettynoescapes)               | ✗     | ✔     |
| [چوب نما](#prettyspace)                                 | ✗     | ✔     |
| [Protobuf](#protobuf)                                   | ✔     | ✔     |
| [اورو](#data-format-avro)                               | ✔     | ✔     |
| [هشدار داده می شود](#data-format-avro-confluent)        | ✔     | ✗     |
| [پارکت](#data-format-parquet)                           | ✔     | ✔     |
| [ORC](#data-format-orc)                                 | ✔     | ✗     |
| [مربوط به حوزه](#rowbinary)                             | ✔     | ✔     |
| [ارزشهای خبری عبارتند از:](#rowbinarywithnamesandtypes) | ✔     | ✔     |
| [بومی](#native)                                         | ✔     | ✔     |
| [خالی](#null)                                           | ✗     | ✔     |
| [XML](#xml)                                             | ✗     | ✔     |
| [کاپپروتو](#capnproto)                                  | ✔     | ✗     |

شما می توانید برخی از پارامترهای پردازش فرمت با تنظیمات کلیک را کنترل کنید. برای اطلاعات بیشتر بخوانید [تنظیمات](../operations/settings/settings.md) بخش.

## جدول دار {#tabseparated}

در قالب جدولبندی داده ها توسط ردیف نوشته شده است. هر سطر شامل مقادیر جدا شده توسط زبانه. هر مقدار است که توسط یک تب به دنبال, به جز ارزش گذشته در ردیف, است که توسط یک خوراک خط به دنبال. به شدت یونیکس خط تغذیه در همه جا فرض شده است. ردیف گذشته نیز باید یک خوراک خط در پایان حاوی. ارزش ها در قالب متن نوشته شده, بدون گذاشتن علامت نقل قول, و با شخصیت های خاص فرار.

این فرمت نیز تحت نام موجود است `TSV`.

این `TabSeparated` فرمت مناسب برای پردازش داده ها با استفاده از برنامه های سفارشی و اسکریپت است. این است که به طور پیش فرض در رابط اچ تی پی و در حالت دسته ای مشتری خط فرمان استفاده می شود. این فرمت همچنین اجازه می دهد تا انتقال داده ها بین سرویس بهداشتی مختلف. مثلا, شما می توانید یک روگرفت از خروجی زیر و ارسال به کلیک, و یا بالعکس.

این `TabSeparated` فرمت پشتیبانی خروجی ارزش کل (هنگام استفاده با بالغ) و ارزش های شدید (وقتی که ‘extremes’ به مجموعه 1). در این موارد, کل ارزش ها و افراط خروجی پس از داده های اصلی. نتیجه اصلی, کل ارزش, و افراط و از یکدیگر توسط یک خط خالی از هم جدا. مثال:

``` sql
SELECT EventDate, count() AS c FROM test.hits GROUP BY EventDate WITH TOTALS ORDER BY EventDate FORMAT TabSeparated``
```

``` text
2014-03-17      1406958
2014-03-18      1383658
2014-03-19      1405797
2014-03-20      1353623
2014-03-21      1245779
2014-03-22      1031592
2014-03-23      1046491

1970-01-01      8873898

2014-03-17      1031592
2014-03-23      1406958
```

### قالببندی داده {#data-formatting}

اعداد صحیح به شکل اعشاری نوشته شده است. اعداد می توانند حاوی اضافی باشند “+” شخصیت در ابتدا (هنگام تجزیه نادیده گرفته شد و هنگام قالب بندی ثبت نشده است). اعداد غیر منفی نمی توانند علامت منفی داشته باشند. هنگام خواندن, مجاز است به تجزیه یک رشته خالی به عنوان یک صفر, یا (برای انواع امضا شده) یک رشته متشکل از فقط یک علامت منفی به عنوان یک صفر. اعداد است که به نوع داده مربوطه مناسب نیست ممکن است به عنوان شماره های مختلف تجزیه, بدون پیغام خطا.

اعداد ممیز شناور به شکل اعشاری نوشته شده است. نقطه به عنوان جداکننده اعشاری استفاده می شود. ورودی های نمایشی پشتیبانی می شوند ‘inf’, ‘+inf’, ‘-inf’ و ‘nan’. ورود اعداد ممیز شناور ممکن است شروع یا پایان با یک نقطه اعشار.
در قالب بندی, دقت ممکن است در اعداد ممیز شناور از دست داده.
در تجزیه, این است که به شدت مورد نیاز برای خواندن نزدیکترین شماره ماشین نمایندگی.

تاریخ در فرمت یی-میلی متر-دی دی دی دی اس نوشته شده و تجزیه در قالب همان, اما با هر شخصیت به عنوان جدا.
تاریخ با زمان در قالب نوشته شده است `YYYY-MM-DD hh:mm:ss` و تجزیه در قالب همان, اما با هر شخصیت به عنوان جدا.
این همه در منطقه زمانی سیستم در زمان مشتری یا سرور شروع می شود (بسته به نوع فرمت داده ها) رخ می دهد. برای تاریخ با زمان, نور روز صرفه جویی در زمان مشخص نشده است. بنابراین اگر یک روگرفت است بار در طول نور روز صرفه جویی در زمان, روگرفت به صراحت مطابقت با داده ها نیست, و تجزیه یکی از دو بار را انتخاب کنید.
در طول یک عملیات به عنوان خوانده شده, تاریخ نادرست و تاریخ با زمان را می توان با سرریز طبیعی و یا تاریخ به عنوان تهی و بار تجزیه, بدون پیغام خطا.

به عنوان یک استثنا, تجزیه تاریخ با زمان نیز در قالب برچسب زمان یونیکس پشتیبانی, اگر از دقیقا شامل 10 رقم اعشار. نتیجه این است زمان وابسته به منطقه نیست. فرمت های یی-ام-دی-دی-اچ: میلی متر: اس اس و نونن به طور خودکار متفاوت هستند.

رشته ها خروجی با شخصیت های خاص بک اسلش فرار. توالی فرار زیر برای خروجی استفاده می شود: `\b`, `\f`, `\r`, `\n`, `\t`, `\0`, `\'`, `\\`. تجزیه همچنین از توالی ها پشتیبانی می کند `\a`, `\v` و `\xHH` (توالی فرار سحر و جادو) و هر `\c` دنباله هایی که `c` هر شخصیت (این توالی ها به تبدیل `c`). بدین ترتیب, خواندن داده ها پشتیبانی از فرمت های که یک خوراک خط را می توان به عنوان نوشته شده `\n` یا `\`, و یا به عنوان یک خوراک خط. مثلا, رشته `Hello world` با خوراک خط بین کلمات به جای فضا را می توان در هر یک از تغییرات زیر تجزیه شده است:

``` text
Hello\nworld

Hello\
world
```

نوع دوم پشتیبانی می شود زیرا خروجی زیر هنگام نوشتن افسردگی های جدا شده از تب استفاده می کند.

حداقل مجموعه ای از شخصیت های که شما نیاز به فرار در هنگام عبور داده ها در قالب جدول پخش: باریکه, خوراک خط (ال اف) و بک اسلش.

فقط یک مجموعه کوچک از علامت فرار. شما به راحتی می توانید بر روی یک مقدار رشته تلو تلو خوردن که ترمینال خود را در خروجی خراب کردن.

ارریس به عنوان یک لیست از ارزش کاما از هم جدا در براکت مربع نوشته شده است. موارد شماره در مجموعه به طور معمول فرمت می شوند. `Date` و `DateTime` انواع در نقل قول تک نوشته شده است. رشته ها در نقل قول های تک با قوانین فرار همان بالا نوشته شده است.

[NULL](../sql-reference/syntax.md) به عنوان فرمت `\N`.

هر عنصر [تو در تو](../sql-reference/data-types/nested-data-structures/nested.md) سازه ها به عنوان مجموعه ای نشان داده شده است.

به عنوان مثال:

``` sql
CREATE TABLE nestedt
(
    `id` UInt8,
    `aux` Nested(
        a UInt8,
        b String
    )
)
ENGINE = TinyLog
```

``` sql
INSERT INTO nestedt Values ( 1, [1], ['a'])
```

``` sql
SELECT * FROM nestedt FORMAT TSV
```

``` text
1  [1]    ['a']
```

## اطلاعات دقیق {#tabseparatedraw}

متفاوت از `TabSeparated` فرمت که در ردیف بدون فرار نوشته شده است.
این فرمت فقط برای خروجی یک نتیجه پرس و جو مناسب است, اما نه برای تجزیه (بازیابی اطلاعات برای وارد کردن در یک جدول).

این فرمت نیز تحت نام موجود است `TSVRaw`.

## اطلاعات دقیق {#tabseparatedwithnames}

متفاوت از `TabSeparated` فرمت در که نام ستون در سطر اول نوشته شده است.
در تجزیه, ردیف اول به طور کامل نادیده گرفته. شما می توانید نام ستون برای تعیین موقعیت خود و یا برای بررسی صحت خود استفاده کنید.
(پشتیبانی از تجزیه ردیف هدر ممکن است در اینده اضافه شده است.)

این فرمت نیز تحت نام موجود است `TSVWithNames`.

## اطلاعات دقیق {#tabseparatedwithnamesandtypes}

متفاوت از `TabSeparated` فرمت در که نام ستون به سطر اول نوشته شده است, در حالی که انواع ستون در ردیف دوم هستند.
در تجزیه, ردیف اول و دوم به طور کامل نادیده گرفته.

این فرمت نیز تحت نام موجود است `TSVWithNamesAndTypes`.

## قالب {#format-template}

این فرمت اجازه می دهد تا تعیین یک رشته فرمت سفارشی با متغیرهایی برای ارزش با یک قاعده فرار مشخص شده است.

با استفاده از تنظیمات `format_template_resultset`, `format_template_row`, `format_template_rows_between_delimiter` and some settings of other formats (e.g. `output_format_json_quote_64bit_integers` هنگام استفاده از `JSON` فرار, مشاهده بیشتر)

تنظیم `format_template_row` مشخص مسیر به فایل, که شامل رشته فرمت برای ردیف با نحو زیر:

`delimiter_1${column_1:serializeAs_1}delimiter_2${column_2:serializeAs_2} ... delimiter_N`,

کجا `delimiter_i` یک جداساز بین مقادیر است (`$` نماد را می توان به عنوان فرار `$$`),
`column_i` یک نام یا فهرست یک ستون است که مقادیر انتخاب شده یا درج شده است (اگر خالی باشد سپس ستون حذف خواهد شد),
`serializeAs_i` یک قانون فرار برای مقادیر ستون است. قوانین فرار زیر پشتیبانی می شوند:

-   `CSV`, `JSON`, `XML` (به طور مشابه به فرمت های نام های مشابه)
-   `Escaped` (به طور مشابه به `TSV`)
-   `Quoted` (به طور مشابه به `Values`)
-   `Raw` (بدون فرار, به طور مشابه به `TSVRaw`)
-   `None` (هیچ قانون فرار, مشاهده بیشتر)

اگر یک قانون فرار حذف شده است, سپس `None` استفاده خواهد شد. `XML` و `Raw` فقط برای خروجی مناسب است.

بنابراین, برای رشته فرمت زیر:

      `Search phrase: ${SearchPhrase:Quoted}, count: ${c:Escaped}, ad price: $$${price:JSON};`

ارزش `SearchPhrase`, `c` و `price` ستون ها که به عنوان فرار `Quoted`, `Escaped` و `JSON` چاپ خواهد شد (برای انتخاب) و یا انتظار می رود (برای درج) میان `Search phrase:`, `, count:`, `, ad price: $` و `;` delimiters بود. به عنوان مثال:

`Search phrase: 'bathroom interior design', count: 2166, ad price: $3;`

این `format_template_rows_between_delimiter` تنظیم مشخص جداساز بین ردیف, که چاپ شده است (یا انتظار می رود) بعد از هر سطر به جز یکی از گذشته (`\n` به طور پیش فرض)

تنظیم `format_template_resultset` مشخص کردن مسیر فایل که شامل یک رشته فرمت برای resultset. رشته فرمت برای حاصل است نحو همان رشته فرمت برای ردیف و اجازه می دهد تا برای مشخص کردن یک پیشوند, پسوند و یک راه برای چاپ برخی از اطلاعات اضافی. این شامل متغیرهایی زیر به جای نام ستون:

-   `data` ردیف با داده ها در `format_template_row` قالب, جدا شده توسط `format_template_rows_between_delimiter`. این حفره یا سوراخ باید اولین حفره یا سوراخ در رشته فرمت باشد.
-   `totals` ردیف با کل ارزش ها در `format_template_row` قالب (هنگام استفاده با مجموع)
-   `min` ردیف با حداقل مقدار در `format_template_row` فرمت (هنگامی که افراط و به مجموعه 1)
-   `max` ردیف با حداکثر ارزش در است `format_template_row` فرمت (هنگامی که افراط و به مجموعه 1)
-   `rows` تعداد کل ردیف خروجی است
-   `rows_before_limit` است حداقل تعداد ردیف وجود دارد که بدون محدودیت بوده است. خروجی تنها در صورتی که پرس و جو شامل حد. اگر پرس و جو شامل گروه های, ردیف ها_افور_لیمیت_تلاست تعداد دقیق ردیف وجود دارد که بدون محدودیت بوده است.
-   `time` زمان اجرای درخواست در ثانیه است
-   `rows_read` است تعداد ردیف خوانده شده است
-   `bytes_read` تعداد بایت (غیر فشرده) خوانده شده است

متغیرهایی `data`, `totals`, `min` و `max` باید فرار حکومت مشخص نیست (یا `None` باید به صراحت مشخص شود). متغیرهایی باقی مانده ممکن است هر گونه قانون فرار مشخص شده اند.
اگر `format_template_resultset` تنظیم یک رشته خالی است, `${data}` به عنوان مقدار پیش فرض استفاده می شود.
برای قرار دادن نمایش داده شد فرمت اجازه می دهد تا پرش برخی از ستون ها و یا برخی از زمینه های اگر پیشوند یا پسوند (به عنوان مثال مراجعه کنید).

انتخاب نمونه:

``` sql
SELECT SearchPhrase, count() AS c FROM test.hits GROUP BY SearchPhrase ORDER BY c DESC LIMIT 5 FORMAT Template SETTINGS
format_template_resultset = '/some/path/resultset.format', format_template_row = '/some/path/row.format', format_template_rows_between_delimiter = '\n    '
```

`/some/path/resultset.format`:

``` text
<!DOCTYPE HTML>
<html> <head> <title>Search phrases</title> </head>
 <body>
  <table border="1"> <caption>Search phrases</caption>
    <tr> <th>Search phrase</th> <th>Count</th> </tr>
    ${data}
  </table>
  <table border="1"> <caption>Max</caption>
    ${max}
  </table>
  <b>Processed ${rows_read:XML} rows in ${time:XML} sec</b>
 </body>
</html>
```

`/some/path/row.format`:

``` text
<tr> <td>${0:XML}</td> <td>${1:XML}</td> </tr>
```

نتیجه:

``` html
<!DOCTYPE HTML>
<html> <head> <title>Search phrases</title> </head>
 <body>
  <table border="1"> <caption>Search phrases</caption>
    <tr> <th>Search phrase</th> <th>Count</th> </tr>
    <tr> <td></td> <td>8267016</td> </tr>
    <tr> <td>bathroom interior design</td> <td>2166</td> </tr>
    <tr> <td>yandex</td> <td>1655</td> </tr>
    <tr> <td>spring 2014 fashion</td> <td>1549</td> </tr>
    <tr> <td>freeform photos</td> <td>1480</td> </tr>
  </table>
  <table border="1"> <caption>Max</caption>
    <tr> <td></td> <td>8873898</td> </tr>
  </table>
  <b>Processed 3095973 rows in 0.1569913 sec</b>
 </body>
</html>
```

درج مثال:

``` text
Some header
Page views: 5, User id: 4324182021466249494, Useless field: hello, Duration: 146, Sign: -1
Page views: 6, User id: 4324182021466249494, Useless field: world, Duration: 185, Sign: 1
Total rows: 2
```

``` sql
INSERT INTO UserActivity FORMAT Template SETTINGS
format_template_resultset = '/some/path/resultset.format', format_template_row = '/some/path/row.format'
```

`/some/path/resultset.format`:

``` text
Some header\n${data}\nTotal rows: ${:CSV}\n
```

`/some/path/row.format`:

``` text
Page views: ${PageViews:CSV}, User id: ${UserID:CSV}, Useless field: ${:CSV}, Duration: ${Duration:CSV}, Sign: ${Sign:CSV}
```

`PageViews`, `UserID`, `Duration` و `Sign` در داخل متغیرهایی نام ستون در جدول هستند. مقادیر پس از `Useless field` در ردیف و بعد از `\nTotal rows:` در پسوند نادیده گرفته خواهد شد.
همه delimiters در داده های ورودی باید به شدت در برابر delimiters در فرمت مشخص شده رشته.

## پاسخ تمپلیتینی {#templateignorespaces}

این فرمت فقط برای ورودی مناسب است.
مشابه به `Template`, اما پرش کاراکتر فضای سفید بین جداکننده ها و ارزش ها در جریان ورودی. با این حال, اگر رشته فرمت حاوی کاراکتر فضای سفید, این شخصیت خواهد شد در جریان ورودی انتظار می رود. همچنین اجازه می دهد برای مشخص متغیرهایی خالی (`${}` یا `${:None}`) به تقسیم برخی از جداساز به قطعات جداگانه به چشم پوشی از فضاهای بین. چنین متغیرهایی تنها برای پرش شخصیت فضای سفید استفاده می شود.
خواندن ممکن است `JSON` با استفاده از این فرمت, اگر ارزش ستون همان نظم در تمام ردیف. برای مثال درخواست زیر می تواند مورد استفاده قرار گیرد برای قرار دادن داده ها از خروجی نمونه ای از فرمت [JSON](#json):

``` sql
INSERT INTO table_name FORMAT TemplateIgnoreSpaces SETTINGS
format_template_resultset = '/some/path/resultset.format', format_template_row = '/some/path/row.format', format_template_rows_between_delimiter = ','
```

`/some/path/resultset.format`:

``` text
{${}"meta"${}:${:JSON},${}"data"${}:${}[${data}]${},${}"totals"${}:${:JSON},${}"extremes"${}:${:JSON},${}"rows"${}:${:JSON},${}"rows_before_limit_at_least"${}:${:JSON}${}}
```

`/some/path/row.format`:

``` text
{${}"SearchPhrase"${}:${}${phrase:JSON}${},${}"c"${}:${}${cnt:JSON}${}}
```

## TSKV {#tskv}

شبیه به جدول, اما خروجی یک مقدار در نام=فرمت ارزش. نام ها به همان شیوه در قالب جدول فرار, و = نماد نیز فرار.

``` text
SearchPhrase=   count()=8267016
SearchPhrase=bathroom interior design    count()=2166
SearchPhrase=yandex     count()=1655
SearchPhrase=2014 spring fashion    count()=1549
SearchPhrase=freeform photos       count()=1480
SearchPhrase=angelina jolie    count()=1245
SearchPhrase=omsk       count()=1112
SearchPhrase=photos of dog breeds    count()=1091
SearchPhrase=curtain designs        count()=1064
SearchPhrase=baku       count()=1000
```

[NULL](../sql-reference/syntax.md) به عنوان فرمت `\N`.

``` sql
SELECT * FROM t_null FORMAT TSKV
```

``` text
x=1    y=\N
```

هنگامی که تعداد زیادی از ستون های کوچک وجود دارد, این فرمت بی اثر است, و به طور کلی هیچ دلیلی برای استفاده وجود دارد. با این وجود از لحاظ کارایی بدتر از جیسوناکرو نیست.

Both data output and parsing are supported in this format. For parsing, any order is supported for the values of different columns. It is acceptable for some values to be omitted – they are treated as equal to their default values. In this case, zeros and blank rows are used as default values. Complex values that could be specified in the table are not supported as defaults.

تجزیه اجازه می دهد تا حضور زمینه های اضافی `tskv` بدون علامت مساوی یا ارزش. این زمینه نادیده گرفته شده است.

## CSV {#csv}

با کاما از هم جدا فرمت ارزش ([RFC](https://tools.ietf.org/html/rfc4180)).

هنگام قالب بندی, ردیف در دو نقل قول محصور. نقل قول دو در داخل یک رشته خروجی به عنوان دو نقل قول دو در یک ردیف است. هیچ قانون دیگری برای فرار از شخصیت وجود دارد. تاریخ و تاریخ زمان در دو نقل قول محصور شده است. اعداد خروجی بدون نقل قول. ارزش ها توسط یک شخصیت جداساز از هم جدا, که `,` به طور پیش فرض. شخصیت جداساز در تنظیمات تعریف شده است [_مخفی کردن _قابلیت _جدید](../operations/settings/settings.md#settings-format_csv_delimiter). ردیف ها با استفاده از خوراک خط یونیکس جدا می شوند. ارریس در سی سی اس وی به شرح زیر مرتب شده است: ابتدا مجموعه ای به یک رشته به عنوان در قالب تبسپار شده مرتب شده است و سپس رشته حاصل خروجی به سی سی اس وی در دو نقل قول است. دسته بندی ها در قالب سی اس وی به صورت ستون های جداگانه مرتب می شوند (به این معنا که لانه خود را در تاپل از دست داده است).

``` bash
$ clickhouse-client --format_csv_delimiter="|" --query="INSERT INTO test.csv FORMAT CSV" < data.csv
```

\* به طور پیش فرض, جداساز است `,`. دیدن [_مخفی کردن _قابلیت _جدید](../operations/settings/settings.md#settings-format_csv_delimiter) تنظیم برای اطلاعات بیشتر.

هنگامی که تجزیه, تمام مقادیر را می توان یا با یا بدون نقل قول تجزیه. هر دو نقل قول دو و تک پشتیبانی می شوند. ردیف همچنین می توانید بدون نقل قول مرتب شود. در این مورد, به شخصیت جداساز و یا خوراک خط تجزیه (کروم و یا ال اف). در هنگام تجزیه ردیف بدون نقل قول فضاهای پیشرو و انتهایی و زبانه ها نادیده گرفته می شوند. برای اشتراک خط, یونیکس (کلیک کنید), پنجره ها (کروم ال اف) و سیستم عامل مک کلاسیک (کروم ال اف) انواع پشتیبانی می شوند.

مقادیر ورودی بدون علامت خالی با مقادیر پیش فرض برای ستون های مربوطه جایگزین می شوند
[_پوشه های ورودی و خروجی](../operations/settings/settings.md#session_settings-input_format_defaults_for_omitted_fields)
فعال است.

`NULL` به عنوان فرمت `\N` یا `NULL` یا یک رشته بدون علامت خالی (تنظیمات را ببینید [_فرستادن به _کوچکنمایی](../operations/settings/settings.md#settings-input_format_csv_unquoted_null_literal_as_null) و [_پوشه های ورودی و خروجی](../operations/settings/settings.md#session_settings-input_format_defaults_for_omitted_fields)).

پشتیبانی از خروجی بالغ و افراط به همان شیوه به عنوان `TabSeparated`.

## اطلاعات دقیق {#csvwithnames}

همچنین چاپ ردیف هدر, شبیه به `TabSeparatedWithNames`.

## سفارشی {#format-customseparated}

مشابه به [قالب](#format-template) اما همه ستون ها را چاپ می کند یا می خواند و از قاعده فرار از تنظیم استفاده می کند `format_custom_escaping_rule` و جداکننده از تنظیمات `format_custom_field_delimiter`, `format_custom_row_before_delimiter`, `format_custom_row_after_delimiter`, `format_custom_row_between_delimiter`, `format_custom_result_before_delimiter` و `format_custom_result_after_delimiter`, نه از رشته فرمت.
همچنین وجود دارد `CustomSeparatedIgnoreSpaces` قالب که شبیه به `TemplateIgnoreSpaces`.

## JSON {#json}

خروجی داده ها در فرمت جانسون. علاوه بر جداول داده, همچنین خروجی نام ستون و انواع, همراه با برخی از اطلاعات اضافی: تعداد کل ردیف خروجی, و تعداد ردیف است که می تواند خروجی بوده است اگر یک محدودیت وجود ندارد. مثال:

``` sql
SELECT SearchPhrase, count() AS c FROM test.hits GROUP BY SearchPhrase WITH TOTALS ORDER BY c DESC LIMIT 5 FORMAT JSON
```

``` json
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

جانسون سازگار با جاوا اسکریپت است. برای اطمینان از این, برخی از شخصیت ها علاوه بر فرار: بریده بریده `/` فرار به عنوان `\/`; معافیت خط جایگزین `U+2028` و `U+2029`, که شکستن برخی از مرورگرهای, به عنوان فرار `\uXXXX`. شخصیت های کنترل اسکی فرار: برگشت به عقب, خوراک فرم, خوراک خط, بازگشت حمل, و تب افقی با جایگزین `\b`, `\f`, `\n`, `\r`, `\t` , و همچنین بایت باقی مانده در محدوده 00-1ف با استفاده از `\uXXXX` sequences. Invalid UTF-8 sequences are changed to the replacement character � so the output text will consist of valid UTF-8 sequences. For compatibility with JavaScript, Int64 and UInt64 integers are enclosed in double-quotes by default. To remove the quotes, you can set the configuration parameter [خروجی _فرمان_جسون_کوات_64بیت_تنظیمی](../operations/settings/settings.md#session_settings-output_format_json_quote_64bit_integers) به 0.

`rows` – The total number of output rows.

`rows_before_limit_at_least` حداقل تعداد ردیف وجود دارد که بدون محدودیت بوده است. خروجی تنها در صورتی که پرس و جو شامل حد.
اگر پرس و جو شامل گروه های, ردیف ها_افور_لیمیت_تلاست تعداد دقیق ردیف وجود دارد که بدون محدودیت بوده است.

`totals` – Total values (when using WITH TOTALS).

`extremes` – Extreme values (when extremes are set to 1).

این فرمت فقط برای خروجی یک نتیجه پرس و جو مناسب است, اما نه برای تجزیه (بازیابی اطلاعات برای وارد کردن در یک جدول).

پشتیبانی از کلیک [NULL](../sql-reference/syntax.md), است که به عنوان نمایش داده `null` در خروجی جانسون.

همچنین نگاه کنید به [جیسانچرو](#jsoneachrow) قالب.

## فوق العاده {#jsoncompact}

متفاوت از جانسون تنها در ردیف داده ها خروجی در ارریس, نه در اشیا.

مثال:

``` json
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
                ["fashion trends spring 2014", "1549"],
                ["freeform photo", "1480"]
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

این فرمت فقط برای خروجی یک نتیجه پرس و جو مناسب است, اما نه برای تجزیه (بازیابی اطلاعات برای وارد کردن در یک جدول).
همچنین نگاه کنید به `JSONEachRow` قالب.

## جیسانچرو {#jsoneachrow}

هنگامی که با استفاده از این فرمت, تاتر خروجی ردیف به عنوان جدا, اجسام جسون-خط حد و مرز مشخصی, اما داده ها به عنوان یک کل است جسون معتبر نیست.

``` json
{"SearchPhrase":"curtain designs","count()":"1064"}
{"SearchPhrase":"baku","count()":"1000"}
{"SearchPhrase":"","count()":"8267016"}
```

هنگام قرار دادن داده ها, شما باید یک شی جانسون جداگانه برای هر سطر فراهم.

### درج داده {#inserting-data}

``` sql
INSERT INTO UserActivity FORMAT JSONEachRow {"PageViews":5, "UserID":"4324182021466249494", "Duration":146,"Sign":-1} {"UserID":"4324182021466249494","PageViews":6,"Duration":185,"Sign":1}
```

کلیک اجازه می دهد تا:

-   هر منظور از جفت کلید ارزش در جسم.
-   حذف برخی از ارزش ها.

تاتر فضاهای بین عناصر و کاما را پس از اشیا نادیده می گیرد. شما می توانید تمام اشیا را در یک خط منتقل کنید. لازم نیست با شکستن خط اونارو جدا کنی

**حذف پردازش مقادیر**

را کلیک کنید جایگزین مقادیر حذف شده با مقادیر پیش فرض برای مربوطه [انواع داده ها](../sql-reference/data-types/index.md).

اگر `DEFAULT expr` مشخص شده است, تاتر با استفاده از قوانین تعویض مختلف بسته به [_پوشه های ورودی و خروجی](../operations/settings/settings.md#session_settings-input_format_defaults_for_omitted_fields) تنظیمات.

جدول زیر را در نظر بگیرید:

``` sql
CREATE TABLE IF NOT EXISTS example_table
(
    x UInt32,
    a DEFAULT x * 2
) ENGINE = Memory;
```

-   اگر `input_format_defaults_for_omitted_fields = 0` سپس مقدار پیش فرض برای `x` و `a` برابر `0` (به عنوان مقدار پیش فرض برای `UInt32` نوع داده).
-   اگر `input_format_defaults_for_omitted_fields = 1` سپس مقدار پیش فرض برای `x` برابر `0` اما مقدار پیش فرض `a` برابر `x * 2`.

!!! note "اخطار"
    هنگام قرار دادن داده ها با `insert_sample_with_metadata = 1`, کلیکهاوس مصرف منابع محاسباتی بیشتر, در مقایسه با درج با `insert_sample_with_metadata = 0`.

### انتخاب داده ها {#selecting-data}

در نظر بگیرید که `UserActivity` جدول به عنوان مثال:

``` text
┌──────────────UserID─┬─PageViews─┬─Duration─┬─Sign─┐
│ 4324182021466249494 │         5 │      146 │   -1 │
│ 4324182021466249494 │         6 │      185 │    1 │
└─────────────────────┴───────────┴──────────┴──────┘
```

پرسوجو `SELECT * FROM UserActivity FORMAT JSONEachRow` بازگشت:

``` text
{"UserID":"4324182021466249494","PageViews":5,"Duration":146,"Sign":-1}
{"UserID":"4324182021466249494","PageViews":6,"Duration":185,"Sign":1}
```

بر خلاف [JSON](#json) قالب, هیچ جایگزینی نامعتبر گفته-8 توالی وجود دارد. ارزش ها در همان راه برای فرار `JSON`.

!!! note "یادداشت"
    هر مجموعه ای از بایت می تواند خروجی در رشته ها. استفاده از `JSONEachRow` فرمت اگر شما اطمینان حاصل کنید که داده ها در جدول را می توان به عنوان جانسون بدون از دست دادن هر گونه اطلاعات فرمت شده است.

### استفاده از ساختارهای تو در تو {#jsoneachrow-nested}

اگر شما یک جدول با [تو در تو](../sql-reference/data-types/nested-data-structures/nested.md) ستون نوع داده, شما می توانید داده های جانسون با همان ساختار وارد. فعال کردن این ویژگی با [تغییر _کم_تر_تنظیم مجدد _جنسان](../operations/settings/settings.md#settings-input_format_import_nested_json) تنظیمات.

برای مثال جدول زیر را در نظر بگیرید:

``` sql
CREATE TABLE json_each_row_nested (n Nested (s String, i Int32) ) ENGINE = Memory
```

همانطور که شما می توانید در `Nested` شرح نوع داده, تاتر رفتار هر یک از اجزای ساختار تو در تو به عنوان یک ستون جداگانه (`n.s` و `n.i` برای جدول ما). شما می توانید داده ها را به روش زیر وارد کنید:

``` sql
INSERT INTO json_each_row_nested FORMAT JSONEachRow {"n.s": ["abc", "def"], "n.i": [1, 23]}
```

برای قرار دادن داده ها به عنوان یک شی جوسون سلسله مراتبی, تنظیم [وارد کردن _ترمپ_م_تر_اس_جسون ثبت شده=1](../operations/settings/settings.md#settings-input_format_import_nested_json).

``` json
{
    "n": {
        "s": ["abc", "def"],
        "i": [1, 23]
    }
}
```

بدون این تنظیم, محل کلیک می اندازد یک استثنا.

``` sql
SELECT name, value FROM system.settings WHERE name = 'input_format_import_nested_json'
```

``` text
┌─name────────────────────────────┬─value─┐
│ input_format_import_nested_json │ 0     │
└─────────────────────────────────┴───────┘
```

``` sql
INSERT INTO json_each_row_nested FORMAT JSONEachRow {"n": {"s": ["abc", "def"], "i": [1, 23]}}
```

``` text
Code: 117. DB::Exception: Unknown field found while parsing JSONEachRow format: n: (at row 1)
```

``` sql
SET input_format_import_nested_json=1
INSERT INTO json_each_row_nested FORMAT JSONEachRow {"n": {"s": ["abc", "def"], "i": [1, 23]}}
SELECT * FROM json_each_row_nested
```

``` text
┌─n.s───────────┬─n.i────┐
│ ['abc','def'] │ [1,23] │
└───────────────┴────────┘
```

## بومی {#native}

فرمت موثر ترین. داده ها توسط بلوک ها در فرمت باینری نوشته شده و خوانده می شوند. برای هر بلوک, تعداد ردیف, تعداد ستون, نام ستون و انواع, و بخش هایی از ستون ها در این بلوک یکی پس از دیگری ثبت. به عبارت دیگر این قالب است “columnar” – it doesn't convert columns to rows. This is the format used in the native interface for interaction between servers, for using the command-line client, and for C++ clients.

شما می توانید این فرمت را به سرعت تولید افسردگی است که تنها می تواند توسط سندرم تونل کارپ به عنوان خوانده شده استفاده کنید. این حس برای کار با این فرمت خود را ندارد.

## خالی {#null}

هیچ چیز خروجی است. اما پرس و جو پردازش شده است و در هنگام استفاده از خط فرمان مشتری داده منتقل می شود به مشتری. این است که برای تست استفاده, از جمله تست عملکرد.
به طور مشخص, این فرمت فقط برای خروجی مناسب است, نه برای تجزیه.

## زیبا {#pretty}

خروجی داده ها به عنوان جداول یونیکد هنر, همچنین با استفاده از توالی انسی فرار برای تنظیم رنگ در ترمینال.
یک شبکه کامل از جدول کشیده شده است, و هر سطر را اشغال دو خط در ترمینال.
هر بلوک نتیجه خروجی به عنوان یک جدول جداگانه است. این لازم است به طوری که بلوک می تواند خروجی بدون نتیجه بافر (بافر می شود به منظور قبل از محاسبه عرض قابل مشاهده از تمام مقادیر لازم).

[NULL](../sql-reference/syntax.md) خروجی به عنوان `ᴺᵁᴸᴸ`.

مثال (نشان داده شده برای [پیش تیمار](#prettycompact) قالب):

``` sql
SELECT * FROM t_null
```

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
└───┴──────┘
```

ردیف در زیبا \* فرمت فرار نیست. به عنوان مثال برای نشان داده شده است [پیش تیمار](#prettycompact) قالب:

``` sql
SELECT 'String with \'quotes\' and \t character' AS Escaping_test
```

``` text
┌─Escaping_test────────────────────────┐
│ String with 'quotes' and      character │
└──────────────────────────────────────┘
```

برای جلوگیری از تخلیه اطلاعات بیش از حد به ترمینال تنها 10000 ردیف اول چاپ شده است. اگر تعداد ردیف بیشتر یا برابر با است 10,000, پیام “Showed first 10 000” چاپ شده است.
این فرمت فقط برای خروجی یک نتیجه پرس و جو مناسب است, اما نه برای تجزیه (بازیابی اطلاعات برای وارد کردن در یک جدول).

فرمت زیبا پشتیبانی خروجی ارزش کل (هنگام استفاده با بالغ) و افراط و (وقتی که ‘extremes’ به مجموعه 1). در این موارد, کل ارزش ها و ارزش های شدید خروجی پس از داده های اصلی هستند, در جداول جداگانه. مثال (نشان داده شده برای [پیش تیمار](#prettycompact) قالب):

``` sql
SELECT EventDate, count() AS c FROM test.hits GROUP BY EventDate WITH TOTALS ORDER BY EventDate FORMAT PrettyCompact
```

``` text
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
│ 1970-01-01 │ 8873898 │
└────────────┴─────────┘

Extremes:
┌──EventDate─┬───────c─┐
│ 2014-03-17 │ 1031592 │
│ 2014-03-23 │ 1406958 │
└────────────┴─────────┘
```

## پیش تیمار {#prettycompact}

متفاوت از [زیبا](#pretty) در که شبکه بین ردیف کشیده شده و نتیجه جمع و جور تر است.
این فرمت به طور پیش فرض در مشتری خط فرمان در حالت تعاملی استفاده می شود.

## بلوک پیش ساخته {#prettycompactmonoblock}

متفاوت از [پیش تیمار](#prettycompact) در که تا 10000 ردیف بافر و سپس خروجی به عنوان یک جدول واحد نه بلوک.

## کتاب های پیش بینی شده {#prettynoescapes}

متفاوت از زیبا که در توالی اس-فرار استفاده نمی شود. این برای نمایش این فرمت در یک مرورگر و همچنین برای استفاده ضروری است ‘watch’ ابزار خط فرمان.

مثال:

``` bash
$ watch -n1 "clickhouse-client --query='SELECT event, value FROM system.events FORMAT PrettyCompactNoEscapes'"
```

شما می توانید رابط قام برای نمایش در مرورگر استفاده کنید.

### کتاب های پیش ساخته {#prettycompactnoescapes}

همان تنظیمات قبلی.

### کتاب های پیش بینی شده {#prettyspacenoescapes}

همان تنظیمات قبلی.

## چوب نما {#prettyspace}

متفاوت از [پیش تیمار](#prettycompact) در که فضای سفید (شخصیت های فضایی) به جای شبکه استفاده می شود.

## مربوط به حوزه {#rowbinary}

فرمت ها و تجزیه داده ها توسط ردیف در فرمت باینری. سطر و ارزش ها به صورت متوالی ذکر شده, بدون جدا.
این فرمت کمتر از فرمت بومی است زیرا مبتنی بر ردیف است.

اعداد صحیح استفاده از ثابت طول نمایندگی کوچک اندی. مثلا, اوینت64 با استفاده از 8 بایت.
تاریخ ساعت به عنوان اوینت32 حاوی برچسب زمان یونیکس به عنوان ارزش نشان داده است.
تاریخ به عنوان یک شی اوینت16 که شامل تعداد روز از 1970-01-01 به عنوان ارزش نشان داده شده است.
رشته به عنوان یک طول ورق (بدون علامت) نشان داده شده است [LEB128](https://en.wikipedia.org/wiki/LEB128)), پس از بایت رشته.
رشته ثابت است که به سادگی به عنوان یک دنباله از بایت نشان داده شده است.

اری به عنوان یک طول ورینت (بدون علامت) نشان داده شده است [LEB128](https://en.wikipedia.org/wiki/LEB128)), پس از عناصر پی در پی از مجموعه.

برای [NULL](../sql-reference/syntax.md#null-literal) حمایت کردن, یک بایت اضافی حاوی 1 یا 0 قبل از هر اضافه [Nullable](../sql-reference/data-types/nullable.md) ارزش. اگر 1, سپس ارزش است `NULL` و این بایت به عنوان یک مقدار جداگانه تفسیر. اگر 0, ارزش پس از بایت است `NULL`.

## ارزشهای خبری عبارتند از: {#rowbinarywithnamesandtypes}

مشابه به [مربوط به حوزه](#rowbinary) اما با هدر اضافه شده است:

-   [LEB128](https://en.wikipedia.org/wiki/LEB128)- کد گذاری تعداد ستون ها)
-   N `String`مشخص کردن نامهای ستون
-   N `String`بازدید کنندگان مشخص انواع ستون

## مقادیر {#data-format-values}

چاپ هر سطر در براکت. ردیف ها توسط کاما جدا می شوند. بعد از ردیف گذشته هیچ کاما وجود ندارد. مقادیر داخل براکت نیز با کاما از هم جدا هستند. اعداد خروجی در قالب اعشاری بدون نقل قول هستند. ارریس خروجی در براکت مربع است. رشته, تاریخ, و تاریخ با زمان خروجی در نقل قول. فرار قوانین و تجزیه شبیه به [جدول دار](#tabseparated) قالب. در قالب بندی فضاهای اضافی وارد نشده اند اما در طول تجزیه مجاز و نادیده گرفته می شوند (به جز فضاهای درون مقادیر مجموعه ای که مجاز نیستند). [NULL](../sql-reference/syntax.md) به عنوان نمایندگی `NULL`.

The minimum set of characters that you need to escape when passing data in Values ​​format: single quotes and backslashes.

این فرمت است که در استفاده می شود `INSERT INTO t VALUES ...` اما شما همچنین می توانید برای قالب بندی نتایج پرس و جو استفاده کنید.

همچنین نگاه کنید به: [در حال خواندن:](../operations/settings/settings.md#settings-input_format_values_interpret_expressions) و [در حال خواندن:](../operations/settings/settings.md#settings-input_format_values_deduce_templates_of_expressions) تنظیمات.

## عمودی {#vertical}

چاپ هر مقدار در یک خط جداگانه با نام ستون مشخص. این فرمت مناسب برای چاپ فقط یک یا چند ردیف است اگر هر سطر شامل تعداد زیادی از ستون.

[NULL](../sql-reference/syntax.md) خروجی به عنوان `ᴺᵁᴸᴸ`.

مثال:

``` sql
SELECT * FROM t_null FORMAT Vertical
```

``` text
Row 1:
──────
x: 1
y: ᴺᵁᴸᴸ
```

ردیف در فرمت عمودی فرار نیست:

``` sql
SELECT 'string with \'quotes\' and \t with some special \n characters' AS test FORMAT Vertical
```

``` text
Row 1:
──────
test: string with 'quotes' and      with some special
 characters
```

این فرمت فقط برای خروجی یک نتیجه پرس و جو مناسب است, اما نه برای تجزیه (بازیابی اطلاعات برای وارد کردن در یک جدول).

## عمودی {#verticalraw}

مشابه به [عمودی](#vertical), اما با فرار غیر فعال. این قالب فقط برای خروجی نتایج پرس و جو مناسب است, نه برای تجزیه (دریافت داده ها و قرار دادن در جدول).

## XML {#xml}

فرمت فقط برای خروجی مناسب است نه برای تجزیه. مثال:

``` xml
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
                        <SearchPhrase>2014 spring fashion</SearchPhrase>
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
                        <SearchPhrase>curtain designs</SearchPhrase>
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

اگر نام ستون یک فرمت قابل قبول ندارد, فقط ‘field’ به عنوان نام عنصر استفاده می شود. به طور کلی ساختار ایکس میل از ساختار جسون پیروی می کند.
Just as for JSON, invalid UTF-8 sequences are changed to the replacement character � so the output text will consist of valid UTF-8 sequences.

در مقادیر رشته, شخصیت `<` و `&` فرار به عنوان `<` و `&`.

ارریس خروجی به عنوان `<array><elem>Hello</elem><elem>World</elem>...</array>`,و تاپل به عنوان `<tuple><elem>Hello</elem><elem>World</elem>...</tuple>`.

## کاپپروتو {#capnproto}

کپن پروتو فرمت پیام باینری شبیه به بافر پروتکل و صرفه جویی است, اما جسون یا مساگپک را دوست ندارد.

پیام های کپ ان پروتو به شدت تایپ شده و نه خود توصیف, به این معنی که نیاز به یک شرح طرح خارجی. طرح در پرواز اعمال می شود و ذخیره سازی برای هر پرس و جو.

``` bash
$ cat capnproto_messages.bin | clickhouse-client --query "INSERT INTO test.hits FORMAT CapnProto SETTINGS format_schema='schema:Message'"
```

کجا `schema.capnp` به نظر می رسد مثل این:

``` capnp
struct Message {
  SearchPhrase @0 :Text;
  c @1 :Uint64;
}
```

Deserialization موثر است و معمولا نمی افزایش بار سیستم.

همچنین نگاه کنید به [شمای فرمت](#formatschema).

## Protobuf {#protobuf}

Protobuf - یک [بافر پروتکل](https://developers.google.com/protocol-buffers/) قالب.

این فرمت نیاز به یک طرح فرمت خارجی دارد. طرح بین نمایش داده شد ذخیره سازی.
تاتر از هر دو `proto2` و `proto3` syntaxes. تکرار / اختیاری / زمینه های مورد نیاز پشتیبانی می شوند.

نمونه های استفاده:

``` sql
SELECT * FROM test.table FORMAT Protobuf SETTINGS format_schema = 'schemafile:MessageType'
```

``` bash
cat protobuf_messages.bin | clickhouse-client --query "INSERT INTO test.table FORMAT Protobuf SETTINGS format_schema='schemafile:MessageType'"
```

جایی که فایل `schemafile.proto` به نظر می رسد مثل این:

``` capnp
syntax = "proto3";

message MessageType {
  string name = 1;
  string surname = 2;
  uint32 birthDate = 3;
  repeated string phoneNumbers = 4;
};
```

برای پیدا کردن مکاتبات بین ستون های جدول و زمینه های بافر پروتکل' نوع پیام تاتر نام خود را مقایسه می کند.
این مقایسه غیر حساس به حروف و شخصیت است `_` هشدار داده می شود `.` (نقطه) به عنوان برابر در نظر گرفته.
اگر نوع ستون و زمینه پیام بافر پروتکل متفاوت تبدیل لازم اعمال می شود.

پیام های تو در تو پشتیبانی می شوند. برای مثال برای این زمینه است `z` در نوع پیام زیر

``` capnp
message MessageType {
  message XType {
    message YType {
      int32 z;
    };
    repeated YType y;
  };
  XType x;
};
```

تاتر تلاش می کند برای پیدا کردن یک ستون به نام `x.y.z` (یا `x_y_z` یا `X.y_Z` و به همین ترتیب).
پیام های تو در تو مناسب برای ورودی یا خروجی هستند [ساختارهای داده تو در تو](../sql-reference/data-types/nested-data-structures/nested.md).

مقادیر پیش فرض تعریف شده در یک طرح اولیه مانند این

``` capnp
syntax = "proto2";

message MessageType {
  optional int32 result_per_page = 3 [default = 10];
}
```

اعمال نمی شود [پیشفرضهای جدول](../sql-reference/statements/create.md#create-default-values) به جای اونها استفاده میشه

ClickHouse ورودی و خروجی protobuf پیام در `length-delimited` قالب.
این بدان معنی است قبل از هر پیام باید طول خود را به عنوان یک نوشته [ورینت](https://developers.google.com/protocol-buffers/docs/encoding#varints).
همچنین نگاه کنید به [چگونه به خواندن / نوشتن طول-حد و مرز مشخصی پیام های ورودی به زبان های محبوب](https://cwiki.apache.org/confluence/display/GEODE/Delimiting+Protobuf+Messages).

## اورو {#data-format-avro}

[هشدار داده می شود](http://avro.apache.org/) یک چارچوب سریال داده ردیف گرا توسعه یافته در درون پروژه هادوپ خود نمایی است.

قالب کلیک اور پشتیبانی از خواندن و نوشتن [پروندههای داد و ستد](http://avro.apache.org/docs/current/spec.html#Object+Container+Files).

### تطبیق انواع داده ها {#data_types-matching}

جدول زیر انواع داده های پشتیبانی شده را نشان می دهد و چگونه با کلیک مطابقت دارند [انواع داده ها](../sql-reference/data-types/index.md) داخل `INSERT` و `SELECT` نمایش داده شد.

| نوع داده اورو `INSERT`                      | نوع داده کلیک                                                                                                              | نوع داده اورو `SELECT`       |
|---------------------------------------------|----------------------------------------------------------------------------------------------------------------------------|------------------------------|
| `boolean`, `int`, `long`, `float`, `double` | [اعضای هیات(8/16/32)](../sql-reference/data-types/int-uint.md), [اوینت (8/16/32)](../sql-reference/data-types/int-uint.md) | `int`                        |
| `boolean`, `int`, `long`, `float`, `double` | [Int64](../sql-reference/data-types/int-uint.md), [UInt64](../sql-reference/data-types/int-uint.md)                        | `long`                       |
| `boolean`, `int`, `long`, `float`, `double` | [Float32](../sql-reference/data-types/float.md)                                                                            | `float`                      |
| `boolean`, `int`, `long`, `float`, `double` | [جسم شناور64](../sql-reference/data-types/float.md)                                                                        | `double`                     |
| `bytes`, `string`, `fixed`, `enum`          | [رشته](../sql-reference/data-types/string.md)                                                                              | `bytes`                      |
| `bytes`, `string`, `fixed`                  | [رشته ثابت)](../sql-reference/data-types/fixedstring.md)                                                                   | `fixed(N)`                   |
| `enum`                                      | [شمارشی (8/16)](../sql-reference/data-types/enum.md)                                                                       | `enum`                       |
| `array(T)`                                  | [& توری)](../sql-reference/data-types/array.md)                                                                            | `array(T)`                   |
| `union(null, T)`, `union(T, null)`          | [Nullable(T)](../sql-reference/data-types/date.md)                                                                         | `union(null, T)`             |
| `null`                                      | [Nullable(هیچ چیز)](../sql-reference/data-types/special-data-types/nothing.md)                                             | `null`                       |
| `int (date)` \*                             | [تاریخ](../sql-reference/data-types/date.md)                                                                               | `int (date)` \*              |
| `long (timestamp-millis)` \*                | [طول تاریخ 64 (3)](../sql-reference/data-types/datetime.md)                                                                | `long (timestamp-millis)` \* |
| `long (timestamp-micros)` \*                | [طول تاریخ 64 (6)](../sql-reference/data-types/datetime.md)                                                                | `long (timestamp-micros)` \* |

\* [انواع منطقی اورو](http://avro.apache.org/docs/current/spec.html#Logical+Types)

انواع داده های ورودی پشتیبانی نشده: `record` (غیر ریشه), `map`

انواع داده های منطقی پشتیبانی نشده: `uuid`, `time-millis`, `time-micros`, `duration`

### درج داده {#inserting-data-1}

برای وارد کردن داده ها از یک فایل اورو به جدول کلیک کنید:

``` bash
$ cat file.avro | clickhouse-client --query="INSERT INTO {some_table} FORMAT Avro"
```

طرح ریشه فایل ورودی ورودی باید باشد `record` نوع.

برای پیدا کردن مکاتبات بین ستون های جدول و زمینه های اور طرحواره کلیک نام خود را مقایسه می کند. این مقایسه حساس به حروف است.
زمینه های استفاده نشده قلم می شوند.

انواع داده ها از ستون جدول کلیک می توانید از زمینه های مربوطه را از داده های اور قرار داده متفاوت است. در هنگام قرار دادن داده ها, تاتر تفسیر انواع داده ها با توجه به جدول بالا و سپس [کست](../sql-reference/functions/type-conversion-functions.md#type_conversion_function-cast) داده ها به نوع ستون مربوطه.

### انتخاب داده ها {#selecting-data-1}

برای انتخاب داده ها از جدول کلیک به یک فایل پیشرو:

``` bash
$ clickhouse-client --query="SELECT * FROM {some_table} FORMAT Avro" > file.avro
```

نام ستون باید:

-   شروع با `[A-Za-z_]`
-   متعاقبا تنها حاوی `[A-Za-z0-9_]`

خروجی فشرده سازی فایل و فاصله همگام سازی را می توان با پیکربندی [_فرماندگی لبه بام](../operations/settings/settings.md#settings-output_format_avro_codec) و [_فرماندگی لبه چشم](../operations/settings/settings.md#settings-output_format_avro_sync_interval) به ترتیب.

## هشدار داده می شود {#data-format-avro-confluent}

ارتباط با پشتیبانی از رمز گشایی پیام های تک شی اور معمولا با استفاده می شود [کافکا](https://kafka.apache.org/) و [رجیستری طرح پساب](https://docs.confluent.io/current/schema-registry/index.html).

هر پیام ایسترو جاسازی یک شناسه طرح است که می تواند به طرح واقعی با کمک رجیستری طرح حل و فصل شود.

طرحواره ذخیره سازی یک بار حل شود.

نشانی وب رجیستری طرحواره با پیکربندی [باز کردن _نمایش مجدد](../operations/settings/settings.md#settings-format_avro_schema_registry_url)

### تطبیق انواع داده ها {#data_types-matching-1}

مثل [اورو](#data-format-avro)

### استفاده {#usage}

به سرعت بررسی طرح قطعنامه شما می توانید استفاده کنید [کفککت](https://github.com/edenhill/kafkacat) با [کلیک-محلی](../operations/utilities/clickhouse-local.md):

``` bash
$ kafkacat -b kafka-broker  -C -t topic1 -o beginning -f '%s' -c 3 | clickhouse-local   --input-format AvroConfluent --format_avro_schema_registry_url 'http://schema-registry' -S "field1 Int64, field2 String"  -q 'select *  from table'
1 a
2 b
3 c
```

برای استفاده `AvroConfluent` با [کافکا](../engines/table-engines/integrations/kafka.md):

``` sql
CREATE TABLE topic1_stream
(
    field1 String,
    field2 String
)
ENGINE = Kafka()
SETTINGS
kafka_broker_list = 'kafka-broker',
kafka_topic_list = 'topic1',
kafka_group_name = 'group1',
kafka_format = 'AvroConfluent';

SET format_avro_schema_registry_url = 'http://schema-registry';

SELECT * FROM topic1_stream;
```

!!! note "اخطار"
    تنظیم `format_avro_schema_registry_url` نیاز به پیکربندی در `users.xml` برای حفظ ارزش پس از راه اندازی مجدد.

## پارکت {#data-format-parquet}

[پارکت چوب کمر درد](http://parquet.apache.org/) فرمت ذخیره سازی ستونی گسترده در اکوسیستم هادوپ است. تاتر پشتیبانی خواندن و نوشتن عملیات برای این فرمت.

### تطبیق انواع داده ها {#data_types-matching-2}

جدول زیر انواع داده های پشتیبانی شده را نشان می دهد و چگونه با کلیک مطابقت دارند [انواع داده ها](../sql-reference/data-types/index.md) داخل `INSERT` و `SELECT` نمایش داده شد.

| نوع داده پارکت (`INSERT`) | نوع داده کلیک                                           | نوع داده پارکت (`SELECT`) |
|---------------------------|---------------------------------------------------------|---------------------------|
| `UINT8`, `BOOL`           | [UInt8](../sql-reference/data-types/int-uint.md)        | `UINT8`                   |
| `INT8`                    | [Int8](../sql-reference/data-types/int-uint.md)         | `INT8`                    |
| `UINT16`                  | [UInt16](../sql-reference/data-types/int-uint.md)       | `UINT16`                  |
| `INT16`                   | [Int16](../sql-reference/data-types/int-uint.md)        | `INT16`                   |
| `UINT32`                  | [UInt32](../sql-reference/data-types/int-uint.md)       | `UINT32`                  |
| `INT32`                   | [Int32](../sql-reference/data-types/int-uint.md)        | `INT32`                   |
| `UINT64`                  | [UInt64](../sql-reference/data-types/int-uint.md)       | `UINT64`                  |
| `INT64`                   | [Int64](../sql-reference/data-types/int-uint.md)        | `INT64`                   |
| `FLOAT`, `HALF_FLOAT`     | [Float32](../sql-reference/data-types/float.md)         | `FLOAT`                   |
| `DOUBLE`                  | [جسم شناور64](../sql-reference/data-types/float.md)     | `DOUBLE`                  |
| `DATE32`                  | [تاریخ](../sql-reference/data-types/date.md)            | `UINT16`                  |
| `DATE64`, `TIMESTAMP`     | [DateTime](../sql-reference/data-types/datetime.md)     | `UINT32`                  |
| `STRING`, `BINARY`        | [رشته](../sql-reference/data-types/string.md)           | `STRING`                  |
| —                         | [رشته ثابت](../sql-reference/data-types/fixedstring.md) | `STRING`                  |
| `DECIMAL`                 | [دهدهی](../sql-reference/data-types/decimal.md)         | `DECIMAL`                 |

کلیک هاوس از دقت قابل تنظیم پشتیبانی می کند `Decimal` نوع. این `INSERT` پرس و جو رفتار پارکت `DECIMAL` نوع به عنوان محل کلیک `Decimal128` نوع.

انواع داده های پارکت پشتیبانی نشده: `DATE32`, `TIME32`, `FIXED_SIZE_BINARY`, `JSON`, `UUID`, `ENUM`.

انواع داده ها از ستون جدول کلیک خانه می تواند از زمینه های مربوطه را از داده پارکت قرار داده متفاوت است. در هنگام قرار دادن داده ها, تاتر تفسیر انواع داده ها با توجه به جدول بالا و سپس [بازیگران](../query_language/functions/type_conversion_functions/#type_conversion_function-cast) داده ها به این نوع داده است که برای ستون جدول کلیک مجموعه.

### درج و انتخاب داده ها {#inserting-and-selecting-data}

شما می توانید داده های پارکت از یک فایل را به جدول کلیک توسط دستور زیر وارد کنید:

``` bash
$ cat {filename} | clickhouse-client --query="INSERT INTO {some_table} FORMAT Parquet"
```

شما می توانید داده ها را از یک جدول کلیک انتخاب کنید و با دستور زیر به برخی از فایل ها در قالب پارکت ذخیره کنید:

``` bash
$ clickhouse-client --query="SELECT * FROM {some_table} FORMAT Parquet" > {some_file.pq}
```

برای تبادل اطلاعات با هادوپ, شما می توانید استفاده کنید [موتور جدول اچ دی اف](../engines/table-engines/integrations/hdfs.md).

## ORC {#data-format-orc}

[آپاچی ORC](https://orc.apache.org/) فرمت ذخیره سازی ستونی گسترده در اکوسیستم هادوپ است. شما فقط می توانید داده ها را در این قالب به کلیک کنید.

### تطبیق انواع داده ها {#data_types-matching-3}

جدول زیر انواع داده های پشتیبانی شده را نشان می دهد و چگونه با کلیک مطابقت دارند [انواع داده ها](../sql-reference/data-types/index.md) داخل `INSERT` نمایش داده شد.

| نوع داده اورک (`INSERT`) | نوع داده کلیک                                       |
|--------------------------|-----------------------------------------------------|
| `UINT8`, `BOOL`          | [UInt8](../sql-reference/data-types/int-uint.md)    |
| `INT8`                   | [Int8](../sql-reference/data-types/int-uint.md)     |
| `UINT16`                 | [UInt16](../sql-reference/data-types/int-uint.md)   |
| `INT16`                  | [Int16](../sql-reference/data-types/int-uint.md)    |
| `UINT32`                 | [UInt32](../sql-reference/data-types/int-uint.md)   |
| `INT32`                  | [Int32](../sql-reference/data-types/int-uint.md)    |
| `UINT64`                 | [UInt64](../sql-reference/data-types/int-uint.md)   |
| `INT64`                  | [Int64](../sql-reference/data-types/int-uint.md)    |
| `FLOAT`, `HALF_FLOAT`    | [Float32](../sql-reference/data-types/float.md)     |
| `DOUBLE`                 | [جسم شناور64](../sql-reference/data-types/float.md) |
| `DATE32`                 | [تاریخ](../sql-reference/data-types/date.md)        |
| `DATE64`, `TIMESTAMP`    | [DateTime](../sql-reference/data-types/datetime.md) |
| `STRING`, `BINARY`       | [رشته](../sql-reference/data-types/string.md)       |
| `DECIMAL`                | [دهدهی](../sql-reference/data-types/decimal.md)     |

تاتر از دقت قابل تنظیم از `Decimal` نوع. این `INSERT` پرس و جو رفتار اورک `DECIMAL` نوع به عنوان محل کلیک `Decimal128` نوع.

انواع داده های پشتیبانی نشده: `DATE32`, `TIME32`, `FIXED_SIZE_BINARY`, `JSON`, `UUID`, `ENUM`.

انواع داده ها از ستون جدول کلیک هاوس لازم نیست برای مطابقت با زمینه های داده اورک مربوطه. در هنگام قرار دادن داده ها, تاتر تفسیر انواع داده ها با توجه به جدول بالا و سپس [کست](../sql-reference/functions/type-conversion-functions.md#type_conversion_function-cast) داده ها به نوع داده تعیین شده برای ستون جدول کلیک.

### درج داده {#inserting-data-2}

شما می توانید داده اورک از یک فایل به جدول کلیک توسط دستور زیر وارد کنید:

``` bash
$ cat filename.orc | clickhouse-client --query="INSERT INTO some_table FORMAT ORC"
```

برای تبادل اطلاعات با هادوپ, شما می توانید استفاده کنید [موتور جدول اچ دی اف](../engines/table-engines/integrations/hdfs.md).

## شمای فرمت {#formatschema}

نام پرونده حاوی شمای قالب با تنظیم تنظیم می شود `format_schema`.
لازم است این تنظیم را هنگام استفاده از یکی از فرمت ها تنظیم کنید `Cap'n Proto` و `Protobuf`.
شمای فرمت ترکیبی از یک نام فایل و نام یک نوع پیام در این فایل است که توسط روده بزرگ جدا شده است,
e.g. `schemafile.proto:MessageType`.
اگر فایل دارای فرمت استاندارد برای فرمت (به عنوان مثال, `.proto` برای `Protobuf`),
این را می توان حذف و در این مورد طرح فرمت به نظر می رسد `schemafile:MessageType`.

اگر داده های ورودی یا خروجی را از طریق [کارگیر](../interfaces/cli.md) در [حالت تعاملی](../interfaces/cli.md#cli_usage), نام پرونده مشخص شده در شمای فرمت
می تواند شامل یک مسیر مطلق و یا یک مسیر نسبت به دایرکتوری جاری در مشتری.
اگر شما با استفاده از مشتری در [حالت دسته ای](../interfaces/cli.md#cli_usage) مسیر طرح باید به دلایل امنیتی نسبی باشد.

اگر داده های ورودی یا خروجی را از طریق [رابط قام](../interfaces/http.md) نام پرونده مشخص شده در شمای قالب
باید در دایرکتوری مشخص شده در واقع [قالب_شکلمات شیمی](../operations/server-configuration-parameters/settings.md#server_configuration_parameters-format_schema_path)
در پیکربندی سرور.

## پرش خطاها {#skippingerrors}

برخی از فرمت های مانند `CSV`, `TabSeparated`, `TSKV`, `JSONEachRow`, `Template`, `CustomSeparated` و `Protobuf` می توانید ردیف شکسته جست و خیز اگر خطای تجزیه رخ داده است و ادامه تجزیه از ابتدای ردیف بعدی. ببینید [وارد کردن _فرست_مرزیابی _نمایش مجدد](../operations/settings/settings.md#settings-input_format_allow_errors_num) و
[ثبت نام](../operations/settings/settings.md#settings-input_format_allow_errors_ratio) تنظیمات.
محدودیت ها:
- در صورت خطای تجزیه `JSONEachRow` پرش تمام داده ها تا زمانی که خط جدید (یا بخش ویژه), بنابراین ردیف باید توسط حد و مرز مشخصی `\n` برای شمارش خطاها به درستی.
- `Template` و `CustomSeparated` استفاده از جداساز پس از ستون گذشته و جداساز بین ردیف برای پیدا کردن ابتدای سطر بعدی, بنابراین اشتباهات پرش کار می کند تنها اگر حداقل یکی از خالی نیست.

[مقاله اصلی](https://clickhouse.tech/docs/en/interfaces/formats/) <!--hide-->
