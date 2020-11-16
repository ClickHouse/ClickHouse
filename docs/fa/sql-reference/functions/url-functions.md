---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 54
toc_title: "\u06A9\u0627\u0631 \u0628\u0627 \u0646\u0634\u0627\u0646\u06CC\u0647\u0627\
  \u06CC \u0627\u06CC\u0646\u062A\u0631\u0646\u062A\u06CC"
---

# توابع برای کار با نشانیهای اینترنتی {#functions-for-working-with-urls}

همه این توابع را دنبال کنید. حداکثر برای بهبود عملکرد ساده شده اند.

## توابع استخراج بخشهایی از نشانی وب {#functions-that-extract-parts-of-a-url}

اگر قسمت مربوطه در نشانی وب وجود نداشته باشد یک رشته خالی برگردانده میشود.

### قرارداد {#protocol}

پروتکل را از نشانی اینترنتی خارج میکند.

Examples of typical returned values: http, https, ftp, mailto, tel, magnet…

### دامنه {#domain}

نام میزبان را از یک نشانی اینترنتی استخراج می کند.

``` sql
domain(url)
```

**پارامترها**

-   `url` — URL. Type: [رشته](../../sql-reference/data-types/string.md).

نشانی وب را می توان با یا بدون یک طرح مشخص شده است. مثالها:

``` text
svn+ssh://some.svn-hosting.com:80/repo/trunk
some.svn-hosting.com:80/repo/trunk
https://yandex.com/time/
```

برای این نمونه ها `domain` تابع نتایج زیر را برمی گرداند:

``` text
some.svn-hosting.com
some.svn-hosting.com
yandex.com
```

**مقادیر بازگشتی**

-   نام میزبان. اگر کلیک هاوس می تواند رشته ورودی را به عنوان نشانی وب تجزیه کند.
-   رشته خالی. اگر کلیکهاوس نمیتواند رشته ورودی را به عنوان نشانی وب تجزیه کند.

نوع: `String`.

**مثال**

``` sql
SELECT domain('svn+ssh://some.svn-hosting.com:80/repo/trunk')
```

``` text
┌─domain('svn+ssh://some.svn-hosting.com:80/repo/trunk')─┐
│ some.svn-hosting.com                                   │
└────────────────────────────────────────────────────────┘
```

### دامینویتهویتوو {#domainwithoutwww}

بازگرداندن دامنه و حذف بیش از یک ‘www.’ اگه از اول شروع بشه

### توپولدومین {#topleveldomain}

دامنه سطح بالا را از یک نشانی اینترنتی استخراج می کند.

``` sql
topLevelDomain(url)
```

**پارامترها**

-   `url` — URL. Type: [رشته](../../sql-reference/data-types/string.md).

نشانی وب را می توان با یا بدون یک طرح مشخص شده است. مثالها:

``` text
svn+ssh://some.svn-hosting.com:80/repo/trunk
some.svn-hosting.com:80/repo/trunk
https://yandex.com/time/
```

**مقادیر بازگشتی**

-   نام دامنه. اگر کلیک هاوس می تواند رشته ورودی را به عنوان نشانی وب تجزیه کند.
-   رشته خالی. اگر کلیکهاوس نمیتواند رشته ورودی را به عنوان نشانی وب تجزیه کند.

نوع: `String`.

**مثال**

``` sql
SELECT topLevelDomain('svn+ssh://www.some.svn-hosting.com:80/repo/trunk')
```

``` text
┌─topLevelDomain('svn+ssh://www.some.svn-hosting.com:80/repo/trunk')─┐
│ com                                                                │
└────────────────────────────────────────────────────────────────────┘
```

### در حال بارگذاری {#firstsignificantsubdomain}

بازگرداندن “first significant subdomain”. این یک مفهوم غیر استاندارد خاص به یاندکس است.متریکا اولین زیر دامنه قابل توجهی یک دامنه سطح دوم است ‘com’, ‘net’, ‘org’ یا ‘co’. در غیر این صورت, این یک دامنه سطح سوم است. به عنوان مثال, `firstSignificantSubdomain (‘https://news.yandex.ru/’) = ‘yandex’, firstSignificantSubdomain (‘https://news.yandex.com.tr/’) = ‘yandex’`. فهرست “insignificant” دامنه های سطح دوم و سایر اطلاعات پیاده سازی ممکن است در اینده تغییر کند.

### در حال بارگذاری {#cuttofirstsignificantsubdomain}

بازگرداندن بخشی از دامنه است که شامل زیر دامنه سطح بالا تا “first significant subdomain” (توضیح بالا را ببینید).

به عنوان مثال, `cutToFirstSignificantSubdomain('https://news.yandex.com.tr/') = 'yandex.com.tr'`.

### مسیر {#path}

مسیر را برمی گرداند. مثال: `/top/news.html` مسیر رشته پرس و جو را شامل نمی شود.

### مسیر {#pathfull}

همان بالا, اما از جمله رشته پرس و جو و قطعه. مثال:/بالا / اخبار.زنگامصفحه = 2 # نظرات

### رشته {#querystring}

بازگرداندن رشته پرس و جو. مثال: صفحه=1&چاپی=213. پرس و جو رشته علامت سوال اولیه را شامل نمی شود, و همچنین # و همه چیز بعد از #.

### قطعه {#fragment}

بازگرداندن شناسه قطعه. قطعه می کند نماد هش اولیه را شامل نمی شود.

### وضعیت زیستشناختی رکورد {#querystringandfragment}

بازگرداندن رشته پرس و جو و شناسه قطعه. مثال: صفحه=1#29390.

### نام) {#extracturlparameterurl-name}

بازگرداندن ارزش ‘name’ پارامتر در نشانی وب, در صورت وجود. در غیر این صورت, یک رشته خالی. اگر پارامترهای بسیاری با این نام وجود دارد, این اولین رخداد را برمی گرداند. این تابع با این فرض کار می کند که نام پارامتر در نشانی اینترنتی دقیقا به همان شیوه در استدلال گذشت کد گذاری شده است.

### شمارش معکوس) {#extracturlparametersurl}

مجموعه ای از نام=رشته ارزش مربوط به پارامترهای نشانی وب را برمی گرداند. ارزش ها به هیچ وجه رمزگشایی نمی.

### extractURLParameterNames(URL) {#extracturlparameternamesurl}

مجموعه ای از نام رشته های مربوط به نام پارامترهای نشانی وب را باز می گرداند. ارزش ها به هیچ وجه رمزگشایی نمی.

### URLHierarchy(URL) {#urlhierarchyurl}

را برمی گرداند مجموعه ای حاوی نشانی اینترنتی, کوتاه در پایان توسط علامت /,? در مسیر و پرس و جو-رشته. کاراکتر جداکننده متوالی به عنوان یکی شمارش می شود. برش در موقعیت بعد از تمام کاراکتر جدا متوالی ساخته شده است.

### URLPathHierarchy(URL) {#urlpathhierarchyurl}

همانطور که در بالا, اما بدون پروتکل و میزبان در نتیجه. / عنصر (ریشه) گنجانده نشده است. به عنوان مثال: تابع استفاده می شود برای پیاده سازی درخت گزارش نشانی اینترنتی در یاندکس. متریک.

``` text
URLPathHierarchy('https://example.com/browse/CONV-6788') =
[
    '/browse/',
    '/browse/CONV-6788'
]
```

### نما & نشانی وب) {#decodeurlcomponenturl}

را برمی گرداند نشانی اینترنتی رمزگشایی.
مثال:

``` sql
SELECT decodeURLComponent('http://127.0.0.1:8123/?query=SELECT%201%3B') AS DecodedURL;
```

``` text
┌─DecodedURL─────────────────────────────┐
│ http://127.0.0.1:8123/?query=SELECT 1; │
└────────────────────────────────────────┘
```

## توابع حذف بخشی از نشانی وب {#functions-that-remove-part-of-a-url}

اگر نشانی وب هیچ چیز مشابه ندارد, نشانی اینترنتی بدون تغییر باقی می ماند.

### بریدن {#cutwww}

حذف بیش از یک ‘www.’ از ابتدای دامنه نشانی اینترنتی در صورت وجود.

### & رشته {#cutquerystring}

حذف رشته پرس و جو. علامت سوال نیز حذف شده است.

### تقسیم {#cutfragment}

حذف شناسه قطعه. علامت شماره نیز حذف شده است.

### هشدار داده می شود {#cutquerystringandfragment}

حذف رشته پرس و جو و شناسه قطعه. علامت سوال و علامت شماره نیز حذف شده است.

### cutURLParameter(URL, نام) {#cuturlparameterurl-name}

حذف ‘name’ پارامتر نشانی وب, در صورت وجود. این تابع با این فرض کار می کند که نام پارامتر در نشانی اینترنتی دقیقا به همان شیوه در استدلال گذشت کد گذاری شده است.

[مقاله اصلی](https://clickhouse.tech/docs/en/query_language/functions/url_functions/) <!--hide-->
