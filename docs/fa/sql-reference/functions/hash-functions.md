---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 50
toc_title: "\u0647\u0634"
---

# توابع هش {#hash-functions}

توابع هش را می توان برای زدن شبه تصادفی قطعی از عناصر استفاده می شود.

## نیم مترد5 {#hash-functions-halfmd5}

[تفسیر](../../sql-reference/functions/type-conversion-functions.md#type_conversion_functions-reinterpretAsString) تمام پارامترهای ورودی به عنوان رشته ها و محاسبه [MD5](https://en.wikipedia.org/wiki/MD5) ارزش هش برای هر یک از. سپس هش ها را ترکیب می کند و اولین بایت 8 هش رشته حاصل را می گیرد و به عنوان تفسیر می کند `UInt64` به ترتیب بایت بزرگ اندی.

``` sql
halfMD5(par1, ...)
```

تابع نسبتا کند است (5 میلیون رشته کوتاه در هر ثانیه در هر هسته پردازنده).
در نظر بگیرید با استفاده از [سیفون64](#hash_functions-siphash64) تابع به جای.

**پارامترها**

تابع طول می کشد تعداد متغیر پارامترهای ورودی. پارامترها می توانند هر یک از [انواع داده های پشتیبانی شده](../../sql-reference/data-types/index.md).

**مقدار بازگشتی**

A [UInt64](../../sql-reference/data-types/int-uint.md) نوع داده مقدار هش.

**مثال**

``` sql
SELECT halfMD5(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS halfMD5hash, toTypeName(halfMD5hash) AS type
```

``` text
┌────────halfMD5hash─┬─type───┐
│ 186182704141653334 │ UInt64 │
└────────────────────┴────────┘
```

## MD5 {#hash_functions-md5}

محاسبه ام دی 5 از یک رشته و مجموعه ای در نتیجه از بایت به عنوان رشته ثابت را برمی گرداند(16).
اگر شما ام دی 5 به طور خاص نیاز ندارد, اما شما نیاز به یک رمزنگاری مناسب و معقول هش 128 بیتی, استفاده از ‘sipHash128’ تابع به جای.
اگر شما می خواهید برای دریافت همان نتیجه به عنوان خروجی توسط ابزار موسسه خدمات مالی, استفاده از پایین تر(سحر و جادو کردن(توسعه هزاره5(بازدید کنندگان))).

## سیفون64 {#hash_functions-siphash64}

تولید 64 بیتی [سیفون](https://131002.net/siphash/) مقدار هش.

``` sql
sipHash64(par1,...)
```

این یک تابع هش رمزنگاری است. این کار حداقل سه بار سریع تر از [MD5](#hash_functions-md5) تابع.

تابع [تفسیر](../../sql-reference/functions/type-conversion-functions.md#type_conversion_functions-reinterpretAsString) تمام پارامترهای ورودی به عنوان رشته و محاسبه مقدار هش برای هر یک از. سپس هش ها را با الگوریتم زیر ترکیب می کند:

1.  پس از هش کردن تمام پارامترهای ورودی, تابع می شود مجموعه ای از رشته هش.
2.  تابع عناصر اول و دوم را می گیرد و هش را برای مجموعه ای از این موارد محاسبه می کند.
3.  سپس تابع مقدار هش را محاسبه می کند که در مرحله قبل محاسبه می شود و عنصر سوم هش های اولیه را محاسبه می کند و هش را برای مجموعه ای از انها محاسبه می کند.
4.  گام قبلی برای تمام عناصر باقی مانده از مجموعه هش اولیه تکرار شده است.

**پارامترها**

تابع طول می کشد تعداد متغیر پارامترهای ورودی. پارامترها می توانند هر یک از [انواع داده های پشتیبانی شده](../../sql-reference/data-types/index.md).

**مقدار بازگشتی**

A [UInt64](../../sql-reference/data-types/int-uint.md) نوع داده مقدار هش.

**مثال**

``` sql
SELECT sipHash64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS SipHash, toTypeName(SipHash) AS type
```

``` text
┌──────────────SipHash─┬─type───┐
│ 13726873534472839665 │ UInt64 │
└──────────────────────┴────────┘
```

## سیفون128 {#hash_functions-siphash128}

محاسبه سیفون از یک رشته.
می پذیرد استدلال رشته از نوع. را برمی گرداند رشته ثابت (16).
متفاوت از سیفون64 در که حالت نهایی صخره نوردی تاشو تنها تا 128 بیت انجام می شود.

## 4تیهاش64 {#cityhash64}

تولید 64 بیتی [هشدار داده می شود](https://github.com/google/cityhash) مقدار هش.

``` sql
cityHash64(par1,...)
```

این یک تابع هش غیر رمزنگاری سریع است. با استفاده از الگوریتم سیتیاش برای پارامترهای رشته و اجرای خاص تابع هش غیر رمزنگاری سریع برای پارامترهای با انواع داده های دیگر. این تابع از ترکیب کننده سیتیاش برای دریافت نتایج نهایی استفاده می کند.

**پارامترها**

تابع طول می کشد تعداد متغیر پارامترهای ورودی. پارامترها می توانند هر یک از [انواع داده های پشتیبانی شده](../../sql-reference/data-types/index.md).

**مقدار بازگشتی**

A [UInt64](../../sql-reference/data-types/int-uint.md) نوع داده مقدار هش.

**مثالها**

مثال تماس:

``` sql
SELECT cityHash64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS CityHash, toTypeName(CityHash) AS type
```

``` text
┌─────────────CityHash─┬─type───┐
│ 12072650598913549138 │ UInt64 │
└──────────────────────┴────────┘
```

مثال زیر نشان می دهد که چگونه برای محاسبه کنترلی از کل جدول با دقت تا سفارش ردیف:

``` sql
SELECT groupBitXor(cityHash64(*)) FROM table
```

## اینتش32 {#inthash32}

محاسبه یک کد هش 32 بیتی از هر نوع عدد صحیح.
این یک تابع هش غیر رمزنگاری نسبتا سریع از کیفیت متوسط برای اعداد است.

## اینتاش64 {#inthash64}

محاسبه یک کد هش 64 بیتی از هر نوع عدد صحیح.
این کار سریع تر از اینتش32. میانگین کیفیت.

## SHA1 {#sha1}

## SHA224 {#sha224}

## SHA256 {#sha256}

محاسبه شا-1, شا-224, یا شا-256 از یک رشته و مجموعه ای در نتیجه از بایت به عنوان رشته گرداند(20), رشته ثابت(28), و یا رشته ثابت(32).
تابع کار می کند نسبتا کند (شا-1 پردازش در مورد 5 میلیون رشته کوتاه در هر ثانیه در هر هسته پردازنده, در حالی که شا-224 و شا-256 روند در مورد 2.2 میلیون).
ما توصیه می کنیم با استفاده از این تابع تنها در مواردی که شما نیاز به یک تابع هش خاص و شما می توانید انتخاب کنید.
حتی در این موارد توصیه می شود که از تابع به صورت افلاین استفاده کنید و مقادیر قبل از محاسبه هنگام وارد کردن جدول به جای استفاده در انتخاب ها.

## نشانی اینترنتی\]) {#urlhashurl-n}

یک تابع هش غیر رمزنگاری سریع و با کیفیت مناسب برای یک رشته از یک نشانی وب با استفاده از نوعی عادی سازی.
`URLHash(s)` – Calculates a hash from a string without one of the trailing symbols `/`,`?` یا `#` در پایان, اگر در حال حاضر.
`URLHash(s, N)` – Calculates a hash from a string up to the N level in the URL hierarchy, without one of the trailing symbols `/`,`?` یا `#` در پایان, اگر در حال حاضر.
سطح همان است که در هرج و مرج است. این تابع خاص به یاندکس است.متریکا

## فرمان 64 {#farmhash64}

تولید 64 بیتی [مزرعه دار](https://github.com/google/farmhash) مقدار هش.

``` sql
farmHash64(par1, ...)
```

تابع با استفاده از `Hash64` روش از همه [روش های موجود](https://github.com/google/farmhash/blob/master/src/farmhash.h).

**پارامترها**

تابع طول می کشد تعداد متغیر پارامترهای ورودی. پارامترها می توانند هر یک از [انواع داده های پشتیبانی شده](../../sql-reference/data-types/index.md).

**مقدار بازگشتی**

A [UInt64](../../sql-reference/data-types/int-uint.md) نوع داده مقدار هش.

**مثال**

``` sql
SELECT farmHash64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS FarmHash, toTypeName(FarmHash) AS type
```

``` text
┌─────────────FarmHash─┬─type───┐
│ 17790458267262532859 │ UInt64 │
└──────────────────────┴────────┘
```

## جواهاش {#hash_functions-javahash}

محاسبه [جواهاش](http://hg.openjdk.java.net/jdk8u/jdk8u/jdk/file/478a4add975b/src/share/classes/java/lang/String.java#l1452) از یک رشته. این تابع هش نه سریع و نه با کیفیت خوب است. تنها دلیل استفاده از این است که این الگوریتم در حال حاضر در سیستم دیگری استفاده می شود و شما باید دقیقا همان نتیجه را محاسبه کنید.

**نحو**

``` sql
SELECT javaHash('');
```

**مقدار بازگشتی**

A `Int32` نوع داده مقدار هش.

**مثال**

پرسوجو:

``` sql
SELECT javaHash('Hello, world!');
```

نتیجه:

``` text
┌─javaHash('Hello, world!')─┐
│               -1880044555 │
└───────────────────────────┘
```

## جواهرشوتف16 {#javahashutf16le}

محاسبه [جواهاش](http://hg.openjdk.java.net/jdk8u/jdk8u/jdk/file/478a4add975b/src/share/classes/java/lang/String.java#l1452) از یک رشته, فرض کنید که شامل بایت به نمایندگی از یک رشته در رمزگذاری اوت-16ل.

**نحو**

``` sql
javaHashUTF16LE(stringUtf16le)
```

**پارامترها**

-   `stringUtf16le` — a string in UTF-16LE encoding.

**مقدار بازگشتی**

A `Int32` نوع داده مقدار هش.

**مثال**

درست پرس و جو با UTF-16LE کد گذاری رشته است.

پرسوجو:

``` sql
SELECT javaHashUTF16LE(convertCharset('test', 'utf-8', 'utf-16le'))
```

نتیجه:

``` text
┌─javaHashUTF16LE(convertCharset('test', 'utf-8', 'utf-16le'))─┐
│                                                      3556498 │
└──────────────────────────────────────────────────────────────┘
```

## هیوهاش {#hash-functions-hivehash}

محاسبه `HiveHash` از یک رشته.

``` sql
SELECT hiveHash('');
```

این فقط [جواهاش](#hash_functions-javahash) با کمی نشانه صفر کردن. این تابع در استفاده [زنبورک کندو](https://en.wikipedia.org/wiki/Apache_Hive) برای نسخه های قبل از 3.0. این تابع هش نه سریع و نه با کیفیت خوب است. تنها دلیل استفاده از این است که این الگوریتم در حال حاضر در سیستم دیگری استفاده می شود و شما باید دقیقا همان نتیجه را محاسبه کنید.

**مقدار بازگشتی**

A `Int32` نوع داده مقدار هش.

نوع: `hiveHash`.

**مثال**

پرسوجو:

``` sql
SELECT hiveHash('Hello, world!');
```

نتیجه:

``` text
┌─hiveHash('Hello, world!')─┐
│                 267439093 │
└───────────────────────────┘
```

## متروهاش64 {#metrohash64}

تولید 64 بیتی [متروهاش](http://www.jandrewrogers.com/2015/05/27/metrohash/) مقدار هش.

``` sql
metroHash64(par1, ...)
```

**پارامترها**

تابع طول می کشد تعداد متغیر پارامترهای ورودی. پارامترها می توانند هر یک از [انواع داده های پشتیبانی شده](../../sql-reference/data-types/index.md).

**مقدار بازگشتی**

A [UInt64](../../sql-reference/data-types/int-uint.md) نوع داده مقدار هش.

**مثال**

``` sql
SELECT metroHash64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS MetroHash, toTypeName(MetroHash) AS type
```

``` text
┌────────────MetroHash─┬─type───┐
│ 14235658766382344533 │ UInt64 │
└──────────────────────┴────────┘
```

## مورد احترام {#jumpconsistenthash}

محاسبه JumpConsistentHash فرم UInt64.
می پذیرد دو استدلال: یک کلید بین 64 نوع و تعداد سطل. بازده Int32.
برای کسب اطلاعات بیشتر به لینک مراجعه کنید: [مورد احترام](https://arxiv.org/pdf/1406.2294.pdf)

## سوفلش2_32, سوفلشه2_64 {#murmurhash2-32-murmurhash2-64}

تولید یک [زمزمه 2](https://github.com/aappleby/smhasher) مقدار هش.

``` sql
murmurHash2_32(par1, ...)
murmurHash2_64(par1, ...)
```

**پارامترها**

هر دو توابع را به تعداد متغیر از پارامترهای ورودی. پارامترها می توانند هر یک از [انواع داده های پشتیبانی شده](../../sql-reference/data-types/index.md).

**مقدار بازگشتی**

-   این `murmurHash2_32` تابع را برمی گرداند مقدار هش داشتن [UInt32](../../sql-reference/data-types/int-uint.md) نوع داده.
-   این `murmurHash2_64` تابع را برمی گرداند مقدار هش داشتن [UInt64](../../sql-reference/data-types/int-uint.md) نوع داده.

**مثال**

``` sql
SELECT murmurHash2_64(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS MurmurHash2, toTypeName(MurmurHash2) AS type
```

``` text
┌──────────MurmurHash2─┬─type───┐
│ 11832096901709403633 │ UInt64 │
└──────────────────────┴────────┘
```

## اطلاعات دقیق {#gccmurmurhash}

محاسبه 64 بیتی [زمزمه 2](https://github.com/aappleby/smhasher) مقدار هش با استفاده از همان دانه هش به عنوان [شورای همکاری خلیج فارس](https://github.com/gcc-mirror/gcc/blob/41d6b10e96a1de98e90a7c0378437c3255814b16/libstdc%2B%2B-v3/include/bits/functional_hash.h#L191). این قابل حمل بین کلانگ و شورای همکاری خلیج فارس ایجاد شده است.

**نحو**

``` sql
gccMurmurHash(par1, ...);
```

**پارامترها**

-   `par1, ...` — A variable number of parameters that can be any of the [انواع داده های پشتیبانی شده](../../sql-reference/data-types/index.md#data_types).

**مقدار بازگشتی**

-   محاسبه مقدار هش.

نوع: [UInt64](../../sql-reference/data-types/int-uint.md).

**مثال**

پرسوجو:

``` sql
SELECT
    gccMurmurHash(1, 2, 3) AS res1,
    gccMurmurHash(('a', [1, 2, 3], 4, (4, ['foo', 'bar'], 1, (1, 2)))) AS res2
```

نتیجه:

``` text
┌─────────────────res1─┬────────────────res2─┐
│ 12384823029245979431 │ 1188926775431157506 │
└──────────────────────┴─────────────────────┘
```

## سوفلش3_32, سوفلشه3_64 {#murmurhash3-32-murmurhash3-64}

تولید یک [سوفلهاش3](https://github.com/aappleby/smhasher) مقدار هش.

``` sql
murmurHash3_32(par1, ...)
murmurHash3_64(par1, ...)
```

**پارامترها**

هر دو توابع را به تعداد متغیر از پارامترهای ورودی. پارامترها می توانند هر یک از [انواع داده های پشتیبانی شده](../../sql-reference/data-types/index.md).

**مقدار بازگشتی**

-   این `murmurHash3_32` تابع یک [UInt32](../../sql-reference/data-types/int-uint.md) نوع داده مقدار هش.
-   این `murmurHash3_64` تابع یک [UInt64](../../sql-reference/data-types/int-uint.md) نوع داده مقدار هش.

**مثال**

``` sql
SELECT murmurHash3_32(array('e','x','a'), 'mple', 10, toDateTime('2019-06-15 23:00:00')) AS MurmurHash3, toTypeName(MurmurHash3) AS type
```

``` text
┌─MurmurHash3─┬─type───┐
│     2152717 │ UInt32 │
└─────────────┴────────┘
```

## سوفلش3_128 {#murmurhash3-128}

تولید 128 بیتی [سوفلهاش3](https://github.com/aappleby/smhasher) مقدار هش.

``` sql
murmurHash3_128( expr )
```

**پارامترها**

-   `expr` — [عبارتها](../syntax.md#syntax-expressions) بازگشت یک [رشته](../../sql-reference/data-types/string.md)- نوع ارزش.

**مقدار بازگشتی**

A [رشته ثابت (16)](../../sql-reference/data-types/fixedstring.md) نوع داده مقدار هش.

**مثال**

``` sql
SELECT murmurHash3_128('example_string') AS MurmurHash3, toTypeName(MurmurHash3) AS type
```

``` text
┌─MurmurHash3──────┬─type────────────┐
│ 6�1�4"S5KT�~~q │ FixedString(16) │
└──────────────────┴─────────────────┘
```

## بیست و 3264 {#hash-functions-xxhash32}

محاسبه `xxHash` از یک رشته. این است که در دو طعم پیشنهاد, 32 و 64 بیت.

``` sql
SELECT xxHash32('');

OR

SELECT xxHash64('');
```

**مقدار بازگشتی**

A `Uint32` یا `Uint64` نوع داده مقدار هش.

نوع: `xxHash`.

**مثال**

پرسوجو:

``` sql
SELECT xxHash32('Hello, world!');
```

نتیجه:

``` text
┌─xxHash32('Hello, world!')─┐
│                 834093149 │
└───────────────────────────┘
```

**همچنین نگاه کنید به**

-   [معلم](http://cyan4973.github.io/xxHash/).

[مقاله اصلی](https://clickhouse.tech/docs/en/query_language/functions/hash_functions/) <!--hide-->
