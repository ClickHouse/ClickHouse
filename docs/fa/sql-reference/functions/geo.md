---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 62
toc_title: "\u06A9\u0627\u0631 \u0628\u0627 \u0645\u062E\u062A\u0635\u0627\u062A \u062C\
  \u063A\u0631\u0627\u0641\u06CC\u0627\u06CC\u06CC"
---

# توابع برای کار با مختصات جغرافیایی {#functions-for-working-with-geographical-coordinates}

## نمایش سایت {#greatcircledistance}

محاسبه فاصله بین دو نقطه بر روی سطح زمین با استفاده از [فرمول دایره بزرگ](https://en.wikipedia.org/wiki/Great-circle_distance).

``` sql
greatCircleDistance(lon1Deg, lat1Deg, lon2Deg, lat2Deg)
```

**پارامترهای ورودی**

-   `lon1Deg` — Longitude of the first point in degrees. Range: `[-180°, 180°]`.
-   `lat1Deg` — Latitude of the first point in degrees. Range: `[-90°, 90°]`.
-   `lon2Deg` — Longitude of the second point in degrees. Range: `[-180°, 180°]`.
-   `lat2Deg` — Latitude of the second point in degrees. Range: `[-90°, 90°]`.

مقادیر مثبت به طول و عرض جغرافیایی شمال شرق مطابقت, و مقادیر منفی به طول و عرض جغرافیایی جنوبی و طول جغرافیایی غرب مطابقت.

**مقدار بازگشتی**

فاصله بین دو نقطه بر روی سطح زمین, در متر.

تولید یک استثنا زمانی که مقادیر پارامتر ورودی در خارج از محدوده قرار می گیرند.

**مثال**

``` sql
SELECT greatCircleDistance(55.755831, 37.617673, -55.755831, -37.617673)
```

``` text
┌─greatCircleDistance(55.755831, 37.617673, -55.755831, -37.617673)─┐
│                                                14132374.194975413 │
└───────────────────────────────────────────────────────────────────┘
```

## نقلقولهای جدید از این نویسنده {#pointinellipses}

بررسی اینکه نقطه متعلق به حداقل یکی از بیضی.
مختصات هندسی در سیستم مختصات دکارتی هستند.

``` sql
pointInEllipses(x, y, x₀, y₀, a₀, b₀,...,xₙ, yₙ, aₙ, bₙ)
```

**پارامترهای ورودی**

-   `x, y` — Coordinates of a point on the plane.
-   `xᵢ, yᵢ` — Coordinates of the center of the `i`- حذف هفتم .
-   `aᵢ, bᵢ` — Axes of the `i`- حذف ت در واحد های ایکس, و مختصات.

پارامترهای ورودی باید باشد `2+4⋅n` کجا `n` تعداد بیضی است.

**مقادیر بازگشتی**

`1` اگر نقطه در داخل حداقل یکی از بیضی; `0`اگر این طور نیست.

**مثال**

``` sql
SELECT pointInEllipses(10., 10., 10., 9.1, 1., 0.9999)
```

``` text
┌─pointInEllipses(10., 10., 10., 9.1, 1., 0.9999)─┐
│                                               1 │
└─────────────────────────────────────────────────┘
```

## نقطه چین {#pointinpolygon}

بررسی اینکه نقطه متعلق به چند ضلعی در هواپیما.

``` sql
pointInPolygon((x, y), [(a, b), (c, d) ...], ...)
```

**مقادیر ورودی**

-   `(x, y)` — Coordinates of a point on the plane. Data type — [تاپل](../../sql-reference/data-types/tuple.md) — A tuple of two numbers.
-   `[(a, b), (c, d) ...]` — Polygon vertices. Data type — [& حذف](../../sql-reference/data-types/array.md). هر راس است که توسط یک جفت مختصات نشان داده شده است `(a, b)`. راس باید در جهت عقربه های ساعت و یا در خلاف جهت عقربه مشخص شده است. حداقل تعداد راس است 3. چند ضلعی باید ثابت باشد.
-   این تابع همچنین از چند ضلعی با سوراخ (برش بخش). در این مورد, اضافه چند ضلعی است که تعریف بخش برش با استفاده از استدلال های اضافی از تابع. تابع چند ضلعی غیر به سادگی متصل را پشتیبانی نمی کند.

**مقادیر بازگشتی**

`1` اگر نقطه در داخل چند ضلعی باشد, `0` اگر این طور نیست.
اگر نقطه در مرز چند ضلعی, تابع ممکن است یا بازگشت 0 یا 1.

**مثال**

``` sql
SELECT pointInPolygon((3., 3.), [(6, 0), (8, 4), (5, 8), (0, 2)]) AS res
```

``` text
┌─res─┐
│   1 │
└─────┘
```

## کد جغرافیایی {#geohashencode}

کد طول و عرض جغرافیایی به عنوان یک geohash-رشته مراجعه کنید (http://geohash.org/, https://en.wikipedia.org/wiki/Geohash).

``` sql
geohashEncode(longitude, latitude, [precision])
```

**مقادیر ورودی**

-   طول جغرافیایی-طول جغرافیایی بخشی از مختصات شما می خواهید به رمز. شناور در محدوده`[-180°, 180°]`
-   عرض جغرافیایی-عرض جغرافیایی بخشی از مختصات شما می خواهید به رمز. شناور در محدوده `[-90°, 90°]`
-   دقت-اختیاری, طول رشته کد گذاری نتیجه, به طور پیش فرض به `12`. عدد صحیح در محدوده `[1, 12]`. هر مقدار کمتر از `1` یا بیشتر از `12` در سکوت به تبدیل `12`.

**مقادیر بازگشتی**

-   عدد و الفبایی `String` مختصات کد گذاری شده (نسخه اصلاح شده از الفبای باس32 رمزگذاری استفاده شده است).

**مثال**

``` sql
SELECT geohashEncode(-5.60302734375, 42.593994140625, 0) AS res
```

``` text
┌─res──────────┐
│ ezs42d000000 │
└──────────────┘
```

## کد جغرافیایی {#geohashdecode}

هر رشته جغرافیایی کد گذاری به طول و عرض جغرافیایی را رمزگشایی می کند.

**مقادیر ورودی**

-   رشته کد گذاری-رشته جغرافیایی کد گذاری.

**مقادیر بازگشتی**

-   (طول جغرافیایی, عرض جغرافیایی) - 2-تاپل از `Float64` ارزش طول و عرض جغرافیایی.

**مثال**

``` sql
SELECT geohashDecode('ezs42') AS res
```

``` text
┌─res─────────────────────────────┐
│ (-5.60302734375,42.60498046875) │
└─────────────────────────────────┘
```

## جغرافیایی 3 {#geotoh3}

بازگشت [H3](https://uber.github.io/h3/#/documentation/overview/introduction) شاخص نقطه `(lon, lat)` با وضوح مشخص شده است.

[H3](https://uber.github.io/h3/#/documentation/overview/introduction) یک سیستم نمایه سازی جغرافیایی است که سطح زمین به کاشی های شش ضلعی حتی تقسیم شده است. این سیستم سلسله مراتبی است, به عنوان مثال. هر شش گوش در سطح بالا را می توان به هفت حتی اما کوچکتر و به همین ترتیب تقسیم.

این شاخص در درجه اول برای مکان های جفتک انداختن و دیگر دستکاری های جغرافیایی استفاده می شود.

**نحو**

``` sql
geoToH3(lon, lat, resolution)
```

**پارامترها**

-   `lon` — Longitude. Type: [جسم شناور64](../../sql-reference/data-types/float.md).
-   `lat` — Latitude. Type: [جسم شناور64](../../sql-reference/data-types/float.md).
-   `resolution` — Index resolution. Range: `[0, 15]`. نوع: [UInt8](../../sql-reference/data-types/int-uint.md).

**مقادیر بازگشتی**

-   عدد شاخص شش گوش.
-   0 در صورت خطا.

نوع: `UInt64`.

**مثال**

پرسوجو:

``` sql
SELECT geoToH3(37.79506683, 55.71290588, 15) as h3Index
```

نتیجه:

``` text
┌────────────h3Index─┐
│ 644325524701193974 │
└────────────────────┘
```

## جعبه جواهر {#geohashesinbox}

بازگرداندن مجموعه ای از رشته های جغرافیایی کد گذاری شده از دقت داده شده است که در داخل و تقاطع مرزهای جعبه داده شده قرار می گیرند, اساسا یک شبکه 2د مسطح به مجموعه.

**مقادیر ورودی**

-   طولی-دقیقه طول جغرافیایی, ارزش شناور در محدوده `[-180°, 180°]`
-   عرضی-دقیقه عرض جغرافیایی, ارزش شناور در محدوده `[-90°, 90°]`
-   طولی-حداکثر طول جغرافیایی, ارزش شناور در محدوده `[-180°, 180°]`
-   عرضی-حداکثر عرض جغرافیایی, ارزش شناور در محدوده `[-90°, 90°]`
-   دقت-جغرافیایی دقیق, `UInt8` در محدوده `[1, 12]`

لطفا توجه داشته باشید که تمام پارامترهای هماهنگ باید از همان نوع باشد: هم `Float32` یا `Float64`.

**مقادیر بازگشتی**

-   مجموعه ای از رشته های دقت طولانی از زمینهاش جعبه پوشش منطقه فراهم, شما باید به ترتیب از اقلام تکیه نمی.
-   \[\]- مجموعه خالی اگر *کمینه* ارزش *عرض جغرافیایی* و *طول جغرافیایی* کمتر از متناظر نیست *حداکثر* ارزشهای خبری عبارتند از:

لطفا توجه داشته باشید که عملکرد یک استثنا را پرتاب می کند اگر مجموعه ای بیش از 10'000'000 باشد.

**مثال**

``` sql
SELECT geohashesInBox(24.48, 40.56, 24.785, 40.81, 4) AS thasos
```

``` text
┌─thasos──────────────────────────────────────┐
│ ['sx1q','sx1r','sx32','sx1w','sx1x','sx38'] │
└─────────────────────────────────────────────┘
```

## هد3گتاسکل {#h3getbasecell}

بازگرداندن تعداد سلول پایه از شاخص.

**نحو**

``` sql
h3GetBaseCell(index)
```

**پارامترها**

-   `index` — Hexagon index number. Type: [UInt64](../../sql-reference/data-types/int-uint.md).

**مقادیر بازگشتی**

-   شش گوش شماره سلول پایه. نوع: [UInt8](../../sql-reference/data-types/int-uint.md).

**مثال**

پرسوجو:

``` sql
SELECT h3GetBaseCell(612916788725809151) as basecell
```

نتیجه:

``` text
┌─basecell─┐
│       12 │
└──────────┘
```

## ه3حکسرام2 {#h3hexaream2}

میانگین منطقه شش گوش در متر مربع در وضوح داده شده.

**نحو**

``` sql
h3HexAreaM2(resolution)
```

**پارامترها**

-   `resolution` — Index resolution. Range: `[0, 15]`. نوع: [UInt8](../../sql-reference/data-types/int-uint.md).

**مقادیر بازگشتی**

-   Area in m². Type: [جسم شناور64](../../sql-reference/data-types/float.md).

**مثال**

پرسوجو:

``` sql
SELECT h3HexAreaM2(13) as area
```

نتیجه:

``` text
┌─area─┐
│ 43.9 │
└──────┘
```

## در حال بارگذاری {#h3indexesareneighbors}

بازده یا نه فراهم هیندکس همسایه هستند.

**نحو**

``` sql
h3IndexesAreNeighbors(index1, index2)
```

**پارامترها**

-   `index1` — Hexagon index number. Type: [UInt64](../../sql-reference/data-types/int-uint.md).
-   `index2` — Hexagon index number. Type: [UInt64](../../sql-reference/data-types/int-uint.md).

**مقادیر بازگشتی**

-   بازگشت `1` اگر شاخص همسایه هستند, `0` وگرنه نوع: [UInt8](../../sql-reference/data-types/int-uint.md).

**مثال**

پرسوجو:

``` sql
SELECT h3IndexesAreNeighbors(617420388351344639, 617420388352655359) AS n
```

نتیجه:

``` text
┌─n─┐
│ 1 │
└───┘
```

## بچه گانه های 3 {#h3tochildren}

بازگرداندن مجموعه ای با شاخص کودک از شاخص داده.

**نحو**

``` sql
h3ToChildren(index, resolution)
```

**پارامترها**

-   `index` — Hexagon index number. Type: [UInt64](../../sql-reference/data-types/int-uint.md).
-   `resolution` — Index resolution. Range: `[0, 15]`. نوع: [UInt8](../../sql-reference/data-types/int-uint.md).

**مقادیر بازگشتی**

-   با شاخص های اچ 3 کودک تنظیم کنید. مجموعه ای از نوع: [UInt64](../../sql-reference/data-types/int-uint.md).

**مثال**

پرسوجو:

``` sql
SELECT h3ToChildren(599405990164561919, 6) AS children
```

نتیجه:

``` text
┌─children───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ [603909588852408319,603909588986626047,603909589120843775,603909589255061503,603909589389279231,603909589523496959,603909589657714687] │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## ساعت 3 {#h3toparent}

بازگرداندن پدر و مادر (درشت) شاخص حاوی شاخص داده.

**نحو**

``` sql
h3ToParent(index, resolution)
```

**پارامترها**

-   `index` — Hexagon index number. Type: [UInt64](../../sql-reference/data-types/int-uint.md).
-   `resolution` — Index resolution. Range: `[0, 15]`. نوع: [UInt8](../../sql-reference/data-types/int-uint.md).

**مقادیر بازگشتی**

-   شاخص اچ 3 پدر و مادر. نوع: [UInt64](../../sql-reference/data-types/int-uint.md).

**مثال**

پرسوجو:

``` sql
SELECT h3ToParent(599405990164561919, 3) as parent
```

نتیجه:

``` text
┌─────────────parent─┐
│ 590398848891879423 │
└────────────────────┘
```

## اچ 3 {#h3tostring}

تبدیل نمایندگی هیندکس از شاخص به نمایندگی رشته.

``` sql
h3ToString(index)
```

**پارامترها**

-   `index` — Hexagon index number. Type: [UInt64](../../sql-reference/data-types/int-uint.md).

**مقادیر بازگشتی**

-   نمایندگی رشته از شاخص اچ 3. نوع: [رشته](../../sql-reference/data-types/string.md).

**مثال**

پرسوجو:

``` sql
SELECT h3ToString(617420388352917503) as h3_string
```

نتیجه:

``` text
┌─h3_string───────┐
│ 89184926cdbffff │
└─────────────────┘
```

## استراینگتوه3 {#stringtoh3}

تبدیل رشته به نمایندگی H3Index (UInt64) نمایندگی.

``` sql
stringToH3(index_str)
```

**پارامترها**

-   `index_str` — String representation of the H3 index. Type: [رشته](../../sql-reference/data-types/string.md).

**مقادیر بازگشتی**

-   عدد شاخص شش گوش. بازده 0 در خطا. نوع: [UInt64](../../sql-reference/data-types/int-uint.md).

**مثال**

پرسوجو:

``` sql
SELECT stringToH3('89184926cc3ffff') as index
```

نتیجه:

``` text
┌──────────────index─┐
│ 617420388351344639 │
└────────────────────┘
```

## انتقال انرژی 3 {#h3getresolution}

بازگرداندن وضوح از شاخص.

**نحو**

``` sql
h3GetResolution(index)
```

**پارامترها**

-   `index` — Hexagon index number. Type: [UInt64](../../sql-reference/data-types/int-uint.md).

**مقادیر بازگشتی**

-   وضوح صفحه اول. گستره: `[0, 15]`. نوع: [UInt8](../../sql-reference/data-types/int-uint.md).

**مثال**

پرسوجو:

``` sql
SELECT h3GetResolution(617420388352917503) as res
```

نتیجه:

``` text
┌─res─┐
│   9 │
└─────┘
```

[مقاله اصلی](https://clickhouse.tech/docs/en/query_language/functions/geo/) <!--hide-->
