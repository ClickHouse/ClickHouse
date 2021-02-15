---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 59
toc_title: "\u06A9\u0627\u0631 \u0628\u0627 \u06CC\u0627\u0646\u062F\u06A9\u0633.\u0648\
  \u0627\u0698\u0647\u0646\u0627\u0645\u0647\u0647\u0627 \u0645\u062A\u0631\u06CC\u06A9\
  \u0627"
---

# توابع برای کار با یاندکس.واژهنامهها متریکا {#functions-for-working-with-yandex-metrica-dictionaries}

به منظور توابع زیر به کار پیکربندی سرور باید مشخص مسیر و نشانی برای گرفتن تمام یاندکس.واژهنامهها متریکا. لغت نامه ها در اولین تماس از هر یک از این توابع لود می شود. اگر لیست مرجع نمی تواند لود شود, یک استثنا پرتاب می شود.

برای اطلاعات در مورد ایجاد لیست مرجع, بخش را ببینید “Dictionaries”.

## موقعیت جغرافیایی چندگانه {#multiple-geobases}

ClickHouse پشتیبانی از کار با چند جایگزین geobases (منطقه ای سلسله مراتب) به طور همزمان به منظور حمایت از دیدگاه های مختلف که در آن کشورهای خاص مناطق متعلق به.

این ‘clickhouse-server’ پیکربندی فایل را با سلسله مراتب منطقه ای مشخص می کند::`<path_to_regions_hierarchy_file>/opt/geo/regions_hierarchy.txt</path_to_regions_hierarchy_file>`

علاوه بر این فایل, همچنین برای فایل های جستجو در این نزدیکی هست که نماد \_ و هر پسوند اضافه به نام (قبل از پسوند فایل).
مثلا, همچنین فایل را پیدا خواهد کرد `/opt/geo/regions_hierarchy_ua.txt`, اگر در حال حاضر.

`ua` کلید فرهنگ لغت نامیده می شود. برای یک فرهنگ لغت بدون پسوند, کلید یک رشته خالی است.

تمام واژهنامهها دوباره لود شده در زمان اجرا (یک بار در هر تعداد معینی از ثانیه, همانطور که در دستور داخلی تعریف شده \_فرهنگ\_ پارامتر پیکربندی, و یا یک بار در ساعت به طور پیش فرض). با این حال, لیست لغت نامه های موجود تعریف شده است یک بار, زمانی که سرور شروع می شود.

All functions for working with regions have an optional argument at the end – the dictionary key. It is referred to as the geobase.
مثال:

``` sql
regionToCountry(RegionID) – Uses the default dictionary: /opt/geo/regions_hierarchy.txt
regionToCountry(RegionID, '') – Uses the default dictionary: /opt/geo/regions_hierarchy.txt
regionToCountry(RegionID, 'ua') – Uses the dictionary for the 'ua' key: /opt/geo/regions_hierarchy_ua.txt
```

### regionToCity(id\[, geobase\]) {#regiontocityid-geobase}

Accepts a UInt32 number – the region ID from the Yandex geobase. If this region is a city or part of a city, it returns the region ID for the appropriate city. Otherwise, returns 0.

### regionToArea(id\[, geobase\]) {#regiontoareaid-geobase}

تبدیل یک منطقه به یک منطقه (نوع 5 در پایگاه داده). در هر راه دیگر, این تابع همان است که ‘regionToCity’.

``` sql
SELECT DISTINCT regionToName(regionToArea(toUInt32(number), 'ua'))
FROM system.numbers
LIMIT 15
```

``` text
┌─regionToName(regionToArea(toUInt32(number), \'ua\'))─┐
│                                                      │
│ Moscow and Moscow region                             │
│ St. Petersburg and Leningrad region                  │
│ Belgorod region                                      │
│ Ivanovsk region                                      │
│ Kaluga region                                        │
│ Kostroma region                                      │
│ Kursk region                                         │
│ Lipetsk region                                       │
│ Orlov region                                         │
│ Ryazan region                                        │
│ Smolensk region                                      │
│ Tambov region                                        │
│ Tver region                                          │
│ Tula region                                          │
└──────────────────────────────────────────────────────┘
```

### regionToDistrict(id\[, geobase\]) {#regiontodistrictid-geobase}

تبدیل یک منطقه به یک منطقه فدرال (نوع 4 در پایگاه داده). در هر راه دیگر, این تابع همان است که ‘regionToCity’.

``` sql
SELECT DISTINCT regionToName(regionToDistrict(toUInt32(number), 'ua'))
FROM system.numbers
LIMIT 15
```

``` text
┌─regionToName(regionToDistrict(toUInt32(number), \'ua\'))─┐
│                                                          │
│ Central federal district                                 │
│ Northwest federal district                               │
│ South federal district                                   │
│ North Caucases federal district                          │
│ Privolga federal district                                │
│ Ural federal district                                    │
│ Siberian federal district                                │
│ Far East federal district                                │
│ Scotland                                                 │
│ Faroe Islands                                            │
│ Flemish region                                           │
│ Brussels capital region                                  │
│ Wallonia                                                 │
│ Federation of Bosnia and Herzegovina                     │
└──────────────────────────────────────────────────────────┘
```

### regionToCountry(id\[, geobase\]) {#regiontocountryid-geobase}

تبدیل یک منطقه به یک کشور. در هر راه دیگر, این تابع همان است که ‘regionToCity’.
مثال: `regionToCountry(toUInt32(213)) = 225` تبدیل مسکو (213) به روسیه (225).

### regionToContinent(id\[, geobase\]) {#regiontocontinentid-geobase}

تبدیل یک منطقه به یک قاره. در هر راه دیگر, این تابع همان است که ‘regionToCity’.
مثال: `regionToContinent(toUInt32(213)) = 10001` تبدیل مسکو (213) به اوراسیا (10001).

### نقلقولهای جدید از این نویسنده) {#regiontotopcontinent-regiontotopcontinent}

بالاترین قاره را در سلسله مراتب منطقه پیدا می کند.

**نحو**

``` sql
regionToTopContinent(id[, geobase]);
```

**پارامترها**

-   `id` — Region ID from the Yandex geobase. [UInt32](../../sql-reference/data-types/int-uint.md).
-   `geobase` — Dictionary key. See [موقعیت جغرافیایی چندگانه](#multiple-geobases). [رشته](../../sql-reference/data-types/string.md). اختیاری.

**مقدار بازگشتی**

-   شناسه قاره سطح بالا (دومی زمانی که شما صعود سلسله مراتب مناطق).
-   0, اگر هیچ کدام وجود دارد.

نوع: `UInt32`.

### regionToPopulation(id\[, geobase\]) {#regiontopopulationid-geobase}

می شود جمعیت برای یک منطقه.
جمعیت را می توان در فایل های با پایگاه داده ثبت شده است. بخش را ببینید “External dictionaries”.
اگر جمعیت برای منطقه ثبت نشده, باز می گردد 0.
در یاندکس پایگاه جغرافیایی, جمعیت ممکن است برای مناطق کودک ثبت, اما نه برای مناطق پدر و مادر.

### regionIn(lhs, rhs\[, geobase\]) {#regioninlhs-rhs-geobase}

بررسی اینکه یک ‘lhs’ منطقه متعلق به ‘rhs’ منطقه. بازگرداندن یک عدد 18 برابر 1 اگر متعلق, یا 0 اگر تعلق ندارد.
The relationship is reflexive – any region also belongs to itself.

### regionHierarchy(id\[, geobase\]) {#regionhierarchyid-geobase}

Accepts a UInt32 number – the region ID from the Yandex geobase. Returns an array of region IDs consisting of the passed region and all parents along the chain.
مثال: `regionHierarchy(toUInt32(213)) = [213,1,3,225,10001,10000]`.

### شناسه بسته:\]) {#regiontonameid-lang}

Accepts a UInt32 number – the region ID from the Yandex geobase. A string with the name of the language can be passed as a second argument. Supported languages are: ru, en, ua, uk, by, kz, tr. If the second argument is omitted, the language ‘ru’ is used. If the language is not supported, an exception is thrown. Returns a string – the name of the region in the corresponding language. If the region with the specified ID doesn't exist, an empty string is returned.

`ua` و `uk` هر دو به معنای اوکراین.

[مقاله اصلی](https://clickhouse.tech/docs/en/query_language/functions/ym_dict_functions/) <!--hide-->
