---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 55
toc_title: "\u06A9\u0627\u0631 \u0628\u0627 \u0646\u0634\u0627\u0646\u06CC\u0647\u0627\
  \u06CC \u0627\u06CC\u0646\u062A\u0631\u0646\u062A\u06CC"
---

# توابع برای کار با نشانی های اینترنتی {#functions-for-working-with-ip-addresses}

## اطلاعات دقیق) {#ipv4numtostringnum}

طول می کشد یک UInt32 شماره. به عنوان یک نشانی اینترنتی 4 در اندی بزرگ تفسیر می کند. بازده یک رشته حاوی مربوطه آدرس IPv4 در قالب A. B. C. d (نقطه جدا کردن اعداد در شکل اعشاری).

## مدت 4 ساعت) {#ipv4stringtonums}

عملکرد معکوس ایپو4نومتوسترینگ. اگر نشانی اینترنتی4 دارای یک فرمت نامعتبر, باز می گردد 0.

## اطلاعات دقیق) {#ipv4numtostringclasscnum}

شبیه به IPv4NumToString اما با استفاده از \<url\> به جای گذشته هشت تایی.

مثال:

``` sql
SELECT
    IPv4NumToStringClassC(ClientIP) AS k,
    count() AS c
FROM test.hits
GROUP BY k
ORDER BY c DESC
LIMIT 10
```

``` text
┌─k──────────────┬─────c─┐
│ 83.149.9.xxx   │ 26238 │
│ 217.118.81.xxx │ 26074 │
│ 213.87.129.xxx │ 25481 │
│ 83.149.8.xxx   │ 24984 │
│ 217.118.83.xxx │ 22797 │
│ 78.25.120.xxx  │ 22354 │
│ 213.87.131.xxx │ 21285 │
│ 78.25.121.xxx  │ 20887 │
│ 188.162.65.xxx │ 19694 │
│ 83.149.48.xxx  │ 17406 │
└────────────────┴───────┘
```

از زمان استفاده ‘xxx’ بسیار غیر معمول است, این ممکن است در اینده تغییر. ما توصیه می کنیم که شما در قالب دقیق این قطعه تکیه نمی.

### اطلاعات دقیق) {#ipv6numtostringx}

یک رشته ثابت(16) مقدار حاوی نشانی اینترنتی6 را در قالب باینری می پذیرد. بازگرداندن یک رشته حاوی این نشانی در قالب متن.
نشانی های ایپو6-نقشه برداری ایپو4 خروجی در قالب هستند:: افف:111.222.33.44. مثالها:

``` sql
SELECT IPv6NumToString(toFixedString(unhex('2A0206B8000000000000000000000011'), 16)) AS addr
```

``` text
┌─addr─────────┐
│ 2a02:6b8::11 │
└──────────────┘
```

``` sql
SELECT
    IPv6NumToString(ClientIP6 AS k),
    count() AS c
FROM hits_all
WHERE EventDate = today() AND substring(ClientIP6, 1, 12) != unhex('00000000000000000000FFFF')
GROUP BY k
ORDER BY c DESC
LIMIT 10
```

``` text
┌─IPv6NumToString(ClientIP6)──────────────┬─────c─┐
│ 2a02:2168:aaa:bbbb::2                   │ 24695 │
│ 2a02:2698:abcd:abcd:abcd:abcd:8888:5555 │ 22408 │
│ 2a02:6b8:0:fff::ff                      │ 16389 │
│ 2a01:4f8:111:6666::2                    │ 16016 │
│ 2a02:2168:888:222::1                    │ 15896 │
│ 2a01:7e00::ffff:ffff:ffff:222           │ 14774 │
│ 2a02:8109:eee:ee:eeee:eeee:eeee:eeee    │ 14443 │
│ 2a02:810b:8888:888:8888:8888:8888:8888  │ 14345 │
│ 2a02:6b8:0:444:4444:4444:4444:4444      │ 14279 │
│ 2a01:7e00::ffff:ffff:ffff:ffff          │ 13880 │
└─────────────────────────────────────────┴───────┘
```

``` sql
SELECT
    IPv6NumToString(ClientIP6 AS k),
    count() AS c
FROM hits_all
WHERE EventDate = today()
GROUP BY k
ORDER BY c DESC
LIMIT 10
```

``` text
┌─IPv6NumToString(ClientIP6)─┬──────c─┐
│ ::ffff:94.26.111.111       │ 747440 │
│ ::ffff:37.143.222.4        │ 529483 │
│ ::ffff:5.166.111.99        │ 317707 │
│ ::ffff:46.38.11.77         │ 263086 │
│ ::ffff:79.105.111.111      │ 186611 │
│ ::ffff:93.92.111.88        │ 176773 │
│ ::ffff:84.53.111.33        │ 158709 │
│ ::ffff:217.118.11.22       │ 154004 │
│ ::ffff:217.118.11.33       │ 148449 │
│ ::ffff:217.118.11.44       │ 148243 │
└────────────────────────────┴────────┘
```

## مدت 6 ساعت) {#ipv6stringtonums}

عملکرد معکوس ایپو6نومتوسترینگ. اگر نشانی اینترنتی6 دارای یک فرمت نامعتبر, یک رشته از بایت پوچ را برمی گرداند.
سحر و جادو می تواند بزرگ یا کوچک.

## IPv4ToIPv6(x) {#ipv4toipv6x}

طول می کشد یک `UInt32` شماره. تفسیر به عنوان یک نشانی اینترنتی4 در [اندی بزرگ](https://en.wikipedia.org/wiki/Endianness). بازگرداندن یک `FixedString(16)` مقدار حاوی نشانی اینترنتی6 در قالب دودویی. مثالها:

``` sql
SELECT IPv6NumToString(IPv4ToIPv6(IPv4StringToNum('192.168.0.1'))) AS addr
```

``` text
┌─addr───────────────┐
│ ::ffff:192.168.0.1 │
└────────────────────┘
```

## cutIPv6(x bytesToCutForIPv6, bytesToCutForIPv4) {#cutipv6x-bytestocutforipv6-bytestocutforipv4}

یک رشته ثابت(16) مقدار حاوی نشانی اینترنتی6 را در قالب باینری می پذیرد. بازگرداندن یک رشته حاوی نشانی از تعداد مشخصی از بایت حذف شده در قالب متن. به عنوان مثال:

``` sql
WITH
    IPv6StringToNum('2001:0DB8:AC10:FE01:FEED:BABE:CAFE:F00D') AS ipv6,
    IPv4ToIPv6(IPv4StringToNum('192.168.0.1')) AS ipv4
SELECT
    cutIPv6(ipv6, 2, 0),
    cutIPv6(ipv4, 0, 2)
```

``` text
┌─cutIPv6(ipv6, 2, 0)─────────────────┬─cutIPv6(ipv4, 0, 2)─┐
│ 2001:db8:ac10:fe01:feed:babe:cafe:0 │ ::ffff:192.168.0.0  │
└─────────────────────────────────────┴─────────────────────┘
```

## IPv4CIDRToRange(ipv4, Cidr), {#ipv4cidrtorangeipv4-cidr}

قبول یک IPv4 و UInt8 ارزش شامل [CIDR](https://en.wikipedia.org/wiki/Classless_Inter-Domain_Routing). یک تاپل را با دو لیگ4 حاوی محدوده پایین تر و محدوده بالاتر زیر شبکه باز کنید.

``` sql
SELECT IPv4CIDRToRange(toIPv4('192.168.5.2'), 16)
```

``` text
┌─IPv4CIDRToRange(toIPv4('192.168.5.2'), 16)─┐
│ ('192.168.0.0','192.168.255.255')          │
└────────────────────────────────────────────┘
```

## IPv6CIDRToRange(ipv6 Cidr), {#ipv6cidrtorangeipv6-cidr}

قبول یک IPv6 و UInt8 ارزش حاوی CIDR. یک تاپل را با دو ایپو6 حاوی محدوده پایین تر و محدوده بالاتر زیر شبکه باز کنید.

``` sql
SELECT IPv6CIDRToRange(toIPv6('2001:0db8:0000:85a3:0000:0000:ac1f:8001'), 32);
```

``` text
┌─IPv6CIDRToRange(toIPv6('2001:0db8:0000:85a3:0000:0000:ac1f:8001'), 32)─┐
│ ('2001:db8::','2001:db8:ffff:ffff:ffff:ffff:ffff:ffff')                │
└────────────────────────────────────────────────────────────────────────┘
```

## تایپه 4 (رشته) {#toipv4string}

یک نام مستعار برای `IPv4StringToNum()` که طول می کشد یک شکل رشته ای از ایپو4 نشانی و ارزش را برمی گرداند [IPv4](../../sql-reference/data-types/domains/ipv4.md) نوع باینری برابر با مقدار بازگشتی است `IPv4StringToNum()`.

``` sql
WITH
    '171.225.130.45' as IPv4_string
SELECT
    toTypeName(IPv4StringToNum(IPv4_string)),
    toTypeName(toIPv4(IPv4_string))
```

``` text
┌─toTypeName(IPv4StringToNum(IPv4_string))─┬─toTypeName(toIPv4(IPv4_string))─┐
│ UInt32                                   │ IPv4                            │
└──────────────────────────────────────────┴─────────────────────────────────┘
```

``` sql
WITH
    '171.225.130.45' as IPv4_string
SELECT
    hex(IPv4StringToNum(IPv4_string)),
    hex(toIPv4(IPv4_string))
```

``` text
┌─hex(IPv4StringToNum(IPv4_string))─┬─hex(toIPv4(IPv4_string))─┐
│ ABE1822D                          │ ABE1822D                 │
└───────────────────────────────────┴──────────────────────────┘
```

## تیپو6 (رشته) {#toipv6string}

یک نام مستعار برای `IPv6StringToNum()` که طول می کشد یک شکل رشته ای از ایپو6 نشانی و ارزش را برمی گرداند [IPv6](../../sql-reference/data-types/domains/ipv6.md) نوع باینری برابر با مقدار بازگشتی است `IPv6StringToNum()`.

``` sql
WITH
    '2001:438:ffff::407d:1bc1' as IPv6_string
SELECT
    toTypeName(IPv6StringToNum(IPv6_string)),
    toTypeName(toIPv6(IPv6_string))
```

``` text
┌─toTypeName(IPv6StringToNum(IPv6_string))─┬─toTypeName(toIPv6(IPv6_string))─┐
│ FixedString(16)                          │ IPv6                            │
└──────────────────────────────────────────┴─────────────────────────────────┘
```

``` sql
WITH
    '2001:438:ffff::407d:1bc1' as IPv6_string
SELECT
    hex(IPv6StringToNum(IPv6_string)),
    hex(toIPv6(IPv6_string))
```

``` text
┌─hex(IPv6StringToNum(IPv6_string))─┬─hex(toIPv6(IPv6_string))─────────┐
│ 20010438FFFF000000000000407D1BC1  │ 20010438FFFF000000000000407D1BC1 │
└───────────────────────────────────┴──────────────────────────────────┘
```

[مقاله اصلی](https://clickhouse.tech/docs/en/query_language/functions/ip_address_functions/) <!--hide-->
