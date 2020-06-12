---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 55
toc_title: "IP adresleriyle \xE7al\u0131\u015Fma"
---

# IP adresleriyle çalışmak için işlevler {#functions-for-working-with-ip-addresses}

## Ipv4numtostring (num) {#ipv4numtostringnum}

Bir Uınt32 numarası alır. Big endian'da bir IPv4 adresi olarak yorumlar. Karşılık gelen IPv4 adresini a. B. C. d biçiminde içeren bir dize döndürür (ondalık formda nokta ile ayrılmış sayılar).

## Ipv4stringtonum (s) {#ipv4stringtonums}

IPv4NumToString ters işlevi. IPv4 adresi geçersiz bir biçime sahipse, 0 döndürür.

## Ipv4numtostringclassc (num) {#ipv4numtostringclasscnum}

Ipv4numtostring'e benzer, ancak son sekizli yerine xxx kullanıyor.

Örnek:

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

Kullanıl sincedığından beri ‘xxx’ son derece sıradışı, bu gelecekte değiştirilebilir. Bu parçanın tam biçimine güvenmemenizi öneririz.

### Ipv6numtostring (x) {#ipv6numtostringx}

IPv6 adresini ikili biçimde içeren bir FixedString(16) değerini kabul eder. Bu adresi metin biçiminde içeren bir dize döndürür.
IPv6 eşlemeli IPv4 adresleri ::ffff:111.222.33.44 biçiminde çıktıdır. Örnekler:

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

## Ipv6stringtonum (s) {#ipv6stringtonums}

IPv6NumToString ters işlevi. IPv6 adresi geçersiz bir biçime sahipse, bir boş bayt dizesi döndürür.
HEX büyük veya küçük harf olabilir.

## Ipv4toıpv6 (x) {#ipv4toipv6x}

Alır bir `UInt32` numara. Bir IPv4 adresi olarak yorumlar [büyük endian](https://en.wikipedia.org/wiki/Endianness). Ret aur ANS a `FixedString(16)` IPv6 adresini ikili biçimde içeren değer. Örnekler:

``` sql
SELECT IPv6NumToString(IPv4ToIPv6(IPv4StringToNum('192.168.0.1'))) AS addr
```

``` text
┌─addr───────────────┐
│ ::ffff:192.168.0.1 │
└────────────────────┘
```

## cutİPv6 (x, bytesToCutForİPv6, bytesToCutForİPv4) {#cutipv6x-bytestocutforipv6-bytestocutforipv4}

IPv6 adresini ikili biçimde içeren bir FixedString(16) değerini kabul eder. Metin biçiminde kaldırılan belirtilen bayt sayısının adresini içeren bir dize döndürür. Mesela:

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

## Ipv4cidrtorange(ıpv4, Cıdr), {#ipv4cidrtorangeipv4-cidr}

İçeren bir IPv4 ve bir Uint8 değerini kabul eder [CIDR](https://en.wikipedia.org/wiki/Classless_Inter-Domain_Routing). Alt ağın alt aralığını ve daha yüksek aralığını içeren iki IPv4 içeren bir tuple döndürür.

``` sql
SELECT IPv4CIDRToRange(toIPv4('192.168.5.2'), 16)
```

``` text
┌─IPv4CIDRToRange(toIPv4('192.168.5.2'), 16)─┐
│ ('192.168.0.0','192.168.255.255')          │
└────────────────────────────────────────────┘
```

## Ipv6cidrtorange(ıpv6, Cıdr), {#ipv6cidrtorangeipv6-cidr}

CIDR'Yİ içeren bir IPv6 ve bir Uİnt8 değerini kabul eder. Alt ağın alt aralığını ve daha yüksek aralığını içeren iki IPv6 içeren bir tuple döndürür.

``` sql
SELECT IPv6CIDRToRange(toIPv6('2001:0db8:0000:85a3:0000:0000:ac1f:8001'), 32);
```

``` text
┌─IPv6CIDRToRange(toIPv6('2001:0db8:0000:85a3:0000:0000:ac1f:8001'), 32)─┐
│ ('2001:db8::','2001:db8:ffff:ffff:ffff:ffff:ffff:ffff')                │
└────────────────────────────────────────────────────────────────────────┘
```

## toıpv4 (dize) {#toipv4string}

İçin bir takma ad `IPv4StringToNum()` bu, IPv4 adresinin bir dize formunu alır ve değerini döndürür [Ipv44](../../sql-reference/data-types/domains/ipv4.md) tarafından döndürülen değere eşit ikili olan tür `IPv4StringToNum()`.

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

## toıpv6 (dize) {#toipv6string}

İçin bir takma ad `IPv6StringToNum()` bu, IPv6 adresinin bir dize formunu alır ve değerini döndürür [IPv6](../../sql-reference/data-types/domains/ipv6.md) tarafından döndürülen değere eşit ikili olan tür `IPv6StringToNum()`.

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

[Orijinal makale](https://clickhouse.tech/docs/en/query_language/functions/ip_address_functions/) <!--hide-->
