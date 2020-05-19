---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 55
toc_title: "IP\u30A2\u30C9\u30EC\u30B9\u306E\u64CD\u4F5C"
---

# IPアドレスを操作するための関数 {#functions-for-working-with-ip-addresses}

## IPv4NumToString(num) {#ipv4numtostringnum}

UInt32番号を取ります。 ビッグエンディアンのIPv4アドレスとして解釈します。 対応するIPv4アドレスを含む文字列を、a.B.C.d(小数点以下のドット区切りの数値)の形式で返します。

## IPv4StringToNum(s) {#ipv4stringtonums}

IPv4NumToStringの逆関数。 IPv4アドレスの形式が無効な場合は、0を返します。

## IPv4NumToStringClassC(num) {#ipv4numtostringclasscnum}

IPv4NumToStringに似ていますが、最後のオクテットの代わりにxxxを使用します。

例:

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

を使用して以来 ‘xxx’ 非常に珍しいですが、これは将来的に変更される可能性があります。 このフラグメントの正確な形式に依存しないことをお勧めします。

### IPv6NumToString(x) {#ipv6numtostringx}

バイナリ形式のIPv6アドレスを含むFixedString(16)値を受け入れます。 このアドレスを含む文字列をテキスト形式で返します。
IPv6マップされたIPv4アドレスは、::ffff:111.222.33.44の形式で出力されます。 例:

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

## IPv6StringToNum(s) {#ipv6stringtonums}

IPv6NumToStringの逆関数。 IPv6アドレスの形式が無効な場合は、nullバイトの文字列を返します。
HEXは大文字または小文字です。

## IPv4ToIPv6(x) {#ipv4toipv6x}

を取る `UInt32` 番号 これをIPv4アドレスとして解釈します。 [ビッグエンディアン](https://en.wikipedia.org/wiki/Endianness). Aを返します `FixedString(16)` IPv6アドレスをバイナリ形式で含む値。 例:

``` sql
SELECT IPv6NumToString(IPv4ToIPv6(IPv4StringToNum('192.168.0.1'))) AS addr
```

``` text
┌─addr───────────────┐
│ ::ffff:192.168.0.1 │
└────────────────────┘
```

## cutIPv6(x,bytesToCutForIPv6,bytesToCutForIPv4) {#cutipv6x-bytestocutforipv6-bytestocutforipv4}

バイナリ形式のIPv6アドレスを含むFixedString(16)値を受け入れます。 テキスト形式で削除された指定されたバイト数のアドレスを含む文字列を返します。 例えば:

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

## IPv4CIDRToRange(ipv4,Cidr), {#ipv4cidrtorangeipv4-cidr}

IPv4およびuint8の値を受け入れます。 [CIDR](https://en.wikipedia.org/wiki/Classless_Inter-Domain_Routing). サブネットの低い範囲と高い範囲を含むIPv4のタプルを返します。

``` sql
SELECT IPv4CIDRToRange(toIPv4('192.168.5.2'), 16)
```

``` text
┌─IPv4CIDRToRange(toIPv4('192.168.5.2'), 16)─┐
│ ('192.168.0.0','192.168.255.255')          │
└────────────────────────────────────────────┘
```

## IPv6CIDRToRange(ipv6,Cidr), {#ipv6cidrtorangeipv6-cidr}

IPv6およびCidrを含むUInt8値を受け入れます。 サブネットの下位範囲と上位の範囲を含むIPv6のタプルを返します。

``` sql
SELECT IPv6CIDRToRange(toIPv6('2001:0db8:0000:85a3:0000:0000:ac1f:8001'), 32);
```

``` text
┌─IPv6CIDRToRange(toIPv6('2001:0db8:0000:85a3:0000:0000:ac1f:8001'), 32)─┐
│ ('2001:db8::','2001:db8:ffff:ffff:ffff:ffff:ffff:ffff')                │
└────────────────────────────────────────────────────────────────────────┘
```

## toIPv4(文字列) {#toipv4string}

エイリアス `IPv4StringToNum()` これは、IPv4アドレスの文字列形式をとり、の値を返します [IPv4](../../sql-reference/data-types/domains/ipv4.md) 返される値と等しいバイナリ型 `IPv4StringToNum()`.

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

## toIPv6(文字列) {#toipv6string}

エイリアス `IPv6StringToNum()` これは、IPv6アドレスの文字列形式を取り、の値を返します [IPv6](../../sql-reference/data-types/domains/ipv6.md) 返される値と等しいバイナリ型 `IPv6StringToNum()`.

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

[元の記事](https://clickhouse.tech/docs/en/query_language/functions/ip_address_functions/) <!--hide-->
