# IP函数 {#iphan-shu}

## IPv4NumToString(num) {#ipv4numtostringnum}

接受一个UInt32（大端）表示的IPv4的地址，返回相应IPv4的字符串表现形式，格式为A.B.C.D（以点分割的十进制数字）。

## IPv4StringToNum(s) {#ipv4stringtonums}

与IPv4NumToString函数相反。如果IPv4地址格式无效，则返回0。

## IPv4NumToStringClassC(num) {#ipv4numtostringclasscnum}

与IPv4NumToString类似，但使用xxx替换最后一个字节。

示例:

``` sql
SELECT
    IPv4NumToStringClassC(ClientIP) AS k,
    count() AS c
FROM test.hits
GROUP BY k
ORDER BY c DESC
LIMIT 10
```

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

由于使用’xxx’是不规范的，因此将来可能会更改。我们建议您不要依赖此格式。

### IPv6NumToString(x) {#ipv6numtostringx}

接受FixedString(16)类型的二进制格式的IPv6地址。以文本格式返回此地址的字符串。
IPv6映射的IPv4地址以::ffff:111.222.33。例如：

``` sql
SELECT IPv6NumToString(toFixedString(unhex('2A0206B8000000000000000000000011'), 16)) AS addr
```

    ┌─addr─────────┐
    │ 2a02:6b8::11 │
    └──────────────┘

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

## IPv6StringToNum(s) {#ipv6stringtonums}

与IPv6NumToString的相反。如果IPv6地址格式无效，则返回空字节字符串。
十六进制可以是大写的或小写的。

## IPv4ToIPv6(x) {#ipv4toipv6x}

接受一个UInt32类型的IPv4地址，返回FixedString(16)类型的IPv6地址。例如：

``` sql
SELECT IPv6NumToString(IPv4ToIPv6(IPv4StringToNum('192.168.0.1'))) AS addr
```

    ┌─addr───────────────┐
    │ ::ffff:192.168.0.1 │
    └────────────────────┘

## cutIPv6(x,bitsToCutForIPv6,bitsToCutForIPv4) {#cutipv6x-bitstocutforipv6-bitstocutforipv4}

接受一个FixedString(16)类型的IPv6地址，返回一个String，这个String中包含了删除指定位之后的地址的文本格式。例如：

``` sql
WITH
    IPv6StringToNum('2001:0DB8:AC10:FE01:FEED:BABE:CAFE:F00D') AS ipv6,
    IPv4ToIPv6(IPv4StringToNum('192.168.0.1')) AS ipv4
SELECT
    cutIPv6(ipv6, 2, 0),
    cutIPv6(ipv4, 0, 2)
```

    ┌─cutIPv6(ipv6, 2, 0)─────────────────┬─cutIPv6(ipv4, 0, 2)─┐
    │ 2001:db8:ac10:fe01:feed:babe:cafe:0 │ ::ffff:192.168.0.0  │
    └─────────────────────────────────────┴─────────────────────┘

## ﾂ古ｶﾂ益ﾂ催ﾂ団ﾂ法ﾂ人), {#ipv4cidrtorangeipv4-cidr}

接受一个IPv4地址以及一个UInt8类型的CIDR。返回包含子网最低范围以及最高范围的元组。

``` sql
SELECT IPv4CIDRToRange(toIPv4('192.168.5.2'), 16)
```

    ┌─IPv4CIDRToRange(toIPv4('192.168.5.2'), 16)─┐
    │ ('192.168.0.0','192.168.255.255')          │
    └────────────────────────────────────────────┘

## ﾂ暗ｪﾂ氾环催ﾂ団ﾂ法ﾂ人), {#ipv6cidrtorangeipv6-cidr}

接受一个IPv6地址以及一个UInt8类型的CIDR。返回包含子网最低范围以及最高范围的元组。

``` sql
SELECT IPv6CIDRToRange(toIPv6('2001:0db8:0000:85a3:0000:0000:ac1f:8001'), 32);
```

    ┌─IPv6CIDRToRange(toIPv6('2001:0db8:0000:85a3:0000:0000:ac1f:8001'), 32)─┐
    │ ('2001:db8::','2001:db8:ffff:ffff:ffff:ffff:ffff:ffff')                │
    └────────────────────────────────────────────────────────────────────────┘

## toIPv4(字符串) {#toipv4string}

`IPv4StringToNum()`的别名，它采用字符串形式的IPv4地址并返回[IPv4](../../sql-reference/functions/ip-address-functions.md)类型的值，该二进制值等于`IPv4StringToNum()`返回的值。

``` sql
WITH
    '171.225.130.45' as IPv4_string
SELECT
    toTypeName(IPv4StringToNum(IPv4_string)),
    toTypeName(toIPv4(IPv4_string))
```

    ┌─toTypeName(IPv4StringToNum(IPv4_string))─┬─toTypeName(toIPv4(IPv4_string))─┐
    │ UInt32                                   │ IPv4                            │
    └──────────────────────────────────────────┴─────────────────────────────────┘

``` sql
WITH
    '171.225.130.45' as IPv4_string
SELECT
    hex(IPv4StringToNum(IPv4_string)),
    hex(toIPv4(IPv4_string))
```

    ┌─hex(IPv4StringToNum(IPv4_string))─┬─hex(toIPv4(IPv4_string))─┐
    │ ABE1822D                          │ ABE1822D                 │
    └───────────────────────────────────┴──────────────────────────┘

## toIPv6(字符串) {#toipv6string}

`IPv6StringToNum()`的别名，它采用字符串形式的IPv6地址并返回[IPv6](../../sql-reference/functions/ip-address-functions.md)类型的值，该二进制值等于`IPv6StringToNum()`返回的值。

``` sql
WITH
    '2001:438:ffff::407d:1bc1' as IPv6_string
SELECT
    toTypeName(IPv6StringToNum(IPv6_string)),
    toTypeName(toIPv6(IPv6_string))
```

    ┌─toTypeName(IPv6StringToNum(IPv6_string))─┬─toTypeName(toIPv6(IPv6_string))─┐
    │ FixedString(16)                          │ IPv6                            │
    └──────────────────────────────────────────┴─────────────────────────────────┘

``` sql
WITH
    '2001:438:ffff::407d:1bc1' as IPv6_string
SELECT
    hex(IPv6StringToNum(IPv6_string)),
    hex(toIPv6(IPv6_string))
```

    ┌─hex(IPv6StringToNum(IPv6_string))─┬─hex(toIPv6(IPv6_string))─────────┐
    │ 20010438FFFF000000000000407D1BC1  │ 20010438FFFF000000000000407D1BC1 │
    └───────────────────────────────────┴──────────────────────────────────┘

[来源文章](https://clickhouse.com/docs/en/query_language/functions/ip_address_functions/) <!--hide-->
