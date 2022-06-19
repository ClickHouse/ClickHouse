---
sidebar_position: 55
sidebar_label: IP Addresses
---

# Functions for Working with IPv4 and IPv6 Addresses

## IPv4NumToString(num)

Takes a UInt32 number. Interprets it as an IPv4 address in big endian. Returns a string containing the corresponding IPv4 address in the format A.B.C.d (dot-separated numbers in decimal form).

Alias: `INET_NTOA`.

## IPv4StringToNum(s)

The reverse function of IPv4NumToString. If the IPv4 address has an invalid format, it throws exception.

Alias: `INET_ATON`.

## IPv4StringToNumOrDefault(s)

Same as `IPv4StringToNum`, but if the IPv4 address has an invalid format, it returns 0.

## IPv4StringToNumOrNull(s)

Same as `IPv4StringToNum`, but if the IPv4 address has an invalid format, it returns null.

## IPv4NumToStringClassC(num)

Similar to IPv4NumToString, but using xxx instead of the last octet.

Example:

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

Since using ‘xxx’ is highly unusual, this may be changed in the future. We recommend that you do not rely on the exact format of this fragment.

### IPv6NumToString(x)

Accepts a FixedString(16) value containing the IPv6 address in binary format. Returns a string containing this address in text format.
IPv6-mapped IPv4 addresses are output in the format ::ffff:111.222.33.44.

Alias: `INET6_NTOA`.

Examples:

``` sql
SELECT IPv6NumToString(toFixedString(unhex('2A0206B8000000000000000000000011'), 16)) AS addr;
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

## IPv6StringToNum

The reverse function of [IPv6NumToString](#ipv6numtostringx). If the IPv6 address has an invalid format, it throws exception.

If the input string contains a valid IPv4 address, returns its IPv6 equivalent.
HEX can be uppercase or lowercase.

Alias: `INET6_ATON`.

**Syntax**

``` sql
IPv6StringToNum(string)
```

**Argument**

-   `string` — IP address. [String](../../sql-reference/data-types/string.md).

**Returned value**

-   IPv6 address in binary format.

Type: [FixedString(16)](../../sql-reference/data-types/fixedstring.md).

**Example**

Query:

``` sql
SELECT addr, cutIPv6(IPv6StringToNum(addr), 0, 0) FROM (SELECT ['notaddress', '127.0.0.1', '1111::ffff'] AS addr) ARRAY JOIN addr;
```

Result:

``` text
┌─addr───────┬─cutIPv6(IPv6StringToNum(addr), 0, 0)─┐
│ notaddress │ ::                                   │
│ 127.0.0.1  │ ::ffff:127.0.0.1                     │
│ 1111::ffff │ 1111::ffff                           │
└────────────┴──────────────────────────────────────┘
```

**See Also**

-   [cutIPv6](#cutipv6x-bytestocutforipv6-bytestocutforipv4).

## IPv6StringToNumOrDefault(s)

Same as `IPv6StringToNum`, but if the IPv6 address has an invalid format, it returns 0.

## IPv6StringToNumOrNull(s)

Same as `IPv6StringToNum`, but if the IPv6 address has an invalid format, it returns null.

## IPv4ToIPv6(x)

Takes a `UInt32` number. Interprets it as an IPv4 address in [big endian](https://en.wikipedia.org/wiki/Endianness). Returns a `FixedString(16)` value containing the IPv6 address in binary format. Examples:

``` sql
SELECT IPv6NumToString(IPv4ToIPv6(IPv4StringToNum('192.168.0.1'))) AS addr;
```

``` text
┌─addr───────────────┐
│ ::ffff:192.168.0.1 │
└────────────────────┘
```

## cutIPv6(x, bytesToCutForIPv6, bytesToCutForIPv4)

Accepts a FixedString(16) value containing the IPv6 address in binary format. Returns a string containing the address of the specified number of bytes removed in text format. For example:

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

## IPv4CIDRToRange(ipv4, Cidr),

Accepts an IPv4 and an UInt8 value containing the [CIDR](https://en.wikipedia.org/wiki/Classless_Inter-Domain_Routing). Return a tuple with two IPv4 containing the lower range and the higher range of the subnet.

``` sql
SELECT IPv4CIDRToRange(toIPv4('192.168.5.2'), 16);
```

``` text
┌─IPv4CIDRToRange(toIPv4('192.168.5.2'), 16)─┐
│ ('192.168.0.0','192.168.255.255')          │
└────────────────────────────────────────────┘
```

## IPv6CIDRToRange(ipv6, Cidr),

Accepts an IPv6 and an UInt8 value containing the CIDR. Return a tuple with two IPv6 containing the lower range and the higher range of the subnet.

``` sql
SELECT IPv6CIDRToRange(toIPv6('2001:0db8:0000:85a3:0000:0000:ac1f:8001'), 32);
```

``` text
┌─IPv6CIDRToRange(toIPv6('2001:0db8:0000:85a3:0000:0000:ac1f:8001'), 32)─┐
│ ('2001:db8::','2001:db8:ffff:ffff:ffff:ffff:ffff:ffff')                │
└────────────────────────────────────────────────────────────────────────┘
```

## toIPv4(string)

An alias to `IPv4StringToNum()` that takes a string form of IPv4 address and returns value of [IPv4](../../sql-reference/data-types/domains/ipv4.md) type, which is binary equal to value returned by `IPv4StringToNum()`.

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

## toIPv4OrDefault(string)

Same as `toIPv4`, but if the IPv4 address has an invalid format, it returns 0.

## toIPv4OrNull(string)

Same as `toIPv4`, but if the IPv4 address has an invalid format, it returns null.

## toIPv6

Converts a string form of IPv6 address to [IPv6](../../sql-reference/data-types/domains/ipv6.md) type. If the IPv6 address has an invalid format, returns an empty value.
Similar to [IPv6StringToNum](#ipv6stringtonums) function, which converts IPv6 address to binary format.

If the input string contains a valid IPv4 address, then the IPv6 equivalent of the IPv4 address is returned.

**Syntax**

```sql
toIPv6(string)
```

**Argument**

-   `string` — IP address. [String](../../sql-reference/data-types/string.md)

**Returned value**

-   IP address.

Type: [IPv6](../../sql-reference/data-types/domains/ipv6.md).

**Examples**

Query:

``` sql
WITH '2001:438:ffff::407d:1bc1' AS IPv6_string
SELECT
    hex(IPv6StringToNum(IPv6_string)),
    hex(toIPv6(IPv6_string));
```

Result:

``` text
┌─hex(IPv6StringToNum(IPv6_string))─┬─hex(toIPv6(IPv6_string))─────────┐
│ 20010438FFFF000000000000407D1BC1  │ 20010438FFFF000000000000407D1BC1 │
└───────────────────────────────────┴──────────────────────────────────┘
```

Query:

``` sql
SELECT toIPv6('127.0.0.1');
```

Result:

``` text
┌─toIPv6('127.0.0.1')─┐
│ ::ffff:127.0.0.1    │
└─────────────────────┘
```

## IPv6StringToNumOrDefault(s)

Same as `toIPv6`, but if the IPv6 address has an invalid format, it returns 0.

## IPv6StringToNumOrNull(s)

Same as `toIPv6`, but if the IPv6 address has an invalid format, it returns null.

## isIPv4String

Determines whether the input string is an IPv4 address or not. If `string` is IPv6 address returns `0`.

**Syntax**

```sql
isIPv4String(string)
```

**Arguments**

-   `string` — IP address. [String](../../sql-reference/data-types/string.md).

**Returned value**

-   `1` if `string` is IPv4 address, `0` otherwise.

Type: [UInt8](../../sql-reference/data-types/int-uint.md).

**Examples**

Query:

```sql
SELECT addr, isIPv4String(addr) FROM ( SELECT ['0.0.0.0', '127.0.0.1', '::ffff:127.0.0.1'] AS addr ) ARRAY JOIN addr;
```

Result:

``` text
┌─addr─────────────┬─isIPv4String(addr)─┐
│ 0.0.0.0          │                  1 │
│ 127.0.0.1        │                  1 │
│ ::ffff:127.0.0.1 │                  0 │
└──────────────────┴────────────────────┘
```

## isIPv6String

Determines whether the input string is an IPv6 address or not. If `string` is IPv4 address returns `0`.

**Syntax**

```sql
isIPv6String(string)
```

**Arguments**

-   `string` — IP address. [String](../../sql-reference/data-types/string.md).

**Returned value**

-   `1` if `string` is IPv6 address, `0` otherwise.

Type: [UInt8](../../sql-reference/data-types/int-uint.md).

**Examples**

Query:

``` sql
SELECT addr, isIPv6String(addr) FROM ( SELECT ['::', '1111::ffff', '::ffff:127.0.0.1', '127.0.0.1'] AS addr ) ARRAY JOIN addr;
```

Result:

``` text
┌─addr─────────────┬─isIPv6String(addr)─┐
│ ::               │                  1 │
│ 1111::ffff       │                  1 │
│ ::ffff:127.0.0.1 │                  1 │
│ 127.0.0.1        │                  0 │
└──────────────────┴────────────────────┘
```

## isIPAddressInRange

Determines if an IP address is contained in a network represented in the [CIDR](https://en.wikipedia.org/wiki/Classless_Inter-Domain_Routing) notation. Returns `1` if true, or `0` otherwise.

**Syntax**

``` sql
isIPAddressInRange(address, prefix)
```

This function accepts both IPv4 and IPv6 addresses (and networks) represented as strings. It returns `0` if the IP version of the address and the CIDR don't match.

**Arguments**

-   `address` — An IPv4 or IPv6 address. [String](../../sql-reference/data-types/string.md).
-   `prefix` — An IPv4 or IPv6 network prefix in CIDR. [String](../../sql-reference/data-types/string.md).

**Returned value**

-   `1` or `0`.

Type: [UInt8](../../sql-reference/data-types/int-uint.md).

**Example**

Query:

``` sql
SELECT isIPAddressInRange('127.0.0.1', '127.0.0.0/8');
```

Result:

``` text
┌─isIPAddressInRange('127.0.0.1', '127.0.0.0/8')─┐
│                                              1 │
└────────────────────────────────────────────────┘
```

Query:

``` sql
SELECT isIPAddressInRange('127.0.0.1', 'ffff::/16');
```

Result:

``` text
┌─isIPAddressInRange('127.0.0.1', 'ffff::/16')─┐
│                                            0 │
└──────────────────────────────────────────────┘
```

Query:

``` sql
SELECT isIPAddressInRange('::ffff:192.168.0.1', '::ffff:192.168.0.4/128');
```

Result:

``` text
┌─isIPAddressInRange('::ffff:192.168.0.1', '::ffff:192.168.0.4/128')─┐
│                                                                  0 │
└────────────────────────────────────────────────────────────────────┘
```
