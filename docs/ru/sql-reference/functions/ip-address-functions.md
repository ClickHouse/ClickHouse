---
sidebar_position: 55
sidebar_label: "Функции для работы с IP-адресами"
---

# Функции для работы с IP-адресами {#funktsii-dlia-raboty-s-ip-adresami}

## IPv4NumToString(num) {#ipv4numtostringnum}

Принимает число типа UInt32. Интерпретирует его, как IPv4-адрес в big endian. Возвращает строку, содержащую соответствующий IPv4-адрес в формате A.B.C.D (числа в десятичной форме через точки).

Синоним: `INET_NTOA`.

## IPv4StringToNum(s) {#ipv4stringtonums}

Функция, обратная к IPv4NumToString. Если IPv4 адрес в неправильном формате, то возвращает 0.

Синоним: `INET_ATON`.

## IPv4NumToStringClassC(num) {#ipv4numtostringclasscnum}

Похоже на IPv4NumToString, но вместо последнего октета используется xxx.

Пример:

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

В связи с тем, что использование xxx весьма необычно, это может быть изменено в дальнейшем. Вам не следует полагаться на конкретный вид этого фрагмента.

### IPv6NumToString(x) {#ipv6numtostringx}

Принимает значение типа FixedString(16), содержащее IPv6-адрес в бинарном виде. Возвращает строку, содержащую этот адрес в текстовом виде.
IPv6-mapped IPv4 адреса выводится в формате ::ffff:111.222.33.44.

Примеры: `INET6_NTOA`.

Примеры:

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

## IPv6StringToNum {#ipv6stringtonums}

Функция, обратная к [IPv6NumToString](#ipv6numtostringx). Если IPv6 адрес передан в неправильном формате, то возвращает строку из нулевых байт.

Если IP адрес является корректным IPv4 адресом, функция возвращает его IPv6 эквивалент.

HEX может быть в любом регистре.

Синоним: `INET6_ATON`.

**Синтаксис**

``` sql
IPv6StringToNum(string)
```

**Аргумент**

-   `string` — IP адрес. [String](../../sql-reference/data-types/string.md).

**Возвращаемое значение**

-   Адрес IPv6 в двоичном представлении.

Тип: [FixedString(16)](../../sql-reference/data-types/fixedstring.md).

**Пример**

Запрос:

``` sql
SELECT addr, cutIPv6(IPv6StringToNum(addr), 0, 0) FROM (SELECT ['notaddress', '127.0.0.1', '1111::ffff'] AS addr) ARRAY JOIN addr;
```

Результат:

``` text
┌─addr───────┬─cutIPv6(IPv6StringToNum(addr), 0, 0)─┐
│ notaddress │ ::                                   │
│ 127.0.0.1  │ ::ffff:127.0.0.1                     │
│ 1111::ffff │ 1111::ffff                           │
└────────────┴──────────────────────────────────────┘
```

**Смотрите также**

-   [cutIPv6](#cutipv6x-bytestocutforipv6-bytestocutforipv4).

## IPv4ToIPv6(x) {#ipv4toipv6x}

Принимает число типа `UInt32`. Интерпретирует его, как IPv4-адрес в [big endian](https://en.wikipedia.org/wiki/Endianness). Возвращает значение `FixedString(16)`, содержащее адрес IPv6 в двоичном формате. Примеры:

``` sql
SELECT IPv6NumToString(IPv4ToIPv6(IPv4StringToNum('192.168.0.1'))) AS addr;
```

``` text
┌─addr───────────────┐
│ ::ffff:192.168.0.1 │
└────────────────────┘
```

## cutIPv6(x, bytesToCutForIPv6, bytesToCutForIPv4) {#cutipv6x-bytestocutforipv6-bytestocutforipv4}

Принимает значение типа FixedString(16), содержащее IPv6-адрес в бинарном виде. Возвращает строку, содержащую адрес из указанного количества байтов, удаленных в текстовом формате. Например:

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

Принимает на вход IPv4 и значение `UInt8`, содержащее [CIDR](https://ru.wikipedia.org/wiki/Бесклассовая_адресация). Возвращает кортеж с двумя IPv4, содержащими нижний и более высокий диапазон подсети.

``` sql
SELECT IPv4CIDRToRange(toIPv4('192.168.5.2'), 16);
```

``` text
┌─IPv4CIDRToRange(toIPv4('192.168.5.2'), 16)─┐
│ ('192.168.0.0','192.168.255.255')          │
└────────────────────────────────────────────┘
```

## IPv6CIDRToRange(ipv6, Cidr), {#ipv6cidrtorangeipv6-cidr}

Принимает на вход IPv6 и значение `UInt8`, содержащее CIDR. Возвращает кортеж с двумя IPv6, содержащими нижний и более высокий диапазон подсети.

``` sql
SELECT IPv6CIDRToRange(toIPv6('2001:0db8:0000:85a3:0000:0000:ac1f:8001'), 32);
```

``` text
┌─IPv6CIDRToRange(toIPv6('2001:0db8:0000:85a3:0000:0000:ac1f:8001'), 32)─┐
│ ('2001:db8::','2001:db8:ffff:ffff:ffff:ffff:ffff:ffff')                │
└────────────────────────────────────────────────────────────────────────┘
```

## toIPv4(string) {#toipv4string}

Псевдоним функции `IPv4StringToNum()` которая принимает строку с адресом IPv4 и возвращает значение типа [IPv4](../../sql-reference/functions/ip-address-functions.md), которое равно значению, возвращаемому функцией `IPv4StringToNum()`.

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

## toIPv6 {#toipv6string}

Приводит строку с адресом в формате IPv6 к типу [IPv6](../../sql-reference/data-types/domains/ipv6.md). Возвращает пустое значение, если входящая строка не является корректным IP адресом.
Похоже на функцию [IPv6StringToNum](#ipv6stringtonums), которая представляет адрес IPv6 в двоичном виде.

Если входящая строка содержит корректный IPv4 адрес, функция возвращает его IPv6 эквивалент.

**Синтаксис**

```sql
toIPv6(string)
```

**Аргумент**

-   `string` — IP адрес. [String](../../sql-reference/data-types/string.md)

**Возвращаемое значение**

-   IP адрес.

Тип: [IPv6](../../sql-reference/data-types/domains/ipv6.md).

**Примеры**

Запрос:

``` sql
WITH '2001:438:ffff::407d:1bc1' AS IPv6_string
SELECT
    hex(IPv6StringToNum(IPv6_string)),
    hex(toIPv6(IPv6_string));
```

Результат:

``` text
┌─hex(IPv6StringToNum(IPv6_string))─┬─hex(toIPv6(IPv6_string))─────────┐
│ 20010438FFFF000000000000407D1BC1  │ 20010438FFFF000000000000407D1BC1 │
└───────────────────────────────────┴──────────────────────────────────┘
```

Запрос:

``` sql
SELECT toIPv6('127.0.0.1');
```

Результат:

``` text
┌─toIPv6('127.0.0.1')─┐
│ ::ffff:127.0.0.1    │
└─────────────────────┘
```

## isIPv4String {#isipv4string}

Определяет, является ли строка адресом IPv4 или нет. Также вернет `0`, если `string` — адрес IPv6.

**Синтаксис**

```sql
isIPv4String(string)
```

**Аргументы**

-   `string` — IP адрес. [String](../../sql-reference/data-types/string.md).

**Возвращаемое значение**

-   `1` если `string` является адресом IPv4 , иначе — `0`.

Тип: [UInt8](../../sql-reference/data-types/int-uint.md).

**Примеры**

Запрос:

```sql
SELECT addr, isIPv4String(addr) FROM ( SELECT ['0.0.0.0', '127.0.0.1', '::ffff:127.0.0.1'] AS addr ) ARRAY JOIN addr;
```

Результат:

``` text
┌─addr─────────────┬─isIPv4String(addr)─┐
│ 0.0.0.0          │                  1 │
│ 127.0.0.1        │                  1 │
│ ::ffff:127.0.0.1 │                  0 │
└──────────────────┴────────────────────┘
```

## isIPv6String {#isipv6string}

Определяет, является ли строка адресом IPv6 или нет. Также вернет `0`, если `string` — адрес IPv4.

**Синтаксис**

```sql
isIPv6String(string)
```

**Аргументы**

-   `string` — IP адрес. [String](../../sql-reference/data-types/string.md).

**Возвращаемое значение**

-   `1` если `string` является адресом IPv6 , иначе — `0`.

Тип: [UInt8](../../sql-reference/data-types/int-uint.md).

**Примеры**

Запрос:

``` sql
SELECT addr, isIPv6String(addr) FROM ( SELECT ['::', '1111::ffff', '::ffff:127.0.0.1', '127.0.0.1'] AS addr ) ARRAY JOIN addr;
```

Результат:

``` text
┌─addr─────────────┬─isIPv6String(addr)─┐
│ ::               │                  1 │
│ 1111::ffff       │                  1 │
│ ::ffff:127.0.0.1 │                  1 │
│ 127.0.0.1        │                  0 │
└──────────────────┴────────────────────┘
```

## isIPAddressInRange {#isipaddressinrange}

Проверяет, попадает ли IP адрес в интервал, заданный в нотации [CIDR](https://en.wikipedia.org/wiki/Classless_Inter-Domain_Routing).

**Синтаксис**

``` sql
isIPAddressInRange(address, prefix)
```
Функция принимает IPv4 или IPv6 адрес виде строки. Возвращает `0`, если версия адреса и интервала не совпадают.

**Аргументы**

-   `address` — IPv4 или IPv6 адрес. [String](../../sql-reference/data-types/string.md).
-   `prefix` — IPv4 или IPv6 подсеть, заданная в нотации CIDR. [String](../../sql-reference/data-types/string.md).

**Возвращаемое значение**

-   `1` или `0`.

Тип: [UInt8](../../sql-reference/data-types/int-uint.md).

**Примеры**

Запрос:

``` sql
SELECT isIPAddressInRange('127.0.0.1', '127.0.0.0/8');
```

Результат:

``` text
┌─isIPAddressInRange('127.0.0.1', '127.0.0.0/8')─┐
│                                              1 │
└────────────────────────────────────────────────┘
```

Запрос:

``` sql
SELECT isIPAddressInRange('127.0.0.1', 'ffff::/16');
```

Результат:

``` text
┌─isIPAddressInRange('127.0.0.1', 'ffff::/16')─┐
│                                            0 │
└──────────────────────────────────────────────┘
```

Запрос:

``` sql
SELECT isIPAddressInRange('::ffff:192.168.0.1', '::ffff:192.168.0.4/128');
```

Результат:

``` text
┌─isIPAddressInRange('::ffff:192.168.0.1', '::ffff:192.168.0.4/128')─┐
│                                                                  0 │
└────────────────────────────────────────────────────────────────────┘
```
