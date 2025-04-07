---
slug: /zh/sql-reference/functions/bit-functions
---
# 位操作函数 {#wei-cao-zuo-han-shu}

位操作函数适用于UInt8，UInt16，UInt32，UInt64，Int8，Int16，Int32，Int64，Float32或Float64中的任何类型。

结果类型是一个整数，其位数等于其参数的最大位。如果至少有一个参数为有符数字，则结果为有符数字。如果参数是浮点数，则将其强制转换为Int64。

## bitAnd(a, b) {#bitanda-b}

## bitOr(a, b) {#bitora-b}

## bitXor(a, b) {#bitxora-b}

## bitNot(a) {#bitnota}

## bitShiftLeft(a, b) {#bitshiftlefta-b}

将值的二进制表示向左移动指定数量的位。

`FixedString` 或 `String` 被视为单个多字节值。

`FixedString` 值的位在移出时会丢失。相反，`String` 值使用额外的字节进行扩展，因此不会丢失任何位。

**语法**

``` sql
bitShiftLeft(a, b)
```

**参数**

-   `a` — 要进行移位操作的值。类型可以为[Integer types](../../sql-reference/data-types/int-uint.md)，[String](../../sql-reference/data-types/string.md)或者[FixedString](../../sql-reference/data-types/fixedstring.md)。
-   `b` — 移位的次数。类型为[Unsigned integer types](../../sql-reference/data-types/int-uint.md)，允许使用64位数字及64位以下的数字类型。

**返回值**

-   移位后的值。

返回值的类型与输入值的类型相同。

**示例**

在以下查询中，[bin](encoding-functions.md#bin)和[hex](encoding-functions.md#hex)函数用于显示移位值的位。

``` sql
SELECT 99 AS a, bin(a), bitShiftLeft(a, 2) AS a_shifted, bin(a_shifted);
SELECT 'abc' AS a, hex(a), bitShiftLeft(a, 4) AS a_shifted, hex(a_shifted);
SELECT toFixedString('abc', 3) AS a, hex(a), bitShiftLeft(a, 4) AS a_shifted, hex(a_shifted);
```

结果：

``` text
┌──a─┬─bin(99)──┬─a_shifted─┬─bin(bitShiftLeft(99, 2))─┐
│ 99 │ 01100011 │       140 │ 10001100                 │
└────┴──────────┴───────────┴──────────────────────────┘
┌─a───┬─hex('abc')─┬─a_shifted─┬─hex(bitShiftLeft('abc', 4))─┐
│ abc │ 616263     │ &0        │ 06162630                    │
└─────┴────────────┴───────────┴─────────────────────────────┘
┌─a───┬─hex(toFixedString('abc', 3))─┬─a_shifted─┬─hex(bitShiftLeft(toFixedString('abc', 3), 4))─┐
│ abc │ 616263                       │ &0        │ 162630                                        │
└─────┴──────────────────────────────┴───────────┴───────────────────────────────────────────────┘
```

## bitShiftRight(a, b) {#bitshiftrighta-b}

将值的二进制表示向右移动指定数量的位。

`FixedString`或`String`被视为单个多字节值。请注意，`String`值的长度会随着位的移出而减少。

**语法**

``` sql
bitShiftRight(a, b)
```

**参数**

-   `a` — 需要进行位移的值。类型可以为[Integer types](../../sql-reference/data-types/int-uint.md)，[String](../../sql-reference/data-types/string.md)或者[FixedString](../../sql-reference/data-types/fixedstring.md)。
-   `b` — 移位的次数。类型为[Unsigned integer types](../../sql-reference/data-types/int-uint.md)，允许使用64位数字及64位以下的数字类型。

**返回值**

-   移位后的值。

返回值的类型与输入值的类型相同。

**示例**

查询语句：

``` sql
SELECT 101 AS a, bin(a), bitShiftRight(a, 2) AS a_shifted, bin(a_shifted);
SELECT 'abc' AS a, hex(a), bitShiftRight(a, 12) AS a_shifted, hex(a_shifted);
SELECT toFixedString('abc', 3) AS a, hex(a), bitShiftRight(a, 12) AS a_shifted, hex(a_shifted);
```

结果：

``` text
┌───a─┬─bin(101)─┬─a_shifted─┬─bin(bitShiftRight(101, 2))─┐
│ 101 │ 01100101 │        25 │ 00011001                   │
└─────┴──────────┴───────────┴────────────────────────────┘
┌─a───┬─hex('abc')─┬─a_shifted─┬─hex(bitShiftRight('abc', 12))─┐
│ abc │ 616263     │           │ 0616                          │
└─────┴────────────┴───────────┴───────────────────────────────┘
┌─a───┬─hex(toFixedString('abc', 3))─┬─a_shifted─┬─hex(bitShiftRight(toFixedString('abc', 3), 12))─┐
│ abc │ 616263                       │           │ 000616                                          │
└─────┴──────────────────────────────┴───────────┴─────────────────────────────────────────────────┘
```

## bitRotateLeft(a, b) {#bitrotatelefta-b}

## bitRotateRight(a, b) {#bitrotaterighta-b}

## bitSlice(s, offset, length)

返回从`offset`索引中的`length`位长的位开始的子字符串，位索引从 1 开始。

**语法**

``` sql
bitSlice(s, offset[, length])
```

**参数**

- `s` — 类型可以是[String](../../sql-reference/data-types/string.md)或者[FixedString](../../sql-reference/data-types/fixedstring.md)。
- `offset` — 带位的起始索引，正值表示左侧偏移，负值表示右侧缩进，位编号从 1 开始。
- `length` — 带位的子串长度。如果您指定一个负值，该函数将返回一个开放子字符串 \[offset, array_length - length\]。如果省略该值，该函数将返回子字符串 \[offset, the_end_string\]。如果长度超过s，将被截断。如果长度不是8的倍数，则在右边填充0。

**返回值**

- 子字符串，类型为[String](../../sql-reference/data-types/string.md)。

**示例**

查询语句：

``` sql
select bin('Hello'), bin(bitSlice('Hello', 1, 8))
select bin('Hello'), bin(bitSlice('Hello', 1, 2))
select bin('Hello'), bin(bitSlice('Hello', 1, 9))
select bin('Hello'), bin(bitSlice('Hello', -4, 8))
```

结果：

``` text
┌─bin('Hello')─────────────────────────────┬─bin(bitSlice('Hello', 1, 8))─┐
│ 0100100001100101011011000110110001101111 │ 01001000                     │
└──────────────────────────────────────────┴──────────────────────────────┘
┌─bin('Hello')─────────────────────────────┬─bin(bitSlice('Hello', 1, 2))─┐
│ 0100100001100101011011000110110001101111 │ 01000000                     │
└──────────────────────────────────────────┴──────────────────────────────┘
┌─bin('Hello')─────────────────────────────┬─bin(bitSlice('Hello', 1, 9))─┐
│ 0100100001100101011011000110110001101111 │ 0100100000000000             │
└──────────────────────────────────────────┴──────────────────────────────┘
┌─bin('Hello')─────────────────────────────┬─bin(bitSlice('Hello', -4, 8))─┐
│ 0100100001100101011011000110110001101111 │ 11110000                      │
└──────────────────────────────────────────┴───────────────────────────────┘
```

## bitTest {#bittest}

取任意整数并将其转换为[binary form](https://en.wikipedia.org/wiki/Binary_number)，返回指定位置的位值。位值从右到左数，从0开始计数。

**语法**

``` sql
SELECT bitTest(number, index)
```

**参数**

-   `number` – 整数。
-   `index` – 要获取位值的位置。 

**返回值**

返回指定位置的位值

类型为：`UInt8`。

**示例**

例如，十进制数字 43 在二进制的表示是 101011。

查询语句：

``` sql
SELECT bitTest(43, 1);
```

结果：

``` text
┌─bitTest(43, 1)─┐
│              1 │
└────────────────┘
```

另一个示例：

查询语句：

``` sql
SELECT bitTest(43, 2);
```

结果：

``` text
┌─bitTest(43, 2)─┐
│              0 │
└────────────────┘
```

## bitTestAll {#bittestall}

返回给定位置所有位的 [logical conjunction](https://en.wikipedia.org/wiki/Logical_conjunction) 进行与操作的结果。位值从右到左数，从0开始计数。

与运算的结果：

0 AND 0 = 0

0 AND 1 = 0

1 AND 0 = 0

1 AND 1 = 1

**语法**

``` sql
SELECT bitTestAll(number, index1, index2, index3, index4, ...)
```

**参数**

-   `number` – 整数。
-   `index1`, `index2`, `index3`, `index4` – 位的位置。例如，对于一组位置 (`index1`, `index2`, `index3`, `index4`) 当且仅当它的所有位置都为真时才为真 (`index1` ⋀ `index2`, ⋀ `index3` ⋀ `index4` ）。

**返回值**

返回逻辑与的结果。

类型为： `UInt8`。

**示例**

例如，十进制数字 43 在二进制的表示是 101011。

查询语句：

``` sql
SELECT bitTestAll(43, 0, 1, 3, 5);
```

结果：

``` text
┌─bitTestAll(43, 0, 1, 3, 5)─┐
│                          1 │
└────────────────────────────┘
```

另一个例子:

查询语句：

``` sql
SELECT bitTestAll(43, 0, 1, 3, 5, 2);
```

结果：

``` text
┌─bitTestAll(43, 0, 1, 3, 5, 2)─┐
│                             0 │
└───────────────────────────────┘
```

## bitTestAny {#bittestany}

返回给定位置所有位的 [logical disjunction](https://en.wikipedia.org/wiki/Logical_disjunction) 进行或操作的结果。位值从右到左数，从0开始计数。

或运算的结果：

0 OR 0 = 0

0 OR 1 = 1

1 OR 0 = 1

1 OR 1 = 1

**语法**

``` sql
SELECT bitTestAny(number, index1, index2, index3, index4, ...)
```

**参数**

-   `number` – 整数。
-   `index1`, `index2`, `index3`, `index4` –  位的位置。

**返回值**

返回逻辑或的结果。

类型为： `UInt8`。

**示例**

例如，十进制数字 43 在二进制的表示是 101011。

查询语句：

``` sql
SELECT bitTestAny(43, 0, 2);
```

结果：

``` text
┌─bitTestAny(43, 0, 2)─┐
│                    1 │
└──────────────────────┘
```

另一个例子:

查询语句：

``` sql
SELECT bitTestAny(43, 4, 2);
```

结果：

``` text
┌─bitTestAny(43, 4, 2)─┐
│                    0 │
└──────────────────────┘
```

## bitCount {#bitcount}

计算数字的二进制表示中值为 1 的位数。

**语法**

``` sql
bitCount(x)
```

**参数**

-   `x` — 类型为[Integer](../../sql-reference/data-types/int-uint.md)或[floating-point](../../sql-reference/data-types/float.md)数字。该函数使用内存中的值表示。它允许支持浮点数。

**返回值**

-   输入数字中值为 1 的位数。

该函数不会将输入值转换为更大的类型 ([sign extension](https://en.wikipedia.org/wiki/Sign_extension))。 因此，例如，`bitCount(toUInt8(-1)) = 8`。

类型为： `UInt8`。

**示例**

以十进制数字 333 为例，它的二进制表示为: 0000000101001101。

查询语句：

``` sql
SELECT bitCount(333);
```

结果：

``` text
┌─bitCount(333)─┐
│             5 │
└───────────────┘
```

## bitHammingDistance {#bithammingdistance}

返回两个整数值的位表示之间的 [Hamming Distance](https://en.wikipedia.org/wiki/Hamming_distance)。可与 [SimHash](../../sql-reference/functions/hash-functions.md#ngramsimhash) 函数一起使用，用于检测半重复字符串。距离越小，这些字符串就越有可能相同。

**语法**

``` sql
bitHammingDistance(int1, int2)
```

**参数**

-   `int1` — 第一个整数值。类型为[Int64](../../sql-reference/data-types/int-uint.md)。
-   `int2` — 第二个整数值。类型为[Int64](../../sql-reference/data-types/int-uint.md)。

**返回值**

-   汉明距离。

类型为： [UInt8](../../sql-reference/data-types/int-uint.md)。

**示例**

查询语句：

``` sql
SELECT bitHammingDistance(111, 121);
```

结果：

``` text
┌─bitHammingDistance(111, 121)─┐
│                            3 │
└──────────────────────────────┘
```

使用[SimHash](../../sql-reference/functions/hash-functions.md#ngramsimhash)函数:

``` sql
SELECT bitHammingDistance(ngramSimHash('cat ate rat'), ngramSimHash('rat ate cat'));
```

结果：

``` text
┌─bitHammingDistance(ngramSimHash('cat ate rat'), ngramSimHash('rat ate cat'))─┐
│                                                                            5 │
└──────────────────────────────────────────────────────────────────────────────┘
```
