---
sidebar_position: 48
sidebar_label: Bit
---

# Bit Functions {#bit-functions}

Bit functions work for any pair of types from `UInt8`, `UInt16`, `UInt32`, `UInt64`, `Int8`, `Int16`, `Int32`, `Int64`, `Float32`, or `Float64`. Some functions support `String` and `FixedString` types.

The result type is an integer with bits equal to the maximum bits of its arguments. If at least one of the arguments is signed, the result is a signed number. If an argument is a floating-point number, it is cast to Int64.

## bitAnd(a, b) {#bitanda-b}

## bitOr(a, b) {#bitora-b}

## bitXor(a, b) {#bitxora-b}

## bitNot(a) {#bitnota}

## bitShiftLeft(a, b) {#bitshiftlefta-b}

Shifts the binary representation of a value to the left by a specified number of bit positions.

A `FixedString` or a `String` is treated as a single multibyte value.

Bits of a `FixedString` value are lost as they are shifted out. On the contrary, a `String` value is extended with additional bytes, so no bits are lost.

**Syntax**

``` sql
bitShiftLeft(a, b)
```

**Arguments**

-   `a` — A value to shift. [Integer types](../../sql-reference/data-types/int-uint.md), [String](../../sql-reference/data-types/string.md) or [FixedString](../../sql-reference/data-types/fixedstring.md).
-   `b` — The number of shift positions. [Unsigned integer types](../../sql-reference/data-types/int-uint.md), 64 bit types or less are allowed.

**Returned value**

-   Shifted value.

The type of the returned value is the same as the type of the input value.

**Example**

In the following queries [bin](encoding-functions.md#bin) and [hex](encoding-functions.md#hex) functions are used to show bits of shifted values.

``` sql
SELECT 99 AS a, bin(a), bitShiftLeft(a, 2) AS a_shifted, bin(a_shifted);
SELECT 'abc' AS a, hex(a), bitShiftLeft(a, 4) AS a_shifted, hex(a_shifted);
SELECT toFixedString('abc', 3) AS a, hex(a), bitShiftLeft(a, 4) AS a_shifted, hex(a_shifted);
```

Result:

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

Shifts the binary representation of a value to the right by a specified number of bit positions.

A `FixedString` or a `String` is treated as a single multibyte value. Note that the length of a `String` value is reduced as bits are shifted out.

**Syntax**

``` sql
bitShiftRight(a, b)
```

**Arguments**

-   `a` — A value to shift. [Integer types](../../sql-reference/data-types/int-uint.md), [String](../../sql-reference/data-types/string.md) or [FixedString](../../sql-reference/data-types/fixedstring.md).
-   `b` — The number of shift positions. [Unsigned integer types](../../sql-reference/data-types/int-uint.md), 64 bit types or less are allowed.

**Returned value**

-   Shifted value.

The type of the returned value is the same as the type of the input value.

**Example**

Query:

``` sql
SELECT 101 AS a, bin(a), bitShiftRight(a, 2) AS a_shifted, bin(a_shifted);
SELECT 'abc' AS a, hex(a), bitShiftRight(a, 12) AS a_shifted, hex(a_shifted);
SELECT toFixedString('abc', 3) AS a, hex(a), bitShiftRight(a, 12) AS a_shifted, hex(a_shifted);
```

Result:

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

Returns a substring starting with the bit from the ‘offset’ index that is ‘length’ bits long. bits indexing starts from
1

**Syntax**

``` sql
bitSlice(s, offset[, length])
```

**Arguments**

- `s` — s is [String](../../sql-reference/data-types/string.md) or [FixedString](../../sql-reference/data-types/fixedstring.md).
- `offset` — The start index with bit, A positive value indicates an offset on the left, and a negative value is an indent on the right. Numbering of the bits begins with 1.
- `length` — The length of substring with bit. If you specify a negative value, the function returns an open substring \[offset, array_length - length\]. If you omit the value, the function returns the substring \[offset, the_end_string\]. If length exceeds s, it will be truncate.If length isn't multiple of 8, will fill 0 on the right.

**Returned value**

- The substring. [String](../../sql-reference/data-types/string.md)

**Example**

Query:

``` sql
select bin('Hello'), bin(bitSlice('Hello', 1, 8))
select bin('Hello'), bin(bitSlice('Hello', 1, 2))
select bin('Hello'), bin(bitSlice('Hello', 1, 9))
select bin('Hello'), bin(bitSlice('Hello', -4, 8))
```

Result:

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

Takes any integer and converts it into [binary form](https://en.wikipedia.org/wiki/Binary_number), returns the value of a bit at specified position. The countdown starts from 0 from the right to the left.

**Syntax**

``` sql
SELECT bitTest(number, index)
```

**Arguments**

-   `number` – Integer number.
-   `index` – Position of bit.

**Returned values**

Returns a value of bit at specified position.

Type: `UInt8`.

**Example**

For example, the number 43 in base-2 (binary) numeral system is 101011.

Query:

``` sql
SELECT bitTest(43, 1);
```

Result:

``` text
┌─bitTest(43, 1)─┐
│              1 │
└────────────────┘
```

Another example:

Query:

``` sql
SELECT bitTest(43, 2);
```

Result:

``` text
┌─bitTest(43, 2)─┐
│              0 │
└────────────────┘
```

## bitTestAll {#bittestall}

Returns result of [logical conjuction](https://en.wikipedia.org/wiki/Logical_conjunction) (AND operator) of all bits at given positions. The countdown starts from 0 from the right to the left.

The conjuction for bitwise operations:

0 AND 0 = 0

0 AND 1 = 0

1 AND 0 = 0

1 AND 1 = 1

**Syntax**

``` sql
SELECT bitTestAll(number, index1, index2, index3, index4, ...)
```

**Arguments**

-   `number` – Integer number.
-   `index1`, `index2`, `index3`, `index4` – Positions of bit. For example, for set of positions (`index1`, `index2`, `index3`, `index4`) is true if and only if all of its positions are true (`index1` ⋀ `index2`, ⋀ `index3` ⋀ `index4`).

**Returned values**

Returns result of logical conjuction.

Type: `UInt8`.

**Example**

For example, the number 43 in base-2 (binary) numeral system is 101011.

Query:

``` sql
SELECT bitTestAll(43, 0, 1, 3, 5);
```

Result:

``` text
┌─bitTestAll(43, 0, 1, 3, 5)─┐
│                          1 │
└────────────────────────────┘
```

Another example:

Query:

``` sql
SELECT bitTestAll(43, 0, 1, 3, 5, 2);
```

Result:

``` text
┌─bitTestAll(43, 0, 1, 3, 5, 2)─┐
│                             0 │
└───────────────────────────────┘
```

## bitTestAny {#bittestany}

Returns result of [logical disjunction](https://en.wikipedia.org/wiki/Logical_disjunction) (OR operator) of all bits at given positions. The countdown starts from 0 from the right to the left.

The disjunction for bitwise operations:

0 OR 0 = 0

0 OR 1 = 1

1 OR 0 = 1

1 OR 1 = 1

**Syntax**

``` sql
SELECT bitTestAny(number, index1, index2, index3, index4, ...)
```

**Arguments**

-   `number` – Integer number.
-   `index1`, `index2`, `index3`, `index4` – Positions of bit.

**Returned values**

Returns result of logical disjuction.

Type: `UInt8`.

**Example**

For example, the number 43 in base-2 (binary) numeral system is 101011.

Query:

``` sql
SELECT bitTestAny(43, 0, 2);
```

Result:

``` text
┌─bitTestAny(43, 0, 2)─┐
│                    1 │
└──────────────────────┘
```

Another example:

Query:

``` sql
SELECT bitTestAny(43, 4, 2);
```

Result:

``` text
┌─bitTestAny(43, 4, 2)─┐
│                    0 │
└──────────────────────┘
```

## bitCount {#bitcount}

Calculates the number of bits set to one in the binary representation of a number.

**Syntax**

``` sql
bitCount(x)
```

**Arguments**

-   `x` — [Integer](../../sql-reference/data-types/int-uint.md) or [floating-point](../../sql-reference/data-types/float.md) number. The function uses the value representation in memory. It allows supporting floating-point numbers.

**Returned value**

-   Number of bits set to one in the input number.

The function does not convert input value to a larger type ([sign extension](https://en.wikipedia.org/wiki/Sign_extension)). So, for example, `bitCount(toUInt8(-1)) = 8`.

Type: `UInt8`.

**Example**

Take for example the number 333. Its binary representation: 0000000101001101.

Query:

``` sql
SELECT bitCount(333);
```

Result:

``` text
┌─bitCount(333)─┐
│             5 │
└───────────────┘
```

## bitHammingDistance {#bithammingdistance}

Returns the [Hamming Distance](https://en.wikipedia.org/wiki/Hamming_distance) between the bit representations of two integer values. Can be used with [SimHash](../../sql-reference/functions/hash-functions.md#ngramsimhash) functions for detection of semi-duplicate strings. The smaller is the distance, the more likely those strings are the same.

**Syntax**

``` sql
bitHammingDistance(int1, int2)
```

**Arguments**

-   `int1` — First integer value. [Int64](../../sql-reference/data-types/int-uint.md).
-   `int2` — Second integer value. [Int64](../../sql-reference/data-types/int-uint.md).

**Returned value**

-   The Hamming distance.

Type: [UInt8](../../sql-reference/data-types/int-uint.md).

**Examples**

Query:

``` sql
SELECT bitHammingDistance(111, 121);
```

Result:

``` text
┌─bitHammingDistance(111, 121)─┐
│                            3 │
└──────────────────────────────┘
```

With [SimHash](../../sql-reference/functions/hash-functions.md#ngramsimhash):

``` sql
SELECT bitHammingDistance(ngramSimHash('cat ate rat'), ngramSimHash('rat ate cat'));
```

Result:

``` text
┌─bitHammingDistance(ngramSimHash('cat ate rat'), ngramSimHash('rat ate cat'))─┐
│                                                                            5 │
└──────────────────────────────────────────────────────────────────────────────┘
```
