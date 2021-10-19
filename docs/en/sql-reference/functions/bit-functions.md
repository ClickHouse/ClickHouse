---
toc_priority: 48
toc_title: Bit
---

# Bit Functions {#bit-functions}

Bit functions work for any pair of types from UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, or Float64.

The result type is an integer with bits equal to the maximum bits of its arguments. If at least one of the arguments is signed, the result is a signed number. If an argument is a floating-point number, it is cast to Int64.

## bitAnd(a, b) {#bitanda-b}

## bitOr(a, b) {#bitora-b}

## bitXor(a, b) {#bitxora-b}

## bitNot(a) {#bitnota}

## bitShiftLeft(a, b) {#bitshiftlefta-b}

## bitShiftRight(a, b) {#bitshiftrighta-b}

## bitRotateLeft(a, b) {#bitrotatelefta-b}

## bitRotateRight(a, b) {#bitrotaterighta-b}

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
