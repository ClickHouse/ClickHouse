---
toc_priority: 52
toc_title: Encoding
---

# Encoding Functions {#encoding-functions}

## char {#char}

Returns the string with the length as the number of passed arguments and each byte has the value of corresponding argument. Accepts multiple arguments of numeric types. If the value of argument is out of range of UInt8 data type, it is converted to UInt8 with possible rounding and overflow.

**Syntax**

``` sql
char(number_1, [number_2, ..., number_n]);
```

**Arguments**

-   `number_1, number_2, ..., number_n` — Numerical arguments interpreted as integers. Types: [Int](../../sql-reference/data-types/int-uint.md), [Float](../../sql-reference/data-types/float.md).

**Returned value**

-   a string of given bytes.

Type: `String`.

**Example**

Query:

``` sql
SELECT char(104.1, 101, 108.9, 108.9, 111) AS hello;
```

Result:

``` text
┌─hello─┐
│ hello │
└───────┘
```

You can construct a string of arbitrary encoding by passing the corresponding bytes. Here is example for UTF-8:

Query:

``` sql
SELECT char(0xD0, 0xBF, 0xD1, 0x80, 0xD0, 0xB8, 0xD0, 0xB2, 0xD0, 0xB5, 0xD1, 0x82) AS hello;
```

Result:

``` text
┌─hello──┐
│ привет │
└────────┘
```

Query:

``` sql
SELECT char(0xE4, 0xBD, 0xA0, 0xE5, 0xA5, 0xBD) AS hello;
```

Result:

``` text
┌─hello─┐
│ 你好  │
└───────┘
```

## hex {#hex}

Returns a string containing the argument’s hexadecimal representation.

Alias: `HEX`.

**Syntax**

``` sql
hex(arg)
```

The function is using uppercase letters `A-F` and not using any prefixes (like `0x`) or suffixes (like `h`).

For integer arguments, it prints hex digits (“nibbles”) from the most significant to least significant (big endian or “human readable” order). It starts with the most significant non-zero byte (leading zero bytes are omitted) but always prints both digits of every byte even if leading digit is zero.

**Example**

Query:

``` sql
SELECT hex(1);
```

Result:

``` text
01
```

Values of type `Date` and `DateTime` are formatted as corresponding integers (the number of days since Epoch for Date and the value of Unix Timestamp for DateTime).

For `String` and `FixedString`, all bytes are simply encoded as two hexadecimal numbers. Zero bytes are not omitted.

Values of floating point and Decimal types are encoded as their representation in memory. As we support little endian architecture, they are encoded in little endian. Zero leading/trailing bytes are not omitted.

**Arguments**

-   `arg` — A value to convert to hexadecimal. Types: [String](../../sql-reference/data-types/string.md), [UInt](../../sql-reference/data-types/int-uint.md), [Float](../../sql-reference/data-types/float.md), [Decimal](../../sql-reference/data-types/decimal.md), [Date](../../sql-reference/data-types/date.md) or [DateTime](../../sql-reference/data-types/datetime.md).

**Returned value**

-   A string with the hexadecimal representation of the argument.

Type: `String`.

**Example**

Query:

``` sql
SELECT hex(toFloat32(number)) as hex_presentation FROM numbers(15, 2);
```

Result:

``` text
┌─hex_presentation─┐
│ 00007041         │
│ 00008041         │
└──────────────────┘
```

Query:

``` sql
SELECT hex(toFloat64(number)) as hex_presentation FROM numbers(15, 2);
```

Result:

``` text
┌─hex_presentation─┐
│ 0000000000002E40 │
│ 0000000000003040 │
└──────────────────┘
```

## unhex {#unhexstr}

Performs the opposite operation of [hex](#hex). It interprets each pair of hexadecimal digits (in the argument) as a number and converts it to the byte represented by the number. The return value is a binary string (BLOB).

If you want to convert the result to a number, you can use the [reverse](../../sql-reference/functions/string-functions.md#reverse) and [reinterpretAs<Type>](../../sql-reference/functions/type-conversion-functions.md#type-conversion-functions) functions.

!!! note "Note"
    If `unhex` is invoked from within the `clickhouse-client`, binary strings display using UTF-8.

Alias: `UNHEX`.

**Syntax**

``` sql
unhex(arg)
```

**Arguments**

-   `arg` — A string containing any number of hexadecimal digits. Type: [String](../../sql-reference/data-types/string.md).

Supports both uppercase and lowercase letters `A-F`. The number of hexadecimal digits does not have to be even. If it is odd, the last digit is interpreted as the least significant half of the `00-0F` byte. If the argument string contains anything other than hexadecimal digits, some implementation-defined result is returned (an exception isn’t thrown). For a numeric argument the inverse of hex(N) is not performed by unhex().

**Returned value**

-   A binary string (BLOB).

Type: [String](../../sql-reference/data-types/string.md).

**Example**

Query:
``` sql
SELECT unhex('303132'), UNHEX('4D7953514C');
```

Result:
``` text
┌─unhex('303132')─┬─unhex('4D7953514C')─┐
│ 012             │ MySQL               │
└─────────────────┴─────────────────────┘
```

Query:

``` sql
SELECT reinterpretAsUInt64(reverse(unhex('FFF'))) AS num;
```

Result:

``` text
┌──num─┐
│ 4095 │
└──────┘
```

## UUIDStringToNum(str) {#uuidstringtonumstr}

Accepts a string containing 36 characters in the format `123e4567-e89b-12d3-a456-426655440000`, and returns it as a set of bytes in a FixedString(16).

## UUIDNumToString(str) {#uuidnumtostringstr}

Accepts a FixedString(16) value. Returns a string containing 36 characters in text format.

## bitmaskToList(num) {#bitmasktolistnum}

Accepts an integer. Returns a string containing the list of powers of two that total the source number when summed. They are comma-separated without spaces in text format, in ascending order.

## bitmaskToArray(num) {#bitmasktoarraynum}

Accepts an integer. Returns an array of UInt64 numbers containing the list of powers of two that total the source number when summed. Numbers in the array are in ascending order.

## bitPositionsToArray(num) {#bitpositionstoarraynum}

Accepts an integer and converts it to an unsigned integer. Returns an array of `UInt64` numbers containing the list of positions of bits of `arg` that equal `1`, in ascending order.

**Syntax**

```sql
bitPositionsToArray(arg)
```

**Arguments**

-   `arg` — Integer value. [Int/UInt](../../sql-reference/data-types/int-uint.md).

**Returned value**

-   An array containing a list of positions of bits that equal `1`, in ascending order.

Type: [Array](../../sql-reference/data-types/array.md)([UInt64](../../sql-reference/data-types/int-uint.md)).

**Example**

Query:

``` sql
SELECT bitPositionsToArray(toInt8(1)) AS bit_positions;
```

Result:

``` text
┌─bit_positions─┐
│ [0]           │
└───────────────┘
```

Query:

``` sql
select bitPositionsToArray(toInt8(-1)) as bit_positions;
```

Result:

``` text
┌─bit_positions─────┐
│ [0,1,2,3,4,5,6,7] │
└───────────────────┘
```
