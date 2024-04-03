---
slug: /en/sql-reference/functions/encoding-functions
sidebar_position: 65
sidebar_label: Encoding
---

# Encoding Functions

## char

Returns the string with the length as the number of passed arguments and each byte has the value of corresponding argument. Accepts multiple arguments of numeric types. If the value of argument is out of range of UInt8 data type, it is converted to UInt8 with possible rounding and overflow.

**Syntax**

``` sql
char(number_1, [number_2, ..., number_n]);
```

**Arguments**

- `number_1, number_2, ..., number_n` — Numerical arguments interpreted as integers. Types: [Int](../../sql-reference/data-types/int-uint.md), [Float](../../sql-reference/data-types/float.md).

**Returned value**

- a string of given bytes.

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

## hex

Returns a string containing the argument’s hexadecimal representation.

Alias: `HEX`.

**Syntax**

``` sql
hex(arg)
```

The function is using uppercase letters `A-F` and not using any prefixes (like `0x`) or suffixes (like `h`).

For integer arguments, it prints hex digits (“nibbles”) from the most significant to least significant (big-endian or “human-readable” order). It starts with the most significant non-zero byte (leading zero bytes are omitted) but always prints both digits of every byte even if the leading digit is zero.

Values of type [Date](../../sql-reference/data-types/date.md) and [DateTime](../../sql-reference/data-types/datetime.md) are formatted as corresponding integers (the number of days since Epoch for Date and the value of Unix Timestamp for DateTime).

For [String](../../sql-reference/data-types/string.md) and [FixedString](../../sql-reference/data-types/fixedstring.md), all bytes are simply encoded as two hexadecimal numbers. Zero bytes are not omitted.

Values of [Float](../../sql-reference/data-types/float.md) and [Decimal](../../sql-reference/data-types/decimal.md) types are encoded as their representation in memory. As we support little-endian architecture, they are encoded in little-endian. Zero leading/trailing bytes are not omitted.

Values of [UUID](../data-types/uuid.md) type are encoded as big-endian order string.

**Arguments**

- `arg` — A value to convert to hexadecimal. Types: [String](../../sql-reference/data-types/string.md), [UInt](../../sql-reference/data-types/int-uint.md), [Float](../../sql-reference/data-types/float.md), [Decimal](../../sql-reference/data-types/decimal.md), [Date](../../sql-reference/data-types/date.md) or [DateTime](../../sql-reference/data-types/datetime.md).

**Returned value**

- A string with the hexadecimal representation of the argument.

Type: [String](../../sql-reference/data-types/string.md).

**Examples**

Query:

``` sql
SELECT hex(1);
```

Result:

``` text
01
```

Query:

``` sql
SELECT hex(toFloat32(number)) AS hex_presentation FROM numbers(15, 2);
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
SELECT hex(toFloat64(number)) AS hex_presentation FROM numbers(15, 2);
```

Result:

``` text
┌─hex_presentation─┐
│ 0000000000002E40 │
│ 0000000000003040 │
└──────────────────┘
```

Query:

``` sql
SELECT lower(hex(toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'))) as uuid_hex
```

Result:

``` text
┌─uuid_hex─────────────────────────┐
│ 61f0c4045cb311e7907ba6006ad3dba0 │
└──────────────────────────────────┘
```


## unhex

Performs the opposite operation of [hex](#hex). It interprets each pair of hexadecimal digits (in the argument) as a number and converts it to the byte represented by the number. The return value is a binary string (BLOB).

If you want to convert the result to a number, you can use the [reverse](../../sql-reference/functions/string-functions.md#reverse) and [reinterpretAs&lt;Type&gt;](../../sql-reference/functions/type-conversion-functions.md#type-conversion-functions) functions.

:::note    
If `unhex` is invoked from within the `clickhouse-client`, binary strings display using UTF-8.
:::

Alias: `UNHEX`.

**Syntax**

``` sql
unhex(arg)
```

**Arguments**

- `arg` — A string containing any number of hexadecimal digits. Type: [String](../../sql-reference/data-types/string.md), [FixedString](../../sql-reference/data-types/fixedstring.md).

Supports both uppercase and lowercase letters `A-F`. The number of hexadecimal digits does not have to be even. If it is odd, the last digit is interpreted as the least significant half of the `00-0F` byte. If the argument string contains anything other than hexadecimal digits, some implementation-defined result is returned (an exception isn’t thrown). For a numeric argument the inverse of hex(N) is not performed by unhex().

**Returned value**

- A binary string (BLOB).

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

## bin

Returns a string containing the argument’s binary representation.

**Syntax**

``` sql
bin(arg)
```

Alias: `BIN`.

For integer arguments, it prints bin digits from the most significant to least significant (big-endian or “human-readable” order). It starts with the most significant non-zero byte (leading zero bytes are omitted) but always prints eight digits of every byte if the leading digit is zero.

Values of type [Date](../../sql-reference/data-types/date.md) and [DateTime](../../sql-reference/data-types/datetime.md) are formatted as corresponding integers (the number of days since Epoch for `Date` and the value of Unix Timestamp for `DateTime`).

For [String](../../sql-reference/data-types/string.md) and [FixedString](../../sql-reference/data-types/fixedstring.md), all bytes are simply encoded as eight binary numbers. Zero bytes are not omitted.

Values of [Float](../../sql-reference/data-types/float.md) and [Decimal](../../sql-reference/data-types/decimal.md) types are encoded as their representation in memory. As we support little-endian architecture, they are encoded in little-endian. Zero leading/trailing bytes are not omitted.

Values of [UUID](../data-types/uuid.md) type are encoded as big-endian order string.

**Arguments**

- `arg` — A value to convert to binary. [String](../../sql-reference/data-types/string.md), [FixedString](../../sql-reference/data-types/fixedstring.md), [UInt](../../sql-reference/data-types/int-uint.md), [Float](../../sql-reference/data-types/float.md), [Decimal](../../sql-reference/data-types/decimal.md), [Date](../../sql-reference/data-types/date.md), or [DateTime](../../sql-reference/data-types/datetime.md).

**Returned value**

- A string with the binary representation of the argument.

Type: [String](../../sql-reference/data-types/string.md).

**Examples**

Query:

``` sql
SELECT bin(14);
```

Result:

``` text
┌─bin(14)──┐
│ 00001110 │
└──────────┘
```

Query:

``` sql
SELECT bin(toFloat32(number)) AS bin_presentation FROM numbers(15, 2);
```

Result:

``` text
┌─bin_presentation─────────────────┐
│ 00000000000000000111000001000001 │
│ 00000000000000001000000001000001 │
└──────────────────────────────────┘
```

Query:

``` sql
SELECT bin(toFloat64(number)) AS bin_presentation FROM numbers(15, 2);
```

Result:

``` text
┌─bin_presentation─────────────────────────────────────────────────┐
│ 0000000000000000000000000000000000000000000000000010111001000000 │
│ 0000000000000000000000000000000000000000000000000011000001000000 │
└──────────────────────────────────────────────────────────────────┘
```

Query:

``` sql
SELECT bin(toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0')) as bin_uuid
```

Result:

``` text
┌─bin_uuid─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ 01100001111100001100010000000100010111001011001100010001111001111001000001111011101001100000000001101010110100111101101110100000 │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```


## unbin

Interprets each pair of binary digits (in the argument) as a number and converts it to the byte represented by the number. The functions performs the opposite operation to [bin](#bin).

**Syntax**

``` sql
unbin(arg)
```

Alias: `UNBIN`.

For a numeric argument `unbin()` does not return the inverse of `bin()`. If you want to convert the result to a number, you can use the [reverse](../../sql-reference/functions/string-functions.md#reverse) and [reinterpretAs&lt;Type&gt;](../../sql-reference/functions/type-conversion-functions.md#reinterpretasuint8163264) functions.

:::note    
If `unbin` is invoked from within the `clickhouse-client`, binary strings are displayed using UTF-8.
:::

Supports binary digits `0` and `1`. The number of binary digits does not have to be multiples of eight. If the argument string contains anything other than binary digits, some implementation-defined result is returned (an exception isn’t thrown). 

**Arguments**

- `arg` — A string containing any number of binary digits. [String](../../sql-reference/data-types/string.md).

**Returned value**

- A binary string (BLOB).

Type: [String](../../sql-reference/data-types/string.md).

**Examples**

Query:

``` sql
SELECT UNBIN('001100000011000100110010'), UNBIN('0100110101111001010100110101000101001100');
```

Result:

``` text
┌─unbin('001100000011000100110010')─┬─unbin('0100110101111001010100110101000101001100')─┐
│ 012                               │ MySQL                                             │
└───────────────────────────────────┴───────────────────────────────────────────────────┘
```

Query:

``` sql
SELECT reinterpretAsUInt64(reverse(unbin('1110'))) AS num;
```

Result:

``` text
┌─num─┐
│  14 │
└─────┘
```

## bitmaskToList(num)

Accepts an integer. Returns a string containing the list of powers of two that total the source number when summed. They are comma-separated without spaces in text format, in ascending order.

## bitmaskToArray(num)

Accepts an integer. Returns an array of UInt64 numbers containing the list of powers of two that total the source number when summed. Numbers in the array are in ascending order.

## bitPositionsToArray(num)

Accepts an integer and converts it to an unsigned integer. Returns an array of `UInt64` numbers containing the list of positions of bits of `arg` that equal `1`, in ascending order.

**Syntax**

```sql
bitPositionsToArray(arg)
```

**Arguments**

- `arg` — Integer value. [Int/UInt](../../sql-reference/data-types/int-uint.md).

**Returned value**

- An array containing a list of positions of bits that equal `1`, in ascending order.

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
SELECT bitPositionsToArray(toInt8(-1)) AS bit_positions;
```

Result:

``` text
┌─bit_positions─────┐
│ [0,1,2,3,4,5,6,7] │
└───────────────────┘
```

## mortonEncode

Calculates the Morton encoding (ZCurve) for a list of unsigned integers.

The function has two modes of operation:
- Simple
- Expanded

### Simple mode

Accepts up to 8 unsigned integers as arguments and produces a UInt64 code.

**Syntax**

```sql
mortonEncode(args)
```

**Parameters**

- `args`: up to 8 [unsigned integers](../../sql-reference/data-types/int-uint.md) or columns of the aforementioned type.

**Returned value**

- A UInt64 code

Type: [UInt64](../../sql-reference/data-types/int-uint.md)

**Example**

Query:

```sql
SELECT mortonEncode(1, 2, 3);
```
Result:

```response
53
```

### Expanded mode

Accepts a range mask ([tuple](../../sql-reference/data-types/tuple.md)) as a first argument and up to 8 [unsigned integers](../../sql-reference/data-types/int-uint.md) as other arguments.

Each number in the mask configures the amount of range expansion:<br/>
1 - no expansion<br/>
2 - 2x expansion<br/>
3 - 3x expansion<br/>
...<br/>
Up to 8x expansion.<br/>

**Syntax**

```sql
mortonEncode(range_mask, args)
```

**Parameters**
- `range_mask`: 1-8.
- `args`: up to 8 [unsigned integers](../../sql-reference/data-types/int-uint.md) or columns of the aforementioned type.

Note: when using columns for `args` the provided `range_mask` tuple should still be a constant. 

**Returned value**

- A UInt64 code

Type: [UInt64](../../sql-reference/data-types/int-uint.md)


**Example**

Range expansion can be beneficial when you need a similar distribution for arguments with wildly different ranges (or cardinality)
For example: 'IP Address' (0...FFFFFFFF) and 'Country code' (0...FF).

Query:

```sql
SELECT mortonEncode((1,2), 1024, 16);
```

Result:

```response
1572864
```

Note: tuple size must be equal to the number of the other arguments.

**Example**

Morton encoding for one argument is always the argument itself:

Query:

```sql
SELECT mortonEncode(1);
```

Result:

```response
1
```

**Example**

It is also possible to expand one argument too:

Query:

```sql
SELECT mortonEncode(tuple(2), 128);
```

Result:

```response
32768
```

**Example**

You can also use column names in the function.

Query:

First create the table and insert some data.

```sql
create table morton_numbers(
    n1 UInt32,
    n2 UInt32,
    n3 UInt16,
    n4 UInt16,
    n5 UInt8,
    n6 UInt8,
    n7 UInt8,
    n8 UInt8
)
Engine=MergeTree()
ORDER BY n1 SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
insert into morton_numbers (*) values(1,2,3,4,5,6,7,8);
```
Use column names instead of constants as function arguments to `mortonEncode`

Query:

```sql
SELECT mortonEncode(n1, n2, n3, n4, n5, n6, n7, n8) FROM morton_numbers;
```

Result:

```response
2155374165
```

**implementation details**

Please note that you can fit only so many bits of information into Morton code as [UInt64](../../sql-reference/data-types/int-uint.md) has. Two arguments will have a range of maximum 2^32 (64/2) each, three arguments a range of max 2^21 (64/3) each and so on. All overflow will be clamped to zero.

## mortonDecode

Decodes a Morton encoding (ZCurve) into the corresponding unsigned integer tuple.

As with the `mortonEncode` function, this function has two modes of operation:
- Simple
- Expanded

### Simple mode

Accepts a resulting tuple size as the first argument and the code as the second argument.

**Syntax**

```sql
mortonDecode(tuple_size, code)
```

**Parameters**
- `tuple_size`: integer value no more than 8.
- `code`: [UInt64](../../sql-reference/data-types/int-uint.md) code.

**Returned value**

- [tuple](../../sql-reference/data-types/tuple.md) of the specified size.

Type: [UInt64](../../sql-reference/data-types/int-uint.md)

**Example**

Query:

```sql
SELECT mortonDecode(3, 53);
```

Result:

```response
["1","2","3"]
```

### Expanded mode

Accepts a range mask (tuple) as a first argument and the code as the second argument.
Each number in the mask configures the amount of range shrink:<br/>
1 - no shrink<br/>
2 - 2x shrink<br/> 
3 - 3x shrink<br/>
...<br/>
Up to 8x shrink.<br/>

Range expansion can be beneficial when you need a similar distribution for arguments with wildly different ranges (or cardinality)
For example: 'IP Address' (0...FFFFFFFF) and 'Country code' (0...FF).
As with the encode function, this is limited to 8 numbers at most.

**Example**

Query:

```sql
SELECT mortonDecode(1, 1);
```

Result:

```response
["1"]
```

**Example**

It is also possible to shrink one argument:

Query:

```sql
SELECT mortonDecode(tuple(2), 32768);
```

Result:

```response
["128"]
```

**Example**

You can also use column names in the function.

First create the table and insert some data.

Query:
```sql
create table morton_numbers(
    n1 UInt32,
    n2 UInt32,
    n3 UInt16,
    n4 UInt16,
    n5 UInt8,
    n6 UInt8,
    n7 UInt8,
    n8 UInt8
)
Engine=MergeTree()
ORDER BY n1 SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
insert into morton_numbers (*) values(1,2,3,4,5,6,7,8);
```
Use column names instead of constants as function arguments to `mortonDecode`

Query:

```sql
select untuple(mortonDecode(8, mortonEncode(n1, n2, n3, n4, n5, n6, n7, n8))) from morton_numbers;
```

Result:

```response
1	2	3	4	5	6	7	8
```




