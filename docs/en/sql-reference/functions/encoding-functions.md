---
description: 'Documentation for Encoding Functions'
sidebar_label: 'Encoding'
sidebar_position: 65
slug: /sql-reference/functions/encoding-functions
title: 'Encoding Functions'
---

# Encoding Functions

## char {#char}

Returns the string with the length as the number of passed arguments and each byte has the value of corresponding argument. Accepts multiple arguments of numeric types. If the value of argument is out of range of UInt8 data type, it is converted to UInt8 with possible rounding and overflow.

**Syntax**

```sql
char(number_1, [number_2, ..., number_n]);
```

**Arguments**

- `number_1, number_2, ..., number_n` — Numerical arguments interpreted as integers. Types: [Int](../data-types/int-uint.md), [Float](../data-types/float.md).

**Returned value**

- a string of given bytes. [String](../data-types/string.md).

**Example**

Query:

```sql
SELECT char(104.1, 101, 108.9, 108.9, 111) AS hello;
```

Result:

```text
┌─hello─┐
│ hello │
└───────┘
```

You can construct a string of arbitrary encoding by passing the corresponding bytes. Here is example for UTF-8:

Query:

```sql
SELECT char(0xD0, 0xBF, 0xD1, 0x80, 0xD0, 0xB8, 0xD0, 0xB2, 0xD0, 0xB5, 0xD1, 0x82) AS hello;
```

Result:

```text
┌─hello──┐
│ привет │
└────────┘
```

Query:

```sql
SELECT char(0xE4, 0xBD, 0xA0, 0xE5, 0xA5, 0xBD) AS hello;
```

Result:

```text
┌─hello─┐
│ 你好  │
└───────┘
```

## hex {#hex}

Returns a string containing the argument's hexadecimal representation.

Alias: `HEX`.

**Syntax**

```sql
hex(arg)
```

The function is using uppercase letters `A-F` and not using any prefixes (like `0x`) or suffixes (like `h`).

For integer arguments, it prints hex digits ("nibbles") from the most significant to least significant (big-endian or "human-readable" order). It starts with the most significant non-zero byte (leading zero bytes are omitted) but always prints both digits of every byte even if the leading digit is zero.

Values of type [Date](../data-types/date.md) and [DateTime](../data-types/datetime.md) are formatted as corresponding integers (the number of days since Epoch for Date and the value of Unix Timestamp for DateTime).

For [String](../data-types/string.md) and [FixedString](../data-types/fixedstring.md), all bytes are simply encoded as two hexadecimal numbers. Zero bytes are not omitted.

Values of [Float](../data-types/float.md) and [Decimal](../data-types/decimal.md) types are encoded as their representation in memory. As we support little-endian architecture, they are encoded in little-endian. Zero leading/trailing bytes are not omitted.

Values of [UUID](../data-types/uuid.md) type are encoded as big-endian order string.

**Arguments**

- `arg` — A value to convert to hexadecimal. Types: [String](../data-types/string.md), [UInt](../data-types/int-uint.md), [Float](../data-types/float.md), [Decimal](../data-types/decimal.md), [Date](../data-types/date.md) or [DateTime](../data-types/datetime.md).

**Returned value**

- A string with the hexadecimal representation of the argument. [String](../data-types/string.md).

**Examples**

Query:

```sql
SELECT hex(1);
```

Result:

```text
01
```

Query:

```sql
SELECT hex(toFloat32(number)) AS hex_presentation FROM numbers(15, 2);
```

Result:

```text
┌─hex_presentation─┐
│ 00007041         │
│ 00008041         │
└──────────────────┘
```

Query:

```sql
SELECT hex(toFloat64(number)) AS hex_presentation FROM numbers(15, 2);
```

Result:

```text
┌─hex_presentation─┐
│ 0000000000002E40 │
│ 0000000000003040 │
└──────────────────┘
```

Query:

```sql
SELECT lower(hex(toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0'))) AS uuid_hex
```

Result:

```text
┌─uuid_hex─────────────────────────┐
│ 61f0c4045cb311e7907ba6006ad3dba0 │
└──────────────────────────────────┘
```


## unhex {#unhex}

Performs the opposite operation of [hex](#hex). It interprets each pair of hexadecimal digits (in the argument) as a number and converts it to the byte represented by the number. The return value is a binary string (BLOB).

If you want to convert the result to a number, you can use the [reverse](../../sql-reference/functions/string-functions.md#reverse) and [reinterpretAs&lt;Type&gt;](/sql-reference/functions/type-conversion-functions) functions.

:::note
If `unhex` is invoked from within the `clickhouse-client`, binary strings display using UTF-8.
:::

Alias: `UNHEX`.

**Syntax**

```sql
unhex(arg)
```

**Arguments**

- `arg` — A string containing any number of hexadecimal digits. [String](../data-types/string.md), [FixedString](../data-types/fixedstring.md).

Supports both uppercase and lowercase letters `A-F`. The number of hexadecimal digits does not have to be even. If it is odd, the last digit is interpreted as the least significant half of the `00-0F` byte. If the argument string contains anything other than hexadecimal digits, some implementation-defined result is returned (an exception isn't thrown). For a numeric argument the inverse of hex(N) is not performed by unhex().

**Returned value**

- A binary string (BLOB). [String](../data-types/string.md).

**Example**

Query:
```sql
SELECT unhex('303132'), UNHEX('4D7953514C');
```

Result:
```text
┌─unhex('303132')─┬─unhex('4D7953514C')─┐
│ 012             │ MySQL               │
└─────────────────┴─────────────────────┘
```

Query:

```sql
SELECT reinterpretAsUInt64(reverse(unhex('FFF'))) AS num;
```

Result:

```text
┌──num─┐
│ 4095 │
└──────┘
```

## bin {#bin}

Returns a string containing the argument's binary representation.

**Syntax**

```sql
bin(arg)
```

Alias: `BIN`.

For integer arguments, it prints bin digits from the most significant to least significant (big-endian or "human-readable" order). It starts with the most significant non-zero byte (leading zero bytes are omitted) but always prints eight digits of every byte if the leading digit is zero.

Values of type [Date](../data-types/date.md) and [DateTime](../data-types/datetime.md) are formatted as corresponding integers (the number of days since Epoch for `Date` and the value of Unix Timestamp for `DateTime`).

For [String](../data-types/string.md) and [FixedString](../data-types/fixedstring.md), all bytes are simply encoded as eight binary numbers. Zero bytes are not omitted.

Values of [Float](../data-types/float.md) and [Decimal](../data-types/decimal.md) types are encoded as their representation in memory. As we support little-endian architecture, they are encoded in little-endian. Zero leading/trailing bytes are not omitted.

Values of [UUID](../data-types/uuid.md) type are encoded as big-endian order string.

**Arguments**

- `arg` — A value to convert to binary. [String](../data-types/string.md), [FixedString](../data-types/fixedstring.md), [UInt](../data-types/int-uint.md), [Float](../data-types/float.md), [Decimal](../data-types/decimal.md), [Date](../data-types/date.md), or [DateTime](../data-types/datetime.md).

**Returned value**

- A string with the binary representation of the argument. [String](../data-types/string.md).

**Examples**

Query:

```sql
SELECT bin(14);
```

Result:

```text
┌─bin(14)──┐
│ 00001110 │
└──────────┘
```

Query:

```sql
SELECT bin(toFloat32(number)) AS bin_presentation FROM numbers(15, 2);
```

Result:

```text
┌─bin_presentation─────────────────┐
│ 00000000000000000111000001000001 │
│ 00000000000000001000000001000001 │
└──────────────────────────────────┘
```

Query:

```sql
SELECT bin(toFloat64(number)) AS bin_presentation FROM numbers(15, 2);
```

Result:

```text
┌─bin_presentation─────────────────────────────────────────────────┐
│ 0000000000000000000000000000000000000000000000000010111001000000 │
│ 0000000000000000000000000000000000000000000000000011000001000000 │
└──────────────────────────────────────────────────────────────────┘
```

Query:

```sql
SELECT bin(toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0')) AS bin_uuid
```

Result:

```text
┌─bin_uuid─────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ 01100001111100001100010000000100010111001011001100010001111001111001000001111011101001100000000001101010110100111101101110100000 │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```


## unbin {#unbin}

Interprets each pair of binary digits (in the argument) as a number and converts it to the byte represented by the number. The functions performs the opposite operation to [bin](#bin).

**Syntax**

```sql
unbin(arg)
```

Alias: `UNBIN`.

For a numeric argument `unbin()` does not return the inverse of `bin()`. If you want to convert the result to a number, you can use the [reverse](../../sql-reference/functions/string-functions.md#reverse) and [reinterpretAs&lt;Type&gt;](/sql-reference/functions/type-conversion-functions#reinterpret) functions.

:::note
If `unbin` is invoked from within the `clickhouse-client`, binary strings are displayed using UTF-8.
:::

Supports binary digits `0` and `1`. The number of binary digits does not have to be multiples of eight. If the argument string contains anything other than binary digits, some implementation-defined result is returned (an exception isn't thrown).

**Arguments**

- `arg` — A string containing any number of binary digits. [String](../data-types/string.md).

**Returned value**

- A binary string (BLOB). [String](../data-types/string.md).

**Examples**

Query:

```sql
SELECT UNBIN('001100000011000100110010'), UNBIN('0100110101111001010100110101000101001100');
```

Result:

```text
┌─unbin('001100000011000100110010')─┬─unbin('0100110101111001010100110101000101001100')─┐
│ 012                               │ MySQL                                             │
└───────────────────────────────────┴───────────────────────────────────────────────────┘
```

Query:

```sql
SELECT reinterpretAsUInt64(reverse(unbin('1110'))) AS num;
```

Result:

```text
┌─num─┐
│  14 │
└─────┘
```

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

- `arg` — Integer value. [Int/UInt](../data-types/int-uint.md).

**Returned value**

- An array containing a list of positions of bits that equal `1`, in ascending order. [Array](../data-types/array.md)([UInt64](../data-types/int-uint.md)).

**Example**

Query:

```sql
SELECT bitPositionsToArray(toInt8(1)) AS bit_positions;
```

Result:

```text
┌─bit_positions─┐
│ [0]           │
└───────────────┘
```

Query:

```sql
SELECT bitPositionsToArray(toInt8(-1)) AS bit_positions;
```

Result:

```text
┌─bit_positions─────┐
│ [0,1,2,3,4,5,6,7] │
└───────────────────┘
```

## mortonEncode {#mortonencode}

Calculates the Morton encoding (ZCurve) for a list of unsigned integers.

The function has two modes of operation:
- Simple
- Expanded

### Simple mode {#simple-mode}

Accepts up to 8 unsigned integers as arguments and produces a UInt64 code.

**Syntax**

```sql
mortonEncode(args)
```

**Parameters**

- `args`: up to 8 [unsigned integers](../data-types/int-uint.md) or columns of the aforementioned type.

**Returned value**

- A UInt64 code. [UInt64](../data-types/int-uint.md)

**Example**

Query:

```sql
SELECT mortonEncode(1, 2, 3);
```
Result:

```response
53
```

### Expanded mode {#expanded-mode}

Accepts a range mask ([tuple](../data-types/tuple.md)) as a first argument and up to 8 [unsigned integers](../data-types/int-uint.md) as other arguments.

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
- `args`: up to 8 [unsigned integers](../data-types/int-uint.md) or columns of the aforementioned type.

Note: when using columns for `args` the provided `range_mask` tuple should still be a constant.

**Returned value**

- A UInt64 code. [UInt64](../data-types/int-uint.md)


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
CREATE TABLE morton_numbers(
    n1 UInt32,
    n2 UInt32,
    n3 UInt16,
    n4 UInt16,
    n5 UInt8,
    n6 UInt8,
    n7 UInt8,
    n8 UInt8
)
ENGINE=MergeTree()
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

Please note that you can fit only so many bits of information into Morton code as [UInt64](../data-types/int-uint.md) has. Two arguments will have a range of maximum 2^32 (64/2) each, three arguments a range of max 2^21 (64/3) each and so on. All overflow will be clamped to zero.

## mortonDecode {#mortondecode}

Decodes a Morton encoding (ZCurve) into the corresponding unsigned integer tuple.

As with the `mortonEncode` function, this function has two modes of operation:
- Simple
- Expanded

### Simple mode {#simple-mode-1}

Accepts a resulting tuple size as the first argument and the code as the second argument.

**Syntax**

```sql
mortonDecode(tuple_size, code)
```

**Parameters**
- `tuple_size`: integer value no more than 8.
- `code`: [UInt64](../data-types/int-uint.md) code.

**Returned value**

- [tuple](../data-types/tuple.md) of the specified size. [UInt64](../data-types/int-uint.md)

**Example**

Query:

```sql
SELECT mortonDecode(3, 53);
```

Result:

```response
["1","2","3"]
```

### Expanded mode {#expanded-mode-1}

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
CREATE TABLE morton_numbers(
    n1 UInt32,
    n2 UInt32,
    n3 UInt16,
    n4 UInt16,
    n5 UInt8,
    n6 UInt8,
    n7 UInt8,
    n8 UInt8
)
ENGINE=MergeTree()
ORDER BY n1 SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
insert into morton_numbers (*) values(1,2,3,4,5,6,7,8);
```
Use column names instead of constants as function arguments to `mortonDecode`

Query:

```sql
SELECT untuple(mortonDecode(8, mortonEncode(n1, n2, n3, n4, n5, n6, n7, n8))) FROM morton_numbers;
```

Result:

```response
1    2    3    4    5    6    7    8
```

## hilbertEncode {#hilbertencode}

Calculates code for Hilbert Curve for a list of unsigned integers.

The function has two modes of operation:
- Simple
- Expanded

### Simple mode {#simple-mode-2}

Simple: accepts up to 2 unsigned integers as arguments and produces a UInt64 code.

**Syntax**

```sql
hilbertEncode(args)
```

**Parameters**

- `args`: up to 2 [unsigned integers](../../sql-reference/data-types/int-uint.md) or columns of the aforementioned type.

**Returned value**

- A UInt64 code

Type: [UInt64](../../sql-reference/data-types/int-uint.md)

**Example**

Query:

```sql
SELECT hilbertEncode(3, 4);
```
Result:

```response
31
```

### Expanded mode {#expanded-mode-2}

Accepts a range mask ([tuple](../../sql-reference/data-types/tuple.md)) as a first argument and up to 2 [unsigned integers](../../sql-reference/data-types/int-uint.md) as other arguments.

Each number in the mask configures the number of bits by which the corresponding argument will be shifted left, effectively scaling the argument within its range.

**Syntax**

```sql
hilbertEncode(range_mask, args)
```

**Parameters**
- `range_mask`: ([tuple](../../sql-reference/data-types/tuple.md))
- `args`: up to 2 [unsigned integers](../../sql-reference/data-types/int-uint.md) or columns of the aforementioned type.

Note: when using columns for `args` the provided `range_mask` tuple should still be a constant.

**Returned value**

- A UInt64 code

Type: [UInt64](../../sql-reference/data-types/int-uint.md)


**Example**

Range expansion can be beneficial when you need a similar distribution for arguments with wildly different ranges (or cardinality)
For example: 'IP Address' (0...FFFFFFFF) and 'Country code' (0...FF).

Query:

```sql
SELECT hilbertEncode((10,6), 1024, 16);
```

Result:

```response
4031541586602
```

Note: tuple size must be equal to the number of the other arguments.

**Example**

For a single argument without a tuple, the function returns the argument itself as the Hilbert index, since no dimensional mapping is needed.

Query:

```sql
SELECT hilbertEncode(1);
```

Result:

```response
1
```

**Example**

If a single argument is provided with a tuple specifying bit shifts, the function shifts the argument left by the specified number of bits.

Query:

```sql
SELECT hilbertEncode(tuple(2), 128);
```

Result:

```response
512
```

**Example**

The function also accepts columns as arguments:

Query:

First create the table and insert some data.

```sql
CREATE TABLE hilbert_numbers(
    n1 UInt32,
    n2 UInt32
)
ENGINE=MergeTree()
ORDER BY n1 SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
insert into hilbert_numbers (*) values(1,2);
```
Use column names instead of constants as function arguments to `hilbertEncode`

Query:

```sql
SELECT hilbertEncode(n1, n2) FROM hilbert_numbers;
```

Result:

```response
13
```

**implementation details**

Please note that you can fit only so many bits of information into Hilbert code as [UInt64](../../sql-reference/data-types/int-uint.md) has. Two arguments will have a range of maximum 2^32 (64/2) each. All overflow will be clamped to zero.

## hilbertDecode {#hilbertdecode}

Decodes a Hilbert curve index back into a tuple of unsigned integers, representing coordinates in multi-dimensional space.

As with the `hilbertEncode` function, this function has two modes of operation:
- Simple
- Expanded

### Simple mode {#simple-mode-3}

Accepts up to 2 unsigned integers as arguments and produces a UInt64 code.

**Syntax**

```sql
hilbertDecode(tuple_size, code)
```

**Parameters**
- `tuple_size`: integer value no more than 2.
- `code`: [UInt64](../../sql-reference/data-types/int-uint.md) code.

**Returned value**

- [tuple](../../sql-reference/data-types/tuple.md) of the specified size.

Type: [UInt64](../../sql-reference/data-types/int-uint.md)

**Example**

Query:

```sql
SELECT hilbertDecode(2, 31);
```

Result:

```response
["3", "4"]
```

### Expanded mode {#expanded-mode-3}

Accepts a range mask (tuple) as a first argument and up to 2 unsigned integers as other arguments.
Each number in the mask configures the number of bits by which the corresponding argument will be shifted left, effectively scaling the argument within its range.

Range expansion can be beneficial when you need a similar distribution for arguments with wildly different ranges (or cardinality)
For example: 'IP Address' (0...FFFFFFFF) and 'Country code' (0...FF).
As with the encode function, this is limited to 8 numbers at most.

**Example**

Hilbert code for one argument is always the argument itself (as a tuple).

Query:

```sql
SELECT hilbertDecode(1, 1);
```

Result:

```response
["1"]
```

**Example**

A single argument with a tuple specifying bit shifts will be right-shifted accordingly.

Query:

```sql
SELECT hilbertDecode(tuple(2), 32768);
```

Result:

```response
["128"]
```

**Example**

The function accepts a column of codes as a second argument:

First create the table and insert some data.

Query:
```sql
CREATE TABLE hilbert_numbers(
    n1 UInt32,
    n2 UInt32
)
ENGINE=MergeTree()
ORDER BY n1 SETTINGS index_granularity = 8192, index_granularity_bytes = '10Mi';
insert into hilbert_numbers (*) values(1,2);
```
Use column names instead of constants as function arguments to `hilbertDecode`

Query:

```sql
SELECT untuple(hilbertDecode(2, hilbertEncode(n1, n2))) FROM hilbert_numbers;
```

Result:

```response
1    2
```

## bech32Encode {#bech32encode}

Encodes a binary data string, along with a human-readable part (HRP), using the [Bech32 or Bech32m](https://en.bitcoin.it/wiki/Bech32) algorithms.

**Syntax**

```sql
bech32Encode(hrp, data[, witver])
```

**Parameters**

- `hrp` — String of 1 - 83 lowercase characters specifying the "human-readable part" of the code. Usually 'bc' or 'tb'. [String](../data-types/string.md) or [FixedString](../data-types/fixedstring.md).
- `data` — String of binary data to encode. [String](../data-types/string.md) or [FixedString](../data-types/fixedstring.md).
- `witver` - Witness version. Optional, default = 1. An [UInt*](../data-types/int-uint.md) specifying the version of the algorithm to run. 0 for Bech32 and 1 or greater for Bech32m.

:::note
When using the [FixedString](../data-types/fixedstring.md) data type, if a value does not fully fill the row it is padded with null characters.
While the `bech32Encode` function will handle this automatically for the hrp argument, for the data argument the values must not be padded.
For this reason it is not recommended to use the [FixedString](../data-types/fixedstring.md) data type for your data values unless you are certain that they are all the same length and ensure that your [FixedString](../data-types/fixedstring.md) column is set to that length as well.
:::

**Returned value**

- A Bech32 address string, consisting of the human-readable part, a separator character which is always '1', and a data part. The length of the string will never exceed 90 characters. If the algorithm cannot generate a valid address from the input, it will return an empty string.

Type: [String](../data-types/string.md).

**Example**

When no witness version is supplied, the default is 1, the updated Bech32m algorithm.

Query:

```sql
SELECT bech32Encode('bc', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'));
```

Result:

```response
bc1w508d6qejxtdg4y5r3zarvary0c5xw7k8zcwmq
```

A witness version of 0 will result in a different address string.

Query:

```sql
SELECT bech32Encode('bc', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'), 0);
```

Result:

```response
bc1w508d6qejxtdg4y5r3zarvary0c5xw7kj7gz7z
```

While 'bc' (Mainnet) and 'tb' (Testnet) are the only allowed hrp values for the SegWit address format, Bech32 allows any hrp that satisfies the above requirements.

Query:

```sql
SELECT bech32Encode('abcdefg', unhex('751e76e8199196d454941c45d1b3a323f1433bd6'), 10);
```

Result:

```response
abcdefg1w508d6qejxtdg4y5r3zarvary0c5xw7k9rp8r4
```

## bech32Decode {#bech32decode}

Decodes a Bech32 address string generated by either the bech32 or bech32m algorithms.

**Syntax**

```sql
bech32Decode(address)
```

**Parameters**

- `address` — Bech32 string to decode. [String](../data-types/string.md) or [FixedString](../data-types/fixedstring.md).

:::note
Unlike the encode function, `Bech32Decode` will automatically handle padded [FixedStrings](../data-types/fixedstring.md).
:::

**Returned value**

- A tuple consisting of the (hrp, data) that was used to encode the string. The data is in binary format.

Type: ([String](../data-types/string.md), [String](../data-types/string.md)).

**Example**

Query:

```sql
SELECT tup.1 AS hrp, hex(tup.2) AS data FROM (SELECT bech32Decode('bc1w508d6qejxtdg4y5r3zarvary0c5xw7kj7gz7z') AS tup);
```

Result:

```response
bc   751E76E8199196D454941C45D1B3A323F1433BD6
```

Query:

```sql
SELECT tup.1 AS hrp, hex(tup.2) AS data FROM (SELECT bech32Decode('tb1w508d6qejxtdg4y5r3zarvary0c5xw7kzp034v') AS tup);
```

Result:

```response
tb   751E76E8199196D454941C45D1B3A323F1433BD6
```

<!-- 
The inner content of the tags below are replaced at doc framework build time with 
docs generated from system.functions. Please do not modify or remove the tags.
See: https://github.com/ClickHouse/clickhouse-docs/blob/main/contribute/autogenerated-documentation-from-source.md
-->

<!--AUTOGENERATED_START-->
<!--AUTOGENERATED_END-->
