# Encoding functions

## char
Accepts multiple arguments of numberic types. Returns a string with the length as the number of passed arguments and each byte has the value of corresponding argument.

## hex {#hex}

Returns a string containing the argument's hexadecimal representation. 

**Syntax**

```sql
hex(arg)
```

The result of the function depends on the type of argument. Uses uppercase letters `A-F`. Does not use `0x` prefixes or `h` suffixes. 

For strings, all bytes are simply encoded as two hexadecimal numbers. 

Numbers are converted to big engine ("human readable") format. For numbers, older zeros are trimmed, but only by entire bytes.

For example:

- `Date` is encoded as the number of days since the beginning of the Unix epoch. 
- `DateTime` is encoded as the number of seconds since the beginning of the Unix epoch. 
- `Float` and `Decimal` is encoded as their hexadecimal representation in memory.

**Parameters**

- `arg` — A value to convert to hexadecimal. Types: [String](../../data_types/string.md), [UInt](../../data_types/int_uint.md), [Float](../../data_types/float.md), [Decimal](../../data_types/decimal.md), [Date](../../data_types/date.md) or [DateTime](../../data_types/datetime.md).

**Returned value**

- A string with the hexadecimal representation of the argument.

Type: `String`.

**Example**

Query:

```sql
SELECT hex(toFloat32(number)) as hex_presentation FROM numbers(15, 2);
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
SELECT hex(toFloat64(number)) as hex_presentation FROM numbers(15, 2);
```

Result:

```text
┌─hex_presentation─┐
│ 0000000000002E40 │
│ 0000000000003040 │
└──────────────────┘
```

## unhex(str)

Accepts a string containing any number of hexadecimal digits, and returns a string containing the corresponding bytes. Supports both uppercase and lowercase letters A-F. The number of hexadecimal digits does not have to be even. If it is odd, the last digit is interpreted as the younger half of the 00-0F byte. If the argument string contains anything other than hexadecimal digits, some implementation-defined result is returned (an exception isn't thrown).
If you want to convert the result to a number, you can use the 'reverse' and 'reinterpretAsType' functions.

## UUIDStringToNum(str)

Accepts a string containing 36 characters in the format `123e4567-e89b-12d3-a456-426655440000`, and returns it as a set of bytes in a FixedString(16).

## UUIDNumToString(str)

Accepts a FixedString(16) value. Returns a string containing 36 characters in text format.

## bitmaskToList(num)

Accepts an integer. Returns a string containing the list of powers of two that total the source number when summed. They are comma-separated without spaces in text format, in ascending order.

## bitmaskToArray(num)

Accepts an integer. Returns an array of UInt64 numbers containing the list of powers of two that total the source number when summed. Numbers in the array are in ascending order.


[Original article](https://clickhouse.yandex/docs/en/query_language/functions/encoding_functions/) <!--hide-->
