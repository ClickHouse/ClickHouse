# Encoding functions

## char {#char}

Accepts multiple arguments of numeric types. Returns a string with the length as the number of passed arguments and each byte has the value of corresponding argument.

**Syntax**

```sql
char(number_1, [number_2, ..., number_n]);
```

**Parameters**

- `number_1, number_2, ..., number_n` — Numerical arguments interpreted as integers. Types: [Int](../../data_types/int_uint.md), [Float](../../data_types/float.md).

**Returned value**

- UTF-8 string consisting of the characters given by the code values of corresponding argument.

Type: `String`.

**Example**

Query:
```sql
SELECT char(104.1, 101, 108.9, 108.9, 111) AS hello
```

Result:
```text
┌─hello─┐
│ hello │
└───────┘
```

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

## hex

Accepts arguments of types: `String`, `unsigned integer`, `float`, `decimal`, `Date`, or `DateTime`. Returns a string containing the argument's hexadecimal representation. Uses uppercase letters `A-F`. Does not use `0x` prefixes or `h` suffixes. For strings, all bytes are simply encoded as two hexadecimal numbers. Numbers are converted to big endian ("human readable") format. For numbers, older zeros are trimmed, but only by entire bytes. For example, `hex (1) = '01'`. `Date` is encoded as the number of days since the beginning of the Unix epoch. `DateTime` is encoded as the number of seconds since the beginning of the Unix epoch. `float` and `decimal` is encoded as their hexadecimal representation in memory.

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
