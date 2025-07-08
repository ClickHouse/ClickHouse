---
description: 'Documentation for Functions for Working with Strings'
sidebar_label: 'Strings'
sidebar_position: 170
slug: /sql-reference/functions/string-functions
title: 'Functions for Working with Strings'
---

import VersionBadge from '@theme/badges/VersionBadge';

# Functions for Working with Strings

Functions for [searching](string-search-functions.md) in strings and for [replacing](string-replace-functions.md) in strings are described separately.

## empty {#empty}

Checks whether the input string is empty. A string is considered non-empty if it contains at least one byte, even if this byte is a space or the null byte.

The function is also available for [arrays](/sql-reference/functions/array-functions#empty) and [UUIDs](uuid-functions.md#empty).

**Syntax**

```sql
empty(x)
```

**Arguments**

- `x` — Input value. [String](../data-types/string.md).

**Returned value**

- Returns `1` for an empty string or `0` for a non-empty string. [UInt8](../data-types/int-uint.md).

**Example**

```sql
SELECT empty('');
```

Result:

```result
┌─empty('')─┐
│         1 │
└───────────┘
```

## notEmpty {#notempty}

Checks whether the input string is non-empty. A string is considered non-empty if it contains at least one byte, even if this byte is a space or the null byte.

The function is also available for [arrays](/sql-reference/functions/array-functions#notEmpty) and [UUIDs](uuid-functions.md#notempty).

**Syntax**

```sql
notEmpty(x)
```

**Arguments**

- `x` — Input value. [String](../data-types/string.md).

**Returned value**

- Returns `1` for a non-empty string or `0` for an empty string string. [UInt8](../data-types/int-uint.md).

**Example**

```sql
SELECT notEmpty('text');
```

Result:

```result
┌─notEmpty('text')─┐
│                1 │
└──────────────────┘
```

## length {#length}

Returns the length of a string in bytes rather than in characters or Unicode code points. The function also works for arrays.

Alias: `OCTET_LENGTH`

**Syntax**

```sql
length(s)
```

**Parameters**

- `s` — An input string or array. [String](../data-types/string)/[Array](../data-types/array).

**Returned value**

- Length of the string or array `s` in bytes. [UInt64](../data-types/int-uint).

**Example**

Query:

```sql
SELECT length('Hello, world!');
```

Result: 

```response
┌─length('Hello, world!')─┐
│                      13 │
└─────────────────────────┘
```

Query:

```sql
SELECT length([1, 2, 3, 4]);
```

Result: 

```response
┌─length([1, 2, 3, 4])─┐
│                    4 │
└──────────────────────┘
```


## lengthUTF8 {#lengthutf8}

Returns the length of a string in Unicode code points rather than in bytes or characters. It assumes that the string contains valid UTF-8 encoded text. If this assumption is violated, no exception is thrown and the result is undefined.

Aliases:
- `CHAR_LENGTH`
- `CHARACTER_LENGTH`

**Syntax**

```sql
lengthUTF8(s)
```

**Parameters**

- `s` — String containing valid UTF-8 encoded text. [String](../data-types/string).

**Returned value**

- Length of the string `s` in Unicode code points. [UInt64](../data-types/int-uint.md).

**Example**

Query:

```sql
SELECT lengthUTF8('Здравствуй, мир!');
```

Result: 

```response
┌─lengthUTF8('Здравствуй, мир!')─┐
│                             16 │
└────────────────────────────────┘
```

## left {#left}

Returns a substring of string `s` with a specified `offset` starting from the left.

**Syntax**

```sql
left(s, offset)
```

**Parameters**

- `s` — The string to calculate a substring from. [String](../data-types/string.md) or [FixedString](../data-types/fixedstring.md).
- `offset` — The number of bytes of the offset. [(U)Int*](../data-types/int-uint).

**Returned value**

- For positive `offset`: A substring of `s` with `offset` many bytes, starting from the left of the string.
- For negative `offset`: A substring of `s` with `length(s) - |offset|` bytes, starting from the left of the string.
- An empty string if `length` is 0.

**Example**

Query:

```sql
SELECT left('Hello', 3);
```

Result:

```response
Hel
```

Query:

```sql
SELECT left('Hello', -3);
```

Result:

```response
He
```

## leftUTF8 {#leftutf8}

Returns a substring of a UTF-8 encoded string `s` with a specified `offset` starting from the left.

**Syntax**

```sql
leftUTF8(s, offset)
```

**Parameters**

- `s` — The UTF-8 encoded string to calculate a substring from. [String](../data-types/string.md) or [FixedString](../data-types/fixedstring.md).
- `offset` — The number of bytes of the offset. [(U)Int*](../data-types/int-uint).

**Returned value**

- For positive `offset`: A substring of `s` with `offset` many bytes, starting from the left of the string.
- For negative `offset`: A substring of `s` with `length(s) - |offset|` bytes, starting from the left of the string.
- An empty string if `length` is 0.

**Example**

Query:

```sql
SELECT leftUTF8('Привет', 4);
```

Result:

```response
Прив
```

Query:

```sql
SELECT leftUTF8('Привет', -4);
```

Result:

```response
Пр
```

## leftPad {#leftpad}

Pads a string from the left with spaces or with a specified string (multiple times, if needed) until the resulting string reaches the specified `length`.

**Syntax**

```sql
leftPad(string, length[, pad_string])
```

Alias: `LPAD`

**Arguments**

- `string` — Input string that should be padded. [String](../data-types/string.md).
- `length` — The length of the resulting string. [UInt or Int](../data-types/int-uint.md). If the value is smaller than the input string length, then the input string is shortened to `length` characters.
- `pad_string` — The string to pad the input string with. [String](../data-types/string.md). Optional. If not specified, then the input string is padded with spaces.

**Returned value**

- A left-padded string of the given length. [String](../data-types/string.md).

**Example**

```sql
SELECT leftPad('abc', 7, '*'), leftPad('def', 7);
```

Result:

```result
┌─leftPad('abc', 7, '*')─┬─leftPad('def', 7)─┐
│ ****abc                │     def           │
└────────────────────────┴───────────────────┘
```

## leftPadUTF8 {#leftpadutf8}

Pads the string from the left with spaces or a specified string (multiple times, if needed) until the resulting string reaches the given length. Unlike [leftPad](#leftpad) which measures the string length in bytes, the string length is measured in code points.

**Syntax**

```sql
leftPadUTF8(string, length[, pad_string])
```

**Arguments**

- `string` — Input string that should be padded. [String](../data-types/string.md).
- `length` — The length of the resulting string. [UInt or Int](../data-types/int-uint.md). If the value is smaller than the input string length, then the input string is shortened to `length` characters.
- `pad_string` — The string to pad the input string with. [String](../data-types/string.md). Optional. If not specified, then the input string is padded with spaces.

**Returned value**

- A left-padded string of the given length. [String](../data-types/string.md).

**Example**

```sql
SELECT leftPadUTF8('абвг', 7, '*'), leftPadUTF8('дежз', 7);
```

Result:

```result
┌─leftPadUTF8('абвг', 7, '*')─┬─leftPadUTF8('дежз', 7)─┐
│ ***абвг                     │    дежз                │
└─────────────────────────────┴────────────────────────┘
```

## right {#right}

Returns a substring of string `s` with a specified `offset` starting from the right.

**Syntax**

```sql
right(s, offset)
```

**Parameters**

- `s` — The string to calculate a substring from. [String](../data-types/string.md) or [FixedString](../data-types/fixedstring.md).
- `offset` — The number of bytes of the offset. [(U)Int*](../data-types/int-uint).

**Returned value**

- For positive `offset`: A substring of `s` with `offset` many bytes, starting from the right of the string.
- For negative `offset`: A substring of `s` with `length(s) - |offset|` bytes, starting from the right of the string.
- An empty string if `length` is 0.

**Example**

Query:

```sql
SELECT right('Hello', 3);
```

Result:

```response
llo
```

Query:

```sql
SELECT right('Hello', -3);
```

Result:

```response
lo
```

## rightUTF8 {#rightutf8}

Returns a substring of UTF-8 encoded string `s` with a specified `offset` starting from the right.

**Syntax**

```sql
rightUTF8(s, offset)
```

**Parameters**

- `s` — The UTF-8 encoded string to calculate a substring from. [String](../data-types/string.md) or [FixedString](../data-types/fixedstring.md).
- `offset` — The number of bytes of the offset. [(U)Int*](../data-types/int-uint).

**Returned value**

- For positive `offset`: A substring of `s` with `offset` many bytes, starting from the right of the string.
- For negative `offset`: A substring of `s` with `length(s) - |offset|` bytes, starting from the right of the string.
- An empty string if `length` is 0.

**Example**

Query:

```sql
SELECT rightUTF8('Привет', 4);
```

Result:

```response
ивет
```

Query:

```sql
SELECT rightUTF8('Привет', -4);
```

Result:

```response
ет
```

## rightPad {#rightpad}

Pads a string from the right with spaces or with a specified string (multiple times, if needed) until the resulting string reaches the specified `length`.

**Syntax**

```sql
rightPad(string, length[, pad_string])
```

Alias: `RPAD`

**Arguments**

- `string` — Input string that should be padded. [String](../data-types/string.md).
- `length` — The length of the resulting string. [UInt or Int](../data-types/int-uint.md). If the value is smaller than the input string length, then the input string is shortened to `length` characters.
- `pad_string` — The string to pad the input string with. [String](../data-types/string.md). Optional. If not specified, then the input string is padded with spaces.

**Returned value**

- A left-padded string of the given length. [String](../data-types/string.md).

**Example**

```sql
SELECT rightPad('abc', 7, '*'), rightPad('abc', 7);
```

Result:

```result
┌─rightPad('abc', 7, '*')─┬─rightPad('abc', 7)─┐
│ abc****                 │ abc                │
└─────────────────────────┴────────────────────┘
```

## rightPadUTF8 {#rightpadutf8}

Pads the string from the right with spaces or a specified string (multiple times, if needed) until the resulting string reaches the given length. Unlike [rightPad](#rightpad) which measures the string length in bytes, the string length is measured in code points.

**Syntax**

```sql
rightPadUTF8(string, length[, pad_string])
```

**Arguments**

- `string` — Input string that should be padded. [String](../data-types/string.md).
- `length` — The length of the resulting string. [UInt or Int](../data-types/int-uint.md). If the value is smaller than the input string length, then the input string is shortened to `length` characters.
- `pad_string` — The string to pad the input string with. [String](../data-types/string.md). Optional. If not specified, then the input string is padded with spaces.

**Returned value**

- A right-padded string of the given length. [String](../data-types/string.md).

**Example**

```sql
SELECT rightPadUTF8('абвг', 7, '*'), rightPadUTF8('абвг', 7);
```

Result:

```result
┌─rightPadUTF8('абвг', 7, '*')─┬─rightPadUTF8('абвг', 7)─┐
│ абвг***                      │ абвг                    │
└──────────────────────────────┴─────────────────────────┘
```

## compareSubstrings {#comparesubstrings}

Compare two strings lexicographically.

**Syntax**

```sql
compareSubstrings(string1, string2, string1_offset, string2_offset, num_bytes);
```

**Arguments**

- `string1` — The first string to compare. [String](../data-types/string.md)
- `string2` - The second string to compare.[String](../data-types/string.md)
- `string1_offset` — The position (zero-based) in `string1` from which the comparison starts. [UInt*](../data-types/int-uint.md).
- `string2_offset` — The position (zero-based index) in `string2` from which the comparison starts. [UInt*](../data-types/int-uint.md).
- `num_bytes` — The maximum number of bytes to compare in both strings. If `string_offset` + `num_bytes` exceeds the end of an input string, `num_bytes` will be reduced accordingly. [UInt*](../data-types/int-uint.md).

**Returned value**

- -1 — If `string1`[`string1_offset` : `string1_offset` + `num_bytes`] < `string2`[`string2_offset` : `string2_offset` + `num_bytes`].
- 0 — If `string1`[`string1_offset` : `string1_offset` + `num_bytes`] = `string2`[`string2_offset` : `string2_offset` + `num_bytes`].
- 1 — If `string1`[`string1_offset` : `string1_offset` + `num_bytes`] > `string2`[`string2_offset` : `string2_offset` + `num_bytes`].

**Example**

Query:

```sql
SELECT compareSubstrings('Saxony', 'Anglo-Saxon', 0, 6, 5) AS result,
```

Result:

```result
┌─result─┐
│      0 │
└────────┘
```

## lower {#lower}

Converts the ASCII Latin symbols in a string to lowercase.

*Syntax**

```sql
lower(input)
```

Alias: `lcase`

**Parameters**

- `input`: A string type [String](../data-types/string.md).

**Returned value**

- A [String](../data-types/string.md) data type value.

**Example**

Query:

```sql
SELECT lower('CLICKHOUSE');
```

```response
┌─lower('CLICKHOUSE')─┐
│ clickhouse          │
└─────────────────────┘
```

## upper {#upper}

Converts the ASCII Latin symbols in a string to uppercase.

**Syntax**

```sql
upper(input)
```

Alias: `ucase`

**Parameters**

- `input` — A string type [String](../data-types/string.md).

**Returned value**

- A [String](../data-types/string.md) data type value.

**Examples**

Query:

```sql
SELECT upper('clickhouse');
```

```response
┌─upper('clickhouse')─┐
│ CLICKHOUSE          │
└─────────────────────┘
```

## lowerUTF8 {#lowerutf8}

Converts a string to lowercase, assuming that the string contains valid UTF-8 encoded text. If this assumption is violated, no exception is thrown and the result is undefined.

:::note
Does not detect the language, e.g. for Turkish the result might not be exactly correct (i/İ vs. i/I). If the length of the UTF-8 byte sequence is different for upper and lower case of a code point (such as `ẞ` and `ß`), the result may be incorrect for this code point.
:::

**Syntax**

```sql
lowerUTF8(input)
```

**Parameters**

- `input` — A string type [String](../data-types/string.md).

**Returned value**

- A [String](../data-types/string.md) data type value.

**Example**

Query:

```sql
SELECT lowerUTF8('MÜNCHEN') AS Lowerutf8;
```

Result:

```response
┌─Lowerutf8─┐
│ münchen   │
└───────────┘
```

## upperUTF8 {#upperutf8}

Converts a string to uppercase, assuming that the string contains valid UTF-8 encoded text. If this assumption is violated, no exception is thrown and the result is undefined.

:::note
Does not detect the language, e.g. for Turkish the result might not be exactly correct (i/İ vs. i/I). If the length of the UTF-8 byte sequence is different for upper and lower case of a code point (such as `ẞ` and `ß`), the result may be incorrect for this code point.
:::

**Syntax**

```sql
upperUTF8(input)
```

**Parameters**

- `input` — A string type [String](../data-types/string.md).

**Returned value**

- A [String](../data-types/string.md) data type value.

**Example**

Query:

```sql
SELECT upperUTF8('München') AS Upperutf8;
```

Result:

```response
┌─Upperutf8─┐
│ MÜNCHEN   │
└───────────┘
```

## isValidUTF8 {#isvalidutf8}

Returns 1, if the set of bytes constitutes valid UTF-8-encoded text, otherwise 0.

**Syntax**

```sql
isValidUTF8(input)
```

**Parameters**

- `input` — A string type [String](../data-types/string.md).

**Returned value**

- Returns `1`, if the set of bytes constitutes valid UTF-8-encoded text, otherwise `0`.

Query:

```sql
SELECT isValidUTF8('\xc3\xb1') AS valid, isValidUTF8('\xc3\x28') AS invalid;
```

Result:

```response
┌─valid─┬─invalid─┐
│     1 │       0 │
└───────┴─────────┘
```

## toValidUTF8 {#tovalidutf8}

Replaces invalid UTF-8 characters by the `�` (U+FFFD) character. All running in a row invalid characters are collapsed into the one replacement character.

**Syntax**

```sql
toValidUTF8(input_string)
```

**Arguments**

- `input_string` — Any set of bytes represented as the [String](../data-types/string.md) data type object.

**Returned value**

- A valid UTF-8 string.

**Example**

```sql
SELECT toValidUTF8('\x61\xF0\x80\x80\x80b');
```

```result
┌─toValidUTF8('a����b')─┐
│ a�b                   │
└───────────────────────┘
```

## repeat {#repeat}

Concatenates a string as many times with itself as specified.

**Syntax**

```sql
repeat(s, n)
```

Alias: `REPEAT`

**Arguments**

- `s` — The string to repeat. [String](../data-types/string.md).
- `n` — The number of times to repeat the string. [UInt* or Int*](../data-types/int-uint.md).

**Returned value**

A string containing string `s` repeated `n` times. If `n` &lt;= 0, the function returns the empty string. [String](../data-types/string.md).

**Example**

```sql
SELECT repeat('abc', 10);
```

Result:

```result
┌─repeat('abc', 10)──────────────┐
│ abcabcabcabcabcabcabcabcabcabc │
└────────────────────────────────┘
```

## space {#space}

Concatenates a space (` `) as many times with itself as specified.

**Syntax**

```sql
space(n)
```

Alias: `SPACE`.

**Arguments**

- `n` — The number of times to repeat the space. [UInt* or Int*](../data-types/int-uint.md).

**Returned value**

The string containing string ` ` repeated `n` times. If `n` &lt;= 0, the function returns the empty string. [String](../data-types/string.md).

**Example**

Query:

```sql
SELECT space(3);
```

Result:

```text
┌─space(3) ────┐
│              │
└──────────────┘
```

## reverse {#reverse}

Reverses the sequence of bytes in a string.

## reverseUTF8 {#reverseutf8}

Reverses a sequence of Unicode code points in a string. Assumes that the string contains valid UTF-8 encoded text. If this assumption is violated, no exception is thrown and the result is undefined.

## concat {#concat}

Concatenates the given arguments.

**Syntax**

```sql
concat(s1, s2, ...)
```

**Arguments**

Values of arbitrary type.

Arguments which are not of types [String](../data-types/string.md) or [FixedString](../data-types/fixedstring.md) are converted to strings using their default serialization. As this decreases performance, it is not recommended to use non-String/FixedString arguments.

**Returned values**

The String created by concatenating the arguments.

If any of arguments is `NULL`, the function returns `NULL`.

**Example**

Query:

```sql
SELECT concat('Hello, ', 'World!');
```

Result:

```result
┌─concat('Hello, ', 'World!')─┐
│ Hello, World!               │
└─────────────────────────────┘
```

Query:

```sql
SELECT concat(42, 144);
```

Result:

```result
┌─concat(42, 144)─┐
│ 42144           │
└─────────────────┘
```

:::note `||` operator
Use the || operator for string concatenation as a concise alternative to `concat()`. For example, `'Hello, ' || 'World!'` is equivalent to `concat('Hello, ', 'World!')`.
:::

## concatAssumeInjective {#concatassumeinjective}

Like [concat](#concat) but assumes that `concat(s1, s2, ...) → sn` is injective. Can be used for optimization of GROUP BY.

A function is called injective if it returns for different arguments different results. In other words: different arguments never produce identical result.

**Syntax**

```sql
concatAssumeInjective(s1, s2, ...)
```

**Arguments**

Values of type String or FixedString.

**Returned values**

The String created by concatenating the arguments.

If any of argument values is `NULL`, the function returns `NULL`.

**Example**

Input table:

```sql
CREATE TABLE key_val(`key1` String, `key2` String, `value` UInt32) ENGINE = TinyLog;
INSERT INTO key_val VALUES ('Hello, ','World',1), ('Hello, ','World',2), ('Hello, ','World!',3), ('Hello',', World!',2);
SELECT * from key_val;
```

```result
┌─key1────┬─key2─────┬─value─┐
│ Hello,  │ World    │     1 │
│ Hello,  │ World    │     2 │
│ Hello,  │ World!   │     3 │
│ Hello   │ , World! │     2 │
└─────────┴──────────┴───────┘
```

```sql
SELECT concat(key1, key2), sum(value) FROM key_val GROUP BY concatAssumeInjective(key1, key2);
```

Result:

```result
┌─concat(key1, key2)─┬─sum(value)─┐
│ Hello, World!      │          3 │
│ Hello, World!      │          2 │
│ Hello, World       │          3 │
└────────────────────┴────────────┘
```

## concatWithSeparator {#concatwithseparator}

Concatenates the given strings with a given separator.

**Syntax**

```sql
concatWithSeparator(sep, expr1, expr2, expr3...)
```

Alias: `concat_ws`

**Arguments**

- sep — separator. Const [String](../data-types/string.md) or [FixedString](../data-types/fixedstring.md).
- exprN — expression to be concatenated. Arguments which are not of types [String](../data-types/string.md) or [FixedString](../data-types/fixedstring.md) are converted to strings using their default serialization. As this decreases performance, it is not recommended to use non-String/FixedString arguments.

**Returned values**

The String created by concatenating the arguments.

If any of the argument values is `NULL`, the function returns `NULL`.

**Example**

```sql
SELECT concatWithSeparator('a', '1', '2', '3', '4')
```

Result:

```result
┌─concatWithSeparator('a', '1', '2', '3', '4')─┐
│ 1a2a3a4                                      │
└──────────────────────────────────────────────┘
```

## concatWithSeparatorAssumeInjective {#concatwithseparatorassumeinjective}

Like `concatWithSeparator` but assumes that `concatWithSeparator(sep, expr1, expr2, expr3...) → result` is injective. Can be used for optimization of GROUP BY.

A function is called injective if it returns for different arguments different results. In other words: different arguments never produce identical result.

## substring {#substring}

Returns the substring of a string `s` which starts at the specified byte index `offset`. Byte counting starts from 1. If `offset` is 0, an empty string is returned. If `offset` is negative, the substring starts `pos` characters from the end of the string, rather than from the beginning. An optional argument `length` specifies the maximum number of bytes the returned substring may have.

**Syntax**

```sql
substring(s, offset[, length])
```

Aliases:
- `substr`
- `mid`
- `byteSlice`

**Arguments**

- `s` — The string to calculate a substring from. [String](../data-types/string.md), [FixedString](../data-types/fixedstring.md) or [Enum](../data-types/enum.md)
- `offset` — The starting position of the substring in `s` . [(U)Int*](../data-types/int-uint.md).
- `length` — The maximum length of the substring. [(U)Int*](../data-types/int-uint.md). Optional.

**Returned value**

A substring of `s` with `length` many bytes, starting at index `offset`. [String](../data-types/string.md).

**Example**

```sql
SELECT 'database' AS db, substr(db, 5), substr(db, 5, 1)
```

Result:

```result
┌─db───────┬─substring('database', 5)─┬─substring('database', 5, 1)─┐
│ database │ base                     │ b                           │
└──────────┴──────────────────────────┴─────────────────────────────┘
```

## substringUTF8 {#substringutf8}

Returns the substring of a string `s` which starts at the specified byte index `offset` for Unicode code points. Byte counting starts from `1`. If `offset` is `0`, an empty string is returned. If `offset` is negative, the substring starts `pos` characters from the end of the string, rather than from the beginning. An optional argument `length` specifies the maximum number of bytes the returned substring may have.

Assumes that the string contains valid UTF-8 encoded text. If this assumption is violated, no exception is thrown and the result is undefined.

**Syntax**

```sql
substringUTF8(s, offset[, length])
```

**Arguments**

- `s` — The string to calculate a substring from. [String](../data-types/string.md), [FixedString](../data-types/fixedstring.md) or [Enum](../data-types/enum.md)
- `offset` — The starting position of the substring in `s` . [(U)Int*](../data-types/int-uint.md).
- `length` — The maximum length of the substring. [(U)Int*](../data-types/int-uint.md). Optional.

**Returned value**

A substring of `s` with `length` many bytes, starting at index `offset`.

**Implementation details**

Assumes that the string contains valid UTF-8 encoded text. If this assumption is violated, no exception is thrown and the result is undefined.

**Example**

```sql
SELECT 'Täglich grüßt das Murmeltier.' AS str,
       substringUTF8(str, 9),
       substringUTF8(str, 9, 5)
```

```response
Täglich grüßt das Murmeltier.    grüßt das Murmeltier.    grüßt
```

## substringIndex {#substringindex}

Returns the substring of `s` before `count` occurrences of the delimiter `delim`, as in Spark or MySQL.

**Syntax**

```sql
substringIndex(s, delim, count)
```
Alias: `SUBSTRING_INDEX`


**Arguments**

- s — The string to extract substring from. [String](../data-types/string.md).
- delim — The character to split. [String](../data-types/string.md).
- count — The number of occurrences of the delimiter to count before extracting the substring. If count is positive, everything to the left of the final delimiter (counting from the left) is returned. If count is negative, everything to the right of the final delimiter (counting from the right) is returned. [UInt or Int](../data-types/int-uint.md)

**Example**

```sql
SELECT substringIndex('www.clickhouse.com', '.', 2)
```

Result:
```sql
┌─substringIndex('www.clickhouse.com', '.', 2)─┐
│ www.clickhouse                               │
└──────────────────────────────────────────────┘
```

## substringIndexUTF8 {#substringindexutf8}

Returns the substring of `s` before `count` occurrences of the delimiter `delim`, specifically for Unicode code points.

Assumes that the string contains valid UTF-8 encoded text. If this assumption is violated, no exception is thrown and the result is undefined.

**Syntax**

```sql
substringIndexUTF8(s, delim, count)
```

**Arguments**

- `s` — The string to extract substring from. [String](../data-types/string.md).
- `delim` — The character to split. [String](../data-types/string.md).
- `count` — The number of occurrences of the delimiter to count before extracting the substring. If count is positive, everything to the left of the final delimiter (counting from the left) is returned. If count is negative, everything to the right of the final delimiter (counting from the right) is returned. [UInt or Int](../data-types/int-uint.md)

**Returned value**

A substring [String](../data-types/string.md) of `s` before `count` occurrences of `delim`.

**Implementation details**

Assumes that the string contains valid UTF-8 encoded text. If this assumption is violated, no exception is thrown and the result is undefined.

**Example**

```sql
SELECT substringIndexUTF8('www.straßen-in-europa.de', '.', 2)
```

```response
www.straßen-in-europa
```

## appendTrailingCharIfAbsent {#appendtrailingcharifabsent}

Appends character `c` to string `s` if `s` is non-empty and does not end with character `c`.

**Syntax**

```sql
appendTrailingCharIfAbsent(s, c)
```

## convertCharset {#convertcharset}

Returns string `s` converted from the encoding `from` to encoding `to`.

**Syntax**

```sql
convertCharset(s, from, to)
```

## base32Encode {#base32encode}

Encodes a string using [Base32](https://datatracker.ietf.org/doc/html/rfc4648#section-6).

**Syntax**

```sql
base32Encode(plaintext)
```

**Arguments**

- `plaintext` — [String](../data-types/string.md) column or constant.

**Returned value**

- A string containing the encoded value of the argument. [String](../data-types/string.md) or [FixedString](../data-types/fixedstring.md).

**Example**

```sql
SELECT base32Encode('Encoded');
```

Result:

```result
┌─base32Encode('Encoded')─┐
│ IVXGG33EMVSA====        │
└─────────────────────────┘
```

## base32Decode {#base32decode}

Accepts a string and decodes it using [Base32](https://datatracker.ietf.org/doc/html/rfc4648#section-6) encoding scheme.

**Syntax**

```sql
base32Decode(encoded)
```

**Arguments**

- `encoded` — [String](../data-types/string.md) or [FixedString](../data-types/fixedstring.md). If the string is not a valid Base32-encoded value, an exception is thrown.

**Returned value**

- A string containing the decoded value of the argument. [String](../data-types/string.md).

**Example**

```sql
SELECT base32Decode('IVXGG33EMVSA====');
```

Result:

```result
┌─base32Decode('IVXGG33EMVSA====')─┐
│ Encoded                          │
└──────────────────────────────────┘
```

## tryBase32Decode {#trybase32decode}

Like `base32Decode` but returns an empty string in case of error.

**Syntax**

```sql
tryBase32Decode(encoded)
```

**Parameters**

- `encoded`: [String](../data-types/string.md) or [FixedString](../data-types/fixedstring.md). If the string is not a valid Base32-encoded value, returns an empty string in case of error.

**Returned value**

- A string containing the decoded value of the argument.

**Examples**

Query:

```sql
SELECT tryBase32Decode('IVXGG33EMVSA====') AS res, tryBase32Decode('invalid') AS res_invalid;
```

```response
┌─res─────┬─res_invalid─┐
│ Encoded │             │
└─────────┴─────────────┘
```

## base58Encode {#base58encode}

Encodes a string using [Base58](https://datatracker.ietf.org/doc/html/draft-msporny-base58) in the "Bitcoin" alphabet.

**Syntax**

```sql
base58Encode(plaintext)
```

**Arguments**

- `plaintext` — [String](../data-types/string.md) column or constant.

**Returned value**

- A string containing the encoded value of the argument. [String](../data-types/string.md) or [FixedString](../data-types/fixedstring.md).

**Example**

```sql
SELECT base58Encode('Encoded');
```

Result:

```result
┌─base58Encode('Encoded')─┐
│ 3dc8KtHrwM              │
└─────────────────────────┘
```

## base58Decode {#base58decode}

Accepts a string and decodes it using [Base58](https://datatracker.ietf.org/doc/html/draft-msporny-base58) encoding scheme using "Bitcoin" alphabet.

**Syntax**

```sql
base58Decode(encoded)
```

**Arguments**

- `encoded` — [String](../data-types/string.md) or [FixedString](../data-types/fixedstring.md). If the string is not a valid Base58-encoded value, an exception is thrown.

**Returned value**

- A string containing the decoded value of the argument. [String](../data-types/string.md).

**Example**

```sql
SELECT base58Decode('3dc8KtHrwM');
```

Result:

```result
┌─base58Decode('3dc8KtHrwM')─┐
│ Encoded                    │
└────────────────────────────┘
```

## tryBase58Decode {#trybase58decode}

Like `base58Decode` but returns an empty string in case of error.

**Syntax**

```sql
tryBase58Decode(encoded)
```

**Parameters**

- `encoded`: [String](../data-types/string.md) or [FixedString](../data-types/fixedstring.md). If the string is not a valid Base58-encoded value, returns an empty string in case of error.

**Returned value**

- A string containing the decoded value of the argument.

**Examples**

Query:

```sql
SELECT tryBase58Decode('3dc8KtHrwM') AS res, tryBase58Decode('invalid') AS res_invalid;
```

```response
┌─res─────┬─res_invalid─┐
│ Encoded │             │
└─────────┴─────────────┘
```

## base64Encode {#base64encode}

Encodes a String or FixedString as base64, according to [RFC 4648](https://datatracker.ietf.org/doc/html/rfc4648#section-4).

Alias: `TO_BASE64`.

**Syntax**

```sql
base64Encode(plaintext)
```

**Arguments**

- `plaintext` — [String](../data-types/string.md) column or constant.

**Returned value**

- A string containing the encoded value of the argument.

**Example**

```sql
SELECT base64Encode('clickhouse');
```

Result:

```result
┌─base64Encode('clickhouse')─┐
│ Y2xpY2tob3VzZQ==           │
└────────────────────────────┘
```

## base64URLEncode {#base64urlencode}

Encodes an URL (String or FixedString) as base64 with URL-specific modifications, according to [RFC 4648](https://datatracker.ietf.org/doc/html/rfc4648#section-5).

**Syntax**

```sql
base64URLEncode(url)
```

**Arguments**

- `url` — [String](../data-types/string.md) column or constant.

**Returned value**

- A string containing the encoded value of the argument.

**Example**

```sql
SELECT base64URLEncode('https://clickhouse.com');
```

Result:

```result
┌─base64URLEncode('https://clickhouse.com')─┐
│ aHR0cDovL2NsaWNraG91c2UuY29t              │
└───────────────────────────────────────────┘
```

## base64Decode {#base64decode}

Accepts a String and decodes it from base64, according to [RFC 4648](https://datatracker.ietf.org/doc/html/rfc4648#section-4). Throws an exception in case of an error.

Alias: `FROM_BASE64`.

**Syntax**

```sql
base64Decode(encoded)
```

**Arguments**

- `encoded` — [String](../data-types/string.md) column or constant. If the string is not a valid Base64-encoded value, an exception is thrown.

**Returned value**

- A string containing the decoded value of the argument.

**Example**

```sql
SELECT base64Decode('Y2xpY2tob3VzZQ==');
```

Result:

```result
┌─base64Decode('Y2xpY2tob3VzZQ==')─┐
│ clickhouse                       │
└──────────────────────────────────┘
```

## base64URLDecode {#base64urldecode}

Accepts a base64-encoded URL and decodes it from base64 with URL-specific modifications, according to [RFC 4648](https://datatracker.ietf.org/doc/html/rfc4648#section-5). Throws an exception in case of an error.

**Syntax**

```sql
base64URLDecode(encodedUrl)
```

**Arguments**

- `encodedURL` — [String](../data-types/string.md) column or constant. If the string is not a valid Base64-encoded value with URL-specific modifications, an exception is thrown.

**Returned value**

- A string containing the decoded value of the argument.

**Example**

```sql
SELECT base64URLDecode('aHR0cDovL2NsaWNraG91c2UuY29t');
```

Result:

```result
┌─base64URLDecode('aHR0cDovL2NsaWNraG91c2UuY29t')─┐
│ https://clickhouse.com                          │
└─────────────────────────────────────────────────┘
```

## tryBase64Decode {#trybase64decode}

Like `base64Decode` but returns an empty string in case of error.

**Syntax**

```sql
tryBase64Decode(encoded)
```

**Arguments**

- `encoded` — [String](../data-types/string.md) column or constant. If the string is not a valid Base64-encoded value, returns an empty string.

**Returned value**

- A string containing the decoded value of the argument.

**Examples**

Query:

```sql
SELECT tryBase64Decode('RW5jb2RlZA==') AS res, tryBase64Decode('invalid') AS res_invalid;
```

```response
┌─res────────┬─res_invalid─┐
│ clickhouse │             │
└────────────┴─────────────┘
```

## tryBase64URLDecode {#trybase64urldecode}

Like `base64URLDecode` but returns an empty string in case of error.

**Syntax**

```sql
tryBase64URLDecode(encodedUrl)
```

**Parameters**

- `encodedURL` — [String](../data-types/string.md) column or constant. If the string is not a valid Base64-encoded value with URL-specific modifications, returns an empty string.

**Returned value**

- A string containing the decoded value of the argument.

**Examples**

Query:

```sql
SELECT tryBase64URLDecode('aHR0cDovL2NsaWNraG91c2UuY29t') AS res, tryBase64Decode('aHR0cHM6Ly9jbGlja') AS res_invalid;
```

```response
┌─res────────────────────┬─res_invalid─┐
│ https://clickhouse.com │             │
└────────────────────────┴─────────────┘
```

## endsWith {#endswith}

Returns whether string `str` ends with `suffix`.

**Syntax**

```sql
endsWith(str, suffix)
```

## endsWithUTF8 {#endswithutf8}

Returns whether string `str` ends with `suffix`, the difference between `endsWithUTF8` and `endsWith` is that `endsWithUTF8` match `str` and `suffix` by UTF-8 characters.

**Syntax**

```sql
endsWithUTF8(str, suffix)
```

**Example**

```sql
SELECT endsWithUTF8('中国', '\xbd'), endsWith('中国', '\xbd')
```

Result:

```result
┌─endsWithUTF8('中国', '½')─┬─endsWith('中国', '½')─┐
│                        0 │                    1 │
└──────────────────────────┴──────────────────────┘
```

## startsWith {#startswith}

Returns whether string `str` starts with `prefix`.

**Syntax**

```sql
startsWith(str, prefix)
```

**Example**

```sql
SELECT startsWith('Spider-Man', 'Spi');
```

## startsWithUTF8 {#startswithutf8}

<VersionBadge minVersion='23.8' />

Returns whether string `str` starts with `prefix`, the difference between `startsWithUTF8` and `startsWith` is that `startsWithUTF8` match `str` and `suffix` by UTF-8 characters.


**Example**

```sql
SELECT startsWithUTF8('中国', '\xe4'), startsWith('中国', '\xe4')
```

Result:

```result
┌─startsWithUTF8('中国', '⥩─┬─startsWith('中国', '⥩─┐
│                          0 │                      1 │
└────────────────────────────┴────────────────────────┘
```

## trim {#trim}

Removes the specified characters from the start or end of a string. If not specified otherwise, the function removes whitespace (ASCII-character 32).

**Syntax**

```sql
trim([[LEADING|TRAILING|BOTH] trim_character FROM] input_string)
```

**Arguments**

- `trim_character` — The characters to trim. [String](../data-types/string.md).
- `input_string` — String for trim. [String](../data-types/string.md).

**Returned value**

A string without leading and/or trailing specified characters. [String](../data-types/string.md).

**Example**

```sql
SELECT trim(BOTH ' ()' FROM '(   Hello, world!   )');
```

Result:

```result
┌─trim(BOTH ' ()' FROM '(   Hello, world!   )')─┐
│ Hello, world!                                 │
└───────────────────────────────────────────────┘
```

## trimLeft {#trimleft}

Removes the consecutive occurrences of whitespace (ASCII-character 32) from the start of a string.

**Syntax**

```sql
trimLeft(input_string[, trim_characters])
```

Alias: `ltrim`.

**Arguments**

- `input_string` — The string to trim. [String](../data-types/string.md).
- `trim_characters` — The characters to trim. Optional. [String](../data-types/string.md). If not specified, `' '` ( single whitespace) is used as trim character.

**Returned value**

A string without leading common whitespaces. [String](../data-types/string.md).

**Example**

```sql
SELECT trimLeft('     Hello, world!     ');
```

Result:

```result
┌─trimLeft('     Hello, world!     ')─┐
│ Hello, world!                       │
└─────────────────────────────────────┘
```

## trimRight {#trimright}

Removes the consecutive occurrences of whitespace (ASCII-character 32) from the end of a string.

**Syntax**

```sql
trimRight(input_string[, trim_characters])
```

Alias: `rtrim`.

**Arguments**

- `input_string` — The string to trim. [String](../data-types/string.md).
- `trim_characters` — The characters to trim. Optional. [String](../data-types/string.md). If not specified, `' '` ( single whitespace) is used as trim character.

**Returned value**

A string without trailing common whitespaces. [String](../data-types/string.md).

**Example**

```sql
SELECT trimRight('     Hello, world!     ');
```

Result:

```result
┌─trimRight('     Hello, world!     ')─┐
│      Hello, world!                   │
└──────────────────────────────────────┘
```

## trimBoth {#trimboth}

Removes the consecutive occurrences of whitespace (ASCII-character 32) from both ends of a string.

**Syntax**

```sql
trimBoth(input_string[, trim_characters])
```

Alias: `trim`.

**Arguments**

- `input_string` — The string to trim. [String](../data-types/string.md).
- `trim_characters` — The characters to trim. Optional. [String](../data-types/string.md). If not specified, `' '` ( single whitespace) is used as trim character.

**Returned value**

A string without leading and trailing common whitespaces. [String](../data-types/string.md).

**Example**

```sql
SELECT trimBoth('     Hello, world!     ');
```

Result:

```result
┌─trimBoth('     Hello, world!     ')─┐
│ Hello, world!                       │
└─────────────────────────────────────┘
```

## CRC32 {#crc32}

Returns the CRC32 checksum of a string using CRC-32-IEEE 802.3 polynomial and initial value `0xffffffff` (zlib implementation).

The result type is UInt32.

## CRC32IEEE {#crc32ieee}

Returns the CRC32 checksum of a string, using CRC-32-IEEE 802.3 polynomial.

The result type is UInt32.

## CRC64 {#crc64}

Returns the CRC64 checksum of a string, using CRC-64-ECMA polynomial.

The result type is UInt64.

## normalizeUTF8NFC {#normalizeutf8nfc}

Converts a string to [NFC normalized form](https://en.wikipedia.org/wiki/Unicode_equivalence#Normal_forms), assuming the string is valid UTF8-encoded text.

**Syntax**

```sql
normalizeUTF8NFC(words)
```

**Arguments**

- `words` — UTF8-encoded input string. [String](../data-types/string.md).

**Returned value**

- String transformed to NFC normalization form. [String](../data-types/string.md).

**Example**

```sql
SELECT length('â'), normalizeUTF8NFC('â') AS nfc, length(nfc) AS nfc_len;
```

Result:

```result
┌─length('â')─┬─nfc─┬─nfc_len─┐
│           2 │ â   │       2 │
└─────────────┴─────┴─────────┘
```

## normalizeUTF8NFD {#normalizeutf8nfd}

Converts a string to [NFD normalized form](https://en.wikipedia.org/wiki/Unicode_equivalence#Normal_forms), assuming the string is valid UTF8-encoded text.

**Syntax**

```sql
normalizeUTF8NFD(words)
```

**Arguments**

- `words` — UTF8-encoded input string. [String](../data-types/string.md).

**Returned value**

- String transformed to NFD normalization form. [String](../data-types/string.md).

**Example**

```sql
SELECT length('â'), normalizeUTF8NFD('â') AS nfd, length(nfd) AS nfd_len;
```

Result:

```result
┌─length('â')─┬─nfd─┬─nfd_len─┐
│           2 │ â   │       3 │
└─────────────┴─────┴─────────┘
```

## normalizeUTF8NFKC {#normalizeutf8nfkc}

Converts a string to [NFKC normalized form](https://en.wikipedia.org/wiki/Unicode_equivalence#Normal_forms), assuming the string is valid UTF8-encoded text.

**Syntax**

```sql
normalizeUTF8NFKC(words)
```

**Arguments**

- `words` — UTF8-encoded input string. [String](../data-types/string.md).

**Returned value**

- String transformed to NFKC normalization form. [String](../data-types/string.md).

**Example**

```sql
SELECT length('â'), normalizeUTF8NFKC('â') AS nfkc, length(nfkc) AS nfkc_len;
```

Result:

```result
┌─length('â')─┬─nfkc─┬─nfkc_len─┐
│           2 │ â    │        2 │
└─────────────┴──────┴──────────┘
```

## normalizeUTF8NFKD {#normalizeutf8nfkd}

Converts a string to [NFKD normalized form](https://en.wikipedia.org/wiki/Unicode_equivalence#Normal_forms), assuming the string is valid UTF8-encoded text.

**Syntax**

```sql
normalizeUTF8NFKD(words)
```

**Arguments**

- `words` — UTF8-encoded input string. [String](../data-types/string.md).

**Returned value**

- String transformed to NFKD normalization form. [String](../data-types/string.md).

**Example**

```sql
SELECT length('â'), normalizeUTF8NFKD('â') AS nfkd, length(nfkd) AS nfkd_len;
```

Result:

```result
┌─length('â')─┬─nfkd─┬─nfkd_len─┐
│           2 │ â    │        3 │
└─────────────┴──────┴──────────┘
```

## encodeXMLComponent {#encodexmlcomponent}

Escapes characters with special meaning in XML such that they can afterwards be place into a XML text node or attribute.

The following characters are replaced: `<`, `&`, `>`, `"`, `'`.
Also see the [list of XML and HTML character entity references](https://en.wikipedia.org/wiki/List_of_XML_and_HTML_character_entity_references).

**Syntax**

```sql
encodeXMLComponent(x)
```

**Arguments**

- `x` — An input string. [String](../data-types/string.md).

**Returned value**

- The escaped string. [String](../data-types/string.md).

**Example**

```sql
SELECT encodeXMLComponent('Hello, "world"!');
SELECT encodeXMLComponent('<123>');
SELECT encodeXMLComponent('&clickhouse');
SELECT encodeXMLComponent('\'foo\'');
```

Result:

```result
Hello, &quot;world&quot;!
&lt;123&gt;
&amp;clickhouse
&apos;foo&apos;
```

## decodeXMLComponent {#decodexmlcomponent}

Un-escapes substrings with special meaning in XML. These substrings are: `&quot;` `&amp;` `&apos;` `&gt;` `&lt;`

This function also replaces numeric character references with Unicode characters. Both decimal (like `&#10003;`) and hexadecimal (`&#x2713;`) forms are supported.

**Syntax**

```sql
decodeXMLComponent(x)
```

**Arguments**

- `x` — An input string. [String](../data-types/string.md).

**Returned value**

- The un-escaped string. [String](../data-types/string.md).

**Example**

```sql
SELECT decodeXMLComponent('&apos;foo&apos;');
SELECT decodeXMLComponent('&lt; &#x3A3; &gt;');
```

Result:

```result
'foo'
< Σ >
```

## decodeHTMLComponent {#decodehtmlcomponent}

Un-escapes substrings with special meaning in HTML. For example: `&hbar;` `&gt;` `&diamondsuit;` `&heartsuit;` `&lt;` etc.

This function also replaces numeric character references with Unicode characters. Both decimal (like `&#10003;`) and hexadecimal (`&#x2713;`) forms are supported.

**Syntax**

```sql
decodeHTMLComponent(x)
```

**Arguments**

- `x` — An input string. [String](../data-types/string.md).

**Returned value**

- The un-escaped string. [String](../data-types/string.md).

**Example**

```sql
SELECT decodeHTMLComponent(''CH');
SELECT decodeHTMLComponent('I&heartsuit;ClickHouse');
```

Result:

```result
'CH'
I♥ClickHouse'
```

## extractTextFromHTML {#extracttextfromhtml}

This function extracts plain text from HTML or XHTML.

It does not conform 100% to the HTML, XML or XHTML specification but the implementation is reasonably accurate and fast. The rules are the following:

1. Comments are skipped. Example: `<!-- test -->`. Comment must end with `-->`. Nested comments are disallowed.
Note: constructions like `<!-->` and `<!--->` are not valid comments in HTML but they are skipped by other rules.
2. CDATA is pasted verbatim. Note: CDATA is XML/XHTML-specific  and processed on a "best-effort" basis.
3. `script` and `style` elements are removed with all their content. Note: it is assumed that closing tag cannot appear inside content. For example, in JS string literal has to be escaped like `"<\/script>"`.
Note: comments and CDATA are possible inside `script` or `style` - then closing tags are not searched inside CDATA. Example: `<script><![CDATA[</script>]]></script>`. But they are still searched inside comments. Sometimes it becomes complicated: `<script>var x = "<!--"; </script> var y = "-->"; alert(x + y);</script>`
Note: `script` and `style` can be the names of XML namespaces - then they are not treated like usual `script` or `style` elements. Example: `<script:a>Hello</script:a>`.
Note: whitespaces are possible after closing tag name: `</script >` but not before: `< / script>`.
4. Other tags or tag-like elements are skipped without inner content. Example: `<a>.</a>`
Note: it is expected that this HTML is illegal: `<a test=">"></a>`
Note: it also skips something like tags: `<>`, `<!>`, etc.
Note: tag without end is skipped to the end of input: `<hello   `
5. HTML and XML entities are not decoded. They must be processed by separate function.
6. Whitespaces in the text are collapsed or inserted by specific rules.
    - Whitespaces at the beginning and at the end are removed.
    - Consecutive whitespaces are collapsed.
    - But if the text is separated by other elements and there is no whitespace, it is inserted.
    - It may cause unnatural examples: `Hello<b>world</b>`, `Hello<!-- -->world` - there is no whitespace in HTML, but the function inserts it. Also consider: `Hello<p>world</p>`, `Hello<br>world`. This behavior is reasonable for data analysis, e.g. to convert HTML to a bag of words.
7. Also note that correct handling of whitespaces requires the support of `<pre></pre>` and CSS `display` and `white-space` properties.

**Syntax**

```sql
extractTextFromHTML(x)
```

**Arguments**

- `x` — input text. [String](../data-types/string.md).

**Returned value**

- Extracted text. [String](../data-types/string.md).

**Example**

The first example contains several tags and a comment and also shows whitespace processing.
The second example shows `CDATA` and `script` tag processing.
In the third example text is extracted from the full HTML response received by the [url](../../sql-reference/table-functions/url.md) function.

```sql
SELECT extractTextFromHTML(' <p> A text <i>with</i><b>tags</b>. <!-- comments --> </p> ');
SELECT extractTextFromHTML('<![CDATA[The content within <b>CDATA</b>]]> <script>alert("Script");</script>');
SELECT extractTextFromHTML(html) FROM url('http://www.donothingfor2minutes.com/', RawBLOB, 'html String');
```

Result:

```result
A text with tags .
The content within <b>CDATA</b>
Do Nothing for 2 Minutes 2:00 &nbsp;
```

## ascii {#ascii}

Returns the ASCII code point (as Int32) of the first character of string `s`.

If `s` is empty, the result is 0. If the first character is not an ASCII character or not part of the Latin-1 supplement range of UTF-16, the result is undefined.

**Syntax**

```sql
ascii(s)
```

## soundex {#soundex}

Returns the [Soundex code](https://en.wikipedia.org/wiki/Soundex) of a string.

**Syntax**

```sql
soundex(val)
```

**Arguments**

- `val` — Input value. [String](../data-types/string.md)

**Returned value**

- The Soundex code of the input value. [String](../data-types/string.md)

**Example**

```sql
SELECT soundex('aksel');
```

Result:

```result
┌─soundex('aksel')─┐
│ A240             │
└──────────────────┘
```

## punycodeEncode {#punycodeencode}

Returns the [Punycode](https://en.wikipedia.org/wiki/Punycode) representation of a string.
The string must be UTF8-encoded, otherwise the behavior is undefined.

**Syntax**

```sql
punycodeEncode(val)
```

**Arguments**

- `val` — Input value. [String](../data-types/string.md)

**Returned value**

- A Punycode representation of the input value. [String](../data-types/string.md)

**Example**

```sql
SELECT punycodeEncode('München');
```

Result:

```result
┌─punycodeEncode('München')─┐
│ Mnchen-3ya                │
└───────────────────────────┘
```

## punycodeDecode {#punycodedecode}

Returns the UTF8-encoded plaintext of a [Punycode](https://en.wikipedia.org/wiki/Punycode)-encoded string.
If no valid Punycode-encoded string is given, an exception is thrown.

**Syntax**

```sql
punycodeEncode(val)
```

**Arguments**

- `val` — Punycode-encoded string. [String](../data-types/string.md)

**Returned value**

- The plaintext of the input value. [String](../data-types/string.md)

**Example**

```sql
SELECT punycodeDecode('Mnchen-3ya');
```

Result:

```result
┌─punycodeDecode('Mnchen-3ya')─┐
│ München                      │
└──────────────────────────────┘
```

## tryPunycodeDecode {#trypunycodedecode}

Like `punycodeDecode` but returns an empty string if no valid Punycode-encoded string is given.

## idnaEncode {#idnaencode}

Returns the ASCII representation (ToASCII algorithm) of a domain name according to the [Internationalized Domain Names in Applications](https://en.wikipedia.org/wiki/Internationalized_domain_name#Internationalizing_Domain_Names_in_Applications) (IDNA) mechanism.
The input string must be UTF-encoded and translatable to an ASCII string, otherwise an exception is thrown.
Note: No percent decoding or trimming of tabs, spaces or control characters is performed.

**Syntax**

```sql
idnaEncode(val)
```

**Arguments**

- `val` — Input value. [String](../data-types/string.md)

**Returned value**

- A ASCII representation according to the IDNA mechanism of the input value. [String](../data-types/string.md)

**Example**

```sql
SELECT idnaEncode('straße.münchen.de');
```

Result:

```result
┌─idnaEncode('straße.münchen.de')─────┐
│ xn--strae-oqa.xn--mnchen-3ya.de     │
└─────────────────────────────────────┘
```

## tryIdnaEncode {#tryidnaencode}

Like `idnaEncode` but returns an empty string in case of an error instead of throwing an exception.

## idnaDecode {#idnadecode}

Returns the Unicode (UTF-8) representation (ToUnicode algorithm) of a domain name according to the [Internationalized Domain Names in Applications](https://en.wikipedia.org/wiki/Internationalized_domain_name#Internationalizing_Domain_Names_in_Applications) (IDNA) mechanism.
In case of an error (e.g. because the input is invalid), the input string is returned.
Note that repeated application of `idnaEncode()` and `idnaDecode()` does not necessarily return the original string due to case normalization.

**Syntax**

```sql
idnaDecode(val)
```

**Arguments**

- `val` — Input value. [String](../data-types/string.md)

**Returned value**

- A Unicode (UTF-8) representation according to the IDNA mechanism of the input value. [String](../data-types/string.md)

**Example**

```sql
SELECT idnaDecode('xn--strae-oqa.xn--mnchen-3ya.de');
```

Result:

```result
┌─idnaDecode('xn--strae-oqa.xn--mnchen-3ya.de')─┐
│ straße.münchen.de                             │
└───────────────────────────────────────────────┘
```

## byteHammingDistance {#bytehammingdistance}

Calculates the [hamming distance](https://en.wikipedia.org/wiki/Hamming_distance) between two byte strings.

**Syntax**

```sql
byteHammingDistance(string1, string2)
```

**Examples**

```sql
SELECT byteHammingDistance('karolin', 'kathrin');
```

Result:

```text
┌─byteHammingDistance('karolin', 'kathrin')─┐
│                                         3 │
└───────────────────────────────────────────┘
```

Alias: `mismatches`

## stringJaccardIndex {#stringjaccardindex}

Calculates the [Jaccard similarity index](https://en.wikipedia.org/wiki/Jaccard_index) between two byte strings.

**Syntax**

```sql
stringJaccardIndex(string1, string2)
```

**Examples**

```sql
SELECT stringJaccardIndex('clickhouse', 'mouse');
```

Result:

```text
┌─stringJaccardIndex('clickhouse', 'mouse')─┐
│                                       0.4 │
└───────────────────────────────────────────┘
```

## stringJaccardIndexUTF8 {#stringjaccardindexutf8}

Like [stringJaccardIndex](#stringjaccardindex) but for UTF8-encoded strings.

## editDistance {#editdistance}

Calculates the [edit distance](https://en.wikipedia.org/wiki/Edit_distance) between two byte strings.

**Syntax**

```sql
editDistance(string1, string2)
```

**Examples**

```sql
SELECT editDistance('clickhouse', 'mouse');
```

Result:

```text
┌─editDistance('clickhouse', 'mouse')─┐
│                                   6 │
└─────────────────────────────────────┘
```

Alias: `levenshteinDistance`

## editDistanceUTF8 {#editdistanceutf8}

Calculates the [edit distance](https://en.wikipedia.org/wiki/Edit_distance) between two UTF8 strings.

**Syntax**

```sql
editDistanceUTF8(string1, string2)
```

**Examples**

```sql
SELECT editDistanceUTF8('我是谁', '我是我');
```

Result:

```text
┌─editDistanceUTF8('我是谁', '我是我')──┐
│                                   1 │
└─────────────────────────────────────┘
```

Alias: `levenshteinDistanceUTF8`

## damerauLevenshteinDistance {#dameraulevenshteindistance}

Calculates the [Damerau-Levenshtein distance](https://en.wikipedia.org/wiki/Damerau%E2%80%93Levenshtein_distance) between two byte strings.

**Syntax**

```sql
damerauLevenshteinDistance(string1, string2)
```

**Examples**

```sql
SELECT damerauLevenshteinDistance('clickhouse', 'mouse');
```

Result:

```text
┌─damerauLevenshteinDistance('clickhouse', 'mouse')─┐
│                                                 6 │
└───────────────────────────────────────────────────┘
```

## jaroSimilarity {#jarosimilarity}

Calculates the [Jaro similarity](https://en.wikipedia.org/wiki/Jaro%E2%80%93Winkler_distance#Jaro_similarity) between two byte strings.

**Syntax**

```sql
jaroSimilarity(string1, string2)
```

**Examples**

```sql
SELECT jaroSimilarity('clickhouse', 'click');
```

Result:

```text
┌─jaroSimilarity('clickhouse', 'click')─┐
│                    0.8333333333333333 │
└───────────────────────────────────────┘
```

## jaroWinklerSimilarity {#jarowinklersimilarity}

Calculates the [Jaro-Winkler similarity](https://en.wikipedia.org/wiki/Jaro%E2%80%93Winkler_distance#Jaro%E2%80%93Winkler_similarity) between two byte strings.

**Syntax**

```sql
jaroWinklerSimilarity(string1, string2)
```

**Examples**

```sql
SELECT jaroWinklerSimilarity('clickhouse', 'click');
```

Result:

```text
┌─jaroWinklerSimilarity('clickhouse', 'click')─┐
│                           0.8999999999999999 │
└──────────────────────────────────────────────┘
```

## initcap {#initcap}

Convert the first letter of each word to upper case and the rest to lower case. Words are sequences of alphanumeric characters separated by non-alphanumeric characters.

:::note
Because `initCap` converts only the first letter of each word to upper case you may observe unexpected behaviour for words containing apostrophes or capital letters. For example:

```sql
SELECT initCap('mother''s daughter'), initCap('joe McAdam');
```

will return

```response
┌─initCap('mother\'s daughter')─┬─initCap('joe McAdam')─┐
│ Mother'S Daughter             │ Joe Mcadam            │
└───────────────────────────────┴───────────────────────┘
```

This is a known behaviour, with no plans currently to fix it.
:::

**Syntax**

```sql
initcap(val)
```

**Arguments**

- `val` — Input value. [String](../data-types/string.md).

**Returned value**

- `val` with the first letter of each word converted to upper case. [String](../data-types/string.md).

**Example**

Query:

```sql
SELECT initcap('building for fast');
```

Result:

```text
┌─initcap('building for fast')─┐
│ Building For Fast            │
└──────────────────────────────┘
```

## initcapUTF8 {#initcaputf8}

Like [initcap](#initcap), `initcapUTF8` converts the first letter of each word to upper case and the rest to lower case. Assumes that the string contains valid UTF-8 encoded text. 
If this assumption is violated, no exception is thrown and the result is undefined.

:::note
This function does not detect the language, e.g. for Turkish the result might not be exactly correct (i/İ vs. i/I).
If the length of the UTF-8 byte sequence is different for upper and lower case of a code point, the result may be incorrect for this code point.
:::

**Syntax**

```sql
initcapUTF8(val)
```

**Arguments**

- `val` — Input value. [String](../data-types/string.md).

**Returned value**

- `val` with the first letter of each word converted to upper case. [String](../data-types/string.md).

**Example**

Query:

```sql
SELECT initcapUTF8('не тормозит');
```

Result:

```text
┌─initcapUTF8('не тормозит')─┐
│ Не Тормозит                │
└────────────────────────────┘
```

## firstLine {#firstline}

Returns the first line from a multi-line string.

**Syntax**

```sql
firstLine(val)
```

**Arguments**

- `val` — Input value. [String](../data-types/string.md)

**Returned value**

- The first line of the input value or the whole value if there is no line
  separators. [String](../data-types/string.md)

**Example**

```sql
SELECT firstLine('foo\nbar\nbaz');
```

Result:

```result
┌─firstLine('foo\nbar\nbaz')─┐
│ foo                        │
└────────────────────────────┘
```

## stringCompare {#stringcompare}

Compare two strings lexicographically.

**Syntax**

```sql
stringCompare(string1, string2[, str1_off, string2_offset, num_bytes]);
```

**Arguments**

- `string1` — The first string to compare. [String](../data-types/string.md)
- `string2` - The second string to compare.[String](../data-types/string.md)
- `string1_offset` — The position (zero-based) in `string1` from which the comparison starts. Optional, positive number.
- `string2_offset` — The position (zero-based index) in `string2` from which the comparison starts. Optional, positive number.
- `num_bytes` — The maximum number of bytes to compare in both strings. If `string_offset` + `num_bytes` exceeds the end of an input string, `num_bytes` will be reduced accordingly.

**Returned value**

- -1 — If `string1`[`string1_offset`: `string1_offset` + `num_bytes`] < `string2`[`string2_offset`:`string2_offset` + `num_bytes`] and `string1_offset` < len(`string1`) and `string2_offset` < len(`string2`).
If `string1_offset` >= len(`string1`) and `string2_offset` < len(`string2`).
- 0 — If `string1`[`string1_offset`: `string1_offset` + `num_bytes`] = `string2`[`string2_offset`:`string2_offset` + `num_bytes`] and `string1_offset` < len(`string1`) and `string2_offset` < len(`string2`).
If `string1_offset` >= len(`string1`) and `string2_offset` >= len(`string2`).
- 1 — If `string1`[`string1_offset`: `string1_offset` + `num_bytes`] > `string2`[`string2_offset`:`string2_offset` + `num_bytes`] and `string1_offset` < len(`string1`) and `string2_offset` < len(`string2`).
If `string1_offset` < len(`string1`) and `string2_offset` >= len(`string2`).

**Example**

```sql
SELECT
    stringCompare('alice', 'bob', 0, 0, 3) AS result1,
    stringCompare('alice', 'alicia', 0, 0, 3) AS result2,
    stringCompare('bob', 'alice', 0, 0, 3) AS result3
```
Result:
```result
   ┌─result1─┬─result2─┬─result3─┐
1. │      -1 │       0 │       1 │
   └─────────┴─────────┴─────────┘
```

```sql
SELECT
    stringCompare('alice', 'alicia') AS result2,
    stringCompare('alice', 'alice') AS result1,
    stringCompare('bob', 'alice') AS result3
```
Result:
```result
   ┌─result2─┬─result1─┬─result3─┐
1. │      -1 │       0 │       1 │
   └─────────┴─────────┴─────────┘
```

## sparseGrams {#sparsegrams}

Finds all substrings of a given string that have a length of at least `n`, 
where the hashes of the (n-1)-grams at the borders of the substring
are strictly greater than those of any (n-1)-gram inside the substring.
Uses [crc32](./string-functions.md#crc32) as a hash function.

**Syntax**

```sql
sparseGrams(s[, min_ngram_length]);
```

**Arguments**

- `s` — An input string. [String](../data-types/string.md)
- `min_ngram_length` — The minimum length of extracted ngram. The default and minimal value is 3.
- `max_ngram_length` — The maximum length of extracted ngram. The default value is 100. Should be not less than 'min_ngram_length'

**Returned value**

- An array of selected substrings. [Array](../data-types/array.md)([String](../data-types/string.md)).

**Example**

```sql
SELECT sparseGrams('alice', 3) AS result
```
Result:
```result
   ┌─result─────────────────────┐
1. │ ['ali','lic','lice','ice'] │
   └────────────────────────────┘
```

## sparseGramsUTF8 {#sparsegramsutf8}

Finds all substrings of a given string that have a length of at least `n`,
where the hashes of the (n-1)-grams at the borders of the substring
are strictly greater than those of any (n-1)-gram inside the substring.
Uses [crc32](./string-functions.md#crc32) as a hash function.
Expects UTF-8 string, throws an exception in case of invalid UTF-8 sequence.

**Syntax**

```sql
sparseGramsUTF8(s[, min_ngram_length]);
```

**Arguments**

- `s` — An input string. [String](../data-types/string.md)
- `min_ngram_length` — The minimum length of extracted ngram. The default and minimal value is 3.
- `max_ngram_length` — The maximum length of extracted ngram. The default value is 100. Should be not less than 'min_ngram_length'

**Returned value**

- An array of selected substrings. [Array](../data-types/array.md)([String](../data-types/string.md)).

**Example**

```sql
SELECT sparseGramsUTF8('алиса', 3) AS result
```
Result:
```result
   ┌─result──────────────┐
1. │ ['али','лис','иса'] │
   └─────────────────────┘
```
## sparseGramsHashes {#sparsegramshashes}

Finds hashes of all substrings of a given string that have a length of at least `n`,
where the hashes of the (n-1)-grams at the borders of the substring
are strictly greater than those of any (n-1)-gram inside the substring.
Uses [crc32](./string-functions.md#crc32) as a hash function.

**Syntax**

```sql
sparseGramsHashes(s[, min_ngram_length]);
```

**Arguments**

- `s` — An input string. [String](../data-types/string.md)
- `min_ngram_length` — The minimum length of extracted ngram. The default and minimal value is 3.
- `max_ngram_length` — The maximum length of extracted ngram. The default value is 100. Should be not less than 'min_ngram_length'

**Returned value**

- An array of selected substrings crc32-c hashes. [Array](../data-types/array.md)([UInt32](../data-types/int-uint.md)).

**Example**

```sql
SELECT sparseGramsHashes('alice', 3) AS result
```
Result:
```result
   ┌─result────────────────────────────────────────┐
1. │ [1265796434,3725069146,1689963195,3410985998] │
   └───────────────────────────────────────────────┘
```

## sparseGramsHashesUTF8 {#sparsegramshashesutf8}

Finds hashes of all substrings of a given string that have a length of at least `n`,
where the hashes of the (n-1)-grams at the borders of the substring
are strictly greater than those of any (n-1)-gram inside the substring.
Uses [crc32](./string-functions.md#crc32) as a hash function.
Expects UTF-8 string, throws an exception in case of invalid UTF-8 sequence.

**Syntax**

```sql
sparseGramsUTF8(s[, min_ngram_length]);
```

**Arguments**

- `s` — An input string. [String](../data-types/string.md)
- `min_ngram_length` — The minimum length of extracted ngram. The default and minimal value is 3.
- `max_ngram_length` — The maximum length of extracted ngram. The default value is 100. Should be not less than 'min_ngram_length'

**Returned value**

- An array of selected substrings crc32-c hashes. [Array](../data-types/array.md)([UInt32](../data-types/int-uint.md)).

**Example**

```sql
SELECT sparseGramsHashesUTF8('алиса', 3) AS result
```
Result:
```result
   ┌─result───────────────────────────┐
1. │ [417784657,728683856,3071092609] │
   └──────────────────────────────────┘
```

## stringBytesUniq {#stringbytesuniq}

Counts the number of distinct bytes in a string.

**Syntax**

```sql
stringBytesUniq(s)
```

**Arguments**

- `s` — The string to analyze. [String](../data-types/string.md).

**Returned value**

- The number of distinct bytes in the string. [UInt16](../data-types/int-uint.md).

**Example**

```sql
SELECT stringBytesUniq('Hello');
```

Result:

```result
┌─stringBytesUniq('Hello')─┐
│                        4 │
└──────────────────────────┘
```

## stringBytesEntropy {#stringbytesentropy}

Calculates Shannon's entropy of byte distribution in a string.

**Syntax**

```sql
stringBytesEntropy(s)
```

**Arguments**

- `s` — The string to analyze. [String](../data-types/string.md).

**Returned value**

- Shannon's entropy of byte distribution in the string. [Float64](../data-types/float.md).

**Example**

```sql
SELECT stringBytesEntropy('Hello, world!');
```

Result:

```result
┌─stringBytesEntropy('Hello, world!')─┐
│                         3.07049960  │
└─────────────────────────────────────┘
```

<!-- 
The inner content of the tags below are replaced at doc framework build time with 
docs generated from system.functions. Please do not modify or remove the tags.
See: https://github.com/ClickHouse/clickhouse-docs/blob/main/contribute/autogenerated-documentation-from-source.md
-->

<!--AUTOGENERATED_START-->
<!--AUTOGENERATED_END-->
