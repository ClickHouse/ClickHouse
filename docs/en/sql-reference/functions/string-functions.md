---
toc_priority: 40
toc_title: Strings
---

# Functions for Working with Strings {#functions-for-working-with-strings}

!!! note "Note"
    Functions for [searching](../../sql-reference/functions/string-search-functions.md) and [replacing](../../sql-reference/functions/string-replace-functions.md) in strings are described separately.

## empty {#empty}

Returns 1 for an empty string or 0 for a non-empty string.
The result type is UInt8.
A string is considered non-empty if it contains at least one byte, even if this is a space or a null byte.
The function also works for arrays.

## notEmpty {#notempty}

Returns 0 for an empty string or 1 for a non-empty string.
The result type is UInt8.
The function also works for arrays.

## length {#length}

Returns the length of a string in bytes (not in characters, and not in code points).
The result type is UInt64.
The function also works for arrays.

## lengthUTF8 {#lengthutf8}

Returns the length of a string in Unicode code points (not in characters), assuming that the string contains a set of bytes that make up UTF-8 encoded text. If this assumption is not met, it returns some result (it doesn’t throw an exception).
The result type is UInt64.

## char_length, CHAR_LENGTH {#char-length}

Returns the length of a string in Unicode code points (not in characters), assuming that the string contains a set of bytes that make up UTF-8 encoded text. If this assumption is not met, it returns some result (it doesn’t throw an exception).
The result type is UInt64.

## character_length, CHARACTER_LENGTH {#character-length}

Returns the length of a string in Unicode code points (not in characters), assuming that the string contains a set of bytes that make up UTF-8 encoded text. If this assumption is not met, it returns some result (it doesn’t throw an exception).
The result type is UInt64.

## lower, lcase {#lower}

Converts ASCII Latin symbols in a string to lowercase.

## upper, ucase {#upper}

Converts ASCII Latin symbols in a string to uppercase.

## lowerUTF8 {#lowerutf8}

Converts a string to lowercase, assuming the string contains a set of bytes that make up a UTF-8 encoded text.
It doesn’t detect the language. So for Turkish the result might not be exactly correct.
If the length of the UTF-8 byte sequence is different for upper and lower case of a code point, the result may be incorrect for this code point.
If the string contains a set of bytes that is not UTF-8, then the behavior is undefined.

## upperUTF8 {#upperutf8}

Converts a string to uppercase, assuming the string contains a set of bytes that make up a UTF-8 encoded text.
It doesn’t detect the language. So for Turkish the result might not be exactly correct.
If the length of the UTF-8 byte sequence is different for upper and lower case of a code point, the result may be incorrect for this code point.
If the string contains a set of bytes that is not UTF-8, then the behavior is undefined.

## isValidUTF8 {#isvalidutf8}

Returns 1, if the set of bytes is valid UTF-8 encoded, otherwise 0.

## toValidUTF8 {#tovalidutf8}

Replaces invalid UTF-8 characters by the `�` (U+FFFD) character. All running in a row invalid characters are collapsed into the one replacement character.

``` sql
toValidUTF8( input_string )
```

Parameters:

-   input_string — Any set of bytes represented as the [String](../../sql-reference/data-types/string.md) data type object.

Returned value: Valid UTF-8 string.

**Example**

``` sql
SELECT toValidUTF8('\x61\xF0\x80\x80\x80b')
```

``` text
┌─toValidUTF8('a����b')─┐
│ a�b                   │
└───────────────────────┘
```

## repeat {#repeat}

Repeats a string as many times as specified and concatenates the replicated values as a single string.

**Syntax**

``` sql
repeat(s, n)
```

**Parameters**

-   `s` — The string to repeat. [String](../../sql-reference/data-types/string.md).
-   `n` — The number of times to repeat the string. [UInt](../../sql-reference/data-types/int-uint.md).

**Returned value**

The single string, which contains the string `s` repeated `n` times. If `n` \< 1, the function returns empty string.

Type: `String`.

**Example**

Query:

``` sql
SELECT repeat('abc', 10)
```

Result:

``` text
┌─repeat('abc', 10)──────────────┐
│ abcabcabcabcabcabcabcabcabcabc │
└────────────────────────────────┘
```

## reverse {#reverse}

Reverses the string (as a sequence of bytes).

## reverseUTF8 {#reverseutf8}

Reverses a sequence of Unicode code points, assuming that the string contains a set of bytes representing a UTF-8 text. Otherwise, it does something else (it doesn’t throw an exception).

## format(pattern, s0, s1, …) {#format}

Formatting constant pattern with the string listed in the arguments. `pattern` is a simplified Python format pattern. Format string contains “replacement fields” surrounded by curly braces `{}`. Anything that is not contained in braces is considered literal text, which is copied unchanged to the output. If you need to include a brace character in the literal text, it can be escaped by doubling: `{{ '{{' }}` and `{{ '}}' }}`. Field names can be numbers (starting from zero) or empty (then they are treated as consequence numbers).

``` sql
SELECT format('{1} {0} {1}', 'World', 'Hello')
```

``` text
┌─format('{1} {0} {1}', 'World', 'Hello')─┐
│ Hello World Hello                       │
└─────────────────────────────────────────┘
```

``` sql
SELECT format('{} {}', 'Hello', 'World')
```

``` text
┌─format('{} {}', 'Hello', 'World')─┐
│ Hello World                       │
└───────────────────────────────────┘
```

## concat {#concat}

Concatenates the strings listed in the arguments, without a separator.

**Syntax**

``` sql
concat(s1, s2, ...)
```

**Parameters**

Values of type String or FixedString.

**Returned values**

Returns the String that results from concatenating the arguments.

If any of argument values is `NULL`, `concat` returns `NULL`.

**Example**

Query:

``` sql
SELECT concat('Hello, ', 'World!')
```

Result:

``` text
┌─concat('Hello, ', 'World!')─┐
│ Hello, World!               │
└─────────────────────────────┘
```

## concatAssumeInjective {#concatassumeinjective}

Same as [concat](#concat), the difference is that you need to ensure that `concat(s1, s2, ...) → sn` is injective, it will be used for optimization of GROUP BY.

The function is named “injective” if it always returns different result for different values of arguments. In other words: different arguments never yield identical result.

**Syntax**

``` sql
concatAssumeInjective(s1, s2, ...)
```

**Parameters**

Values of type String or FixedString.

**Returned values**

Returns the String that results from concatenating the arguments.

If any of argument values is `NULL`, `concatAssumeInjective` returns `NULL`.

**Example**

Input table:

``` sql
CREATE TABLE key_val(`key1` String, `key2` String, `value` UInt32) ENGINE = TinyLog;
INSERT INTO key_val VALUES ('Hello, ','World',1), ('Hello, ','World',2), ('Hello, ','World!',3), ('Hello',', World!',2);
SELECT * from key_val;
```

``` text
┌─key1────┬─key2─────┬─value─┐
│ Hello,  │ World    │     1 │
│ Hello,  │ World    │     2 │
│ Hello,  │ World!   │     3 │
│ Hello   │ , World! │     2 │
└─────────┴──────────┴───────┘
```

Query:

``` sql
SELECT concat(key1, key2), sum(value) FROM key_val GROUP BY concatAssumeInjective(key1, key2)
```

Result:

``` text
┌─concat(key1, key2)─┬─sum(value)─┐
│ Hello, World!      │          3 │
│ Hello, World!      │          2 │
│ Hello, World       │          3 │
└────────────────────┴────────────┘
```

## substring(s, offset, length), mid(s, offset, length), substr(s, offset, length) {#substring}

Returns a substring starting with the byte from the ‘offset’ index that is ‘length’ bytes long. Character indexing starts from one (as in standard SQL). The ‘offset’ and ‘length’ arguments must be constants.

## substringUTF8(s, offset, length) {#substringutf8}

The same as ‘substring’, but for Unicode code points. Works under the assumption that the string contains a set of bytes representing a UTF-8 encoded text. If this assumption is not met, it returns some result (it doesn’t throw an exception).

## appendTrailingCharIfAbsent(s, c) {#appendtrailingcharifabsent}

If the ‘s’ string is non-empty and does not contain the ‘c’ character at the end, it appends the ‘c’ character to the end.

## convertCharset(s, from, to) {#convertcharset}

Returns the string ‘s’ that was converted from the encoding in ‘from’ to the encoding in ‘to’.

## base64Encode(s) {#base64encode}

Encodes ‘s’ string into base64

## base64Decode(s) {#base64decode}

Decode base64-encoded string ‘s’ into original string. In case of failure raises an exception.

## tryBase64Decode(s) {#trybase64decode}

Similar to base64Decode, but in case of error an empty string would be returned.

## endsWith(s, suffix) {#endswith}

Returns whether to end with the specified suffix. Returns 1 if the string ends with the specified suffix, otherwise it returns 0.

## startsWith(str, prefix) {#startswith}

Returns 1 whether string starts with the specified prefix, otherwise it returns 0.

``` sql
SELECT startsWith('Spider-Man', 'Spi');
```

**Returned values**

-   1, if the string starts with the specified prefix.
-   0, if the string doesn’t start with the specified prefix.

**Example**

Query:

``` sql
SELECT startsWith('Hello, world!', 'He');
```

Result:

``` text
┌─startsWith('Hello, world!', 'He')─┐
│                                 1 │
└───────────────────────────────────┘
```

## trim {#trim}

Removes all specified characters from the start or end of a string.
By default removes all consecutive occurrences of common whitespace (ASCII character 32) from both ends of a string.

**Syntax**

``` sql
trim([[LEADING|TRAILING|BOTH] trim_character FROM] input_string)
```

**Parameters**

-   `trim_character` — specified characters for trim. [String](../../sql-reference/data-types/string.md).
-   `input_string` — string for trim. [String](../../sql-reference/data-types/string.md).

**Returned value**

A string without leading and (or) trailing specified characters.

Type: `String`.

**Example**

Query:

``` sql
SELECT trim(BOTH ' ()' FROM '(   Hello, world!   )')
```

Result:

``` text
┌─trim(BOTH ' ()' FROM '(   Hello, world!   )')─┐
│ Hello, world!                                 │
└───────────────────────────────────────────────┘
```

## trimLeft {#trimleft}

Removes all consecutive occurrences of common whitespace (ASCII character 32) from the beginning of a string. It doesn’t remove other kinds of whitespace characters (tab, no-break space, etc.).

**Syntax**

``` sql
trimLeft(input_string)
```

Alias: `ltrim(input_string)`.

**Parameters**

-   `input_string` — string to trim. [String](../../sql-reference/data-types/string.md).

**Returned value**

A string without leading common whitespaces.

Type: `String`.

**Example**

Query:

``` sql
SELECT trimLeft('     Hello, world!     ')
```

Result:

``` text
┌─trimLeft('     Hello, world!     ')─┐
│ Hello, world!                       │
└─────────────────────────────────────┘
```

## trimRight {#trimright}

Removes all consecutive occurrences of common whitespace (ASCII character 32) from the end of a string. It doesn’t remove other kinds of whitespace characters (tab, no-break space, etc.).

**Syntax**

``` sql
trimRight(input_string)
```

Alias: `rtrim(input_string)`.

**Parameters**

-   `input_string` — string to trim. [String](../../sql-reference/data-types/string.md).

**Returned value**

A string without trailing common whitespaces.

Type: `String`.

**Example**

Query:

``` sql
SELECT trimRight('     Hello, world!     ')
```

Result:

``` text
┌─trimRight('     Hello, world!     ')─┐
│      Hello, world!                   │
└──────────────────────────────────────┘
```

## trimBoth {#trimboth}

Removes all consecutive occurrences of common whitespace (ASCII character 32) from both ends of a string. It doesn’t remove other kinds of whitespace characters (tab, no-break space, etc.).

**Syntax**

``` sql
trimBoth(input_string)
```

Alias: `trim(input_string)`.

**Parameters**

-   `input_string` — string to trim. [String](../../sql-reference/data-types/string.md).

**Returned value**

A string without leading and trailing common whitespaces.

Type: `String`.

**Example**

Query:

``` sql
SELECT trimBoth('     Hello, world!     ')
```

Result:

``` text
┌─trimBoth('     Hello, world!     ')─┐
│ Hello, world!                       │
└─────────────────────────────────────┘
```

## CRC32(s) {#crc32}

Returns the CRC32 checksum of a string, using CRC-32-IEEE 802.3 polynomial and initial value `0xffffffff` (zlib implementation).

The result type is UInt32.

## CRC32IEEE(s) {#crc32ieee}

Returns the CRC32 checksum of a string, using CRC-32-IEEE 802.3 polynomial.

The result type is UInt32.

## CRC64(s) {#crc64}

Returns the CRC64 checksum of a string, using CRC-64-ECMA polynomial.

The result type is UInt64.

## normalizeQuery {#normalized-query}

Replaces literals, sequences of literals and complex aliases with placeholders.

**Syntax** 
``` sql
normalizeQuery(x)
```

**Parameters** 

-   `x` — Sequence of characters. [String](../../sql-reference/data-types/string.md).

**Returned value**

-   Sequence of characters with placeholders.

Type: [String](../../sql-reference/data-types/string.md).

**Example**

Query:

``` sql
SELECT normalizeQuery('[1, 2, 3, x]') AS query;
```

Result:

``` text
┌─query────┐
│ [?.., x] │
└──────────┘
```

## normalizedQueryHash {#normalized-query-hash}

Returns identical 64bit hash values without the values of literals for similar queries. It helps to analyze query log.

**Syntax** 

``` sql
normalizedQueryHash(x)
```

**Parameters** 

-   `x` — Sequence of characters. [String](../../sql-reference/data-types/string.md).

**Returned value**

-   Hash value.

Type: [UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges).

**Example**

Query:

``` sql
SELECT normalizedQueryHash('SELECT 1 AS `xyz`') != normalizedQueryHash('SELECT 1 AS `abc`') AS res;
```

Result:

``` text
┌─res─┐
│   1 │
└─────┘
```

[Original article](https://clickhouse.tech/docs/en/query_language/functions/string_functions/) <!--hide-->
