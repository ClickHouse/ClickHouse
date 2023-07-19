---
slug: /en/sql-reference/functions/string-functions
sidebar_position: 170
sidebar_label: Strings
---

# Functions for Working with Strings

Functions for [searching](string-search-functions.md) in strings and for [replacing](string-replace-functions.md) in strings are described separately.

## empty

Checks whether the input string is empty.

A string is considered non-empty if it contains at least one byte, even if this byte is a space or the null byte.

The function is also available for [arrays](array-functions.md#function-empty) and [UUIDs](uuid-functions.md#empty).

**Syntax**

``` sql
empty(x)
```

**Arguments**

- `x` — Input value. [String](../data-types/string.md).

**Returned value**

- Returns `1` for an empty string or `0` for a non-empty string.

Type: [UInt8](../data-types/int-uint.md).

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

## notEmpty

Checks whether the input string is non-empty.

A string is considered non-empty if it contains at least one byte, even if this byte is a space or the null byte.

The function is also available for [arrays](array-functions.md#function-notempty) and [UUIDs](uuid-functions.md#notempty).

**Syntax**

``` sql
notEmpty(x)
```

**Arguments**

- `x` — Input value. [String](../data-types/string.md).

**Returned value**

- Returns `1` for a non-empty string or `0` for an empty string string.

Type: [UInt8](../data-types/int-uint.md).

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

## length

Returns the length of a string in bytes (not: in characters or Unicode code points).

The function also works for arrays.

Alias: `OCTET_LENGTH`

## lengthUTF8

Returns the length of a string in Unicode code points (not: in bytes or characters). It assumes that the string contains valid UTF-8 encoded text. If this assumption is violated, no exception is thrown and the result is undefined.

Alias:
- `CHAR_LENGTH``
- `CHARACTER_LENGTH`

## leftPad

Pads a string from the left with spaces or with a specified string (multiple times, if needed) until the resulting string reaches the specified `length`.

**Syntax**

``` sql
leftPad(string, length[, pad_string])
```

Alias: `LPAD`

**Arguments**

- `string` — Input string that should be padded. [String](../data-types/string.md).
- `length` — The length of the resulting string. [UInt or Int](../data-types/int-uint.md). If the value is smaller than the input string length, then the input string is shortened to `length` characters.
- `pad_string` — The string to pad the input string with. [String](../data-types/string.md). Optional. If not specified, then the input string is padded with spaces.

**Returned value**

- A left-padded string of the given length.

Type: [String](../data-types/string.md).

**Example**

``` sql
SELECT leftPad('abc', 7, '*'), leftPad('def', 7);
```

Result:

```result
┌─leftPad('abc', 7, '*')─┬─leftPad('def', 7)─┐
│ ****abc                │     def           │
└────────────────────────┴───────────────────┘
```

## leftPadUTF8

Pads the string from the left with spaces or a specified string (multiple times, if needed) until the resulting string reaches the given length. Unlike [leftPad](#leftpad) which measures the string length in bytes, the string length is measured in code points.

**Syntax**

``` sql
leftPadUTF8(string, length[, pad_string])
```

**Arguments**

- `string` — Input string that should be padded. [String](../data-types/string.md).
- `length` — The length of the resulting string. [UInt or Int](../data-types/int-uint.md). If the value is smaller than the input string length, then the input string is shortened to `length` characters.
- `pad_string` — The string to pad the input string with. [String](../data-types/string.md). Optional. If not specified, then the input string is padded with spaces.

**Returned value**

- A left-padded string of the given length.

Type: [String](../data-types/string.md).

**Example**

``` sql
SELECT leftPadUTF8('абвг', 7, '*'), leftPadUTF8('дежз', 7);
```

Result:

```result
┌─leftPadUTF8('абвг', 7, '*')─┬─leftPadUTF8('дежз', 7)─┐
│ ***абвг                     │    дежз                │
└─────────────────────────────┴────────────────────────┘
```

## rightPad

Pads a string from the right with spaces or with a specified string (multiple times, if needed) until the resulting string reaches the specified `length`.

**Syntax**

``` sql
rightPad(string, length[, pad_string])
```

Alias: `RPAD`

**Arguments**

- `string` — Input string that should be padded. [String](../data-types/string.md).
- `length` — The length of the resulting string. [UInt or Int](../data-types/int-uint.md). If the value is smaller than the input string length, then the input string is shortened to `length` characters.
- `pad_string` — The string to pad the input string with. [String](../data-types/string.md). Optional. If not specified, then the input string is padded with spaces.

**Returned value**

- A left-padded string of the given length.

Type: [String](../data-types/string.md).

**Example**

``` sql
SELECT rightPad('abc', 7, '*'), rightPad('abc', 7);
```

Result:

```result
┌─rightPad('abc', 7, '*')─┬─rightPad('abc', 7)─┐
│ abc****                 │ abc                │
└─────────────────────────┴────────────────────┘
```

## rightPadUTF8

Pads the string from the right with spaces or a specified string (multiple times, if needed) until the resulting string reaches the given length. Unlike [rightPad](#rightpad) which measures the string length in bytes, the string length is measured in code points.

**Syntax**

``` sql
rightPadUTF8(string, length[, pad_string])
```

**Arguments**

- `string` — Input string that should be padded. [String](../data-types/string.md).
- `length` — The length of the resulting string. [UInt or Int](../data-types/int-uint.md). If the value is smaller than the input string length, then the input string is shortened to `length` characters.
- `pad_string` — The string to pad the input string with. [String](../data-types/string.md). Optional. If not specified, then the input string is padded with spaces.

**Returned value**

- A right-padded string of the given length.

Type: [String](../data-types/string.md).

**Example**

``` sql
SELECT rightPadUTF8('абвг', 7, '*'), rightPadUTF8('абвг', 7);
```

Result:

```result
┌─rightPadUTF8('абвг', 7, '*')─┬─rightPadUTF8('абвг', 7)─┐
│ абвг***                      │ абвг                    │
└──────────────────────────────┴─────────────────────────┘
```

## lower

Converts the ASCII Latin symbols in a string to lowercase.

Alias: `lcase`

## upper

Converts the ASCII Latin symbols in a string to uppercase.

Alias: `ucase`

## lowerUTF8

Converts a string to lowercase, assuming that the string contains valid UTF-8 encoded text. If this assumption is violated, no exception is thrown and the result is undefined.

Does not detect the language, e.g. for Turkish the result might not be exactly correct (i/İ vs. i/I).

If the length of the UTF-8 byte sequence is different for upper and lower case of a code point, the result may be incorrect for this code point.

## upperUTF8

Converts a string to uppercase, assuming that the string contains valid UTF-8 encoded text. If this assumption is violated, no exception is thrown and the result is undefined.

Does not detect the language, e.g. for Turkish the result might not be exactly correct (i/İ vs. i/I).

If the length of the UTF-8 byte sequence is different for upper and lower case of a code point, the result may be incorrect for this code point.

## isValidUTF8

Returns 1, if the set of bytes constitutes valid UTF-8-encoded text, otherwise 0.

## toValidUTF8

Replaces invalid UTF-8 characters by the `�` (U+FFFD) character. All running in a row invalid characters are collapsed into the one replacement character.

**Syntax**

``` sql
toValidUTF8(input_string)
```

**Arguments**

- `input_string` — Any set of bytes represented as the [String](../../sql-reference/data-types/string.md) data type object.

**Returned value**

- A valid UTF-8 string.

**Example**

``` sql
SELECT toValidUTF8('\x61\xF0\x80\x80\x80b');
```

```result
┌─toValidUTF8('a����b')─┐
│ a�b                   │
└───────────────────────┘
```

## repeat

Concatenates a string as many times with itself as specified.

**Syntax**

``` sql
repeat(s, n)
```

Alias: `REPEAT`

**Arguments**

- `s` — The string to repeat. [String](../../sql-reference/data-types/string.md).
- `n` — The number of times to repeat the string. [UInt* or Int*](../../sql-reference/data-types/int-uint.md).

**Returned value**

A string containing string `s` repeated `n` times. If `n` <= 0, the function returns the empty string.

Type: `String`.

**Example**

``` sql
SELECT repeat('abc', 10);
```

Result:

```result
┌─repeat('abc', 10)──────────────┐
│ abcabcabcabcabcabcabcabcabcabc │
└────────────────────────────────┘
```

## space

Concatenates a space (` `) as many times with itself as specified.

**Syntax**

``` sql
space(n)
```

Alias: `SPACE`.

**Arguments**

- `n` — The number of times to repeat the space. [UInt* or Int*](../../sql-reference/data-types/int-uint.md).

**Returned value**

The string containing string ` ` repeated `n` times. If `n` <= 0, the function returns the empty string.

Type: `String`.

**Example**

Query:

``` sql
SELECT space(3);
```

Result:

``` text
┌─space(3) ────┐
│              │
└──────────────┘
```

## reverse

Reverses the sequence of bytes in a string.

## reverseUTF8

Reverses a sequence of Unicode code points in a string. Assumes that the string contains valid UTF-8 encoded text. If this assumption is violated, no exception is thrown and the result is undefined.

## format

Format the `pattern` string with the strings listed in the arguments, similar to formatting in Python. The pattern string can contain replacement fields surrounded by curly braces `{}`. Anything not contained in braces is considered literal text and copied verbatim into the output. Literal brace character can be escaped by two braces: `{{ '{{' }}` and `{{ '}}' }}`. Field names can be numbers (starting from zero) or empty (then they are implicitly given monotonically increasing numbers).

**Syntax**

```sql
format(pattern, s0, s1, …)
```

**Example**

``` sql
SELECT format('{1} {0} {1}', 'World', 'Hello')
```

```result
┌─format('{1} {0} {1}', 'World', 'Hello')─┐
│ Hello World Hello                       │
└─────────────────────────────────────────┘
```

With implicit numbers:

``` sql
SELECT format('{} {}', 'Hello', 'World')
```

```result
┌─format('{} {}', 'Hello', 'World')─┐
│ Hello World                       │
└───────────────────────────────────┘
```

## concat

Concatenates the strings listed in the arguments without separator.

**Syntax**

``` sql
concat(s1, s2, ...)
```

**Arguments**

Values of type String or FixedString.

**Returned values**

The String created by concatenating the arguments.

If any of arguments is `NULL`, the function returns `NULL`.

**Example**

``` sql
SELECT concat('Hello, ', 'World!');
```

Result:

```result
┌─concat('Hello, ', 'World!')─┐
│ Hello, World!               │
└─────────────────────────────┘
```

## concatAssumeInjective

Like [concat](#concat) but assumes that `concat(s1, s2, ...) → sn` is injective. Can be used for optimization of GROUP BY.

A function is called injective if it returns for different arguments different results. In other words: different arguments never produce identical result.

**Syntax**

``` sql
concatAssumeInjective(s1, s2, ...)
```

**Arguments**

Values of type String or FixedString.

**Returned values**

The String created by concatenating the arguments.

If any of argument values is `NULL`, the function returns `NULL`.

**Example**

Input table:

``` sql
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

``` sql
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

## concatWithSeparator

Concatenates the given strings with a given separator.

**Syntax**

``` sql
concatWithSeparator(sep, expr1, expr2, expr3...)
```

**Arguments**

- sep — separator. Const [String](../../sql-reference/data-types/string.md) or [FixedString](../../sql-reference/data-types/fixedstring.md).
- exprN — expression to be concatenated. [String](../../sql-reference/data-types/string.md) or [FixedString](../../sql-reference/data-types/fixedstring.md).

**Returned values**

The String created by concatenating the arguments.

If any of the argument values is `NULL`, the function returns `NULL`.

**Example**

``` sql
SELECT concatWithSeparator('a', '1', '2', '3', '4')
```

Result:

```result
┌─concatWithSeparator('a', '1', '2', '3', '4')─┐
│ 1a2a3a4                           │
└───────────────────────────────────┘
```

## concatWithSeparatorAssumeInjective

Like `concatWithSeparator` but assumes that `concatWithSeparator(sep, expr1, expr2, expr3...) → result` is injective. Can be used for optimization of GROUP BY.

A function is called injective if it returns for different arguments different results. In other words: different arguments never produce identical result.

## substring(s, offset, length)

Returns a substring with `length` many bytes, starting at the byte at index `offset`. Character indexing starts from 1.

**Syntax**

```sql
substring(s, offset, length)
```

Alias:
- `substr`
- `mid`

## substringUTF8

Like `substring` but for Unicode code points. Assumes that the string contains valid UTF-8 encoded text. If this assumption is violated, no exception is thrown and the result is undefined.

## appendTrailingCharIfAbsent

Appends character `c` to string `s` if `s` is non-empty and does not end with character `c`.

**Syntax**

```sql
appendTrailingCharIfAbsent(s, c)
```

## convertCharset

Returns string `s` converted from the encoding `from` to encoding `to`.

**Syntax**

```sql
convertCharset(s, from, to)
```

## base58Encode

Encodes a String using [Base58](https://tools.ietf.org/id/draft-msporny-base58-01.html) in the "Bitcoin" alphabet.

**Syntax**

```sql
base58Encode(plaintext)
```

**Arguments**

- `plaintext` — [String](../../sql-reference/data-types/string.md) column or constant.

**Returned value**

- A string containing the encoded value of the argument.

Type: [String](../../sql-reference/data-types/string.md).

**Example**

``` sql
SELECT base58Encode('Encoded');
```

Result:

```result
┌─base58Encode('Encoded')─┐
│ 3dc8KtHrwM              │
└─────────────────────────┘
```

## base58Decode

Accepts a String and decodes it using [Base58](https://tools.ietf.org/id/draft-msporny-base58-01.html) encoding scheme using "Bitcoin" alphabet.

**Syntax**

```sql
base58Decode(encoded)
```

**Arguments**

- `encoded` — [String](../../sql-reference/data-types/string.md) column or constant. If the string is not a valid Base58-encoded value, an exception is thrown.

**Returned value**

- A string containing the decoded value of the argument.

Type: [String](../../sql-reference/data-types/string.md).

**Example**

``` sql
SELECT base58Decode('3dc8KtHrwM');
```

Result:

```result
┌─base58Decode('3dc8KtHrwM')─┐
│ Encoded                    │
└────────────────────────────┘
```

## tryBase58Decode

Like `base58Decode` but returns an empty string in case of error.

## base64Encode

Encodes a String or FixedString as base64.

Alias: `TO_BASE64`.

## base64Decode

Decodes a base64-encoded String or FixedString. Throws an exception in case of error.

Alias: `FROM_BASE64`.

## tryBase64Decode

Like `base64Decode` but returns an empty string in case of error.

## endsWith

Returns whether string `str` ends with `suffix`.

**Syntax**

```sql
endsWith(str, suffix)
```

## startsWith

Returns whether string `str` starts with `prefix`.

**Syntax**

```sql
startsWith(str, prefix)
```

**Example**

``` sql
SELECT startsWith('Spider-Man', 'Spi');
```

## trim

Removes the specified characters from the start or end of a string. If not specified otherwise, the function removes whitespace (ASCII-character 32).

**Syntax**

``` sql
trim([[LEADING|TRAILING|BOTH] trim_character FROM] input_string)
```

**Arguments**

- `trim_character` — Specified characters for trim. [String](../../sql-reference/data-types/string.md).
- `input_string` — String for trim. [String](../../sql-reference/data-types/string.md).

**Returned value**

A string without leading and/or trailing specified characters.

Type: `String`.

**Example**

``` sql
SELECT trim(BOTH ' ()' FROM '(   Hello, world!   )');
```

Result:

```result
┌─trim(BOTH ' ()' FROM '(   Hello, world!   )')─┐
│ Hello, world!                                 │
└───────────────────────────────────────────────┘
```

## trimLeft

Removes the consecutive occurrences of whitespace (ASCII-character 32) from the start of a string.

**Syntax**

``` sql
trimLeft(input_string)
```

Alias: `ltrim(input_string)`.

**Arguments**

- `input_string` — string to trim. [String](../../sql-reference/data-types/string.md).

**Returned value**

A string without leading common whitespaces.

Type: `String`.

**Example**

``` sql
SELECT trimLeft('     Hello, world!     ');
```

Result:

```result
┌─trimLeft('     Hello, world!     ')─┐
│ Hello, world!                       │
└─────────────────────────────────────┘
```

## trimRight

Removes the consecutive occurrences of whitespace (ASCII-character 32) from the end of a string.

**Syntax**

``` sql
trimRight(input_string)
```

Alias: `rtrim(input_string)`.

**Arguments**

- `input_string` — string to trim. [String](../../sql-reference/data-types/string.md).

**Returned value**

A string without trailing common whitespaces.

Type: `String`.

**Example**

``` sql
SELECT trimRight('     Hello, world!     ');
```

Result:

```result
┌─trimRight('     Hello, world!     ')─┐
│      Hello, world!                   │
└──────────────────────────────────────┘
```

## trimBoth

Removes the consecutive occurrences of whitespace (ASCII-character 32) from both ends of a string.

**Syntax**

``` sql
trimBoth(input_string)
```

Alias: `trim(input_string)`.

**Arguments**

- `input_string` — string to trim. [String](../../sql-reference/data-types/string.md).

**Returned value**

A string without leading and trailing common whitespaces.

Type: `String`.

**Example**

``` sql
SELECT trimBoth('     Hello, world!     ');
```

Result:

```result
┌─trimBoth('     Hello, world!     ')─┐
│ Hello, world!                       │
└─────────────────────────────────────┘
```

## CRC32

Returns the CRC32 checksum of a string using CRC-32-IEEE 802.3 polynomial and initial value `0xffffffff` (zlib implementation).

The result type is UInt32.

## CRC32IEEE

Returns the CRC32 checksum of a string, using CRC-32-IEEE 802.3 polynomial.

The result type is UInt32.

## CRC64

Returns the CRC64 checksum of a string, using CRC-64-ECMA polynomial.

The result type is UInt64.

## normalizeQuery

Replaces literals, sequences of literals and complex aliases with placeholders.

**Syntax**

``` sql
normalizeQuery(x)
```

**Arguments**

- `x` — Sequence of characters. [String](../../sql-reference/data-types/string.md).

**Returned value**

- Sequence of characters with placeholders.

Type: [String](../../sql-reference/data-types/string.md).

**Example**

``` sql
SELECT normalizeQuery('[1, 2, 3, x]') AS query;
```

Result:

```result
┌─query────┐
│ [?.., x] │
└──────────┘
```

## normalizedQueryHash

Returns identical 64bit hash values without the values of literals for similar queries. Can be helpful to analyze query log.

**Syntax**

``` sql
normalizedQueryHash(x)
```

**Arguments**

- `x` — Sequence of characters. [String](../../sql-reference/data-types/string.md).

**Returned value**

- Hash value.

Type: [UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges).

**Example**

``` sql
SELECT normalizedQueryHash('SELECT 1 AS `xyz`') != normalizedQueryHash('SELECT 1 AS `abc`') AS res;
```

Result:

```result
┌─res─┐
│   1 │
└─────┘
```

## normalizeUTF8NFC

Converts a string to [NFC normalized form](https://en.wikipedia.org/wiki/Unicode_equivalence#Normal_forms), assuming the string is valid UTF8-encoded text.

**Syntax**

``` sql
normalizeUTF8NFC(words)
```

**Arguments**

- `words` — UTF8-encoded input string. [String](../../sql-reference/data-types/string.md).

**Returned value**

- String transformed to NFC normalization form.

Type: [String](../../sql-reference/data-types/string.md).

**Example**

``` sql
SELECT length('â'), normalizeUTF8NFC('â') AS nfc, length(nfc) AS nfc_len;
```

Result:

```result
┌─length('â')─┬─nfc─┬─nfc_len─┐
│           2 │ â   │       2 │
└─────────────┴─────┴─────────┘
```

## normalizeUTF8NFD

Converts a string to [NFD normalized form](https://en.wikipedia.org/wiki/Unicode_equivalence#Normal_forms), assuming the string is valid UTF8-encoded text.

**Syntax**

``` sql
normalizeUTF8NFD(words)
```

**Arguments**

- `words` — UTF8-encoded input string. [String](../../sql-reference/data-types/string.md).

**Returned value**

- String transformed to NFD normalization form.

Type: [String](../../sql-reference/data-types/string.md).

**Example**

``` sql
SELECT length('â'), normalizeUTF8NFD('â') AS nfd, length(nfd) AS nfd_len;
```

Result:

```result
┌─length('â')─┬─nfd─┬─nfd_len─┐
│           2 │ â   │       3 │
└─────────────┴─────┴─────────┘
```

## normalizeUTF8NFKC

Converts a string to [NFKC normalized form](https://en.wikipedia.org/wiki/Unicode_equivalence#Normal_forms), assuming the string is valid UTF8-encoded text.

**Syntax**

``` sql
normalizeUTF8NFKC(words)
```

**Arguments**

- `words` — UTF8-encoded input string. [String](../../sql-reference/data-types/string.md).

**Returned value**

- String transformed to NFKC normalization form.

Type: [String](../../sql-reference/data-types/string.md).

**Example**

``` sql
SELECT length('â'), normalizeUTF8NFKC('â') AS nfkc, length(nfkc) AS nfkc_len;
```

Result:

```result
┌─length('â')─┬─nfkc─┬─nfkc_len─┐
│           2 │ â    │        2 │
└─────────────┴──────┴──────────┘
```

## normalizeUTF8NFKD

Converts a string to [NFKD normalized form](https://en.wikipedia.org/wiki/Unicode_equivalence#Normal_forms), assuming the string is valid UTF8-encoded text.

**Syntax**

``` sql
normalizeUTF8NFKD(words)
```

**Arguments**

- `words` — UTF8-encoded input string. [String](../../sql-reference/data-types/string.md).

**Returned value**

- String transformed to NFKD normalization form.

Type: [String](../../sql-reference/data-types/string.md).

**Example**

``` sql
SELECT length('â'), normalizeUTF8NFKD('â') AS nfkd, length(nfkd) AS nfkd_len;
```

Result:

```result
┌─length('â')─┬─nfkd─┬─nfkd_len─┐
│           2 │ â    │        3 │
└─────────────┴──────┴──────────┘
```

## encodeXMLComponent

Escapes characters with special meaning in XML such that they can afterwards be place into a XML text node or attribute.

The following characters are replaced: `<`, `&`, `>`, `"`, `'`.
Also see the [list of XML and HTML character entity references](https://en.wikipedia.org/wiki/List_of_XML_and_HTML_character_entity_references).

**Syntax**

``` sql
encodeXMLComponent(x)
```

**Arguments**

- `x` — An input string. [String](../../sql-reference/data-types/string.md).

**Returned value**

- The escaped string.

Type: [String](../../sql-reference/data-types/string.md).

**Example**

``` sql
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

## decodeXMLComponent

Un-escapes substrings with special meaning in XML. These substrings are: `&quot;` `&amp;` `&apos;` `&gt;` `&lt;`

This function also replaces numeric character references with Unicode characters. Both decimal (like `&#10003;`) and hexadecimal (`&#x2713;`) forms are supported.

**Syntax**

``` sql
decodeXMLComponent(x)
```

**Arguments**

- `x` — An input string. [String](../../sql-reference/data-types/string.md).

**Returned value**

- The un-escaped string.

Type: [String](../../sql-reference/data-types/string.md).

**Example**

``` sql
SELECT decodeXMLComponent('&apos;foo&apos;');
SELECT decodeXMLComponent('&lt; &#x3A3; &gt;');
```

Result:

```result
'foo'
< Σ >
```

## extractTextFromHTML

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

``` sql
extractTextFromHTML(x)
```

**Arguments**

- `x` — input text. [String](../../sql-reference/data-types/string.md).

**Returned value**

- Extracted text.

Type: [String](../../sql-reference/data-types/string.md).

**Example**

The first example contains several tags and a comment and also shows whitespace processing.
The second example shows `CDATA` and `script` tag processing.
In the third example text is extracted from the full HTML response received by the [url](../../sql-reference/table-functions/url.md) function.

``` sql
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

## soundex

Returns the [Soundex code](https://en.wikipedia.org/wiki/Soundex) of a string.

**Syntax**

``` sql
soundex(val)
```

**Arguments**

- `val` - Input value. [String](../data-types/string.md)

**Returned value**

- The Soundex code of the input value. [String](../data-types/string.md)

**Example**

``` sql
select soundex('aksel');
```

Result:

```result
┌─soundex('aksel')─┐
│ A240             │
└──────────────────┘
```

## initcap

Convert the first letter of each word to upper case and the rest to lower case. Words are sequences of alphanumeric characters separated by non-alphanumeric characters.

## initcapUTF8

Like [initcap](#initcap), assuming that the string contains valid UTF-8 encoded text. If this assumption is violated, no exception is thrown and the result is undefined.

Does not detect the language, e.g. for Turkish the result might not be exactly correct (i/İ vs. i/I).

If the length of the UTF-8 byte sequence is different for upper and lower case of a code point, the result may be incorrect for this code point.

## firstLine

Returns the first line from a multi-line string.

**Syntax**

```sql
firstLine(val)
```

**Arguments**

- `val` - Input value. [String](../data-types/string.md)

**Returned value**

- The first line of the input value or the whole value if there is no line
  separators. [String](../data-types/string.md)

**Example**

```sql
select firstLine('foo\nbar\nbaz');
```

Result:

```result
┌─firstLine('foo\nbar\nbaz')─┐
│ foo                        │
└────────────────────────────┘
```
