---
sidebar_position: 40
sidebar_label: Strings
---

# Functions for Working with Strings {#functions-for-working-with-strings}

:::note    
Functions for [searching](../../sql-reference/functions/string-search-functions.md) and [replacing](../../sql-reference/functions/string-replace-functions.md) in strings are described separately.
:::

## empty {#empty}

Checks whether the input string is empty.

**Syntax**

``` sql
empty(x)
```

A string is considered non-empty if it contains at least one byte, even if this is a space or a null byte.

The function also works for [arrays](array-functions.md#function-empty) or [UUID](uuid-functions.md#empty).

**Arguments**

-   `x` — Input value. [String](../data-types/string.md).

**Returned value**

-   Returns `1` for an empty string or `0` for a non-empty string.

Type: [UInt8](../data-types/int-uint.md).

**Example**

Query:

```sql
SELECT empty('');
```

Result:

```text
┌─empty('')─┐
│         1 │
└───────────┘
```

## notEmpty {#notempty}

Checks whether the input string is non-empty.

**Syntax**

``` sql
notEmpty(x)
```

A string is considered non-empty if it contains at least one byte, even if this is a space or a null byte.

The function also works for [arrays](array-functions.md#function-notempty) or [UUID](uuid-functions.md#notempty).

**Arguments**

-   `x` — Input value. [String](../data-types/string.md).

**Returned value**

-   Returns `1` for a non-empty string or `0` for an empty string string.

Type: [UInt8](../data-types/int-uint.md).

**Example**

Query:

```sql
SELECT notEmpty('text');
```

Result:

```text
┌─notEmpty('text')─┐
│                1 │
└──────────────────┘
```

## length {#length}

Returns the length of a string in bytes (not in characters, and not in code points).
The result type is UInt64.
The function also works for arrays.

## lengthUTF8 {#lengthutf8}

Returns the length of a string in Unicode code points (not in characters), assuming that the string contains a set of bytes that make up UTF-8 encoded text. If this assumption is not met, it returns some result (it does not throw an exception).
The result type is UInt64.

## char_length, CHAR_LENGTH {#char-length}

Returns the length of a string in Unicode code points (not in characters), assuming that the string contains a set of bytes that make up UTF-8 encoded text. If this assumption is not met, it returns some result (it does not throw an exception).
The result type is UInt64.

## character_length, CHARACTER_LENGTH {#character-length}

Returns the length of a string in Unicode code points (not in characters), assuming that the string contains a set of bytes that make up UTF-8 encoded text. If this assumption is not met, it returns some result (it does not throw an exception).
The result type is UInt64.

## leftPad {#leftpad}

Pads the current string from the left with spaces or a specified string (multiple times, if needed) until the resulting string reaches the given length. Similarly to the MySQL `LPAD` function.

**Syntax**

``` sql
leftPad('string', 'length'[, 'pad_string'])
```

**Arguments**

-   `string` — Input string that needs to be padded. [String](../data-types/string.md).
-   `length` — The length of the resulting string. [UInt](../data-types/int-uint.md). If the value is less than the input string length, then the input string is returned as-is.
-   `pad_string` — The string to pad the input string with. [String](../data-types/string.md). Optional. If not specified, then the input string is padded with spaces.

**Returned value**

-   The resulting string of the given length.

Type: [String](../data-types/string.md).

**Example**

Query:

``` sql
SELECT leftPad('abc', 7, '*'), leftPad('def', 7);
```

Result:

``` text
┌─leftPad('abc', 7, '*')─┬─leftPad('def', 7)─┐
│ ****abc                │     def           │
└────────────────────────┴───────────────────┘
```

## leftPadUTF8 {#leftpadutf8}

Pads the current string from the left with spaces or a specified string (multiple times, if needed) until the resulting string reaches the given length. Similarly to the MySQL `LPAD` function. While in the [leftPad](#leftpad) function the length is measured in bytes, here in the `leftPadUTF8` function it is measured in code points.

**Syntax**

``` sql
leftPadUTF8('string','length'[, 'pad_string'])
```

**Arguments**

-   `string` — Input string that needs to be padded. [String](../data-types/string.md).
-   `length` — The length of the resulting string. [UInt](../data-types/int-uint.md). If the value is less than the input string length, then the input string is returned as-is.
-   `pad_string` — The string to pad the input string with. [String](../data-types/string.md). Optional. If not specified, then the input string is padded with spaces.

**Returned value**

-   The resulting string of the given length.

Type: [String](../data-types/string.md).

**Example**

Query:

``` sql
SELECT leftPadUTF8('абвг', 7, '*'), leftPadUTF8('дежз', 7);
```

Result:

``` text
┌─leftPadUTF8('абвг', 7, '*')─┬─leftPadUTF8('дежз', 7)─┐
│ ***абвг                     │    дежз                │
└─────────────────────────────┴────────────────────────┘
```

## rightPad {#rightpad}

Pads the current string from the right with spaces or a specified string (multiple times, if needed) until the resulting string reaches the given length. Similarly to the MySQL `RPAD` function.

**Syntax**

``` sql
rightPad('string', 'length'[, 'pad_string'])
```

**Arguments**

-   `string` — Input string that needs to be padded. [String](../data-types/string.md).
-   `length` — The length of the resulting string. [UInt](../data-types/int-uint.md). If the value is less than the input string length, then the input string is returned as-is.
-   `pad_string` — The string to pad the input string with. [String](../data-types/string.md). Optional. If not specified, then the input string is padded with spaces.

**Returned value**

-   The resulting string of the given length.

Type: [String](../data-types/string.md).

**Example**

Query:

``` sql
SELECT rightPad('abc', 7, '*'), rightPad('abc', 7);
```

Result:

``` text
┌─rightPad('abc', 7, '*')─┬─rightPad('abc', 7)─┐
│ abc****                 │ abc                │
└─────────────────────────┴────────────────────┘
```

## rightPadUTF8 {#rightpadutf8}

Pads the current string from the right with spaces or a specified string (multiple times, if needed) until the resulting string reaches the given length. Similarly to the MySQL `RPAD` function. While in the [rightPad](#rightpad) function the length is measured in bytes, here in the `rightPadUTF8` function it is measured in code points.

**Syntax**

``` sql
rightPadUTF8('string','length'[, 'pad_string'])
```

**Arguments**

-   `string` — Input string that needs to be padded. [String](../data-types/string.md).
-   `length` — The length of the resulting string. [UInt](../data-types/int-uint.md). If the value is less than the input string length, then the input string is returned as-is.
-   `pad_string` — The string to pad the input string with. [String](../data-types/string.md). Optional. If not specified, then the input string is padded with spaces.

**Returned value**

-   The resulting string of the given length.

Type: [String](../data-types/string.md).

**Example**

Query:

``` sql
SELECT rightPadUTF8('абвг', 7, '*'), rightPadUTF8('абвг', 7);
```

Result:

``` text
┌─rightPadUTF8('абвг', 7, '*')─┬─rightPadUTF8('абвг', 7)─┐
│ абвг***                      │ абвг                    │
└──────────────────────────────┴─────────────────────────┘
```

## lower, lcase {#lower}

Converts ASCII Latin symbols in a string to lowercase.

## upper, ucase {#upper}

Converts ASCII Latin symbols in a string to uppercase.

## lowerUTF8 {#lowerutf8}

Converts a string to lowercase, assuming the string contains a set of bytes that make up a UTF-8 encoded text.
It does not detect the language. So for Turkish the result might not be exactly correct.
If the length of the UTF-8 byte sequence is different for upper and lower case of a code point, the result may be incorrect for this code point.
If the string contains a set of bytes that is not UTF-8, then the behavior is undefined.

## upperUTF8 {#upperutf8}

Converts a string to uppercase, assuming the string contains a set of bytes that make up a UTF-8 encoded text.
It does not detect the language. So for Turkish the result might not be exactly correct.
If the length of the UTF-8 byte sequence is different for upper and lower case of a code point, the result may be incorrect for this code point.
If the string contains a set of bytes that is not UTF-8, then the behavior is undefined.

## isValidUTF8 {#isvalidutf8}

Returns 1, if the set of bytes is valid UTF-8 encoded, otherwise 0.

## toValidUTF8 {#tovalidutf8}

Replaces invalid UTF-8 characters by the `�` (U+FFFD) character. All running in a row invalid characters are collapsed into the one replacement character.

``` sql
toValidUTF8(input_string)
```

**Arguments**

-   `input_string` — Any set of bytes represented as the [String](../../sql-reference/data-types/string.md) data type object.

Returned value: Valid UTF-8 string.

**Example**

``` sql
SELECT toValidUTF8('\x61\xF0\x80\x80\x80b');
```

``` text
┌─toValidUTF8('a����b')─┐
│ a�b                   │
└───────────────────────┘
```

## repeat {#repeat}

Repeats a string as many times as specified and concatenates the replicated values as a single string.

Alias: `REPEAT`.

**Syntax**

``` sql
repeat(s, n)
```

**Arguments**

-   `s` — The string to repeat. [String](../../sql-reference/data-types/string.md).
-   `n` — The number of times to repeat the string. [UInt](../../sql-reference/data-types/int-uint.md).

**Returned value**

The single string, which contains the string `s` repeated `n` times. If `n` \< 1, the function returns empty string.

Type: `String`.

**Example**

Query:

``` sql
SELECT repeat('abc', 10);
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

Reverses a sequence of Unicode code points, assuming that the string contains a set of bytes representing a UTF-8 text. Otherwise, it does something else (it does not throw an exception).

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

**Arguments**

Values of type String or FixedString.

**Returned values**

Returns the String that results from concatenating the arguments.

If any of argument values is `NULL`, `concat` returns `NULL`.

**Example**

Query:

``` sql
SELECT concat('Hello, ', 'World!');
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

**Arguments**

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
SELECT concat(key1, key2), sum(value) FROM key_val GROUP BY concatAssumeInjective(key1, key2);
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

Returns a substring starting with the byte from the ‘offset’ index that is ‘length’ bytes long. Character indexing starts from one (as in standard SQL).

## substringUTF8(s, offset, length) {#substringutf8}

The same as ‘substring’, but for Unicode code points. Works under the assumption that the string contains a set of bytes representing a UTF-8 encoded text. If this assumption is not met, it returns some result (it does not throw an exception).

## appendTrailingCharIfAbsent(s, c) {#appendtrailingcharifabsent}

If the ‘s’ string is non-empty and does not contain the ‘c’ character at the end, it appends the ‘c’ character to the end.

## convertCharset(s, from, to) {#convertcharset}

Returns the string ‘s’ that was converted from the encoding in ‘from’ to the encoding in ‘to’.

## base64Encode(s) {#base64encode}

Encodes ‘s’ string into base64

Alias: `TO_BASE64`.

## base64Decode(s) {#base64decode}

Decode base64-encoded string ‘s’ into original string. In case of failure raises an exception.

Alias: `FROM_BASE64`.

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
-   0, if the string does not start with the specified prefix.

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

**Arguments**

-   `trim_character` — Specified characters for trim. [String](../../sql-reference/data-types/string.md).
-   `input_string` — String for trim. [String](../../sql-reference/data-types/string.md).

**Returned value**

A string without leading and (or) trailing specified characters.

Type: `String`.

**Example**

Query:

``` sql
SELECT trim(BOTH ' ()' FROM '(   Hello, world!   )');
```

Result:

``` text
┌─trim(BOTH ' ()' FROM '(   Hello, world!   )')─┐
│ Hello, world!                                 │
└───────────────────────────────────────────────┘
```

## trimLeft {#trimleft}

Removes all consecutive occurrences of common whitespace (ASCII character 32) from the beginning of a string. It does not remove other kinds of whitespace characters (tab, no-break space, etc.).

**Syntax**

``` sql
trimLeft(input_string)
```

Alias: `ltrim(input_string)`.

**Arguments**

-   `input_string` — string to trim. [String](../../sql-reference/data-types/string.md).

**Returned value**

A string without leading common whitespaces.

Type: `String`.

**Example**

Query:

``` sql
SELECT trimLeft('     Hello, world!     ');
```

Result:

``` text
┌─trimLeft('     Hello, world!     ')─┐
│ Hello, world!                       │
└─────────────────────────────────────┘
```

## trimRight {#trimright}

Removes all consecutive occurrences of common whitespace (ASCII character 32) from the end of a string. It does not remove other kinds of whitespace characters (tab, no-break space, etc.).

**Syntax**

``` sql
trimRight(input_string)
```

Alias: `rtrim(input_string)`.

**Arguments**

-   `input_string` — string to trim. [String](../../sql-reference/data-types/string.md).

**Returned value**

A string without trailing common whitespaces.

Type: `String`.

**Example**

Query:

``` sql
SELECT trimRight('     Hello, world!     ');
```

Result:

``` text
┌─trimRight('     Hello, world!     ')─┐
│      Hello, world!                   │
└──────────────────────────────────────┘
```

## trimBoth {#trimboth}

Removes all consecutive occurrences of common whitespace (ASCII character 32) from both ends of a string. It does not remove other kinds of whitespace characters (tab, no-break space, etc.).

**Syntax**

``` sql
trimBoth(input_string)
```

Alias: `trim(input_string)`.

**Arguments**

-   `input_string` — string to trim. [String](../../sql-reference/data-types/string.md).

**Returned value**

A string without leading and trailing common whitespaces.

Type: `String`.

**Example**

Query:

``` sql
SELECT trimBoth('     Hello, world!     ');
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

**Arguments**

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

**Arguments**

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

## normalizeUTF8NFC {#normalizeutf8nfc}

Converts a string to [NFC normalized form](https://en.wikipedia.org/wiki/Unicode_equivalence#Normal_forms), assuming the string contains a set of bytes that make up a UTF-8 encoded text.

**Syntax**

``` sql
normalizeUTF8NFC(words)
```

**Arguments**

-   `words` — Input string that contains UTF-8 encoded text. [String](../../sql-reference/data-types/string.md).

**Returned value**

-   String transformed to NFC normalization form.

Type: [String](../../sql-reference/data-types/string.md).

**Example**

Query:

``` sql
SELECT length('â'), normalizeUTF8NFC('â') AS nfc, length(nfc) AS nfc_len;
```

Result:

``` text
┌─length('â')─┬─nfc─┬─nfc_len─┐
│           2 │ â   │       2 │
└─────────────┴─────┴─────────┘
```

## normalizeUTF8NFD {#normalizeutf8nfd}

Converts a string to [NFD normalized form](https://en.wikipedia.org/wiki/Unicode_equivalence#Normal_forms), assuming the string contains a set of bytes that make up a UTF-8 encoded text.

**Syntax**

``` sql
normalizeUTF8NFD(words)
```

**Arguments**

-   `words` — Input string that contains UTF-8 encoded text. [String](../../sql-reference/data-types/string.md).

**Returned value**

-   String transformed to NFD normalization form.

Type: [String](../../sql-reference/data-types/string.md).

**Example**

Query:

``` sql
SELECT length('â'), normalizeUTF8NFD('â') AS nfd, length(nfd) AS nfd_len;
```

Result:

``` text
┌─length('â')─┬─nfd─┬─nfd_len─┐
│           2 │ â   │       3 │
└─────────────┴─────┴─────────┘
```

## normalizeUTF8NFKC {#normalizeutf8nfkc}

Converts a string to [NFKC normalized form](https://en.wikipedia.org/wiki/Unicode_equivalence#Normal_forms), assuming the string contains a set of bytes that make up a UTF-8 encoded text.

**Syntax**

``` sql
normalizeUTF8NFKC(words)
```

**Arguments**

-   `words` — Input string that contains UTF-8 encoded text. [String](../../sql-reference/data-types/string.md).

**Returned value**

-   String transformed to NFKC normalization form.

Type: [String](../../sql-reference/data-types/string.md).

**Example**

Query:

``` sql
SELECT length('â'), normalizeUTF8NFKC('â') AS nfkc, length(nfkc) AS nfkc_len;
```

Result:

``` text
┌─length('â')─┬─nfkc─┬─nfkc_len─┐
│           2 │ â    │        2 │
└─────────────┴──────┴──────────┘
```

## normalizeUTF8NFKD {#normalizeutf8nfkd}

Converts a string to [NFKD normalized form](https://en.wikipedia.org/wiki/Unicode_equivalence#Normal_forms), assuming the string contains a set of bytes that make up a UTF-8 encoded text.

**Syntax**

``` sql
normalizeUTF8NFKD(words)
```

**Arguments**

-   `words` — Input string that contains UTF-8 encoded text. [String](../../sql-reference/data-types/string.md).

**Returned value**

-   String transformed to NFKD normalization form.

Type: [String](../../sql-reference/data-types/string.md).

**Example**

Query:

``` sql
SELECT length('â'), normalizeUTF8NFKD('â') AS nfkd, length(nfkd) AS nfkd_len;
```

Result:

``` text
┌─length('â')─┬─nfkd─┬─nfkd_len─┐
│           2 │ â    │        3 │
└─────────────┴──────┴──────────┘
```

## encodeXMLComponent {#encode-xml-component}

Escapes characters to place string into XML text node or attribute.

The following five XML predefined entities will be replaced: `<`, `&`, `>`, `"`, `'`.

**Syntax**

``` sql
encodeXMLComponent(x)
```

**Arguments**

-   `x` — The sequence of characters. [String](../../sql-reference/data-types/string.md).

**Returned value**

-   The sequence of characters with escape characters.

Type: [String](../../sql-reference/data-types/string.md).

**Example**

Query:

``` sql
SELECT encodeXMLComponent('Hello, "world"!');
SELECT encodeXMLComponent('<123>');
SELECT encodeXMLComponent('&clickhouse');
SELECT encodeXMLComponent('\'foo\'');
```

Result:

``` text
Hello, &quot;world&quot;!
&lt;123&gt;
&amp;clickhouse
&apos;foo&apos;
```

## decodeXMLComponent {#decode-xml-component}

Replaces XML predefined entities with characters. Predefined entities are `&quot;` `&amp;` `&apos;` `&gt;` `&lt;`
This function also replaces numeric character references with Unicode characters. Both decimal (like `&#10003;`) and hexadecimal (`&#x2713;`) forms are supported.

**Syntax**

``` sql
decodeXMLComponent(x)
```

**Arguments**

-   `x` — A sequence of characters. [String](../../sql-reference/data-types/string.md).

**Returned value**

-   The sequence of characters after replacement.

Type: [String](../../sql-reference/data-types/string.md).

**Example**

Query:

``` sql
SELECT decodeXMLComponent('&apos;foo&apos;');
SELECT decodeXMLComponent('&lt; &#x3A3; &gt;');
```

Result:

``` text
'foo'
< Σ >
```

**See Also**

-   [List of XML and HTML character entity references](https://en.wikipedia.org/wiki/List_of_XML_and_HTML_character_entity_references)



## extractTextFromHTML {#extracttextfromhtml}

A function to extract text from HTML or XHTML.
It does not necessarily 100% conform to any of the HTML, XML or XHTML standards, but the implementation is reasonably accurate and it is fast. The rules are the following:

1. Comments are skipped. Example: `<!-- test -->`. Comment must end with `-->`. Nested comments are not possible.
Note: constructions like `<!-->` and `<!--->` are not valid comments in HTML but they are skipped by other rules.
2. CDATA is pasted verbatim. Note: CDATA is XML/XHTML specific. But it is processed for "best-effort" approach.
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

-   `x` — input text. [String](../../sql-reference/data-types/string.md).

**Returned value**

-   Extracted text.

Type: [String](../../sql-reference/data-types/string.md).

**Example**

The first example contains several tags and a comment and also shows whitespace processing.
The second example shows `CDATA` and `script` tag processing.
In the third example text is extracted from the full HTML response received by the [url](../../sql-reference/table-functions/url.md) function.

Query:

``` sql
SELECT extractTextFromHTML(' <p> A text <i>with</i><b>tags</b>. <!-- comments --> </p> ');
SELECT extractTextFromHTML('<![CDATA[The content within <b>CDATA</b>]]> <script>alert("Script");</script>');
SELECT extractTextFromHTML(html) FROM url('http://www.donothingfor2minutes.com/', RawBLOB, 'html String');
```

Result:

``` text
A text with tags .
The content within <b>CDATA</b>
Do Nothing for 2 Minutes 2:00 &nbsp;
```
