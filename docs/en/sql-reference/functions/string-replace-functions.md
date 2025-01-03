---
slug: /en/sql-reference/functions/string-replace-functions
sidebar_position: 150
sidebar_label: Replacing in Strings
---

# Functions for Replacing in Strings

[General strings functions](string-functions.md) and [functions for searching in strings](string-search-functions.md) are described separately.

## overlay

Replace part of the string `input` with another string `replace`, starting at the 1-based index `offset`.

**Syntax**

```sql
overlay(s, replace, offset[, length])
```

**Parameters**

- `s`: A string type [String](../data-types/string.md).
- `replace`: A string type [String](../data-types/string.md).
- `offset`: An integer type [Int](../data-types/int-uint.md) (1-based). If `offset` is negative, it is counted from the end of the string `s`.
- `length`: Optional. An integer type [Int](../data-types/int-uint.md). `length` specifies the length of the snippet within the input string `s` to be replaced. If `length` is not specified, the number of bytes removed from `s` equals the length of `replace`; otherwise `length` bytes are removed.

**Returned value**

- A [String](../data-types/string.md) data type value.

**Example**

```sql
SELECT overlay('My father is from Mexico.', 'mother', 4) AS res;
```

Result:

```text
┌─res──────────────────────┐
│ My mother is from Mexico.│
└──────────────────────────┘
```

```sql
SELECT overlay('My father is from Mexico.', 'dad', 4, 6) AS res;
```

Result:

```text
┌─res───────────────────┐
│ My dad is from Mexico.│
└───────────────────────┘
```

## overlayUTF8

Replace part of the string `input` with another string `replace`, starting at the 1-based index `offset`.

Assumes that the string contains valid UTF-8 encoded text.
If this assumption is violated, no exception is thrown and the result is undefined.

**Syntax**

```sql
overlayUTF8(s, replace, offset[, length])
```

**Parameters**

- `s`: A string type [String](../data-types/string.md).
- `replace`: A string type [String](../data-types/string.md).
- `offset`: An integer type [Int](../data-types/int-uint.md) (1-based). If `offset` is negative, it is counted from the end of the input string `s`.
- `length`: Optional. An integer type [Int](../data-types/int-uint.md). `length` specifies the length of the snippet within the input string `s` to be replaced. If `length` is not specified, the number of characters removed from `s` equals the length of `replace`; otherwise `length` characters are removed.

**Returned value**

- A [String](../data-types/string.md) data type value.

**Example**

```sql
SELECT overlay('Mein Vater ist aus Österreich.', 'der Türkei', 20) AS res;
```

Result:

```text
┌─res───────────────────────────┐
│ Mein Vater ist aus der Türkei.│
└───────────────────────────────┘
```

## replaceOne

Replaces the first occurrence of the substring `pattern` in `haystack` by the `replacement` string.

**Syntax**

```sql
replaceOne(haystack, pattern, replacement)
```

## replaceAll

Replaces all occurrences of the substring `pattern` in `haystack` by the `replacement` string.

**Syntax**

```sql
replaceAll(haystack, pattern, replacement)
```

Alias: `replace`.

## replaceRegexpOne

Replaces the first occurrence of the substring matching the regular expression `pattern` (in [re2 syntax](https://github.com/google/re2/wiki/Syntax)) in `haystack` by the `replacement` string.

`replacement` can contain substitutions `\0-\9`.
Substitutions `\1-\9` correspond to the 1st to 9th capturing group (submatch), substitution `\0` corresponds to the entire match.

To use a verbatim `\` character in the `pattern` or `replacement` strings, escape it using `\`.
Also keep in mind that string literals require extra escaping.

**Syntax**

```sql
replaceRegexpOne(haystack, pattern, replacement)
```

**Example**

Converting ISO dates to American format:

``` sql
SELECT DISTINCT
    EventDate,
    replaceRegexpOne(toString(EventDate), '(\\d{4})-(\\d{2})-(\\d{2})', '\\2/\\3/\\1') AS res
FROM test.hits
LIMIT 7
FORMAT TabSeparated
```

Result:

``` text
2014-03-17      03/17/2014
2014-03-18      03/18/2014
2014-03-19      03/19/2014
2014-03-20      03/20/2014
2014-03-21      03/21/2014
2014-03-22      03/22/2014
2014-03-23      03/23/2014
```

Copying a string ten times:

``` sql
SELECT replaceRegexpOne('Hello, World!', '.*', '\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0') AS res
```

Result:

``` text
┌─res────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Hello, World!Hello, World!Hello, World!Hello, World!Hello, World!Hello, World!Hello, World!Hello, World!Hello, World!Hello, World! │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## replaceRegexpAll

Like `replaceRegexpOne` but replaces all occurrences of the pattern.

Alias: `REGEXP_REPLACE`.

**Example**

``` sql
SELECT replaceRegexpAll('Hello, World!', '.', '\\0\\0') AS res
```

Result:

``` text
┌─res────────────────────────┐
│ HHeelllloo,,  WWoorrlldd!! │
└────────────────────────────┘
```

As an exception, if a regular expression worked on an empty substring, the replacement is not made more than once, e.g.:

``` sql
SELECT replaceRegexpAll('Hello, World!', '^', 'here: ') AS res
```

Result:

``` text
┌─res─────────────────┐
│ here: Hello, World! │
└─────────────────────┘
```

## regexpQuoteMeta

Adds a backslash before these characters with special meaning in regular expressions: `\0`, `\\`, `|`, `(`, `)`, `^`, `$`, `.`, `[`, `]`, `?`, `*`, `+`, `{`, `:`, `-`.

This implementation slightly differs from re2::RE2::QuoteMeta. It escapes zero byte as `\0` instead of `\x00` and it escapes only required characters.
For more information, see [RE2](https://github.com/google/re2/blob/master/re2/re2.cc#L473)

**Syntax**

```sql
regexpQuoteMeta(s)
```

## format

Format the `pattern` string with the values (strings, integers, etc.) listed in the arguments, similar to formatting in Python. The pattern string can contain replacement fields surrounded by curly braces `{}`. Anything not contained in braces is considered literal text and copied verbatim into the output. Literal brace character can be escaped by two braces: `{{ '{{' }}` and `{{ '}}' }}`. Field names can be numbers (starting from zero) or empty (then they are implicitly given monotonically increasing numbers).

**Syntax**

```sql
format(pattern, s0, s1, ...)
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

## translate

Replaces characters in the string `s` using a one-to-one character mapping defined by `from` and `to` strings. `from` and `to` must be constant ASCII strings of the same size. Non-ASCII characters in the original string are not modified.

**Syntax**

```sql
translate(s, from, to)
```

**Example**

``` sql
SELECT translate('Hello, World!', 'delor', 'DELOR') AS res
```

Result:

``` text
┌─res───────────┐
│ HELLO, WORLD! │
└───────────────┘
```

## translateUTF8

Like [translate](#translate) but assumes `s`, `from` and `to` are UTF-8 encoded strings.

**Syntax**

``` sql
translateUTF8(s, from, to)
```

**Parameters**

- `s`: A string type [String](../data-types/string.md).
- `from`: A string type [String](../data-types/string.md).
- `to`: A string type [String](../data-types/string.md).

**Returned value**

- A [String](../data-types/string.md) data type value.

**Examples**

Query:

``` sql
SELECT translateUTF8('Münchener Straße', 'üß', 'us') AS res;
```

``` response
┌─res──────────────┐
│ Munchener Strase │
└──────────────────┘
```

## printf

The `printf` function formats the given string with the values (strings, integers, floating-points etc.) listed in the arguments, similar to printf function in C++. The format string can contain format specifiers starting with `%` character. Anything not contained in `%` and the following format specifier is considered literal text and copied verbatim into the output. Literal `%` character can be escaped by `%%`.

**Syntax**

``` sql
printf(format, arg1, arg2, ...)
```

**Example**

Query:

``` sql
select printf('%%%s %s %d', 'Hello', 'World', 2024);
```


``` response
┌─printf('%%%s %s %d', 'Hello', 'World', 2024)─┐
│ %Hello World 2024                            │
└──────────────────────────────────────────────┘
```
