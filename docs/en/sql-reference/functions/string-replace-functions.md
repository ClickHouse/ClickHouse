---
slug: /en/sql-reference/functions/string-replace-functions
sidebar_position: 42
sidebar_label: Replacing in Strings
---

# Functions for Searching and Replacing in Strings

:::note
Functions for [searching](../../sql-reference/functions/string-search-functions.md) and [other manipulations with strings](../../sql-reference/functions/string-functions.md) are described separately.
:::

## replaceOne(haystack, pattern, replacement)

Replaces the first occurrence of the substring ‘pattern’ (if it exists) in ‘haystack’ by the ‘replacement’ string.
‘pattern’ and ‘replacement’ must be constants.

## replaceAll(haystack, pattern, replacement), replace(haystack, pattern, replacement)

Replaces all occurrences of the substring ‘pattern’ in ‘haystack’ by the ‘replacement’ string.

## replaceRegexpOne(haystack, pattern, replacement)

Replaces the first occurrence of the substring matching the regular expression ‘pattern’ in ‘haystack‘ by the ‘replacement‘ string.
‘pattern‘ must be a constant [re2 regular expression](https://github.com/google/re2/wiki/Syntax).
‘replacement’ must be a plain constant string or a constant string containing substitutions `\0-\9`.
Substitutions `\1-\9` correspond to the 1st to 9th capturing group (submatch), substitution `\0` corresponds to the entire match.
To use a verbatim `\` character in the ‘pattern‘ or ‘replacement‘ string, escape it using `\`.
Also keep in mind that string literals require an extra escaping.

Example 1. Converting ISO dates to American format:

``` sql
SELECT DISTINCT
    EventDate,
    replaceRegexpOne(toString(EventDate), '(\\d{4})-(\\d{2})-(\\d{2})', '\\2/\\3/\\1') AS res
FROM test.hits
LIMIT 7
FORMAT TabSeparated
```

``` text
2014-03-17      03/17/2014
2014-03-18      03/18/2014
2014-03-19      03/19/2014
2014-03-20      03/20/2014
2014-03-21      03/21/2014
2014-03-22      03/22/2014
2014-03-23      03/23/2014
```

Example 2. Copying a string ten times:

``` sql
SELECT replaceRegexpOne('Hello, World!', '.*', '\\0\\0\\0\\0\\0\\0\\0\\0\\0\\0') AS res
```

``` text
┌─res────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ Hello, World!Hello, World!Hello, World!Hello, World!Hello, World!Hello, World!Hello, World!Hello, World!Hello, World!Hello, World! │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## replaceRegexpAll(haystack, pattern, replacement)

Like ‘replaceRegexpOne‘, but replaces all occurrences of the pattern. Example:

``` sql
SELECT replaceRegexpAll('Hello, World!', '.', '\\0\\0') AS res
```

``` text
┌─res────────────────────────┐
│ HHeelllloo,,  WWoorrlldd!! │
└────────────────────────────┘
```

As an exception, if a regular expression worked on an empty substring, the replacement is not made more than once.
Example:

``` sql
SELECT replaceRegexpAll('Hello, World!', '^', 'here: ') AS res
```

``` text
┌─res─────────────────┐
│ here: Hello, World! │
└─────────────────────┘
```

## regexpQuoteMeta(s)

The function adds a backslash before some predefined characters in the string.
Predefined characters: `\0`, `\\`, `|`, `(`, `)`, `^`, `$`, `.`, `[`, `]`, `?`, `*`, `+`, `{`, `:`, `-`.
This implementation slightly differs from re2::RE2::QuoteMeta. It escapes zero byte as `\0` instead of `\x00` and it escapes only required characters.
For more information, see the link: [RE2](https://github.com/google/re2/blob/master/re2/re2.cc#L473)


## translate(s, from, to)

The function replaces characters in the string ‘s’ in accordance with one-to-one character mapping defined by ‘from’ and ‘to’ strings. ‘from’ and ‘to’ must be constant ASCII strings of the same size. Non-ASCII characters in the original string are not modified.

Example:

``` sql
SELECT translate('Hello, World!', 'delor', 'DELOR') AS res
```

``` text
┌─res───────────┐
│ HELLO, WORLD! │
└───────────────┘
```

## translateUTF8(string, from, to)

Similar to previous function, but works with UTF-8 arguments. ‘from’ and ‘to’ must be valid constant UTF-8 strings of the same size.

Example:

``` sql
SELECT translateUTF8('Hélló, Wórld¡', 'óé¡', 'oe!') AS res
```

``` text
┌─res───────────┐
│ Hello, World! │
└───────────────┘
```
