---
toc_priority: 42
toc_title: For Replacing in Strings
---

# Functions for Searching and Replacing in Strings {#functions-for-searching-and-replacing-in-strings}

!!! note "Note"
    Functions for [searching](../../sql-reference/functions/string-search-functions.md) and [other manipulations with strings](../../sql-reference/functions/string-functions.md) are described separately.

## replaceOne(haystack, pattern, replacement) {#replaceonehaystack-pattern-replacement}

Replaces the first occurrence, if it exists, of the ‘pattern’ substring in ‘haystack’ with the ‘replacement’ substring.
Hereafter, ‘pattern’ and ‘replacement’ must be constants.

## replaceAll(haystack, pattern, replacement), replace(haystack, pattern, replacement) {#replaceallhaystack-pattern-replacement-replacehaystack-pattern-replacement}

Replaces all occurrences of the ‘pattern’ substring in ‘haystack’ with the ‘replacement’ substring.

## replaceRegexpOne(haystack, pattern, replacement) {#replaceregexponehaystack-pattern-replacement}

Replacement using the ‘pattern’ regular expression. A re2 regular expression.
Replaces only the first occurrence, if it exists.
A pattern can be specified as ‘replacement’. This pattern can include substitutions `\0-\9`.
The substitution `\0` includes the entire regular expression. Substitutions `\1-\9` correspond to the subpattern numbers.To use the `\` character in a template, escape it using `\`.
Also keep in mind that a string literal requires an extra escape.

Example 1. Converting the date to American format:

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

## replaceRegexpAll(haystack, pattern, replacement) {#replaceregexpallhaystack-pattern-replacement}

This does the same thing, but replaces all the occurrences. Example:

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

## regexpQuoteMeta(s) {#regexpquotemetas}

The function adds a backslash before some predefined characters in the string.
Predefined characters: `\0`, `\\`, `|`, `(`, `)`, `^`, `$`, `.`, `[`, `]`, `?`, `*`, `+`, `{`, `:`, `-`.
This implementation slightly differs from re2::RE2::QuoteMeta. It escapes zero byte as `\0` instead of `\x00` and it escapes only required characters.
For more information, see the link: [RE2](https://github.com/google/re2/blob/master/re2/re2.cc#L473)

