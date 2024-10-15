---
slug: /en/sql-reference/functions/string-search-functions
sidebar_position: 160
sidebar_label: Searching in Strings
---

# Functions for Searching in Strings

All functions in this section search case-sensitively by default. Case-insensitive search is usually provided by separate function variants.

:::note
Case-insensitive search follows the lowercase-uppercase rules of the English language. E.g. Uppercased `i` in the English language is
`I` whereas in the Turkish language it is `İ` - results for languages other than English may be unexpected.
:::

Functions in this section also assume that the searched string (referred to in this section as `haystack`) and the search string (referred to in this section as `needle`) are single-byte encoded text. If this assumption is
violated, no exception is thrown and results are undefined. Search with UTF-8 encoded strings is usually provided by separate function
variants. Likewise, if a UTF-8 function variant is used and the input strings are not UTF-8 encoded text, no exception is thrown and the
results are undefined. Note that no automatic Unicode normalization is performed, however you can use the
[normalizeUTF8*()](https://clickhouse.com../functions/string-functions/) functions for that.

[General strings functions](string-functions.md) and [functions for replacing in strings](string-replace-functions.md) are described separately.

## position

Returns the position (in bytes, starting at 1) of a substring `needle` in a string `haystack`.

**Syntax**

``` sql
position(haystack, needle[, start_pos])
```

Alias:
- `position(needle IN haystack)`

**Arguments**

- `haystack` — String in which the search is performed. [String](../../sql-reference/syntax.md#syntax-string-literal).
- `needle` — Substring to be searched. [String](../../sql-reference/syntax.md#syntax-string-literal).
- `start_pos` – Position (1-based) in `haystack` at which the search starts. [UInt](../data-types/int-uint.md). Optional.

**Returned value**

- Starting position in bytes and counting from 1, if the substring was found. [UInt64](../data-types/int-uint.md).
- 0, if the substring was not found. [UInt64](../data-types/int-uint.md).

If substring `needle` is empty, these rules apply:
- if no `start_pos` was specified: return `1`
- if `start_pos = 0`: return `1`
- if `start_pos >= 1` and `start_pos <= length(haystack) + 1`: return `start_pos`
- otherwise: return `0`

The same rules also apply to functions `locate`, `positionCaseInsensitive`, `positionUTF8` and `positionCaseInsensitiveUTF8`.

**Examples**

Query:

``` sql
SELECT position('Hello, world!', '!');
```

Result:

``` text
┌─position('Hello, world!', '!')─┐
│                             13 │
└────────────────────────────────┘
```

Example with `start_pos` argument:

Query:

``` sql
SELECT
    position('Hello, world!', 'o', 1),
    position('Hello, world!', 'o', 7)
```

Result:

``` text
┌─position('Hello, world!', 'o', 1)─┬─position('Hello, world!', 'o', 7)─┐
│                                 5 │                                 9 │
└───────────────────────────────────┴───────────────────────────────────┘
```

Example for `needle IN haystack` syntax:

Query:

```sql
SELECT 6 = position('/' IN s) FROM (SELECT 'Hello/World' AS s);
```

Result:

```text
┌─equals(6, position(s, '/'))─┐
│                           1 │
└─────────────────────────────┘
```

Examples with empty `needle` substring:

Query:

``` sql
SELECT
    position('abc', ''),
    position('abc', '', 0),
    position('abc', '', 1),
    position('abc', '', 2),
    position('abc', '', 3),
    position('abc', '', 4),
    position('abc', '', 5)
```

Result:

``` text
┌─position('abc', '')─┬─position('abc', '', 0)─┬─position('abc', '', 1)─┬─position('abc', '', 2)─┬─position('abc', '', 3)─┬─position('abc', '', 4)─┬─position('abc', '', 5)─┐
│                   1 │                      1 │                      1 │                      2 │                      3 │                      4 │                      0 │
└─────────────────────┴────────────────────────┴────────────────────────┴────────────────────────┴────────────────────────┴────────────────────────┴────────────────────────┘
```

## locate

Like [position](#position) but with arguments `haystack` and `locate` switched.

The behavior of this function depends on the ClickHouse version:
- in versions < v24.3, `locate` was an alias of function `position` and accepted arguments `(haystack, needle[, start_pos])`.
- in versions >= 24.3,, `locate` is an individual function (for better compatibility with MySQL) and accepts arguments `(needle, haystack[, start_pos])`. The previous behavior
  can be restored using setting [function_locate_has_mysql_compatible_argument_order = false](../../operations/settings/settings.md#function-locate-has-mysql-compatible-argument-order);

**Syntax**

``` sql
locate(needle, haystack[, start_pos])
```

## positionCaseInsensitive

A case insensitive invariant of [position](#position).

**Example**

Query:

``` sql
SELECT positionCaseInsensitive('Hello, world!', 'hello');
```

Result:

``` text
┌─positionCaseInsensitive('Hello, world!', 'hello')─┐
│                                                 1 │
└───────────────────────────────────────────────────┘
```

## positionUTF8

Like [position](#position) but assumes `haystack` and `needle` are UTF-8 encoded strings.

**Examples**

Function `positionUTF8` correctly counts character `ö` (represented by two points) as a single Unicode codepoint:

Query:

``` sql
SELECT positionUTF8('Motörhead', 'r');
```

Result:

``` text
┌─position('Motörhead', 'r')─┐
│                          5 │
└────────────────────────────┘
```

## positionCaseInsensitiveUTF8

Like [positionUTF8](#positionutf8) but searches case-insensitively.

## multiSearchAllPositions

Like [position](#position) but returns an array of positions (in bytes, starting at 1) for multiple `needle` substrings in a `haystack` string.

:::note
All `multiSearch*()` functions only support up to 2<sup>8</sup> needles.
:::

**Syntax**

``` sql
multiSearchAllPositions(haystack, [needle1, needle2, ..., needleN])
```

**Arguments**

- `haystack` — String in which the search is performed. [String](../../sql-reference/syntax.md#syntax-string-literal).
- `needle` — Substrings to be searched. [Array](../data-types/array.md).

**Returned value**

- Array of the starting position in bytes and counting from 1, if the substring was found.
- 0, if the substring was not found.

**Example**

Query:

``` sql
SELECT multiSearchAllPositions('Hello, World!', ['hello', '!', 'world']);
```

Result:

``` text
┌─multiSearchAllPositions('Hello, World!', ['hello', '!', 'world'])─┐
│ [0,13,0]                                                          │
└───────────────────────────────────────────────────────────────────┘
```
## multiSearchAllPositionsCaseInsensitive

Like [multiSearchAllPositions](#multisearchallpositions) but ignores case.

**Syntax**

```sql
multiSearchAllPositionsCaseInsensitive(haystack, [needle1, needle2, ..., needleN])
```

**Parameters**

- `haystack` — String in which the search is performed. [String](../../sql-reference/syntax.md#syntax-string-literal).
- `needle` — Substrings to be searched. [Array](../data-types/array.md).

**Returned value**

- Array of the starting position in bytes and counting from 1 (if the substring was found).
- 0 if the substring was not found.

**Example**

Query:

```sql
SELECT multiSearchAllPositionsCaseInsensitive('ClickHouse',['c','h']);
```

Result:

```response
["1","6"]
```

## multiSearchAllPositionsUTF8

Like [multiSearchAllPositions](#multisearchallpositions) but assumes `haystack` and the `needle` substrings are UTF-8 encoded strings.

**Syntax**

```sql
multiSearchAllPositionsUTF8(haystack, [needle1, needle2, ..., needleN])
```

**Parameters**

- `haystack` — UTF-8 encoded string in which the search is performed. [String](../../sql-reference/syntax.md#syntax-string-literal).
- `needle` — UTF-8 encoded substrings to be searched. [Array](../data-types/array.md).

**Returned value**

- Array of the starting position in bytes and counting from 1 (if the substring was found).
- 0 if the substring was not found.

**Example**

Given `ClickHouse` as a UTF-8 string, find the positions of `C` (`\x43`) and `H` (`\x48`).

Query:

```sql
SELECT multiSearchAllPositionsUTF8('\x43\x6c\x69\x63\x6b\x48\x6f\x75\x73\x65',['\x43','\x48']);
```

Result:

```response
["1","6"]
```

## multiSearchAllPositionsCaseInsensitiveUTF8

Like [multiSearchAllPositionsUTF8](#multisearchallpositionsutf8) but ignores case.

**Syntax**

```sql
multiSearchAllPositionsCaseInsensitiveUTF8(haystack, [needle1, needle2, ..., needleN])
```

**Parameters**

- `haystack` — UTF-8 encoded string in which the search is performed. [String](../../sql-reference/syntax.md#syntax-string-literal).
- `needle` — UTF-8 encoded substrings to be searched. [Array](../data-types/array.md).

**Returned value**

- Array of the starting position in bytes and counting from 1 (if the substring was found).
- 0 if the substring was not found.

**Example**

Given `ClickHouse` as a UTF-8 string, find the positions of `c` (`\x63`) and `h` (`\x68`).

Query:

```sql
SELECT multiSearchAllPositionsCaseInsensitiveUTF8('\x43\x6c\x69\x63\x6b\x48\x6f\x75\x73\x65',['\x63','\x68']);
```

Result:

```response
["1","6"]
```

## multiSearchFirstPosition

Like [`position`](#position) but returns the leftmost offset in a `haystack` string which matches any of multiple `needle` strings.

Functions [`multiSearchFirstPositionCaseInsensitive`](#multisearchfirstpositioncaseinsensitive), [`multiSearchFirstPositionUTF8`](#multisearchfirstpositionutf8) and [`multiSearchFirstPositionCaseInsensitiveUTF8`](#multisearchfirstpositioncaseinsensitiveutf8) provide case-insensitive and/or UTF-8 variants of this function.

**Syntax**

```sql
multiSearchFirstPosition(haystack, [needle1, needle2, ..., needleN])
```

**Parameters**

- `haystack` — String in which the search is performed. [String](../../sql-reference/syntax.md#syntax-string-literal).
- `needle` —  Substrings to be searched. [Array](../data-types/array.md).

**Returned value**

- Leftmost offset in a `haystack` string which matches any of multiple `needle` strings.
- 0, if there was no match.

**Example**

Query:

```sql
SELECT multiSearchFirstPosition('Hello World',['llo', 'Wor', 'ld']);
```

Result:

```response
3
```

## multiSearchFirstPositionCaseInsensitive

Like [`multiSearchFirstPosition`](#multisearchfirstposition) but ignores case.

**Syntax**

```sql
multiSearchFirstPositionCaseInsensitive(haystack, [needle1, needle2, ..., needleN])
```

**Parameters**

- `haystack` — String in which the search is performed. [String](../../sql-reference/syntax.md#syntax-string-literal).
- `needle` — Array of substrings to be searched. [Array](../data-types/array.md).

**Returned value**

- Leftmost offset in a `haystack` string which matches any of multiple `needle` strings.
- 0, if there was no match.

**Example**

Query:

```sql
SELECT multiSearchFirstPositionCaseInsensitive('HELLO WORLD',['wor', 'ld', 'ello']);
```

Result:

```response
2
```

## multiSearchFirstPositionUTF8

Like [`multiSearchFirstPosition`](#multisearchfirstposition) but assumes `haystack` and `needle` to be UTF-8 strings.

**Syntax**

```sql
multiSearchFirstPositionUTF8(haystack, [needle1, needle2, ..., needleN])
```

**Parameters**

- `haystack` — UTF-8 string in which the search is performed. [String](../../sql-reference/syntax.md#syntax-string-literal).
- `needle` — Array of UTF-8 substrings to be searched. [Array](../data-types/array.md).

**Returned value**

- Leftmost offset in a `haystack` string which matches any of multiple `needle` strings.
- 0, if there was no match.

**Example**

Find the leftmost offset in UTF-8 string `hello world` which matches any of the given needles.

Query:

```sql
SELECT multiSearchFirstPositionUTF8('\x68\x65\x6c\x6c\x6f\x20\x77\x6f\x72\x6c\x64',['wor', 'ld', 'ello']);
```

Result:

```response
2
```

## multiSearchFirstPositionCaseInsensitiveUTF8

Like [`multiSearchFirstPosition`](#multisearchfirstposition) but assumes `haystack` and `needle` to be UTF-8 strings and ignores case.

**Syntax**

```sql
multiSearchFirstPositionCaseInsensitiveUTF8(haystack, [needle1, needle2, ..., needleN])
```

**Parameters**

- `haystack` — UTF-8 string in which the search is performed. [String](../../sql-reference/syntax.md#syntax-string-literal).
- `needle` — Array of UTF-8 substrings to be searched. [Array](../data-types/array.md)

**Returned value**

- Leftmost offset in a `haystack` string which matches any of multiple `needle` strings, ignoring case.
- 0, if there was no match.

**Example**

Find the leftmost offset in UTF-8 string `HELLO WORLD` which matches any of the given needles.

Query:

```sql
SELECT multiSearchFirstPositionCaseInsensitiveUTF8('\x48\x45\x4c\x4c\x4f\x20\x57\x4f\x52\x4c\x44',['wor', 'ld', 'ello']);
```

Result:

```response
2
```

## multiSearchFirstIndex

Returns the index `i` (starting from 1) of the leftmost found needle<sub>i</sub> in the string `haystack` and 0 otherwise.

Functions [`multiSearchFirstIndexCaseInsensitive`](#multisearchfirstindexcaseinsensitive), [`multiSearchFirstIndexUTF8`](#multisearchfirstindexutf8) and [`multiSearchFirstIndexCaseInsensitiveUTF8`](#multisearchfirstindexcaseinsensitiveutf8) provide case-insensitive and/or UTF-8 variants of this function.

**Syntax**

```sql
multiSearchFirstIndex(haystack, [needle1, needle2, ..., needleN])
```
**Parameters**

- `haystack` — String in which the search is performed. [String](../../sql-reference/syntax.md#syntax-string-literal).
- `needle` — Substrings to be searched. [Array](../data-types/array.md).

**Returned value**

- index (starting from 1) of the leftmost found needle. Otherwise 0, if there was no match. [UInt8](../data-types/int-uint.md).

**Example**

Query:

```sql
SELECT multiSearchFirstIndex('Hello World',['World','Hello']);
```

Result:

```response
1
```

## multiSearchFirstIndexCaseInsensitive

Returns the index `i` (starting from 1) of the leftmost found needle<sub>i</sub> in the string `haystack` and 0 otherwise. Ignores case.

**Syntax**

```sql
multiSearchFirstIndexCaseInsensitive(haystack, [needle1, needle2, ..., needleN])
```

**Parameters**

- `haystack` — String in which the search is performed. [String](../../sql-reference/syntax.md#syntax-string-literal).
- `needle` — Substrings to be searched. [Array](../data-types/array.md).

**Returned value**

- index (starting from 1) of the leftmost found needle. Otherwise 0, if there was no match. [UInt8](../data-types/int-uint.md).

**Example**

Query:

```sql
SELECT multiSearchFirstIndexCaseInsensitive('hElLo WoRlD',['World','Hello']);
```

Result:

```response
1
```

## multiSearchFirstIndexUTF8

Returns the index `i` (starting from 1) of the leftmost found needle<sub>i</sub> in the string `haystack` and 0 otherwise. Assumes `haystack` and `needle` are UTF-8 encoded strings.

**Syntax**

```sql
multiSearchFirstIndexUTF8(haystack, [needle1, needle2, ..., needleN])
```

**Parameters**

- `haystack` — UTF-8 string in which the search is performed. [String](../../sql-reference/syntax.md#syntax-string-literal).
- `needle` — Array of UTF-8 substrings to be searched. [Array](../data-types/array.md)

**Returned value**

- index (starting from 1) of the leftmost found needle, Otherwise 0, if there was no match. [UInt8](../data-types/int-uint.md).

**Example**

Given `Hello World` as a UTF-8 string, find the first index of UTF-8 strings `Hello` and `World`.

Query:

```sql
SELECT multiSearchFirstIndexUTF8('\x48\x65\x6c\x6c\x6f\x20\x57\x6f\x72\x6c\x64',['\x57\x6f\x72\x6c\x64','\x48\x65\x6c\x6c\x6f']);
```

Result:

```response
1
```

## multiSearchFirstIndexCaseInsensitiveUTF8

Returns the index `i` (starting from 1) of the leftmost found needle<sub>i</sub> in the string `haystack` and 0 otherwise. Assumes `haystack` and `needle` are UTF-8 encoded strings. Ignores case.

**Syntax**

```sql
multiSearchFirstIndexCaseInsensitiveUTF8(haystack, [needle1, needle2, ..., needleN])
```

**Parameters**

- `haystack` — UTF-8 string in which the search is performed. [String](../../sql-reference/syntax.md#syntax-string-literal).
- `needle` — Array of UTF-8 substrings to be searched. [Array](../data-types/array.md).

**Returned value**

- index (starting from 1) of the leftmost found needle. Otherwise 0, if there was no match. [UInt8](../data-types/int-uint.md).

**Example**

Given `HELLO WORLD` as a UTF-8 string, find the first index of UTF-8 strings `hello` and `world`.

Query:

```sql
SELECT multiSearchFirstIndexCaseInsensitiveUTF8('\x48\x45\x4c\x4c\x4f\x20\x57\x4f\x52\x4c\x44',['\x68\x65\x6c\x6c\x6f','\x77\x6f\x72\x6c\x64']);
```

Result:

```response
1
```

## multiSearchAny

Returns 1, if at least one string needle<sub>i</sub> matches the string `haystack` and 0 otherwise.

Functions [`multiSearchAnyCaseInsensitive`](#multisearchanycaseinsensitive), [`multiSearchAnyUTF8`](#multisearchanyutf8) and [`multiSearchAnyCaseInsensitiveUTF8`](#multisearchanycaseinsensitiveutf8) provide case-insensitive and/or UTF-8 variants of this function.

**Syntax**

```sql
multiSearchAny(haystack, [needle1, needle2, ..., needleN])
```

**Parameters**

- `haystack` — String in which the search is performed. [String](../../sql-reference/syntax.md#syntax-string-literal).
- `needle` — Substrings to be searched. [Array](../data-types/array.md).

**Returned value**

- 1, if there was at least one match.
- 0, if there was not at least one match.

**Example**

Query:

```sql
SELECT multiSearchAny('ClickHouse',['C','H']);
```

Result:

```response
1
```

## multiSearchAnyCaseInsensitive

Like [multiSearchAny](#multisearchany) but ignores case.

**Syntax**

```sql
multiSearchAnyCaseInsensitive(haystack, [needle1, needle2, ..., needleN])
```

**Parameters**

- `haystack` — String in which the search is performed. [String](../../sql-reference/syntax.md#syntax-string-literal).
- `needle` — Substrings to be searched. [Array](../data-types/array.md)

**Returned value**

- 1, if there was at least one case-insensitive match.
- 0, if there was not at least one case-insensitive match.

**Example**

Query:

```sql
SELECT multiSearchAnyCaseInsensitive('ClickHouse',['c','h']);
```

Result:

```response
1
```

## multiSearchAnyUTF8

Like [multiSearchAny](#multisearchany) but assumes `haystack` and the `needle` substrings are UTF-8 encoded strings.

*Syntax**

```sql
multiSearchAnyUTF8(haystack, [needle1, needle2, ..., needleN])
```

**Parameters**

- `haystack` — UTF-8 string in which the search is performed. [String](../../sql-reference/syntax.md#syntax-string-literal).
- `needle` — UTF-8 substrings to be searched. [Array](../data-types/array.md).

**Returned value**

- 1, if there was at least one match.
- 0, if there was not at least one match.

**Example**

Given `ClickHouse` as a UTF-8 string, check if there are any `C` ('\x43') or `H` ('\x48') letters in the word.

Query:

```sql
SELECT multiSearchAnyUTF8('\x43\x6c\x69\x63\x6b\x48\x6f\x75\x73\x65',['\x43','\x48']);
```

Result:

```response
1
```

## multiSearchAnyCaseInsensitiveUTF8

Like [multiSearchAnyUTF8](#multisearchanyutf8) but ignores case.

*Syntax**

```sql
multiSearchAnyCaseInsensitiveUTF8(haystack, [needle1, needle2, ..., needleN])
```

**Parameters**

- `haystack` — UTF-8 string in which the search is performed. [String](../../sql-reference/syntax.md#syntax-string-literal).
- `needle` — UTF-8 substrings to be searched. [Array](../data-types/array.md)

**Returned value**

- 1, if there was at least one case-insensitive match.
- 0, if there was not at least one case-insensitive match.

**Example**

Given `ClickHouse` as a UTF-8 string, check if there is any letter `h`(`\x68`) in the word, ignoring case.

Query:

```sql
SELECT multiSearchAnyCaseInsensitiveUTF8('\x43\x6c\x69\x63\x6b\x48\x6f\x75\x73\x65',['\x68']);
```

Result:

```response
1
```

## match {#match}

Returns whether string `haystack` matches the regular expression `pattern` in [re2 regular expression syntax](https://github.com/google/re2/wiki/Syntax).

Matching is based on UTF-8, e.g. `.` matches the Unicode code point `¥` which is represented in UTF-8 using two bytes. The regular
expression must not contain null bytes. If the haystack or the pattern are not valid UTF-8, then the behavior is undefined.

Unlike re2's default behavior, `.` matches line breaks. To disable this, prepend the pattern with `(?-s)`.

If you only want to search substrings in a string, you can use functions [like](#like) or [position](#position) instead - they work much faster than this function.

**Syntax**

```sql
match(haystack, pattern)
```

Alias: `haystack REGEXP pattern operator`

## multiMatchAny

Like `match` but returns 1 if at least one of the patterns match and 0 otherwise.

:::note
Functions in the `multi[Fuzzy]Match*()` family use the the (Vectorscan)[https://github.com/VectorCamp/vectorscan] library. As such, they are only enabled if ClickHouse is compiled with support for vectorscan.

To turn off all functions that use hyperscan, use setting `SET allow_hyperscan = 0;`.

Due to restrictions of vectorscan, the length of the `haystack` string must be less than 2<sup>32</sup> bytes.

Hyperscan is generally vulnerable to regular expression denial of service (ReDoS) attacks (e.g. see
(here)[https://www.usenix.org/conference/usenixsecurity22/presentation/turonova], (here)[https://doi.org/10.1007/s10664-021-10033-1] and
(here)[https://doi.org/10.1145/3236024.3236027]. Users are adviced to check the provided patterns carefully.
:::

If you only want to search multiple substrings in a string, you can use function [multiSearchAny](#multisearchany) instead - it works much faster than this function.

**Syntax**

```sql
multiMatchAny(haystack, \[pattern<sub>1</sub>, pattern<sub>2</sub>, ..., pattern<sub>n</sub>\])
```

## multiMatchAnyIndex

Like `multiMatchAny` but returns any index that matches the haystack.

**Syntax**

```sql
multiMatchAnyIndex(haystack, \[pattern<sub>1</sub>, pattern<sub>2</sub>, ..., pattern<sub>n</sub>\])
```

## multiMatchAllIndices

Like `multiMatchAny` but returns the array of all indices that match the haystack in any order.

**Syntax**

```sql
multiMatchAllIndices(haystack, \[pattern<sub>1</sub>, pattern<sub>2</sub>, ..., pattern<sub>n</sub>\])
```

## multiFuzzyMatchAny

Like `multiMatchAny` but returns 1 if any pattern matches the haystack within a constant [edit distance](https://en.wikipedia.org/wiki/Edit_distance). This function relies on the experimental feature of [hyperscan](https://intel.github.io/hyperscan/dev-reference/compilation.html#approximate-matching) library, and can be slow for some corner cases. The performance depends on the edit distance value and patterns used, but it's always more expensive compared to a non-fuzzy variants.

:::note
`multiFuzzyMatch*()` function family do not support UTF-8 regular expressions (it threats them as a sequence of bytes) due to restrictions of hyperscan.
:::

**Syntax**

```sql
multiFuzzyMatchAny(haystack, distance, \[pattern<sub>1</sub>, pattern<sub>2</sub>, ..., pattern<sub>n</sub>\])
```

## multiFuzzyMatchAnyIndex

Like `multiFuzzyMatchAny` but returns any index that matches the haystack within a constant edit distance.

**Syntax**

```sql
multiFuzzyMatchAnyIndex(haystack, distance, \[pattern<sub>1</sub>, pattern<sub>2</sub>, ..., pattern<sub>n</sub>\])
```

## multiFuzzyMatchAllIndices

Like `multiFuzzyMatchAny` but returns the array of all indices in any order that match the haystack within a constant edit distance.

**Syntax**

```sql
multiFuzzyMatchAllIndices(haystack, distance, \[pattern<sub>1</sub>, pattern<sub>2</sub>, ..., pattern<sub>n</sub>\])
```

## extract

Returns the first match of a regular expression in a string.
If `haystack` does not match the `pattern` regex, an empty string is returned. 

If the regular expression has capturing groups, the function matches the input string against the first capturing group.

**Syntax**

```sql
extract(haystack, pattern)
```

*Arguments**

- `haystack` — Input string. [String](../data-types/string.md).
- `pattern` — Regular expression with [re2 regular expression syntax](https://github.com/google/re2/wiki/Syntax).

**Returned value**

- The first match of the regular expression in the haystack string. [String](../data-types/string.md).

**Example**

Query:

```sql
SELECT extract('number: 1, number: 2, number: 3', '\\d+') AS result;
```

Result:

```response
┌─result─┐
│ 1      │
└────────┘
```

## extractAll

Returns an array of all matches of a regular expression in a string. If `haystack` does not match the `pattern` regex, an empty string is returned.

The behavior with respect to sub-patterns is the same as in function [`extract`](#extract).

**Syntax**

```sql
extractAll(haystack, pattern)
```

*Arguments**

- `haystack` — Input string. [String](../data-types/string.md).
- `pattern` — Regular expression with [re2 regular expression syntax](https://github.com/google/re2/wiki/Syntax).

**Returned value**

- Array of matches of the regular expression in the haystack string. [Array](../data-types/array.md)([String](../data-types/string.md)).

**Example**

Query:

```sql
SELECT extractAll('number: 1, number: 2, number: 3', '\\d+') AS result;
```

Result:

```response
┌─result────────┐
│ ['1','2','3'] │
└───────────────┘
```

## extractAllGroupsHorizontal

Matches all groups of the `haystack` string using the `pattern` regular expression. Returns an array of arrays, where the first array includes all fragments matching the first group, the second array - matching the second group, etc.

This function is slower than [extractAllGroupsVertical](#extractallgroupsvertical).

**Syntax**

``` sql
extractAllGroupsHorizontal(haystack, pattern)
```

**Arguments**

- `haystack` — Input string. [String](../data-types/string.md).
- `pattern` — Regular expression with [re2 regular expression syntax](https://github.com/google/re2/wiki/Syntax). Must contain groups, each group enclosed in parentheses. If `pattern` contains no groups, an exception is thrown. [String](../data-types/string.md).

**Returned value**

- Array of arrays of matches. [Array](../data-types/array.md).

:::note
If `haystack` does not match the `pattern` regex, an array of empty arrays is returned.
:::

**Example**

``` sql
SELECT extractAllGroupsHorizontal('abc=111, def=222, ghi=333', '("[^"]+"|\\w+)=("[^"]+"|\\w+)');
```

Result:

``` text
┌─extractAllGroupsHorizontal('abc=111, def=222, ghi=333', '("[^"]+"|\\w+)=("[^"]+"|\\w+)')─┐
│ [['abc','def','ghi'],['111','222','333']]                                                │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

## extractGroups

Match all groups of given input string with a given regular expression, returns an array of arrays of matches.

**Syntax**

``` sql
extractGroups(haystack, pattern)
```

**Arguments**

- `haystack` — Input string. [String](../data-types/string.md).
- `pattern` — Regular expression with [re2 regular expression syntax](https://github.com/google/re2/wiki/Syntax). Must contain groups, each group enclosed in parentheses. If `pattern` contains no groups, an exception is thrown. [String](../data-types/string.md).

**Returned value**

- Array of arrays of matches. [Array](../data-types/array.md).

**Example**

``` sql
SELECT extractGroups('hello abc=111 world', '("[^"]+"|\\w+)=("[^"]+"|\\w+)') AS result;
```

Result:

``` text
┌─result────────┐
│ ['abc','111'] │
└───────────────┘
```

## extractAllGroupsVertical

Matches all groups of the `haystack` string using the `pattern` regular expression. Returns an array of arrays, where each array includes matching fragments from every group. Fragments are grouped in order of appearance in the `haystack`.

**Syntax**

``` sql
extractAllGroupsVertical(haystack, pattern)
```

**Arguments**

- `haystack` — Input string. [String](../data-types/string.md).
- `pattern` — Regular expression with [re2 regular expression syntax](https://github.com/google/re2/wiki/Syntax). Must contain groups, each group enclosed in parentheses. If `pattern` contains no groups, an exception is thrown. [String](../data-types/string.md).

**Returned value**

- Array of arrays of matches. [Array](../data-types/array.md).

:::note
If `haystack` does not match the `pattern` regex, an empty array is returned.
:::

**Example**

``` sql
SELECT extractAllGroupsVertical('abc=111, def=222, ghi=333', '("[^"]+"|\\w+)=("[^"]+"|\\w+)');
```

Result:

``` text
┌─extractAllGroupsVertical('abc=111, def=222, ghi=333', '("[^"]+"|\\w+)=("[^"]+"|\\w+)')─┐
│ [['abc','111'],['def','222'],['ghi','333']]                                            │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

## like

Returns whether string `haystack` matches the LIKE expression `pattern`.

A LIKE expression can contain normal characters and the following metasymbols:

- `%` indicates an arbitrary number of arbitrary characters (including zero characters).
- `_` indicates a single arbitrary character.
- `\` is for escaping literals `%`, `_` and `\`.

Matching is based on UTF-8, e.g. `_` matches the Unicode code point `¥` which is represented in UTF-8 using two bytes.

If the haystack or the LIKE expression are not valid UTF-8, the behavior is undefined.

No automatic Unicode normalization is performed, you can use the [normalizeUTF8*()](https://clickhouse.com../functions/string-functions/) functions for that.

To match against literal `%`, `_` and `\` (which are LIKE metacharacters), prepend them with a backslash: `\%`, `\_` and `\\`.
The backslash loses its special meaning (i.e. is interpreted literally) if it prepends a character different than `%`, `_` or `\`.
Note that ClickHouse requires backslashes in strings [to be quoted as well](../syntax.md#string), so you would actually need to write `\\%`, `\\_` and `\\\\`.

For LIKE expressions of the form `%needle%`, the function is as fast as the `position` function.
All other LIKE expressions are internally converted to a regular expression and executed with a performance similar to function `match`.

**Syntax**

```sql
like(haystack, pattern)
```

Alias: `haystack LIKE pattern` (operator)

## notLike {#notlike}

Like `like` but negates the result.

Alias: `haystack NOT LIKE pattern` (operator)

## ilike

Like `like` but searches case-insensitively.

Alias: `haystack ILIKE pattern` (operator)

## notILike

Like `ilike` but negates the result.

Alias: `haystack NOT ILIKE pattern` (operator)

## ngramDistance

Calculates the 4-gram distance between a `haystack` string and a `needle` string. For this, it counts the symmetric difference between two multisets of 4-grams and normalizes it by the sum of their cardinalities. Returns a [Float32](../data-types/float.md/#float32-float64) between 0 and 1. The smaller the result is, the more similar the strings are to each other.

Functions [`ngramDistanceCaseInsensitive`](#ngramdistancecaseinsensitive), [`ngramDistanceUTF8`](#ngramdistanceutf8), [`ngramDistanceCaseInsensitiveUTF8`](#ngramdistancecaseinsensitiveutf8) provide case-insensitive and/or UTF-8 variants of this function.

**Syntax**

```sql
ngramDistance(haystack, needle)
```

**Parameters**

- `haystack`: First comparison string. [String literal](../syntax#string)
- `needle`: Second comparison string. [String literal](../syntax#string)

**Returned value**

- Value between 0 and 1 representing the similarity between the two strings. [Float32](../data-types/float.md/#float32-float64)

**Implementation details**

This function will throw an exception if constant `needle` or `haystack` arguments are more than 32Kb in size. If any non-constant `haystack` or `needle` arguments are more than 32Kb in size, then the distance is always 1.

**Examples**

The more similar two strings are to each other, the closer the result will be to 0 (identical).

Query:

```sql
SELECT ngramDistance('ClickHouse','ClickHouse!');
```

Result:

```response
0.06666667
```

The less similar two strings are to each, the larger the result will be.


Query:

```sql
SELECT ngramDistance('ClickHouse','House');
```

Result:

```response
0.5555556
```

## ngramDistanceCaseInsensitive

Provides a case-insensitive variant of [ngramDistance](#ngramdistance).

**Syntax**

```sql
ngramDistanceCaseInsensitive(haystack, needle)
```

**Parameters**

- `haystack`: First comparison string. [String literal](../syntax#string)
- `needle`: Second comparison string. [String literal](../syntax#string)

**Returned value**

- Value between 0 and 1 representing the similarity between the two strings. [Float32](../data-types/float.md/#float32-float64)

**Examples**

With [ngramDistance](#ngramdistance) differences in case will affect the similarity value:

Query:

```sql
SELECT ngramDistance('ClickHouse','clickhouse');
```

Result:

```response
0.71428573
```

With [ngramDistanceCaseInsensitive](#ngramdistancecaseinsensitive) case is ignored so two identical strings differing only in case will now return a low similarity value:

Query:

```sql
SELECT ngramDistanceCaseInsensitive('ClickHouse','clickhouse');
```

Result:

```response
0
```

## ngramDistanceUTF8

Provides a UTF-8 variant of [ngramDistance](#ngramdistance). Assumes that `needle` and `haystack` strings are UTF-8 encoded strings.

**Syntax**

```sql
ngramDistanceUTF8(haystack, needle)
```

**Parameters**

- `haystack`: First UTF-8 encoded comparison string. [String literal](../syntax#string)
- `needle`: Second UTF-8 encoded comparison string. [String literal](../syntax#string)

**Returned value**

- Value between 0 and 1 representing the similarity between the two strings. [Float32](../data-types/float.md/#float32-float64)

**Example**

Query:

```sql
SELECT ngramDistanceUTF8('abcde','cde');
```

Result:

```response
0.5
```

## ngramDistanceCaseInsensitiveUTF8

Provides a case-insensitive variant of [ngramDistanceUTF8](#ngramdistanceutf8).

**Syntax**

```sql
ngramDistanceCaseInsensitiveUTF8(haystack, needle)
```

**Parameters**

- `haystack`: First UTF-8 encoded comparison string. [String literal](../syntax#string)
- `needle`: Second UTF-8 encoded comparison string. [String literal](../syntax#string)

**Returned value**

- Value between 0 and 1 representing the similarity between the two strings. [Float32](../data-types/float.md/#float32-float64)

**Example**

Query:

```sql
SELECT ngramDistanceCaseInsensitiveUTF8('abcde','CDE');
```

Result:

```response
0.5
```

## ngramSearch

Like `ngramDistance` but calculates the non-symmetric difference between a `needle` string and a `haystack` string, i.e. the number of n-grams from the needle minus the common number of n-grams normalized by the number of `needle` n-grams. Returns a [Float32](../data-types/float.md/#float32-float64) between 0 and 1. The bigger the result is, the more likely `needle` is in the `haystack`. This function is useful for fuzzy string search. Also see function [`soundex`](../../sql-reference/functions/string-functions#soundex).

Functions [`ngramSearchCaseInsensitive`](#ngramsearchcaseinsensitive), [`ngramSearchUTF8`](#ngramsearchutf8), [`ngramSearchCaseInsensitiveUTF8`](#ngramsearchcaseinsensitiveutf8) provide case-insensitive and/or UTF-8 variants of this function.

**Syntax**

```sql
ngramSearch(haystack, needle)
```

**Parameters**

- `haystack`: First comparison string. [String literal](../syntax#string)
- `needle`: Second comparison string. [String literal](../syntax#string)

**Returned value**

- Value between 0 and 1 representing the likelihood of the `needle` being in the `haystack`. [Float32](../data-types/float.md/#float32-float64)

**Implementation details**

:::note
The UTF-8 variants use the 3-gram distance. These are not perfectly fair n-gram distances. We use 2-byte hashes to hash n-grams and then calculate the (non-)symmetric difference between these hash tables – collisions may occur. With UTF-8 case-insensitive format we do not use fair `tolower` function – we zero the 5-th bit (starting from zero) of each codepoint byte and first bit of zeroth byte if bytes more than one – this works for Latin and mostly for all Cyrillic letters.
:::

**Example**

Query:

```sql
SELECT ngramSearch('Hello World','World Hello');
```

Result:

```response
0.5
```

## ngramSearchCaseInsensitive

Provides a case-insensitive variant of [ngramSearch](#ngramsearch).

**Syntax**

```sql
ngramSearchCaseInsensitive(haystack, needle)
```

**Parameters**

- `haystack`: First comparison string. [String literal](../syntax#string)
- `needle`: Second comparison string. [String literal](../syntax#string)

**Returned value**

- Value between 0 and 1 representing the likelihood of the `needle` being in the `haystack`. [Float32](../data-types/float.md/#float32-float64)

The bigger the result is, the more likely `needle` is in the `haystack`.

**Example**

Query:

```sql
SELECT ngramSearchCaseInsensitive('Hello World','hello');
```

Result:

```response
1
```

## ngramSearchUTF8

Provides a UTF-8 variant of [ngramSearch](#ngramsearch) in which `needle` and `haystack` are assumed to be UTF-8 encoded strings.

**Syntax**

```sql
ngramSearchUTF8(haystack, needle)
```

**Parameters**

- `haystack`: First UTF-8 encoded comparison string. [String literal](../syntax#string)
- `needle`: Second UTF-8 encoded comparison string. [String literal](../syntax#string)

**Returned value**

- Value between 0 and 1 representing the likelihood of the `needle` being in the `haystack`. [Float32](../data-types/float.md/#float32-float64)

The bigger the result is, the more likely `needle` is in the `haystack`.

**Example**

Query:

```sql
SELECT ngramSearchUTF8('абвгдеёжз', 'гдеёзд');
```

Result:

```response
0.5
```

## ngramSearchCaseInsensitiveUTF8

Provides a case-insensitive variant of [ngramSearchUTF8](#ngramsearchutf8) in which `needle` and `haystack`.

**Syntax**

```sql
ngramSearchCaseInsensitiveUTF8(haystack, needle)
```

**Parameters**

- `haystack`: First UTF-8 encoded comparison string. [String literal](../syntax#string)
- `needle`: Second UTF-8 encoded comparison string. [String literal](../syntax#string)

**Returned value**

- Value between 0 and 1 representing the likelihood of the `needle` being in the `haystack`. [Float32](../data-types/float.md/#float32-float64)

The bigger the result is, the more likely `needle` is in the `haystack`.

**Example**

Query:

```sql
SELECT ngramSearchCaseInsensitiveUTF8('абвГДЕёжз', 'АбвгдЕЁжз');
```

Result:

```response
0.57142854
```

## countSubstrings

Returns how often a substring `needle` occurs in a string `haystack`.

Functions [`countSubstringsCaseInsensitive`](#countsubstringscaseinsensitive) and [`countSubstringsCaseInsensitiveUTF8`](#countsubstringscaseinsensitiveutf8) provide case-insensitive and case-insensitive + UTF-8 variants of this function respectively.

**Syntax**

``` sql
countSubstrings(haystack, needle[, start_pos])
```

**Arguments**

- `haystack` — String in which the search is performed. [String](../../sql-reference/syntax.md#syntax-string-literal).
- `needle` — Substring to be searched. [String](../../sql-reference/syntax.md#syntax-string-literal).
- `start_pos` – Position (1-based) in `haystack` at which the search starts. [UInt](../data-types/int-uint.md). Optional.

**Returned value**

- The number of occurrences. [UInt64](../data-types/int-uint.md).

**Examples**

``` sql
SELECT countSubstrings('aaaa', 'aa');
```

Result:

``` text
┌─countSubstrings('aaaa', 'aa')─┐
│                             2 │
└───────────────────────────────┘
```

Example with `start_pos` argument:

```sql
SELECT countSubstrings('abc___abc', 'abc', 4);
```

Result:

``` text
┌─countSubstrings('abc___abc', 'abc', 4)─┐
│                                      1 │
└────────────────────────────────────────┘
```
## countSubstringsCaseInsensitive

Returns how often a substring `needle` occurs in a string `haystack`. Ignores case.

**Syntax**

``` sql
countSubstringsCaseInsensitive(haystack, needle[, start_pos])
```

**Arguments**

- `haystack` — String in which the search is performed. [String](../../sql-reference/syntax.md#syntax-string-literal).
- `needle` — Substring to be searched. [String](../../sql-reference/syntax.md#syntax-string-literal).
- `start_pos` – Position (1-based) in `haystack` at which the search starts. [UInt](../data-types/int-uint.md). Optional.

**Returned value**

- The number of occurrences. [UInt64](../data-types/int-uint.md).

**Examples**

Query:

``` sql
SELECT countSubstringsCaseInsensitive('AAAA', 'aa');
```

Result:

``` text
┌─countSubstringsCaseInsensitive('AAAA', 'aa')─┐
│                                            2 │
└──────────────────────────────────────────────┘
```

Example with `start_pos` argument:

Query:

```sql
SELECT countSubstringsCaseInsensitive('abc___ABC___abc', 'abc', 4);
```

Result:

``` text
┌─countSubstringsCaseInsensitive('abc___ABC___abc', 'abc', 4)─┐
│                                                           2 │
└─────────────────────────────────────────────────────────────┘
```

## countSubstringsCaseInsensitiveUTF8

Returns how often a substring `needle` occurs in a string `haystack`. Ignores case and assumes that `haystack` is a UTF8 string.

**Syntax**

``` sql
countSubstringsCaseInsensitiveUTF8(haystack, needle[, start_pos])
```

**Arguments**

- `haystack` — UTF-8 string in which the search is performed. [String](../../sql-reference/syntax.md#syntax-string-literal).
- `needle` — Substring to be searched. [String](../../sql-reference/syntax.md#syntax-string-literal).
- `start_pos` – Position (1-based) in `haystack` at which the search starts. [UInt](../data-types/int-uint.md). Optional.

**Returned value**

- The number of occurrences. [UInt64](../data-types/int-uint.md).

**Examples**

Query:

``` sql
SELECT countSubstringsCaseInsensitiveUTF8('ложка, кошка, картошка', 'КА');
```

Result:

``` text
┌─countSubstringsCaseInsensitiveUTF8('ложка, кошка, картошка', 'КА')─┐
│                                                                  4 │
└────────────────────────────────────────────────────────────────────┘
```

Example with `start_pos` argument:

Query:

```sql
SELECT countSubstringsCaseInsensitiveUTF8('ложка, кошка, картошка', 'КА', 13);
```

Result:

``` text
┌─countSubstringsCaseInsensitiveUTF8('ложка, кошка, картошка', 'КА', 13)─┐
│                                                                      2 │
└────────────────────────────────────────────────────────────────────────┘
```

## countMatches

Returns the number of regular expression matches for a `pattern` in a `haystack`.

**Syntax**

``` sql
countMatches(haystack, pattern)
```

**Arguments**

- `haystack` — The string to search in. [String](../../sql-reference/syntax.md#syntax-string-literal).
- `pattern` — The regular expression with [re2 regular expression syntax](https://github.com/google/re2/wiki/Syntax). [String](../data-types/string.md).

**Returned value**

- The number of matches. [UInt64](../data-types/int-uint.md).

**Examples**

``` sql
SELECT countMatches('foobar.com', 'o+');
```

Result:

``` text
┌─countMatches('foobar.com', 'o+')─┐
│                                2 │
└──────────────────────────────────┘
```

``` sql
SELECT countMatches('aaaa', 'aa');
```

Result:

``` text
┌─countMatches('aaaa', 'aa')────┐
│                             2 │
└───────────────────────────────┘
```

## countMatchesCaseInsensitive

Returns the number of regular expression matches for a pattern in a haystack like [`countMatches`](#countmatches) but matching ignores the case.

**Syntax**

``` sql
countMatchesCaseInsensitive(haystack, pattern)
```

**Arguments**

- `haystack` — The string to search in. [String](../../sql-reference/syntax.md#syntax-string-literal).
- `pattern` — The regular expression with [re2 regular expression syntax](https://github.com/google/re2/wiki/Syntax). [String](../data-types/string.md).

**Returned value**

- The number of matches. [UInt64](../data-types/int-uint.md).

**Examples**

Query:

``` sql
SELECT countMatchesCaseInsensitive('AAAA', 'aa');
```

Result:

``` text
┌─countMatchesCaseInsensitive('AAAA', 'aa')────┐
│                                            2 │
└──────────────────────────────────────────────┘
```

## regexpExtract

Extracts the first string in `haystack` that matches the regexp pattern and corresponds to the regex group index.

**Syntax**

``` sql
regexpExtract(haystack, pattern[, index])
```

Alias: `REGEXP_EXTRACT(haystack, pattern[, index])`.

**Arguments**

- `haystack` — String, in which regexp pattern will to be matched. [String](../../sql-reference/syntax.md#syntax-string-literal).
- `pattern` — String, regexp expression, must be constant. [String](../../sql-reference/syntax.md#syntax-string-literal).
- `index` – An integer number greater or equal 0 with default 1. It represents which regex group to extract. [UInt or Int](../data-types/int-uint.md). Optional.

**Returned value**

`pattern` may contain multiple regexp groups, `index` indicates which regex group to extract. An index of 0 means matching the entire regular expression. [String](../data-types/string.md).

**Examples**

``` sql
SELECT
    regexpExtract('100-200', '(\\d+)-(\\d+)', 1),
    regexpExtract('100-200', '(\\d+)-(\\d+)', 2),
    regexpExtract('100-200', '(\\d+)-(\\d+)', 0),
    regexpExtract('100-200', '(\\d+)-(\\d+)');
```

Result:

``` text
┌─regexpExtract('100-200', '(\\d+)-(\\d+)', 1)─┬─regexpExtract('100-200', '(\\d+)-(\\d+)', 2)─┬─regexpExtract('100-200', '(\\d+)-(\\d+)', 0)─┬─regexpExtract('100-200', '(\\d+)-(\\d+)')─┐
│ 100                                          │ 200                                          │ 100-200                                      │ 100                                       │
└──────────────────────────────────────────────┴──────────────────────────────────────────────┴──────────────────────────────────────────────┴───────────────────────────────────────────┘
```

## hasSubsequence

Returns 1 if `needle` is a subsequence of `haystack`, or 0 otherwise.
A subsequence of a string is a sequence that can be derived from the given string by deleting zero or more elements without changing the order of the remaining elements.


**Syntax**

``` sql
hasSubsequence(haystack, needle)
```

**Arguments**

- `haystack` — String in which the search is performed. [String](../../sql-reference/syntax.md#syntax-string-literal).
- `needle` — Subsequence to be searched. [String](../../sql-reference/syntax.md#syntax-string-literal).

**Returned value**

- 1, if needle is a subsequence of haystack, 0 otherwise. [UInt8](../data-types/int-uint.md).

**Examples**

Query:

``` sql
SELECT hasSubsequence('garbage', 'arg');
```

Result:

``` text
┌─hasSubsequence('garbage', 'arg')─┐
│                                1 │
└──────────────────────────────────┘
```

## hasSubsequenceCaseInsensitive

Like [hasSubsequence](#hassubsequence) but searches case-insensitively.

**Syntax**

``` sql
hasSubsequenceCaseInsensitive(haystack, needle)
```

**Arguments**

- `haystack` — String in which the search is performed. [String](../../sql-reference/syntax.md#syntax-string-literal).
- `needle` — Subsequence to be searched. [String](../../sql-reference/syntax.md#syntax-string-literal).

**Returned value**

- 1, if needle is a subsequence of haystack, 0 otherwise [UInt8](../data-types/int-uint.md).

**Examples**

Query:

``` sql
SELECT hasSubsequenceCaseInsensitive('garbage', 'ARG');
```

Result:

``` text
┌─hasSubsequenceCaseInsensitive('garbage', 'ARG')─┐
│                                               1 │
└─────────────────────────────────────────────────┘
```

## hasSubsequenceUTF8

Like [hasSubsequence](#hassubsequence) but assumes `haystack` and `needle` are UTF-8 encoded strings.

**Syntax**

``` sql
hasSubsequenceUTF8(haystack, needle)
```

**Arguments**

- `haystack` — String in which the search is performed. UTF-8 encoded [String](../../sql-reference/syntax.md#syntax-string-literal).
- `needle` — Subsequence to be searched. UTF-8 encoded [String](../../sql-reference/syntax.md#syntax-string-literal).

**Returned value**

- 1, if needle is a subsequence of haystack, 0, otherwise. [UInt8](../data-types/int-uint.md).

Query:

**Examples**

``` sql
select hasSubsequenceUTF8('ClickHouse - столбцовая система управления базами данных', 'система');
```

Result:

``` text
┌─hasSubsequenceUTF8('ClickHouse - столбцовая система управления базами данных', 'система')─┐
│                                                                                         1 │
└───────────────────────────────────────────────────────────────────────────────────────────┘
```

## hasSubsequenceCaseInsensitiveUTF8

Like [hasSubsequenceUTF8](#hassubsequenceutf8) but searches case-insensitively.

**Syntax**

``` sql
hasSubsequenceCaseInsensitiveUTF8(haystack, needle)
```

**Arguments**

- `haystack` — String in which the search is performed. UTF-8 encoded [String](../../sql-reference/syntax.md#syntax-string-literal).
- `needle` — Subsequence to be searched. UTF-8 encoded [String](../../sql-reference/syntax.md#syntax-string-literal).

**Returned value**

- 1, if needle is a subsequence of haystack, 0 otherwise. [UInt8](../data-types/int-uint.md).

**Examples**

Query:

``` sql
select hasSubsequenceCaseInsensitiveUTF8('ClickHouse - столбцовая система управления базами данных', 'СИСТЕМА');
```

Result:

``` text
┌─hasSubsequenceCaseInsensitiveUTF8('ClickHouse - столбцовая система управления базами данных', 'СИСТЕМА')─┐
│                                                                                                        1 │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

## hasToken

Returns 1 if a given token is present in a haystack, or 0 otherwise.

**Syntax**

```sql
hasToken(haystack, token)
```

**Parameters**

- `haystack`: String in which the search is performed. [String](../../sql-reference/syntax.md#syntax-string-literal).
- `token`: Maximal length substring between two non alphanumeric ASCII characters (or boundaries of haystack).

**Returned value**

- 1, if the token is present in the haystack, 0 otherwise. [UInt8](../data-types/int-uint.md).

**Implementation details**

Token must be a constant string. Supported by tokenbf_v1 index specialization.

**Example**

Query:

```sql
SELECT hasToken('Hello World','Hello');
```

```response
1
```

## hasTokenOrNull

Returns 1 if a given token is present, 0 if not present, and null if the token is ill-formed.

**Syntax**

```sql
hasTokenOrNull(haystack, token)
```

**Parameters**

- `haystack`: String in which the search is performed. [String](../../sql-reference/syntax.md#syntax-string-literal).
- `token`: Maximal length substring between two non alphanumeric ASCII characters (or boundaries of haystack).

**Returned value**

- 1, if the token is present in the haystack, 0 if it is not present, and null if the token is ill formed. 

**Implementation details**

Token must be a constant string. Supported by tokenbf_v1 index specialization.

**Example**

Where `hasToken` would throw an error for an ill-formed token, `hasTokenOrNull` returns `null` for an ill-formed token.

Query:

```sql
SELECT hasTokenOrNull('Hello World','Hello,World');
```

```response
null
```

## hasTokenCaseInsensitive

Returns 1 if a given token is present in a haystack, 0 otherwise. Ignores case.

**Syntax**

```sql
hasTokenCaseInsensitive(haystack, token)
```

**Parameters**

- `haystack`: String in which the search is performed. [String](../../sql-reference/syntax.md#syntax-string-literal).
- `token`: Maximal length substring between two non alphanumeric ASCII characters (or boundaries of haystack).

**Returned value**

- 1, if the token is present in the haystack, 0 otherwise. [UInt8](../data-types/int-uint.md).

**Implementation details**

Token must be a constant string. Supported by tokenbf_v1 index specialization.

**Example**

Query:

```sql
SELECT hasTokenCaseInsensitive('Hello World','hello');
```

```response
1
```

## hasTokenCaseInsensitiveOrNull

Returns 1 if a given token is present in a haystack, 0 otherwise. Ignores case and returns null if the token is ill-formed.

**Syntax**

```sql
hasTokenCaseInsensitiveOrNull(haystack, token)
```

**Parameters**

- `haystack`: String in which the search is performed. [String](../../sql-reference/syntax.md#syntax-string-literal).
- `token`: Maximal length substring between two non alphanumeric ASCII characters (or boundaries of haystack).

**Returned value**

- 1, if the token is present in the haystack, 0 if the token is not present, otherwise [`null`](../data-types/nullable.md) if the token is ill-formed. [UInt8](../data-types/int-uint.md).

**Implementation details**

Token must be a constant string. Supported by tokenbf_v1 index specialization.

**Example**


Where `hasTokenCaseInsensitive` would throw an error for an ill-formed token, `hasTokenCaseInsensitiveOrNull` returns `null` for an ill-formed token.

Query:

```sql
SELECT hasTokenCaseInsensitiveOrNull('Hello World','hello,world');
```

```response
null
```
