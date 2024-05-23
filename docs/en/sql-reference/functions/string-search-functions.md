---
slug: /en/sql-reference/functions/string-search-functions
sidebar_position: 160
sidebar_label: Searching in Strings
---

# Functions for Searching in Strings

All functions in this section search by default case-sensitively. Case-insensitive search is usually provided by separate function variants.
Note that case-insensitive search follows the lowercase-uppercase rules of the English language. E.g. Uppercased `i` in English language is
`I` whereas in Turkish language it is `İ` - results for languages other than English may be unexpected.

Functions in this section also assume that the searched string and the search string are single-byte encoded text. If this assumption is
violated, no exception is thrown and results are undefined. Search with UTF-8 encoded strings is usually provided by separate function
variants. Likewise, if a UTF-8 function variant is used and the input strings are not UTF-8 encoded text, no exception is thrown and the
results are undefined. Note that no automatic Unicode normalization is performed, you can use the
[normalizeUTF8*()](https://clickhouse.com/docs/en/sql-reference/functions/string-functions/) functions for that.

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
- `start_pos` – Position (1-based) in `haystack` at which the search starts. [UInt](../../sql-reference/data-types/int-uint.md). Optional.

**Returned values**

- Starting position in bytes and counting from 1, if the substring was found.
- 0, if the substring was not found.

If substring `needle` is empty, these rules apply:
- if no `start_pos` was specified: return `1`
- if `start_pos = 0`: return `1`
- if `start_pos >= 1` and `start_pos <= length(haystack) + 1`: return `start_pos`
- otherwise: return `0`

The same rules also apply to functions `locate`, `positionCaseInsensitive`, `positionUTF8` and `positionCaseInsensitiveUTF8`.

Type: `Integer`.

**Examples**

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

``` sql
SELECT
    position('Hello, world!', 'o', 1),
    position('Hello, world!', 'o', 7)
```

``` text
┌─position('Hello, world!', 'o', 1)─┬─position('Hello, world!', 'o', 7)─┐
│                                 5 │                                 9 │
└───────────────────────────────────┴───────────────────────────────────┘
```

Example for `needle IN haystack` syntax:

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

Like [position](#position) but searches case-insensitively.

## positionUTF8

Like [position](#position) but assumes `haystack` and `needle` are UTF-8 encoded strings.

**Examples**

Function `positionUTF8` correctly counts character `ö` (represented by two points) as a single Unicode codepoint:

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
- `needle` — Substrings to be searched. Array

**Returned values**

- Array of the starting position in bytes and counting from 1 (if the substring was found) or 0 (if the substring was not found)

**Example**

``` sql
SELECT multiSearchAllPositions('Hello, World!', ['hello', '!', 'world']);
```

Result:

``` text
┌─multiSearchAllPositions('Hello, World!', ['hello', '!', 'world'])─┐
│ [0,13,0]                                                          │
└───────────────────────────────────────────────────────────────────┘
```

## multiSearchAllPositionsUTF8

Like [multiSearchAllPositions](#multiSearchAllPositions) but assumes `haystack` and the `needle`-s are UTF-8 encoded strings.

## multiSearchFirstPosition

Like `position` but returns the leftmost offset in a `haystack` string which matches any of multiple `needle` strings.

Functions `multiSearchFirstPositionCaseInsensitive`, `multiSearchFirstPositionUTF8` and `multiSearchFirstPositionCaseInsensitiveUTF8` provide case-insensitive and/or UTF-8 variants of this function.

**Syntax**

```sql
multiSearchFirstPosition(haystack, \[needle<sub>1</sub>, needle<sub>2</sub>, …, needle<sub>n</sub>\])
```

## multiSearchFirstIndex

Returns the index `i` (starting from 1) of the leftmost found needle<sub>i</sub> in the string `haystack` and 0 otherwise.

Functions `multiSearchFirstIndexCaseInsensitive`, `multiSearchFirstIndexUTF8` and `multiSearchFirstIndexCaseInsensitiveUTF8` provide case-insensitive and/or UTF-8 variants of this function.

**Syntax**

```sql
multiSearchFirstIndex(haystack, \[needle<sub>1</sub>, needle<sub>2</sub>, …, needle<sub>n</sub>\])
```

## multiSearchAny {#multisearchany}

Returns 1, if at least one string needle<sub>i</sub> matches the string `haystack` and 0 otherwise.

Functions `multiSearchAnyCaseInsensitive`, `multiSearchAnyUTF8` and `multiSearchAnyCaseInsensitiveUTF8` provide case-insensitive and/or UTF-8 variants of this function.

**Syntax**

```sql
multiSearchAny(haystack, \[needle<sub>1</sub>, needle<sub>2</sub>, …, needle<sub>n</sub>\])
```

## match {#match}

Returns whether string `haystack` matches the regular expression `pattern` in [re2 regular syntax](https://github.com/google/re2/wiki/Syntax).

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
multiMatchAny(haystack, \[pattern<sub>1</sub>, pattern<sub>2</sub>, …, pattern<sub>n</sub>\])
```

## multiMatchAnyIndex

Like `multiMatchAny` but returns any index that matches the haystack.

**Syntax**

```sql
multiMatchAnyIndex(haystack, \[pattern<sub>1</sub>, pattern<sub>2</sub>, …, pattern<sub>n</sub>\])
```

## multiMatchAllIndices

Like `multiMatchAny` but returns the array of all indices that match the haystack in any order.

**Syntax**

```sql
multiMatchAllIndices(haystack, \[pattern<sub>1</sub>, pattern<sub>2</sub>, …, pattern<sub>n</sub>\])
```

## multiFuzzyMatchAny

Like `multiMatchAny` but returns 1 if any pattern matches the haystack within a constant [edit distance](https://en.wikipedia.org/wiki/Edit_distance). This function relies on the experimental feature of [hyperscan](https://intel.github.io/hyperscan/dev-reference/compilation.html#approximate-matching) library, and can be slow for some corner cases. The performance depends on the edit distance value and patterns used, but it's always more expensive compared to a non-fuzzy variants.

:::note
`multiFuzzyMatch*()` function family do not support UTF-8 regular expressions (it threats them as a sequence of bytes) due to restrictions of hyperscan.
:::

**Syntax**

```sql
multiFuzzyMatchAny(haystack, distance, \[pattern<sub>1</sub>, pattern<sub>2</sub>, …, pattern<sub>n</sub>\])
```

## multiFuzzyMatchAnyIndex

Like `multiFuzzyMatchAny` but returns any index that matches the haystack within a constant edit distance.

**Syntax**

```sql
multiFuzzyMatchAnyIndex(haystack, distance, \[pattern<sub>1</sub>, pattern<sub>2</sub>, …, pattern<sub>n</sub>\])
```

## multiFuzzyMatchAllIndices

Like `multiFuzzyMatchAny` but returns the array of all indices in any order that match the haystack within a constant edit distance.

**Syntax**

```sql
multiFuzzyMatchAllIndices(haystack, distance, \[pattern<sub>1</sub>, pattern<sub>2</sub>, …, pattern<sub>n</sub>\])
```

## extract

Extracts a fragment of a string using a regular expression. If `haystack` does not match the `pattern` regex, an empty string is returned.

For regex without subpatterns, the function uses the fragment that matches the entire regex. Otherwise, it uses the fragment that matches the first subpattern.

**Syntax**

```sql
extract(haystack, pattern)
```

## extractAll

Extracts all fragments of a string using a regular expression. If `haystack` does not match the `pattern` regex, an empty string is returned.

Returns an array of strings consisting of all matches of the regex.

The behavior with respect to subpatterns is the same as in function `extract`.

**Syntax**

```sql
extractAll(haystack, pattern)
```

## extractAllGroupsHorizontal

Matches all groups of the `haystack` string using the `pattern` regular expression. Returns an array of arrays, where the first array includes all fragments matching the first group, the second array - matching the second group, etc.

This function is slower than [extractAllGroupsVertical](#extractallgroups-vertical).

**Syntax**

``` sql
extractAllGroupsHorizontal(haystack, pattern)
```

**Arguments**

- `haystack` — Input string. Type: [String](../../sql-reference/data-types/string.md).
- `pattern` — Regular expression with [re2 syntax](https://github.com/google/re2/wiki/Syntax). Must contain groups, each group enclosed in parentheses. If `pattern` contains no groups, an exception is thrown. Type: [String](../../sql-reference/data-types/string.md).

**Returned value**

- Type: [Array](../../sql-reference/data-types/array.md).

If `haystack` does not match the `pattern` regex, an array of empty arrays is returned.

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

## extractAllGroupsVertical

Matches all groups of the `haystack` string using the `pattern` regular expression. Returns an array of arrays, where each array includes matching fragments from every group. Fragments are grouped in order of appearance in the `haystack`.

**Syntax**

``` sql
extractAllGroupsVertical(haystack, pattern)
```

**Arguments**

- `haystack` — Input string. Type: [String](../../sql-reference/data-types/string.md).
- `pattern` — Regular expression with [re2 syntax](https://github.com/google/re2/wiki/Syntax). Must contain groups, each group enclosed in parentheses. If `pattern` contains no groups, an exception is thrown. Type: [String](../../sql-reference/data-types/string.md).

**Returned value**

- Type: [Array](../../sql-reference/data-types/array.md).

If `haystack` does not match the `pattern` regex, an empty array is returned.

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

## like {#like}

Returns whether string `haystack` matches the LIKE expression `pattern`.

A LIKE expression can contain normal characters and the following metasymbols:

- `%` indicates an arbitrary number of arbitrary characters (including zero characters).
- `_` indicates a single arbitrary character.
- `\` is for escaping literals `%`, `_` and `\`.

Matching is based on UTF-8, e.g. `_` matches the Unicode code point `¥` which is represented in UTF-8 using two bytes.

If the haystack or the LIKE expression are not valid UTF-8, the behavior is undefined.

No automatic Unicode normalization is performed, you can use the [normalizeUTF8*()](https://clickhouse.com/docs/en/sql-reference/functions/string-functions/) functions for that.

To match against literal `%`, `_` and `/` (which are LIKE metacharacters), prepend them with a backslash: `\%`, `\_` and `\\`.
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

Calculates the 4-gram distance between a `haystack` string and a `needle` string. For that, it counts the symmetric difference between two multisets of 4-grams and normalizes it by the sum of their cardinalities. Returns a Float32 between 0 and 1. The smaller the result is, the more strings are similar to each other. Throws an exception if constant `needle` or `haystack` arguments are more than 32Kb in size. If any of non-constant `haystack` or `needle` arguments is more than 32Kb in size, the distance is always 1.

Functions `ngramDistanceCaseInsensitive, ngramDistanceUTF8, ngramDistanceCaseInsensitiveUTF8` provide case-insensitive and/or UTF-8 variants of this function.

**Syntax**

```sql
ngramDistance(haystack, needle)
```

## ngramSearch

Like `ngramDistance` but calculates the non-symmetric difference between a `needle` string and a `haystack` string, i.e. the number of n-grams from needle minus the common number of n-grams normalized by the number of `needle` n-grams. Returns a Float32 between 0 and 1. The bigger the result is, the more likely `needle` is in the `haystack`. This function is useful for fuzzy string search. Also see function `soundex`.

Functions `ngramSearchCaseInsensitive, ngramSearchUTF8, ngramSearchCaseInsensitiveUTF8` provide case-insensitive and/or UTF-8 variants of this function.

:::note
The UTF-8 variants use the 3-gram distance. These are not perfectly fair n-gram distances. We use 2-byte hashes to hash n-grams and then calculate the (non-)symmetric difference between these hash tables – collisions may occur. With UTF-8 case-insensitive format we do not use fair `tolower` function – we zero the 5-th bit (starting from zero) of each codepoint byte and first bit of zeroth byte if bytes more than one – this works for Latin and mostly for all Cyrillic letters.
:::

**Syntax**

```sql
ngramSearch(haystack, needle)
```

## countSubstrings

Returns how often substring `needle` occurs in string `haystack`.

Functions `countSubstringsCaseInsensitive` and `countSubstringsCaseInsensitiveUTF8` provide a case-insensitive and case-insensitive + UTF-8 variants of this function.

**Syntax**

``` sql
countSubstrings(haystack, needle[, start_pos])
```

**Arguments**

- `haystack` — String in which the search is performed. [String](../../sql-reference/syntax.md#syntax-string-literal).
- `needle` — Substring to be searched. [String](../../sql-reference/syntax.md#syntax-string-literal).
- `start_pos` – Position (1-based) in `haystack` at which the search starts. [UInt](../../sql-reference/data-types/int-uint.md). Optional.

**Returned values**

- The number of occurrences.

Type: [UInt64](../../sql-reference/data-types/int-uint.md).

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

## countMatches

Returns the number of regular expression matches for a `pattern` in a `haystack`.

**Syntax**

``` sql
countMatches(haystack, pattern)
```

**Arguments**

- `haystack` — The string to search in. [String](../../sql-reference/syntax.md#syntax-string-literal).
- `pattern` — The regular expression with [re2 syntax](https://github.com/google/re2/wiki/Syntax). [String](../../sql-reference/data-types/string.md).

**Returned value**

- The number of matches.

Type: [UInt64](../../sql-reference/data-types/int-uint.md).

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

Like `countMatches(haystack, pattern)` but matching ignores the case.

## regexpExtract

Extracts the first string in haystack that matches the regexp pattern and corresponds to the regex group index.

**Syntax**

``` sql
regexpExtract(haystack, pattern[, index])
```

Alias: `REGEXP_EXTRACT(haystack, pattern[, index])`.

**Arguments**

- `haystack` — String, in which regexp pattern will to be matched. [String](../../sql-reference/syntax.md#syntax-string-literal).
- `pattern` — String, regexp expression, must be constant. [String](../../sql-reference/syntax.md#syntax-string-literal).
- `index` – An integer number greater or equal 0 with default 1. It represents which regex group to extract. [UInt or Int](../../sql-reference/data-types/int-uint.md). Optional.

**Returned values**

`pattern` may contain multiple regexp groups, `index` indicates which regex group to extract. An index of 0 means matching the entire regular expression.

Type: `String`.

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

Returns 1 if needle is a subsequence of haystack, or 0 otherwise.
A subsequence of a string is a sequence that can be derived from the given string by deleting zero or more elements without changing the order of the remaining elements.


**Syntax**

``` sql
hasSubsequence(haystack, needle)
```

**Arguments**

- `haystack` — String in which the search is performed. [String](../../sql-reference/syntax.md#syntax-string-literal).
- `needle` — Subsequence to be searched. [String](../../sql-reference/syntax.md#syntax-string-literal).

**Returned values**

- 1, if needle is a subsequence of haystack.
- 0, otherwise.

Type: `UInt8`.

**Examples**

``` sql
SELECT hasSubsequence('garbage', 'arg') ;
```

Result:

``` text
┌─hasSubsequence('garbage', 'arg')─┐
│                                1 │
└──────────────────────────────────┘
```

## hasSubsequenceCaseInsensitive

Like [hasSubsequence](#hasSubsequence) but searches case-insensitively.

## hasSubsequenceUTF8

Like [hasSubsequence](#hasSubsequence) but assumes `haystack` and `needle` are UTF-8 encoded strings.

## hasSubsequenceCaseInsensitiveUTF8

Like [hasSubsequenceUTF8](#hasSubsequenceUTF8) but searches case-insensitively.
