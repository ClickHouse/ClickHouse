---
toc_priority: 41
toc_title: For Searching in Strings
---

# Functions for Searching in Strings {#functions-for-searching-strings}

The search is case-sensitive by default in all these functions. There are separate variants for case insensitive search.

!!! note "Note"
    Functions for [replacing](../../sql-reference/functions/string-replace-functions.md) and [other manipulations with strings](../../sql-reference/functions/string-functions.md) are described separately.

## position(haystack, needle), locate(haystack, needle) {#position}

Returns the position (in bytes) of the found substring in the string, starting from 1.

Works under the assumption that the string contains a set of bytes representing a single-byte encoded text. If this assumption is not met and a character can’t be represented using a single byte, the function doesn’t throw an exception and returns some unexpected result. If character can be represented using two bytes, it will use two bytes and so on.

For a case-insensitive search, use the function [positionCaseInsensitive](#positioncaseinsensitive).

**Syntax**

``` sql
position(haystack, needle[, start_pos])
```

Alias: `locate(haystack, needle[, start_pos])`.

**Parameters**

-   `haystack` — string, in which substring will to be searched. [String](../../sql-reference/syntax.md#syntax-string-literal).
-   `needle` — substring to be searched. [String](../../sql-reference/syntax.md#syntax-string-literal).
-   `start_pos` – Optional parameter, position of the first character in the string to start search. [UInt](../../sql-reference/data-types/int-uint.md)

**Returned values**

-   Starting position in bytes (counting from 1), if substring was found.
-   0, if the substring was not found.

Type: `Integer`.

**Examples**

The phrase “Hello, world!” contains a set of bytes representing a single-byte encoded text. The function returns some expected result:

Query:

``` sql
SELECT position('Hello, world!', '!')
```

Result:

``` text
┌─position('Hello, world!', '!')─┐
│                             13 │
└────────────────────────────────┘
```

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

The same phrase in Russian contains characters which can’t be represented using a single byte. The function returns some unexpected result (use [positionUTF8](#positionutf8) function for multi-byte encoded text):

Query:

``` sql
SELECT position('Привет, мир!', '!')
```

Result:

``` text
┌─position('Привет, мир!', '!')─┐
│                            21 │
└───────────────────────────────┘
```

## positionCaseInsensitive {#positioncaseinsensitive}

The same as [position](#position) returns the position (in bytes) of the found substring in the string, starting from 1. Use the function for a case-insensitive search.

Works under the assumption that the string contains a set of bytes representing a single-byte encoded text. If this assumption is not met and a character can’t be represented using a single byte, the function doesn’t throw an exception and returns some unexpected result. If character can be represented using two bytes, it will use two bytes and so on.

**Syntax**

``` sql
positionCaseInsensitive(haystack, needle[, start_pos])
```

**Parameters**

-   `haystack` — string, in which substring will to be searched. [String](../../sql-reference/syntax.md#syntax-string-literal).
-   `needle` — substring to be searched. [String](../../sql-reference/syntax.md#syntax-string-literal).
-   `start_pos` – Optional parameter, position of the first character in the string to start search. [UInt](../../sql-reference/data-types/int-uint.md)

**Returned values**

-   Starting position in bytes (counting from 1), if substring was found.
-   0, if the substring was not found.

Type: `Integer`.

**Example**

Query:

``` sql
SELECT positionCaseInsensitive('Hello, world!', 'hello')
```

Result:

``` text
┌─positionCaseInsensitive('Hello, world!', 'hello')─┐
│                                                 1 │
└───────────────────────────────────────────────────┘
```

## positionUTF8 {#positionutf8}

Returns the position (in Unicode points) of the found substring in the string, starting from 1.

Works under the assumption that the string contains a set of bytes representing a UTF-8 encoded text. If this assumption is not met, the function doesn’t throw an exception and returns some unexpected result. If character can be represented using two Unicode points, it will use two and so on.

For a case-insensitive search, use the function [positionCaseInsensitiveUTF8](#positioncaseinsensitiveutf8).

**Syntax**

``` sql
positionUTF8(haystack, needle[, start_pos])
```

**Parameters**

-   `haystack` — string, in which substring will to be searched. [String](../../sql-reference/syntax.md#syntax-string-literal).
-   `needle` — substring to be searched. [String](../../sql-reference/syntax.md#syntax-string-literal).
-   `start_pos` – Optional parameter, position of the first character in the string to start search. [UInt](../../sql-reference/data-types/int-uint.md)

**Returned values**

-   Starting position in Unicode points (counting from 1), if substring was found.
-   0, if the substring was not found.

Type: `Integer`.

**Examples**

The phrase “Hello, world!” in Russian contains a set of Unicode points representing a single-point encoded text. The function returns some expected result:

Query:

``` sql
SELECT positionUTF8('Привет, мир!', '!')
```

Result:

``` text
┌─positionUTF8('Привет, мир!', '!')─┐
│                                12 │
└───────────────────────────────────┘
```

The phrase “Salut, étudiante!”, where character `é` can be represented using a one point (`U+00E9`) or two points (`U+0065U+0301`) the function can be returned some unexpected result:

Query for the letter `é`, which is represented one Unicode point `U+00E9`:

``` sql
SELECT positionUTF8('Salut, étudiante!', '!')
```

Result:

``` text
┌─positionUTF8('Salut, étudiante!', '!')─┐
│                                     17 │
└────────────────────────────────────────┘
```

Query for the letter `é`, which is represented two Unicode points `U+0065U+0301`:

``` sql
SELECT positionUTF8('Salut, étudiante!', '!')
```

Result:

``` text
┌─positionUTF8('Salut, étudiante!', '!')─┐
│                                     18 │
└────────────────────────────────────────┘
```

## positionCaseInsensitiveUTF8 {#positioncaseinsensitiveutf8}

The same as [positionUTF8](#positionutf8), but is case-insensitive. Returns the position (in Unicode points) of the found substring in the string, starting from 1.

Works under the assumption that the string contains a set of bytes representing a UTF-8 encoded text. If this assumption is not met, the function doesn’t throw an exception and returns some unexpected result. If character can be represented using two Unicode points, it will use two and so on.

**Syntax**

``` sql
positionCaseInsensitiveUTF8(haystack, needle[, start_pos])
```

**Parameters**

-   `haystack` — string, in which substring will to be searched. [String](../../sql-reference/syntax.md#syntax-string-literal).
-   `needle` — substring to be searched. [String](../../sql-reference/syntax.md#syntax-string-literal).
-   `start_pos` – Optional parameter, position of the first character in the string to start search. [UInt](../../sql-reference/data-types/int-uint.md)

**Returned value**

-   Starting position in Unicode points (counting from 1), if substring was found.
-   0, if the substring was not found.

Type: `Integer`.

**Example**

Query:

``` sql
SELECT positionCaseInsensitiveUTF8('Привет, мир!', 'Мир')
```

Result:

``` text
┌─positionCaseInsensitiveUTF8('Привет, мир!', 'Мир')─┐
│                                                  9 │
└────────────────────────────────────────────────────┘
```

## multiSearchAllPositions {#multisearchallpositions}

The same as [position](../../sql-reference/functions/string-search-functions.md#position) but returns `Array` of positions (in bytes) of the found corresponding substrings in the string. Positions are indexed starting from 1.

The search is performed on sequences of bytes without respect to string encoding and collation.

-   For case-insensitive ASCII search, use the function `multiSearchAllPositionsCaseInsensitive`.
-   For search in UTF-8, use the function [multiSearchAllPositionsUTF8](#multiSearchAllPositionsUTF8).
-   For case-insensitive UTF-8 search, use the function multiSearchAllPositionsCaseInsensitiveUTF8.

**Syntax**

``` sql
multiSearchAllPositions(haystack, [needle1, needle2, ..., needlen])
```

**Parameters**

-   `haystack` — string, in which substring will to be searched. [String](../../sql-reference/syntax.md#syntax-string-literal).
-   `needle` — substring to be searched. [String](../../sql-reference/syntax.md#syntax-string-literal).

**Returned values**

-   Array of starting positions in bytes (counting from 1), if the corresponding substring was found and 0 if not found.

**Example**

Query:

``` sql
SELECT multiSearchAllPositions('Hello, World!', ['hello', '!', 'world'])
```

Result:

``` text
┌─multiSearchAllPositions('Hello, World!', ['hello', '!', 'world'])─┐
│ [0,13,0]                                                          │
└───────────────────────────────────────────────────────────────────┘
```

## multiSearchAllPositionsUTF8 {#multiSearchAllPositionsUTF8}

See `multiSearchAllPositions`.

## multiSearchFirstPosition(haystack, \[needle<sub>1</sub>, needle<sub>2</sub>, …, needle<sub>n</sub>\]) {#multisearchfirstposition}

The same as `position` but returns the leftmost offset of the string `haystack` that is matched to some of the needles.

For a case-insensitive search or/and in UTF-8 format use functions `multiSearchFirstPositionCaseInsensitive, multiSearchFirstPositionUTF8, multiSearchFirstPositionCaseInsensitiveUTF8`.

## multiSearchFirstIndex(haystack, \[needle<sub>1</sub>, needle<sub>2</sub>, …, needle<sub>n</sub>\]) {#multisearchfirstindexhaystack-needle1-needle2-needlen}

Returns the index `i` (starting from 1) of the leftmost found needle<sub>i</sub> in the string `haystack` and 0 otherwise.

For a case-insensitive search or/and in UTF-8 format use functions `multiSearchFirstIndexCaseInsensitive, multiSearchFirstIndexUTF8, multiSearchFirstIndexCaseInsensitiveUTF8`.

## multiSearchAny(haystack, \[needle<sub>1</sub>, needle<sub>2</sub>, …, needle<sub>n</sub>\]) {#function-multisearchany}

Returns 1, if at least one string needle<sub>i</sub> matches the string `haystack` and 0 otherwise.

For a case-insensitive search or/and in UTF-8 format use functions `multiSearchAnyCaseInsensitive, multiSearchAnyUTF8, multiSearchAnyCaseInsensitiveUTF8`.

!!! note "Note"
    In all `multiSearch*` functions the number of needles should be less than 2<sup>8</sup> because of implementation specification.

## match(haystack, pattern) {#matchhaystack-pattern}

Checks whether the string matches the `pattern` regular expression. A `re2` regular expression. The [syntax](https://github.com/google/re2/wiki/Syntax) of the `re2` regular expressions is more limited than the syntax of the Perl regular expressions.

Returns 0 if it doesn’t match, or 1 if it matches.

Note that the backslash symbol (`\`) is used for escaping in the regular expression. The same symbol is used for escaping in string literals. So in order to escape the symbol in a regular expression, you must write two backslashes (\\) in a string literal.

The regular expression works with the string as if it is a set of bytes. The regular expression can’t contain null bytes.
For patterns to search for substrings in a string, it is better to use LIKE or ‘position’, since they work much faster.

## multiMatchAny(haystack, \[pattern<sub>1</sub>, pattern<sub>2</sub>, …, pattern<sub>n</sub>\]) {#multimatchanyhaystack-pattern1-pattern2-patternn}

The same as `match`, but returns 0 if none of the regular expressions are matched and 1 if any of the patterns matches. It uses [hyperscan](https://github.com/intel/hyperscan) library. For patterns to search substrings in a string, it is better to use `multiSearchAny` since it works much faster.

!!! note "Note"
    The length of any of the `haystack` string must be less than 2<sup>32</sup> bytes otherwise the exception is thrown. This restriction takes place because of hyperscan API.

## multiMatchAnyIndex(haystack, \[pattern<sub>1</sub>, pattern<sub>2</sub>, …, pattern<sub>n</sub>\]) {#multimatchanyindexhaystack-pattern1-pattern2-patternn}

The same as `multiMatchAny`, but returns any index that matches the haystack.

## multiMatchAllIndices(haystack, \[pattern<sub>1</sub>, pattern<sub>2</sub>, …, pattern<sub>n</sub>\]) {#multimatchallindiceshaystack-pattern1-pattern2-patternn}

The same as `multiMatchAny`, but returns the array of all indicies that match the haystack in any order.

## multiFuzzyMatchAny(haystack, distance, \[pattern<sub>1</sub>, pattern<sub>2</sub>, …, pattern<sub>n</sub>\]) {#multifuzzymatchanyhaystack-distance-pattern1-pattern2-patternn}

The same as `multiMatchAny`, but returns 1 if any pattern matches the haystack within a constant [edit distance](https://en.wikipedia.org/wiki/Edit_distance). This function is also in an experimental mode and can be extremely slow. For more information see [hyperscan documentation](https://intel.github.io/hyperscan/dev-reference/compilation.html#approximate-matching).

## multiFuzzyMatchAnyIndex(haystack, distance, \[pattern<sub>1</sub>, pattern<sub>2</sub>, …, pattern<sub>n</sub>\]) {#multifuzzymatchanyindexhaystack-distance-pattern1-pattern2-patternn}

The same as `multiFuzzyMatchAny`, but returns any index that matches the haystack within a constant edit distance.

## multiFuzzyMatchAllIndices(haystack, distance, \[pattern<sub>1</sub>, pattern<sub>2</sub>, …, pattern<sub>n</sub>\]) {#multifuzzymatchallindiceshaystack-distance-pattern1-pattern2-patternn}

The same as `multiFuzzyMatchAny`, but returns the array of all indices in any order that match the haystack within a constant edit distance.

!!! note "Note"
    `multiFuzzyMatch*` functions do not support UTF-8 regular expressions, and such expressions are treated as bytes because of hyperscan restriction.

!!! note "Note"
    To turn off all functions that use hyperscan, use setting `SET allow_hyperscan = 0;`.

## extract(haystack, pattern) {#extracthaystack-pattern}

Extracts a fragment of a string using a regular expression. If ‘haystack’ doesn’t match the ‘pattern’ regex, an empty string is returned. If the regex doesn’t contain subpatterns, it takes the fragment that matches the entire regex. Otherwise, it takes the fragment that matches the first subpattern.

## extractAll(haystack, pattern) {#extractallhaystack-pattern}

Extracts all the fragments of a string using a regular expression. If ‘haystack’ doesn’t match the ‘pattern’ regex, an empty string is returned. Returns an array of strings consisting of all matches to the regex. In general, the behavior is the same as the ‘extract’ function (it takes the first subpattern, or the entire expression if there isn’t a subpattern).

## extractAllGroupsHorizontal {#extractallgroups-horizontal}

Matches all groups of the `haystack` string using the `pattern` regular expression. Returns an array of arrays, where the first array includes all fragments matching the first group, the second array - matching the second group, etc.  

!!! note "Note"
    `extractAllGroupsHorizontal` function is slower than [extractAllGroupsVertical](#extractallgroups-vertical).

**Syntax** 

``` sql
extractAllGroupsHorizontal(haystack, pattern)
```

**Parameters** 

-   `haystack` — Input string. Type: [String](../../sql-reference/data-types/string.md).
-   `pattern` — Regular expression with [re2 syntax](https://github.com/google/re2/wiki/Syntax). Must contain groups, each group enclosed in parentheses. If `pattern` contains no groups, an exception is thrown. Type: [String](../../sql-reference/data-types/string.md). 

**Returned value**

-   Type: [Array](../../sql-reference/data-types/array.md).

If `haystack` doesn’t match the `pattern` regex, an array of empty arrays is returned. 

**Example**

Query:

``` sql
SELECT extractAllGroupsHorizontal('abc=111, def=222, ghi=333', '("[^"]+"|\\w+)=("[^"]+"|\\w+)')
```

Result:

``` text
┌─extractAllGroupsHorizontal('abc=111, def=222, ghi=333', '("[^"]+"|\\w+)=("[^"]+"|\\w+)')─┐
│ [['abc','def','ghi'],['111','222','333']]                                                │
└──────────────────────────────────────────────────────────────────────────────────────────┘
```

**See also**
-   [extractAllGroupsVertical](#extractallgroups-vertical)

## extractAllGroupsVertical {#extractallgroups-vertical}

Matches all groups of the `haystack` string using the `pattern` regular expression. Returns an array of arrays, where each array includes matching fragments from every group. Fragments are grouped in order of appearance in the `haystack`.

**Syntax** 

``` sql
extractAllGroupsVertical(haystack, pattern)
```

**Parameters** 

-   `haystack` — Input string. Type: [String](../../sql-reference/data-types/string.md).
-   `pattern` — Regular expression with [re2 syntax](https://github.com/google/re2/wiki/Syntax). Must contain groups, each group enclosed in parentheses. If `pattern` contains no groups, an exception is thrown. Type: [String](../../sql-reference/data-types/string.md).

**Returned value**

-   Type: [Array](../../sql-reference/data-types/array.md).

If `haystack` doesn’t match the `pattern` regex, an empty array is returned. 

**Example**

Query:

``` sql
SELECT extractAllGroupsVertical('abc=111, def=222, ghi=333', '("[^"]+"|\\w+)=("[^"]+"|\\w+)')
```

Result:

``` text
┌─extractAllGroupsVertical('abc=111, def=222, ghi=333', '("[^"]+"|\\w+)=("[^"]+"|\\w+)')─┐
│ [['abc','111'],['def','222'],['ghi','333']]                                            │
└────────────────────────────────────────────────────────────────────────────────────────┘
```

**See also**
-   [extractAllGroupsHorizontal](#extractallgroups-horizontal)

## like(haystack, pattern), haystack LIKE pattern operator {#function-like}

Checks whether a string matches a simple regular expression.
The regular expression can contain the metasymbols `%` and `_`.

`%` indicates any quantity of any bytes (including zero characters).

`_` indicates any one byte.

Use the backslash (`\`) for escaping metasymbols. See the note on escaping in the description of the ‘match’ function.

For regular expressions like `%needle%`, the code is more optimal and works as fast as the `position` function.
For other regular expressions, the code is the same as for the ‘match’ function.

## notLike(haystack, pattern), haystack NOT LIKE pattern operator {#function-notlike}

The same thing as ‘like’, but negative.

## ilike {#ilike}

Case insensitive variant of [like](https://clickhouse.tech/docs/en/sql-reference/functions/string-search-functions/#function-like) function. You can use `ILIKE` operator instead of the `ilike` function.

**Syntax**

``` sql
ilike(haystack, pattern)
```

**Parameters**

-   `haystack` — Input string. [String](../../sql-reference/syntax.md#syntax-string-literal).
-   `pattern` — If `pattern` doesn't contain percent signs or underscores, then the `pattern` only represents the string itself. An underscore (`_`) in `pattern` stands for (matches) any single character. A percent sign (`%`) matches any sequence of zero or more characters.

Some `pattern` examples:

``` text
'abc' ILIKE 'abc'    true
'abc' ILIKE 'a%'     true
'abc' ILIKE '_b_'    true
'abc' ILIKE 'c'      false
```

**Returned values**

-   True, if the string matches `pattern`.
-   False, if the string doesn't match `pattern`.

**Example**

Input table:

``` text
┌─id─┬─name─────┬─days─┐
│  1 │ January  │   31 │
│  2 │ February │   29 │
│  3 │ March    │   31 │
│  4 │ April    │   30 │
└────┴──────────┴──────┘
```

Query:

``` sql
SELECT * FROM Months WHERE ilike(name, '%j%')
```

Result:

``` text
┌─id─┬─name────┬─days─┐
│  1 │ January │   31 │
└────┴─────────┴──────┘
```

**See Also**

-   [like](https://clickhouse.tech/docs/en/sql-reference/functions/string-search-functions/#function-like) <!--hide-->

## ngramDistance(haystack, needle) {#ngramdistancehaystack-needle}

Calculates the 4-gram distance between `haystack` and `needle`: counts the symmetric difference between two multisets of 4-grams and normalizes it by the sum of their cardinalities. Returns float number from 0 to 1 – the closer to zero, the more strings are similar to each other. If the constant `needle` or `haystack` is more than 32Kb, throws an exception. If some of the non-constant `haystack` or `needle` strings are more than 32Kb, the distance is always one.

For case-insensitive search or/and in UTF-8 format use functions `ngramDistanceCaseInsensitive, ngramDistanceUTF8, ngramDistanceCaseInsensitiveUTF8`.

## ngramSearch(haystack, needle) {#ngramsearchhaystack-needle}

Same as `ngramDistance` but calculates the non-symmetric difference between `needle` and `haystack` – the number of n-grams from needle minus the common number of n-grams normalized by the number of `needle` n-grams. The closer to one, the more likely `needle` is in the `haystack`. Can be useful for fuzzy string search.

For case-insensitive search or/and in UTF-8 format use functions `ngramSearchCaseInsensitive, ngramSearchUTF8, ngramSearchCaseInsensitiveUTF8`.

!!! note "Note"
    For UTF-8 case we use 3-gram distance. All these are not perfectly fair n-gram distances. We use 2-byte hashes to hash n-grams and then calculate the (non-)symmetric difference between these hash tables – collisions may occur. With UTF-8 case-insensitive format we do not use fair `tolower` function – we zero the 5-th bit (starting from zero) of each codepoint byte and first bit of zeroth byte if bytes more than one – this works for Latin and mostly for all Cyrillic letters.

## countSubstrings(haystack, needle) {#countSubstrings}

Count the number of substring occurrences

For a case-insensitive search, use the function `countSubstringsCaseInsensitive` (or `countSubstringsCaseInsensitiveUTF8`).

**Syntax**

``` sql
countSubstrings(haystack, needle[, start_pos])
```

**Parameters**

-   `haystack` — The string to search in. [String](../../sql-reference/syntax.md#syntax-string-literal).
-   `needle` — The substring to search for. [String](../../sql-reference/syntax.md#syntax-string-literal).
-   `start_pos` – Optional parameter, position of the first character in the string to start search. [UInt](../../sql-reference/data-types/int-uint.md)

**Returned values**

-   Number of occurrences.

Type: `Integer`.

**Examples**

Query:

``` sql
SELECT countSubstrings('foobar.com', '.')
```

Result:

``` text
┌─countSubstrings('foobar.com', '.')─┐
│                                  1 │
└────────────────────────────────────┘
```

Query:

``` sql
SELECT countSubstrings('aaaa', 'aa')
```

Result:

``` text
┌─countSubstrings('aaaa', 'aa')─┐
│                             2 │
└───────────────────────────────┘
```

[Original article](https://clickhouse.tech/docs/en/query_language/functions/string_search_functions/) <!--hide-->

## countMatches(haystack, pattern) {#countmatcheshaystack-pattern}

Returns the number of regular expression matches for a `pattern` in a `haystack`.
