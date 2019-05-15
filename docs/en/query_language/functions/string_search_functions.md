# Functions for Searching Strings

The search is case-sensitive by default in all these functions. There are separate variants for case insensitive search.

## position(haystack, needle), locate(haystack, needle)

Search for the substring `needle` in the string `haystack`.
Returns the position (in bytes) of the found substring, starting from 1, or returns 0 if the substring was not found.

For a case-insensitive search, use the function `positionCaseInsensitive`.

## positionUTF8(haystack, needle)

The same as `position`, but the position is returned in Unicode code points. Works under the assumption that the string contains a set of bytes representing a UTF-8 encoded text. If this assumption is not met, it returns some result (it doesn't throw an exception).

For a case-insensitive search, use the function `positionCaseInsensitiveUTF8`.

## multiSearchAllPositions(haystack, [needle<sub>1</sub>, needle<sub>2</sub>, ..., needle<sub>n</sub>])

The same as `position`, but returns `Array` of the `position`s for all needle<sub>i</sub>.

For a case-insensitive search or/and in UTF-8 format use functions `multiSearchAllPositionsCaseInsensitive, multiSearchAllPositionsUTF8, multiSearchAllPositionsCaseInsensitiveUTF8`.

## multiSearchFirstPosition(haystack, [needle<sub>1</sub>, needle<sub>2</sub>, ..., needle<sub>n</sub>])

The same as `position` but returns the leftmost offset of the string `haystack` that is matched to some of the needles.

For a case-insensitive search or/and in UTF-8 format use functions `multiSearchFirstPositionCaseInsensitive, multiSearchFirstPositionUTF8, multiSearchFirstPositionCaseInsensitiveUTF8`.

## multiSearchFirstIndex(haystack, [needle<sub>1</sub>, needle<sub>2</sub>, ..., needle<sub>n</sub>])

Returns the index `i` (starting from 1) of the leftmost found needle<sub>i</sub> in the string `haystack` and 0 otherwise.

For a case-insensitive search or/and in UTF-8 format use functions `multiSearchFirstIndexCaseInsensitive, multiSearchFirstIndexUTF8, multiSearchFirstIndexCaseInsensitiveUTF8`.

## multiSearchAny(haystack, [needle<sub>1</sub>, needle<sub>2</sub>, ..., needle<sub>n</sub>])

Returns 1, if at least one string needle<sub>i</sub> matches the string `haystack` and 0 otherwise.

For a case-insensitive search or/and in UTF-8 format use functions `multiSearchAnyCaseInsensitive, multiSearchAnyUTF8, multiSearchAnyCaseInsensitiveUTF8`.

**Note: in all `multiSearch*` functions the number of needles should be less than 2<sup>8</sup> because of implementation specification.**

## match(haystack, pattern)

Checks whether the string matches the `pattern` regular expression. A `re2` regular expression. The [syntax](https://github.com/google/re2/wiki/Syntax) of the `re2` regular expressions is more limited than the syntax of the Perl regular expressions.

Returns 0 if it doesn't match, or 1 if it matches.

Note that the backslash symbol (`\`) is used for escaping in the regular expression. The same symbol is used for escaping in string literals. So in order to escape the symbol in a regular expression, you must write two backslashes (\\) in a string literal.

The regular expression works with the string as if it is a set of bytes. The regular expression can't contain null bytes.
For patterns to search for substrings in a string, it is better to use LIKE or 'position', since they work much faster.

## multiMatchAny(haystack, [pattern<sub>1</sub>, pattern<sub>2</sub>, ..., pattern<sub>n</sub>])

The same as `match`, but returns 0 if none of the regular expressions are matched and 1 if any of the patterns matches. It uses [hyperscan](https://github.com/intel/hyperscan) library. For patterns to search substrings in a string, it is better to use `multiSearchAny` since it works much faster.

**Note: the length of any of the `haystack` string must be less than 2<sup>32</sup> bytes otherwise the exception is thrown. This restriction takes place because of hyperscan API.**

## multiMatchAnyIndex(haystack, [pattern<sub>1</sub>, pattern<sub>2</sub>, ..., pattern<sub>n</sub>])

The same as `multiMatchAny`, but returns any index that matches the haystack.

## multiFuzzyMatchAny(haystack, distance, [pattern<sub>1</sub>, pattern<sub>2</sub>, ..., pattern<sub>n</sub>])

The same as `multiMatchAny`, but returns 1 if any pattern matches the haystack within a constant [edit distance](https://en.wikipedia.org/wiki/Edit_distance). This function is also in an experimental mode and can be extremely slow. For more information see [hyperscan documentation](https://intel.github.io/hyperscan/dev-reference/compilation.html#approximate-matching).

## multiFuzzyMatchAnyIndex(haystack, distance, [pattern<sub>1</sub>, pattern<sub>2</sub>, ..., pattern<sub>n</sub>])

The same as `multiFuzzyMatchAny`, but returns any index that matches the haystack within a constant edit distance.

**Note: `multiFuzzyMatch*` functions do not support UTF-8 regular expressions, and such expressions are treated as bytes because of hyperscan restriction.**

**Note: to turn off all functions that use hyperscan, use setting `SET allow_hyperscan = 0;`.**

## extract(haystack, pattern)

Extracts a fragment of a string using a regular expression. If 'haystack' doesn't match the 'pattern' regex, an empty string is returned. If the regex doesn't contain subpatterns, it takes the fragment that matches the entire regex. Otherwise, it takes the fragment that matches the first subpattern.

## extractAll(haystack, pattern)

Extracts all the fragments of a string using a regular expression. If 'haystack' doesn't match the 'pattern' regex, an empty string is returned. Returns an array of strings consisting of all matches to the regex. In general, the behavior is the same as the 'extract' function (it takes the first subpattern, or the entire expression if there isn't a subpattern).

## like(haystack, pattern), haystack LIKE pattern operator

Checks whether a string matches a simple regular expression.
The regular expression can contain the metasymbols `%` and `_`.

``% indicates any quantity of any bytes (including zero characters).

`_` indicates any one byte.

Use the backslash (`\`) for escaping metasymbols. See the note on escaping in the description of the 'match' function.

For regular expressions like `%needle%`, the code is more optimal and works as fast as the `position` function.
For other regular expressions, the code is the same as for the 'match' function.

## notLike(haystack, pattern), haystack NOT LIKE pattern operator

The same thing as 'like', but negative.

## ngramDistance(haystack, needle)

Calculates the 4-gram distance between `haystack` and `needle`: counts the symmetric difference between two multisets of 4-grams and normalizes it by the sum of their cardinalities. Returns float number from 0 to 1 -- the closer to zero, the more strings are similar to each other. If the constant `needle` or `haystack` is more than 32Kb, throws an exception. If some of the non-constant `haystack` or `needle` strings are more than 32Kb, the distance is always one.

For case-insensitive search or/and in UTF-8 format use functions `ngramDistanceCaseInsensitive, ngramDistanceUTF8, ngramDistanceCaseInsensitiveUTF8`.

**Note: For UTF-8 case we use 3-gram distance. All these are not perfectly fair n-gram distances. We use 2-byte hashes to hash n-grams and then calculate the symmetric difference between these hash tables -- collisions may occur. With UTF-8 case-insensitive format we do not use fair `tolower` function -- we zero the 5-th bit (starting from zero) of each codepoint byte -- this works for Latin and mostly for all Cyrillic letters.**


[Original article](https://clickhouse.yandex/docs/en/query_language/functions/string_search_functions/) <!--hide-->
