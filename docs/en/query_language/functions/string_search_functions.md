# Functions for Searching Strings

The search is case-sensitive by default in all these functions. There are separate variants for case insensitive search.

## position(haystack, needle), locate(haystack, needle)

Search for the substring `needle` in the string `haystack`.
Returns the position (in bytes) of the found substring, starting from 1, or returns 0 if the substring was not found.

For a case-insensitive search, use the function `positionCaseInsensitive`.

## positionUTF8(haystack, needle)

The same as `position`, but the position is returned in Unicode code points. Works under the assumption that the string contains a set of bytes representing a UTF-8 encoded text. If this assumption is not met, it returns some result (it doesn't throw an exception).

For a case-insensitive search, use the function `positionCaseInsensitiveUTF8`.

## multiPosition(haystack, [needle_1, needle_2, ..., needle_n])

The same as `position`, but returns `Array` of the `position`s for all `needle_i`.

For a case-insensitive search or/and in UTF-8 format use functions `multiPositionCaseInsensitive, multiPositionUTF8, multiPositionCaseInsensitiveUTF8`.

## firstMatch(haystack, [needle_1, needle_2, ..., needle_n])

Returns the index `i` (starting from 1) of the first found `needle_i` in the string `haystack` and 0 otherwise.

For a case-insensitive search or/and in UTF-8 format use functions `firstMatchCaseInsensitive, firstMatchUTF8, firstMatchCaseInsensitiveUTF8`.

## multiSearch(haystack, [needle_1, needle_2, ..., needle_n])

Returns 1, if at least one string `needle_i` matches the string `haystack` and 0 otherwise.

For a case-insensitive search or/and in UTF-8 format use functions `multiSearchCaseInsensitive, multiSearchUTF8, multiSearchCaseInsensitiveUTF8`.

## match(haystack, pattern)

Checks whether the string matches the `pattern` regular expression. A `re2` regular expression. The [syntax](https://github.com/google/re2/wiki/Syntax) of the `re2` regular expressions is more limited than the syntax of the Perl regular expressions.

Returns 0 if it doesn't match, or 1 if it matches.

Note that the backslash symbol (`\`) is used for escaping in the regular expression. The same symbol is used for escaping in string literals. So in order to escape the symbol in a regular expression, you must write two backslashes (\\) in a string literal.

The regular expression works with the string as if it is a set of bytes. The regular expression can't contain null bytes.
For patterns to search for substrings in a string, it is better to use LIKE or 'position', since they work much faster.

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


[Original article](https://clickhouse.yandex/docs/en/query_language/functions/string_search_functions/) <!--hide-->
