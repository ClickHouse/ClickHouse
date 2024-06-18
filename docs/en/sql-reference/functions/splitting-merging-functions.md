---
slug: /en/sql-reference/functions/splitting-merging-functions
sidebar_position: 165
sidebar_label: Splitting Strings
---

# Functions for Splitting Strings

## splitByChar

Splits a string into substrings separated by a specified character. Uses a constant string `separator` which consists of exactly one character.
Returns an array of selected substrings. Empty substrings may be selected if the separator occurs at the beginning or end of the string, or if there are multiple consecutive separators.

**Syntax**

``` sql
splitByChar(separator, s[, max_substrings]))
```

**Arguments**

- `separator` — The separator which should contain exactly one character. [String](../data-types/string.md).
- `s` — The string to split. [String](../data-types/string.md).
- `max_substrings` — An optional `Int64` defaulting to 0. If `max_substrings` > 0, the returned array will contain at most `max_substrings` substrings, otherwise the function will return as many substrings as possible.

**Returned value(s)**

- An array of selected substrings. [Array](../data-types/array.md)([String](../data-types/string.md)).

 Empty substrings may be selected when:

- A separator occurs at the beginning or end of the string;
- There are multiple consecutive separators;
- The original string `s` is empty.

:::note
The behavior of parameter `max_substrings` changed starting with ClickHouse v22.11. In versions older than that, `max_substrings > 0` meant that `max_substring`-many splits were performed and that the remainder of the string was returned as the final element of the list.
For example,
- in v22.10: `SELECT splitByChar('=', 'a=b=c=d', 2);` returned `['a','b','c=d']`
- in v22.11: `SELECT splitByChar('=', 'a=b=c=d', 2);` returned `['a','b']`

A behavior similar to ClickHouse pre-v22.11 can be achieved by setting
[splitby_max_substrings_includes_remaining_string](../../operations/settings/settings.md#splitby_max_substrings_includes_remaining_string)
`SELECT splitByChar('=', 'a=b=c=d', 2) SETTINGS splitby_max_substrings_includes_remaining_string = 1 -- ['a', 'b=c=d']`
:::

**Example**

``` sql
SELECT splitByChar(',', '1,2,3,abcde');
```

Result:

``` text
┌─splitByChar(',', '1,2,3,abcde')─┐
│ ['1','2','3','abcde']           │
└─────────────────────────────────┘
```

## splitByString

Splits a string into substrings separated by a string. It uses a constant string `separator` of multiple characters as the separator. If the string `separator` is empty, it will split the string `s` into an array of single characters.

**Syntax**

``` sql
splitByString(separator, s[, max_substrings]))
```

**Arguments**

- `separator` — The separator. [String](../data-types/string.md).
- `s` — The string to split. [String](../data-types/string.md).
- `max_substrings` — An optional `Int64` defaulting to 0. When `max_substrings` > 0, the returned substrings will be no more than `max_substrings`, otherwise the function will return as many substrings as possible.

**Returned value(s)**

- An array of selected substrings. [Array](../data-types/array.md)([String](../data-types/string.md)).

Empty substrings may be selected when:

- A non-empty separator occurs at the beginning or end of the string;
- There are multiple consecutive non-empty separators;
- The original string `s` is empty while the separator is not empty.

:::note
Setting [splitby_max_substrings_includes_remaining_string](../../operations/settings/settings.md#splitby_max_substrings_includes_remaining_string) (default: 0) controls if the remaining string is included in the last element of the result array when argument `max_substrings` > 0.
:::

**Example**

``` sql
SELECT splitByString(', ', '1, 2 3, 4,5, abcde');
```

Result:

``` text
┌─splitByString(', ', '1, 2 3, 4,5, abcde')─┐
│ ['1','2 3','4,5','abcde']                 │
└───────────────────────────────────────────┘
```

``` sql
SELECT splitByString('', 'abcde');
```

Result:

``` text
┌─splitByString('', 'abcde')─┐
│ ['a','b','c','d','e']      │
└────────────────────────────┘
```

## splitByRegexp

Splits a string into substrings separated by a regular expression. It uses a regular expression string `regexp` as the separator. If the `regexp` is empty, it will split the string `s` into an array of single characters. If no match is found for this regular expression, the string `s` won't be split.

**Syntax**

``` sql
splitByRegexp(regexp, s[, max_substrings]))
```

**Arguments**

- `regexp` — Regular expression. Constant. [String](../data-types/string.md) or [FixedString](../data-types/fixedstring.md).
- `s` — The string to split. [String](../data-types/string.md).
- `max_substrings` — An optional `Int64` defaulting to 0. When `max_substrings` > 0, the returned substrings will be no more than `max_substrings`, otherwise the function will return as many substrings as possible.


**Returned value(s)**

- An array of selected substrings. [Array](../data-types/array.md)([String](../data-types/string.md)).


Empty substrings may be selected when:

- A non-empty regular expression match occurs at the beginning or end of the string;
- There are multiple consecutive non-empty regular expression matches;
- The original string `s` is empty while the regular expression is not empty.

:::note
Setting [splitby_max_substrings_includes_remaining_string](../../operations/settings/settings.md#splitby_max_substrings_includes_remaining_string) (default: 0) controls if the remaining string is included in the last element of the result array when argument `max_substrings` > 0.
:::

**Example**

``` sql
SELECT splitByRegexp('\\d+', 'a12bc23de345f');
```

Result:

``` text
┌─splitByRegexp('\\d+', 'a12bc23de345f')─┐
│ ['a','bc','de','f']                    │
└────────────────────────────────────────┘
```

``` sql
SELECT splitByRegexp('', 'abcde');
```

Result:

``` text
┌─splitByRegexp('', 'abcde')─┐
│ ['a','b','c','d','e']      │
└────────────────────────────┘
```

## splitByWhitespace

Splits a string into substrings separated by whitespace characters. 
Returns an array of selected substrings.

**Syntax**

``` sql
splitByWhitespace(s[, max_substrings]))
```

**Arguments**

- `s` — The string to split. [String](../data-types/string.md).
- `max_substrings` — An optional `Int64` defaulting to 0. When `max_substrings` > 0, the returned substrings will be no more than `max_substrings`, otherwise the function will return as many substrings as possible.


**Returned value(s)**

- An array of selected substrings. [Array](../data-types/array.md)([String](../data-types/string.md)).
 
:::note
Setting [splitby_max_substrings_includes_remaining_string](../../operations/settings/settings.md#splitby_max_substrings_includes_remaining_string) (default: 0) controls if the remaining string is included in the last element of the result array when argument `max_substrings` > 0.
:::

**Example**

``` sql
SELECT splitByWhitespace('  1!  a,  b.  ');
```

Result:

``` text
┌─splitByWhitespace('  1!  a,  b.  ')─┐
│ ['1!','a,','b.']                    │
└─────────────────────────────────────┘
```

## splitByNonAlpha

Splits a string into substrings separated by whitespace and punctuation characters. 
Returns an array of selected substrings.

**Syntax**

``` sql
splitByNonAlpha(s[, max_substrings]))
```

**Arguments**

- `s` — The string to split. [String](../data-types/string.md).
- `max_substrings` — An optional `Int64` defaulting to 0. When `max_substrings` > 0, the returned substrings will be no more than `max_substrings`, otherwise the function will return as many substrings as possible.


**Returned value(s)**

- An array of selected substrings. [Array](../data-types/array.md)([String](../data-types/string.md)).

:::note
Setting [splitby_max_substrings_includes_remaining_string](../../operations/settings/settings.md#splitby_max_substrings_includes_remaining_string) (default: 0) controls if the remaining string is included in the last element of the result array when argument `max_substrings` > 0.
:::

**Example**

``` sql
SELECT splitByNonAlpha('  1!  a,  b.  ');
```

``` text
┌─splitByNonAlpha('  1!  a,  b.  ')─┐
│ ['1','a','b']                     │
└───────────────────────────────────┘
```

## arrayStringConcat

Concatenates string representations of values listed in the array with the separator. `separator` is an optional parameter: a constant string, set to an empty string by default.
Returns the string.

**Syntax**

```sql
arrayStringConcat(arr\[, separator\])
```

**Example**

``` sql
SELECT arrayStringConcat(['12/05/2021', '12:50:00'], ' ') AS DateString;
```

Result:

```text
┌─DateString──────────┐
│ 12/05/2021 12:50:00 │
└─────────────────────┘
```

## alphaTokens

Selects substrings of consecutive bytes from the ranges a-z and A-Z.Returns an array of substrings.

**Syntax**

``` sql
alphaTokens(s[, max_substrings]))
```

Alias: `splitByAlpha`

**Arguments**

- `s` — The string to split. [String](../data-types/string.md).
- `max_substrings` — An optional `Int64` defaulting to 0. When `max_substrings` > 0, the returned substrings will be no more than `max_substrings`, otherwise the function will return as many substrings as possible.

**Returned value(s)**

- An array of selected substrings. [Array](../data-types/array.md)([String](../data-types/string.md)).

:::note
Setting [splitby_max_substrings_includes_remaining_string](../../operations/settings/settings.md#splitby_max_substrings_includes_remaining_string) (default: 0) controls if the remaining string is included in the last element of the result array when argument `max_substrings` > 0.
:::

**Example**

``` sql
SELECT alphaTokens('abca1abc');
```

``` text
┌─alphaTokens('abca1abc')─┐
│ ['abca','abc']          │
└─────────────────────────┘
```

## extractAllGroups

Extracts all groups from non-overlapping substrings matched by a regular expression.

**Syntax**

``` sql
extractAllGroups(text, regexp)
```

**Arguments**

- `text` — [String](../data-types/string.md) or [FixedString](../data-types/fixedstring.md).
- `regexp` — Regular expression. Constant. [String](../data-types/string.md) or [FixedString](../data-types/fixedstring.md).

**Returned values**

- If the function finds at least one matching group, it returns `Array(Array(String))` column, clustered by group_id (1 to N, where N is number of capturing groups in `regexp`). If there is no matching group, it returns an empty array. [Array](../data-types/array.md).

**Example**

``` sql
SELECT extractAllGroups('abc=123, 8="hkl"', '("[^"]+"|\\w+)=("[^"]+"|\\w+)');
```

Result:

``` text
┌─extractAllGroups('abc=123, 8="hkl"', '("[^"]+"|\\w+)=("[^"]+"|\\w+)')─┐
│ [['abc','123'],['8','"hkl"']]                                         │
└───────────────────────────────────────────────────────────────────────┘
```

## ngrams

Splits a UTF-8 string into n-grams of `ngramsize` symbols.

**Syntax** 

``` sql
ngrams(string, ngramsize)
```

**Arguments**

- `string` — String. [String](../data-types/string.md) or [FixedString](../data-types/fixedstring.md).
- `ngramsize` — The size of an n-gram. [UInt](../data-types/int-uint.md).

**Returned values**

- Array with n-grams. [Array](../data-types/array.md)([String](../data-types/string.md)).

**Example**

``` sql
SELECT ngrams('ClickHouse', 3);
```

Result:

``` text
┌─ngrams('ClickHouse', 3)───────────────────────────┐
│ ['Cli','lic','ick','ckH','kHo','Hou','ous','use'] │
└───────────────────────────────────────────────────┘
```

## tokens

Splits a string into tokens using non-alphanumeric ASCII characters as separators.

**Arguments**

- `input_string` — Any set of bytes represented as the [String](../data-types/string.md) data type object.

**Returned value**

- The resulting array of tokens from input string. [Array](../data-types/array.md).

**Example**

``` sql
SELECT tokens('test1,;\\ test2,;\\ test3,;\\   test4') AS tokens;
```

Result:

``` text
┌─tokens────────────────────────────┐
│ ['test1','test2','test3','test4'] │
└───────────────────────────────────┘
```
