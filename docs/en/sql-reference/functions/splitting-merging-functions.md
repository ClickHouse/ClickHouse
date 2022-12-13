---
sidebar_position: 47
sidebar_label: Splitting and Merging Strings and Arrays
---

# Functions for Splitting and Merging Strings and Arrays

## splitByChar(separator, s)

Splits a string into substrings separated by a specified character. It uses a constant string `separator` which consisting of exactly one character.
Returns an array of selected substrings. Empty substrings may be selected if the separator occurs at the beginning or end of the string, or if there are multiple consecutive separators.

**Syntax**

``` sql
splitByChar(separator, s)
```

**Arguments**

-   `separator` — The separator which should contain exactly one character. [String](../../sql-reference/data-types/string.md).
-   `s` — The string to split. [String](../../sql-reference/data-types/string.md).

**Returned value(s)**

Returns an array of selected substrings. Empty substrings may be selected when:

-   A separator occurs at the beginning or end of the string;
-   There are multiple consecutive separators;
-   The original string `s` is empty.

Type: [Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md)).

**Example**

``` sql
SELECT splitByChar(',', '1,2,3,abcde');
```

``` text
┌─splitByChar(',', '1,2,3,abcde')─┐
│ ['1','2','3','abcde']           │
└─────────────────────────────────┘
```

## splitByString(separator, s)

Splits a string into substrings separated by a string. It uses a constant string `separator` of multiple characters as the separator. If the string `separator` is empty, it will split the string `s` into an array of single characters.

**Syntax**

``` sql
splitByString(separator, s)
```

**Arguments**

-   `separator` — The separator. [String](../../sql-reference/data-types/string.md).
-   `s` — The string to split. [String](../../sql-reference/data-types/string.md).

**Returned value(s)**

Returns an array of selected substrings. Empty substrings may be selected when:

Type: [Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md)).

-   A non-empty separator occurs at the beginning or end of the string;
-   There are multiple consecutive non-empty separators;
-   The original string `s` is empty while the separator is not empty.

**Example**

``` sql
SELECT splitByString(', ', '1, 2 3, 4,5, abcde');
```

``` text
┌─splitByString(', ', '1, 2 3, 4,5, abcde')─┐
│ ['1','2 3','4,5','abcde']                 │
└───────────────────────────────────────────┘
```

``` sql
SELECT splitByString('', 'abcde');
```

``` text
┌─splitByString('', 'abcde')─┐
│ ['a','b','c','d','e']      │
└────────────────────────────┘
```

## splitByRegexp(regexp, s)

Splits a string into substrings separated by a regular expression. It uses a regular expression string `regexp` as the separator. If the `regexp` is empty, it will split the string `s` into an array of single characters. If no match is found for this regular expression, the string `s` won't be split.

**Syntax**

``` sql
splitByRegexp(regexp, s)
```

**Arguments**

-   `regexp` — Regular expression. Constant. [String](../data-types/string.md) or [FixedString](../data-types/fixedstring.md).
-   `s` — The string to split. [String](../../sql-reference/data-types/string.md).

**Returned value(s)**

Returns an array of selected substrings. Empty substrings may be selected when:

-   A non-empty regular expression match occurs at the beginning or end of the string;
-   There are multiple consecutive non-empty regular expression matches;
-   The original string `s` is empty while the regular expression is not empty.

Type: [Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md)).

**Example**

Query:

``` sql
SELECT splitByRegexp('\\d+', 'a12bc23de345f');
```

Result:

``` text
┌─splitByRegexp('\\d+', 'a12bc23de345f')─┐
│ ['a','bc','de','f']                    │
└────────────────────────────────────────┘
```

Query:

``` sql
SELECT splitByRegexp('', 'abcde');
```

Result:

``` text
┌─splitByRegexp('', 'abcde')─┐
│ ['a','b','c','d','e']      │
└────────────────────────────┘
```

## splitByWhitespace(s)

Splits a string into substrings separated by whitespace characters. 
Returns an array of selected substrings.

**Syntax**

``` sql
splitByWhitespace(s)
```

**Arguments**

-   `s` — The string to split. [String](../../sql-reference/data-types/string.md).

**Returned value(s)**

Returns an array of selected substrings.

Type: [Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md)).

**Example**

``` sql
SELECT splitByWhitespace('  1!  a,  b.  ');
```

``` text
┌─splitByWhitespace('  1!  a,  b.  ')─┐
│ ['1!','a,','b.']                    │
└─────────────────────────────────────┘
```

## splitByNonAlpha(s)

Splits a string into substrings separated by whitespace and punctuation characters. 
Returns an array of selected substrings.

**Syntax**

``` sql
splitByNonAlpha(s)
```

**Arguments**

-   `s` — The string to split. [String](../../sql-reference/data-types/string.md).

**Returned value(s)**

Returns an array of selected substrings.

Type: [Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md)).

**Example**

``` sql
SELECT splitByNonAlpha('  1!  a,  b.  ');
```

``` text
┌─splitByNonAlpha('  1!  a,  b.  ')─┐
│ ['1','a','b']                     │
└───────────────────────────────────┘
```

## arrayStringConcat(arr\[, separator\])

Concatenates string representations of values listed in the array with the separator. `separator` is an optional parameter: a constant string, set to an empty string by default.
Returns the string.

## alphaTokens(s)

Selects substrings of consecutive bytes from the ranges a-z and A-Z.Returns an array of substrings.

**Example**

``` sql
SELECT alphaTokens('abca1abc');
```

``` text
┌─alphaTokens('abca1abc')─┐
│ ['abca','abc']          │
└─────────────────────────┘
```

## extractAllGroups(text, regexp)

Extracts all groups from non-overlapping substrings matched by a regular expression.

**Syntax**

``` sql
extractAllGroups(text, regexp)
```

**Arguments**

-   `text` — [String](../data-types/string.md) or [FixedString](../data-types/fixedstring.md).
-   `regexp` — Regular expression. Constant. [String](../data-types/string.md) or [FixedString](../data-types/fixedstring.md).

**Returned values**

-   If the function finds at least one matching group, it returns `Array(Array(String))` column, clustered by group_id (1 to N, where N is number of capturing groups in `regexp`).

-   If there is no matching group, returns an empty array.

Type: [Array](../data-types/array.md).

**Example**

Query:

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

Splits the UTF-8 string into n-grams of `ngramsize` symbols.

**Syntax** 

``` sql
ngrams(string, ngramsize)
```

**Arguments**

-   `string` — String. [String](../../sql-reference/data-types/string.md) or [FixedString](../../sql-reference/data-types/fixedstring.md).
-   `ngramsize` — The size of an n-gram. [UInt](../../sql-reference/data-types/int-uint.md).

**Returned values**

-   Array with n-grams.

Type: [Array](../../sql-reference/data-types/array.md)([String](../../sql-reference/data-types/string.md)).

**Example**

Query:

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

-   `input_string` — Any set of bytes represented as the [String](../../sql-reference/data-types/string.md) data type object.

**Returned value**

-   The resulting array of tokens from input string.

Type: [Array](../data-types/array.md).

**Example**

Query:

``` sql
SELECT tokens('test1,;\\ test2,;\\ test3,;\\   test4') AS tokens;
```

Result:

``` text
┌─tokens────────────────────────────┐
│ ['test1','test2','test3','test4'] │
└───────────────────────────────────┘
```