# Functions for splitting and merging strings and arrays

## splitByChar

Splits a string into substrings separated by a specified character. It uses a constant string `separator` which consisting of exactly one character.

**Syntax**

```sql
splitByChar(<separator>, <s>)
```

**Parameters**

- `separator` — The separator which should contain exactly one character. [String](../../data_types/string.md).
- `s` — The string to split. [String](../../data_types/string.md).

**Returned value(s)**

Returns an array of selected substrings. Empty substrings may be selected when:

* A separator occurs at the beginning or end of the string;
* There are multiple consecutive separators;
* The original string `s` is empty.

Type: [Array](../../data_types/array.md) of [String](../../data_types/string.md).

**Example**

```sql
SELECT splitByChar(',', '1,2,3,abcde')
```
```text
┌─splitByChar(',', '1,2,3,abcde')─┐
│ ['1','2','3','abcde']           │
└─────────────────────────────────┘
```

## splitByString

Splits a string into substrings separated by a string. It uses a constant string `separator` of multiple characters as the separator. If the string `separator` is empty, it will split the string `s` into an array of single characters.

**Syntax**

```sql
splitByString(<separator>, <s>)
```

**Parameters**

- `separator` — The separator. [String](../../data_types/string.md).
- `s` — The string to split. [String](../../data_types/string.md).

**Returned value(s)**

Returns an array of selected substrings. Empty substrings may be selected when:

* A non-empty separator occurs at the beginning or end of the string;
* There are multiple consecutive non-empty separators;
* The original string `s` is empty while the separator is not empty.

Type: [Array](../../data_types/array.md) of [String](../../data_types/string.md).

**Example**

```sql
SELECT splitByString(', ', '1, 2 3, 4,5, abcde')
```
```text
┌─splitByString(', ', '1, 2 3, 4,5, abcde')─┐
│ ['1','2 3','4,5','abcde']                 │
└───────────────────────────────────────────┘
```

```sql
SELECT splitByString('', 'abcde')
```
```text
┌─splitByString('', 'abcde')─┐
│ ['a','b','c','d','e']      │
└────────────────────────────┘
```

## arrayStringConcat(arr\[, separator\])

Concatenates the strings listed in the array with the separator.'separator' is an optional parameter: a constant string, set to an empty string by default.
Returns the string.

## alphaTokens(s)

Selects substrings of consecutive bytes from the ranges a-z and A-Z.Returns an array of substrings.

**Example**

```sql
SELECT alphaTokens('abca1abc')
```
```text
┌─alphaTokens('abca1abc')─┐
│ ['abca','abc']          │
└─────────────────────────┘
```
[Original article](https://clickhouse.tech/docs/en/query_language/functions/splitting_merging_functions/) <!--hide-->
