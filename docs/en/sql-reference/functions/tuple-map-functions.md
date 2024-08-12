---
slug: /en/sql-reference/functions/tuple-map-functions
sidebar_position: 120
sidebar_label: Maps
---

## map

Creates a value of type [Map(key, value)](../data-types/map.md) from key-value pairs.

**Syntax**

```sql
map(key1, value1[, key2, value2, ...])
```

**Arguments**

- `key_n` — The keys of the map entries. Any type supported as key type of [Map](../data-types/map.md).
- `value_n` — The values of the map entries. Any type supported as value type of [Map](../data-types/map.md).

**Returned value**

- A map containing `key:value` pairs. [Map(key, value)](../data-types/map.md).

**Examples**

Query:

```sql
SELECT map('key1', number, 'key2', number * 2) FROM numbers(3);
```

Result:

``` text
┌─map('key1', number, 'key2', multiply(number, 2))─┐
│ {'key1':0,'key2':0}                              │
│ {'key1':1,'key2':2}                              │
│ {'key1':2,'key2':4}                              │
└──────────────────────────────────────────────────┘
```

## mapFromArrays

Creates a map from an array of keys and an array of values.

The function is a convenient alternative to syntax `CAST([...], 'Map(key_type, value_type)')`.
For example, instead of writing
- `CAST((['aa', 'bb'], [4, 5]), 'Map(String, UInt32)')`, or
- `CAST([('aa',4), ('bb',5)], 'Map(String, UInt32)')`

you can write `mapFromArrays(['aa', 'bb'], [4, 5])`.

**Syntax**

```sql
mapFromArrays(keys, values)
```

Alias: `MAP_FROM_ARRAYS(keys, values)`

**Arguments**

- `keys` —  Array of keys to create the map from. [Array(T)](../data-types/array.md) where `T` can be any type supported by [Map](../data-types/map.md) as key type.
- `values`  - Array or map of values to create the map from. [Array](../data-types/array.md) or [Map](../data-types/map.md).

**Returned value**

- A map with keys and values constructed from the key array and value array/map.

**Example**

Query:

```sql
select mapFromArrays(['a', 'b', 'c'], [1, 2, 3])
```

Result:

```
┌─mapFromArrays(['a', 'b', 'c'], [1, 2, 3])─┐
│ {'a':1,'b':2,'c':3}                       │
└───────────────────────────────────────────┘
```

`mapFromArrays` also accepts arguments of type [Map](../data-types/map.md). These are casted to array of tuples during execution.

```sql
SELECT mapFromArrays([1, 2, 3], map('a', 1, 'b', 2, 'c', 3))
```

Result:

```
┌─mapFromArrays([1, 2, 3], map('a', 1, 'b', 2, 'c', 3))─┐
│ {1:('a',1),2:('b',2),3:('c',3)}                       │
└───────────────────────────────────────────────────────┘
```

## extractKeyValuePairs

Converts a string of key-value pairs to a [Map(String, String)](../data-types/map.md).
Parsing is tolerant towards noise (e.g. log files).
Key-value pairs in the input string consist of a key, followed by a key-value delimiter, and a value.
Key value pairs are separated by a pair delimiter.
Keys and values can be quoted.

**Syntax**

``` sql
extractKeyValuePairs(data[, key_value_delimiter[, pair_delimiter[, quoting_character]]])
```

Alias:
- `str_to_map`
- `mapFromString`

**Arguments**

- `data` - String to extract key-value pairs from. [String](../data-types/string.md) or [FixedString](../data-types/fixedstring.md).
- `key_value_delimiter` - Single character delimiting keys and values. Defaults to `:`. [String](../data-types/string.md) or [FixedString](../data-types/fixedstring.md).
- `pair_delimiters` - Set of character delimiting pairs. Defaults to ` `, `,` and `;`. [String](../data-types/string.md) or [FixedString](../data-types/fixedstring.md).
- `quoting_character` - Single character used as quoting character. Defaults to `"`. [String](../data-types/string.md) or [FixedString](../data-types/fixedstring.md).

**Returned values**

- A of key-value pairs. Type: [Map(String, String)](../data-types/map.md) 

**Examples**

Query

``` sql
SELECT extractKeyValuePairs('name:neymar, age:31 team:psg,nationality:brazil') as kv
```

Result:

``` Result:
┌─kv──────────────────────────────────────────────────────────────────────┐
│ {'name':'neymar','age':'31','team':'psg','nationality':'brazil'}        │
└─────────────────────────────────────────────────────────────────────────┘
```

With a single quote `'` as quoting character:

``` sql
SELECT extractKeyValuePairs('name:\'neymar\';\'age\':31;team:psg;nationality:brazil,last_key:last_value', ':', ';,', '\'') as kv
```

Result:

``` text
┌─kv───────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ {'name':'neymar','age':'31','team':'psg','nationality':'brazil','last_key':'last_value'}                                 │
└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

Escape sequences without escape sequences support:

``` sql
SELECT extractKeyValuePairs('age:a\\x0A\\n\\0') AS kv
```

Result:

``` text
┌─kv─────────────────────┐
│ {'age':'a\\x0A\\n\\0'} │
└────────────────────────┘
```

To restore a map string key-value pairs serialized with `toString`:

```sql
SELECT
    map('John', '33', 'Paula', '31') AS m,
    toString(m) as map_serialized,
    extractKeyValuePairs(map_serialized, ':', ',', '\'') AS map_restored
FORMAT Vertical;
```

Result:

```
Row 1:
──────
m:              {'John':'33','Paula':'31'}
map_serialized: {'John':'33','Paula':'31'}
map_restored:   {'John':'33','Paula':'31'}
```

## extractKeyValuePairsWithEscaping

Same as `extractKeyValuePairs` but supports escaping.

Supported escape sequences: `\x`, `\N`, `\a`, `\b`, `\e`, `\f`, `\n`, `\r`, `\t`, `\v` and `\0`.
Non standard escape sequences are returned as it is (including the backslash) unless they are one of the following:
`\\`, `'`, `"`, `backtick`, `/`, `=` or ASCII control characters (c <= 31).

This function will satisfy the use case where pre-escaping and post-escaping are not suitable. For instance, consider the following
input string: `a: "aaaa\"bbb"`. The expected output is: `a: aaaa\"bbbb`.
- Pre-escaping: Pre-escaping it will output: `a: "aaaa"bbb"` and `extractKeyValuePairs` will then output: `a: aaaa`
- Post-escaping: `extractKeyValuePairs` will output `a: aaaa\` and post-escaping will keep it as it is.

Leading escape sequences will be skipped in keys and will be considered invalid for values.

**Examples**

Escape sequences with escape sequence support turned on:

``` sql
SELECT extractKeyValuePairsWithEscaping('age:a\\x0A\\n\\0') AS kv
```

Result:

``` result
┌─kv────────────────┐
│ {'age':'a\n\n\0'} │
└───────────────────┘
```

## mapAdd

Collect all the keys and sum corresponding values.

**Syntax**

```sql
mapAdd(arg1, arg2 [, ...])
```

**Arguments**

Arguments are [maps](../data-types/map.md) or [tuples](../data-types/tuple.md#tuplet1-t2) of two [arrays](../data-types/array.md#data-type-array), where items in the first array represent keys, and the second array contains values for the each key. All key arrays should have same type, and all value arrays should contain items which are promoted to the one type ([Int64](../data-types/int-uint.md#int-ranges), [UInt64](../data-types/int-uint.md#uint-ranges) or [Float64](../data-types/float.md#float32-float64)). The common promoted type is used as a type for the result array.

**Returned value**

- Depending on the arguments returns one [map](../data-types/map.md) or [tuple](../data-types/tuple.md#tuplet1-t2), where the first array contains the sorted keys and the second array contains values.

**Example**

Query with `Map` type:

```sql
SELECT mapAdd(map(1,1), map(1,1));
```

Result:

```text
┌─mapAdd(map(1, 1), map(1, 1))─┐
│ {1:2}                        │
└──────────────────────────────┘
```

Query with a tuple:

```sql
SELECT mapAdd(([toUInt8(1), 2], [1, 1]), ([toUInt8(1), 2], [1, 1])) as res, toTypeName(res) as type;
```

Result:

```text
┌─res───────────┬─type───────────────────────────────┐
│ ([1,2],[2,2]) │ Tuple(Array(UInt8), Array(UInt64)) │
└───────────────┴────────────────────────────────────┘
```

## mapSubtract

Collect all the keys and subtract corresponding values.

**Syntax**

```sql
mapSubtract(Tuple(Array, Array), Tuple(Array, Array) [, ...])
```

**Arguments**

Arguments are [maps](../data-types/map.md) or [tuples](../data-types/tuple.md#tuplet1-t2) of two [arrays](../data-types/array.md#data-type-array), where items in the first array represent keys, and the second array contains values for the each key. All key arrays should have same type, and all value arrays should contain items which are promote to the one type ([Int64](../data-types/int-uint.md#int-ranges), [UInt64](../data-types/int-uint.md#uint-ranges) or [Float64](../data-types/float.md#float32-float64)). The common promoted type is used as a type for the result array.

**Returned value**

- Depending on the arguments returns one [map](../data-types/map.md) or [tuple](../data-types/tuple.md#tuplet1-t2), where the first array contains the sorted keys and the second array contains values.

**Example**

Query with `Map` type:

```sql
SELECT mapSubtract(map(1,1), map(1,1));
```

Result:

```text
┌─mapSubtract(map(1, 1), map(1, 1))─┐
│ {1:0}                             │
└───────────────────────────────────┘
```

Query with a tuple map:

```sql
SELECT mapSubtract(([toUInt8(1), 2], [toInt32(1), 1]), ([toUInt8(1), 2], [toInt32(2), 1])) as res, toTypeName(res) as type;
```

Result:

```text
┌─res────────────┬─type──────────────────────────────┐
│ ([1,2],[-1,0]) │ Tuple(Array(UInt8), Array(Int64)) │
└────────────────┴───────────────────────────────────┘
```

## mapPopulateSeries

Fills missing key-value pairs in a map with integer keys.
To support extending the keys beyond the largest value, a maximum key can be specified.
More specifically, the function returns a map in which the the keys form a series from the smallest to the largest key (or `max` argument if it specified) with step size of 1, and corresponding values.
If no value is specified for a key, a default value is used as value.
In case keys repeat, only the first value (in order of appearance) is associated with the key.

**Syntax**

```sql
mapPopulateSeries(map[, max])
mapPopulateSeries(keys, values[, max])
```

For array arguments the number of elements in `keys` and `values` must be the same for each row.

**Arguments**

Arguments are [Maps](../data-types/map.md) or two [Arrays](../data-types/array.md#data-type-array), where the first and second array contains keys and values for the each key.

Mapped arrays:

- `map` — Map with integer keys. [Map](../data-types/map.md).

or

- `keys` — Array of keys. [Array](../data-types/array.md#data-type-array)([Int](../data-types/int-uint.md#uint-ranges)).
- `values` — Array of values. [Array](../data-types/array.md#data-type-array)([Int](../data-types/int-uint.md#uint-ranges)).
- `max` — Maximum key value. Optional. [Int8, Int16, Int32, Int64, Int128, Int256](../data-types/int-uint.md#int-ranges).

**Returned value**

- Depending on the arguments a [Map](../data-types/map.md) or a [Tuple](../data-types/tuple.md#tuplet1-t2) of two [Arrays](../data-types/array.md#data-type-array): keys in sorted order, and values the corresponding keys.

**Example**

Query with `Map` type:

```sql
SELECT mapPopulateSeries(map(1, 10, 5, 20), 6);
```

Result:

```text
┌─mapPopulateSeries(map(1, 10, 5, 20), 6)─┐
│ {1:10,2:0,3:0,4:0,5:20,6:0}             │
└─────────────────────────────────────────┘
```

Query with mapped arrays:

```sql
SELECT mapPopulateSeries([1,2,4], [11,22,44], 5) AS res, toTypeName(res) AS type;
```

Result:

```text
┌─res──────────────────────────┬─type──────────────────────────────┐
│ ([1,2,3,4,5],[11,22,0,44,0]) │ Tuple(Array(UInt8), Array(UInt8)) │
└──────────────────────────────┴───────────────────────────────────┘
```

## mapContains

Returns if a given key is contained in a given map.

**Syntax**

```sql
mapContains(map, key)
```

**Arguments**

- `map` — Map. [Map](../data-types/map.md).
- `key` — Key. Type must match the key type of `map`.

**Returned value**

- `1` if `map` contains `key`, `0` if not. [UInt8](../data-types/int-uint.md).

**Example**

Query:

```sql
CREATE TABLE tab (a Map(String, String)) ENGINE = Memory;

INSERT INTO tab VALUES ({'name':'eleven','age':'11'}), ({'number':'twelve','position':'6.0'});

SELECT mapContains(a, 'name') FROM tab;

```

Result:

```text
┌─mapContains(a, 'name')─┐
│                      1 │
│                      0 │
└────────────────────────┘
```

## mapKeys

Returns the keys of a given map.

This function can be optimized by enabling setting [optimize_functions_to_subcolumns](../../operations/settings/settings.md#optimize-functions-to-subcolumns).
With enabled setting, the function only reads the [keys](../data-types/map.md#map-subcolumns) subcolumn instead the whole map.
The query `SELECT mapKeys(m) FROM table` is transformed to `SELECT m.keys FROM table`.

**Syntax**

```sql
mapKeys(map)
```

**Arguments**

- `map` — Map. [Map](../data-types/map.md).

**Returned value**

- Array containing all keys from the `map`. [Array](../data-types/array.md).

**Example**

Query:

```sql
CREATE TABLE tab (a Map(String, String)) ENGINE = Memory;

INSERT INTO tab VALUES ({'name':'eleven','age':'11'}), ({'number':'twelve','position':'6.0'});

SELECT mapKeys(a) FROM tab;
```

Result:

```text
┌─mapKeys(a)────────────┐
│ ['name','age']        │
│ ['number','position'] │
└───────────────────────┘
```

## mapValues

Returns the values of a given map.

This function can be optimized by enabling setting [optimize_functions_to_subcolumns](../../operations/settings/settings.md#optimize-functions-to-subcolumns).
With enabled setting, the function only reads the [values](../data-types/map.md#map-subcolumns) subcolumn instead the whole map.
The query `SELECT mapValues(m) FROM table` is transformed to `SELECT m.values FROM table`.

**Syntax**

```sql
mapValues(map)
```

**Arguments**

- `map` — Map. [Map](../data-types/map.md).

**Returned value**

- Array containing all the values from `map`. [Array](../data-types/array.md).

**Example**

Query:

```sql
CREATE TABLE tab (a Map(String, String)) ENGINE = Memory;

INSERT INTO tab VALUES ({'name':'eleven','age':'11'}), ({'number':'twelve','position':'6.0'});

SELECT mapValues(a) FROM tab;
```

Result:

```text
┌─mapValues(a)─────┐
│ ['eleven','11']  │
│ ['twelve','6.0'] │
└──────────────────┘
```

## mapContainsKeyLike

**Syntax**

```sql
mapContainsKeyLike(map, pattern)
```

**Arguments**
- `map` — Map. [Map](../data-types/map.md).
- `pattern`  - String pattern to match.

**Returned value**

- `1` if `map` contains `key` like specified pattern, `0` if not.

**Example**

Query:

```sql
CREATE TABLE tab (a Map(String, String)) ENGINE = Memory;

INSERT INTO tab VALUES ({'abc':'abc','def':'def'}), ({'hij':'hij','klm':'klm'});

SELECT mapContainsKeyLike(a, 'a%') FROM tab;
```

Result:

```text
┌─mapContainsKeyLike(a, 'a%')─┐
│                           1 │
│                           0 │
└─────────────────────────────┘
```

## mapExtractKeyLike

Give a map with string keys and a LIKE pattern, this function returns a map with elements where the key matches the pattern.

**Syntax**

```sql
mapExtractKeyLike(map, pattern)
```

**Arguments**

- `map` — Map. [Map](../data-types/map.md).
- `pattern`  - String pattern to match.

**Returned value**

- A map containing elements the key matching the specified pattern. If no elements match the pattern, an empty map is returned.

**Example**

Query:

```sql
CREATE TABLE tab (a Map(String, String)) ENGINE = Memory;

INSERT INTO tab VALUES ({'abc':'abc','def':'def'}), ({'hij':'hij','klm':'klm'});

SELECT mapExtractKeyLike(a, 'a%') FROM tab;
```

Result:

```text
┌─mapExtractKeyLike(a, 'a%')─┐
│ {'abc':'abc'}              │
│ {}                         │
└────────────────────────────┘
```

## mapApply

Applies a function to each element of a map.

**Syntax**

```sql
mapApply(func, map)
```

**Arguments**

- `func`  - [Lambda function](../../sql-reference/functions/index.md#higher-order-functions---operator-and-lambdaparams-expr-function).
- `map` — [Map](../data-types/map.md).

**Returned value**

- Returns a map obtained from the original map by application of `func(map1[i], ..., mapN[i])` for each element.

**Example**

Query:

```sql
SELECT mapApply((k, v) -> (k, v * 10), _map) AS r
FROM
(
    SELECT map('key1', number, 'key2', number * 2) AS _map
    FROM numbers(3)
)
```

Result:

```text
┌─r─────────────────────┐
│ {'key1':0,'key2':0}   │
│ {'key1':10,'key2':20} │
│ {'key1':20,'key2':40} │
└───────────────────────┘
```

## mapFilter

Filters a map by applying a function to each map element.

**Syntax**

```sql
mapFilter(func, map)
```

**Arguments**

- `func`  - [Lambda function](../../sql-reference/functions/index.md#higher-order-functions---operator-and-lambdaparams-expr-function).
- `map` — [Map](../data-types/map.md).

**Returned value**

- Returns a map containing only the elements in `map` for which `func(map1[i], ..., mapN[i])` returns something other than 0.

**Example**

Query:

```sql
SELECT mapFilter((k, v) -> ((v % 2) = 0), _map) AS r
FROM
(
    SELECT map('key1', number, 'key2', number * 2) AS _map
    FROM numbers(3)
)
```

Result:

```text
┌─r───────────────────┐
│ {'key1':0,'key2':0} │
│ {'key2':2}          │
│ {'key1':2,'key2':4} │
└─────────────────────┘
```

## mapUpdate

**Syntax**

```sql
mapUpdate(map1, map2)
```

**Arguments**

- `map1` [Map](../data-types/map.md).
- `map2` [Map](../data-types/map.md).

**Returned value**

- Returns a map1 with values updated of values for the corresponding keys in map2.

**Example**

Query:

```sql
SELECT mapUpdate(map('key1', 0, 'key3', 0), map('key1', 10, 'key2', 10)) AS map;
```

Result:

```text
┌─map────────────────────────────┐
│ {'key3':0,'key1':10,'key2':10} │
└────────────────────────────────┘
```

## mapConcat

Concatenates multiple maps based on the equality of their keys.
If elements with the same key exist in more than one input map, all elements are added to the result map, but only the first one is accessible via operator `[]`

**Syntax**

```sql
mapConcat(maps)
```

**Arguments**

-   `maps` – Arbitrarily many [Maps](../data-types/map.md).

**Returned value**

- Returns a map with concatenated maps passed as arguments.

**Examples**

Query:

```sql
SELECT mapConcat(map('key1', 1, 'key3', 3), map('key2', 2)) AS map;
```

Result:

```text
┌─map──────────────────────────┐
│ {'key1':1,'key3':3,'key2':2} │
└──────────────────────────────┘
```

Query:

```sql
SELECT mapConcat(map('key1', 1, 'key2', 2), map('key1', 3)) AS map, map['key1'];
```

Result:

```text
┌─map──────────────────────────┬─elem─┐
│ {'key1':1,'key2':2,'key1':3} │    1 │
└──────────────────────────────┴──────┘
```

## mapExists(\[func,\], map)

Returns 1 if at least one key-value pair in `map` exists for which `func(key, value)` returns something other than 0. Otherwise, it returns 0.

:::note
`mapExists` is a [higher-order function](../../sql-reference/functions/index.md#higher-order-functions).
You can pass a lambda function to it as the first argument.
:::

**Example**

Query:

```sql
SELECT mapExists((k, v) -> (v = 1), map('k1', 1, 'k2', 2)) AS res
```

Result:

```
┌─res─┐
│   1 │
└─────┘
```

## mapAll(\[func,\] map)

Returns 1 if `func(key, value)` returns something other than 0 for all key-value pairs in `map`. Otherwise, it returns 0.

:::note
Note that the `mapAll` is a [higher-order function](../../sql-reference/functions/index.md#higher-order-functions).
You can pass a lambda function to it as the first argument.
:::

**Example**

Query:

```sql
SELECT mapAll((k, v) -> (v = 1), map('k1', 1, 'k2', 2)) AS res
```

Result:

```
┌─res─┐
│   0 │
└─────┘
```

## mapSort(\[func,\], map)

Sorts the elements of a map in ascending order.
If the `func` function is specified, the sorting order is determined by the result of the `func` function applied to the keys and values of the map.

**Examples**

``` sql
SELECT mapSort(map('key2', 2, 'key3', 1, 'key1', 3)) AS map;
```

``` text
┌─map──────────────────────────┐
│ {'key1':3,'key2':2,'key3':1} │
└──────────────────────────────┘
```

``` sql
SELECT mapSort((k, v) -> v, map('key2', 2, 'key3', 1, 'key1', 3)) AS map;
```

``` text
┌─map──────────────────────────┐
│ {'key3':1,'key2':2,'key1':3} │
└──────────────────────────────┘
```

For more details see the [reference](../../sql-reference/functions/array-functions.md#array_functions-sort) for `arraySort` function.

## mapReverseSort(\[func,\], map)

Sorts the elements of a map in descending order.
If the `func` function is specified, the sorting order is determined by the result of the `func` function applied to the keys and values of the map.

**Examples**

``` sql
SELECT mapReverseSort(map('key2', 2, 'key3', 1, 'key1', 3)) AS map;
```

``` text
┌─map──────────────────────────┐
│ {'key3':1,'key2':2,'key1':3} │
└──────────────────────────────┘
```

``` sql
SELECT mapReverseSort((k, v) -> v, map('key2', 2, 'key3', 1, 'key1', 3)) AS map;
```

``` text
┌─map──────────────────────────┐
│ {'key1':3,'key2':2,'key3':1} │
└──────────────────────────────┘
```

For more details see function [arrayReverseSort](../../sql-reference/functions/array-functions.md#array_functions-reverse-sort).
