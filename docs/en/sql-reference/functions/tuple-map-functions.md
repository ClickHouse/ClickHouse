---
slug: /en/sql-reference/functions/tuple-map-functions
sidebar_position: 120
sidebar_label: Maps
---

## map

Arranges `key:value` pairs into [Map(key, value)](../../sql-reference/data-types/map.md) data type.

**Syntax**

```sql
map(key1, value1[, key2, value2, ...])
```

**Arguments**

- `key` — The key part of the pair. Arbitrary type, except [Nullable](../../sql-reference/data-types/nullable.md) and [LowCardinality](../../sql-reference/data-types/lowcardinality.md) nested with [Nullable](../../sql-reference/data-types/nullable.md).
- `value` — The value part of the pair. Arbitrary type, including [Map](../../sql-reference/data-types/map.md) and [Array](../../sql-reference/data-types/array.md).

**Returned value**

- Data structure as `key:value` pairs.

Type: [Map(key, value)](../../sql-reference/data-types/map.md).

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

Query:

```sql
CREATE TABLE table_map (a Map(String, UInt64)) ENGINE = MergeTree() ORDER BY a;
INSERT INTO table_map SELECT map('key1', number, 'key2', number * 2) FROM numbers(3);
SELECT a['key2'] FROM table_map;
```

Result:

```text
┌─arrayElement(a, 'key2')─┐
│                       0 │
│                       2 │
│                       4 │
└─────────────────────────┘
```

**See Also**

- [Map(key, value)](../../sql-reference/data-types/map.md) data type

## mapFromArrays

Merges an [Array](../../sql-reference/data-types/array.md) of keys and an [Array](../../sql-reference/data-types/array.md) of values into a [Map(key, value)](../../sql-reference/data-types/map.md). Notice that the second argument could also be a [Map](../../sql-reference/data-types/map.md), thus it is casted to an Array when executing.


The function is a more convenient alternative to `CAST((key_array, value_array_or_map), 'Map(key_type, value_type)')`. For example, instead of writing `CAST((['aa', 'bb'], [4, 5]), 'Map(String, UInt32)')`, you can write `mapFromArrays(['aa', 'bb'], [4, 5])`.


**Syntax**

```sql
mapFromArrays(keys, values)
```

Alias: `MAP_FROM_ARRAYS(keys, values)`

**Arguments**

- `keys` — Given key array to create a map from. The nested type of array must be: [String](../../sql-reference/data-types/string.md), [Integer](../../sql-reference/data-types/int-uint.md), [LowCardinality](../../sql-reference/data-types/lowcardinality.md), [FixedString](../../sql-reference/data-types/fixedstring.md), [UUID](../../sql-reference/data-types/uuid.md), [Date](../../sql-reference/data-types/date.md), [DateTime](../../sql-reference/data-types/datetime.md), [Date32](../../sql-reference/data-types/date32.md), [Enum](../../sql-reference/data-types/enum.md)
- `values`  - Given value array or map to create a map from.

**Returned value**

- A map whose keys and values are constructed from the key array and value array/map.

**Example**

Query:

```sql
select mapFromArrays(['a', 'b', 'c'], [1, 2, 3])


┌─mapFromArrays(['a', 'b', 'c'], [1, 2, 3])─┐
│ {'a':1,'b':2,'c':3}                       │
└───────────────────────────────────────────┘

SELECT mapFromArrays([1, 2, 3], map('a', 1, 'b', 2, 'c', 3))

┌─mapFromArrays([1, 2, 3], map('a', 1, 'b', 2, 'c', 3))─┐
│ {1:('a',1),2:('b',2),3:('c',3)}                       │
└───────────────────────────────────────────────────────┘
```

## extractKeyValuePairs

Extracts key-value pairs, i.e. a [Map(String, String)](../../sql-reference/data-types/map.md), from a string. Parsing is robust towards noise (e.g. log files).

A key-value pair consists of a key, followed by a `key_value_delimiter` and a value. Key value pairs must be separated by `pair_delimiter`. Quoted keys and values are also supported. 

**Syntax**

``` sql
extractKeyValuePairs(data[, key_value_delimiter[, pair_delimiter[, quoting_character]]])
```

Alias:
- `str_to_map`
- `mapFromString`

**Arguments**

- `data` - String to extract key-value pairs from. [String](../../sql-reference/data-types/string.md) or [FixedString](../../sql-reference/data-types/fixedstring.md).
- `key_value_delimiter` - Character to be used as delimiter between the key and the value. Defaults to `:`. [String](../../sql-reference/data-types/string.md) or [FixedString](../../sql-reference/data-types/fixedstring.md).
- `pair_delimiters` - Set of character to be used as delimiters between pairs. Defaults to ` `, `,` and `;`. [String](../../sql-reference/data-types/string.md) or [FixedString](../../sql-reference/data-types/fixedstring.md).
- `quoting_character` - Character to be used as quoting character. Defaults to `"`. [String](../../sql-reference/data-types/string.md) or [FixedString](../../sql-reference/data-types/fixedstring.md).

**Returned values**

- A [Map(String, String)](../../sql-reference/data-types/map.md) of key-value pairs.

**Examples**

Simple case:

``` sql
SELECT extractKeyValuePairs('name:neymar, age:31 team:psg,nationality:brazil') as kv
```

Result:

``` Result:
┌─kv──────────────────────────────────────────────────────────────────────┐
│ {'name':'neymar','age':'31','team':'psg','nationality':'brazil'}        │
└─────────────────────────────────────────────────────────────────────────┘
```

Single quote as quoting character:

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

## extractKeyValuePairsWithEscaping

Same as `extractKeyValuePairs` but with escaping support.

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

Arguments are [maps](../../sql-reference/data-types/map.md) or [tuples](../../sql-reference/data-types/tuple.md#tuplet1-t2) of two [arrays](../../sql-reference/data-types/array.md#data-type-array), where items in the first array represent keys, and the second array contains values for the each key. All key arrays should have same type, and all value arrays should contain items which are promoted to the one type ([Int64](../../sql-reference/data-types/int-uint.md#int-ranges), [UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges) or [Float64](../../sql-reference/data-types/float.md#float32-float64)). The common promoted type is used as a type for the result array.

**Returned value**

- Depending on the arguments returns one [map](../../sql-reference/data-types/map.md) or [tuple](../../sql-reference/data-types/tuple.md#tuplet1-t2), where the first array contains the sorted keys and the second array contains values.

**Example**

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

## mapSubtract

Collect all the keys and subtract corresponding values.

**Syntax**

```sql
mapSubtract(Tuple(Array, Array), Tuple(Array, Array) [, ...])
```

**Arguments**

Arguments are [maps](../../sql-reference/data-types/map.md) or [tuples](../../sql-reference/data-types/tuple.md#tuplet1-t2) of two [arrays](../../sql-reference/data-types/array.md#data-type-array), where items in the first array represent keys, and the second array contains values for the each key. All key arrays should have same type, and all value arrays should contain items which are promote to the one type ([Int64](../../sql-reference/data-types/int-uint.md#int-ranges), [UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges) or [Float64](../../sql-reference/data-types/float.md#float32-float64)). The common promoted type is used as a type for the result array.

**Returned value**

- Depending on the arguments returns one [map](../../sql-reference/data-types/map.md) or [tuple](../../sql-reference/data-types/tuple.md#tuplet1-t2), where the first array contains the sorted keys and the second array contains values.

**Example**

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

## mapPopulateSeries

Fills missing keys in the maps (key and value array pair), where keys are integers. Also, it supports specifying the max key, which is used to extend the keys array.

**Syntax**

```sql
mapPopulateSeries(keys, values[, max])
mapPopulateSeries(map[, max])
```

Generates a map (a tuple with two arrays or a value of `Map` type, depending on the arguments), where keys are a series of numbers, from minimum to maximum keys (or `max` argument if it specified) taken from the map with a step size of one, and corresponding values. If the value is not specified for the key, then it uses the default value in the resulting map. For repeated keys, only the first value (in order of appearing) gets associated with the key.

For array arguments the number of elements in `keys` and `values` must be the same for each row.

**Arguments**

Arguments are [maps](../../sql-reference/data-types/map.md) or two [arrays](../../sql-reference/data-types/array.md#data-type-array), where the first array represent keys, and the second array contains values for the each key.

Mapped arrays:

- `keys` — Array of keys. [Array](../../sql-reference/data-types/array.md#data-type-array)([Int](../../sql-reference/data-types/int-uint.md#uint-ranges)).
- `values` — Array of values. [Array](../../sql-reference/data-types/array.md#data-type-array)([Int](../../sql-reference/data-types/int-uint.md#uint-ranges)).
- `max` — Maximum key value. Optional. [Int8, Int16, Int32, Int64, Int128, Int256](../../sql-reference/data-types/int-uint.md#int-ranges).

or

- `map` — Map with integer keys. [Map](../../sql-reference/data-types/map.md).

**Returned value**

- Depending on the arguments returns a [map](../../sql-reference/data-types/map.md) or a [tuple](../../sql-reference/data-types/tuple.md#tuplet1-t2) of two [arrays](../../sql-reference/data-types/array.md#data-type-array): keys in sorted order, and values the corresponding keys.

**Example**

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

## mapContains

Determines  whether the `map` contains the `key` parameter.

**Syntax**

```sql
mapContains(map, key)
```

**Arguments**

- `map` — Map. [Map](../../sql-reference/data-types/map.md).
- `key` — Key. Type matches the type of keys of `map` parameter.

**Returned value**

- `1` if `map` contains `key`, `0` if not.

Type: [UInt8](../../sql-reference/data-types/int-uint.md).

**Example**

Query:

```sql
CREATE TABLE test (a Map(String,String)) ENGINE = Memory;

INSERT INTO test VALUES ({'name':'eleven','age':'11'}), ({'number':'twelve','position':'6.0'});

SELECT mapContains(a, 'name') FROM test;

```

Result:

```text
┌─mapContains(a, 'name')─┐
│                      1 │
│                      0 │
└────────────────────────┘
```

## mapKeys

Returns all keys from the `map` parameter.

Can be optimized by enabling the [optimize_functions_to_subcolumns](../../operations/settings/settings.md#optimize-functions-to-subcolumns) setting. With `optimize_functions_to_subcolumns = 1` the function reads only [keys](../../sql-reference/data-types/map.md#map-subcolumns) subcolumn instead of reading and processing the whole column data. The query `SELECT mapKeys(m) FROM table` transforms to `SELECT m.keys FROM table`.

**Syntax**

```sql
mapKeys(map)
```

**Arguments**

- `map` — Map. [Map](../../sql-reference/data-types/map.md).

**Returned value**

- Array containing all keys from the `map`.

Type: [Array](../../sql-reference/data-types/array.md).

**Example**

Query:

```sql
CREATE TABLE test (a Map(String,String)) ENGINE = Memory;

INSERT INTO test VALUES ({'name':'eleven','age':'11'}), ({'number':'twelve','position':'6.0'});

SELECT mapKeys(a) FROM test;
```

Result:

```text
┌─mapKeys(a)────────────┐
│ ['name','age']        │
│ ['number','position'] │
└───────────────────────┘
```

## mapValues

Returns all values from the `map` parameter.

Can be optimized by enabling the [optimize_functions_to_subcolumns](../../operations/settings/settings.md#optimize-functions-to-subcolumns) setting. With `optimize_functions_to_subcolumns = 1` the function reads only [values](../../sql-reference/data-types/map.md#map-subcolumns) subcolumn instead of reading and processing the whole column data. The query `SELECT mapValues(m) FROM table` transforms to `SELECT m.values FROM table`.

**Syntax**

```sql
mapValues(map)
```

**Arguments**

- `map` — Map. [Map](../../sql-reference/data-types/map.md).

**Returned value**

- Array containing all the values from `map`.

Type: [Array](../../sql-reference/data-types/array.md).

**Example**

Query:

```sql
CREATE TABLE test (a Map(String,String)) ENGINE = Memory;

INSERT INTO test VALUES ({'name':'eleven','age':'11'}), ({'number':'twelve','position':'6.0'});

SELECT mapValues(a) FROM test;
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
- `map` — Map. [Map](../../sql-reference/data-types/map.md).
- `pattern`  - String pattern to match.

**Returned value**

- `1` if `map` contains `key` like specified pattern, `0` if not.

**Example**

Query:

```sql
CREATE TABLE test (a Map(String,String)) ENGINE = Memory;

INSERT INTO test VALUES ({'abc':'abc','def':'def'}), ({'hij':'hij','klm':'klm'});

SELECT mapContainsKeyLike(a, 'a%') FROM test;
```

Result:

```text
┌─mapContainsKeyLike(a, 'a%')─┐
│                           1 │
│                           0 │
└─────────────────────────────┘
```

## mapExtractKeyLike

**Syntax**

```sql
mapExtractKeyLike(map, pattern)
```

**Arguments**

- `map` — Map. [Map](../../sql-reference/data-types/map.md).
- `pattern`  - String pattern to match.

**Returned value**

- A map contained elements the key of which matches the specified pattern. If there are no elements matched the pattern, it will return an empty map.

**Example**

Query:

```sql
CREATE TABLE test (a Map(String,String)) ENGINE = Memory;

INSERT INTO test VALUES ({'abc':'abc','def':'def'}), ({'hij':'hij','klm':'klm'});

SELECT mapExtractKeyLike(a, 'a%') FROM test;
```

Result:

```text
┌─mapExtractKeyLike(a, 'a%')─┐
│ {'abc':'abc'}              │
│ {}                         │
└────────────────────────────┘
```

## mapApply

**Syntax**

```sql
mapApply(func, map)
```

**Arguments**

- `func`  - [Lambda function](../../sql-reference/functions/index.md#higher-order-functions---operator-and-lambdaparams-expr-function).
- `map` — [Map](../../sql-reference/data-types/map.md).

**Returned value**

- Returns a map obtained from the original map by application of `func(map1[i], …, mapN[i])` for each element.

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

**Syntax**

```sql
mapFilter(func, map)
```

**Arguments**

- `func`  - [Lambda function](../../sql-reference/functions/index.md#higher-order-functions---operator-and-lambdaparams-expr-function).
- `map` — [Map](../../sql-reference/data-types/map.md).

**Returned value**

- Returns a map containing only the elements in `map` for which `func(map1[i], …, mapN[i])` returns something other than 0.


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

- `map1` [Map](../../sql-reference/data-types/map.md).
- `map2` [Map](../../sql-reference/data-types/map.md).

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

**Syntax**

```sql
mapConcat(maps)
```

**Arguments**

-   `maps` – Arbitrary number of arguments of [Map](../../sql-reference/data-types/map.md) type.

**Returned value**

- Returns a map with concatenated maps passed as arguments. If there are same keys in two or more maps, all of them are added to the result map, but only the first one is accessible via operator `[]`

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

Returns 1 if there is at least one key-value pair in `map` for which `func(key, value)` returns something other than 0. Otherwise, it returns 0.

Note that the `mapExists` is a [higher-order function](../../sql-reference/functions/index.md#higher-order-functions). You can pass a lambda function to it as the first argument.

**Example**

Query:

```sql
SELECT mapExists((k, v) -> (v = 1), map('k1', 1, 'k2', 2)) AS res
```

Result:

```text
┌─res─┐
│   1 │
└─────┘
```

## mapAll(\[func,\] map)

Returns 1 if `func(key, value)` returns something other than 0 for all key-value pairs in `map`. Otherwise, it returns 0.

Note that the `mapAll` is a [higher-order function](../../sql-reference/functions/index.md#higher-order-functions). You can pass a lambda function to it as the first argument.

**Example**

Query:

```sql
SELECT mapAll((k, v) -> (v = 1), map('k1', 1, 'k2', 2)) AS res
```

Result:

```text
┌─res─┐
│   0 │
└─────┘
```

## mapSort(\[func,\], map)

Sorts the elements of the `map` in ascending order. If the `func` function is specified, sorting order is determined by the result of the `func` function applied to the keys and values of the map.

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

Sorts the elements of the `map` in descending order. If the `func` function is specified, sorting order is determined by the result of the `func` function applied to the keys and values of the map.


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

For more details see the [reference](../../sql-reference/functions/array-functions.md#array_functions-reverse-sort) for `arrayReverseSort` function.
