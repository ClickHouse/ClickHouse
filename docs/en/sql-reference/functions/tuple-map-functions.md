---
toc_priority: 46
toc_title: Working with maps
---

# Functions for maps {#functions-for-working-with-tuple-maps}

## map {#function-map}

Arranges `key:value` pairs into [Map(key, value)](../../sql-reference/data-types/map.md) data type.

**Syntax**

```sql
map(key1, value1[, key2, value2, ...])
```

**Arguments**

-   `key` — The key part of the pair. [String](../../sql-reference/data-types/string.md) or [Integer](../../sql-reference/data-types/int-uint.md).
-   `value` — The value part of the pair. [String](../../sql-reference/data-types/string.md), [Integer](../../sql-reference/data-types/int-uint.md) or [Array](../../sql-reference/data-types/array.md).

**Returned value**

-   Data structure as `key:value` pairs.

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

-   [Map(key, value)](../../sql-reference/data-types/map.md) data type

## mapAdd {#function-mapadd}

Collect all the keys and sum corresponding values.

**Syntax**

```sql
mapAdd(arg1, arg2 [, ...])
```

**Arguments**

Arguments are [maps](../../sql-reference/data-types/map.md) or [tuples](../../sql-reference/data-types/tuple.md#tuplet1-t2) of two [arrays](../../sql-reference/data-types/array.md#data-type-array), where items in the first array represent keys, and the second array contains values for the each key. All key arrays should have same type, and all value arrays should contain items which are promoted to the one type ([Int64](../../sql-reference/data-types/int-uint.md#int-ranges), [UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges) or [Float64](../../sql-reference/data-types/float.md#float32-float64)). The common promoted type is used as a type for the result array.

**Returned value**

-   Depending on the arguments returns one [map](../../sql-reference/data-types/map.md) or [tuple](../../sql-reference/data-types/tuple.md#tuplet1-t2), where the first array contains the sorted keys and the second array contains values.

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

## mapSubtract {#function-mapsubtract}

Collect all the keys and subtract corresponding values.

**Syntax**

```sql
mapSubtract(Tuple(Array, Array), Tuple(Array, Array) [, ...])
```

**Arguments**

Arguments are [maps](../../sql-reference/data-types/map.md) or [tuples](../../sql-reference/data-types/tuple.md#tuplet1-t2) of two [arrays](../../sql-reference/data-types/array.md#data-type-array), where items in the first array represent keys, and the second array contains values for the each key. All key arrays should have same type, and all value arrays should contain items which are promote to the one type ([Int64](../../sql-reference/data-types/int-uint.md#int-ranges), [UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges) or [Float64](../../sql-reference/data-types/float.md#float32-float64)). The common promoted type is used as a type for the result array.

**Returned value**

-   Depending on the arguments returns one [map](../../sql-reference/data-types/map.md) or [tuple](../../sql-reference/data-types/tuple.md#tuplet1-t2), where the first array contains the sorted keys and the second array contains values.

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

## mapPopulateSeries {#function-mappopulateseries}

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

-   `keys` — Array of keys. [Array](../../sql-reference/data-types/array.md#data-type-array)([Int](../../sql-reference/data-types/int-uint.md#uint-ranges)).
-   `values` — Array of values. [Array](../../sql-reference/data-types/array.md#data-type-array)([Int](../../sql-reference/data-types/int-uint.md#uint-ranges)).
-   `max` — Maximum key value. Optional. [Int8, Int16, Int32, Int64, Int128, Int256](../../sql-reference/data-types/int-uint.md#int-ranges).

or

-   `map` — Map with integer keys. [Map](../../sql-reference/data-types/map.md).

**Returned value**

-   Depending on the arguments returns a [map](../../sql-reference/data-types/map.md) or a [tuple](../../sql-reference/data-types/tuple.md#tuplet1-t2) of two [arrays](../../sql-reference/data-types/array.md#data-type-array): keys in sorted order, and values the corresponding keys.

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

## mapContains {#mapcontains}

Determines  whether the `map` contains the `key` parameter.

**Syntax**

```sql
mapContains(map, key)
```

**Parameters**

-   `map` — Map. [Map](../../sql-reference/data-types/map.md).
-   `key` — Key. Type matches the type of keys of `map` parameter.

**Returned value**

-   `1` if `map` contains `key`, `0` if not.

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

## mapKeys {#mapkeys}

Returns all keys from the `map` parameter.

Can be optimized by enabling the [optimize_functions_to_subcolumns](../../operations/settings/settings.md#optimize-functions-to-subcolumns) setting. With `optimize_functions_to_subcolumns = 1` the function reads only [keys](../../sql-reference/data-types/map.md#map-subcolumns) subcolumn instead of reading and processing the whole column data. The query `SELECT mapKeys(m) FROM table` transforms to `SELECT m.keys FROM table`.

**Syntax**

```sql
mapKeys(map)
```

**Parameters**

-   `map` — Map. [Map](../../sql-reference/data-types/map.md).

**Returned value**

-   Array containing all keys from the `map`.

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

## mapValues {#mapvalues}

Returns all values from the `map` parameter.

Can be optimized by enabling the [optimize_functions_to_subcolumns](../../operations/settings/settings.md#optimize-functions-to-subcolumns) setting. With `optimize_functions_to_subcolumns = 1` the function reads only [values](../../sql-reference/data-types/map.md#map-subcolumns) subcolumn instead of reading and processing the whole column data. The query `SELECT mapValues(m) FROM table` transforms to `SELECT m.values FROM table`.

**Syntax**

```sql
mapKeys(map)
```

**Parameters**

-   `map` — Map. [Map](../../sql-reference/data-types/map.md).

**Returned value**

-   Array containing all the values from `map`.

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
  
## mapContainsKeyLike {#mapContainsKeyLike}  
  
**Syntax**

```sql
mapContainsKeyLike(map, pattern)
```  
  
**Parameters**
  
-   `map` — Map. [Map](../../sql-reference/data-types/map.md).  
-   `pattern`  - String pattern to match.  
  
**Returned value**

-   `1` if `map` contains `key` like specified pattern, `0` if not.  
  
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
  
## mapExtractKeyLike {#mapExtractKeyLike}  
  
**Syntax**

```sql
mapExtractKeyLike(map, pattern)
```  
  
**Parameters**
  
-   `map` — Map. [Map](../../sql-reference/data-types/map.md).  
-   `pattern`  - String pattern to match.  
  
**Returned value**

- A map contained elements the key of which matchs the specified pattern. If there are no elements matched the pattern, it will return an empty map.
  
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
  

[Original article](https://clickhouse.com/docs/en/sql-reference/functions/tuple-map-functions/) <!--hide-->
