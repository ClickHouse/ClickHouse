---
toc_priority: 46
toc_title: Working with maps
---

# Functions for maps {#functions-for-working-with-tuple-maps}

## map {#function-map}

Arranges `key:value` pairs into [Map(key, value)](../../sql-reference/data-types/map.md) data type.

**Syntax** 

``` sql
map(key1, value1[, key2, value2, ...])
```

**Arguments** 

-   `key` — The key part of the pair. [String](../../sql-reference/data-types/string.md) or [Integer](../../sql-reference/data-types/int-uint.md).
-   `value` — The value part of the pair. [String](../../sql-reference/data-types/string.md), [Integer](../../sql-reference/data-types/int-uint.md) or [Array](../../sql-reference/data-types/array.md).

**Returned value**

-  Data structure as `key:value` pairs.

Type: [Map(key, value)](../../sql-reference/data-types/map.md).

**Examples**

Query:

``` sql
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

``` sql
CREATE TABLE table_map (a Map(String, UInt64)) ENGINE = MergeTree() ORDER BY a;
INSERT INTO table_map SELECT map('key1', number, 'key2', number * 2) FROM numbers(3);
SELECT a['key2'] FROM table_map;
```

Result:

``` text
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

``` sql
mapAdd(Tuple(Array, Array), Tuple(Array, Array) [, ...])
```

**Arguments** 

Arguments are [tuples](../../sql-reference/data-types/tuple.md#tuplet1-t2) of two [arrays](../../sql-reference/data-types/array.md#data-type-array), where items in the first array represent keys, and the second array contains values for the each key. All key arrays should have same type, and all value arrays should contain items which are promote to the one type ([Int64](../../sql-reference/data-types/int-uint.md#int-ranges), [UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges) or [Float64](../../sql-reference/data-types/float.md#float32-float64)). The common promoted type is used as a type for the result array.

**Returned value**

-   Returns one [tuple](../../sql-reference/data-types/tuple.md#tuplet1-t2), where the first array contains the sorted keys and the second array contains values.

**Example**

Query:

``` sql
SELECT mapAdd(([toUInt8(1), 2], [1, 1]), ([toUInt8(1), 2], [1, 1])) as res, toTypeName(res) as type;
```

Result:

``` text
┌─res───────────┬─type───────────────────────────────┐
│ ([1,2],[2,2]) │ Tuple(Array(UInt8), Array(UInt64)) │
└───────────────┴────────────────────────────────────┘
```

## mapSubtract {#function-mapsubtract}

Collect all the keys and subtract corresponding values.

**Syntax** 

``` sql
mapSubtract(Tuple(Array, Array), Tuple(Array, Array) [, ...])
```

**Arguments** 

Arguments are [tuples](../../sql-reference/data-types/tuple.md#tuplet1-t2) of two [arrays](../../sql-reference/data-types/array.md#data-type-array), where items in the first array represent keys, and the second array contains values for the each key. All key arrays should have same type, and all value arrays should contain items which are promote to the one type ([Int64](../../sql-reference/data-types/int-uint.md#int-ranges), [UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges) or [Float64](../../sql-reference/data-types/float.md#float32-float64)). The common promoted type is used as a type for the result array.

**Returned value**

-   Returns one [tuple](../../sql-reference/data-types/tuple.md#tuplet1-t2), where the first array contains the sorted keys and the second array contains values.

**Example**

Query:

```sql
SELECT mapSubtract(([toUInt8(1), 2], [toInt32(1), 1]), ([toUInt8(1), 2], [toInt32(2), 1])) as res, toTypeName(res) as type;
```

Result:

```text
┌─res────────────┬─type──────────────────────────────┐
│ ([1,2],[-1,0]) │ Tuple(Array(UInt8), Array(Int64)) │
└────────────────┴───────────────────────────────────┘
```

## mapPopulateSeries {#function-mappopulateseries}

Fills missing keys in the maps (key and value array pair), where keys are integers. Also, it supports specifying the max key, which is used to extend the keys array.

**Syntax** 

``` sql
mapPopulateSeries(keys, values[, max])
```

Generates a map, where keys are a series of numbers, from minimum to maximum keys (or `max` argument if it specified) taken from `keys` array with a step size of one, and corresponding values taken from `values` array. If the value is not specified for the key, then it uses the default value in the resulting map. For repeated keys, only the first value (in order of appearing) gets associated with the key.

The number of elements in `keys` and `values` must be the same for each row.

**Arguments**

-   `keys` — Array of keys. [Array](../../sql-reference/data-types/array.md#data-type-array)([Int](../../sql-reference/data-types/int-uint.md#uint-ranges)).
-   `values` — Array of values. [Array](../../sql-reference/data-types/array.md#data-type-array)([Int](../../sql-reference/data-types/int-uint.md#uint-ranges)).

**Returned value**

-  Returns a [tuple](../../sql-reference/data-types/tuple.md#tuplet1-t2) of two [arrays](../../sql-reference/data-types/array.md#data-type-array): keys in sorted order, and values the corresponding keys.

**Example**

Query:

```sql
select mapPopulateSeries([1,2,4], [11,22,44], 5) as res, toTypeName(res) as type;
```

Result:

```text
┌─res──────────────────────────┬─type──────────────────────────────┐
│ ([1,2,3,4,5],[11,22,0,44,0]) │ Tuple(Array(UInt8), Array(UInt8)) │
└──────────────────────────────┴───────────────────────────────────┘
```

[Original article](https://clickhouse.tech/docs/en/sql-reference/functions/tuple-map-functions/) <!--hide-->
