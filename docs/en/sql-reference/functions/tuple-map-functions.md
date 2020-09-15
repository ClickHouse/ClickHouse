---
toc_priority: 46
toc_title: Working with maps
---

# Functions for maps {#functions-for-working-with-tuple-maps}

## mapAdd(Tuple(Array, Array), Tuple(Array, Array) [, ...]) {#function-mapadd}

Collect all the keys and sum corresponding values.

Arguments are tuples of two arrays, where items in the first array represent keys, and the second array
contains values for the each key.
All key arrays should have same type, and all value arrays should contain items which are promotable to the one type (Int64, UInt64 or Float64).
The common promoted type is used as a type for the result array.

Returns one tuple, where the first array contains the sorted keys and the second array contains values.

```sql
SELECT mapAdd(([toUInt8(1), 2], [1, 1]), ([toUInt8(1), 2], [1, 1])) as res, toTypeName(res) as type;
```

```text
┌─res───────────┬─type───────────────────────────────┐
│ ([1,2],[2,2]) │ Tuple(Array(UInt8), Array(UInt64)) │
└───────────────┴────────────────────────────────────┘
```

## mapSubtract(Tuple(Array, Array), Tuple(Array, Array) [, ...]) {#function-mapsubtract}

Collect all the keys and subtract corresponding values.

Arguments are tuples of two arrays, where items in the first array represent keys, and the second array
contains values for the each key.
All key arrays should have same type, and all value arrays should contain items which are promotable to the one type (Int64, UInt64 or Float64).
The common promoted type is used as a type for the result array.

Returns one tuple, where the first array contains the sorted keys and the second array contains values.

```sql
SELECT mapSubtract(([toUInt8(1), 2], [toInt32(1), 1]), ([toUInt8(1), 2], [toInt32(2), 1])) as res, toTypeName(res) as type;
```

```text
┌─res────────────┬─type──────────────────────────────┐
│ ([1,2],[-1,0]) │ Tuple(Array(UInt8), Array(Int64)) │
└────────────────┴───────────────────────────────────┘
````

## mapPopulateSeries {#function-mappopulateseries}

Syntax: `mapPopulateSeries((keys : Array(<IntegerType>), values : Array(<IntegerType>)[, max : <IntegerType>])`

Generates a map, where keys are a series of numbers, from minimum to maximum keys (or `max` argument if it specified) taken from `keys` array with step size of one,
and corresponding values taken from `values` array. If the value is not specified for the key, then it uses default value in the resulting map.
For repeated keys only the first value (in order of appearing) gets associated with the key.

The number of elements in `keys` and `values` must be the same for each row.

Returns a tuple of two arrays: keys in sorted order, and values the corresponding keys.

``` sql
select mapPopulateSeries([1,2,4], [11,22,44], 5) as res, toTypeName(res) as type;
```

``` text
┌─res──────────────────────────┬─type──────────────────────────────┐
│ ([1,2,3,4,5],[11,22,0,44,0]) │ Tuple(Array(UInt8), Array(UInt8)) │
└──────────────────────────────┴───────────────────────────────────┘
```
