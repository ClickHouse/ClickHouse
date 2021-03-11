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
