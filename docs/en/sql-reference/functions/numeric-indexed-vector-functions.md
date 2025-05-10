---
description: 'Documentation for NumericIndexedVector and Its Functions'
sidebar_label: 'NumericIndexedVector'
sidebar_position: 26
slug: /sql-reference/functions/numeric-indexed-vector-functions
title: 'NumericIndexedVector Functions'
---

# NumericIndexedVector

NumericIndexedVector is an abstract data structure that encapsulates a vector and implements vector aggregating and point-wise operations. There are two underlying storage methods: BSI and RawSum (not yet implemented). For theoretical basis and usage scenarios, refer to the paper [Large-Scale Metric Computation in Online Controlled Experiment Platform](https://arxiv.org/pdf/2405.08411).

## BSI {#bsi}

In the BSI(Bit-Sliced Index) storage mode, NumericIndexedVector compresses and stores vectors based on Roaring Bitmap and Bit-Sliced Index. Aggregating operations and point-wise operations are directly on the compressed data, which can significantly improve the efficiency of storage and query.

A vector contains indices and their corresponding element values. The following are some characteristics and constraints of this data structure in BSI storage mode:

- The Index type can be one of `UInt8`, `UInt16`, or `UInt32`. **Note:** The index only supports sizes up to `UInt32`; `UInt64` and `Int64` are supported in RawSum type.
- The Value type can be one of `Int8`, `Int16`, `Int32`, `Int64`, `UInt8`, `UInt16`, `UInt32`, `UInt64`, `Float32`, or `Float64`. **Note:** The Value type does not automatically expand. For example, if you use `UInt8` as the Value type, any sum that exceeds the capacity of `UInt8` will result in an overflow rather than being promoted to a higher type; similarly, operations on integers will yield integer results (e.g., division will not automatically convert to a floating-point result). Therefore, it is important to plan and design the Value type ahead of time. In real-world scenarios, floating-point types (`Float32`/`Float64`) are commonly used.
- The underlying storage uses Bit-Sliced Index. with bitmap storing the Indexes. RoaringBitmap is used as the specific implementation of bitmap. A best practice is to concentrate the index in several Roaring Bitmap containers as much as possible to maximize compression and query performance.
- The Bit-Sliced Index mechanism converts Value into binary. For floating-point types, the conversion uses fixed-point representation, which may lead to precision loss. The precision can be adjusted by customizing the number of bits allocated for the fractional part; the default is 24 bits, which is sufficient for most scenarios. You can customize the number of integer bits and fractional bits when constructing NumericIndexedVector using aggregate function groupNumericIndexedVector with `-State`.
- In point-wise operations between two NumericIndexedVectors, any index that is missing in one of the vectors is treated as 0 (resulting in outcomes such as preserving the value from the vector that has the index when adding, or producing 0 when multiplying). It’s important to mention that results with value 0 are omitted—they aren’t stored in the resulting NumericIndexedVector and are ignored in subsequent chained operations.

## RawSum {#raw-sum}


Use two sorted arrays to store Index and Value respectively; the sorting logic is to sort the Index from small to large, and the user must ensure that there is no duplicate Index. This storage structure is used to support the scenario where the Index type is `UInt64`/`Int64`. The aggregating operations and point-wise operations defined on it are accelerated by vectorized instructions, which have not yet been implemented.

# Create a numericIndexedVector object

There are two ways to create this structure: one is to use the aggregate function `groupNumericIndexedVector` with `-State`, and the other is to build it from a map using `numericIndexedVectorBuild`. The `groupNumericIndexedVectorState` function allows customization of the number of integer and fractional bits through parameters, while `numericIndexedVectorBuild` does not currently support such customization.

## groupNumericIndexedVector {#group-numeric-indexed-vector}

Constructs a NumericIndexedVector from two data columns and returns the sum of all values as a Float64 type. If the suffix `State` is added, it returns a NumericIndexedVector object.

**Syntax**

```sql
groupNumericIndexedVector(col1, col2)
groupNumericIndexedVector(type, integer_bit_num, fraction_bit_num)(col1, col2)
```

**Parameters**

- `type`: String, optional. Specifies the storage format. Currently, only `'BSI'` is supported.
- `integer_bit_num`: `UInt32`, optional. Effective under the `'BSI'` storage format, this parameter indicates the number of bits allocated for the integer part. The valid range is `[0, 64]`. When the Index type is an integer type, the default value corresponds to the number of bits used to store the Index. For example, if the Index type is UInt16, the default `integer_bit_num` is 16.
- `fraction_bit_num`: `UInt32`, optional. Effective under the `'BSI'` storage format, this parameter indicates the number of bits allocated for the fractional part. The valid range is `[0, 24]`. When the Value type is an integer, the default value is 0; when the Value type is a floating-point type, the default value is 24.
- `col1`: The Index column. Supported types: `UInt8`/`UInt16`/`UInt32`.
- `col2`: The Value column. Supported types: `Int8`/`Int16`/`Int32`/`Int64`/`UInt8`/`UInt16`/`UInt32`/`UInt64`/`Float32`/`Float64`.

**Return Value**

A `Float64` value representing the sum of all values.

**Example**

Test data:

```text
UserID  PlayTime
1       10
2       20
3       30
```

Query & Result:

```sql
SELECT groupNumericIndexedVector(UserID, PlayTime) AS num FROM t;
┌─num─┐
│  60 │
└─────┘

SELECT groupNumericIndexedVectorState(UserID, PlayTime) as res, toTypeName(res), numericIndexedVectorAllValueSum(res) FROM t;
┌─res─┬─toTypeName(res)─────────────────────────────────────────────┬─numericIndexedVectorAllValueSum(res)──┐
│     │ AggregateFunction(groupNumericIndexedVector, UInt8, UInt8)  │ 60                                    │
└─────┴─────────────────────────────────────────────────────────────┴───────────────────────────────────────┘

SELECT groupNumericIndexedVectorStateIf(UserID, PlayTime, day = '2025-04-22') as res, toTypeName(res), numericIndexedVectorAllValueSum(res) FROM t;
┌─res─┬─toTypeName(res)────────────────────────────────────────────┬─numericIndexedVectorAllValueSum(res)──┐
│     │ AggregateFunction(groupNumericIndexedVector, UInt8, UInt8) │ 30                                    │
└─────┴────────────────────────────────────────────────────────────┴───────────────────────────────────────┘
```

## numericIndexedVectorBuild {#numeric-indexed-vector-build}

Creates a NumericIndexedVector from a map. The map’s keys represent the Index and the map’s value represents the element value.

Syntax

```sql
numericIndexedVectorBuild(map)
```

Arguments

- `map` – A mapping from Index to Value.

Example

```sql
SELECT numericIndexedVectorBuild(mapFromArrays([1, 2, 3], [10, 20, 30])) AS res, toTypeName(res);
```

Result

```text
┌─res─┬─toTypeName(res)────────────────────────────────────────────┐
│     │ AggregateFunction(groupNumericIndexedVector, UInt8, UInt8) │
└─────┴────────────────────────────────────────────────────────────┘
```

# numericIndexedVectorToMap

Converts a NumericIndexedVector to a map.

Syntax

```sql
numericIndexedVectorToMap(numericIndexedVector)
```

Arguments

- `numericIndexedVector` – A NumericIndexedVector object.

Example

```sql
SELECT numericIndexedVectorToMap(numericIndexedVectorBuild(mapFromArrays([1, 2, 3], [10, 20, 30]))) AS res;
```

Result

```text
┌─res──────────────┐
│ {1:10,2:20,3:30} │
└──────────────────┘
```

# numericIndexedVectorCardinality

Returns the cardinality (the number of unique indexes) of the NumericIndexedVector.

Syntax

```sql
numericIndexedVectorCardinality(numericIndexedVector)
```

Arguments

- `numericIndexedVector` – A NumericIndexedVector object.

Example

```sql
SELECT numericIndexedVectorCardinality(numericIndexedVectorBuild(mapFromArrays([1, 2, 3], [10, 20, 30]))) AS res;
```

Result

```text
┌─res─┐
│  3  │
└─────┘
```

# numericIndexedVectorAllValueSum

Returns the sum of all the Value elements in the NumericIndexedVector.

Syntax

```sql
numericIndexedVectorAllValueSum(numericIndexedVector)
```

Arguments

- `numericIndexedVector` – A NumericIndexedVector object.

Example

```sql
SELECT numericIndexedVectorAllValueSum(numericIndexedVectorBuild(mapFromArrays([1, 2, 3], [10, 20, 30]))) AS res;
```

Result

```text
┌─res─┐
│  60 │
└─────┘
```


# numericIndexedVectorGetValue

Retrieves the Value corresponding to a specified Index.

Syntax

```sql
numericIndexedVectorGetValue(numericIndexedVector, index)
```

Arguments

- `numericIndexedVector` – A NumericIndexedVector object.
- `index` – The index for which the Value is to be retrieved.

Example

```sql
SELECT numericIndexedVectorGetValue(numericIndexedVectorBuild(mapFromArrays([1, 2, 3], [10, 20, 30])), 3) AS res;
```

Result

```text
┌─res─┐
│  30 │
└─────┘
```

# numericIndexedVectorPointwiseAdd

Performs pointwise addition between a NumericIndexedVector and either another NumericIndexedVector or a numeric constant. The function returns a new NumericIndexedVector.

Syntax

```sql
numericIndexedVectorPointwiseAdd(numericIndexedVector, numericIndexedVector | numeric)
```

Arguments

- `numericIndexedVector` – A NumericIndexedVector object.
- `numeric` - A numeric constant.

Example

```sql
with
    numericIndexedVectorBuild(mapFromArrays([1, 2, 3], arrayMap(x -> toInt32(x), [10, 20, 30]))) as vec1,
    numericIndexedVectorBuild(mapFromArrays([2, 3, 4], arrayMap(x -> toInt32(x), [10, 20, 30]))) as vec2
SELECT
    numericIndexedVectorToMap(numericIndexedVectorPointwiseAdd(vec1, vec2)) AS res1,
    numericIndexedVectorToMap(numericIndexedVectorPointwiseAdd(vec1, 2)) AS res2;
```

Result

```text
┌─res1──────────────────┬─res2─────────────┐
│ {1:10,2:30,3:50,4:30} │ {1:12,2:22,3:32} │
└───────────────────────┴──────────────────┘
```

# numericIndexedVectorPointwiseSubtract

Performs pointwise subtraction between a NumericIndexedVector and either another NumericIndexedVector or a numeric constant. The function returns a new NumericIndexedVector.

Syntax

```sql
numericIndexedVectorPointwiseSubtract(numericIndexedVector, numericIndexedVector | numeric)
```

Arguments

- `numericIndexedVector` – A NumericIndexedVector object.
- `numeric` - A numeric constant.

Example

```sql
with
    numericIndexedVectorBuild(mapFromArrays([1, 2, 3], arrayMap(x -> toInt32(x), [10, 20, 30]))) as vec1,
    numericIndexedVectorBuild(mapFromArrays([2, 3, 4], arrayMap(x -> toInt32(x), [10, 20, 30]))) as vec2
SELECT
    numericIndexedVectorToMap(numericIndexedVectorPointwiseSubtract(vec1, vec2)) AS res1,
    numericIndexedVectorToMap(numericIndexedVectorPointwiseSubtract(vec1, 2)) AS res2;
```

Result

```text
┌─res1───────────────────┬─res2────────────┐
│ {1:10,2:10,3:10,4:-30} │ {1:8,2:18,3:28} │
└────────────────────────┴─────────────────┘
```

# numericIndexedVectorPointwiseMultiply

Performs pointwise multiplication between a NumericIndexedVector and either another NumericIndexedVector or a numeric constant. The function returns a new NumericIndexedVector.

Syntax

```sql
numericIndexedVectorPointwiseMultiply(numericIndexedVector, numericIndexedVector | numeric)
```

Arguments

- `numericIndexedVector` – A NumericIndexedVector object.
- `numeric` - A numeric constant.

Example

```sql
with
    numericIndexedVectorBuild(mapFromArrays([1, 2, 3], arrayMap(x -> toInt32(x), [10, 20, 30]))) as vec1,
    numericIndexedVectorBuild(mapFromArrays([2, 3, 4], arrayMap(x -> toInt32(x), [10, 20, 30]))) as vec2
SELECT
    numericIndexedVectorToMap(numericIndexedVectorPointwiseMultiply(vec1, vec2)) AS res1,
    numericIndexedVectorToMap(numericIndexedVectorPointwiseMultiply(vec1, 2)) AS res2;
```

Result

```text
┌─res1──────────┬─res2─────────────┐
│ {2:200,3:600} │ {1:20,2:40,3:60} │
└───────────────┴──────────────────┘
```

# numericIndexedVectorPointwiseDivide

Performs pointwise division between a NumericIndexedVector and either another NumericIndexedVector or a numeric constant. The function returns a new NumericIndexedVector.

Syntax

```sql
numericIndexedVectorPointwiseDivide(numericIndexedVector, numericIndexedVector | numeric)
```

Arguments

- `numericIndexedVector` – A NumericIndexedVector object.
- `numeric` - A numeric constant.

Example

```sql
with
    numericIndexedVectorBuild(mapFromArrays([1, 2, 3], arrayMap(x -> toFloat64(x), [10, 20, 30]))) as vec1,
    numericIndexedVectorBuild(mapFromArrays([2, 3, 4], arrayMap(x -> toFloat64(x), [10, 20, 30]))) as vec2
SELECT
    numericIndexedVectorToMap(numericIndexedVectorPointwiseDivide(vec1, vec2)) AS res1,
    numericIndexedVectorToMap(numericIndexedVectorPointwiseDivide(vec1, 2)) AS res2;
```

Result

```text
┌─res1────────┬─res2────────────┐
│ {2:2,3:1.5} │ {1:5,2:10,3:15} │
└─────────────┴─────────────────┘
```

# numericIndexedVectorPointwiseEqual

Performs pointwise comparison between a NumericIndexedVector and either another NumericIndexedVector or a numeric constant. The result is a NumericIndexedVector containing the indices where the values are equal, with all corresponding Value entries set to 1.

Syntax

```sql
numericIndexedVectorPointwiseEqual(numericIndexedVector, numericIndexedVector | numeric)
```

Arguments

- `numericIndexedVector` – A NumericIndexedVector object.
- `numeric` - A numeric constant.

Example

```sql
with
    numericIndexedVectorBuild(mapFromArrays([1, 2, 3], arrayMap(x -> toFloat64(x), [10, 20, 30]))) as vec1,
    numericIndexedVectorBuild(mapFromArrays([2, 3, 4], arrayMap(x -> toFloat64(x), [20, 20, 30]))) as vec2
SELECT
    numericIndexedVectorToMap(numericIndexedVectorPointwiseEqual(vec1, vec2)) AS res1,
    numericIndexedVectorToMap(numericIndexedVectorPointwiseEqual(vec1, 20)) AS res2;
```

Result

```text
┌─res1──┬─res2──┐
│ {2:1} │ {2:1} │
└───────┴───────┘
```

# numericIndexedVectorPointwiseNotEqual

Performs pointwise comparison between a NumericIndexedVector and either another NumericIndexedVector or a numeric constant.  The result is a NumericIndexedVector containing the indices where the values are not equal, with all corresponding Value entries set to 1.

Syntax

```sql
numericIndexedVectorPointwiseNotEqual(numericIndexedVector, numericIndexedVector | numeric)
```

Arguments

- `numericIndexedVector` – A NumericIndexedVector object.
- `numeric` - A numeric constant.

Example

```sql
with
    numericIndexedVectorBuild(mapFromArrays([1, 2, 3], arrayMap(x -> toFloat64(x), [10, 20, 30]))) as vec1,
    numericIndexedVectorBuild(mapFromArrays([2, 3, 4], arrayMap(x -> toFloat64(x), [20, 20, 30]))) as vec2
SELECT
    numericIndexedVectorToMap(numericIndexedVectorPointwiseNotEqual(vec1, vec2)) AS res1,
    numericIndexedVectorToMap(numericIndexedVectorPointwiseNotEqual(vec1, 20)) AS res2;
```

Result

```text
┌─res1──────────┬─res2──────┐
│ {1:1,3:1,4:1} │ {1:1,3:1} │
└───────────────┴───────────┘
```

# numericIndexedVectorPointwiseLess

Performs pointwise comparison between a NumericIndexedVector and either another NumericIndexedVector or a numeric constant. The result is a NumericIndexedVector containing the indices where the first vector’s value is less than the second vector’s value, with all corresponding Value entries set to 1.

Syntax

```sql
numericIndexedVectorPointwiseLess(numericIndexedVector, numericIndexedVector | numeric)
```

Arguments

- `numericIndexedVector` – A NumericIndexedVector object.
- `numeric` - A numeric constant.

Example

```sql
with
    numericIndexedVectorBuild(mapFromArrays([1, 2, 3], arrayMap(x -> toFloat64(x), [10, 20, 30]))) as vec1,
    numericIndexedVectorBuild(mapFromArrays([2, 3, 4], arrayMap(x -> toFloat64(x), [20, 40, 30]))) as vec2
SELECT
    numericIndexedVectorToMap(numericIndexedVectorPointwiseLess(vec1, vec2)) AS res1,
    numericIndexedVectorToMap(numericIndexedVectorPointwiseLess(vec1, 20)) AS res2;
```

Result

```text
┌─res1──────┬─res2──┐
│ {3:1,4:1} │ {1:1} │
└───────────┴───────┘
```

# numericIndexedVectorPointwiseLessEqual

Performs pointwise comparison between a NumericIndexedVector and either another NumericIndexedVector or a numeric constant. The result is a NumericIndexedVector containing the indices where the first vector’s value is less than or equal to the second vector’s value, with all corresponding Value entries set to 1.

Syntax

```sql
numericIndexedVectorPointwiseLessEqual(numericIndexedVector, numericIndexedVector | numeric)
```

Arguments

- `numericIndexedVector` – A NumericIndexedVector object.
- `numeric` - A numeric constant.

Example

```sql
with
    numericIndexedVectorBuild(mapFromArrays([1, 2, 3], arrayMap(x -> toFloat64(x), [10, 20, 30]))) as vec1,
    numericIndexedVectorBuild(mapFromArrays([2, 3, 4], arrayMap(x -> toFloat64(x), [20, 40, 30]))) as vec2
SELECT
    numericIndexedVectorToMap(numericIndexedVectorPointwiseLessEqual(vec1, vec2)) AS res1,
    numericIndexedVectorToMap(numericIndexedVectorPointwiseLessEqual(vec1, 20)) AS res2;
```

Result

```text
┌─res1──────────┬─res2──────┐
│ {2:1,3:1,4:1} │ {1:1,2:1} │
└───────────────┴───────────┘
```

# numericIndexedVectorPointwiseGreater

Performs pointwise comparison between a NumericIndexedVector and either another NumericIndexedVector or a numeric constant. The result is a NumericIndexedVector containing the indices where the first vector’s value is greater than the second vector’s value, with all corresponding Value entries set to 1.

Syntax

```sql
numericIndexedVectorPointwiseGreater(numericIndexedVector, numericIndexedVector | numeric)
```

Arguments

- `numericIndexedVector` – A NumericIndexedVector object.
- `numeric` - A numeric constant.

Example

```sql
with
    numericIndexedVectorBuild(mapFromArrays([1, 2, 3], arrayMap(x -> toFloat64(x), [10, 20, 50]))) as vec1,
    numericIndexedVectorBuild(mapFromArrays([2, 3, 4], arrayMap(x -> toFloat64(x), [20, 40, 30]))) as vec2
SELECT
    numericIndexedVectorToMap(numericIndexedVectorPointwiseGreater(vec1, vec2)) AS res1,
    numericIndexedVectorToMap(numericIndexedVectorPointwiseGreater(vec1, 20)) AS res2;
```

Result

```text
┌─res1──────┬─res2──┐
│ {1:1,3:1} │ {3:1} │
└───────────┴───────┘
```

# numericIndexedVectorPointwiseGreaterEqual

Performs pointwise comparison between a NumericIndexedVector and either another NumericIndexedVector or a numeric constant. The result is a NumericIndexedVector containing the indices where the first vector’s value is greater than or equal to the second vector’s value, with all corresponding Value entries set to 1.

Syntax

```sql
numericIndexedVectorPointwiseGreaterEqual(numericIndexedVector, numericIndexedVector | numeric)
```

Arguments

- `numericIndexedVector` – A NumericIndexedVector object.
- `numeric` - A numeric constant.

Example

```sql
with
    numericIndexedVectorBuild(mapFromArrays([1, 2, 3], arrayMap(x -> toFloat64(x), [10, 20, 50]))) as vec1,
    numericIndexedVectorBuild(mapFromArrays([2, 3, 4], arrayMap(x -> toFloat64(x), [20, 40, 30]))) as vec2
SELECT
    numericIndexedVectorToMap(numericIndexedVectorPointwiseGreaterEqual(vec1, vec2)) AS res1,
    numericIndexedVectorToMap(numericIndexedVectorPointwiseGreaterEqual(vec1, 20)) AS res2;
```

Result

```text
┌─res1──────────┬─res2──────┐
│ {1:1,2:1,3:1} │ {2:1,3:1} │
└───────────────┴───────────┘
```
