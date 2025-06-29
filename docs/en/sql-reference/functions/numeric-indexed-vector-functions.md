---
description: 'Documentation for NumericIndexedVector and Its Functions'
sidebar_label: 'NumericIndexedVector'
sidebar_position: 26
slug: /sql-reference/functions/numeric-indexed-vector-functions
title: 'NumericIndexedVector Functions'
---

# NumericIndexedVector

NumericIndexedVector is an abstract data structure that encapsulates a vector and implements vector aggregating and pointwise operations. Bit-Sliced Index is its storage method. For theoretical basis and usage scenarios, refer to the paper [Large-Scale Metric Computation in Online Controlled Experiment Platform](https://arxiv.org/pdf/2405.08411).

## BSI {#bit-sliced-index}

In the BSI (Bit-Sliced Index) storage method, the data is stored in [Bit-Sliced Index](https://dl.acm.org/doi/abs/10.1145/253260.253268) and then compressed using [Roaring Bitmap](https://github.com/RoaringBitmap/RoaringBitmap). Aggregating operations and pointwise operations are directly on the compressed data, which can significantly improve the efficiency of storage and query.

A vector contains indices and their corresponding values. The following are some characteristics and constraints of this data structure in BSI storage mode:

- The index type can be one of `UInt8`, `UInt16`, or `UInt32`. **Note:** Considering the performance of 64-bit implementation of Roaring Bitmap, BSI format does not support `UInt64`/`Int64`.
- The value type can be one of `Int8`, `Int16`, `Int32`, `Int64`, `UInt8`, `UInt16`, `UInt32`, `UInt64`, `Float32`, or `Float64`. **Note:** The value type does not automatically expand. For example, if you use `UInt8` as the value type, any sum that exceeds the capacity of `UInt8` will result in an overflow rather than being promoted to a higher type; similarly, operations on integers will yield integer results (e.g., division will not automatically convert to a floating-point result). Therefore, it is important to plan and design the value type ahead of time. In real-world scenarios, floating-point types (`Float32`/`Float64`) are commonly used.
- Only two vectors with the same index type and value type can perform operations.
- The underlying storage uses Bit-Sliced Index, with bitmap storing indexes. Roaring Bitmap is used as the specific implementation of bitmap. A best practice is to concentrate the index in several Roaring Bitmap containers as much as possible to maximize compression and query performance.
- The Bit-Sliced Index mechanism converts value into binary. For floating-point types, the conversion uses fixed-point representation, which may lead to precision loss. The precision can be adjusted by customizing the number of bits used for the fractional part, default is 24 bits, which is sufficient for most scenarios. You can customize the number of integer bits and fractional bits when constructing NumericIndexedVector using aggregate function groupNumericIndexedVector with `-State`.
- There are three cases for indices: non-zero value, zero value and non-existent. In NumericIndexedVector, only non-zero value and zero value will be stored. In addition, in pointwise operations between two NumericIndexedVectors, the value of non-existent index will be treated as 0. In the division scenario, the result is zero when the divisor is zero.

# Create a numericIndexedVector object

There are two ways to create this structure: one is to use the aggregate function `groupNumericIndexedVector` with `-State`. Of course, you can continue to add suffix `-if` to accept an additional condition. The aggregate function will only process the rows that trigger the condition. The other is to build it from a map using `numericIndexedVectorBuild`. The `groupNumericIndexedVectorState` function allows customization of the number of integer and fractional bits through parameters, while `numericIndexedVectorBuild` does not.

## groupNumericIndexedVector {#group-numeric-indexed-vector}

Constructs a NumericIndexedVector from two data columns and returns the sum of all values as a `Float64` type. If the suffix `State` is added, it returns a NumericIndexedVector object.

**Syntax**

```sql
groupNumericIndexedVectorState(col1, col2)
groupNumericIndexedVectorState(type, integer_bit_num, fraction_bit_num)(col1, col2)
```

**Parameters**

- `type`: String, optional. Specifies the storage format. Currently, only `'BSI'` is supported.
- `integer_bit_num`: `UInt32`, optional. Effective under the `'BSI'` storage format, this parameter indicates the number of bits used for the integer part. When the index type is an integer type, the default value corresponds to the number of bits used to store the index. For example, if the index type is UInt16, the default `integer_bit_num` is 16. For Float32 and Float64 index types, the default value of integer_bit_num is 40, so the integer part of the data that can be represented is in the range `[-2^39, 2^39 - 1]`. The legal range is `[0, 64]`. 
- `fraction_bit_num`: `UInt32`, optional. Effective under the `'BSI'` storage format, this parameter indicates the number of bits used for the fractional part. When the value type is an integer, the default value is 0; when the value type is Float32 or Float64 types, the default value is 24. The valid range is `[0, 24]`. 
- There is also a constraint that the valid range of integer_bit_num + fraction_bit_num is [0, 64].
- `col1`: The index column. Supported types: `UInt8`/`UInt16`/`UInt32`/`Int8`/`Int16`/`Int32`.
- `col2`: The value column. Supported types: `Int8`/`Int16`/`Int32`/`Int64`/`UInt8`/`UInt16`/`UInt32`/`UInt64`/`Float32`/`Float64`.

**Return value**

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

SELECT groupNumericIndexedVectorStateIf('BSI', 32, 0)(UserID, PlayTime, day = '2025-04-22') as res, toTypeName(res), numericIndexedVectorAllValueSum(res) FROM t;
┌─res─┬─toTypeName(res)──────────────────────────────────────────────────────────┬─numericIndexedVectorAllValueSum(res)──┐
│     │ AggregateFunction('BSI', 32, 0)(groupNumericIndexedVector, UInt8, UInt8) │ 30                                    │
└─────┴──────────────────────────────────────────────────────────────────────────┴───────────────────────────────────────┘
```

## numericIndexedVectorBuild {#numeric-indexed-vector-build}

Creates a NumericIndexedVector from a map. The map’s keys represent the vector's index and map's value represents the vector's value.

Syntax

```sql
numericIndexedVectorBuild(map)
```

Arguments

- `map` – A mapping from index to value.

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

Returns the cardinality (number of unique indexes) of the NumericIndexedVector.

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

Returns sum of all the values in NumericIndexedVector.

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

Retrieves the value corresponding to a specified index.

Syntax

```sql
numericIndexedVectorGetValue(numericIndexedVector, index)
```

Arguments

- `numericIndexedVector` – A NumericIndexedVector object.
- `index` – The index for which value is to be retrieved.

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

# numericIndexedVectorShortDebugString

Returns internal information of the NumericIndexedVector in a json format. This function is primarily used for debugging purposes.

Syntax

```sql
numericIndexedVectorShortDebugString(numericIndexedVector)
```

Arguments

- `numericIndexedVector` – A NumericIndexedVector object.

Example

```sql
SELECT numericIndexedVectorShortDebugString(numericIndexedVectorBuild(mapFromArrays([1, 2, 3], [10, 20, 30]))) AS res\G;
```
Result

```text
Row 1:
──────
res: {"vector_type":"BSI","index_type":"char8_t","value_type":"char8_t","integer_bit_num":8,"fraction_bit_num":0,"zero_indexes_info":{"cardinality":"0"},"non_zero_indexes_info":{"total_cardinality":"3","all_value_sum":60,"number_of_bitmaps":"8","bitmap_info":{"cardinality":{"0":"0","1":"2","2":"2","3":"2","4":"2","5":"0","6":"0","7":"0"}}}}
```

- `vector_type`: Storage type of the vector, currently only `BSI` is supported.
- `index_type`: Type of the index.
- `value_type`: Type of the value.

The following information is valid in BSI vector type.

- `integer_bit_num`: Number of bits used for the integer part.
- `fraction_bit_num`: Number of bits used for the fractional part.
- `zero_indexes info`: Information of indexes with value equal to 0
    - `cardinality`: The number of indexes with value equal to 0.
- `non_zero_indexes info`: Information of indexes with value not equal to 0
    - `total_cardinality`: Number of indexes with value not equal to 0.
    - `all value sum`: Sum of all values.
    - `number_of_bitmaps`: Number of bitmaps used by this indexes which value not equal 0.
    - `bitmap_info`: Information of each bitmap
        - `cardinality`: Number of indexes in each bitmap.

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
WITH
    numericIndexedVectorBuild(mapFromArrays([1, 2, 3], arrayMap(x -> toInt32(x), [10, 20, 30]))) AS vec1,
    numericIndexedVectorBuild(mapFromArrays([2, 3, 4], arrayMap(x -> toInt32(x), [10, 20, 30]))) AS vec2
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
WITH
    numericIndexedVectorBuild(mapFromArrays([1, 2, 3], arrayMap(x -> toInt32(x), [10, 20, 30]))) AS vec1,
    numericIndexedVectorBuild(mapFromArrays([2, 3, 4], arrayMap(x -> toInt32(x), [10, 20, 30]))) AS vec2
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
WITH
    numericIndexedVectorBuild(mapFromArrays([1, 2, 3], arrayMap(x -> toInt32(x), [10, 20, 30]))) AS vec1,
    numericIndexedVectorBuild(mapFromArrays([2, 3, 4], arrayMap(x -> toInt32(x), [10, 20, 30]))) AS vec2
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

Performs pointwise division between a NumericIndexedVector and either another NumericIndexedVector or a numeric constant. The function returns a new NumericIndexedVector. The result is zero when the divisor is zero.

Syntax

```sql
numericIndexedVectorPointwiseDivide(numericIndexedVector, numericIndexedVector | numeric)
```

Arguments

- `numericIndexedVector` – A NumericIndexedVector object.
- `numeric` - A numeric constant.

Example

```sql
WITH
    numericIndexedVectorBuild(mapFromArrays([1, 2, 3], arrayMap(x -> toFloat64(x), [10, 20, 30]))) AS vec1,
    numericIndexedVectorBuild(mapFromArrays([2, 3, 4], arrayMap(x -> toFloat64(x), [10, 20, 30]))) AS vec2
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

Performs pointwise comparison between a NumericIndexedVector and either another NumericIndexedVector or a numeric constant. The result is a NumericIndexedVector containing the indices where the values are equal, with all corresponding values set to 1.

Syntax

```sql
numericIndexedVectorPointwiseEqual(numericIndexedVector, numericIndexedVector | numeric)
```

Arguments

- `numericIndexedVector` – A NumericIndexedVector object.
- `numeric` - A numeric constant.

Example

```sql
WITH
    numericIndexedVectorBuild(mapFromArrays([1, 2, 3], arrayMap(x -> toFloat64(x), [10, 20, 30]))) AS vec1,
    numericIndexedVectorBuild(mapFromArrays([2, 3, 4], arrayMap(x -> toFloat64(x), [20, 20, 30]))) AS vec2
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

Performs pointwise comparison between a NumericIndexedVector and either another NumericIndexedVector or a numeric constant.  The result is a NumericIndexedVector containing the indices where the values are not equal, with all corresponding values set to 1.

Syntax

```sql
numericIndexedVectorPointwiseNotEqual(numericIndexedVector, numericIndexedVector | numeric)
```

Arguments

- `numericIndexedVector` – A NumericIndexedVector object.
- `numeric` - A numeric constant.

Example

```sql
WITH
    numericIndexedVectorBuild(mapFromArrays([1, 2, 3], arrayMap(x -> toFloat64(x), [10, 20, 30]))) AS vec1,
    numericIndexedVectorBuild(mapFromArrays([2, 3, 4], arrayMap(x -> toFloat64(x), [20, 20, 30]))) AS vec2
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

Performs pointwise comparison between a NumericIndexedVector and either another NumericIndexedVector or a numeric constant. The result is a NumericIndexedVector containing the indices where the first vector’s value is less than the second vector’s value, with all corresponding values set to 1.

Syntax

```sql
numericIndexedVectorPointwiseLess(numericIndexedVector, numericIndexedVector | numeric)
```

Arguments

- `numericIndexedVector` – A NumericIndexedVector object.
- `numeric` - A numeric constant.

Example

```sql
WITH
    numericIndexedVectorBuild(mapFromArrays([1, 2, 3], arrayMap(x -> toFloat64(x), [10, 20, 30]))) AS vec1,
    numericIndexedVectorBuild(mapFromArrays([2, 3, 4], arrayMap(x -> toFloat64(x), [20, 40, 30]))) AS vec2
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

Performs pointwise comparison between a NumericIndexedVector and either another NumericIndexedVector or a numeric constant. The result is a NumericIndexedVector containing the indices where the first vector’s value is less than or equal to the second vector’s value, with all corresponding values set to 1.

Syntax

```sql
numericIndexedVectorPointwiseLessEqual(numericIndexedVector, numericIndexedVector | numeric)
```

Arguments

- `numericIndexedVector` – A NumericIndexedVector object.
- `numeric` - A numeric constant.

Example

```sql
WITH
    numericIndexedVectorBuild(mapFromArrays([1, 2, 3], arrayMap(x -> toFloat64(x), [10, 20, 30]))) AS vec1,
    numericIndexedVectorBuild(mapFromArrays([2, 3, 4], arrayMap(x -> toFloat64(x), [20, 40, 30]))) AS vec2
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

Performs pointwise comparison between a NumericIndexedVector and either another NumericIndexedVector or a numeric constant. The result is a NumericIndexedVector containing the indices where the first vector’s value is greater than the second vector’s value, with all corresponding values set to 1.

Syntax

```sql
numericIndexedVectorPointwiseGreater(numericIndexedVector, numericIndexedVector | numeric)
```

Arguments

- `numericIndexedVector` – A NumericIndexedVector object.
- `numeric` - A numeric constant.

Example

```sql
WITH
    numericIndexedVectorBuild(mapFromArrays([1, 2, 3], arrayMap(x -> toFloat64(x), [10, 20, 50]))) AS vec1,
    numericIndexedVectorBuild(mapFromArrays([2, 3, 4], arrayMap(x -> toFloat64(x), [20, 40, 30]))) AS vec2
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

Performs pointwise comparison between a NumericIndexedVector and either another NumericIndexedVector or a numeric constant. The result is a NumericIndexedVector containing the indices where the first vector’s value is greater than or equal to the second vector’s value, with all corresponding values set to 1.

Syntax

```sql
numericIndexedVectorPointwiseGreaterEqual(numericIndexedVector, numericIndexedVector | numeric)
```

Arguments

- `numericIndexedVector` – A NumericIndexedVector object.
- `numeric` - A numeric constant.

Example

```sql
WITH
    numericIndexedVectorBuild(mapFromArrays([1, 2, 3], arrayMap(x -> toFloat64(x), [10, 20, 50]))) AS vec1,
    numericIndexedVectorBuild(mapFromArrays([2, 3, 4], arrayMap(x -> toFloat64(x), [20, 40, 30]))) AS vec2
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
