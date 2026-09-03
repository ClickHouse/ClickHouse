---
description: 'Documentation for NumericIndexedVector and Its Functions'
sidebar_label: 'NumericIndexedVector'
slug: /sql-reference/functions/numeric-indexed-vector-functions
title: 'NumericIndexedVector Functions'
doc_type: 'reference'
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

## Create a numericIndexedVector object {#create-numeric-indexed-vector-object}

There are two ways to create this structure: one is to use the aggregate function `groupNumericIndexedVector` with `-State`.
You can add suffix `-if` to accept an additional condition.
The aggregate function will only process the rows that trigger the condition.
The other is to build it from a map using `numericIndexedVectorBuild`.
The `groupNumericIndexedVectorState` function allows customization of the number of integer and fractional bits through parameters, while `numericIndexedVectorBuild` does not.

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

:::note
The documentation below is generated from the `system.functions` system table.
:::

<!-- 
the tags below are used to generate the documentation from system tables, and should not be removed.
For more details see https://github.com/ClickHouse/clickhouse-docs/blob/main/contribute/autogenerated-documentation-from-source.md
-->

<!--AUTOGENERATED_START-->
<!--AUTOGENERATED_END-->
