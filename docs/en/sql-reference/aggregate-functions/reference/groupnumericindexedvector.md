---
slug: /zh/sql-reference/aggregate-functions/reference/groupnumericindexedvector
---

# groupNumericIndexedVector

Constructs a NumericIndexedVector from two data columns and returns the sum of all values as a Float64 type. If the suffix `State` is added, it returns a [NumericIndexedVector object](../../../sql-reference/functions/numeric-indexed-vector-functions.md).

**Syntax**

```sql
groupNumericIndexedVector(col1, col2)
groupNumericIndexedVector(type, integer_bit_num, fraction_bit_num)(col1, col2)
```

**Parameters**

- `type`: String, optional. Specifies the storage format. Currently, only `'BSI'` is supported.
- `integer_bit_num`: UInt32, optional. Effective under the `'BSI'` storage format, this parameter indicates the number of bits allocated for the integer part. The valid range is `[0, 64]`. When the Index type is an integer type, the default value corresponds to the number of bits used to store the Index. For example, if the Index type is UInt16, the default `integer_bit_num` is 16.
- `fraction_bit_num`: UInt32, optional. Effective under the `'BSI'` storage format, this parameter indicates the number of bits allocated for the fractional part. The valid range is `[0, 24]`. When the Value type is an integer, the default value is 0; when the Value type is a floating-point type, the default value is 24.
- `col1`: The Index column. Supported types: UInt8/UInt16/UInt32.
- `col2`: The Value column. Supported types: Int8/Int16/Int32/Int64/UInt8/UInt16/UInt32/UInt64/Float32/Float64.

**Return Value**

A Float64 value representing the sum of all values.

**Example**

Test data:

```text
UserID  PlayTime
1       10
2       20
3       30
```

Query:

```sql
SELECT groupNumericIndexedVector(UserID, PlayTime) AS num FROM t
```

Result:

```text
┌─num─┐
│  60 │
└─────┘
```
