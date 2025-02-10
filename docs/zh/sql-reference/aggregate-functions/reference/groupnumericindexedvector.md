---
slug: /zh/sql-reference/aggregate-functions/reference/groupnumericindexedvector
---

# groupNumericIndexedVector

从两个数据列构造 NumericIndexedVector，返回所有 value 的和（Float64) 类型，如果添加后缀`State`，返回的是 [NumericIndexedVector 对象](../../../sql-reference/functions/numeric-indexed-vector-functions.md)。

语法

``` sql
groupNumericIndexedVector(col1, col2)
groupNumericIndexedVector(type, integer_bit_num, fraction_bit_num)(col1, col2)
```

参数
- `type`: String，可选。指定存储格式，目前仅支持 `'BSI'`。
- `integer_bit_num`：UInt32，可选。`'BSI'` 存储格式下有效，表示整数比特位的数量，有效范围：`[0, 64]`。当 Index 类型位整型时，默认值位对应 Index 的存储比特数，比如Index 类型是 UInt16，默认 integer\_bit\_num 为 16。
- `fraction_bit_num`：UInt32，可选。`'BSI'`存储格式下有效，表示小数比特位的数量，有效范围：[0, 24]。当 Value 类型为整型时，默认值为 0；当 Value 类型为浮点类型时，默认值为 24。
- `col1`：Index 列。类型支持 UInt8/UInt16/UInt32。
- `col2`：Value 列。类型支持 Int8/Int16/Int32/Int64/UInt8/UInt16/UInt32/UInt64/Float32/Float64。

返回值

Float64 类型的值。所有 Value 的求和。

例子

测试数据：

``` text
UserID PlayTime
1          10
2          20
3          30
```

查询：

``` sql
SELECT groupNumericIndexedVector(UserID, PlayTime) as num FROM t
```

Result:

``` text
┌─num─┐
│  60 │
└─────┘
```

