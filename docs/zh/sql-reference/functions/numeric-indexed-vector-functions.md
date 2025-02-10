---
slug: /zh/sql-reference/functions/numeric-indexed-vector-functions
---

# NumericIndexedVector

基于 bitmap 和 Bit-Sliced Index 对向量进行压缩存储，并在压缩域上实现向量的一些 pointwise 操作。显著提高了存储和查询的效率。

向量包含下标（Index）和元素值（Value）；目前数据结构的一些特点和约束如下：
- Index 类型 UInt8/UInt16/UInt32。注意下标仅支持 UInt32 以内的大小，暂不支持UInt64/Int64。
- value 类型 Int8/Int16/Int32/Int64/UInt8/UInt16/UInt32/UInt64/Float32/Float64。注意 Value 的类型不会自动扩展，比如使用 UInt8 作为 Value 的类型，那么求和超过 UInt8 会溢出，不会自动扩展到更高的类型；再比如整数的操作结果仍然是整数（比如除法，不会自动转换成浮点数）。因此需要提前规划设计好 Value 的类型，真实场景常用浮点数（Float32/Float64）。
- 底层存储使用 Bit-Sliced Index。Bitmap 中存储的是 Index。具体细节参考论文 [Large-Scale Metric Computation in Online Controlled Experiment Platform](https://arxiv.org/pdf/2405.08411)。
- Bit-Sliced Index 机制会将 Value 转成二进制，浮点类型的 Value 在转成二进制时使用的定点小数，因此会有精度损失，可以通过自定义小数的二进制位数来改变精度，默认使用 24 位，足够大部分场景的需求。使用 groupNumericIndexedVectorState生成向量时可以自定义整数位和小数位的值。
- 对于两个 numericIndexedVector 的 pointwise 运算，不同向量中缺失的 index 被视为 0（从而产生相应的结果，比如相加时保留单边存在的值，相乘时结果为 0等）。

可以使用两种方式创建此结构，一种是使用聚合函数 groupNumericIndexedVectorState，另一种是通过从一个 map 中使用 numericIndexedVectorBuild 创建。groupNumericIndexedVectorState可以通过参数自定义整数位和小数位的大小。numericIndexedVectorBuild 暂时不支持自定义。

# numericIndexedVectorBuild

通过 map 创建 numericIndexedVector。map 的 key 是 Index，value 是向量的元素值 Value。

语法

``` sql
numericIndexedVectorBuild(map)
```

参数

- `map` - Index 到 Value 的映射。

示例

``` sql
SELECT numericIndexedVectorBuild(mapFromArrays([1, 2, 3], [10, 20, 30])) as res, toTypeName(res);
```

结果

``` text
┌─res─┬─toTypeName(res)────────────────────────────────────────────┐
│     │ AggregateFunction(groupNumericIndexedVector, UInt8, UInt8) │
└─────┴────────────────────────────────────────────────────────────┘
```

# numericIndexedVectorToMap

将 numericIndexedVector 转换成 map。

语法

``` sql
numericIndexedVectorToMap(numericIndexedVector)
```

参数

- `numericIndexedVector` - numericIndexedVector 对象。

示例

``` sql
SELECT numericIndexedVectorToMap(numericIndexedVectorBuild(mapFromArrays([1, 2, 3], [10, 20, 30]))) as res;
```

结果

``` text
┌─res──────────────┐
│ {1:10,2:20,3:30} │
└──────────────────┘
```

# numericIndexedVectorCardinality

Index 的基数。

语法

``` sql
numericIndexedVectorCardinality(numericIndexedVector)
```

参数

- `numericIndexedVector` numericIndexedVector 对象。

示例

``` sql
SELECT numericIndexedVectorCardinality(numericIndexedVectorBuild(mapFromArrays([1, 2, 3], [10, 20, 30]))) as res;
```

结果

``` text
┌─res─┐
│  3  │
└─────┘
```

# numericIndexedVectorAllValueSum

所有 Value 的和。

语法

``` sql
numericIndexedVectorAllValueSum(numericIndexedVector)
```

参数

- `numericIndexedVector` numericIndexedVector 对象。

示例

``` sql
SELECT numericIndexedVectorAllValueSum(numericIndexedVectorBuild(mapFromArrays([1, 2, 3], [10, 20, 30]))) as res;
```

结果

``` text
┌─res─┐
│ 60  │
└─────┘
```

# numericIndexedVectorGetValue

获取 Index 对应的 Value。

语法

``` sql
numericIndexedVectorGetValue(numericIndexedVector, index)
```

参数

- `numericIndexedVector` - numericIndexedVector 对象。
- `index` - 下标/索引。

示例

``` sql
SELECT numericIndexedVectorGetValue(numericIndexedVectorBuild(mapFromArrays([1, 2, 3], [10, 20, 30])), 3) as res;
```

结果

``` text
┌─res─┐
│ 30  │
└─────┘
```

# numericIndexedVectorPointwiseAdd

两个 numericIndexedVector 的逐元素加法。返回的结果是一个 numericIndexedVector。

语法

``` sql
numericIndexedVectorPointwiseAdd(numericIndexedVector, numericIndexedVector)
```

参数

- `numericIndexedVector` numericIndexedVector 对象。

示例

``` sql
SELECT numericIndexedVectorToMap(
    numericIndexedVectorPointwiseAdd(
        numericIndexedVectorBuild(mapFromArrays([1, 2, 3], [10, 20, 30])),
        numericIndexedVectorBuild(mapFromArrays([2, 3, 4], [10, 20, 30]))
    )
) as res;
```

结果

``` text
┌─res───────────────────┐
│ {1:10,2:30,3:50,4:30} │
└───────────────────────┘
```

# numericIndexedVectorPointwiseSubtract

两个 numericIndexedVector 的逐元素减法。返回的结果是一个 numericIndexedVector。

语法

``` sql
numericIndexedVectorPointwiseSubtract(numericIndexedVector, numericIndexedVector)
```

参数

- `numericIndexedVector` numericIndexedVector 对象。

示例

``` sql
SELECT numericIndexedVectorToMap(
    numericIndexedVectorPointwiseSubtract(
        numericIndexedVectorBuild(mapFromArrays([1, 2, 3], arrayMap(x -> toInt32(x), [10, 20, 30]))),
        numericIndexedVectorBuild(mapFromArrays([2, 3, 4], arrayMap(x -> toInt32(x), [10, 20, 30])))
    )
) as res;
```

结果

``` text
┌─res───────────────────┐
│{1:10,2:10,3:10,4:-30} │
└───────────────────────┘
```

# numericIndexedVectorPointwiseMultiply

两个 numericIndexedVector 的逐元素乘法。返回的结果是一个 numericIndexedVector。

语法

``` sql
numericIndexedVectorPointwiseMultiply(numericIndexedVector, numericIndexedVector)
```

参数

- `numericIndexedVector` numericIndexedVector 对象。

示例

``` sql
SELECT numericIndexedVectorToMap(
    numericIndexedVectorPointwiseMultiply(
        numericIndexedVectorBuild(mapFromArrays([1, 2, 3], arrayMap(x -> toInt32(x), [10, 20, 30]))),
        numericIndexedVectorBuild(mapFromArrays([2, 3, 4], arrayMap(x -> toInt32(x), [10, 20, 30])))
    )
) as res;
```

结果

``` text
┌─res───────────┐
│ {2:200,3:600} │
└───────────────┘
```

# numericIndexedVectorPointwiseDivide

两个 numericIndexedVector 的逐元素除法。返回的结果是一个 numericIndexedVector。

语法

``` sql
numericIndexedVectorPointwiseDivide(numericIndexedVector, numericIndexedVector)
```

参数

- `numericIndexedVector` numericIndexedVector 对象。

示例

``` sql
SELECT numericIndexedVectorToMap(
    numericIndexedVectorPointwiseDivide(
        numericIndexedVectorBuild(mapFromArrays([1, 2, 3], arrayMap(x -> toFloat64(x), [10, 20, 30]))),
        numericIndexedVectorBuild(mapFromArrays([2, 3, 4], arrayMap(x -> toFloat64(x), [10, 20, 30])))
    )
) as res;
```

结果

``` text
┌─res─────────┐
│ {2:2,3:1.5} │
└─────────────┘
```

# numericIndexedVectorPointwiseEqual

两个 numericIndexedVector 的逐元素比较。返回的结果是值相等的 Index 组成的 numericIndexedVector，所有的 Value 值均为 1。

语法

``` sql
numericIndexedVectorPointwiseEqual(numericIndexedVector, numericIndexedVector)
```

参数

- `numericIndexedVector` numericIndexedVector 对象。

示例

``` sql
SELECT numericIndexedVectorToMap(
    numericIndexedVectorPointwiseEqual(
        numericIndexedVectorBuild(mapFromArrays([1, 2, 3], arrayMap(x -> toFloat64(x), [10, 20, 30]))),
        numericIndexedVectorBuild(mapFromArrays([2, 3, 4], arrayMap(x -> toFloat64(x), [20, 20, 30])))
    )
) as res;
```

结果

``` text
┌─res───┐
│ {2:1} │
└───────┘
```

# numericIndexedVectorPointwiseNotEqual

两个 numericIndexedVector 的逐元素比较。返回的结果是值不等的 Index 组成的 numericIndexedVector，所有的 Value 值均为 1。

语法

``` sql
numericIndexedVectorPointwiseNotEqual(numericIndexedVector, numericIndexedVector)
```

参数

- `numericIndexedVector` numericIndexedVector 对象。

示例

``` sql
SELECT numericIndexedVectorToMap(
    numericIndexedVectorPointwiseNotEqual(
        numericIndexedVectorBuild(mapFromArrays([1, 2, 3], arrayMap(x -> toFloat64(x), [10, 20, 30]))),
        numericIndexedVectorBuild(mapFromArrays([2, 3, 4], arrayMap(x -> toFloat64(x), [20, 20, 30])))
    )
) as res;
```

结果

``` text
┌─res───────────┐
│ {1:1,3:1,4:1} │
└───────────────┘
```

# numericIndexedVectorPointwiseLess

两个 numericIndexedVector 的逐元素比较。返回的结果是小于成立的 Index 组成的 numericIndexedVector，所有的 Value 值均为 1。

语法

``` sql
numericIndexedVectorPointwiseLess(numericIndexedVector, numericIndexedVector)
```

参数

- `numericIndexedVector` numericIndexedVector 对象。

示例

``` sql
SELECT numericIndexedVectorToMap(
    numericIndexedVectorPointwiseLess(
        numericIndexedVectorBuild(mapFromArrays([1, 2, 3], arrayMap(x -> toFloat64(x), [10, 20, 30]))),
        numericIndexedVectorBuild(mapFromArrays([2, 3, 4], arrayMap(x -> toFloat64(x), [20, 40, 30])))
    )
) as res;
```

结果

``` text
┌─res───────┐
│ {3:1,4:1} │
└───────────┘
```


# numericIndexedVectorPointwiseLessEqual

两个 numericIndexedVector 的逐元素比较。返回的结果是小于等于成立的 Index 组成的 numericIndexedVector，所有的 Value 值均为 1。

语法

``` sql
numericIndexedVectorPointwiseLessEqual(numericIndexedVector, numericIndexedVector)
```

参数

- `numericIndexedVector` numericIndexedVector 对象。

示例

``` sql
SELECT numericIndexedVectorToMap(
    numericIndexedVectorPointwiseLessEqual(
        numericIndexedVectorBuild(mapFromArrays([1, 2, 3], arrayMap(x -> toFloat64(x), [10, 20, 30]))),
        numericIndexedVectorBuild(mapFromArrays([2, 3, 4], arrayMap(x -> toFloat64(x), [20, 40, 30])))
    )
) as res;
```

结果

``` text
┌─res───────────┐
│ {2:1,3:1,4:1} │
└───────────────┘
```

# numericIndexedVectorPointwiseGreater

两个 numericIndexedVector 的逐元素比较。返回的结果是大于成立的 Index 组成的 numericIndexedVector，所有的 Value 值均为 1。

语法

``` sql
numericIndexedVectorPointwiseGreater(numericIndexedVector, numericIndexedVector)
```

参数

- `numericIndexedVector` numericIndexedVector 对象。

示例

``` sql
SELECT numericIndexedVectorToMap(
    numericIndexedVectorPointwiseGreater(
        numericIndexedVectorBuild(mapFromArrays([1, 2, 3], arrayMap(x -> toFloat64(x), [10, 20, 50]))),
        numericIndexedVectorBuild(mapFromArrays([2, 3, 4], arrayMap(x -> toFloat64(x), [20, 40, 30])))
    )
) as res;
```

结果

``` text
┌─res───────┐
│ {1:1,3:1} │
└───────────┘
```

# numericIndexedVectorPointwiseGreaterEqual

两个 numericIndexedVector 的逐元素比较。返回的结果是大于等于成立的 Index 组成的 numericIndexedVector，所有的 Value 值均为 1。

语法

``` sql
numericIndexedVectorPointwiseGreaterEqual(numericIndexedVector, numericIndexedVector)
```

参数

- `numericIndexedVector` numericIndexedVector 对象。

示例

``` sql
SELECT numericIndexedVectorToMap(
    numericIndexedVectorPointwiseGreaterEqual(
        numericIndexedVectorBuild(mapFromArrays([1, 2, 3], arrayMap(x -> toFloat64(x), [10, 20, 50]))),
        numericIndexedVectorBuild(mapFromArrays([2, 3, 4], arrayMap(x -> toFloat64(x), [20, 40, 30])))
    )
) as res;
```

结果

``` text
┌─res───────────┐
│ {1:1,2:1,3:1} │
└───────────────┘
```

