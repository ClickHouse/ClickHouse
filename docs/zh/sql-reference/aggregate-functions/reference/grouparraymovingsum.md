---
sidebar_position: 113
---

# groupArrayMovingSum {#agg_function-grouparraymovingsum}


计算输入值的移动和。

**语法**

``` sql
groupArrayMovingSum(numbers_for_summing)
groupArrayMovingSum(window_size)(numbers_for_summing)
```

该函数可以将窗口大小作为参数。 如果未指定，则该函数的窗口大小等于列中的行数。

**参数**

-   `numbers_for_summing` — [表达式](../../../sql-reference/syntax.md#syntax-expressions) 生成数值数据类型值。
-   `window_size` — 窗口大小。

**返回值**

-   与输入数据大小相同的数组。
对于输入数据类型是[Decimal](../../../sql-reference/data-types/decimal.md) 数组元素类型是 `Decimal128` 。
对于其他的数值类型, 获取其对应的 `NearestFieldType` 。

**示例**

样表:

``` sql
CREATE TABLE t
(
    `int` UInt8,
    `float` Float32,
    `dec` Decimal32(2)
)
ENGINE = TinyLog
```

``` text
┌─int─┬─float─┬──dec─┐
│   1 │   1.1 │ 1.10 │
│   2 │   2.2 │ 2.20 │
│   4 │   4.4 │ 4.40 │
│   7 │  7.77 │ 7.77 │
└─────┴───────┴──────┘
```

查询:

``` sql
SELECT
    groupArrayMovingSum(int) AS I,
    groupArrayMovingSum(float) AS F,
    groupArrayMovingSum(dec) AS D
FROM t
```

``` text
┌─I──────────┬─F───────────────────────────────┬─D──────────────────────┐
│ [1,3,7,14] │ [1.1,3.3000002,7.7000003,15.47] │ [1.10,3.30,7.70,15.47] │
└────────────┴─────────────────────────────────┴────────────────────────┘
```

``` sql
SELECT
    groupArrayMovingSum(2)(int) AS I,
    groupArrayMovingSum(2)(float) AS F,
    groupArrayMovingSum(2)(dec) AS D
FROM t
```

``` text
┌─I──────────┬─F───────────────────────────────┬─D──────────────────────┐
│ [1,3,6,11] │ [1.1,3.3000002,6.6000004,12.17] │ [1.10,3.30,6.60,12.17] │
└────────────┴─────────────────────────────────┴────────────────────────┘
```
