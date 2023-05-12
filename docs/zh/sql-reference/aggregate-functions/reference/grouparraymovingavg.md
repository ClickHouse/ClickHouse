---
sidebar_position: 114
---

# groupArrayMovingAvg {#agg_function-grouparraymovingavg}

计算输入值的移动平均值。

**语法**

``` sql
groupArrayMovingAvg(numbers_for_summing)
groupArrayMovingAvg(window_size)(numbers_for_summing)
```

该函数可以将窗口大小作为参数。 如果未指定，则该函数的窗口大小等于列中的行数。

**参数**

-   `numbers_for_summing` — [表达式](../../../sql-reference/syntax.md#syntax-expressions) 生成数值数据类型值。
-   `window_size` — 窗口大小。

**返回值**

-   与输入数据大小相同的数组。

对于输入数据类型是[Integer](../../../sql-reference/data-types/int-uint.md),
和[floating-point](../../../sql-reference/data-types/float.md),
对应的返回值类型是 `Float64` 。
对于输入数据类型是[Decimal](../../../sql-reference/data-types/decimal.md) 返回值类型是 `Decimal128` 。

该函数对于 `Decimal128` 使用 [四舍五入到零](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero). 它截断无意义的小数位来保证结果的数据类型。

**示例**

样表 `t`:

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
    groupArrayMovingAvg(int) AS I,
    groupArrayMovingAvg(float) AS F,
    groupArrayMovingAvg(dec) AS D
FROM t
```

``` text
┌─I────────────────────┬─F─────────────────────────────────────────────────────────────────────────────┬─D─────────────────────┐
│ [0.25,0.75,1.75,3.5] │ [0.2750000059604645,0.8250000178813934,1.9250000417232513,3.8499999940395355] │ [0.27,0.82,1.92,3.86] │
└──────────────────────┴───────────────────────────────────────────────────────────────────────────────┴───────────────────────┘
```

``` sql
SELECT
    groupArrayMovingAvg(2)(int) AS I,
    groupArrayMovingAvg(2)(float) AS F,
    groupArrayMovingAvg(2)(dec) AS D
FROM t
```

``` text
┌─I───────────────┬─F───────────────────────────────────────────────────────────────────────────┬─D─────────────────────┐
│ [0.5,1.5,3,5.5] │ [0.550000011920929,1.6500000357627869,3.3000000715255737,6.049999952316284] │ [0.55,1.65,3.30,6.08] │
└─────────────────┴─────────────────────────────────────────────────────────────────────────────┴───────────────────────┘
```
