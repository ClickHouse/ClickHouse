---
sidebar_position: 114
---

# groupArrayMovingAvg {#agg_function-grouparraymovingavg}

Calculates the moving average of input values.

``` sql
groupArrayMovingAvg(numbers_for_summing)
groupArrayMovingAvg(window_size)(numbers_for_summing)
```

The function can take the window size as a parameter. If left unspecified, the function takes the window size equal to the number of rows in the column.

**Arguments**

-   `numbers_for_summing` — [Expression](../../../sql-reference/syntax.md#syntax-expressions) resulting in a numeric data type value.
-   `window_size` — Size of the calculation window.

**Returned values**

-   Array of the same size and type as the input data.

The function uses [rounding towards zero](https://en.wikipedia.org/wiki/Rounding#Rounding_towards_zero). It truncates the decimal places insignificant for the resulting data type.

**Example**

The sample table `b`:

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

The queries:

``` sql
SELECT
    groupArrayMovingAvg(int) AS I,
    groupArrayMovingAvg(float) AS F,
    groupArrayMovingAvg(dec) AS D
FROM t
```

``` text
┌─I─────────┬─F───────────────────────────────────┬─D─────────────────────┐
│ [0,0,1,3] │ [0.275,0.82500005,1.9250001,3.8675] │ [0.27,0.82,1.92,3.86] │
└───────────┴─────────────────────────────────────┴───────────────────────┘
```

``` sql
SELECT
    groupArrayMovingAvg(2)(int) AS I,
    groupArrayMovingAvg(2)(float) AS F,
    groupArrayMovingAvg(2)(dec) AS D
FROM t
```

``` text
┌─I─────────┬─F────────────────────────────────┬─D─────────────────────┐
│ [0,1,3,5] │ [0.55,1.6500001,3.3000002,6.085] │ [0.55,1.65,3.30,6.08] │
└───────────┴──────────────────────────────────┴───────────────────────┘
```
