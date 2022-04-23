---
sidebar_position: 108
---

## exponentialMovingAverage {#exponential-moving-average}

Сalculates the exponential moving average of values for the determined time. 

**Syntax**

```sql
exponentialMovingAverage(x)(value, timestamp)
```

Each `value` corresponds to the determinate `timestamp`. The half-life `x` is the time lag at which the exponential weights decay by one-half. The function returns a weighted average: the older the time point, the less weight the corresponding value is considered to be.

**Arguments**

-   `value` — Value. [Integer](../../../sql-reference/data-types/int-uint.md), [Float](../../../sql-reference/data-types/float.md) or [Decimal](../../../sql-reference/data-types/decimal.md).
-   `timestamp` — Timestamp. [Integer](../../../sql-reference/data-types/int-uint.md), [Float](../../../sql-reference/data-types/float.md) or [Decimal](../../../sql-reference/data-types/decimal.md).

**Parameters**

-   `x` — Half-life period. [Integer](../../../sql-reference/data-types/int-uint.md), [Float](../../../sql-reference/data-types/float.md) or [Decimal](../../../sql-reference/data-types/decimal.md).

**Returned values**

- Returnes an [exponentially smoothed moving average](https://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average) of the values for the past `x` time at the latest point of time.

Type: [Float64](../../../sql-reference/data-types/float.md#float32-float64).

**Examples**

Input table:

``` text
┌──temperature─┬─timestamp──┐
│          95  │         1  │
│          95  │         2  │
│          95  │         3  │
│          96  │         4  │
│          96  │         5  │
│          96  │         6  │
│          96  │         7  │
│          97  │         8  │
│          97  │         9  │
│          97  │        10  │
│          97  │        11  │
│          98  │        12  │
│          98  │        13  │
│          98  │        14  │
│          98  │        15  │
│          99  │        16  │
│          99  │        17  │
│          99  │        18  │
│         100  │        19  │
│         100  │        20  │
└──────────────┴────────────┘
```

Query: 

```sql
SELECT exponentialMovingAverage(5)(temperature, timestamp);
```

Result:

``` text
┌──exponentialMovingAverage(5)(temperature, timestamp)──┐
│                                    92.25779635374204  │
└───────────────────────────────────────────────────────┘
```

Query: 

```sql
SELECT
    value,
    time,
    round(exp_smooth, 3),
    bar(exp_smooth, 0, 1, 50) AS bar
FROM
(
    SELECT
        (number = 0) OR (number >= 25) AS value,
        number AS time,
        exponentialMovingAverage(10)(value, time) OVER (Rows BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS exp_smooth
    FROM numbers(50)
)
```

Result:

``` text
┌─value─┬─time─┬─round(exp_smooth, 3)─┬─bar────────────────────────────────────────┐
│     1 │    0 │                0.067 │ ███▎                                      │
│     0 │    1 │                0.062 │ ███                                        │
│     0 │    2 │                0.058 │ ██▊                                       │
│     0 │    3 │                0.054 │ ██▋                                       │
│     0 │    4 │                0.051 │ ██▌                                        │
│     0 │    5 │                0.047 │ ██▎                                       │
│     0 │    6 │                0.044 │ ██▏                                       │
│     0 │    7 │                0.041 │ ██                                         │
│     0 │    8 │                0.038 │ █▊                                        │
│     0 │    9 │                0.036 │ █▋                                        │
│     0 │   10 │                0.033 │ █▋                                        │
│     0 │   11 │                0.031 │ █▌                                         │
│     0 │   12 │                0.029 │ █▍                                        │
│     0 │   13 │                0.027 │ █▎                                        │
│     0 │   14 │                0.025 │ █▎                                        │
│     0 │   15 │                0.024 │ █▏                                        │
│     0 │   16 │                0.022 │ █                                          │
│     0 │   17 │                0.021 │ █                                          │
│     0 │   18 │                0.019 │ ▊                                         │
│     0 │   19 │                0.018 │ ▊                                         │
│     0 │   20 │                0.017 │ ▋                                         │
│     0 │   21 │                0.016 │ ▋                                         │
│     0 │   22 │                0.015 │ ▋                                         │
│     0 │   23 │                0.014 │ ▋                                         │
│     0 │   24 │                0.013 │ ▋                                         │
│     1 │   25 │                0.079 │ ███▊                                      │
│     1 │   26 │                 0.14 │ ███████                                    │
│     1 │   27 │                0.198 │ █████████▊                                │
│     1 │   28 │                0.252 │ ████████████▌                              │
│     1 │   29 │                0.302 │ ███████████████                            │
│     1 │   30 │                0.349 │ █████████████████▍                        │
│     1 │   31 │                0.392 │ ███████████████████▌                       │
│     1 │   32 │                0.433 │ █████████████████████▋                    │
│     1 │   33 │                0.471 │ ███████████████████████▌                   │
│     1 │   34 │                0.506 │ █████████████████████████▎                │
│     1 │   35 │                0.539 │ ██████████████████████████▊               │
│     1 │   36 │                 0.57 │ ████████████████████████████▌              │
│     1 │   37 │                0.599 │ █████████████████████████████▊            │
│     1 │   38 │                0.626 │ ███████████████████████████████▎          │
│     1 │   39 │                0.651 │ ████████████████████████████████▌          │
│     1 │   40 │                0.674 │ █████████████████████████████████▋        │
│     1 │   41 │                0.696 │ ██████████████████████████████████▋       │
│     1 │   42 │                0.716 │ ███████████████████████████████████▋      │
│     1 │   43 │                0.735 │ ████████████████████████████████████▋     │
│     1 │   44 │                0.753 │ █████████████████████████████████████▋    │
│     1 │   45 │                 0.77 │ ██████████████████████████████████████▍   │
│     1 │   46 │                0.785 │ ███████████████████████████████████████▎  │
│     1 │   47 │                  0.8 │ ███████████████████████████████████████▊  │
│     1 │   48 │                0.813 │ ████████████████████████████████████████▋ │
│     1 │   49 │                0.825 │ █████████████████████████████████████████▎│
└───────┴──────┴──────────────────────┴────────────────────────────────────────────┘
```
