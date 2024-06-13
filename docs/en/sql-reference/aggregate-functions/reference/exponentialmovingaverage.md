---
slug: /en/sql-reference/aggregate-functions/reference/exponentialmovingaverage
sidebar_position: 108
sidebar_title: exponentialMovingAverage
---

## exponentialMovingAverage

Calculates the exponential moving average of values for the determined time.

**Syntax**

```sql
exponentialMovingAverage(x)(value, timeunit)
```

Each `value` corresponds to the determinate `timeunit`. The half-life `x` is the time lag at which the exponential weights decay by one-half. The function returns a weighted average: the older the time point, the less weight the corresponding value is considered to be.

**Arguments**

- `value` — Value. [Integer](../../../sql-reference/data-types/int-uint.md), [Float](../../../sql-reference/data-types/float.md) or [Decimal](../../../sql-reference/data-types/decimal.md).
- `timeunit` — Timeunit. [Integer](../../../sql-reference/data-types/int-uint.md), [Float](../../../sql-reference/data-types/float.md) or [Decimal](../../../sql-reference/data-types/decimal.md). Timeunit is not timestamp (seconds), it's -- an index of the time interval. Can be calculated using [intDiv](../../functions/arithmetic-functions.md#intdiva-b).

**Parameters**

- `x` — Half-life period. [Integer](../../../sql-reference/data-types/int-uint.md), [Float](../../../sql-reference/data-types/float.md) or [Decimal](../../../sql-reference/data-types/decimal.md).

**Returned values**

- Returns an [exponentially smoothed moving average](https://en.wikipedia.org/wiki/Moving_average#Exponential_moving_average) of the values for the past `x` time at the latest point of time.

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
```

```sql
CREATE TABLE data
ENGINE = Memory AS
SELECT
    10 AS value,
    toDateTime('2020-01-01') + (3600 * number) AS time
FROM numbers_mt(10);


-- Calculate timeunit using intDiv
SELECT
    value,
    time,
    exponentialMovingAverage(1)(value, intDiv(toUInt32(time), 3600)) OVER (ORDER BY time ASC) AS res,
    intDiv(toUInt32(time), 3600) AS timeunit
FROM data
ORDER BY time ASC;

┌─value─┬────────────────time─┬─────────res─┬─timeunit─┐
│    10 │ 2020-01-01 00:00:00 │           5 │   438288 │
│    10 │ 2020-01-01 01:00:00 │         7.5 │   438289 │
│    10 │ 2020-01-01 02:00:00 │        8.75 │   438290 │
│    10 │ 2020-01-01 03:00:00 │       9.375 │   438291 │
│    10 │ 2020-01-01 04:00:00 │      9.6875 │   438292 │
│    10 │ 2020-01-01 05:00:00 │     9.84375 │   438293 │
│    10 │ 2020-01-01 06:00:00 │    9.921875 │   438294 │
│    10 │ 2020-01-01 07:00:00 │   9.9609375 │   438295 │
│    10 │ 2020-01-01 08:00:00 │  9.98046875 │   438296 │
│    10 │ 2020-01-01 09:00:00 │ 9.990234375 │   438297 │
└───────┴─────────────────────┴─────────────┴──────────┘


-- Calculate timeunit using toRelativeHourNum
SELECT
    value,
    time,
    exponentialMovingAverage(1)(value, toRelativeHourNum(time)) OVER (ORDER BY time ASC) AS res,
    toRelativeHourNum(time) AS timeunit
FROM data
ORDER BY time ASC;

┌─value─┬────────────────time─┬─────────res─┬─timeunit─┐
│    10 │ 2020-01-01 00:00:00 │           5 │   438288 │
│    10 │ 2020-01-01 01:00:00 │         7.5 │   438289 │
│    10 │ 2020-01-01 02:00:00 │        8.75 │   438290 │
│    10 │ 2020-01-01 03:00:00 │       9.375 │   438291 │
│    10 │ 2020-01-01 04:00:00 │      9.6875 │   438292 │
│    10 │ 2020-01-01 05:00:00 │     9.84375 │   438293 │
│    10 │ 2020-01-01 06:00:00 │    9.921875 │   438294 │
│    10 │ 2020-01-01 07:00:00 │   9.9609375 │   438295 │
│    10 │ 2020-01-01 08:00:00 │  9.98046875 │   438296 │
│    10 │ 2020-01-01 09:00:00 │ 9.990234375 │   438297 │
└───────┴─────────────────────┴─────────────┴──────────┘
```
