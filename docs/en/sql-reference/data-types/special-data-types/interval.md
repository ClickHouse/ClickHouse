---
toc_priority: 61
toc_title: Interval
---

# Interval {#data-type-interval}

The family of data types representing time and date intervals. The resulting types of the [INTERVAL](../../../sql-reference/operators/index.md#operator-interval) operator.

!!! warning "Warning"
    `Interval` data type values can’t be stored in tables.

Structure:

-   Time interval as an unsigned integer value.
-   Type of an interval.

Supported interval types:

-   `SECOND`
-   `MINUTE`
-   `HOUR`
-   `DAY`
-   `WEEK`
-   `MONTH`
-   `QUARTER`
-   `YEAR`

For each interval type, there is a separate data type. For example, the `DAY` interval corresponds to the `IntervalDay` data type:

``` sql
SELECT toTypeName(INTERVAL 4 DAY)
```

``` text
┌─toTypeName(toIntervalDay(4))─┐
│ IntervalDay                  │
└──────────────────────────────┘
```

## Usage Remarks {#data-type-interval-usage-remarks}

You can use `Interval`-type values in arithmetical operations with [Date](../../../sql-reference/data-types/date.md) and [DateTime](../../../sql-reference/data-types/datetime.md)-type values. For example, you can add 4 days to the current time:

``` sql
SELECT now() as current_date_time, current_date_time + INTERVAL 4 DAY
```

``` text
┌───current_date_time─┬─plus(now(), toIntervalDay(4))─┐
│ 2019-10-23 10:58:45 │           2019-10-27 10:58:45 │
└─────────────────────┴───────────────────────────────┘
```

Intervals with different types can’t be combined. You can’t use intervals like `4 DAY 1 HOUR`. Specify intervals in units that are smaller or equal to the smallest unit of the interval, for example, the interval `1 day and an hour` interval can be expressed as `25 HOUR` or `90000 SECOND`.

You can’t perform arithmetical operations with `Interval`-type values, but you can add intervals of different types consequently to values in `Date` or `DateTime` data types. For example:

``` sql
SELECT now() AS current_date_time, current_date_time + INTERVAL 4 DAY + INTERVAL 3 HOUR
```

``` text
┌───current_date_time─┬─plus(plus(now(), toIntervalDay(4)), toIntervalHour(3))─┐
│ 2019-10-23 11:16:28 │                                    2019-10-27 14:16:28 │
└─────────────────────┴────────────────────────────────────────────────────────┘
```

The following query causes an exception:

``` sql
select now() AS current_date_time, current_date_time + (INTERVAL 4 DAY + INTERVAL 3 HOUR)
```

``` text
Received exception from server (version 19.14.1):
Code: 43. DB::Exception: Received from localhost:9000. DB::Exception: Wrong argument types for function plus: if one argument is Interval, then another must be Date or DateTime..
```

## See Also {#see-also}

-   [INTERVAL](../../../sql-reference/operators/index.md#operator-interval) operator
-   [toInterval](../../../sql-reference/functions/type-conversion-functions.md#function-tointerval) type conversion functions
