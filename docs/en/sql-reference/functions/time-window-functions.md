---
slug: /en/sql-reference/functions/time-window-functions
sidebar_position: 175
sidebar_label: Time Window
---

# Time Window Functions

Time window functions return the inclusive lower and exclusive upper bound of the corresponding window. The functions for working with WindowView are listed below:

## tumble

A tumbling time window assigns records to non-overlapping, continuous windows with a fixed duration (`interval`). 

``` sql
tumble(time_attr, interval [, timezone])
```

**Arguments**
- `time_attr` - Date and time. [DateTime](../../sql-reference/data-types/datetime.md) data type.
- `interval` - Window interval in [Interval](../../sql-reference/data-types/special-data-types/interval.md) data type.
- `timezone` — [Timezone name](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone) (optional). 

**Returned values**

- The inclusive lower and exclusive upper bound of the corresponding tumbling window.

Type: `Tuple(DateTime, DateTime)`

**Example**

Query:

``` sql
SELECT tumble(now(), toIntervalDay('1'))
```

Result:

``` text
┌─tumble(now(), toIntervalDay('1'))─────────────┐
│ ['2020-01-01 00:00:00','2020-01-02 00:00:00'] │
└───────────────────────────────────────────────┘
```

## hop

A hopping time window has a fixed duration (`window_interval`) and hops by a specified hop interval (`hop_interval`). If the `hop_interval` is smaller than the `window_interval`, hopping windows are overlapping. Thus, records can be assigned to multiple windows. 

``` sql
hop(time_attr, hop_interval, window_interval [, timezone])
```

**Arguments**

- `time_attr` - Date and time. [DateTime](../../sql-reference/data-types/datetime.md) data type.
- `hop_interval` - Hop interval in [Interval](../../sql-reference/data-types/special-data-types/interval.md) data type. Should be a positive number.
- `window_interval` - Window interval in [Interval](../../sql-reference/data-types/special-data-types/interval.md) data type. Should be a positive number.
- `timezone` — [Timezone name](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone) (optional). 

**Returned values**

- The inclusive lower and exclusive upper bound of the corresponding hopping window. Since one record can be assigned to multiple hop windows, the function only returns the bound of the **first** window when hop function is used **without** `WINDOW VIEW`.

Type: `Tuple(DateTime, DateTime)`

**Example**

Query:

``` sql
SELECT hop(now(), INTERVAL '1' SECOND, INTERVAL '2' SECOND)
```

Result:

``` text
┌─hop(now(), toIntervalSecond('1'), toIntervalSecond('2'))──┐
│ ('2020-01-14 16:58:22','2020-01-14 16:58:24')             │
└───────────────────────────────────────────────────────────┘
```

## tumbleStart

Returns the inclusive lower bound of the corresponding tumbling window.

``` sql
tumbleStart(bounds_tuple);
tumbleStart(time_attr, interval [, timezone]);
```

## tumbleEnd

Returns the exclusive upper bound of the corresponding tumbling window.

``` sql
tumbleEnd(bounds_tuple);
tumbleEnd(time_attr, interval [, timezone]);
```

## hopStart

Returns the inclusive lower bound of the corresponding hopping window.

``` sql
hopStart(bounds_tuple);
hopStart(time_attr, hop_interval, window_interval [, timezone]);
```

## hopEnd

Returns the exclusive upper bound of the corresponding hopping window.

``` sql
hopEnd(bounds_tuple);
hopEnd(time_attr, hop_interval, window_interval [, timezone]);
```

## Related content

- Blog: [Working with time series data in ClickHouse](https://clickhouse.com/blog/working-with-time-series-data-and-functions-ClickHouse)
