---
slug: /en/sql-reference/functions/time-window-functions
sidebar_position: 175
sidebar_label: Time Window
---

# Time Window Functions

Time window functions return the inclusive lower and exclusive upper bound of the corresponding window. The functions for working with [WindowView](../statements/create/view.md/#window-view-experimental) are listed below:

## tumble

A tumbling time window assigns records to non-overlapping, continuous windows with a fixed duration (`interval`).

**Syntax**

``` sql
tumble(time_attr, interval [, timezone])
```

**Arguments**
- `time_attr` — Date and time. [DateTime](../data-types/datetime.md).
- `interval` — Window interval in [Interval](../data-types/special-data-types/interval.md).
- `timezone` — [Timezone name](../../operations/server-configuration-parameters/settings.md#timezone) (optional).

**Returned values**

- The inclusive lower and exclusive upper bound of the corresponding tumbling window. [Tuple](../data-types/tuple.md)([DateTime](../data-types/datetime.md), [DateTime](../data-types/datetime.md)).

**Example**

Query:

``` sql
SELECT tumble(now(), toIntervalDay('1'));
```

Result:

``` text
┌─tumble(now(), toIntervalDay('1'))─────────────┐
│ ('2024-07-04 00:00:00','2024-07-05 00:00:00') │
└───────────────────────────────────────────────┘
```

## tumbleStart

Returns the inclusive lower bound of the corresponding [tumbling window](#tumble).

**Syntax**

``` sql
tumbleStart(time_attr, interval [, timezone]);
```

**Arguments**

- `time_attr` — Date and time. [DateTime](../data-types/datetime.md).
- `interval` — Window interval in [Interval](../data-types/special-data-types/interval.md).
- `timezone` — [Timezone name](../../operations/server-configuration-parameters/settings.md#timezone) (optional).

The parameters above can also be passed to the function as a [tuple](../data-types/tuple.md).

**Returned values**

- The inclusive lower bound of the corresponding tumbling window. [DateTime](../data-types/datetime.md), [Tuple](../data-types/tuple.md) or [UInt32](../data-types/int-uint.md).

**Example**

Query:

```sql
SELECT tumbleStart(now(), toIntervalDay('1'));
```

Result:

```response
┌─tumbleStart(now(), toIntervalDay('1'))─┐
│                    2024-07-04 00:00:00 │
└────────────────────────────────────────┘
```

## tumbleEnd

Returns the exclusive upper bound of the corresponding [tumbling window](#tumble).

**Syntax**

``` sql
tumbleEnd(time_attr, interval [, timezone]);
```

**Arguments**

- `time_attr` — Date and time. [DateTime](../data-types/datetime.md).
- `interval` — Window interval in [Interval](../data-types/special-data-types/interval.md).
- `timezone` — [Timezone name](../../operations/server-configuration-parameters/settings.md#timezone) (optional).

The parameters above can also be passed to the function as a [tuple](../data-types/tuple.md).

**Returned values**

- The inclusive lower bound of the corresponding tumbling window. [DateTime](../data-types/datetime.md), [Tuple](../data-types/tuple.md) or [UInt32](../data-types/int-uint.md).

**Example**

Query:

```sql
SELECT tumbleEnd(now(), toIntervalDay('1'));
```

Result:

```response
┌─tumbleEnd(now(), toIntervalDay('1'))─┐
│                  2024-07-05 00:00:00 │
└──────────────────────────────────────┘
```

## hop

A hopping time window has a fixed duration (`window_interval`) and hops by a specified hop interval (`hop_interval`). If the `hop_interval` is smaller than the `window_interval`, hopping windows are overlapping. Thus, records can be assigned to multiple windows.

``` sql
hop(time_attr, hop_interval, window_interval [, timezone])
```

**Arguments**

- `time_attr` — Date and time. [DateTime](../data-types/datetime.md).
- `hop_interval` — Positive Hop interval. [Interval](../data-types/special-data-types/interval.md).
- `window_interval` — Positive Window interval. [Interval](../data-types/special-data-types/interval.md).
- `timezone` — [Timezone name](../../operations/server-configuration-parameters/settings.md#timezone) (optional).

**Returned values**

- The inclusive lower and exclusive upper bound of the corresponding hopping window. [Tuple](../data-types/tuple.md)([DateTime](../data-types/datetime.md), [DateTime](../data-types/datetime.md))`.

:::note
Since one record can be assigned to multiple hop windows, the function only returns the bound of the **first** window when hop function is used **without** `WINDOW VIEW`.
:::

**Example**

Query:

``` sql
SELECT hop(now(), INTERVAL '1' DAY, INTERVAL '2' DAY);
```

Result:

``` text
┌─hop(now(), toIntervalDay('1'), toIntervalDay('2'))─┐
│ ('2024-07-03 00:00:00','2024-07-05 00:00:00')      │
└────────────────────────────────────────────────────┘
```

## hopStart

Returns the inclusive lower bound of the corresponding [hopping window](#hop).

**Syntax**

``` sql
hopStart(time_attr, hop_interval, window_interval [, timezone]);
```
**Arguments**

- `time_attr` — Date and time. [DateTime](../data-types/datetime.md).
- `hop_interval` — Positive Hop interval. [Interval](../data-types/special-data-types/interval.md).
- `window_interval` — Positive Window interval. [Interval](../data-types/special-data-types/interval.md).
- `timezone` — [Timezone name](../../operations/server-configuration-parameters/settings.md#timezone) (optional).

The parameters above can also be passed to the function as a [tuple](../data-types/tuple.md).

**Returned values**

- The inclusive lower bound of the corresponding hopping window. [DateTime](../data-types/datetime.md), [Tuple](../data-types/tuple.md) or [UInt32](../data-types/int-uint.md).

:::note
Since one record can be assigned to multiple hop windows, the function only returns the bound of the **first** window when hop function is used **without** `WINDOW VIEW`.
:::

**Example**

Query:

``` sql
SELECT hopStart(now(), INTERVAL '1' DAY, INTERVAL '2' DAY);
```

Result:

``` text
┌─hopStart(now(), toIntervalDay('1'), toIntervalDay('2'))─┐
│                                     2024-07-03 00:00:00 │
└─────────────────────────────────────────────────────────┘
```

## hopEnd

Returns the exclusive upper bound of the corresponding [hopping window](#hop).

**Syntax**

``` sql
hopEnd(time_attr, hop_interval, window_interval [, timezone]);
```
**Arguments**

- `time_attr` — Date and time. [DateTime](../data-types/datetime.md).
- `hop_interval` — Positive Hop interval. [Interval](../data-types/special-data-types/interval.md).
- `window_interval` — Positive Window interval. [Interval](../data-types/special-data-types/interval.md).
- `timezone` — [Timezone name](../../operations/server-configuration-parameters/settings.md#timezone) (optional).

The parameters above can also be passed to the function as a [tuple](../data-types/tuple.md).

**Returned values**

- The exclusive upper bound of the corresponding hopping window. [DateTime](../data-types/datetime.md), [Tuple](../data-types/tuple.md) or [UInt32](../data-types/int-uint.md).

:::note
Since one record can be assigned to multiple hop windows, the function only returns the bound of the **first** window when hop function is used **without** `WINDOW VIEW`.
:::

**Example**

Query:

``` sql
SELECT hopEnd(now(), INTERVAL '1' DAY, INTERVAL '2' DAY);
```

Result:

``` text
┌─hopEnd(now(), toIntervalDay('1'), toIntervalDay('2'))─┐
│                                   2024-07-05 00:00:00 │
└───────────────────────────────────────────────────────┘

```

## Related content

- Blog: [Working with time series data in ClickHouse](https://clickhouse.com/blog/working-with-time-series-data-and-functions-ClickHouse)
