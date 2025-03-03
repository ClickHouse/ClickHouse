---
slug: /sql-reference/data-types/time64
sidebar_position: 17
sidebar_label: Time64
---

# Time64

The Time64 data type allows storing time-of-day values with sub-second precision. Unlike DateTime64, it does not include a calendar date, but only represents time within a single day. The precision defines the resolution of stored values in fractional seconds.

Tick size (precision): 10<sup>-precision</sup> seconds. Valid range: [ 0 : 9 ].
Typically, are used - 3 (milliseconds), 6 (microseconds), 9 (nanoseconds).

**Syntax:**

``` sql
Time64(precision, [timezone])
```

Internally, Time64 stores data as an Int64 number of ticks since the start of the day (000:00:00.000000000). The tick resolution is determined by the precision parameter. Optionally, a time zone can be specified at the column level, which affects how time values are interpreted and displayed in text format.

Unlike DateTime64, Time64 does not store a date component, meaning that it only represents a time within a 24-hour cycle. See details in [Time](../../sql-reference/data-types/time.md).

Supported range of values: \[000:00:00, 999:59:59.99999999\]

## Examples {#examples}

1. Creating a table with `Time64`-type column and inserting data into it:

``` sql
CREATE TABLE t64
(
    `timestamp` Time64(3),
    `event_id` UInt8
)
ENGINE = TinyLog;
```

``` sql
-- Parse Time
-- - from integer interpreted as number of seconds since 1970-01-01.
-- - from string,
INSERT INTO t64 VALUES (15463123, 1), (154600.123, 2), ('100:00:00', 3);

SELECT * FROM t64;
```

``` text
   ┌─────timestamp─┬─event_id─┐
1. │ 004:17:43.123 │        1 │
2. │ 042:56:40.123 │        2 │
3. │ 100:00:00.000 │        3 │
   └───────────────┴──────────┘
```

2. Filtering on `Time64` values

``` sql
SELECT * FROM t64 WHERE timestamp = toTime64('100:00:00', 3);
```

``` text
   ┌─────timestamp─┬─event_id─┐
1. │ 100:00:00.000 │        3 │
   └───────────────┴──────────┘
```

Unlike `Time`, `Time64` values are not converted from `String` automatically.

``` sql
SELECT * FROM t64 WHERE timestamp = toTime64(154600.123, 3);
```

``` text
   ┌─────timestamp─┬─event_id─┐
1. │ 042:56:40.123 │        2 │
   └───────────────┴──────────┘
```

Contrary to inserting, the `toTime64` function will treat all values as the decimal variant, so precision needs to
be given after the decimal point.

3. Getting a time zone for a `Time64`-type value:

``` sql
SELECT toTime64(now(), 3) AS column, toTypeName(column) AS x;
```

``` text
   ┌────────column─┬─x─────────┐
1. │ 019:14:16.000 │ Time64(3) │
   └───────────────┴───────────┘
```


**See Also**

- [Type conversion functions](../../sql-reference/functions/type-conversion-functions.md)
- [Functions for working with dates and times](../../sql-reference/functions/date-time-functions.md)
- [The `date_time_input_format` setting](../../operations/settings/settings-formats.md#date_time_input_format)
- [The `date_time_output_format` setting](../../operations/settings/settings-formats.md#date_time_output_format)
- [The `timezone` server configuration parameter](../../operations/server-configuration-parameters/settings.md#timezone)
- [The `session_timezone` setting](../../operations/settings/settings.md#session_timezone)
- [Operators for working with dates and times](../../sql-reference/operators/index.md#operators-for-working-with-dates-and-times)
- [`Date` data type](../../sql-reference/data-types/date.md)
- [`Time` data type](../../sql-reference/data-types/time.md)
- [`DateTime` data type](../../sql-reference/data-types/datetime.md)
