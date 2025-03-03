---
slug: /sql-reference/data-types/time
sidebar_position: 15
sidebar_label: Time
---

# Time

The `Time` data type is used to store a time-of-day value independent of any calendar date. It is ideal for representing daily schedules, event times, or any situation where only the time component (hours, minutes, seconds, and optionally fractions of a second) is important.

Syntax:

``` sql
Time([timezone])
```

Supported range of values: \[-999:59:59, 999:59:59\].

Resolution: 1 second.

## Speed {#speed}

The `Date` data type is faster than `Time` under _most_ conditions. But the `Time` data type is around the same as `DateTime` data type.

Due to the implementation delatils, the `Time` and `DateTime` type requires 4 bytes of storage, while `Date` requires 2 bytes. However, when the database compresses the database, this difference is amplified.

## Usage Remarks {#usage-remarks}

The point in time is saved as a [Unix timestamp](https://en.wikipedia.org/wiki/Unix_time), regardless of the time zone or daylight saving time.

**Note:** The Time data type does not observe time zones. It represents a time‐of‐day value on its own, without any date or regional offset context. Attempting to apply or change a time zone on Time columns has no effect and is not supported.

## Examples {#examples}

**1.** Creating a table with a `Time`-type column and inserting data into it:

``` sql
CREATE TABLE dt
(
    `time` Time,
    `event_id` UInt8
)
ENGINE = TinyLog;
```

``` sql
-- Parse Time
-- - from string,
-- - from integer interpreted as number of seconds since 1970-01-01.
INSERT INTO dt VALUES ('100:00:00', 1), (12453, 3);

SELECT * FROM dt;
```

``` text
   ┌──────time─┬─event_id─┐
1. │ 100:00:00 │        1 │
2. │ 003:27:33 │        3 │
   └───────────┴──────────┘
```

**2.** Filtering on `Time` values

``` sql
SELECT * FROM dt WHERE time = toTime('100:00:00')
```

``` text
   ┌──────time─┬─event_id─┐
1. │ 100:00:00 │        1 │
   └───────────┴──────────┘
```

`Time` column values can be filtered using a string value in `WHERE` predicate. It will be converted to `Time` automatically:

``` sql
SELECT * FROM dt WHERE time = '100:00:00'
```

``` text
   ┌──────time─┬─event_id─┐
1. │ 100:00:00 │        1 │
   └───────────┴──────────┘
```

**3.** Getting a time zone for a `Time`-type column:

``` sql
SELECT toTime(now()) AS column, toTypeName(column) AS x
```

``` text
   ┌────column─┬─x────┐
1. │ 018:55:15 │ Time │
   └───────────┴──────┘
```


## See Also {#see-also}

- [Type conversion functions](../functions/type-conversion-functions.md)
- [Functions for working with dates and times](../functions/date-time-functions.md)
- [Functions for working with arrays](../functions/array-functions.md)
- [The `date_time_input_format` setting](../../operations/settings/settings-formats.md#date_time_input_format)
- [The `date_time_output_format` setting](../../operations/settings/settings-formats.md#date_time_output_format)
- [The `timezone` server configuration parameter](../../operations/server-configuration-parameters/settings.md#timezone)
- [The `session_timezone` setting](../../operations/settings/settings.md#session_timezone)
- [Operators for working with dates and times](../operators/index.md#operators-datetime)
- [The `DateTime` data type](datetime.md)
- [The `Date` data type](date.md)
