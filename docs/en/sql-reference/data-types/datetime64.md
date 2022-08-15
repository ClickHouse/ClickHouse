---
sidebar_position: 49
sidebar_label: DateTime64
---

# Datetime64

Allows to store an instant in time, that can be expressed as a calendar date and a time of a day, with defined sub-second precision

Tick size (precision): 10<sup>-precision</sup> seconds. Valid range: [ 0 : 9 ].
Typically are used - 3 (milliseconds), 6 (microseconds), 9 (nanoseconds).

**Syntax:**

``` sql
DateTime64(precision, [timezone])
```

Internally, stores data as a number of ‘ticks’ since epoch start (1970-01-01 00:00:00 UTC) as Int64. The tick resolution is determined by the precision parameter. Additionally, the `DateTime64` type can store time zone that is the same for the entire column, that affects how the values of the `DateTime64` type values are displayed in text format and how the values specified as strings are parsed (‘2020-01-01 05:00:01.000’). The time zone is not stored in the rows of the table (or in resultset), but is stored in the column metadata. See details in [DateTime](../../sql-reference/data-types/datetime.md).

Supported range of values: \[1900-01-01 00:00:00, 2299-12-31 23:59:59.99999999\] (Note: The precision of the maximum value is 8).

## Examples

1. Creating a table with `DateTime64`-type column and inserting data into it:

``` sql
CREATE TABLE dt
(
    `timestamp` DateTime64(3, 'Asia/Istanbul'),
    `event_id` UInt8
)
ENGINE = TinyLog;
```

``` sql
INSERT INTO dt Values (1546300800123, 1), (1546300800.123, 2), ('2019-01-01 00:00:00', 3);
```

``` sql
SELECT * FROM dt;
```

``` text
┌───────────────timestamp─┬─event_id─┐
│ 2019-01-01 03:00:00.123 │        1 │
│ 2019-01-01 03:00:00.123 │        2 │
│ 2019-01-01 00:00:00.000 │        3 │
└─────────────────────────┴──────────┘
```

-   When inserting datetime as an integer, it is treated as an appropriately scaled Unix Timestamp (UTC). `1546300800000` (with precision 3) represents `'2019-01-01 00:00:00'` UTC. However, as `timestamp` column has `Asia/Istanbul` (UTC+3) timezone specified, when outputting as a string the value will be shown as `'2019-01-01 03:00:00'`. Inserting datetime as a decimal will treat it similarly as an integer, except the value before the decimal point is the Unix Timestamp up to and including the seconds, and after the decimal point will be treated as the precision.
-   When inserting string value as datetime, it is treated as being in column timezone. `'2019-01-01 00:00:00'` will be treated as being in `Asia/Istanbul` timezone and stored as `1546290000000`.

2. Filtering on `DateTime64` values

``` sql
SELECT * FROM dt WHERE timestamp = toDateTime64('2019-01-01 00:00:00', 3, 'Asia/Istanbul');
```

``` text
┌───────────────timestamp─┬─event_id─┐
│ 2019-01-01 00:00:00.000 │        2 │
└─────────────────────────┴──────────┘
```

Unlike `DateTime`, `DateTime64` values are not converted from `String` automatically.

``` sql
SELECT * FROM dt WHERE timestamp = toDateTime64(1546300800.123, 3);
```

``` text
┌───────────────timestamp─┬─event_id─┐
│ 2019-01-01 00:00:00.123 │        1 │
│ 2019-01-01 00:00:00.123 │        2 │
└─────────────────────────┴──────────┘
```

Contrary to inserting, the `toDateTime64` function will treat all values as the decimal variant, so precision needs to
be given after the decimal point.

3. Getting a time zone for a `DateTime64`-type value:

``` sql
SELECT toDateTime64(now(), 3, 'Asia/Istanbul') AS column, toTypeName(column) AS x;
```

``` text
┌──────────────────column─┬─x──────────────────────────────┐
│ 2019-10-16 04:12:04.000 │ DateTime64(3, 'Asia/Istanbul') │
└─────────────────────────┴────────────────────────────────┘
```

4. Timezone conversion

``` sql
SELECT
toDateTime64(timestamp, 3, 'Europe/London') as lon_time,
toDateTime64(timestamp, 3, 'Asia/Istanbul') as mos_time
FROM dt;
```

``` text
┌───────────────lon_time──┬────────────────mos_time─┐
│ 2019-01-01 00:00:00.000 │ 2019-01-01 03:00:00.000 │
│ 2018-12-31 21:00:00.000 │ 2019-01-01 00:00:00.000 │
└─────────────────────────┴─────────────────────────┘
```

**See Also**

-   [Type conversion functions](../../sql-reference/functions/type-conversion-functions.md)
-   [Functions for working with dates and times](../../sql-reference/functions/date-time-functions.md)
-   [Functions for working with arrays](../../sql-reference/functions/array-functions.md)
-   [The `date_time_input_format` setting](../../operations/settings/settings.md#settings-date_time_input_format)
-   [The `date_time_output_format` setting](../../operations/settings/settings.md#settings-date_time_output_format)
-   [The `timezone` server configuration parameter](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone)
-   [Operators for working with dates and times](../../sql-reference/operators/index.md#operators-datetime)
-   [`Date` data type](../../sql-reference/data-types/date.md)
-   [`DateTime` data type](../../sql-reference/data-types/datetime.md)
