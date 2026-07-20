---
description: 'Documentation for the DateTime64 data type in ClickHouse, which stores
  timestamps with sub-second precision'
sidebar_label: 'DateTime64'
sidebar_position: 18
slug: /sql-reference/data-types/datetime64
title: 'DateTime64'
doc_type: 'reference'
---

Allows to store an instant in time, that can be expressed as a calendar date and a time of a day, with defined sub-second precision

Tick size (precision): 10<sup>-precision</sup> seconds. Valid range: [ 0 : 9 ].
Typically, are used - 3 (milliseconds), 6 (microseconds), 9 (nanoseconds).

Default value: 3 (milliseconds).

**Syntax:**

```sql
DateTime64(precision, [timezone])
```

Internally, stores data as a number of 'ticks' since epoch start (1970-01-01 00:00:00 UTC) as Int64. The tick resolution is determined by the precision parameter. Additionally, the `DateTime64` type can store time zone that is the same for the entire column, that affects how the values of the `DateTime64` type values are displayed in text format and how the values specified as strings are parsed ('2020-01-01 05:00:01.000'). The time zone is not stored in the rows of the table (or in resultset), but is stored in the column metadata. See details in [DateTime](../../sql-reference/data-types/datetime.md).

Supported range of values: \[0000-01-01 00:00:00, 9999-12-31 23:59:59.999999999\]

The number of digits after the decimal point depends on the precision parameter.

Note: The full range above is available for precisions up to 7. Because ticks are stored in an `Int64`, higher precisions cover a narrower range: with precision 8 the maximum value is around `4892-10-07`, and with the maximum precision of 9 digits (nanoseconds) the supported range is `1677-09-21 00:12:44` to `2262-04-11 23:47:16` in UTC.

## Examples {#examples}

1. Creating a table with `DateTime64`-type column and insert data into it:

```sql
CREATE TABLE dt64
(
    `timestamp` DateTime64(3, 'Asia/Istanbul'),
    `event_id` UInt8
)
ENGINE = MergeTree;
```

```sql
-- Parse DateTime64
-- - from an integer interpreted as the number of seconds since 1970-01-01 (like DateTime),
-- - from a decimal interpreted as the number of seconds, the fractional part giving sub-second precision,
-- - from a string.

INSERT INTO dt64
VALUES
(1546300800, 1),
(1546300800.123, 2),
('2019-01-01 00:00:00', 3);

SELECT * FROM dt64;
```

```text
┌───────────────timestamp─┬─event_id─┐
│ 2019-01-01 03:00:00.000 │        1 │
│ 2019-01-01 03:00:00.123 │        2 │
│ 2019-01-01 00:00:00.000 │        3 │
└─────────────────────────┴──────────┘
```

- When inserting datetime as a number, it is treated as a Unix Timestamp (UTC) in seconds, like [DateTime](../../sql-reference/data-types/datetime.md). `1546300800` represents `'2019-01-01 00:00:00'` UTC. However, as `timestamp` column has `Asia/Istanbul` (UTC+3) timezone specified, when outputting as a string the value will be shown as `'2019-01-01 03:00:00'`. Inserting a number with a fractional part works the same way: the part before the decimal point is the Unix Timestamp in seconds and the part after it provides sub-second precision according to the column's precision.

  :::note Backward incompatible change
  In versions before 26.7, a bare unquoted integer in the `JSON` and `Values`/`Quoted` input paths (the latter covering every format that parses fields with the `Quoted` escaping rule: `Values`, `MySQLDump`, and `Template`/`CustomSeparated`/`Regexp` configured with `Quoted` field escaping) was interpreted as the raw underlying value (the number of ticks at the column precision) rather than as a number of seconds, so `1546300800000` (at precision 3) meant `'2019-01-01 00:00:00'`. It is now interpreted as seconds since the epoch, consistent with the `Values` format, `CAST` and [toDateTime64](../../sql-reference/functions/type-conversion-functions.md#todatetime64). Quoted strings and ClickHouse's own (always quoted) output are unaffected. To restore the previous behavior in these paths, set `input_format_read_datetime_number_as_raw_value = 1` (or `SET compatibility = '26.6'`); this also affects the `JSONExtract` function and the `JSON` data type. Note that `JSONExtract` and the `JSON` data type parse a fractional number through `Float64`, so a timestamp with more digits than `Float64` preserves can round to the adjacent value, while the row input formats parse the original text exactly. The tab-separated, CSV and other escaped text input formats are not governed by this setting and keep their existing interpretation of an unquoted number (a large value is read as ticks).
  :::
- When inserting string value as datetime, it is treated as being in column timezone. `'2019-01-01 00:00:00'` will be treated as being in `Asia/Istanbul` timezone and stored as `1546290000000`.

2. Filtering on `DateTime64` values

```sql
SELECT * FROM dt64 WHERE timestamp = toDateTime64('2019-01-01 00:00:00', 3, 'Asia/Istanbul');
```

```text
┌───────────────timestamp─┬─event_id─┐
│ 2019-01-01 00:00:00.000 │        3 │
└─────────────────────────┴──────────┘
```

Unlike `DateTime`, `DateTime64` values are not converted from `String` automatically.

```sql
SELECT * FROM dt64 WHERE timestamp = toDateTime64(1546300800.123, 3);
```

```text
┌───────────────timestamp─┬─event_id─┐
│ 2019-01-01 03:00:00.123 │        1 │
│ 2019-01-01 03:00:00.123 │        2 │
└─────────────────────────┴──────────┘
```

As with inserting a number, the `toDateTime64` function treats a numeric argument as a number of seconds, so sub-second
precision needs to be given after the decimal point.

3. Getting a time zone for a `DateTime64`-type value:

```sql
SELECT toDateTime64(now(), 3, 'Asia/Istanbul') AS column, toTypeName(column) AS x;
```

```text
┌──────────────────column─┬─x──────────────────────────────┐
│ 2023-06-05 00:09:52.000 │ DateTime64(3, 'Asia/Istanbul') │
└─────────────────────────┴────────────────────────────────┘
```

4. Timezone conversion

```sql
SELECT
toDateTime64(timestamp, 3, 'Europe/London') AS lon_time,
toDateTime64(timestamp, 3, 'Asia/Istanbul') AS istanbul_time
FROM dt64;
```

```text
┌────────────────lon_time─┬───────────istanbul_time─┐
│ 2019-01-01 00:00:00.123 │ 2019-01-01 03:00:00.123 │
│ 2019-01-01 00:00:00.123 │ 2019-01-01 03:00:00.123 │
│ 2018-12-31 21:00:00.000 │ 2019-01-01 00:00:00.000 │
└─────────────────────────┴─────────────────────────┘
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
- [`DateTime` data type](../../sql-reference/data-types/datetime.md)
