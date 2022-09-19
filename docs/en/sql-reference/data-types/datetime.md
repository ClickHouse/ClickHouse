---
toc_priority: 48
toc_title: DateTime
---

# Datetime {#data_type-datetime}

Allows to store an instant in time, that can be expressed as a calendar date and a time of a day.

Syntax:

``` sql
DateTime([timezone])
```

Supported range of values: \[1970-01-01 00:00:00, 2106-02-07 06:28:15\].

Resolution: 1 second.

## Usage Remarks {#usage-remarks}

The point in time is saved as a [Unix timestamp](https://en.wikipedia.org/wiki/Unix_time), regardless of the time zone or daylight saving time. The time zone affects how the values of the `DateTime` type values are displayed in text format and how the values specified as strings are parsed (‘2020-01-01 05:00:01’).

Timezone agnostic unix timestamp is stored in tables, and the timezone is used to transform it to text format or back during data import/export or to make calendar calculations on the values (example: `toDate`, `toHour` functions et cetera). The time zone is not stored in the rows of the table (or in resultset), but is stored in the column metadata.

A list of supported time zones can be found in the [IANA Time Zone Database](https://www.iana.org/time-zones) and also can be queried by `SELECT * FROM system.time_zones`. [The list](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones) is also available at Wikipedia.

You can explicitly set a time zone for `DateTime`-type columns when creating a table. Example: `DateTime('UTC')`. If the time zone isn’t set, ClickHouse uses the value of the [timezone](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone) parameter in the server settings or the operating system settings at the moment of the ClickHouse server start.

The [clickhouse-client](../../interfaces/cli.md) applies the server time zone by default if a time zone isn’t explicitly set when initializing the data type. To use the client time zone, run `clickhouse-client` with the `--use_client_time_zone` parameter.

ClickHouse outputs values depending on the value of the [date_time_output_format](../../operations/settings/settings.md#settings-date_time_output_format) setting. `YYYY-MM-DD hh:mm:ss` text format by default. Additionaly you can change the output with the [formatDateTime](../../sql-reference/functions/date-time-functions.md#formatdatetime) function.

When inserting data into ClickHouse, you can use different formats of date and time strings, depending on the value of the [date_time_input_format](../../operations/settings/settings.md#settings-date_time_input_format) setting.

## Examples {#examples}

**1.** Creating a table with a `DateTime`-type column and inserting data into it:

``` sql
CREATE TABLE dt
(
    `timestamp` DateTime('Asia/Istanbul'),
    `event_id` UInt8
)
ENGINE = TinyLog;
```

``` sql
INSERT INTO dt Values (1546300800, 1), ('2019-01-01 00:00:00', 2);
```

``` sql
SELECT * FROM dt;
```

``` text
┌───────────timestamp─┬─event_id─┐
│ 2019-01-01 03:00:00 │        1 │
│ 2019-01-01 00:00:00 │        2 │
└─────────────────────┴──────────┘
```

-   When inserting datetime as an integer, it is treated as Unix Timestamp (UTC). `1546300800` represents `'2019-01-01 00:00:00'` UTC. However, as `timestamp` column has `Asia/Istanbul` (UTC+3) timezone specified, when outputting as string the value will be shown as `'2019-01-01 03:00:00'`
-   When inserting string value as datetime, it is treated as being in column timezone. `'2019-01-01 00:00:00'` will be treated as being in `Asia/Istanbul` timezone and saved as `1546290000`.

**2.** Filtering on `DateTime` values

``` sql
SELECT * FROM dt WHERE timestamp = toDateTime('2019-01-01 00:00:00', 'Asia/Istanbul')
```

``` text
┌───────────timestamp─┬─event_id─┐
│ 2019-01-01 00:00:00 │        2 │
└─────────────────────┴──────────┘
```

`DateTime` column values can be filtered using a string value in `WHERE` predicate. It will be converted to `DateTime` automatically:

``` sql
SELECT * FROM dt WHERE timestamp = '2019-01-01 00:00:00'
```

``` text
┌───────────timestamp─┬─event_id─┐
│ 2019-01-01 03:00:00 │        1 │
└─────────────────────┴──────────┘
```

**3.** Getting a time zone for a `DateTime`-type column:

``` sql
SELECT toDateTime(now(), 'Asia/Istanbul') AS column, toTypeName(column) AS x
```

``` text
┌──────────────column─┬─x─────────────────────────┐
│ 2019-10-16 04:12:04 │ DateTime('Asia/Istanbul') │
└─────────────────────┴───────────────────────────┘
```

**4.** Timezone conversion

``` sql
SELECT
toDateTime(timestamp, 'Europe/London') as lon_time,
toDateTime(timestamp, 'Asia/Istanbul') as mos_time
FROM dt
```

``` text
┌───────────lon_time──┬────────────mos_time─┐
│ 2019-01-01 00:00:00 │ 2019-01-01 03:00:00 │
│ 2018-12-31 21:00:00 │ 2019-01-01 00:00:00 │
└─────────────────────┴─────────────────────┘
```

As timezone conversion only changes the metadata, the operation has no computation cost.


## Limitations on timezones support

Some timezones may not be supported completely. There are a few cases:

If the offset from UTC is not a multiple of 15 minutes, the calculation of hours and minutes can be incorrect. For example, the time zone in Monrovia, Liberia has offset UTC -0:44:30 before 7 Jan 1972. If you are doing calculations on the historical time in Monrovia timezone, the time processing functions may give incorrect results. The results after 7 Jan 1972 will be correct nevertheless.

If the time transition (due to daylight saving time or for other reasons) was performed at a point of time that is not a multiple of 15 minutes, you can also get incorrect results at this specific day.

Non-monotonic calendar dates. For example, in Happy Valley - Goose Bay, the time was transitioned one hour backwards at 00:01:00 7 Nov 2010 (one minute after midnight). So after 6th Nov has ended, people observed a whole one minute of 7th Nov, then time was changed back to 23:01 6th Nov and after another 59 minutes the 7th Nov started again. ClickHouse does not (yet) support this kind of fun. During these days the results of time processing functions may be slightly incorrect.

Similar issue exists for Casey Antarctic station in year 2010. They changed time three hours back at 5 Mar, 02:00. If you are working in antarctic station, please don't afraid to use ClickHouse. Just make sure you set timezone to UTC or be aware of inaccuracies.

Time shifts for multiple days. Some pacific islands changed their timezone offset from UTC+14 to UTC-12. That's alright but some inaccuracies may present if you do calculations with their timezone for historical time points at the days of conversion.


## See Also {#see-also}

-   [Type conversion functions](../../sql-reference/functions/type-conversion-functions.md)
-   [Functions for working with dates and times](../../sql-reference/functions/date-time-functions.md)
-   [Functions for working with arrays](../../sql-reference/functions/array-functions.md)
-   [The `date_time_input_format` setting](../../operations/settings/settings.md#settings-date_time_input_format)
-   [The `date_time_output_format` setting](../../operations/settings/settings.md#settings-date_time_output_format)
-   [The `timezone` server configuration parameter](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone)
-   [Operators for working with dates and times](../../sql-reference/operators/index.md#operators-datetime)
-   [The `Date` data type](../../sql-reference/data-types/date.md)

[Original article](https://clickhouse.com/docs/en/data_types/datetime/) <!--hide-->
