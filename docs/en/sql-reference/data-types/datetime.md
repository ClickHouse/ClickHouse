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

Supported range of values: \[1970-01-01 00:00:00, 2105-12-31 23:59:59\].

Resolution: 1 second.

## Usage Remarks {#usage-remarks}

The point in time is saved as a [Unix timestamp](https://en.wikipedia.org/wiki/Unix_time), regardless of the time zone or daylight saving time. Additionally, the `DateTime` type can store time zone that is the same for the entire column, that affects how the values of the `DateTime` type values are displayed in text format and how the values specified as strings are parsed (‘2020-01-01 05:00:01’). The time zone is not stored in the rows of the table (or in resultset), but is stored in the column metadata.
A list of supported time zones can be found in the [IANA Time Zone Database](https://www.iana.org/time-zones).
The `tzdata` package, containing [IANA Time Zone Database](https://www.iana.org/time-zones), should be installed in the system. Use the `timedatectl list-timezones` command to list timezones known by a local system.

You can explicitly set a time zone for `DateTime`-type columns when creating a table. If the time zone isn’t set, ClickHouse uses the value of the [timezone](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone) parameter in the server settings or the operating system settings at the moment of the ClickHouse server start.

The [clickhouse-client](../../interfaces/cli.md) applies the server time zone by default if a time zone isn’t explicitly set when initializing the data type. To use the client time zone, run `clickhouse-client` with the `--use_client_time_zone` parameter.

ClickHouse outputs values depending on the value of the [date\_time\_output\_format](../../operations/settings/settings.md#settings-date_time_output_format) setting. `YYYY-MM-DD hh:mm:ss` text format by default. Additionaly you can change the output with the [formatDateTime](../../sql-reference/functions/date-time-functions.md#formatdatetime) function.

When inserting data into ClickHouse, you can use different formats of date and time strings, depending on the value of the [date_time_input_format](../../operations/settings/settings.md#settings-date_time_input_format) setting.

## Examples {#examples}

**1.** Creating a table with a `DateTime`-type column and inserting data into it:

``` sql
CREATE TABLE dt
(
    `timestamp` DateTime('Europe/Moscow'),
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

-   When inserting datetime as an integer, it is treated as Unix Timestamp (UTC). `1546300800` represents `'2019-01-01 00:00:00'` UTC. However, as `timestamp` column has `Europe/Moscow` (UTC+3) timezone specified, when outputting as string the value will be shown as `'2019-01-01 03:00:00'`
-   When inserting string value as datetime, it is treated as being in column timezone. `'2019-01-01 00:00:00'` will be treated as being in `Europe/Moscow` timezone and saved as `1546290000`.

**2.** Filtering on `DateTime` values

``` sql
SELECT * FROM dt WHERE timestamp = toDateTime('2019-01-01 00:00:00', 'Europe/Moscow')
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
SELECT toDateTime(now(), 'Europe/Moscow') AS column, toTypeName(column) AS x
```

``` text
┌──────────────column─┬─x─────────────────────────┐
│ 2019-10-16 04:12:04 │ DateTime('Europe/Moscow') │
└─────────────────────┴───────────────────────────┘
```

**4.** Timezone conversion

``` sql
SELECT
toDateTime(timestamp, 'Europe/London') as lon_time,
toDateTime(timestamp, 'Europe/Moscow') as mos_time
FROM dt
```

``` text
┌───────────lon_time──┬────────────mos_time─┐
│ 2019-01-01 00:00:00 │ 2019-01-01 03:00:00 │
│ 2018-12-31 21:00:00 │ 2019-01-01 00:00:00 │
└─────────────────────┴─────────────────────┘
```

## See Also {#see-also}

-   [Type conversion functions](../../sql-reference/functions/type-conversion-functions.md)
-   [Functions for working with dates and times](../../sql-reference/functions/date-time-functions.md)
-   [Functions for working with arrays](../../sql-reference/functions/array-functions.md)
-   [The `date_time_input_format` setting](../../operations/settings/settings.md#settings-date_time_input_format)
-   [The `date_time_output_format` setting](../../operations/settings/settings.md#settings-date_time_output_format)
-   [The `timezone` server configuration parameter](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-timezone)
-   [Operators for working with dates and times](../../sql-reference/operators/index.md#operators-datetime)
-   [The `Date` data type](../../sql-reference/data-types/date.md)

[Original article](https://clickhouse.tech/docs/en/data_types/datetime/) <!--hide-->
