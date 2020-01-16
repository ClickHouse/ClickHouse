# DateTime {#data_type-datetime}

Allows to store an instant in time, that can be expressed as a calendar date and a time of a day. `DateTime` allows to take into account time zones for stored values.

Syntax:

```sql
DateTime([timezone])
```

Range of values: [1970-01-01 00:00:00, 2105-12-31 23:59:59].

Resolution: 1 second.


## Usage Remarks

A moment of time is stored as Unix timestamp, independently of time zones and daylight savings. Additionally `DateTime` can store time zone, that affects how `DateTime` values are displayed in text format and how input strings are parsed for storage. You can find the list of supported time zones in the [IANA Time Zone Database](https://www.iana.org/time-zones).

You can explicitly set a time zone for `DateTime`-type columns when creating a table. If the time zone isn't set, ClickHouse uses the value of the [timezone](../operations/server_settings/settings.md#server_settings-timezone) parameter in the server settings or the operating system settings at the moment of the ClickHouse server start.

The [clickhouse-client](../interfaces/cli.md) applies the server time zone by default if a time zone isn't explicitly set when initializing the data type. To use the client time zone, run `clickhouse-client` with the `--use_client_time_zone` parameter.

ClickHouse outputs values in `YYYY-MM-DD hh:mm:ss` text format by default. You can change the format with the [formatDateTime](../query_language/functions/date_time_functions.md#formatdatetime) function.

When inserting data into ClickHouse, you can use different formats of date and time strings, depending on the value of the [date_time_input_format](../operations/settings/settings.md#settings-date_time_input_format) setting.

## Examples

**1.** Creating a table with a `DateTime`-type column and inserting data into it:

```sql
CREATE TABLE dt
(
    `timestamp` DateTime('Europe/Moscow'), 
    `event_id` UInt8
)
ENGINE = TinyLog
```
```sql
INSERT INTO dt Values (1546300800, 1), ('2019-01-01 00:00:00', 2)
```
```sql
SELECT * FROM dt
```
```text
┌───────────timestamp─┬─event_id─┐
│ 2019-01-01 03:00:00 │        1 │
│ 2019-01-01 00:00:00 │        2 │
└─────────────────────┴──────────┘
```

Unix timestamp `1546300800` represents the `'2019-01-01 00:00:00'` date and time in `Europe/London (UTC+0)` time zone, but the `timestamp` column stores values in the `Europe/Moscow (UTC+3)` timezone, so the value inserted as Unix timestamp represents the `2019-01-01 03:00:00` date and time.

```sql
SELECT * FROM dt WHERE timestamp = toDateTime('2019-01-01 00:00:00', 'Europe/Moscow')
```
```text
┌───────────timestamp─┬─event_id─┐
│ 2019-01-01 00:00:00 │        2 │
└─────────────────────┴──────────┘
```

**2.** Getting a time zone for a `DateTime`-type value:

```sql
SELECT toDateTime(now(), 'Europe/Moscow') AS column, toTypeName(column) AS x
```
```text
┌──────────────column─┬─x─────────────────────────┐
│ 2019-10-16 04:12:04 │ DateTime('Europe/Moscow') │
└─────────────────────┴───────────────────────────┘
```

## See Also

- [Type conversion functions](../query_language/functions/type_conversion_functions.md)
- [Functions for working with dates and times](../query_language/functions/date_time_functions.md)
- [Functions for working with arrays](../query_language/functions/array_functions.md)
- [The `date_time_input_format` setting](../operations/settings/settings.md#settings-date_time_input_format)
- [The `timezone` server configuration parameter](../operations/server_settings/settings.md#server_settings-timezone)
- [Operators for working with dates and times](../query_language/operators.md#operators-datetime)
- [The `Date` data type](date.md)

[Original article](https://clickhouse.yandex/docs/en/data_types/datetime/) <!--hide-->
