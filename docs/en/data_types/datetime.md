# DateTime {#data_type-datetime}

Data structure for storing a Unix timestamp. It can also store a time zone.

Syntax:

```sql
DateTime([timezone])
```

Range of values in the Unix timestamp: [1970-01-01 00:00:00, 2105-12-31 23:59:59].

Resolution: 1 second.

## Usage remarks

ClickHouse stores date and time values in Unix timestamp format, independent of time zones and daylight savings. The time zone value affects how `DateTime` values are displayed in text format and how input strings are parsed for storage. You can find the list of supported time zones in the [IANA Time Zone Database](https://www.iana.org/time-zones).

You can explicitly set a time zone for `DateTime`-type columns when creating a table. If the time zone isn't set, ClickHouse uses the value of the [timezone](../operations/server_settings/settings.md#server_settings-timezone) parameter in the server settings or the operating system settings from when the ClickHouse server started. 

The [clickhouse-client](../interfaces/cli.md) applies the server time zone by default if a time zone isn't explicitly set when initializing the data type. To use the client time zone, use the `--use_client_time_zone` parameter.

ClickHouse outputs values in `YYYY-MM-DD hh:mm:ss` text format by default. You can change the format with the [formatDateTime](../query_language/functions/date_time_functions.md#formatdatetime) function.

When inserting data into ClickHouse, you can use different formats of date and time strings, depending on the value of the [date_time_input_format](../operations/settings/settings.md#settings-date_time_input_format) settings.

## Examples

**1.** Creating a table with a `DateTime`-type column:

```sql
CREATE TABLE dt( timestamp DateTime('Europe/Moscow') ) ENGINE TinyLog
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
