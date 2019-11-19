# DateTime {#data_type-datetime}

Data structure storing Unix timestamp. Also, it can store a time zone.

Syntax:

```sql
DateTime([timezone])
```

Range of values in the Unix timestamp: [1970-01-01 00:00:00, 2105-12-31 23:59:59].

Resolution: 1 second.

## Usage remarks

ClickHouse stores date and time values in the Unix timestamp format that is independent of the time zones and daylight saving rules. The time zone value affects displaying `DateTime` values in text formats and parsing the input strings for storage. You can find the list of supported time zones in [IANA Time Zone Database](https://www.iana.org/time-zones).

You can explicitly set a time zone for `DateTime`-type column when creating a table. If time zone isn't set, ClickHouse uses the value of the [timezone](../operations/server_settings/settings.md#server_settings-timezone) server configuration parameter or the operating system settings at the moment of the ClickHouse server start. 

The [clickhouse-client](../interfaces/cli.md) applies the server time zone by default if a time zone isn't explicitly defined when initializing the data type. To use the client time zone, run it with the `--use_client_time_zone` parameter.

ClickHouse outputs values in the `YYYY-MM-DD hh:mm:ss` text format by default. You can change the format with the [formatDateTime](../query_language/functions/date_time_functions.md#formatdatetime) function.

When inserting data into ClickHouse, you can use different formats of date and time strings, depending on the [date_time_input_format](../operations/settings/settings.md#settings-date_time_input_format) setting value.

## Examples

Creating a table with a `DateTime`-type column:

```sql
CREATE TABLE dt(
    timestamp DateTime('Europe/Moscow')
)
```

Getting a time zone for a `DateTime`-type value:

```sql
SELECT
    toDateTime(now(), 'Europe/Moscow') AS column,
    toTypeName(column) AS x
```
```text
┌──────────────column─┬─x─────────────────────────┐
│ 2019-10-16 04:12:04 │ DateTime('Europe/Moscow') │
└─────────────────────┴───────────────────────────┘
```

## See Also

- [Type Conversion Functions](../query_language/functions/type_conversion_functions.md)
- [Functions for Working with Dates and Times](../query_language/functions/date_time_functions.md)
- [Functions for Working with Arrays](../query_language/functions/array_functions.md)
- [The `date_time_input_format` setting](../operations/settings/settings.md#settings-date_time_input_format)
- [The `timezone` server configuration parameter](../operations/server_settings/settings.md#server_settings-timezone)
- [Operator for Working with Dates and Times](../query_language/operators.md#operators-datetime)
- [The `Date` data type](date.md)

[Original article](https://clickhouse.yandex/docs/en/data_types/datetime/) <!--hide-->
