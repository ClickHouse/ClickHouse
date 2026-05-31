
#include <Core/Field.h>
#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeTime.h>
#include <DataTypes/DataTypeTime64.h>
#include <DataTypes/DataTypeFactory.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

enum class ArgumentKind : uint8_t
{
    Optional,
    Mandatory
};

PreformattedMessage getExceptionMessage(
    const String & message, size_t argument_index, const char * argument_name,
    const std::string & context_data_type_name, Field::Types::Which field_type)
{
    return PreformattedMessage::create("Parameter #{} '{}' for {}{}, expected {} literal",
        argument_index, argument_name, context_data_type_name, message, field_type);
}

template <typename T, ArgumentKind Kind>
std::conditional_t<Kind == ArgumentKind::Optional, std::optional<T>, T>
getArgument(const ASTPtr & arguments, size_t argument_index, const char * argument_name [[maybe_unused]], const std::string context_data_type_name)
{
    using NearestResultType = NearestFieldType<T>;
    const auto field_type = Field::TypeToEnum<NearestResultType>::value;
    const ASTLiteral * argument = nullptr;

    if (!arguments || arguments->children.size() <= argument_index
        || !(argument = arguments->children[argument_index]->as<ASTLiteral>())
        || argument->value.getType() != field_type)
    {
        if constexpr (Kind == ArgumentKind::Optional)
            return {};
        else
        {
            if (argument && argument->value.getType() != field_type)
                throw Exception(getExceptionMessage(fmt::format(" has wrong type: {}", argument->value.getTypeName()),
                    argument_index, argument_name, context_data_type_name, field_type), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            throw Exception(
                getExceptionMessage(" is missing", argument_index, argument_name, context_data_type_name, field_type),
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }
    }

    return argument->value.safeGet<NearestResultType>();
}

static DataTypePtr create(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.empty())
        return std::make_shared<DataTypeDateTime>();

    const auto scale = getArgument<UInt64, ArgumentKind::Optional>(arguments, 0, "scale", "DateTime");
    const auto timezone = getArgument<String, ArgumentKind::Optional>(arguments, scale ? 1 : 0, "timezone", "DateTime");

    if (!scale && !timezone)
        throw Exception(getExceptionMessage(" has wrong type: ", 0, "scale", "DateTime", Field::Types::Which::UInt64),
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    /// If scale is defined, the data type is DateTime when scale = 0 otherwise the data type is DateTime64
    if (scale && scale.value() != 0)
        return std::make_shared<DataTypeDateTime64>(scale.value(), timezone.value_or(String{}));

    return std::make_shared<DataTypeDateTime>(timezone.value_or(String{}));
}

static DataTypePtr create32(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.empty())
        return std::make_shared<DataTypeDateTime>();

    if (arguments->children.size() != 1)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                        "DateTime32 data type can optionally have only one argument - time zone name");

    const auto timezone = getArgument<String, ArgumentKind::Mandatory>(arguments, 0, "timezone", "DateTime32");

    return std::make_shared<DataTypeDateTime>(timezone);
}

static DataTypePtr create64(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.empty())
        return std::make_shared<DataTypeDateTime64>(DataTypeDateTime64::default_scale);

    if (arguments->children.size() > 2)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                        "DateTime64 data type can optionally have two argument - scale and time zone name");

    const auto scale = getArgument<UInt64, ArgumentKind::Mandatory>(arguments, 0, "scale", "DateTime64");
    const auto timezone = getArgument<String, ArgumentKind::Optional>(arguments, 1, "timezone", "DateTime64");

    return std::make_shared<DataTypeDateTime64>(scale, timezone.value_or(String{}));
}

void registerDataTypeDateTime(DataTypeFactory & factory)
{
    factory.registerDataType("DateTime", create, DataTypeFactory::Case::Insensitive,
        Documentation{
            .description = R"DOCS_MD(
Allows to store an instant in time, that can be expressed as a calendar date and a time of a day.

Syntax:

```sql
DateTime([timezone])
```

Supported range of values: \[1970-01-01 00:00:00, 2106-02-07 06:28:15\].

Resolution: 1 second.

## Speed {#speed}

The `Date` data type is faster than `DateTime` under _most_ conditions.

The `Date` type requires 2 bytes of storage, while `DateTime` requires 4. However, during compression, the size difference between Date and DateTime becomes more significant. This amplification is due to the minutes and seconds in `DateTime` being less compressible. Filtering and aggregating `Date` instead of `DateTime` is also faster.

## Usage Remarks {#usage-remarks}

The point in time is saved as a [Unix timestamp](https://en.wikipedia.org/wiki/Unix_time), regardless of the time zone or daylight saving time. The time zone affects how the values of the `DateTime` type values are displayed in text format and how the values specified as strings are parsed ('2020-01-01 05:00:01').

Timezone agnostic Unix timestamp is stored in tables, and the timezone is used to transform it to text format or back during data import/export or to make calendar calculations on the values (example: `toDate`, `toHour` functions etc.). The time zone is not stored in the rows of the table (or in resultset), but is stored in the column metadata.

A list of supported time zones can be found in the [IANA Time Zone Database](https://www.iana.org/time-zones) and also can be queried by `SELECT * FROM system.time_zones`. [The list](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones) is also available at Wikipedia.

You can explicitly set a time zone for `DateTime`-type columns when creating a table. Example: `DateTime('UTC')`. If the time zone isn't set, ClickHouse uses the value of the [timezone](../../operations/server-configuration-parameters/settings.md#timezone) parameter in the server settings or the operating system settings at the moment of the ClickHouse server start.

The [clickhouse-client](../../interfaces/client.md) applies the server time zone by default if a time zone isn't explicitly set when initializing the data type. To use the client time zone, run `clickhouse-client` with the `--use_client_time_zone` parameter.

ClickHouse outputs values depending on the value of the [date_time_output_format](../../operations/settings/settings-formats.md#date_time_output_format) setting. `YYYY-MM-DD hh:mm:ss` text format by default. Additionally, you can change the output with the [formatDateTime](../../sql-reference/functions/date-time-functions.md#formatDateTime) function.

When inserting data into ClickHouse, you can use different formats of date and time strings, depending on the value of the [date_time_input_format](../../operations/settings/settings-formats.md#date_time_input_format) setting.

## Examples {#examples}

**1.** Creating a table with a `DateTime`-type column and inserting data into it:

```sql
CREATE TABLE dt
(
    `timestamp` DateTime('Asia/Istanbul'),
    `event_id` UInt8
)
ENGINE = TinyLog;
```

```sql
-- Parse DateTime
-- - from string,
-- - from integer interpreted as number of seconds since 1970-01-01.
INSERT INTO dt VALUES ('2019-01-01 00:00:00', 1), (1546300800, 2);

SELECT * FROM dt;
```

```text
┌───────────timestamp─┬─event_id─┐
│ 2019-01-01 00:00:00 │        1 │
│ 2019-01-01 03:00:00 │        2 │
└─────────────────────┴──────────┘
```

- When inserting datetime as an integer, it is treated as Unix Timestamp (UTC). `1546300800` represents `'2019-01-01 00:00:00'` UTC. However, as `timestamp` column has `Asia/Istanbul` (UTC+3) timezone specified, when outputting as string the value will be shown as `'2019-01-01 03:00:00'`
- When inserting string value as datetime, it is treated as being in column timezone. `'2019-01-01 00:00:00'` will be treated as being in `Asia/Istanbul` timezone and saved as `1546290000`.

**2.** Filtering on `DateTime` values

```sql
SELECT * FROM dt WHERE timestamp = toDateTime('2019-01-01 00:00:00', 'Asia/Istanbul')
```

```text
┌───────────timestamp─┬─event_id─┐
│ 2019-01-01 00:00:00 │        1 │
└─────────────────────┴──────────┘
```

`DateTime` column values can be filtered using a string value in `WHERE` predicate. It will be converted to `DateTime` automatically:

```sql
SELECT * FROM dt WHERE timestamp = '2019-01-01 00:00:00'
```

```text
┌───────────timestamp─┬─event_id─┐
│ 2019-01-01 00:00:00 │        1 │
└─────────────────────┴──────────┘
```

**3.** Getting a time zone for a `DateTime`-type column:

```sql
SELECT toDateTime(now(), 'Asia/Istanbul') AS column, toTypeName(column) AS x
```

```text
┌──────────────column─┬─x─────────────────────────┐
│ 2019-10-16 04:12:04 │ DateTime('Asia/Istanbul') │
└─────────────────────┴───────────────────────────┘
```

**4.** Timezone conversion

```sql
SELECT
toDateTime(timestamp, 'Europe/London') AS lon_time,
toDateTime(timestamp, 'Asia/Istanbul') AS istanbul_time
FROM dt
```

```text
┌───────────lon_time──┬───────istanbul_time─┐
│ 2019-01-01 00:00:00 │ 2019-01-01 03:00:00 │
│ 2018-12-31 21:00:00 │ 2019-01-01 00:00:00 │
└─────────────────────┴─────────────────────┘
```

As timezone conversion only changes the metadata, the operation has no computation cost.

## Limitations on time zones support {#limitations-on-time-zones-support}

Some time zones may not be supported completely. There are a few cases:

If the offset from UTC is not a multiple of 15 minutes, the calculation of hours and minutes can be incorrect. For example, the time zone in Monrovia, Liberia has offset UTC -0:44:30 before 7 Jan 1972. If you are doing calculations on the historical time in Monrovia timezone, the time processing functions may give incorrect results. The results after 7 Jan 1972 will be correct nevertheless.

If the time transition (due to daylight saving time or for other reasons) was performed at a point of time that is not a multiple of 15 minutes, you can also get incorrect results at this specific day.

Non-monotonic calendar dates. For example, in Happy Valley - Goose Bay, the time was transitioned one hour backwards at 00:01:00 7 Nov 2010 (one minute after midnight). So after 6th Nov has ended, people observed a whole one minute of 7th Nov, then time was changed back to 23:01 6th Nov and after another 59 minutes the 7th Nov started again. ClickHouse does not (yet) support this kind of fun. During these days the results of time processing functions may be slightly incorrect.

Similar issue exists for Casey Antarctic station in year 2010. They changed time three hours back at 5 Mar, 02:00. If you are working in antarctic station, please don't be afraid to use ClickHouse. Just make sure you set timezone to UTC or be aware of inaccuracies.

Time shifts for multiple days. Some pacific islands changed their timezone offset from UTC+14 to UTC-12. That's alright but some inaccuracies may present if you do calculations with their timezone for historical time points at the days of conversion.

## Handling daylight saving time (DST) {#handling-daylight-saving-time-dst}

ClickHouse's DateTime type with time zones can exhibit unexpected behavior during Daylight Saving Time (DST) transitions, particularly when:

- [`date_time_output_format`](../../operations/settings/settings-formats.md#date_time_output_format) is set to `simple`.
- Clocks move backward ("Fall Back"), causing a one-hour overlap.
- Clocks move forward ("Spring Forward"), causing a one-hour gap.

By default, ClickHouse always picks the earlier occurrence of an overlapping time and may interpret nonexistent times during forward shifts.

For example, consider the following transition from Daylight Saving Time (DST) to Standard Time.

- On October 29, 2023, at 02:00:00, clocks move backward to 01:00:00 (BST → GMT).
- The hour 01:00:00 – 01:59:59 appears twice (once in BST and once in GMT)
- ClickHouse always picks the first occurrence (BST), causing unexpected results when adding time intervals.

```sql
SELECT '2023-10-29 01:30:00'::DateTime('Europe/London') AS time, time + toIntervalHour(1) AS one_hour_later

┌────────────────time─┬──────one_hour_later─┐
│ 2023-10-29 01:30:00 │ 2023-10-29 01:30:00 │
└─────────────────────┴─────────────────────┘
```

Similarly, during the transition from Standard Time to Daylight Saving Time, an hour can appear to be skipped.

For example:

- On March 26, 2023, at `00:59:59`, clocks jump forward to 02:00:00 (GMT → BST).
- The hour `01:00:00` – `01:59:59` does not exist.

```sql
SELECT '2023-03-26 01:30:00'::DateTime('Europe/London') AS time, time + toIntervalHour(1) AS one_hour_later

┌────────────────time─┬──────one_hour_later─┐
│ 2023-03-26 00:30:00 │ 2023-03-26 02:30:00 │
└─────────────────────┴─────────────────────┘
```

In this case, ClickHouse shifts the non-existent time `2023-03-26 01:30:00` back to `2023-03-26 00:30:00`.

## See Also {#see-also}

- [Type conversion functions](../../sql-reference/functions/type-conversion-functions.md)
- [Functions for working with dates and times](../../sql-reference/functions/date-time-functions.md)
- [Functions for working with arrays](../../sql-reference/functions/array-functions.md)
- [The `date_time_input_format` setting](../../operations/settings/settings-formats.md#date_time_input_format)
- [The `date_time_output_format` setting](../../operations/settings/settings-formats.md#date_time_output_format)
- [The `timezone` server configuration parameter](../../operations/server-configuration-parameters/settings.md#timezone)
- [The `session_timezone` setting](../../operations/settings/settings.md#session_timezone)
- [Operators for working with dates and times](../../sql-reference/operators#operators-for-working-with-dates-and-times)
- [The `Date` data type](../../sql-reference/data-types/date.md)
)DOCS_MD",
            .syntax = "DateTime([timezone])",
            .related = {"DateTime64", "Date"},
        });
    factory.registerDataType("DateTime32", create32, DataTypeFactory::Case::Insensitive,
        Documentation{
            .description = R"DOCS_MD(
A spelling of the `DateTime` data type that always uses 32-bit storage with second resolution. It accepts an optional time zone argument, e.g. `DateTime32('Europe/Amsterdam')`, and is otherwise identical to `DateTime`.
)DOCS_MD",
            .syntax = "DateTime32([timezone])",
            .related = {"DateTime"},
        });
    factory.registerDataType("DateTime64", create64, DataTypeFactory::Case::Insensitive,
        Documentation{
            .description = R"DOCS_MD(
Allows to store an instant in time, that can be expressed as a calendar date and a time of a day, with defined sub-second precision

Tick size (precision): 10<sup>-precision</sup> seconds. Valid range: [ 0 : 9 ].
Typically, are used - 3 (milliseconds), 6 (microseconds), 9 (nanoseconds).

**Syntax:**

```sql
DateTime64(precision, [timezone])
```

Internally, stores data as a number of 'ticks' since epoch start (1970-01-01 00:00:00 UTC) as Int64. The tick resolution is determined by the precision parameter. Additionally, the `DateTime64` type can store time zone that is the same for the entire column, that affects how the values of the `DateTime64` type values are displayed in text format and how the values specified as strings are parsed ('2020-01-01 05:00:01.000'). The time zone is not stored in the rows of the table (or in resultset), but is stored in the column metadata. See details in [DateTime](../../sql-reference/data-types/datetime.md).

Supported range of values: \[1900-01-01 00:00:00, 2299-12-31 23:59:59.999999999\]

The number of digits after the decimal point depends on the precision parameter.

Note: The precision of the maximum value is 8. If the maximum precision of 9 digits (nanoseconds) is used, the maximum supported value is `2262-04-11 23:47:16` in UTC.

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
-- Parse DateTime
-- - from an integer interpreted as the number of milliseconds (because of precision 3) since 1970-01-01,
-- - from a decimal interpreted as the number of seconds before the decimal part, and based on the precision after the decimal point,
-- - from a string.

INSERT INTO dt64
VALUES
(1546300800123, 1),
(1546300800.123, 2),
('2019-01-01 00:00:00', 3);

SELECT * FROM dt64;
```

```text
┌───────────────timestamp─┬─event_id─┐
│ 2019-01-01 03:00:00.123 │        1 │
│ 2019-01-01 03:00:00.123 │        2 │
│ 2019-01-01 00:00:00.000 │        3 │
└─────────────────────────┴──────────┘
```

- When inserting datetime as an integer, it is treated as an appropriately scaled Unix Timestamp (UTC). `1546300800000` (with precision 3) represents `'2019-01-01 00:00:00'` UTC. However, as `timestamp` column has `Asia/Istanbul` (UTC+3) timezone specified, when outputting as a string the value will be shown as `'2019-01-01 03:00:00'`. Inserting datetime as a decimal will treat it similarly as an integer, except the value before the decimal point is the Unix Timestamp up to and including the seconds, and after the decimal point will be treated as the precision.
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

Contrary to inserting, the `toDateTime64` function will treat all values as the decimal variant, so precision needs to
be given after the decimal point.

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
)DOCS_MD",
            .syntax = "DateTime64(precision, [timezone])",
            .related = {"DateTime"},
        });

    factory.registerAlias("TIMESTAMP", "DateTime", DataTypeFactory::Case::Insensitive);
}

static DataTypePtr createTime(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.empty())
        return std::make_shared<DataTypeTime>();

    const auto scale = getArgument<UInt64, ArgumentKind::Optional>(arguments, 0, "scale", "Time");
    const auto timezone = getArgument<String, ArgumentKind::Optional>(arguments, scale ? 1 : 0, "timezone", "Time");

    if (timezone && !timezone->empty())
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Specifying timezone for Time type is not allowed");

    if (!scale && !timezone)
        throw Exception(getExceptionMessage(" has wrong type: ", 0, "scale", "Time", Field::Types::Which::UInt64),
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

    /// If scale is defined, the data type is Time when scale = 0 otherwise the data type is Time64
    if (scale && scale.value() != 0)
        return std::make_shared<DataTypeTime64>(scale.value());

    return std::make_shared<DataTypeTime>();
}

static DataTypePtr createTime64(const ASTPtr & arguments)
{
    if (!arguments || arguments->children.empty())
        return std::make_shared<DataTypeTime64>(DataTypeTime64::default_scale);

    if (arguments->children.size() > 2)
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                        "Time64 data type can optionally have only one argument - scale");

    const auto scale = getArgument<UInt64, ArgumentKind::Mandatory>(arguments, 0, "scale", "Time64");
    const auto timezone = getArgument<String, ArgumentKind::Optional>(arguments, 1, "timezone", "Time64");

    if (timezone && !timezone->empty())
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "Specifying timezone for Time64 type is not allowed");

    return std::make_shared<DataTypeTime64>(scale);
}

void registerDataTypeTime(DataTypeFactory & factory)
{
    factory.registerDataType("Time", createTime, DataTypeFactory::Case::Insensitive,
        Documentation{
            .description = R"DOCS_MD(
Data type `Time` represents a time with hour, minute, and second components.
It is independent of any calendar date and is suitable for values which do not need day, months and year components.

Syntax:

``` sql
Time
```

Text representation range: [-999:59:59, 999:59:59].

Resolution: 1 second.

## Implementation details {#implementation-details}

**Representation and Performance**.
Data type `Time` internally stores a signed 32-bit integer that encodes the seconds.
Values of type `Time` and `DateTime` have the same byte size and thus comparable performance.

**Normalization**.
When parsing strings to `Time`, the time components are normalized and not validated.
For example, `25:70:70` is interpreted as `26:11:10`.

**Negative values**.
Leading minus signs are supported and preserved.
Negative values typically arise from arithmetic operations on `Time` values.
For `Time` type, negative inputs are preserved for both text (e.g., `'-01:02:03'`) and numeric inputs (e.g., `-3723`).

**Saturation**.
The time-of-day component is capped to the range [-999:59:59, 999:59:59].
Values with hours beyond 999 (or below -999) are represented and round-tripped via text as `999:59:59` (or `-999:59:59`).

**Time zones**.
`Time` does not support time zones, i.e. `Time` value are interpreted without regional context.
Specifying a time zone for `Time` as a type parameter or during value creation throws an error.
Likewise, attempts to apply or change the time zone on `Time` columns are not supported and result in an error.
`Time` values are not silently reinterpreted under different time zones.

## Examples {#examples}

**1.** Creating a table with a `Time`-type column and inserting data into it:

``` sql
CREATE TABLE tab
(
    `event_id` UInt8,
    `time` Time
)
ENGINE = TinyLog;
```

``` sql
-- Parse Time
-- - from string,
-- - from integer interpreted as number of seconds since 00:00:00.
INSERT INTO tab VALUES (1, '14:30:25'), (2, 52225);

SELECT * FROM tab ORDER BY event_id;
```

``` text
   ┌─event_id─┬──────time─┐
1. │        1 │ 14:30:25 │
2. │        2 │ 14:30:25 │
   └──────────┴───────────┘
```

**2.** Filtering on `Time` values

``` sql
SET use_legacy_to_time = 0;
SELECT * FROM tab WHERE time = toTime('14:30:25')
```

``` text
   ┌─event_id─┬──────time─┐
1. │        1 │ 14:30:25 │
2. │        2 │ 14:30:25 │
   └──────────┴───────────┘
```

`Time` column values can be filtered using a string value in `WHERE` predicate. It will be converted to `Time` automatically:

``` sql
SELECT * FROM tab WHERE time = '14:30:25'
```

``` text
   ┌─event_id─┬──────time─┐
1. │        1 │ 14:30:25 │
2. │        2 │ 14:30:25 │
   └──────────┴───────────┘
```

**3.** Inspecting the resulting type:

``` sql
SELECT CAST('14:30:25' AS Time) AS column, toTypeName(column) AS type
```

``` text
   ┌────column─┬─type─┐
1. │ 14:30:25 │ Time │
   └───────────┴──────┘
```

## Addition with Date {#addition-with-date}

A [Time](time.md) value can be added to a [Date](date.md) or [Date32](date32.md) value to produce a [DateTime](datetime.md) or [DateTime64](datetime64.md):

```sql
SET use_legacy_to_time = 0;
SELECT toDate('2024-07-15') + toTime('14:30:25') as datetime;
```

```text
   ┌────────────datetime─┐
1. │ 2024-07-15 14:30:25 │
   └─────────────────────┘
```

See [Date and Time Addition](../operators/index.md#date-time-addition) for details on all supported combinations and result types.

## See Also {#see-also}

- [Type conversion functions](../functions/type-conversion-functions.md)
- [Functions for working with dates and times](../functions/date-time-functions.md)
- [Functions for working with arrays](../functions/array-functions.md)
- [The `date_time_input_format` setting](../../operations/settings/settings-formats.md#date_time_input_format)
- [The `date_time_output_format` setting](../../operations/settings/settings-formats.md#date_time_output_format)
- [The `timezone` server configuration parameter](../../operations/server-configuration-parameters/settings.md#timezone)
- [The `session_timezone` setting](../../operations/settings/settings.md#session_timezone)
- [The `DateTime` data type](datetime.md)
- [The `Date` data type](date.md)
)DOCS_MD",
            .syntax = "Time",
            .related = {"Time64", "DateTime"},
        });
    factory.registerDataType("Time64", createTime64, DataTypeFactory::Case::Insensitive,
        Documentation{
            .description = R"DOCS_MD(
Data type `Time64` represents a time-of-day with fractional seconds.
It has no calendar date components (day, month, year).
The `precision` parameter defines the number of fractional digits and therefore the tick size.

Tick size (precision): 10<sup>-precision</sup> seconds. Valid range: 0..9. Common choices are 3 (milliseconds), 6 (microseconds), and 9 (nanoseconds).

**Syntax:**

``` sql
Time64(precision)
```

Internally, `Time64` stores a signed 64-bit decimal (Decimal64) number of fractional seconds.
The tick resolution is determined by the `precision` parameter.
Time zones are not supported: specifying a time zone with `Time64` will throw an error.

Unlike `DateTime64`, `Time64` does not store a date component.
See also [`Time`](../../sql-reference/data-types/time.md).

Text representation range: [-999:59:59.000, 999:59:59.999] for `precision = 3`. In general, the minimum is `-999:59:59` and the maximum is `999:59:59` with up to `precision` fractional digits (for `precision = 9`, the minimum is `-999:59:59.999999999`).

## Implementation details {#implementation-details}

**Representation**.
Signed `Decimal64` value counting fractional second with `precision` fractional digits.

**Normalization**.
When parsing strings to `Time64`, the time components are normalized and not validated.
For example, `25:70:70` is interpreted as `26:11:10`.

**Negative values**.
Leading minus signs are supported and preserved.
Negative values typically arise from arithmetic operations on `Time64` values.
For `Time64`, negative inputs are preserved for both text (e.g., `'-01:02:03.123'`) and numeric inputs (e.g., `-3723.123`).

**Saturation**.
The time-of-day component is capped to the range [-999:59:59.xxx, 999:59:59.xxx] when converting to components or serialising to text.
The stored numeric value may exceed this range; however, any component extraction (hours, minutes, seconds) and textual representation use the saturated value.

**Time zones**.
`Time64` does not support time zones.
Specifying a time zone when creating a `Time64` type or value throws an error.
Likewise, attempts to apply or change the time zone on `Time64` columns is not supported and results in an error.

## Examples {#examples}

1. Creating a table with a `Time64`-type column and inserting data into it:

``` sql
CREATE TABLE tab64
(
    `event_id` UInt8,
    `time` Time64(3)
)
ENGINE = TinyLog;
```

``` sql
-- Parse Time64
-- - from string,
-- - from a number of seconds since 00:00:00 (fractional part according to precision).
INSERT INTO tab64 VALUES (1, '14:30:25'), (2, 52225.123), (3, '14:30:25');

SELECT * FROM tab64 ORDER BY event_id;
```

``` text
   ┌─event_id─┬────────time─┐
1. │        1 │ 14:30:25.000 │
2. │        2 │ 14:30:25.123 │
3. │        3 │ 14:30:25.000 │
   └──────────┴──────────────┘
```

2. Filtering on `Time64` values

``` sql
SELECT * FROM tab64 WHERE time = toTime64('14:30:25', 3);
```

``` text
   ┌─event_id─┬────────time─┐
1. │        1 │ 14:30:25.000 │
2. │        3 │ 14:30:25.000 │
   └──────────┴──────────────┘
```

``` sql
SELECT * FROM tab64 WHERE time = toTime64(52225.123, 3);
```

``` text
   ┌─event_id─┬────────time─┐
1. │        2 │ 14:30:25.123 │
   └──────────┴──────────────┘
```

Note: `toTime64` parses numeric literals as seconds with a fractional part according to the specified precision, so provide the intended fractional digits explicitly.

3. Inspecting the resulting type:

``` sql
SELECT CAST('14:30:25.250' AS Time64(3)) AS column, toTypeName(column) AS type;
```

``` text
   ┌────────column─┬─type──────┐
1. │ 14:30:25.250 │ Time64(3) │
   └───────────────┴───────────┘
```

## Addition with Date {#addition-with-date}

A [Time64](time64.md) value can be added to a [Date](date.md) or [Date32](date32.md) value to produce a [DateTime64](datetime64.md) with the same scale as the `Time64`:

```sql
SET use_legacy_to_time = 0;
SELECT toDate('2024-07-15') + toTime64('14:30:25.123456', 6) AS dt, toTypeName(dt);
```

```text
   ┌─────────────────────────dt─┬─toTypeName(dt)─┐
1. │ 2024-07-15 14:30:25.123456 │ DateTime64(6)  │
   └────────────────────────────┴────────────────┘
```

See [Date and Time Addition](../operators/index.md#date-time-addition) for details on all supported combinations and result types.

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
)DOCS_MD",
            .syntax = "Time64(precision)",
            .related = {"Time"},
        });
}

}
