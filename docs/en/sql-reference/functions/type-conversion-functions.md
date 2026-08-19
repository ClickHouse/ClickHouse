---
description: 'Documentation for type conversion functions'
sidebar_label: 'Type conversion'
slug: /sql-reference/functions/type-conversion-functions
title: 'Type conversion functions'
doc_type: 'reference'
---

# Type conversion functions

## Common issues with data conversion {#common-issues-with-data-conversion}

ClickHouse generally uses the [same behavior as C++ programs](https://en.cppreference.com/w/cpp/language/implicit_conversion).

`to<type>` functions and [cast](#CAST) behave differently in some cases, for example in case of [LowCardinality](../data-types/lowcardinality.md): [cast](#CAST) removes [LowCardinality](../data-types/lowcardinality.md) trait `to<type>` functions don't. The same with [Nullable](../data-types/nullable.md), this behaviour is not compatible with SQL standard, and it can be changed using [cast_keep_nullable](../../operations/settings/settings.md/#cast_keep_nullable) setting.

:::note
Be aware of potential data loss if values of a datatype are converted to a smaller datatype (for example from `Int64` to `Int32`) or between
incompatible datatypes (for example from `String` to `Int`). Make sure to check carefully if the result is as expected.
:::

Example:

```sql
SELECT
    toTypeName(toLowCardinality('') AS val) AS source_type,
    toTypeName(toString(val)) AS to_type_result_type,
    toTypeName(CAST(val, 'String')) AS cast_result_type

┌─source_type────────────┬─to_type_result_type────┬─cast_result_type─┐
│ LowCardinality(String) │ LowCardinality(String) │ String           │
└────────────────────────┴────────────────────────┴──────────────────┘

SELECT
    toTypeName(toNullable('') AS val) AS source_type,
    toTypeName(toString(val)) AS to_type_result_type,
    toTypeName(CAST(val, 'String')) AS cast_result_type

┌─source_type──────┬─to_type_result_type─┬─cast_result_type─┐
│ Nullable(String) │ Nullable(String)    │ String           │
└──────────────────┴─────────────────────┴──────────────────┘

SELECT
    toTypeName(toNullable('') AS val) AS source_type,
    toTypeName(toString(val)) AS to_type_result_type,
    toTypeName(CAST(val, 'String')) AS cast_result_type
SETTINGS cast_keep_nullable = 1

┌─source_type──────┬─to_type_result_type─┬─cast_result_type─┐
│ Nullable(String) │ Nullable(String)    │ Nullable(String) │
└──────────────────┴─────────────────────┴──────────────────┘
```

## Notes on `toString` functions {#to-string-functions}

The `toString` family of functions allows for converting between numbers, strings (but not fixed strings), dates, and dates with times.
All of these functions accept one argument.

- When converting to or from a string, the value is formatted or parsed using the same rules as for the TabSeparated format (and almost all other text formats). If the string can't be parsed, an exception is thrown and the request is canceled.
- When converting dates to numbers or vice versa, the date corresponds to the number of days since the beginning of the Unix epoch.
- When converting dates with times to numbers or vice versa, the date with time corresponds to the number of seconds since the beginning of the Unix epoch.
- The `toString` function of the `DateTime` argument can take a second String argument containing the name of the time zone, for example: `Europe/Amsterdam`. In this case, the time is formatted according to the specified time zone.

## Notes on `toDate`/`toDateTime` functions {#to-date-and-date-time-functions}

The date and date-with-time formats for the `toDate`/`toDateTime` functions are defined as follows:

```response
YYYY-MM-DD
YYYY-MM-DD hh:mm:ss
```

As an exception, if converting from UInt32, Int32, UInt64, or Int64 numeric types to Date, and if the number is greater than or equal to 65536, the number is interpreted as a Unix timestamp (and not as the number of days) and is rounded to the date.
This allows support for the common occurrence of writing `toDate(unix_timestamp)`, which otherwise would be an error and would require writing the more cumbersome `toDate(toDateTime(unix_timestamp))`.

Conversion between a date and a date with time is performed the natural way: by adding a null time or dropping the time.

Conversion between numeric types uses the same rules as assignments between different numeric types in C++.

**Example**

Query:

```sql
SELECT
    now() AS ts,
    time_zone,
    toString(ts, time_zone) AS str_tz_datetime
FROM system.time_zones
WHERE time_zone LIKE 'Europe%'
LIMIT 10
```

Result:

```response
┌──────────────────ts─┬─time_zone─────────┬─str_tz_datetime─────┐
│ 2023-09-08 19:14:59 │ Europe/Amsterdam  │ 2023-09-08 21:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Andorra    │ 2023-09-08 21:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Astrakhan  │ 2023-09-08 23:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Athens     │ 2023-09-08 22:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Belfast    │ 2023-09-08 20:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Belgrade   │ 2023-09-08 21:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Berlin     │ 2023-09-08 21:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Bratislava │ 2023-09-08 21:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Brussels   │ 2023-09-08 21:14:59 │
│ 2023-09-08 19:14:59 │ Europe/Bucharest  │ 2023-09-08 22:14:59 │
└─────────────────────┴───────────────────┴─────────────────────┘
```

Also see the [`toUnixTimestamp`](/sql-reference/functions/date-time-functions#toUnixTimestamp) function.

<!-- 
The inner content of the tags below are replaced at doc framework build time with 
docs generated from system.functions. Please do not modify or remove the tags.
See: https://github.com/ClickHouse/clickhouse-docs/blob/main/contribute/autogenerated-documentation-from-source.md
-->

<!--AUTOGENERATED_START-->
<!--AUTOGENERATED_END-->
