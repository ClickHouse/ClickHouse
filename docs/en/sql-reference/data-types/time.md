---
description: 'Documentation for the Time data type in ClickHouse, which stores
  the time range with second precision'
slug: /sql-reference/data-types/time
sidebar_position: 15
sidebar_label: 'Time'
title: 'Time'
doc_type: 'reference'
---

# Time

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
