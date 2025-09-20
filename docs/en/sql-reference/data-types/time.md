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

Data type `Time` represents a time with hour, minute, and second components. It is independent of any calendar date and is suitable for values where only the time-of-day component is needed.

Syntax:

``` sql
Time
```

Text representation range: [-999:59:59, 999:59:59].

Resolution: 1 second.

## Performance {#performance}

Operations on `Time` are comparable to `DateTime` because both are represented as integers internally. Choose the type based on semantics and storage size, not raw speed.

Storage size:
- `Time`: 4 bytes per value (signed 32-bit integer)
- `DateTime`: 4 bytes per value
- `Date`: 2 bytes per value

On-disk size after compression depends on data distribution. Narrower types often compress better, but the actual difference is workload- and data-dependent.

## Implementation details {#implementation-details}

- Representation: `Time` stores a signed 32-bit integer that encodes seconds since 00:00:00 (no date component is stored).
- Normalization: When parsing text, components are normalized rather than strictly validated. For example, `25:70:70` is interpreted as `26:11:10`.
- Negative values: A leading minus sign is supported and preserved on output. Negative values typically arise from arithmetic; they do not represent a calendar concept of "negative time-of-day". When parsing from text, negative inputs are clamped to `00:00:00`; numeric inputs preserve the sign.
- Display saturation: When formatting values for output, hours above 999 are saturated to `999:59:59`.
- Time zones: `Time` does not support time zones. The value is interpreted without any regional context. Specifying a time zone for `Time` (as a type parameter or during value creation) throws an error.

Note: Attempting to apply or change a time zone on `Time` columns is not supported and results in an error. `Time` values are not silently reinterpreted under different time zones.

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
