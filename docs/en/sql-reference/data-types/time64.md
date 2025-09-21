---
description: 'Documentation for the Time64 data type in ClickHouse, which stores
  the time range with sub-second precision'
slug: /sql-reference/data-types/time64
sidebar_position: 17
sidebar_label: 'Time64'
title: 'Time64'
doc_type: 'reference'
---

# Time64

Data type `Time64` represents a time-of-day with fractional seconds.
It has no calendar date components (day, month, year).
The `precision` parameter defines the number of fractional digits and therefore the tick size.

Tick size (precision): 10<sup>-precision</sup> seconds. Valid range: 0..9. Common choices are 3 (milliseconds), 6 (microseconds), and 9 (nanoseconds).

**Syntax:**

``` sql
Time64(precision)
```

Internally, `Time64` stores a signed 64-bit decimal (Decimal64) number of fractional seconds since 00:00:00.
The tick resolution is determined by the `precision` parameter.
Time zones are not supported: specifying a time zone with `Time64` will throw an error.

Unlike `DateTime64`, `Time64` does not store a date component.
See also [`Time`](../../sql-reference/data-types/time.md).

Displayed range of values: 00:00:00.000 to 999:59:59.999 for `precision = 3` (the number of fractional digits depends on `precision`).

## Implementation details {#implementation-details}

**Representation**.
Signed `Decimal64` value counting fractional seconds since 00:00:00 with `precision` fractional digits.

**Normalization**.
When parsing strings to `Time64`, the time components are normalized and not validated.
For example, `25:70:70` is interpreted as `26:11:10`.

**Negative values**
Leading minus signs are supported and preserved.
Negative values typically arise from arithmetic operations on `Time64` values.
When parsing text to `Time64`, negative inputs are clamped to `00:00:00`; numeric inputs preserve the sign.

**Display saturation**.
When formatting values for output, hours above 999 are saturated to `999:59:59.xxx`.

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
3. │        3 │ 14:30:25.000 │
   └──────────┴──────────────┘
```

Unlike `Time`, `Time64` values are not converted from `String` automatically.

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
