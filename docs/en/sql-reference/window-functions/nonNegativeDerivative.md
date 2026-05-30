---
description: 'Documentation for the nonNegativeDerivative window function'
sidebar_label: 'nonNegativeDerivative'
sidebar_position: 12
slug: /sql-reference/window-functions/nonNegativeDerivative
title: 'nonNegativeDerivative'
doc_type: 'reference'
---

Computes the non-negative derivative of `metric_column` with respect to `timestamp_column`.
This is a ClickHouse-specific window function, not part of standard SQL.

The result is the rate of change of the metric per `INTERVAL`, with any negative value clamped to `0`.
This is useful for monotonically increasing metrics, such as counters, where a decrease usually indicates a reset rather than a real negative rate.

**Syntax**

```sql
nonNegativeDerivative(metric_column, timestamp_column[, INTERVAL X UNITS])
  OVER ([[PARTITION BY grouping_column] [ORDER BY sorting_column]
        [ROWS or RANGE expression_to_bound_rows_within_the_group]] | [window_name])
FROM table_name
WINDOW window_name as ([[PARTITION BY grouping_column] [ORDER BY sorting_column])
```

For more detail on window function syntax see: [Window Functions - Syntax](./index.md/#syntax).

**Arguments**

- `metric_column` — The column whose derivative is computed. [(U)Int*](../data-types/int-uint.md) or [Float*](../data-types/float.md).
- `timestamp_column` — The column used to order rows and to measure the time difference between them. [DateTime](../data-types/datetime.md) or [DateTime64](../data-types/datetime64.md).
- `INTERVAL X UNITS` — Optional. The time unit the result is scaled to. Defaults to `INTERVAL 1 SECOND`. Only fixed-length units are supported (`NANOSECOND`, `MICROSECOND`, `MILLISECOND`, `SECOND`, `MINUTE`, `HOUR`, `DAY`, `WEEK`); variable-length units (`MONTH`, `QUARTER`, `YEAR`) raise an exception.

**Returned value**

For each row, the value is computed as:

- `0` for the first row, and
- ${\text{metric}_i - \text{metric}_{i-1} \over \text{timestamp}_i - \text{timestamp}_{i-1}} * \text{interval}$ for the $i$-th row.

If the computed value would be negative, it is clamped to `0`. The return type is [Float64](../data-types/float.md).

**Example**

The following example computes the per-second rate of change of a sensor reading.
Note that the third row drops from `110` to `105`, so its derivative is clamped to `0`.

```sql title="Query"
CREATE TABLE sensor_readings
(
    `sensor_id` UInt32,
    `ts`        DateTime,
    `reading`   Float64
)
ENGINE = Memory;

INSERT INTO sensor_readings VALUES
    (1, '2024-01-01 00:00:00', 100),
    (1, '2024-01-01 00:00:10', 110),
    (1, '2024-01-01 00:00:20', 105),
    (1, '2024-01-01 00:00:30', 130);
```

```sql title="Query"
SELECT
    ts,
    reading,
    nonNegativeDerivative(reading, ts) OVER (ORDER BY ts ASC) AS deriv_per_second
FROM sensor_readings
ORDER BY ts ASC;
```

```response title="Response"
   ┌──────────────────ts─┬─reading─┬─deriv_per_second─┐
1. │ 2024-01-01 00:00:00 │     100 │                0 │
2. │ 2024-01-01 00:00:10 │     110 │                1 │
3. │ 2024-01-01 00:00:20 │     105 │                0 │
4. │ 2024-01-01 00:00:30 │     130 │              2.5 │
   └─────────────────────┴─────────┴──────────────────┘
```