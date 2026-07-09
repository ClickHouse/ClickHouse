---
description: 'nonNegativeDerivative 窗口函数的文档'
sidebar_label: 'nonNegativeDerivative'
sidebar_position: 12
slug: /sql-reference/window-functions/nonNegativeDerivative
title: 'nonNegativeDerivative'
doc_type: 'reference'
---

计算 `metric_column` 相对于 `timestamp_column` 的非负导数。
这是一个 ClickHouse 特有的窗口函数，不属于标准 SQL。

对于每一行，导数是相对于*窗口求值顺序中的前一行*计算的，该顺序由窗口的 `ORDER BY` 子句决定，而非由 `timestamp_column` 决定。
`timestamp_column` 参数仅用于测量当前行与前一行之间的时间差，其本身不参与行的排序。

:::warning
`nonNegativeDerivative` 不会按 `timestamp_column` 对行排序，排序由窗口的 `ORDER BY` 完成。
为使下方公式成立，`timestamp_column` 在窗口求值顺序中必须严格递增，因此通常应按 `timestamp_column` 升序对窗口排序（例如将 `... OVER (ORDER BY ts ASC)` 与 `nonNegativeDerivative(metric, ts)` 配合使用）。
当当前行与前一行之间的时间差为非正值时（即使用 `ORDER BY timestamp_column DESC` 或存在重复时间戳时），该函数对该行返回 `0`，而不按公式计算。
:::

结果为每个 `INTERVAL` 内指标的变化率，所有负值均被截断为 `0`。
这对于单调递增的指标（如计数器）非常有用——数值下降通常意味着发生了重置，而非真实的负变化率。

**语法**

```sql
nonNegativeDerivative(metric_column, timestamp_column[, INTERVAL X UNITS])
  OVER ([[PARTITION BY grouping_column] [ORDER BY sorting_column]
        [ROWS or RANGE expression_to_bound_rows_within_the_group]] | [window_name])
FROM table_name
WINDOW window_name AS ([PARTITION BY grouping_column] [ORDER BY sorting_column] [ROWS or RANGE expression_to_bound_rows_within_the_group])
```

有关窗口函数语法的更多详情，请参阅：[窗口函数 - 语法](./index.md/#syntax)。

**参数**

- `metric_column` — 需要计算导数的列。[(U)Int*](../data-types/int-uint.md) 或 [Float*](../data-types/float.md)。
- `timestamp_column` — 用于测量窗口顺序中当前行与前一行之间时间差的列。该列不参与行的排序，排序由窗口的 `ORDER BY` 完成，通常应使用同一列。[DateTime](../data-types/datetime.md) 或 [DateTime64](../data-types/datetime64.md)。
- `INTERVAL X UNITS` — 可选。结果换算到的时间单位，默认为 `INTERVAL 1 SECOND`。仅支持固定长度单位（`NANOSECOND`、`MICROSECOND`、`MILLISECOND`、`SECOND`、`MINUTE`、`HOUR`、`DAY`、`WEEK`）；可变长度单位（`MONTH`、`QUARTER`、`YEAR`）会抛出异常。

**返回值**

对于每一行，计算方式如下：

- 第一行返回 `0`；
- 与前一行时间差为非正值的行（即 $\text{timestamp}_i - \text{timestamp}_{i-1} \le 0$，如降序排列或存在重复时间戳时）返回 `0`；
- 其余情况返回 ${\text{metric}_i - \text{metric}_{i-1} \over \text{timestamp}_i - \text{timestamp}_{i-1}} * \text{interval}$。

若计算结果为负值，则截断为 `0`。返回类型为 [Float64](../data-types/float.md)。

**示例**

以下示例计算传感器读数的每秒变化率。
注意第三行的值从 `110` 降至 `105`，因此其导数被截断为 `0`。

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