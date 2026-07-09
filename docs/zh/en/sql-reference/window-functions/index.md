---
description: '窗口函数概览页面'
sidebar_label: '窗口函数'
sidebar_position: 1
slug: /sql-reference/window-functions/
title: '窗口函数'
doc_type: 'reference'
---

窗口函数可让您对与当前行相关的一组行执行计算。
它们可用于执行与聚合函数类似的计算，但不同之处在于，窗口函数不会将多行分组为单个输出；相反，仍会返回每一行。

<div id="standard-window-functions">
  ## 标准窗口函数
</div>

ClickHouse 支持窗口及窗口函数的标准 SQL 语法。
下表显示了当前支持的功能：

| 功能                                                                   | 支持？ | 注释                                                                                                                                                                                                                                                                                                     |
| -------------------------------------------------------------------- | --- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| 即席窗口定义 (`count(*) OVER (PARTITION BY id ORDER BY time DESC)`)        | ✅   |                                                                                                                                                                                                                                                                                                        |
| 包含窗口函数的表达式，例如 `(count(*) OVER ()) / 2`                               | ✅   |                                                                                                                                                                                                                                                                                                        |
| `WINDOW` 子句 (`SELECT ... FROM table WINDOW w AS (PARTITION BY id)`)  | ✅   |                                                                                                                                                                                                                                                                                                        |
| `ROWS` 窗口帧                                                           | ✅   |                                                                                                                                                                                                                                                                                                        |
| `RANGE` 窗口帧                                                          | ✅   | 未显式指定窗口帧时，默认使用它 (`RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`) 。                                                                                                                                                                                                                                |
| 用于 `DateTime` `RANGE OFFSET` 窗口帧的 `INTERVAL` 语法                      | ❌   | 请改为指定秒数 (`RANGE` 适用于任何数值类型) 。                                                                                                                                                                                                                                                                          |
| `GROUPS` 窗口帧                                                         | ❌   |                                                                                                                                                                                                                                                                                                        |
| 在窗口帧上计算聚合函数 (`sum(value) OVER (ORDER BY time)`)                      | ✅   | 支持所有聚合函数。                                                                                                                                                                                                                                                                                              |
| `rank()`, `dense_rank()`/`denseRank()`, `row_number()`               | ✅   |                                                                                                                                                                                                                                                                                                        |
| `percent_rank()`/`percentRank()`                                     | ✅   | 可高效计算某个值在分区内的相对排名。它可替代更冗长且计算开销更高的手动 SQL 计算写法：`ifNull((rank() OVER (PARTITION BY x ORDER BY y) - 1) / nullif(count(1) OVER (PARTITION BY x) - 1, 0), 0)`。                                                                                                                                               |
| `cume_dist()`                                                        | ✅   | 计算某个值在一组值中的累计分布。返回值小于或等于当前行值的行的百分比。                                                                                                                                                                                                                                                                    |
| `lag/lead(value, offset)`                                            | ✅   | 你也可以使用以下变通方法之一：<br /> 1) `any(value) OVER (... ROWS BETWEEN <offset> PRECEDING AND <offset> PRECEDING)`，或者对 `lead` 使用 `FOLLOWING` 替代 `PRECEDING` <br /> 2) `lagInFrame/leadInFrame`，它们与 `lag/lead` 类似，但会遵循窗口帧。若要获得与 `lag/lead` 完全一致的行为，请使用 `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`。 |
| [`ntile(buckets)`](./ntile.md)                                       | ✅   | 例如，可将窗口指定为 `(PARTITION BY x ORDER BY y ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)`。                                                                                                                                                                                                     |

<div id="syntax">
  ## 语法
</div>

```text
aggregate_function (column_name)
  OVER ([[PARTITION BY grouping_column] [ORDER BY sorting_column] 
        [ROWS or RANGE expression_to_bound_rows_within_the_group]] | [window_name])
FROM table_name
WINDOW window_name as ([
  [PARTITION BY grouping_column]
  [ORDER BY sorting_column]
  [ROWS or RANGE expression_to_bound_rows_within_the_group]
])
```

* `PARTITION BY` - 定义如何将结果集划分为多个组。
* `ORDER BY` - 定义在计算 aggregate&#95;function 时，如何对组内的行进行排序。
* `ROWS or RANGE` - 定义窗口帧的边界，aggregate&#95;function 会在该窗口帧内计算。
* `WINDOW` - 允许多个表达式复用同一个窗口定义。

```text
      PARTITION
┌─────────────────┐  <-- UNBOUNDED PRECEDING (BEGINNING of the PARTITION)
│                 │
│                 │
│=================│  <-- N PRECEDING  <─┐
│      N ROWS     │                     │  F
│  Before CURRENT │                     │  R
│~~~~~~~~~~~~~~~~~│  <-- CURRENT ROW    │  A
│     M ROWS      │                     │  M
│   After CURRENT │                     │  E
│=================│  <-- M FOLLOWING  <─┘
│                 │
│                 │
└─────────────────┘  <--- UNBOUNDED FOLLOWING (END of the PARTITION)
```

<div id="functions">
  ## 仅可用作窗口函数的函数
</div>

以下函数只能用作窗口函数。其中大多数是标准 SQL 函数；`lagInFrame`、`leadInFrame` 和 `nonNegativeDerivative` 是 ClickHouse 扩展。

| Function                                                                                                   | Description                                                         |
| ---------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------- |
| [`row_number()`](./row_number.md)                                                                          | 从 1 开始对当前行在其分区内进行编号。                                                |
| [`first_value(x)`](./first_value.md)                                                                       | 返回其有序窗口帧内计算出的第一个值。                                                  |
| [`last_value(x)`](./last_value.md)                                                                         | 返回其有序窗口帧内计算出的最后一个值。                                                 |
| [`nth_value(x, offset)`](./nth_value.md)                                                                   | 返回其有序窗口帧中第 n 行 (偏移量) 对应的第一个非 NULL 值。                                |
| [`rank()`](./rank.md)                                                                                      | 对当前行在其分区内进行排名，排名中允许出现空缺。                                            |
| [`dense_rank()`](./dense_rank.md)                                                                          | 对当前行在其分区内进行排名，排名中不出现空缺。                                             |
| [`lagInFrame(x)`](./lagInFrame.md)                                                                         | 返回有序窗口帧内位于当前行之前指定物理偏移位置的行上计算出的值。                                    |
| [`leadInFrame(x)`](./leadInFrame.md)                                                                       | 返回有序窗口帧内位于当前行之后偏移若干行处计算出的值。                                         |
| [`ntile(buckets)`](./ntile.md)                                                                             | 将分区内的有序行划分为给定数量的桶，并返回当前行所在桶的编号。                                     |
| [`nonNegativeDerivative(metric_column, timestamp_column[, INTERVAL X UNITS])`](./nonNegativeDerivative.md) | 计算 `metric_column` 相对于 `timestamp_column` 的非负导数。该函数为 ClickHouse 特有。 |

<div id="examples">
  ## 示例
</div>

下面来看几个窗口函数的使用示例。

<div id="numbering-rows">
  ### 行编号
</div>

```sql
CREATE TABLE salaries
(
    `team` String,
    `player` String,
    `salary` UInt32,
    `position` String
)
Engine = Memory;

INSERT INTO salaries FORMAT Values
    ('Port Elizabeth Barbarians', 'Gary Chen', 195000, 'F'),
    ('New Coreystad Archdukes', 'Charles Juarez', 190000, 'F'),
    ('Port Elizabeth Barbarians', 'Michael Stanley', 150000, 'D'),
    ('New Coreystad Archdukes', 'Scott Harrison', 150000, 'D'),
    ('Port Elizabeth Barbarians', 'Robert George', 195000, 'M');
```

```sql
SELECT
    player,
    salary,
    row_number() OVER (ORDER BY salary ASC) AS row
FROM salaries;
```

```text
┌─player──────────┬─salary─┬─row─┐
│ Michael Stanley │ 150000 │   1 │
│ Scott Harrison  │ 150000 │   2 │
│ Charles Juarez  │ 190000 │   3 │
│ Gary Chen       │ 195000 │   4 │
│ Robert George   │ 195000 │   5 │
└─────────────────┴────────┴─────┘
```

```sql
SELECT
    player,
    salary,
    row_number() OVER (ORDER BY salary ASC) AS row,
    rank() OVER (ORDER BY salary ASC) AS rank,
    dense_rank() OVER (ORDER BY salary ASC) AS denseRank
FROM salaries;
```

```text
┌─player──────────┬─salary─┬─row─┬─rank─┬─denseRank─┐
│ Michael Stanley │ 150000 │   1 │    1 │         1 │
│ Scott Harrison  │ 150000 │   2 │    1 │         1 │
│ Charles Juarez  │ 190000 │   3 │    3 │         2 │
│ Gary Chen       │ 195000 │   4 │    4 │         3 │
│ Robert George   │ 195000 │   5 │    4 │         3 │
└─────────────────┴────────┴─────┴──────┴───────────┘
```

<div id="aggregation-functions">
  ### 聚合函数
</div>

比较每位球员的薪资与其所在球队的平均薪资。

```sql
SELECT
    player,
    salary,
    team,
    avg(salary) OVER (PARTITION BY team) AS teamAvg,
    salary - teamAvg AS diff
FROM salaries;
```

```text
┌─player──────────┬─salary─┬─team──────────────────────┬─teamAvg─┬───diff─┐
│ Charles Juarez  │ 190000 │ New Coreystad Archdukes   │  170000 │  20000 │
│ Scott Harrison  │ 150000 │ New Coreystad Archdukes   │  170000 │ -20000 │
│ Gary Chen       │ 195000 │ Port Elizabeth Barbarians │  180000 │  15000 │
│ Michael Stanley │ 150000 │ Port Elizabeth Barbarians │  180000 │ -30000 │
│ Robert George   │ 195000 │ Port Elizabeth Barbarians │  180000 │  15000 │
└─────────────────┴────────┴───────────────────────────┴─────────┴────────┘
```

将每位球员的薪资与其所在球队的最高薪资进行比较。

```sql
SELECT
    player,
    salary,
    team,
    max(salary) OVER (PARTITION BY team) AS teamMax,
    salary - teamMax AS diff
FROM salaries;
```

```text
┌─player──────────┬─salary─┬─team──────────────────────┬─teamMax─┬───diff─┐
│ Charles Juarez  │ 190000 │ New Coreystad Archdukes   │  190000 │      0 │
│ Scott Harrison  │ 150000 │ New Coreystad Archdukes   │  190000 │ -40000 │
│ Gary Chen       │ 195000 │ Port Elizabeth Barbarians │  195000 │      0 │
│ Michael Stanley │ 150000 │ Port Elizabeth Barbarians │  195000 │ -45000 │
│ Robert George   │ 195000 │ Port Elizabeth Barbarians │  195000 │      0 │
└─────────────────┴────────┴───────────────────────────┴─────────┴────────┘
```

<div id="partitioning-by-column">
  ### 按列分区
</div>

```sql
CREATE TABLE wf_partition
(
    `part_key` UInt64,
    `value` UInt64,
    `order` UInt64    
)
ENGINE = Memory;

INSERT INTO wf_partition FORMAT Values
   (1,1,1), (1,2,2), (1,3,3), (2,0,0), (3,0,0);

SELECT
    part_key,
    value,
    order,
    groupArray(value) OVER (PARTITION BY part_key) AS frame_values
FROM wf_partition
ORDER BY
    part_key ASC,
    value ASC;

┌─part_key─┬─value─┬─order─┬─frame_values─┐
│        1 │     1 │     1 │ [1,2,3]      │   <┐   
│        1 │     2 │     2 │ [1,2,3]      │    │  1-st group
│        1 │     3 │     3 │ [1,2,3]      │   <┘ 
│        2 │     0 │     0 │ [0]          │   <- 2-nd group
│        3 │     0 │     0 │ [0]          │   <- 3-d group
└──────────┴───────┴───────┴──────────────┘
```

<div id="frame-bounding">
  ### 窗口帧边界
</div>

```sql
CREATE TABLE wf_frame
(
    `part_key` UInt64,
    `value` UInt64,
    `order` UInt64
)
ENGINE = Memory;

INSERT INTO wf_frame FORMAT Values
   (1,1,1), (1,2,2), (1,3,3), (1,4,4), (1,5,5);
```

```sql
-- Frame is bounded by bounds of a partition (BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
SELECT
    part_key,
    value,
    order,
    groupArray(value) OVER (
        PARTITION BY part_key 
        ORDER BY order ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS frame_values
FROM wf_frame
ORDER BY
    part_key ASC,
    value ASC;
    
┌─part_key─┬─value─┬─order─┬─frame_values─┐
│        1 │     1 │     1 │ [1,2,3,4,5]  │
│        1 │     2 │     2 │ [1,2,3,4,5]  │
│        1 │     3 │     3 │ [1,2,3,4,5]  │
│        1 │     4 │     4 │ [1,2,3,4,5]  │
│        1 │     5 │     5 │ [1,2,3,4,5]  │
└──────────┴───────┴───────┴──────────────┘
```

```sql
-- short form - no bound expression, no order by,
-- an equalent of `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`
SELECT
    part_key,
    value,
    order,
    groupArray(value) OVER (PARTITION BY part_key) AS frame_values_short,
    groupArray(value) OVER (PARTITION BY part_key
         ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS frame_values
FROM wf_frame
ORDER BY
    part_key ASC,
    value ASC;
┌─part_key─┬─value─┬─order─┬─frame_values_short─┬─frame_values─┐
│        1 │     1 │     1 │ [1,2,3,4,5]        │ [1,2,3,4,5]  │
│        1 │     2 │     2 │ [1,2,3,4,5]        │ [1,2,3,4,5]  │
│        1 │     3 │     3 │ [1,2,3,4,5]        │ [1,2,3,4,5]  │
│        1 │     4 │     4 │ [1,2,3,4,5]        │ [1,2,3,4,5]  │
│        1 │     5 │     5 │ [1,2,3,4,5]        │ [1,2,3,4,5]  │
└──────────┴───────┴───────┴────────────────────┴──────────────┘
```

```sql
-- frame is bounded by the beginning of a partition and the current row
SELECT
    part_key,
    value,
    order,
    groupArray(value) OVER (
        PARTITION BY part_key 
        ORDER BY order ASC
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS frame_values
FROM wf_frame
ORDER BY
    part_key ASC,
    value ASC;

┌─part_key─┬─value─┬─order─┬─frame_values─┐
│        1 │     1 │     1 │ [1]          │
│        1 │     2 │     2 │ [1,2]        │
│        1 │     3 │     3 │ [1,2,3]      │
│        1 │     4 │     4 │ [1,2,3,4]    │
│        1 │     5 │     5 │ [1,2,3,4,5]  │
└──────────┴───────┴───────┴──────────────┘
```

```sql
-- short form (frame is bounded by the beginning of a partition and the current row)
-- an equalent of `ORDER BY order ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`
SELECT
    part_key,
    value,
    order,
    groupArray(value) OVER (PARTITION BY part_key ORDER BY order ASC) AS frame_values_short,
    groupArray(value) OVER (PARTITION BY part_key ORDER BY order ASC
       ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS frame_values
FROM wf_frame
ORDER BY
    part_key ASC,
    value ASC;

┌─part_key─┬─value─┬─order─┬─frame_values_short─┬─frame_values─┐
│        1 │     1 │     1 │ [1]                │ [1]          │
│        1 │     2 │     2 │ [1,2]              │ [1,2]        │
│        1 │     3 │     3 │ [1,2,3]            │ [1,2,3]      │
│        1 │     4 │     4 │ [1,2,3,4]          │ [1,2,3,4]    │
│        1 │     5 │     5 │ [1,2,3,4,5]        │ [1,2,3,4,5]  │
└──────────┴───────┴───────┴────────────────────┴──────────────┘
```

```sql
-- frame is bounded by the beginning of a partition and the current row, but order is backward
SELECT
    part_key,
    value,
    order,
    groupArray(value) OVER (PARTITION BY part_key ORDER BY order DESC) AS frame_values
FROM wf_frame
ORDER BY
    part_key ASC,
    value ASC;

┌─part_key─┬─value─┬─order─┬─frame_values─┐
│        1 │     1 │     1 │ [5,4,3,2,1]  │
│        1 │     2 │     2 │ [5,4,3,2]    │
│        1 │     3 │     3 │ [5,4,3]      │
│        1 │     4 │     4 │ [5,4]        │
│        1 │     5 │     5 │ [5]          │
└──────────┴───────┴───────┴──────────────┘
```

```sql
-- sliding frame - 1 PRECEDING ROW AND CURRENT ROW
SELECT
    part_key,
    value,
    order,
    groupArray(value) OVER (
        PARTITION BY part_key 
        ORDER BY order ASC
        ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
    ) AS frame_values
FROM wf_frame
ORDER BY
    part_key ASC,
    value ASC;

┌─part_key─┬─value─┬─order─┬─frame_values─┐
│        1 │     1 │     1 │ [1]          │
│        1 │     2 │     2 │ [1,2]        │
│        1 │     3 │     3 │ [2,3]        │
│        1 │     4 │     4 │ [3,4]        │
│        1 │     5 │     5 │ [4,5]        │
└──────────┴───────┴───────┴──────────────┘
```

```sql
-- sliding frame - ROWS BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING 
SELECT
    part_key,
    value,
    order,
    groupArray(value) OVER (
        PARTITION BY part_key 
        ORDER BY order ASC
        ROWS BETWEEN 1 PRECEDING AND UNBOUNDED FOLLOWING
    ) AS frame_values
FROM wf_frame
ORDER BY
    part_key ASC,
    value ASC;

┌─part_key─┬─value─┬─order─┬─frame_values─┐
│        1 │     1 │     1 │ [1,2,3,4,5]  │
│        1 │     2 │     2 │ [1,2,3,4,5]  │
│        1 │     3 │     3 │ [2,3,4,5]    │
│        1 │     4 │     4 │ [3,4,5]      │
│        1 │     5 │     5 │ [4,5]        │
└──────────┴───────┴───────┴──────────────┘
```

```sql
-- row_number does not respect the frame, so rn_1 = rn_2 = rn_3 != rn_4
SELECT
    part_key,
    value,
    order,
    groupArray(value) OVER w1 AS frame_values,
    row_number() OVER w1 AS rn_1,
    sum(1) OVER w1 AS rn_2,
    row_number() OVER w2 AS rn_3,
    sum(1) OVER w2 AS rn_4
FROM wf_frame
WINDOW
    w1 AS (PARTITION BY part_key ORDER BY order DESC),
    w2 AS (
        PARTITION BY part_key 
        ORDER BY order DESC 
        ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
    )
ORDER BY
    part_key ASC,
    value ASC;

┌─part_key─┬─value─┬─order─┬─frame_values─┬─rn_1─┬─rn_2─┬─rn_3─┬─rn_4─┐
│        1 │     1 │     1 │ [5,4,3,2,1]  │    5 │    5 │    5 │    2 │
│        1 │     2 │     2 │ [5,4,3,2]    │    4 │    4 │    4 │    2 │
│        1 │     3 │     3 │ [5,4,3]      │    3 │    3 │    3 │    2 │
│        1 │     4 │     4 │ [5,4]        │    2 │    2 │    2 │    2 │
│        1 │     5 │     5 │ [5]          │    1 │    1 │    1 │    1 │
└──────────┴───────┴───────┴──────────────┴──────┴──────┴──────┴──────┘
```

```sql
-- first_value and last_value respect the frame
SELECT
    groupArray(value) OVER w1 AS frame_values_1,
    first_value(value) OVER w1 AS first_value_1,
    last_value(value) OVER w1 AS last_value_1,
    groupArray(value) OVER w2 AS frame_values_2,
    first_value(value) OVER w2 AS first_value_2,
    last_value(value) OVER w2 AS last_value_2
FROM wf_frame
WINDOW
    w1 AS (PARTITION BY part_key ORDER BY order ASC),
    w2 AS (PARTITION BY part_key ORDER BY order ASC ROWS BETWEEN 1 PRECEDING AND CURRENT ROW)
ORDER BY
    part_key ASC,
    value ASC;

┌─frame_values_1─┬─first_value_1─┬─last_value_1─┬─frame_values_2─┬─first_value_2─┬─last_value_2─┐
│ [1]            │             1 │            1 │ [1]            │             1 │            1 │
│ [1,2]          │             1 │            2 │ [1,2]          │             1 │            2 │
│ [1,2,3]        │             1 │            3 │ [2,3]          │             2 │            3 │
│ [1,2,3,4]      │             1 │            4 │ [3,4]          │             3 │            4 │
│ [1,2,3,4,5]    │             1 │            5 │ [4,5]          │             4 │            5 │
└────────────────┴───────────────┴──────────────┴────────────────┴───────────────┴──────────────┘
```

```sql
-- second value within the frame
SELECT
    groupArray(value) OVER w1 AS frame_values_1,
    nth_value(value, 2) OVER w1 AS second_value
FROM wf_frame
WINDOW w1 AS (PARTITION BY part_key ORDER BY order ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
ORDER BY
    part_key ASC,
    value ASC;

┌─frame_values_1─┬─second_value─┐
│ [1]            │            0 │
│ [1,2]          │            2 │
│ [1,2,3]        │            2 │
│ [1,2,3,4]      │            2 │
│ [2,3,4,5]      │            3 │
└────────────────┴──────────────┘
```

```sql
-- second value within the frame + Null for missing values
SELECT
    groupArray(value) OVER w1 AS frame_values_1,
    nth_value(toNullable(value), 2) OVER w1 AS second_value
FROM wf_frame
WINDOW w1 AS (PARTITION BY part_key ORDER BY order ASC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
ORDER BY
    part_key ASC,
    value ASC;

┌─frame_values_1─┬─second_value─┐
│ [1]            │         ᴺᵁᴸᴸ │
│ [1,2]          │            2 │
│ [1,2,3]        │            2 │
│ [1,2,3,4]      │            2 │
│ [2,3,4,5]      │            3 │
└────────────────┴──────────────┘
```

<div id="real-world-examples">
  ## 实际应用示例
</div>

以下示例用于解决常见的实际问题。

<div id="maximumtotal-salary-per-department">
  ### 各部门的最高/总薪资
</div>

```sql
CREATE TABLE employees
(
    `department` String,
    `employee_name` String,
    `salary` Float
)
ENGINE = Memory;

INSERT INTO employees FORMAT Values
   ('Finance', 'Jonh', 200),
   ('Finance', 'Joan', 210),
   ('Finance', 'Jean', 505),
   ('IT', 'Tim', 200),
   ('IT', 'Anna', 300),
   ('IT', 'Elen', 500);
```

```sql
SELECT
    department,
    employee_name AS emp,
    salary,
    max_salary_per_dep,
    total_salary_per_dep,
    round((salary / total_salary_per_dep) * 100, 2) AS `share_per_dep(%)`
FROM
(
    SELECT
        department,
        employee_name,
        salary,
        max(salary) OVER wndw AS max_salary_per_dep,
        sum(salary) OVER wndw AS total_salary_per_dep
    FROM employees
    WINDOW wndw AS (
        PARTITION BY department
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )
    ORDER BY
        department ASC,
        employee_name ASC
);

┌─department─┬─emp──┬─salary─┬─max_salary_per_dep─┬─total_salary_per_dep─┬─share_per_dep(%)─┐
│ Finance    │ Jean │    505 │                505 │                  915 │            55.19 │
│ Finance    │ Joan │    210 │                505 │                  915 │            22.95 │
│ Finance    │ Jonh │    200 │                505 │                  915 │            21.86 │
│ IT         │ Anna │    300 │                500 │                 1000 │               30 │
│ IT         │ Elen │    500 │                500 │                 1000 │               50 │
│ IT         │ Tim  │    200 │                500 │                 1000 │               20 │
└────────────┴──────┴────────┴────────────────────┴──────────────────────┴──────────────────┘
```

<div id="cumulative-sum">
  ### 累计和
</div>

```sql
CREATE TABLE warehouse
(
    `item` String,
    `ts` DateTime,
    `value` Float
)
ENGINE = Memory

INSERT INTO warehouse VALUES
    ('sku38', '2020-01-01', 9),
    ('sku38', '2020-02-01', 1),
    ('sku38', '2020-03-01', -4),
    ('sku1', '2020-01-01', 1),
    ('sku1', '2020-02-01', 1),
    ('sku1', '2020-03-01', 1);
```

```sql
SELECT
    item,
    ts,
    value,
    sum(value) OVER (PARTITION BY item ORDER BY ts ASC) AS stock_balance
FROM warehouse
ORDER BY
    item ASC,
    ts ASC;

┌─item──┬──────────────────ts─┬─value─┬─stock_balance─┐
│ sku1  │ 2020-01-01 00:00:00 │     1 │             1 │
│ sku1  │ 2020-02-01 00:00:00 │     1 │             2 │
│ sku1  │ 2020-03-01 00:00:00 │     1 │             3 │
│ sku38 │ 2020-01-01 00:00:00 │     9 │             9 │
│ sku38 │ 2020-02-01 00:00:00 │     1 │            10 │
│ sku38 │ 2020-03-01 00:00:00 │    -4 │             6 │
└───────┴─────────────────────┴───────┴───────────────┘
```

<div id="moving--sliding-average-per-3-rows">
  ### 移动平均 / 滑动平均 (每 3 行)
</div>

```sql
CREATE TABLE sensors
(
    `metric` String,
    `ts` DateTime,
    `value` Float
)
ENGINE = Memory;

insert into sensors values('cpu_temp', '2020-01-01 00:00:00', 87),
                          ('cpu_temp', '2020-01-01 00:00:01', 77),
                          ('cpu_temp', '2020-01-01 00:00:02', 93),
                          ('cpu_temp', '2020-01-01 00:00:03', 87),
                          ('cpu_temp', '2020-01-01 00:00:04', 87),
                          ('cpu_temp', '2020-01-01 00:00:05', 87),
                          ('cpu_temp', '2020-01-01 00:00:06', 87),
                          ('cpu_temp', '2020-01-01 00:00:07', 87);
```

```sql
SELECT
    metric,
    ts,
    value,
    avg(value) OVER (
        PARTITION BY metric 
        ORDER BY ts ASC 
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS moving_avg_temp
FROM sensors
ORDER BY
    metric ASC,
    ts ASC;

┌─metric───┬──────────────────ts─┬─value─┬───moving_avg_temp─┐
│ cpu_temp │ 2020-01-01 00:00:00 │    87 │                87 │
│ cpu_temp │ 2020-01-01 00:00:01 │    77 │                82 │
│ cpu_temp │ 2020-01-01 00:00:02 │    93 │ 85.66666666666667 │
│ cpu_temp │ 2020-01-01 00:00:03 │    87 │ 85.66666666666667 │
│ cpu_temp │ 2020-01-01 00:00:04 │    87 │                89 │
│ cpu_temp │ 2020-01-01 00:00:05 │    87 │                87 │
│ cpu_temp │ 2020-01-01 00:00:06 │    87 │                87 │
│ cpu_temp │ 2020-01-01 00:00:07 │    87 │                87 │
└──────────┴─────────────────────┴───────┴───────────────────┘
```

<div id="moving--sliding-average-per-10-seconds">
  ### 移动平均 / 滑动平均 (每 10 秒)
</div>

```sql
SELECT
    metric,
    ts,
    value,
    avg(value) OVER (PARTITION BY metric ORDER BY ts
      RANGE BETWEEN 10 PRECEDING AND CURRENT ROW) AS moving_avg_10_seconds_temp
FROM sensors
ORDER BY
    metric ASC,
    ts ASC;
    
┌─metric───┬──────────────────ts─┬─value─┬─moving_avg_10_seconds_temp─┐
│ cpu_temp │ 2020-01-01 00:00:00 │    87 │                         87 │
│ cpu_temp │ 2020-01-01 00:01:10 │    77 │                         77 │
│ cpu_temp │ 2020-01-01 00:02:20 │    93 │                         93 │
│ cpu_temp │ 2020-01-01 00:03:30 │    87 │                         87 │
│ cpu_temp │ 2020-01-01 00:04:40 │    87 │                         87 │
│ cpu_temp │ 2020-01-01 00:05:50 │    87 │                         87 │
│ cpu_temp │ 2020-01-01 00:06:00 │    87 │                         87 │
│ cpu_temp │ 2020-01-01 00:07:10 │    87 │                         87 │
└──────────┴─────────────────────┴───────┴────────────────────────────┘
```

<div id="moving--sliding-average-per-10-days">
  ### 移动 / 滑动平均 (每 10 天)
</div>

温度以秒级精度存储，但通过使用 `Range` 和 `ORDER BY toDate(ts)`，我们构造了一个大小为 10 个单位的窗口帧；由于使用了 `toDate(ts)`，这里的单位是天。

```sql
CREATE TABLE sensors
(
    `metric` String,
    `ts` DateTime,
    `value` Float
)
ENGINE = Memory;

insert into sensors values('ambient_temp', '2020-01-01 00:00:00', 16),
                          ('ambient_temp', '2020-01-01 12:00:00', 16),
                          ('ambient_temp', '2020-01-02 11:00:00', 9),
                          ('ambient_temp', '2020-01-02 12:00:00', 9),                          
                          ('ambient_temp', '2020-02-01 10:00:00', 10),
                          ('ambient_temp', '2020-02-01 12:00:00', 10),
                          ('ambient_temp', '2020-02-10 12:00:00', 12),                          
                          ('ambient_temp', '2020-02-10 13:00:00', 12),
                          ('ambient_temp', '2020-02-20 12:00:01', 16),
                          ('ambient_temp', '2020-03-01 12:00:00', 16),
                          ('ambient_temp', '2020-03-01 12:00:00', 16),
                          ('ambient_temp', '2020-03-01 12:00:00', 16);
```

```sql
SELECT
    metric,
    ts,
    value,
    round(avg(value) OVER (PARTITION BY metric ORDER BY toDate(ts) 
       RANGE BETWEEN 10 PRECEDING AND CURRENT ROW),2) AS moving_avg_10_days_temp
FROM sensors
ORDER BY
    metric ASC,
    ts ASC;

┌─metric───────┬──────────────────ts─┬─value─┬─moving_avg_10_days_temp─┐
│ ambient_temp │ 2020-01-01 00:00:00 │    16 │                      16 │
│ ambient_temp │ 2020-01-01 12:00:00 │    16 │                      16 │
│ ambient_temp │ 2020-01-02 11:00:00 │     9 │                    12.5 │
│ ambient_temp │ 2020-01-02 12:00:00 │     9 │                    12.5 │
│ ambient_temp │ 2020-02-01 10:00:00 │    10 │                      10 │
│ ambient_temp │ 2020-02-01 12:00:00 │    10 │                      10 │
│ ambient_temp │ 2020-02-10 12:00:00 │    12 │                      11 │
│ ambient_temp │ 2020-02-10 13:00:00 │    12 │                      11 │
│ ambient_temp │ 2020-02-20 12:00:01 │    16 │                   13.33 │
│ ambient_temp │ 2020-03-01 12:00:00 │    16 │                      16 │
│ ambient_temp │ 2020-03-01 12:00:00 │    16 │                      16 │
│ ambient_temp │ 2020-03-01 12:00:00 │    16 │                      16 │
└──────────────┴─────────────────────┴───────┴─────────────────────────┘
```

<div id="references">
  ## 参考资料
</div>

<div id="github-issues">
  ### GitHub Issues
</div>

有关窗口函数初步支持的路线图，请参见[此 issue](https://github.com/ClickHouse/ClickHouse/issues/18097)。

所有与窗口函数相关的 GitHub issue 都带有 [comp-window-functions](https://github.com/ClickHouse/ClickHouse/labels/comp-window-functions) 标签。

<div id="tests">
  ### 测试
</div>

这些测试包含当前已支持语法的示例：

https://github.com/ClickHouse/ClickHouse/blob/master/tests/performance/window&#95;functions.xml

https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0&#95;stateless/01591&#95;window&#95;functions.sql

<div id="postgres-docs">
  ### Postgres 文档
</div>

https://www.postgresql.org/docs/current/sql-select.html#SQL-WINDOW

https://www.postgresql.org/docs/devel/sql-expressions.html#SYNTAX-WINDOW-FUNCTIONS

https://www.postgresql.org/docs/devel/functions-window.html

https://www.postgresql.org/docs/devel/tutorial-window.html

<div id="mysql-docs">
  ### MySQL 文档
</div>

https://dev.mysql.com/doc/refman/8.0/en/window-function-descriptions.html

https://dev.mysql.com/doc/refman/8.0/en/window-functions-usage.html

https://dev.mysql.com/doc/refman/8.0/en/window-functions-frames.html

<div id="related-content">
  ## 相关内容
</div>

* 博客：[在 ClickHouse 中使用时间序列数据](https://clickhouse.com/blog/working-with-time-series-data-and-functions-ClickHouse)
* 博客：[适用于 Git 提交序列的窗口函数和数组函数](https://clickhouse.com/blog/clickhouse-window-array-functions-git-commits)
* 博客：[将数据导入 ClickHouse - 第 3 部分 - 使用 S3](https://clickhouse.com/blog/getting-data-into-clickhouse-part-3-s3)