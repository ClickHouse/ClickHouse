---
description: 'Documentation for the cume_dist window function'
sidebar_label: 'cume_dist'
sidebar_position: 11
slug: /sql-reference/window-functions/cume_dist
title: 'cume_dist'
doc_type: 'reference'
---

Computes the cumulative distribution of a value within a group of values, i.e., the percentage of rows with values less than or equal to the current row's value. Can be used to determine relative standing of a value within a partition.

**Syntax**

```sql
cume_dist ()
  OVER ([[PARTITION BY grouping_column] [ORDER BY sorting_column]
        [RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]] | [window_name])
FROM table_name
WINDOW window_name as ([PARTITION BY grouping_column] [ORDER BY sorting_column] RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
```

The default and required window frame definition is `RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`.

For more detail on window function syntax see: [Window Functions - Syntax](./index.md/#syntax).

**Returned value**

- The relative rank of the current row. The return type is Float64 in the range [0, 1]. [Float64](../data-types/float.md).

**Example**

The following example calculates the cumulative distribution of salaries within a team:

```sql title="Query"
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
    ('Port Elizabeth Barbarians', 'Robert George', 195000, 'M'),
    ('South Hampton Seagulls', 'Douglas Benson', 150000, 'M'),
    ('South Hampton Seagulls', 'James Henderson', 140000, 'M');
```

```sql title="Query"
SELECT player, salary,
       cume_dist() OVER (ORDER BY salary DESC) AS cume_dist
FROM salaries;
```

```response title="Response"
   ┌─player──────────┬─salary─┬───────────cume_dist─┐
1. │ Robert George   │ 195000 │  0.2857142857142857 │
2. │ Gary Chen       │ 195000 │  0.2857142857142857 │
3. │ Charles Juarez  │ 190000 │ 0.42857142857142855 │
4. │ Douglas Benson  │ 150000 │  0.8571428571428571 │
5. │ Michael Stanley │ 150000 │  0.8571428571428571 │
6. │ Scott Harrison  │ 150000 │  0.8571428571428571 │
7. │ James Henderson │ 140000 │                   1 │
   └─────────────────┴────────┴─────────────────────┘
```

**Implementation Details**

The `cume_dist()` function calculates the relative position using the following formula:

```text
cume_dist = (number of rows ≤ current row value) / (total number of rows in partition)
```

Rows with equal values (peers) receive the same cumulative distribution value, which corresponds to the highest position of the peer group.
