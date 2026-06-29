---
description: 'Documentation for the ntile window function'
sidebar_label: 'ntile'
sidebar_position: 13
slug: /sql-reference/window-functions/ntile
title: 'ntile'
doc_type: 'reference'
---

Divides the ordered rows within a partition into a specified number of buckets (groups) of as equal a size as possible, and returns the bucket number that the current row belongs to. Buckets are numbered starting from 1. For each partition, the rows are assigned to buckets in order: if the number of rows is not divisible by the number of buckets, the earlier buckets receive one more row than the later ones.

**Syntax**

```sql
ntile (buckets)
  OVER ([[PARTITION BY grouping_column] [ORDER BY sorting_column]
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING] | [window_name])
FROM table_name
WINDOW window_name as ([PARTITION BY grouping_column] [ORDER BY sorting_column] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
```

The argument `buckets` must be a constant positive integer.

An `ORDER BY` clause is required. The window frame must be the whole partition (`ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`), which is also the default frame used when none is specified explicitly.

For more detail on window function syntax see: [Window Functions - Syntax](./index.md/#syntax).

**Returned value**

- The bucket number of the current row within its partition. [UInt64](../data-types/int-uint.md).

**Example**

The following example divides the players into four buckets ordered by descending salary.

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
       ntile(4) OVER (ORDER BY salary DESC, player ASC) AS bucket
FROM salaries;
```

```response title="Response"
   ┌─player──────────┬─salary─┬─bucket─┐
1. │ Gary Chen       │ 195000 │      1 │
2. │ Robert George   │ 195000 │      1 │
3. │ Charles Juarez  │ 190000 │      2 │
4. │ Douglas Benson  │ 150000 │      2 │
5. │ Michael Stanley │ 150000 │      3 │
6. │ Scott Harrison  │ 150000 │      3 │
7. │ James Henderson │ 140000 │      4 │
   └─────────────────┴────────┴────────┘
```

Here there are seven rows and four buckets, so the first three buckets contain two rows each and the last bucket contains a single row.
