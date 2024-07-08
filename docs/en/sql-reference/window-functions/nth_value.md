---
slug: /en/sql-reference/window-functions/leadInFrame
sidebar_label: leadInFrame
sidebar_position: 5
---

# nth_value

Return the first non-NULL value evaluated against the nth row (offset) in its ordered frame.

The [dense_rank](./dense_rank.md) function provides the same behaviour but without gaps in ranking.

**Syntax**

```sql
nth_value (x, offset)
  OVER ([[PARTITION BY grouping_column] [ORDER BY sorting_column] 
        [ROWS or RANGE expression_to_bound_rows_withing_the_group]] | [window_name])
FROM table_name
WINDOW window_name as ([[PARTITION BY grouping_column] [ORDER BY sorting_column])
```

For more detail on window function syntax see: [Window Functions - Syntax](./index.md/#syntax).

**Parameters**

- `x` — Column name.
- `offset` — nth row to evaluate current row against.

**Returned value**

- The first non-NULL value evaluated against the nth row (offset) in its ordered frame.

**Example**

In this example the `nth-value` function is used to find the third-highest salary from a fictional dataset of salaries of Premier League football players.

Query:

```sql
DROP TABLE IF EXISTS salaries;
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
    ('Port Elizabeth Barbarians', 'Michael Stanley', 10000, 'D'),
    ('New Coreystad Archdukes', 'Scott Harrison', 180000, 'D'),
    ('Port Elizabeth Barbarians', 'Robert George', 195000, 'M'),
    ('South Hampton Seagulls', 'Douglas Benson', 150000, 'M'),
    ('South Hampton Seagulls', 'James Henderson', 140000, 'M');
```

```sql
SELECT salary, nth_value(salary,3) OVER(ORDER BY salary DESC) FROM salaries GROUP BY salary;
```

Result:

```response
   ┌─player──────────┬─salary─┬─rank─┐
1. │ Gary Chen       │ 195000 │    1 │
2. │ Robert George   │ 195000 │    1 │
3. │ Charles Juarez  │ 190000 │    3 │
4. │ Douglas Benson  │ 150000 │    4 │
5. │ Michael Stanley │ 150000 │    4 │
6. │ Scott Harrison  │ 150000 │    4 │
7. │ James Henderson │ 140000 │    7 │
   └─────────────────┴────────┴──────┘
```