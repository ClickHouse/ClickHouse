---
slug: /ja/sql-reference/window-functions/percent_rank
sidebar_label: percent_rank
sidebar_position: 8
---

# percent_rank

行の相対的な順位（すなわちパーセンタイル）をウィンドウパーティション内で返します。

**構文**

別名: `percentRank`（大文字小文字を区別）

```sql
percent_rank (column_name)
  OVER ([[PARTITION BY grouping_column] [ORDER BY sorting_column] 
        [RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]] | [window_name])
FROM table_name
WINDOW window_name as ([PARTITION BY grouping_column] [ORDER BY sorting_column] RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
```

デフォルトで必要なウィンドウフレーム定義は `RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` です。

ウィンドウ関数の構文の詳細については、こちらを参照してください: [ウィンドウ関数 - 構文](./index.md/#syntax)。

**例**

クエリ:

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
    ('Port Elizabeth Barbarians', 'Robert George', 195000, 'M'),
    ('South Hampton Seagulls', 'Douglas Benson', 150000, 'M'),
    ('South Hampton Seagulls', 'James Henderson', 140000, 'M');
```

```sql
SELECT player, salary, 
       percent_rank() OVER (ORDER BY salary DESC) AS percent_rank
FROM salaries;
```

結果:

```response

   ┌─player──────────┬─salary─┬───────percent_rank─┐
1. │ Gary Chen       │ 195000 │                  0 │
2. │ Robert George   │ 195000 │                  0 │
3. │ Charles Juarez  │ 190000 │ 0.3333333333333333 │
4. │ Michael Stanley │ 150000 │                0.5 │
5. │ Scott Harrison  │ 150000 │                0.5 │
6. │ Douglas Benson  │ 150000 │                0.5 │
7. │ James Henderson │ 140000 │                  1 │
   └─────────────────┴────────┴────────────────────┘

```

