---
slug: /ja/sql-reference/window-functions/rank
sidebar_label: rank
sidebar_position: 6
---

# rank

現在の行をそのパーティション内でギャップを持ってランク付けします。言い換えれば、処理中の行の値が以前の行と等しい場合、それらは同じランクを受け取ります。次の行のランクは、前の行のランクに、前のランクが与えられた回数分のギャップを加えたものになります。

[dense_rank](./dense_rank.md)関数は、ランク付けにギャップがない同じ動作を提供します。

**構文**

```sql
rank (column_name)
  OVER ([[PARTITION BY grouping_column] [ORDER BY sorting_column] 
        [ROWS または RANGE expression_to_bound_rows_withing_the_group]] | [window_name])
FROM table_name
WINDOW window_name as ([[PARTITION BY grouping_column] [ORDER BY sorting_column])
```

ウィンドウ関数の構文の詳細については、[ウィンドウ関数 - 構文](./index.md/#syntax)を参照してください。

**戻り値**

- ギャップを含む、パーティション内の現在の行の番号。[UInt64](../data-types/int-uint.md)。

**例**

以下の例は、動画インストラクション[ClickHouseにおけるランキングウィンドウ関数](https://youtu.be/Yku9mmBYm_4?si=XIMu1jpYucCQEoXA)で提供された例に基づいています。

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
       rank() OVER (ORDER BY salary DESC) AS rank
FROM salaries;
```

結果:

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
