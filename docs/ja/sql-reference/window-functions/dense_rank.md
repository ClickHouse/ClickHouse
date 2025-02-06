---
slug: /ja/sql-reference/window-functions/dense_rank
sidebar_label: dense_rank
sidebar_position: 7
---

# dense_rank

パーティション内の現在の行を、ギャップなしでランク付けします。つまり、新しい行の値が以前の行のいずれかの値と等しい場合、それはランクにギャップがないままに次の連続するランクを取得します。

[rank](./rank.md) 関数は同様の動作を提供しますが、ランクにギャップがあります。

**構文**

エイリアス: `denseRank` (大文字小文字区別)

```sql
dense_rank (column_name)
  OVER ([[PARTITION BY grouping_column] [ORDER BY sorting_column] 
        [ROWS or RANGE expression_to_bound_rows_withing_the_group]] | [window_name])
FROM table_name
WINDOW window_name as ([[PARTITION BY grouping_column] [ORDER BY sorting_column])
```

ウィンドウ関数の構文の詳細については、[ウィンドウ関数 - 構文](./index.md/#syntax) を参照してください。

**返される値**

- ランクにギャップなしの、パーティション内の現在の行の番号。[UInt64](../data-types/int-uint.md)。

**例**

以下の例は、動画の指導 [Ranking window functions in ClickHouse](https://youtu.be/Yku9mmBYm_4?si=XIMu1jpYucCQEoXA) に基づいています。

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
       dense_rank() OVER (ORDER BY salary DESC) AS dense_rank
FROM salaries;
```

結果:

```response
   ┌─player──────────┬─salary─┬─dense_rank─┐
1. │ Gary Chen       │ 195000 │          1 │
2. │ Robert George   │ 195000 │          1 │
3. │ Charles Juarez  │ 190000 │          2 │
4. │ Michael Stanley │ 150000 │          3 │
5. │ Douglas Benson  │ 150000 │          3 │
6. │ Scott Harrison  │ 150000 │          3 │
7. │ James Henderson │ 140000 │          4 │
   └─────────────────┴────────┴────────────┘
```
