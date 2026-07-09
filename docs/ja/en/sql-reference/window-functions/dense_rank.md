---
description: 'dense_rank ウィンドウ関数のドキュメント'
sidebar_label: 'dense_rank'
sidebar_position: 7
slug: /sql-reference/window-functions/dense_rank
title: 'dense_rank'
doc_type: 'reference'
---

現在の行を、そのパーティション内でギャップなく順位付けします。つまり、新しく現れた行の値がそれ以前のいずれかの行の値と同じ場合、その行には順位にギャップを入れずに次の連続した順位が割り当てられます。

[rank](./rank.md) 関数も同様の動作をしますが、順位にはギャップが入ります。

**構文**

別名: `denseRank` (大文字と小文字を区別) 

```sql
dense_rank ()
  OVER ([[PARTITION BY grouping_column] [ORDER BY sorting_column]
        [ROWS or RANGE expression_to_bound_rows_withing_the_group]] | [window_name])
FROM table_name
WINDOW window_name as ([[PARTITION BY grouping_column] [ORDER BY sorting_column])
```

ウィンドウ関数の構文の詳細は、[ウィンドウ関数 - 構文](./index.md/#syntax)を参照してください。

**戻り値**

* 現在の行が属するパーティション内での、ギャップのない順位を表す数値。[UInt64](../data-types/int-uint.md)。

**例**

次の例は、解説動画[ClickHouseのランキングウィンドウ関数](https://youtu.be/Yku9mmBYm_4?si=XIMu1jpYucCQEoXA)で紹介されている例に基づいています。

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
       dense_rank() OVER (ORDER BY salary DESC) AS dense_rank
FROM salaries;
```

```response title="Response"
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