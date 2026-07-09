---
description: 'Documentation de la fonction de fenêtre row_number'
sidebar_label: 'row_number'
sidebar_position: 2
slug: /sql-reference/window-functions/row_number
title: 'row_number'
doc_type: 'reference'
---

Numérote la ligne en cours au sein de sa partition à partir de 1.

**Syntaxe**

```sql
row_number (column_name)
  OVER ([[PARTITION BY grouping_column] [ORDER BY sorting_column] 
        [ROWS or RANGE expression_to_bound_rows_withing_the_group]] | [window_name])
FROM table_name
WINDOW window_name as ([[PARTITION BY grouping_column] [ORDER BY sorting_column])
```

Pour plus de détails sur la syntaxe des fonctions de fenêtre, voir : [Fonctions de fenêtre - Syntaxe](./index.md/#syntax).

**Valeur renvoyée**

* Un nombre indiquant la ligne en cours dans sa partition. [UInt64](../data-types/int-uint.md).

**Exemple**

L&#39;exemple suivant est basé sur celui présenté dans la vidéo explicative [Fonctions de fenêtre de classement dans ClickHouse](https://youtu.be/Yku9mmBYm_4?si=XIMu1jpYucCQEoXA).

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
    ('Port Elizabeth Barbarians', 'Robert George', 195000, 'M');
```

```sql title="Query"
SELECT player, salary, 
       row_number() OVER (ORDER BY salary DESC) AS row_number
FROM salaries;
```

```response title="Response"
   ┌─player──────────┬─salary─┬─row_number─┐
1. │ Gary Chen       │ 195000 │          1 │
2. │ Robert George   │ 195000 │          2 │
3. │ Charles Juarez  │ 190000 │          3 │
4. │ Scott Harrison  │ 150000 │          4 │
5. │ Michael Stanley │ 150000 │          5 │
   └─────────────────┴────────┴────────────┘
```