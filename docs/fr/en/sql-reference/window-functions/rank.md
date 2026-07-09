---
description: 'Documentation de la fonction de fenêtre rank'
sidebar_label: 'rank'
sidebar_position: 6
slug: /sql-reference/window-functions/rank
title: 'rank'
doc_type: 'reference'
---

Attribue un rang à la ligne en cours au sein de sa partition, avec des écarts. En d&#39;autres termes, si la valeur d&#39;une ligne rencontrée est égale à celle d&#39;une ligne précédente, elle recevra le même rang que cette ligne précédente.
Le rang de la ligne suivante est alors égal à celui de la ligne précédente, plus un écart correspondant au nombre de fois où le rang précédent a été attribué.

La fonction [dense&#95;rank](./dense_rank.md) offre le même comportement, mais sans écarts dans le classement.

**Syntaxe**

```sql
rank ()
  OVER ([[PARTITION BY grouping_column] [ORDER BY sorting_column]
        [ROWS or RANGE expression_to_bound_rows_withing_the_group]] | [window_name])
FROM table_name
WINDOW window_name as ([[PARTITION BY grouping_column] [ORDER BY sorting_column])
```

Pour plus de détails sur la syntaxe des fonctions de fenêtre, voir : [Fonctions de fenêtre - Syntaxe](./index.md/#syntax).

**Valeur renvoyée**

* Un nombre pour la ligne en cours dans sa partition, y compris les écarts. [UInt64](../data-types/int-uint.md).

**Exemple**

L&#39;exemple suivant est basé sur celui présenté dans la vidéo explicative [Ranking window functions in ClickHouse](https://youtu.be/Yku9mmBYm_4?si=XIMu1jpYucCQEoXA).

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
       rank() OVER (ORDER BY salary DESC) AS rank
FROM salaries;
```

```response title="Response"
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