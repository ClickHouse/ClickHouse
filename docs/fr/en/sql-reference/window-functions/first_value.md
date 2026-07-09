---
description: 'Documentation de référence de la fonction de fenêtre first_value'
sidebar_label: 'first_value'
sidebar_position: 3
slug: /sql-reference/window-functions/first_value
title: 'first_value'
doc_type: 'reference'
---

Renvoie la première valeur évaluée dans sa frame triée. Par défaut, les arguments `NULL` sont ignorés, mais le modificateur `RESPECT NULLS` peut être utilisé pour outrepasser ce comportement.

**Syntaxe**

```sql
first_value (column_name) [[RESPECT NULLS] | [IGNORE NULLS]]
  OVER ([[PARTITION BY grouping_column] [ORDER BY sorting_column] 
        [ROWS or RANGE expression_to_bound_rows_withing_the_group]] | [window_name])
FROM table_name
WINDOW window_name as ([PARTITION BY grouping_column] [ORDER BY sorting_column])
```

Alias : `any`.

:::note
L’utilisation du modificateur facultatif `RESPECT NULLS` après `first_value(column_name)` permet de ne pas ignorer les arguments `NULL`.
Consultez [le traitement des NULL](../aggregate-functions/index.md/#null-processing) pour plus d’informations.

Alias : `firstValueRespectNulls`
:::

Pour plus de détails sur la syntaxe des fonctions de fenêtre, consultez : [Fonctions de fenêtre - Syntaxe](./index.md/#syntax).

**Valeur renvoyée**

* La première valeur évaluée dans son frame ordonné.

**Exemple**

Dans cet exemple, la fonction `first_value` est utilisée pour trouver le joueur de football le mieux payé à partir d’un jeu de données fictif contenant les salaires des joueurs de Premier League.

```sql title="Query"
DROP TABLE IF EXISTS salaries;
CREATE TABLE salaries
(
    `team` String,
    `player` String,
    `salary` UInt32,
    `position` String
)
Engine = Memory;

INSERT INTO salaries FORMAT VALUES
    ('Port Elizabeth Barbarians', 'Gary Chen', 196000, 'F'),
    ('New Coreystad Archdukes', 'Charles Juarez', 190000, 'F'),
    ('Port Elizabeth Barbarians', 'Michael Stanley', 100000, 'D'),
    ('New Coreystad Archdukes', 'Scott Harrison', 180000, 'D'),
    ('Port Elizabeth Barbarians', 'Robert George', 195000, 'M'),
    ('South Hampton Seagulls', 'Douglas Benson', 150000, 'M'),
    ('South Hampton Seagulls', 'James Henderson', 140000, 'M');
```

```sql title="Query"
SELECT player, salary, 
       first_value(player) OVER (ORDER BY salary DESC) AS highest_paid_player
FROM salaries;
```

```response title="Response"
   ┌─player──────────┬─salary─┬─highest_paid_player─┐
1. │ Gary Chen       │ 196000 │ Gary Chen           │
2. │ Robert George   │ 195000 │ Gary Chen           │
3. │ Charles Juarez  │ 190000 │ Gary Chen           │
4. │ Scott Harrison  │ 180000 │ Gary Chen           │
5. │ Douglas Benson  │ 150000 │ Gary Chen           │
6. │ James Henderson │ 140000 │ Gary Chen           │
7. │ Michael Stanley │ 100000 │ Gary Chen           │
   └─────────────────┴────────┴─────────────────────┘
```