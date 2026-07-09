---
description: 'Документация по оконной функции last_value'
sidebar_label: 'last_value'
sidebar_position: 4
slug: /sql-reference/window-functions/last_value
title: 'last_value'
doc_type: 'справочник'
---

Возвращает последнее значение, вычисленное в пределах упорядоченной рамки окна. По умолчанию аргументы `NULL` пропускаются, однако это поведение можно переопределить с помощью модификатора `RESPECT NULLS`.

**Синтаксис**

```sql
last_value (column_name) [[RESPECT NULLS] | [IGNORE NULLS]]
  OVER ([[PARTITION BY grouping_column] [ORDER BY sorting_column] 
        [ROWS or RANGE expression_to_bound_rows_withing_the_group]] | [window_name])
FROM table_name
WINDOW window_name as ([[PARTITION BY grouping_column] [ORDER BY sorting_column])
```

Псевдоним: `anyLast`.

:::note
Использование необязательного модификатора `RESPECT NULLS` после `first_value(column_name)` гарантирует, что значения `NULL` не будут пропущены.
Дополнительные сведения см. в разделе [обработка NULL](../aggregate-functions/index.md/#null-processing).

Псевдоним: `lastValueRespectNulls`
:::

Подробнее о синтаксисе оконных функций см. в разделе [Оконные функции — синтаксис](./index.md/#syntax).

**Возвращаемое значение**

* Последнее значение, вычисленное в пределах упорядоченной рамки окна.

**Пример**

В этом примере функция `last_value` используется для поиска футболиста с самой низкой зарплатой в вымышленном наборе данных о зарплатах игроков Премьер-лиги.

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
       last_value(player) OVER (ORDER BY salary DESC RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS lowest_paid_player
FROM salaries;
```

```response title="Response"
   ┌─player──────────┬─salary─┬─lowest_paid_player─┐
1. │ Gary Chen       │ 196000 │ Michael Stanley    │
2. │ Robert George   │ 195000 │ Michael Stanley    │
3. │ Charles Juarez  │ 190000 │ Michael Stanley    │
4. │ Scott Harrison  │ 180000 │ Michael Stanley    │
5. │ Douglas Benson  │ 150000 │ Michael Stanley    │
6. │ James Henderson │ 140000 │ Michael Stanley    │
7. │ Michael Stanley │ 100000 │ Michael Stanley    │
   └─────────────────┴────────┴────────────────────┘
```