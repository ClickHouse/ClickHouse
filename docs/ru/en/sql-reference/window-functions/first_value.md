---
description: 'Документация по оконной функции first_value'
sidebar_label: 'first_value'
sidebar_position: 3
slug: /sql-reference/window-functions/first_value
title: 'first_value'
doc_type: 'reference'
---

Возвращает первое значение в пределах упорядоченной рамки окна. По умолчанию аргументы `NULL` пропускаются, однако для изменения этого поведения можно использовать модификатор `RESPECT NULLS`.

**Синтаксис**

```sql
first_value (column_name) [[RESPECT NULLS] | [IGNORE NULLS]]
  OVER ([[PARTITION BY grouping_column] [ORDER BY sorting_column] 
        [ROWS or RANGE expression_to_bound_rows_withing_the_group]] | [window_name])
FROM table_name
WINDOW window_name as ([PARTITION BY grouping_column] [ORDER BY sorting_column])
```

Псевдоним: `any`.

:::note
Использование необязательного модификатора `RESPECT NULLS` после `first_value(column_name)` гарантирует, что `NULL`-аргументы не будут пропускаться.
Дополнительные сведения см. в разделе [Обработка NULL](../aggregate-functions/index.md/#null-processing).

Псевдоним: `firstValueRespectNulls`
:::

Подробнее о синтаксисе оконных функций см. здесь: [Оконные функции — синтаксис](./index.md/#syntax).

**Возвращаемое значение**

* Первое значение, вычисленное в пределах своей упорядоченной рамки окна.

**Пример**

В этом примере функция `first_value` используется для поиска самого высокооплачиваемого футболиста в вымышленном наборе данных о зарплатах игроков Премьер-лиги.

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