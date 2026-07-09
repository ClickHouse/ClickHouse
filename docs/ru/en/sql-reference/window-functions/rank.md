---
description: 'Документация по оконной функции rank'
sidebar_label: 'rank'
sidebar_position: 6
slug: /sql-reference/window-functions/rank
title: 'rank'
doc_type: 'reference'
---

Присваивает текущей строке ранг в пределах её партиции с пропусками. Иными словами, если значение строки совпадает со значением предыдущей строки, ей присваивается тот же ранг, что и предыдущей строке.
Ранг следующей строки при этом равен рангу предыдущей строки плюс пропуск, равный количеству присвоений предыдущего ранга.

Функция [dense&#95;rank](./dense_rank.md) работает так же, но без пропусков в ранжировании.

**Синтаксис**

```sql
rank ()
  OVER ([[PARTITION BY grouping_column] [ORDER BY sorting_column]
        [ROWS or RANGE expression_to_bound_rows_withing_the_group]] | [window_name])
FROM table_name
WINDOW window_name as ([[PARTITION BY grouping_column] [ORDER BY sorting_column])
```

Подробнее о синтаксисе оконных функций см. здесь: [Оконные функции — синтаксис](./index.md/#syntax).

**Возвращаемое значение**

* Номер текущей строки в пределах её партиции с учётом пропусков. [UInt64](../data-types/int-uint.md).

**Пример**

Следующий пример основан на материале из обучающего видео [Оконные функции ранжирования в ClickHouse](https://youtu.be/Yku9mmBYm_4?si=XIMu1jpYucCQEoXA).

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