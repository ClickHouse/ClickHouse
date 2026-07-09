---
description: 'Документация по оконной функции ntile'
sidebar_label: 'ntile'
sidebar_position: 13
slug: /sql-reference/window-functions/ntile
title: 'ntile'
doc_type: 'reference'
---

Разбивает упорядоченные строки в пределах партиции на указанное число корзин (групп) максимально одинакового размера и возвращает номер корзины, к которой относится текущая строка. Корзины нумеруются начиная с 1. В каждой партиции строки распределяются по корзинам по порядку: если количество строк не делится на количество корзин без остатка, более ранние корзины получают на одну строку больше, чем более поздние.

**Синтаксис**

```sql
ntile (buckets)
  OVER ([[PARTITION BY grouping_column] [ORDER BY sorting_column]
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING] | [window_name])
FROM table_name
WINDOW window_name as ([PARTITION BY grouping_column] [ORDER BY sorting_column] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
```

Аргумент `buckets` должен быть положительной целочисленной константой.

Обязательна секция `ORDER BY`. Рамка окна должна охватывать всю партицию (`ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`); это также рамка по умолчанию, которая используется, если она не указана явно.

Подробнее о синтаксисе оконных функций см.: [Оконные функции — синтаксис](./index.md/#syntax).

**Возвращаемое значение**

* Номер бакета текущей строки в пределах её партиции. [UInt64](../data-types/int-uint.md).

**Пример**

В следующем примере игроки распределяются по четырём бакетам в порядке убывания зарплаты.

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
       ntile(4) OVER (ORDER BY salary DESC, player ASC) AS bucket
FROM salaries;
```

```response title="Response"
   ┌─player──────────┬─salary─┬─bucket─┐
1. │ Gary Chen       │ 195000 │      1 │
2. │ Robert George   │ 195000 │      1 │
3. │ Charles Juarez  │ 190000 │      2 │
4. │ Douglas Benson  │ 150000 │      2 │
5. │ Michael Stanley │ 150000 │      3 │
6. │ Scott Harrison  │ 150000 │      3 │
7. │ James Henderson │ 140000 │      4 │
   └─────────────────┴────────┴────────┘
```

Здесь семь строк и четыре бакета, поэтому в первых трёх бакетах — по две строки, а в последнем — одна строка.