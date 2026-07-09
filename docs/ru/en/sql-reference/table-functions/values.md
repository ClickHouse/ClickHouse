---
description: 'создаёт временное хранилище и заполняет столбцы значениями.'
keywords: ['values', 'табличная функция']
sidebar_label: 'values'
sidebar_position: 210
slug: /sql-reference/table-functions/values
title: 'values'
doc_type: 'reference'
---

Табличная функция `Values` позволяет создавать временное хранилище и заполнять
столбцы значениями. Она полезна для быстрого тестирования или генерации примеров данных.

:::note
`Values` — регистронезависимая функция. То есть допустимы оба варианта: `VALUES` и `values`.
:::

<div id="syntax">
  ## Синтаксис
</div>

Базовый синтаксис табличной функции `VALUES`:

```sql
VALUES([structure,] values...)
```

Обычно используется следующим образом:

```sql
VALUES(
    ['column1_name Type1, column2_name Type2, ...'],
    (value1_row1, value2_row1, ...),
    (value1_row2, value2_row2, ...),
    ...
)
```

<div id="arguments">
  ## Аргументы
</div>

* `column1_name Type1, ...` (необязательно). [String](/ru/sql-reference/data-types/string)
  задаёт имена и типы столбцов. Если этот аргумент опущен, столбцы будут
  называться `c1`, `c2` и т. д.
* `(value1_row1, value2_row1)`. [Tuple](/ru/sql-reference/data-types/tuple)
  содержит значения любого типа.

:::note
Кортежи, разделённые запятыми, также можно заменить отдельными значениями. В этом случае
каждое значение считается новой строкой. Подробнее см. в разделе [examples](#examples).
:::

<div id="returned-value">
  ## Возвращаемое значение
</div>

* Возвращает временную таблицу, содержащую переданные значения.

<div id="examples">
  ## Примеры
</div>

```sql title="Query"
SELECT *
FROM VALUES(
    'person String, place String',
    ('Noah', 'Paris'),
    ('Emma', 'Tokyo'),
    ('Liam', 'Sydney'),
    ('Olivia', 'Berlin'),
    ('Ilya', 'London'),
    ('Sophia', 'London'),
    ('Jackson', 'Madrid'),
    ('Alexey', 'Amsterdam'),
    ('Mason', 'Venice'),
    ('Isabella', 'Prague')
)
```

```response title="Response"
    ┌─person───┬─place─────┐
 1. │ Noah     │ Paris     │
 2. │ Emma     │ Tokyo     │
 3. │ Liam     │ Sydney    │
 4. │ Olivia   │ Berlin    │
 5. │ Ilya     │ London    │
 6. │ Sophia   │ London    │
 7. │ Jackson  │ Madrid    │
 8. │ Alexey   │ Amsterdam │
 9. │ Mason    │ Venice    │
10. │ Isabella │ Prague    │
    └──────────┴───────────┘
```

`VALUES` также можно использовать с отдельными значениями вместо кортежей. Например:

```sql title="Query"
SELECT *
FROM VALUES(
    'person String',
    'Noah',
    'Emma',
    'Liam',
    'Olivia',
    'Ilya',
    'Sophia',
    'Jackson',
    'Alexey',
    'Mason',
    'Isabella'
)
```

```response title="Response"
    ┌─person───┐
 1. │ Noah     │
 2. │ Emma     │
 3. │ Liam     │
 4. │ Olivia   │
 5. │ Ilya     │
 6. │ Sophia   │
 7. │ Jackson  │
 8. │ Alexey   │
 9. │ Mason    │
10. │ Isabella │
    └──────────┘
```

Или без указания спецификации строки (`'column1_name Type1, column2_name Type2, ...'`
в разделе [синтаксис](#syntax)); тогда имена столбцам присваиваются автоматически.

Например:

```sql title="Query"
-- tuples as values
SELECT *
FROM VALUES(
    ('Noah', 'Paris'),
    ('Emma', 'Tokyo'),
    ('Liam', 'Sydney'),
    ('Olivia', 'Berlin'),
    ('Ilya', 'London'),
    ('Sophia', 'London'),
    ('Jackson', 'Madrid'),
    ('Alexey', 'Amsterdam'),
    ('Mason', 'Venice'),
    ('Isabella', 'Prague')
)
```

```response title="Response"
    ┌─c1───────┬─c2────────┐
 1. │ Noah     │ Paris     │
 2. │ Emma     │ Tokyo     │
 3. │ Liam     │ Sydney    │
 4. │ Olivia   │ Berlin    │
 5. │ Ilya     │ London    │
 6. │ Sophia   │ London    │
 7. │ Jackson  │ Madrid    │
 8. │ Alexey   │ Amsterdam │
 9. │ Mason    │ Venice    │
10. │ Isabella │ Prague    │
    └──────────┴───────────┘
```

```sql title="Query"
-- single values
SELECT *
FROM VALUES(
    'Noah',
    'Emma',
    'Liam',
    'Olivia',
    'Ilya',
    'Sophia',
    'Jackson',
    'Alexey',
    'Mason',
    'Isabella'
)
```

```response title="Response"
    ┌─c1───────┐
 1. │ Noah     │
 2. │ Emma     │
 3. │ Liam     │
 4. │ Olivia   │
 5. │ Ilya     │
 6. │ Sophia   │
 7. │ Jackson  │
 8. │ Alexey   │
 9. │ Mason    │
10. │ Isabella │
    └──────────┘
```

<div id="sql-standard-values-clause">
  ## Стандартная клауза SQL `VALUES`
</div>

Начиная с версии 26.3 ClickHouse также поддерживает стандартную клаузу SQL `VALUES` в качестве табличного выражения
в `FROM`, как в PostgreSQL, MySQL, DuckDB и SQL Server. Этот синтаксис
внутренне преобразуется в использование табличной функции `values`, описанной выше.

```sql title="Query"
SELECT * FROM (VALUES (1, 'a'), (2, 'b'), (3, 'c')) AS t(id, val);
```

```response title="Response"
┌─id─┬─val─┐
│  1 │ a   │
│  2 │ b   │
│  3 │ c   │
└────┴─────┘
```

Его можно использовать в CTE:

```sql title="Query"
WITH cte AS (SELECT * FROM (VALUES (1, 'one'), (2, 'two')) AS t(id, name))
SELECT * FROM cte;
```

И в JOIN-операциях:

```sql title="Query"
SELECT t1.id, t1.val, t2.val2
FROM (VALUES (1, 'a'), (2, 'b')) AS t1(id, val)
JOIN (VALUES (1, 'x'), (2, 'y')) AS t2(id, val2) ON t1.id = t2.id;
```

:::note
Псевдонимы столбцов после `AS t(col1, col2, ...)` задаются в соответствии со стандартным синтаксисом SQL для
именования столбцов производных таблиц. Если они не указаны, столбцам присваиваются имена `c1`, `c2` и т. д.
:::

<div id="see-also">
  ## См. также
</div>

* [Формат Values](/ru/interfaces/formats/Values)