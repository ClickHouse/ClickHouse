---
description: 'Это псевдоним для any, но он был введён для совместимости с
  оконными функциями, где иногда необходимо обрабатывать значения `NULL` (по умолчанию
  все агрегатные функции ClickHouse игнорируют значения NULL).'
slug: /sql-reference/aggregate-functions/reference/first_value
title: 'first_value'
doc_type: 'reference'
---

Это псевдоним для [`any`](../../../sql-reference/aggregate-functions/reference/any.md), но он был введён для совместимости с [оконными функциями](../../window-functions/index.md), где иногда необходимо обрабатывать значения `NULL` (по умолчанию все агрегатные функции ClickHouse игнорируют значения NULL).

Поддерживает использование модификатора для учёта значений NULL (`RESPECT NULLS`) как в [оконных функциях](../../window-functions/index.md), так и в обычных агрегациях.

Как и у `any`, без оконных функций результат будет случайным, если исходный поток не упорядочен, а возвращаемый тип совпадает с входным типом (`NULL` возвращается только в том случае, если входной тип — `Nullable` или добавлен комбинатор `-OrNull`).

<div id="examples">
  ## примеры
</div>

```sql
CREATE TABLE test_data
(
    a Int64,
    b Nullable(Int64)
)
ENGINE = Memory;

INSERT INTO test_data (a, b) VALUES (1,null), (2,3), (4, 5), (6,null);
```

<div id="example1">
  ### Пример 1
</div>

По умолчанию значение NULL не учитывается.

```sql
SELECT first_value(b) FROM test_data;
```

```text
┌─any(b)─┐
│      3 │
└────────┘
```

<div id="example2">
  ### Пример 2
</div>

Значение NULL игнорируется.

```sql
SELECT first_value(b) ignore nulls FROM test_data
```

```text
┌─any(b) IGNORE NULLS ─┐
│                    3 │
└──────────────────────┘
```

<div id="example3">
  ### Пример 3
</div>

Допускается значение NULL.

```sql
SELECT first_value(b) respect nulls FROM test_data
```

```text
┌─any(b) RESPECT NULLS ─┐
│                  ᴺᵁᴸᴸ │
└───────────────────────┘
```

<div id="example4">
  ### Пример 4
</div>

Стабилизация результата с помощью подзапроса с `ORDER BY`.

```sql
SELECT
    first_value_respect_nulls(b),
    first_value(b)
FROM
(
    SELECT *
    FROM test_data
    ORDER BY a ASC
)
```

```text
┌─any_respect_nulls(b)─┬─any(b)─┐
│                 ᴺᵁᴸᴸ │      3 │
└──────────────────────┴────────┘
```