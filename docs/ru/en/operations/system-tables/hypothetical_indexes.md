---
description: 'Системная таблица, содержащая список гипотетических (what-if) индексов, определённых в текущем сеансе'
keywords: ['системная таблица', 'hypothetical_indexes', 'what-if']
sidebar_label: 'hypothetical_indexes'
sidebar_position: 81
slug: /operations/system-tables/hypothetical_indexes
title: 'system.hypothetical_indexes'
doc_type: 'reference'
---

<div id="system-hypothetical-indexes">
  # system.hypothetical_indexes
</div>

Перечисляет все гипотетические индексы пропуска данных (what-if), определённые в текущем сеансе. См. [`CREATE HYPOTHETICAL INDEX`](/ru/sql-reference/statements/hypothetical-index#create-hypothetical-index) и [`EXPLAIN WHATIF`](/ru/sql-reference/statements/explain#explain-whatif).

Содержимое ограничено текущим сеансом: каждое соединение видит только свои собственные гипотетические индексы, а таблица пуста, если в текущем сеансе не было создано ни одного индекса.

Текущие `(database, table)` определяются по UUID во время выполнения запроса, поэтому они отражают `RENAME TABLE`, а записи для удалённых таблиц скрываются автоматически.

<div id="columns">
  ## Столбцы
</div>

| Столбец       | Тип      | Описание                                                                           |
| ------------- | -------- | ---------------------------------------------------------------------------------- |
| `database`    | `String` | Целевая база данных.                                                               |
| `table`       | `String` | Целевая таблица.                                                                   |
| `name`        | `String` | Имя индекса.                                                                       |
| `type`        | `String` | Тип индекса (`minmax`, `set`, `bloom_filter` и т. д.).                             |
| `type_full`   | `String` | Выражение типа индекса с аргументами, например `bloom_filter(0.01)`.               |
| `expression`  | `String` | Выражение индекса в том виде, в котором оно указано в `CREATE HYPOTHETICAL INDEX`. |
| `granularity` | `UInt64` | Количество гранул данных в одной грануле индекса.                                  |

<div id="example">
  ## Пример
</div>

```sql
CREATE HYPOTHETICAL INDEX i1 ON t (b) TYPE bloom_filter(0.01)  GRANULARITY 1;
CREATE HYPOTHETICAL INDEX i2 ON t (b) TYPE bloom_filter(0.001) GRANULARITY 1;

SELECT database, table, name, type, type_full, expression, granularity
FROM system.hypothetical_indexes;
```

```text
┌─database─┬─table─┬─name─┬─type─────────┬─type_full───────────┬─expression─┬─granularity─┐
│ default  │ t     │ i1   │ bloom_filter │ bloom_filter(0.01)  │ b          │           1 │
│ default  │ t     │ i2   │ bloom_filter │ bloom_filter(0.001) │ b          │           1 │
└──────────┴───────┴──────┴──────────────┴─────────────────────┴────────────┴─────────────┘
```

`type` — это базовое имя типа, а `type_full` включает аргументы, чтобы пользователи могли различать параметризованные варианты, например `bloom_filter(0.01)` и `bloom_filter(0.001)`.

<div id="see-also">
  ## См. также
</div>

* [`CREATE HYPOTHETICAL INDEX`](/ru/sql-reference/statements/hypothetical-index#create-hypothetical-index)
* [`EXPLAIN WHATIF`](/ru/sql-reference/statements/explain#explain-whatif)