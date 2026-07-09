---
description: 'Движок таблицы GenerateRandom создаёт случайные данные для заданной
  схемы таблицы.'
sidebar_label: 'GenerateRandom'
sidebar_position: 140
slug: /engines/table-engines/special/generate
title: 'Движок таблицы GenerateRandom'
doc_type: 'reference'
---

Движок таблицы GenerateRandom создаёт случайные данные для заданной схемы таблицы.

Примеры использования:

* Используйте в тестах для заполнения большой таблицы воспроизводимыми данными.
* Генерируйте случайные входные данные для фаззинг-тестов.

<div id="usage-in-clickhouse-server">
  ## Использование на сервере ClickHouse
</div>

```sql
ENGINE = GenerateRandom([random_seed [,max_string_length [,max_array_length]]])
```

Параметры `max_array_length` и `max_string_length` задают максимальную длину для всех
столбцов типа array или map, а также для строк в сгенерированных данных соответственно.

Движок таблицы GenerateRandom поддерживает только запросы `SELECT`.

Он поддерживает все [типы данных](../../../sql-reference/data-types/index.md), которые могут храниться в таблице, кроме `AggregateFunction`.

<div id="example">
  ## Пример
</div>

**1.** Создайте таблицу `generate_engine_table`:

```sql
CREATE TABLE generate_engine_table (name String, value UInt32) ENGINE = GenerateRandom(1, 5, 3)
```

**2.** Выполните запрос к данным:

```sql
SELECT * FROM generate_engine_table LIMIT 3
```

```text
┌─name─┬──────value─┐
│ c4xJ │ 1412771199 │
│ r    │ 1791099446 │
│ 7#$  │  124312908 │
└──────┴────────────┘
```

<div id="details-of-implementation">
  ## Подробности реализации
</div>

* Не поддерживаются:
  * `ALTER`
  * `SELECT ... SAMPLE`
  * `INSERT`
  * Индексы
  * Репликация