---
description: 'Документация по типу данных UUID в ClickHouse'
sidebar_label: 'UUID'
sidebar_position: 24
slug: /sql-reference/data-types/uuid
title: 'UUID'
doc_type: 'reference'
---

Универсальный уникальный идентификатор (UUID) — это 16-байтное значение, используемое для идентификации записей. Подробную информацию о UUID см. в [Википедии](https://en.wikipedia.org/wiki/Universally_unique_identifier).

Хотя существуют разные варианты UUID, например UUIDv4 и UUIDv7 (см. [здесь](https://datatracker.ietf.org/doc/html/draft-ietf-uuidrev-rfc4122bis)), ClickHouse не проверяет, соответствуют ли вставленные UUID определённому варианту.
Внутри ClickHouse UUID рассматриваются как последовательность из 16 случайных байтов с [представлением 8-4-4-4-12](https://en.wikipedia.org/wiki/Universally_unique_identifier#Textual_representation) на уровне SQL.

Пример значения UUID:

```text
61f0c404-5cb3-11e7-907b-a6006ad3dba0
```

UUID по умолчанию — полностью нулевой. Например, он используется, когда вставляется новая запись, но значение для столбца UUID не указано:

```text
00000000-0000-0000-0000-000000000000
```

:::warning
По историческим причинам UUID сортируются по второй половине.

Для значений UUIDv4 это не проблема, однако для столбцов UUIDv7, используемых в определениях первичного индекса, это может ухудшать производительность (использование в ключах сортировки или ключах партиционирования допустимо).
Точнее, значения UUIDv7 состоят из временной метки в первой половине и счётчика во второй.
Поэтому сортировка UUIDv7 в разреженных индексах первичного ключа (то есть первых значений каждой гранулы индекса) будет выполняться по полю счётчика.
Если бы UUID сортировались по первой половине (временной метке), то на этапе анализа первичного индекса в начале запросов можно было бы отбросить все метки во всех частях, кроме одной.
Однако при сортировке по второй половине (счётчику) для всех частей, как ожидается, будет возвращаться как минимум одна метка, что приводит к лишним обращениям к диску.
:::

Пример:

```sql title="Query"
CREATE TABLE tab (uuid UUID) ENGINE = MergeTree PRIMARY KEY (uuid);

INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
SELECT * FROM tab;
```

```text title="Response"
┌─uuid─────────────────────────────────┐
│ 019d2555-7874-7e9d-a284-9b45a0b2f165 │
│ 019d2555-7874-7e9d-a284-9b46c3353be7 │
│ 019d2555-7878-77fc-a36f-4081aa58ec2b │
│ 019d2555-7878-77fc-a36f-40826555fb9b │
│ 019d2555-7870-7432-ba62-5250ac595328 │
│ 019d2555-7870-7432-ba62-5251da22bd19 │
│ 019d2555-786c-73e9-a031-4a7936df7d56 │
│ 019d2555-786c-73e9-a031-4a7a35a9544f │
│ 019d2555-7868-7333-89d1-2bd1639899c3 │
│ 019d2555-7868-7333-89d1-2bd297eb7d42 │
└──────────────────────────────────────┘

```

В качестве обходного решения UUID можно преобразовать во временную метку, извлечённую из его второй половины:

```sql title="Query"
CREATE TABLE tab (uuid UUID) ENGINE = MergeTree PRIMARY KEY (UUIDv7ToDateTime(uuid));
-- Or alternatively:                      [...] PRIMARY KEY (toStartOfHour(UUIDv7ToDateTime(uuid)));

INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
INSERT INTO tab SELECT generateUUIDv7() FROM numbers(2);
SELECT * FROM tab;
```

Результат (если вставлены те же данные):

```text title="Response"
┌─uuid─────────────────────────────────┐
│ 019d2555-7868-7333-89d1-2bd1639899c3 │
│ 019d2555-7868-7333-89d1-2bd297eb7d42 │
│ 019d2555-786c-73e9-a031-4a7936df7d56 │
│ 019d2555-786c-73e9-a031-4a7a35a9544f │
│ 019d2555-7870-7432-ba62-5250ac595328 │
│ 019d2555-7870-7432-ba62-5251da22bd19 │
│ 019d2555-7874-7e9d-a284-9b45a0b2f165 │
│ 019d2555-7874-7e9d-a284-9b46c3353be7 │
│ 019d2555-7878-77fc-a36f-4081aa58ec2b │
│ 019d2555-7878-77fc-a36f-40826555fb9b │
└──────────────────────────────────────┘

```

ORDER BY (UUIDv7ToDateTime(uuid), uuid)

<div id="generating-uuids">
  ## Генерация UUID
</div>

ClickHouse предоставляет функцию [generateUUIDv4](../../sql-reference/functions/uuid-functions.md) для создания случайных UUID версии 4.

<div id="usage-example">
  ## Пример использования
</div>

**Пример 1**

В этом примере показано, как создать таблицу со столбцом UUID и вставить в неё значение.

```sql title="Query"
CREATE TABLE t_uuid (x UUID, y String) ENGINE=TinyLog

INSERT INTO t_uuid SELECT generateUUIDv4(), 'Example 1'

SELECT * FROM t_uuid
```

```text title="Response"
┌────────────────────────────────────x─┬─y─────────┐
│ 417ddc5d-e556-4d27-95dd-a34d84e46a50 │ Example 1 │
└──────────────────────────────────────┴───────────┘
```

**Пример 2**

В этом примере при вставке записи значение столбца UUID не указывается, то есть вставляется UUID по умолчанию:

```sql
INSERT INTO t_uuid (y) VALUES ('Example 2')

SELECT * FROM t_uuid
```

```text
┌────────────────────────────────────x─┬─y─────────┐
│ 417ddc5d-e556-4d27-95dd-a34d84e46a50 │ Example 1 │
│ 00000000-0000-0000-0000-000000000000 │ Example 2 │
└──────────────────────────────────────┴───────────┘
```

<div id="restrictions">
  ## Ограничения
</div>

Тип данных UUID поддерживает только те функции, которые поддерживает и тип данных [String](../../sql-reference/data-types/string.md) (например, [min](/ru/sql-reference/aggregate-functions/reference/min), [max](/ru/sql-reference/aggregate-functions/reference/max) и [count](/ru/sql-reference/aggregate-functions/reference/count)).

Для типа данных UUID не поддерживаются арифметические операции (например, [abs](/ru/sql-reference/functions/arithmetic-functions#abs)) и агрегатные функции, такие как [sum](/ru/sql-reference/aggregate-functions/reference/sum) и [avg](/ru/sql-reference/aggregate-functions/reference/avg).