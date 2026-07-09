---
description: 'Документация по типу данных Date в ClickHouse'
sidebar_label: 'Date'
sidebar_position: 12
slug: /sql-reference/data-types/date
title: 'Date'
doc_type: 'reference'
---

Дата. Хранится в двух байтах как количество дней, прошедших с 1970-01-01 (без знака). Позволяет хранить значения от момента сразу после начала эпохи Unix до верхнего порога, заданного константой на этапе компиляции (в настоящее время это до 2149 года, но последний полностью поддерживаемый год — 2148).

Поддерживаемый диапазон значений: [1970-01-01, 2149-06-06].

Значение даты хранится без часового пояса.

**Пример**

Создание таблицы со столбцом типа `Date` и вставка в неё данных:

```sql
CREATE TABLE dt
(
    `timestamp` Date,
    `event_id` UInt8
)
ENGINE = TinyLog;
```

```sql
-- Parse Date
-- - from string,
-- - from 'small' integer interpreted as number of days since 1970-01-01, and
-- - from 'big' integer interpreted as number of seconds since 1970-01-01.
INSERT INTO dt VALUES ('2019-01-01', 1), (17897, 2), (1546300800, 3);

SELECT * FROM dt;
```

```text
┌──timestamp─┬─event_id─┐
│ 2019-01-01 │        1 │
│ 2019-01-01 │        2 │
│ 2019-01-01 │        3 │
└────────────┴──────────┘
```

**См. также**

* [Функции для работы с датой и временем](../../sql-reference/functions/date-time-functions.md)
* [Операторы для работы с датой и временем](../../sql-reference/operators#operators-for-working-with-dates-and-times)
* [Тип данных `DateTime`](../../sql-reference/data-types/datetime.md)