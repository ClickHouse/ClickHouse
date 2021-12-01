---
toc_priority: 47
toc_title: Date
---

# Date {#data-type-date}

Дата. Хранится в двух байтах в виде (беззнакового) числа дней, прошедших от 1970-01-01. Позволяет хранить значения от чуть больше, чем начала unix-эпохи до верхнего порога, определяющегося константой на этапе компиляции (сейчас - до 2106 года, последний полностью поддерживаемый год - 2105).

Дата хранится без учёта часового пояса.

## Примеры {#examples}

**1.** Создание таблицы и добавление в неё данных:

``` sql
CREATE TABLE dt
(
    `timestamp` Date,
    `event_id` UInt8
)
ENGINE = TinyLog;
```

``` sql
INSERT INTO dt Values (1546300800, 1), ('2019-01-01', 2);
```

``` sql
SELECT * FROM dt;
```

``` text
┌──timestamp─┬─event_id─┐
│ 2019-01-01 │        1 │
│ 2019-01-01 │        2 │
└────────────┴──────────┘
```

## Смотрите также {#see-also}

-   [Функции для работы с датой и временем](../../sql-reference/functions/date-time-functions.md)
-   [Операторы для работы с датой и временем](../../sql-reference/operators/index.md#operators-datetime)
-   [Тип данных `DateTime`](../../sql-reference/data-types/datetime.md)


