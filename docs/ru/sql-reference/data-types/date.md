---
sidebar_position: 47
sidebar_label: Date
---

# Date {#data-type-date}

Дата. Хранится в двух байтах в виде (беззнакового) числа дней, прошедших от 1970-01-01. Позволяет хранить значения от чуть больше, чем начала unix-эпохи до верхнего порога, определяющегося константой на этапе компиляции (сейчас - до 2106 года, последний полностью поддерживаемый год - 2105).

Диапазон значений: \[1970-01-01, 2149-06-06\].

Дата хранится без учёта часового пояса.

**Пример**

Создание таблицы и добавление в неё данных:

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
SELECT * FROM dt;
```

``` text
┌──timestamp─┬─event_id─┐
│ 2019-01-01 │        1 │
│ 2019-01-01 │        2 │
└────────────┴──────────┘
```

**См. также**

-   [Функции для работы с датой и временем](../../sql-reference/functions/date-time-functions.md)
-   [Операторы для работы с датой и временем](../../sql-reference/operators/index.md#operators-datetime)
-   [Тип данных `DateTime`](../../sql-reference/data-types/datetime.md)


