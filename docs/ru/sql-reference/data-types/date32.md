---
sidebar_position: 48
sidebar_label: Date32
---

# Date32 {#data_type-datetime32}

Дата. Поддерживается такой же диапазон дат, как для типа [Datetime64](../../sql-reference/data-types/datetime64.md). Значение хранится в четырех байтах и соответствует числу дней с 1900-01-01 по 2299-12-31.

**Пример**

Создание таблицы со столбцом типа `Date32`и добавление в нее данных:

``` sql
CREATE TABLE new
(
    `timestamp` Date32,
    `event_id` UInt8
)
ENGINE = TinyLog;
```

``` sql
INSERT INTO new VALUES (4102444800, 1), ('2100-01-01', 2);
SELECT * FROM new;
```

``` text
┌──timestamp─┬─event_id─┐
│ 2100-01-01 │        1 │
│ 2100-01-01 │        2 │
└────────────┴──────────┘
```

**См. также**

-   [toDate32](../../sql-reference/functions/type-conversion-functions.md#todate32)
-   [toDate32OrZero](../../sql-reference/functions/type-conversion-functions.md#todate32-or-zero)
-   [toDate32OrNull](../../sql-reference/functions/type-conversion-functions.md#todate32-or-null)

