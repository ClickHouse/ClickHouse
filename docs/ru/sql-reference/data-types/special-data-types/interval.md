---
slug: /ru/sql-reference/data-types/special-data-types/interval
sidebar_position: 61
sidebar_label: Interval
---

# Interval {#data-type-interval}

Семейство типов данных, представляющих интервалы дат и времени. Оператор [INTERVAL](../../../sql-reference/data-types/special-data-types/interval.md#operator-interval) возвращает значения этих типов.

:::danger Внимание
Нельзя использовать типы данных `Interval` для хранения данных в таблице.
:::

Структура:

-   Интервал времени в виде положительного целого числа.
-   Тип интервала.

Поддержанные типы интервалов:

-   `SECOND`
-   `MINUTE`
-   `HOUR`
-   `DAY`
-   `WEEK`
-   `MONTH`
-   `QUARTER`
-   `YEAR`

Каждому типу интервала соответствует отдельный тип данных. Например, тип данных `IntervalDay` соответствует интервалу `DAY`:

``` sql
SELECT toTypeName(INTERVAL 4 DAY)
```

``` text
┌─toTypeName(toIntervalDay(4))─┐
│ IntervalDay                  │
└──────────────────────────────┘
```

## Использование {#data-type-interval-usage-remarks}

Значения типов `Interval` можно использовать в арифметических операциях со значениями типов [Date](../../../sql-reference/data-types/special-data-types/interval.md) и [DateTime](../../../sql-reference/data-types/special-data-types/interval.md). Например, можно добавить 4 дня к текущей дате:

``` sql
SELECT now() as current_date_time, current_date_time + INTERVAL 4 DAY
```

``` text
┌───current_date_time─┬─plus(now(), toIntervalDay(4))─┐
│ 2019-10-23 10:58:45 │           2019-10-27 10:58:45 │
└─────────────────────┴───────────────────────────────┘
```

Также можно использовать различные типы интервалов одновременно:

``` sql
SELECT now() AS current_date_time, current_date_time + (INTERVAL 4 DAY + INTERVAL 3 HOUR)
```

``` text
┌───current_date_time─┬─plus(current_date_time, plus(toIntervalDay(4), toIntervalHour(3)))─┐
│ 2024-08-08 18:31:39 │                                                2024-08-12 21:31:39 │
└─────────────────────┴────────────────────────────────────────────────────────────────────┘
```

И сравнивать значения из разными интервалами:

``` sql
SELECT toIntervalMicrosecond(3600000000) = toIntervalHour(1);
```

``` text
┌─less(toIntervalMicrosecond(179999999), toIntervalMinute(3))─┐
│                                                           1 │
└─────────────────────────────────────────────────────────────┘
```

## Смотрите также {#smotrite-takzhe}

-   Оператор [INTERVAL](../../../sql-reference/data-types/special-data-types/interval.md#operator-interval)
-   Функция приведения типа [toInterval](../../../sql-reference/data-types/special-data-types/interval.md#function-tointerval)
