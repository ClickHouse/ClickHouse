---
description: 'Документация по специальному типу данных Interval'
sidebar_label: 'Интервал'
sidebar_position: 61
slug: /sql-reference/data-types/special-data-types/interval
title: 'Интервал'
doc_type: 'reference'
---

Семейство типов данных, представляющих интервалы даты и времени. Результирующие типы оператора [INTERVAL](/ru/sql-reference/operators#interval).

Структура:

* Интервал времени в виде беззнакового целого значения.
* Тип интервала.

Поддерживаемые типы интервалов:

* `NANOSECOND`
* `MICROSECOND`
* `MILLISECOND`
* `SECOND`
* `MINUTE`
* `HOUR`
* `DAY`
* `WEEK`
* `MONTH`
* `QUARTER`
* `YEAR`

Для каждого типа интервала существует отдельный тип данных. Например, интервал `DAY` соответствует типу данных `IntervalDay`:

```sql
SELECT toTypeName(INTERVAL 4 DAY)
```

```text
┌─toTypeName(toIntervalDay(4))─┐
│ IntervalDay                  │
└──────────────────────────────┘
```

<div id="usage-remarks">
  ## Замечания по использованию
</div>

Вы можете использовать значения типа `Interval` в арифметических операциях со значениями типов [Date](../../../sql-reference/data-types/date.md) и [дата и время](../../../sql-reference/data-types/datetime.md). Например, к текущему времени можно добавить 4 дня:

```sql
SELECT now() AS current_date_time, current_date_time + INTERVAL 4 DAY
```

```text
┌───current_date_time─┬─plus(now(), toIntervalDay(4))─┐
│ 2019-10-23 10:58:45 │           2019-10-27 10:58:45 │
└─────────────────────┴───────────────────────────────┘
```

Также можно использовать сразу несколько интервалов:

```sql
SELECT now() AS current_date_time, current_date_time + (INTERVAL 4 DAY + INTERVAL 3 HOUR)
```

```text
┌───current_date_time─┬─plus(current_date_time, plus(toIntervalDay(4), toIntervalHour(3)))─┐
│ 2024-08-08 18:31:39 │                                                2024-08-12 21:31:39 │
└─────────────────────┴────────────────────────────────────────────────────────────────────┘
```

А для сравнения значений с разными интервалами:

```sql
SELECT toIntervalMicrosecond(179999999) < toIntervalMinute(3);
```

```text
┌─less(toIntervalMicrosecond(179999999), toIntervalMinute(3))─┐
│                                                           1 │
└─────────────────────────────────────────────────────────────┘
```

```sql
SELECT toIntervalMicrosecond(3600000000) = toIntervalHour(1);
```

```text
┌─equals(toIntervalMicrosecond(3600000000), toIntervalHour(1))─┐
│                                                            1 │
└──────────────────────────────────────────────────────────────┘
```

<div id="mixed-type-intervals">
  ## Интервалы смешанных типов
</div>

Интервалы смешанных типов, например из нескольких часов и нескольких минут, можно создавать с помощью синтаксиса `INTERVAL 'value' <from_kind> TO <to_kind>`.
В результате получается кортеж из двух или более интервалов.

Поддерживаемые комбинации:

| Синтаксис          | Строковый формат | Пример                                |
| ------------------ | ---------------- | ------------------------------------- |
| `YEAR TO MONTH`    | `Y-M`            | `INTERVAL '2-6' YEAR TO MONTH`        |
| `DAY TO HOUR`      | `D H`            | `INTERVAL '5 12' DAY TO HOUR`         |
| `DAY TO MINUTE`    | `D H:M`          | `INTERVAL '5 12:30' DAY TO MINUTE`    |
| `DAY TO SECOND`    | `D H:M:S`        | `INTERVAL '5 12:30:45' DAY TO SECOND` |
| `HOUR TO MINUTE`   | `H:M`            | `INTERVAL '1:30' HOUR TO MINUTE`      |
| `HOUR TO SECOND`   | `H:M:S`          | `INTERVAL '1:30:45' HOUR TO SECOND`   |
| `MINUTE TO SECOND` | `M:S`            | `INTERVAL '5:30' MINUTE TO SECOND`    |

Поля, кроме первого, проверяются в соответствии со стандартом SQL: `MONTH` 0-11, `HOUR` 0-23, `MINUTE` 0-59, `SECOND` 0-59.

```sql
SELECT INTERVAL '1:30' HOUR TO MINUTE;
```

```text
┌─(toIntervalHour(1), toIntervalMinute(30))─┐
│ (1,30)                                     │
└────────────────────────────────────────────┘
```

Необязательный начальный знак `+` или `-` распространяется на все компоненты:

```sql
SELECT INTERVAL '+1:30' HOUR TO MINUTE;
-- this is equivalent to:
-- SELECT INTERVAL '1:30' HOUR TO MINUTE;
```

```text
┌─(toIntervalHour(1), toIntervalMinute(30))─┐
│ (1,30)                                     │
└────────────────────────────────────────────┘
```

<div id="see-also">
  ## См. также
</div>

* оператор [INTERVAL](/ru/sql-reference/operators#interval)
* функции преобразования типов [toInterval](/ru/sql-reference/functions/type-conversion-functions#toIntervalYear)