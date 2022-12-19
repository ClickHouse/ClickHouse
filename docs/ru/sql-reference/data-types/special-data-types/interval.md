---
toc_priority: 61
toc_title: Interval
---

# Interval {#data-type-interval}

Семейство типов данных, представляющих интервалы дат и времени. Оператор [INTERVAL](../../../sql-reference/data-types/special-data-types/interval.md#operator-interval) возвращает значения этих типов.

!!! warning "Внимание"
    Нельзя использовать типы данных `Interval` для хранения данных в таблице.

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

Нельзя объединять интервалы различных типов. Нельзя использовать интервалы вида `4 DAY 1 HOUR`. Вместо этого выражайте интервал в единицах меньших или равных минимальной единице интервала, например, интервал «1 день и 1 час» можно выразить как `25 HOUR` или `90000 SECOND`.

Арифметические операции со значениями типов `Interval` не доступны, однако можно последовательно добавлять различные интервалы к значениям типов `Date` и `DateTime`. Например:

``` sql
SELECT now() AS current_date_time, current_date_time + INTERVAL 4 DAY + INTERVAL 3 HOUR
```

``` text
┌───current_date_time─┬─plus(plus(now(), toIntervalDay(4)), toIntervalHour(3))─┐
│ 2019-10-23 11:16:28 │                                    2019-10-27 14:16:28 │
└─────────────────────┴────────────────────────────────────────────────────────┘
```

Следующий запрос приведёт к генерированию исключения:

``` sql
select now() AS current_date_time, current_date_time + (INTERVAL 4 DAY + INTERVAL 3 HOUR)
```

``` text
Received exception from server (version 19.14.1):
Code: 43. DB::Exception: Received from localhost:9000. DB::Exception: Wrong argument types for function plus: if one argument is Interval, then another must be Date or DateTime..
```

## Смотрите также {#smotrite-takzhe}

-   Оператор [INTERVAL](../../../sql-reference/data-types/special-data-types/interval.md#operator-interval)
-   Функция приведения типа [toInterval](../../../sql-reference/data-types/special-data-types/interval.md#function-tointerval)
