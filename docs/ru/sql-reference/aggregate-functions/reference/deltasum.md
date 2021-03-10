---
toc_priority: 141
---

# deltaSum {#agg_functions-deltasum}

Суммирует арифметическую разницу между последовательными значениями. Функция работает аналогично функции [runningDifference](../../functions/other-functions.md#other_functions-runningdifference), но реализована как агрегатная функция.

!!! note "Note"
    Если разница отрицательна — она будет проигнорирована.

**Синтаксис**

``` sql
deltaSum(value)
```

**Параметры**

-   `value` — входные значения, должны быть типа [Integer](../../data-types/int-uint.md) или [Float](../../data-types/float.md).

**Возвращаемое значение**

-   накопленная арифметическая разница, типа `Integer` или `Float`.

**Примеры**

Запрос:

``` sql
select deltaSum(arrayJoin([1, 2, 3]));
```

Результат:

``` text
┌─deltaSum(arrayJoin([1, 2, 3]))─┐
│                              2 │
└────────────────────────────────┘
```

Запрос:

``` sql
select deltaSum(arrayJoin([1, 2, 3, 0, 3, 4, 2, 3]));
```

Результат:

``` text
┌─deltaSum(arrayJoin([1, 2, 3, 0, 3, 4, 2, 3]))─┐
│                                             7 │
└───────────────────────────────────────────────┘
```

Запрос:

``` sql
select deltaSum(arrayJoin([2.25, 3, 4.5]));
```

Результат:

``` text
┌─deltaSum(arrayJoin([2.25, 3, 4.5]))─┐
│                                2.25 │
└─────────────────────────────────────┘
```

[Оригинальная статья](https://clickhouse.tech/docs/ru/sql-reference/aggregate-functions/reference/deltasum)<!--hide-->
