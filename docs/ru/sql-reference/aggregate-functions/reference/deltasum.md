---
toc_priority: 141
---

# deltaSum {#agg_functions-deltasum}

Суммирует арифметическую разницу между последовательными строками. Если разница отрицательна — она будет проигнорирована.

Чтобы эта функция работала должным образом, исходные данные должны быть отсортированы.

Если вы хотите использовать эту функцию в [материализованном представлении](../../../sql-reference/statements/create/view.md#materialized), вместо нее лучше используйте функцию [deltaSumTimestamp](deltasumtimestamp.md).

**Синтаксис**

``` sql
deltaSum(value)
```

**Аргументы**

-   `value` — входные значения, должны быть типа [Integer](../../data-types/int-uint.md) или [Float](../../data-types/float.md).

**Возвращаемое значение**

-   Накопленная арифметическая разница, типа `Integer` или `Float`.

**Примеры**

Запрос:

``` sql
SELECT deltaSum(arrayJoin([1, 2, 3]));
```

Результат:

``` text
┌─deltaSum(arrayJoin([1, 2, 3]))─┐
│                              2 │
└────────────────────────────────┘
```

Запрос:

``` sql
SELECT deltaSum(arrayJoin([1, 2, 3, 0, 3, 4, 2, 3]));
```

Результат:

``` text
┌─deltaSum(arrayJoin([1, 2, 3, 0, 3, 4, 2, 3]))─┐
│                                             7 │
└───────────────────────────────────────────────┘
```

Запрос:

``` sql
SELECT deltaSum(arrayJoin([2.25, 3, 4.5]));
```

Результат:

``` text
┌─deltaSum(arrayJoin([2.25, 3, 4.5]))─┐
│                                2.25 │
└─────────────────────────────────────┘
```

## Смотрите также {#see-also}

-   [runningDifference](../../functions/other-functions.md#runningdifferencex)
