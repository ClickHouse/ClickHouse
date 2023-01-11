---
sidebar_position: 141
---

# deltaSumTimestamp {#agg_functions-deltasumtimestamp}

Суммирует разницу между последовательными строками. Если разница отрицательна — она будет проигнорирована.

Эта функция предназначена в первую очередь для [материализованных представлений](../../../sql-reference/statements/create/view.md#materialized), упорядоченных по некоторому временному бакету согласно timestamp, например, по бакету `toStartOfMinute`. Поскольку строки в таком материализованном представлении будут иметь одинаковый timestamp, невозможно объединить их в "правом" порядке. Функция отслеживает `timestamp` наблюдаемых значений, поэтому возможно правильно упорядочить состояния во время слияния.

Чтобы вычислить разницу между упорядоченными последовательными строками, вы можете использовать функцию [deltaSum](../../../sql-reference/aggregate-functions/reference/deltasum.md#agg_functions-deltasum) вместо функции `deltaSumTimestamp`.

**Синтаксис**

``` sql
deltaSumTimestamp(value, timestamp)
```

**Аргументы**

-   `value` — входные значения, должны быть типа [Integer](../../data-types/int-uint.md), или [Float](../../data-types/float.md), или [Date](../../data-types/date.md), или [DateTime](../../data-types/datetime.md).
-   `timestamp` — параметр для упорядочивания значений, должен быть типа [Integer](../../data-types/int-uint.md), или [Float](../../data-types/float.md), или [Date](../../data-types/date.md), или [DateTime](../../data-types/datetime.md).

**Возвращаемое значение**

-   Накопленная разница между последовательными значениями, упорядоченными по параметру `timestamp`.

Тип: [Integer](../../data-types/int-uint.md), или [Float](../../data-types/float.md), или [Date](../../data-types/date.md), или [DateTime](../../data-types/datetime.md).

**Пример**

Запрос:

```sql
SELECT deltaSumTimestamp(value, timestamp)
FROM (SELECT number AS timestamp, [0, 4, 8, 3, 0, 0, 0, 1, 3, 5][number] AS value FROM numbers(1, 10));
```

Результат:

``` text
┌─deltaSumTimestamp(value, timestamp)─┐
│                                  13 │
└─────────────────────────────────────┘
```
