---
toc_priority: 141
---

# deltaSumTimestamp {#agg_functions-deltasum}

Прибавляет разницу между последовательными строками. Если разница отрицательна — она будет проигнорирована. Параметр `timestamp` используется для упорядочивания значений.

Эта функция предназначена в первую очередь для [материализованных представлений](../../../sql-reference/statements/create/view.md#materialized), упорядоченных по некоторому временному бакету согласно timestamp, например по бакету `toStartOfMinute`. Поэтому строки в таком материализованном представлении будут иметь одинаковый timestamp. Невозможно, чтобы они были объединены в "правом" порядке. Эта функция отслеживает `timestamp` значений, которые она видит. Поэтому можно правильно упорядочить состояния во время слияния.

Чтобы вычислить разницу между упорядоченными последовательными строками, вы можете использовать функцию [deltaSum](./deltasum.md) вместо функции `deltaSumTimestamp`.

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
FROM (select number as timestamp, [0, 4, 8, 3, 0, 0, 0, 1, 3, 5][number] as value from numbers(1, 10));
```

Результат:

``` text
┌─deltaSumTimestamp(value, timestamp)─┐
│                                  13 │
└─────────────────────────────────────┘
```
