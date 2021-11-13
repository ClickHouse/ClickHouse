## exponentialMovingAverage {#exponential-moving-average}

Вычисляет экспоненциальное скользящее среднее.

**Синтакис:**

```sql
exponentialMovingAverage(x)(value, timestamp)
```

Каждой точке `timestamp` на временном отрезке соответствует определенное значение `value`. Период полу-распада — временной интервал `х`, в течение которого учитываются предыдущие значения. Функция возвращает взвешенное среднее: чем старше временная точка, с тем более меньшим весом считается соответствующее ей значение.

**Аргументы:**
- `value` - входные значения, должны быть типа [Integer](../../../sql-reference/data-types/int-uint.md), [Float](../../../sql-reference/data-types/float.md) или [Decimal](../../../sql-reference/data-types/decimal.md).
- `timestamp` - параметр для упорядочивания значений, должен быть типа [Integer](../../../sql-reference/data-types/int-uint.md).

**Параметры**
- `x` - период полу-распада в секундах, должен быть типа [Integer](../../../sql-reference/data-types/int-uint.md).

**Возвращаемые значения:**
- Возвращает экспоненциальное скользящее среднее. 

Тип: [Float64](../../../sql-reference/data-types/float.md#float32-float64).

**Пример**

Исходная таблица:

``` text
┌──temperature─┬─timestamp──┐
│          95  │         1  │
│          95  │         2  │
│          95  │         3  │
│          96  │         4  │
│          96  │         5  │
│          96  │         6  │
│          96  │         7  │
│          97  │         8  │
│          97  │         9  │
│          97  │        10  │
│          97  │        11  │
│          98  │        12  │
│          98  │        13  │
│          98  │        14  │
│          98  │        15  │
│          99  │        16  │
│          99  │        17  │
│          99  │        18  │
│         100  │        19  │
│         100  │        20  │
└──────────────┴────────────┘
```

Запрос: 

```sql
exponentialMovingAverage(5)(temperature, timestamp)
```

Результат:

``` text
┌──exponentialMovingAverage(5)(temperature, timestamp)──┐
│                                    92.25779635374204  │
└───────────────────────────────────────────────────────┘
```