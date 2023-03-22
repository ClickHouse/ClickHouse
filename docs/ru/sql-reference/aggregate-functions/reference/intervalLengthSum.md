---
toc_priority: 146
toc_title: intervalLengthSum
---

# intervalLengthSum {#agg_function-intervallengthsum}

Вычисляет длину объединения интервалов (отрезков на числовой оси).

**Синтаксис**

``` sql
intervalLengthSum(start, end)
```

**Аргументы**

-   `start` — начальное значение интервала. [Int32](../../../sql-reference/data-types/int-uint.md#uint8-uint16-uint32-uint64-int8-int16-int32-int64), [Int64](../../../sql-reference/data-types/int-uint.md#uint8-uint16-uint32-uint64-int8-int16-int32-int64), [UInt32](../../../sql-reference/data-types/int-uint.md#uint8-uint16-uint32-uint64-int8-int16-int32-int64), [UInt64](../../../sql-reference/data-types/int-uint.md#uint8-uint16-uint32-uint64-int8-int16-int32-int64), [Float32](../../../sql-reference/data-types/float.md#float32-float64), [Float64](../../../sql-reference/data-types/float.md#float32-float64), [DateTime](../../../sql-reference/data-types/datetime.md#data_type-datetime) или [Date](../../../sql-reference/data-types/date.md#data_type-date).
-   `end` — конечное значение интервала. [Int32](../../../sql-reference/data-types/int-uint.md#uint8-uint16-uint32-uint64-int8-int16-int32-int64), [Int64](../../../sql-reference/data-types/int-uint.md#uint8-uint16-uint32-uint64-int8-int16-int32-int64), [UInt32](../../../sql-reference/data-types/int-uint.md#uint8-uint16-uint32-uint64-int8-int16-int32-int64), [UInt64](../../../sql-reference/data-types/int-uint.md#uint8-uint16-uint32-uint64-int8-int16-int32-int64), [Float32](../../../sql-reference/data-types/float.md#float32-float64), [Float64](../../../sql-reference/data-types/float.md#float32-float64), [DateTime](../../../sql-reference/data-types/datetime.md#data_type-datetime) или [Date](../../../sql-reference/data-types/date.md#data_type-date).

!!! info "Примечание"
    Аргументы должны быть одного типа. В противном случае ClickHouse сгенерирует исключение.

**Возвращаемое значение**

-   Длина объединения всех интервалов (отрезков на числовой оси). В зависимости от типа аргумента возвращаемое значение может быть типа [UInt64](../../../sql-reference/data-types/int-uint.md#uint8-uint16-uint32-uint64-int8-int16-int32-int64) или [Float64](../../../sql-reference/data-types/float.md#float32-float64).

**Примеры**

1. Входная таблица:

``` text
┌─id─┬─start─┬─end─┐
│ a  │   1.1 │ 2.9 │
│ a  │   2.5 │ 3.2 │
│ a  │     4 │   5 │
└────┴───────┴─────┘
```

В этом примере используются аргументы типа Float32. Функция возвращает значение типа Float64.

Результатом функции будет сумма длин интервалов `[1.1, 3.2]` (объединение `[1.1, 2.9]` и `[2.5, 3.2]`) и `[4, 5]`

Запрос:

``` sql
SELECT id, intervalLengthSum(start, end), toTypeName(intervalLengthSum(start, end)) FROM fl_interval GROUP BY id ORDER BY id;
```

Результат:

``` text
┌─id─┬─intervalLengthSum(start, end)─┬─toTypeName(intervalLengthSum(start, end))─┐
│ a  │                           3.1 │ Float64                                   │
└────┴───────────────────────────────┴───────────────────────────────────────────┘
```

2. Входная таблица:

``` text
┌─id─┬───────────────start─┬─────────────────end─┐
│ a  │ 2020-01-01 01:12:30 │ 2020-01-01 02:10:10 │
│ a  │ 2020-01-01 02:05:30 │ 2020-01-01 02:50:31 │
│ a  │ 2020-01-01 03:11:22 │ 2020-01-01 03:23:31 │
└────┴─────────────────────┴─────────────────────┘
```

В этом примере используются аргументы типа DateTime. Функция возвращает значение, выраженное в секундах.

Запрос:

``` sql
SELECT id, intervalLengthSum(start, end), toTypeName(intervalLengthSum(start, end)) FROM dt_interval GROUP BY id ORDER BY id;
```

Результат:

``` text
┌─id─┬─intervalLengthSum(start, end)─┬─toTypeName(intervalLengthSum(start, end))─┐
│ a  │                          6610 │ UInt64                                    │
└────┴───────────────────────────────┴───────────────────────────────────────────┘
```

3. Входная таблица:

``` text
┌─id─┬──────start─┬────────end─┐
│ a  │ 2020-01-01 │ 2020-01-04 │
│ a  │ 2020-01-12 │ 2020-01-18 │
└────┴────────────┴────────────┘
```

В этом примере используются аргументы типа Date. Функция возвращает значение, выраженное в днях.

Запрос:

``` sql
SELECT id, intervalLengthSum(start, end), toTypeName(intervalLengthSum(start, end)) FROM date_interval GROUP BY id ORDER BY id;
```

Результат:

``` text
┌─id─┬─intervalLengthSum(start, end)─┬─toTypeName(intervalLengthSum(start, end))─┐
│ a  │                             9 │ UInt64                                    │
└────┴───────────────────────────────┴───────────────────────────────────────────┘
```
