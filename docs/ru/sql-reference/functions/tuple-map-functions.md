---
toc_priority: 46
toc_title: Работа с контейнерами map
---

# Функции для работы с контейнерами map {#functions-for-working-with-tuple-maps}

## mapAdd {#function-mapadd}

Собирает все ключи и суммирует соответствующие значения.

**Синтаксис** 

``` sql
mapAdd(Tuple(Array, Array), Tuple(Array, Array) [, ...])
```

**Параметры** 

Аргументами являются [кортежи](../../sql-reference/data-types/tuple.md#tuplet1-t2) из двух [массивов](../../sql-reference/data-types/array.md#data-type-array), где элементы в первом массиве представляют ключи, а второй массив содержит значения для каждого ключа.
Все массивы ключей должны иметь один и тот же тип, а все массивы значений должны содержать элементы, которые можно приводить к одному типу ([Int64](../../sql-reference/data-types/int-uint.md#int-ranges), [UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges) или [Float64](../../sql-reference/data-types/float.md#float32-float64)).
Общий приведенный тип используется в качестве типа для результирующего массива.

**Возвращаемое значение**

-   Возвращает один [кортеж](../../sql-reference/data-types/tuple.md#tuplet1-t2), в котором первый массив содержит отсортированные ключи, а второй - значения.

**Пример**

Запрос:

``` sql
SELECT mapAdd(([toUInt8(1), 2], [1, 1]), ([toUInt8(1), 2], [1, 1])) as res, toTypeName(res) as type;
```

Результат:

``` text
┌─res───────────┬─type───────────────────────────────┐
│ ([1,2],[2,2]) │ Tuple(Array(UInt8), Array(UInt64)) │
└───────────────┴────────────────────────────────────┘
```

## mapSubtract {#function-mapsubtract}

Собирает все ключи и вычитает соответствующие значения.

**Синтаксис** 

``` sql
mapSubtract(Tuple(Array, Array), Tuple(Array, Array) [, ...])
```

**Параметры** 

Аргументами являются [кортежи](../../sql-reference/data-types/tuple.md#tuplet1-t2) из двух [массивов](../../sql-reference/data-types/array.md#data-type-array), где элементы в первом массиве представляют ключи, а второй массив содержит значения для каждого ключа.
Все массивы ключей должны иметь один и тот же тип, а все массивы значений должны содержать элементы, которые можно приводить к одному типу ([Int64](../../sql-reference/data-types/int-uint.md#int-ranges), [UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges) или [Float64](../../sql-reference/data-types/float.md#float32-float64)).
Общий приведенный тип используется в качестве типа для результирующего массива.

**Возвращаемое значение**

-   Возвращает один [tuple](../../sql-reference/data-types/tuple.md#tuplet1-t2), в котором первый массив содержит отсортированные ключи, а второй - значения.

**Пример**

Запрос:

```sql
SELECT mapSubtract(([toUInt8(1), 2], [toInt32(1), 1]), ([toUInt8(1), 2], [toInt32(2), 1])) as res, toTypeName(res) as type;
```

Результат:

```text
┌─res────────────┬─type──────────────────────────────┐
│ ([1,2],[-1,0]) │ Tuple(Array(UInt8), Array(Int64)) │
└────────────────┴───────────────────────────────────┘
```

## mapPopulateSeries {#function-mappopulateseries}

Заполняет недостающие ключи в контейнере map (пара массивов ключей и значений), где ключи являются целыми числами. Кроме того, он поддерживает указание максимального ключа, который используется для расширения массива ключей.

**Синтаксис** 

``` sql
mapPopulateSeries(keys, values[, max])
```

Генерирует контейнер map, где ключи - это серия чисел, от минимального до максимального ключа (или аргумент `max`, если он указан), взятых из массива `keys` с размером шага один, и соответствующие значения, взятые из массива `values`. Если значение не указано для ключа, то в результирующем контейнере используется значение по умолчанию.

Количество элементов в `keys` и `values` должно быть одинаковым для каждой строки.

**Параметры**

-   `keys` — Массив ключей [Array](../../sql-reference/data-types/array.md#data-type-array)([Int](../../sql-reference/data-types/int-uint.md#int-ranges)).
-   `values` — Массив значений. [Array](../../sql-reference/data-types/array.md#data-type-array)([Int](../../sql-reference/data-types/int-uint.md#int-ranges)).

**Возвращаемое значение**

-  Возвращает [кортеж](../../sql-reference/data-types/tuple.md#tuplet1-t2) из двух [массивов](../../sql-reference/data-types/array.md#data-type-array): ключи отсортированные по порядку и значения соответствующих ключей.

**Пример**

Запрос:

```sql
select mapPopulateSeries([1,2,4], [11,22,44], 5) as res, toTypeName(res) as type;
```

Результат:

```text
┌─res──────────────────────────┬─type──────────────────────────────┐
│ ([1,2,3,4,5],[11,22,0,44,0]) │ Tuple(Array(UInt8), Array(UInt8)) │
└──────────────────────────────┴───────────────────────────────────┘
```

[Оригинальная статья](https://clickhouse.tech/docs/en/query_language/functions/tuple-map-functions/) <!--hide-->
