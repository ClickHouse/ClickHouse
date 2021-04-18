---
toc_priority: 46
toc_title: Работа с контейнерами map
---

# Функции для работы с контейнерами map {#functions-for-working-with-tuple-maps}

## map {#function-map}

Преобразовывает пары `ключ:значение` в тип данных [Map(key, value)](../../sql-reference/data-types/map.md).

**Синтаксис** 

``` sql
map(key1, value1[, key2, value2, ...])
```

**Аргументы** 

-   `key` — ключ. [String](../../sql-reference/data-types/string.md) или [Integer](../../sql-reference/data-types/int-uint.md).
-   `value` — значение. [String](../../sql-reference/data-types/string.md), [Integer](../../sql-reference/data-types/int-uint.md) или [Array](../../sql-reference/data-types/array.md).

**Возвращаемое значение**

-   Структура данных в виде пар `ключ:значение`.

Тип: [Map(key, value)](../../sql-reference/data-types/map.md).

**Примеры**

Запрос:

``` sql
SELECT map('key1', number, 'key2', number * 2) FROM numbers(3);
```

Результат:

``` text
┌─map('key1', number, 'key2', multiply(number, 2))─┐
│ {'key1':0,'key2':0}                              │
│ {'key1':1,'key2':2}                              │
│ {'key1':2,'key2':4}                              │
└──────────────────────────────────────────────────┘
```

Запрос:

``` sql
CREATE TABLE table_map (a Map(String, UInt64)) ENGINE = MergeTree() ORDER BY a;
INSERT INTO table_map SELECT map('key1', number, 'key2', number * 2) FROM numbers(3);
SELECT a['key2'] FROM table_map;
```

Результат:

``` text
┌─arrayElement(a, 'key2')─┐
│                       0 │
│                       2 │
│                       4 │
└─────────────────────────┘
```

**Смотрите также** 

-   тип данных [Map(key, value)](../../sql-reference/data-types/map.md)

## mapAdd {#function-mapadd}

Собирает все ключи и суммирует соответствующие значения.

**Синтаксис** 

``` sql
mapAdd(Tuple(Array, Array), Tuple(Array, Array) [, ...])
```

**Аргументы** 

Аргументами являются [кортежи](../../sql-reference/data-types/tuple.md#tuplet1-t2) из двух [массивов](../../sql-reference/data-types/array.md#data-type-array), где элементы в первом массиве представляют ключи, а второй массив содержит значения для каждого ключа.
Все массивы ключей должны иметь один и тот же тип, а все массивы значений должны содержать элементы, которые можно приводить к одному типу ([Int64](../../sql-reference/data-types/int-uint.md#int-ranges), [UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges) или [Float64](../../sql-reference/data-types/float.md#float32-float64)).
Общий приведенный тип используется в качестве типа для результирующего массива.

**Возвращаемое значение**

-   Возвращает один [кортеж](../../sql-reference/data-types/tuple.md#tuplet1-t2), в котором первый массив содержит отсортированные ключи, а второй — значения.

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

**Аргументы**

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

**Аргументы**

-   `keys` — массив ключей [Array](../../sql-reference/data-types/array.md#data-type-array)([Int](../../sql-reference/data-types/int-uint.md#int-ranges)).
-   `values` — массив значений. [Array](../../sql-reference/data-types/array.md#data-type-array)([Int](../../sql-reference/data-types/int-uint.md#int-ranges)).

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

## mapContains {#mapcontains}

Определяет, содержит ли контейнер `map` ключ `key`.

**Синтаксис**

``` sql
mapContains(map, key)
```

**Аргументы** 

-   `map` — контейнер Map. [Map](../../sql-reference/data-types/map.md).
-   `key` — ключ. Тип соответстует типу ключей параметра  `map`.

**Возвращаемое значение**

-   `1` если `map` включает `key`, иначе `0`.

Тип: [UInt8](../../sql-reference/data-types/int-uint.md).

**Пример**

Запрос:

```sql
CREATE TABLE test (a Map(String,String)) ENGINE = Memory;

INSERT INTO test VALUES ({'name':'eleven','age':'11'}), ({'number':'twelve','position':'6.0'});

SELECT mapContains(a, 'name') FROM test;

```

Результат:

```text
┌─mapContains(a, 'name')─┐
│                      1 │
│                      0 │
└────────────────────────┘
```

## mapKeys {#mapkeys}

Возвращает все ключи контейнера `map`.

**Синтаксис**

```sql
mapKeys(map)
```

**Аргументы**

-   `map` — контейнер Map. [Map](../../sql-reference/data-types/map.md).

**Возвращаемое значение**

-   Массив со всеми ключами контейнера `map`.

Тип: [Array](../../sql-reference/data-types/array.md).

**Пример**

Запрос:

```sql
CREATE TABLE test (a Map(String,String)) ENGINE = Memory;

INSERT INTO test VALUES ({'name':'eleven','age':'11'}), ({'number':'twelve','position':'6.0'});

SELECT mapKeys(a) FROM test;
```

Результат:

```text
┌─mapKeys(a)────────────┐
│ ['name','age']        │
│ ['number','position'] │
└───────────────────────┘
```

## mapValues {#mapvalues}

Возвращает все значения контейнера `map`.

**Синтаксис**

```sql
mapKeys(map)
```

**Аргументы**

-   `map` — контейнер Map. [Map](../../sql-reference/data-types/map.md).

**Возвращаемое значение**

-   Массив со всеми значениями контейнера `map`.

Тип: [Array](../../sql-reference/data-types/array.md).

**Примеры**

Запрос:

```sql
CREATE TABLE test (a Map(String,String)) ENGINE = Memory;

INSERT INTO test VALUES ({'name':'eleven','age':'11'}), ({'number':'twelve','position':'6.0'});

SELECT mapValues(a) FROM test;
```

Результат:

```text
┌─mapValues(a)─────┐
│ ['eleven','11']  │
│ ['twelve','6.0'] │
└──────────────────┘
```

