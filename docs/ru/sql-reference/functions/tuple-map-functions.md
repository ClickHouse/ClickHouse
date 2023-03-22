---
sidebar_position: 46
sidebar_label: Работа с контейнерами map
---

# Функции для работы с контейнерами map {#functions-for-working-with-tuple-maps}

## map {#function-map}

Преобразовывает пары `ключ:значение` в тип данных [Map(key, value)](../../sql-reference/data-types/map.md).

**Синтаксис**

``` sql
map(key1, value1[, key2, value2, ...])
```

**Аргументы**

-   `key` — ключ. [String](../../sql-reference/data-types/string.md), [Integer](../../sql-reference/data-types/int-uint.md), [LowCardinality](../../sql-reference/data-types/lowcardinality.md), [FixedString](../../sql-reference/data-types/fixedstring.md), [UUID](../../sql-reference/data-types/uuid.md), [Date](../../sql-reference/data-types/date.md), [DateTime](../../sql-reference/data-types/datetime.md), [Date32](../../sql-reference/data-types/date32.md), [Enum](../../sql-reference/data-types/enum.md).
-   `value` — значение. Любой тип, включая [Map](../../sql-reference/data-types/map.md) и [Array](../../sql-reference/data-types/array.md).

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
mapAdd(arg1, arg2 [, ...])
```

**Аргументы**

Аргументами являются контейнеры [Map](../../sql-reference/data-types/map.md) или [кортежи](../../sql-reference/data-types/tuple.md#tuplet1-t2) из двух [массивов](../../sql-reference/data-types/array.md#data-type-array), где элементы в первом массиве представляют ключи, а второй массив содержит значения для каждого ключа.
Все массивы ключей должны иметь один и тот же тип, а все массивы значений должны содержать элементы, которые можно приводить к одному типу ([Int64](../../sql-reference/data-types/int-uint.md#int-ranges), [UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges) или [Float64](../../sql-reference/data-types/float.md#float32-float64)).
Общий приведенный тип используется в качестве типа для результирующего массива.

**Возвращаемое значение**

-   В зависимости от типа аргументов возвращает один [Map](../../sql-reference/data-types/map.md) или [кортеж](../../sql-reference/data-types/tuple.md#tuplet1-t2), в котором первый массив содержит отсортированные ключи, а второй — значения.

**Пример**

Запрос с кортежем:

``` sql
SELECT mapAdd(([toUInt8(1), 2], [1, 1]), ([toUInt8(1), 2], [1, 1])) as res, toTypeName(res) as type;
```

Результат:

``` text
┌─res───────────┬─type───────────────────────────────┐
│ ([1,2],[2,2]) │ Tuple(Array(UInt8), Array(UInt64)) │
└───────────────┴────────────────────────────────────┘
```

Запрос с контейнером `Map`:

```sql
SELECT mapAdd(map(1,1), map(1,1));
```

Результат:

```text
┌─mapAdd(map(1, 1), map(1, 1))─┐
│ {1:2}                        │
└──────────────────────────────┘
```

## mapSubtract {#function-mapsubtract}

Собирает все ключи и вычитает соответствующие значения.

**Синтаксис**

``` sql
mapSubtract(Tuple(Array, Array), Tuple(Array, Array) [, ...])
```

**Аргументы**

Аргументами являются контейнеры [Map](../../sql-reference/data-types/map.md) или [кортежи](../../sql-reference/data-types/tuple.md#tuplet1-t2) из двух [массивов](../../sql-reference/data-types/array.md#data-type-array), где элементы в первом массиве представляют ключи, а второй массив содержит значения для каждого ключа.
Все массивы ключей должны иметь один и тот же тип, а все массивы значений должны содержать элементы, которые можно приводить к одному типу ([Int64](../../sql-reference/data-types/int-uint.md#int-ranges), [UInt64](../../sql-reference/data-types/int-uint.md#uint-ranges) или [Float64](../../sql-reference/data-types/float.md#float32-float64)).
Общий приведенный тип используется в качестве типа для результирующего массива.

**Возвращаемое значение**

-   В зависимости от аргумента возвращает один [Map](../../sql-reference/data-types/map.md) или [кортеж](../../sql-reference/data-types/tuple.md#tuplet1-t2), в котором первый массив содержит отсортированные ключи, а второй — значения.

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

Запрос с контейнером `Map`:

```sql
SELECT mapSubtract(map(1,1), map(1,1));
```

Результат:

```text
┌─mapSubtract(map(1, 1), map(1, 1))─┐
│ {1:0}                             │
└───────────────────────────────────┘
```

## mapPopulateSeries {#function-mappopulateseries}

Заполняет недостающие ключи в контейнере map (пара массивов ключей и значений), где ключи являются целыми числами. Кроме того, он поддерживает указание максимального ключа, который используется для расширения массива ключей.

**Синтаксис**

``` sql
mapPopulateSeries(keys, values[, max])
mapPopulateSeries(map[, max])
```

Генерирует контейнер map, где ключи - это серия чисел, от минимального до максимального ключа (или аргумент `max`, если он указан), взятых из массива `keys` с размером шага один, и соответствующие значения, взятые из массива `values`. Если значение не указано для ключа, то в результирующем контейнере используется значение по умолчанию.

Количество элементов в `keys` и `values` должно быть одинаковым для каждой строки.

**Аргументы**

Аргументами являются контейнер [Map](../../sql-reference/data-types/map.md) или два [массива](../../sql-reference/data-types/array.md#data-type-array), где первый массив представляет ключи, а второй массив содержит значения для каждого ключа.

Сопоставленные массивы:

-   `keys` — массив ключей. [Array](../../sql-reference/data-types/array.md#data-type-array)([Int](../../sql-reference/data-types/int-uint.md#int-ranges)).
-   `values` — массив значений. [Array](../../sql-reference/data-types/array.md#data-type-array)([Int](../../sql-reference/data-types/int-uint.md#int-ranges)).
-   `max` — максимальное значение ключа. Необязательный параметр. [Int8, Int16, Int32, Int64, Int128, Int256](../../sql-reference/data-types/int-uint.md#int-ranges).

или

-   `map` — контейнер `Map` с целочисленными ключами. [Map](../../sql-reference/data-types/map.md).

**Возвращаемое значение**

-   В зависимости от аргумента возвращает контейнер [Map](../../sql-reference/data-types/map.md) или [кортеж](../../sql-reference/data-types/tuple.md#tuplet1-t2) из двух [массивов](../../sql-reference/data-types/array.md#data-type-array): ключи отсортированные по порядку и значения соответствующих ключей.

**Пример**

Запрос с сопоставленными массивами:

```sql
SELECT mapPopulateSeries([1,2,4], [11,22,44], 5) AS res, toTypeName(res) AS type;
```

Результат:

```text
┌─res──────────────────────────┬─type──────────────────────────────┐
│ ([1,2,3,4,5],[11,22,0,44,0]) │ Tuple(Array(UInt8), Array(UInt8)) │
└──────────────────────────────┴───────────────────────────────────┘
```

Запрос с контейнером `Map`:

```sql
SELECT mapPopulateSeries(map(1, 10, 5, 20), 6);
```

Результат:

```text
┌─mapPopulateSeries(map(1, 10, 5, 20), 6)─┐
│ {1:10,2:0,3:0,4:0,5:20,6:0}             │
└─────────────────────────────────────────┘
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

Функцию можно оптимизировать, если включить настройку [optimize_functions_to_subcolumns](../../operations/settings/settings.md#optimize-functions-to-subcolumns). При `optimize_functions_to_subcolumns = 1` функция читает только подстолбец [keys](../../sql-reference/data-types/map.md#map-subcolumns) вместо чтения и обработки данных всего столбца. Запрос `SELECT mapKeys(m) FROM table` преобразуется к запросу `SELECT m.keys FROM table`.

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

Функцию можно оптимизировать, если включить настройку [optimize_functions_to_subcolumns](../../operations/settings/settings.md#optimize-functions-to-subcolumns). При `optimize_functions_to_subcolumns = 1` функция читает только подстолбец [values](../../sql-reference/data-types/map.md#map-subcolumns) вместо чтения и обработки данных всего столбца. Запрос `SELECT mapValues(m) FROM table` преобразуется к запросу `SELECT m.values FROM table`.

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
