---
sidebar_position: 52
sidebar_label: Array(T)
---

# Array(T) {#data-type-array}

Массив из элементов типа `T`. `T` может любым, в том числе массивом. Таким образом поддерживаются многомерные массивы. Первый элемент массива имеет индекс 1.

## Создание массива {#creating-an-array}

Массив можно создать с помощью функции:

``` sql
array(T)
```

Также можно использовать квадратные скобки

``` sql
[]
```

Пример создания массива:

``` sql
SELECT array(1, 2) AS x, toTypeName(x)
```

``` text
┌─x─────┬─toTypeName(array(1, 2))─┐
│ [1,2] │ Array(UInt8)            │
└───────┴─────────────────────────┘
```

``` sql
SELECT [1, 2] AS x, toTypeName(x)
```

``` text
┌─x─────┬─toTypeName([1, 2])─┐
│ [1,2] │ Array(UInt8)       │
└───────┴────────────────────┘
```

## Особенности работы с типами данных {#working-with-data-types}

Максимальный размер массива ограничен одним миллионом элементов.

При создании массива «на лету» ClickHouse автоматически определяет тип аргументов как наиболее узкий тип данных, в котором можно хранить все перечисленные аргументы. Если среди аргументов есть [NULL](../../sql-reference/data-types/array.md#null-literal) или аргумент типа [Nullable](nullable.md#data_type-nullable), то тип элементов массива — [Nullable](nullable.md).

Если ClickHouse не смог подобрать тип данных, то он сгенерирует исключение. Это произойдёт, например, при попытке создать массив одновременно со строками и числами `SELECT array(1, 'a')`.

Примеры автоматического определения типа данных:

``` sql
SELECT array(1, 2, NULL) AS x, toTypeName(x)
```

``` text
┌─x──────────┬─toTypeName(array(1, 2, NULL))─┐
│ [1,2,NULL] │ Array(Nullable(UInt8))        │
└────────────┴───────────────────────────────┘
```

Если попытаться создать массив из несовместимых типов данных, то ClickHouse выбросит исключение:

``` sql
SELECT array(1, 'a')
```

``` text
Received exception from server (version 1.1.54388):
Code: 386. DB::Exception: Received from localhost:9000, 127.0.0.1. DB::Exception: There is no supertype for types UInt8, String because some of them are String/FixedString and some of them are not.
```

## Размер массива {#array-size}

Узнать размер массива можно с помощью подстолбца `size0` без чтения всего столбца. Для многомерных массивов можно использовать подстолбец `sizeN-1`, где `N` — требуемое измерение.

**Пример**

Запрос:

```sql
CREATE TABLE t_arr (`arr` Array(Array(Array(UInt32)))) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_arr VALUES ([[[12, 13, 0, 1],[12]]]);

SELECT arr.size0, arr.size1, arr.size2 FROM t_arr;
```

Результат:

``` text
┌─arr.size0─┬─arr.size1─┬─arr.size2─┐
│         1 │ [2]       │ [[4,1]]   │
└───────────┴───────────┴───────────┘
```
