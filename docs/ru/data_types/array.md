<a name="data_type-array"></a>

# Array(T)

Массив из элементов типа `T`.

`T` может любым, в том числе, массивом. Используйте многомерные массивы с осторожностью. ClickHouse поддерживает многомерные массивы ограниченно, например, их нельзя хранить в таблицах семейства `MergeTree`.

## Создание массива

Массив можно создать с помощью функции:

```
array(T)
```

Также можно использовать квадратные скобки

```
[]
```

Пример создания массива:
```
:) SELECT array(1, 2) AS x, toTypeName(x)

SELECT
    [1, 2] AS x,
    toTypeName(x)

┌─x─────┬─toTypeName(array(1, 2))─┐
│ [1,2] │ Array(UInt8)            │
└───────┴─────────────────────────┘

1 rows in set. Elapsed: 0.002 sec.

:) SELECT [1, 2] AS x, toTypeName(x)

SELECT
    [1, 2] AS x,
    toTypeName(x)

┌─x─────┬─toTypeName([1, 2])─┐
│ [1,2] │ Array(UInt8)       │
└───────┴────────────────────┘

1 rows in set. Elapsed: 0.002 sec.
```

## Особенности работы с типами данных

При создании массива "на лету" ClickHouse автоматически определяет тип аргументов как наиболее узкий тип данных, в котором можно хранить все перечисленные аргументы. Если среди аргументов есть [NULL](../query_language/syntax.md#null-literal) или аргумент типа [Nullable](nullable.md#data_type-nullable), то тип элементов массива — [Nullable](nullable.md#data_type-nullable).

Если ClickHouse не смог подобрать тип данных, то он сгенерирует исключение. Это произойдёт, например, при попытке создать массив одновременно со строками и числами `SELECT array(1, 'a')`.

Примеры автоматического определения типа данных:

```
:) SELECT array(1, 2, NULL) AS x, toTypeName(x)

SELECT
    [1, 2, NULL] AS x,
    toTypeName(x)

┌─x──────────┬─toTypeName(array(1, 2, NULL))─┐
│ [1,2,NULL] │ Array(Nullable(UInt8))        │
└────────────┴───────────────────────────────┘

1 rows in set. Elapsed: 0.002 sec.
```

Если попытаться создать массив из несовместимых типов данных, то ClickHouse выбросит исключение:

```
:) SELECT array(1, 'a')

SELECT [1, 'a']

Received exception from server (version 1.1.54388):
Code: 386. DB::Exception: Received from localhost:9000, 127.0.0.1. DB::Exception: There is no supertype for types UInt8, String because some of them are String/FixedString and some of them are not.

0 rows in set. Elapsed: 0.246 sec.
```
