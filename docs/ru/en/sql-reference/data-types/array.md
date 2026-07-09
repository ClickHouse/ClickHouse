---
description: 'Документация по типу данных Array в ClickHouse'
sidebar_label: 'Array(T)'
sidebar_position: 32
slug: /sql-reference/data-types/array
title: 'Array(T)'
doc_type: 'reference'
---

Массив элементов типа `T`; нумерация индексов начинается с 1. `T` может быть любым типом данных, включая массив.

<div id="creating-an-array">
  ## Создание Array
</div>

Для создания массива можно использовать функцию:

```sql
array(T)
```

Также можно использовать `[]`.

```sql
[]
```

Пример создания массива:

```sql
SELECT array(1, 2) AS x, toTypeName(x)
```

```text
┌─x─────┬─toTypeName(array(1, 2))─┐
│ [1,2] │ Array(UInt8)            │
└───────┴─────────────────────────┘
```

```sql
SELECT [1, 2] AS x, toTypeName(x)
```

```text
┌─x─────┬─toTypeName([1, 2])─┐
│ [1,2] │ Array(UInt8)       │
└───────┴────────────────────┘
```

<div id="working-with-data-types">
  ## Работа с типами данных
</div>

При создании массива на лету ClickHouse автоматически определяет тип аргумента как самый узкий тип данных, в котором могут храниться все перечисленные аргументы. Если среди значений есть [Nullable](/ru/sql-reference/data-types/nullable) или литеральные [NULL](/ru/operations/settings/formats#input_format_null_as_default), тип элемента массива также становится [Nullable](../../sql-reference/data-types/nullable.md).

Если ClickHouse не может определить тип данных, он генерирует исключение. Например, это происходит при попытке создать массив, одновременно содержащий строки и числа (`SELECT array(1, 'a')`).

Примеры автоматического определения типа данных:

```sql
SELECT array(1, 2, NULL) AS x, toTypeName(x)
```

```text
┌─x──────────┬─toTypeName(array(1, 2, NULL))─┐
│ [1,2,NULL] │ Array(Nullable(UInt8))        │
└────────────┴───────────────────────────────┘
```

Если попытаться создать массив из несовместимых типов данных, ClickHouse генерирует исключение:

```sql
SELECT array(1, 'a')
```

```text
Received exception from server (version 1.1.54388):
Code: 386. DB::Exception: Received from localhost:9000, 127.0.0.1. DB::Exception: There is no supertype for types UInt8, String because some of them are String/FixedString and some of them are not.
```

<div id="array-size">
  ## Размер массива
</div>

Размер массива можно определить с помощью подстолбца `size0`, не считывая весь столбец. Для многомерных массивов можно использовать `sizeN-1`, где `N` — требуемая размерность.

**Пример**

```sql title="Query"
CREATE TABLE t_arr (`arr` Array(Array(Array(UInt32)))) ENGINE = MergeTree ORDER BY tuple();

INSERT INTO t_arr VALUES ([[[12, 13, 0, 1],[12]]]);

SELECT arr.size0, arr.size1, arr.size2 FROM t_arr;
```

```text title="Response"
┌─arr.size0─┬─arr.size1─┬─arr.size2─┐
│         1 │ [2]       │ [[4,1]]   │
└───────────┴───────────┴───────────┘
```

<div id="reading-nested-subcolumns-from-array">
  ## Чтение вложенных подстолбцов из Array
</div>

Если вложенный тип `T` внутри `Array` содержит подстолбцы (например, если это [именованный кортеж](./tuple.md)), можно читать их из типа `Array(T)` с теми же именами подстолбцов. Тип такого подстолбца будет `Array` от типа исходного подстолбца.

**Пример**

```sql
CREATE TABLE t_arr (arr Array(Tuple(field1 UInt32, field2 String))) ENGINE = MergeTree ORDER BY tuple();
INSERT INTO t_arr VALUES ([(1, 'Hello'), (2, 'World')]), ([(3, 'This'), (4, 'is'), (5, 'subcolumn')]);
SELECT arr.field1, toTypeName(arr.field1), arr.field2, toTypeName(arr.field2) from t_arr;
```

```test
┌─arr.field1─┬─toTypeName(arr.field1)─┬─arr.field2────────────────┬─toTypeName(arr.field2)─┐
│ [1,2]      │ Array(UInt32)          │ ['Hello','World']         │ Array(String)          │
│ [3,4,5]    │ Array(UInt32)          │ ['This','is','subcolumn'] │ Array(String)          │
└────────────┴────────────────────────┴───────────────────────────┴────────────────────────┘
```