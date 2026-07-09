---
description: 'Документация по типу данных Tuple в ClickHouse'
sidebar_label: 'Tuple(T1, T2, ...)'
sidebar_position: 34
slug: /sql-reference/data-types/tuple
title: 'Tuple(T1, T2, ...)'
doc_type: 'reference'
---

Tuple — это набор элементов, каждый из которых имеет свой [тип](/ru/sql-reference/data-types). Tuple должен содержать как минимум один элемент.

Tuple используются для временной группировки столбцов. Столбцы можно группировать, когда в запросе используется выражение IN, а также для указания некоторых формальных параметров лямбда-функций. Подробнее см. в разделах [IN operators](../../sql-reference/operators/in.md) и [Higher order functions](/ru/sql-reference/functions/overview#higher-order-functions).

Tuple могут быть результатом запроса. В этом случае в текстовых форматах, отличных от JSON, значения разделяются запятыми и заключаются в `()`. В форматах JSON Tuple выводятся как массивы (в `[]`).

<div id="creating-tuples">
  ## Создание Tuple
</div>

Для создания кортежа можно использовать функцию:

```sql
tuple(T1, T2, ...)
```

Пример создания кортежа:

```sql
SELECT tuple(1, 'a') AS x, toTypeName(x)
```

```text
┌─x───────┬─toTypeName(tuple(1, 'a'))─┐
│ (1,'a') │ Tuple(UInt8, String)      │
└─────────┴───────────────────────────┘
```

Tuple может состоять из одного элемента

Пример:

```sql
SELECT tuple('a') AS x;
```

```text
┌─x─────┐
│ ('a') │
└───────┘
```

Синтаксис `(tuple_element1, tuple_element2)` можно использовать для создания кортежа из нескольких элементов, не вызывая функцию `tuple()`.

Пример:

```sql
SELECT (1, 'a') AS x, (today(), rand(), 'someString') AS y, ('a') AS not_a_tuple;
```

```text
┌─x───────┬─y──────────────────────────────────────┬─not_a_tuple─┐
│ (1,'a') │ ('2022-09-21',2006973416,'someString') │ a           │
└─────────┴────────────────────────────────────────┴─────────────┘
```

<div id="data-type-detection">
  ## Определение типа данных
</div>

При создании кортежей на лету ClickHouse определяет типы их аргументов как наименьшие типы, способные вместить переданные значения. Если значение — [NULL](/ru/operations/settings/formats#input_format_null_as_default), то определяемый тип — [Nullable](../../sql-reference/data-types/nullable.md).

Пример автоматического определения типа данных:

```sql
SELECT tuple(1, NULL) AS x, toTypeName(x)
```

```text
┌─x─────────┬─toTypeName(tuple(1, NULL))──────┐
│ (1, NULL) │ Tuple(UInt8, Nullable(Nothing)) │
└───────────┴─────────────────────────────────┘
```

<div id="referring-to-tuple-elements">
  ## Обращение к элементам Tuple
</div>

На элементы Tuple можно ссылаться по имени или по индексу:

```sql title="Query"
CREATE TABLE named_tuples (`a` Tuple(s String, i Int64)) ENGINE = Memory;
INSERT INTO named_tuples VALUES (('y', 10)), (('x',-10));

SELECT a.s FROM named_tuples; -- by name
SELECT a.2 FROM named_tuples; -- by index
```

```text title="Response"
┌─a.s─┐
│ y   │
│ x   │
└─────┘

┌─tupleElement(a, 2)─┐
│                 10 │
│                -10 │
└────────────────────┘
```

<div id="comparison-operations-with-tuple">
  ## Операции сравнения для Tuple
</div>

Два кортежа сравниваются последовательно: их элементы сопоставляются слева направо. Если элемент первого кортежа больше (меньше) соответствующего элемента второго кортежа, то первый кортеж больше (меньше) второго; в противном случае (если оба элемента равны) сравнивается следующий элемент.

Пример:

```sql
SELECT (1, 'z') > (1, 'a') c1, (2022, 01, 02) > (2023, 04, 02) c2, (1,2,3) = (3,2,1) c3;
```

```text
┌─c1─┬─c2─┬─c3─┐
│  1 │  0 │  0 │
└────┴────┴────┘
```

Практические примеры:

```sql
CREATE TABLE test
(
    `year` Int16,
    `month` Int8,
    `day` Int8
)
ENGINE = Memory AS
SELECT *
FROM values((2022, 12, 31), (2000, 1, 1));

SELECT * FROM test;

┌─year─┬─month─┬─day─┐
│ 2022 │    12 │  31 │
│ 2000 │     1 │   1 │
└──────┴───────┴─────┘

SELECT *
FROM test
WHERE (year, month, day) > (2010, 1, 1);

┌─year─┬─month─┬─day─┐
│ 2022 │    12 │  31 │
└──────┴───────┴─────┘
CREATE TABLE test
(
    `key` Int64,
    `duration` UInt32,
    `value` Float64
)
ENGINE = Memory AS
SELECT *
FROM values((1, 42, 66.5), (1, 42, 70), (2, 1, 10), (2, 2, 0));

SELECT * FROM test;

┌─key─┬─duration─┬─value─┐
│   1 │       42 │  66.5 │
│   1 │       42 │    70 │
│   2 │        1 │    10 │
│   2 │        2 │     0 │
└─────┴──────────┴───────┘

-- Let's find a value for each key with the biggest duration, if durations are equal, select the biggest value

SELECT
    key,
    max(duration),
    argMax(value, (duration, value))
FROM test
GROUP BY key
ORDER BY key ASC;

┌─key─┬─max(duration)─┬─argMax(value, tuple(duration, value))─┐
│   1 │            42 │                                    70 │
│   2 │             2 │                                     0 │
└─────┴───────────────┴───────────────────────────────────────┘
```

<div id="nullable-tuple">
  ## Nullable(Tuple(T1, T2, ...))
</div>

:::note Бета-возможность
Требуется `SET enable_nullable_tuple_type = 1`
Это бета-возможность.
:::

Позволяет всему кортежу принимать значение `NULL`, в отличие от `Tuple(Nullable(T1), Nullable(T2), ...)`, где значение `NULL` могут принимать только отдельные элементы.

| Тип                                        | Кортеж может быть NULL | Элементы могут быть NULL |
| ------------------------------------------ | ---------------------- | ------------------------ |
| `Nullable(Tuple(String, Int64))`           | ✅                      | ❌                        |
| `Tuple(Nullable(String), Nullable(Int64))` | ❌                      | ✅                        |

Пример:

```sql
SET enable_nullable_tuple_type = 1;

CREATE TABLE test (
    id UInt32,
    data Nullable(Tuple(String, Int64))
) ENGINE = Memory;

INSERT INTO test VALUES (1, ('hello', 42)), (2, NULL);

SELECT * FROM test WHERE data IS NULL;
```

```txt
 ┌─id─┬─data─┐
 │  2 │ ᴺᵁᴸᴸ │
 └────┴──────┘
```