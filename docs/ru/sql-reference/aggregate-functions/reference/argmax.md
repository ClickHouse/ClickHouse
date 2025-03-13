---
slug: /ru/sql-reference/aggregate-functions/reference/argmax
sidebar_position: 106
---

# argMax {#agg-function-argmax}

Вычисляет значение `arg` при максимальном значении `val`. Если несколько строк имеют одинаковое `val`, в которых равное значение является максимальным, то возвращаемое `arg` не является детерминированным. Обе части, arg и max, ведут себя как агрегатные функции, они обе пропускают Null во время обработки и возвращают не Null значения, если не Null значения доступны.

**Синтаксис**

``` sql
argMax(arg, val)
```

**Аргументы**

-   `arg` — аргумент.
-   `val` — значение.

**Возвращаемое значение**

-   значение `arg`, соответствующее максимальному значению `val`.

Тип: соответствует типу `arg`.

**Пример**

Исходная таблица:

``` text
┌─user─────┬─salary─┐
│ director │   5000 │
│ manager  │   3000 │
│ worker   │   1000 │
└──────────┴────────┘
```

Запрос:

``` sql
SELECT argMax(user, salary), argMax(tuple(user, salary), salary) FROM salary;
```

Результат:

``` text
┌─argMax(user, salary)─┬─argMax(tuple(user, salary), salary)─┐
│ director             │ ('director',5000)                   │
└──────────────────────┴─────────────────────────────────────┘
```

**Дополнительный пример**

```sql
CREATE TABLE test
(
    a Nullable(String),
    b Nullable(Int64)
)
ENGINE = Memory AS
SELECT *
FROM VALUES(('a', 1), ('b', 2), ('c', 2), (NULL, 3), (NULL, NULL), ('d', NULL));

select * from test;
┌─a────┬────b─┐
│ a    │    1 │
│ b    │    2 │
│ c    │    2 │
│ ᴺᵁᴸᴸ │    3 │
│ ᴺᵁᴸᴸ │ ᴺᵁᴸᴸ │
│ d    │ ᴺᵁᴸᴸ │
└──────┴──────┘

SELECT argMax(a, b), max(b) FROM test;
┌─argMax(a, b)─┬─max(b)─┐
│ b            │      3 │ -- argMax = 'b' потому что это первое not Null значение, max(b) из другой строки!
└──────────────┴────────┘

SELECT argMax(tuple(a), b) FROM test;
┌─argMax(tuple(a), b)─┐
│ (NULL)              │ -- Кортеж `Tuple`, который содержит только `NULL` значения является не `NULL` кортежем, поэтому агрегатыне функции не будут пропускать эту строку с `NULL` значениями.
└─────────────────────┘

SELECT (argMax((a, b), b) as t).1 argMaxA, t.2 argMaxB FROM test;
┌─argMaxA─┬─argMaxB─┐
│ ᴺᵁᴸᴸ    │       3 │ -- Вы можете использовать кортеж Tuple и получить оба значения для соответсвующего max(b).
└─────────┴─────────┘

SELECT argMax(a, b), max(b) FROM test WHERE a IS NULL AND b IS NULL;
┌─argMax(a, b)─┬─max(b)─┐
│ ᴺᵁᴸᴸ         │   ᴺᵁᴸᴸ │ -- Все агрегированные строки содержат хотя бы одно `NULL` значение, поэтому все строки пропускаются и результатом будет `NULL`.
└──────────────┴────────┘

SELECT argMax(a, (b,a)) FROM test;
┌─argMax(a, tuple(b, a))─┐
│ c                      │ -- Есть две строки с b=2, кортеж `Tuple` в функции `Max` позволяет получить не первый `arg`.
└────────────────────────┘

SELECT argMax(a, tuple(b)) FROM test;
┌─argMax(a, tuple(b))─┐
│ b                   │ -- Кортеж `Tuple` может использоваться в `Max`, чтобы не пропускать `NULL` значения в `Max`.
└─────────────────────┘
```

**Смотрите также**

- [Tuple](/docs/ru/sql-reference/data-types/tuple.md)
