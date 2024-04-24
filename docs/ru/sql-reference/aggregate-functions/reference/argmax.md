---
slug: /ru/sql-reference/aggregate-functions/reference/argmax
sidebar_position: 106
---

# argMax {#agg-function-argmax}

Вычисляет значение `arg` при максимальном значении `val`.

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
