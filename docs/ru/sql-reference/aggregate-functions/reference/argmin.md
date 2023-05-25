---
slug: /ru/sql-reference/aggregate-functions/reference/argmin
sidebar_position: 105
---

# argMin {#agg-function-argmin}

Вычисляет значение `arg` при минимальном значении `val`. Если есть несколько разных значений `arg` для минимальных значений `val`, возвращает первое попавшееся из таких значений.

**Синтаксис**

``` sql
argMin(arg, val)
```

**Аргументы**

-   `arg` — аргумент.
-   `val` — значение.

**Возвращаемое значение**

-   Значение `arg`, соответствующее минимальному значению `val`.

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
SELECT argMin(user, salary) FROM salary;
```

Результат:

``` text
┌─argMin(user, salary)─┐
│ worker               │
└──────────────────────┘
```
