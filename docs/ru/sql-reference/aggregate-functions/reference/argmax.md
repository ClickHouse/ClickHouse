---
toc_priority: 106
---

# argMax {#agg-function-argmax}

Вычисляет значение `arg` при максимальном значении `val`. Если есть несколько разных значений `arg` для максимальных значений `val`, возвращает первое попавшееся из таких значений.

Если функции передан кортеж, то будет выведен кортеж с максимальным значением `val`. Удобно использовать для работы с [SimpleAggregateFunction](../../../sql-reference/data-types/simpleaggregatefunction.md).

**Синтаксис**

``` sql
argMax(arg, val)
```

или

``` sql
argMax(tuple(arg, val))
```

**Аргументы**

-   `arg` — аргумент.
-   `val` — значение.

**Возвращаемое значение**

-   значение `arg`, соответствующее максимальному значению `val`.

Тип: соответствует типу `arg`. 

Если передан кортеж:

-   кортеж `(arg, val)` c максимальным значением `val` и соответствующим ему `arg`.

Тип: [Tuple](../../../sql-reference/data-types/tuple.md).

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
SELECT argMax(user, salary), argMax(tuple(user, salary), salary), argMax(tuple(user, salary)) FROM salary;
```

Результат:

``` text
┌─argMax(user, salary)─┬─argMax(tuple(user, salary), salary)─┬─argMax(tuple(user, salary))─┐
│ director             │ ('director',5000)                   │ ('director',5000)           │
└──────────────────────┴─────────────────────────────────────┴─────────────────────────────┘
```

