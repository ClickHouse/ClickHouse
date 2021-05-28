---
toc_priority: 105
---

# argMin {#agg-function-argmin}

Вычисляет значение `arg` при минимальном значении `val`. Если есть несколько разных значений `arg` для минимальных значений `val`, возвращает первое попавшееся из таких значений.

Если функции передан кортеж, то будет выведен кортеж с минимальным значением `val`. Удобно использовать для работы с [SimpleAggregateFunction](../../../sql-reference/data-types/simpleaggregatefunction.md).

**Синтаксис**

``` sql
argMin(arg, val)
```

или

``` sql
argMin(tuple(arg, val))
```

**Аргументы**

-   `arg` — аргумент.
-   `val` — значение.

**Возвращаемое значение**

-   Значение `arg`, соответствующее минимальному значению `val`.

Тип: соответствует типу `arg`. 

Если передан кортеж:

-   Кортеж `(arg, val)` c минимальным значением `val` и соответствующим ему `arg`.

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
SELECT argMin(user, salary), argMin(tuple(user, salary)) FROM salary;
```

Результат:

``` text
┌─argMin(user, salary)─┬─argMin(tuple(user, salary))─┐
│ worker               │ ('worker',1000)             │
└──────────────────────┴─────────────────────────────┘
```

