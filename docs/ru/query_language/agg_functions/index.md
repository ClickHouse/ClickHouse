<a name="aggregate_functions"></a>

# Агрегатные функции

Агрегатные функции работают в [привычном](http://www.sql-tutorial.com/sql-aggregate-functions-sql-tutorial) для специалистов по базам данных смысле.

ClickHouse поддерживает также:

-  [Параметрические агрегатные функции](parametric_functions.md#aggregate_functions_parametric), которые помимо стоблцов принимаю и другие параметры.
-  [Комбинаторы](combinators.md#aggregate_functions_combinators), которые изменяют поведение агрегатных фунций.

## Обработка NULL

При агрегации все `NULL` пропускаются.

**Примеры**

Рассмотрим таблицу:

```
┌─x─┬────y─┐
│ 1 │    2 │
│ 2 │ ᴺᵁᴸᴸ │
│ 3 │    2 │
│ 3 │    3 │
│ 3 │ ᴺᵁᴸᴸ │
└───┴──────┘
```

Выполним суммирование значений в столбце `y`:

```
:) SELECT sum(y) FROM t_null_big

SELECT sum(y)
FROM t_null_big

┌─sum(y)─┐
│      7 │
└────────┘

1 rows in set. Elapsed: 0.002 sec.
```

Функция `sum` работает с `NULL` как с `0`. В частности, это означает, что если на вход в функцию подать выборку, где все значения `NULL`, то результат будет `0`, а не `NULL`.


Теперь с помощью фукции `groupArray` сформируем массив из стобца `y`:

```
:) SELECT groupArray(y) FROM t_null_big

SELECT groupArray(y)
FROM t_null_big

┌─groupArray(y)─┐
│ [2,2,3]       │
└───────────────┘

1 rows in set. Elapsed: 0.002 sec.
```

`groupArray` не включает `NULL` в результирующий массив.
