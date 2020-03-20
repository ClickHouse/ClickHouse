# Агрегатные функции {#aggregate-functions}

Агрегатные функции работают в [привычном](http://www.sql-tutorial.com/sql-aggregate-functions-sql-tutorial) для специалистов по базам данных смысле.

ClickHouse поддерживает также:

-   [Параметрические агрегатные функции](parametric_functions.md#aggregate_functions_parametric), которые помимо столбцов принимаю и другие параметры.
-   [Комбинаторы](combinators.md#aggregate_functions_combinators), которые изменяют поведение агрегатных функций.

## Обработка NULL {#obrabotka-null}

При агрегации все `NULL` пропускаются.

**Примеры**

Рассмотрим таблицу:

``` text
┌─x─┬────y─┐
│ 1 │    2 │
│ 2 │ ᴺᵁᴸᴸ │
│ 3 │    2 │
│ 3 │    3 │
│ 3 │ ᴺᵁᴸᴸ │
└───┴──────┘
```

Выполним суммирование значений в столбце `y`:

``` sql
SELECT sum(y) FROM t_null_big
```

``` text
┌─sum(y)─┐
│      7 │
└────────┘
```

Функция `sum` работает с `NULL` как с `0`. В частности, это означает, что если на вход в функцию подать выборку, где все значения `NULL`, то результат будет `0`, а не `NULL`.

Теперь с помощью функции `groupArray` сформируем массив из столбца `y`:

``` sql
SELECT groupArray(y) FROM t_null_big
```

``` text
┌─groupArray(y)─┐
│ [2,2,3]       │
└───────────────┘
```

`groupArray` не включает `NULL` в результирующий массив.

[Оригинальная статья](https://clickhouse.tech/docs/ru/query_language/agg_functions/) <!--hide-->
