---
toc_priority: 1
---

# count {#agg_function-count}

Вычисляет количество строк или не NULL значений.

ClickHouse поддерживает следующие виды синтаксиса для `count`:

-   `count(expr)` или `COUNT(DISTINCT expr)`.
-   `count()` или `COUNT(*)`. Синтаксис `count()` специфичен для ClickHouse.

**Аргументы**

Функция может принимать:

-   Ноль параметров.
-   Одно [выражение](../../syntax.md#syntax-expressions).

**Возвращаемое значение**

-   Если функция вызывается без параметров, она вычисляет количество строк.
-   Если передаётся [выражение](../../syntax.md#syntax-expressions), то функция подсчитывает количество раз, когда выражение не равно NULL. Если выражение имеет тип [Nullable](../../../sql-reference/data-types/nullable.md), то результат `count` не становится `Nullable`. Функция возвращает 0, если выражение равно `NULL` для всех строк.

В обоих случаях тип возвращаемого значения [UInt64](../../../sql-reference/data-types/int-uint.md).

**Подробности**

ClickHouse поддерживает синтаксис `COUNT(DISTINCT ...)`. Поведение этой конструкции зависит от настройки [count_distinct_implementation](../../../operations/settings/settings.md#settings-count_distinct_implementation). Она определяет, какая из функций [uniq\*](../../../sql-reference/aggregate-functions/reference/uniq.md#agg_function-uniq) используется для выполнения операции. По умолчанию — функция [uniqExact](../../../sql-reference/aggregate-functions/reference/uniqexact.md#agg_function-uniqexact).

Запрос `SELECT count() FROM table` оптимизирован по умолчанию с использованием метаданных из MergeTree. Если вы хотите управлять безопасностью на уровне строк, отключите оптимизацию при помощи настройки [optimize_trivial_count_query](../../../operations/settings/settings.md#optimize-trivial-count-query).

При этом запрос `SELECT count(nullable_column) FROM table` может быть оптимизирован включением настройки [optimize_functions_to_subcolumns](../../../operations/settings/settings.md#optimize-functions-to-subcolumns). При `optimize_functions_to_subcolumns = 1` функция читает только подстолбец [null](../../../sql-reference/data-types/nullable.md#finding-null) вместо чтения всех данных столбца. Запрос `SELECT count(n) FROM table` преобразуется к запросу `SELECT sum(NOT n.null) FROM table`.

**Примеры**

Пример 1:

``` sql
SELECT count() FROM t
```

``` text
┌─count()─┐
│       5 │
└─────────┘
```

Пример 2:

``` sql
SELECT name, value FROM system.settings WHERE name = 'count_distinct_implementation'
```

``` text
┌─name──────────────────────────┬─value─────┐
│ count_distinct_implementation │ uniqExact │
└───────────────────────────────┴───────────┘
```

``` sql
SELECT count(DISTINCT num) FROM t
```

``` text
┌─uniqExact(num)─┐
│              3 │
└────────────────┘
```

Этот пример показывает, что `count(DISTINCT num)` выполняется с помощью функции `uniqExact` в соответствии со значением настройки `count_distinct_implementation`.
