---
toc_priority: 1
---

# count {#agg_function-count}

Вычисляет количество строк или не NULL значений .

ClickHouse поддерживает следующие виды синтаксиса для `count`:

-   `count(expr)` или `COUNT(DISTINCT expr)`.
-   `count()` или `COUNT(*)`. Синтаксис `count()` специфичен для ClickHouse.

**Параметры**

Функция может принимать:

-   Ноль параметров.
-   Одно [выражение](../../syntax.md#syntax-expressions).

**Возвращаемое значение**

-   Если функция вызывается без параметров, она вычисляет количество строк.
-   Если передаётся [выражение](../../syntax.md#syntax-expressions) , то функция вычисляет количество раз, когда выражение возвращает не NULL. Если выражение возвращает значение типа [Nullable](../../../sql-reference/data-types/nullable.md), то результат `count` не становится `Nullable`. Функция возвращает 0, если выражение возвращает `NULL` для всех строк.

В обоих случаях тип возвращаемого значения [UInt64](../../../sql-reference/data-types/int-uint.md).

**Подробности**

ClickHouse поддерживает синтаксис `COUNT(DISTINCT ...)`. Поведение этой конструкции зависит от настройки [count_distinct_implementation](../../../operations/settings/settings.md#settings-count_distinct_implementation). Она определяет, какая из функций [uniq\*](../../../sql-reference/aggregate-functions/reference/uniq.md#agg_function-uniq) используется для выполнения операции. По умолчанию — функция [uniqExact](../../../sql-reference/aggregate-functions/reference/uniqexact.md#agg_function-uniqexact).

Запрос `SELECT count() FROM table` не оптимизирован, поскольку количество записей в таблице не хранится отдельно. Он выбирает небольшой столбец из таблицы и подсчитывает количество значений в нём.

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

[Оригинальная статья](https://clickhouse.tech/docs/en/sql-reference/aggregate-functions/reference/count/) <!--hide-->
