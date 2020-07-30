# AggregateFunction {#data-type-aggregatefunction}

Промежуточное состояние агрегатной функции. Чтобы его получить, используются агрегатные функции с суффиксом `-State`. Чтобы в дальнейшем получить агрегированные данные необходимо использовать те же агрегатные функции с суффиксом `-Merge`.

`AggregateFunction(name, types\_of\_arguments…)` — параметрический тип данных.

**Параметры**

-   Имя агрегатной функции.

        Для параметрических агрегатных функций указываются также их параметры.

-   Типы аргументов агрегатной функции.

**Пример**

``` sql
CREATE TABLE t
(
    column1 AggregateFunction(uniq, UInt64),
    column2 AggregateFunction(anyIf, String, UInt8),
    column3 AggregateFunction(quantiles(0.5, 0.9), UInt64)
) ENGINE = ...
```

[uniq](../../sql-reference/aggregate-functions/reference/uniq.md#agg_function-uniq), anyIf ([any](../../sql-reference/aggregate-functions/reference/any.md#agg_function-any)+[If](../../sql-reference/aggregate-functions/combinators.md#agg-functions-combinator-if)) и [quantiles](../../sql-reference/aggregate-functions/reference/quantiles.md) — агрегатные функции, поддержанные в ClickHouse.

## Особенности использования {#osobennosti-ispolzovaniia}

### Вставка данных {#vstavka-dannykh}

Для вставки данных используйте `INSERT SELECT` с агрегатными `-State`-функциями.

**Примеры функций**

``` sql
uniqState(UserID)
quantilesState(0.5, 0.9)(SendTiming)
```

В отличие от соответствующих функций `uniq` и `quantiles`, `-State`-функциями возвращают не готовое значение, а состояние. То есть, значение типа `AggregateFunction`.

В запросах `SELECT` значения типа `AggregateFunction` выводятся во всех форматах, которые поддерживает ClickHouse, в виде implementation-specific бинарных данных. Если с помощью `SELECT` выполнить дамп данных, например, в формат `TabSeparated`, то потом этот дамп можно загрузить обратно с помощью запроса `INSERT`.

### Выборка данных {#vyborka-dannykh}

При выборке данных из таблицы `AggregatingMergeTree`, используйте `GROUP BY` и те же агрегатные функции, что и при вставке данных, но с суффиксом `-Merge`.

Агрегатная функция с суффиксом `-Merge` берёт множество состояний, объединяет их, и возвращает результат полной агрегации данных.

Например, следующие два запроса возвращают один и тот же результат:

``` sql
SELECT uniq(UserID) FROM table

SELECT uniqMerge(state) FROM (SELECT uniqState(UserID) AS state FROM table GROUP BY RegionID)
```

## Пример использования {#primer-ispolzovaniia}

Смотрите в описании движка [AggregatingMergeTree](../../sql-reference/data-types/aggregatefunction.md).

[Оригинальная статья](https://clickhouse.tech/docs/ru/data_types/nested_data_structures/aggregatefunction/) <!--hide-->
