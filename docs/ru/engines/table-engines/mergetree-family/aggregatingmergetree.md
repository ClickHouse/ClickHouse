---
slug: /ru/engines/table-engines/mergetree-family/aggregatingmergetree
sidebar_position: 35
sidebar_label: AggregatingMergeTree
---

# AggregatingMergeTree {#aggregatingmergetree}

Движок наследует функциональность [MergeTree](mergetree.md#table_engines-mergetree), изменяя логику слияния кусков данных. Все строки с одинаковым первичным ключом (точнее, с одинаковым [ключом сортировки](mergetree.md)) ClickHouse заменяет на одну (в пределах одного куска данных), которая хранит объединение состояний агрегатных функций.

Таблицы типа `AggregatingMergeTree` могут использоваться для инкрементальной агрегации данных, в том числе, для агрегирующих материализованных представлений.

Движок обрабатывает все столбцы типа [AggregateFunction](../../../sql-reference/data-types/aggregatefunction.md).

Использование `AggregatingMergeTree` оправдано только в том случае, когда это уменьшает количество строк на порядки.

## Создание таблицы {#sozdanie-tablitsy}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = AggregatingMergeTree([default_aggregate_function])
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

Описание параметров запроса смотрите в [описании запроса](../../../engines/table-engines/mergetree-family/aggregatingmergetree.md).

### Параметры AggregatingMergeTree

#### default_aggregate_function

`default_aggregate_function` - название одной из [SimpleAggregateFunction](../../../sql-reference/data-types/simpleaggregatefunction.md), которая будет применена ко всем столбцам, тип которых отличается от [AggregateFunction](../../../sql-reference/data-types/aggregatefunction.md) или [SimpleAggregateFunction](../../../sql-reference/data-types/simpleaggregatefunction.md).
Функция по умолчанию не применяется к столбцам, входящим в первичный ключ.
Необязательный параметр.

Если `default_aggregate_function` не указан для колонки и тип колонки не AggregateFunction и не SimpleAggregateFunction, то в качестве значения будет взято любое (включая NULL) из значений в столбце в рамках того же первичного ключа.

:::warning
При использовании этого параметра при создании таблицы не производится проверка, принимает ли указанная функция типы данных, представленные в столбцах.
Если функция не может работать с типом данных, исключение будет выброшено лишь во время вставки данных или при следующем мерже.
:::

**Секции запроса**

При создании таблицы `AggregatingMergeTree` используются те же [секции](mergetree.md), что и при создании таблицы `MergeTree`.

<details markdown="1">

<summary>Устаревший способ создания таблицы</summary>

:::note Важно
Не используйте этот способ в новых проектах и по возможности переведите старые проекты на способ описанный выше.
:::

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE [=] AggregatingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity)
```

Все параметры имеют то же значение, что в и `MergeTree`.
</details>

## SELECT/INSERT данных {#selectinsert-dannykh}

Для вставки данных используйте `INSERT SELECT` с агрегатными `-State`-функциями.

При выборке данных из таблицы `AggregatingMergeTree`, используйте `GROUP BY` и те же агрегатные функции, что и при вставке данных, но с суффиксом `-Merge`.

В запросах `SELECT` значения типа `AggregateFunction` выводятся во всех форматах, которые поддерживает ClickHouse, в виде implementation-specific бинарных данных. Если с помощью `SELECT` выполнить дамп данных, например, в формат `TabSeparated`, то потом этот дамп можно загрузить обратно с помощью запроса `INSERT`.

## Пример агрегирущего материализованного представления {#primer-agregirushchego-materializovannogo-predstavleniia}

Создаём материализованное представление типа `AggregatingMergeTree`, следящее за таблицей `test.visits`:

``` sql
CREATE MATERIALIZED VIEW test.basic
ENGINE = AggregatingMergeTree() PARTITION BY toYYYYMM(StartDate) ORDER BY (CounterID, StartDate)
AS SELECT
    CounterID,
    StartDate,
    sumState(Sign)    AS Visits,
    uniqState(UserID) AS Users
FROM test.visits
GROUP BY CounterID, StartDate;
```

Вставляем данные в таблицу `test.visits`:

``` sql
INSERT INTO test.visits ...
```

Данные окажутся и в таблице и в представлении `test.basic`, которое выполнит агрегацию.

Чтобы получить агрегированные данные, выполним запрос вида `SELECT ... GROUP BY ...` из представления `test.basic`:

``` sql
SELECT
    StartDate,
    sumMerge(Visits) AS Visits,
    uniqMerge(Users) AS Users
FROM test.basic
GROUP BY StartDate
ORDER BY StartDate;
```
