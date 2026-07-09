---
description: 'Заменяет все строки с одинаковым первичным ключом (или, точнее, с одинаковым
  [ключом сортировки](../../../engines/table-engines/mergetree-family/mergetree.md))
  одной строкой (в пределах одной части данных), в которой хранится комбинация состояний
  агрегатных функций.'
sidebar_label: 'AggregatingMergeTree'
sidebar_position: 60
slug: /engines/table-engines/mergetree-family/aggregatingmergetree
title: 'Движок таблицы AggregatingMergeTree'
doc_type: 'справочник'
---

Этот движок наследуется от [MergeTree](/ru/engines/table-engines/mergetree-family/mergetree), изменяя логику слияния частей данных. ClickHouse заменяет все строки с одинаковым первичным ключом (или, точнее, с одинаковым [ключом сортировки](../../../engines/table-engines/mergetree-family/mergetree.md)) одной строкой (в пределах одной части данных), в которой хранится комбинация состояний агрегатных функций.

Вы можете использовать таблицы `AggregatingMergeTree` для инкрементальной агрегации данных, в том числе для агрегированных materialized view.

Ниже показан пример использования AggregatingMergeTree и агрегатных функций:

<div class="vimeo-container">
  <iframe width="1030" height="579" src="https://www.youtube.com/embed/pryhI4F_zqQ" title="Состояния агрегации в ClickHouse" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen />
</div>

Этот движок обрабатывает все столбцы следующих типов:

* [`AggregateFunction`](../../../sql-reference/data-types/aggregatefunction.md)
* [`SimpleAggregateFunction`](../../../sql-reference/data-types/simpleaggregatefunction.md)

`AggregatingMergeTree` целесообразно использовать, если он сокращает число строк на порядки.

<div id="creating-a-table">
  ## Создание таблицы
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = AggregatingMergeTree()
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[TTL expr]
[SETTINGS name=value, ...]
```

Описание параметров запроса см. в разделе [описание запроса](../../../sql-reference/statements/create/table.md).

**Секции запроса**

При создании таблицы `AggregatingMergeTree` требуются те же [секции](../../../engines/table-engines/mergetree-family/mergetree.md), что и при создании таблицы `MergeTree`.

<details markdown="1">
  <summary>Устаревший метод создания таблицы</summary>

  :::note
  Не используйте этот метод в новых проектах и, по возможности, переведите существующие проекты на метод, описанный выше.
  :::

  ```sql
  CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
  (
      name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
      name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
      ...
  ) ENGINE [=] AggregatingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity)
  ```

  Все параметры имеют то же значение, что и в `MergeTree`.
</details>

<div id="select-and-insert">
  ## SELECT и INSERT
</div>

Для вставки данных используйте запрос [INSERT SELECT](../../../sql-reference/statements/insert-into.md) с агрегатными функциями с суффиксом `-State`.
При выборке данных из таблицы `AggregatingMergeTree` используйте предложение `GROUP BY` и те же агрегатные функции, что и при вставке данных, но с суффиксом `-Merge`.

В результатах запроса `SELECT` значения типа `AggregateFunction` имеют зависящее от реализации двоичное представление во всех выходных форматах ClickHouse. Например, если выгрузить данные в формате `TabSeparated` с помощью запроса `SELECT`, этот дамп можно затем загрузить обратно с помощью запроса `INSERT`.

<div id="example-of-an-aggregated-materialized-view">
  ## Пример агрегирующей materialized view
</div>

В этом примере предполагается, что у вас есть база данных `test`. Если её ещё нет, создайте её с помощью приведённой ниже команды:

```sql
CREATE DATABASE test;
```

Теперь создайте таблицу `test.visits`, в которой содержатся необработанные данные:

```sql
CREATE TABLE test.visits
 (
    StartDate DateTime64 NOT NULL,
    CounterID UInt64,
    Sign Nullable(Int32),
    UserID Nullable(Int32)
) ENGINE = MergeTree ORDER BY (StartDate, CounterID);
```

Далее вам понадобится таблица `AggregatingMergeTree`, которая будет хранить `AggregationFunction`, отслеживающие общее число визитов и количество уникальных пользователей.

Создайте `AggregatingMergeTree` `materialized view`, которая следит за таблицей `test.visits` и использует тип [`AggregateFunction`](/ru/sql-reference/data-types/aggregatefunction):

```sql
CREATE TABLE test.agg_visits (
    StartDate DateTime64 NOT NULL,
    CounterID UInt64,
    Visits AggregateFunction(sum, Nullable(Int32)),
    Users AggregateFunction(uniq, Nullable(Int32))
)
ENGINE = AggregatingMergeTree() ORDER BY (StartDate, CounterID);
```

Создайте materialized view для заполнения `test.agg_visits` из `test.visits`:

```sql
CREATE MATERIALIZED VIEW test.visits_mv TO test.agg_visits
AS SELECT
    StartDate,
    CounterID,
    sumState(Sign) AS Visits,
    uniqState(UserID) AS Users
FROM test.visits
GROUP BY StartDate, CounterID;
```

Вставьте данные в таблицу `test.visits`:

```sql
INSERT INTO test.visits (StartDate, CounterID, Sign, UserID)
 VALUES (1667446031000, 1, 3, 4), (1667446031000, 1, 6, 3);
```

Данные вставляются как в `test.visits`, так и в `test.agg_visits`.

Чтобы получить агрегированные данные, выполните запрос к materialized view `test.visits_mv`, например `SELECT ... GROUP BY ...`:

```sql
SELECT
    StartDate,
    sumMerge(Visits) AS Visits,
    uniqMerge(Users) AS Users
FROM test.visits_mv
GROUP BY StartDate
ORDER BY StartDate;
```

```text
┌───────────────StartDate─┬─Visits─┬─Users─┐
│ 2022-11-03 03:27:11.000 │      9 │     2 │
└─────────────────────────┴────────┴───────┘
```

Добавьте в `test.visits` ещё пару записей, но на этот раз укажите для одной из них другую временную метку:

```sql
INSERT INTO test.visits (StartDate, CounterID, Sign, UserID)
 VALUES (1669446031000, 2, 5, 10), (1667446031000, 3, 7, 5);
```

Снова выполните запрос `SELECT`, и вы получите следующий результат:

```text
┌───────────────StartDate─┬─Visits─┬─Users─┐
│ 2022-11-03 03:27:11.000 │     16 │     3 │
│ 2022-11-26 07:00:31.000 │      5 │     1 │
└─────────────────────────┴────────┴───────┘
```

В некоторых случаях может потребоваться избежать предварительной агрегации строк при вставке, чтобы перенести затраты на агрегацию со времени вставки
на время merge. Обычно, чтобы избежать ошибки, в предложение `GROUP BY`
в определении materialized view необходимо включать столбцы, не участвующие в агрегации. Однако для этого можно использовать функцию [`initializeAggregation`](/ru/sql-reference/functions/other-functions#initializeAggregation)
с настройкой `optimize_on_insert = 0` (по умолчанию она включена). В этом случае `GROUP BY`
больше не требуется:

```sql
CREATE MATERIALIZED VIEW test.visits_mv TO test.agg_visits
AS SELECT
    StartDate,
    CounterID,
    initializeAggregation('sumState', Sign) AS Visits,
    initializeAggregation('uniqState', UserID) AS Users
FROM test.visits;
```

:::note
При использовании `initializeAggregation` для каждой отдельной строки создаётся состояние агрегатной функции без группировки.
Каждая исходная строка создаёт одну строку в materialized view, а фактическая агрегация происходит позже — при
слиянии частей в `AggregatingMergeTree`. Это верно только при `optimize_on_insert = 0`.
:::

<div id="tuple-element-aggregation">
  ## Агрегация элементов Tuple
</div>

Когда включена настройка `allow_tuple_element_aggregation`, столбцы `Tuple` рекурсивно приводятся к плоскому виду, так что каждый конечный элемент участвует в агрегации независимо. Это означает, что подстолбцы `AggregateFunction` или `SimpleAggregateFunction` внутри `Tuple` агрегируются в соответствии с соответствующими им функциями, как если бы это были столбцы верхнего уровня.

Подстолбцы, входящие в `Tuple` в ключе сортировки, исключаются из агрегации. Неагрегатные подстолбцы обрабатываются как обычные столбцы (сохраняется их первое значение).

:::note
Эта настройка неизменяема и должна быть указана при создании таблицы.
:::

```sql
CREATE TABLE agg_tuples
(
    key UInt32,
    metrics Tuple(
        total_visits SimpleAggregateFunction(sum, UInt64),
        unique_users SimpleAggregateFunction(max, UInt64)
    )
) ENGINE = AggregatingMergeTree()
ORDER BY key
SETTINGS allow_tuple_element_aggregation = 1;

INSERT INTO agg_tuples VALUES (1, (100, 5));
INSERT INTO agg_tuples VALUES (1, (200, 8));
INSERT INTO agg_tuples VALUES (2, (50, 3));

OPTIMIZE TABLE agg_tuples FINAL;

SELECT key, metrics.total_visits, metrics.unique_users FROM agg_tuples ORDER BY key;
```

```text
┌─key─┬─metrics.total_visits─┬─metrics.unique_users─┐
│   1 │                  300 │                    8 │
│   2 │                   50 │                    3 │
└─────┴──────────────────────┴──────────────────────┘
```

`total_visits` агрегируется функцией `sum` (100 + 200 = 300), а `unique_users` — функцией `max` (max(5, 8) = 8).

<div id="related-content">
  ## Связанные материалы
</div>

* Блог: [Использование комбинаторов агрегатных функций в ClickHouse](https://clickhouse.com/blog/aggregate-functions-combinators-in-clickhouse-for-arrays-maps-and-states)