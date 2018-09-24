<a name="table_engine-aggregatingmergetree"></a>

# AggregatingMergeTree

Движок наследует функциональность [MergeTree](mergetree.md#table_engines-mergetree), изменяя логику слияния кусков данных. Все строки с одинаковым первичным ключом ClickHouse заменяет на одну (в пределах одного куска данных), которая хранит объединение состояний агрегатных функций.

Таблицы типа `AggregatingMergeTree` могут использоваться для инкрементальной агрегации данных, в том числе, для агрегирующих материализованных представлений.

Движок обрабатывает все столбцы типа [AggregateFunction](../../data_types/nested_data_structures/aggregatefunction.md#data_type-aggregatefunction).

Использование `AggregatingMergeTree` оправдано только в том случае, когда это уменьшает количество строк на порядки.

## Конфигурирование движка при создании таблицы

```
ENGINE [=] AggregatingMergeTree() [PARTITION BY expr] [ORDER BY expr] [SAMPLE BY expr] [SETTINGS name=value, ...]
```

**Подчиненные секции ENGINE**

`AggregatingMergeTree` использует те же [секции ENGINE](mergetree.md#table_engines-mergetree-configuring), что и `MergeTree`.


### Устаревший способ конфигурирования движка

!!!attention
    Не используйте этот способ в новых проектах и по возможности переведите старые проекты на способ описанный выше.

```sql
AggregatingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity)
```

Все параметры имеют то же значение, что в и `MergeTree`.

## SELECT/INSERT данных

Для вставки данных используйте `INSERT SELECT` с агрегатными `-State`-функциями.

При выборке данных из таблицы `AggregatingMergeTree`, используйте `GROUP BY` и те же агрегатные функции, что и при вставке данных, но с суффиксом `-Merge`.

В запросах `SELECT` значения типа `AggregateFunction` выводятся во всех форматах, которые поддерживает ClickHouse, в виде implementation-specific бинарных данных. Если с помощью `SELECT` выполнить дамп данных, например, в формат `TabSeparated`, то потом этот дамп можно загрузить обратно с помощью запроса `INSERT`.

## Пример агрегирущего материализованного представления

Создаём материализованное представление типа `AggregatingMergeTree`, следящее за таблицей `test.visits`:

```sql
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

```sql
INSERT INTO test.visits ...
```

Данные окажутся и в таблице и в представлении `test.basic`, которое выполнит агрегацию.

Чтобы получить агрегированные данные, выполним запрос вида `SELECT ... GROUP BY ...` из представления `test.basic`:

```sql
SELECT
    StartDate,
    sumMerge(Visits) AS Visits,
    uniqMerge(Users) AS Users
FROM test.basic
GROUP BY StartDate
ORDER BY StartDate;
```
