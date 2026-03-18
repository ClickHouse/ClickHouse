---
slug: /ru/engines/table-engines/mergetree-family/replacingmergetree
sidebar_position: 33
sidebar_label: ReplacingMergeTree
---

# ReplacingMergeTree {#replacingmergetree}

Движок отличается от [MergeTree](mergetree.md#table_engines-mergetree) тем, что выполняет удаление дублирующихся записей с одинаковым значением [ключа сортировки](mergetree.md) (секция `ORDER BY`, не `PRIMARY KEY`).

Дедупликация данных производится лишь во время слияний. Слияние происходят в фоне в неизвестный момент времени, на который вы не можете ориентироваться. Некоторая часть данных может остаться необработанной. Хотя вы можете вызвать внеочередное слияние с помощью запроса `OPTIMIZE`, на это не стоит рассчитывать, так как запрос `OPTIMIZE` приводит к чтению и записи большого объёма данных.

Таким образом, `ReplacingMergeTree` подходит для фоновой чистки дублирующихся данных в целях экономии места, но не даёт гарантии отсутствия дубликатов.

## Создание таблицы {#sozdanie-tablitsy}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = ReplacingMergeTree([ver [, is_deleted]])
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

Описание параметров запроса смотрите в [описании запроса](../../../engines/table-engines/mergetree-family/replacingmergetree.md).

:::warning Внимание
Уникальность строк определяется `ORDER BY` секцией таблицы, а не `PRIMARY KEY`.
:::

## Параметры ReplacingMergeTree

### ver

`ver` — столбец с номером версии. Тип `UInt*`, `Date`, `DateTime` или `DateTime64`. Необязательный параметр.

При слиянии `ReplacingMergeTree` оставляет только строку для каждого уникального ключа сортировки:

    - Последнюю в выборке, если `ver` не задан. Под выборкой здесь понимается набор строк в наборе кусков данных, участвующих в слиянии. Последний по времени создания кусок (последняя вставка) будет последним в выборке. Таким образом, после дедупликации для каждого значения ключа сортировки останется самая последняя строка из самой последней вставки.
    - С максимальной версией, если `ver` задан. Если `ver` одинаковый у нескольких строк, то для них используется правило -- если `ver` не задан, т.е. в результате слияния останется самая последняя строка из самой последней вставки.

Пример: 

```sql
-- without ver - the last inserted 'wins'
CREATE TABLE myFirstReplacingMT
(
    `key` Int64,
    `someCol` String,
    `eventTime` DateTime
)
ENGINE = ReplacingMergeTree
ORDER BY key;

INSERT INTO myFirstReplacingMT Values (1, 'first', '2020-01-01 01:01:01');
INSERT INTO myFirstReplacingMT Values (1, 'second', '2020-01-01 00:00:00');

SELECT * FROM myFirstReplacingMT FINAL;

┌─key─┬─someCol─┬───────────eventTime─┐
│   1 │ second  │ 2020-01-01 00:00:00 │
└─────┴─────────┴─────────────────────┘


-- with ver - the row with the biggest ver 'wins'
CREATE TABLE mySecondReplacingMT
(
    `key` Int64,
    `someCol` String,
    `eventTime` DateTime
)
ENGINE = ReplacingMergeTree(eventTime)
ORDER BY key;

INSERT INTO mySecondReplacingMT Values (1, 'first', '2020-01-01 01:01:01');
INSERT INTO mySecondReplacingMT Values (1, 'second', '2020-01-01 00:00:00');

SELECT * FROM mySecondReplacingMT FINAL;

┌─key─┬─someCol─┬───────────eventTime─┐
│   1 │ first   │ 2020-01-01 01:01:01 │
└─────┴─────────┴─────────────────────┘
```
### is_deleted

`is_deleted` —  Имя столбца, который используется во время слияния для обозначения того, нужно ли отображать строку или она подлежит удалению; `1` - для удаления строки, `0` - для отображения строки.

  Тип данных столбца — `UInt8`.

:::note
`is_deleted` может быть использован, если `ver` используется.

Строка удаляется в следующих случаях:

    - при использовании инструкции `OPTIMIZE ... FINAL CLEANUP`
    - при использовании инструкции `OPTIMIZE ... FINAL`
    - есть новые версии строки

Не рекомендуется выполнять `FINAL CLEANUP`, это может привести к неожиданным результатам, например удаленные строки могут вновь появиться.

Вне зависимости от производимых изменений над данными, версия должна увеличиваться. Если у двух строк одна и та же версия, то остается только последняя вставленная строка.
:::

Пример:

```sql
-- with ver and is_deleted
CREATE OR REPLACE TABLE myThirdReplacingMT
(
    `key` Int64,
    `someCol` String,
    `eventTime` DateTime,
    `is_deleted` UInt8
)
ENGINE = ReplacingMergeTree(eventTime, is_deleted)
ORDER BY key;

INSERT INTO myThirdReplacingMT Values (1, 'first', '2020-01-01 01:01:01', 0);
INSERT INTO myThirdReplacingMT Values (1, 'first', '2020-01-01 01:01:01', 1); 

select * from myThirdReplacingMT final;

0 rows in set. Elapsed: 0.003 sec.

-- delete rows with is_deleted
OPTIMIZE TABLE myThirdReplacingMT FINAL CLEANUP; 

INSERT INTO myThirdReplacingMT Values (1, 'first', '2020-01-01 00:00:00', 0);

select * from myThirdReplacingMT final; 

┌─key─┬─someCol─┬───────────eventTime─┬─is_deleted─┐
│   1 │ first   │ 2020-01-01 00:00:00 │          0 │
└─────┴─────────┴─────────────────────┴────────────┘
```

## Секции запроса

При создании таблицы `ReplacingMergeTree` используются те же [секции](mergetree.md), что и при создании таблицы `MergeTree`.

<details markdown="1">

<summary>Устаревший способ создания таблицы</summary>

:::warning Внимание
Не используйте этот способ в новых проектах и по возможности переведите старые проекты на способ, описанный выше.
:::

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE [=] ReplacingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, [ver])
```

Все параметры, кроме `ver` имеют то же значение, что в и `MergeTree`.

-   `ver` — столбец с номером версии. Необязательный параметр. Описание смотрите выше по тексту.

</details>
