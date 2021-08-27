---
toc_priority: 33
toc_title: ReplacingMergeTree
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
) ENGINE = ReplacingMergeTree([ver])
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

Описание параметров запроса смотрите в [описании запроса](../../../engines/table-engines/mergetree-family/replacingmergetree.md).

!!! note "Внимание"
    Уникальность строк определяется `ORDER BY` секцией таблицы, а не `PRIMARY KEY`.

**Параметры ReplacingMergeTree**

-   `ver` — столбец с версией, тип `UInt*`, `Date` или `DateTime`. Необязательный параметр.

    При слиянии `ReplacingMergeTree` оставляет только строку для каждого уникального ключа сортировки:

    - Последнюю в выборке, если `ver` не задан. Под выборкой здесь понимается набор строк в наборе партов, участвующих в слиянии. Последний по времени создания парт (последний инсерт) будет последним в выборке. Таким образом, после дедупликации для каждого значения ключа сортировки останется самая последняя строка из самого последнего инсерта.
    - С максимальной версией, если `ver` задан.

**Секции запроса**

При создании таблицы `ReplacingMergeTree` используются те же [секции](mergetree.md), что и при создании таблицы `MergeTree`.

<details markdown="1">

<summary>Устаревший способ создания таблицы</summary>

!!! attention "Внимание"
    Не используйте этот способ в новых проектах и по возможности переведите старые проекты на способ описанный выше.

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE [=] ReplacingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, [ver])
```

Все параметры, кроме `ver` имеют то же значение, что в и `MergeTree`.

-   `ver` — столбец с версией. Необязательный параметр. Описание смотрите выше по тексту.

</details>

[Оригинальная статья](https://clickhouse.tech/docs/ru/operations/table_engines/replacingmergetree/) <!--hide-->
