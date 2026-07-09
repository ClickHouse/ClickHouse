---
description: 'отличается от MergeTree тем, что удаляет дублирующиеся записи с
  одним и тем же значением ключа сортировки (раздел таблицы `ORDER BY`, а не
  `PRIMARY KEY`).'
sidebar_label: 'ReplacingMergeTree'
sidebar_position: 40
slug: /engines/table-engines/mergetree-family/replacingmergetree
title: 'Движок таблицы ReplacingMergeTree'
doc_type: 'reference'
---

Этот движок отличается от [MergeTree](/ru/engines/table-engines/mergetree-family/mergetree) тем, что удаляет дублирующиеся записи с одним и тем же значением [ключа сортировки](../../../engines/table-engines/mergetree-family/mergetree.md) (раздел таблицы `ORDER BY`, а не `PRIMARY KEY`).

Дедупликация данных происходит только во время слияния. Слияние выполняется в фоновом режиме в непредсказуемый момент, поэтому планировать его нельзя. Часть данных может так и остаться необработанной. Хотя вы можете запустить внеплановое слияние с помощью запроса `OPTIMIZE`, не стоит на это рассчитывать, поскольку запрос `OPTIMIZE` читает и записывает большие объёмы данных.

Таким образом, `ReplacingMergeTree` подходит для фонового удаления дубликатов ради экономии места, но не гарантирует полного отсутствия дубликатов.

:::note
Подробное руководство по ReplacingMergeTree, включая рекомендации и способы оптимизации производительности, доступно [здесь](/ru/guides/replacing-merge-tree).
:::

<div id="creating-a-table">
  ## Создание таблицы
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = ReplacingMergeTree([ver [, is_deleted]])
[PARTITION BY expr]
[ORDER BY expr]
[PRIMARY KEY expr]
[SAMPLE BY expr]
[SETTINGS name=value, ...]
```

Описание параметров запроса см. в [описании оператора](../../../sql-reference/statements/create/table.md).

:::note
Уникальность строк определяется разделом таблицы `ORDER BY`, а не `PRIMARY KEY`.
:::

<div id="replacingmergetree-parameters">
  ## Параметры ReplacingMergeTree
</div>

<div id="ver">
  ### `ver`
</div>

`ver` — столбец с версией. Тип `UInt*`, `Date`, `DateTime` или `DateTime64`. Необязательный параметр.

При слиянии `ReplacingMergeTree` из всех строк с одинаковым ключом сортировки оставляет только одну:

* Последнюю в выборке, если `ver` не задан. Выборка — это набор строк в наборе частей, участвующих в слиянии. Последней в выборке будет самая поздно созданная часть (последняя вставка). Таким образом, после дедупликации для каждого уникального ключа сортировки останется самая последняя строка из самой недавней вставки.
* С максимальной версией, если `ver` указан. Если у нескольких строк `ver` одинаковый, для них применяется правило &quot;если `ver` не указан&quot;, то есть останется строка, вставленная последней.

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

<div id="is_deleted">
  ### `is_deleted`
</div>

`is_deleted` — имя столбца, используемого при слиянии, чтобы определить, представляют ли данные в этой строке состояние или строка подлежит удалению; `1` — это строка &quot;удалено&quot;, `0` — это строка &quot;состояние&quot;.

Тип данных столбца — `UInt8`.

:::note
`is_deleted` можно включить только при использовании `ver`.

Независимо от операции с данными, версию следует увеличивать. Если две вставленные строки имеют одинаковый номер версии, сохраняется последняя вставленная строка.

По умолчанию ClickHouse сохраняет последнюю строку для ключа, даже если это строка удаления. Это сделано для того, чтобы любые будущие строки с более низкими версиями можно было
безопасно вставить, и строка удаления при этом всё равно была применена.

Чтобы окончательно удалить такие строки удаления, включите настройку таблицы `allow_experimental_replacing_merge_with_cleanup` и затем выполните одно из следующих действий:

1. Задайте настройки таблицы `enable_replacing_merge_with_cleanup_for_min_age_to_force_merge`, `min_age_to_force_merge_on_partition_only` и `min_age_to_force_merge_seconds`. Если все части в партиции старше `min_age_to_force_merge_seconds`, ClickHouse объединит их
   в одну часть и удалит все строки удаления.

2. Вручную выполните `OPTIMIZE TABLE table [PARTITION partition | PARTITION ID 'partition_id'] FINAL CLEANUP`.
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
ORDER BY key
SETTINGS allow_experimental_replacing_merge_with_cleanup = 1;

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

<div id="query-clauses">
  ## Секции запроса
</div>

При создании таблицы `ReplacingMergeTree` требуются те же [секции](../../../engines/table-engines/mergetree-family/mergetree.md), что и при создании таблицы `MergeTree`.

<details markdown="1">
  <summary>Устаревший метод создания таблицы</summary>

  :::note
  Не используйте этот метод в новых проектах и по возможности переведите старые проекты на метод, описанный выше.
  :::

  ```sql
  CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
  (
      name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
      name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
      ...
  ) ENGINE [=] ReplacingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity, [ver])
  ```

  Все параметры, кроме `ver`, имеют тот же смысл, что и в `MergeTree`.

  * `ver` — столбец с версией. Необязательный параметр. Описание см. в тексте выше.
</details>

<div id="query-time-de-duplication--final">
  ## Дедупликация на этапе выполнения запроса &amp; FINAL
</div>

Во время слияния ReplacingMergeTree выявляет повторяющиеся строки, используя значения столбцов `ORDER BY` (используемых при создании таблицы) в качестве уникального идентификатора, и сохраняет только строку с наибольшей версией. Однако это обеспечивает лишь итоговую корректность — нет гарантии, что строки будут дедуплицированы, поэтому полагаться на это не следует. Соответственно, запросы могут возвращать некорректные результаты, поскольку при их выполнении могут учитываться строки обновления и удаления.

Чтобы получать корректные результаты, пользователям нужно дополнять фоновые слияния дедупликацией на этапе выполнения запроса и исключением удалённых строк. Это можно сделать с помощью оператора `FINAL`. Например, рассмотрим следующий пример:

```sql
CREATE TABLE rmt_example
(
    `number` UInt16
)
ENGINE = ReplacingMergeTree
ORDER BY number

INSERT INTO rmt_example SELECT floor(randUniform(0, 100)) AS number
FROM numbers(1000000000)

0 rows in set. Elapsed: 19.958 sec. Processed 1.00 billion rows, 8.00 GB (50.11 million rows/s., 400.84 MB/s.)
```

Запросы без `FINAL` дают неверный результат подсчёта (точный результат зависит от слияний):

```sql
SELECT count()
FROM rmt_example

┌─count()─┐
│     200 │
└─────────┘

1 row in set. Elapsed: 0.002 sec.
```

Использование FINAL дает правильный результат:

```sql
SELECT count()
FROM rmt_example
FINAL

┌─count()─┐
│     100 │
└─────────┘

1 row in set. Elapsed: 0.002 sec.
```

Чтобы узнать больше о `FINAL`, включая способы оптимизации его производительности, рекомендуем ознакомиться с нашим [подробным руководством по ReplacingMergeTree](/ru/guides/replacing-merge-tree).