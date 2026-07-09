---
description: 'Документация по управлению проекциями'
sidebar_label: 'PROJECTION'
sidebar_position: 49
slug: /sql-reference/statements/alter/projection
title: 'Проекции'
doc_type: 'reference'
---

На этой странице объясняется, что такое проекции, как их использовать и какие возможности доступны для управления проекциями.

<div id="overview">
  ## Обзор проекций
</div>

Проекции хранят данные в формате, оптимизированном для выполнения запросов. Эта возможность полезна в следующих случаях:

* Выполнение запросов по столбцу, который не входит в первичный ключ
* Предварительная агрегация столбцов, что снижает как вычислительные затраты, так и IO

Для таблицы можно определить одну или несколько проекций, и во время анализа запроса ClickHouse выберет проекцию, для которой требуется просканировать меньше всего данных, не изменяя исходный запрос пользователя.

:::note[Использование диска]
Проекции внутренне создают новую скрытую таблицу, а это значит, что потребуется больше IO и места на диске.
Например, если для проекции задан другой первичный ключ, все данные исходной таблицы будут дублироваться.
:::

Более подробную техническую информацию о внутреннем устройстве проекций см. на этой [странице](/ru/guides/best-practices/sparse-primary-indexes.md/#option-3-projections).

<div id="examples">
  ## Использование проекций
</div>

<div id="example-filtering-without-using-primary-keys">
  ### Пример фильтрации без использования первичных ключей
</div>

Создадим таблицу:

```sql
CREATE TABLE visits_order
(
   `user_id` UInt64,
   `user_name` String,
   `pages_visited` Nullable(Float64),
   `user_agent` String
)
ENGINE = MergeTree()
PRIMARY KEY user_agent
```

С помощью `ALTER TABLE` можно добавить проекцию в уже существующую таблицу:

```sql
ALTER TABLE visits_order ADD PROJECTION user_name_projection (
    SELECT *
    ORDER BY user_name
)

ALTER TABLE visits_order MATERIALIZE PROJECTION user_name_projection
```

Вставка данных:

```sql
INSERT INTO visits_order SELECT
    number,
    'test',
    1.5 * (number / 2),
    'Android'
FROM numbers(1, 100);
```

Проекция позволит нам быстро фильтровать по `user_name`, даже если в исходной таблице `user_name` не был определён как `PRIMARY_KEY`.
Во время выполнения запроса ClickHouse определяет, что при использовании проекции будет обработано меньше данных, поскольку данные упорядочены по `user_name`.

```sql
SELECT
    *
FROM visits_order
WHERE user_name='test'
LIMIT 2
```

Чтобы проверить, использует ли запрос проекцию, можно просмотреть таблицу `system.query_log`. В поле `projections` указано имя использованной проекции, а если проекция не использовалась — поле будет пустым:

```sql
SELECT query, projections FROM system.query_log WHERE query_id='<query_id>'
```

<div id="example-pre-aggregation-query">
  ### Пример запроса с предварительной агрегацией
</div>

Создайте таблицу с проекцией `projection_visits_by_user`:

```sql
CREATE TABLE visits
(
   `user_id` UInt64,
   `user_name` String,
   `pages_visited` Nullable(Float64),
   `user_agent` String,
   PROJECTION projection_visits_by_user
   (
       SELECT
           user_agent,
           sum(pages_visited)
       GROUP BY user_id, user_agent
   )
)
ENGINE = MergeTree()
ORDER BY user_agent
```

Вставьте данные:

```sql
INSERT INTO visits SELECT
    number,
    'test',
    1.5 * (number / 2),
    'Android'
FROM numbers(1, 100);
```

```sql
INSERT INTO visits SELECT
    number,
    'test',
    1. * (number / 2),
   'IOS'
FROM numbers(100, 500);
```

Выполните первый запрос с `GROUP BY`, используя поле `user_agent`.
Этот запрос не будет использовать определённую проекцию, поскольку предварительная агрегация ей не соответствует.

```sql
SELECT
    user_agent,
    count(DISTINCT user_id)
FROM visits
GROUP BY user_agent
```

Чтобы использовать проекцию, вы можете выполнять запросы, которые выбирают часть полей предварительной агрегации и `GROUP BY` или все эти поля:

```sql
SELECT
    user_agent
FROM visits
WHERE user_id > 50 AND user_id < 150
GROUP BY user_agent
```

```sql
SELECT
    user_agent,
    sum(pages_visited)
FROM visits
GROUP BY user_agent
```

Как уже упоминалось, вы можете просмотреть таблицу `system.query_log`, чтобы понять, использовалась ли проекция.
Поле `projections` показывает имя использованной проекции.
Оно будет пустым, если проекция не использовалась:

```sql
SELECT query, projections FROM system.query_log WHERE query_id='<query_id>'
```

<div id="projection-indexes">
  ### Создание и использование проекционных индексов
</div>

Создание [проекционного индекса](../../../engines/table-engines/mergetree-family/mergetree.md#projection-index):

```sql
CREATE TABLE events
(
    `event_time` DateTime,
    `event_id` UInt64,
    `user_id` UInt64,
    `huge_string` String,
    PROJECTION order_by_user_id INDEX user_id TYPE basic
)
ENGINE = MergeTree()
ORDER BY (event_id);
```

<details markdown="1">
  <summary>Создание проекции с явным полем `_part_offset`</summary>

  Индексы проекций также можно создать с помощью следующего синтаксиса (не рекомендуется):

  ```sql
  CREATE TABLE events
  (
      `event_time` DateTime,
      `event_id` UInt64,
      `user_id` UInt64,
      `huge_string` String,
      PROJECTION order_by_user_id
      (
          SELECT
              _part_offset
          ORDER BY user_id
      )
  )
  ENGINE = MergeTree()
  ORDER BY (event_id);
  ```
</details>

Вставим несколько примеров данных:

```sql
INSERT INTO events SELECT * FROM generateRandom() LIMIT 100000;
```

Поле `_part_offset` сохраняет своё значение при слияниях и мутациях, что делает его полезным для вторичной индексации. Мы можем использовать это в запросах:

```sql
SELECT
    count()
FROM events
WHERE _part_starting_offset + _part_offset IN (
    SELECT _part_starting_offset + _part_offset
    FROM events
    WHERE user_id = 42
)
SETTINGS enable_shared_storage_snapshot_in_query = 1
```

<div id="example-projection-with-where">
  ### Пример проекции с предложением WHERE
</div>

Проекции могут включать предложение `WHERE`, чтобы хранить только подмножество строк. Это полезно, когда в запросах часто используется известный предикат: проекция материализует только подходящие строки, сокращая объем хранилища и повышая производительность запросов.

Создание таблицы и добавление отфильтрованной проекции:

```sql
CREATE TABLE events
(
    `event_type` String,
    `time` DateTime,
    `message` String
)
ENGINE = MergeTree()
ORDER BY time;

ALTER TABLE events ADD PROJECTION proj_pageview (
    SELECT event_type, time, message
    WHERE event_type = 'pageview'
    ORDER BY time
);

ALTER TABLE events MATERIALIZE PROJECTION proj_pageview;
```

Вставка данных:

```sql
INSERT INTO events VALUES
    ('pageview', '2024-01-01', 'homepage'),
    ('click', '2024-01-02', 'button'),
    ('pageview', '2024-01-03', 'about');
```

Когда условие `WHERE` в запросе **подразумевает** условие `WHERE` проекции (то есть каждое условие из фильтра проекции также присутствует в фильтре запроса), оптимизатор может автоматически использовать проекцию, если определит, что это целесообразно:

```sql
-- This query implies the projection's WHERE, so the projection may be used:
SELECT time, message FROM events WHERE event_type = 'pageview';

-- A stricter query also implies the projection's WHERE:
SELECT time, message FROM events WHERE event_type = 'pageview' AND time > '2024-01-01';

-- This query does NOT imply the projection, so the base table is scanned:
SELECT time, message FROM events WHERE event_type = 'click';
```

Проверка импликации носит консервативный характер — она использует точное сопоставление конъюнктов в канонической форме выражения. Из-за этого могут быть упущены некоторые допустимые возможности для оптимизации (например, импликации диапазонов), но к некорректным результатам это никогда не приведёт.

<div id="manipulating-projections">
  ## Управление проекциями
</div>

Доступны следующие операции с [проекциями](/ru/engines/table-engines/mergetree-family/mergetree.md/#projections):

<div id="add-projection">
  ### ADD PROJECTION
</div>

Используйте приведённый ниже оператор, чтобы добавить описание проекции в метаданные таблицы:

```sql
-- Normal projection (supports WHERE)
ALTER TABLE [db.]name [ON CLUSTER cluster] ADD PROJECTION [IF NOT EXISTS] name ( SELECT <COLUMN LIST EXPR> [WHERE <expr>] [ORDER BY] ) [WITH SETTINGS ( setting_name1 = setting_value1, setting_name2 = setting_value2, ...)]

-- Aggregate projection (supports WHERE)
ALTER TABLE [db.]name [ON CLUSTER cluster] ADD PROJECTION [IF NOT EXISTS] name ( SELECT <COLUMN LIST EXPR> [WHERE <expr>] [GROUP BY] ) [WITH SETTINGS ( setting_name1 = setting_value1, setting_name2 = setting_value2, ...)]
```

:::note
Когда проекция задаёт предложение `WHERE`, материализуются только строки, соответствующие предикату. Оптимизатор может использовать такую проекцию, если `WHERE` запроса логически влечёт `WHERE` проекции и эта проекция выгодна для плана запроса. Это относится как к обычным, так и к агрегатным проекциям.
:::

<div id="with-settings">
  #### Клауза `WITH SETTINGS`
</div>

`WITH SETTINGS` задаёт **настройки на уровне проекции**, которые определяют, как проекция хранит данные (например, `index_granularity` или `index_granularity_bytes`).
Они напрямую соответствуют **настройкам таблицы семейства MergeTree**, но применяются **только к этой проекции**.

Пример:

```sql
ALTER TABLE t
ADD PROJECTION p (
    SELECT x ORDER BY x
) WITH SETTINGS (
    index_granularity = 4096,
    index_granularity_bytes = 1048576
);
```

Настройки проекции переопределяют действующие для неё настройки таблицы с учетом правил валидации (например, недопустимые или несовместимые переопределения будут отклонены).

<div id="drop-projection">
  ### DROP PROJECTION
</div>

Используйте приведённый ниже оператор, чтобы удалить описание проекции из метаданных таблицы, а также удалить файлы проекции с диска.
Это реализовано как [мутация](/ru/sql-reference/statements/alter/index.md#mutations).

```sql
ALTER TABLE [db.]name [ON CLUSTER cluster] DROP PROJECTION [IF EXISTS] name
```

<div id="materialize-projection">
  ### MATERIALIZE PROJECTION
</div>

Выполните приведённый ниже оператор, чтобы перестроить проекцию `name` в партиции `partition_name`.
Это реализовано как [мутация](/ru/sql-reference/statements/alter/index.md#mutations).

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] MATERIALIZE PROJECTION [IF EXISTS] name [IN PARTITION partition_name]
```

<div id="clear-projection">
  ### CLEAR PROJECTION
</div>

Используйте приведённый ниже оператор, чтобы удалить файлы проекции с диска, не удаляя её описание.
Это реализовано как [мутация](/ru/sql-reference/statements/alter/index.md#mutations).

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] CLEAR PROJECTION [IF EXISTS] name [IN PARTITION partition_name]
```

Команды `ADD`, `DROP` и `CLEAR` считаются легковесными, поскольку они лишь изменяют метаданные или удаляют файлы.
Кроме того, они реплицируются и синхронизируют метаданные проекций через ClickHouse Keeper или ZooKeeper.

:::note
Управление проекциями поддерживается только для таблиц с движком [`*MergeTree`](/ru/engines/table-engines/mergetree-family/mergetree.md) (включая [реплицируемые](/ru/engines/table-engines/mergetree-family/replication.md) варианты).
:::

<div id="control-projections-merges">
  ### Управление поведением слияния проекций
</div>

Когда вы выполняете запрос, ClickHouse выбирает, читать ли данные из исходной таблицы или из одной из её проекций.
Решение о чтении из исходной таблицы или одной из её проекций принимается отдельно для каждой части таблицы.
Обычно ClickHouse стремится прочитать как можно меньше данных и использует несколько приёмов, чтобы определить, из какой части лучше читать, например сэмплирование первичного ключа части.
В некоторых случаях у частей исходной таблицы нет соответствующих частей проекции.
Это может происходить, например, потому что создание проекции для таблицы в SQL по умолчанию является „отложенным“ — оно влияет только на вновь вставленные данные, оставляя существующие части без изменений.

Поскольку одна из проекций уже содержит предварительно вычисленные агрегированные значения, ClickHouse старается читать из соответствующих частей проекции, чтобы избежать повторной агрегации во время выполнения запроса. Если у конкретной части нет соответствующей части проекции, запрос выполняется по исходной части.

Но что происходит, если строки в исходной таблице изменяются нетривиальным образом в результате нетривиальных фоновых операций слияния частей данных?
Например, предположим, что таблица хранится с использованием движка таблицы `ReplacingMergeTree`.
Если во время слияния в нескольких входных частях обнаруживается одна и та же строка, сохраняется только самая новая версия строки (из части, вставленной последней), а все более старые версии отбрасываются.

Аналогично, если таблица хранится с использованием движка таблицы `AggregatingMergeTree`, операция слияния может сворачивать одинаковые строки во входных частях (на основе значений первичного ключа) в одну строку для обновления промежуточных состояний агрегации.

До ClickHouse v24.8 части проекции либо незаметно рассинхронизировались с основными данными, либо некоторые операции, такие как обновления и удаления, вообще нельзя было выполнить, поскольку база данных автоматически генерировала исключение, если у таблицы были проекции.

Начиная с v24.8, новый параметр на уровне таблицы [`deduplicate_merge_projection_mode`](/ru/operations/settings/merge-tree-settings#deduplicate_merge_projection_mode) управляет поведением в случае, если вышеупомянутые нетривиальные фоновые операции слияния происходят в частях исходной таблицы.

Удаляющие мутации — ещё один пример операций слияния частей, в результате которых строки удаляются из частей исходной таблицы. Начиная с v24.7, у нас также есть параметр для управления поведением в отношении удаляющих мутаций, вызванных легковесными удалениями: [`lightweight_mutation_projection_mode`](/ru/operations/settings/merge-tree-settings#deduplicate_merge_projection_mode).

Ниже приведены возможные значения для `deduplicate_merge_projection_mode` и `lightweight_mutation_projection_mode`:

* `throw` (по умолчанию): Генерируется исключение, что предотвращает рассинхронизацию частей проекции.
* `drop`: Затронутые части таблицы проекции удаляются. Для затронутых частей проекции запросы будут выполняться по исходной части таблицы.
* `rebuild`: Затронутая часть проекции перестраивается, чтобы оставаться согласованной с данными в исходной части таблицы.

<div id="limitations">
  ## Ограничения
</div>

Невозможно использовать столбец `ALIAS` в предложении `ORDER BY` проекции. Например:

```sql
CREATE TABLE t
(
    id UInt64,
    a UInt32,
    ab_sum UInt64 ALIAS a + 1,
--highlight-next-line
    PROJECTION p (SELECT a ORDER BY ab_sum)
)
ENGINE = MergeTree ORDER BY id;
-- Fails with UNKNOWN_IDENTIFIER
```

Столбцы `ALIAS` физически не хранятся и вычисляются на лету во время выполнения запроса, поэтому они недоступны на этапе записи части проекции, когда вычисляется выражение сортировки.

Вместо этого используйте столбцы `MATERIALIZED` или вставьте выражение напрямую:

```sql
-- using MATERIALIZED column
CREATE TABLE t
(
    id UInt64,
    a UInt32,
    ab_sum UInt64 MATERIALIZED a + 1,
    PROJECTION p (SELECT a ORDER BY ab_sum)
)
ENGINE = MergeTree ORDER BY id;

-- using an inline expression
CREATE TABLE t
(
    id UInt64,
    a UInt32,
    PROJECTION p (SELECT a ORDER BY a + 1)
)
ENGINE = MergeTree ORDER BY id;
```

<div id="see-also">
  ## См. также
</div>

* [&quot;Управление проекциями во время слияний&quot; (запись в блоге)](https://clickhouse.com/blog/clickhouse-release-24-08#control-of-projections-during-merges)
* [&quot;Проекции&quot; (руководство)](/ru/data-modeling/projections#using-projections-to-speed-up-UK-price-paid)
* [&quot;Materialized views в сравнении с проекциями&quot;](https://clickhouse.com/docs/managing-data/materialized-views-versus-projections)