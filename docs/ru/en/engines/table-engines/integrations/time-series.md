---
description: 'Движок таблицы, хранящий временные ряды, то есть набор значений, связанных
  с временными метками и тегами (или метками).'
sidebar_label: 'TimeSeries'
sidebar_position: 60
slug: /engines/table-engines/special/time_series
title: 'Движок таблицы TimeSeries'
doc_type: 'справочник'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="timeseries-table-engine">
  # Движок таблицы TimeSeries
</div>

<ExperimentalBadge />

<CloudNotSupportedBadge />

Движок таблицы для хранения временных рядов, то есть набора значений, связанных с временными метками и тегами (или labels):

```sql
metric_name1[tag1=value1, tag2=value2, ...] = {timestamp1: value1, timestamp2: value2, ...}
metric_name2[...] = ...
```

:::info
Это экспериментальная возможность, которая в будущих выпусках может измениться с нарушением обратной совместимости.
Включите использование движка таблицы TimeSeries
с помощью настройки [allow&#95;experimental&#95;time&#95;series&#95;table](/ru/operations/settings/settings#allow_experimental_time_series_table).
Выполните команду `set allow_experimental_time_series_table = 1`.
:::

<div id="syntax">
  ## Синтаксис
</div>

```sql
CREATE TABLE name [(columns)] ENGINE=TimeSeries
[SETTINGS var1=value1, ...]
[SAMPLES db.samples_table_name | [SAMPLES INNER COLUMNS (...)] [SAMPLES INNER ENGINE engine(arguments)]]
[TAGS db.tags_table_name | [TAGS INNER COLUMNS (...)] [TAGS INNER ENGINE engine(arguments)]]
[METRICS db.metrics_table_name | [METRICS INNER COLUMNS (...)] [METRICS INNER ENGINE engine(arguments)]]
```

:::note
Ключевое слово `SAMPLES` имеет псевдоним `DATA`, сохранённый для обратной совместимости.
:::

<div id="usage">
  ## Использование
</div>

Проще начать, оставив всё по умолчанию (таблицу `TimeSeries` можно создать без указания списка столбцов):

```sql
CREATE TABLE my_table ENGINE=TimeSeries
```

Эту таблицу можно использовать со следующими протоколами (в конфигурации сервера должен быть назначен порт):

* [prometheus remote-write](/ru/interfaces/prometheus#remote-write)
* [prometheus remote-read](/ru/interfaces/prometheus#remote-read)

<div id="outer-columns">
  ### Внешние столбцы
</div>

Столбцы таблицы TimeSeries создаются автоматически. Это внешние столбцы: они не хранят данные, а только предоставляют интерфейс для SELECT/INSERT. Сами данные хранятся в [целевых таблицах](#target-tables). Ниже приведён список внешних столбцов:

| Name            | Type                                                | Description                                                                                                                                                                                                                        |
| --------------- | --------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `metric_name`   | `String`                                            | Имя метрики                                                                                                                                                                                                                        |
| `tags`          | `Map(String, String)`                               | Карта тегов (labels) для временного ряда                                                                                                                                                                                           |
| `time_series`   | `Array(Tuple(DateTime64(3), Float64))` по умолчанию | Массив пар (временная метка, значение) для временного ряда. Типы временной метки и скалярного элемента кортежа можно определить по объявлению samples `INNER COLUMNS` (см. [Указание внешних столбцов](#specifying-outer-columns)) |
| `metric_family` | `String`                                            | Имя семейства метрик (для метаданных метрик)                                                                                                                                                                                       |
| `type`          | `String`                                            | Тип метрики (например, &quot;counter&quot;, &quot;gauge&quot;)                                                                                                                                                                     |
| `unit`          | `String`                                            | Единица измерения метрики                                                                                                                                                                                                          |
| `help`          | `String`                                            | Описание метрики                                                                                                                                                                                                                   |

Пример:

```sql
INSERT INTO my_table (metric_name, tags, time_series) VALUES
    ('cpu_usage', {'job': 'node_exporter', 'instance': 'host1:9100'},
     [(toDateTime64('2024-01-01 00:00:00', 3), 0.5), (toDateTime64('2024-01-01 00:01:00', 3), 0.7)])
```

`metric_name` может быть пустым при вставке — это означает, что имя метрики задаётся в `tags`, в поле `__name__`, например:

```sql
INSERT INTO my_table (tags, time_series) VALUES
    ({'__name__': 'cpu_usage', 'job': 'test'},
     [(toDateTime64('2024-01-01 00:00:00', 3), 0.5)])
```

Чтобы вставить метаданные метрик, вставьте значения в столбцы `metric_family`, `type`, `unit` и `help`:

```sql
INSERT INTO my_table (metric_name, tags, time_series, metric_family, type, unit, help) VALUES
    ('http_requests_total', {'method': 'GET'}, [(now64(), 100.0)],
     'http_requests_total', 'counter', 'requests', 'Total HTTP requests')
```

<div id="specifying-outer-columns">
  ### Указание внешних столбцов
</div>

Внешний столбец `time_series` можно явно указать в операторе `CREATE TABLE`, чтобы переопределить его тип по умолчанию `Array(Tuple(DateTime64(3), Float64))`. ClickHouse извлекает из кортежа тип временной метки и скалярный тип и передаёт их во внутреннюю таблицу samples:

```sql
CREATE TABLE my_table (time_series Array(Tuple(UInt32, Float32))) ENGINE=TimeSeries
```

Это эквивалентно непосредственному объявлению типов столбцов временной метки и значения в предложении `INNER COLUMNS` таблицы samples:

```sql
CREATE TABLE my_table ENGINE=TimeSeries
SAMPLES INNER COLUMNS (timestamp UInt32, value Float32)
```

Если обе формы используются в одном и том же операторе `CREATE TABLE`, объявленные типы должны совпадать.

<div id="target-tables">
  ## Целевые таблицы
</div>

У таблицы `TimeSeries` нет собственных данных — всё хранится в её целевых таблицах.
Это похоже на то, как работает [materialized view](../../../sql-reference/statements/create/view#materialized-view),
с той разницей, что у materialized view одна целевая таблица,
тогда как у таблицы `TimeSeries` есть три целевые таблицы: [образцы](#samples-table), [теги](#tags-table) и [метрики](#metrics-table).

Целевые таблицы можно либо явно указать в запросе `CREATE TABLE`,
либо движок таблицы `TimeSeries` может автоматически создать внутренние целевые таблицы.

Строки, вставляемые в таблицу `TimeSeries`, преобразуются, разбиваются на блоки и записываются в эти три целевые таблицы.

Целевые таблицы следующие:

<div id="samples-table">
  ### Таблица *samples*
</div>

Таблица *samples* содержит временные ряды, связанные с некоторым идентификатором.

Таблица *samples* должна иметь следующие столбцы:

| Имя         | Обязателен? | Тип по умолчанию | Возможные типы          | Описание                                        |
| ----------- | ----------- | ---------------- | ----------------------- | ----------------------------------------------- |
| `id`        | [x]         | `UUID`           | любой                   | Идентифицирует комбинацию имени метрики и тегов |
| `timestamp` | [x]         | `DateTime64(3)`  | `DateTime64(X)`         | Момент времени                                  |
| `value`     | [x]         | `Float64`        | `Float32` или `Float64` | Значение, связанное с `timestamp`               |

<div id="tags-table">
  ### Таблица tags
</div>

Таблица *tags* содержит идентификаторы, вычисляемые для каждой комбинации имени Метрики и тегов.

Таблица *tags* должна содержать следующие столбцы:

| Name                 | Mandatory? | Default type                          | Possible types                                                                                                          | Description                                                                                                                                                                              |
| -------------------- | ---------- | ------------------------------------- | ----------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `id`                 | [x]        | `UUID`                                | any (must match the type of `id` in the [samples](#samples-table) table)                                                | `id` идентифицирует комбинацию имени Метрики и тегов. Выражение DEFAULT определяет, как вычисляется такой идентификатор                                                                  |
| `metric_name`        | [x]        | `LowCardinality(String)`              | `String` or `LowCardinality(String)`                                                                                    | Имя Метрики                                                                                                                                                                              |
| `<tag_value_column>` | [ ]        | `String`                              | `String` or `LowCardinality(String)` or `LowCardinality(Nullable(String))`                                              | Значение конкретного тега; имя тега и имя соответствующего столбца задаются в настройке [tags&#95;to&#95;columns](#settings)                                                             |
| `tags`               | [x]        | `Map(LowCardinality(String), String)` | `Map(String, String)` or `Map(LowCardinality(String), String)` or `Map(LowCardinality(String), LowCardinality(String))` | Map тегов, за исключением тега `__name__`, содержащего имя Метрики, а также тегов с именами, перечисленными в настройке [tags&#95;to&#95;columns](#settings)                             |
| `all_tags`           | [ ]        | `Map(String, String)`                 | `Map(String, String)` or `Map(LowCardinality(String), String)` or `Map(LowCardinality(String), LowCardinality(String))` | Эфемерный столбец; каждая строка представляет собой map всех тегов, за исключением только тега `__name__`, содержащего имя Метрики. Этот столбец используется только при вычислении `id` |
| `min_time`           | [ ]        | `Nullable(DateTime64(3))`             | `DateTime64(X)` or `Nullable(DateTime64(X))`                                                                            | Минимальная временная метка временного ряда с данным `id`. Столбец создаётся, если [store&#95;min&#95;time&#95;and&#95;max&#95;time](#settings) равно `true`                             |
| `max_time`           | [ ]        | `Nullable(DateTime64(3))`             | `DateTime64(X)` or `Nullable(DateTime64(X))`                                                                            | Максимальная временная метка временного ряда с данным `id`. Столбец создаётся, если [store&#95;min&#95;time&#95;and&#95;max&#95;time](#settings) равно `true`                            |

<div id="metrics-table">
  ### Таблица metrics
</div>

Таблица *metrics* содержит информацию о собираемых метриках, их типах и описаниях.

Таблица *metrics* должна иметь следующие столбцы:

| Имя                  | Обязательно? | Тип по умолчанию         | Возможные типы                        | Описание                                                                                                                                                   |
| -------------------- | ------------ | ------------------------ | ------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `metric_family_name` | [x]          | `String`                 | `String` или `LowCardinality(String)` | Имя семейства метрик                                                                                                                                       |
| `type`               | [x]          | `LowCardinality(String)` | `String` или `LowCardinality(String)` | Тип семейства метрик: &quot;counter&quot;, &quot;gauge&quot;, &quot;summary&quot;, &quot;stateset&quot;, &quot;histogram&quot;, &quot;gaugehistogram&quot; |
| `unit`               | [x]          | `LowCardinality(String)` | `String` или `LowCardinality(String)` | Единица измерения метрики                                                                                                                                  |
| `help`               | [x]          | `String`                 | `String` или `LowCardinality(String)` | Описание метрики                                                                                                                                           |

<div id="creation">
  ## Создание
</div>

Таблицу с движком таблицы `TimeSeries` можно создать несколькими способами.
Самый простой оператор

```sql
CREATE TABLE my_table ENGINE=TimeSeries
```

на самом деле создаст следующую таблицу (это можно увидеть, выполнив `SHOW CREATE TABLE my_table`):

```sql
CREATE TABLE my_table
(
    `metric_name` String,
    `tags` Map(String, String),
    `time_series` Array(Tuple(DateTime64(3), Float64)),
    `metric_family` String,
    `type` String,
    `unit` String,
    `help` String
)
ENGINE = TimeSeries
SAMPLES INNER COLUMNS
(
    `id` UUID,
    `timestamp` DateTime64(3),
    `value` Float64
)
SAMPLES INNER ENGINE = MergeTree ORDER BY (id, timestamp)
TAGS INNER COLUMNS
(
    `id` UUID DEFAULT reinterpretAsUUID(sipHash128(metric_name, all_tags)),
    `metric_name` LowCardinality(String),
    `tags` Map(LowCardinality(String), String),
    `all_tags` Map(String, String) EPHEMERAL,
    `min_time` SimpleAggregateFunction(min, Nullable(DateTime64(3))),
    `max_time` SimpleAggregateFunction(max, Nullable(DateTime64(3)))
)
TAGS INNER ENGINE = AggregatingMergeTree PRIMARY KEY metric_name ORDER BY (metric_name, id) SETTINGS allow_dimensions_outside_sorting_key = 1
METRICS INNER COLUMNS
(
    `metric_family_name` String,
    `type` LowCardinality(String),
    `unit` LowCardinality(String),
    `help` String
)
METRICS INNER ENGINE = ReplacingMergeTree ORDER BY metric_family_name
```

Итак, столбцы были созданы автоматически, и также есть три внутренние целевые таблицы с собственными определениями столбцов,
которые хранятся в предложениях `INNER COLUMNS`.

Внутренние целевые таблицы имеют имена вида `.inner_id.samples.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`,
`.inner_id.tags.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`, `.inner_id.metrics.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`,
и у каждой целевой таблицы есть собственный набор столбцов:

```sql
CREATE TABLE default.`.inner_id.samples.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
(
    `id` UUID,
    `timestamp` DateTime64(3),
    `value` Float64
)
ENGINE = MergeTree
ORDER BY (id, timestamp)
```

```sql
CREATE TABLE default.`.inner_id.tags.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
(
    `id` UUID DEFAULT reinterpretAsUUID(sipHash128(metric_name, all_tags)),
    `metric_name` LowCardinality(String),
    `tags` Map(LowCardinality(String), String),
    `all_tags` Map(String, String) EPHEMERAL,
    `min_time` SimpleAggregateFunction(min, Nullable(DateTime64(3))),
    `max_time` SimpleAggregateFunction(max, Nullable(DateTime64(3)))
)
ENGINE = AggregatingMergeTree
PRIMARY KEY metric_name
ORDER BY (metric_name, id)
SETTINGS allow_dimensions_outside_sorting_key = 1
```

```sql
CREATE TABLE default.`.inner_id.metrics.xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`
(
    `metric_family_name` String,
    `type` LowCardinality(String),
    `unit` LowCardinality(String),
    `help` String
)
ENGINE = ReplacingMergeTree
ORDER BY metric_family_name
```

<div id="create-as">
  ## Создание таблицы AS на основе существующей таблицы
</div>

Оператор `CREATE TABLE new_table AS existing_table` копирует из `existing_table`:

* `SETTINGS`
* `INNER COLUMNS` для каждого типа
* `INNER ENGINE` для каждого типа

Этот оператор недопустим, если у `existing_table` есть внешние цели.
Список внешних столбцов создаётся заново, а не копируется.

<div id="adjusting-column-types">
  ## Настройка типов столбцов
</div>

Вы можете настраивать типы столбцов во внутренних целевых таблицах с помощью предложения `INNER COLUMNS`. Например, чтобы хранить временные метки в микросекундах, а значения — в формате `Float32`:

```sql
CREATE TABLE my_table ENGINE=TimeSeries
SAMPLES INNER COLUMNS (timestamp DateTime64(6), value Float32)
```

То же условие можно использовать для указания кодеков и других атрибутов столбца:

```sql
CREATE TABLE my_table ENGINE=TimeSeries
SAMPLES INNER COLUMNS (timestamp DateTime64(3) CODEC(DoubleDelta))
```

<div id="id-column">
  ## Столбец `id`
</div>

Столбец `id` содержит идентификаторы; каждый идентификатор вычисляется для комбинации имени метрики и тегов.
Тип и выражение `DEFAULT`, используемое для генерации идентификаторов, можно настроить с помощью предложения `TAGS INNER COLUMNS`:

```sql
CREATE TABLE my_table ENGINE=TimeSeries
TAGS INNER COLUMNS (id UInt64 DEFAULT sipHash64(metric_name, all_tags))
```

Тип столбца `id` должен быть одним из следующих: `UUID`, `UInt64`, `UInt128` или `FixedString(16)`. Если выражение `DEFAULT` не задано, ClickHouse автоматически выберет его на основе типа `id`. Типы `id`, объявленные во внутренних таблицах samples и tags, должны совпадать.

Параметр `id_generator` позволяет выполнить ту же настройку без использования предложения `INNER COLUMNS`:

```sql
CREATE TABLE my_table ENGINE=TimeSeries
SETTINGS id_generator = 'sipHash64(metric_name, all_tags)'
```

Если этот параметр задан, он используется для генерации `id`, даже если в `DEFAULT` для столбца указано другое выражение.

<div id="tags-and-all-tags">
  ## Столбцы `tags` и `all_tags`
</div>

Есть два столбца, содержащих карты тегов, — `tags` и `all_tags`. В этом примере они означают одно и то же, однако могут различаться,
если используется настройка `tags_to_columns`. Эта настройка позволяет указать, что конкретный тег должен храниться в отдельном столбце, а не
в карте внутри столбца `tags`:

```sql
CREATE TABLE my_table
ENGINE = TimeSeries 
SETTINGS tags_to_columns = {'instance': 'instance', 'job': 'job'}
```

Этот оператор добавит столбцы `instance` и `job` во внутреннюю целевую таблицу [tags](#tags-table).
В этом случае столбец `tags` не будет содержать теги `instance` и `job`,
а столбец `all_tags` будет их содержать. Столбец `all_tags` является эфемерным, и его единственное назначение — использоваться в выражении DEFAULT
для столбца `id`.

<div id="inner-table-engines">
  ## Движки таблиц внутренних целевых таблиц
</div>

По умолчанию внутренние целевые таблицы используют следующие движки таблиц:

* таблица [samples](#samples-table) использует [MergeTree](../mergetree-family/mergetree);
* таблица [tags](#tags-table) использует [AggregatingMergeTree](../mergetree-family/aggregatingmergetree), поскольку в эту таблицу одни и те же данные часто вставляются по нескольку раз, поэтому нужен способ
  удалять дубликаты, а также потому, что для столбцов `min_time` и `max_time` требуется выполнять агрегацию;
* таблица [metrics](#metrics-table) использует [ReplacingMergeTree](../mergetree-family/replacingmergetree), поскольку в эту таблицу одни и те же данные часто вставляются по нескольку раз, поэтому нужен способ
  удалять дубликаты.

Для внутренних целевых таблиц также можно использовать другие движки таблиц, если это указано:

```sql
CREATE TABLE my_table ENGINE=TimeSeries
SAMPLES ENGINE=ReplicatedMergeTree
TAGS ENGINE=ReplicatedAggregatingMergeTree
METRICS ENGINE=ReplicatedReplacingMergeTree
```

Таблица [tags](#tags-table) хранит столбцы тегов (и Map&#39;ы `tags`/`all_tags`) вне своего ключа сортировки,
что `AggregatingMergeTree` по умолчанию не допускает (см. [`allow_dimensions_outside_sorting_key`](../mergetree-family/aggregatingmergetree)).
Здесь это безопасно, потому что эти столбцы функционально зависят от `id`, который входит в ключ сортировки, поэтому все
строки, которые фоновое слияние объединяет, имеют одинаковые значения. Когда внутренняя таблица tags создаётся автоматически или её
движок указывается inline, как показано выше, `TimeSeries` автоматически устанавливает для неё `allow_dimensions_outside_sorting_key = 1`;
для созданной вручную [внешней](#external-target-tables) агрегирующей таблицы tags это нужно задать самостоятельно.

<div id="external-target-tables">
  ## Внешние целевые таблицы
</div>

Можно настроить таблицу `TimeSeries` на использование таблицы, созданной вручную:

```sql
CREATE TABLE samples_for_my_table
(
    `id` UUID,
    `timestamp` DateTime64(3),
    `value` Float64
)
ENGINE = MergeTree
ORDER BY (id, timestamp);

CREATE TABLE tags_for_my_table ...

CREATE TABLE metrics_for_my_table ...

CREATE TABLE my_table ENGINE=TimeSeries SAMPLES samples_for_my_table TAGS tags_for_my_table METRICS metrics_for_my_table;
```

Типы столбцов внешних таблиц (`id`, `timestamp`, `value` и `<tag_value_column>`, перечисленные в [`tags_to_columns`](#settings)) должны соответствовать тем, которые таблица `TimeSeries` иначе генерировала бы сама (ограничения типов см. в [Таблица samples](#samples-table), [Таблица tags](#tags-table) и [Таблица metrics](#metrics-table)). О несоответствии типов сообщается во время `CREATE`.

Выражение генератора `id` для внешней цели tags вычисляется во время `INSERT` в следующем порядке: сначала настройка [`id_generator`](#settings) (если задана), затем `DEFAULT`, объявленный для столбца `id` внешней таблицы (если он есть), затем канонический генератор, выведенный из типа `id`. Таким образом, эта настройка имеет приоритет над любым `DEFAULT`, объявленным для внешней таблицы — подробности см. в разделе [Столбец `id`](#id-column).

<div id="altering-settings">
  ## Изменение настроек
</div>

После `CREATE` можно изменить две настройки:

* `id_generator`
* `filter_by_min_time_and_max_time`

```sql
ALTER TABLE my_table MODIFY SETTING id_generator = 'sipHash64(metric_name, all_tags)';
ALTER TABLE my_table MODIFY SETTING filter_by_min_time_and_max_time = 0;
```

Обратите внимание: если изменить `id_generator`, когда данные уже есть в таблице тегов, для одной и той же комбинации метрики и тега могут создаваться разные идентификаторы — старые строки сохраняют прежние идентификаторы, а новые используют новый генератор.

Остальные настройки нельзя изменить с помощью `ALTER ... MODIFY SETTING`, потому что они зашиты в схему внутренних таблиц в момент `CREATE`.

<div id="settings">
  ## Настройки
</div>

Ниже приведён список настроек, которые можно указать при определении таблицы `TimeSeries`:

| Name                                 | Type       | Default              | Description                                                                                                                                                                                                                                             |
| ------------------------------------ | ---------- | -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `id_generator`                       | Expression | зависит от типа `id` | Выражение, вычисляющее идентификатор (fingerprint) временного ряда по его тегам. Если оно не задано, используется выражение по умолчанию для столбца `id`. Если и выражение по умолчанию для столбца `id` не задано, выражение выбирается автоматически |
| `tags_to_columns`                    | Map        | {}                   | Map, указывающий, какие теги нужно поместить в отдельные столбцы таблицы [tags](#tags-table). Синтаксис: `{'tag1': 'column1', 'tag2' : column2, ...}`                                                                                                   |
| `use_all_tags_column_to_generate_id` | Bool       | true                 | При генерации выражения для вычисления идентификатора временного ряда этот флаг включает использование столбца `all_tags`                                                                                                                               |
| `store_min_time_and_max_time`        | Bool       | true                 | Если установлено значение true, таблица будет хранить `min_time` и `max_time` для каждого временного ряда                                                                                                                                               |
| `aggregate_min_time_and_max_time`    | Bool       | true                 | При создании внутренней целевой таблицы `tags` этот флаг включает использование `SimpleAggregateFunction(min, Nullable(DateTime64(3)))` вместо `Nullable(DateTime64(3))` в качестве типа столбца `min_time`, и аналогично для столбца `max_time`        |
| `filter_by_min_time_and_max_time`    | Bool       | true                 | Если установлено значение true, таблица будет использовать столбцы `min_time` и `max_time` для фильтрации временных рядов                                                                                                                               |

<div id="functions">
  # Функции
</div>

Ниже приведён список функций, принимающих таблицу `TimeSeries` в качестве аргумента:

* [timeSeriesSamples](../../../sql-reference/table-functions/timeSeriesSamples.md)
* [timeSeriesTags](../../../sql-reference/table-functions/timeSeriesTags.md)
* [timeSeriesMetrics](../../../sql-reference/table-functions/timeSeriesMetrics.md)