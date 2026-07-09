---
description: 'Этот движок обеспечивает интеграцию в режиме только для чтения с существующими
  таблицами Apache Paimon в Amazon S3, Azure, HDFS и с таблицами, хранящимися локально.'
sidebar_label: 'Paimon'
sidebar_position: 95
slug: /engines/table-engines/integrations/paimon
title: 'Движок таблицы Paimon'
doc_type: 'справочник'
---

Этот движок обеспечивает интеграцию в режиме только для чтения с существующими таблицами Apache [Paimon](https://paimon.apache.org/) в Amazon S3, Azure, HDFS и с таблицами, хранящимися локально.
Поддерживаются чтение снимка, инкрементальное чтение и базовое отсечение партиций на уровне движка.

<div id="create-table">
  ## Создание таблицы
</div>

Обратите внимание: таблица Paimon уже должна существовать в хранилище; эта команда не принимает DDL-параметры для создания новой таблицы.
Создание таблиц `Paimon*` требует включения `allow_experimental_paimon_storage_engine` (по умолчанию он отключён), поэтому перед выполнением `CREATE TABLE` включите его.

```sql
SET allow_experimental_paimon_storage_engine = 1;

CREATE TABLE paimon_table_s3
    ENGINE = PaimonS3(url,  [, access_key_id, secret_access_key] [,format] [,compression])

CREATE TABLE paimon_table_azure
    ENGINE = PaimonAzure(connection_string|storage_account_url, container_name, blobpath, [,account_name], [,account_key] [,format] [,compression_method])

CREATE TABLE paimon_table_hdfs
    ENGINE = PaimonHDFS(path_to_table, [,format] [,compression_method])

CREATE TABLE paimon_table_local
    ENGINE = PaimonLocal(path_to_table, [,format] [,compression_method])
```

<div id="engine-arguments">
  ## Аргументы движка
</div>

Описание аргументов совпадает с описанием аргументов для движков `S3`, `AzureBlobStorage`, `HDFS` и `File` соответственно.
`format` обозначает формат файлов данных в таблице Paimon.

Параметры движка можно задавать с помощью [именованных коллекций](../../../operations/named-collections.md)

<div id="example">
  ### Пример
</div>

```sql
CREATE TABLE paimon_table ENGINE=PaimonS3('http://test.s3.amazonaws.com/clickhouse-bucket/test_table', 'test', 'test')
```

Использование именованных коллекций:

```xml
<clickhouse>
    <named_collections>
        <paimon_conf>
            <url>http://test.s3.amazonaws.com/clickhouse-bucket/</url>
            <access_key_id>test</access_key_id>
            <secret_access_key>test</secret_access_key>
        </paimon_conf>
    </named_collections>
</clickhouse>
```

```sql
CREATE TABLE paimon_table ENGINE=PaimonS3(paimon_conf, filename = 'test_table')
```

<div id="capabilities">
  ## Возможности
</div>

* Чтение выполняется из последнего снимка таблицы.
* При включении поддерживается инкрементальное чтение на основе идентификатора зафиксированного снимка.
* Отсечение партиций при включенном `use_paimon_partition_pruning`.
* Опциональное фоновое обновление метаданных при соответствующей настройке.
* Стабильный UUID таблицы при использовании баз данных Atomic/Replicated, что позволяет использовать макросы `{uuid}` в путях Keeper.

<div id="settings">
  ## Настройки
</div>

Этот движок использует те же настройки, что и соответствующие движки Объектного хранилища, а также добавляет настройки, специфичные для Paimon:

* `allow_experimental_paimon_storage_engine` — включает создание движков таблиц `Paimon`, `PaimonS3`, `PaimonAzure`, `PaimonHDFS` и `PaimonLocal`. По умолчанию: `0` (отключено).
* `paimon_incremental_read` — включает режим инкрементального чтения.
* `paimon_metadata_refresh_interval_sec` — интервал фонового обновления метаданных в секундах. Если задано значение больше 0, фоновая задача периодически загружает из Объектного хранилища актуальные снимок и схему. По умолчанию: 30.
* `paimon_keeper_path` — путь Keeper для состояния инкрементального чтения. Должен быть задан и уникален для каждой таблицы; поддерживает макросы, такие как `{database}`, `{table}`, `{uuid}`.
* `paimon_replica_name` — имя реплики для состояния инкрементального чтения. Должно быть задано и уникально для каждой реплики; поддерживает макросы, такие как `{replica}`.

<div id="incremental-read-examples">
  ## Примеры инкрементального чтения
</div>

Инкрементальное чтение с состоянием в Keeper:

```sql
CREATE TABLE paimon_inc
ENGINE = PaimonS3(paimon_conf, filename = 'paimon_all_types')
SETTINGS
    paimon_incremental_read = 1,
    paimon_keeper_path = '/clickhouse/{database}/{uuid}',
    paimon_replica_name = '{replica}';
```

<div id="query-level-settings-for-incremental-read">
  ### Настройки уровня запроса для инкрементального чтения
</div>

Следующие настройки относятся к **уровню запроса** (передаются через `SELECT ... SETTINGS`, а не в `CREATE TABLE`). Они управляют поведением инкрементального чтения для каждого запроса:

* `paimon_target_snapshot_id` — читать только дельту указанного снимка. Зафиксированная водяная метка в Keeper **не** продвигается, поэтому один и тот же снимок можно перечитывать любое количество раз. Значение по умолчанию: `-1` (отключено).
* `max_consume_snapshots` — максимальное число снимков, обрабатываемых за одно инкрементальное чтение. Если в источнике накопилось много непрочитанных снимков, этот параметр ограничивает, сколько из них обрабатывается за один запрос, чтобы контролировать размер батча. `0` означает отсутствие ограничения. Значение по умолчанию: `0`.

**Чтение целевого снимка** — всегда возвращает дельту снимка 1, независимо от текущей водяной метки:

```sql
SELECT count()
FROM paimon_inc
SETTINGS paimon_target_snapshot_id = 1;
```

**Ограничение числа снимков в батче** — если в очереди три новых снимка, обрабатывайте не более двух за один запрос:

```sql
SELECT count()
FROM paimon_inc
SETTINGS max_consume_snapshots = 2;
```

<div id="paimon-to-mergetree-via-refresh-mv">
  ## Из Paimon в MergeTree через refreshable materialized view
</div>

Вы можете построить сквозной конвейер, который непрерывно синхронизирует данные из таблицы Paimon в таблицу семейства MergeTree с помощью refreshable materialized view в режиме `APPEND`. В каждом цикле обновления из Paimon считываются только новые инкрементальные данные, которые затем добавляются в целевую таблицу.

**Шаг 1 — Создайте исходную таблицу Paimon с включенными инкрементальным чтением и обновлением метаданных.**

В примере ниже используется `PaimonLocal`. Замените движок на `PaimonS3`, `PaimonAzure`, `PaimonHDFS` или автоматически определяющий тип хранилища движок `Paimon` — в зависимости от используемого бэкенда хранилища:

```sql
SET allow_experimental_paimon_storage_engine = 1;

-- Local storage
CREATE TABLE paimon_mv_source
ENGINE = PaimonLocal('/path/to/paimon/table')
SETTINGS
    paimon_incremental_read = 1,
    paimon_keeper_path = '/clickhouse/tables/{uuid}',
    paimon_replica_name = '{replica}',
    paimon_metadata_refresh_interval_sec = 1;

-- S3 storage (the `Paimon` engine defaults to the S3 implementation when no `disk` is specified)
CREATE TABLE paimon_mv_source
ENGINE = Paimon('http://minio:9000/bucket/path/to/table', 'access_key', 'secret_key')
SETTINGS
    paimon_incremental_read = 1,
    paimon_keeper_path = '/clickhouse/tables/{uuid}',
    paimon_replica_name = '{replica}',
    paimon_metadata_refresh_interval_sec = 1;
```

`paimon_metadata_refresh_interval_sec` задаёт интервал фонового обновления метаданных в секундах. Если значение больше 0, фоновая задача периодически подтягивает из Объектного хранилища последний снимок и актуальную схему, чтобы цикл обновления MV мог видеть новые данные после коммита, не дожидаясь, пока запрос запустит обновление метаданных. Значение по умолчанию — 30. Используйте с осторожностью при работе с большим количеством таблиц, чтобы избежать чрезмерной нагрузки на Объектное хранилище и избыточных I/O-операций в Keeper.

**Шаг 2 — Создайте целевую таблицу MergeTree (схема клонирована из таблицы Paimon):**

```sql
CREATE TABLE paimon_mv_dest AS paimon_mv_source
ENGINE = MergeTree()
ORDER BY tuple();
```

**Шаг 3 — Создайте refreshable materialized view:**

```sql
CREATE MATERIALIZED VIEW paimon_mv
REFRESH EVERY 10 SECOND
APPEND
TO paimon_mv_dest
AS SELECT * FROM paimon_mv_source;
```

Каждые 10 секунд MV выполняет `SELECT * FROM paimon_mv_source`: запрос возвращает только строки, добавленные после последнего закоммиченного снимка, и дописывает их в `paimon_mv_dest`.

**Очистка:**

```sql
SYSTEM STOP VIEW paimon_mv;
DROP VIEW IF EXISTS paimon_mv SYNC;
DROP TABLE IF EXISTS paimon_mv_dest SYNC;
DROP TABLE IF EXISTS paimon_mv_source SYNC;
```

:::note
Остановите MV перед удалением, чтобы фоновое обновление не блокировало DDL-операции.
:::

<div id="limitations">
  ## Ограничения
</div>

* Для инкрементального чтения требуется настроенный Keeper (ZooKeeper).
* Для инкрементального чтения параметр `paimon_keeper_path` должен быть задан и быть уникальным для каждой таблицы.
* `paimon_replica_name` должен быть уникальным для каждой реплики в пределах одного и того же пути Keeper.
* Инкрементальное чтение использует доставку не более одного раза: коммит снимка продвигается при сборе файлов данных, до того как данные будут фактически обработаны. Если запрос завершается с ошибкой после сбора файлов, пропущенные снимки не будут повторно прочитаны при повторной попытке.
* Движок таблицы работает в режиме только для чтения; изменение данных не поддерживается.
* Инкрементальное чтение не обрабатывает удаление исторических данных из источника Paimon. Если исходные данные Paimon удаляются или обновляются, соответствующие строки, уже записанные в целевую таблицу ClickHouse семейства MergeTree, не будут автоматически удалены. Чтобы очистить устаревшие данные, необходимо вручную выполнить `ALTER TABLE ... DELETE` для таблицы семейства MergeTree.

<div id="aliases">
  ## Псевдонимы
</div>

Движок таблицы `Paimon` автоматически определяет backend хранилища по настройке `disk` и в зависимости от этого использует `PaimonS3`, `PaimonAzure` или `PaimonLocal`. Если `disk` не указан, по умолчанию используется реализация `PaimonS3`.

<div id="virtual-columns">
  ## Виртуальные столбцы
</div>

* `_path` — Путь к файлу. Тип: `LowCardinality(String)`.
* `_file` — Имя файла. Тип: `LowCardinality(String)`.
* `_size` — Размер файла в байтах. Тип: `Nullable(UInt64)`. Если размер файла неизвестен, значение — `NULL`.
* `_time` — Время последнего изменения файла. Тип: `Nullable(DateTime)`. Если время неизвестно, значение — `NULL`.
* `_etag` — etag файла. Тип: `LowCardinality(String)`. Если etag неизвестен, значение — `NULL`.

<div id="data-types-supported">
  ## Поддерживаемые типы данных
</div>

| Тип данных Paimon                 | Тип данных ClickHouse     |
| --------------------------------- | ------------------------- |
| BOOLEAN                           | Int8                      |
| TINYINT                           | Int8                      |
| SMALLINT                          | Int16                     |
| INTEGER                           | Int32                     |
| BIGINT                            | Int64                     |
| FLOAT                             | Float32                   |
| DOUBLE                            | Float64                   |
| STRING,VARCHAR,BYTES,VARBINARY    | String                    |
| DATE                              | Date                      |
| TIME(p),TIME                      | Time(&#39;UTC&#39;)       |
| TIMESTAMP(p) WITH LOCAL TIME ZONE | DateTime64                |
| TIMESTAMP(p)                      | DateTime64(&#39;UTC&#39;) |
| CHAR                              | FixedString(1)            |
| BINARY(n)                         | FixedString(n)            |
| DECIMAL(P,S)                      | Decimal(P,S)              |
| ARRAY                             | Array                     |
| MAP                               | Map                       |

<div id="partition-supported">
  ## Поддерживаемые партиции
</div>

Типы данных, поддерживаемые в ключах партиционирования Paimon:

* `CHAR`
* `VARCHAR`
* `BOOLEAN`
* `DECIMAL`
* `TINYINT`
* `SMALLINT`
* `INTEGER`
* `DATE`
* `TIME`
* `TIMESTAMP`
* `TIMESTAMP WITH LOCAL TIME ZONE`
* `BIGINT`
* `FLOAT`
* `DOUBLE`