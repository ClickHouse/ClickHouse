---
description: 'Предоставляет табличный интерфейс только для чтения для таблиц Apache Iceberg,
  хранящихся в Amazon S3, Azure, HDFS или локально.'
sidebar_label: 'iceberg'
sidebar_position: 90
slug: /sql-reference/table-functions/iceberg
title: 'iceberg'
doc_type: 'reference'
---

Предоставляет табличный интерфейс только для чтения для таблиц Apache [Iceberg](https://iceberg.apache.org/), хранящихся в Amazon S3, Azure, HDFS или локально.

<div id="syntax">
  ## Синтаксис
</div>

```sql
icebergS3(url [, NOSIGN | access_key_id, secret_access_key, [session_token]] [,format] [,compression_method] [,extra_credentials])
icebergS3(named_collection[, option=value [,..]])

icebergAzure(connection_string|storage_account_url, container_name, blobpath, [,account_name], [,account_key] [,format] [,compression_method])
icebergAzure(named_collection[, option=value [,..]])

icebergHDFS(path_to_table, [,format] [,compression_method])
icebergHDFS(named_collection[, option=value [,..]])

icebergLocal(path_to_table, [,format] [,compression_method])
icebergLocal(named_collection[, option=value [,..]])
```

<div id="arguments">
  ## Аргументы
</div>

Описание аргументов совпадает с описанием аргументов для табличных функций `s3`, `azureBlobStorage`, `HDFS` и `file` соответственно.
`format` обозначает формат файлов данных в таблице Iceberg.

Для `icebergS3` можно использовать необязательный параметр `extra_credentials`, чтобы передать `role_arn` для ролевого доступа в ClickHouse Cloud. Инструкции по настройке см. в разделе [Secure S3](/ru/cloud/data-sources/secure-s3).

<div id="returned-value">
  ### Возвращаемое значение
</div>

Таблица с указанной структурой для чтения данных из указанной таблицы Iceberg.

<div id="example">
  ### Пример
</div>

```sql
SELECT * FROM icebergS3('http://test.s3.amazonaws.com/clickhouse-bucket/test_table', 'test', 'test')
```

:::important
В настоящее время ClickHouse поддерживает чтение формата Iceberg версий v1 и v2 с помощью табличных функций `icebergS3`, `icebergAzure`, `icebergHDFS` и `icebergLocal`, а также движков таблиц `IcebergS3`, `icebergAzure`, `IcebergHDFS` и `IcebergLocal`.
:::

<div id="defining-a-named-collection">
  ## Определение именованной коллекции
</div>

Вот пример настройки именованной коллекции для хранения URL и учетных данных:

```xml
<clickhouse>
    <named_collections>
        <iceberg_conf>
            <url>http://test.s3.amazonaws.com/clickhouse-bucket/</url>
            <access_key_id>test</access_key_id>
            <secret_access_key>test</secret_access_key>
            <format>auto</format>
            <structure>auto</structure>
        </iceberg_conf>
    </named_collections>
</clickhouse>
```

```sql
SELECT * FROM icebergS3(iceberg_conf, filename = 'test_table')
DESCRIBE icebergS3(iceberg_conf, filename = 'test_table')
```

<div id="iceberg-writes-catalogs">
  ## Использование каталога данных
</div>

Таблицы Iceberg также можно использовать с различными каталогами данных, такими как [REST Catalog](https://iceberg.apache.org/rest-catalog-spec/), [AWS Glue Data Catalog](https://docs.aws.amazon.com/prescriptive-guidance/latest/serverless-etl-aws-glue/aws-glue-data-catalog.html) и [Unity Catalog](https://www.unitycatalog.io/).

:::important
При использовании каталога большинству пользователей стоит использовать движок базы данных `DataLakeCatalog`, который подключает ClickHouse к вашему каталогу и позволяет находить таблицы. Этот движок базы данных можно использовать вместо ручного создания отдельных таблиц с помощью движка таблицы `IcebergS3`.
:::

Чтобы использовать каталоги данных, создайте таблицу с движком `IcebergS3` и укажите необходимые настройки.

Например, REST Catalog с хранилищем MinIO:

```sql
CREATE TABLE `database_name.table_name`
ENGINE = IcebergS3(
  'http://minio:9000/warehouse-rest/table_name/',
  'minio_access_key',
  'minio_secret_key'
)
```

Или при использовании AWS Glue Data Catalog с S3:

```sql
CREATE TABLE `my_database.my_table`  
ENGINE = IcebergS3(
  's3://my-data-bucket/warehouse/my_database/my_table/',
  'aws_access_key',
  'aws_secret_key'
)
```

<div id="schema-evolution">
  ## Изменение схемы
</div>

На данный момент в CH можно читать таблицы Iceberg, схема которых со временем изменилась. Сейчас поддерживается чтение таблиц, в которых столбцы добавлялись, удалялись и меняли порядок. Также можно изменить столбец с обязательным значением на столбец, в котором допускается NULL. Кроме того, поддерживается допустимое приведение для простых типов, а именно:  

* int -&gt; long
* float -&gt; double
* decimal(P, S) -&gt; decimal(P&#39;, S) where P&#39; &gt; P.

В настоящее время нельзя изменять вложенные структуры или типы элементов в массивах и Map.

<div id="partition-pruning">
  ## Отсечение партиций
</div>

ClickHouse поддерживает отсечение партиций при выполнении SELECT-запросов к таблицам Iceberg, что помогает повысить производительность запросов за счет пропуска нерелевантных файлов данных. Чтобы включить отсечение партиций, установите `use_iceberg_partition_pruning = 1`. Дополнительные сведения об отсечении партиций в Iceberg см. по адресу https://iceberg.apache.org/spec/#partitioning

<div id="time-travel">
  ## Time travel
</div>

ClickHouse поддерживает time travel для таблиц Iceberg, позволяя запрашивать исторические данные по определённой временной метке или идентификатору снимка.

<div id="deleted-rows">
  ## Обработка таблиц с удалёнными строками
</div>

В настоящее время поддерживаются только таблицы Iceberg с [позиционными удалениями](https://iceberg.apache.org/spec/#position-delete-files).

Следующие методы удаления **не поддерживаются**:

* [Удаления по совпадению значений](https://iceberg.apache.org/spec/#equality-delete-files)
* [Векторы удаления](https://iceberg.apache.org/spec/#deletion-vectors) (добавлены в v3)

<div id="basic-usage">
  ### Базовое использование
</div>

```sql
 SELECT * FROM example_table ORDER BY 1 
 SETTINGS iceberg_timestamp_ms = 1714636800000
```

```sql
 SELECT * FROM example_table ORDER BY 1 
 SETTINGS iceberg_snapshot_id = 3547395809148285433
```

Примечание: нельзя одновременно указывать параметры `iceberg_timestamp_ms` и `iceberg_snapshot_id` в одном запросе.

<div id="important-considerations">
  ### Важные замечания
</div>

* **Снимки** обычно создаются, когда:

* В таблицу записываются новые данные

* Выполняется тот или иной вид компактизации данных

* **Изменения схемы обычно не создают снимки** — это приводит к важным особенностям поведения при использовании time travel с таблицами, в которых происходило изменение схемы.

<div id="example-scenarios">
  ### Примеры сценариев
</div>

Все сценарии написаны с использованием Spark, поскольку CH пока не поддерживает запись в таблицы Iceberg.

<div id="scenario-1">
  #### Сценарий 1: Изменения схемы без новых снимков
</div>

Рассмотрим следующую последовательность операций:

```sql
 -- Create a table with two columns
  CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example (
  order_number bigint, 
  product_code string
  ) 
  USING iceberg 
  OPTIONS ('format-version'='2')

- - Insert data into the table
  INSERT INTO spark_catalog.db.time_travel_example VALUES 
    (1, 'Mars')

  ts1 = now() // A piece of pseudo code

- - Alter table to add a new column
  ALTER TABLE spark_catalog.db.time_travel_example ADD COLUMN (price double)
 
  ts2 = now()

- - Insert data into the table
  INSERT INTO spark_catalog.db.time_travel_example VALUES (2, 'Venus', 100)

   ts3 = now()

- - Query the table at each timestamp
  SELECT * FROM spark_catalog.db.time_travel_example TIMESTAMP AS OF ts1;

+------------+------------+
|order_number|product_code|
+------------+------------+
|           1|        Mars|
+------------+------------+
  SELECT * FROM spark_catalog.db.time_travel_example TIMESTAMP AS OF ts2;

+------------+------------+
|order_number|product_code|
+------------+------------+
|           1|        Mars|
+------------+------------+

  SELECT * FROM spark_catalog.db.time_travel_example TIMESTAMP AS OF ts3;

+------------+------------+-----+
|order_number|product_code|price|
+------------+------------+-----+
|           1|        Mars| NULL|
|           2|       Venus|100.0|
+------------+------------+-----+
```

Результаты запроса для разных временных меток:

* Для ts1 &amp; ts2: отображаются только исходные два столбца
* Для ts3: отображаются все три столбца, при этом в столбце цены для первой строки указано NULL

<div id="scenario-2">
  #### Сценарий 2:  Различия между исторической и текущей схемами
</div>

Запрос time travel на текущий момент может показать схему, отличающуюся от схемы текущей таблицы:

```sql
-- Create a table
  CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example_2 (
  order_number bigint, 
  product_code string
  ) 
  USING iceberg 
  OPTIONS ('format-version'='2')

-- Insert initial data into the table
  INSERT INTO spark_catalog.db.time_travel_example_2 VALUES (2, 'Venus');

-- Alter table to add a new column
  ALTER TABLE spark_catalog.db.time_travel_example_2 ADD COLUMN (price double);

  ts = now();

-- Query the table at a current moment but using timestamp syntax

  SELECT * FROM spark_catalog.db.time_travel_example_2 TIMESTAMP AS OF ts;

    +------------+------------+
    |order_number|product_code|
    +------------+------------+
    |           2|       Venus|
    +------------+------------+

-- Query the table at a current moment
  SELECT * FROM spark_catalog.db.time_travel_example_2;
    +------------+------------+-----+
    |order_number|product_code|price|
    +------------+------------+-----+
    |           2|       Venus| NULL|
    +------------+------------+-----+
```

Это происходит потому, что `ALTER TABLE` не создает новый снимок, а для текущей таблицы Spark берет значение `schema_id` из последнего файла метаданных, а не из снимка.

<div id="scenario-3">
  #### Сценарий 3:  Различия между исторической и текущей схемой
</div>

Во-вторых, при использовании time travel невозможно получить состояние таблицы до записи в неё каких-либо данных:

```sql
-- Create a table
  CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example_3 (
  order_number bigint, 
  product_code string
  ) 
  USING iceberg 
  OPTIONS ('format-version'='2');

  ts = now();

-- Query the table at a specific timestamp
  SELECT * FROM spark_catalog.db.time_travel_example_3 TIMESTAMP AS OF ts; -- Finises with error: Cannot find a snapshot older than ts.
```

В ClickHouse поведение такое же, как в Spark. Мысленно замените запросы SELECT в Spark на запросы SELECT в ClickHouse, и всё будет работать так же.

<div id="metadata-file-resolution">
  ## Определение файла metadata.json
</div>

При использовании табличной функции `iceberg` в ClickHouse системе необходимо найти нужный файл metadata.json, описывающий структуру таблицы Iceberg. Вот как происходит этот процесс:

<div id="candidate-search">
  ### Поиск кандидатов (в порядке приоритета)
</div>

1. **Прямое указание пути**:
   *Если задан `iceberg_metadata_file_path`, система будет использовать этот точный путь, объединив его с путём к каталогу таблицы Iceberg.

* Если этот параметр указан, все остальные настройки разрешения игнорируются.

2. **Сопоставление UUID таблицы**:
   *Если указан `iceberg_metadata_table_uuid`, система будет:
   *Искать только файлы `.metadata.json` в каталоге `metadata`
   *Отбирать файлы, содержащие поле `table-uuid`, совпадающее с указанным UUID (регистронезависимо)

3. **Поиск по умолчанию**:
   *Если ни один из указанных выше параметров не задан, кандидатами становятся все файлы `.metadata.json` в каталоге `metadata`

<div id="most-recent-file">
  ### Выбор самого нового файла
</div>

После определения файлов-кандидатов по приведённым выше правилам система выбирает самый новый из них:

* Если `iceberg_recent_metadata_file_by_last_updated_ms_field` включен:

* Выбирается файл с наибольшим значением `last-updated-ms`

* В противном случае:

* Выбирается файл с наибольшим номером версии

* (`V` обозначает версию в именах файлов формата `V.metadata.json` или `V-uuid.metadata.json`)

**Note**: Все упомянутые настройки являются настройками table function (а не глобальными настройками или настройками на уровне запроса) и должны быть указаны, как показано ниже:

```sql
SELECT * FROM iceberg('s3://bucket/path/to/iceberg_table', 
    SETTINGS iceberg_metadata_table_uuid = 'a90eed4c-f74b-4e5b-b630-096fb9d09021');
```

**Примечание**: Хотя каталоги Iceberg обычно отвечают за определение метаданных, табличная функция `iceberg` в ClickHouse напрямую интерпретирует файлы, хранящиеся в S3, как таблицы Iceberg, поэтому важно понимать эти правила определения.

<div id="metadata-cache">
  ## Кэш метаданных
</div>

Движок таблицы `Iceberg` и табличная функция поддерживают кэширование метаданных: в кэше хранится информация о файлах манифеста, списке манифестов и JSON-файле метаданных. Кэш хранится в памяти. Эта возможность управляется настройкой `use_iceberg_metadata_files_cache`, которая включена по умолчанию.

<div id="aliases">
  ## Псевдонимы
</div>

Табличная функция `iceberg` теперь является псевдонимом `icebergS3`.

<div id="virtual-columns">
  ## Виртуальные столбцы
</div>

* `_path` — Путь к файлу. Тип: `LowCardinality(String)`.
* `_file` — Имя файла. Тип: `LowCardinality(String)`.
* `_size` — Размер файла в байтах. Тип: `Nullable(UInt64)`. Если размер файла неизвестен, значение — `NULL`.
* `_time` — Время последнего изменения файла. Тип: `Nullable(DateTime)`. Если время неизвестно, значение — `NULL`.
* `_etag` — ETag файла. Тип: `LowCardinality(String)`. Если ETag неизвестен, значение — `NULL`.

<div id="writes-into-iceberg-table">
  ## Запись в таблицу Iceberg
</div>

Начиная с версии 25.7, ClickHouse поддерживает изменение пользовательских таблиц Iceberg.

Сейчас это экспериментальная возможность, поэтому её нужно сначала включить:

```sql
SET allow_insert_into_iceberg = 1;
```

<div id="create-iceberg-table">
  ### Создание таблицы
</div>

Чтобы создать собственную пустую таблицу Iceberg, используйте те же команды, что и для чтения, но явно укажите схему.
При записи поддерживаются все форматы данных из спецификации Iceberg, такие как Parquet, Avro и ORC.

<div id="example">
  ### Пример
</div>

```sql
CREATE TABLE iceberg_writes_example
(
    x Nullable(String),
    y Nullable(Int32)
)
ENGINE = IcebergLocal('/home/scanhex12/iceberg_example/')
```

Примечание: чтобы создать файл с подсказкой о версии, включите настройку `iceberg_use_version_hint`.
Если вы хотите сжать файл metadata.json, укажите имя кодека в настройке `iceberg_metadata_compression_method`.

<div id="writes-inserts">
  ### INSERT
</div>

После создания новой таблицы можно вставить данные, используя стандартный синтаксис ClickHouse.

<div id="example">
  ### Пример
</div>

```sql
INSERT INTO iceberg_writes_example VALUES ('Pavel', 777), ('Ivanov', 993);

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Pavel
y: 777

Row 2:
──────
x: Ivanov
y: 993
```

<div id="iceberg-writes-delete">
  ### DELETE
</div>

ClickHouse также поддерживает удаление лишних строк в формате merge-on-read.
Этот запрос создаст новый снимок с файлами позиционного удаления.

<div id="example">
  ### Пример
</div>

```sql
ALTER TABLE iceberg_writes_example DELETE WHERE x != 'Ivanov';

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Ivanov
y: 993
```

<div id="iceberg-writes-schema-evolution">
  ### Изменение схемы
</div>

ClickHouse позволяет добавлять, удалять, изменять или переименовывать столбцы с простыми типами (не Tuple, Array и Map).

<div id="example">
  ### Пример
</div>

```sql
ALTER TABLE iceberg_writes_example MODIFY COLUMN y Nullable(Int64);
SHOW CREATE TABLE iceberg_writes_example;

   ┌─statement─────────────────────────────────────────────────┐
1. │ CREATE TABLE default.iceberg_writes_example              ↴│
   │↳(                                                        ↴│
   │↳    `x` Nullable(String),                                ↴│
   │↳    `y` Nullable(Int64)                                  ↴│
   │↳)                                                        ↴│
   │↳ENGINE = IcebergLocal('/home/scanhex12/iceberg_example/') │
   └───────────────────────────────────────────────────────────┘

ALTER TABLE iceberg_writes_example ADD COLUMN z Nullable(Int32);
SHOW CREATE TABLE iceberg_writes_example;

   ┌─statement─────────────────────────────────────────────────┐
1. │ CREATE TABLE default.iceberg_writes_example              ↴│
   │↳(                                                        ↴│
   │↳    `x` Nullable(String),                                ↴│
   │↳    `y` Nullable(Int64),                                 ↴│
   │↳    `z` Nullable(Int32)                                  ↴│
   │↳)                                                        ↴│
   │↳ENGINE = IcebergLocal('/home/scanhex12/iceberg_example/') │
   └───────────────────────────────────────────────────────────┘

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Ivanov
y: 993
z: ᴺᵁᴸᴸ

ALTER TABLE iceberg_writes_example DROP COLUMN z;
SHOW CREATE TABLE iceberg_writes_example;
   ┌─statement─────────────────────────────────────────────────┐
1. │ CREATE TABLE default.iceberg_writes_example              ↴│
   │↳(                                                        ↴│
   │↳    `x` Nullable(String),                                ↴│
   │↳    `y` Nullable(Int64)                                  ↴│
   │↳)                                                        ↴│
   │↳ENGINE = IcebergLocal('/home/scanhex12/iceberg_example/') │
   └───────────────────────────────────────────────────────────┘

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Ivanov
y: 993

ALTER TABLE iceberg_writes_example RENAME COLUMN y TO value;
SHOW CREATE TABLE iceberg_writes_example;

   ┌─statement─────────────────────────────────────────────────┐
1. │ CREATE TABLE default.iceberg_writes_example              ↴│
   │↳(                                                        ↴│
   │↳    `x` Nullable(String),                                ↴│
   │↳    `value` Nullable(Int64)                              ↴│
   │↳)                                                        ↴│
   │↳ENGINE = IcebergLocal('/home/scanhex12/iceberg_example/') │
   └───────────────────────────────────────────────────────────┘

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Ivanov
value: 993
```

<div id="iceberg-writes-compaction">
  ### Компактизация
</div>

ClickHouse поддерживает компактизацию таблиц Iceberg. В настоящее время она может объединять файлы позиционного удаления с файлами данных, одновременно обновляя метаданные. Идентификаторы предыдущих снимков и временные метки остаются неизменными, поэтому функцию time travel по-прежнему можно использовать с теми же значениями.

Как это использовать:

```sql
SET allow_experimental_iceberg_compaction = 1

OPTIMIZE TABLE iceberg_writes_example;

SELECT *
FROM iceberg_writes_example
FORMAT VERTICAL;

Row 1:
──────
x: Ivanov
y: 993
```

<div id="iceberg-expire-snapshots">
  ### Удаление устаревших снимков
</div>

Таблицы Iceberg накапливают снимки при каждой операции INSERT, DELETE или UPDATE. Со временем это может привести к появлению большого количества снимков и связанных с ними файлов данных. Команда `expire_snapshots` удаляет старые снимки и очищает файлы данных, на которые больше не ссылается ни один сохранённый снимок.

**Синтаксис:**

```sql
ALTER TABLE iceberg_table EXECUTE expire_snapshots(
    ['timestamp']
    [, expire_before = 'timestamp']
    [, retention_period = '3d']
    [, retain_last = 100]
    [, snapshot_ids = [1, 2, 3, 4]]
    [, dry_run = 1]
);
```

По умолчанию, какие снимки сохранять, определяется [политикой хранения](#iceberg-snapshot-retention-policy) (свойствами таблицы `min-snapshots-to-keep`, `max-snapshot-age-ms` и переопределениями для отдельных ссылок). Если указан `snapshot_ids`, политика хранения не применяется, и на истечение рассматриваются только перечисленные снимки.

**Аргументы:**

* `'timestamp'` (позиционный) или `expire_before = 'timestamp'` — строка даты и времени (например, `'2024-06-01 00:00:00'`), интерпретируемая в **часовом поясе сервера**. Служит предохранителем: снимки, у которых `timestamp-ms` равен этому значению или больше него, защищены от истечения, даже если по политике хранения они иначе подлежали бы удалению. Можно использовать вместе с `snapshot_ids`; в этом случае перечисленные снимки с временной меткой, равной `timestamp` или более поздней, не истекают.
* `retention_period = '<duration>'` — переопределяет значение `history.expire.max-snapshot-age-ms` на уровне таблицы только для этого вызова. Снимки старше этого периода (отсчитываемого от текущего момента) становятся кандидатами на истечение. Значение задаётся строкой длительности, состоящей из одной или нескольких подряд записанных пар `{number}{unit}`. Поддерживаемые единицы: `y` (365 дней), `w` (7 дней), `d` (24 часа), `h` (60 минут), `m` (60 секунд), `s` (1 секунда), `ms` (1 миллисекунда). Единицы можно комбинировать, например: `'3d'`, `'12h'`, `'1d12h30m'`, `'500ms'`.
* `retain_last = N` — переопределяет значение `history.expire.min-snapshots-to-keep` на уровне таблицы только для этого вызова. Как минимум `N` снимков сохраняются всегда, независимо от их возраста.
* `snapshot_ids = [id1, id2, ...]` — приводит к истечению ровно указанных идентификаторов снимков (кроме снимков, на которые ссылаются current snapshot, ветви или теги). Этот режим полностью обходит политику хранения и не может использоваться вместе с `retention_period` или `retain_last`.
* `dry_run = 1` — вычисляет, что было бы удалено, и возвращает метрики без записи новых метаданных и удаления файлов.

:::note
`retention_period` и `retain_last` переопределяют только **табличные** значения хранения по умолчанию. Переопределения хранения для отдельных ссылок (ветвей/тегов), настроенные в свойствах таблицы Iceberg (например, `refs.<branch>.min-snapshots-to-keep`), никогда не переопределяются — они всегда применяются так, как указано в метаданных таблицы.
:::

**Пример:**

```sql
SET allow_insert_into_iceberg = 1;

-- Create some snapshots by inserting data
INSERT INTO iceberg_table VALUES (1);
INSERT INTO iceberg_table VALUES (2);
INSERT INTO iceberg_table VALUES (3);

-- Expire using retention policy only
ALTER TABLE iceberg_table EXECUTE expire_snapshots();

-- Expire with a safety fuse: protect snapshots newer than the timestamp (positional syntax)
ALTER TABLE iceberg_table EXECUTE expire_snapshots('2025-01-01 00:00:00');

-- Same using the named argument form
ALTER TABLE iceberg_table EXECUTE expire_snapshots(expire_before = '2025-01-01 00:00:00');

-- Override retention parameters for one execution
ALTER TABLE iceberg_table EXECUTE expire_snapshots(retention_period = '3d', retain_last = 10);

-- Expire explicit snapshots
ALTER TABLE iceberg_table EXECUTE expire_snapshots(snapshot_ids = [101, 102, 103]);

-- Dry-run preview (no metadata updates, no file deletes)
ALTER TABLE iceberg_table EXECUTE expire_snapshots(retention_period = '1d', dry_run = 1);
```

**Вывод:**

Команда возвращает таблицу с двумя столбцами (`metric_name String`, `metric_value Int64`), в которой для каждой метрики есть одна строка. Имена метрик соответствуют [спецификации Iceberg](https://iceberg.apache.org/docs/latest/spark-procedures/#output):

| metric&#95;name                       | Описание                                                            |
| ------------------------------------- | ------------------------------------------------------------------- |
| `deleted_data_files_count`            | Количество удалённых файлов данных                                  |
| `deleted_position_delete_files_count` | Количество удалённых файлов позиционного удаления                   |
| `deleted_equality_delete_files_count` | Количество удалённых файлов удаления по равенству                   |
| `deleted_manifest_files_count`        | Количество удалённых файлов манифеста                               |
| `deleted_manifest_lists_count`        | Количество удалённых файлов списка манифестов                       |
| `deleted_statistics_files_count`      | Количество удалённых файлов статистики (в настоящее время всегда 0) |
| `dry_run`                             | `1` для режима dry-run, `0` для обычного выполнения                 |

Команда выполняет следующие шаги:

1. Оценивает политику хранения (см. ниже), чтобы определить, какие снимки необходимо сохранить
2. Если был передан аргумент временной метки, дополнительно защищает все снимки с этой временной меткой или более новые
3. Удаляет снимки, которые не сохраняются политикой и не защищены ограничением по временной метке
4. Вычисляет, какие файлы связаны исключительно с удаляемыми снимками
5. В обычном режиме: создаёт новые метаданные без удалённых снимков
6. В обычном режиме: физически удаляет недостижимые файлы списка манифестов, файлы манифеста и файлы данных
7. В режиме `dry_run = 1`: пропускает шаги 5 и 6 и возвращает только вычисленные метрики

<div id="iceberg-snapshot-retention-policy">
  #### Политика хранения снимков
</div>

Команда `expire_snapshots` учитывает [политику хранения снимков Iceberg](https://iceberg.apache.org/spec/#snapshot-retention-policy). Хранение настраивается через свойства таблицы Iceberg и переопределения для отдельных ссылок:

| Свойство                               | Область | По умолчанию                                                                    | Описание                                                                                            |
| -------------------------------------- | ------- | ------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------- |
| `history.expire.min-snapshots-to-keep` | Таблица | `iceberg_expire_default_min_snapshots_to_keep` (по умолчанию `1`)               | Минимальное количество снимков, которое нужно сохранять в цепочке предков каждой ветки              |
| `history.expire.max-snapshot-age-ms`   | Таблица | `iceberg_expire_default_max_snapshot_age_ms` (по умолчанию `432000000`, 5 дней) | Максимальный возраст (в мс) снимков, сохраняемых в ветке                                            |
| `history.expire.max-ref-age-ms`        | Таблица | `iceberg_expire_default_max_ref_age_ms` (по умолчанию `∞`)                      | Максимальный возраст (в мс) ссылки на снимок (ветки или тега), после которого удаляется сама ссылка |

Каждая ссылка на снимок (`refs` в метаданных Iceberg) может переопределять эти значения с помощью полей конкретной ссылки: `min-snapshots-to-keep`, `max-snapshot-age-ms` и `max-ref-age-ms`.

**Проверка правил хранения:**

* **Для каждой ветки** (включая `main`): цепочка предков обходится начиная с head ветки. Снимки сохраняются, пока выполняется хотя бы одно из следующих условий:
  * Снимок входит в число первых `min-snapshots-to-keep` в цепочке
  * Возраст снимка не превышает `max-snapshot-age-ms` (то есть `now - timestamp-ms <= max-snapshot-age-ms`)
* **Для тегов**: помеченный снимок сохраняется, если только тег не превысил свой `max-ref-age-ms`; в этом случае ссылка на тег удаляется
* **Ссылки, кроме `main`**, возраст которых превышает `max-ref-age-ms`, удаляются целиком (ветка `main` никогда не удаляется)
* **Висячие ссылки**, указывающие на несуществующие снимки, удаляются с предупреждением
* **Текущий снимок сохраняется всегда**, независимо от настроек хранения

**Требуемые привилегии:**

Требуется привилегия `ALTER TABLE EXECUTE`, которая является дочерней по отношению к `ALTER TABLE` в иерархии управления доступом ClickHouse. Её можно выдать напрямую или через родительскую привилегию:

```sql
-- Grant only EXECUTE permission
GRANT ALTER TABLE EXECUTE ON my_iceberg_table TO my_user;

-- Or grant all ALTER TABLE permissions (includes ALTER TABLE EXECUTE)
GRANT ALTER TABLE ON my_iceberg_table TO my_user;
```

:::note

* Поддерживаются только таблицы Iceberg формата версии 2 (снимки v1 не гарантируют наличие `manifest-list`, который необходим для безопасного определения файлов для очистки)
* Текущий снимок всегда сохраняется, даже если он старше указанной временной метки
* Требуется, чтобы настройка `allow_insert_into_iceberg` была включена
* Требуется, чтобы настройка `allow_experimental_expire_snapshots` была включена
* При обновлении метаданных в ClickHouse собственная авторизация каталога (авторизация REST-каталога, AWS Glue IAM и т. д.) применяется независимо
  :::

<div id="iceberg-remove-orphan-files">
  ### Удаление осиротевших файлов
</div>

Осиротевшие файлы — это файлы в хранилище, на которые не ссылается ни один снимок в метаданных таблицы Iceberg. Они накапливаются из-за неудачных операций записи, неполной очистки после компактизации и прерванных операций, что приводит к неограниченному росту хранилища. Команда `remove_orphan_files` выявляет и удаляет эти осиротевшие файлы.

**Синтаксис:**

```sql
-- Positional form: single unnamed older_than argument
ALTER TABLE iceberg_table EXECUTE remove_orphan_files('timestamp')

-- Named form
ALTER TABLE iceberg_table EXECUTE remove_orphan_files(
    older_than = 'timestamp',
    location = 'path',
    dry_run = 0|1
)

-- No arguments: use all defaults (older_than = 3 days ago)
ALTER TABLE iceberg_table EXECUTE remove_orphan_files()
```

**Параметры:**

| Параметр     | Тип                        | По умолчанию                                                                | Описание                                                                                                                                                                                |
| ------------ | -------------------------- | --------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `older_than` | `String` (временная метка) | 3 дня назад (настраивается через `iceberg_orphan_files_older_than_seconds`) | Кандидатами в осиротевшие считаются только файлы, у которых время последнего изменения старше этой временной метки. Служит защитой от удаления файлов из незавершённых операций записи. |
| `location`   | `String`                   | Расположение таблицы                                                        | Ограничивает сканирование указанным подкаталогом в расположении таблицы (например, `'data/'` или `'metadata/'`).                                                                        |
| `dry_run`    | `UInt64`                   | `0`                                                                         | При значении `1` определяет осиротевшие файлы и возвращает сводку результатов, ничего не удаляя.                                                                                        |

**Примеры:**

```sql
-- Remove orphan files older than a specific timestamp
ALTER TABLE iceberg_table EXECUTE remove_orphan_files('2026-03-01 00:00:00');

-- Dry run: preview which files would be deleted
ALTER TABLE iceberg_table EXECUTE remove_orphan_files(dry_run = 1);

-- Scan only the data directory
ALTER TABLE iceberg_table EXECUTE remove_orphan_files(
    older_than = '2026-03-01 00:00:00',
    location = 'data/'
);

-- Combine positional older_than with named arguments
ALTER TABLE iceberg_table EXECUTE remove_orphan_files(
    '2026-03-01 00:00:00',
    dry_run = 1
);
```

**Вывод:**

Команда возвращает таблицу со столбцами `metric_name` и `metric_value`, в которой показано количество удалённых (или тех, которые были бы удалены в режиме dry&#95;run) файлов по категориям. Категории файлов определяются с помощью эвристик на основе соглашений об именовании файлов; файлы, не соответствующие ни одному конкретному шаблону, по умолчанию относятся к `deleted_data_files_count`:

| metric&#95;name                                     | metric&#95;value |
| --------------------------------------------------- | ---------------- |
| deleted&#95;data&#95;files&#95;count                | 5                |
| deleted&#95;position&#95;delete&#95;files&#95;count | 2                |
| deleted&#95;equality&#95;delete&#95;files&#95;count | 0                |
| deleted&#95;manifest&#95;files&#95;count            | 3                |
| deleted&#95;manifest&#95;lists&#95;count            | 1                |
| deleted&#95;metadata&#95;files&#95;count            | 0                |
| deleted&#95;statistics&#95;files&#95;count          | 0                |
| skipped&#95;missing&#95;metadata&#95;count          | 0                |
| failed&#95;deletions&#95;count                      | 0                |

**Настройки:**

| Настройка                                 | Тип      | По умолчанию     | Описание                                                             |
| ----------------------------------------- | -------- | ---------------- | -------------------------------------------------------------------- |
| `allow_iceberg_remove_orphan_files`       | `Bool`   | `false`          | Флаг, включающий эту возможность (экспериментальную).                |
| `iceberg_orphan_files_older_than_seconds` | `UInt64` | `259200` (3 дня) | Порог `older_than` по умолчанию в секундах, если аргумент не указан. |

:::note

* **Требуется Iceberg format version 2 (или выше).** Таблицы версии 1 отклоняются, поскольку в них отсутствуют указатели `manifest-list` в снимках, необходимые для безопасного определения набора достижимых файлов. При выполнении команды для таблицы v1 возвращается ошибка `BAD_ARGUMENTS`.
* Обе настройки `allow_insert_into_iceberg` и `allow_iceberg_remove_orphan_files` должны быть включены
* Рекомендуется запускать `expire_snapshots` перед `remove_orphan_files`, чтобы сначала очищались файлы, на которые ссылаются только устаревшие снимки
* Используйте `dry_run = 1`, чтобы предварительно просмотреть осиротевшие файлы перед удалением
* Порог `older_than` защищает от удаления файлов из незавершённых операций записи — значение по умолчанию в 3 дня обеспечивает достаточный запас безопасности
  :::

<div id="see-also">
  ## См. также
</div>

* [Движок Iceberg](/ru/engines/table-engines/integrations/iceberg.md)
* [Табличная функция Iceberg для кластера](/ru/sql-reference/table-functions/icebergCluster.md)