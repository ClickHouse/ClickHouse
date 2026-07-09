---
description: 'Этот движок обеспечивает доступ только для чтения к существующим таблицам
  Apache Iceberg в Amazon S3, Azure, HDFS и к локально хранящимся таблицам.'
sidebar_label: 'Iceberg'
sidebar_position: 90
slug: /engines/table-engines/integrations/iceberg
title: 'Движок таблицы Iceberg'
doc_type: 'reference'
---

:::warning
Мы рекомендуем использовать [Iceberg Table Function](/ru/sql-reference/table-functions/iceberg.md) для работы с данными Iceberg в ClickHouse. Сейчас Iceberg Table Function предоставляет достаточную функциональность, обеспечивая частичный интерфейс только для чтения для таблиц Iceberg.

Iceberg Table Engine доступен, но может иметь ограничения. ClickHouse изначально не проектировался для поддержки таблиц со схемами, которые изменяются извне, что может влиять на работу Iceberg Table Engine. В результате некоторые возможности, доступные для обычных таблиц, могут быть недоступны или работать некорректно, особенно при использовании старого анализатора.

Для максимальной совместимости мы рекомендуем использовать Iceberg Table Function, пока продолжается улучшение поддержки Iceberg Table Engine.
:::

Этот движок обеспечивает доступ только для чтения к существующим таблицам Apache [Iceberg](https://iceberg.apache.org/) в Amazon S3, Azure, HDFS и к локально хранящимся таблицам.

<div id="create-table">
  ## Создание таблицы
</div>

Обратите внимание: таблица Iceberg уже должна существовать в хранилище; эта команда не поддерживает DDL-параметры для создания новой таблицы.

```sql
CREATE TABLE iceberg_table_s3
    ENGINE = IcebergS3(url,  [, NOSIGN | access_key_id, secret_access_key, [session_token]], format, [,compression], [,extra_credentials])

CREATE TABLE iceberg_table_azure
    ENGINE = IcebergAzure(connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression])

CREATE TABLE iceberg_table_hdfs
    ENGINE = IcebergHDFS(path_to_table, [,format] [,compression_method])

CREATE TABLE iceberg_table_local
    ENGINE = IcebergLocal(path_to_table, [,format] [,compression_method])
```

<div id="engine-arguments">
  ## Аргументы движка
</div>

Описание аргументов совпадает с описанием аргументов для движков `S3`, `AzureBlobStorage`, `HDFS` и `File` соответственно.
`format` обозначает формат файлов данных в таблице Iceberg.

Для `IcebergS3` можно использовать необязательный параметр `extra_credentials` для передачи `role_arn` при доступе на основе ролей в ClickHouse Cloud. Инструкции по настройке см. в разделе [Защищённый S3](/ru/cloud/data-sources/secure-s3).

Параметры движка можно указать с помощью [именованных коллекций](../../../operations/named-collections.md)

<div id="example">
  ### Пример
</div>

```sql
CREATE TABLE iceberg_table ENGINE=IcebergS3('http://test.s3.amazonaws.com/clickhouse-bucket/test_table', 'test', 'test')
```

Использование именованных коллекций:

```xml
<clickhouse>
    <named_collections>
        <iceberg_conf>
            <url>http://test.s3.amazonaws.com/clickhouse-bucket/</url>
            <access_key_id>test</access_key_id>
            <secret_access_key>test</secret_access_key>
        </iceberg_conf>
    </named_collections>
</clickhouse>
```

```sql
CREATE TABLE iceberg_table ENGINE=IcebergS3(iceberg_conf, filename = 'test_table')

```

<div id="aliases">
  ## Псевдонимы
</div>

Движок таблицы `Iceberg` автоматически определяет тип хранилища по настройке `disk` и в соответствии с этим использует `IcebergS3`, `IcebergAzure` или `IcebergLocal`. Если `disk` не указан, по умолчанию используется реализация `IcebergS3`.

<div id="data-types">
  ## Типы данных
</div>

В таблице ниже показано, как типы данных Iceberg сопоставляются с типами данных ClickHouse при автоматическом определении схемы (для чтения).

<div id="primitive-types">
  ### Примитивные типы
</div>

| Тип Iceberg        | Тип ClickHouse         | Примечания                                                   |
| ------------------ | ---------------------- | ------------------------------------------------------------ |
| `boolean`          | `Bool`                 |                                                              |
| `int`              | `Int32`                |                                                              |
| `long`, `bigint`   | `Int64`                |                                                              |
| `float`            | `Float32`              |                                                              |
| `double`           | `Float64`              |                                                              |
| `date`             | `Date32`               |                                                              |
| `time`             | `Int64`                | Микросекунды с полуночи                                      |
| `timestamp`        | `DateTime64(6)`        | Микросекунды, без часового пояса                             |
| `timestamptz`      | `DateTime64(6, 'UTC')` | Микросекунды, часовой пояс UTC                               |
| `timestamp_ns`     | `DateTime64(9)`        | Наносекунды, без часового пояса (только в Iceberg v3 и выше) |
| `timestamptz_ns`   | `DateTime64(9, 'UTC')` | Наносекунды, часовой пояс UTC (только в Iceberg v3 и выше)   |
| `string`, `binary` | `String`               |                                                              |
| `uuid`             | `UUID`                 |                                                              |
| `fixed(N)`         | `FixedString(N)`       |                                                              |
| `decimal(P, S)`    | `Decimal(P, S)`        |                                                              |

<div id="complex-types">
  ### Сложные типы
</div>

| Тип Iceberg | Тип ClickHouse |
| ----------- | -------------- |
| `list`      | `Array`        |
| `map`       | `Map`          |
| `struct`    | `Tuple`        |

<div id="schema-evolution">
  ## Изменение схемы
</div>

ClickHouse поддерживает чтение таблиц Iceberg, схема которых со временем менялась. Это относится к таблицам, в которых столбцы были добавлены, удалены или переупорядочены, а также к столбцам, изменённым с обязательных на Nullable. Кроме того, поддерживаются следующие приведения типов:

* int -&gt; long
* float -&gt; double
* decimal(P, S) -&gt; decimal(P&#39;, S), где P&#39; &gt; P.

В настоящее время нельзя изменять вложенные структуры или типы элементов внутри массивов и Map.

Чтобы читать таблицу, схема которой изменилась после её создания с использованием динамического определения схемы, задайте `allow_dynamic_metadata_for_data_lakes = true` при создании таблицы.

<div id="partition-pruning">
  ## Отсечение партиций
</div>

ClickHouse поддерживает отсечение партиций при выполнении запросов SELECT к таблицам Iceberg, что помогает повысить производительность запросов за счёт пропуска ненужных файлов данных. Чтобы включить отсечение партиций, установите `use_iceberg_partition_pruning = 1`. Дополнительные сведения об отсечении партиций в Iceberg см. по адресу https://iceberg.apache.org/spec/#partitioning

<div id="time-travel">
  ## Путешествие во времени
</div>

ClickHouse поддерживает функцию путешествия во времени для таблиц Iceberg, позволяя выполнять запросы к историческим данным, указывая конкретную временную метку или идентификатор снимка.

<div id="deleted-rows">
  ## Обработка таблиц с удаленными строками
</div>

ClickHouse поддерживает чтение таблиц Iceberg, в которых используются следующие методы удаления:

* [Позиционные удаления](https://iceberg.apache.org/spec/#position-delete-files)
* [Удаления по равенству](https://iceberg.apache.org/spec/#equality-delete-files) (поддерживаются начиная с версии 25.8+)

Следующий метод удаления **не поддерживается**:

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

Примечание: в одном запросе нельзя указывать оба параметра — `iceberg_timestamp_ms` и `iceberg_snapshot_id`.

<div id="important-considerations">
  ### Важные моменты
</div>

* **Снимки** обычно создаются, когда:
  * В таблицу записываются новые данные
  * Выполняется тот или иной вид компакции данных

* **Изменения схемы обычно не приводят к созданию снимков** — это вызывает важные особенности при использовании путешествия во времени с таблицами, в которых происходило изменение схемы.

<div id="example-scenarios">
  ### Примеры сценариев
</div>

Все сценарии приведены с использованием Spark, поскольку CH пока не поддерживает запись в таблицы Iceberg.

<div id="scenario-1">
  #### Сценарий 1: Изменения схемы без новых снимков
</div>

Рассмотрим следующую последовательность операций:

```sql
 -- Create a table with two columns
  CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example (
  order_number int, 
  product_code string
  ) 
  USING iceberg 
  OPTIONS ('format-version'='2')

-- Insert data into the table
  INSERT INTO spark_catalog.db.time_travel_example VALUES 
    (1, 'Mars')

  ts1 = now() // A piece of pseudo code

-- Alter table to add a new column
  ALTER TABLE spark_catalog.db.time_travel_example ADD COLUMN (price double)
 
  ts2 = now()

-- Insert data into the table
  INSERT INTO spark_catalog.db.time_travel_example VALUES (2, 'Venus', 100)

   ts3 = now()

-- Query the table at each timestamp
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

* На ts1 &amp; ts2: отображаются только исходные два столбца
* На ts3: отображаются все три столбца, а значение price в первой строке — NULL

<div id="scenario-2">
  #### Сценарий 2: Различия между исторической и текущей схемой
</div>

Запрос путешествия во времени на текущий момент может показать схему, отличающуюся от схемы текущей таблицы:

```sql
-- Create a table
  CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example_2 (
  order_number int, 
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

Это происходит потому, что `ALTER TABLE` не создаёт новый снимок, а для текущей таблицы Spark берёт значение `schema_id` из последнего файла метаданных, а не из снимка.

<div id="scenario-3">
  #### Сценарий 3: Различия между исторической и текущей схемами
</div>

Второй момент: при использовании путешествия во времени нельзя получить состояние таблицы на момент до записи в неё каких-либо данных:

```sql
-- Create a table
  CREATE TABLE IF NOT EXISTS spark_catalog.db.time_travel_example_3 (
  order_number int, 
  product_code string
  ) 
  USING iceberg 
  OPTIONS ('format-version'='2');

  ts = now();

-- Query the table at a specific timestamp
  SELECT * FROM spark_catalog.db.time_travel_example_3 TIMESTAMP AS OF ts; -- Finises with error: Cannot find a snapshot older than ts.
```

В ClickHouse поведение такое же, как в Spark. Мысленно замените запросы SELECT в Spark на запросы SELECT в ClickHouse — и всё будет работать так же.

<div id="metadata-file-resolution">
  ## Определение файла метаданных
</div>

При использовании движка таблицы `Iceberg` в ClickHouse системе необходимо найти нужный файл metadata.json, который описывает структуру таблицы Iceberg. Вот как работает этот процесс:

<div id="candidate-search">
  ### Поиск кандидатов
</div>

1. **Прямое указание пути**:

* Если задан параметр `iceberg_metadata_file_path`, система использует этот точный путь, комбинируя его с путём к каталогу таблицы Iceberg.
* Если этот параметр указан, все остальные параметры разрешения игнорируются.

2. **Сопоставление UUID таблицы**:

* Если указан `iceberg_metadata_table_uuid`, система будет:
  * Рассматривать только файлы `.metadata.json` в каталоге `metadata`
  * Отбирать файлы, содержащие поле `table-uuid`, совпадающее с указанным UUID (регистронезависимо)

3. **Поиск по умолчанию**:

* Если ни один из указанных выше параметров не задан, кандидатами становятся все файлы `.metadata.json` в каталоге `metadata`

<div id="most-recent-file">
  ### Выбор самого нового файла
</div>

После определения файлов-кандидатов по приведённым выше правилам система выбирает самый новый из них:

* Если включён параметр `iceberg_recent_metadata_file_by_last_updated_ms_field`:
  * Выбирается файл с наибольшим значением `last-updated-ms`

* В противном случае:
  * Выбирается файл с наибольшим номером версии
  * (Версия указывается как `V` в именах файлов формата `V.metadata.json` или `V-uuid.metadata.json`)

**Примечание**: Все упомянутые настройки (если явно не указано иное) являются настройками на уровне движка и должны быть заданы при создании таблицы, как показано ниже:

```sql
CREATE TABLE example_table ENGINE = Iceberg(
    's3://bucket/path/to/iceberg_table'
) SETTINGS iceberg_metadata_table_uuid = '6f6f6407-c6a5-465f-a808-ea8900e35a38';
```

**Примечание**: хотя каталоги Iceberg обычно отвечают за разрешение метаданных, движок таблицы `Iceberg` в ClickHouse напрямую интерпретирует файлы, хранящиеся в S3, как таблицы Iceberg, поэтому важно понимать эти правила разрешения.

<div id="data-cache">
  ## Кэш данных
</div>

Движок таблицы `Iceberg` и табличная функция поддерживают кэширование данных так же, как и хранилища `S3`, `AzureBlobStorage` и `HDFS`. См. [здесь](../../../engines/table-engines/integrations/s3.md#data-cache).

<div id="metadata-cache">
  ## Кэш метаданных
</div>

Движок таблицы `Iceberg` и табличная функция поддерживают кэш метаданных, в котором хранится информация о файлах manifest, списке manifest и metadata JSON. Кэш хранится в памяти. Эта возможность управляется настройкой `use_iceberg_metadata_files_cache`, которая включена по умолчанию.

<div id="async-metadata-prefetch">
  ## Асинхронная предзагрузка метаданных
</div>

Асинхронную предзагрузку метаданных можно включить при создании таблицы `Iceberg`, задав `iceberg_metadata_async_prefetch_period_ms`. Если задано значение 0 (по умолчанию) или если кэширование метаданных не включено, асинхронная предзагрузка отключена.
Чтобы включить эту возможность, нужно указать ненулевое значение в миллисекундах. Оно задает интервал между циклами предзагрузки.

Если предзагрузка включена, сервер будет выполнять периодическую фоновую операцию: просматривать удаленный каталог и обнаруживать новую версию метаданных. Затем он разберет ее и рекурсивно обойдет снимок, загружая активные файлы список manifest и файл manifest.
Файлы, уже доступные в кэше метаданных, не будут загружаться повторно. В конце каждого цикла предзагрузки последний снимок метаданных будет доступен в кэше метаданных.

```sql
CREATE TABLE example_table ENGINE = Iceberg(
    's3://bucket/path/to/iceberg_table'
) SETTINGS
    iceberg_metadata_async_prefetch_period_ms = 60000;
```

Чтобы максимально эффективно использовать асинхронную предзагрузку метаданных при операциях чтения, параметр `iceberg_metadata_staleness_ms` следует указывать как параметр запроса или сеанса. По умолчанию (0 — не указано) в контексте каждого запроса сервер будет получать актуальные метаданные из удалённого каталога.
Если указать допустимый порог устаревания метаданных, сервер сможет использовать кэшированную версию снимка метаданных без обращения к удалённому каталогу. Если версия метаданных есть в кэше и была загружена в пределах заданного окна устаревания, она будет использоваться для обработки запроса.
В противном случае из удалённого каталога будет получена последняя версия.

```sql
SELECT count() FROM icebench_table WHERE ...
SETTINGS iceberg_metadata_staleness_ms=120000
```

**Примечание**: Асинхронная предварительная загрузка метаданных выполняется в `ICEBERG_SCEDULE_POOL` — это пул потоков на стороне сервера для фоновых операций с активными таблицами `Iceberg`. Размер этого пула потоков задается параметром конфигурации сервера `iceberg_background_schedule_pool_size` (по умолчанию — 10).

**Примечание**: В настоящее время предполагается, что размер кэша метаданных достаточен для полного хранения последнего снимка метаданных всех активных таблиц, если асинхронная предварительная загрузка включена.

<div id="see-also">
  ## См. также
</div>

* [табличная функция Iceberg](/ru/sql-reference/table-functions/iceberg.md)