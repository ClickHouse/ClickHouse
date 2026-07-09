---
description: 'Документация для highlight-next-line'
sidebar_label: 'Внешние диски для хранения данных'
sidebar_position: 68
slug: /operations/storing-data
title: 'Внешние диски для хранения данных'
doc_type: 'guide'
---

Данные, обрабатываемые в ClickHouse, обычно хранятся в локальной файловой системе
машины, на которой запущен сервер ClickHouse. Для этого требуются диски большой ёмкости,
которые могут стоить дорого. Чтобы не хранить данные локально, поддерживаются различные варианты хранения:

1. Объектное хранилище [Amazon S3](https://aws.amazon.com/s3/).
2. [Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs).
3. Не поддерживается: Hadoop Distributed File System ([HDFS](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html))

<br />

:::note
ClickHouse также поддерживает внешние движки таблиц, которые отличаются от
варианта внешнего хранилища, описанного на этой странице, поскольку позволяют читать данные,
хранящиеся в одном из распространённых файловых форматов (например, Parquet). На этой странице описывается
конфигурация хранилища для таблиц семейства ClickHouse `MergeTree` или семейства `Log`.

1. для работы с данными, хранящимися на дисках `Amazon S3`, используйте движок таблицы [S3](/ru/engines/table-engines/integrations/s3.md).
2. для работы с данными, хранящимися в Azure Blob Storage, используйте движок таблицы [AzureBlobStorage](/ru/engines/table-engines/integrations/azureBlobStorage.md).
3. для работы с данными в Hadoop Distributed File System (не поддерживается) используйте движок таблицы [HDFS](/ru/engines/table-engines/integrations/hdfs.md).
   :::

<div id="configuring-external-storage">
  ## Настройка внешнего хранилища
</div>

Семейства движков таблиц [`MergeTree`](/ru/engines/table-engines/mergetree-family/mergetree.md) и [`Log`](/ru/engines/table-engines/log-family/log.md)
могут хранить данные в `S3`, `AzureBlobStorage`, `HDFS` (не поддерживается), используя диск типов `s3`,
`azure_blob_storage`, `hdfs` (не поддерживается) соответственно.

Для конфигурации диска требуется:

1. Раздел `type` со значением одного из следующих типов: `s3`, `azure_blob_storage`, `hdfs` (не поддерживается), `local_blob_storage`, `web`.
2. Конфигурация конкретного типа внешнего хранилища.

Начиная с версии ClickHouse 24.1 можно использовать новую опцию конфигурации.
Для этого требуется указать:

1. `type` со значением `object_storage`
2. `object_storage_type` со значением одного из следующих типов: `s3`, `azure_blob_storage` (или просто `azure`, начиная с `24.3`), `hdfs` (не поддерживается), `local_blob_storage` (или просто `local`, начиная с `24.3`), `web`.

<br />

При желании можно указать `metadata_type` (по умолчанию это `local`), также можно задать значения `plain`, `web` и, начиная с `24.4`, `plain_rewritable`.
Использование типа метаданных `plain` описано в [разделе plain storage](/ru/operations/storing-data#plain-storage), тип метаданных `web` можно использовать только с типом Объектного хранилища `web`, а тип метаданных `local` хранит файлы метаданных локально (каждый файл метаданных содержит сопоставление с файлами в Объектном хранилище и некоторую дополнительную метаинформацию о них).

Например:

```xml
<s3>
    <type>s3</type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3>
```

эквивалентно следующей конфигурации (начиная с версии `24.1`):

```xml
<s3>
    <type>object_storage</type>
    <object_storage_type>s3</object_storage_type>
    <metadata_type>local</metadata_type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3>
```

Конфигурация ниже:

```xml
<s3_plain>
    <type>s3_plain</type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3_plain>
```

равно:

```xml
<s3_plain>
    <type>object_storage</type>
    <object_storage_type>s3</object_storage_type>
    <metadata_type>plain</metadata_type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3_plain>
```

Пример полной конфигурации хранилища будет выглядеть так:

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <s3>
                <type>s3</type>
                <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
                <use_environment_credentials>1</use_environment_credentials>
            </s3>
        </disks>
        <policies>
            <s3>
                <volumes>
                    <main>
                        <disk>s3</disk>
                    </main>
                </volumes>
            </s3>
        </policies>
    </storage_configuration>
</clickhouse>
```

Начиная с версии 24.1, это может выглядеть и так:

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <s3>
                <type>object_storage</type>
                <object_storage_type>s3</object_storage_type>
                <metadata_type>local</metadata_type>
                <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
                <use_environment_credentials>1</use_environment_credentials>
            </s3>
        </disks>
        <policies>
            <s3>
                <volumes>
                    <main>
                        <disk>s3</disk>
                    </main>
                </volumes>
            </s3>
        </policies>
    </storage_configuration>
</clickhouse>
```

Чтобы сделать определённый тип хранилища используемым по умолчанию для всех таблиц `MergeTree`,
добавьте следующий раздел в файл конфигурации:

```xml
<clickhouse>
    <merge_tree>
        <storage_policy>s3</storage_policy>
    </merge_tree>
</clickhouse>
```

Если вы хотите настроить определённую политику хранения для конкретной таблицы,
её можно указать в настройках при создании таблицы:

```sql
CREATE TABLE test (a Int32, b String)
ENGINE = MergeTree() ORDER BY a
SETTINGS storage_policy = 's3';
```

Вы также можете использовать `disk` вместо `storage_policy`. В этом случае раздел `storage_policy` в конфигурационном файле не требуется,
достаточно раздела `disk`.

```sql
CREATE TABLE test (a Int32, b String)
ENGINE = MergeTree() ORDER BY a
SETTINGS disk = 's3';
```

<div id="refresh-parts-interval-and-table-disk">
  ## refresh_parts_interval and table_disk
</div>

Эта настройка предназначена для нереплицируемых таблиц MergeTree, в которых части могут записываться извне, а метаданные нужно обновлять из хранилища.

Настройка MergeTree `refresh_parts_interval` включает периодическое обновление списка частей данных из нижележащего хранилища (например, чтобы подхватывать части, записанные извне). Важно различать **общие метаданные для всех реплик** и **локальные метаданные отдельной реплики** (например, S3 с локальными метаданными у каждой реплики): только при общих метаданных новые части будут видны всем репликам. Само по себе использование Объектного хранилища не означает, что метаданные будут общими.

* **Объектное хранилище (например, `disk = 's3'`) само по себе не означает общие метаданные.** Когда метаданные по умолчанию хранятся локально у каждой реплики, каждая реплика независимо управляет своими указателями на blob-объекты в Объектном хранилище. Изменения, внесённые на одной реплике, не видны другим. В таком случае `refresh_parts_interval` не сделает новые части видимыми на всех репликах, потому что метаданные, которые читает каждая реплика, локальны для неё.

* **Для автоматического обновления частей метаданные файловой системы должны быть общими** (или таблица должна использовать принадлежащие ей метаданные в режиме только для чтения, чтобы обновление было применимо). Установка `table_disk = true` вместе с локальным для таблицы диском (например, `SETTINGS disk = disk(type=object_storage, ...), table_disk = true`) — один из способов получить нужную семантику: таблица сама управляет жизненным циклом метаданных, а хранилище рассматривается как только для чтения, поэтому `refresh_parts_interval` работает, и части, добавленные извне, могут быть обнаружены.

* **При глобально определённом диске** (например, `disk = 's3'` в `storage_configuration`) и локальных метаданных по умолчанию у каждой реплики будет собственное состояние метаданных. Хотя blob-объекты могут находиться в S3, такое хранилище не считается общим для целей `refresh_parts_interval`, и новые части, созданные вне ClickHouse или на другой реплике, обнаружены не будут.

Чтобы автоматическое обновление частей работало, убедитесь, что метаданные являются общими, либо используйте диск уровня таблицы с `table_disk = true`, как показано выше. Если полагаться только на `refresh_parts_interval` при локальных метаданных реплики, части не будут обновляться ожидаемым образом.

:::note
`refresh_parts_interval` не используется для таблиц ReplicatedMergeTree.
Реплицируемые таблицы уже синхронизируют части через механизм репликации.
Эта настройка применима только к нереплицируемым таблицам MergeTree, в которых части записываются извне и требуется обновление метаданных.
:::

<div id="dynamic-configuration">
  ## Динамическая конфигурация
</div>

Также можно указать конфигурацию хранилища без предварительного определения
диска в конфигурационном файле, а настроить её через
параметры запроса `CREATE`/`ATTACH`.

Следующий пример запроса основан на приведённой выше динамической конфигурации диска и
показывает, как использовать локальный диск для кэширования данных из таблицы, хранящейся по URL.

```sql
ATTACH TABLE uk_price_paid UUID 'cf712b4f-2ca8-435c-ac23-c4393efe52f7'
(
    price UInt32,
    date Date,
    postcode1 LowCardinality(String),
    postcode2 LowCardinality(String),
    type Enum8('other' = 0, 'terraced' = 1, 'semi-detached' = 2, 'detached' = 3, 'flat' = 4),
    is_new UInt8,
    duration Enum8('unknown' = 0, 'freehold' = 1, 'leasehold' = 2),
    addr1 String,
    addr2 String,
    street LowCardinality(String),
    locality LowCardinality(String),
    town LowCardinality(String),
    district LowCardinality(String),
    county LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (postcode1, postcode2, addr1, addr2)
  -- highlight-start
  SETTINGS disk = disk(
    type=web,
    endpoint='https://raw.githubusercontent.com/ClickHouse/web-tables-demo/main/web/'
  );
  -- highlight-end
```

В примере ниже к внешнему хранилищу добавляется кэш.

```sql
ATTACH TABLE uk_price_paid UUID 'cf712b4f-2ca8-435c-ac23-c4393efe52f7'
(
    price UInt32,
    date Date,
    postcode1 LowCardinality(String),
    postcode2 LowCardinality(String),
    type Enum8('other' = 0, 'terraced' = 1, 'semi-detached' = 2, 'detached' = 3, 'flat' = 4),
    is_new UInt8,
    duration Enum8('unknown' = 0, 'freehold' = 1, 'leasehold' = 2),
    addr1 String,
    addr2 String,
    street LowCardinality(String),
    locality LowCardinality(String),
    town LowCardinality(String),
    district LowCardinality(String),
    county LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (postcode1, postcode2, addr1, addr2)
-- highlight-start
  SETTINGS disk = disk(
    type=cache,
    max_size='1Gi',
    path='/var/lib/clickhouse/custom_disk_cache/',
    disk=disk(
      type=web,
      endpoint='https://raw.githubusercontent.com/ClickHouse/web-tables-demo/main/web/'
      )
  );
-- highlight-end
```

В настройках ниже обратите внимание, что диск с `type=web` вложен в
диск с `type=cache`.

:::note
В примере используется `type=web`, но любой тип диска можно настроить как динамический,
включая локальный диск. Для локальных дисков аргумент path должен находиться внутри
параметра config сервера `custom_local_disks_base_directory`, у которого нет
значения по умолчанию, поэтому при использовании локального диска задайте и его.
:::

Также возможна комбинация конфигурации на основе config и конфигурации,
определённой через SQL:

```sql
ATTACH TABLE uk_price_paid UUID 'cf712b4f-2ca8-435c-ac23-c4393efe52f7'
(
    price UInt32,
    date Date,
    postcode1 LowCardinality(String),
    postcode2 LowCardinality(String),
    type Enum8('other' = 0, 'terraced' = 1, 'semi-detached' = 2, 'detached' = 3, 'flat' = 4),
    is_new UInt8,
    duration Enum8('unknown' = 0, 'freehold' = 1, 'leasehold' = 2),
    addr1 String,
    addr2 String,
    street LowCardinality(String),
    locality LowCardinality(String),
    town LowCardinality(String),
    district LowCardinality(String),
    county LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (postcode1, postcode2, addr1, addr2)
  -- highlight-start
  SETTINGS disk = disk(
    type=cache,
    max_size='1Gi',
    path='/var/lib/clickhouse/custom_disk_cache/',
    disk=disk(
      type=web,
      endpoint='https://raw.githubusercontent.com/ClickHouse/web-tables-demo/main/web/'
      )
  );
  -- highlight-end
```

где `web` берётся из файла конфигурации сервера:

```xml
<storage_configuration>
    <disks>
        <web>
            <type>web</type>
            <endpoint>'https://raw.githubusercontent.com/ClickHouse/web-tables-demo/main/web/'</endpoint>
        </web>
    </disks>
</storage_configuration>
```

<div id="s3-storage">
  ### Использование хранилища S3
</div>

<div id="required-parameters-s3">
  #### Обязательные параметры
</div>

| Параметр            | Описание                                                                                                                                                                                                 |
| ------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `endpoint`          | URL конечной точки S3 в формате `path` или `virtual hosted` [адресации](https://docs.aws.amazon.com/AmazonS3/latest/dev/VirtualHosting.html). Должен включать бакет и корневой путь для хранения данных. |
| `access_key_id`     | Идентификатор ключа доступа S3, используемый для аутентификации.                                                                                                                                         |
| `secret_access_key` | Секретный ключ доступа S3, используемый для аутентификации.                                                                                                                                              |

<div id="optional-parameters-s3">
  #### Необязательные параметры
</div>

| Параметр                                                                                                       | Описание                                                                                                                                                                                                                                                                                                                                                                   | Значение по умолчанию                    |
| -------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------- |
| `region`                                                                                                       | Название региона S3.                                                                                                                                                                                                                                                                                                                                                       | *                                        |
| `support_batch_delete`                                                                                         | Определяет, нужно ли проверять поддержку батч-удаления. При использовании Google Cloud Storage (GCS) установите `false`, поскольку GCS не поддерживает батч-удаление.                                                                                                                                                                                                      | `true`                                   |
| `use_environment_credentials`                                                                                  | Считывает учетные данные AWS из переменных окружения: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` и `AWS_SESSION_TOKEN`, если они заданы. Примечание: учетные данные из окружения используются всеми S3-дисками совместно. Чтобы использовать разные учетные данные для разных дисков, вместо этого явно укажите `access_key_id` и `secret_access_key` для каждого диска. | `false`                                  |
| `use_insecure_imds_request`                                                                                    | Если `true`, при получении учетных данных из метаданных Amazon EC2 используется небезопасный запрос к IMDS.                                                                                                                                                                                                                                                                | `false`                                  |
| `expiration_window_seconds`                                                                                    | Льготный период (в секундах) для проверки, не истекли ли срок действия учетных данных, зависящих от времени истечения.                                                                                                                                                                                                                                                     | `120`                                    |
| `proxy`                                                                                                        | Конфигурация прокси для конечной точки S3. Каждый элемент `uri` в блоке `proxy` должен содержать URL прокси-сервера.                                                                                                                                                                                                                                                       | -                                        |
| `connect_timeout_ms`                                                                                           | Тайм-аут подключения сокета в миллисекундах.                                                                                                                                                                                                                                                                                                                               | `10000` (10 секунд)                      |
| `request_timeout_ms`                                                                                           | Тайм-аут запроса в миллисекундах.                                                                                                                                                                                                                                                                                                                                          | `5000` (5 секунд)                        |
| `retry_attempts`                                                                                               | Количество повторных попыток для неудачных запросов.                                                                                                                                                                                                                                                                                                                       | `10`                                     |
| `single_read_retries`                                                                                          | Количество повторных попыток при разрыве соединения во время чтения.                                                                                                                                                                                                                                                                                                       | `4`                                      |
| `min_bytes_for_seek`                                                                                           | Минимальное количество байтов для использования операции seek вместо последовательного чтения.                                                                                                                                                                                                                                                                             | `1 MB`                                   |
| `metadata_path`                                                                                                | Путь в локальной файловой системе для хранения файлов метаданных S3.                                                                                                                                                                                                                                                                                                       | `/var/lib/clickhouse/disks/<disk_name>/` |
| `skip_access_check`                                                                                            | Если `true`, пропускает проверки доступа к диску при запуске.                                                                                                                                                                                                                                                                                                              | `false`                                  |
| `header`                                                                                                       | Добавляет указанный HTTP-заголовок в запросы. Можно указывать несколько раз.                                                                                                                                                                                                                                                                                               | *                                        |
| `server_side_encryption_customer_key_base64`                                                                   | Обязательные заголовки для доступа к объектам S3, зашифрованным с помощью SSE-C.                                                                                                                                                                                                                                                                                           | -                                        |
| `server_side_encryption_kms_key_id`                                                                            | Обязательные заголовки для доступа к объектам S3 с [шифрованием SSE-KMS](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingKMSEncryption.html). Пустая строка означает использование ключа S3, управляемого AWS.                                                                                                                                                  | *                                        |
| `server_side_encryption_kms_encryption_context`                                                                | Заголовок с контекстом шифрования для SSE-KMS (используется с `server_side_encryption_kms_key_id`).                                                                                                                                                                                                                                                                        | -                                        |
| `server_side_encryption_kms_bucket_key_enabled`                                                                | Включает ключи S3 бакета для SSE-KMS (используется с `server_side_encryption_kms_key_id`).                                                                                                                                                                                                                                                                                 | Соответствует настройке бакета           |
| `s3_max_put_rps`                                                                                               | Максимальное число PUT-запросов в секунду до применения ограничения скорости.                                                                                                                                                                                                                                                                                              | `0` (без ограничений)                    |
| `s3_max_put_burst`                                                                                             | Максимальное число одновременных PUT-запросов до достижения лимита RPS.                                                                                                                                                                                                                                                                                                    | То же, что и `s3_max_put_rps`            |
| `s3_max_get_rps`                                                                                               | Максимальное число GET-запросов в секунду до применения ограничения скорости.                                                                                                                                                                                                                                                                                              | `0` (без ограничений)                    |
| `s3_max_get_burst`                                                                                             | Максимальное число одновременных GET-запросов до достижения лимита RPS.                                                                                                                                                                                                                                                                                                    | То же, что и `s3_max_get_rps`            |
| `read_resource`                                                                                                | Имя ресурса, используемого для [планирования](/ru/operations/workload-scheduling.md) запросов на чтение.                                                                                                                                                                                                                                                                      | Пустая строка (отключено)                |
| `write_resource`                                                                                               | Имя ресурса для [планирования](/ru/operations/workload-scheduling.md) запросов записи.                                                                                                                                                                                                                                                                                        | Пустая строка (отключено)                |
| `key_template`                                                                                                 | Задает формат формирования ключей объектов в синтаксисе [re2](https://github.com/google/re2/wiki/Syntax). Требует флаг `storage_metadata_write_full_object_key`. Несовместим с `root path` в параметре `endpoint`. Требует `key_compatibility_prefix`.                                                                                                                     | *                                        |
| `key_compatibility_prefix`                                                                                     | Требуется при использовании `key_template`. Указывает предыдущий `root path` из `endpoint` для чтения старых версий метаданных.                                                                                                                                                                                                                                            | -                                        |
| `read_only`                                                                                                    | Разрешено только чтение с диска.                                                                                                                                                                                                                                                                                                                                           | *                                        |
| :::note                                                                                                        |                                                                                                                                                                                                                                                                                                                                                                            |                                          |
| Google Cloud Storage (GCS) также поддерживается через тип `s3`. См. [GCS backed MergeTree](/ru/integrations/gcs). |                                                                                                                                                                                                                                                                                                                                                                            |                                          |
| :::                                                                                                            |                                                                                                                                                                                                                                                                                                                                                                            |                                          |

<div id="plain-storage">
  ### Использование Plain Storage
</div>

В `22.10` был представлен новый тип диска `s3_plain`, который представляет собой хранилище с однократной записью.
Параметры его конфигурации такие же, как у типа диска `s3`.
В отличие от типа диска `s3`, он хранит данные в исходном виде. Иными словами,
вместо случайно сгенерированных имён blob-объектов он использует обычные имена файлов
(так же, как ClickHouse хранит файлы на локальном диске) и не хранит
метаданные локально. Например, они восстанавливаются по данным на `s3`.

Этот тип диска позволяет хранить статическую версию таблицы, поскольку не
позволяет выполнять слияние существующих данных и не допускает вставку новых
данных. Один из сценариев использования этого типа диска — создание на нём
резервных копий, что можно сделать с помощью
`BACKUP TABLE data TO Disk('plain_disk_name', 'backup_name')`. После этого
можно выполнить `RESTORE TABLE data AS data_restored FROM Disk('plain_disk_name', 'backup_name')`
или использовать `ATTACH TABLE data (...) ENGINE = MergeTree() SETTINGS disk = 'plain_disk_name'`.

Конфигурация:

```xml
<s3_plain>
    <type>s3_plain</type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3_plain>
```

Начиная с версии `24.1`, можно настроить любой диск Объектного хранилища (`s3`, `azure`, `hdfs` (не поддерживается), `local`) с использованием
типа метаданных `plain`.

Конфигурация:

```xml
<s3_plain>
    <type>object_storage</type>
    <object_storage_type>azure</object_storage_type>
    <metadata_type>plain</metadata_type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3_plain>
```

<div id="s3-plain-rewritable-storage">
  ### Использование перезаписываемого хранилища S3 Plain
</div>

Новый тип диска `s3_plain_rewritable` был добавлен в `24.4`.
Как и тип диска `s3_plain`, он не требует дополнительного хранилища для
файлов метаданных. Вместо этого метаданные хранятся в S3.
В отличие от типа диска `s3_plain`, `s3_plain_rewritable` позволяет выполнять слияния
и поддерживает операции `INSERT`.
[Мутации](/ru/sql-reference/statements/alter#mutations) и репликация таблиц не поддерживаются.

Этот тип диска подходит, в частности, для нереплицируемых таблиц `MergeTree`. Хотя
тип диска `s3` подходит для нереплицируемых таблиц `MergeTree`, вы можете выбрать
тип диска `s3_plain_rewritable`, если вам не нужны локальные метаданные
для таблицы и вы готовы мириться с ограниченным набором операций. Это может
быть полезно, например, для системных таблиц.

Конфигурация:

```xml
<s3_plain_rewritable>
    <type>s3_plain_rewritable</type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3_plain_rewritable>
```

равно

```xml
<s3_plain_rewritable>
    <type>object_storage</type>
    <object_storage_type>s3</object_storage_type>
    <metadata_type>plain_rewritable</metadata_type>
    <endpoint>https://s3.eu-west-1.amazonaws.com/clickhouse-eu-west-1.clickhouse.com/data/</endpoint>
    <use_environment_credentials>1</use_environment_credentials>
</s3_plain_rewritable>
```

Начиная с версии `24.5` можно настраивать любой диск объектного хранилища
(`s3`, `azure`, `local`) с типом метаданных `plain_rewritable`.

<div id="azure-blob-storage">
  ### Использование Azure Blob Storage
</div>

Движки таблиц семейства `MergeTree` могут хранить данные в [Azure Blob Storage](https://azure.microsoft.com/en-us/services/storage/blobs/)
с помощью диска типа `azure_blob_storage`.

Конфигурационная разметка:

```xml
<storage_configuration>
    ...
    <disks>
        <blob_storage_disk>
            <type>azure_blob_storage</type>
            <storage_account_url>http://account.blob.core.windows.net</storage_account_url>
            <container_name>container</container_name>
            <account_name>account</account_name>
            <account_key>pass123</account_key>
            <metadata_path>/var/lib/clickhouse/disks/blob_storage_disk/</metadata_path>
            <cache_path>/var/lib/clickhouse/disks/blob_storage_disk/cache/</cache_path>
            <skip_access_check>false</skip_access_check>
        </blob_storage_disk>
    </disks>
    ...
</storage_configuration>
```

<div id="azure-blob-storage-connection-parameters">
  #### Параметры подключения
</div>

| Параметр                            | Описание                                                                                                                                                                                                                                       | Значение по умолчанию |
| ----------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------- |
| `storage_account_url` (обязательно) | URL учётной записи Azure Blob Storage. Примеры: `http://account.blob.core.windows.net` или `http://azurite1:10000/devstoreaccount1`.                                                                                                           | -                     |
| `container_name`                    | Имя целевого контейнера.                                                                                                                                                                                                                       | `default-container`   |
| `container_already_exists`          | Определяет поведение при создании контейнера: <br />- `false`: создаёт новый контейнер <br />- `true`: подключается напрямую к существующему контейнеру <br />- Не задано: проверяет, существует ли контейнер, и при необходимости создаёт его | -                     |

Параметры аутентификации (диск попытается использовать все доступные методы **и** Managed Identity Credential):

| Параметр            | Описание                                                                        |
| ------------------- | ------------------------------------------------------------------------------- |
| `connection_string` | Для аутентификации с использованием строки подключения.                         |
| `account_name`      | Для аутентификации с использованием Shared Key (используется с `account_key`).  |
| `account_key`       | Для аутентификации с использованием Shared Key (используется с `account_name`). |

<div id="azure-blob-storage-limit-parameters">
  #### Параметры ограничений
</div>

| Параметр                             | Описание                                                                         |
| ------------------------------------ | -------------------------------------------------------------------------------- |
| `s3_max_single_part_upload_size`     | Максимальный размер однократной загрузки блока в Blob Storage.                   |
| `min_bytes_for_seek`                 | Минимальный размер области с поддержкой `seek`.                                  |
| `max_single_read_retries`            | Максимальное число попыток прочитать фрагмент данных из Blob Storage.            |
| `max_single_download_retries`        | Максимальное число попыток скачать буфер, доступный для чтения, из Blob Storage. |
| `thread_pool_size`                   | Максимальное число потоков для инициализации `IDiskRemote`.                      |
| `s3_max_inflight_parts_for_one_file` | Максимальное число параллельных PUT-запросов для одного объекта.                 |

<div id="azure-blob-storage-other-parameters">
  #### Другие параметры
</div>

| Параметр                         | Описание                                                                                   | Значение по умолчанию                    |
| -------------------------------- | ------------------------------------------------------------------------------------------ | ---------------------------------------- |
| `metadata_path`                  | Путь в локальной файловой системе для хранения файлов метаданных Blob Storage.             | `/var/lib/clickhouse/disks/<disk_name>/` |
| `skip_access_check`              | Если `true`, пропускает проверку доступа к диску при запуске.                              | `false`                                  |
| `read_resource`                  | Имя ресурса для запросов на чтение при [планировании](/ru/operations/workload-scheduling.md). | Пустая строка (отключено)                |
| `write_resource`                 | Имя ресурса для запросов на запись при [планировании](/ru/operations/workload-scheduling.md). | Пустая строка (отключено)                |
| `metadata_keep_free_space_bytes` | Объём свободного места на диске метаданных, который нужно зарезервировать.                 | -                                        |

Примеры рабочих конфигураций можно найти в каталоге интеграционных тестов (см., например, [test&#95;merge&#95;tree&#95;azure&#95;blob&#95;storage](https://github.com/ClickHouse/ClickHouse/blob/master/tests/integration/test_merge_tree_azure_blob_storage/configs/config.d/storage_conf.xml) или [test&#95;azure&#95;blob&#95;storage&#95;zero&#95;copy&#95;replication](https://github.com/ClickHouse/ClickHouse/blob/master/tests/integration/test_azure_blob_storage_zero_copy_replication/configs/config.d/storage_conf.xml)).

:::note Репликация с нулевым копированием не готова для продакшн
Репликация с нулевым копированием по умолчанию отключена в ClickHouse версии 22.8 и выше. Эта возможность не рекомендуется для использования в продакшн.
:::

<div id="using-hdfs-storage-unsupported">
  ## Использование хранилища HDFS (не поддерживается)
</div>

В этой конфигурации-примере:

* диск имеет тип `hdfs` (не поддерживается)
* данные размещены по адресу `hdfs://hdfs1:9000/clickhouse/`

Обратите внимание: HDFS не поддерживается, поэтому при его использовании возможны проблемы. Если возникнет какая-либо проблема, можете отправить pull request с исправлением.

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <hdfs>
                <type>hdfs</type>
                <endpoint>hdfs://hdfs1:9000/clickhouse/</endpoint>
                <skip_access_check>true</skip_access_check>
            </hdfs>
            <hdd>
                <type>local</type>
                <path>/</path>
            </hdd>
        </disks>
        <policies>
            <hdfs>
                <volumes>
                    <main>
                        <disk>hdfs</disk>
                    </main>
                    <external>
                        <disk>hdd</disk>
                    </external>
                </volumes>
            </hdfs>
        </policies>
    </storage_configuration>
</clickhouse>
```

Имейте в виду, что в отдельных нестандартных сценариях HDFS может не работать.

<div id="encrypted-virtual-file-system">
  ### Использование шифрования данных
</div>

Вы можете шифровать данные, хранящиеся на внешних дисках [S3](/ru/engines/table-engines/mergetree-family/mergetree.md/#table_engine-mergetree-s3) или [HDFS](#using-hdfs-storage-unsupported) (не поддерживается), а также на локальном диске. Чтобы включить режим шифрования, в конфигурационном файле необходимо определить диск с типом `encrypted` и выбрать диск, на котором будут сохраняться данные. Диск `encrypted` шифрует все записываемые файлы на лету, а при чтении файлов с диска `encrypted` автоматически расшифровывает их. Поэтому с диском `encrypted` можно работать так же, как с обычным диском.

Пример конфигурации диска:

```xml
<disks>
  <disk1>
    <type>local</type>
    <path>/path1/</path>
  </disk1>
  <disk2>
    <type>encrypted</type>
    <disk>disk1</disk>
    <path>path2/</path>
    <key>_16_ascii_chars_</key>
  </disk2>
</disks>
```

Например, когда ClickHouse записывает данные из некоторой таблицы в файл `store/all_1_1_0/data.bin` на `disk1`, этот файл фактически записывается на физический диск по пути `/path1/store/all_1_1_0/data.bin`.

При записи того же файла на `disk2` он фактически записывается на физический диск по пути `/path1/path2/store/all_1_1_0/data.bin` в зашифрованном виде.

<div id="required-parameters-encrypted-disk">
  ### Обязательные параметры
</div>

| Parameter | Type   | Description                                                                                                                                                    |
| --------- | ------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `type`    | String | Для создания зашифрованного диска должно быть установлено значение `encrypted`.                                                                                |
| `disk`    | String | Тип диска, используемого для нижележащего хранилища.                                                                                                           |
| `key`     | Uint64 | Ключ для шифрования и дешифрования. Его можно указать в шестнадцатеричном формате с помощью `key_hex`. Несколько ключей можно указать с помощью атрибута `id`. |

<div id="optional-parameters-encrypted-disk">
  ### Необязательные параметры
</div>

| Параметр         | Тип    | По умолчанию     | Описание                                                                                                                                                             |
| ---------------- | ------ | ---------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `path`           | String | Корневой каталог | Расположение на диске, где будут сохраняться данные.                                                                                                                 |
| `current_key_id` | String | -                | Идентификатор ключа, используемого для шифрования. Все указанные ключи могут использоваться для расшифровки.                                                         |
| `algorithm`      | Enum   | `AES_128_CTR`    | Алгоритм шифрования. Варианты: <br />- `AES_128_CTR` (ключ длиной 16 байт) <br />- `AES_192_CTR` (ключ длиной 24 байта) <br />- `AES_256_CTR` (ключ длиной 32 байта) |

Пример конфигурации диска:

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <disk_s3>
                <type>s3</type>
                <endpoint>...
            </disk_s3>
            <disk_s3_encrypted>
                <type>encrypted</type>
                <disk>disk_s3</disk>
                <algorithm>AES_128_CTR</algorithm>
                <key_hex id="0">00112233445566778899aabbccddeeff</key_hex>
                <key_hex id="1">ffeeddccbbaa99887766554433221100</key_hex>
                <current_key_id>1</current_key_id>
            </disk_s3_encrypted>
        </disks>
    </storage_configuration>
</clickhouse>
```

<div id="using-local-cache">
  ### Использование локального кэша
</div>

Начиная с версии 22.3 можно настроить локальный кэш для дисков в конфигурации хранилища.
Для версий 22.3–22.7 кэш поддерживается только для типа диска `s3`. Для версий &gt;= 22.8 кэш поддерживается для любых типов дисков: S3, Azure, Local, Encrypted и т. д.
Для версий &gt;= 23.5 кэш поддерживается только для удалённых типов дисков: S3, Azure, HDFS (не поддерживается).
Кэш использует политику `LRU`.

Пример конфигурации для версий 22.8 и выше:

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <s3>
                <type>s3</type>
                <endpoint>...</endpoint>
                ... s3 configuration ...
            </s3>
            <cache>
                <type>cache</type>
                <disk>s3</disk>
                <path>/s3_cache/</path>
                <max_size>10Gi</max_size>
            </cache>
        </disks>
        <policies>
            <s3_cache>
                <volumes>
                    <main>
                        <disk>cache</disk>
                    </main>
                </volumes>
            </s3_cache>
        <policies>
    </storage_configuration>
```

Пример конфигурации для версий до 22.8:

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <s3>
                <type>s3</type>
                <endpoint>...</endpoint>
                ... s3 configuration ...
                <data_cache_enabled>1</data_cache_enabled>
                <data_cache_max_size>10737418240</data_cache_max_size>
            </s3>
        </disks>
        <policies>
            <s3_cache>
                <volumes>
                    <main>
                        <disk>s3</disk>
                    </main>
                </volumes>
            </s3_cache>
        <policies>
    </storage_configuration>
```

Настройки **конфигурации диска** для File Cache:

Эти настройки следует задавать в разделе конфигурации диска.

| Параметр                              | Тип     | По умолчанию | Описание                                                                                                                                                                                                        |
| ------------------------------------- | ------- | ------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `path`                                | String  | -            | **Обязательно**. Путь к каталогу, где будет храниться кэш.                                                                                                                                                      |
| `max_size`                            | Size    | -            | **Обязательно**. Максимальный размер кэша в байтах или в удобочитаемом формате (например, `10Gi`). При достижении лимита файлы вытесняются по политике LRU. Поддерживаются форматы `ki`, `Mi`, `Gi` (с v22.10). |
| `cache_on_write_operations`           | булевый | `false`      | Включает сквозное кэширование при записи для запросов `INSERT` и фоновых слияний. Может быть переопределено для каждого запроса с помощью `enable_filesystem_cache_on_write_operations`.                        |
| `enable_filesystem_query_cache_limit` | булевый | `false`      | Включает ограничение размера кэша для каждого запроса на основе `max_query_cache_size`.                                                                                                                         |
| `enable_cache_hits_threshold`         | булевый | `false`      | Если включено, данные кэшируются только после нескольких чтений.                                                                                                                                                |
| `cache_hits_threshold`                | Integer | `0`          | Количество чтений, после которого данные будут кэшироваться (требуется `enable_cache_hits_threshold`).                                                                                                          |
| `enable_bypass_cache_with_threshold`  | булевый | `false`      | Пропускает кэш для больших диапазонов чтения.                                                                                                                                                                   |
| `bypass_cache_threshold`              | Size    | `256Mi`      | Размер диапазона чтения, при котором кэш будет пропущен (требуется `enable_bypass_cache_with_threshold`).                                                                                                       |
| `max_file_segment_size`               | Size    | `8Mi`        | Максимальный размер одного файла кэша в байтах или в удобочитаемом формате.                                                                                                                                     |
| `max_elements`                        | Integer | `10000000`   | Максимальное количество файлов кэша.                                                                                                                                                                            |
| `load_metadata_threads`               | Integer | `16`         | Количество потоков для загрузки метаданных кэша при запуске.                                                                                                                                                    |
| `use_split_cache`                     | булевый | `false`      | Использовать раздельный кэш для system/data.                                                                                                                                                                    |
| `split_cache_ratio`                   | Double  | `0.1`        | Отношение системного сегмента к общему размеру кэша для split&#95;cache.                                                                                                                                        |

> **Note**: Значения размера поддерживают единицы измерения, такие как `ki`, `Mi`, `Gi` и т. д. (например, `10Gi`).

<div id="file-cache-query-profile-settings">
  ## Настройки запроса/профиля для File Cache
</div>

| Параметр                                                                | Type    | По умолчанию            | Описание                                                                                                                                                                                |
| ----------------------------------------------------------------------- | ------- | ----------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `enable_filesystem_cache`                                               | Boolean | `true`                  | Включает или отключает использование кэша для отдельного запроса, даже при использовании типа диска `cache`.                                                                            |
| `read_from_filesystem_cache_if_exists_otherwise_bypass_cache`           | Boolean | `false`                 | Если включено, использует кэш только при наличии данных; новые данные кэшироваться не будут.                                                                                            |
| `enable_filesystem_cache_on_write_operations`                           | Boolean | `false` (Cloud: `true`) | Включает сквозное кэширование при записи. Требует `cache_on_write_operations` в конфигурации кэша.                                                                                      |
| `enable_filesystem_cache_log`                                           | Boolean | `false`                 | Включает подробное логирование использования кэша в `system.filesystem_cache_log`.                                                                                                      |
| `filesystem_cache_allow_background_download`                            | Boolean | `true`                  | Разрешает завершать загрузку частично загруженных сегментов в фоновом режиме. Отключите, чтобы загрузка выполнялась в основном потоке для текущего запроса/сеанса.                      |
| `max_query_cache_size`                                                  | Size    | `false`                 | Максимальный размер кэша для одного запроса. Требует `enable_filesystem_query_cache_limit` в конфигурации кэша.                                                                         |
| `filesystem_cache_skip_download_if_exceeds_per_query_cache_write_limit` | Boolean | `true`                  | Определяет поведение при достижении `max_query_cache_size`: <br />- `true`: Прекращает загрузку новых данных <br />- `false`: Вытесняет старые данные, чтобы освободить место для новых |

:::warning
Параметры конфигурации кэша и параметры запросов к кэшу соответствуют последней версии ClickHouse,
в более ранних версиях часть возможностей может не поддерживаться.
:::

<div id="cache-system-tables-file-cache">
  #### Системные таблицы файлового кэша
</div>

| Название таблицы              | Описание                                                                | Требования                                     |
| ----------------------------- | ----------------------------------------------------------------------- | ---------------------------------------------- |
| `system.filesystem_cache`     | Отображает текущее состояние файлового кэша.                            | None                                           |
| `system.filesystem_cache_log` | Показывает подробную статистику использования кэша для каждого запроса. | Требуется `enable_filesystem_cache_log = true` |

<div id="cache-commands-file-cache">
  #### Команды управления кэшем
</div>

<div id="system-clear-filesystem-cache-on-cluster">
  ##### `SYSTEM CLEAR|DROP FILESYSTEM CACHE (<cache_name>) (ON CLUSTER)` -- `ON CLUSTER`
</div>

Эта команда поддерживается только без указания `<cache_name>`

<div id="show-filesystem-caches">
  ##### `SHOW FILESYSTEM CACHES`
</div>

Показать список файловых кэшей, настроенных на сервере.
(Для версий `22.8` и ниже команда называется `SHOW CACHES`)

```sql title="Query"
SHOW FILESYSTEM CACHES
```

```text title="Response"
┌─Caches────┐
│ s3_cache  │
└───────────┘
```

<div id="describe-filesystem-cache">
  ##### `DESCRIBE FILESYSTEM CACHE '<cache_name>'`
</div>

Показать конфигурацию кэша и некоторую общую статистику для указанного кэша.
Имя кэша можно получить с помощью команды `SHOW FILESYSTEM CACHES`. (В версиях
`22.8` и ниже команда называется `DESCRIBE CACHE`)

```sql title="Query"
DESCRIBE FILESYSTEM CACHE 's3_cache'
```

```text title="Response"
┌────max_size─┬─max_elements─┬─max_file_segment_size─┬─boundary_alignment─┬─cache_on_write_operations─┬─cache_hits_threshold─┬─current_size─┬─current_elements─┬─path───────┬─background_download_threads─┬─enable_bypass_cache_with_threshold─┐
│ 10000000000 │      1048576 │             104857600 │            4194304 │                         1 │                    0 │         3276 │               54 │ /s3_cache/ │                           2 │                                  0 │
└─────────────┴──────────────┴───────────────────────┴────────────────────┴───────────────────────────┴──────────────────────┴──────────────┴──────────────────┴────────────┴─────────────────────────────┴────────────────────────────────────┘
```

| Текущие метрики кэша      | Асинхронные метрики кэша | События профиля для кэша                                                                  |
| ------------------------- | ------------------------ | ----------------------------------------------------------------------------------------- |
| `FilesystemCacheSize`     | `FilesystemCacheBytes`   | `CachedReadBufferReadFromSourceBytes`, `CachedReadBufferReadFromCacheBytes`               |
| `FilesystemCacheElements` | `FilesystemCacheFiles`   | `CachedReadBufferReadFromSourceMicroseconds`, `CachedReadBufferReadFromCacheMicroseconds` |
|                           |                          | `CachedReadBufferCacheWriteBytes`, `CachedReadBufferCacheWriteMicroseconds`               |
|                           |                          | `CachedWriteBufferCacheWriteBytes`, `CachedWriteBufferCacheWriteMicroseconds`             |

<div id="web-storage">
  ### Использование статического Web-хранилища (только для чтения)
</div>

Это диск только для чтения. Данные на нем можно только читать, но нельзя изменять. Новая таблица
подключается к этому диску с помощью запроса `ATTACH TABLE` (см. пример ниже). Локальный диск
фактически не используется: каждый запрос `SELECT` приводит к `http`-запросу для
получения необходимых данных. Любая попытка изменить данные таблицы приведет к
исключению, то есть запросы следующих типов не допускаются: [`CREATE TABLE`](/ru/sql-reference/statements/create/table.md),
[`ALTER TABLE`](/ru/sql-reference/statements/alter/index.md), [`RENAME TABLE`](/ru/sql-reference/statements/rename#rename-table),
[`DETACH TABLE`](/ru/sql-reference/statements/detach.md) и [`TRUNCATE TABLE`](/ru/sql-reference/statements/truncate.md).
Web-хранилище можно использовать только для чтения. Например, для размещения
примеров данных или миграции данных. Для этого есть инструмент `clickhouse-static-files-uploader`,
который подготавливает каталог данных для заданной таблицы (`SELECT data_paths FROM system.tables WHERE name = 'table_name'`).
Для каждой нужной таблицы вы получаете каталог с файлами. Эти файлы можно загрузить,
например, на веб-сервер со статическими файлами. После такой подготовки
вы можете подключить эту таблицу к любому серверу ClickHouse через `DiskWeb`.

В этой конфигурации:

* диск имеет тип `web`
* данные размещены по адресу `http://nginx:80/test1/`
* используется кэш в локальном хранилище

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <web>
                <type>web</type>
                <endpoint>http://nginx:80/test1/</endpoint>
            </web>
            <cached_web>
                <type>cache</type>
                <disk>web</disk>
                <path>cached_web_cache/</path>
                <max_size>100000000</max_size>
            </cached_web>
        </disks>
        <policies>
            <web>
                <volumes>
                    <main>
                        <disk>web</disk>
                    </main>
                </volumes>
            </web>
            <cached_web>
                <volumes>
                    <main>
                        <disk>cached_web</disk>
                    </main>
                </volumes>
            </cached_web>
        </policies>
    </storage_configuration>
</clickhouse>
```

:::tip
Хранилище также можно временно настроить прямо в запросе, если веб-датасет
не предполагается использовать регулярно; см. [динамическую конфигурацию](#dynamic-configuration) и не редактируйте
конфигурационный файл.

В GitHub размещён [демо-набор данных](https://github.com/ClickHouse/web-tables-demo). Чтобы подготовить собственные таблицы для веб-
хранилища, см. инструмент [clickhouse-static-files-uploader](/ru/operations/utilities/static-files-disk-uploader)
:::

В этом запросе `ATTACH TABLE` указанный `UUID` соответствует имени каталога с данными, а конечная точка — URL исходного содержимого GitHub.

```sql
-- highlight-next-line
ATTACH TABLE uk_price_paid UUID 'cf712b4f-2ca8-435c-ac23-c4393efe52f7'
(
    price UInt32,
    date Date,
    postcode1 LowCardinality(String),
    postcode2 LowCardinality(String),
    type Enum8('other' = 0, 'terraced' = 1, 'semi-detached' = 2, 'detached' = 3, 'flat' = 4),
    is_new UInt8,
    duration Enum8('unknown' = 0, 'freehold' = 1, 'leasehold' = 2),
    addr1 String,
    addr2 String,
    street LowCardinality(String),
    locality LowCardinality(String),
    town LowCardinality(String),
    district LowCardinality(String),
    county LowCardinality(String)
)
ENGINE = MergeTree
ORDER BY (postcode1, postcode2, addr1, addr2)
  -- highlight-start
  SETTINGS disk = disk(
      type=web,
      endpoint='https://raw.githubusercontent.com/ClickHouse/web-tables-demo/main/web/'
      );
  -- highlight-end
```

Готовый тестовый пример. Вам нужно добавить эту конфигурацию в config:

```xml
<clickhouse>
    <storage_configuration>
        <disks>
            <web>
                <type>web</type>
                <endpoint>https://clickhouse-datasets.s3.yandex.net/disk-with-static-files-tests/test-hits/</endpoint>
            </web>
        </disks>
        <policies>
            <web>
                <volumes>
                    <main>
                        <disk>web</disk>
                    </main>
                </volumes>
            </web>
        </policies>
    </storage_configuration>
</clickhouse>
```

Затем выполните следующий запрос:

```sql
ATTACH TABLE test_hits UUID '1ae36516-d62d-4218-9ae3-6516d62da218'
(
    WatchID UInt64,
    JavaEnable UInt8,
    Title String,
    GoodEvent Int16,
    EventTime DateTime,
    EventDate Date,
    CounterID UInt32,
    ClientIP UInt32,
    ClientIP6 FixedString(16),
    RegionID UInt32,
    UserID UInt64,
    CounterClass Int8,
    OS UInt8,
    UserAgent UInt8,
    URL String,
    Referer String,
    URLDomain String,
    RefererDomain String,
    Refresh UInt8,
    IsRobot UInt8,
    RefererCategories Array(UInt16),
    URLCategories Array(UInt16),
    URLRegions Array(UInt32),
    RefererRegions Array(UInt32),
    ResolutionWidth UInt16,
    ResolutionHeight UInt16,
    ResolutionDepth UInt8,
    FlashMajor UInt8,
    FlashMinor UInt8,
    FlashMinor2 String,
    NetMajor UInt8,
    NetMinor UInt8,
    UserAgentMajor UInt16,
    UserAgentMinor FixedString(2),
    CookieEnable UInt8,
    JavascriptEnable UInt8,
    IsMobile UInt8,
    MobilePhone UInt8,
    MobilePhoneModel String,
    Params String,
    IPNetworkID UInt32,
    TraficSourceID Int8,
    SearchEngineID UInt16,
    SearchPhrase String,
    AdvEngineID UInt8,
    IsArtifical UInt8,
    WindowClientWidth UInt16,
    WindowClientHeight UInt16,
    ClientTimeZone Int16,
    ClientEventTime DateTime,
    SilverlightVersion1 UInt8,
    SilverlightVersion2 UInt8,
    SilverlightVersion3 UInt32,
    SilverlightVersion4 UInt16,
    PageCharset String,
    CodeVersion UInt32,
    IsLink UInt8,
    IsDownload UInt8,
    IsNotBounce UInt8,
    FUniqID UInt64,
    HID UInt32,
    IsOldCounter UInt8,
    IsEvent UInt8,
    IsParameter UInt8,
    DontCountHits UInt8,
    WithHash UInt8,
    HitColor FixedString(1),
    UTCEventTime DateTime,
    Age UInt8,
    Sex UInt8,
    Income UInt8,
    Interests UInt16,
    Robotness UInt8,
    GeneralInterests Array(UInt16),
    RemoteIP UInt32,
    RemoteIP6 FixedString(16),
    WindowName Int32,
    OpenerName Int32,
    HistoryLength Int16,
    BrowserLanguage FixedString(2),
    BrowserCountry FixedString(2),
    SocialNetwork String,
    SocialAction String,
    HTTPError UInt16,
    SendTiming Int32,
    DNSTiming Int32,
    ConnectTiming Int32,
    ResponseStartTiming Int32,
    ResponseEndTiming Int32,
    FetchTiming Int32,
    RedirectTiming Int32,
    DOMInteractiveTiming Int32,
    DOMContentLoadedTiming Int32,
    DOMCompleteTiming Int32,
    LoadEventStartTiming Int32,
    LoadEventEndTiming Int32,
    NSToDOMContentLoadedTiming Int32,
    FirstPaintTiming Int32,
    RedirectCount Int8,
    SocialSourceNetworkID UInt8,
    SocialSourcePage String,
    ParamPrice Int64,
    ParamOrderID String,
    ParamCurrency FixedString(3),
    ParamCurrencyID UInt16,
    GoalsReached Array(UInt32),
    OpenstatServiceName String,
    OpenstatCampaignID String,
    OpenstatAdID String,
    OpenstatSourceID String,
    UTMSource String,
    UTMMedium String,
    UTMCampaign String,
    UTMContent String,
    UTMTerm String,
    FromTag String,
    HasGCLID UInt8,
    RefererHash UInt64,
    URLHash UInt64,
    CLID UInt32,
    YCLID UInt64,
    ShareService String,
    ShareURL String,
    ShareTitle String,
    ParsedParams Nested(
        Key1 String,
        Key2 String,
        Key3 String,
        Key4 String,
        Key5 String,
        ValueDouble Float64),
    IslandID FixedString(16),
    RequestNum UInt32,
    RequestTry UInt8
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(EventDate)
ORDER BY (CounterID, EventDate, intHash32(UserID))
SAMPLE BY intHash32(UserID)
SETTINGS storage_policy='web';
```

<div id="required-parameters-s3">
  #### Обязательные параметры
</div>

| Параметр   | Описание                                                                                                                 |
| ---------- | ------------------------------------------------------------------------------------------------------------------------ |
| `type`     | `web`. В противном случае диск не будет создан.                                                                          |
| `endpoint` | URL конечной точки в формате `path`. URL конечной точки должен содержать корневой путь, где хранятся загруженные данные. |

<div id="optional-parameters-s3">
  #### Необязательные параметры
</div>

| Параметр                            | Описание                                                                                       | Значение по умолчанию |
| ----------------------------------- | ---------------------------------------------------------------------------------------------- | --------------------- |
| `min_bytes_for_seek`                | Минимальное число байт, при котором используется операция seek вместо последовательного чтения | `1` MB                |
| `remote_fs_read_backoff_threashold` | Максимальное время ожидания при попытке чтения данных с удалённого диска                       | `10000` секунд        |
| `remote_fs_read_backoff_max_tries`  | Максимальное количество попыток чтения с задержкой                                             | `5`                   |

Если запрос завершается исключением `DB:Exception Unreachable URL`, попробуйте скорректировать настройки: [http&#95;connection&#95;timeout](/ru/operations/settings/settings.md/#http_connection_timeout), [http&#95;receive&#95;timeout](/ru/operations/settings/settings.md/#http_receive_timeout), [keep&#95;alive&#95;timeout](/ru/operations/server-configuration-parameters/settings#keep_alive_timeout).

Чтобы получить файлы для загрузки, выполните:
`clickhouse static-files-disk-uploader --metadata-path <path> --output-dir <dir>` (`--metadata-path` можно найти с помощью запроса `SELECT data_paths FROM system.tables WHERE name = 'table_name'`).

При загрузке файлов через `endpoint` их нужно помещать в путь `<endpoint>/store/`, но в config должен быть указан только `endpoint`.

Если при загрузке диска URL недоступен в момент запуска таблиц сервером, все ошибки перехватываются. Если в этом случае возникли ошибки, таблицы можно перезагрузить (снова сделать видимыми) с помощью `DETACH TABLE table_name` -&gt; `ATTACH TABLE table_name`. Если метаданные были успешно загружены при запуске сервера, таблицы становятся доступны сразу.

Используйте настройку [http&#95;max&#95;single&#95;read&#95;retries](/ru/operations/storing-data#web-storage), чтобы ограничить максимальное количество повторных попыток в рамках одного HTTP-чтения.

<div id="zero-copy">
  ### Репликация с нулевым копированием (не готова к использованию в продакшне)
</div>

Репликация с нулевым копированием возможна, но не рекомендуется для дисков `S3` и `HDFS` (не поддерживаются). Репликация с нулевым копированием означает, что если данные удалённо хранятся на нескольких машинах и их нужно синхронизировать, реплицируются только метаданные (пути к частям данных), а не сами данные.

:::note Репликация с нулевым копированием не готова к использованию в продакшне
В ClickHouse версии 22.8 и выше репликация с нулевым копированием по умолчанию отключена. Эту возможность не рекомендуется использовать в продакшне.
:::