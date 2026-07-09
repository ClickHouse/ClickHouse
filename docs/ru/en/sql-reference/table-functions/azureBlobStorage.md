---
description: 'Предоставляет табличный интерфейс для select/вставки файлов в Azure Blob
  Storage. Аналогична функции s3.'
keywords: ['azure blob storage']
sidebar_label: 'azureBlobStorage'
sidebar_position: 10
slug: /sql-reference/table-functions/azureBlobStorage
title: 'azureBlobStorage'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

<div id="azureblobstorage-table-function">
  # Табличная функция azureBlobStorage
</div>

Предоставляет интерфейс в виде таблицы для чтения и вставки файлов в [Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs). Эта табличная функция аналогична [функции s3](../../sql-reference/table-functions/s3.md).

<div id="syntax">
  ## Синтаксис
</div>

<Tabs>
  <TabItem value="connection_string" label="Строка подключения" default>
    Учетные данные встроены в строку подключения, поэтому отдельные `account_name`/`account_key` не требуются:

    ```sql
    azureBlobStorage(connection_string, container_name, blobpath [, format, compression, structure])
    ```
  </TabItem>

  <TabItem value="storage_account_url" label="URL учетной записи хранилища">
    Требуются отдельные аргументы `account_name` и `account_key`:

    ```sql
    azureBlobStorage(storage_account_url, container_name, blobpath, account_name, account_key [, format, compression, structure])
    ```
  </TabItem>

  <TabItem value="named_collection" label="Именованная коллекция">
    Полный список поддерживаемых ключей см. ниже в разделе [Именованные коллекции](#named-collections):

    ```sql
    azureBlobStorage(named_collection[, option=value [,..]])
    ```
  </TabItem>
</Tabs>

<div id="arguments">
  ## Аргументы
</div>

| Аргумент                         | Описание                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
| -------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `connection_string`              | Строка подключения со встроенными учетными данными (имя учётной записи хранилища + ключ аккаунта или SAS token). При использовании этой формы `account_name` и `account_key` **не** следует передавать отдельно. См. [Настройка строки подключения](https://learn.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string?toc=%2Fazure%2Fstorage%2Fblobs%2Ftoc.json\&bc=%2Fazure%2Fstorage%2Fblobs%2Fbreadcrumb%2Ftoc.json#configure-a-connection-string-for-an-azure-storage-account).                                                                             |
| `storage_account_url`            | URL конечной точки учётной записи хранилища, например `https://myaccount.blob.core.windows.net/`. При использовании этой формы **необходимо** также передать `account_name` и `account_key`.                                                                                                                                                                                                                                                                                                                                                                                   |
| `container_name`                 | Имя контейнера.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          |
| `blobpath`                       | Путь к файлу. Поддерживает следующие подстановочные шаблоны в режиме только для чтения: `*`, `**`, `?`, `{abc,def}` и `{N..M}`, где `N`, `M` — числа, `'abc'`, `'def'` — строки.                                                                                                                                                                                                                                                                                                                                                                                         |
| `account_name`                   | Имя учётной записи хранилища. **Обязательно** при использовании `storage_account_url` без SAS; **не** должно передаваться при использовании `connection_string`.                                                                                                                                                                                                                                                                                                                                                                                                               |
| `account_key`                    | Ключ учётной записи хранилища. **Обязательно** при использовании `storage_account_url` без SAS; **не** должен передаваться при использовании `connection_string`.                                                                                                                                                                                                                                                                                                                                                                                                              |
| `format`                         | [Формат](/ru/sql-reference/formats) файла.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  |
| `compression`                    | Поддерживаемые значения: `none`, `gzip/gz`, `brotli/br`, `xz/LZMA`, `zstd/zst`. По умолчанию сжатие определяется автоматически по расширению файла (то же самое, что и установка `auto`).                                                                                                                                                                                                                                                                                                                                                                                |
| `structure`                      | Структура таблицы. Формат: `'column1_name column1_type, column2_name column2_type, ...'`.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `partition_strategy`             | Необязательно. Поддерживаемые значения: `WILDCARD` или `HIVE`. Для `WILDCARD` требуется `{_partition_id}` в пути, который заменяется на ключ партиционирования. `HIVE` не допускает подстановочных шаблонов, предполагает, что путь является корнем таблицы, и создает директории партиций в стиле Hive, где в качестве имен файлов используются Snowflake IDs, а в качестве расширения — формат файла. По умолчанию используется настройка `file_like_engine_default_partition_strategy` (`WILDCARD` при значении настройки `compatibility` ниже `26.6`, иначе `HIVE`). |
| `partition_columns_in_data_file` | Необязательно. Используется только со стратегией партиционирования `HIVE`. Указывает ClickHouse, следует ли ожидать, что столбцы партиции будут записаны в файл данных. По умолчанию `false`.                                                                                                                                                                                                                                                                                                                                                                            |
| `extra_credentials`              | Использует `client_id` и `tenant_id` для аутентификации. Если указаны `extra_credentials`, они имеют приоритет над `account_name` и `account_key`.                                                                                                                                                                                                                                                                                                                                                                                                                       |

<div id="named-collections">
  ## Именованные коллекции
</div>

Аргументы также можно передавать с помощью [именованных коллекций](/ru/operations/named-collections). В этом случае поддерживаются следующие ключи:

| Key                   | Required | Description                                                                                                                   |
| --------------------- | -------- | ----------------------------------------------------------------------------------------------------------------------------- |
| `container`           | Yes      | Имя контейнера. Соответствует позиционному аргументу `container_name`.                                                        |
| `blob_path`           | Yes      | Путь к файлу (с необязательными подстановочными шаблонами). Соответствует позиционному аргументу `blobpath`.                  |
| `connection_string`   | No*      | Строка подключения со встроенными учетными данными. *Необходимо указать либо `connection_string`, либо `storage_account_url`. |
| `storage_account_url` | No*      | URL конечной точки учётной записи хранилища. *Необходимо указать либо `connection_string`, либо `storage_account_url`.              |
| `account_name`        | No       | Обязательно при использовании `storage_account_url`                                                                           |
| `account_key`         | No       | Обязательно при использовании `storage_account_url`                                                                           |
| `format`              | No       | Формат файла.                                                                                                                 |
| `compression`         | No       | Тип сжатия.                                                                                                                   |
| `structure`           | No       | Структура таблицы.                                                                                                            |
| `client_id`           | No       | Идентификатор клиента для аутентификации.                                                                                     |
| `tenant_id`           | No       | Идентификатор тенанта для аутентификации.                                                                                     |

:::note
Имена ключей в именованной коллекции отличаются от имен позиционных аргументов функции: `container` (не `container_name`) и `blob_path` (не `blobpath`).
:::

**Пример:**

```sql
CREATE NAMED COLLECTION azure_my_data AS
    storage_account_url = 'https://myaccount.blob.core.windows.net/',
    container = 'mycontainer',
    blob_path = 'data/*.parquet',
    account_name = 'myaccount',
    account_key = 'mykey...==',
    format = 'Parquet';

SELECT *
FROM azureBlobStorage(azure_my_data)
LIMIT 5;
```

Вы также можете переопределить значения именованной коллекции при выполнении запроса:

```sql
SELECT *
FROM azureBlobStorage(azure_my_data, blob_path = 'other_data/*.csv', format = 'CSVWithNames')
LIMIT 5;
```

<div id="returned_value">
  ## Возвращаемое значение
</div>

Таблица с указанной структурой для чтения данных из указанного файла или записи в него.

<div id="examples">
  ## Примеры
</div>

<div id="reading-with-storage-account-url">
  ### Чтение с использованием формы `storage_account_url`
</div>

```sql
SELECT *
FROM azureBlobStorage(
    'https://myaccount.blob.core.windows.net/',
    'mycontainer',
    'data/*.parquet',
    'myaccount',
    'mykey...==',
    'Parquet'
)
LIMIT 5;
```

<div id="reading-with-connection-string">
  ### Чтение с использованием `connection_string`
</div>

```sql
SELECT *
FROM azureBlobStorage(
    'DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=mykey...==;EndPointSuffix=core.windows.net',
    'mycontainer',
    'data/*.csv',
    'CSVWithNames'
)
LIMIT 5;
```

<div id="writing-with-partitions">
  ### Запись с использованием партиций
</div>

```sql
INSERT INTO TABLE FUNCTION azureBlobStorage(
    'DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=mykey...==;EndPointSuffix=core.windows.net',
    'mycontainer',
    'test_{_partition_id}.csv',
    'CSV',
    'auto',
    'column1 UInt32, column2 UInt32, column3 UInt32'
) PARTITION BY column3
VALUES (1, 2, 3), (3, 2, 1), (78, 43, 3);
```

Затем прочитайте конкретную партицию:

```sql
SELECT *
FROM azureBlobStorage(
    'DefaultEndpointsProtocol=https;AccountName=myaccount;AccountKey=mykey...==;EndPointSuffix=core.windows.net',
    'mycontainer',
    'test_1.csv',
    'CSV',
    'auto',
    'column1 UInt32, column2 UInt32, column3 UInt32'
);
```

```response
┌─column1─┬─column2─┬─column3─┐
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

<div id="virtual-columns">
  ## Виртуальные столбцы
</div>

* `_path` — Путь к файлу. Тип: `LowCardinality(String)`.
* `_file` — Имя файла. Тип: `LowCardinality(String)`.
* `_size` — Размер файла в байтах. Тип: `Nullable(UInt64)`. Если размер файла неизвестен, значение — `NULL`.
* `_time` — Время последнего изменения файла. Тип: `Nullable(DateTime)`. Если время неизвестно, значение — `NULL`.

<div id="partitioned-write">
  ## Запись с разбиением по партициям
</div>

<div id="partition-strategy">
  ### Стратегия партиционирования
</div>

Поддерживается только для запросов `INSERT`.

`WILDCARD`: заменяет подстановочный знак `{_partition_id}` в пути к файлу на фактический ключ партиционирования. Выбирается по умолчанию только при значениях настройки `compatibility` ниже `26.6`; в противном случае по умолчанию используется `HIVE` (см. настройку `file_like_engine_default_partition_strategy`).

`HIVE` реализует секционирование в стиле Hive для чтения и записи. Файлы создаются в следующем формате: `<prefix>/<key1=val1/key2=val2...>/<snowflakeid>.<toLower(file_format)>`.

**Пример стратегии партиционирования `HIVE`**

```sql
INSERT INTO TABLE FUNCTION azureBlobStorage(
    azure_conf2,
    storage_account_url = 'https://myaccount.blob.core.windows.net/',
    container = 'cont',
    blob_path = 'azure_table_root',
    format = 'CSVWithNames',
    compression = 'auto',
    structure = 'year UInt16, country String, id Int32',
    partition_strategy = 'hive'
) PARTITION BY (year, country)
VALUES (2020, 'Russia', 1), (2021, 'Brazil', 2);
```

```result
SELECT _path, * FROM azureBlobStorage(
    azure_conf2,
    storage_account_url = 'https://myaccount.blob.core.windows.net/',
    container = 'cont',
    blob_path = 'azure_table_root/**.csvwithnames'
)

   ┌─_path───────────────────────────────────────────────────────────────────────────┬─id─┬─year─┬─country─┐
1. │ cont/azure_table_root/year=2021/country=Brazil/7351307847391293440.csvwithnames │  2 │ 2021 │ Brazil  │
2. │ cont/azure_table_root/year=2020/country=Russia/7351307847378710528.csvwithnames │  1 │ 2020 │ Russia  │
   └─────────────────────────────────────────────────────────────────────────────────┴────┴──────┴─────────┘
```

<div id="hive-style-partitioning">
  ## Настройка use_hive_partitioning
</div>

Это подсказка для ClickHouse, позволяющая разбирать файлы с секционированием в стиле Hive при чтении. На запись она не влияет. Для симметричных операций чтения и записи используйте аргумент `partition_strategy`.

Когда настройка `use_hive_partitioning` установлена в 1, ClickHouse обнаруживает секционирование в стиле Hive в пути (`/name=value/`) и позволяет использовать столбцы партиции как виртуальные столбцы в запросе. Эти виртуальные столбцы будут иметь те же имена, что и в секционированном пути.

**Пример**

Использование виртуального столбца, созданного с помощью секционирования в стиле Hive

```sql
SELECT * FROM azureBlobStorage(config, storage_account_url='...', container='...', blob_path='http://data/path/date=*/country=*/code=*/*.parquet') WHERE date > '2020-01-01' AND country = 'Netherlands' AND code = 42;
```

<div id="using-shared-access-signatures-sas-sas-tokens">
  ## Использование Shared Access Signatures (SAS)
</div>

Shared Access Signature (SAS) — это URI, который предоставляет ограниченный доступ к контейнеру или файлу в Azure Storage. Используйте его, чтобы предоставить ограниченный по времени доступ к ресурсам учётной записи хранилища, не передавая ключ этой учётной записи. Подробнее [здесь](https://learn.microsoft.com/en-us/rest/api/storageservices/delegate-access-with-shared-access-signature).

Функция `azureBlobStorage` поддерживает Shared Access Signatures (SAS).

[Токен Blob SAS](https://learn.microsoft.com/en-us/azure/ai-services/translator/document-translation/how-to-guides/create-sas-tokens?tabs=Containers) содержит всю информацию, необходимую для аутентификации запроса, включая целевой blob-объект, разрешения и срок действия. Чтобы сформировать URL blob-объекта, добавьте токен SAS к конечной точке сервиса Blob. Например, если конечная точка — `https://clickhousedocstest.blob.core.windows.net/`, запрос будет таким:

```sql
SELECT count()
FROM azureBlobStorage('BlobEndpoint=https://clickhousedocstest.blob.core.windows.net/;SharedAccessSignature=sp=r&st=2025-01-29T14:58:11Z&se=2025-01-29T22:58:11Z&spr=https&sv=2022-11-02&sr=c&sig=Ac2U0xl4tm%2Fp7m55IilWl1yHwk%2FJG0Uk6rMVuOiD0eE%3D', 'exampledatasets', 'example.csv')

┌─count()─┐
│      10 │
└─────────┘

1 row in set. Elapsed: 0.425 sec.
```

В качестве альтернативы можно использовать сгенерированный [Blob SAS URL](https://learn.microsoft.com/en-us/azure/ai-services/translator/document-translation/how-to-guides/create-sas-tokens?tabs=Containers):

```sql
SELECT count()
FROM azureBlobStorage('https://clickhousedocstest.blob.core.windows.net/?sp=r&st=2025-01-29T14:58:11Z&se=2025-01-29T22:58:11Z&spr=https&sv=2022-11-02&sr=c&sig=Ac2U0xl4tm%2Fp7m55IilWl1yHwk%2FJG0Uk6rMVuOiD0eE%3D', 'exampledatasets', 'example.csv')

┌─count()─┐
│      10 │
└─────────┘

1 row in set. Elapsed: 0.153 sec.
```

<div id="related">
  ## См. также
</div>

* [Табличный движок AzureBlobStorage](/ru/engines/table-engines/integrations/azureBlobStorage.md)