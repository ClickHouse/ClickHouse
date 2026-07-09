---
description: 'Расширение табличной функции s3, которое позволяет параллельно обрабатывать
  файлы из Amazon S3 и Google Cloud Storage на множестве узлов указанного кластера.'
sidebar_label: 's3Cluster'
sidebar_position: 181
slug: /sql-reference/table-functions/s3Cluster
title: 's3Cluster'
doc_type: 'reference'
---

Это расширение табличной функции [s3](/ru/sql-reference/table-functions/s3.md).

Позволяет параллельно обрабатывать файлы из [Amazon S3](https://aws.amazon.com/s3/) и Google Cloud Storage [Google Cloud Storage](https://cloud.google.com/storage/) на множестве узлов указанного кластера. На узле-инициаторе создаётся connection ко всем узлам кластера, разворачиваются звёздочки в пути к файлам S3, после чего каждый файл динамически распределяется. На узле-воркере запрашивается у инициатора следующая task для обработки, и она выполняется. Это повторяется, пока не будут завершены все tasks.

<div id="syntax">
  ## Синтаксис
</div>

```sql
s3Cluster(cluster_name, url[, NOSIGN | access_key_id, secret_access_key,[session_token]][, format][, structure][, compression_method][, headers][, extra_credentials])
s3Cluster(cluster_name, named_collection[, option=value [,..]])
```

<div id="arguments">
  ## Аргументы
</div>

| Argument                                | Description                                                                                                                                                                                                                                                                                                                     |
| --------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `cluster_name`                          | Имя кластера, используемое для формирования набора адресов и параметров подключения к удаленным и локальным серверам.                                                                                                                                                                                                           |
| `url`                                   | Путь к файлу или набору файлов. Поддерживает следующие подстановочные шаблоны в режиме только для чтения: `*`, `**`, `?`, `{'abc','def'}` и `{N..M}`, где `N`, `M` — числа, `abc`, `def` — строки. Дополнительные сведения см. в разделе [Wildcards In Path](../../engines/table-engines/integrations/s3.md#wildcards-in-path). |
| `NOSIGN`                                | Если вместо учетных данных указано это ключевое слово, все запросы будут отправляться без подписи.                                                                                                                                                                                                                              |
| `access_key_id` and `secret_access_key` | Ключи, задающие учетные данные для использования с указанной конечной точкой. Необязательно.                                                                                                                                                                                                                                    |
| `session_token`                         | Токен сеанса для использования с указанными ключами. Необязателен при передаче ключей.                                                                                                                                                                                                                                          |
| `format`                                | [format](/ru/sql-reference/formats) файла.                                                                                                                                                                                                                                                                                         |
| `structure`                             | Структура таблицы. Формат: `'column1_name column1_type, column2_name column2_type, ...'`.                                                                                                                                                                                                                                       |
| `compression_method`                    | Параметр необязателен. Поддерживаемые значения: `none`, `gzip` или `gz`, `brotli` или `br`, `xz` или `LZMA`, `zstd` или `zst`. По умолчанию метод сжатия определяется автоматически по расширению файла.                                                                                                                        |
| `headers`                               | Параметр необязателен. Позволяет передавать заголовки в запросе к S3. Передавайте в формате `headers(key=value)`, например `headers('x-amz-request-payer' = 'requester')`. Пример использования см. [здесь](/ru/sql-reference/table-functions/s3#accessing-requester-pays-buckets).                                                |
| `extra_credentials`                     | Необязательно. Через этот параметр можно передать `roleARN`. Пример см. [здесь](/ru/cloud/data-sources/secure-s3#access-your-s3-bucket-with-the-clickhouseaccess-role).                                                                                                                                                            |

Аргументы также можно передавать с помощью [именованных коллекций](/ru/operations/named-collections.md). В этом случае `url`, `access_key_id`, `secret_access_key`, `format`, `structure`, `compression_method` работают так же, и поддерживаются некоторые дополнительные параметры:

| Argument                      | Description                                                                                                                                                                                                                                      |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `filename`                    | Добавляется к URL, если указан.                                                                                                                                                                                                                  |
| `use_environment_credentials` | Включен по умолчанию, позволяет передавать дополнительные параметры через переменные окружения `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`, `AWS_CONTAINER_CREDENTIALS_FULL_URI`, `AWS_CONTAINER_AUTHORIZATION_TOKEN`, `AWS_EC2_METADATA_DISABLED`. |
| `no_sign_request`             | Отключен по умолчанию.                                                                                                                                                                                                                           |
| `expiration_window_seconds`   | Значение по умолчанию — 120.                                                                                                                                                                                                                     |

<div id="returned_value">
  ## Возвращаемое значение
</div>

Таблица с указанной структурой для чтения данных из указанного файла или записи данных в него.

<div id="examples">
  ## Примеры
</div>

Выберите данные из всех файлов в каталогах `/root/data/clickhouse` и `/root/data/database/`, задействовав все узлы кластера `cluster_simple`:

```sql
SELECT * FROM s3Cluster(
    'cluster_simple',
    'http://minio1:9001/root/data/{clickhouse,database}/*',
    'minio',
    'ClickHouse_Minio_P@ssw0rd',
    'CSV',
    'name String, value UInt32, polygon Array(Array(Tuple(Float64, Float64)))'
) ORDER BY (name, value, polygon);
```

Подсчитайте общее количество строк во всех файлах в кластере `cluster_simple`:

:::tip
Если в вашем списке файлов есть числовые диапазоны с ведущими нулями, используйте конструкцию с фигурными скобками для каждой цифры отдельно или символ `?`.
:::

Для использования в продакшне рекомендуется применять [именованные коллекции](/ru/operations/named-collections.md). Вот пример:

```sql

CREATE NAMED COLLECTION creds AS
        access_key_id = 'minio',
        secret_access_key = 'ClickHouse_Minio_P@ssw0rd';
SELECT count(*) FROM s3Cluster(
    'cluster_simple', creds, url='https://s3-object-url.csv',
    format='CSV', structure='name String, value UInt32, polygon Array(Array(Tuple(Float64, Float64)))'
)
```

<div id="accessing-private-and-public-buckets">
  ## Доступ к приватным и публичным бакетам
</div>

Пользователи могут использовать те же подходы, что и для функции s3, которые описаны [здесь](/ru/sql-reference/table-functions/s3#accessing-public-buckets).

<div id="optimizing-performance">
  ## Оптимизация производительности
</div>

Подробнее об оптимизации производительности функции s3 см. в [нашем подробном руководстве](/ru/integrations/s3/performance).

<div id="related">
  ## См. также
</div>

* [Движок S3](../../engines/table-engines/integrations/s3.md)
* [Табличная функция s3](../../sql-reference/table-functions/s3.md)