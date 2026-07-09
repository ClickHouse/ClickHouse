---
description: 'Предоставляет интерфейс, похожий на таблицу, для `SELECT` и `INSERT`
  данных из Google Cloud Storage. Требуется роль IAM `Storage Object User`.'
keywords: ['gcs', 'бакет']
sidebar_label: 'gcs'
sidebar_position: 70
slug: /sql-reference/table-functions/gcs
title: 'gcs'
doc_type: 'reference'
---

Предоставляет интерфейс, похожий на таблицу, для `SELECT` и `INSERT` данных из [Google Cloud Storage](https://cloud.google.com/storage/). Требуется [роль IAM `Storage Object User`](https://cloud.google.com/storage/docs/access-control/iam-roles).

Это псевдоним [табличной функции s3](../../sql-reference/table-functions/s3.md).

Если в вашем кластере несколько реплик, для распараллеливания вставок можно использовать [функцию s3Cluster](../../sql-reference/table-functions/s3Cluster.md) (она работает с GCS).

<div id="syntax">
  ## Синтаксис
</div>

```sql
gcs(url [, NOSIGN | hmac_key, hmac_secret] [,format] [,structure] [,compression_method])
gcs(named_collection[, option=value [,..]])
```

:::tip GCS
Табличная функция GCS интегрируется с Google Cloud Storage через GCS XML API и ключи HMAC.
Подробнее о конечной точке и HMAC см. в [документации Google по обеспечению совместимости](https://cloud.google.com/storage/docs/interoperability).
:::

<div id="arguments">
  ## Аргументы
</div>

| Аргумент                   | Описание                                                                                                                                                                                                 |
| -------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `url`                      | Путь к файлу в бакете. В режиме только для чтения поддерживаются следующие подстановочные шаблоны: `*`, `**`, `?`, `{abc,def}` и `{N..M}`, где `N`, `M` — числа, `'abc'`, `'def'` — строки.              |
| `NOSIGN`                   | Если это ключевое слово указано вместо учетных данных, все запросы будут отправляться без подписи.                                                                                                       |
| `hmac_key` и `hmac_secret` | Ключи, задающие учетные данные для использования с указанной конечной точкой. Необязательны.                                                                                                             |
| `format`                   | [Формат](/ru/sql-reference/formats) файла.                                                                                                                                                                  |
| `structure`                | Структура таблицы. Формат: `'column1_name column1_type, column2_name column2_type, ...'`.                                                                                                                |
| `compression_method`       | Параметр необязателен. Поддерживаемые значения: `none`, `gzip` или `gz`, `brotli` или `br`, `xz` или `LZMA`, `zstd` или `zst`. По умолчанию метод сжатия определяется автоматически по расширению файла. |

:::note GCS
Путь GCS имеет такой формат, поскольку конечная точка Google XML API отличается от JSON API:

```text
  https://storage.googleapis.com/<bucket>/<folder>/<filename(s)>
```

а не ~~https://storage.cloud.google.com~~.
:::

Аргументы также можно передавать с помощью [именованных коллекций](/ru/operations/named-collections.md). В этом случае `url`, `format`, `structure`, `compression_method` работают так же, также поддерживаются некоторые дополнительные параметры:

| Параметр                      | Описание                                                                                                                                                                                                                                         |
| ----------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `access_key_id`               | `hmac_key`, необязательный.                                                                                                                                                                                                                      |
| `secret_access_key`           | `hmac_secret`, необязательный.                                                                                                                                                                                                                   |
| `filename`                    | Если указан, добавляется к `url`.                                                                                                                                                                                                                |
| `use_environment_credentials` | Включен по умолчанию; позволяет передавать дополнительные параметры через переменные окружения `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI`, `AWS_CONTAINER_CREDENTIALS_FULL_URI`, `AWS_CONTAINER_AUTHORIZATION_TOKEN`, `AWS_EC2_METADATA_DISABLED`. |
| `no_sign_request`             | Отключен по умолчанию.                                                                                                                                                                                                                           |
| `expiration_window_seconds`   | Значение по умолчанию — 120.                                                                                                                                                                                                                     |

<div id="returned_value">
  ## Возвращаемое значение
</div>

Таблица указанной структуры для чтения или записи данных в указанный файл.

<div id="examples">
  ## Примеры
</div>

Выберите первые две строки из файла GCS `https://storage.googleapis.com/clickhouse_public_datasets/my-test-bucket-768/data.csv.gz`. Метод сжатия определяется автоматически по расширению файла `.gz`:

```sql
SELECT *
FROM gcs('https://storage.googleapis.com/clickhouse_public_datasets/my-test-bucket-768/data.csv.gz', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
LIMIT 2;
```

```text
┌─column1─┬─column2─┬─column3─┐
│       1 │       2 │       3 │
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

Тот же запрос, что и выше, но с явно указанным методом сжатия `gzip` вместо автоопределения:

```sql
SELECT *
FROM gcs('https://storage.googleapis.com/clickhouse_public_datasets/my-test-bucket-768/data.csv.gz', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32', 'gzip')
LIMIT 2;
```

```text
┌─column1─┬─column2─┬─column3─┐
│       1 │       2 │       3 │
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

<div id="usage">
  ## Использование
</div>

Предположим, что у нас есть несколько файлов со следующими URI в GCS:

* &#39;https://storage.googleapis.com/my-test-bucket-768/some&#95;prefix/some&#95;file&#95;1.csv&#39;
* &#39;https://storage.googleapis.com/my-test-bucket-768/some&#95;prefix/some&#95;file&#95;2.csv&#39;
* &#39;https://storage.googleapis.com/my-test-bucket-768/some&#95;prefix/some&#95;file&#95;3.csv&#39;
* &#39;https://storage.googleapis.com/my-test-bucket-768/some&#95;prefix/some&#95;file&#95;4.csv&#39;
* &#39;https://storage.googleapis.com/my-test-bucket-768/another&#95;prefix/some&#95;file&#95;1.csv&#39;
* &#39;https://storage.googleapis.com/my-test-bucket-768/another&#95;prefix/some&#95;file&#95;2.csv&#39;
* &#39;https://storage.googleapis.com/my-test-bucket-768/another&#95;prefix/some&#95;file&#95;3.csv&#39;
* &#39;https://storage.googleapis.com/my-test-bucket-768/another&#95;prefix/some&#95;file&#95;4.csv&#39;

Подсчитайте количество строк в файлах, имена которых заканчиваются числами от 1 до 3:

```sql
SELECT count(*)
FROM gcs('https://storage.googleapis.com/clickhouse_public_datasets/my-test-bucket-768/{some,another}_prefix/some_file_{1..3}.csv', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
```

```text
┌─count()─┐
│      18 │
└─────────┘
```

Подсчитайте общее число строк во всех файлах этих двух каталогов:

```sql
SELECT count(*)
FROM gcs('https://storage.googleapis.com/clickhouse_public_datasets/my-test-bucket-768/{some,another}_prefix/*', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
```

```text
┌─count()─┐
│      24 │
└─────────┘
```

:::warning
Если в списке файлов есть числовые диапазоны с ведущими нулями, используйте конструкцию с фигурными скобками отдельно для каждой цифры или `?`.
:::

Подсчитайте общее количество строк в файлах с именами `file-000.csv`, `file-001.csv`, ... , `file-999.csv`:

```sql
SELECT count(*)
FROM gcs('https://storage.googleapis.com/clickhouse_public_datasets/my-test-bucket-768/big_prefix/file-{000..999}.csv', 'CSV', 'name String, value UInt32');
```

```text
┌─count()─┐
│      12 │
└─────────┘
```

Вставьте данные в файл `test-data.csv.gz`:

```sql
INSERT INTO FUNCTION gcs('https://storage.googleapis.com/my-test-bucket-768/test-data.csv.gz', 'CSV', 'name String, value UInt32', 'gzip')
VALUES ('test-data', 1), ('test-data-2', 2);
```

Вставьте в файл `test-data.csv.gz` данные из существующей таблицы:

```sql
INSERT INTO FUNCTION gcs('https://storage.googleapis.com/my-test-bucket-768/test-data.csv.gz', 'CSV', 'name String, value UInt32', 'gzip')
SELECT name, value FROM existing_table;
```

Шаблон glob `**` можно использовать для рекурсивного обхода каталогов. Рассмотрим пример ниже: он позволит рекурсивно получить все файлы из каталога `my-test-bucket-768`:

```sql
SELECT * FROM gcs('https://storage.googleapis.com/my-test-bucket-768/**', 'CSV', 'name String, value UInt32', 'gzip');
```

Ниже рекурсивно извлекаются данные из всех файлов `test-data.csv.gz` в любых папках внутри бакета `my-test-bucket`:

```sql
SELECT * FROM gcs('https://storage.googleapis.com/my-test-bucket-768/**/test-data.csv.gz', 'CSV', 'name String, value UInt32', 'gzip');
```

Для использования в продакшне рекомендуется использовать [именованные коллекции](/ru/operations/named-collections.md). Вот пример:

```sql

CREATE NAMED COLLECTION creds AS
        access_key_id = '***',
        secret_access_key = '***';
SELECT count(*)
FROM gcs(creds, url='https://s3-object-url.csv')
```

<div id="partitioned-write">
  ## Запись с разбиением на партиции
</div>

Если при вставке данных в таблицу `GCS` указать выражение `PARTITION BY`, для каждого значения партиции будет создан отдельный файл. Разбиение данных на отдельные файлы помогает повысить эффективность чтения.

**Примеры**

1. Использование идентификатора партиции в ключе создает отдельные файлы:

```sql
INSERT INTO TABLE FUNCTION
    gcs('http://bucket.amazonaws.com/my_bucket/file_{_partition_id}.csv', 'CSV', 'a String, b UInt32, c UInt32')
    PARTITION BY a VALUES ('x', 2, 3), ('x', 4, 5), ('y', 11, 12), ('y', 13, 14), ('z', 21, 22), ('z', 23, 24);
```

В результате данные записываются в три файла: `file_x.csv`, `file_y.csv` и `file_z.csv`.

2. Использование идентификатора партиции в имени бакета приводит к созданию файлов в разных бакетах:

```sql
INSERT INTO TABLE FUNCTION
    gcs('http://bucket.amazonaws.com/my_bucket_{_partition_id}/file.csv', 'CSV', 'a UInt32, b UInt32, c UInt32')
    PARTITION BY a VALUES (1, 2, 3), (1, 4, 5), (10, 11, 12), (10, 13, 14), (20, 21, 22), (20, 23, 24);
```

В результате данные записываются в три файла в разных бакетах: `my_bucket_1/file.csv`, `my_bucket_10/file.csv` и `my_bucket_20/file.csv`.

<div id="related">
  ## См. также
</div>

* [Табличная функция S3](s3.md)
* [Движок S3](../../engines/table-engines/integrations/s3.md)