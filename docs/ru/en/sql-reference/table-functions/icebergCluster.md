---
description: 'Расширение для табличной функции iceberg, позволяющее обрабатывать файлы
  из Apache Iceberg параллельно на множестве узлов в указанном кластере.'
sidebar_label: 'icebergCluster'
sidebar_position: 91
slug: /sql-reference/table-functions/icebergCluster
title: 'icebergCluster'
doc_type: 'reference'
---

Это расширение для табличной функции [iceberg](/ru/sql-reference/table-functions/iceberg.md).

Позволяет обрабатывать файлы из Apache [Iceberg](https://iceberg.apache.org/) параллельно на множестве узлов в указанном кластере. На узле-инициаторе оно устанавливает соединение со всеми узлами кластера и динамически распределяет каждый файл. На узле-воркере оно запрашивает у инициатора следующую задачу на обработку и выполняет её. Это повторяется, пока не будут завершены все задачи.

<div id="syntax">
  ## Синтаксис
</div>

```sql
icebergS3Cluster(cluster_name, url [, NOSIGN | access_key_id, secret_access_key, [session_token]] [,format] [,compression_method] [,extra_credentials])
icebergS3Cluster(cluster_name, named_collection[, option=value [,..]])

icebergAzureCluster(cluster_name, connection_string|storage_account_url, container_name, blobpath, [,account_name], [,account_key] [,format] [,compression_method])
icebergAzureCluster(cluster_name, named_collection[, option=value [,..]])

icebergHDFSCluster(cluster_name, path_to_table, [,format] [,compression_method])
icebergHDFSCluster(cluster_name, named_collection[, option=value [,..]])
```

<div id="arguments">
  ## Аргументы
</div>

* `cluster_name` — имя кластера, используемое для формирования набора адресов и параметров подключения к удалённым и локальным серверам.
* Описание всех остальных аргументов совпадает с описанием аргументов в эквивалентной табличной функции [iceberg](/ru/sql-reference/table-functions/iceberg.md).
* Необязательный параметр `extra_credentials` можно использовать для передачи `role_arn` для ролевого доступа в ClickHouse Cloud. Инструкции по настройке см. в разделе [Secure S3](/ru/cloud/data-sources/secure-s3).

**Возвращаемое значение**

Таблица с указанной структурой для чтения данных из кластера в указанной таблице Iceberg.

**Примеры**

```sql
SELECT * FROM icebergS3Cluster('cluster_simple', 'http://test.s3.amazonaws.com/clickhouse-bucket/test_table', 'test', 'test')
```

<div id="virtual-columns">
  ## Виртуальные столбцы
</div>

* `_path` — Путь к файлу. Тип: `LowCardinality(String)`.
* `_file` — Имя файла. Тип: `LowCardinality(String)`.
* `_size` — Размер файла в байтах. Тип: `Nullable(UInt64)`. Если размер файла неизвестен, значение — `NULL`.
* `_time` — Время последнего изменения файла. Тип: `Nullable(DateTime)`. Если время неизвестно, значение — `NULL`.
* `_etag` — etag файла. Тип: `LowCardinality(String)`. Если etag неизвестен, значение — `NULL`.

**См. также**

* [движок Iceberg](/ru/engines/table-engines/integrations/iceberg.md)
* [табличная функция Iceberg](/ru/sql-reference/table-functions/iceberg.md)