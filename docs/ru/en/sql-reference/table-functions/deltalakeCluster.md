---
description: 'Это расширение табличной функции deltaLake.'
sidebar_label: 'deltaLakeCluster'
sidebar_position: 46
slug: /sql-reference/table-functions/deltalakeCluster
title: 'deltaLakeCluster'
doc_type: 'reference'
---

Это расширение табличной функции [deltaLake](/ru/sql-reference/table-functions/deltalake.md).

Позволяет параллельно обрабатывать файлы из таблиц [Delta Lake](https://github.com/delta-io/delta) в Amazon S3 на множестве узлов указанного кластера. На узле-инициаторе создаётся соединение со всеми узлами кластера, и каждый файл динамически распределяется между ними. На узле-воркере у инициатора запрашивается следующая задача для обработки, после чего она выполняется. Это повторяется, пока не будут завершены все задачи.

<div id="syntax">
  ## Синтаксис
</div>

```sql
deltaLakeCluster(cluster_name, url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression] [,extra_credentials])
deltaLakeCluster(cluster_name, named_collection[, option=value [,..]])

deltaLakeS3Cluster(cluster_name, url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression] [,extra_credentials])
deltaLakeS3Cluster(cluster_name, named_collection[, option=value [,..]])

deltaLakeAzureCluster(cluster_name, connection_string|storage_account_url, container_name, blobpath, [,account_name], [,account_key] [,format] [,compression_method])
deltaLakeAzureCluster(cluster_name, named_collection[, option=value [,..]])
```

`deltaLakeS3Cluster` — это псевдоним `deltaLakeCluster`, оба работают с S3.

<div id="arguments">
  ## Аргументы
</div>

* `cluster_name` — имя кластера, используемое для формирования набора адресов и параметров подключения к удалённым и локальным серверам.
* Описание всех остальных аргументов совпадает с описанием аргументов в эквивалентной табличной функции [deltaLake](/ru/sql-reference/table-functions/deltalake.md).
* Необязательный параметр `extra_credentials` можно использовать для передачи `role_arn` для ролевого доступа в ClickHouse Cloud. Инструкции по настройке см. в разделе [Secure S3](/ru/cloud/data-sources/secure-s3).

<div id="returned_value">
  ## Возвращаемое значение
</div>

Таблица с указанной структурой для чтения данных из указанной таблицы Delta Lake в S3 на указанном кластере.

<div id="virtual-columns">
  ## Виртуальные столбцы
</div>

* `_path` — Путь к файлу. Тип: `LowCardinality(String)`.
* `_file` — Имя файла. Тип: `LowCardinality(String)`.
* `_size` — Размер файла в байтах. Тип: `Nullable(UInt64)`. Если размер файла неизвестен, значение — `NULL`.
* `_time` — Время последнего изменения файла. Тип: `Nullable(DateTime)`. Если время неизвестно, значение — `NULL`.
* `_etag` — ETag файла. Тип: `LowCardinality(String)`. Если ETag неизвестен, значение — `NULL`.

<div id="related">
  ## См. также
</div>

* [движок DeltaLake](/ru/engines/table-engines/integrations/deltalake.md)
* [табличная функция DeltaLake](/ru/sql-reference/table-functions/deltalake.md)