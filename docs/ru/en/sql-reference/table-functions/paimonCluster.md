---
description: 'Расширение табличной функции paimon, которое позволяет параллельно
  обрабатывать файлы из Apache Paimon на множестве узлов указанного кластера.'
sidebar_label: 'paimonCluster'
sidebar_position: 91
slug: /sql-reference/table-functions/paimonCluster
title: 'paimonCluster'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

<div id="paimoncluster-table-function">
  # Табличная функция paimonCluster
</div>

<ExperimentalBadge />

Это расширение табличной функции [paimon](/ru/sql-reference/table-functions/paimon.md).

Позволяет параллельно обрабатывать файлы из Apache [Paimon](https://paimon.apache.org/) на множестве узлов в указанном кластере. На узле-инициаторе создаётся connection ко всем узлам кластера, после чего файлы динамически распределяются между ними. На узле-воркере у инициатора запрашивается следующая задача на обработку, и она выполняется. Это повторяется, пока не будут завершены все задачи.

<div id="syntax">
  ## Синтаксис
</div>

```sql
paimonS3Cluster(cluster_name, url [,aws_access_key_id, aws_secret_access_key] [,format] [,structure] [,compression] [,extra_credentials])

paimonAzureCluster(cluster_name, connection_string|storage_account_url, container_name, blobpath, [,account_name], [,account_key] [,format] [,compression_method])

paimonHDFSCluster(cluster_name, path_to_table, [,format] [,compression_method])
```

<div id="arguments">
  ## Аргументы
</div>

* `cluster_name` — имя кластера, используемое для построения набора адресов и параметров подключения к удалённым и локальным серверам.
* Описание всех остальных аргументов совпадает с описанием аргументов в эквивалентной табличной функции [paimon](/ru/sql-reference/table-functions/paimon.md).
* Необязательный параметр `extra_credentials` можно использовать для передачи `role_arn` для ролевого доступа в ClickHouse Cloud. Шаги по настройке см. в разделе [Secure S3](/ru/cloud/data-sources/secure-s3).

**Возвращаемое значение**

Таблица с указанной структурой для чтения данных из кластера в указанной таблице Paimon.

<div id="virtual-columns">
  ## Виртуальные столбцы
</div>

* `_path` — путь к файлу. Тип: `LowCardinality(String)`.
* `_file` — имя файла. Тип: `LowCardinality(String)`.
* `_size` — размер файла в байтах. Тип: `Nullable(UInt64)`. Если размер файла неизвестен, значение — `NULL`.
* `_time` — время последнего изменения файла. Тип: `Nullable(DateTime)`. Если время неизвестно, значение — `NULL`.
* `_etag` — ETag файла. Тип: `LowCardinality(String)`. Если ETag неизвестен, значение — `NULL`.

**См. также**

* [Табличная функция Paimon](/ru/sql-reference/table-functions/paimon.md)