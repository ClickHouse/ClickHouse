---
description: 'Позволяет обрабатывать файлы из Azure Blob Storage параллельно на
  множестве узлов в указанном кластере.'
sidebar_label: 'azureBlobStorageCluster'
sidebar_position: 15
slug: /sql-reference/table-functions/azureBlobStorageCluster
title: 'azureBlobStorageCluster'
doc_type: 'reference'
---

Позволяет обрабатывать файлы из [Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs) параллельно на множестве узлов в указанном кластере. На узле-инициаторе создаётся соединение со всеми узлами кластера, раскрываются символы * в пути к файлу S3, после чего каждый файл динамически распределяется. Узел-воркер запрашивает у инициатора следующую задачу на обработку и выполняет её. Это повторяется, пока не будут завершены все задачи.
Эта табличная функция аналогична функции [s3Cluster](../../sql-reference/table-functions/s3Cluster.md).

<div id="syntax">
  ## Синтаксис
</div>

```sql
azureBlobStorageCluster(cluster_name, connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression, structure])
```

<div id="arguments">
  ## Аргументы
</div>

| Argument            | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   |
| ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `cluster_name`      | Имя кластера, используемое для формирования набора адресов и параметров подключения к удалённым и локальным серверам.                                                                                                                                                                                                                                                                                                                                                                                         |
| `connection_string` | `storage_account_url` — `connection_string` включает имя аккаунта и ключ ([Create connection string](https://learn.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string?toc=%2Fazure%2Fstorage%2Fblobs%2Ftoc.json\&bc=%2Fazure%2Fstorage%2Fblobs%2Fbreadcrumb%2Ftoc.json#configure-a-connection-string-for-an-azure-storage-account)) или здесь можно указать URL аккаунта хранилища, а имя аккаунта и ключ аккаунта передать отдельно (см. параметры `account_name` и `account_key`) |
| `container_name`    | Имя контейнера                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                |
| `blobpath`          | путь к файлу. Поддерживает следующие подстановочные шаблоны в режиме только для чтения: `*`, `**`, `?`, `{abc,def}` и `{N..M}`, где `N`, `M` — числа, `'abc'`, `'def'` — строки.                                                                                                                                                                                                                                                                                                                              |
| `account_name`      | если используется `storage_account_url`, здесь можно указать имя аккаунта                                                                                                                                                                                                                                                                                                                                                                                                                                     |
| `account_key`       | если используется `storage_account_url`, здесь можно указать ключ аккаунта                                                                                                                                                                                                                                                                                                                                                                                                                                    |
| `format`            | Формат файла: [format](/ru/sql-reference/formats).                                                                                                                                                                                                                                                                                                                                                                                                                                                               |
| `compression`       | Поддерживаемые значения: `none`, `gzip/gz`, `brotli/br`, `xz/LZMA`, `zstd/zst`. По умолчанию сжатие определяется автоматически по расширению файла. (эквивалентно значению `auto`).                                                                                                                                                                                                                                                                                                                           |
| `structure`         | Структура таблицы. Формат: `'column1_name column1_type, column2_name column2_type, ...'`.                                                                                                                                                                                                                                                                                                                                                                                                                     |

<div id="returned_value">
  ## Возвращаемое значение
</div>

Таблица с указанной структурой для чтения или записи данных в указанном файле.

<div id="examples">
  ## Примеры
</div>

Как и в случае с движком таблицы [AzureBlobStorage](/ru/engines/table-engines/integrations/azureBlobStorage), для локальной разработки с Azure Storage можно использовать эмулятор Azurite. Подробнее [здесь](https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite?tabs=docker-hub%2Cblob-storage). Ниже предполагается, что Azurite доступен по имени хоста `azurite1`.

Подсчитайте количество строк в файле `test_cluster_*.csv`, используя все узлы кластера `cluster_simple`:

```sql
SELECT count(*) FROM azureBlobStorageCluster(
        'cluster_simple', 'http://azurite1:10000/devstoreaccount1', 'testcontainer', 'test_cluster_count.csv', 'devstoreaccount1',
        'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'CSV',
        'auto', 'key UInt64')
```

<div id="using-shared-access-signatures-sas-sas-tokens">
  ## Использование Shared Access Signatures (SAS)
</div>

Примеры см. в [azureBlobStorage](/ru/sql-reference/table-functions/azureBlobStorage#using-shared-access-signatures-sas-sas-tokens).

<div id="related">
  ## См. также
</div>

* [Движок AzureBlobStorage](../../engines/table-engines/integrations/azureBlobStorage.md)
* [Табличная функция AzureBlobStorage](../../sql-reference/table-functions/azureBlobStorage.md)