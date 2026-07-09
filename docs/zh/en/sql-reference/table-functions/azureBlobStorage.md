---
description: '提供类表接口，用于在 Azure Blob 存储中查询/插入文件。类似于 s3 函数。'
keywords: ['Azure Blob 存储']
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
  # azureBlobStorage 表函数
</div>

提供类似表的接口，用于在 [Azure Blob 存储](https://azure.microsoft.com/en-us/products/storage/blobs) 中查询/插入文件。该表函数与 [s3 函数](../../sql-reference/table-functions/s3.md) 类似。

<div id="syntax">
  ## 语法
</div>

<Tabs>
  <TabItem value="connection_string" label="连接字符串" default>
    凭据已包含在连接字符串中，因此无需单独提供 `account_name`/`account_key`：

    ```sql
    azureBlobStorage(connection_string, container_name, blobpath [, format, compression, structure])
    ```
  </TabItem>

  <TabItem value="storage_account_url" label="存储账户 URL">
    需要将 `account_name` 和 `account_key` 作为单独的参数提供：

    ```sql
    azureBlobStorage(storage_account_url, container_name, blobpath, account_name, account_key [, format, compression, structure])
    ```
  </TabItem>

  <TabItem value="named_collection" label="命名集合">
    有关支持键的完整列表，请参阅下方的[命名集合](#named-collections)：

    ```sql
    azureBlobStorage(named_collection[, option=value [,..]])
    ```
  </TabItem>
</Tabs>

<div id="arguments">
  ## 参数
</div>

| Argument                         | Description                                                                                                                                                                                                                                                                                                                                                    |
| -------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `connection_string`              | 包含内嵌凭据 (账户名称 + 账户密钥或 SAS token) 的连接字符串。使用这种形式时，**不应**单独传递 `account_name` 和 `account_key`。请参见[配置连接字符串](https://learn.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string?toc=%2Fazure%2Fstorage%2Fblobs%2Ftoc.json\&bc=%2Fazure%2Fstorage%2Fblobs%2Fbreadcrumb%2Ftoc.json#configure-a-connection-string-for-an-azure-storage-account)。 |
| `storage_account_url`            | 存储账户端点 URL，例如 `https://myaccount.blob.core.windows.net/`。使用这种形式时，**必须**同时传递 `account_name` 和 `account_key`。                                                                                                                                                                                                                                                    |
| `container_name`                 | 容器名称。                                                                                                                                                                                                                                                                                                                                                          |
| `blobpath`                       | 文件路径。在只读模式下支持以下通配符：`*`、`**`、`?`、`{abc,def}` 和 `{N..M}`，其中 `N`、`M` 表示数字，`'abc'`、`'def'` 表示字符串。                                                                                                                                                                                                                                                                  |
| `account_name`                   | 存储账户名称。使用不带 SAS 的 `storage_account_url` 时**必需**；使用 `connection_string` 时**不得**传递。                                                                                                                                                                                                                                                                              |
| `account_key`                    | 存储账户密钥。使用不带 SAS 的 `storage_account_url` 时**必需**；使用 `connection_string` 时**不得**传递。                                                                                                                                                                                                                                                                              |
| `format`                         | 文件的[格式](/zh/sql-reference/formats)。                                                                                                                                                                                                                                                                                                                               |
| `compression`                    | 支持的值：`none`、`gzip/gz`、`brotli/br`、`xz/LZMA`、`zstd/zst`。默认会根据文件扩展名自动检测压缩方式 (等同于设置为 `auto`) 。                                                                                                                                                                                                                                                                    |
| `structure`                      | 表的结构。格式为 `'column1_name column1_type, column2_name column2_type, ...'`。                                                                                                                                                                                                                                                                                        |
| `partition_strategy`             | 可选。支持的值：`WILDCARD` 或 `HIVE`。`WILDCARD` 要求路径中包含 `{_partition_id}`，该占位符会被替换为分区键。`HIVE` 不允许使用通配符，假定该路径是表的根路径，并生成 Hive 风格的分区目录，文件名使用 Snowflake IDs，扩展名使用文件格式。默认值为 `file_like_engine_default_partition_strategy` 设置 (在低于 `26.6` 的 `compatibility` 设置中为 `WILDCARD`，否则为 `HIVE`) 。                                                                                     |
| `partition_columns_in_data_file` | 可选。仅与 `HIVE` 分区策略一起使用。用于告知 ClickHouse 是否应当预期数据文件中会写入分区列。默认为 `false`。                                                                                                                                                                                                                                                                                           |
| `extra_credentials`              | 使用 `client_id` 和 `tenant_id` 进行身份验证。如果提供了 `extra_credentials`，其优先级高于 `account_name` 和 `account_key`。                                                                                                                                                                                                                                                           |

<div id="named-collections">
  ## 命名集合
</div>

参数也可以通过[命名集合](/zh/operations/named-collections)传递。在这种情况下，支持以下键：

| 键                     | 必填 | 描述                                                                   |
| --------------------- | -- | -------------------------------------------------------------------- |
| `container`           | 是  | 容器名称。对应位置参数 `container_name`。                                        |
| `blob_path`           | 是  | 文件路径 (可选使用通配符) 。对应位置参数 `blobpath`。                                   |
| `connection_string`   | 否* | 包含内嵌凭据的连接字符串。*必须提供 `connection_string` 或 `storage_account_url` 其中之一。 |
| `storage_account_url` | 否* | 存储账户端点 URL。*必须提供 `connection_string` 或 `storage_account_url` 其中之一。   |
| `account_name`        | 否  | 使用 `storage_account_url` 时必需。                                        |
| `account_key`         | 否  | 使用 `storage_account_url` 时必需。                                        |
| `format`              | 否  | 文件格式。                                                                |
| `compression`         | 否  | 压缩类型。                                                                |
| `structure`           | 否  | 表结构。                                                                 |
| `client_id`           | 否  | 用于身份验证的客户端 ID。                                                       |
| `tenant_id`           | 否  | 用于身份验证的租户 ID。                                                        |

:::note
命名集合中的键名与函数的位置参数名不同：`container` (不是 `container_name`) 和 `blob_path` (不是 `blobpath`) 。
:::

**示例：**

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

您也可以在查询时覆盖命名集合的值：

```sql
SELECT *
FROM azureBlobStorage(azure_my_data, blob_path = 'other_data/*.csv', format = 'CSVWithNames')
LIMIT 5;
```

<div id="returned_value">
  ## 返回值
</div>

一个具有指定结构的表，用于读取或写入指定文件中的数据。

<div id="examples">
  ## 示例
</div>

<div id="reading-with-storage-account-url">
  ### 通过 `storage_account_url` 形式读取
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
  ### 使用 `connection_string` 形式读取
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
  ### 按分区写入
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

然后再读取某个特定分区：

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
  ## 虚拟列
</div>

* `_path` — 文件路径。类型：`LowCardinality(String)`。
* `_file` — 文件名。类型：`LowCardinality(String)`。
* `_size` — 文件大小 (以字节为单位) 。类型：`Nullable(UInt64)`。如果文件大小未知，则值为 `NULL`。
* `_time` — 文件的最后修改时间。类型：`Nullable(DateTime)`。如果时间未知，则值为 `NULL`。

<div id="partitioned-write">
  ## 分区写入
</div>

<div id="partition-strategy">
  ### 分区策略
</div>

仅支持 `INSERT` 查询。

`WILDCARD`：将文件路径中的 `{_partition_id}` 通配符替换为实际的分区键。仅在 `26.6` 之前的 `compatibility` 设置下才会默认选择该策略；否则默认值为 `HIVE` (参见 `file_like_engine_default_partition_strategy` 设置) 。

`HIVE` 为读写操作实现了 Hive 风格分区。它会按以下格式生成文件：`<prefix>/<key1=val1/key2=val2...>/<snowflakeid>.<toLower(file_format)>`。

**`HIVE` 分区策略示例**

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
  ## use_hive_partitioning 设置
</div>

这是一个用于提示 ClickHouse 在读取时解析 Hive 风格分区文件的选项。它对写入没有影响。若要实现对称的读写，请使用 `partition_strategy` 参数。

当 `use_hive_partitioning` 设置为 1 时，ClickHouse 会检测路径中的 Hive 风格分区 (`/name=value/`) ，并允许在查询中将分区列用作虚拟列。这些虚拟列将与分区路径中的名称相同。

**示例**

使用通过 Hive 风格分区创建的虚拟列

```sql
SELECT * FROM azureBlobStorage(config, storage_account_url='...', container='...', blob_path='http://data/path/date=*/country=*/code=*/*.parquet') WHERE date > '2020-01-01' AND country = 'Netherlands' AND code = 42;
```

<div id="using-shared-access-signatures-sas-sas-tokens">
  ## 使用共享访问签名 (SAS)
</div>

共享访问签名 (SAS) 是一种 URI，可对 Azure Storage 容器或文件授予受限访问权限。使用它可以在不共享存储账户密钥的情况下，为存储账户资源提供有时效限制的访问权限。更多详情请参见[此处](https://learn.microsoft.com/en-us/rest/api/storageservices/delegate-access-with-shared-access-signature)。

`azureBlobStorage` 函数支持共享访问签名 (SAS)。

[Blob SAS token](https://learn.microsoft.com/en-us/azure/ai-services/translator/document-translation/how-to-guides/create-sas-tokens?tabs=Containers) 包含对请求进行身份验证所需的全部信息，包括目标 blob、权限和有效期。要构造 blob URL，请将 SAS token 追加到 blob 服务端点后面。例如，如果该端点为 `https://clickhousedocstest.blob.core.windows.net/`，则请求变为：

```sql
SELECT count()
FROM azureBlobStorage('BlobEndpoint=https://clickhousedocstest.blob.core.windows.net/;SharedAccessSignature=sp=r&st=2025-01-29T14:58:11Z&se=2025-01-29T22:58:11Z&spr=https&sv=2022-11-02&sr=c&sig=Ac2U0xl4tm%2Fp7m55IilWl1yHwk%2FJG0Uk6rMVuOiD0eE%3D', 'exampledatasets', 'example.csv')

┌─count()─┐
│      10 │
└─────────┘

1 row in set. Elapsed: 0.425 sec.
```

或者，用户也可以使用生成的 [Blob SAS URL](https://learn.microsoft.com/en-us/azure/ai-services/translator/document-translation/how-to-guides/create-sas-tokens?tabs=Containers)：

```sql
SELECT count()
FROM azureBlobStorage('https://clickhousedocstest.blob.core.windows.net/?sp=r&st=2025-01-29T14:58:11Z&se=2025-01-29T22:58:11Z&spr=https&sv=2022-11-02&sr=c&sig=Ac2U0xl4tm%2Fp7m55IilWl1yHwk%2FJG0Uk6rMVuOiD0eE%3D', 'exampledatasets', 'example.csv')

┌─count()─┐
│      10 │
└─────────┘

1 row in set. Elapsed: 0.153 sec.
```

<div id="related">
  ## 相关
</div>

* [AzureBlobStorage 表引擎](/zh/engines/table-engines/integrations/azureBlobStorage.md)