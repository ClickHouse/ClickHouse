---
description: '该引擎可与 Azure Blob 存储生态系统集成。'
sidebar_label: 'Azure Blob 存储'
sidebar_position: 10
slug: /engines/table-engines/integrations/azureBlobStorage
title: 'AzureBlobStorage 表引擎'
doc_type: 'reference'
---

该引擎可与 [Azure Blob 存储](https://azure.microsoft.com/en-us/products/storage/blobs) 生态系统集成。

<div id="create-table">
  ## 创建表
</div>

```sql
CREATE TABLE azure_blob_storage_table (name String, value UInt32)
    ENGINE = AzureBlobStorage(connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression, partition_strategy, partition_columns_in_data_file, extra_credentials(client_id=, tenant_id=)])
    [PARTITION BY expr]
    [SETTINGS ...]
```

<div id="engine-parameters">
  ### 引擎参数
</div>

* `endpoint` — 包含容器和前缀的 AzureBlobStorage 端点 URL。根据所使用的身份验证方法，还可以选择性包含 account&#95;name。(`http://azurite1:{port}/[account_name]{container_name}/{data_prefix}`) 也可以通过 storage&#95;account&#95;url、account&#95;name 和 container 分别提供这些参数。如需指定前缀，应使用 endpoint。
* `endpoint_contains_account_name` - 此标志用于指定 endpoint 是否包含 account&#95;name，因为只有某些身份验证方法才需要它。 (默认值：true)
* `connection_string|storage_account_url` — connection&#95;string 包含账户名称和密钥 ([创建 connection string](https://learn.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string?toc=%2Fazure%2Fstorage%2Fblobs%2Ftoc.json\&bc=%2Fazure%2Fstorage%2Fblobs%2Fbreadcrumb%2Ftoc.json#configure-a-connection-string-for-an-azure-storage-account)) ；你也可以在此处提供 storage account URL，并将账户名称和账户密钥作为单独参数提供 (参见参数 account&#95;name 和 account&#95;key)
* `container_name` - 容器名称
* `blobpath` - 文件路径。在 readonly 模式下支持以下通配符：`*`、`**`、`?`、`{abc,def}` 和 `{N..M}`，其中 `N`、`M` — 数字，`'abc'`、`'def'` — 字符串。
* `account_name` - 如果使用 storage&#95;account&#95;url，则可在此指定账户名称
* `account_key` - 如果使用 storage&#95;account&#95;url，则可在此指定账户密钥
* `format` — 文件的 [format](/zh/interfaces/formats.md)。
* `compression` — 支持的值：`none`、`gzip/gz`、`brotli/br`、`xz/LZMA`、`zstd/zst`。默认会根据文件扩展名自动检测压缩格式。 (等同于设置为 `auto`。)
* `partition_strategy` – 可选值：`wildcard` 或 `hive`。`wildcard` 要求路径中包含 `{_partition_id}`，其会被替换为分区键。`hive` 不允许使用通配符，假定该路径是表根目录，并生成 Hive 风格的分区目录，使用 Snowflake IDs 作为文件名，文件格式作为扩展名。默认值取决于 `file_like_engine_default_partition_strategy` 设置 (在早于 `26.6` 的 `compatibility` 设置中为 `wildcard`，否则为 `hive`) 。
* `partition_columns_in_data_file` - 仅用于 `hive` 分区策略。用于告知 ClickHouse 是否应预期分区列会写入数据文件中。默认为 `false`。
* `extra_credentials` - 使用 `client_id` 和 `tenant_id` 进行身份验证。如果提供了 extra&#95;credentials，则其优先级高于 `account_name` 和 `account_key`。

**示例**

用户可以使用 Azurite 模拟器在本地开发 Azure Storage。更多详情请参见[此处](https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite?tabs=docker-hub%2Cblob-storage)。如果使用 Azurite 的本地实例，用户可能需要在下面的命令中将 `http://localhost:10000` 替换为 `http://azurite1:10000`；这里假定 Azurite 可通过主机 `azurite1` 访问。

```sql
CREATE TABLE test_table (key UInt64, data String)
    ENGINE = AzureBlobStorage('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite1:10000/devstoreaccount1/;', 'testcontainer', 'test_table', 'CSV');

INSERT INTO test_table VALUES (1, 'a'), (2, 'b'), (3, 'c');

SELECT * FROM test_table;
```

```text
┌─key──┬─data──┐
│  1   │   a   │
│  2   │   b   │
│  3   │   c   │
└──────┴───────┘
```

<div id="virtual-columns">
  ## 虚拟列
</div>

* `_path` — 文件路径。类型：`LowCardinality(String)`。
* `_file` — 文件名。类型：`LowCardinality(String)`。
* `_size` — 文件大小 (以字节为单位) 。类型：`Nullable(UInt64)`。如果大小未知，则值为 `NULL`。
* `_time` — 文件的最后修改时间。类型：`Nullable(DateTime)`。如果时间未知，则值为 `NULL`。

<div id="authentication">
  ## 身份验证
</div>

目前有 3 种身份验证方式：

* `Managed Identity` - 可通过提供 `endpoint`、`connection_string` 或 `storage_account_url` 使用。
* `SAS Token` - 可通过提供 `endpoint`、`connection_string` 或 `storage_account_url` 使用。可通过 URL 中是否包含 `?` 来识别。示例请参见 [azureBlobStorage](/zh/sql-reference/table-functions/azureBlobStorage#using-shared-access-signatures-sas-sas-tokens)。
* `Workload Identity` - 可通过提供 `endpoint` 或 `storage_account_url` 使用。如果在 config 中设置了 `use_workload_identity` 参数，则会使用 [Workload Identity](https://github.com/Azure/azure-sdk-for-cpp/tree/main/sdk/identity/azure-identity#authenticate-azure-hosted-applications) 进行身份验证。

<div id="data-cache">
  ### 数据缓存
</div>

`Azure` 表引擎支持将数据缓存在本地磁盘上。
有关文件系统缓存的配置选项和用法，请参见本[节](/zh/operations/storing-data.md/#using-local-cache)。
缓存依据存储对象的 path 和 ETag 来确定，因此 ClickHouse 不会读取过期的缓存版本。

要启用缓存，请使用设置 `filesystem_cache_name = '<name>'` 和 `enable_filesystem_cache = 1`。

```sql
SELECT *
FROM azureBlobStorage('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite1:10000/devstoreaccount1/;', 'testcontainer', 'test_table', 'CSV')
SETTINGS filesystem_cache_name = 'cache_for_azure', enable_filesystem_cache = 1;
```

1. 将以下内容添加到 ClickHouse 配置文件中：

```xml
<clickhouse>
    <filesystem_caches>
        <cache_for_azure>
            <path>path to cache directory</path>
            <max_size>10Gi</max_size>
        </cache_for_azure>
    </filesystem_caches>
</clickhouse>
```

2. 复用 ClickHouse `storage_configuration` 部分中的 cache 配置 (以及相应的 cache 存储) ，[详见此处](/zh/operations/storing-data.md/#using-local-cache)

<div id="partition-by">
  ### PARTITION BY
</div>

`PARTITION BY` — 可选。在大多数情况下，不需要分区键；即使需要，通常也不必细到按月以下的粒度。分区不会加快查询速度 (这与 ORDER BY 表达式不同) 。绝不要使用粒度过细的分区。不要按客户标识符或名称对数据进行分区 (应改为将客户标识符或名称作为 ORDER BY 表达式中的第一列) 。

如需按月分区，请使用 `toYYYYMM(date_column)` 表达式，其中 `date_column` 是一个 [Date](/zh/sql-reference/data-types/date.md) 类型的日期列。这里的分区名称采用 `"YYYYMM"` 格式。

<div id="partition-strategy">
  #### 分区策略
</div>

`wildcard`：将文件路径中的 `{_partition_id}` 通配符替换为实际的分区键。不支持读取。仅在 `compatibility` 设置早于 `26.6` 时才默认选择；否则默认值为 `hive` (参见 `file_like_engine_default_partition_strategy` 设置) 。

`hive` 为读写实现了 hive style partitioning。读取通过递归 glob pattern 实现。写入会按以下格式生成文件：`<prefix>/<key1=val1/key2=val2...>/<snowflakeid>.<toLower(file_format)>`。

注意：使用 `hive` 分区策略时，`use_hive_partitioning` 设置不生效。

`hive` 分区策略示例：

```sql
arthur :) create table azure_table (year UInt16, country String, counter UInt8) ENGINE=AzureBlobStorage(account_name='devstoreaccount1', account_key='Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', storage_account_url = 'http://localhost:30000/devstoreaccount1', container='cont', blob_path='hive_partitioned', format='Parquet', compression='auto', partition_strategy='hive') PARTITION BY (year, country);

arthur :) insert into azure_table values (2020, 'Russia', 1), (2021, 'Brazil', 2);

arthur :) select _path, * from azure_table;

   ┌─_path──────────────────────────────────────────────────────────────────────┬─year─┬─country─┬─counter─┐
1. │ cont/hive_partitioned/year=2020/country=Russia/7351305360873664512.parquet │ 2020 │ Russia  │       1 │
2. │ cont/hive_partitioned/year=2021/country=Brazil/7351305360894636032.parquet │ 2021 │ Brazil  │       2 │
   └────────────────────────────────────────────────────────────────────────────┴──────┴─────────┴─────────┘
```

<div id="see-also">
  ## 另请参阅
</div>

[Azure Blob 存储表函数](/zh/sql-reference/table-functions/azureBlobStorage)