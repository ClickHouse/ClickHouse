---
description: 'DataLakeCatalog 数据库引擎使您能够将 ClickHouse 连接到外部目录，并查询开放表格式数据'
sidebar_label: 'DataLakeCatalog'
slug: /engines/database-engines/datalakecatalog
title: 'DataLakeCatalog'
doc_type: 'reference'
---

`DataLakeCatalog` 数据库引擎使您能够将 ClickHouse 连接到外部目录，并查询开放表格式数据，而无需复制数据。
这让 ClickHouse 成为一个功能强大的查询引擎，可与您现有的数据湖基础设施无缝集成。

<div id="supported-catalogs">
  ## 支持的目录
</div>

`DataLakeCatalog` 引擎支持以下数据目录：

* **AWS Glue Catalog** - 适用于 AWS 环境中的 Iceberg 表
* **Databricks Unity Catalog** - 适用于 Delta Lake 和 Iceberg 表
* **Hive Metastore** - 传统 Hadoop 生态系统中的目录
* **REST Catalogs** - 任何支持 Iceberg REST 规范的目录

<div id="creating-a-database">
  ## 创建数据库
</div>

要使用 `DataLakeCatalog` 引擎，您需要启用以下相关设置：

```sql
SET allow_experimental_database_iceberg = 1;
SET allow_experimental_database_unity_catalog = 1;
SET allow_experimental_database_glue_catalog = 1;
SET allow_experimental_database_hms_catalog = 1;
SET allow_experimental_database_paimon_rest_catalog = 1;
```

可使用以下语法创建使用 `DataLakeCatalog` 引擎的数据库：

```sql
CREATE DATABASE database_name
ENGINE = DataLakeCatalog(catalog_endpoint[, user, password])
SETTINGS
catalog_type,
[...]
```

支持以下设置：

| Setting                 | Description                                                                                                                                        |
| ----------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| `catalog_type`          | 目录类型：`glue`、`unity` (Delta) 、`rest` (Iceberg) 、`hive`、`onelake` (Iceberg)                                                                          |
| `warehouse`             | 在目录中使用的仓库/数据库名称。                                                                                                                                   |
| `catalog_credential`    | 目录的身份验证凭据 (例如 API key 或令牌)                                                                                                                         |
| `auth_header`           | 用于与目录服务进行身份验证的自定义 HTTP 请求头                                                                                                                         |
| `auth_scope`            | 用于身份验证的 OAuth2 scope (如果使用 OAuth)                                                                                                                  |
| `storage_endpoint`      | 底层存储的端点 URL                                                                                                                                        |
| `oauth_server_uri`      | 用于身份验证的 OAuth2 授权服务器 URI                                                                                                                           |
| `vended_credentials`    | 布尔值，指示是否使用目录下发的凭据 (支持 AWS S3 和 Azure ADLS Gen2)                                                                                                    |
| `aws_access_key_id`     | 用于访问 S3/Glue 的 AWS access key ID (如果不使用下发凭据)                                                                                                       |
| `aws_secret_access_key` | 用于访问 S3/Glue 的 AWS secret access key (如果不使用下发凭据)                                                                                                   |
| `region`                | 服务所在的 AWS 区域 (例如 `us-east-1`)                                                                                                                      |
| `dlf_access_key_id`     | 用于访问 DLF 的 access key ID                                                                                                                           |
| `dlf_access_key_secret` | 用于访问 DLF 的 access key Secret                                                                                                                       |
| `force_add_bucket`      | 当根据目录提供的表位置和 `storage_endpoint` 构造对象存储 URL 时，即使端点中已包含存储桶/容器名称，也会在前面再加上该名称。默认值：`false`。如果目录返回的路径不包含存储桶，且需要在构造 URL 时补上，请将其设为 `true` (Polaris 风格路径) 。 |

<div id="examples">
  ## 示例
</div>

请参阅以下章节，查看 `DataLakeCatalog` 引擎的使用示例：

* [Unity Catalog](/zh/use-cases/data-lake/unity-catalog)
* [Glue Catalog](/zh/use-cases/data-lake/glue-catalog)
* OneLake Catalog
  可通过启用 `allow_experimental_database_iceberg` 或 `allow_database_iceberg` 使用。

```sql
CREATE DATABASE database_name
ENGINE = DataLakeCatalog(catalog_endpoint)
SETTINGS
    catalog_type = 'onelake',
    warehouse = warehouse,
    onelake_tenant_id = tenant_id,
    oauth_server_uri = server_uri,
    auth_scope = auth_scope,
    onelake_client_id = client_id,
    onelake_client_secret = client_secret;
SHOW TABLES IN database_name;
SELECT count() from database_name.table_name;
```