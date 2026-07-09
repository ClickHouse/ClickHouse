---
description: 'DataLakeCatalog データベースエンジンを使用すると、ClickHouse を外部データカタログに接続し、データを複製せずにオープンテーブルフォーマットのデータをクエリできます'
sidebar_label: 'DataLakeCatalog'
slug: /engines/database-engines/datalakecatalog
title: 'DataLakeCatalog'
doc_type: 'reference'
---

`DataLakeCatalog` データベースエンジンを使用すると、ClickHouse を外部
データカタログに接続し、データを複製することなくオープンテーブルフォーマットのデータをクエリできます。
これにより ClickHouse は、既存の
データレイクインフラストラクチャとシームレスに連携する強力なクエリエンジンになります。

<div id="supported-catalogs">
  ## サポートされているカタログ
</div>

`DataLakeCatalog` エンジンは、以下のデータカタログをサポートしています。

* **AWS Glue カタログ** - AWS 環境の Iceberg テーブル向け
* **Databricks Unity Catalog** - Delta Lake および Iceberg テーブル向け
* **Hive Metastore** - 従来の Hadoop エコシステム向けカタログ
* **REST Catalogs** - Iceberg REST 仕様をサポートする任意のカタログ

<div id="creating-a-database">
  ## データベースの作成
</div>

`DataLakeCatalog` エンジンを使用するには、以下の該当する設定を有効にする必要があります。

```sql
SET allow_experimental_database_iceberg = 1;
SET allow_experimental_database_unity_catalog = 1;
SET allow_experimental_database_glue_catalog = 1;
SET allow_experimental_database_hms_catalog = 1;
SET allow_experimental_database_paimon_rest_catalog = 1;
```

`DataLakeCatalog` エンジンを使用するデータベースは、次の構文で作成できます。

```sql
CREATE DATABASE database_name
ENGINE = DataLakeCatalog(catalog_endpoint[, user, password])
SETTINGS
catalog_type,
[...]
```

以下の設定がサポートされています。

| Setting                 | Description                                                                                                                                                                                                                    |
| ----------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `catalog_type`          | カタログの種類: `glue`、`unity` (Delta)、`rest` (Iceberg)、`hive`、`onelake` (Iceberg)                                                                                                                                                    |
| `warehouse`             | カタログで使用する warehouse/database 名。                                                                                                                                                                                                |
| `catalog_credential`    | カタログの認証資格情報 (例: API key や token)                                                                                                                                                                                               |
| `auth_header`           | カタログサービスで認証に使用するカスタム HTTP header                                                                                                                                                                                               |
| `auth_scope`            | 認証用の OAuth2 スコープ (OAuth を使用する場合)                                                                                                                                                                                               |
| `storage_endpoint`      | underlying storage のエンドポイント URL                                                                                                                                                                                                |
| `oauth_server_uri`      | 認証に使用する OAuth2 認可 server の URI                                                                                                                                                                                                 |
| `vended_credentials`    | カタログから提供される credentials を使用するかどうかを示すブール値 (AWS S3 および Azure ADLS Gen2 をサポート)                                                                                                                                                    |
| `aws_access_key_id`     | S3/Glue へのアクセスに使用する AWS access key ID (vended credentials を使用しない場合)                                                                                                                                                            |
| `aws_secret_access_key` | S3/Glue へのアクセスに使用する AWS secret access key (vended credentials を使用しない場合)                                                                                                                                                        |
| `region`                | サービスの AWS region (例: `us-east-1`)                                                                                                                                                                                              |
| `dlf_access_key_id`     | DLF アクセス用の access key ID                                                                                                                                                                                                       |
| `dlf_access_key_secret` | DLF アクセス用の access key secret                                                                                                                                                                                                   |
| `force_add_bucket`      | カタログから提供された table location と `storage_endpoint` をもとに object storage URL を構築する際、エンドポイントに bucket/コンテナー 名がすでに含まれていても、それを先頭に追加します。デフォルト: `false`。bucket を含まないパスを返し、URL 構築時に bucket の追加が必要なカタログ (Polaris 形式のパス) の場合は、`true` に設定します。 |

<div id="examples">
  ## 例
</div>

`DataLakeCatalog` エンジン の使用例については、以下のセクションを参照してください。

* [Unity Catalog](/ja/use-cases/data-lake/unity-catalog)
* [Glue カタログ](/ja/use-cases/data-lake/glue-catalog)
* OneLake Catalog
  `allow_experimental_database_iceberg` または `allow_database_iceberg` を有効にすると使用できます。

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