---
description: 'Azure Blob Storage 内のファイルを SELECT/INSERT できるテーブル形式インターフェイスを提供します。s3 関数に似ています。'
keywords: ['azure blob storage']
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
  # azureBlobStorage テーブル関数
</div>

[Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs) 内のファイルに対して select/insert を行うためのテーブル形式インターフェイスを提供します。このテーブル関数は、[s3 function](../../sql-reference/table-functions/s3.md) に似ています。

<div id="syntax">
  ## 構文
</div>

<Tabs>
  <TabItem value="connection_string" label="接続文字列" default>
    認証情報は接続文字列に埋め込まれているため、`account_name`/`account_key` を別途指定する必要はありません。

    ```sql
    azureBlobStorage(connection_string, container_name, blobpath [, format, compression, structure])
    ```
  </TabItem>

  <TabItem value="storage_account_url" label="ストレージアカウント URL">
    `account_name` と `account_key` を個別の引数として指定する必要があります。

    ```sql
    azureBlobStorage(storage_account_url, container_name, blobpath, account_name, account_key [, format, compression, structure])
    ```
  </TabItem>

  <TabItem value="named_collection" label="名前付きコレクション">
    サポートされているキーの一覧については、以下の [名前付きコレクション](#named-collections) を参照してください。

    ```sql
    azureBlobStorage(named_collection[, option=value [,..]])
    ```
  </TabItem>
</Tabs>

<div id="arguments">
  ## 引数
</div>

| Argument                         | Description                                                                                                                                                                                                                                                                                                                                                                                       |
| -------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `connection_string`              | 埋め込み認証情報 (アカウント名 + アカウントキー、または SAS token) を含む接続文字列です。この形式を使用する場合、`account_name` と `account_key` を個別に渡しては**なりません**。[接続文字列を構成する](https://learn.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string?toc=%2Fazure%2Fstorage%2Fblobs%2Ftoc.json\&bc=%2Fazure%2Fstorage%2Fblobs%2Fbreadcrumb%2Ftoc.json#configure-a-connection-string-for-an-azure-storage-account) を参照してください。 |
| `storage_account_url`            | ストレージアカウントのエンドポイント URL。例: `https://myaccount.blob.core.windows.net/`。この形式を使用する場合、`account_name` と `account_key` も**必ず**渡す必要があります。                                                                                                                                                                                                                                                                 |
| `container_name`                 | コンテナー名。                                                                                                                                                                                                                                                                                                                                                                                           |
| `blobpath`                       | ファイルパス。読み取り専用モードでは、次のワイルドカードをサポートします: `*`, `**`, `?`, `{abc,def}` および `{N..M}`。ここで `N`, `M` は数値、`'abc'`, `'def'` は文字列です。                                                                                                                                                                                                                                                                          |
| `account_name`                   | ストレージアカウント名。SAS を使用しない `storage_account_url` を使う場合は**必須**です。`connection_string` を使う場合は渡しては**なりません**。                                                                                                                                                                                                                                                                                              |
| `account_key`                    | ストレージアカウントキー。SAS を使用しない `storage_account_url` を使う場合は**必須**です。`connection_string` を使う場合は渡しては**なりません**。                                                                                                                                                                                                                                                                                             |
| `format`                         | ファイルの [フォーマット](/ja/sql-reference/formats) です。                                                                                                                                                                                                                                                                                                                                                        |
| `compression`                    | サポートされる値: `none`, `gzip/gz`, `brotli/br`, `xz/LZMA`, `zstd/zst`。デフォルトでは、ファイル拡張子から圧縮を自動判別します (`auto` に設定した場合と同じです) 。                                                                                                                                                                                                                                                                               |
| `structure`                      | テーブルの構造。形式は `'column1_name column1_type, column2_name column2_type, ...'` です。                                                                                                                                                                                                                                                                                                                     |
| `partition_strategy`             | 任意です。サポートされる値: `WILDCARD` または `HIVE`。`WILDCARD` ではパス内に `{_partition_id}` が必要で、これはパーティションキーに置き換えられます。`HIVE` ではワイルドカードは使用できず、パスをテーブルのルートと見なし、ファイル名に Snowflake IDs、拡張子にファイルフォーマットを使用した Hive 形式のパーティションディレクトリを生成します。デフォルトは `file_like_engine_default_partition_strategy` 設定です (`26.6` より前の `compatibility` 設定では `WILDCARD`、それ以外では `HIVE`) 。                                                           |
| `partition_columns_in_data_file` | 任意です。`HIVE` パーティション方式でのみ使用されます。パーティションカラムがデータファイル内に書き込まれているものとして ClickHouse が扱うかどうかを指定します。デフォルトは `false` です。                                                                                                                                                                                                                                                                                      |
| `extra_credentials`              | 認証には `client_id` と `tenant_id` を使用します。`extra_credentials` が指定されている場合、`account_name` と `account_key` よりも優先されます。                                                                                                                                                                                                                                                                                    |

<div id="named-collections">
  ## 名前付きコレクション
</div>

引数は、[名前付きコレクション](/ja/operations/named-collections)を使って渡すこともできます。この場合、以下のキーがサポートされます。

| Key                   | Required | Description                                                                               |
| --------------------- | -------- | ----------------------------------------------------------------------------------------- |
| `container`           | はい       | コンテナー名。位置引数 `container_name` に対応します。                                                      |
| `blob_path`           | はい       | ファイルパス (ワイルドカード指定可) 。位置引数 `blobpath` に対応します。                                              |
| `connection_string`   | いいえ*     | 埋め込み認証情報を含む接続文字列。*`connection_string` または `storage_account_url` のいずれかを指定する必要があります。        |
| `storage_account_url` | いいえ*     | ストレージ アカウントのエンドポイント URL。*`connection_string` または `storage_account_url` のいずれかを指定する必要があります。 |
| `account_name`        | いいえ      | `storage_account_url` を使用する場合は必須です                                                        |
| `account_key`         | いいえ      | `storage_account_url` を使用する場合は必須です                                                        |
| `format`              | いいえ      | ファイルフォーマット。                                                                               |
| `compression`         | いいえ      | 圧縮の種類。                                                                                    |
| `structure`           | いいえ      | テーブル構造。                                                                                   |
| `client_id`           | いいえ      | 認証用のクライアント ID。                                                                            |
| `tenant_id`           | いいえ      | 認証用の tenant ID。                                                                           |

:::note
名前付きコレクションのキー名は、位置関数引数名とは異なります。`container` (`container_name` ではない) と `blob_path` (`blobpath` ではない) を使用します。
:::

**例:**

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

クエリ実行時に 名前付きコレクション の値を上書きすることもできます。

```sql
SELECT *
FROM azureBlobStorage(azure_my_data, blob_path = 'other_data/*.csv', format = 'CSVWithNames')
LIMIT 5;
```

<div id="returned_value">
  ## 戻り値
</div>

指定したファイルのデータを読み書きするための、指定した構造を持つテーブル。

<div id="examples">
  ## 例
</div>

<div id="reading-with-storage-account-url">
  ### `storage_account_url` 形式を使った読み取り
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
  ### `connection_string` 形式を使用した読み取り
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
  ### パーティションを使った書き込み
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

次に、特定のパーティションを読み出します:

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
  ## 仮想カラム
</div>

* `_path` — ファイルのパス。型: `LowCardinality(String)`。
* `_file` — ファイル名。型: `LowCardinality(String)`。
* `_size` — ファイルサイズ (バイト単位) 。型: `Nullable(UInt64)`。ファイルサイズが不明な場合、値は `NULL` です。
* `_time` — ファイルの最終更新時刻。型: `Nullable(DateTime)`。時刻が不明な場合、値は `NULL` です。

<div id="partitioned-write">
  ## パーティション化して書き込む
</div>

<div id="partition-strategy">
  ### パーティション方式
</div>

`INSERT` クエリでのみサポートされています。

`WILDCARD`: ファイルパス内の `{_partition_id}` ワイルドカードを実際のパーティションキーに置き換えます。デフォルトで選択されるのは、`26.6` より前の `compatibility` 設定の場合のみです。それ以外のデフォルトは `HIVE` です (`file_like_engine_default_partition_strategy` 設定を参照) 。

`HIVE` は、読み取りと書き込みに対して Hive スタイルのパーティション化を実装します。ファイルは次のフォーマットで生成されます: `<prefix>/<key1=val1/key2=val2...>/<snowflakeid>.<toLower(file_format)>`。

**`HIVE` パーティション方式の例**

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
  ## use_hive_partitioning 設定
</div>

これは、読み取り時に Hive-style partitioning のファイルを ClickHouse が解析するためのヒントです。書き込みには影響しません。読み取りと書き込みで対称的な動作にするには、`partition_strategy` 引数を使用します。

`use_hive_partitioning` 設定を 1 にすると、ClickHouse はパス内の Hive-style partitioning (`/name=value/`) を検出し、クエリ内でパーティションカラムを仮想カラムとして使用できるようになります。これらの仮想カラムは、パーティション化されたパス内と同じ名前になります。

**例**

Hive-style partitioning で作成された仮想カラムを使用する

```sql
SELECT * FROM azureBlobStorage(config, storage_account_url='...', container='...', blob_path='http://data/path/date=*/country=*/code=*/*.parquet') WHERE date > '2020-01-01' AND country = 'Netherlands' AND code = 42;
```

<div id="using-shared-access-signatures-sas-sas-tokens">
  ## Shared Access Signature (SAS) の使用
</div>

Shared Access Signature (SAS) は、Azure Storage のコンテナーまたはファイルに対する制限付きアクセスを付与する URI です。これを使用すると、ストレージ アカウントのキーを共有せずに、ストレージ アカウントのリソースへの期限付きアクセスを提供できます。詳細は[こちら](https://learn.microsoft.com/en-us/rest/api/storageservices/delegate-access-with-shared-access-signature)を参照してください。

`azureBlobStorage` 関数は Shared Access Signature (SAS) をサポートしています。

[Blob SAS token](https://learn.microsoft.com/en-us/azure/ai-services/translator/document-translation/how-to-guides/create-sas-tokens?tabs=Containers) には、対象のブロブ、権限、有効期間など、リクエストの認証に必要なすべての情報が含まれています。ブロブ URL を作成するには、ブロブ サービスのエンドポイントに SAS token を追加します。たとえば、エンドポイントが `https://clickhousedocstest.blob.core.windows.net/` の場合、リクエストは次のようになります。

```sql
SELECT count()
FROM azureBlobStorage('BlobEndpoint=https://clickhousedocstest.blob.core.windows.net/;SharedAccessSignature=sp=r&st=2025-01-29T14:58:11Z&se=2025-01-29T22:58:11Z&spr=https&sv=2022-11-02&sr=c&sig=Ac2U0xl4tm%2Fp7m55IilWl1yHwk%2FJG0Uk6rMVuOiD0eE%3D', 'exampledatasets', 'example.csv')

┌─count()─┐
│      10 │
└─────────┘

1 row in set. Elapsed: 0.425 sec.
```

または、生成された[ブロブ SAS URL](https://learn.microsoft.com/en-us/azure/ai-services/translator/document-translation/how-to-guides/create-sas-tokens?tabs=Containers)を使用することもできます。

```sql
SELECT count()
FROM azureBlobStorage('https://clickhousedocstest.blob.core.windows.net/?sp=r&st=2025-01-29T14:58:11Z&se=2025-01-29T22:58:11Z&spr=https&sv=2022-11-02&sr=c&sig=Ac2U0xl4tm%2Fp7m55IilWl1yHwk%2FJG0Uk6rMVuOiD0eE%3D', 'exampledatasets', 'example.csv')

┌─count()─┐
│      10 │
└─────────┘

1 row in set. Elapsed: 0.153 sec.
```

<div id="related">
  ## 関連情報
</div>

* [AzureBlobStorage テーブルエンジン](/ja/engines/table-engines/integrations/azureBlobStorage.md)