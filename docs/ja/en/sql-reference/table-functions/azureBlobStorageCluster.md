---
description: '指定したクラスター内の多数のノードを使用して、Azure Blob Storage のファイルを並列に処理できます。'
sidebar_label: 'azureBlobStorageCluster'
sidebar_position: 15
slug: /sql-reference/table-functions/azureBlobStorageCluster
title: 'azureBlobStorageCluster'
doc_type: 'reference'
---

指定したクラスター内の多数のノードを使用して、[Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs) 内のファイルを並列に処理できます。イニシエーターでは、クラスター内のすべてのノードへの接続を確立し、S3 のファイルパス内のアスタリスクを展開して、各ファイルを動的に振り分けます。ワーカーノードでは、次に処理するタスクについてイニシエーターに問い合わせ、そのタスクを処理します。これをすべてのタスクが完了するまで繰り返します。
このテーブル関数は [s3Cluster function](../../sql-reference/table-functions/s3Cluster.md) に似ています。

<div id="syntax">
  ## 構文
</div>

```sql
azureBlobStorageCluster(cluster_name, connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression, structure])
```

<div id="arguments">
  ## 引数
</div>

| Argument            | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                         |
| ------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `cluster_name`      | リモートおよびローカルの server へのアドレスのセットと接続パラメーターの構築に使用されるクラスター名。                                                                                                                                                                                                                                                                                                                                                                                                                             |
| `connection_string` | `storage_account_url` — connection&#95;string には account name と key が含まれます ([接続文字列の作成](https://learn.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string?toc=%2Fazure%2Fstorage%2Fblobs%2Ftoc.json\&bc=%2Fazure%2Fstorage%2Fblobs%2Fbreadcrumb%2Ftoc.json#configure-a-connection-string-for-an-azure-storage-account)) 。または、ここで storage account URL を指定し、account name と account key を個別のパラメーターとして指定することもできます (パラメーター `account_name` と `account_key` を参照) 。 |
| `container_name`    | コンテナー名                                                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `blobpath`          | ファイルパス。`readonly` モードでは、次のワイルドカードをサポートします: `*`, `**`, `?`, `{abc,def}` と `{N..M}` (ここで `N`, `M` は数値、`'abc'`, `'def'` は文字列) 。                                                                                                                                                                                                                                                                                                                                                        |
| `account_name`      | storage&#95;account&#95;url を使用する場合は、ここで account name を指定できます                                                                                                                                                                                                                                                                                                                                                                                                                       |
| `account_key`       | storage&#95;account&#95;url を使用する場合は、ここで account key を指定できます                                                                                                                                                                                                                                                                                                                                                                                                                        |
| `format`            | ファイルの[フォーマット](/ja/sql-reference/formats)。                                                                                                                                                                                                                                                                                                                                                                                                                                              |
| `compression`       | サポートされる値: `none`, `gzip/gz`, `brotli/br`, `xz/LZMA`, `zstd/zst`。デフォルトでは、ファイル拡張子から圧縮方式を自動判別します (`auto` を設定した場合と同じです) 。                                                                                                                                                                                                                                                                                                                                                               |
| `structure`         | テーブルの構造。形式は `'column1_name column1_type, column2_name column2_type, ...'` です。                                                                                                                                                                                                                                                                                                                                                                                                       |

<div id="returned_value">
  ## 戻り値
</div>

指定されたファイルに対するデータの読み取りまたは書き込みに使用される、指定された構造のテーブル。

<div id="examples">
  ## 例
</div>

[AzureBlobStorage](/ja/engines/table-engines/integrations/azureBlobStorage) テーブルエンジンと同様に、ローカルで Azure Storage を開発する際には Azurite エミュレーターを使用できます。詳細は[こちら](https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite?tabs=docker-hub%2Cblob-storage)を参照してください。以下では、Azurite がホスト名 `azurite1` で利用可能であると仮定します。

`cluster_simple` クラスター内のすべてのノードを使用して、ファイル `test_cluster_*.csv` の件数を取得します。

```sql
SELECT count(*) FROM azureBlobStorageCluster(
        'cluster_simple', 'http://azurite1:10000/devstoreaccount1', 'testcontainer', 'test_cluster_count.csv', 'devstoreaccount1',
        'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'CSV',
        'auto', 'key UInt64')
```

<div id="using-shared-access-signatures-sas-sas-tokens">
  ## Shared Access Signatures (SAS) の使用
</div>

例については、[azureBlobStorage](/ja/sql-reference/table-functions/azureBlobStorage#using-shared-access-signatures-sas-sas-tokens)を参照してください。

<div id="related">
  ## 関連
</div>

* [AzureBlobStorage エンジン](../../engines/table-engines/integrations/azureBlobStorage.md)
* [azureBlobStorage テーブル関数](../../sql-reference/table-functions/azureBlobStorage.md)