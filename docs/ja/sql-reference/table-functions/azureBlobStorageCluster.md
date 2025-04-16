---
slug: /ja/sql-reference/table-functions/azureBlobStorageCluster
sidebar_position: 15
sidebar_label: azureBlobStorageCluster
title: "azureBlobStorageCluster テーブル関数"
---

指定されたクラスタ内の多くのノードから並行して [Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs) のファイルを処理することを可能にします。起動ノードでは、クラスタ内のすべてのノードに接続を作成し、S3ファイルパス内のアスタリスクを展開し、各ファイルを動的に分配します。ワーカーノードでは、処理すべき次のタスクを起動ノードに問い合わせ、それを処理します。これはすべてのタスクが終了するまで繰り返されます。このテーブル関数は、[s3Cluster 関数](../../sql-reference/table-functions/s3Cluster.md) と似ています。

**構文**

``` sql
azureBlobStorageCluster(cluster_name, connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression, structure])
```

**引数**

- `cluster_name` — リモートおよびローカルサーバへのアドレスと接続パラメータのセットを構築するために使用されるクラスタの名前。
- `connection_string|storage_account_url` — connection_string にはアカウント名とキーが含まれます ([接続文字列の作成](https://learn.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string?toc=%2Fazure%2Fstorage%2Fblobs%2Ftoc.json&bc=%2Fazure%2Fstorage%2Fblobs%2Fbreadcrumb%2Ftoc.json#configure-a-connection-string-for-an-azure-storage-account))。または、ストレージアカウントURLをここで指定し、アカウント名とアカウントキーを別のパラメータとして提供することもできます（パラメータaccount_nameとaccount_keyを参照）。
- `container_name` - コンテナ名。
- `blobpath` - ファイルパス。読み取り専用モードで次のワイルドカードをサポートします: `*`, `**`, `?`, `{abc,def}`, `{N..M}` ここで `N`, `M`は数値、`'abc'`, `'def'`は文字列。
- `account_name` - storage_account_url が使用される場合、ここでアカウント名を指定できます。
- `account_key` - storage_account_url が使用される場合、ここでアカウントキーを指定できます。
- `format` — ファイルの[フォーマット](../../interfaces/formats.md#formats)。
- `compression` — サポートされる値: `none`, `gzip/gz`, `brotli/br`, `xz/LZMA`, `zstd/zst`。デフォルトでは、ファイル拡張子によって圧縮を自動検出します（`auto`に設定するのと同様）。
- `structure` — テーブルの構造。フォーマット `'column1_name column1_type, column2_name column2_type, ...'`。

**返される値**

指定されたファイルでデータを読み書きするための指定された構造のテーブル。

**例**

`cluster_simple` クラスタ内のすべてのノードを使用して、ファイル `test_cluster_*.csv` のカウントを選択します：

``` sql
SELECT count(*) from azureBlobStorageCluster(
        'cluster_simple', 'http://azurite1:10000/devstoreaccount1', 'test_container', 'test_cluster_count.csv', 'devstoreaccount1',
        'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==', 'CSV',
        'auto', 'key UInt64')
```

**参照**

- [AzureBlobStorage エンジン](../../engines/table-engines/integrations/azureBlobStorage.md)
- [azureBlobStorage テーブル関数](../../sql-reference/table-functions/azureBlobStorage.md)
