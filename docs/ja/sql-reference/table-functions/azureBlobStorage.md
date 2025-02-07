---
slug: /ja/sql-reference/table-functions/azureBlobStorage
sidebar_position: 10
sidebar_label: azureBlobStorage
keywords: [azure blob storage]
---

# azureBlobStorage テーブル関数

[Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs) のファイルを選択/挿入するためのテーブルのようなインターフェースを提供します。このテーブル関数は [s3 関数](../../sql-reference/table-functions/s3.md)と似ています。

**構文**

``` sql
azureBlobStorage(- connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression, structure])
```

**引数**

- `connection_string|storage_account_url` — connection_string はアカウント名とキーを含みます ([connection string の作成](https://learn.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string?toc=%2Fazure%2Fstorage%2Fblobs%2Ftoc.json&bc=%2Fazure%2Fstorage%2Fblobs%2Fbreadcrumb%2Ftoc.json#configure-a-connection-string-for-an-azure-storage-account)) または、ストレージアカウントのURLをここに提供し、アカウント名とアカウントキーを別々のパラメータとして指定することもできます（パラメータのaccount_nameとaccount_keyを参照）
- `container_name` - コンテナー名
- `blobpath` - ファイルパス。リードオンリーモードでは以下のワイルドカードがサポートされています: `*`, `**`, `?`, `{abc,def}` および `{N..M}` ここで `N`, `M` — 数字, `'abc'`, `'def'` — 文字列。
- `account_name` - storage_account_url が使用されている場合は、ここでアカウント名を指定できます
- `account_key` - storage_account_url が使用されている場合は、ここでアカウントキーを指定できます
- `format` — ファイルの[フォーマット](../../interfaces/formats.md#formats)。
- `compression` — サポートされている値: `none`, `gzip/gz`, `brotli/br`, `xz/LZMA`, `zstd/zst`。デフォルトでは、ファイル拡張子によって圧縮を自動的に検出します（`auto`に設定した場合と同じ）。
- `structure` — テーブルの構造。フォーマット `'column1_name column1_type, column2_name column2_type, ...'`。

**返される値**

指定されたファイルにおいてデータを読み書きするための指定された構造のテーブル。

**例**

以下を使用して Azure Blob Storage にデータを書き込みます:

```sql
INSERT INTO TABLE FUNCTION azureBlobStorage('http://azurite1:10000/devstoreaccount1',
    'test_container', 'test_{_partition_id}.csv', 'devstoreaccount1', 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==',
    'CSV', 'auto', 'column1 UInt32, column2 UInt32, column3 UInt32') PARTITION BY column3 VALUES (1, 2, 3), (3, 2, 1), (78, 43, 3);
```

そのデータは次の方法で読み取ることができます

```sql
SELECT * FROM azureBlobStorage('http://azurite1:10000/devstoreaccount1',
    'test_container', 'test_1.csv', 'devstoreaccount1', 'Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==',
    'CSV', 'auto', 'column1 UInt32, column2 UInt32, column3 UInt32');
```

```response
┌───column1─┬────column2─┬───column3─┐
│     3     │       2    │      1    │
└───────────┴────────────┴───────────┘
```

または connection_string を使用する

```sql
SELECT count(*) FROM azureBlobStorage('DefaultEndpointsProtocol=https;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;EndPointSuffix=core.windows.net',
    'test_container', 'test_3.csv', 'CSV', 'auto' , 'column1 UInt32, column2 UInt32, column3 UInt32');
```

``` text
┌─count()─┐
│      2  │
└─────────┘
```

## 仮想カラム {#virtual-columns}

- `_path` — ファイルへのパス。タイプ: `LowCardinality(String)`。
- `_file` — ファイル名。タイプ: `LowCardinality(String)`。
- `_size` — ファイルのサイズ（バイト単位）。タイプ: `Nullable(UInt64)`。ファイルサイズが不明な場合、値は `NULL` になります。
- `_time` — ファイルの最終更新時刻。タイプ: `Nullable(DateTime)`。時刻が不明な場合、値は `NULL` になります。

**関連項目**

- [AzureBlobStorage テーブルエンジン](/docs/ja/engines/table-engines/integrations/azureBlobStorage.md)

## Hiveスタイルのパーティショニング {#hive-style-partitioning}

`use_hive_partitioning` を 1 に設定すると、ClickHouse はパス内の Hiveスタイルのパーティショニング(`/name=value/`)を検出し、パーティションカラムをクエリ内で仮想カラムとして使用できるようにします。これらの仮想カラムは、パーティションされたパス内と同じ名前を持ちますが、先頭に `_` が付きます。

**例**

Hiveスタイルのパーティショニングで作成された仮想カラムを使用する

``` sql
SET use_hive_partitioning = 1;
SELECT * from azureBlobStorage(config, storage_account_url='...', container='...', blob_path='http://data/path/date=*/country=*/code=*/*.parquet') where _date > '2020-01-01' and _country = 'Netherlands' and _code = 42;
```
