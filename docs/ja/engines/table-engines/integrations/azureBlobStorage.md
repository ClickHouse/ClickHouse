---
slug: /ja/engines/table-engines/integrations/azureBlobStorage
sidebar_position: 10
sidebar_label: Azure Blob Storage
---

# AzureBlobStorage テーブルエンジン

このエンジンは、[Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs) エコシステムとの統合を提供します。

## テーブル作成

``` sql
CREATE TABLE azure_blob_storage_table (name String, value UInt32)
    ENGINE = AzureBlobStorage(connection_string|storage_account_url, container_name, blobpath, [account_name, account_key, format, compression])
    [PARTITION BY expr]
    [SETTINGS ...]
```

### エンジンパラメータ

- `endpoint` — container & prefix を含む AzureBlobStorage のエンドポイント URL。認証方法によっては account_name を含む場合があります。(http://azurite1:{port}/[account_name]{container_name}/{data_prefix}) または、これらのパラメータを別々に提供することもできます（storage_account_url, account_name & container を使用）。prefix を指定するには、エンドポイントを使用してください。
- `endpoint_contains_account_name` - このフラグは、エンドポイントに account_name が含まれているかどうかを指定するために使用します。これは特定の認証方法でのみ必要です。(デフォルト: true)
- `connection_string|storage_account_url` — connection_string にはアカウント名とキーが含まれます ([Create connection string](https://learn.microsoft.com/en-us/azure/storage/common/storage-configure-connection-string?toc=%2Fazure%2Fstorage%2Fblobs%2Ftoc.json&bc=%2Fazure%2Fstorage%2Fblobs%2Fbreadcrumb%2Ftoc.json#configure-a-connection-string-for-an-azure-storage-account)) または、ストレージアカウント URL として提供し、アカウント名とアカウントキーを別々のパラメータとして指定できます（パラメータ account_name & account_key 参照）。
- `container_name` - コンテナ名
- `blobpath` - ファイルパス。読み取り専用モードで以下のワイルドカードをサポートします: `*`, `**`, `?`, `{abc,def}` および `{N..M}`、ここで `N`, `M` — 数値, `'abc'`, `'def'` — 文字列。
- `account_name` - storage_account_url を使用している場合、ここでアカウント名を指定できます。
- `account_key` - storage_account_url を使用している場合、ここでアカウントキーを指定できます。
- `format` — ファイルの[フォーマット](/docs/ja/interfaces/formats.md)。
- `compression` — サポートされている値: `none`, `gzip/gz`, `brotli/br`, `xz/LZMA`, `zstd/zst`。デフォルトでは、ファイル拡張子によって圧縮を自動検出します。（`auto` と設定した場合と同じ動作）。

**例**

``` sql
CREATE TABLE test_table (key UInt64, data String)
    ENGINE = AzureBlobStorage('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite1:10000/devstoreaccount1/;',
    'test_container', 'test_table', 'CSV');

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

## 仮想カラム {#virtual-columns}

- `_path` — ファイルへのパス。型: `LowCardinalty(String)`。
- `_file` — ファイル名。型: `LowCardinalty(String)`。
- `_size` — ファイルのバイト単位でのサイズ。型: `Nullable(UInt64)`。サイズが不明な場合、値は `NULL`。
- `_time` — ファイルの最終更新日時。型: `Nullable(DateTime)`。日時が不明な場合、値は `NULL`。

## 認証

現在、3つの認証方法があります:
- `Managed Identity` - `endpoint`、`connection_string` または `storage_account_url` を提供することで使用できます。
- `SAS Token` - `endpoint`、`connection_string` または `storage_account_url` を提供することで使用できます。URL に '?' が含まれる場合に識別されます。
- `Workload Identity` - `endpoint` または `storage_account_url` を提供することで使用できます。config に `use_workload_identity` パラメータが設定されている場合、[workload identity](https://github.com/Azure/azure-sdk-for-cpp/tree/main/sdk/identity/azure-identity#authenticate-azure-hosted-applications) が認証に使用されます。

### データキャッシュ {#data-cache}

`Azure` テーブルエンジンはローカルディスクへのデータキャッシュをサポートしています。
ファイルシステムキャッシュの設定オプションと使用法については、この[セクション](/docs/ja/operations/storing-data.md/#using-local-cache)を参照してください。
キャッシュはパスおよびストレージオブジェクトの ETag に基づいて行われるため、ClickHouse は古いキャッシュバージョンを読み取ることはありません。

キャッシュを有効にするには、設定を `filesystem_cache_name = '<name>'` および `enable_filesystem_cache = 1` とします。

```sql
SELECT *
FROM azureBlobStorage('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite1:10000/devstoreaccount1/;', 'test_container', 'test_table', 'CSV')
SETTINGS filesystem_cache_name = 'cache_for_azure', enable_filesystem_cache = 1;
```

1. ClickHouse の設定ファイルに次のセクションを追加してください:

``` xml
<clickhouse>
    <filesystem_caches>
        <cache_for_azure>
            <path>path to cache directory</path>
            <max_size>10Gi</max_size>
        </cache_for_azure>
    </filesystem_caches>
</clickhouse>
```

2. ClickHouse の `storage_configuration` セクションからキャッシュ設定（およびしたがってキャッシュストレージ）を再利用します。[こちらで説明されています](/docs/ja/operations/storing-data.md/#using-local-cache)

## 参照

[Azure Blob Storage テーブル関数](/docs/ja/sql-reference/table-functions/azureBlobStorage)
