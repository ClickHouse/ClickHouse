---
slug: /ja/engines/table-engines/integrations/azure-queue
sidebar_position: 181
sidebar_label: AzureQueue
---

# AzureQueue テーブルエンジン

このエンジンは、[Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs) エコシステムとの統合を提供し、ストリーミングデータのインポートを可能にします。

## テーブルの作成 {#creating-a-table}

``` sql
CREATE TABLE test (name String, value UInt32)
    ENGINE = AzureQueue(...)
    [SETTINGS]
    [mode = '',]
    [after_processing = 'keep',]
    [keeper_path = '',]
    ...
```

**エンジンパラメータ**

`AzureQueue` のパラメータは、`AzureBlobStorage` テーブルエンジンがサポートするものと同じです。パラメータの詳細は[こちら](../../../engines/table-engines/integrations/azureBlobStorage.md)をご覧ください。

**例**

```sql
CREATE TABLE azure_queue_engine_table (name String, value UInt32)
ENGINE=AzureQueue('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite1:10000/devstoreaccount1/data/')
SETTINGS
    mode = 'unordered'
```

## 設定 {#settings}

サポートされている設定のセットは、`S3Queue` テーブルエンジンと同じですが、`s3queue_` プレフィックスはありません。設定の完全なリストについては[こちら](../../../engines/table-engines/integrations/s3queue.md#settings)をご覧ください。
テーブルに設定された設定のリストを取得するには、`system.s3_queue_settings` テーブルを使用します。`24.10` から使用可能です。

## 説明 {#description}

`SELECT` はストリーミングインポートには特に有用ではありません（デバッグを除く）、なぜなら各ファイルは一度しかインポートできないからです。[Materialized View](../../../sql-reference/statements/create/view.md) を使用してリアルタイムスレッドを作成する方が実用的です。手順は次の通りです：

1. エンジンを使用して、S3 の指定されたパスから消費するためのテーブルを作成し、それをデータストリームとして考慮します。
2. 必要な構造でテーブルを作成します。
3. エンジンからデータを変換し、前に作成したテーブルに投入する Materialized View を作成します。

`MATERIALIZED VIEW` がエンジンに接続されると、バックグラウンドでデータの収集を開始します。

例：

``` sql
  CREATE TABLE azure_queue_engine_table (name String, value UInt32)
    ENGINE=AzureQueue('<endpoint>', 'CSV', 'gzip')
    SETTINGS
        mode = 'unordered';

  CREATE TABLE stats (name String, value UInt32)
    ENGINE = MergeTree() ORDER BY name;

  CREATE MATERIALIZED VIEW consumer TO stats
    AS SELECT name, value FROM azure_queue_engine_table;

  SELECT * FROM stats ORDER BY name;
```

## 仮想カラム {#virtual-columns}

- `_path` — ファイルへのパス。
- `_file` — ファイルの名前。

仮想カラムに関する詳細については[こちら](../../../engines/table-engines/index.md#table_engines-virtual_columns)をご覧ください。
