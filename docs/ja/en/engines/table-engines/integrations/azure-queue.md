---
description: 'このエンジンは [Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs) と連携し、
  ストリーミングデータをインポートできます。'
sidebar_label: 'AzureQueue'
sidebar_position: 181
slug: /engines/table-engines/integrations/azure-queue
title: 'AzureQueue テーブルエンジン'
doc_type: 'reference'
---

このエンジンは [Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs) と連携し、ストリーミングデータをインポートできます。

<div id="creating-a-table">
  ## テーブルの作成
</div>

```sql
CREATE TABLE test (name String, value UInt32)
    ENGINE = AzureQueue(...)
    [SETTINGS]
    [mode = '',]
    [after_processing = 'keep',]
    [keeper_path = '',]
    ...
```

**エンジンパラメータ**

`AzureQueue` のパラメータは、`AzureBlobStorage` テーブルエンジンがサポートするものと同じです。パラメータのセクションは[こちら](../../../engines/table-engines/integrations/azureBlobStorage.md)を参照してください。

[AzureBlobStorage](/ja/engines/table-engines/integrations/azureBlobStorage) テーブルエンジンと同様に、Azure Storage のローカル開発には Azurite エミュレータを使用できます。詳細は[こちら](https://learn.microsoft.com/en-us/azure/storage/common/storage-use-azurite?tabs=docker-hub%2Cblob-storage)を参照してください。

**例**

```sql
CREATE TABLE azure_queue_engine_table
(
    `key` UInt64,
    `data` String
)
ENGINE = AzureQueue('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite1:10000/devstoreaccount1/;', 'testcontainer', '*', 'CSV')
SETTINGS mode = 'unordered'
```

<div id="settings">
  ## 設定
</div>

サポートされている設定は、基本的に `S3Queue` テーブルエンジンと同じですが、`s3queue_` プレフィックスは付きません。[設定の完全な一覧](../../../engines/table-engines/integrations/s3queue.md#settings)を参照してください。
テーブルに設定されている項目の一覧を取得するには、`system.azure_queue_settings` テーブルを使用します。`24.10` 以降で利用できます。

以下は、AzureQueue でのみ使用でき、S3Queue には適用されない設定です。

<div id="after_processing_move_connection_string">
  ### `after_processing_move_connection_string`
</div>

宛先が別の Azure コンテナーである場合に、正常に処理されたファイルの移動先として使用する Azure Blob Storage の接続文字列です。

設定可能な値:

* String.

デフォルト値: 空の文字列。

<div id="after_processing_move_container">
  ### `after_processing_move_container`
</div>

処理に成功したファイルの移動先となるコンテナー名。宛先が別の Azure コンテナーである場合に使用します。

設定可能な値:

* String。

デフォルト値: 空文字列。

例:

```sql
CREATE TABLE azure_queue_engine_table
(
    `key` UInt64,
    `data` String
)
ENGINE = AzureQueue('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite1:10000/devstoreaccount1/;', 'testcontainer', '*', 'CSV')
SETTINGS
    mode = 'unordered',
    after_processing = 'move',
    after_processing_move_connection_string = 'DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite1:10000/devstoreaccount1/;',
    after_processing_move_container = 'dst-container';
```

<div id="select">
  ## AzureQueue テーブルエンジンからの SELECT
</div>

AzureQueue テーブルに対する SELECT クエリは、デフォルトでは禁止されています。これは、データを一度読み取ったら queue から削除するという一般的な queue のパターンに従ったものです。誤ってデータが失われるのを防ぐため、SELECT は禁止されています。
ただし、場合によってはこれが有用なこともあります。これを行うには、設定 `stream_like_engine_allow_direct_select` を `True` にする必要があります。
AzureQueue エンジンには、SELECT クエリ用の特別な設定 `commit_on_select` があります。読み取り後も queue 内のデータを保持するには `False` に、削除するには `True` に設定してください。

<div id="description">
  ## 説明
</div>

`SELECT` はストリーミングインポートでは (デバッグ用途を除き) あまり有用ではありません。各ファイルは一度しかインポートできないためです。より実用的なのは、[materialized view](../../../sql-reference/statements/create/view.md) を使ってリアルタイム処理用のスレッドを作成することです。手順は次のとおりです。

1. エンジンを使用して、Azure Blob Storage の指定したパスからデータを取り込むためのテーブルを作成し、それをデータストリームとして扱います。
2. 必要な構造を持つテーブルを作成します。
3. エンジンからのデータを変換し、あらかじめ作成したテーブルに格納する materialized view を作成します。

`MATERIALIZED VIEW` がエンジンに結び付けられると、バックグラウンドでデータの収集を開始します。

エンジンの引数は `AzureQueue(connection_string, container_name, blobpath, format[, compression])` の形式です。

例:

```sql
CREATE TABLE azure_queue_engine_table (key UInt64, data String)
  ENGINE=AzureQueue('DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://azurite1:10000/devstoreaccount1/;', 'testcontainer', '*', 'CSV')
  SETTINGS
      mode = 'unordered';

CREATE TABLE stats (key UInt64, data String)
  ENGINE = MergeTree() ORDER BY key;

CREATE MATERIALIZED VIEW consumer TO stats
  AS SELECT key, data FROM azure_queue_engine_table;

SELECT * FROM stats ORDER BY key;
```

<div id="virtual-columns">
  ## 仮想カラム
</div>

* `_path` — ファイルのパス。
* `_file` — ファイル名。

仮想カラムの詳細については、[こちら](../../../engines/table-engines/index.md#table_engines-virtual_columns)をご覧ください。

<div id="introspection">
  ## イントロスペクション
</div>

テーブル設定 `enable_logging_to_queue_log=1` を使用して、そのテーブルのログを有効にします。

イントロスペクション機能は [S3Queue テーブルエンジン](/ja/engines/table-engines/integrations/s3queue#introspection) と同じですが、いくつか明確な違いがあります。

1. サーバーバージョンが &gt;= 25.1 の場合、キューのメモリ上の状態には `system.azure_queue_metadata_cache` を使用します。古いバージョンでは `system.s3queue_metadata_cache` を使用します (これには `azure` テーブルの情報も含まれます) 。
2. ClickHouse のメイン設定で `system.azure_queue_log` を有効にします。例:

```xml
  <azure_queue_log>
    <database>system</database>
    <table>azure_queue_log</table>
  </azure_queue_log>
```

この永続テーブルには、`system.s3queue_metadata_cache` と同じ情報が格納されていますが、処理済みファイルと失敗したファイルに関するものです。

このテーブルの構造は次のとおりです。

```sql

CREATE TABLE system.azure_queue_log
(
    `hostname` LowCardinality(String) COMMENT 'Hostname',
    `event_date` Date COMMENT 'Event date of writing this log row',
    `event_time` DateTime COMMENT 'Event time of writing this log row',
    `database` String COMMENT 'The name of a database where current S3Queue table lives.',
    `table` String COMMENT 'The name of S3Queue table.',
    `uuid` String COMMENT 'The UUID of S3Queue table',
    `file_name` String COMMENT 'File name of the processing file',
    `rows_processed` UInt64 COMMENT 'Number of processed rows',
    `status` Enum8('Processed' = 0, 'Failed' = 1) COMMENT 'Status of the processing file',
    `processing_start_time` Nullable(DateTime) COMMENT 'Time of the start of processing the file',
    `processing_end_time` Nullable(DateTime) COMMENT 'Time of the end of processing the file',
    `exception` String COMMENT 'Exception message if happened'
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, event_time)
COMMENT 'Contains logging entries with the information files processes by S3Queue engine.'

```

例:

```sql
SELECT *
FROM system.azure_queue_log
LIMIT 1
FORMAT Vertical

Row 1:
──────
hostname:              clickhouse
event_date:            2024-12-16
event_time:            2024-12-16 13:42:47
database:              default
table:                 azure_queue_engine_table
uuid:                  1bc52858-00c0-420d-8d03-ac3f189f27c8
file_name:             test_1.csv
rows_processed:        3
status:                Processed
processing_start_time: 2024-12-16 13:42:47
processing_end_time:   2024-12-16 13:42:47
exception:

1 row in set. Elapsed: 0.002 sec.

```