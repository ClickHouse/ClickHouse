---
slug: /ja/engines/table-engines/integrations/s3queue
sidebar_position: 181
sidebar_label: S3Queue
---

# S3Queue テーブルエンジン

このエンジンは [Amazon S3](https://aws.amazon.com/s3/) エコシステムとの統合を提供し、ストリーミングインポートを可能にします。このエンジンは [Kafka](../../../engines/table-engines/integrations/kafka.md)、[RabbitMQ](../../../engines/table-engines/integrations/rabbitmq.md) エンジンに似ていますが、S3 特有の機能を提供します。

## テーブルの作成 {#creating-a-table}

``` sql
CREATE TABLE s3_queue_engine_table (name String, value UInt32)
    ENGINE = S3Queue(path, [NOSIGN, | aws_access_key_id, aws_secret_access_key,] format, [compression])
    [SETTINGS]
    [mode = '',]
    [after_processing = 'keep',]
    [keeper_path = '',]
    [loading_retries = 0,]
    [processing_threads_num = 1,]
    [enable_logging_to_s3queue_log = 0,]
    [polling_min_timeout_ms = 1000,]
    [polling_max_timeout_ms = 10000,]
    [polling_backoff_ms = 0,]
    [tracked_file_ttl_sec = 0,]
    [tracked_files_limit = 1000,]
    [cleanup_interval_min_ms = 10000,]
    [cleanup_interval_max_ms = 30000,]
```

:::warning
バージョン `24.7` 以前では、`mode`、`after_processing`、`keeper_path` を除くすべての設定に `s3queue_` プレフィックスを使用する必要があります。
:::

**エンジンパラメータ**

`S3Queue` のパラメータは `S3` テーブルエンジンがサポートするものと同じです。詳細は[こちら](../../../engines/table-engines/integrations/s3.md#parameters)を参照してください。

**例**

```sql
CREATE TABLE s3queue_engine_table (name String, value UInt32)
ENGINE=S3Queue('https://clickhouse-public-datasets.s3.amazonaws.com/my-test-bucket-768/*', 'CSV', 'gzip')
SETTINGS
    mode = 'unordered';
```

ネームドコレクションを使用する場合：

``` xml
<clickhouse>
    <named_collections>
        <s3queue_conf>
            <url>'https://clickhouse-public-datasets.s3.amazonaws.com/my-test-bucket-768/*</url>
            <access_key_id>test<access_key_id>
            <secret_access_key>test</secret_access_key>
        </s3queue_conf>
    </named_collections>
</clickhouse>
```

```sql
CREATE TABLE s3queue_engine_table (name String, value UInt32)
ENGINE=S3Queue(s3queue_conf, format = 'CSV', compression_method = 'gzip')
SETTINGS
    mode = 'ordered';
```

## 設定 {#settings}

テーブルに設定された設定のリストを取得するには、`system.s3_queue_settings` テーブルを使用します。`24.10` から使用可能です。

### mode {#mode}

可能な値：

- unordered — 無順序モードでは、すでに処理されたすべてのファイルのセットが ZooKeeper の持続的なノードで追跡されます。
- ordered — 順序付きモードでは、ファイルは辞書順で処理されます。つまり、ある時点で 'BBB' という名前のファイルが処理され、後で 'AA' という名前のファイルがバケットに追加された場合、無視されます。辞書的な意味での最大の名前のファイルと、読み込みが失敗した後に再試行されるファイルの名前のみが ZooKeeper に保存されます。

デフォルト値：バージョン 24.6 より前は `ordered`。24.6 以降はデフォルト値がなく、手動で設定する必要があります。以前のバージョンで作成されたテーブルは、下位互換性のためにデフォルト値が `Ordered` のままとなります。

### after_processing {#after_processing}

処理が成功した後にファイルを削除または保持。

可能な値：

- keep.
- delete.

デフォルト値：`keep`。

### keeper_path {#keeper_path}

ZooKeeper のパスはテーブルエンジンの設定として指定するか、グローバル設定から提供されるパスとテーブル UUID からデフォルトパスを形成することができます。

可能な値：

- 文字列。

デフォルト値：`/`。

### s3queue_loading_retries {#loading_retries}

指定された回数までファイルの読み込みを再試行します。デフォルトでは再試行はありません。

可能な値：

- 正の整数。

デフォルト値：`0`。

### s3queue_processing_threads_num {#processing_threads_num}

処理を行うスレッド数を指定します。`Unordered` モードにのみ適用されます。

デフォルト値：`1`。

### s3queue_enable_logging_to_s3queue_log {#enable_logging_to_s3queue_log}

`system.s3queue_log` へのログを有効にします。

デフォルト値：`0`。

### s3queue_polling_min_timeout_ms {#polling_min_timeout_ms}

次回のポーリングまでの最小タイムアウト（ミリ秒単位）。

可能な値：

- 正の整数。

デフォルト値：`1000`。

### s3queue_polling_max_timeout_ms {#polling_max_timeout_ms}

次回のポーリングまでの最大タイムアウト（ミリ秒単位）。

可能な値：

- 正の整数。

デフォルト値：`10000`。

### s3queue_polling_backoff_ms {#polling_backoff_ms}

ポーリングのバックオフ（ミリ秒単位）。

可能な値：

- 正の整数。

デフォルト値：`0`。

### s3queue_tracked_files_limit {#tracked_files_limit}

「unordered」モードが使用されている場合に Zookeeper ノードの数を制限することができ、「ordered」モードでは何も行いません。制限に達した場合、ZooKeeper ノードから最も古い処理済みファイルが削除され、再処理されます。

可能な値：

- 正の整数。

デフォルト値：`1000`。

### s3queue_tracked_file_ttl_sec {#tracked_file_ttl_sec}

「unordered」モードで ZooKeeper ノードに保存される処理済みファイルを保存する秒数（デフォルトでは永久保存）、`ordered` モードの場合は何もしません。指定された秒数後、ファイルは再インポートされます。

可能な値：

- 正の整数。

デフォルト値：`0`。

### s3queue_cleanup_interval_min_ms {#cleanup_interval_min_ms}

「Ordered」モードの場合、バックグラウンドタスクの再スケジュール間隔の最小境界を定義します。このタスクは、追跡ファイルの TTL と最大追跡ファイルセットの管理を担当します。

デフォルト値：`10000`。

### s3queue_cleanup_interval_max_ms {#cleanup_interval_max_ms}

「Ordered」モードの場合、バックグラウンドタスクの再スケジュール間隔の最大境界を定義します。このタスクは、追跡ファイルの TTL と最大追跡ファイルセットの管理を担当します。

デフォルト値：`30000`。

### s3queue_buckets {#buckets}

「Ordered」モードの場合。バージョン `24.6` から使用可能です。S3Queue テーブルの複数のレプリカが同じメタデータディレクトリで動作している場合、`s3queue_buckets` の値は少なくともレプリカの数に等しくする必要があります。`s3queue_processing_threads` 設定が使用されている場合は、`s3queue_buckets` 設定の値をさらに大きくすることが妥当です。これは `S3Queue` 処理の実際の並行性を定義するためです。

## S3 関連の設定 {#s3-settings}

エンジンはすべての S3 関連の設定をサポートしています。S3 設定に関する詳細は[こちら](../../../engines/table-engines/integrations/s3.md)を参照してください。

## S3Queue 順序付きモード {#ordered-mode}

`S3Queue` の処理モードは、ZooKeeper に保存するメタデータを少なくすることができますが、時間的にあとで追加されたファイルがアルファベット順でより大きな名前を持つ必要があるという制限があります。

`S3Queue` の `ordered` モードでは、`unordered` モードと同様に `(s3queue_)processing_threads_num` 設定（`s3queue_` プレフィックスはオプション）をサポートしており、`S3` ファイルをサーバーでローカルに処理するスレッド数を制御できます。さらに、`ordered` モードは、`(s3queue_)buckets` という設定も導入しており、これは「論理スレッド」を意味します。つまり、複数のサーバーで `S3Queue` テーブルのレプリカがあり、この設定が処理単位の数を定義する分散シナリオでは、各 `S3Queue` レプリカの各処理スレッドが処理のために特定の `bucket` をロックしようとします。各 `bucket` はファイル名のハッシュによって特定のファイルに関連付けられます。したがって、分散シナリオでは、`(s3queue_)buckets` 設定がレプリカの数以上であることが強く推奨されます。バケットの数がレプリカの数よりも多いのは問題ありません。最も最適なシナリオは、`(s3queue_)buckets` 設定が `number_of_replicas` と `(s3queue_)processing_threads_num` の積と等しいことです。

バージョン `24.6` より前では `(s3queue_)processing_threads_num` 設定の使用は推奨されません。

バージョン `24.6` からは `(s3queue_)buckets` 設定が使用可能です。

## 説明 {#description}

`SELECT` はストリーミングインポートには特に有用ではありません（デバッグを除く）ので、各ファイルは一度しかインポートできません。実際には、[マテリアライズドビュー](../../../sql-reference/statements/create/view.md)を使用してリアルタイムスレッドを作成する方が実用的です。これを行うには：

1. S3 の指定されたパスからデータを消費するテーブルをエンジンを使って作成し、それをデータストリームと見なします。
2. 希望する構造を持つテーブルを作成します。
3. エンジンからデータを変換し、前に作成したテーブルにデータを投入するマテリアライズドビューを作成します。

`MATERIALIZED VIEW` がエンジンに結合されると、バックグラウンドでデータの収集を開始します。

例：

``` sql
  CREATE TABLE s3queue_engine_table (name String, value UInt32)
    ENGINE=S3Queue('https://clickhouse-public-datasets.s3.amazonaws.com/my-test-bucket-768/*', 'CSV', 'gzip')
    SETTINGS
        mode = 'unordered';

  CREATE TABLE stats (name String, value UInt32)
    ENGINE = MergeTree() ORDER BY name;

  CREATE MATERIALIZED VIEW consumer TO stats
    AS SELECT name, value FROM s3queue_engine_table;

  SELECT * FROM stats ORDER BY name;
```

## 仮想カラム {#virtual-columns}

- `_path` — ファイルへのパス。
- `_file` — ファイルの名前。

仮想カラムの詳細は[こちら](../../../engines/table-engines/index.md#table_engines-virtual_columns)を参照してください。

## パスのワイルドカード {#wildcards-in-path}

`path` 引数は、bash のようなワイルドカードを使って複数のファイルを指定できます。処理されるには、ファイルが存在し、パスパターン全体に一致する必要があります。ファイルのリストは `SELECT` 時に決定されます（`CREATE` 時ではありません）。

- `*` — `/` を除く任意の文字を任意の回数置き換えます（空文字列を含む）。
- `**` — `/` を含め、任意の文字を任意の回数置き換えます（空文字列を含む）。
- `?` — 任意の単一の文字を置き換えます。
- `{some_string,another_string,yet_another_one}` — 文字列 `'some_string', 'another_string', 'yet_another_one'` のいずれかを置き換えます。
- `{N..M}` — N と M を含む範囲の任意の数字を置き換えます。N と M は先行ゼロを持つことができます（例：`000..078`）。

`{}` を使った構築は [remote](../../../sql-reference/table-functions/remote.md) テーブル関数と似ています。

## 制限事項 {#limitations}

1. 重複した行が発生する可能性があるのは次のような場合です：

- ファイル処理の途中で解析時に例外が発生し、`s3queue_loading_retries` によって再試行が有効になっている場合。

- `S3Queue` が zookeeper 内の同じパスを指す複数のサーバーで設定され、キーパーセッションが期限切れになる前に、あるサーバーが処理済みファイルをコミットするのに失敗した場合。これにより、最初のサーバーによって部分的または完全に処理された可能性のあるファイルを別のサーバーが処理する可能性があります。

- 異常なサーバーの終了。

2. `S3Queue` が zookeeper 内の同じパスを指す複数のサーバーで設定されていて、`Ordered` モードが使用されている場合、`s3queue_loading_retries` は動作しません。これはすぐに修正されます。

## 内部検査 {#introspection}

内部検査には、`system.s3queue` ステートレステーブルと `system.s3queue_log` 永続テーブルを使用します。

1. `system.s3queue`。このテーブルは永続的ではなく、`S3Queue` のメモリ内の状態を示します：現在処理中のファイル、処理済みまたは失敗したファイル。

``` sql
┌─statement──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ CREATE TABLE system.s3queue
(
    `database` String,
    `table` String,
    `file_name` String,
    `rows_processed` UInt64,
    `status` String,
    `processing_start_time` Nullable(DateTime),
    `processing_end_time` Nullable(DateTime),
    `ProfileEvents` Map(String, UInt64)
    `exception` String
)
ENGINE = SystemS3Queue
COMMENT 'Contains in-memory state of S3Queue metadata and currently processed rows per file.' │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

例：

``` sql

SELECT *
FROM system.s3queue

Row 1:
──────
zookeeper_path:        /clickhouse/s3queue/25ea5621-ae8c-40c7-96d0-cec959c5ab88/3b3f66a1-9866-4c2e-ba78-b6bfa154207e
file_name:             wikistat/original/pageviews-20150501-030000.gz
rows_processed:        5068534
status:                Processed
processing_start_time: 2023-10-13 13:09:48
processing_end_time:   2023-10-13 13:10:31
ProfileEvents:         {'ZooKeeperTransactions':3,'ZooKeeperGet':2,'ZooKeeperMulti':1,'SelectedRows':5068534,'SelectedBytes':198132283,'ContextLock':1,'S3QueueSetFileProcessingMicroseconds':2480,'S3QueueSetFileProcessedMicroseconds':9985,'S3QueuePullMicroseconds':273776,'LogTest':17}
exception:
```

2. `system.s3queue_log`。永続的なテーブルです。`processed` および `failed` ファイルについて `system.s3queue` と同じ情報を持っています。

このテーブルの構造は次の通りです：

``` sql
SHOW CREATE TABLE system.s3queue_log

Query id: 0ad619c3-0f2a-4ee4-8b40-c73d86e04314

┌─statement──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ CREATE TABLE system.s3queue_log
(
    `event_date` Date,
    `event_time` DateTime,
    `table_uuid` String,
    `file_name` String,
    `rows_processed` UInt64,
    `status` Enum8('Processed' = 0, 'Failed' = 1),
    `processing_start_time` Nullable(DateTime),
    `processing_end_time` Nullable(DateTime),
    `ProfileEvents` Map(String, UInt64),
    `exception` String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, event_time)
SETTINGS index_granularity = 8192 │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

`system.s3queue_log` を利用するためには、サーバーの構成ファイルにその設定を定義します：

``` xml
    <s3queue_log>
        <database>system</database>
        <table>s3queue_log</table>
    </s3queue_log>
```

例：

``` sql
SELECT *
FROM system.s3queue_log

Row 1:
──────
event_date:            2023-10-13
event_time:            2023-10-13 13:10:12
table_uuid:
file_name:             wikistat/original/pageviews-20150501-020000.gz
rows_processed:        5112621
status:                Processed
processing_start_time: 2023-10-13 13:09:48
processing_end_time:   2023-10-13 13:10:12
ProfileEvents:         {'ZooKeeperTransactions':3,'ZooKeeperGet':2,'ZooKeeperMulti':1,'SelectedRows':5112621,'SelectedBytes':198577687,'ContextLock':1,'S3QueueSetFileProcessingMicroseconds':1934,'S3QueueSetFileProcessedMicroseconds':17063,'S3QueuePullMicroseconds':5841972,'LogTest':17}
exception:
```
