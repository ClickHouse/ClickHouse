---
description: 'このエンジンは Amazon S3 エコシステムとのインテグレーションを提供し、
  ストリーミングインポートを可能にします。Kafka および RabbitMQ エンジンに似ていますが、S3 固有の
  機能も提供します。'
sidebar_label: 'S3Queue'
sidebar_position: 181
slug: /engines/table-engines/integrations/s3queue
title: 'S3Queue テーブルエンジン'
doc_type: 'reference'
---

import ScalePlanFeatureBadge from '@theme/badges/ScalePlanFeatureBadge'

<div id="s3queue-table-engine">
  # S3Queue テーブルエンジン
</div>

このエンジンは [Amazon S3](https://aws.amazon.com/s3/) エコシステムとのインテグレーションを提供し、ストリーミングインポートを可能にします。このエンジンは [Kafka](../../../engines/table-engines/integrations/kafka.md)、[RabbitMQ](../../../engines/table-engines/integrations/rabbitmq.md) エンジンに似ていますが、S3 固有の機能を備えています。

[S3Queue 実装の元の PR](https://github.com/ClickHouse/ClickHouse/pull/49086/files#diff-e1106769c9c8fbe48dd84f18310ef1a250f2c248800fde97586b3104e9cd6af8R183) にある次の注意点を理解しておくことが重要です。`MATERIALIZED VIEW` がこのエンジンに接続されると、S3Queue テーブルエンジンはバックグラウンドでデータの収集を開始します。

<div id="creating-a-table">
  ## テーブルを作成
</div>

```sql
CREATE TABLE s3_queue_engine_table (name String, value UInt32)
    ENGINE = S3Queue(path, [NOSIGN, | aws_access_key_id, aws_secret_access_key,] format, [compression], [headers], [extra_credentials])
    [SETTINGS]
    [mode = '',]
    [after_processing = 'keep',]
    [keeper_path = '',]
    [loading_retries = 10,]
    [processing_threads_num = 16,]
    [parallel_inserts = false,]
    [enable_logging_to_queue_log = true,]
    [last_processed_path = "",]
    [tracked_files_limit = 1000,]
    [tracked_file_ttl_sec = 0,]
    [polling_min_timeout_ms = 1000,]
    [polling_max_timeout_ms = 600000,]
    [polling_backoff_ms = 30000,]
    [cleanup_interval_min_ms = 60000,]
    [cleanup_interval_max_ms = 60000,]
    [buckets = 0,]
    [list_objects_batch_size = 1000,]
    [enable_hash_ring_filtering = 0,]
    [max_processed_files_before_commit = 100,]
    [max_processed_rows_before_commit = 0,]
    [max_processed_bytes_before_commit = 0,]
    [max_processing_time_sec_before_commit = 0,]
```

:::warning
`24.7` より前のバージョンでは、`mode`、`after_processing`、`keeper_path` を除くすべての設定で `s3queue_` プレフィックスを使用する必要があります。
:::

**エンジンパラメータ**

`S3Queue` のパラメータは、`S3` テーブルエンジンでサポートされているものと同じです。パラメータのセクションは[こちら](../../../engines/table-engines/integrations/s3.md#parameters)を参照してください。

**例**

```sql
CREATE TABLE s3queue_engine_table (name String, value UInt32)
ENGINE=S3Queue('https://clickhouse-public-datasets.s3.amazonaws.com/my-test-bucket-768/*', 'CSV', 'gzip')
SETTINGS
    mode = 'unordered';
```

named collectionsを使用する場合:

```xml
<clickhouse>
    <named_collections>
        <s3queue_conf>
            <url>https://clickhouse-public-datasets.s3.amazonaws.com/my-test-bucket-768/*</url>
            <access_key_id>test</access_key_id>
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

<div id="settings">
  ## 設定
</div>

テーブルに設定されている設定の一覧を取得するには、`system.s3_queue_settings` テーブルを使用します。`24.10` から利用できます。

:::note 設定名 (24.7+)
バージョン 24.7 以降では、S3Queue の設定は `s3queue_` プレフィックスの有無にかかわらず指定できます。

* **新しい構文** (24.7+): `processing_threads_num`, `tracked_file_ttl_sec` など
* **従来の構文** (すべてのバージョン): `s3queue_processing_threads_num`, `s3queue_tracked_file_ttl_sec` など

24.7+ では、どちらの形式もサポートされています。このページの例では、プレフィックスのない新しい構文を使用しています。
:::

<div id="mode">
  ### モード
</div>

設定可能な値:

* unordered — 順不同モードでは、すでに処理されたすべてのファイルの集合が、ZooKeeper の永続ノードによって追跡されます。
* ordered — ordered モードでは、ファイルは辞書順で処理されます。つまり、ある時点で &#39;BBB&#39; という名前のファイルが処理され、その後で &#39;AA&#39; という名前のファイルがバケットに追加された場合、そのファイルは無視されます。ZooKeeper に保存されるのは、正常に取り込まれたファイルのうち辞書順で最大の名前と、読み込みに失敗して再試行対象となるファイルの名前だけです。

デフォルト値: 24.6 より前のバージョンでは `ordered`。24.6 以降はデフォルト値がなくなり、この設定を手動で指定する必要があります。以前のバージョンで作成されたテーブルでは、互換性のためデフォルト値は `Ordered` のままです。

<div id="after_processing">
  ### `after_processing`
</div>

正常に処理された後のファイルの扱い。

設定可能な値:

* keep.
* delete.
* move.
* tag.

デフォルト値: `keep`.

move には追加の設定が必要です。同じ バケット 内で move する場合は、新しい パスプレフィックス を `after_processing_move_prefix` として指定する必要があります。

別の S3 バケット に move する場合は、対象 バケット の URI を `after_processing_move_uri` として、S3 の認証情報を `after_processing_move_access_key_id` および `after_processing_move_secret_access_key` として指定する必要があります。

例:

```sql
CREATE TABLE s3queue_engine_table (name String, value UInt32)
ENGINE=S3Queue('https://clickhouse-public-datasets.s3.amazonaws.com/my-test-bucket-768/*', 'CSV', 'gzip')
SETTINGS
    mode = 'unordered',
    after_processing = 'move',
    after_processing_retries = 20,
    after_processing_move_prefix = 'dst_prefix',
    after_processing_move_uri = 'https://clickhouse-public-datasets.s3.amazonaws.com/dst-bucket',
    after_processing_move_access_key_id = 'test',
    after_processing_move_secret_access_key = 'test';
```

ある Azure コンテナーから別の Azure コンテナーへ移動するには、Blob Storage の接続文字列を `after_processing_move_connection_string` として、コンテナー名を `after_processing_move_container` として指定する必要があります。[AzureQueue の Settings](../../../engines/table-engines/integrations/azure-queue.md#settings)を参照してください。

タグ付けを行うには、タグのキーと値をそれぞれ `after_processing_tag_key` および `after_processing_tag_value` として指定する必要があります。

<div id="after_processing_retries">
  ### `after_processing_retries`
</div>

指定された後処理アクションを断念するまでの再試行回数です。

設定可能な値:

* 0以上の整数。

デフォルト値: `10`。

<div id="after_processing_move_access_key_id">
  ### `after_processing_move_access_key_id`
</div>

宛先が別の S3 バケット の場合に、正常に処理されたファイルの移動先となる S3 バケット の Access Key ID。

設定可能な値:

* String.

デフォルト値: 空文字列。

<div id="after_processing_move_prefix">
  ### `after_processing_move_prefix`
</div>

正常に処理されたファイルの移動先となるパスプレフィックスです。同じバケット内で移動する場合と、別のバケットに移動する場合の両方に適用されます。

設定可能な値:

* String.

デフォルト値: 空文字列。

<div id="after_processing_move_preserve_path">
  ### `after_processing_move_preserve_path`
</div>

`true` の場合、正常に処理されたファイルを移動する際に、ソースオブジェクトの完全なパスが `after_processing_move_prefix` に追加されるため、バケット配下のソースディレクトリ構造が宛先でも保持されます。`false` の場合は、ファイル名のみが使用され、ソースディレクトリ構造はフラット化されます。

設定可能な値:

* `true` / `false`.

デフォルト値: `false`.

<div id="after_processing_move_secret_access_key">
  ### `after_processing_move_secret_access_key`
</div>

宛先が別の S3バケットである場合に、正常に処理されたファイルの移動先となる S3バケットの Secret Access Key。

設定可能な値:

* String。

デフォルト値: 空文字列。

<div id="after_processing_move_uri">
  ### `after_processing_move_uri`
</div>

宛先が別の S3 バケットである場合に、正常に処理されたファイルの移動先となる S3 バケットの URI を指定します。

設定可能な値:

* String.

デフォルト値: 空文字列。

<div id="after_processing_tag_key">
  ### `after_processing_tag_key`
</div>

`after_processing='tag'` の場合に、正常に処理されたファイルにタグを付けるためのタグキー。

設定可能な値:

* String。

デフォルト値: 空文字列。

<div id="after_processing_tag_value">
  ### `after_processing_tag_value`
</div>

`after_processing='tag'` の場合に、正常に処理されたファイルに付与するタグの値です。

設定可能な値:

* String。

デフォルト値: 空文字列。

<div id="keeper_path">
  ### `keeper_path`
</div>

ZooKeeper 内のキューのメタデータへのパスです。明示的に指定しない場合、ClickHouse は `s3queue_default_zookeeper_path`、データベース UUID、テーブル UUID からこのパスを組み立てます。絶対パス (`/` で始まる値) はそのまま使用され、相対値は設定されたプレフィックスに追加されます。`{database}` や `{uuid}` などのマクロは、エンジンが ZooKeeper に接続する前に展開されます。

補助的な ZooKeeper クラスターを指定するには、値の先頭に設定済みの名前を付けます。たとえば `analytics_keeper:/clickhouse/queue/orders` のように指定します。この名前は `<auxiliary_zookeepers>` に存在している必要があります。存在しない場合、エンジンは `Unknown auxiliary ZooKeeper name ...` を報告します。文字列全体 (プレフィックスを含む) は `SHOW CREATE TABLE` に保持されるため、ステートメントをそのまま再現できます。

設定可能な値:

* String.

デフォルト値: `/`.

<div id="loading_retries">
  ### `loading_retries`
</div>

ファイルの読み込みを、指定した回数まで再試行します。
設定可能な値:

* 0以上の整数。

デフォルト値: `10`。

<div id="processing_threads_num">
  ### `processing_threads_num`
</div>

処理を実行するスレッド数。`Unordered` モードでのみ適用されます。

デフォルト値: CPU 数または 16。

<div id="parallel_inserts">
  ### `parallel_inserts`
</div>

デフォルトでは、`processing_threads_num` は 1 つの `INSERT` のみを生成するため、複数スレッドで並列に実行されるのはファイルのダウンロードと解析だけです。
しかし、これでは並列度が制限されるため、より高いスループットが必要な場合は `parallel_inserts=true` を使用してください。これによりデータを並列に挿入できます (ただし、MergeTree family で生成されるデータパーツの数は増える点に注意してください) 。

:::note
`INSERT` は `max_process*_before_commit` 設定に応じて生成されます。
:::

デフォルト値: `false`。

<div id="enable_logging_to_queue_log">
  ### `enable_logging_to_queue_log`
</div>

`system.s3queue_log` へのログ出力を有効にします。

デフォルト値: `1`。

<div id="polling_min_timeout_ms">
  ### `polling_min_timeout_ms`
</div>

ClickHouse が次回のポーリングを試行するまでの最小待機時間を、ミリ秒単位で指定します。

設定可能な値:

* 正の整数。

デフォルト値: `1000`。

<div id="polling_max_timeout_ms">
  ### `polling_max_timeout_ms`
</div>

ClickHouse が次回のポーリングを開始するまでに待機する最大時間を、ミリ秒単位で定義します。

設定可能な値:

* 正の整数。

デフォルト値: `600000`.

<div id="polling_backoff_ms">
  ### `polling_backoff_ms`
</div>

新しいファイルが見つからなかった場合に、前回のポーリング間隔に追加される待機時間を指定します。次回のポーリングは、前回の間隔とこのバックオフ値の合計、または最大間隔のいずれか小さい方が経過した後に実行されます。

設定可能な値:

* 正の整数。

デフォルト値: `30000`.

<div id="tracked_files_limit">
  ### `tracked_files_limit`
</div>

&#39;unordered&#39; モード使用時に ZooKeeper ノード数を制限できます。&#39;ordered&#39; モードでは効果はありません。
制限に達すると、最も古く処理されたファイルが ZooKeeper ノードから削除され、再度処理されます。

設定可能な値:

* 正の整数。

デフォルト値: `1000`。

<div id="tracked_file_ttl_sec">
  ### `tracked_file_ttl_sec`
</div>

&#39;unordered&#39; モードでは、処理済みファイルを ZooKeeperノードに保存しておく最大秒数です (デフォルトでは無期限に保存されます) 。&#39;ordered&#39; モードでは効果はありません。
指定した秒数が経過すると、そのファイルは再インポートされます。

設定可能な値:

* 正の整数。

デフォルト値: `0`。

<div id="cleanup_interval_min_ms">
  ### `cleanup_interval_min_ms`
</div>

&#39;Ordered&#39; モード用です。追跡対象ファイルの有効期限 (TTL) と追跡対象ファイル数の上限を維持するバックグラウンドタスクの再スケジュール間隔の下限を定義します。

デフォルト値: `60000`。

<div id="cleanup_interval_max_ms">
  ### `cleanup_interval_max_ms`
</div>

「Ordered」モード用です。追跡対象ファイルの有効期限 (TTL) と追跡対象ファイル数の上限を管理するバックグラウンドタスクについて、再スケジュール間隔の上限を定義します。

デフォルト値: `60000`.

<div id="buckets">
  ### `buckets`
</div>

`Ordered` モード用です。`24.6` 以降で利用できます。S3Queue テーブルのレプリカが複数あり、それぞれが Keeper 内の同じメタデータディレクトリを使用している場合、`buckets` の値は少なくともレプリカ数と同じである必要があります。`processing_threads` 設定も使用する場合は、`buckets` 設定の値をさらに増やすのが適切です。これは、`S3Queue` 処理の実際の並列度を決定するためです。

<div id="use_persistent_processing_nodes">
  ### `use_persistent_processing_nodes`
</div>

デフォルトでは、S3Queue テーブルは常に一時的な処理ノードを使用します。これは、S3Queue が ZooKeeper に処理済みファイルをコミットする前、ただし処理開始後に ZooKeeper の session が期限切れになると、データが重複する可能性があるためです。この設定を有効にすると、Keeper session の期限切れによって重複が発生する可能性をサーバーが排除します。

<div id="persistent_processing_node_ttl_seconds">
  ### `persistent_processing_node_ttl_seconds`
</div>

サーバーが正常に終了しなかった場合、`use_persistent_processing_nodes` が有効になっていると、削除されずに残った処理ノードが存在する可能性があります。この設定は、それらの処理ノードを安全にクリーンアップできる期間を定義します。同じ有効期限 (TTL) は `Ordered` モードのバケットロックにも使用されます。バケットロックは単一の処理ノードより長く保持される場合があるため、その点も考慮して値を設定する必要があります。

デフォルト値: `21600` (6時間) 。

<div id="s3-settings">
  ## S3 関連の設定
</div>

このエンジンは、S3 関連のすべての設定に対応しています。S3 の設定の詳細については、[こちら](../../../engines/table-engines/integrations/s3.md)を参照してください。

<div id="s3-role-based-access">
  ## S3 のロールベースアクセス
</div>

<ScalePlanFeatureBadge feature="S3 Role-Based Access" />

S3Queue テーブルエンジンは、ロールベースアクセスに対応しています。
ロールを設定してバケットにアクセスする手順については、[こちら](/ja/cloud/data-sources/secure-s3)のドキュメントを参照してください。

ロールの設定後は、以下のように `extra_credentials` パラメータで `roleARN` を渡せます。

```sql
CREATE TABLE s3_table
(
    ts DateTime,
    value UInt64
)
ENGINE = S3Queue(
                'https://<your_bucket>/*.csv',
                extra_credentials(role_arn = 'arn:aws:iam::111111111111:role/<your_role>')
                ,'CSV')
SETTINGS
    ...
```

<div id="ordered-mode">
  ## S3Queue ordered モード
</div>

`S3Queue` の処理モードでは、ZooKeeper に保存するメタデータを減らせますが、後から追加されるファイルには、英数字順でそれ以前のファイルより大きい名前が必要になるという制約があります。

`S3Queue` の `ordered` モードは、`unordered` と同様に `(s3queue_)processing_threads_num` 設定 (`s3queue_` プレフィックスは省略可能) をサポートしており、サーバー上で `S3` ファイルをローカルに処理するスレッド数を制御できます。

パーティション化なしの `ordered` モードでは、プレフィックス全体の履歴を再度一覧取得するのを避けるため、ClickHouse は最後に処理した key から S3 の一覧取得を再開する場合があります。バケット化された `ordered` モードでは、未処理ファイルのスキップを避けるため、再開位置として、すべての バケット にまたがる処理済み key のうち最小のものが保守的に選択されます。
この一覧取得再開の最適化は、パーティション化なしの `ordered` モードにおける S3 バックエンドの queue でのみ使用されます (AzureQueue や `partitioning_mode` が設定されている場合は対象外です) 。
さらに、`ordered` モードでは `(s3queue_)buckets` という別の設定も導入されており、これは「論理スレッド」を意味します。これは、分散環境で `S3Queue` table のレプリカを持つ server が複数ある場合に、この設定が処理ユニット数を定義するという意味です。たとえば、各 `S3Queue` レプリカ上の各処理スレッドは、処理のために特定の `バケット` のロックを取得しようとします。各 `バケット` には、ファイル名の hash によって特定のファイルが割り当てられます。したがって、分散環境では `(s3queue_)buckets` 設定を少なくとも replicas 数と同じか、それ以上にすることを強く推奨します。バケット 数が replicas 数より大きくても問題ありません。最も望ましいのは、`(s3queue_)buckets` 設定を `number_of_replicas` と `(s3queue_)processing_threads_num` の積と等しくすることです。
`(s3queue_)processing_threads_num` 設定は、バージョン `24.6` より前では使用を推奨しません。
`(s3queue_)buckets` 設定はバージョン `24.6` 以降で利用できます。

<div id="select">
  ## S3Queue テーブルエンジンでの SELECT
</div>

S3Queue テーブルに対する SELECT クエリは、デフォルトで禁止されています。これは、データを一度読み取るとキューから削除するという一般的なキューのパターンに従ったものです。SELECT が禁止されているのは、意図しないデータ損失を防ぐためです。
ただし、場合によっては SELECT が有用なこともあります。その場合は、設定 `stream_like_engine_allow_direct_select` を `True` に設定する必要があります。
S3Queue エンジンには、SELECT クエリ用の特別な設定 `commit_on_select` があります。読み取り後もキュー内のデータを保持するには `False`、削除するには `True` に設定してください。

<div id="description">
  ## 説明
</div>

`SELECT` は、各ファイルをインポートできるのが一度きりであるため、ストリーミングインポートには (デバッグ用途を除き) あまり適していません。[materialized view](../../../sql-reference/statements/create/view.md) を使ってリアルタイム処理のスレッドを作成するほうが実用的です。これを行うには、次のようにします。

1. このエンジンを使って、S3 の指定したパスからデータを取り込むためのテーブルを作成し、それをデータストリームと見なします。
2. 必要な構造を持つテーブルを作成します。
3. エンジンからのデータを変換し、事前に作成したテーブルに格納する materialized view を作成します。

`MATERIALIZED VIEW` をこのエンジンに関連付けると、バックグラウンドでデータの収集が始まります。

例:

```sql
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

<div id="virtual-columns">
  ## 仮想カラム
</div>

* `_path` — ファイルのパス。
* `_file` — ファイル名。
* `_size` — ファイルのサイズ。
* `_time` — ファイルの作成時刻。

仮想カラムの詳細については、[こちら](../../../engines/table-engines/index.md#table_engines-virtual_columns)をご覧ください。

<div id="wildcards-in-path">
  ## パス内のワイルドカード
</div>

`path` 引数では、Bash 風のワイルドカードを使って複数のファイルを指定できます。処理対象にするには、ファイルが存在し、パスパターン全体に一致している必要があります。ファイルの一覧は `SELECT` の実行時に決定されます (`CREATE` 時点ではありません) 。

* `*` — 空文字列を含む、`/` 以外の任意の文字列に一致します。
* `**` — 空文字列を含む、`/` を含む任意の文字列に一致します。
* `?` — 任意の 1 文字に一致します。
* `{some_string,another_string,yet_another_one}` — `'some_string'`、`'another_string'`、`'yet_another_one'` のいずれかの文字列に一致します。
* `{N..M}` — N から M までの範囲内の任意の数値に一致します (両端を含みます) 。N と M には先頭にゼロを付けることもできます。たとえば `000..078` です。

`{}` を使った構文は、[remote](../../../sql-reference/table-functions/remote.md) テーブル関数に似ています。

<div id="limitations">
  ## 制限事項
</div>

1. 重複した行が発生する原因として、次のものがあります。

* ファイル処理の途中でパース中に例外が発生し、`s3queue_loading_retries` によって再試行が有効になっている場合。

* `S3Queue` が ZooKeeper 内の同じパスを指すように複数のサーバーで設定されており、あるサーバーが処理済みファイルをコミットする前に Keeper セッションの有効期限が切れると、別のサーバーがそのファイルの処理を引き継ぐ可能性があります。その結果、最初のサーバーですでに部分的または完全に処理されていたファイルが再度処理されることがあります。ただし、`use_persistent_processing_nodes = 1` の場合、バージョン 25.8 以降ではこれは当てはまりません。

* サーバーの異常終了。

2. `S3Queue` が ZooKeeper 内の同じパスを指すように複数のサーバーで設定され、`Ordered` モードが使用されている場合、`s3queue_loading_retries` は機能しません。これはまもなく修正される予定です。

<div id="introspection">
  ## イントロスペクション
</div>

イントロスペクションには、ステートレステーブル `system.s3queue_metadata_cache` と、永続テーブル `system.s3queue_log` を使用します。

1. `system.s3queue_metadata_cache`。このテーブルは永続的ではなく、`S3Queue` のメモリ内の状態、つまり現在どのファイルが処理中で、どのファイルが処理済みまたは失敗したかを表示します。

```sql
┌─statement──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│ CREATE TABLE system.s3queue_metadata_cache
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

例:

```sql

SELECT *
FROM system.s3queue_metadata_cache

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

2. `system.s3queue_log`。永続テーブルです。`processed` および `failed` ファイルに関する情報である点を除き、`system.s3queue_metadata_cache` と同じ情報を持ちます。

このテーブルは次の構造を持ちます:

```sql
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
ORDER BY (event_date, event_time) │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

`system.s3queue_log` を使用するには、サーバー設定ファイルにその設定を定義します：

```xml
    <s3queue_log>
        <database>system</database>
        <table>s3queue_log</table>
    </s3queue_log>
```

例:

```sql
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