---
sidebar_label: Kafka テーブルエンジン
sidebar_position: 5
slug: /ja/integrations/kafka/kafka-table-engine
description: Kafka テーブルエンジンの使用
---

# Kafka テーブルエンジンの使用

:::note
Kafka テーブルエンジンは[ClickHouse Cloud](https://clickhouse.com/cloud)ではサポートされていません。[ClickPipes](../clickpipes/kafka.md)または[Kafka Connect](./kafka-clickhouse-connect-sink.md)を検討してください。
:::

### Kafka を ClickHouse へ

Kafka テーブルエンジンを使用するには、[ClickHouse の Materialized View](../../../guides/developer/cascading-materialized-views.md)について基本的な知識が必要です。

#### 概要

初めに注目するのは最も一般的なユースケースです。Kafka テーブルエンジンを使用して Kafka から ClickHouse にデータを挿入します。

Kafka テーブルエンジンは、ClickHouse が Kafka トピックから直接データを読み込むことを可能にします。トピック上のメッセージを表示するのに便利ですが、設計上、クエリがテーブルに発行されたときにキューからデータを消費し、コンシューマオフセットを増やしてから結果を呼び出し元に返します。したがって、これらのオフセットをリセットしない限り、データを再読することはできません。

このテーブルエンジンから読み込んだデータを保存するためには、データをキャプチャして別のテーブルに挿入する方法が必要です。トリガーに基づく Materialized View がネイティブにその機能を提供します。Materialized View はテーブルエンジンの読み取りを開始し、ドキュメントのバッチを受信します。TO 句がデータの宛先を決定する - 通常は [MergeTree ファミリー](../../../engines/table-engines/mergetree-family/index.md)のテーブルです。このプロセスは以下の図で視覚化されています。

<img src={require('./images/kafka_01.png').default} class="image" alt="Kafka テーブルエンジン" style={{width: '80%'}}/>

#### ステップ

##### 1. 準備

対象トピックにデータがある場合、以下の要素をデータセットに適用できます。あるいは、サンプルの Github データセットが[こちら](https://datasets-documentation.s3.eu-west-3.amazonaws.com/kafka/github_all_columns.ndjson)で提供されています。このデータセットは以下の例で使用されており、完全なデータセット（[こちら](https://ghe.clickhouse.tech/)）に比べてスキーマが縮小され、行のサブセット（具体的には[ClickHouse リポジトリ](https://github.com/ClickHouse/ClickHouse)に関連する Github イベントに限定）を使用していますが、それでもデータセットと共に公開されているほとんどのクエリを実行するには十分です。

##### 2. ClickHouse を設定

安全な Kafka に接続している場合はこのステップが必要です。これらの設定は SQL DDL コマンドを通じて渡すことができず、ClickHouse の config.xml に設定する必要があります。SASL で保護されたインスタンスに接続していると仮定します。これは Confluent Cloud と対話する際の最も簡単な方法です。

```xml
<clickhouse>
   <kafka>
       <sasl_username>username</sasl_username>
       <sasl_password>password</sasl_password>
       <security_protocol>sasl_ssl</security_protocol>
       <sasl_mechanisms>PLAIN</sasl_mechanisms>
   </kafka>
</clickhouse>
```

上記のスニペットを新しいファイルとして conf.d/ ディレクトリに置くか、既存の設定ファイルにマージします。設定できる設定については[こちら](../../../engines/table-engines/integrations/kafka.md#configuration)を参照してください。

このチュートリアルでは `KafkaEngine` というデータベースを作成します。

```sql
CREATE DATABASE KafkaEngine;
```

データベースを作成したら、そこに切り替える必要があります。

```sql
USE KafkaEngine;
```

##### 3. 目的のテーブルを作成する

目的のテーブルを準備します。以下の例では簡潔にするために縮小された GitHub スキーマを使用します。MergeTree テーブルエンジンを使用していますが、この例は [MergeTree ファミリー](../../../engines/table-engines/mergetree-family/index.md)のいずれのメンバーにも簡単に適応できます。

```sql
CREATE TABLE github
(
    file_time DateTime,
    event_type Enum('CommitCommentEvent' = 1, 'CreateEvent' = 2, 'DeleteEvent' = 3, 'ForkEvent' = 4, 'GollumEvent' = 5, 'IssueCommentEvent' = 6, 'IssuesEvent' = 7, 'MemberEvent' = 8, 'PublicEvent' = 9, 'PullRequestEvent' = 10, 'PullRequestReviewCommentEvent' = 11, 'PushEvent' = 12, 'ReleaseEvent' = 13, 'SponsorshipEvent' = 14, 'WatchEvent' = 15, 'GistEvent' = 16, 'FollowEvent' = 17, 'DownloadEvent' = 18, 'PullRequestReviewEvent' = 19, 'ForkApplyEvent' = 20, 'Event' = 21, 'TeamAddEvent' = 22),
    actor_login LowCardinality(String),
    repo_name LowCardinality(String),
    created_at DateTime,
    updated_at DateTime,
    action Enum('none' = 0, 'created' = 1, 'added' = 2, 'edited' = 3, 'deleted' = 4, 'opened' = 5, 'closed' = 6, 'reopened' = 7, 'assigned' = 8, 'unassigned' = 9, 'labeled' = 10, 'unlabeled' = 11, 'review_requested' = 12, 'review_request_removed' = 13, 'synchronize' = 14, 'started' = 15, 'published' = 16, 'update' = 17, 'create' = 18, 'fork' = 19, 'merged' = 20),
    comment_id UInt64,
    path String,
    ref LowCardinality(String),
    ref_type Enum('none' = 0, 'branch' = 1, 'tag' = 2, 'repository' = 3, 'unknown' = 4),
    creator_user_login LowCardinality(String),
    number UInt32,
    title String,
    labels Array(LowCardinality(String)),
    state Enum('none' = 0, 'open' = 1, 'closed' = 2),
    assignee LowCardinality(String),
    assignees Array(LowCardinality(String)),
    closed_at DateTime,
    merged_at DateTime,
    merge_commit_sha String,
    requested_reviewers Array(LowCardinality(String)),
    merged_by LowCardinality(String),
    review_comments UInt32,
    member_login LowCardinality(String)
) ENGINE = MergeTree ORDER BY (event_type, repo_name, created_at)
```

##### 4. トピックを作成してデータを挿入する

次に、トピックを作成します。これを行うためのいくつかのツールがあります。Kafka をローカルマシンまたは Docker コンテナ内で実行している場合、[RPK](https://docs.redpanda.com/current/get-started/rpk-install/) がよく機能します。以下のコマンドを実行して 5パーティションの `github` というトピックを作成できます。

```bash
rpk topic create -p 5 github --brokers <host>:<port>
```

Confluent Cloud 上で Kafka を実行している場合は、[Confluent CLI](https://docs.confluent.io/platform/current/tutorials/examples/clients/docs/kcat.html#produce-records)を使用することをお勧めします。

```bash
confluent kafka topic create --if-not-exists github
```

次に、このトピックにデータを投入する必要があります。これは [kcat](https://github.com/edenhill/kcat) を使用して行います。認証が無効な状態で Kafka をローカルで実行している場合は、次のようなコマンドを実行できます。

```bash
cat github_all_columns.ndjson | 
kcat -P \
  -b <host>:<port> \
  -t github
```

または、SASL を使用して認証する Kafka クラスターの場合：

```bash
cat github_all_columns.ndjson | 
kcat -P \
  -b <host>:<port> \
  -t github
  -X security.protocol=sasl_ssl \
  -X sasl.mechanisms=PLAIN \
  -X sasl.username=<username>  \
  -X sasl.password=<password> \
```

データセットには 200,000 行が含まれているため、数秒で取り込まれるはずです。より大きなデータセットを扱いたい場合は、[the large datasets section](https://github.com/ClickHouse/kafka-samples/tree/main/producer#large-datasets) of the [ClickHouse/kafka-samples](https://github.com/ClickHouse/kafka-samples) GitHub リポジトリをご覧ください。

##### 5. Kafka テーブルエンジンの作成

以下の例は、マージツリーテーブルと同じスキーマを持つテーブルエンジンを作成します。これは厳密には必要ありません。ターゲットテーブルにエイリアスやエフェメラルカラムを持つことができます。ただし、設定は重要です。Kafka トピックから JSON を消費するためのデータタイプとして `JSONEachRow` を使用している点に注意してください。`github` および `clickhouse` はそれぞれトピック名とコンシューマグループ名を表しています。トピックは実際には値のリストにすることができます。

```sql
CREATE TABLE github_queue
(
    file_time DateTime,
    event_type Enum('CommitCommentEvent' = 1, 'CreateEvent' = 2, 'DeleteEvent' = 3, 'ForkEvent' = 4, 'GollumEvent' = 5, 'IssueCommentEvent' = 6, 'IssuesEvent' = 7, 'MemberEvent' = 8, 'PublicEvent' = 9, 'PullRequestEvent' = 10, 'PullRequestReviewCommentEvent' = 11, 'PushEvent' = 12, 'ReleaseEvent' = 13, 'SponsorshipEvent' = 14, 'WatchEvent' = 15, 'GistEvent' = 16, 'FollowEvent' = 17, 'DownloadEvent' = 18, 'PullRequestReviewEvent' = 19, 'ForkApplyEvent' = 20, 'Event' = 21, 'TeamAddEvent' = 22),
    actor_login LowCardinality(String),
    repo_name LowCardinality(String),
    created_at DateTime,
    updated_at DateTime,
    action Enum('none' = 0, 'created' = 1, 'added' = 2, 'edited' = 3, 'deleted' = 4, 'opened' = 5, 'closed' = 6, 'reopened' = 7, 'assigned' = 8, 'unassigned' = 9, 'labeled' = 10, 'unlabeled' = 11, 'review_requested' = 12, 'review_request_removed' = 13, 'synchronize' = 14, 'started' = 15, 'published' = 16, 'update' = 17, 'create' = 18, 'fork' = 19, 'merged' = 20),
    comment_id UInt64,
    path String,
    ref LowCardinality(String),
    ref_type Enum('none' = 0, 'branch' = 1, 'tag' = 2, 'repository' = 3, 'unknown' = 4),
    creator_user_login LowCardinality(String),
    number UInt32,
    title String,
    labels Array(LowCardinality(String)),
    state Enum('none' = 0, 'open' = 1, 'closed' = 2),
    assignee LowCardinality(String),
    assignees Array(LowCardinality(String)),
    closed_at DateTime,
    merged_at DateTime,
    merge_commit_sha String,
    requested_reviewers Array(LowCardinality(String)),
    merged_by LowCardinality(String),
    review_comments UInt32,
    member_login LowCardinality(String)
)
   ENGINE = Kafka('kafka_host:9092', 'github', 'clickhouse',
            'JSONEachRow') settings kafka_thread_per_consumer = 0, kafka_num_consumers = 1;
```

エンジンの設定とパフォーマンスチューニングについては後述します。この時点で、テーブル `github_queue` に対する単純な select を行うといくつかの行が読み取れるはずです。その際には、コンシューマオフセットが進むため、これらの行を再読するには[リセット](#common-operations)が必要です。制限と必要なパラメーター `stream_like_engine_allow_direct_select` に注意してください。

##### 6. Materialized View の作成

Materialized View は、以前に作成した2つのテーブルを接続し、カフカテーブルエンジンからデータを読み取り、ターゲットのマージツリーテーブルにデータを挿入します。多くのデータ変換を行えます。ここでは簡単な読み取りと挿入を行います。* を使用すると、カラム名が同一（大文字小文字を区別）であることが前提となります。

```sql
CREATE MATERIALIZED VIEW github_mv TO github AS
SELECT *
FROM github_queue;
```

作成時点で、Materialized View は Kafka エンジンに接続し、読み取りを開始し、行をターゲットテーブルに挿入します。このプロセスは無期限に続き、Kafka へのメッセージの追加が消費され続けます。Kafka にさらにメッセージを追加するために挿入スクリプトを再度実行してみてください。

##### 7. 挿入された行を確認する

ターゲットテーブルにデータが存在することを確認します。

```sql
SELECT count() FROM github;
```

200,000 行が表示されるはずです:
```response
┌─count()─┐
│  200000 │
└─────────┘
```

#### 一般的な操作

##### メッセージ消費の停止と再開

メッセージ消費を停止するには、Kafka エンジンテーブルをデタッチします。

```sql
DETACH TABLE github_queue;
```

これはコンシューマグループのオフセットに影響を与えません。消費を再開し、前回のオフセットから続行するにはテーブルを再アタッチします。

```sql
ATTACH TABLE github_queue;
```

##### Kafka メタデータの追加

元の Kafka メッセージから、ClickHouse に取り込まれた後のメタデータを追跡することは有用です。たとえば、特定のトピックやパーティションからどれだけ消費したかを知りたい場合です。この目的のために、Kafka テーブルエンジンにはいくつかの[仮想カラム](../../../engines/table-engines/index.md#table_engines-virtual_columns)が公開されています。これらをターゲットテーブルのカラムとして保持するには、スキーマと Materialized View の select 文を変更します。

まず、ターゲットテーブルにカラムを追加する前に、上で説明した停止操作を実行します。

```sql
DETACH TABLE github_queue;
```

以下は、行がどのトピックおよびパーティションから来たのかを識別する情報カラムを追加します。

```sql
ALTER TABLE github
   ADD COLUMN topic String,
   ADD COLUMN partition UInt64;
```

次に、必要なように仮想カラムをマップする必要があります。
仮想カラムは `_` で始まります。
仮想カラムの完全なリストは[こちら](../../../engines/table-engines/integrations/kafka.md#virtual-columns)にあります。

仮想カラムでテーブルを更新するには、Materialized View を削除し、Kafka エンジンテーブルを再アタッチし、Materialized View を再作成する必要があります。

```sql
DROP VIEW github_mv;
```

```sql
ATTACH TABLE github_queue;
```

```sql
CREATE MATERIALIZED VIEW github_mv TO github AS
SELECT *, _topic as topic, _partition as partition
FROM github_queue;
```

新たに消費された行にはメタデータが含まれます。

```sql
SELECT actor_login, event_type, created_at, topic, partition 
FROM github 
LIMIT 10;
```

結果は次のようになります。

| actor_login | event_type | created_at | topic | partition |
| :--- | :--- | :--- | :--- | :--- |
| IgorMinar | CommitCommentEvent | 2011-02-12 02:22:00 | github | 0 |
| queeup | CommitCommentEvent | 2011-02-12 02:23:23 | github | 0 |
| IgorMinar | CommitCommentEvent | 2011-02-12 02:23:24 | github | 0 |
| IgorMinar | CommitCommentEvent | 2011-02-12 02:24:50 | github | 0 |
| IgorMinar | CommitCommentEvent | 2011-02-12 02:25:20 | github | 0 |
| dapi | CommitCommentEvent | 2011-02-12 06:18:36 | github | 0 |
| sourcerebels | CommitCommentEvent | 2011-02-12 06:34:10 | github | 0 |
| jamierumbelow | CommitCommentEvent | 2011-02-12 12:21:40 | github | 0 |
| jpn | CommitCommentEvent | 2011-02-12 12:24:31 | github | 0 |
| Oxonium | CommitCommentEvent | 2011-02-12 12:31:28 | github | 0 |

##### Kafka エンジンの設定を変更する

Kafka エンジンテーブルをドロップして新しい設定で再作成することをお勧めします。このプロセス中に Materialized View を変更する必要はありません - Kafka エンジンテーブルが再作成されるとメッセージ消費は再開されます。

##### 問題のデバッグ

認証の問題などのエラーは、Kafka エンジン DDL に対する応答に報告されません。問題の診断には、主要な ClickHouse ログファイル clickhouse-server.err.log を使用することをお勧めします。基盤となる Kafka クライアントライブラリ [librdkafka](https://github.com/edenhill/librdkafka) のさらなるトレースログは設定を通じて有効にできます。

```xml
<kafka>
   <debug>all</debug>
</kafka>
```

##### 破損したメッセージの処理

Kafka はしばしばデータの「一時保管所」として使用されます。これにより、トピックに混在したメッセージ形式や不一致のフィールド名が含まれることがあります。この状況を避け、Kafka Streams や ksqlDB などの Kafka 機能を活用し、メッセージが Kafka に挿入される前に、整形され、一貫性のあるものにします。これらのオプションが利用できない場合、ClickHouse には役立ついくつかの機能があります。

* メッセージフィールドを文字列として処理します。必要に応じて、Materialized View 文で関数を使用してクレンジングやキャスティングを行うことができます。これは本番環境でのソリューションを表すものではありませんが、1回限りのインジェストには役立つかもしれません。
* JSONEachRow フォーマットを使用して、トピックから JSON を消費している場合は、設定 [`input_format_skip_unknown_fields`](../../../operations/settings/settings-formats.md#settings-input-format-skip-unknown-fields) を使用します。データを書き込む際に、デフォルトで ClickHouse はターゲットテーブルに存在しないカラムが入力データに含まれているときに例外を投げます。ただし、このオプションが有効にされている場合、これらの余分なカラムは無視されます。これもまた本番レベルのソリューションではなく、他の人を混乱させるかもしれません。
* `kafka_skip_broken_messages` 設定を考慮してください。これは、ユーザーがブロックごとに破損したメッセージに対する許容度を指定する必要があり、kafka_max_block_size の文脈で考慮されます。この許容度を超えると（絶対メッセージ単位で測定）、通常の例外処理に戻り、他のメッセージがスキップされます。

##### 配信セマンティクスと重複に関する課題

Kafka テーブルエンジンは少なくとも一度のセマンティクスを持っています。稀な状況で重複が生じる可能性があります。たとえば、メッセージが Kafka から読み取られ、ClickHouse に正常に挿入された場合、新しいオフセットをコミットする前に Kafka への接続が失われることがあります。この状況ではブロックの再試行が必要です。このブロックは、分散テーブルや ReplicatedMergeTree をターゲットテーブルとして使用することで[データの重複排除](../../../engines/table-engines/mergetree-family/replication.md#table_engines-replication)することができます。これは重複行の可能性を減らしますが、同一ブロックに依存します。Kafka の再バランスなどのイベントによってこの仮定が無効になる可能性があり、稀に重複を引き起こす可能性があります。

##### クォーラムベースの挿入

ClickHouse でのより高い配信保証が必要な場合には[クォーラムベースの挿入](../../../operations/settings/settings.md#settings-insert_quorum)を行う必要があります。これは Materialized View やターゲットテーブルに設定することはできません。ただし、ユーザープロファイルに設定することができます。

```xml
<profiles>
  <default>
    <insert_quorum>2</insert_quorum>
  </default>
</profiles>
```

### ClickHouse から Kafka へ

あまり一般的でないユースケースですが、ClickHouse のデータも Kafka に保持できます。たとえば、行を手動で Kafka テーブルエンジンに挿入します。このデータは同じ Kafka エンジンによって読み取られ、その Materialized View はデータを MergeTree テーブルに配置します。最後に、既存のソーステーブルから Kafka に挿入する際の Materialized View の応用を示します。

#### ステップ

私たちの初期の目的は以下の図で最もよく示されています：

<img src={require('./images/kafka_02.png').default} class="image" alt="Kafka テーブルエンジンの挿入" style={{width: '80%'}}/>

[Kafka を ClickHouse へ](#kafka-to-clickhouse)のステップで作成されたテーブルとビューを持っていることを前提とし、トピックが完全に消費されていると仮定します。

##### 1. 直接行を挿入する

まず、ターゲットテーブルの件数を確認します。

```sql
SELECT count() FROM github;
```

200,000 行があるはずです：
```response
┌─count()─┐
│  200000 │
└─────────┘
```

GitHub ターゲットテーブルから Kafka テーブルエンジン github_queue に行を挿入します。JSONEachRow フォーマットを活用し、SELECT を 100 に制限する点に注意してください。

```sql
INSERT INTO github_queue SELECT * FROM github LIMIT 100 FORMAT JSONEachRow
```

GitHub の行数を再確認して、100 増加したことを確認します。上記の図に示すように、行は Kafka へ Kafka テーブルエンジンを介して挿入され、その後 Materialized View によって GitHub ターゲットテーブルに再挿入されました！

```sql
SELECT count() FROM github;
```

100 行追加されているはずです：
```response
┌─count()─┐
│  200100 │
└─────────┘
```

##### 2. Materialized View の使用

Materialized View を利用して、テーブルへのドキュメント挿入時に Kafka エンジン（およびトピック）にメッセージをプッシュできます。GitHub テーブルに行が挿入されると、Materialized View がトリガーされ、行が再び Kafka エンジンおよび新しいトピックに挿入されます。これもまた以下の図で最もよく示されています：

<img src={require('./images/kafka_03.png').default} class="image" alt="Kafka テーブルエンジン挿入と Materialized View" style={{width: '80%'}}/>

`github_out` または同等の新しい Kafka トピックを作成します。このトピックを指す Kafka テーブルエンジン `github_out_queue` を作成します。

```sql
CREATE TABLE github_out_queue
(
    file_time DateTime,
    event_type Enum('CommitCommentEvent' = 1, 'CreateEvent' = 2, 'DeleteEvent' = 3, 'ForkEvent' = 4, 'GollumEvent' = 5, 'IssueCommentEvent' = 6, 'IssuesEvent' = 7, 'MemberEvent' = 8, 'PublicEvent' = 9, 'PullRequestEvent' = 10, 'PullRequestReviewCommentEvent' = 11, 'PushEvent' = 12, 'ReleaseEvent' = 13, 'SponsorshipEvent' = 14, 'WatchEvent' = 15, 'GistEvent' = 16, 'FollowEvent' = 17, 'DownloadEvent' = 18, 'PullRequestReviewEvent' = 19, 'ForkApplyEvent' = 20, 'Event' = 21, 'TeamAddEvent' = 22),
    actor_login LowCardinality(String),
    repo_name LowCardinality(String),
    created_at DateTime,
    updated_at DateTime,
    action Enum('none' = 0, 'created' = 1, 'added' = 2, 'edited' = 3, 'deleted' = 4, 'opened' = 5, 'closed' = 6, 'reopened' = 7, 'assigned' = 8, 'unassigned' = 9, 'labeled' = 10, 'unlabeled' = 11, 'review_requested' = 12, 'review_request_removed' = 13, 'synchronize' = 14, 'started' = 15, 'published' = 16, 'update' = 17, 'create' = 18, 'fork' = 19, 'merged' = 20),
    comment_id UInt64,
    path String,
    ref LowCardinality(String),
    ref_type Enum('none' = 0, 'branch' = 1, 'tag' = 2, 'repository' = 3, 'unknown' = 4),
    creator_user_login LowCardinality(String),
    number UInt32,
    title String,
    labels Array(LowCardinality(String)),
    state Enum('none' = 0, 'open' = 1, 'closed' = 2),
    assignee LowCardinality(String),
    assignees Array(LowCardinality(String)),
    closed_at DateTime,
    merged_at DateTime,
    merge_commit_sha String,
    requested_reviewers Array(LowCardinality(String)),
    merged_by LowCardinality(String),
    review_comments UInt32,
    member_login LowCardinality(String)
)
   ENGINE = Kafka('host:port', 'github_out', 'clickhouse_out',
            'JSONEachRow') settings kafka_thread_per_consumer = 0, kafka_num_consumers = 1;
```

次に、新しい Materialized View `github_out_mv` を作成して GitHub テーブルを指し、トリガーされると上記のエンジンに行を挿入するようにします。GitHub テーブルへの追加は、結果として新しい Kafka トピックにプッシュされます。

```sql
CREATE MATERIALIZED VIEW github_out_mv TO github_out_queue AS
SELECT file_time, event_type, actor_login, repo_name, 
       created_at, updated_at, action, comment_id, path, 
       ref, ref_type, creator_user_login, number, title, 
       labels, state, assignee, assignees, closed_at, merged_at,
       merge_commit_sha, requested_reviewers, merged_by, 
       review_comments, member_login 
FROM github 
FORMAT JsonEachRow;
```

オリジナルの github トピックに挿入すると、ドキュメントが「github_clickhouse」トピックに魔法のように現れます。ネイティブ Kafka ツールを使用してこれを確認してください。たとえば、Confluent Cloud ホストのトピックに対して [kcat](https://github.com/edenhill/kcat) を使用して github トピックに 100 行をインサートします。

```sql
head -n 10 github_all_columns.ndjson | 
kcat -P \
  -b <host>:<port> \
  -t github
  -X security.protocol=sasl_ssl \
  -X sasl.mechanisms=PLAIN \
  -X sasl.username=<username> \
  -X sasl.password=<password> 
```

`github_out` トピックの読み取りでメッセージの配信が確認されます。

```sql
kcat -C \
  -b <host>:<port> \
  -t github_out \
  -X security.protocol=sasl_ssl \
  -X sasl.mechanisms=PLAIN \
  -X sasl.username=<username> \
  -X sasl.password=<password> \
  -e -q | 
wc -l
```

これは複雑な例ですが、Kafka エンジンと組み合わせて使用する場合の Materialized View の力を示しています。

### クラスターとパフォーマンス

#### ClickHouse クラスターを使用する

Kafka コンシューマグループを通じて、複数の ClickHouse インスタンスが同じトピックを読むことができます。各コンシューマは 1:1 マッピングでトピックパーティションに割り当てられます。Kafka テーブルエンジンを使用して ClickHouse 消費をスケーリングする際には、クラスタ内のコンシューマの総数がトピックのパーティション数を超えることができないことを考慮してください。したがって、事前にトピックのパーティションを適切に構成してください。

複数の ClickHouse インスタンスが、Kafka テーブルエンジンの作成中に指定された同じコンシューマグループ ID を使用してトピックを読み込むように構成できます。したがって、各インスタンスは一つまたは複数のパーティションから読み取り、セグメントをローカルのターゲットテーブルに挿入します。ターゲットテーブルは、データの重複を処理するために ReplicatedMergeTree を使用して構成することができます。このアプローチにより、十分な Kafka パーティションがある場合は、ClickHouse クラスターに合わせて Kafka リードをスケールアウトすることができます。

<img src={require('./images/kafka_04.png').default} class="image" alt="レプリケートされた Kafka テーブルエンジン" style={{width: '80%'}}/>

#### パフォーマンスのチューニング

Kafka エンジン テーブルのスループットパフォーマンスを向上させる際には次の点を考慮してください。

* パフォーマンスはメッセージサイズ、フォーマット、およびターゲットテーブルのタイプによって異なります。単一テーブルエンジン上でのスループットは、100k 行/秒を目標にするべきです。既定では、メッセージはブロックで読み取られ、kafka_max_block_size パラメータによって制御されます。これはデフォルトでは [max_insert_block_size](../../../operations/settings/settings.md#setting-max_insert_block_size) に設定されており、デフォルトは 1,048,576 です。メッセージが非常に大きくない限り、これを増加させるべきです。500k から 1M の範囲は一般的です。スループットパフォーマンスへの影響をテストし評価してください。
* テーブルエンジンのコンシューマ数は kafka_num_consumers を使用して増やすことができます。ただし、デフォルトでは kafka_thread_per_consumer を 1 に変更しない限り、挿入はシングルスレッドで行われます。これを 1 に設定すると、フラッシュが並行して実行されるようになります。なお、1 消費者あたり 1 スレッドの Kafka エンジン テーブルを作成することは、複数の Kafka エンジンを個別のマテリアライズドビューとともに作成することと論理的に同等です。
* コンシューマの増加は無料の操作ではありません。各コンシューマは独自のバッファとスレッドを保持し、サーバーへのオーバーヘッドを増やします。オーバーヘッドを意識し、まずクラスタ全体で線形にスケールしてください。可能であれば線形にスケールアウトします。
* Kafka メッセージのスループットが変動し、遅延が許容される場合は、stream_flush_interval_ms を増やして、より大きなブロックがフラッシュされるようにします。
* [background_message_broker_schedule_pool_size](../../../operations/settings/settings.md#background_message_broker_schedule_pool_size)は、バックグラウンドタスクを実行するスレッドの数です。これらのスレッドは Kafka ストリーミングに使用されます。この設定は ClickHouse サーバーの起動時に適用され、ユーザーセッションでは変更できず、デフォルトは 16 です。ログにタイムアウトが見られる場合、この設定を増やすべきかもしれません。
* Kafka との通信には、ライブラリ librdkafka が使用されており、それ自体がスレッドを生成します。大量の Kafka テーブルやコンシューマの結果、大量のコンテキスト切り替えが発生する可能性があります。この負荷をクラスタ全体に分散し、可能であればターゲットテーブルのみを複製するか、複数のトピックを読み取るためのテーブルエンジンを使用することを検討してください - 値のリストがサポートされています。異なるトピックからのデータをフィルタリングする形で単一テーブルから複数のマテリアライズドビューを読み取ることができます。

設定の変更はテストされるべきです。Kafka コンシューマの遅延を監視して、適切なスケールアウトが行われていることを確認してください。

#### 追加の設定

上記の設定に加えて、以下も興味深いかもしれません：

* [Kafka_max_wait_ms](../../../operations/settings/settings.md#kafka-max-wait-ms) - Kafka からメッセージを読み取る際の再試行までの待機時間をミリ秒単位で設定します。ユーザープロファイルレベルで設定され、デフォルトは 5000 です。

[librdkafka のすべての設定 ](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)は、ClickHouse 構成ファイルの _kafka_ 要素内に配置することができます - 設定名は XML 要素として、ピリオドをアンダースコアに置き換えて指定してください。

```xml
<clickhouse>
   <kafka>
       <enable_ssl_certificate_verification>false</enable_ssl_certificate_verification>
   </kafka>
</clickhouse>
```

これらは専門的な設定であり、Kafka のドキュメントを参照して詳細な説明を確認することをお勧めします。
