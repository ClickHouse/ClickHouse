---
sidebar_label: KafkaとのVector
sidebar_position: 3
slug: /ja/integrations/kafka/kafka-vector
description: KafkaとClickHouseでVectorを使用する
---
import ConnectionDetails from '@site/docs/ja/_snippets/_gather_your_details_http.mdx';

## KafkaとClickHouseでVectorを使用する

Vectorは、Kafkaからデータを読み取り、ClickHouseにイベントを送信できる、ベンダーに依存しないデータパイプラインです。

ClickHouseとのVectorの[入門ガイド](../etl-tools/vector-to-clickhouse.md)は、ログのユースケースとファイルからイベントを読み取ることに焦点を当てています。私たちは、Kafkaトピックに保持されている[Githubサンプルデータセット](https://datasets-documentation.s3.eu-west-3.amazonaws.com/kafka/github_all_columns.ndjson)を利用します。

Vectorは、[sources](https://vector.dev/docs/about/concepts/#sources)を使用して、プッシュまたはプルモデルでデータを取得します。[sinks](https://vector.dev/docs/about/concepts/#sinks)は、イベントの宛先を提供します。そのため、KafkaのソースとClickHouseのシンクを使用します。注意点として、Kafkaはシンクとしてサポートされていますが、ClickHouseのソースは存在しません。したがって、ClickHouseからKafkaにデータを転送したいユーザーにはVectorは適切ではありません。

Vectorはまた、データの[変換](https://vector.dev/docs/reference/configuration/transforms/)もサポートしています。これはこのガイドの範囲外です。データセットにおいて変換が必要な場合は、Vectorのドキュメントを参照してください。

現在のClickHouseシンクの実装は、HTTPインターフェースを利用しています。ClickHouseシンクは現時点でJSONスキーマの利用をサポートしていません。データはプレーンなJSON形式または文字列としてKafkaに公開される必要があります。

### ライセンス
Vectorは、[MPL-2.0ライセンス](https://github.com/vectordotdev/vector/blob/master/LICENSE)の下で配布されています。

### 接続情報を収集する
<ConnectionDetails />

### 手順

1. Kafkaの`github`トピックを作成し、[Githubデータセット](https://datasets-documentation.s3.eu-west-3.amazonaws.com/kafka/github_all_columns.ndjson)を挿入します。

```bash
cat /opt/data/github/github_all_columns.ndjson | kcat -b <host>:<port> -X security.protocol=sasl_ssl -X sasl.mechanisms=PLAIN -X sasl.username=<username> -X sasl.password=<password> -t github
```

このデータセットは`ClickHouse/ClickHouse`リポジトリに焦点を当てた200,000行で構成されています。

2. ターゲットテーブルが作成されていることを確認してください。以下ではデフォルトのデータベースを使用します。

```sql

CREATE TABLE github
(
    file_time DateTime,
    event_type Enum('CommitCommentEvent' = 1, 'CreateEvent' = 2, 'DeleteEvent' = 3, 'ForkEvent' = 4,
                    'GollumEvent' = 5, 'IssueCommentEvent' = 6, 'IssuesEvent' = 7, 'MemberEvent' = 8, 'PublicEvent' = 9, 'PullRequestEvent' = 10, 'PullRequestReviewCommentEvent' = 11, 'PushEvent' = 12, 'ReleaseEvent' = 13, 'SponsorshipEvent' = 14, 'WatchEvent' = 15, 'GistEvent' = 16, 'FollowEvent' = 17, 'DownloadEvent' = 18, 'PullRequestReviewEvent' = 19, 'ForkApplyEvent' = 20, 'Event' = 21, 'TeamAddEvent' = 22),
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
) ENGINE = MergeTree ORDER BY (event_type, repo_name, created_at);

```

3. [Vectorをダウンロードしてインストール](https://vector.dev/docs/setup/quickstart/)します。`kafka.toml`という設定ファイルを作成し、KafkaとClickHouseのインスタンスに合わせて値を変更します。

```toml
[sources.github]
type = "kafka"
auto_offset_reset = "smallest"
bootstrap_servers = "<kafka_host>:<kafka_port>"
group_id = "vector"
topics = [ "github" ]
tls.enabled = true
sasl.enabled = true
sasl.mechanism = "PLAIN"
sasl.username = "<username>"
sasl.password = "<password>"
decoding.codec = "json"

[sinks.clickhouse]
type = "clickhouse"
inputs = ["github"]
endpoint = "http://localhost:8123"
database = "default"
table = "github"
skip_unknown_fields = true
auth.strategy = "basic"
auth.user = "username"
auth.password = "password"
buffer.max_events = 10000
batch.timeout_secs = 1
```

この設定とVectorの動作に関する重要な注意点:

- この例はConfluent Cloudに対してテストされています。そのため、`sasl.*`および`ssl.enabled`のセキュリティオプションはセルフマネージドの場合には適切でない可能性があります。
- 設定パラメータ`bootstrap_servers`にはプロトコル接頭辞は必要ありません。例えば `pkc-2396y.us-east-1.aws.confluent.cloud:9092`
- ソースパラメータの`decoding.codec = "json"`は、メッセージが単一のJSONオブジェクトとしてClickHouseシンクに渡されることを保証します。文字列としてメッセージを扱い、デフォルトの`bytes`値を使用する場合、メッセージの内容は`message`フィールドに追加されます。ほとんどの場合、これは[Vector入門](../etl-tools/vector-to-clickhouse.md#4-parse-the-logs)ガイドで説明されているようにClickHouseで処理が必要です。
- Vectorはメッセージに[多くのフィールドを追加](https://vector.dev/docs/reference/configuration/sources/kafka/#output-data)します。この例では、ClickHouseシンクの設定パラメータ`skip_unknown_fields = true`を介してこれらのフィールドを無視しています。これは、ターゲットテーブルスキーマの一部でないフィールドを無視します。例えば`offset`などのメタフィールドが追加されるようにスキーマを調整しても問題ありません。
- シンクがソースイベントへの参照として`inputs`パラメータをどのように使用するかに注意してください。
- ClickHouseシンクの動作については[こちら](https://vector.dev/docs/reference/configuration/sinks/clickhouse/#buffers-and-batches)に記載されています。最適なスループットを得るために、ユーザーは`buffer.max_events`、`batch.timeout_secs`および`batch.max_bytes`パラメータのチューニングを行うことができます。 ClickHouseの[推奨事項](../../../concepts/why-clickhouse-is-so-fast.md#performance-when-inserting-data)に従い、1000のイベント数は1バッチあたりの最低値と考えられるべきです。高スループットのユースケースでは、`buffer.max_events` パラメータを増やすことができます。スループットに変動がある場合は、`batch.timeout_secs` パラメータを変更する必要があるかもしれません。
- `auto_offset_reset = "smallest"`パラメータは、Kafkaソースがトピックの先頭から開始することを強制します。これにより、ステップ(1)で公開されたメッセージを消費することが保証されます。異なる動作を必要とする場合は[こちら](https://vector.dev/docs/reference/configuration/sources/kafka/#auto_offset_reset)をご覧ください。

1. Vectorを開始します。

```bash
vector --config ./kafka.toml
```

デフォルトでは、ClickHouseへの挿入が開始される前に[ヘルスチェック](https://vector.dev/docs/reference/configuration/sinks/clickhouse/#healthcheck)が必要です。これは接続可能性を確保し、スキーマを確認するためです。問題が発生した場合にさらに詳細なログを取得するには、`VECTOR_LOG=debug`を付けることが役立つ場合があります。

5. データの挿入を確認します。

```sql
SELECT count() as count FROM github;
```

| count |
| :--- |
| 200000 |
