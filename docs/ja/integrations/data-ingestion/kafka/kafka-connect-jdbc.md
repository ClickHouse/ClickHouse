---
sidebar_label: Kafka Connect JDBC コネクタ
sidebar_position: 4
slug: /ja/integrations/kafka/kafka-connect-jdbc
description: Kafka Connect と ClickHouse を使用した JDBC コネクタ シンク
---
import ConnectionDetails from '@site/docs/ja/_snippets/_gather_your_details_http.mdx';

# JDBC コネクタ

:::note
このコネクタは、データが簡単でプリミティブ・データ型（例：int）で構成されている場合にのみ使用するべきです。ClickHouse固有の型、例えばマップはサポートされていません。
:::

以下の例では、Kafka Connect の Confluent ディストリビューションを使用します。

以下では、Kafka の単一トピックからメッセージを取り込み、ClickHouse テーブルに行を挿入する簡単なインストールについて説明します。Kafka 環境を持っていない場合、Confluent Cloud の使用を推奨します。これは、無料で利用できる十分なレベルを提供しています。

JDBC コネクタにはスキーマが必要であることに注意してください（JDBC コネクタでプレーンな JSON や CSV を使用することはできません）。スキーマは各メッセージにエンコードすることもできますが、関連オーバーヘッドを避けるために [Confluent スキーマレジスト](https://www.confluent.io/blog/kafka-connect-deep-dive-converters-serialization-explained/#json-schemas)y の利用を強く推奨します。提供された挿入スクリプトはメッセージからスキーマを自動的に推測し、それをレジストリに挿入します。このスクリプトは他のデータセットでも再利用可能です。Kafka のキーは文字列であると仮定しています。Kafka スキーマの詳細は[こちら](https://docs.confluent.io/platform/current/schema-registry/index.html)で確認できます。

### ライセンス
JDBC コネクタは [Confluent Community License](https://www.confluent.io/confluent-community-license) の下で配布されています。

### 手順
#### 接続の詳細を収集する
<ConnectionDetails />

#### 1. Kafka Connect と コネクタをインストールする

Confluent パッケージをダウンロードし、ローカルにインストールしたと仮定しています。コネクタのインストールの手順については[こちら](https://docs.confluent.io/kafka-connect-jdbc/current/#install-the-jdbc-connector)を参照してください。

confluent-hub のインストール方法を使用すると、ローカルの構成ファイルが更新されます。

Kafka から ClickHouse にデータを送信するには、コネクタの Sink コンポーネントを使用します。

#### 2. JDBC ドライバをダウンロードしてインストールする

ClickHouse JDBC ドライバー `clickhouse-jdbc-<version>-shaded.jar` を[こちら](https://github.com/ClickHouse/clickhouse-java/releases)からダウンロードしてインストールします。そして、[こちら](https://docs.confluent.io/kafka-connect-jdbc/current/#installing-jdbc-drivers)の詳細に従って Kafka Connect にインストールします。他のドライバも動作する可能性がありますが、テストされていません。

:::note

一般的な問題: ドキュメントは jar を `share/java/kafka-connect-jdbc/` にコピーすることを推奨しています。ドライバが見つからない場合、ドライバを `share/confluent-hub-components/confluentinc-kafka-connect-jdbc/lib/` にコピーするか、`plugin.path` を変更してドライバを含めるようにします。詳しくは以下を参照してください。

:::

#### 3. 構成を準備する

[こちらの手順](https://docs.confluent.io/cloud/current/cp-component/connect-cloud-config.html#set-up-a-local-connect-worker-with-cp-install)に従って、インストールタイプに関連する Connect をセットアップし、スタンドアロンと分散クラスターの違いに注意してください。Confluent Cloud を使用する場合、分散セットアップが関連します。

以下のパラメータは、ClickHouse で JDBC コネクタを使用する場合に関連します。全パラメータ一覧は[こちら](https://docs.confluent.io/kafka-connect-jdbc/current/sink-connector/index.html)で確認できます。

* `connection.url` - 形式は `jdbc:clickhouse://&lt;clickhouse host>:&lt;clickhouse http port>/&lt;target database>` とする必要があります。
* `connection.user` - ターゲットデータベースへの書き込みアクセス権を持つユーザー
* `table.name.format` - データを挿入する ClickHouse テーブル。存在している必要があります。
* `batch.size` - 一度に送信する行数。ClickHouse の[推奨事項](../../../concepts/why-clickhouse-is-so-fast.md#performance-when-inserting-data)に基づき、1000 を最小値として考慮してください。
* `tasks.max` - JDBC Sink コネクタは 1 つ以上のタスクの実行をサポートしています。これによりパフォーマンスを向上させることができます。バッチサイズと共に、パフォーマンス向上の主要手段を表します。
* `value.converter.schemas.enable` - スキーマレジストリを使用している場合は false に設定し、メッセージにスキーマを埋め込む場合は true に設定します。
* `value.converter` - データタイプに応じて設定します。たとえば JSON の場合は “io.confluent.connect.json.JsonSchemaConverter”。
* `key.converter` - “org.apache.kafka.connect.storage.StringConverter” に設定します。文字列キーを利用します。
* `pk.mode` - ClickHouse には関連しないため、none に設定します。
* `auto.create` - サポートされておらず、false に設定する必要があります。
* `auto.evolve` - 将来的にサポートされる可能性があるため、false を推奨します。
* `insert.mode` - “insert” に設定します。他のモードは現在サポートされていません。
* `key.converter` - キーのタイプに応じて設定します。
* `value.converter` - トピック上のデータタイプに基づいて設定します。このデータはサポートされたスキーマを持っている必要があります。JSON、Avro、または Protobuf 形式など。

サンプルデータセットをテストに使用する場合、以下を設定してください：

* `value.converter.schemas.enable` - スキーマレジストリを使用するため false に設定します。スキーマを各メッセージに埋め込む場合は true に設定します。
* `key.converter` - Set to “org.apache.kafka.connect.storage.StringConverter”. 文字列キーを利用します。
* `value.converter` - Set “io.confluent.connect.json.JsonSchemaConverter”.
* `value.converter.schema.registry.url` - スキーマサーバーの URL を設定し、`value.converter.schema.registry.basic.auth.user.info` によりスキーマサーバーへの認証情報を入力します。

Github のサンプルデータ用の構成ファイルの例は[こちら](https://github.com/ClickHouse/kafka-samples/tree/main/github_events/jdbc_sink)で確認できます。Connect がスタンドアロンモードで実行され、Kafka が Confluent Cloud にホストされていることを前提としています。

#### 4. ClickHouse テーブルを作成する

テーブルが作成されていることを確認し、既存の例から存在する場合は削除します。以下に示すのは、削減された Github データセットに対応した例です。未対応の Array や Map 型の不在に注意してください：

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
    state Enum('none' = 0, 'open' = 1, 'closed' = 2),
    assignee LowCardinality(String),
    closed_at DateTime,
    merged_at DateTime,
    merge_commit_sha String,
    merged_by LowCardinality(String),
    review_comments UInt32,
    member_login LowCardinality(String)
) ENGINE = MergeTree ORDER BY (event_type, repo_name, created_at)
```

#### 5. Kafka Connect を起動する

Kafka Connect を[スタンドアロン](https://docs.confluent.io/cloud/current/cp-component/connect-cloud-config.html#standalone-cluster)または[分散](https://docs.confluent.io/cloud/current/cp-component/connect-cloud-config.html#distributed-cluster)モードで起動します。

```bash
./bin/connect-standalone connect.properties.ini github-jdbc-sink.properties.ini
```

#### 6. Kafka にデータを追加する

提供された[スクリプトと設定](https://github.com/ClickHouse/kafka-samples/tree/main/producer)を使用して Kafka にメッセージを挿入します。github.config を修正して Kafka の認証情報を含める必要があります。このスクリプトは現在、Confluent Cloud での使用向けに構成されています。

```bash
python producer.py -c github.config
```

このスクリプトは、任意の ndjson ファイルを Kafka トピックに挿入するために使用できます。スキーマを自動的に推測しようとします。提供されたサンプル設定は 10k のメッセージのみを挿入しますが、必要があれば[こちら](https://github.com/ClickHouse/clickhouse-docs/tree/main/docs/en/integrations/data-ingestion/kafka/code/producer/github.config#L25)で修正してください。この構成はまた、Kafka への挿入中にデータセットから互換性のない Array フィールドを削除します。

これは JDBC コネクタがメッセージを INSERT 文に変換するために必要です。自身のデータを使用する場合、スキーマを各メッセージに挿入（`value.converter.schemas.enable` を true に設定）するか、クライアントがメッセージをスキーマレジストリに参照しながら公開することを確認してください。

Kafka Connect はメッセージの消費を始め、ClickHouse に行を挿入していくはずです。「[JDBC Compliant Mode] トランザクションはサポートされていません。」の警告は予期されており、無視することができます。

ターゲットテーブル「Github」で簡単な読み取りを行うと、データ挿入が確認できます。

```sql
SELECT count() FROM default.github;
```

```response
| count\(\) |
| :--- |
| 10000 |
```

### 推奨されるさらなる読み物

* [Kafka Sink Configuration Parameters](https://docs.confluent.io/kafka-connect-jdbc/current/sink-connector/sink_config_options.html#sink-config-options)
* [Kafka Connect Deep Dive – JDBC Source Connector](https://www.confluent.io/blog/kafka-connect-deep-dive-jdbc-source-connector)
* [Kafka Connect JDBC Sink deep-dive: Working with Primary Keys](https://rmoff.net/2021/03/12/kafka-connect-jdbc-sink-deep-dive-working-with-primary-keys/)
* [Kafka Connect in Action: JDBC Sink](https://www.youtube.com/watch?v=b-3qN_tlYR4&t=981s) - 読むより視聴を好む方に。
* [Kafka Connect Deep Dive – Converters and Serialization Explained](https://www.confluent.io/blog/kafka-connect-deep-dive-converters-serialization-explained/#json-schemas)
