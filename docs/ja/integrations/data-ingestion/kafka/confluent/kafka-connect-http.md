---
sidebar_label: Confluent Platform用のHTTP Sink Connector
sidebar_position: 3
slug: /ja/integrations/kafka/cloud/confluent/http
description: Kafka ConnectとHTTP Connector Sink with  and ClickHouse
---
import ConnectionDetails from '@site/docs/ja/_snippets/_gather_your_details_http.mdx';

# Confluent HTTP Sink Connector
HTTP Sink Connectorはデータ型に依存せず、Kafkaスキーマを必要としません。また、MapsやArraysのようなClickHouse固有のデータ型をサポートしています。この追加の柔軟性は、設定の複雑さの増加を少し引き起こします。

以下に、単一のKafkaトピックからメッセージを取得し、ClickHouseテーブルに行を挿入する簡単なインストール手順を説明します。

:::note
  HTTP Connectorは[Confluent Enterprise License](https://docs.confluent.io/kafka-connect-http/current/overview.html#license)の下で配布されています。
:::

### クイックスタート手順

#### 1. 接続情報を集める
<ConnectionDetails />

#### 2. Kafka ConnectとHTTP Sink Connectorを実行する

以下の2つのオプションがあります：

* **セルフマネージド:** Confluentパッケージをダウンロードしてローカルにインストールします。コネクタのインストール手順は[こちら](https://docs.confluent.io/kafka-connect-http/current/overview.html)をご覧ください。confluent-hubインストール方法を使用する場合、ローカルの設定ファイルが更新されます。

* **Confluent Cloud:** KafkaホスティングにConfluent Cloudを使用する場合、HTTP Sinkの完全管理バージョンが利用可能です。これは、ClickHouse環境がConfluent Cloudからアクセス可能である必要があります。

:::note
  以下の例はConfluent Cloudを使用しています。
:::

#### 3. ClickHouseに宛先テーブルを作成する

接続テストの前に、ClickHouse Cloudにテストテーブルを作成してみましょう。このテーブルはKafkaからデータを受け取ります：

```sql
CREATE TABLE default.my_table
(
    `side` String,
    `quantity` Int32,
    `symbol` String,
    `price` Int32,
    `account` String,
    `userid` String
)
ORDER BY tuple()
```

#### 4. HTTP Sinkを設定する
KafkaトピックとHTTP Sink Connectorのインスタンスを作成します。
<img src={require('./images/create_http_sink.png').default} class="image" alt="Create HTTP Sink" style={{width: '50%'}}/>

<br />

HTTP Sink Connectorを設定します：
* 作成したトピック名を入力
* 認証
    * `HTTP Url` - `INSERT`クエリを指定したClickHouse CloudのURL `<protocol>://<clickhouse_host>:<clickhouse_port>?query=INSERT%20INTO%20<database>.<table>%20FORMAT%20JSONEachRow`を指定します。**注意**：クエリはエンコードされている必要があります。
    * `Endpoint Authentication type` - BASIC
    * `Auth username` - ClickHouseのユーザー名
    * `Auth password` - ClickHouseのパスワード

:::note
  このHTTP Urlはエラーが発生しやすいです。正確なエスケープを心がけてください。
:::

<img src={require('./images/http_auth.png').default} class="image" alt="Auth options for Confluent HTTP Sink" style={{width: '70%'}}/>
<br/>

* 設定
    * `Input Kafka record value format` ソースデータに依存しますが、ほとんどの場合JSONまたはAvroです。以下の設定では`JSON`を想定しています。
    * `advanced configurations` セクションでは：
        * `HTTP Request Method` - POSTに設定
        * `Request Body Format` - json
        * `Batch batch size` - ClickHouseの推奨に基づき、**最低でも1000**に設定します。
        * `Batch json as array` - true
        * `Retry on HTTP codes` - 400-500ですが、必要に応じて適応。例：ClickHouseの前にHTTPプロキシがある場合は変更が必要な場合があります。
        * `Maximum Reties` - デフォルト（10）が適切ですが、より強力なリトライを望む場合は調整してください。

<img src={require('./images/http_advanced.png').default} class="image" alt="Advanced options for Confluent HTTP Sink" style={{width: '50%'}}/>

#### 5. 接続性のテスト
HTTP Sinkで設定したトピックにメッセージを作成
<img src={require('./images/create_message_in_topic.png').default} class="image" alt="Create a message in the topic" style={{width: '50%'}}/>

<br/>

そして、作成されたメッセージがClickHouseインスタンスに書き込まれたことを確認します。

### トラブルシューティング
#### HTTP Sinkがメッセージをバッチ処理しない

[Sinkドキュメント](https://docs.confluent.io/kafka-connectors/http/current/overview.html#http-sink-connector-for-cp)からの引用：
> Kafkaヘッダ値が異なるメッセージの場合、HTTP Sinkコネクタは要求をバッチ処理しません。

1. Kafkaレコードに同じキーがあることを確認します。
2. HTTP APIのURLにパラメータを追加すると、各レコードがユニークなURLを生成する可能性があります。このため、追加のURLパラメータを使用する場合、バッチ処理は無効になります。

#### 400 Bad Request
##### CANNOT_PARSE_QUOTED_STRING
HTTP Sinkが`String`カラムにJSONオブジェクトを挿入する際に以下のメッセージで失敗する場合：

```
Code: 26. DB::ParsingException: Cannot parse JSON string: expected opening quote: (while reading the value of key key_name): While executing JSONEachRowRowInputFormat: (at row 1). (CANNOT_PARSE_QUOTED_STRING)
```

URLに`input_format_json_read_objects_as_strings=1`設定をエンコードされた文字列として追加します `SETTINGS%20input_format_json_read_objects_as_strings%3D1`

### GitHubデータセットをロードする（オプション）

この例では、GitHubデータセットの配列フィールドを保ちます。例では空のgithubトピックを想定し、メッセージの挿入に[kcat](https://github.com/edenhill/kcat)を使用します。

##### 1. 設定を準備する

[これらの手順](https://docs.confluent.io/cloud/current/cp-component/connect-cloud-config.html#set-up-a-local-connect-worker-with-cp-install) に従って、インストールタイプに関連するConnectをセットアップしてください。スタンドアロンと分散型クラスターの違いに注意してください。Confluent Cloudを使用する場合、分散設定が該当します。

最も重要なパラメータは`http.api.url`です。ClickHouse用の[HTTPインターフェース](../../../../interfaces/http.md) では、INSERT文をURLのパラメータとしてエンコードする必要があります。これはフォーマット（この場合、`JSONEachRow`）とターゲットデータベースを含む必要があります。フォーマットはKafkaデータと一貫していなければなりません。これらのパラメータはURLエスケープされなければなりません。GitHubデータセット用のこのフォーマットの例を以下に示します（ClickHouseがローカルで実行されていると想定）：

```
<protocol>://<clickhouse_host>:<clickhouse_port>?query=INSERT%20INTO%20<database>.<table>%20FORMAT%20JSONEachRow

http://localhost:8123?query=INSERT%20INTO%20default.github%20FORMAT%20JSONEachRow
```

HTTP SinkをClickHouseで使用するための追加パラメータは以下の通りです。完全なパラメータリストは[こちら](https://docs.confluent.io/kafka-connect-http/current/connector_config.html)をご覧ください：

* `request.method` - **POST**に設定
* `retry.on.status.codes` - エラーステータスコード(400-500)でリトライするように設定します。データで予想されるエラーに基づいて詳細化してください。
* `request.body.format` - 多くの場合、これはJSONです。
* `auth.type` - ClickHouseのセキュリティを使用する場合、BASICに設定します。他のClickHouse互換認証メカニズムは現在サポートされていません。
* `ssl.enabled` - SSLを使用する場合はtrueに設定します。
* `connection.user` - ClickHouseのユーザー名。
* `connection.password` - ClickHouseのパスワード。
* `batch.max.size` - 1回のバッチで送信する行数です。適切に大きな数になるように設定します。ClickHouse [推奨事項](../../../../concepts/why-clickhouse-is-so-fast.md#performance-when-inserting-data) に基づき、1000が最小と考えられます。
* `tasks.max` - HTTP Sinkコネクタは1つ以上のタスクを実行することをサポートします。これによりパフォーマンスを向上させることができます。バッチサイズとともに、これがパフォーマンスを向上させる主な手段です。
* `key.converter` - キーのタイプに応じて設定します。
* `value.converter` - トピック上のデータのタイプに基づいて設定します。このデータはスキーマを必要としません。ここでのフォーマットは、`http.api.url`パラメータで指定されたFORMATと一貫している必要があります。最も単純なのはJSONを使用し、org.apache.kafka.connect.json.JsonConverterコンバータを使用することです。値を文字列として扱うことも可能です（org.apache.kafka.connect.storage.StringConverterコンバータを通じて）- ただし、これはINSERTステートメントで関数を使用して値を抽出する必要があるでしょう。[Avroフォーマット](../../../../interfaces/formats.md#data-format-avro) もClickHouseでサポートされており、io.confluent.connect.avro.AvroConverterコンバータを使用する場合に利用可能です。

プロキシの設定やリトライ、高度なSSL設定を含む設定の詳細は[こちら](https://docs.confluent.io/kafka-connect-http/current/connector_config.html)をご覧ください。

Githubサンプルデータ用の例の設定ファイルは[こちら](https://github.com/ClickHouse/clickhouse-docs/tree/main/docs/en/integrations/data-ingestion/kafka/code/connectors/http_sink)にあります。Connectがスタンドアロンモードで実行され、KafkaがConfluent Cloudでホストされていると仮定しています。

##### 2. ClickHouseテーブルを作成する

テーブルが作成されていることを確認してください。MergeTreeを使用した最小限のGitHubデータセットの例を以下に示します。


```sql
CREATE TABLE github
(
    file_time DateTime,
    event_type Enum('CommitCommentEvent' = 1, 'CreateEvent' = 2, 'DeleteEvent' = 3, 'ForkEvent' = 4,'GollumEvent' = 5, 'IssueCommentEvent' = 6, 'IssuesEvent' = 7, 'MemberEvent' = 8, 'PublicEvent' = 9, 'PullRequestEvent' = 10, 'PullRequestReviewCommentEvent' = 11, 'PushEvent' = 12, 'ReleaseEvent' = 13, 'SponsorshipEvent' = 14, 'WatchEvent' = 15, 'GistEvent' = 16, 'FollowEvent' = 17, 'DownloadEvent' = 18, 'PullRequestReviewEvent' = 19, 'ForkApplyEvent' = 20, 'Event' = 21, 'TeamAddEvent' = 22),
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

##### 3. Kafkaにデータを追加する

Kafkaにメッセージを挿入します。以下では[kcat](https://github.com/edenhill/kcat)を使用して10,000件のメッセージを挿入します。

```bash
head -n 10000 github_all_columns.ndjson | kcat -b <host>:<port> -X security.protocol=sasl_ssl -X sasl.mechanisms=PLAIN -X sasl.username=<username>  -X sasl.password=<password> -t github
```

ターゲットテーブル「Github」で簡単な読み取りを行い、データの挿入を確認します。

```sql
SELECT count() FROM default.github;

| count() |
| :--- |
| 10000 |

```
