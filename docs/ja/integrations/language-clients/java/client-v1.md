---
sidebar_label: クライアント V1
sidebar_position: 3
keywords: [clickhouse, java, client, integrate]
description: Java ClickHouse Connector v1
slug: /ja/integrations/java/client-v1
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import CodeBlock from '@theme/CodeBlock';

# クライアント (V1)

DBサーバとそのプロトコルを介して通信するためのJavaクライアントライブラリです。現在の実装では[HTTPインターフェース](/docs/ja/interfaces/http)のみをサポートしています。このライブラリは、サーバにリクエストを送信するための独自のAPIを提供します。

*注意*: このコンポーネントは間もなく非推奨になります。

## セットアップ

<Tabs groupId="client-v1-setup">
<TabItem value="maven" label="Maven" >

```xml 
<!-- https://mvnrepository.com/artifact/com.clickhouse/clickhouse-http-client -->
<dependency>
    <groupId>com.clickhouse</groupId>
    <artifactId>clickhouse-http-client</artifactId>
    <version>0.6.5</version>
</dependency>
```

</TabItem>
<TabItem value="gradle-kt" label="Gradle (Kotlin)">

```kotlin
// https://mvnrepository.com/artifact/com.clickhouse/clickhouse-http-client
implementation("com.clickhouse:clickhouse-http-client:0.6.5")
```
</TabItem>
<TabItem value="gradle" label="Gradle">

```groovy
// https://mvnrepository.com/artifact/com.clickhouse/clickhouse-http-client
implementation 'com.clickhouse:clickhouse-http-client:0.6.5'
```

</TabItem>
</Tabs>

バージョン `0.5.0` 以降、ドライバーは新しいクライアントHTTPライブラリを使用し、それを依存関係として追加する必要があります。

<Tabs groupId="client-v1-http-client">
<TabItem value="maven" label="Maven" >

```xml 
<!-- https://mvnrepository.com/artifact/org.apache.httpcomponents.client5/httpclient5 -->
<dependency>
    <groupId>org.apache.httpcomponents.client5</groupId>
    <artifactId>httpclient5</artifactId>
    <version>5.3.1</version>
</dependency>
```

</TabItem>
<TabItem value="gradle-kt" label="Gradle (Kotlin)">

```kotlin
// https://mvnrepository.com/artifact/org.apache.httpcomponents.client5/httpclient5
implementation("org.apache.httpcomponents.client5:httpclient5:5.3.1")
```
</TabItem>
<TabItem value="gradle" label="Gradle">

```groovy
// https://mvnrepository.com/artifact/org.apache.httpcomponents.client5/httpclient5
implementation 'org.apache.httpcomponents.client5:httpclient5:5.3.1'
```

</TabItem>
</Tabs>

## 初期化

接続URL形式: `protocol://host[:port][/database][?param[=value][&param[=value]][#tag[,tag]]`, 例として:

- `http://localhost:8443?ssl=true&sslmode=NONE`
- `https://(https://explorer@play.clickhouse.com:443`

単一ノードに接続:

```java showLineNumbers
ClickHouseNode server = ClickHouseNode.of("http://localhost:8123/default?compress=0");
```
複数ノードのクラスタに接続:

```java showLineNumbers
ClickHouseNodes servers = ClickHouseNodes.of(
    "jdbc:ch:http://server1.domain,server2.domain,server3.domain/my_db"
    + "?load_balancing_policy=random&health_check_interval=5000&failover=2");
```

## クエリAPI

```java showLineNumbers
try (ClickHouseClient client = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP);
     ClickHouseResponse response = client.read(servers)
        .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
        .query("select * from numbers limit :limit")
        .params(1000)
        .executeAndWait()) {
            ClickHouseResponseSummary summary = response.getSummary();
            long totalRows = summary.getTotalRowsToRead();
}
```

## ストリーミングクエリAPI 

```java showLineNumbers
try (ClickHouseClient client = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP);
     ClickHouseResponse response = client.read(servers)
        .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
        .query("select * from numbers limit :limit")
        .params(1000)
        .executeAndWait()) {
            for (ClickHouseRecord r : response.records()) {
            int num = r.getValue(0).asInteger();
            // 型変換
            String str = r.getValue(0).asString();
            LocalDate date = r.getValue(0).asDate();
        }
}
```

[リポジトリ](https://github.com/ClickHouse/clickhouse-java/tree/main/examples/client)内の[完全なコード例](https://github.com/ClickHouse/clickhouse-java/blob/main/examples/client/src/main/java/com/clickhouse/examples/jdbc/Main.java#L73)を参照してください。

## 挿入API

```java showLineNumbers

try (ClickHouseClient client = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP);
     ClickHouseResponse response = client.read(servers).write()
        .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
        .query("insert into my_table select c2, c3 from input('c1 UInt8, c2 String, c3 Int32')")
        .data(myInputStream) // `myInputStream` はRowBinary形式のデータソース
        .executeAndWait()) {
            ClickHouseResponseSummary summary = response.getSummary();
            summary.getWrittenRows();
}
```

[リポジトリ](https://github.com/ClickHouse/clickhouse-java/tree/main/examples/client)内の[完全なコード例](https://github.com/ClickHouse/clickhouse-java/blob/main/examples/client/src/main/java/com/clickhouse/examples/jdbc/Main.java#L39)を参照してください。

**RowBinary エンコーディング**

RowBinary形式はその[ページ](/docs/ja/interfaces/formats#rowbinarywithnamesandtypes)に記述されています。

[コード](https://github.com/ClickHouse/clickhouse-kafka-connect/blob/main/src/main/java/com/clickhouse/kafka/connect/sink/db/ClickHouseWriter.java#L622)の例があります。

## 機能
### 圧縮

このクライアントはデフォルトでLZ4圧縮を使用します。それには以下の依存関係が必要です。

<Tabs groupId="client-v1-compression-deps">
<TabItem value="maven" label="Maven" >

```xml 
<!-- https://mvnrepository.com/artifact/org.lz4/lz4-java -->
<dependency>
    <groupId>org.lz4</groupId>
    <artifactId>lz4-java</artifactId>
    <version>1.8.0</version>
</dependency>
```

</TabItem>
<TabItem value="gradle-kt" label="Gradle (Kotlin)">

```kotlin
// https://mvnrepository.com/artifact/org.lz4/lz4-java
implementation("org.lz4:lz4-java:1.8.0")
```
</TabItem>
<TabItem value="gradle" label="Gradle">

```groovy
// https://mvnrepository.com/artifact/org.lz4/lz4-java
implementation 'org.lz4:lz4-java:1.8.0'
```

</TabItem>
</Tabs>

代わりにgzipを使用したい場合は、接続URLに`compress_algorithm=gzip`を設定してください。

また、数通りの方法で圧縮を無効にすることができます。

1. 接続URLに `compress=0` を設定して無効にする: `http://localhost:8123/default?compress=0`
2. クライアント設定経由で無効にする:

```java showLineNumbers
ClickHouseClient client = ClickHouseClient.builder()
   .config(new ClickHouseConfig(Map.of(ClickHouseClientOption.COMPRESS, false)))
   .nodeSelector(ClickHouseNodeSelector.of(ClickHouseProtocol.HTTP))
   .build();
```

異なる圧縮オプションについての詳細は、[圧縮ドキュメント](/ja/native-protocol/compression)を参照してください。

### 複数クエリ

同じセッション内で、複数のクエリを一つづつワーカースレッドで実行します:

```java showLineNumbers
CompletableFuture<List<ClickHouseResponseSummary>> future = ClickHouseClient.send(servers.apply(servers.getNodeSelector()),
    "create database if not exists my_base",
    "use my_base",
    "create table if not exists test_table(s String) engine=Memory",
    "insert into test_table values('1')('2')('3')",
    "select * from test_table limit 1",
    "truncate table test_table",
    "drop table if exists test_table");
List<ClickHouseResponseSummary> results = future.get();
```

### 名前付きパラメータ

パラメータリストの位置に依存する代わりに、名前でパラメータを渡すことができます。この機能は`params`機能を使用することで利用可能になります。

```java showLineNumbers
try (ClickHouseClient client = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP);
     ClickHouseResponse response = client.read(servers)
        .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
        .query("select * from my_table where name=:name limit :limit")
        .params("Ben", 1000)
        .executeAndWait()) {
            //...
        }
}
```

:::note パラメータ
`String`型を含む全ての`params`シグネチャ(`String`, `String[]`, `Map<String, String>`)は、渡されるキーが有効なClickHouseのSQL文字列であると仮定します。たとえば:

```java showLineNumbers
try (ClickHouseClient client = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP);
     ClickHouseResponse response = client.read(servers)
        .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
        .query("select * from my_table where name=:name")
        .params(Map.of("name","'Ben'"))
        .executeAndWait()) {
            //...
        }
}
```

Stringオブジェクトを手動でClickHouse SQLに解析したくない場合は、`com.clickhouse.data`にあるヘルパー関数`ClickHouseValues.convertToSqlExpression`を使用できます:

```java showLineNumbers
try (ClickHouseClient client = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP);
     ClickHouseResponse response = client.read(servers)
        .format(ClickHouseFormat.RowBinaryWithNamesAndTypes)
        .query("select * from my_table where name=:name")
        .params(Map.of("name", ClickHouseValues.convertToSqlExpression("Ben's")))
        .executeAndWait()) {
            //...
        }
}
```

上記の例では、`ClickHouseValues.convertToSqlExpression`が内部のシングルクォーテーションをエスケープし、変数を有効なシングルクォーテーションで囲みます。

他の型、例えば`Integer`、`UUID`、`Array`、`Enum`などは、`params`内で自動的に変換されます。
:::

## ノード検出

Javaクライアントは、ClickHouseノードを自動で検出する機能を提供します。自動検出はデフォルトで無効になっています。手動で有効にするには、`auto_discovery`を `true`に設定します:

```java
properties.setProperty("auto_discovery", "true");
```

または接続URL内で:

```plaintext
jdbc:ch://my-server/system?auto_discovery=true
```

自動検出が有効な場合、接続URL内に全てのClickHouseノードを指定する必要はありません。URLに指定されたノードはシードとして扱われ、Javaクライアントはシステムテーブルやclickhouse-keeperもしくはzookeeperからさらに多くのノードを自動で検出します。

自動検出設定に関するオプションは以下の通りです:

| プロパティ               | デフォルト | 説明                                                                                                  |
|-------------------------|------------|-------------------------------------------------------------------------------------------------------|
| auto_discovery          | `false`    | クライアントがシステムテーブルまたはclickhouse-keeper/zookeeperからさらに多くのノードを検出するかどうか |
| node_discovery_interval | `0`        | ノード検出の間隔をミリ秒で指定し、ゼロや負の値は一回限りの検出を意味します                           |
| node_discovery_limit    | `100`      | 一度に検出できるノードの最大数。ゼロや負の値は制限なしを意味します                                      |

### ロードバランシング

Javaクライアントはロードバランシングポリシーに従って、リクエストを送信するClickHouseノードを選択します。一般に、ロードバランシングポリシーは以下のことを担当します:

1. 管理されているノードリストからノードを取得する
2. ノードの状態を管理する
3. (もし自動検出が有効であれば)ノード検出のためのバックグラウンドプロセスをオプションでスケジュールし、ヘルスチェックを実行する

ロードバランシングの設定に関するオプション一覧は以下の通りです:

| プロパティ               | デフォルト                                       | 説明                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    |
|--------------------------|-------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| load_balancing_policy    | `""`                                            | ロードバランシングポリシーとして利用できるのは: <li>`firstAlive` - 管理ノードリストから最初の正常なノードにリクエストが送られます</li><li>`random` - 管理ノードリストからランダムに選ばれたノードにリクエストが送られます</li><li>`roundRobin` - 管理ノードリストの各ノードに順番にリクエストが送られます</li><li>`ClickHouseLoadBalancingPolicy`を実装した完全修飾クラス名 - カスタムロードバランシングポリシー</li>指定がなければ、管理ノードリストの最初のノードにリクエストが送られます |
| load_balancing_tags      | `""`                                            | ノードをフィルタリングするためのロードバランシングタグ。指定されたタグを持つノードにのみリクエストが送られます                                                                                                                                                                                                                                                                                                                                                                                                           |
| health_check_interval    | `0`                                             | ヘルスチェックの間隔をミリ秒で指定し、ゼロや負の値は一回限りを意味します                                                                                                                                                                                                                                                                                                                                                                                                                                      |
| health_check_method      | `ClickHouseHealthCheckMethod.SELECT_ONE`        | ヘルスチェックの方法。以下のうち一つ: <li>`ClickHouseHealthCheckMethod.SELECT_ONE` - `select 1`クエリでのチェック</li> <li>`ClickHouseHealthCheckMethod.PING` - 一般により高速なプロトコル固有のチェック</li>                                                                                                                                                                                                                                                                                         |
| node_check_interval      | `0`                                             | ノードチェックの間隔をミリ秒で指定し、負の数はゼロと見なされます。特定のノードの最後のチェックから指定された時間が経過している場合にノードの状態がチェックされます。<br/>`health_check_interval`と`node_check_interval`の違いは、`health_check_interval`はノードのリスト（すべてもしくは誤っているものに対して）の状態をチェックするバックグラウンドジョブをスケジュールするオプションであるのに対し、`node_check_interval`は特定のノードの最後のチェックから指定された時間が経過していることを指定するオプションです |
| check_all_nodes          | `false`                                         | すべてのノードと誤動作しているノードのどちらに対してヘルスチェックを実行するかを指定します                                                                                                                                                                                                                                                                                                                                                                                                                    |


### フェイルオーバーとリトライ

Javaクライアントは、失敗したクエリに対するフェイルオーバーとリトライの動作をセットアップするための設定オプションを提供します:

| プロパティ                  | デフォルト | 説明                                                                                                                                                                       |
|-----------------------------|------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| failover                    | `0`        | リクエストに対してフェイルオーバーが発生できる最大回数。ゼロまたは負の値はフェイルオーバーなしを意味します。フェイルオーバーは負の値を意味し、異なるノードに失敗したリクエストを送信します。  |
| retry                       | `0`        | リクエストに対してリトライが発生できる最大回数。ゼロまたは負の値はリトライなしを意味します。ClickHouseサーバが`NETWORK_ERROR`エラーコードを返した場合のみ、リトライが行われます。  |
| repeat_on_session_lock      | `true`     | セッションがロックされてタイムアウトする場合(`session_timeout` または `connect_timeout` に従う)の実行を繰り返すかどうかを指定します。ClickHouseサーバが`SESSION_IS_LOCKED`エラーコードを返した場合に、失敗したリクエストが繰り返されます。 |

### カスタムHTTPヘッダーの追加

JavaクライアントはリクエストにカスタムHTTPヘッダーを追加するためのHTTP/Sトランスポートレイヤーをサポートしています。
`custom_http_headers`プロパティを使用し、ヘッダーを`,`で区切って指定します。ヘッダーキー/値は`=`で分割する必要があります。

## Javaクライアントサポート

```java
options.put("custom_http_headers", "X-ClickHouse-Quota=test, X-ClickHouse-Test=test");
```

## JDBCドライバー  

```java
properties.setProperty("custom_http_headers", "X-ClickHouse-Quota=test, X-ClickHouse-Test=test");
```


