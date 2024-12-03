---
sidebar_label: Client V2
sidebar_position: 2
keywords: [clickhouse, java, client, integrate]
description: Java ClickHouse Connector v2
slug: /ja/integrations/java/client-v2
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import CodeBlock from '@theme/CodeBlock';

# Java クライアント (V2)

DBサーバーとそのプロトコルを介して通信するためのJavaクライアントライブラリです。現在の実装は[HTTPインターフェース](/docs/ja/interfaces/http)のみをサポートしています。このライブラリは、サーバーにリクエストを送信するための独自のAPIを提供します。また、さまざまなバイナリデータ形式（RowBinary* & Native*）で作業するためのツールも提供します。

## セットアップ

- Maven Central (プロジェクトウェブページ): https://mvnrepository.com/artifact/com.clickhouse/client-v2
- ナイトリービルド (リポジトリリンク): https://s01.oss.sonatype.org/content/repositories/snapshots/com/clickhouse/

<Tabs groupId="client-v2-setup">
<TabItem value="maven" label="Maven" >

```xml 
<dependency>
    <groupId>com.clickhouse</groupId>
    <artifactId>client-v2</artifactId>
    <version>0.6.5</version>
</dependency>
```

</TabItem>
<TabItem value="gradle-kt" label="Gradle (Kotlin)">

```kotlin
// https://mvnrepository.com/artifact/com.clickhouse/client-v2
implementation("com.clickhouse:client-v2:0.6.5")
```
</TabItem>
<TabItem value="gradle" label="Gradle">

```groovy
// https://mvnrepository.com/artifact/com.clickhouse/client-v2
implementation 'com.clickhouse:client-v2:0.6.5'
```

</TabItem>
</Tabs>

## 初期化

`com.clickhouse.client.api.Client.Builder#build()`によってクライアントオブジェクトが初期化されます。各クライアントは独自のコンテキストを持ち、オブジェクト間で共有されません。ビルダーには便利な設定のためのメソッドが用意されています。

例:
```java showLineNumbers
 Client client = new Client.Builder()
                .addEndpoint("https://clickhouse-cloud-instance:8443/")
                .setUsername(user)
                .setPassword(password)
                .build();
```

`Client`は`AutoCloseable`であり、不要になったら閉じる必要があります。

## 設定

すべての設定はインスタンスメソッド（設定メソッドとして知られる）によって定義され、それぞれの値のスコープとコンテキストを明確にします。
主要な設定パラメータは一つのスコープ（クライアントまたは操作）に定義され、互いにオーバーライドされません。

設定はクライアント作成時に定義されます。`com.clickhouse.client.api.Client.Builder`を参照してください。

## 共通定義

### ClickHouseFormat

[サポートされるフォーマット](/docs/ja/interfaces/formats)の列挙です。ClickHouseがサポートするすべてのフォーマットを含みます。

* `raw` - ユーザーは生データをトランスコードする必要があります
* `full` - クライアントはデータを自分でトランスコードでき、生データストリームを受け入れます
* `-` - このフォーマットではClickHouseでは操作サポートされていません

このクライアントバージョンがサポートするもの:

| フォーマット                                                                                                                      | 入力   | 出力   |
|-----------------------------------------------------------------------------------------------------------------------------------|:------:|:------:|
| [TabSeparated](/docs/ja/interfaces/formats#tabseparated)                                                                          | raw    | raw    |
| [TabSeparatedRaw](/docs/ja/interfaces/formats#tabseparatedraw)                                                                    | raw    | raw    |
| [TabSeparatedWithNames](/docs/ja/interfaces/formats#tabseparatedwithnames)                                                        | raw    | raw    |
| [TabSeparatedWithNamesAndTypes](/docs/ja/interfaces/formats#tabseparatedwithnamesandtypes)                                        | raw    | raw    |
| [TabSeparatedRawWithNames](/docs/ja/interfaces/formats#tabseparatedrawwithnames)                                                  | raw    | raw    |
| [TabSeparatedRawWithNamesAndTypes](/docs/ja/interfaces/formats#tabseparatedrawwithnamesandtypes)                                  | raw    | raw    |
| [Template](/docs/ja/interfaces/formats#format-template)                                                                           | raw    | raw    |
| [TemplateIgnoreSpaces](/docs/ja/interfaces/formats#templateignorespaces)                                                          | raw    |  -     |
| [CSV](/docs/ja/interfaces/formats#csv)                                                                                            | raw    | raw    |
| [CSVWithNames](/docs/ja/interfaces/formats#csvwithnames)                                                                          | raw    | raw    |
| [CSVWithNamesAndTypes](/docs/ja/interfaces/formats#csvwithnamesandtypes)                                                          | raw    | raw    |
| [CustomSeparated](/docs/ja/interfaces/formats#format-customseparated)                                                             | raw    | raw    |
| [CustomSeparatedWithNames](/docs/ja/interfaces/formats#customseparatedwithnames)                                                  | raw    | raw    |
| [CustomSeparatedWithNamesAndTypes](/docs/ja/interfaces/formats#customseparatedwithnamesandtypes)                                  | raw    | raw    |
| [SQLInsert](/docs/ja/interfaces/formats#sqlinsert)                                                                                | -      | raw    |
| [Values](/docs/ja/interfaces/formats#data-format-values)                                                                          | raw    | raw    |
| [Vertical](/docs/ja/interfaces/formats#vertical)                                                                                  | -      | raw    |
| [JSON](/docs/ja/interfaces/formats#json)                                                                                          | raw    | raw    |
| [JSONAsString](/docs/ja/interfaces/formats#jsonasstring)                                                                          | raw    | -      |
| [JSONAsObject](/docs/ja/interfaces/formats#jsonasobject)                                                                          | raw    | -      |
| [JSONStrings](/docs/ja/interfaces/formats#jsonstrings)                                                                            | raw    | raw    |
| [JSONColumns](/docs/ja/interfaces/formats#jsoncolumns)                                                                            | raw    | raw    |
| [JSONColumnsWithMetadata](/docs/ja/interfaces/formats#jsoncolumnsmonoblock)                                                       | raw    | raw    |
| [JSONCompact](/docs/ja/interfaces/formats#jsoncompact)                                                                            | raw    | raw    |
| [JSONCompactStrings](/docs/ja/interfaces/formats#jsoncompactstrings)                                                              | -      | raw    |
| [JSONCompactColumns](/docs/ja/interfaces/formats#jsoncompactcolumns)                                                              | raw    | raw    |
| [JSONEachRow](/docs/ja/interfaces/formats#jsoneachrow)                                                                            | raw    | raw    |
| [PrettyJSONEachRow](/docs/ja/interfaces/formats#prettyjsoneachrow)                                                                | -      | raw    |
| [JSONEachRowWithProgress](/docs/ja/interfaces/formats#jsoneachrowwithprogress)                                                    | -      | raw    |
| [JSONStringsEachRow](/docs/ja/interfaces/formats#jsonstringseachrow)                                                              | raw    | raw    |
| [JSONStringsEachRowWithProgress](/docs/ja/interfaces/formats#jsonstringseachrowwithprogress)                                      | -      | raw    |
| [JSONCompactEachRow](/docs/ja/interfaces/formats#jsoncompacteachrow)                                                              | raw    | raw    |
| [JSONCompactEachRowWithNames](/docs/ja/interfaces/formats#jsoncompacteachrowwithnames)                                            | raw    | raw    |
| [JSONCompactEachRowWithNamesAndTypes](/docs/ja/interfaces/formats#jsoncompacteachrowwithnamesandtypes)                            | raw    | raw    |
| [JSONCompactStringsEachRow](/docs/ja/interfaces/formats#jsoncompactstringseachrow)                                                | raw    | raw    |
| [JSONCompactStringsEachRowWithNames](/docs/ja/interfaces/formats#jsoncompactstringseachrowwithnames)                              | raw    | raw    |
| [JSONCompactStringsEachRowWithNamesAndTypes](/docs/ja/interfaces/formats#jsoncompactstringseachrowwithnamesandtypes)              | raw    | raw    |
| [JSONObjectEachRow](/docs/ja/interfaces/formats#jsonobjecteachrow)                                                                | raw    | raw    |
| [BSONEachRow](/docs/ja/interfaces/formats#bsoneachrow)                                                                            | raw    | raw    |
| [TSKV](/docs/ja/interfaces/formats#tskv)                                                                                          | raw    | raw    |
| [Pretty](/docs/ja/interfaces/formats#pretty)                                                                                      | -      | raw    |
| [PrettyNoEscapes](/docs/ja/interfaces/formats#prettynoescapes)                                                                    | -      | raw    |
| [PrettyMonoBlock](/docs/ja/interfaces/formats#prettymonoblock)                                                                    | -      | raw    |
| [PrettyNoEscapesMonoBlock](/docs/ja/interfaces/formats#prettynoescapesmonoblock)                                                  | -      | raw    |
| [PrettyCompact](/docs/ja/interfaces/formats#prettycompact)                                                                        | -      | raw    |
| [PrettyCompactNoEscapes](/docs/ja/interfaces/formats#prettycompactnoescapes)                                                      | -      | raw    |
| [PrettyCompactMonoBlock](/docs/ja/interfaces/formats#prettycompactmonoblock)                                                      | -      | raw    |
| [PrettyCompactNoEscapesMonoBlock](/docs/ja/interfaces/formats#prettycompactnoescapesmonoblock)                                    | -      | raw    |
| [PrettySpace](/docs/ja/interfaces/formats#prettyspace)                                                                            | -      | raw    |
| [PrettySpaceNoEscapes](/docs/ja/interfaces/formats#prettyspacenoescapes)                                                          | -      | raw    |
| [PrettySpaceMonoBlock](/docs/ja/interfaces/formats#prettyspacemonoblock)                                                          | -      | raw    |
| [PrettySpaceNoEscapesMonoBlock](/docs/ja/interfaces/formats#prettyspacenoescapesmonoblock)                                        | -      | raw    |
| [Prometheus](/docs/ja/interfaces/formats#prometheus)                                                                              | -      | raw    |
| [Protobuf](/docs/ja/interfaces/formats#protobuf)                                                                                  | raw    | raw    |
| [ProtobufSingle](/docs/ja/interfaces/formats#protobufsingle)                                                                      | raw    | raw    |
| [ProtobufList](/docs/ja/interfaces/formats#protobuflist)                                                                          | raw    | raw    |
| [Avro](/docs/ja/interfaces/formats#data-format-avro)                                                                              | raw    | raw    |
| [AvroConfluent](/docs/ja/interfaces/formats#data-format-avro-confluent)                                                           | raw    | -      |
| [Parquet](/docs/ja/interfaces/formats#data-format-parquet)                                                                        | raw    | raw    |
| [ParquetMetadata](/docs/ja/interfaces/formats#data-format-parquet-metadata)                                                       | raw    | -      |
| [Arrow](/docs/ja/interfaces/formats#data-format-arrow)                                                                            | raw    | raw    |
| [ArrowStream](/docs/ja/interfaces/formats#data-format-arrow-stream)                                                               | raw    | raw    |
| [ORC](/docs/ja/interfaces/formats#data-format-orc)                                                                                | raw    | raw    |
| [One](/docs/ja/interfaces/formats#data-format-one)                                                                                | raw    | -      |
| [Npy](/docs/ja/interfaces/formats#data-format-npy)                                                                                | raw    | raw    |
| [RowBinary](/docs/ja/interfaces/formats#rowbinary)                                                                                | full   | full   |
| [RowBinaryWithNames](/docs/ja/interfaces/formats#rowbinarywithnamesandtypes)                                                      | full   | full   |
| [RowBinaryWithNamesAndTypes](/docs/ja/interfaces/formats#rowbinarywithnamesandtypes)                                              | full   | full   |
| [RowBinaryWithDefaults](/docs/ja/interfaces/formats#rowbinarywithdefaults)                                                        | full   | -      |
| [Native](/docs/ja/interfaces/formats#native)                                                                                      | full   | raw    |
| [Null](/docs/ja/interfaces/formats#null)                                                                                          | -      | raw    |
| [XML](/docs/ja/interfaces/formats#xml)                                                                                            | -      | raw    |
| [CapnProto](/docs/ja/interfaces/formats#capnproto)                                                                                | raw    | raw    |
| [LineAsString](/docs/ja/interfaces/formats#lineasstring)                                                                          | raw    | raw    |
| [Regexp](/docs/ja/interfaces/formats#data-format-regexp)                                                                          | raw    | -      |
| [RawBLOB](/docs/ja/interfaces/formats#rawblob)                                                                                    | raw    | raw    |
| [MsgPack](/docs/ja/interfaces/formats#msgpack)                                                                                    | raw    | raw    |
| [MySQLDump](/docs/ja/interfaces/formats#mysqldump)                                                                                | raw    | -      |
| [DWARF](/docs/ja/interfaces/formats#dwarf)                                                                                        | raw    | -      |
| [Markdown](/docs/ja/interfaces/formats#markdown)                                                                                  | -      | raw    |
| [Form](/docs/ja/interfaces/formats#form)                                                                                          | raw    | -      |


## データ挿入API

### insert(String tableName, InputStream data, ClickHouseFormat format) 

指定されたフォーマットでエンコードされたバイトの`InputStream`としてデータを受け入れます。`data`が`format`でエンコードされていることが期待されます。

**シグネチャ**

```java
CompletableFuture<InsertResponse> insert(String tableName, InputStream data, ClickHouseFormat format, InsertSettings settings)
CompletableFuture<InsertResponse> insert(String tableName, InputStream data, ClickHouseFormat format)
```

**パラメータ**

`tableName` - 対象テーブル名。

`data` - エンコードされたデータの入力ストリーム。

`format` - データがエンコードされた形式。

`settings` - リクエスト設定。

**戻り値**

`InsertResponse`型のFuture - 操作結果とサーバサイドのメトリクスなどの追加情報。

**例**

```java showLineNumbers
try (InputStream dataStream = getDataStream()) {
    try (InsertResponse response = client.insert(TABLE_NAME, dataStream, ClickHouseFormat.JSONEachRow,
            insertSettings).get(3, TimeUnit.SECONDS)) {

        log.info("Insert finished: {} rows written", response.getMetrics().getMetric(ServerMetrics.NUM_ROWS_WRITTEN).getLong());
    } catch (Exception e) {
        log.error("Failed to write JSONEachRow data", e);
        throw new RuntimeException(e);
    }
}

```

### insert(String tableName, List<?> data, InsertSettings settings)

データベースに書き込みリクエストを送信します。オブジェクトのリストは効率的な形式に変換され、その後サーバーに送信されます。リストアイテムのクラスは、事前に`register(Class, TableSchema)`メソッドを使用して登録する必要があります。

**シグネチャ**
```java
client.insert(String tableName, List<?> data, InsertSettings settings)
client.insert(String tableName, List<?> data)
```

**パラメータ**

`tableName` - 対象のテーブル名。

`data` - DTO (Data Transfer Object) オブジェクトのコレクション。

`settings` - リクエスト設定。

**戻り値**

`InsertResponse`型のFuture - 操作結果とサーバサイドのメトリクスなどの追加情報。

**例**

```java showLineNumbers
// 重要なステップ（1回だけ実行）：テーブルスキーマに従ってオブジェクトシリアライザを事前コンパイルするためにクラスを登録。
client.register(ArticleViewEvent.class, client.getTableSchema(TABLE_NAME));


List<ArticleViewEvent> events = loadBatch();

try (InsertResponse response = client.insert(TABLE_NAME, events).get()) {
    // 応答を処理し、リクエストをサーブした後に閉じられ、接続が解放されます。
}
```

### InsertSettings

挿入操作のための設定オプション。

**設定メソッド**

<dl>
  <dt>setQueryId(String queryId)</dt>
  <dd>操作に割り当てられるクエリIDを設定します</dd>

  <dt>setDeduplicationToken(String token)</dt>
  <dd>重複除外トークンを設定します。このトークンはサーバーに送信され、クエリを識別するために使用されます。</dd>

  <dt>waitEndOfQuery(Boolean waitEndOfQuery)</dt>
  <dd>クエリの終了を待ってから応答を送信するようサーバーにリクエストします。</dd>

  <dt>setInputStreamCopyBufferSize(int size)</dt>
  <dd>コピー時のバッファサイズ。ユーザー提供の入力ストリームから出力ストリームへの書き込み操作中に使用されるバッファです。</dd>
</dl>

### InsertResponse 

挿入操作の結果を保持する応答オブジェクトです。このオブジェクトはサーバーから応答を受け取った場合にのみ利用可能です。

:::note
このオブジェクトは、前の応答のデータがすべて読み取られるまで接続が再利用できないため、できるだけ早く閉じて接続を解放する必要があります。
:::

<dl>
    <dt>OperationMetrics getMetrics()</dt>
    <dd>操作メトリクスを含むオブジェクトを返します</dd>
    <dt>String getQueryId()</dt>
    <dd>アプリケーションによって操作に割り当てられたクエリ ID（操作設定またはサーバーによって）を返します。</dd>
</dl>

## クエリAPI

### query(String sqlQuery)

`sqlQuery`をそのまま送信します。応答フォーマットはクエリ設定によって設定されます。`QueryResponse`は、対応するフォーマットのリーダーが消費すべき応答ストリームへの参照を保持します。

**シグネチャ**

```java 
CompletableFuture<QueryResponse> query(String sqlQuery, QuerySettings settings)
CompletableFuture<QueryResponse> query(String sqlQuery)
```

**パラメータ**

`sqlQuery` - 単一のSQLステートメント。クエリはそのままサーバーに送信されます。

`settings` - リクエスト設定。

**戻り値**

`QueryResponse`型のFuture - 結果データセットとサーバサイドのメトリクスなどの追加情報を含みます。データセットを消費した後に応答オブジェクトを閉じる必要があります。

**例**

```java 
final String sql = "select * from " + TABLE_NAME + " where title <> '' limit 10";

// デフォルトフォーマットはRowBinaryWithNamesAndTypesFormatReaderであるため、リーダーはすべてのカラム情報を持っています
try (QueryResponse response = client.query(sql).get(3, TimeUnit.SECONDS);) {

    // データに便利にアクセスするためのリーダーを作成
    ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response);

    while (reader.hasNext()) {
        reader.next(); // ストリームから次のレコードを読み取り、解析する

        // 値を取得
        double id = reader.getDouble("id");
        String title = reader.getString("title");
        String url = reader.getString("url");

        // データを収集
    }
} catch (Exception e) {
    log.error("Failed to read data", e);
}

// ビジネスロジックは可能な限り早くHTTP接続を解放するように、読取りブロックの外に置いてください。
```

### query(String sqlQuery, Map<String, Object> queryParams, QuerySettings settings) 

`sqlQuery`をそのまま送信します。さらに、クエリパラメータを送信し、サーバーがSQL式をコンパイルできるようにします。

**シグネチャ**
```java 
CompletableFuture<QueryResponse> query(String sqlQuery, Map<String, Object> queryParams, QuerySettings settings)
```

**パラメータ**

`sqlQuery` - `{}`を含むSQL式。

`queryParams` - サーバーでSQL式を完成させるための変数のマップ。

`settings` - リクエスト設定。

**戻り値**

`QueryResponse`型のFuture - 結果データセットとサーバサイドのメトリクスなどの追加情報を含みます。データセットを消費した後に応答オブジェクトを閉じる必要があります。

**例**

```java showLineNumbers

// パラメータを定義。それらはリクエストと一緒にサーバーに送信されます。
Map<String, Object> queryParams = new HashMap<>();
queryParams.put("param1", 2);

try (QueryResponse queryResponse =
        client.query("SELECT * FROM " + table + " WHERE col1 >= {param1:UInt32}", queryParams, new QuerySettings()).get()) {

    // データに便利にアクセスするためのリーダーを作成
    ClickHouseBinaryFormatReader reader = client.newBinaryFormatReader(response);

    while (reader.hasNext()) {
        reader.next(); // ストリームから次のレコードを読み取り、解析する

        // データを読み取る
    }

} catch (Exception e) {
    log.error("Failed to read data", e);
}

```

### queryAll(String sqlQuery)

`RowBinaryWithNamesAndTypes`形式でデータをクエリします。結果をコレクションとして返します。読み取りパフォーマンスはリーダーと同じですが、全データセットを保持するためにはより多くのメモリが必要です。

**シグネチャ**
```java 
List<GenericRecord> queryAll(String sqlQuery)
```

**パラメータ**

`sqlQuery` - サーバーからデータをクエリするためのSQL式。

**戻り値**

結果データを行スタイルでアクセスするために`GenericRecord`オブジェクトのリストで表現された完全なデータセットを返します。

**例**

```java showLineNumbers
try {
    log.info("テーブル全体を読み取り、レコードごとに処理中");
    final String sql = "select * from " + TABLE_NAME + " where title <> ''";

    // 完全な結果セットを読み取り、レコードごとに処理
    client.queryAll(sql).forEach(row -> {
        double id = row.getDouble("id");
        String title = row.getString("title");
        String url = row.getString("url");

        log.info("id: {}, title: {}, url: {}", id, title, url);
    });
} catch (Exception e) {
    log.error("Failed to read data", e);
}
```

### QuerySettings

クエリ操作のための設定オプション。

**設定メソッド**

<dl>
  <dt>setQueryId(String queryId)</dt>
  <dd>操作に割り当てられるクエリIDを設定します</dd>
  <dt>setFormat(ClickHouseFormat format)</dt>
  <dd>応答フォーマットを設定します。完全なリストは`RowBinaryWithNamesAndTypes`を参照してください。</dd>
  <dt>setMaxExecutionTime(Integer maxExecutionTime)</dt>
  <dd>サーバーでの操作実行時間を設定します。読み取りタイムアウトには影響しません。</dd>
  <dt>waitEndOfQuery(Boolean waitEndOfQuery)</dt>
  <dd>クエリの終了を待ってから応答を送信するようサーバーにリクエストします。</dd>
  <dt>setUseServerTimeZone(Boolean useServerTimeZone)</dt>
  <dd>サーバーのタイムゾーン（クライアント設定を参照）が、操作の結果の日時型を解析するために使用されます。デフォルト`false`</dd>
  <dt>setUseTimeZone(String timeZone)</dt>
  <dd>時間の変換のためにサーバーに`timeZone`を使用するよう要求します。<a href="/docs/ja/operations/settings/settings#session_timezone" target="_blank">session_timezone</a>を参照してください。</dd>
</dl>

### QueryResponse 

クエリ実行の結果を保持する応答オブジェクトです。このオブジェクトはサーバーからの応答を受け取った場合にのみ利用可能です。

:::note
このオブジェクトは、前の応答のデータが完全に読み込まれるまで接続を再利用できないため、できるだけ早く閉じて接続を解放する必要があります。
:::

<dl>
    <dt>ClickHouseFormat getFormat()</dt>
    <dd>応答のデータがエンコードされているフォーマットを返します。</dd>
    <dt>InputStream getInputStream()</dt>
    <dd>指定された形式での非圧縮バイトストリームを返します。</dd>
    <dt>OperationMetrics getMetrics()</dt>
    <dd>操作メトリクスを含むオブジェクトを返します</dd>
    <dt>String getQueryId()</dt>
    <dd>アプリケーションによって操作に割り当てられたクエリ ID（操作設定またはサーバーによって）を返します。</dd>
    <dt>TimeZone getTimeZone()</dt>
    <dd>応答でのDate/DateTime型を処理するために使用するタイムゾーンを返します。</dd>
</dl>

### 例 

- [リポジトリ](https://github.com/ClickHouse/clickhouse-java/tree/main/examples/client-v2)に例コードがあります
- 見本のSpring Service [実装](https://github.com/ClickHouse/clickhouse-java/tree/main/examples/demo-service)

## 共通API

### getTableSchema(String table)

`table`のスキーマを取得します。

**シグネチャ**

```java 
TableSchema getTableSchema(String table)
TableSchema getTableSchema(String table, String database)
```

**パラメータ**

`table` - スキーマデータを取得するためのテーブル名。

`database` - 対象のテーブルが定義されているデータベース。

**戻り値**

テーブルカラムのリストを持つ`TableSchema`オブジェクトを返します。

### getTableSchemaFromQuery(String sql)

SQLステートメントからスキーマを取得します。

**シグネチャ**

```java 
TableSchema getTableSchemaFromQuery(String sql)
```

**パラメータ**

`sql` - スキーマを返すべき"SELECT" SQLステートメント。

**戻り値**

`sql`式に一致するカラムを持つ`TableSchema`オブジェクトを返します。

### TableSchema

### register(Class<?> clazz, TableSchema schema)

Javaクラス用に書き込み/読み込みデータとするSerDeレイヤーをコンパイルします。このメソッドは、ゲッター/セッターペアと対応するカラムのためにシリアライザとデシリアライザを作成します。
カラムの一致はメソッド名からその名前を抽出することで見つけられます。例えば、`getFirstName`はカラム `first_name`または`firstname`に対応します。

**シグネチャ**

```java 
void register(Class<?> clazz, TableSchema schema)
```

**パラメータ**

`clazz` - データの読み書きに使用されるPOJOを表すクラス。

`schema` - POJOプロパティと一致させるためのデータスキーマ。

**例**

```java showLineNumbers 
client.register(ArticleViewEvent.class, client.getTableSchema(TABLE_NAME));
```

## 使用例 

完全な例のコードは、 `example` [フォルダ](https://github.com/ClickHouse/clickhouse-java/tree/main/examples)にリポジトリ内に保存されています：

- [client-v2](https://github.com/ClickHouse/clickhouse-java/tree/main/examples/client-v2) - 主な例のセット。
- [demo-service](https://github.com/ClickHouse/clickhouse-java/tree/main/examples/demo-service) - Spring Bootアプリケーションでクライアントを使用する方法の例。
- [demo-kotlin-service](https://github.com/ClickHouse/clickhouse-java/tree/main/examples/demo-kotlin-service) - Ktor (Kotlin) アプリケーションでクライアントを使用する方法の例。
