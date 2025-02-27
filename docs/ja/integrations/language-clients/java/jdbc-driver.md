---
sidebar_label: JDBC ドライバー
sidebar_position: 4
keywords: [clickhouse, java, jdbc, ドライバー, 統合]
description: ClickHouse JDBC ドライバー
slug: /ja/integrations/java/jdbc-driver
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import CodeBlock from '@theme/CodeBlock';


# JDBC ドライバー

`clickhouse-jdbc` は標準の JDBC インターフェースを実装しています。[clickhouse-client](/docs/ja/integrations/clickhouse-client-local.md) の上に構築されており、カスタムタイプマッピング、トランザクションサポート、標準的な同期 `UPDATE` および `DELETE` 文などの追加機能を提供します。これにより、レガシーアプリケーションやツールでも簡単に使用できます。

:::note
最新の JDBC (0.6.5) バージョンは Client-V1 を使用しています
:::

`clickhouse-jdbc` API は同期的であり、一般により大きなオーバーヘッド（例：SQL パース、タイプマッピング/変換など）があります。パフォーマンスが重要な場合や、ClickHouse へのより直接的なアクセスを好む場合は、[clickhouse-client](/docs/ja/integrations/clickhouse-client-local.md) の使用を検討してください。

## 環境要件

- [OpenJDK](https://openjdk.java.net) バージョン >= 8


### セットアップ

<Tabs groupId="client-v1-compression-deps">
<TabItem value="maven" label="Maven" >

```xml 
<!-- https://mvnrepository.com/artifact/com.clickhouse/clickhouse-jdbc -->
<dependency>
    <groupId>com.clickhouse</groupId>
    <artifactId>clickhouse-jdbc</artifactId>
    <version>0.6.5</version>
    <!-- すべての依存関係を含む uber jar を使用し、より小さい jar のために分類子を http に変更 -->
    <classifier>all</classifier>    
</dependency>
```

</TabItem>
<TabItem value="gradle-kt" label="Gradle (Kotlin)">

```kotlin
// https://mvnrepository.com/artifact/com.clickhouse/clickhouse-jdbc
// すべての依存関係を含む uber jar を使用し、より小さい jar のために分類子を http に変更
implementation("com.clickhouse:clickhouse-jdbc:0.6.5:all")
```
</TabItem>
<TabItem value="gradle" label="Gradle">

```groovy
// https://mvnrepository.com/artifact/com.clickhouse/clickhouse-jdbc
// すべての依存関係を含む uber jar を使用し、より小さい jar のために分類子を http に変更
implementation 'com.clickhouse:clickhouse-jdbc:0.6.5:all'
```

</TabItem>
</Tabs>

バージョン `0.5.0` 以降、Apache HTTP クライアントを使用して Client がパックされています。パッケージに共有バージョンがないため、依存関係としてロガーを追加する必要があります。

<Tabs groupId="client-v1-compression-deps">
<TabItem value="maven" label="Maven" >

```xml 
<!-- https://mvnrepository.com/artifact/org.slf4j/slf4j-api -->
<dependency>
    <groupId>org.slf4j</groupId>
    <artifactId>slf4j-api</artifactId>
    <version>2.0.16</version>
</dependency>
```

</TabItem>
<TabItem value="gradle-kt" label="Gradle (Kotlin)">

```kotlin
// https://mvnrepository.com/artifact/org.slf4j/slf4j-api
implementation("org.slf4j:slf4j-api:2.0.16")
```
</TabItem>
<TabItem value="gradle" label="Gradle">

```groovy
// https://mvnrepository.com/artifact/org.slf4j/slf4j-api
implementation 'org.slf4j:slf4j-api:2.0.16'
```

</TabItem>
</Tabs>

## 設定

**ドライバークラス**: `com.clickhouse.jdbc.ClickHouseDriver`

**URL 構文**: `jdbc:(ch|clickhouse)[:<protocol>]://endpoint1[,endpoint2,...][/<database>][?param1=value1&param2=value2][#tag1,tag2,...]`, 例:

- `jdbc:ch://localhost` は `jdbc:clickhouse:http://localhost:8123` と同じです
- `jdbc:ch:https://localhost` は `jdbc:clickhouse:http://localhost:8443?ssl=true&sslmode=STRICT` と同じです
- `jdbc:ch:grpc://localhost` は `jdbc:clickhouse:grpc://localhost:9100` と同じです

**接続プロパティ**:

| プロパティ                | デフォルト | 説明 |
| ------------------------ | ------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| continueBatchOnError     | `false` | エラーが発生したときにバッチ処理を続行するかどうか                                                                                                                                                                                                                                                                                                                                                                   |
| createDatabaseIfNotExist | `false` | データベースが存在しない場合に作成するかどうか                                                                                                                                                                                                                                                                                                                                                                            |
| custom_http_headers      |         | カンマ区切りのカスタム HTTP ヘッダー、例: `User-Agent=client1,X-Gateway-Id=123`                                                                                                                                                                                                                                                                                                                                    |
| custom_http_params       |         | カンマ区切りのカスタム HTTP クエリパラメータ、例: `extremes=0,max_result_rows=100`                                                                                                                                                                                                                                                                                                                                |
| nullAsDefault            | `0`     | `0` - null 値をそのまま扱い、null 値を非null カラムに挿入する際に例外を投げる; `1` - null 値をそのまま扱い、挿入時のnull チェックを無効にする; `2` - クエリおよび挿入の両方でnull を対応するデータタイプのデフォルト値に置き換える                                                                                                                                                                 |
| jdbcCompliance           | `true`  | 標準の同期UPDATE/DELETE およびフェイクトランザクションをサポートするかどうか                                                                                                                                                                                                                                                                                                                                                 |
| typeMappings             |         | ClickHouse データタイプと Java クラスとのマッピングをカスタマイズし、[getColumnType()](https://docs.oracle.com/javase/8/docs/api/java/sql/ResultSetMetaData.html#getColumnType-int-) および [getObject(Class<?>)](https://docs.oracle.com/javase/8/docs/api/java/sql/ResultSet.html#getObject-java.lang.String-java.lang.Class-) の結果に影響します。例: `UInt128=java.lang.String,UInt256=java.lang.String` |
| wrapperObject            | `false` | [getObject()](https://docs.oracle.com/javase/8/docs/api/java/sql/ResultSet.html#getObject-int-) がArray /Tuple に対して java.sql.Array / java.sql.Struct を返すかどうか                                                                                                                                                                                                                                                  |

注: 詳細は [JDBC specific configuration](https://github.com/ClickHouse/clickhouse-java/blob/main/clickhouse-jdbc/src/main/java/com/clickhouse/jdbc/JdbcConfig.java) を参照してください。

## サポートされているデータタイプ

JDBC ドライバーはクライアントライブラリと同じデータフォーマットをサポートしています。 

:::note
- AggregatedFunction - :warning: `SELECT * FROM table ...` はサポートされていません
- Decimal - 一貫性のために 21.9+ で `SET output_format_decimal_trailing_zeros=1` 
- Enum - 文字列と整数の両方として扱われることができます
- UInt64 - `long` にマップされます (client-v1 では) 
:::

## 接続を作成する

```java
String url = "jdbc:ch://my-server/system"; // デフォルトで http プロトコルとポート 8123 を使用

Properties properties = new Properties();

ClickHouseDataSource dataSource = new ClickHouseDataSource(url, properties);
try (Connection conn = dataSource.getConnection("default", "password");
    Statement stmt = conn.createStatement()) {
}
```

## シンプルなステートメント

```java showLineNumbers

try (Connection conn = dataSource.getConnection(...);
    Statement stmt = conn.createStatement()) {
    ResultSet rs = stmt.executeQuery("select * from numbers(50000)");
    while(rs.next()) {
        // ...
    }
}
```

## 挿入

:::note
- `PreparedStatement` を `Statement` の代わりに使用してください
:::

使用は簡単ですが、入力関数（下記参照）と比較してパフォーマンスが遅くなります:

```java showLineNumbers
try (PreparedStatement ps = conn.prepareStatement("insert into mytable(* except (description))")) {
    ps.setString(1, "test"); // id
    ps.setObject(2, LocalDateTime.now()); // タイムスタンプ
    ps.addBatch(); // パラメータはバイナリ形式でバッファされたストリームに即座に書き込まれます
    ...
    ps.executeBatch(); // 手元のすべてをストリームして ClickHouse に送信します
}
```

### 入力テーブル関数を使用

優れたパフォーマンス特性を持つオプション:

```java showLineNumbers
try (PreparedStatement ps = conn.prepareStatement(
    "insert into mytable select col1, col2 from input('col1 String, col2 DateTime64(3), col3 Int32')")) {
    // カラム定義が解析され、ドライバーは 3 つのパラメータがあることを知ります: col1, col2 および col3
    ps.setString(1, "test"); // col1
    ps.setObject(2, LocalDateTime.now()); // col2, setTimestamp は遅いので推奨されません
    ps.setInt(3, 123); // col3
    ps.addBatch(); // パラメータはバイナリ形式でバッファーされたストリームに即座に書き込まれます
    ...
    ps.executeBatch(); // 手元のすべてをストリームして ClickHouse に送信します
}
```
- 可能な限り [入力関数ドキュメント](/ja/sql-reference/table-functions/input/) を参照してください

### プレースホルダを使用した挿入

このオプションは、クライアント側で解析される長い SQL 式が必要になるため、小さな挿入の場合にのみ推奨されます:

```java showLineNumbers
try (PreparedStatement ps = conn.prepareStatement("insert into mytable values(trim(?),?,?)")) {
    ps.setString(1, "test"); // id
    ps.setObject(2, LocalDateTime.now()); // タイムスタンプ
    ps.setString(3, null); // description
    ps.addBatch(); // クエリにパラメータを追加
    ...
    ps.executeBatch(); // 構成されたクエリを発行: insert into mytable values(...)(...)...(...)
}
```

## DateTime とタイムゾーンの処理

`java.sql.Timestamp` ではなく `java.time.LocalDateTime` または `java.time.OffsetDateTime`、および `java.sql.Date` ではなく `java.time.LocalDate` を使用してください。

```java showLineNumbers
try (PreparedStatement ps = conn.prepareStatement("select date_time from mytable where date_time > ?")) {
    ps.setObject(2, LocalDateTime.now());
    ResultSet rs = ps.executeQuery();
    while(rs.next()) {
        LocalDateTime dateTime = (LocalDateTime) rs.getObject(1);
    }
    ...
}
```

## `AggregateFunction` の処理

:::note
現時点では、`groupBitmap` のみがサポートされています。
:::

```java showLineNumbers
// 入力関数を使用したバッチ挿入
try (ClickHouseConnection conn = newConnection(props);
        Statement s = conn.createStatement();
        PreparedStatement stmt = conn.prepareStatement(
                "insert into test_batch_input select id, name, value from input('id Int32, name Nullable(String), desc Nullable(String), value AggregateFunction(groupBitmap, UInt32)')")) {
    s.execute("drop table if exists test_batch_input;"
            + "create table test_batch_input(id Int32, name Nullable(String), value AggregateFunction(groupBitmap, UInt32))engine=Memory");
    Object[][] objs = new Object[][] {
            new Object[] { 1, "a", "aaaaa", ClickHouseBitmap.wrap(1, 2, 3, 4, 5) },
            new Object[] { 2, "b", null, ClickHouseBitmap.wrap(6, 7, 8, 9, 10) },
            new Object[] { 3, null, "33333", ClickHouseBitmap.wrap(11, 12, 13) }
    };
    for (Object[] v : objs) {
        stmt.setInt(1, (int) v[0]);
        stmt.setString(2, (String) v[1]);
        stmt.setString(3, (String) v[2]);
        stmt.setObject(4, v[3]);
        stmt.addBatch();
    }
    int[] results = stmt.executeBatch();
    ...
}

// クエリパラメータとして bitmap を使用
try (PreparedStatement stmt = conn.prepareStatement(
    "SELECT bitmapContains(my_bitmap, toUInt32(1)) as v1, bitmapContains(my_bitmap, toUInt32(2)) as v2 from {tt 'ext_table'}")) {
    stmt.setObject(1, ClickHouseExternalTable.builder().name("ext_table")
            .columns("my_bitmap AggregateFunction(groupBitmap,UInt32)").format(ClickHouseFormat.RowBinary)
            .content(new ByteArrayInputStream(ClickHouseBitmap.wrap(1, 3, 5).toBytes()))
            .asTempTable()
            .build());
    ResultSet rs = stmt.executeQuery();
    Assert.assertTrue(rs.next());
    Assert.assertEquals(rs.getInt(1), 1);
    Assert.assertEquals(rs.getInt(2), 0);
    Assert.assertFalse(rs.next());
}
```

<br/>

## HTTP ライブラリの設定

ClickHouse JDBC コネクターは、[HttpClient](https://docs.oracle.com/en/java/javase/11/docs/api/java.net.http/java/net/http/HttpClient.html)、[HttpURLConnection](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/net/HttpURLConnection.html)、[Apache HttpClient](https://hc.apache.org/httpcomponents-client-5.2.x/) の3つの HTTP ライブラリをサポートしています。

:::note
HttpClient は JDK 11 以上でのみサポートされています。
:::

JDBC ドライバーは、デフォルトで `HttpClient` を使用します。ClickHouse JDBC コネクターで使用する HTTP ライブラリを変更するには、次のプロパティを設定します。

```java
properties.setProperty("http_connection_provider", "APACHE_HTTP_CLIENT");
```

対応する値の完全なリストは次の通りです:

| プロパティ値         | HTTP ライブラリ       |
| ------------------- | ----------------- |
| HTTP_CLIENT         | HTTPClient        |
| HTTP_URL_CONNECTION | HttpURLConnection |
| APACHE_HTTP_CLIENT  | Apache HttpClient |

<br/>

## SSL を使用して ClickHouse に接続する

SSL を用いて ClickHouse にセキュアに JDBC で接続するには、SSL パラメータを含むように JDBC プロパティを設定する必要があります。これは通常、JDBC URL またはプロパティオブジェクトに `sslmode` や `sslrootcert` などの SSL プロパティを指定することを含みます。

## SSL プロパティ

| 名称                 | デフォルト値  | オプション値      | 説明                                                                         |
| ------------------ | ------------- | --------------- | ---------------------------------------------------------------------------- |
| ssl                | false         | true, false     | 接続で SSL/TLS を有効にするかどうか                                          |
| sslmode            | strict        | strict, none    | SSL/TLS 証明書を確認するかどうか                                             |
| sslrootcert        |               |                 | SSL/TLS ルート証明書のパス                                                  |
| sslcert            |               |                 | SSL/TLS 証明書のパス                                                        |
| sslkey             |               |                 | PKCS#8 フォーマットの RSA キー                                              |
| key_store_type     |               | JKS, PKCS12     | キーストア/トラストストアファイルのタイプまたはフォーマットを指定           |
| trust_store        |               |                 | トラストストアファイルのパス                                                |
| key_store_password |               |                 | キーストア設定で指定されたファイルにアクセスするためのパスワード              |

これらのプロパティは、Java アプリケーションが ClickHouse サーバーと暗号化された接続で通信し、データ転送中のセキュリティを強化します。

```java showLineNumbers
  String url = "jdbc:ch://your-server:8443/system";

  Properties properties = new Properties();
  properties.setProperty("ssl", "true");
  properties.setProperty("sslmode", "strict"); // NONE はすべてのサーバーを信用し、STRICT は信頼できるもののみ
  properties.setProperty("sslrootcert", "/mine.crt");
  try (Connection con = DriverManager
          .getConnection(url, properties)) {

      try (PreparedStatement stmt = con.prepareStatement(

          // ここにコードを記述

      }
  }
```

## 大規模挿入時の JDBC タイムアウトの解決

ClickHouse で実行時間の長い大規模挿入を行う場合、以下のような JDBC タイムアウトエラーに遭遇することがあります:

```plaintext
Caused by: java.sql.SQLException: Read timed out, server myHostname [uri=https://hostname.aws.clickhouse.cloud:8443]
```

これらのエラーはデータ挿入プロセスを妨げ、システムの安定性に影響を与える可能性があります。この問題を解決するには、クライアントの OS のいくつかのタイムアウト設定を調整する必要があります。

### Mac OS

Mac OS では、次の設定を調整して問題を解決できます:

- `net.inet.tcp.keepidle`: 60000
- `net.inet.tcp.keepintvl`: 45000
- `net.inet.tcp.keepinit`: 45000
- `net.inet.tcp.keepcnt`: 8
- `net.inet.tcp.always_keepalive`: 1

### Linux

Linux では、同等の設定だけでは問題が解決しない場合があります。ソケットのキープアライブ設定を扱う方法が異なるため、追加の手順が必要です。次の手順に従ってください:

1. `/etc/sysctl.conf` や関連する設定ファイルで次の Linux カーネルパラメータを調整します:

   - `net.inet.tcp.keepidle`: 60000
   - `net.inet.tcp.keepintvl`: 45000
   - `net.inet.tcp.keepinit`: 45000
   - `net.inet.tcp.keepcnt`: 8
   - `net.inet.tcp.always_keepalive`: 1
   - `net.ipv4.tcp_keepalive_intvl`: 75
   - `net.ipv4.tcp_keepalive_probes`: 9
   - `net.ipv4.tcp_keepalive_time`: 60 (デフォルトの300秒からこの値を下げることを検討してください)

2. カーネルパラメータを変更した後、次のコマンドを実行して変更を適用します:

   ```shell
   sudo sysctl -p
   ```

これらの設定を行った後、クライアントがソケットのキープアライブオプションを有効にしていることを確認する必要があります:

```java
properties.setProperty("socket_keepalive", "true");
```

:::note
現在、`clickhouse-java` によってサポートされている他の 2 つの HTTP クライアントライブラリでは、ソケットオプションの設定が許可されていないため、ソケットのキープアライブを設定する際は Apache HTTP クライアントライブラリを使用する必要があります。詳しくは [HTTP ライブラリの設定](/docs/ja/integrations/java#configuring-http-library) を参照してください。
:::

また、JDBC URL に同等のパラメータを追加することもできます。

JDBC ドライバーのデフォルトのソケットおよび接続タイムアウトは 30 秒です。このタイムアウトを大規模データ挿入操作をサポートするために拡大することができます。`ClickHouseClient` で `options` メソッドを使用し、`ClickHouseClientOption` によって定義された `SOCKET_TIMEOUT` および `CONNECTION_TIMEOUT` オプションと共に使用します:

```java showLineNumbers
final int MS_12H = 12 * 60 * 60 * 1000; // 12 時間(ミリ秒)
final String sql = "insert into table_a (c1, c2, c3) select c1, c2, c3 from table_b;";

try (ClickHouseClient client = ClickHouseClient.newInstance(ClickHouseProtocol.HTTP)) {
    client.read(servers).write()
        .option(ClickHouseClientOption.SOCKET_TIMEOUT, MS_12H)
        .option(ClickHouseClientOption.CONNECTION_TIMEOUT, MS_12H)
        .query(sql)
        .executeAndWait();
}
```

