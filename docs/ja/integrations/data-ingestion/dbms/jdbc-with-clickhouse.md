---
sidebar_label: JDBC
sidebar_position: 2
keywords: [clickhouse, jdbc, connect, integrate]
slug: /ja/integrations/jdbc/jdbc-with-clickhouse
description: ClickHouse JDBC Bridgeを使用すると、JDBCドライバーが利用可能な任意の外部データソースからClickHouseがデータを取得できます
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';

# ClickHouseと外部データソースをJDBCで接続する

:::note
JDBCを使用するにはClickHouse JDBC Bridgeが必要であり、ローカルマシンで`clickhouse-local`を使用してデータベースからClickHouse Cloudにデータをストリームする必要があります。詳細については、ドキュメントの**移行**セクションにある[**clickhouse-localの使用**](/docs/ja/integrations/migration/clickhouse-local-etl.md#example-2-migrating-from-mysql-to-clickhouse-cloud-with-the-jdbc-bridge)ページを参照してください。
:::

**概要:** <a href="https://github.com/ClickHouse/clickhouse-jdbc-bridge" target="_blank">ClickHouse JDBC Bridge</a>は、[jdbc テーブル関数](/docs/ja/sql-reference/table-functions/jdbc.md)または[JDBC テーブルエンジン](/docs/ja/engines/table-engines/integrations/jdbc.md)と組み合わせて、<a href="https://en.wikipedia.org/wiki/JDBC_driver" target="_blank">JDBCドライバー</a>が利用可能な任意の外部データソースからClickHouseにデータをアクセスできるようにします:
<img src={require('./images/jdbc-01.png').default} class="image" alt="ClickHouse JDBC Bridge"/>
これは、外部データソースに対してネイティブに組み込まれた[統合エンジン](/docs/ja/engines/table-engines/index.md#integration-engines-integration-engines)、テーブル関数、または外部ディクショナリが利用できない場合に便利ですが、データソースのJDBCドライバーが存在します。

ClickHouse JDBC Bridgeを使用すると、複数の外部データソースに対して並行してリアルタイムでClickHouse上で分散クエリを実行することができます。

このレッスンでは、ClickHouseを外部データソースに接続するためにClickHouse JDBC Bridgeをインストール、設定、および実行する方法を示します。このレッスンでは、外部データソースとしてMySQLを使用します。

さあ、始めましょう！

:::note 前提条件
あなたのマシンには以下が必要です:
1. Unixシェルとインターネットアクセス
2. <a href="https://www.gnu.org/software/wget/" target="_blank">wget</a>がインストールされている
3. **Java**の最新バージョン（例：<a href="https://openjdk.java.net" target="_blank">OpenJDK</a> バージョン >= 17）がインストールされている
4. **MySQL**の最新バージョン（例：<a href="https://www.mysql.com" target="_blank">MySQL</a> バージョン >=8）がインストールされて動作している
5. **ClickHouse**が[インストール](/docs/ja/getting-started/install.md)されて動作している
:::

## ClickHouse JDBC Bridgeをローカルにインストールする

最も簡単なClickHouse JDBC Bridgeの使用方法は、ClickHouseが動作しているのと同じホストにインストールして実行することです:<img src={require('./images/jdbc-02.png').default} class="image" alt="ClickHouse JDBC Bridge locally"/>

ClickHouseが動作しているマシンのUnixシェルに接続し、ClickHouse JDBC Bridgeをインストールするためのローカルフォルダを作成しましょう。そのフォルダの名前や場所はお好みで決めてください:
```bash
mkdir ~/clickhouse-jdbc-bridge
```

次に、<a href="https://github.com/ClickHouse/clickhouse-jdbc-bridge/releases/" target="_blank">最新版</a>のClickHouse JDBC Bridgeをそのフォルダにダウンロードします:
```bash
cd ~/clickhouse-jdbc-bridge
wget https://github.com/ClickHouse/clickhouse-jdbc-bridge/releases/download/v2.0.7/clickhouse-jdbc-bridge-2.0.7-shaded.jar
```

MySQLに接続できるように、名前付きデータソースを作成します:
```bash
cd ~/clickhouse-jdbc-bridge
mkdir -p config/datasources
touch config/datasources/mysql8.json
```

次に、以下の構成をファイル `~/clickhouse-jdbc-bridge/config/datasources/mysql8.json` にコピー＆ペーストできます:
```json
{
  "mysql8": {
  "driverUrls": [
    "https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.28/mysql-connector-java-8.0.28.jar"
  ],
  "jdbcUrl": "jdbc:mysql://<host>:<port>",
  "username": "<username>",
  "password": "<password>"
  }
}
```

:::note
上記の設定ファイルには
- データソースの名前は自由に設定できます。ここでは`mysql8`を使用しました。
- `jdbcUrl` の値には、実行中のMySQLインスタンスに合わせて `<host>` と `<port>` を適切な値に置き換える必要があります（例：`"jdbc:mysql://localhost:3306"`）
- `<username>` と `<password>` をMySQLのクレデンシャルに置き換える必要があります。パスワードを使用しない場合は、上記の設定ファイルから`"password": "<password>"` の行を削除できます。
- `driverUrls` の値では、MySQL JDBCドライバーの<a href="https://repo1.maven.org/maven2/mysql/mysql-connector-java/" target="_blank">最新バージョン</a>をダウンロードできるURLを指定しました。これですべてです。ClickHouse JDBC BridgeはそのJDBCドライバーを自動的に（OS特定のディレクトリに）ダウンロードします。
:::

<br/>

これでClickHouse JDBC Bridgeを開始する準備が整いました:
```bash
cd ~/clickhouse-jdbc-bridge
java -jar clickhouse-jdbc-bridge-2.0.7-shaded.jar
```
:::note
ClickHouse JDBC Bridgeをフォアグラウンドモードで開始しました。ブリッジを停止するには、上記のUnixシェルウィンドウをフォアグラウンドにして`CTRL+C`を押すことができます。
:::

## ClickHouseからJDBC接続を使用する

ClickHouseは、[jdbc テーブル関数](/docs/ja/sql-reference/table-functions/jdbc.md)または[JDBC テーブルエンジン](/docs/ja/engines/table-engines/integrations/jdbc.md)を使用してMySQLデータにアクセスできます。

以下の例を実行する最も簡単な方法は、例をコピーして[`clickhouse-client`](/docs/ja/interfaces/cli.md)または[Play UI](/docs/ja/interfaces/http.md)に貼り付けることです。

- jdbc テーブル関数:

```sql
SELECT * FROM jdbc('mysql8', 'mydatabase', 'mytable');
```
:::note
jdbc テーブル関数の最初のパラメーターとして、上記で設定した名前付きデータソースの名前を使用しています。
:::

- JDBC テーブルエンジン:
```sql
CREATE TABLE mytable (
     <column> <column_type>,
     ...
)
ENGINE = JDBC('mysql8', 'mydatabase', 'mytable');

SELECT * FROM mytable;
```
:::note
jdbc エンジンクロースの最初のパラメーターとして、上記で設定した名前付きデータソースの名前を使用しています。

ClickHouse JDBCエンジンテーブルのスキーマと接続されたMySQLテーブルのスキーマは一致している必要があります。例として、カラム名と順序が同じであり、カラムのデータ型が互換性がある必要があります。
:::

## 外部からClickHouse JDBC Bridgeをインストールする

分散ClickHouseクラスター（複数のClickHouseホストを持つクラスター）の場合、ClickHouse JDBC Bridgeを専用のホストにインストールして実行するのが理にかなっています:
<img src={require('./images/jdbc-03.png').default} class="image" alt="ClickHouse JDBC Bridge externally"/>
この方法の利点は、各ClickHouseホストがJDBC Bridgeにアクセスできることです。そうでない場合は、外部データソースにアクセスするためのBridgeが必要な各ClickHouseインスタンスごとにローカルにJDBC Bridgeをインストールする必要があります。

ClickHouse JDBC Bridgeを外部にインストールするためには、以下の手順を実行します:

1. このガイドのセクション1で説明されている手順に従って、専用のホストにClickHouse JDBC Bridgeをインストール、設定、および実行します。

2. 各ClickHouseホストで、<a href="https://clickhouse.com/docs/ja/operations/configuration-files/#configuration_files" target="_blank">ClickHouseサーバー設定</a>に次の設定ブロックを追加します（選択した設定形式に応じて、XMLまたはYAMLバージョンを使用してください）:

<Tabs>
<TabItem value="xml" label="XML">

```xml
<jdbc_bridge>
   <host>JDBC-Bridge-Host</host>
   <port>9019</port>
</jdbc_bridge>
```

</TabItem>
<TabItem value="yaml" label="YAML">

```yaml
jdbc_bridge:
    host: JDBC-Bridge-Host
    port: 9019
```

</TabItem>
</Tabs>

:::note
   - `JDBC-Bridge-Host`を専用のClickHouse JDBC Bridgeホストのホスト名またはIPアドレスに置き換える必要があります
   - デフォルトのClickHouse JDBC Bridgeポート`9019`を指定しました。他のポートを使用する場合は、上記の設定をそれに応じて調整する必要があります
:::

[//]: # (## 4. Additional Infos)

[//]: # ()

[//]: # (TODO: )

[//]: # (- mention that for jdbc table function it is more performant &#40;not two queries each time&#41; to also specify the schema as a parameter)

[//]: # ()

[//]: # (- mention adhoc query vs table query, saved query, named query)

[//]: # ()

[//]: # (- mention insert into )

