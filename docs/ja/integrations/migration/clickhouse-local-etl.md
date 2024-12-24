---
sidebar_label: clickhouse-localの使用
sidebar_position: 20
keywords: [clickhouse, 移行, マイグレーション, データ, etl, elt, clickhouse-local, clickhouse-client]
slug: '/ja/cloud/migration/clickhouse-local'
---

import Tabs from '@theme/Tabs';
import TabItem from '@theme/TabItem';
import CodeBlock from '@theme/CodeBlock';

import AddARemoteSystem from '@site/docs/ja/_snippets/_add_remote_ip_access_list_detail.md';


# clickhouse-localを使用したClickHouseへの移行

<img src={require('./images/ch-local-01.png').default} class="image" alt="セルフマネージド ClickHouse の移行" style={{width: '40%', padding: '30px'}}/>


ClickHouse、より具体的には[`clickhouse-local`](/docs/ja/operations/utilities/clickhouse-local.md)をETLツールとして使用し、現在のデータベースシステムからClickHouse Cloudにデータを移行することができます。これは、現在のデータベースシステムにClickHouseが提供する[インテグレーションエンジン](/docs/ja/engines/table-engines/#integration-engines)または[テーブル関数](/docs/ja/sql-reference/table-functions/)があるか、またはベンダーが提供するJDBCドライバまたはODBCドライバが利用可能である限りです。

この移行方法を「ピボット」方式と呼ぶこともあります。中間のピボットポイントまたはハブを使用して、ソースデータベースから宛先データベースにデータを移動するためです。たとえば、この方法は、セキュリティ要件のためにプライベートまたは内部ネットワークからの外向き接続のみが許可されている場合に必要になるかもしれません。そのため、clickhouse-localを使用してソースデータベースからデータを取得し、clickhouse-localがピボットポイントとして機能することで、データを宛先のClickHouseデータベースへプッシュする必要があります。

ClickHouseは、[MySQL](/docs/ja/engines/table-engines/integrations/mysql/)、[PostgreSQL](/docs/ja/engines/table-engines/integrations/postgresql)、[MongoDB](/docs/ja/engines/table-engines/integrations/mongodb)、そして[SQLite](/docs/ja/engines/table-engines/integrations/sqlite)のインテグレーションエンジンとテーブル関数（即座にインテグレーションエンジンを作成する）を提供しています。他のすべての一般的なデータベースシステムには、ベンダーから利用可能なJDBCドライバまたはODBCドライバがあります。

## clickhouse-localとは？

<img src={require('./images/ch-local-02.png').default} class="image" alt="セルフマネージド ClickHouse の移行" style={{width: '100%', padding: '30px'}}/>

通常、ClickHouseはクラスタ形式で実行され、いくつかのClickHouseデータベースエンジンのインスタンスが異なるサーバで分散的に実行されます。

シングルサーバでは、ClickHouseデータベースエンジンは`clickhouse-server`プログラムの一部として実行されます。データベースアクセス（パス、ユーザー、セキュリティなど）はサーバー構成ファイルで設定されます。

`clickhouse-local`ツールは、ClickHouseサーバーを設定したり起動したりすることなく、多くの入力と出力に対して超高速SQLデータ処理が可能な、コマンドラインユーティリティの形式でClickHouseデータベースエンジンを使用できるようにします。

## clickhouse-localのインストール

`clickhouse-local`を使用するには、現在のソースデータベースシステムとClickHouse Cloudターゲットサービスの両方にネットワークアクセスできるホストマシンが必要です。

そのホストマシンで、コンピュータのオペレーティングシステムに基づいて`clickhouse-local`の適切なビルドをダウンロードします:

<Tabs groupId="os">
<TabItem value="linux" label="Linux" >

1. `clickhouse-local`をローカルにダウンロードする最も簡単な方法は、次のコマンドを実行することです:
  ```bash
  curl https://clickhouse.com/ | sh
  ```

1. `clickhouse-local`を実行します（バージョンが表示されるだけです）:
  ```bash
  ./clickhouse-local
  ```

</TabItem>
<TabItem value="mac" label="macOS">

1. `clickhouse-local`をローカルにダウンロードする最も簡単な方法は、次のコマンドを実行することです:
  ```bash
  curl https://clickhouse.com/ | sh
  ```

1. `clickhouse-local`を実行します（バージョンが表示されるだけです）:
  ```bash
  ./clickhouse local
  ```

</TabItem>
</Tabs>

:::info 重要
このガイド全体での例は、`clickhouse-local`を実行するためにLinuxコマンド（`./clickhouse-local`）を使用しています。Macで`clickhouse-local`を実行する場合は、`./clickhouse local`を使用してください。
:::


:::tip リモートシステムをClickHouse CloudサービスのIPアクセスリストに追加する
`remoteSecure`関数がClickHouse Cloudサービスに接続するためには、リモートシステムのIPアドレスがIPアクセスリストで許可されている必要があります。このヒントの下にある**Manage your IP Access List**を展開して詳細を確認してください。
:::

  <AddARemoteSystem />

## 例1: MySQLからClickHouse Cloudへのインテグレーションエンジンによる移行

ソースのMySQLデータベースからデータを読み取るために[インテグレーションテーブルエンジン](/docs/ja/engines/table-engines/integrations/mysql/)（[mysqlテーブル関数](/docs/ja/sql-reference/table-functions/mysql/)によって即座に作成）を使用し、ClickHouseクラウドサービス上の宛先テーブルにデータを書き込むために[remoteSecureテーブル関数](/docs/ja/sql-reference/table-functions/remote/)を使用します。

<img src={require('./images/ch-local-03.png').default} class="image" alt="セルフマネージド ClickHouse の移行" style={{width: '40%', padding: '30px'}}/>


### 宛先ClickHouse Cloudサービス上で:

#### 宛先データベースを作成する:

  ```sql
  CREATE DATABASE db
  ```

#### MySQLテーブルと等価なスキーマを持つ宛先テーブルを作成する:

  ```sql
  CREATE TABLE db.table ...
  ```

:::note
ClickHouse Cloud宛先テーブルのスキーマとソースMySQLテーブルのスキーマは一致している必要があります（カラム名と順序が同じであり、カラムのデータ型が互換性がある必要があります）。
:::

### clickhouse-localホストマシンで:

#### マイグレーションクエリでclickhouse-localを実行する:

  ```sql
  ./clickhouse-local --query "
INSERT INTO FUNCTION
remoteSecure('HOSTNAME.clickhouse.cloud:9440', 'db.table', 'default', 'PASS')
SELECT * FROM mysql('host:port', 'database', 'table', 'user', 'password');"
  ```

:::note
データは`clickhouse-local`ホストマシンにローカルで保存されません。代わりに、データはソースMySQLテーブルから読み取られ、即座にClickHouse Cloudサービス上の宛先テーブルに書き込まれます。
:::


## 例2: MySQLからClickHouse CloudへのJDBCブリッジによる移行

ソースMySQLデータベースからデータを読み取るために[ClickHouse JDBC Bridge](https://github.com/ClickHouse/clickhouse-jdbc-bridge)およびMySQL JDBCドライバとともに[JDBCインテグレーションテーブルエンジン](/docs/ja/engines/table-engines/integrations/jdbc.md)（[jdbcテーブル関数](/docs/ja/sql-reference/table-functions/jdbc.md)によって即座に作成）を使用し、ClickHouseクラウドサービス上の宛先テーブルにデータを書き込むために[remoteSecureテーブル関数](/docs/ja/sql-reference/table-functions/remote.md)を使用します。

<img src={require('./images/ch-local-04.png').default} class="image" alt="セルフマネージド ClickHouse の移行" style={{width: '40%', padding: '30px'}}/>

### 宛先ClickHouse Cloudサービスで:

#### 宛先データベースを作成する:
  ```sql
  CREATE DATABASE db
  ```

#### MySQLテーブルと等価なスキーマを持つ宛先テーブルを作成する:

  ```sql
  CREATE TABLE db.table ...
  ```

:::note
ClickHouse Cloud宛先テーブルのスキーマとソースMySQLテーブルのスキーマは一致している必要があります。例えば、カラム名と順序が同じである必要があり、カラムのデータ型は互換性がある必要があります。
:::

### clickhouse-localホストマシンで:

#### ClickHouse JDBC Bridgeをローカルにインストール、設定、開始する:

[ガイド](/docs/ja/integrations/data-ingestion/dbms/jdbc-with-clickhouse.md#install-the-clickhouse-jdbc-bridge-locally)での手順に従ってください。このガイドにはMySQLからのデータソースを設定するための手順も含まれています。

#### マイグレーションクエリでclickhouse-localを実行する:

  ```sql
  ./clickhouse-local --query "
INSERT INTO FUNCTION
remoteSecure('HOSTNAME.clickhouse.cloud:9440', 'db.table', 'default', 'PASS')
SELECT * FROM jdbc('datasource', 'database', 'table');"
  ```

:::note
データは`clickhouse-local`ホストマシンにローカルで保存されません。代わりに、データはMySQLソーステーブルから読み取られ、即座にClickHouse Cloudサービス上の宛先テーブルに書き込まれます。
:::


