---
sidebar_position: 10
sidebar_label: ClickHouseからClickHouse Cloudへの移行
slug: /ja/cloud/migration/clickhouse-to-cloud
---
import AddARemoteSystem from '@site/docs/ja/_snippets/_add_remote_ip_access_list_detail.md';

# セルフマネージドのClickHouseからClickHouse Cloudへの移行

<img src={require('./images/self-managed-01.png').default} class="image" alt="セルフマネージドのClickHouseの移行" style={{width: '80%', padding: '30px'}}/>

このガイドでは、セルフマネージドのClickHouseサーバーからClickHouse Cloudへの移行方法、およびClickHouse Cloudサービス間の移行方法を示します。`SELECT`および`INSERT`クエリで[`remoteSecure`](../../sql-reference/table-functions/remote.md)関数を使用すると、リモートのClickHouseサーバーへのアクセスが可能となり、テーブルの移行が`INSERT INTO`クエリに`SELECT`を埋め込むだけで簡単になります。

## セルフマネージドのClickHouseからClickHouse Cloudへの移行

<img src={require('./images/self-managed-02.png').default} class="image" alt="セルフマネージドのClickHouseの移行" style={{width: '30%', padding: '30px'}}/>

:::note
ソーステーブルがシャードされているかレプリケートされているかに関係なく、ClickHouse Cloudでは宛先テーブルを作成するだけです（このテーブルに対するエンジンパラメータは省略可能で、自動的にReplicatedMergeTreeテーブルになります）。ClickHouse Cloudは垂直および水平スケーリングを自動的に処理します。レプリケーションやシャードの管理について考える必要はありません。
:::

この例では、セルフマネージドのClickHouseサーバーが*ソース*であり、ClickHouse Cloudサービスが*宛先*です。

### 概要

プロセスは以下の通りです：

1. ソースサービスに読み取り専用ユーザーを追加
2. 宛先サービスにソーステーブル構造を複製
3. ソースから宛先にデータを取り込むか、ネットワークの可用性に応じてソースからデータをプッシュ
4. 宛先のIPアクセスリストからソースサーバーを削除 (該当する場合)
5. ソースサービスから読み取り専用ユーザーを削除

### システム間でのテーブル移行:
この例では、セルフマネージドのClickHouseサーバーからClickHouse Cloudに1つのテーブルを移行します。

### ソースClickHouseシステムの場合 (現在データがホストされているシステム)

- ソーステーブル (`db.table` という例) を読み取る権限を持つ読み取り専用ユーザーを追加
```sql
CREATE USER exporter
IDENTIFIED WITH SHA256_PASSWORD BY 'password-here'
SETTINGS readonly = 1;
```

```sql
GRANT SELECT ON db.table TO exporter;
```

- テーブル定義をコピー
```sql
select create_table_query
from system.tables
where database = 'db' and table = 'table'
```

### 宛先ClickHouse Cloudシステムで:

- 宛先データベースを作成：
```sql
CREATE DATABASE db
```

- ソースからのCREATE TABLE文を使用して宛先を作成。

:::tip
CREATE文を実行するとき、ENGINEをパラメータなしのReplicatedMergeTreeに変更します。ClickHouse Cloudは常にテーブルをレプリケートし、正しいパラメータを提供します。ただし、`ORDER BY`、`PRIMARY KEY`、`PARTITION BY`、`SAMPLE BY`、`TTL`、`SETTINGS`句は保持してください。
:::

```sql
CREATE TABLE db.table ...
```

- `remoteSecure` 関数を使用して、セルフマネージドのソースからデータを取得

<img src={require('./images/self-managed-03.png').default} class="image" alt="セルフマネージドのClickHouseの移行" style={{width: '30%', padding: '30px'}}/>

```sql
INSERT INTO db.table SELECT * FROM
remoteSecure('source-hostname', db, table, 'exporter', 'password-here')
```

:::note
ソースシステムが外部ネットワークから利用できない場合は、データをプルするのではなくプッシュすることができます。`remoteSecure` 関数はSELECTとINSERTの両方で機能します。次のオプションをご覧ください。
:::

- `remoteSecure` 関数を使用して、データをClickHouse Cloudサービスにプッシュ

<img src={require('./images/self-managed-04.png').default} class="image" alt="セルフマネージドのClickHouseの移行" style={{width: '30%', padding: '30px'}}/>

:::tip リモートシステムをClickHouse CloudサービスのIPアクセスリストに追加する
`remoteSecure` 関数がClickHouse Cloudサービスに接続できるように、リモートシステムのIPアドレスをIPアクセスリストで許可する必要があります。このチップの下の**IPアクセスリストの管理**を展開して詳細を確認してください。
:::

  <AddARemoteSystem />

```sql
INSERT INTO FUNCTION
remoteSecure('HOSTNAME.clickhouse.cloud:9440', 'db.table',
'default', 'PASS') SELECT * FROM db.table
```

## ClickHouse Cloudサービス間の移行

<img src={require('./images/self-managed-05.png').default} class="image" alt="セルフマネージドのClickHouseの移行" style={{width: '80%', padding: '30px'}}/>

ClickHouse Cloudサービス間でデータを移行する際の例:
- 復元したバックアップからデータを移行
- 開発サービスからステージングサービスへのデータのコピー (またはステージングから本番へ)

この例では、二つのClickHouse Cloudサービスがあり、それらは*ソース*と*宛先*と呼ばれます。データはソースから宛先にプルされます。プッシュすることもできますが、ここでは読み取り専用ユーザーを使用するプルを示します。

<img src={require('./images/self-managed-06.png').default} class="image" alt="セルフマネージドのClickHouseの移行" style={{width: '80%', padding: '30px'}}/>

移行にはいくつかのステップがあります：
1. 一つのClickHouse Cloudサービスを*ソース*として識別し、他方を*宛先*として識別
1. ソースサービスに読み取り専用ユーザーを追加
1. 宛先サービスにソーステーブル構造を複製
1. 一時的にソースサービスのIPアクセスを許可
1. ソースから宛先にデータをコピー
1. 宛先でIPアクセスリストを再設定
1. ソースサービスから読み取り専用ユーザーを削除

#### ソースサービスに読み取り専用ユーザーを追加

- ソーステーブル (`db.table` という例) を読み取る権限を持つ読み取り専用ユーザーを追加
  ```sql
  CREATE USER exporter
  IDENTIFIED WITH SHA256_PASSWORD BY 'password-here'
  SETTINGS readonly = 1;
  ```

  ```sql
  GRANT SELECT ON db.table TO exporter;
  ```

- テーブル定義をコピー
  ```sql
  select create_table_query
  from system.tables
  where database = 'db' and table = 'table'
  ```

#### 宛先サービスでテーブル構造を複製

宛先にデータベースが存在しない場合は、作成します：

- 宛先データベースを作成:
  ```sql
  CREATE DATABASE db
  ```

- ソースからのCREATE TABLE文を使用して宛先を作成。

  宛先でソースからの`select create_table_query...` の出力を使用してテーブルを作成します：

  ```sql
  CREATE TABLE db.table ...
  ```

#### ソースサービスへのリモートアクセスを許可

ソースから宛先へデータをプルするには、ソースサービスが接続を許可する必要があります。一時的にソースサービスの「IPアクセスリスト」機能を無効にします。

:::tip
ソースClickHouse Cloudサービスを引き続き使用する場合、データを移行する前に既存のIPアクセスリストをJSONファイルにエクスポートすると、移行後にアクセスリストをインポートできます。
:::

許可リストを変更し、一時的に**Anywhere**からのアクセスを許可します。[IPアクセスリスト](/docs/ja/cloud/security/setting-ip-filters)のドキュメントで詳細を確認してください。

#### ソースから宛先へのデータコピー

- `remoteSecure` 関数を使用して、ソースClickHouse Cloudサービスからデータをプルします
  宛先に接続します。このコマンドを宛先ClickHouse Cloudサービスで実行してください：

  ```sql
  INSERT INTO db.table SELECT * FROM
  remoteSecure('source-hostname', db, table, 'exporter', 'password-here')
  ```

- 宛先サービスでデータを確認

#### ソースでIPアクセスリストを再設定

  以前にアクセスリストをエクスポートしておいた場合は、**Share**を使用して再インポートできます。そうでない場合は、アクセスリストにエントリを再追加します。

#### 読み取り専用の`exporter`ユーザーを削除

```sql
DROP USER exporter
```

- サービスIPアクセスリストをスイッチしてアクセスを制限

