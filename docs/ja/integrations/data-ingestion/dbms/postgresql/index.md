---
slug: /ja/integrations/postgresql
title: PostgreSQLへの接続
keywords: [clickhouse, postgres, postgresql, connect, integrate, table, engine]
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

# ClickHouseからPostgreSQLへの接続

このページでは、PostgreSQLとClickHouseを統合するための次のオプションを取り上げます：
- セルフマネージドのClickHouseとClickHouse Cloudの両方に対して、PostgreSQLデータベースをレプリケーションするための特定のCDCツールである`PeerDB by ClickHouse`を使用する。
- PostgreSQLテーブルを読み取るための`PostgreSQL`テーブルエンジンを使用する。
- PostgreSQLのデータベースをClickHouseのデータベースと同期するためのエクスペリメンタルな`MaterializedPostgreSQL`データベースエンジンを使用する。

## PeerDB by ClickHouseを使用する
PeerDBはClickHouseと協力し、最速のPostgres CDCを提供しています！始めるには、[PeerDB Cloud](https://www.peerdb.io/) でアカウントを作成し、セットアップ手順については[ドキュメント](https://docs.peerdb.io/connect/clickhouse/clickhouse-cloud)を参照してください。

## PostgreSQLテーブルエンジンを使用する

`PostgreSQL`テーブルエンジンを使用すると、ClickHouseからリモートのPostgreSQLサーバー上のデータに対して**SELECT**および**INSERT**操作を行うことができます。
この記事では、1つのテーブルを使った基本的な統合方法を説明します。

### 1. PostgreSQLのセットアップ
1.  `postgresql.conf`で、PostgreSQLがネットワークインターフェースをリッスンするようにするため、次のエントリを追加します：
  ```
  listen_addresses = '*'
  ```

2. ClickHouseから接続するためのユーザーを作成します。デモ用に、フルのスーパーユーザー権限を付与します。
  ```sql
  CREATE ROLE clickhouse_user SUPERUSER LOGIN PASSWORD 'ClickHouse_123';
  ```

3. PostgreSQLで新しいデータベースを作成します：
  ```sql
  CREATE DATABASE db_in_psg;
  ```

4. 新しいテーブルを作成します：
  ```sql
  CREATE TABLE table1 (
      id         integer primary key,
      column1    varchar(10)
  );
  ```

5. テストのためにいくつかの行を追加します：
  ```sql
  INSERT INTO table1
    (id, column1)
  VALUES
    (1, 'abc'),
    (2, 'def');
  ```

6. 新しいユーザーを使用してのレプリケーションで、新しいデータベースへの接続を許可するようにPostgreSQLを設定するため、`pg_hba.conf`ファイルに次のエントリを追加します。PostgreSQLサーバーのサブネットかIPアドレスでアドレス行を更新します：
  ```
  # TYPE  DATABASE        USER            ADDRESS                 METHOD
  host    db_in_psg             clickhouse_user 192.168.1.0/24          password
  ```

7. `pg_hba.conf`設定をリロードします（バージョンに応じてコマンドを調整してください）：
  ```
  /usr/pgsql-12/bin/pg_ctl reload
  ```

8. 新しい`clickhouse_user`がログインできることを確認します：
  ```
  psql -U clickhouse_user -W -d db_in_psg -h <your_postgresql_host>
  ```

:::note
ClickHouse Cloudでこの機能を使用している場合、ClickHouse CloudのIPアドレスがPostgreSQLインスタンスにアクセスできるようにする必要があるかもしれません。
PostgreSQLの[Cloud Endpoints API](/docs/ja/cloud/security/cloud-endpoints-api.md)で外部トラフィックの詳細を確認してください。
:::

### 2. ClickHouseでのテーブル定義
1. `clickhouse-client`にログインします：
  ```
  clickhouse-client --user default --password ClickHouse123!
  ```

2. 新しいデータベースを作成します：
  ```sql
  CREATE DATABASE db_in_ch;
  ```

3. `PostgreSQL`を使用するテーブルを作成します：
  ```sql
  CREATE TABLE db_in_ch.table1
  (
      id UInt64,
      column1 String
  )
  ENGINE = PostgreSQL('postgres-host.domain.com:5432', 'db_in_psg', 'table1', 'clickhouse_user', 'ClickHouse_123');
  ```

  必要最小限のパラメータは次の通りです：

  |parameter|説明                 |例              |
  |---------|----------------------------|---------------------|
  |host:port|ホスト名またはIPとポート     |postgres-host.domain.com:5432|
  |database |PostgreSQLデータベース名         |db_in_psg                  |
  |user     |Postgresへの接続用のユーザー名|clickhouse_user     |
  |password |Postgresへの接続用のパスワード|ClickHouse_123       |

  :::note
  完全なパラメータリストについては、[PostgreSQLテーブルエンジン](../../../../engines/table-engines/integrations/postgresql.md)のドキュメントページを参照してください。
  :::

### 3 統合のテスト

1. ClickHouseで最初の行を表示します：
  ```sql
  SELECT * FROM db_in_ch.table1
  ```

  ClickHouseのテーブルは、既に存在するPostgreSQLのテーブル内の2つの行で自動的に埋められるはずです：
  ```response
  Query id: 34193d31-fe21-44ac-a182-36aaefbd78bf

  ┌─id─┬─column1─┐
  │  1 │ abc     │
  │  2 │ def     │
  └────┴─────────┘
  ```

2. PostgreSQLに戻り、テーブルにいくつかの行を追加します：
  ```sql
  INSERT INTO table1
    (id, column1)
  VALUES
    (3, 'ghi'),
    (4, 'jkl');
  ```

4. これらの新しい行がClickHouseのテーブルに表示されるはずです：
  ```sql
  SELECT * FROM db_in_ch.table1
  ```

  応答は次のようになります：
  ```response
  Query id: 86fa2c62-d320-4e47-b564-47ebf3d5d27b

  ┌─id─┬─column1─┐
  │  1 │ abc     │
  │  2 │ def     │
  │  3 │ ghi     │
  │  4 │ jkl     │
  └────┴─────────┘
  ```

5. ClickHouseテーブルに行を追加するとどうなるか見てみましょう：
  ```sql
  INSERT INTO db_in_ch.table1
    (id, column1)
  VALUES
    (5, 'mno'),
    (6, 'pqr');
  ```

6. ClickHouseで追加された行がPostgreSQLのテーブルに表示されるはずです：
  ```sql
  db_in_psg=# SELECT * FROM table1;
  id | column1
  ----+---------
    1 | abc
    2 | def
    3 | ghi
    4 | jkl
    5 | mno
    6 | pqr
  (6 rows)
  ```

この例では、`PostgreSQL`テーブルエンジンを使用したPostgreSQLとClickHouseの基本的な統合を示しました。
スキーマの指定、特定のカラムのみを返す、複数のレプリカへの接続などの機能については、[PostgreSQLテーブルエンジンのドキュメントページ](/docs/ja/engines/table-engines/integrations/postgresql.md)を参照してください。また、[ClickHouseとPostgreSQL - a match made in data heaven - part 1](https://clickhouse.com/blog/migrating-data-between-clickhouse-postgres)ブログもチェックしてください。

## MaterializedPostgreSQLデータベースエンジンを使用する

<CloudNotSupportedBadge />
<ExperimentalBadge />

PostgreSQLデータベースエンジンは、PostgreSQLのレプリケーション機能を使用して、データベースの全体またはスキーマとテーブルのサブセットを持つデータベースのレプリカを作成します。
この記事では、1つのデータベース、1つのスキーマ、および1つのテーブルを使用した基本的な統合方法を示します。

***以下の手順では、PostgreSQL CLI (psql) と ClickHouse CLI (clickhouse-client) を使用します。PostgreSQLサーバーはLinuxにインストールされています。以下は、新しいテストインストールされたPostgreSQLデータベースに対する最小設定です***

### 1. PostgreSQLで
1. `postgresql.conf`で、最小のリッスンレベル、レプリケーションWALレベル、およびレプリケーションスロットを設定します：

次のエントリを追加します：
```
listen_addresses = '*'
max_replication_slots = 10
wal_level = logical
```
_*ClickHouseは最低`logical` WALレベルと最低`2`レプリケーションスロットが必要です_

2. 管理者アカウントを使用して、ClickHouseから接続するためのユーザーを作成します：
```sql
CREATE ROLE clickhouse_user SUPERUSER LOGIN PASSWORD 'ClickHouse_123';
```
_*デモンストレーションのために、完全なスーパーユーザー権限が与えられています。_

3. 新しいデータベースを作成します：
```sql
CREATE DATABASE db1;
```

4. `psql`で新しいデータベースに接続します：
```
\connect db1
```

5. 新しいテーブルを作成します：
```sql
CREATE TABLE table1 (
    id         integer primary key,
    column1    varchar(10)
);
```

6. 初期行を追加します：
```sql
INSERT INTO table1
(id, column1)
VALUES
(1, 'abc'),
(2, 'def');
```

7. 新しいユーザーを使用してのレプリケーションで、PostgreSQLを新しいデータベースへの接続を許可するように設定します。
以下は、`pg_hba.conf`ファイルに追加する最小エントリです：
```
# TYPE  DATABASE        USER            ADDRESS                 METHOD
host    db1             clickhouse_user 192.168.1.0/24          password
```
_*デモンストレーションのために、クリアテキストパスワード認証メソッドを使用しています。PostgreSQLドキュメントに従ってアドレス行をサーバーのサブネットまたはアドレスで更新します_

8. `pg_hba.conf`設定を次のようにリロードします（バージョンに応じて調整してください）：
```
/usr/pgsql-12/bin/pg_ctl reload
```

9. 新しい`clickhouse_user`でログインをテストします：
```
 psql -U clickhouse_user -W -d db1 -h <your_postgresql_host>
```

### 2. ClickHouseで
1. ClickHouse CLIにログインします：
```
clickhouse-client --user default --password ClickHouse123!
```

2. データベースエンジンのPosgreSQLエクスペリメンタル機能を有効にします：
```sql
SET allow_experimental_database_materialized_postgresql=1
```

3. レプリケーションされる新しいデータベースを作成し、初期テーブルを定義します：
```sql
CREATE DATABASE db1_postgres
ENGINE = MaterializedPostgreSQL('postgres-host.domain.com:5432', 'db1', 'clickhouse_user', 'ClickHouse_123')
SETTINGS materialized_postgresql_tables_list = 'table1';
```
最小オプション：

|parameter|説明                 |例              |
|---------|----------------------------|---------------------|
|host:port|ホスト名またはIPとポート     |postgres-host.domain.com:5432|
|database |PostgreSQLデータベース名         |db1                  |
|user     |Postgresへの接続用のユーザー名|clickhouse_user     |
|password |Postgresへの接続用のパスワード|ClickHouse_123       |
|settings |エンジンの追加設定| materialized_postgresql_tables_list = 'table1'|

:::info
PostgreSQLデータベースエンジンの完全なガイドについては、<https://clickhouse.com/docs/ja/engines/database-engines/materialized-postgresql/#settings>を参照してください。
:::

4. 初期テーブルにデータがあるか確認します：

```sql
ch_env_2 :) select * from db1_postgres.table1;

SELECT *
FROM db1_postgres.table1

Query id: df2381ac-4e30-4535-b22e-8be3894aaafc

┌─id─┬─column1─┐
│  1 │ abc     │
└────┴─────────┘
┌─id─┬─column1─┐
│  2 │ def     │
└────┴─────────┘
```

### 3. 基本的なレプリケーションのテスト
1. PostgreSQLで、新しい行を追加します：
```sql
INSERT INTO table1
(id, column1)
VALUES
(3, 'ghi'),
(4, 'jkl');
```

2. ClickHouseで、新しい行が表示されることを確認します：
```sql
ch_env_2 :) select * from db1_postgres.table1;

SELECT *
FROM db1_postgres.table1

Query id: b0729816-3917-44d3-8d1a-fed912fb59ce

┌─id─┬─column1─┐
│  1 │ abc     │
└────┴─────────┘
┌─id─┬─column1─┐
│  4 │ jkl     │
└────┴─────────┘
┌─id─┬─column1─┐
│  3 │ ghi     │
└────┴─────────┘
┌─id─┬─column1─┐
│  2 │ def     │
└────┴─────────┘
```

### 4. まとめ
この統合ガイドは、テーブルを持つデータベースをレプリケートする方法のシンプルな例に焦点を当てていますが、データベース全体のレプリケーションや既存のレプリケーションに新しいテーブルやスキーマを追加するなど、より高度なオプションも存在します。このレプリケーションではDDLコマンドはサポートされていませんが、構造が変更された場合にテーブルを検出してリロードするようエンジンを設定できます。

:::info
高度なオプションで利用可能な機能の詳細については、リファレンスドキュメントを参照してください：<https://clickhouse.com/docs/ja/engines/database-engines/materialized-postgresql/>
:::

## 関連コンテンツ
- ブログ: [ClickHouseとPostgreSQL - a match made in data heaven - part 1](https://clickhouse.com/blog/migrating-data-between-clickhouse-postgres)
- ブログ: [ClickHouseとPostgreSQL - a Match Made in Data Heaven - part 2](https://clickhouse.com/blog/migrating-data-between-clickhouse-postgres-part-2)
