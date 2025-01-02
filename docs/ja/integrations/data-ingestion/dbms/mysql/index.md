---
sidebar_label: MySQL
sidebar_position: 10
slug: /ja/integrations/mysql
description: MySQLテーブルエンジンを使用すると、ClickHouseをMySQLに接続できます。
keywords: [clickhouse, mysql, 接続, 統合, テーブル, エンジン]
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';
import ExperimentalBadge from '@theme/badges/ExperimentalBadge';

# MySQLをClickHouseと統合

このページでは、MySQLをClickHouseと統合するための2つのオプションについて説明します：

- MySQLテーブルを読み取るための`MySQL`テーブルエンジンを使用する
- MySQLのデータベースとClickHouseのデータベースを同期するための`MaterializedMySQL`データベースエンジンを使用する

## MySQLテーブルエンジンを使用してClickHouseをMySQLに接続

`MySQL`テーブルエンジンを使用すると、ClickHouseをMySQLに接続できます。ClickHouseまたはMySQLのテーブルで**SELECT**および**INSERT**ステートメントを行うことができます。このセクションでは、`MySQL`テーブルエンジンを使用する基本的な方法を示します。

### 1. MySQLを設定

1. MySQLにデータベースを作成:
  ```sql
  CREATE DATABASE db1;
  ```

2. テーブルを作成:
  ```sql
  CREATE TABLE db1.table1 (
    id INT,
    column1 VARCHAR(255)
  );
  ```

3. サンプル行を挿入:
  ```sql
  INSERT INTO db1.table1
    (id, column1)
  VALUES
    (1, 'abc'),
    (2, 'def'),
    (3, 'ghi');
  ```

4. ClickHouseから接続するためのユーザーを作成:
  ```sql
  CREATE USER 'mysql_clickhouse'@'%' IDENTIFIED BY 'Password123!';
  ```

5. 必要に応じて権限を付与。（デモ目的で、`mysql_clickhouse`ユーザーには管理者権限が付与されています）
  ```sql
  GRANT ALL PRIVILEGES ON *.* TO 'mysql_clickhouse'@'%';
  ```

:::note
この機能をClickHouse Cloudで使用する場合、ClickHouse CloudのIPアドレスがMySQLインスタンスにアクセスすることを許可する必要があるかもしれません。次のリンクを確認してください：[Cloud Endpoints API](/docs/ja/cloud/security/cloud-endpoints-api.md)。
:::

### 2. ClickHouseにテーブルを定義

1. 次に、`MySQL`テーブルエンジンを使用するClickHouseテーブルを作成しましょう:
  ```sql
  CREATE TABLE mysql_table1 (
    id UInt64,
    column1 String
  )
  ENGINE = MySQL('mysql-host.domain.com','db1','table1','mysql_clickhouse','Password123!')
  ```

  必須パラメータは以下の通りです：

  |パラメータ|説明                |例                     |
  |---------|-----------------------------|---------------------|
  |host     |ホスト名またはIP             |mysql-host.domain.com|
  |database |mysqlデータベース名          |db1                  |
  |table    |mysqlテーブル名              |table1               |
  |user     |mysqlに接続するためのユーザー名|mysql_clickhouse     |
  |password |mysqlに接続するためのパスワード|Password123!         |

  :::note
  全パラメータのリストについては、[MySQLテーブルエンジン](/docs/ja/engines/table-engines/integrations/mysql.md)のドキュメントページをご覧ください。
  :::

### 3. 統合をテスト

1. MySQLでサンプル行を挿入:
  ```sql
  INSERT INTO db1.table1
    (id, column1)
  VALUES
    (4, 'jkl');
  ```

2. 既存のMySQLテーブルの行が新たに追加された行とともにClickHouseテーブルにあることを確認:
  ```sql
  SELECT
      id,
      column1
  FROM mysql_table1
  ```

  4行が表示されるはずです：
  ```response
  Query id: 6d590083-841e-4e95-8715-ef37d3e95197

  ┌─id─┬─column1─┐
  │  1 │ abc     │
  │  2 │ def     │
  │  3 │ ghi     │
  │  4 │ jkl     │
  └────┴─────────┘

  4 rows in set. Elapsed: 0.044 sec.
  ```

3. ClickHouseテーブルに行を追加しましょう：
  ```sql
  INSERT INTO mysql_table1
    (id, column1)
  VALUES
    (5,'mno')
  ```

4. 新しい行がMySQLに表示されることを確認：
  ```bash
  mysql> select id,column1 from db1.table1;
  ```

  新しい行が表示されるはずです：
  ```response
  +------+---------+
  | id   | column1 |
  +------+---------+
  |    1 | abc     |
  |    2 | def     |
  |    3 | ghi     |
  |    4 | jkl     |
  |    5 | mno     |
  +------+---------+
  5 rows in set (0.01 sec)
  ```

### まとめ

`MySQL`テーブルエンジンを使用すると、データを相互に交換するためにClickHouseをMySQLに接続できます。詳細については、[MySQLテーブルエンジン](/docs/ja/sql-reference/table-functions/mysql.md)のドキュメントページを確認してください。

## ClickHouseでMySQLデータベースを複製

<CloudNotSupportedBadge />
<ExperimentalBadge />

`MaterializedMySQL`データベースエンジンを使用すると、MySQLデータベース内の既存のすべてのテーブルとそれらのテーブル内のすべてのデータを含むデータベースをClickHouse内に定義できます。MySQL側ではDDLおよびDML操作を引き続き行うことができ、ClickHouseは変更を検出し、MySQLデータベースのレプリカとして機能します。

このセクションでは、このレプリケーションを実装するためにMySQLとClickHouseを設定する方法を示します。

### 1. MySQLを設定

1. レプリケーションとネイティブ認証を許可するようにMySQLデータベースを設定します。ClickHouseはネイティブパスワード認証のみをサポートしています。`/etc/my.cnf`に次のエントリを追加してください：
  ```
  default_authentication_plugin = mysql_native_password
  gtid_mode = ON
  enforce_gtid_consistency = ON
  ```

2. ClickHouseから接続するユーザーを作成：
  ```sql
  CREATE USER clickhouse_user IDENTIFIED BY 'ClickHouse_123';
  ```

3. 新しいユーザーに必要な権限を付与します。デモ目的で、ここでは完全な管理者権限が付与されています：
  ```sql
  GRANT ALL PRIVILEGES ON *.* TO 'clickhouse_user'@'%';
  ```

  :::note
  MySQLユーザーに必要な最小限の権限は**RELOAD**、**REPLICATION SLAVE**、**REPLICATION CLIENT**、および**SELECT PRIVILEGE**です。
  :::

4.  MySQLにデータベースを作成:
  ```sql
  CREATE DATABASE db1;
  ```

5. テーブルを作成:
  ```sql
  CREATE TABLE db1.table_1 (
      id INT,
      column1 VARCHAR(10),
      PRIMARY KEY (`id`)
  ) ENGINE = InnoDB;
  ```

6. サンプル行を挿入:
  ```sql
  INSERT INTO db1.table_1
    (id, column1)
  VALUES
    (1, 'abc'),
    (2, 'def'),
    (3, 'ghi');
  ```

### 2. ClickHouseを設定

1. エクスペリメンタルな機能を使用するためのパラメータを設定:
  ```sql
  set allow_experimental_database_materialized_mysql = 1;
  ```

2. `MaterializedMySQL`データベースエンジンを使用するデータベースを作成:
  ```sql
  CREATE DATABASE db1_mysql
  ENGINE = MaterializedMySQL(
    'mysql-host.domain.com:3306',
    'db1',
    'clickhouse_user',
    'ClickHouse_123'
  );
  ```

  必須パラメータは以下の通りです：

  |パラメータ|説明                          |例                     |
  |---------|------------------------------|---------------------|
  |host:port|ホスト名またはIPとポート        |mysql-host.domain.com|
  |database |mysqlデータベース名            |db1                  |
  |user     |mysqlに接続するためのユーザー名|clickhouse_user    |
  |password |mysqlに接続するためのパスワード|ClickHouse_123       |

  :::note
  全パラメータのリストについては、[MaterializedMySQLデータベースエンジン](/docs/ja/engines/database-engines/materialized-mysql.md)のドキュメントページをご覧ください。
  :::

### 3. 統合をテスト

1. MySQLでサンプル行を挿入:
  ```sql
  INSERT INTO db1.table_1
    (id, column1)
  VALUES
    (4, 'jkl');
  ```

2. 新しい行がClickHouseテーブルに表示されることを確認：
  ```sql
  SELECT
      id,
      column1
  FROM db1_mysql.table_1
  ```

  応答は次のようになります：
  ```response
  Query id: d61a5840-63ca-4a3d-8fac-c93235985654

  ┌─id─┬─column1─┐
  │  1 │ abc     │
  └────┴─────────┘
  ┌─id─┬─column1─┐
  │  4 │ jkl     │
  └────┴─────────┘
  ┌─id─┬─column1─┐
  │  2 │ def     │
  └────┴─────────┘
  ┌─id─┬─column1─┐
  │  3 │ ghi     │
  └────┴─────────┘

  4 rows in set. Elapsed: 0.030 sec.
  ```

3. MySQLのテーブルが変更されたと仮定します。MySQLの`db1.table_1`にカラムを追加してみましょう：
  ```sql
  alter table db1.table_1 add column column2 varchar(10) after column1;
  ```

4. 変更されたテーブルに行を挿入しましょう：
  ```sql
  INSERT INTO db1.table_1
    (id, column1, column2)
  VALUES
    (5, 'mno', 'pqr');
  ```

5. ClickHouseのテーブルにも新しいカラムと新しい行が表示されることを確認：

  ```sql
  SELECT
      id,
      column1,
      column2
  FROM db1_mysql.table_1
  ```

  以前の行は`column2`に`NULL`が表示されます：
  ```response
  Query id: 2c32fd15-3c83-480b-9bfc-cba5d932d674

  Connecting to localhost:9000 as user default.
  Connected to ClickHouse server version 22.2.2 revision 54455.

  ┌─id─┬─column1─┬─column2─┐
  │  3 │ ghi     │ ᴺᵁᴸᴸ    │
  └────┴─────────┴─────────┘
  ┌─id─┬─column1─┬─column2─┐
  │  2 │ def     │ ᴺᵁᴸᴸ    │
  └────┴─────────┴─────────┘
  ┌─id─┬─column1─┬─column2─┐
  │  1 │ abc     │ ᴺᵁᴸᴸ    │
  │  5 │ mno     │ pqr     │
  └────┴─────────┴─────────┘
  ┌─id─┬─column1─┬─column2─┐
  │  4 │ jkl     │ ᴺᵁᴸᴸ    │
  └────┴─────────┴─────────┘

  5 rows in set. Elapsed: 0.017 sec.
  ```

### まとめ

これで完了です！`MaterializedMySQL`データベースエンジンは、MySQLデータベースをClickHouseで同期します。詳細と制限については、[MaterializedMySQLドキュメントページ](../../../../engines/database-engines/materialized-mysql.md)を確認してください。
