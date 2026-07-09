---
description: 'GRANT ステートメントのリファレンス'
sidebar_label: 'GRANT'
sidebar_position: 38
slug: /sql-reference/statements/grant
title: 'GRANT ステートメント'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="grant-statement">
  # GRANT ステートメント
</div>

* ClickHouse のユーザーアカウントまたはロールに[権限](#privileges)を付与します。
* ユーザーアカウントまたは他のロールにロールを割り当てます。

権限を取り消すには、[REVOKE](../../sql-reference/statements/revoke.md)ステートメントを使用します。付与されている権限は、[SHOW GRANTS](../../sql-reference/statements/show.md#show-grants)ステートメントを使って一覧表示することもできます。

<div id="granting-privilege-syntax">
  ## 権限付与の構文
</div>

```sql
GRANT [ON CLUSTER cluster_name] privilege[(column_name [,...])] [,...] ON {db.table[*]|db[*].*|*.*|table[*]|*} TO {user | role | CURRENT_USER} [,...] [WITH GRANT OPTION] [WITH REPLACE OPTION]
```

* `privilege` — 権限の種類。
* `role` — ClickHouse ユーザーロール。
* `user` — ClickHouse のユーザーアカウント。

`WITH GRANT OPTION` 句は、`user` または `role` に `GRANT` クエリを実行する権限を付与します。ユーザーは、自身が持つものと同じスコープ、またはそれより狭いスコープの権限を付与できます。
`WITH REPLACE OPTION` 句は、`user` または `role` の既存の権限を新しい権限に置き換えます。指定しない場合は、権限が追加されます。

<div id="assigning-role-syntax">
  ## ロール割り当ての構文
</div>

```sql
GRANT [ON CLUSTER cluster_name] role [,...] TO {user | another_role | CURRENT_USER} [,...] [WITH ADMIN OPTION] [WITH REPLACE OPTION]
```

* `role` — ClickHouse ユーザーロール。
* `user` — ClickHouse ユーザーアカウント。

`WITH ADMIN OPTION` 句は、`user` または `role` に [ADMIN OPTION](#admin-option) 権限を付与します。
`WITH REPLACE OPTION` 句は、`user` または `role` に対して既存のロールを新しいロールに置き換えます。指定しない場合は、ロールが追加されます。

<div id="grant-current-grants-syntax">
  ## GRANT CURRENT GRANTS 構文
</div>

```sql
GRANT CURRENT GRANTS{(privilege[(column_name [,...])] [,...] ON {db.table|db.*|*.*|table|*}) | ON {db.table|db.*|*.*|table|*}} TO {user | role | CURRENT_USER} [,...] [WITH GRANT OPTION] [WITH REPLACE OPTION]
```

* `privilege` — 権限の種類。
* `role` — ClickHouse のユーザーロール。
* `user` — ClickHouse のユーザーアカウント。

`CURRENT GRANTS` ステートメントを使用すると、指定したすべての権限を、指定したユーザーまたはロールに付与できます。
権限を 1 つも指定しなかった場合、指定したユーザーまたはロールには `CURRENT_USER` で利用可能なすべての権限が付与されます。

<div id="usage">
  ## 使用方法
</div>

`GRANT` を使用するには、アカウントに `GRANT OPTION` 権限が必要です。権限を付与できるのは、自分のアカウント権限の範囲内に限られます。

たとえば、administrator は次のクエリを使用して `john` アカウントに権限を付与しています。

```sql
GRANT SELECT(x,y) ON db.table TO john WITH GRANT OPTION
```

これは、`john` に次を実行する権限があることを意味します。

* `SELECT x,y FROM db.table`.
* `SELECT x FROM db.table`.
* `SELECT y FROM db.table`.

`john` は `SELECT z FROM db.table` を実行できません。`SELECT * FROM db.table` も実行できません。このクエリを処理する場合、ClickHouse は `x` と `y` についてもデータを返しません。唯一の例外は、テーブルに `x` と `y` のカラムしか含まれていない場合です。この場合、ClickHouse はすべてのデータを返します。

また、`john` には `GRANT OPTION` 権限もあるため、同じスコープまたはそれより小さいスコープの権限を他のユーザーに付与できます。

`system` データベースへのアクセスは常に許可されます (このデータベースはクエリの処理に使用されるためです) 。

:::note
新しいユーザーがデフォルトでアクセスできるシステムテーブルは多数ありますが、権限の付与なしにすべてのシステムテーブルへデフォルトでアクセスできるとは限りません。
さらに、`system.zookeeper` など一部のシステムテーブルへのアクセスは、セキュリティ上の理由から Cloud ユーザーには制限されています。
:::

1 つのクエリで、複数のアカウントに複数の権限を付与できます。クエリ `GRANT SELECT, INSERT ON *.* TO john, robin` により、アカウント `john` と `robin` は、server 上のすべてのデータベース内のすべてのテーブルに対して `INSERT` クエリと `SELECT` クエリを実行できます。

<div id="wildcard-grants">
  ## ワイルドカードによる権限付与
</div>

権限を指定する際は、テーブル名またはデータベース名の代わりにアスタリスク (`*`) を使用できます。たとえば、`GRANT SELECT ON db.* TO john` というクエリでは、`john` は `db` データベース内のすべてのテーブルに対して `SELECT` クエリを実行できます。
また、データベース名を省略することもできます。この場合、権限は現在のデータベースに対して付与されます。
たとえば、`GRANT SELECT ON * TO john` は現在のデータベース内のすべてのテーブルに対する権限を付与し、`GRANT SELECT ON mytable TO john` は現在のデータベース内の `mytable` テーブルに対する権限を付与します。

:::note
以下で説明する機能は、ClickHouse バージョン 24.10 以降で利用できます。
:::

テーブル名またはデータベース名の末尾にアスタリスクを付けることもできます。この機能を使うと、テーブルのパスの抽象的なプレフィックスに対して権限を付与できます。
例: `GRANT SELECT ON db.my_tables* TO john`。このクエリでは、`john` は `db` データベース内の、プレフィックス `my_tables*` を持つすべてのテーブルに対して `SELECT` クエリを実行できます。

その他の例:

`GRANT SELECT ON db.my_tables* TO john`

```sql
SELECT * FROM db.my_tables -- granted
SELECT * FROM db.my_tables_0 -- granted
SELECT * FROM db.my_tables_1 -- granted

SELECT * FROM db.other_table -- not_granted
SELECT * FROM db2.my_tables -- not_granted
```

`GRANT SELECT ON db*.* TO john`

```sql
SELECT * FROM db.my_tables -- granted
SELECT * FROM db.my_tables_0 -- granted
SELECT * FROM db.my_tables_1 -- granted
SELECT * FROM db.other_table -- granted
SELECT * FROM db2.my_tables -- granted
```

権限が付与されたパス内で新しく作成されたすべてのテーブルは、自動的に親からすべての権限を継承します。
たとえば、`GRANT SELECT ON db.* TO john`クエリを実行したあとに新しいテーブル`db.new_table`を作成すると、ユーザー`john`は`SELECT * FROM db.new_table`クエリを実行できるようになります。

アスタリスクを指定できるのは、プレフィックス**のみ**です:

```sql
GRANT SELECT ON db.* TO john -- correct
GRANT SELECT ON db*.* TO john -- correct

GRANT SELECT ON *.my_table TO john -- wrong
GRANT SELECT ON foo*bar TO john -- wrong
GRANT SELECT ON *suffix TO john -- wrong
GRANT SELECT(foo) ON db.table* TO john -- wrong
```

<div id="privileges">
  ## 権限
</div>

権限とは、特定の種類のクエリを実行できるようにユーザーに付与される許可のことです。

権限には階層構造があり、許可されるクエリの集合は権限のスコープによって決まります。

ClickHouse における権限の階層は以下のとおりです。

* [`ALL`](#all)
  * [`アクセス管理`](#access-management)
    * `ALLOW SQL SECURITY NONE`
    * `ALTER QUOTA`
    * `ALTER ROLE`
    * `ALTER ROW POLICY`
    * `ALTER SETTINGS PROFILE`
    * `ALTER USER`
    * `CREATE QUOTA`
    * `CREATE ROLE`
    * `CREATE ROW POLICY`
    * `CREATE SETTINGS PROFILE`
    * `CREATE USER`
    * `DROP QUOTA`
    * `DROP ROLE`
    * `DROP ROW POLICY`
    * `DROP SETTINGS PROFILE`
    * `DROP USER`
    * `ROLE ADMIN`
    * `SHOW ACCESS`
      * `SHOW QUOTAS`
      * `SHOW ROLES`
      * `SHOW ROW POLICIES`
      * `SHOW SETTINGS PROFILES`
      * `SHOW USERS`
  * [`ALTER`](#alter)
    * `ALTER DATABASE`
      * `ALTER DATABASE SETTINGS`
    * `ALTER TABLE`
      * `ALTER COLUMN`
        * `ALTER ADD COLUMN`
        * `ALTER CLEAR COLUMN`
        * `ALTER COMMENT COLUMN`
        * `ALTER DROP COLUMN`
        * `ALTER MATERIALIZE COLUMN`
        * `ALTER MODIFY COLUMN`
        * `ALTER RENAME COLUMN`
      * `ALTER CONSTRAINT`
        * `ALTER ADD CONSTRAINT`
        * `ALTER DROP CONSTRAINT`
        * `ALTER MODIFY CONSTRAINT`
      * `ALTER DELETE`
      * `ALTER FETCH PARTITION`
      * `ALTER FREEZE PARTITION`
      * `ALTER INDEX`
        * `ALTER ADD INDEX`
        * `ALTER CLEAR INDEX`
        * `ALTER DROP INDEX`
        * `ALTER MATERIALIZE INDEX`
        * `ALTER ORDER BY`
        * `ALTER SAMPLE BY`
      * `ALTER MATERIALIZE TTL`
      * `ALTER MODIFY COMMENT`
      * `ALTER MOVE PARTITION`
      * `ALTER PROJECTION`
      * `ALTER SETTINGS`
      * `ALTER STATISTICS`
        * `ALTER ADD STATISTICS`
        * `ALTER DROP STATISTICS`
        * `ALTER MATERIALIZE STATISTICS`
        * `ALTER MODIFY STATISTICS`
      * `ALTER TTL`
      * `ALTER UPDATE`
      * `ALTER TABLE EXECUTE`
    * `ALTER VIEW`
      * `ALTER VIEW MODIFY QUERY`
      * `ALTER VIEW REFRESH`
      * `ALTER VIEW MODIFY SQL SECURITY`
  * [`BACKUP`](#backup)
  * [`CLUSTER`](#cluster)
  * [`CREATE`](#create)
    * `CREATE ARBITRARY TEMPORARY TABLE`
      * `CREATE TEMPORARY TABLE`
    * `CREATE DATABASE`
    * `CREATE DICTIONARY`
    * `CREATE FUNCTION`
    * `CREATE RESOURCE`
    * `CREATE TABLE`
    * `CREATE VIEW`
    * `CREATE WORKLOAD`
  * [`dictGet`](#dictget)
  * [`displaySecretsInShowAndSelect`](#displaysecretsinshowandselect)
  * [`DROP`](#drop)
    * `DROP DATABASE`
    * `DROP DICTIONARY`
    * `DROP FUNCTION`
    * `DROP RESOURCE`
    * `DROP TABLE`
    * `DROP VIEW`
    * `DROP WORKLOAD`
  * [`INSERT`](#insert)
  * [`INTROSPECTION`](#introspection)
    * `addressToLine`
    * `addressToLineWithInlines`
    * `addressToSymbol`
    * `demangle`
  * `KILL QUERY`
  * `KILL TRANSACTION`
  * `MOVE PARTITION BETWEEN SHARDS`
  * [`NAMED COLLECTION ADMIN`](#named-collection-admin)
    * `ALTER NAMED COLLECTION`
    * `CREATE NAMED COLLECTION`
    * `DROP NAMED COLLECTION`
    * `NAMED COLLECTION`
    * `SHOW NAMED COLLECTIONS`
    * `SHOW NAMED COLLECTIONS SECRETS`
  * [`OPTIMIZE`](#optimize)
  * [`SELECT`](#select)
  * [`SET DEFINER`](/ja/sql-reference/statements/create/view#sql_security)
  * [`SHOW`](#show)
    * `SHOW COLUMNS`
    * `SHOW DATABASES`
    * `SHOW DICTIONARIES`
    * `SHOW TABLES`
  * `SHOW FILESYSTEM CACHES`
  * [`SOURCES`](#sources)
    * `AZURE`
    * `FILE`
    * `HDFS`
    * `HIVE`
    * `JDBC`
    * `KAFKA`
    * `MONGO`
    * `MYSQL`
    * `NATS`
    * `ODBC`
    * `POSTGRES`
    * `RABBITMQ`
    * `REDIS`
    * `REMOTE`
    * `S3`
    * `SQLITE`
    * `URL`
  * [`SYSTEM`](#system)
    * `SYSTEM CLEANUP`
    * `SYSTEM DROP CACHE`
      * `SYSTEM DROP COMPILED EXPRESSION CACHE`
      * `SYSTEM DROP CONNECTIONS CACHE`
      * `SYSTEM DROP DISTRIBUTED CACHE`
      * `SYSTEM DROP DNS CACHE`
      * `SYSTEM DROP FILESYSTEM CACHE`
      * `SYSTEM DROP FORMAT SCHEMA CACHE`
      * `SYSTEM DROP MARK CACHE`
      * `SYSTEM DROP MMAP CACHE`
      * `SYSTEM DROP PAGE CACHE`
      * `SYSTEM DROP PRIMARY INDEX CACHE`
      * `SYSTEM DROP QUERY CACHE`
      * `SYSTEM DROP S3 CLIENT CACHE`
      * `SYSTEM DROP SCHEMA CACHE`
      * `SYSTEM DROP UNCOMPRESSED CACHE`
    * `SYSTEM DROP PRIMARY INDEX CACHE`
    * `SYSTEM DROP REPLICA`
    * `SYSTEM FAILPOINT`
    * `SYSTEM FETCHES`
    * `SYSTEM FLUSH`
      * `SYSTEM FLUSH ASYNC INSERT QUEUE`
      * `SYSTEM FLUSH LOGS`
    * `SYSTEM JEMALLOC`
    * `SYSTEM KILL QUERY`
    * `SYSTEM KILL TRANSACTION`
    * `SYSTEM LISTEN`
    * `SYSTEM LOAD PRIMARY KEY`
    * `SYSTEM MERGES`
    * `SYSTEM MOVES`
    * `SYSTEM PULLING REPLICATION LOG`
    * `SYSTEM REDUCE BLOCKING PARTS`
    * `SYSTEM REPLICATION QUEUES`
    * `SYSTEM REPLICA READINESS`
    * `SYSTEM RESET DDL WORKER`
    * `SYSTEM RESTART DISK`
    * `SYSTEM RESTART REPLICA`
    * `SYSTEM RESTORE REPLICA`
    * `SYSTEM RELOAD`
      * `SYSTEM RELOAD ASYNCHRONOUS METRICS`
      * `SYSTEM RELOAD CONFIG`
        * `SYSTEM RELOAD DICTIONARY`
        * `SYSTEM RELOAD EMBEDDED DICTIONARIES`
        * `SYSTEM RELOAD FUNCTION`
        * `SYSTEM RELOAD MODEL`
        * `SYSTEM RELOAD USERS`
    * `SYSTEM SENDS`
      * `SYSTEM DISTRIBUTED SENDS`
      * `SYSTEM REPLICATED SENDS`
    * `SYSTEM SHUTDOWN`
    * `SYSTEM SYNC DATABASE REPLICA`
    * `SYSTEM SYNC FILE CACHE`
    * `SYSTEM SYNC FILESYSTEM CACHE`
    * `SYSTEM SYNC REPLICA`
    * `SYSTEM SYNC TRANSACTION LOG`
    * `SYSTEM THREAD FUZZER`
    * `SYSTEM TTL MERGES`
    * `SYSTEM UNFREEZE`
    * `SYSTEM UNLOAD PRIMARY KEY`
    * `SYSTEM VIEWS`
    * `SYSTEM VIRTUAL PARTS UPDATE`
    * `SYSTEM WAIT LOADING PARTS`
  * [`TABLE ENGINE`](#table-engine)
  * [`TRUNCATE`](#truncate)
  * `UNDROP TABLE`
* [`NONE`](#none)

この階層がどのように扱われるかの例:

* `ALTER` 権限には、他のすべての `ALTER*` 権限が含まれます。
* `ALTER CONSTRAINT` には、`ALTER ADD CONSTRAINT`、`ALTER DROP CONSTRAINT`、`ALTER MODIFY CONSTRAINT` の各権限が含まれます。

権限は異なるレベルで適用されます。レベルを把握しておくと、その権限に対して使用できる構文がわかります。

レベル (低いものから高いものへ) :

* `COLUMN` — 権限はカラム、テーブル、データベース、またはグローバルに付与できます。
* `TABLE` — 権限はテーブル、データベース、またはグローバルに付与できます。
* `VIEW` — 権限はビュー、データベース、またはグローバルに付与できます。
* `DICTIONARY` — 権限は Dictionary、データベース、またはグローバルに付与できます。
* `DATABASE` — 権限はデータベースまたはグローバルに付与できます。
* `GLOBAL` — 権限はグローバルにのみ付与できます。
* `GROUP` — 異なるレベルの権限をグループ化します。`GROUP` レベルの権限が付与されると、使用した構文に対応するグループ内の権限のみが付与されます。

許可される構文の例:

* `GRANT SELECT(x) ON db.table TO user`
* `GRANT SELECT ON db.* TO user`

許可されない構文の例:

* `GRANT CREATE USER(x) ON db.table TO user`
* `GRANT CREATE USER ON db.* TO user`

特別な権限 [ALL](#all) は、ユーザーアカウントまたはロールにすべての権限を付与します。

デフォルトでは、ユーザーアカウントまたはロールには権限がありません。

ユーザーまたはロールに権限がない場合は、[NONE](#none) 権限として表示されます。

一部のクエリは実装上、複数の権限を必要とします。たとえば、[RENAME](../../sql-reference/statements/optimize.md) クエリを実行するには、`SELECT`、`CREATE TABLE`、`INSERT`、`DROP TABLE` の各権限が必要です。

<div id="select">
  ### SELECT
</div>

[SELECT](../../sql-reference/statements/select/index.md) クエリの実行を許可します。

権限レベル: `COLUMN`。

**説明**

この権限が付与されたユーザーは、指定したデータベース内の指定したテーブルにおいて、指定したカラムの一覧に対して `SELECT` クエリを実行できます。ユーザーが指定外のカラムを含めた場合、クエリはデータを返しません。

次の権限について考えてみましょう。

```sql
GRANT SELECT(x,y) ON db.table TO john
```

この権限により、`john` は `db.table` の `x` および/または `y` カラムのデータを含む任意の `SELECT` クエリ (たとえば `SELECT x FROM db.table`) を実行できます。`john` は `SELECT z FROM db.table` を実行できません。`SELECT * FROM db.table` も実行できません。このクエリを処理する場合、ClickHouse は `x` と `y` を含め、どのデータも返しません。唯一の例外は、テーブルに `x` と `y` のカラムしかない場合で、このとき ClickHouse はすべてのデータを返します。

<div id="insert">
  ### INSERT
</div>

[INSERT](../../sql-reference/statements/insert-into.md) クエリの実行を許可します。

権限レベル: `COLUMN`。

**説明**

この権限が付与されたユーザーは、指定したデータベースおよびテーブルで、指定したカラムの一覧に対して `INSERT` クエリを実行できます。指定されたもの以外のカラムをユーザーが含めた場合、そのクエリではデータは挿入されません。

**例**

```sql
GRANT INSERT(x,y) ON db.table TO john
```

付与された権限により、`john` は `db.table` の `x` カラムおよび/または `y` カラムにデータを挿入できます。

<div id="alter">
  ### ALTER
</div>

以下の権限階層に従って、[ALTER](../../sql-reference/statements/alter/index.md)クエリの実行を許可します。

* `ALTER`。レベル: `COLUMN`。
  * `ALTER TABLE`。レベル: `GROUP`
  * `ALTER UPDATE`。レベル: `COLUMN`。別名: `UPDATE`
  * `ALTER DELETE`。レベル: `COLUMN`。別名: `DELETE`
  * `ALTER COLUMN`。レベル: `GROUP`
  * `ALTER ADD COLUMN`。レベル: `COLUMN`。別名: `ADD COLUMN`
  * `ALTER DROP COLUMN`。レベル: `COLUMN`。別名: `DROP COLUMN`
  * `ALTER MODIFY COLUMN`。レベル: `COLUMN`。別名: `MODIFY COLUMN`
  * `ALTER COMMENT COLUMN`。レベル: `COLUMN`。別名: `COMMENT COLUMN`
  * `ALTER CLEAR COLUMN`。レベル: `COLUMN`。別名: `CLEAR COLUMN`
  * `ALTER RENAME COLUMN`。レベル: `COLUMN`。別名: `RENAME COLUMN`
  * `ALTER INDEX`。レベル: `GROUP`。別名: `INDEX`
  * `ALTER ORDER BY`。レベル: `TABLE`。別名: `ALTER MODIFY ORDER BY`, `MODIFY ORDER BY`
  * `ALTER SAMPLE BY`。レベル: `TABLE`。別名: `ALTER MODIFY SAMPLE BY`, `MODIFY SAMPLE BY`
  * `ALTER ADD INDEX`。レベル: `TABLE`。別名: `ADD INDEX`
  * `ALTER DROP INDEX`。レベル: `TABLE`。別名: `DROP INDEX`
  * `ALTER MATERIALIZE INDEX`。レベル: `TABLE`。別名: `MATERIALIZE INDEX`
  * `ALTER CLEAR INDEX`。レベル: `TABLE`。別名: `CLEAR INDEX`
  * `ALTER CONSTRAINT`。レベル: `GROUP`。別名: `CONSTRAINT`
  * `ALTER ADD CONSTRAINT`。レベル: `TABLE`。別名: `ADD CONSTRAINT`
  * `ALTER DROP CONSTRAINT`。レベル: `TABLE`。別名: `DROP CONSTRAINT`
  * `ALTER MODIFY CONSTRAINT`。レベル: `TABLE`。別名: `MODIFY CONSTRAINT`
  * `ALTER TTL`。レベル: `TABLE`。別名: `ALTER MODIFY TTL`, `MODIFY TTL`
  * `ALTER MATERIALIZE TTL`。レベル: `TABLE`。別名: `MATERIALIZE TTL`
  * `ALTER SETTINGS`。レベル: `TABLE`。別名: `ALTER SETTING`, `ALTER MODIFY SETTING`, `MODIFY SETTING`
  * `ALTER MOVE PARTITION`。レベル: `TABLE`。別名: `ALTER MOVE PART`, `MOVE PARTITION`, `MOVE PART`
  * `ALTER FETCH PARTITION`。レベル: `TABLE`。別名: `ALTER FETCH PART`, `FETCH PARTITION`, `FETCH PART`
  * `ALTER FREEZE PARTITION`。レベル: `TABLE`。別名: `FREEZE PARTITION`
  * `ALTER EXECUTE`。レベル: `TABLE`。別名: `ALTER TABLE EXECUTE`
  * `ALTER VIEW`。レベル: `GROUP`
  * `ALTER VIEW REFRESH`。レベル: `VIEW`。別名: `REFRESH VIEW`
  * `ALTER VIEW MODIFY QUERY`。レベル: `VIEW`。別名: `ALTER TABLE MODIFY QUERY`
  * `ALTER VIEW MODIFY SQL SECURITY`。レベル: `VIEW`。別名: `ALTER TABLE MODIFY SQL SECURITY`

この階層がどのように扱われるかの例:

* `ALTER` 権限には、他のすべての `ALTER*` 権限が含まれます。
* `ALTER CONSTRAINT` には、`ALTER ADD CONSTRAINT`、`ALTER DROP CONSTRAINT`、`ALTER MODIFY CONSTRAINT` の各権限が含まれます。

**注記**

* `MODIFY SETTING` 権限では、table engineの設定を変更できます。settings やサーバーの設定パラメーターには影響しません。
* `ATTACH` 操作には [CREATE](#create) 権限が必要です。
* `DETACH` 操作には [DROP](#drop) 権限が必要です。
* [KILL MUTATION](../../sql-reference/statements/kill.md#kill-mutation)クエリでmutationを停止するには、そのmutationを開始するための権限が必要です。たとえば、`ALTER UPDATE` クエリを停止するには、`ALTER UPDATE`、`ALTER TABLE`、または `ALTER` 権限が必要です。

<div id="backup">
  ### BACKUP
</div>

クエリで[`BACKUP`]を実行できるようにします。バックアップの詳細については、[「バックアップと復元」](/ja/operations/backup/overview)を参照してください。

<div id="create">
  ### CREATE
</div>

以下の権限階層に従って、[CREATE](../../sql-reference/statements/create/index.md) および [ATTACH](../../sql-reference/statements/attach.md) の DDL クエリの実行を許可します。

* `CREATE`. レベル: `GROUP`
  * `CREATE DATABASE`. レベル: `DATABASE`
  * `CREATE TABLE`. レベル: `TABLE`
    * `CREATE ARBITRARY TEMPORARY TABLE`. レベル: `GLOBAL`
      * `CREATE TEMPORARY TABLE`. レベル: `GLOBAL`
  * `CREATE VIEW`. レベル: `VIEW`
  * `CREATE DICTIONARY`. レベル: `DICTIONARY`

**注記**

* 作成したテーブルを削除するには、ユーザーに [DROP](#drop) 権限が必要です。

<div id="cluster">
  ### CLUSTER
</div>

`ON CLUSTER` クエリの実行を許可します。

```sql title="Syntax"
GRANT CLUSTER ON *.* TO <username>
```

デフォルトでは、`ON CLUSTER` を含むクエリを実行するには、ユーザーに `CLUSTER` 権限が付与されている必要があります。
事前に `CLUSTER` 権限を付与せずにクエリで `ON CLUSTER` を使おうとすると、次のエラーが表示されます。

```text
Not enough privileges. To execute this query, it's necessary to have the grant CLUSTER ON *.*. 
```

既定の動作は、`config.xml` の `access_control_improvements` セクションにある `on_cluster_queries_require_cluster_grant` 設定 (以下を参照) を `false` に設定することで変更できます。

```yaml title="config.xml"
<access_control_improvements>
    <on_cluster_queries_require_cluster_grant>true</on_cluster_queries_require_cluster_grant>
</access_control_improvements>
```

<div id="drop">
  ### DROP
</div>

以下の権限階層に従って、[DROP](../../sql-reference/statements/drop.md) および [DETACH](../../sql-reference/statements/detach.md) クエリを実行できます。

* `DROP`。レベル: `GROUP`
  * `DROP DATABASE`。レベル: `DATABASE`
  * `DROP TABLE`。レベル: `TABLE`
  * `DROP VIEW`。レベル: `VIEW`
  * `DROP DICTIONARY`。レベル: `DICTIONARY`

<div id="truncate">
  ### TRUNCATE
</div>

[TRUNCATE](../../sql-reference/statements/truncate.md) クエリの実行を許可します。

権限レベル: `TABLE`。

<div id="optimize">
  ### OPTIMIZE
</div>

[OPTIMIZE TABLE](../../sql-reference/statements/optimize.md) クエリの実行を許可します。

権限レベル: `TABLE`。

<div id="show">
  ### SHOW
</div>

以下の権限階層に従って、`SHOW`、`DESCRIBE`、`USE`、`EXISTS` クエリの実行を許可します。

* `SHOW`。レベル: `GROUP`
  * `SHOW DATABASES`。レベル: `DATABASE`。`SHOW DATABASES`、`SHOW CREATE DATABASE`、`USE <database>` クエリの実行を許可します。
  * `SHOW TABLES`。レベル: `TABLE`。`SHOW TABLES`、`EXISTS <table>`、`CHECK <table>` クエリの実行を許可します。
  * `SHOW COLUMNS`。レベル: `COLUMN`。`SHOW CREATE TABLE`、`DESCRIBE` クエリの実行を許可します。
  * `SHOW DICTIONARIES`。レベル: `DICTIONARY`。`SHOW DICTIONARIES`、`SHOW CREATE DICTIONARY`、`EXISTS <dictionary>` クエリの実行を許可します。

**注記**

指定されたテーブル、Dictionary、またはデータベースに関する何らかの権限を持っている場合、そのユーザーは `SHOW` 権限も持ちます。

<div id="kill-query">
  ### KILL QUERY
</div>

次の権限階層に従って、[KILL](../../sql-reference/statements/kill.md#kill-query) クエリの実行を許可します。

権限レベル: `GLOBAL`。

**注記**

`KILL QUERY` 権限を持つユーザーは、他のユーザーのクエリを強制終了できます。

<div id="access-management">
  ### アクセス管理
</div>

ユーザー、ロール、および行ポリシーを管理するクエリを実行する権限をユーザーに付与します。

* `ACCESS MANAGEMENT`. レベル: `GROUP`
  * `CREATE USER`. レベル: `GLOBAL`
  * `ALTER USER`. レベル: `GLOBAL`
  * `DROP USER`. レベル: `GLOBAL`
  * `CREATE ROLE`. レベル: `GLOBAL`
  * `ALTER ROLE`. レベル: `GLOBAL`
  * `DROP ROLE`. レベル: `GLOBAL`
  * `ROLE ADMIN`. レベル: `GLOBAL`
  * `CREATE ROW POLICY`. レベル: `GLOBAL`. 別名: `CREATE POLICY`
  * `ALTER ROW POLICY`. レベル: `GLOBAL`. 別名: `ALTER POLICY`
  * `DROP ROW POLICY`. レベル: `GLOBAL`. 別名: `DROP POLICY`
  * `CREATE QUOTA`. レベル: `GLOBAL`
  * `ALTER QUOTA`. レベル: `GLOBAL`
  * `DROP QUOTA`. レベル: `GLOBAL`
  * `CREATE SETTINGS PROFILE`. レベル: `GLOBAL`. 別名: `CREATE PROFILE`
  * `ALTER SETTINGS PROFILE`. レベル: `GLOBAL`. 別名: `ALTER PROFILE`
  * `DROP SETTINGS PROFILE`. レベル: `GLOBAL`. 別名: `DROP PROFILE`
  * `SHOW ACCESS`. レベル: `GROUP`
    * `SHOW_USERS`. レベル: `GLOBAL`. 別名: `SHOW CREATE USER`
    * `SHOW_ROLES`. レベル: `GLOBAL`. 別名: `SHOW CREATE ROLE`
    * `SHOW_ROW_POLICIES`. レベル: `GLOBAL`. 別名: `SHOW POLICIES`, `SHOW CREATE ROW POLICY`, `SHOW CREATE POLICY`
    * `SHOW_QUOTAS`. レベル: `GLOBAL`. 別名: `SHOW CREATE QUOTA`
    * `SHOW_SETTINGS_PROFILES`. レベル: `GLOBAL`. 別名: `SHOW PROFILES`, `SHOW CREATE SETTINGS PROFILE`, `SHOW CREATE PROFILE`
  * `ALLOW SQL SECURITY NONE`. レベル: `GLOBAL`. 別名: `CREATE SQL SECURITY NONE`, `SQL SECURITY NONE`, `SECURITY NONE`

`ROLE ADMIN` 権限により、admin オプション付きでユーザー自身に付与されていないロールを含め、任意のロールを付与および取り消すことができます。

<div id="system">
  ### SYSTEM
</div>

以下の権限階層に従って、ユーザーによる [SYSTEM](../../sql-reference/statements/system.md) クエリの実行を許可します。

* `SYSTEM`. レベル: `GROUP`
  * `SYSTEM SHUTDOWN`. レベル: `GLOBAL`. 別名: `SYSTEM KILL`, `SHUTDOWN`
  * `SYSTEM DROP CACHE`. 別名: `DROP CACHE`
    * `SYSTEM DROP DNS CACHE`. レベル: `GLOBAL`. 別名: `SYSTEM CLEAR DNS CACHE`, `SYSTEM DROP DNS`, `DROP DNS CACHE`, `DROP DNS`
    * `SYSTEM DROP MARK CACHE`. レベル: `GLOBAL`. 別名: `SYSTEM CLEAR MARK CACHE`, `SYSTEM DROP MARK`, `DROP MARK CACHE`, `DROP MARKS`
    * `SYSTEM DROP UNCOMPRESSED CACHE`. レベル: `GLOBAL`. 別名: `SYSTEM CLEAR UNCOMPRESSED CACHE`, `SYSTEM DROP UNCOMPRESSED`, `DROP UNCOMPRESSED CACHE`, `DROP UNCOMPRESSED`
  * `SYSTEM RELOAD`. レベル: `GROUP`
    * `SYSTEM RELOAD CONFIG`. レベル: `GLOBAL`. 別名: `RELOAD CONFIG`
    * `SYSTEM RELOAD DICTIONARY`. レベル: `GLOBAL`. 別名: `SYSTEM RELOAD DICTIONARIES`, `RELOAD DICTIONARY`, `RELOAD DICTIONARIES`
      * `SYSTEM RELOAD EMBEDDED DICTIONARIES`. レベル: `GLOBAL`. 別名: `RELOAD EMBEDDED DICTIONARIES`
  * `SYSTEM MERGES`. レベル: `TABLE`. 別名: `SYSTEM STOP MERGES`, `SYSTEM START MERGES`, `STOP MERGES`, `START MERGES`
  * `SYSTEM TTL MERGES`. レベル: `TABLE`. 別名: `SYSTEM STOP TTL MERGES`, `SYSTEM START TTL MERGES`, `STOP TTL MERGES`, `START TTL MERGES`
  * `SYSTEM FETCHES`. レベル: `TABLE`. 別名: `SYSTEM STOP FETCHES`, `SYSTEM START FETCHES`, `STOP FETCHES`, `START FETCHES`
  * `SYSTEM MOVES`. レベル: `TABLE`. 別名: `SYSTEM STOP MOVES`, `SYSTEM START MOVES`, `STOP MOVES`, `START MOVES`
  * `SYSTEM SENDS`. レベル: `GROUP`. 別名: `SYSTEM STOP SENDS`, `SYSTEM START SENDS`, `STOP SENDS`, `START SENDS`
    * `SYSTEM DISTRIBUTED SENDS`. レベル: `TABLE`. 別名: `SYSTEM STOP DISTRIBUTED SENDS`, `SYSTEM START DISTRIBUTED SENDS`, `STOP DISTRIBUTED SENDS`, `START DISTRIBUTED SENDS`
    * `SYSTEM REPLICATED SENDS`. レベル: `TABLE`. 別名: `SYSTEM STOP REPLICATED SENDS`, `SYSTEM START REPLICATED SENDS`, `STOP REPLICATED SENDS`, `START REPLICATED SENDS`
  * `SYSTEM REPLICATION QUEUES`. レベル: `TABLE`. 別名: `SYSTEM STOP REPLICATION QUEUES`, `SYSTEM START REPLICATION QUEUES`, `STOP REPLICATION QUEUES`, `START REPLICATION QUEUES`
  * `SYSTEM SYNC REPLICA`. レベル: `TABLE`. 別名: `SYNC REPLICA`
  * `SYSTEM RESTART REPLICA`. レベル: `TABLE`. 別名: `RESTART REPLICA`
  * `SYSTEM FLUSH`. レベル: `GROUP`
    * `SYSTEM FLUSH DISTRIBUTED`. レベル: `TABLE`. 別名: `FLUSH DISTRIBUTED`
    * `SYSTEM FLUSH LOGS`. レベル: `GLOBAL`. 別名: `FLUSH LOGS`

`SYSTEM RELOAD EMBEDDED DICTIONARIES` 権限は、`SYSTEM RELOAD DICTIONARY ON *.*` 権限によって暗黙的に付与されます。

<div id="introspection">
  ### INTROSPECTION
</div>

[イントロスペクション](../../operations/optimizing-performance/sampling-query-profiler.md)関数の利用を許可します。

* `INTROSPECTION`。レベル: `GROUP`。別名: `INTROSPECTION FUNCTIONS`
  * `addressToLine`。レベル: `GLOBAL`
  * `addressToLineWithInlines`。レベル: `GLOBAL`
  * `addressToSymbol`。レベル: `GLOBAL`
  * `demangle`。レベル: `GLOBAL`

<div id="sources">
  ### SOURCES
</div>

外部データソースの使用を許可します。[テーブルエンジン](../../engines/table-engines/index.md)および[テーブル関数](/ja/sql-reference/table-functions)に適用されます。

* `READ`. レベル: `GLOBAL_WITH_PARAMETER`
* `WRITE`. レベル: `GLOBAL_WITH_PARAMETER`

使用可能なパラメータ:

* `AZURE`
* `FILE`
* `HDFS`
* `HIVE`
* `JDBC`
* `KAFKA`
* `MONGO`
* `MYSQL`
* `NATS`
* `ODBC`
* `POSTGRES`
* `RABBITMQ`
* `REDIS`
* `REMOTE`
* `S3`
* `SQLITE`
* `URL`

:::note
ソースに対する READ/WRITE 権限の分離は、バージョン 25.7 以降で、かつサーバー設定
`access_control_improvements.enable_read_write_grants`
が有効な場合にのみ利用できます。

それ以外の場合は、`GRANT AZURE ON *.* TO user` 構文を使用してください。これは新しい `GRANT READ, WRITE ON AZURE TO user` と同等です。
:::

例:

* [MySQL テーブルエンジン](../../engines/table-engines/integrations/mysql.md)でテーブルを作成するには、`CREATE TABLE (ON db.table_name)` 権限と `MYSQL` 権限が必要です。
* [mysql テーブル関数](../../sql-reference/table-functions/mysql.md)を使用するには、`CREATE TEMPORARY TABLE` 権限と `MYSQL` 権限が必要です。

<div id="source-filter-grants">
  ### ソースフィルターに対する権限付与
</div>

:::note
この機能はバージョン25.8以降で利用でき、サーバー設定
`access_control_improvements.enable_read_write_grants`
が有効な場合にのみ使用できます
:::

正規表現フィルターを使用して、特定のソースURIへのアクセス権を付与できます。これにより、ユーザーがアクセスできる外部データソースをきめ細かく制御できます。

**構文:**

```sql
GRANT READ ON S3('regexp_pattern') TO user
```

この権限を付与すると、ユーザーは指定した正規表現パターンに一致する S3 URI からのみ読み取りできます。

**例:**

特定の S3 バケット内のパスへのアクセスを許可します:

```sql
-- Allow user to read only from s3://foo/ paths
GRANT READ ON S3('s3://foo/.*') TO john

-- Allow user to read from specific file patterns
GRANT READ ON S3('s3://mybucket/data/2024/.*\.parquet') TO analyst

-- Multiple filters can be granted to the same user
GRANT READ ON S3('s3://foo/.*') TO john
GRANT READ ON S3('s3://bar/.*') TO john
```

:::warning
ソースフィルターはパラメータに **regexp** を取るため、次の権限付与
`GRANT READ ON URL('http://www.google.com') TO john;`

によってクエリが許可されます

```sql
SELECT * FROM url('https://www.google.com');
SELECT * FROM url('https://www-google.com');
```

`.` は正規表現では `任意の1文字` として扱われるためです。
これにより、潜在的な脆弱性が生じる可能性があります。正しいGRANTは次のとおりです

```sql
GRANT READ ON URL('https://www\.google\.com') TO john;
```

:::

**GRANT OPTION を使用した再付与:**

元の権限に `WITH GRANT OPTION` が付与されている場合は、`GRANT CURRENT GRANTS` を使用して再付与できます。

```sql
-- Original grant with GRANT OPTION
GRANT READ ON S3('s3://foo/.*') TO john WITH GRANT OPTION

-- John can now regrant this access to others
GRANT CURRENT GRANTS(READ ON S3) TO alice
```

**重要な制限事項:**

* **部分的な取り消しはできません:** 付与済みのフィルタパターンの一部だけを取り消すことはできません。必要に応じて、いったん権限全体を取り消し、新しいパターンで付与し直す必要があります。
* **ワイルドカードによる権限付与はできません:** `GRANT READ ON *('regexp')` や、これに類するワイルドカードのみのパターンは使用できません。特定のログソースを指定する必要があります。

<div id="dictget">
  ### dictGet
</div>

* `dictGet`. 別名: `dictHas`, `dictGetHierarchy`, `dictIsIn`

ユーザーが [dictGet](/ja/sql-reference/functions/ext-dict-functions#dictGet)、[dictHas](../../sql-reference/functions/ext-dict-functions.md#dictHas)、[dictGetHierarchy](../../sql-reference/functions/ext-dict-functions.md#dictGetHierarchy)、[dictIsIn](../../sql-reference/functions/ext-dict-functions.md#dictIsIn) 関数を実行できるようにします。

権限レベル: `DICTIONARY`.

**例**

* `GRANT dictGet ON mydb.mydictionary TO john`
* `GRANT dictGet ON mydictionary TO john`

<div id="displaysecretsinshowandselect">
  ### displaySecretsInShowAndSelect
</div>

[`display_secrets_in_show_and_select` サーバー設定](../../operations/server-configuration-parameters/settings#display_secrets_in_show_and_select)
と
[`format_display_secrets_in_show_and_select` フォーマット設定](../../operations/settings/formats#format_display_secrets_in_show_and_select)
の両方が有効になっている場合、ユーザーは `SHOW` クエリおよび `SELECT` クエリでシークレットを表示できます。

<div id="named-collection-admin">
  ### NAMED COLLECTION ADMIN
</div>

指定した named collection に対する特定の操作を許可します。バージョン 23.7 より前は NAMED COLLECTION CONTROL と呼ばれていましたが、23.7 以降は NAMED COLLECTION ADMIN が追加され、NAMED COLLECTION CONTROL は別名として維持されています。

* `NAMED COLLECTION ADMIN`. レベル: `NAMED_COLLECTION`. 別名: `NAMED COLLECTION CONTROL`
  * `CREATE NAMED COLLECTION`. レベル: `NAMED_COLLECTION`
  * `DROP NAMED COLLECTION`. レベル: `NAMED_COLLECTION`
  * `ALTER NAMED COLLECTION`. レベル: `NAMED_COLLECTION`
  * `SHOW NAMED COLLECTIONS`. レベル: `NAMED_COLLECTION`. 別名: `SHOW NAMED COLLECTIONS`
  * `SHOW NAMED COLLECTIONS SECRETS`. レベル: `NAMED_COLLECTION`. 別名: `SHOW NAMED COLLECTIONS SECRETS`
  * `NAMED COLLECTION`. レベル: `NAMED_COLLECTION`. 別名: `NAMED COLLECTION USAGE, USE NAMED COLLECTION`

ほかのすべての grant (CREATE、DROP、ALTER、SHOW) とは異なり、grant NAMED COLLECTION が追加されたのは 23.7 で、ほかはそれより前の 22.12 に追加されました。

**例**

named collection の名前が abc であるとすると、ユーザー john に CREATE NAMED COLLECTION 権限を付与します。

* `GRANT CREATE NAMED COLLECTION ON abc TO john`

<div id="table-engine">
  ### TABLE ENGINE
</div>

テーブルの作成時に、指定したテーブルエンジンを使用できるようにします。[テーブルエンジン](../../engines/table-engines/index.md)に適用されます。

**例**

* `GRANT TABLE ENGINE ON * TO john`
* `GRANT TABLE ENGINE ON TinyLog TO john`

:::note
デフォルトでは、後方互換性のため、特定のテーブルエンジンを指定してテーブルを作成する場合、grant は無視されます。
ただし、config.xml で [`table_engines_require_grant` を true に設定](https://github.com/ClickHouse/ClickHouse/blob/df970ed64eaf472de1e7af44c21ec95956607ebb/programs/server/config.xml#L853-L855)すると、この動作を変更できます。
:::

外部ソースを持つ一部のテーブルエンジンでは、対応するソースに対する `READ`/`WRITE` 権限が必要になる場合があります。[SOURCES](#sources)を参照してください。

たとえば、AzureBlobStorage テーブルエンジンでは、次の grant が必要になる場合があります。

* `GRANT READ, WRITE ON AZURE TO john`

<div id="all">
  ### ALL
</div>

<CloudNotSupportedBadge />

対象エンティティに対するすべての権限を、ユーザーアカウントまたはロールに付与します。

:::note
権限 `ALL` は ClickHouse Cloud ではサポートされていません。ClickHouse Cloud では `default` ユーザーの権限が制限されているため、`default_role` を付与することで、ユーザーに最大限の権限を付与できます。詳細は[こちら](/ja/cloud/security/manage-cloud-users)を参照してください。
また、`default` ユーザーとして `GRANT CURRENT GRANTS` を使用することで、`ALL` と同様の効果を得ることもできます。
:::

<div id="none">
  ### NONE
</div>

いかなる権限も付与しません。

<div id="admin-option">
  ### ADMIN OPTION
</div>

`ADMIN OPTION` 権限があると、ユーザーは自分のロールを他のユーザーに付与できます。