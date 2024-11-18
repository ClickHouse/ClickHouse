---
slug: /ja/sql-reference/statements/grant
sidebar_position: 38
sidebar_label: GRANT
---

# GRANT ステートメント

- ClickHouseユーザーアカウントまたはロールに[権限](#privileges)を付与します。
- ユーザーアカウントにロールを割り当てたり、他のロールに割り当てたりします。

権限を取り消すには、[REVOKE](../../sql-reference/statements/revoke.md)ステートメントを使用します。また、[SHOW GRANTS](../../sql-reference/statements/show.md#show-grants)ステートメントを使用して付与された権限を一覧表示することもできます。

## 権限の付与構文

``` sql
GRANT [ON CLUSTER cluster_name] privilege[(column_name [,...])] [,...] ON {db.table[*]|db[*].*|*.*|table[*]|*} TO {user | role | CURRENT_USER} [,...] [WITH GRANT OPTION] [WITH REPLACE OPTION]
```

- `privilege` — 権限の種類。
- `role` — ClickHouseユーザーロール。
- `user` — ClickHouseユーザーアカウント。

`WITH GRANT OPTION`句は、`user`または`role`に`GRANT`クエリを実行する権限を付与します。ユーザーは自分が持つ範囲とそれ以下の権限を付与することができます。`WITH REPLACE OPTION`句は、指定しない場合、新しい権限で古い権限を`user`または`role`に置換し、付与される権限を追加します。

## ロールを割り当てる構文

``` sql
GRANT [ON CLUSTER cluster_name] role [,...] TO {user | another_role | CURRENT_USER} [,...] [WITH ADMIN OPTION] [WITH REPLACE OPTION]
```

- `role` — ClickHouseユーザーロール。
- `user` — ClickHouseユーザーアカウント。

`WITH ADMIN OPTION`句は、`user`または`role`に[ADMIN OPTION](#admin-option)権限を付与します。`WITH REPLACE OPTION`句は、指定しない場合、新しいロールで古いロールを`user`または`role`に置換し、追加します。

## 現在の権限を付与する構文
``` sql
GRANT CURRENT GRANTS{(privilege[(column_name [,...])] [,...] ON {db.table|db.*|*.*|table|*}) | ON {db.table|db.*|*.*|table|*}} TO {user | role | CURRENT_USER} [,...] [WITH GRANT OPTION] [WITH REPLACE OPTION]
```

- `privilege` — 権限の種類。
- `role` — ClickHouseユーザーロール。
- `user` — ClickHouseユーザーアカウント。

`CURRENT GRANTS`ステートメントを使用すると、指定した全ての権限をユーザーまたはロールに付与することができます。権限が指定されていない場合、指定されたユーザーまたはロールは`CURRENT_USER`の利用可能なすべての権限を受け取ります。

## 使用法

`GRANT`を使用するためには、アカウントに`GRANT OPTION`権限が必要です。自分のアカウント権限の範囲内でのみ権限を付与することができます。

たとえば、管理者が`john`アカウントに次のクエリで権限を付与したとします：

``` sql
GRANT SELECT(x,y) ON db.table TO john WITH GRANT OPTION
```

これは、`john`が次の操作を行うための許可を受けたことを意味します：

- `SELECT x,y FROM db.table`
- `SELECT x FROM db.table`
- `SELECT y FROM db.table`

`john`は`SELECT z FROM db.table`を実行することはできません。また、`SELECT * FROM db.table`も利用できません。このクエリを処理する際、ClickHouseはデータを返さず、`x`や`y`も返されません。唯一の例外はテーブルが`x`と`y`カラムのみを含む場合であり、この場合、ClickHouseはすべてのデータを返します。

また、`john`は`GRANT OPTION`権限を持っているため、他のユーザーに対して同じまたは少ない範囲の権限を付与することができます。

`system`データベースへのアクセスは常に許可されています（クエリ処理のためにこのデータベースが使用されます）。

1つのクエリで複数の権限を複数のアカウントに付与することができます。クエリ`GRANT SELECT, INSERT ON *.* TO john, robin`は、`john`および`robin`アカウントがサーバー上のすべてのデータベースのすべてのテーブルに対して`INSERT`および`SELECT`クエリを実行することを許可します。

## ワイルドカード付与

権限を指定する際に、テーブルまたはデータベース名の代わりにアスタリスク（`*`）を使用することができます。たとえば、`GRANT SELECT ON db.* TO john`クエリは`john`が`db`データベース内のすべてのテーブルに対して`SELECT`クエリを実行できるようにします。また、データベース名を省略することもできます。この場合、現在のデータベースに対して権限が付与されます。例えば、`GRANT SELECT ON * TO john`は現在のデータベース内のすべてのテーブルに対する権限を付与し、`GRANT SELECT ON mytable TO john`は現在のデータベース内の`mytable`テーブルに対する権限を付与します。

:::note
以下で説明する機能はClickHouseのバージョン24.10から利用可能です。
:::

テーブルまたはデータベース名の末尾にアスタリスクを付けることもできます。この機能により、テーブルのパスの抽象的なプレフィックスに対して権限を付与することができます。例：`GRANT SELECT ON db.my_tables* TO john`。このクエリは、`db`データベースのすべての`my_tables`プレフィックスを持つテーブルに対して`john`が`SELECT`クエリを実行できるようにします。

もっと例を挙げると：

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

付与されたパス内の新しく作成されたテーブルはすべて、親からすべての権限を自動的に継承します。たとえば、`GRANT SELECT ON db.* TO john`クエリを実行し、その後新しいテーブル`db.new_table`を作成した場合、ユーザー`john`は`SELECT * FROM db.new_table`クエリを実行することができます。

プレフィックスのみに対してアスタリスクを指定できます：
```sql
GRANT SELECT ON db.* TO john -- correct
GRANT SELECT ON db*.* TO john -- correct

GRANT SELECT ON *.my_table TO john -- wrong
GRANT SELECT ON foo*bar TO john -- wrong
GRANT SELECT ON *suffix TO john -- wrong
GRANT SELECT(foo) ON db.table* TO john -- wrong
```

## 権限

権限とは、特定の種類のクエリを実行する許可です。

権限には階層的構造があります。許可されたクエリのセットは権限のスコープに依存します。

権限の階層:

- [SELECT](#select)
- [INSERT](#insert)
- [ALTER](#alter)
    - `ALTER TABLE`
        - `ALTER UPDATE`
        - `ALTER DELETE`
        - `ALTER COLUMN`
            - `ALTER ADD COLUMN`
            - `ALTER DROP COLUMN`
            - `ALTER MODIFY COLUMN`
            - `ALTER COMMENT COLUMN`
            - `ALTER CLEAR COLUMN`
            - `ALTER RENAME COLUMN`
        - `ALTER INDEX`
            - `ALTER ORDER BY`
            - `ALTER SAMPLE BY`
            - `ALTER ADD INDEX`
            - `ALTER DROP INDEX`
            - `ALTER MATERIALIZE INDEX`
            - `ALTER CLEAR INDEX`
        - `ALTER CONSTRAINT`
            - `ALTER ADD CONSTRAINT`
            - `ALTER DROP CONSTRAINT`
        - `ALTER TTL`
            - `ALTER MATERIALIZE TTL`
        - `ALTER SETTINGS`
        - `ALTER MOVE PARTITION`
        - `ALTER FETCH PARTITION`
        - `ALTER FREEZE PARTITION`
    - `ALTER VIEW`
        - `ALTER VIEW REFRESH`
        - `ALTER VIEW MODIFY QUERY`
        - `ALTER VIEW MODIFY SQL SECURITY`
- [CREATE](#create)
    - `CREATE DATABASE`
    - `CREATE TABLE`
        - `CREATE ARBITRARY TEMPORARY TABLE`
            - `CREATE TEMPORARY TABLE`
    - `CREATE VIEW`
    - `CREATE DICTIONARY`
    - `CREATE FUNCTION`
- [DROP](#drop)
    - `DROP DATABASE`
    - `DROP TABLE`
    - `DROP VIEW`
    - `DROP DICTIONARY`
    - `DROP FUNCTION`
- [TRUNCATE](#truncate)
- [OPTIMIZE](#optimize)
- [SHOW](#show)
    - `SHOW DATABASES`
    - `SHOW TABLES`
    - `SHOW COLUMNS`
    - `SHOW DICTIONARIES`
- [KILL QUERY](#kill-query)
- [ACCESS MANAGEMENT](#access-management)
    - `CREATE USER`
    - `ALTER USER`
    - `DROP USER`
    - `CREATE ROLE`
    - `ALTER ROLE`
    - `DROP ROLE`
    - `CREATE ROW POLICY`
    - `ALTER ROW POLICY`
    - `DROP ROW POLICY`
    - `CREATE QUOTA`
    - `ALTER QUOTA`
    - `DROP QUOTA`
    - `CREATE SETTINGS PROFILE`
    - `ALTER SETTINGS PROFILE`
    - `DROP SETTINGS PROFILE`
    - `SHOW ACCESS`
        - `SHOW_USERS`
        - `SHOW_ROLES`
        - `SHOW_ROW_POLICIES`
        - `SHOW_QUOTAS`
        - `SHOW_SETTINGS_PROFILES`
    - `ROLE ADMIN`
- [SYSTEM](#system)
    - `SYSTEM SHUTDOWN`
    - `SYSTEM DROP CACHE`
        - `SYSTEM DROP DNS CACHE`
        - `SYSTEM DROP MARK CACHE`
        - `SYSTEM DROP UNCOMPRESSED CACHE`
    - `SYSTEM RELOAD`
        - `SYSTEM RELOAD CONFIG`
        - `SYSTEM RELOAD DICTIONARY`
            - `SYSTEM RELOAD EMBEDDED DICTIONARIES`
        - `SYSTEM RELOAD FUNCTION`
        - `SYSTEM RELOAD FUNCTIONS`
    - `SYSTEM MERGES`
    - `SYSTEM TTL MERGES`
    - `SYSTEM FETCHES`
    - `SYSTEM MOVES`
    - `SYSTEM SENDS`
        - `SYSTEM DISTRIBUTED SENDS`
        - `SYSTEM REPLICATED SENDS`
    - `SYSTEM REPLICATION QUEUES`
    - `SYSTEM SYNC REPLICA`
    - `SYSTEM RESTART REPLICA`
    - `SYSTEM FLUSH`
        - `SYSTEM FLUSH DISTRIBUTED`
        - `SYSTEM FLUSH LOGS`
    - `CLUSTER`（`access_control_improvements.on_cluster_queries_require_cluster_grant`構成指令も参照）
- [INTROSPECTION](#introspection)
    - `addressToLine`
    - `addressToLineWithInlines`
    - `addressToSymbol`
    - `demangle`
- [SOURCES](#sources)
    - `AZURE`
    - `FILE`
    - `HDFS`
    - `HIVE`
    - `JDBC`
    - `KAFKA`
    - `MONGO`
    - `MYSQL`
    - `NATS`
    - `ODBC`
    - `POSTGRES`
    - `RABBITMQ`
    - `REDIS`
    - `REMOTE`
    - `S3`
    - `SQLITE`
    - `URL`
- [dictGet](#dictget)
- [displaySecretsInShowAndSelect](#displaysecretsinshowandselect)
- [NAMED COLLECTION ADMIN](#named-collection-admin)
    - `CREATE NAMED COLLECTION`
    - `DROP NAMED COLLECTION`
    - `ALTER NAMED COLLECTION`
    - `SHOW NAMED COLLECTIONS`
    - `SHOW NAMED COLLECTIONS SECRETS`
    - `NAMED COLLECTION`
- [TABLE ENGINE](#table-engine)

この階層がどのように扱われるかの例:

- `ALTER`権限は他のすべての`ALTER*`権限を含みます。
- `ALTER CONSTRAINT`には、`ALTER ADD CONSTRAINT`および`ALTER DROP CONSTRAINT`権限が含まれます。

権限は異なるレベルで適用されます。レベルを知ることは、権限のために利用可能な構文を示唆します。

レベル（低い順から高い順）：

- `COLUMN` — カラム、テーブル、データベース、またはグローバルに権限を付与できます。
- `TABLE` — テーブル、データベース、またはグローバルに権限を付与できます。
- `VIEW` — ビュー、データベース、またはグローバルに権限を付与できます。
- `DICTIONARY` — Dictionary、データベース、またはグローバルに権限を付与できます。
- `DATABASE` — データベースまたはグローバルに権限を付与できます。
- `GLOBAL` — グローバルにのみ権限を付与できます。
- `GROUP` — 異なるレベルの権限をグループ化します。`GROUP`レベルの権限が付与されると、使用された構文に対応するグループからの権限のみが付与されます。

許可される構文の例：

- `GRANT SELECT(x) ON db.table TO user`
- `GRANT SELECT ON db.* TO user`

許可されない構文の例：

- `GRANT CREATE USER(x) ON db.table TO user`
- `GRANT CREATE USER ON db.* TO user`

特別な権限[ALL](#all)は、全ての権限をユーザーアカウントまたはロールに付与します。

デフォルトでは、ユーザーアカウントまたはロールには権限がありません。

ユーザーまたはロールが権限を持たない場合、[NONE](#none)権限として表示されます。

いくつかのクエリはその実装上、権限のセットを必要とします。たとえば、[RENAME](../../sql-reference/statements/optimize.md)クエリを実行するには、`SELECT`、`CREATE TABLE`、`INSERT`、および`DROP TABLE`権限が必要です。

### SELECT

[SELECT](../../sql-reference/statements/select/index.md)クエリの実行を許可します。

権限レベル：`COLUMN`。

**説明**

この権限を持つユーザーは、指定されたテーブルおよびデータベース内の指定されたカラムのデータに対して`SELECT`クエリを実行できます。ユーザーが指定されていないカラムを含めると、クエリはデータを返しません。

以下の権限を考慮してください：

``` sql
GRANT SELECT(x,y) ON db.table TO john
```

この権限は、`john`が`db.table`の`x`および/または`y`カラムに関するデータを含む任意の`SELECT`クエリを実行できるようにします。たとえば、`SELECT x FROM db.table`。`john`は`SELECT z FROM db.table`を実行することはできません。`SELECT * FROM db.table`も利用できません。このクエリを処理する際、ClickHouseはデータを返さず、`x`や`y`も返されません。唯一の例外はテーブルが`x`と`y`カラムのみを含む場合であり、この場合、ClickHouseはすべてのデータを返します。

### INSERT

[INSERT](../../sql-reference/statements/insert-into.md)クエリの実行を許可します。

権限レベル：`COLUMN`。

**説明**

この権限を持つユーザーは、指定されたテーブルおよびデータベースの指定されたカラムに対して`INSERT`クエリを実行できます。ユーザーが指定されていないカラムを含めると、クエリはデータを挿入しません。

**例**

``` sql
GRANT INSERT(x,y) ON db.table TO john
```

付与された権限は、`john`が`db.table`の`x`および/または`y`カラムにデータを挿入できるようにします。

### ALTER

[ALTER](../../sql-reference/statements/alter/index.md)クエリの実行を、以下の権限の階層に従って許可します：

- `ALTER`. レベル: `COLUMN`.
    - `ALTER TABLE`. レベル: `GROUP`
        - `ALTER UPDATE`. レベル: `COLUMN`. 別名: `UPDATE`
        - `ALTER DELETE`. レベル: `COLUMN`. 別名: `DELETE`
        - `ALTER COLUMN`. レベル: `GROUP`
            - `ALTER ADD COLUMN`. レベル: `COLUMN`. 別名: `ADD COLUMN`
            - `ALTER DROP COLUMN`. レベル: `COLUMN`. 別名: `DROP COLUMN`
            - `ALTER MODIFY COLUMN`. レベル: `COLUMN`. 別名: `MODIFY COLUMN`
            - `ALTER COMMENT COLUMN`. レベル: `COLUMN`. 別名: `COMMENT COLUMN`
            - `ALTER CLEAR COLUMN`. レベル: `COLUMN`. 別名: `CLEAR COLUMN`
            - `ALTER RENAME COLUMN`. レベル: `COLUMN`. 別名: `RENAME COLUMN`
        - `ALTER INDEX`. レベル: `GROUP`. 別名: `INDEX`
            - `ALTER ORDER BY`. レベル: `TABLE`. 別名: `ALTER MODIFY ORDER BY`, `MODIFY ORDER BY`
            - `ALTER SAMPLE BY`. レベル: `TABLE`. 別名: `ALTER MODIFY SAMPLE BY`, `MODIFY SAMPLE BY`
            - `ALTER ADD INDEX`. レベル: `TABLE`. 別名: `ADD INDEX`
            - `ALTER DROP INDEX`. レベル: `TABLE`. 別名: `DROP INDEX`
            - `ALTER MATERIALIZE INDEX`. レベル: `TABLE`. 別名: `MATERIALIZE INDEX`
            - `ALTER CLEAR INDEX`. レベル: `TABLE`. 別名: `CLEAR INDEX`
        - `ALTER CONSTRAINT`. レベル: `GROUP`. 別名: `CONSTRAINT`
            - `ALTER ADD CONSTRAINT`. レベル: `TABLE`. 別名: `ADD CONSTRAINT`
            - `ALTER DROP CONSTRAINT`. レベル: `TABLE`. 別名: `DROP CONSTRAINT`
        - `ALTER TTL`. レベル: `TABLE`. 別名: `ALTER MODIFY TTL`, `MODIFY TTL`
            - `ALTER MATERIALIZE TTL`. レベル: `TABLE`. 別名: `MATERIALIZE TTL`
        - `ALTER SETTINGS`. レベル: `TABLE`. 別名: `ALTER SETTING`, `ALTER MODIFY SETTING`, `MODIFY SETTING`
        - `ALTER MOVE PARTITION`. レベル: `TABLE`. 別名: `ALTER MOVE PART`, `MOVE PARTITION`, `MOVE PART`
        - `ALTER FETCH PARTITION`. レベル: `TABLE`. 別名: `ALTER FETCH PART`, `FETCH PARTITION`, `FETCH PART`
        - `ALTER FREEZE PARTITION`. レベル: `TABLE`. 別名: `FREEZE PARTITION`
    - `ALTER VIEW` レベル: `GROUP`
        - `ALTER VIEW REFRESH`. レベル: `VIEW`. 別名: `ALTER LIVE VIEW REFRESH`, `REFRESH VIEW`
        - `ALTER VIEW MODIFY QUERY`. レベル: `VIEW`. 別名: `ALTER TABLE MODIFY QUERY`
        - `ALTER VIEW MODIFY SQL SECURITY`. レベル: `VIEW`. 別名: `ALTER TABLE MODIFY SQL SECURITY`

この階層がどのように扱われるかの例:

- `ALTER`権限は他のすべての`ALTER*`権限を含みます。
- `ALTER CONSTRAINT`には`ALTER ADD CONSTRAINT`および`ALTER DROP CONSTRAINT`権限が含まれます。

**注意事項**

- `MODIFY SETTING`権限はテーブルエンジン設定の変更を許可します。設定またはサーバー構成パラメータには影響しません。
- `ATTACH`操作には[CREATE](#create)権限が必要です。
- `DETACH`操作には[DROP](#drop)権限が必要です。
- [KILL MUTATION](../../sql-reference/statements/kill.md#kill-mutation)クエリでミューテーションを停止するには、このミューテーションを開始する権限が必要です。例えば、`ALTER UPDATE`クエリを停止する場合、`ALTER UPDATE`、`ALTER TABLE`、または`ALTER`権限が必要です。

### CREATE

[CREATE](../../sql-reference/statements/create/index.md)および[ATTACH](../../sql-reference/statements/attach.md)DDLクエリの実行を以下の権限の階層に従って許可します：

- `CREATE`. レベル: `GROUP`
    - `CREATE DATABASE`. レベル: `DATABASE`
    - `CREATE TABLE`. レベル: `TABLE`
        - `CREATE ARBITRARY TEMPORARY TABLE`. レベル: `GLOBAL`
            - `CREATE TEMPORARY TABLE`. レベル: `GLOBAL`
    - `CREATE VIEW`. レベル: `VIEW`
    - `CREATE DICTIONARY`. レベル: `DICTIONARY`

**注意事項**

- 作成したテーブルを削除するには、ユーザーは[DROP](#drop)権限が必要です。

### DROP

[DROP](../../sql-reference/statements/drop.md)および[DETACH](../../sql-reference/statements/detach.md)クエリの実行を以下の権限の階層に従って許可します：

- `DROP`. レベル: `GROUP`
    - `DROP DATABASE`. レベル: `DATABASE`
    - `DROP TABLE`. レベル: `TABLE`
    - `DROP VIEW`. レベル: `VIEW`
    - `DROP DICTIONARY`. レベル: `DICTIONARY`

### TRUNCATE

[TRUNCATE](../../sql-reference/statements/truncate.md)クエリの実行を許可します。

権限レベル: `TABLE`.

### OPTIMIZE

[OPTIMIZE TABLE](../../sql-reference/statements/optimize.md)クエリの実行を許可します。

権限レベル: `TABLE`.

### SHOW

`SHOW`、`DESCRIBE`、`USE`および`EXISTS`クエリの実行を以下の権限の階層に従って許可します：

- `SHOW`. レベル: `GROUP`
    - `SHOW DATABASES`. レベル: `DATABASE`. `SHOW DATABASES`、`SHOW CREATE DATABASE`、`USE <database>`クエリを実行可能にします。
    - `SHOW TABLES`. レベル: `TABLE`. `SHOW TABLES`、`EXISTS <table>`、`CHECK <table>`クエリを実行可能にします。
    - `SHOW COLUMNS`. レベル: `COLUMN`. `SHOW CREATE TABLE`、`DESCRIBE`クエリを実行可能にします。
    - `SHOW DICTIONARIES`. レベル: `DICTIONARY`. `SHOW DICTIONARIES`、`SHOW CREATE DICTIONARY`、`EXISTS <dictionary>`クエリを実行可能にします。

**注意事項**

ユーザーは指定されたテーブル、Dictionary、データベースに関する他の権限を持っている場合、`SHOW`権限を持っています。

### KILL QUERY

[KILL](../../sql-reference/statements/kill.md#kill-query)クエリの実行を以下の権限の階層に従って許可します：

権限レベル: `GLOBAL`.

**注意事項**

`KILL QUERY`権限は、あるユーザーが他のユーザーのクエリを停止することを許可します。

### ACCESS MANAGEMENT

ユーザーがユーザー、ロール、および行ポリシーを管理するクエリを実行することを許可します。

- `ACCESS MANAGEMENT`. レベル: `GROUP`
    - `CREATE USER`. レベル: `GLOBAL`
    - `ALTER USER`. レベル: `GLOBAL`
    - `DROP USER`. レベル: `GLOBAL`
    - `CREATE ROLE`. レベル: `GLOBAL`
    - `ALTER ROLE`. レベル: `GLOBAL`
    - `DROP ROLE`. レベル: `GLOBAL`
    - `ROLE ADMIN`. レベル: `GLOBAL`
    - `CREATE ROW POLICY`. レベル: `GLOBAL`. 別名: `CREATE POLICY`
    - `ALTER ROW POLICY`. レベル: `GLOBAL`. 別名: `ALTER POLICY`
    - `DROP ROW POLICY`. レベル: `GLOBAL`. 別名: `DROP POLICY`
    - `CREATE QUOTA`. レベル: `GLOBAL`
    - `ALTER QUOTA`. レベル: `GLOBAL`
    - `DROP QUOTA`. レベル: `GLOBAL`
    - `CREATE SETTINGS PROFILE`. レベル: `GLOBAL`. 別名: `CREATE PROFILE`
    - `ALTER SETTINGS PROFILE`. レベル: `GLOBAL`. 別名: `ALTER PROFILE`
    - `DROP SETTINGS PROFILE`. レベル: `GLOBAL`. 別名: `DROP PROFILE`
    - `SHOW ACCESS`. レベル: `GROUP`
        - `SHOW_USERS`. レベル: `GLOBAL`. 別名: `SHOW CREATE USER`
        - `SHOW_ROLES`. レベル: `GLOBAL`. 別名: `SHOW CREATE ROLE`
        - `SHOW_ROW_POLICIES`. レベル: `GLOBAL`. 別名: `SHOW POLICIES`, `SHOW CREATE ROW POLICY`, `SHOW CREATE POLICY`
        - `SHOW_QUOTAS`. レベル: `GLOBAL`. 別名: `SHOW CREATE QUOTA`
        - `SHOW_SETTINGS_PROFILES`. レベル: `GLOBAL`. 別名: `SHOW PROFILES`, `SHOW CREATE SETTINGS PROFILE`, `SHOW CREATE PROFILE`
    - `ALLOW SQL SECURITY NONE`. レベル: `GLOBAL`. 別名: `CREATE SQL SECURITY NONE`, `SQL SECURITY NONE`, `SECURITY NONE`

`ROLE ADMIN`権限は、ユーザーが他のユーザーに、ユーザーに付与されていない管理オプションを含むすべてのロールを割り当ておよび取り消すことを許可します。

### SYSTEM

ユーザーが[SYSTEM](../../sql-reference/statements/system.md)クエリを次の権限の階層に従って実行できるようにします。

- `SYSTEM`. レベル: `GROUP`
    - `SYSTEM SHUTDOWN`. レベル: `GLOBAL`. 別名: `SYSTEM KILL`, `SHUTDOWN`
    - `SYSTEM DROP CACHE`. 別名: `DROP CACHE`
        - `SYSTEM DROP DNS CACHE`. レベル: `GLOBAL`. 別名: `SYSTEM DROP DNS`, `DROP DNS CACHE`, `DROP DNS`
        - `SYSTEM DROP MARK CACHE`. レベル: `GLOBAL`. 別名: `SYSTEM DROP MARK`, `DROP MARK CACHE`, `DROP MARKS`
        - `SYSTEM DROP UNCOMPRESSED CACHE`. レベル: `GLOBAL`. 別名: `SYSTEM DROP UNCOMPRESSED`, `DROP UNCOMPRESSED CACHE`, `DROP UNCOMPRESSED`
    - `SYSTEM RELOAD`. レベル: `GROUP`
        - `SYSTEM RELOAD CONFIG`. レベル: `GLOBAL`. 別名: `RELOAD CONFIG`
        - `SYSTEM RELOAD DICTIONARY`. レベル: `GLOBAL`. 別名: `SYSTEM RELOAD DICTIONARIES`, `RELOAD DICTIONARY`, `RELOAD DICTIONARIES`
            - `SYSTEM RELOAD EMBEDDED DICTIONARIES`. レベル: `GLOBAL`. 別名: `RELOAD EMBEDDED DICTIONARIES`
    - `SYSTEM MERGES`. レベル: `TABLE`. 別名: `SYSTEM STOP MERGES`, `SYSTEM START MERGES`, `STOP MERGES`, `START MERGES`
    - `SYSTEM TTL MERGES`. レベル: `TABLE`. 別名: `SYSTEM STOP TTL MERGES`, `SYSTEM START TTL MERGES`, `STOP TTL MERGES`, `START TTL MERGES`
    - `SYSTEM FETCHES`. レベル: `TABLE`. 別名: `SYSTEM STOP FETCHES`, `SYSTEM START FETCHES`, `STOP FETCHES`, `START FETCHES`
    - `SYSTEM MOVES`. レベル: `TABLE`. 別名: `SYSTEM STOP MOVES`, `SYSTEM START MOVES`, `STOP MOVES`, `START MOVES`
    - `SYSTEM SENDS`. レベル: `GROUP`. 別名: `SYSTEM STOP SENDS`, `SYSTEM START SENDS`, `STOP SENDS`, `START SENDS`
        - `SYSTEM DISTRIBUTED SENDS`. レベル: `TABLE`. 別名: `SYSTEM STOP DISTRIBUTED SENDS`, `SYSTEM START DISTRIBUTED SENDS`, `STOP DISTRIBUTED SENDS`, `START DISTRIBUTED SENDS`
        - `SYSTEM REPLICATED SENDS`. レベル: `TABLE`. 別名: `SYSTEM STOP REPLICATED SENDS`, `SYSTEM START REPLICATED SENDS`, `STOP REPLICATED SENDS`, `START REPLICATED SENDS`
    - `SYSTEM REPLICATION QUEUES`. レベル: `TABLE`. 別名: `SYSTEM STOP REPLICATION QUEUES`, `SYSTEM START REPLICATION QUEUES`, `STOP REPLICATION QUEUES`, `START REPLICATION QUEUES`
    - `SYSTEM SYNC REPLICA`. レベル: `TABLE`. 別名: `SYNC REPLICA`
    - `SYSTEM RESTART REPLICA`. レベル: `TABLE`. 別名: `RESTART REPLICA`
    - `SYSTEM FLUSH`. レベル: `GROUP`
        - `SYSTEM FLUSH DISTRIBUTED`. レベル: `TABLE`. 別名: `FLUSH DISTRIBUTED`
        - `SYSTEM FLUSH LOGS`. レベル: `GLOBAL`. 別名: `FLUSH LOGS`

`SYSTEM RELOAD EMBEDDED DICTIONARIES`権限は、`SYSTEM RELOAD DICTIONARY ON *.*`権限で暗黙的に付与されます。

### INTROSPECTION

[introspection](../../operations/optimizing-performance/sampling-query-profiler.md)関数の使用を許可します。

- `INTROSPECTION`. レベル: `GROUP`. 別名: `INTROSPECTION FUNCTIONS`
    - `addressToLine`. レベル: `GLOBAL`
    - `addressToLineWithInlines`. レベル: `GLOBAL`
    - `addressToSymbol`. レベル: `GLOBAL`
    - `demangle`. レベル: `GLOBAL`

### SOURCES

外部データソースの使用を許可します。[テーブルエンジン](../../engines/table-engines/index.md)および[テーブル関数](../../sql-reference/table-functions/index.md#table-functions)に適用されます。

- `SOURCES`. レベル: `GROUP`
    - `AZURE`. レベル: `GLOBAL`
    - `FILE`. レベル: `GLOBAL`
    - `HDFS`. レベル: `GLOBAL`
    - `HIVE`. レベル: `GLOBAL`
    - `JDBC`. レベル: `GLOBAL`
    - `KAFKA`. レベル: `GLOBAL`
    - `MONGO`. レベル: `GLOBAL`
    - `MYSQL`. レベル: `GLOBAL`
    - `NATS`. レベル: `GLOBAL`
    - `ODBC`. レベル: `GLOBAL`
    - `POSTGRES`. レベル: `GLOBAL`
    - `RABBITMQ`. レベル: `GLOBAL`
    - `REDIS`. レベル: `GLOBAL`
    - `REMOTE`. レベル: `GLOBAL`
    - `S3`. レベル: `GLOBAL`
    - `SQLITE`. レベル: `GLOBAL`
    - `URL`. レベル: `GLOBAL`

`SOURCES`権限は、すべてのソースの使用を許可します。また、各ソースに対して個別に権限を付与することもできます。ソースを使用するには、追加の権限が必要です。

例：

- [MySQLテーブルエンジン](../../engines/table-engines/integrations/mysql.md)を使用してテーブルを作成するには、`CREATE TABLE (ON db.table_name)`および`MYSQL`権限が必要です。
- [mysqlテーブル関数](../../sql-reference/table-functions/mysql.md)を使用するには、`CREATE TEMPORARY TABLE`および`MYSQL`権限が必要です。

### dictGet

- `dictGet`. 別名: `dictHas`, `dictGetHierarchy`, `dictIsIn`

ユーザーが[dictGet](../../sql-reference/functions/ext-dict-functions.md#dictget)、[dictHas](../../sql-reference/functions/ext-dict-functions.md#dicthas)、[dictGetHierarchy](../../sql-reference/functions/ext-dict-functions.md#dictgethierarchy)、[dictIsIn](../../sql-reference/functions/ext-dict-functions.md#dictisin)関数を実行することを許可します。

権限レベル: `DICTIONARY`.

**例**

- `GRANT dictGet ON mydb.mydictionary TO john`
- `GRANT dictGet ON mydictionary TO john`

### displaySecretsInShowAndSelect

`SHOW`および`SELECT`クエリ内で秘密を表示することをユーザーに許可します。
[`display_secrets_in_show_and_select`サーバー設定](../../operations/server-configuration-parameters/settings#display_secrets_in_show_and_select)
および
[`format_display_secrets_in_show_and_select`フォーマット設定](../../operations/settings/formats#format_display_secrets_in_show_and_select)
の両方がオンになっている場合。

### NAMED COLLECTION ADMIN

指定された命名コレクションに対する特定の操作を許可します。バージョン23.7以前ではNAMED COLLECTION CONTROLと呼ばれていましたが、23.7以降にはNAMED COLLECTION ADMINが追加され、NAMED COLLECTION CONTROLはエイリアスとして保持されています。

- `NAMED COLLECTION ADMIN`. レベル: `NAMED_COLLECTION`. 別名: `NAMED COLLECTION CONTROL`
    - `CREATE NAMED COLLECTION`. レベル: `NAMED_COLLECTION`
    - `DROP NAMED COLLECTION`. レベル: `NAMED_COLLECTION`
    - `ALTER NAMED COLLECTION`. レベル: `NAMED_COLLECTION`
    - `SHOW NAMED COLLECTIONS`. レベル: `NAMED_COLLECTION`. 別名: `SHOW NAMED COLLECTIONS`
    - `SHOW NAMED COLLECTIONS SECRETS`. レベル: `NAMED_COLLECTION`. 別名: `SHOW NAMED COLLECTIONS SECRETS`
    - `NAMED COLLECTION`. レベル: `NAMED_COLLECTION`. 別名: `NAMED COLLECTION USAGE, USE NAMED COLLECTION`

23.7では、GRANT NAMED COLLECTION以外のすべてのGRANT（CREATE, DROP, ALTER, SHOW）が追加され、23.7以降にのみGRANT NAMED COLLECTIONが追加されました。

**例**

命名コレクションがabcと呼ばれる場合、ユーザーjohnにCREATE NAMED COLLECTIONの権限を付与します。
- `GRANT CREATE NAMED COLLECTION ON abc TO john`

### TABLE ENGINE

テーブルを作成する際に指定されたテーブルエンジンを使用することを許可します。[テーブルエンジン](../../engines/table-engines/index.md)に適用されます。

**例**

- `GRANT TABLE ENGINE ON * TO john`
- `GRANT TABLE ENGINE ON TinyLog TO john`

### ALL

規制対象エンティティに対するすべての権限をユーザーアカウントまたはロールに付与します。

### NONE

いかなる権限も付与しません。

### ADMIN OPTION

`ADMIN OPTION`権限は、ユーザーが自身のロールを他のユーザーに付与することを許可します。
