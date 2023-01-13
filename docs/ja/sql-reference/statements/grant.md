---
machine_translated: true
machine_translated_rev: cbd8aa9052361a7ee11c209560cff7175c2b8e42
toc_priority: 39
toc_title: GRANT
---

# GRANT {#grant}

-   助成金 [特権](#grant-privileges) ClickHouseユーザーアカウントまたはロールへ。
-   員の役割をユーザーアカウントまたはその他の役割です。

権限を取り消すには [REVOKE](../../sql-reference/statements/revoke.md) 声明。 また、付与された権限を [SHOW GRANTS](../../sql-reference/statements/show.md#show-grants-statement) 声明。

## 権限構文の付与 {#grant-privigele-syntax}

``` sql
GRANT [ON CLUSTER cluster_name] privilege[(column_name [,...])] [,...] ON {db.table|db.*|*.*|table|*} TO {user | role | CURRENT_USER} [,...] [WITH GRANT OPTION] [WITH REPLACE OPTION]
```

-   `privilege` — Type of privilege.
-   `role` — ClickHouse user role.
-   `user` — ClickHouse user account.

この `WITH GRANT OPTION` 句の付与 `user` または `role` 実行する許可を得て `GRANT` クエリ。 ユーザーは、持っているスコープとそれ以下の権限を付与できます。
この `WITH REPLACE OPTION` 句は `user`または` role`の新しい特権で古い特権を置き換えます, 指定しない場合は、古い特権を古いものに追加してください

## ロール構文の割り当て {#assign-role-syntax}

``` sql
GRANT [ON CLUSTER cluster_name] role [,...] TO {user | another_role | CURRENT_USER} [,...] [WITH ADMIN OPTION] [WITH REPLACE OPTION]
```

-   `role` — ClickHouse user role.
-   `user` — ClickHouse user account.

この `WITH ADMIN OPTION` 句の付与 [ADMIN OPTION](#admin-option-privilege) への特権 `user` または `role`.
この `WITH REPLACE OPTION` 句は`user`または` role`の新しい役割によって古い役割を置き換えます, 指定しない場合は、古い特権を古いものに追加してください

## 使用法 {#grant-usage}

使用するには `GRANT` アカウントには `GRANT OPTION` 特権だ 権限を付与できるのは、アカウント権限の範囲内でのみです。

たとえば、管理者は `john` クエリによるアカウント:

``` sql
GRANT SELECT(x,y) ON db.table TO john WITH GRANT OPTION
```

つまり `john` 実行する権限があります:

-   `SELECT x,y FROM db.table`.
-   `SELECT x FROM db.table`.
-   `SELECT y FROM db.table`.

`john` 実行できない `SELECT z FROM db.table`. この `SELECT * FROM db.table` また、利用できません。 このクエリを処理すると、ClickHouseはデータを返しません。 `x` そして `y`. 唯一の例外は、テーブルにのみ含まれている場合です `x` そして `y` 列。 この場合、ClickHouseはすべてのデータを返します。

また `john` は、 `GRANT OPTION` そのため、同じまたはより小さいスコープの特権を持つ他のユーザーに付与できます。

アスタリスクを使用できる権限の指定 (`*`)テーブルまたはデータベース名の代わりに。 例えば、 `GRANT SELECT ON db.* TO john` クエリ許可 `john` 実行するには `SELECT` すべてのテーブルに対するクエリ `db` データベース。 また、データベース名を省略できます。 この場合、現在のデータベースに特権が付与されます。 例えば, `GRANT SELECT ON * TO john` 現在のデータベ, `GRANT SELECT ON mytable TO john` の権限を付与します。 `mytable` 現在のデータベース内のテーブル。

へのアクセス `system` databaseは常に許可されます(このデータベースはクエリの処理に使用されるため)。

一つのクエリで複数のアカウントに複数の権限を付与できます。 クエリ `GRANT SELECT, INSERT ON *.* TO john, robin` アカウントを許可 `john` そして `robin` 実行するには `INSERT` そして `SELECT` クエリのテーブルのすべてのデータベースに、サーバーにコピーします。

## 特権 {#grant-privileges}

特権は、特定の種類のクエリを実行する権限です。

権限には階層構造があります。 許可されるクエリのセットは、特権スコープに依存します。

特権の階層:

-   [SELECT](#grant-select)
-   [INSERT](#grant-insert)
-   [ALTER](#grant-alter)
    -   `ALTER TABLE`
        -   `ALTER UPDATE`
        -   `ALTER DELETE`
        -   `ALTER COLUMN`
            -   `ALTER ADD COLUMN`
            -   `ALTER DROP COLUMN`
            -   `ALTER MODIFY COLUMN`
            -   `ALTER COMMENT COLUMN`
            -   `ALTER CLEAR COLUMN`
            -   `ALTER RENAME COLUMN`
        -   `ALTER INDEX`
            -   `ALTER ORDER BY`
            -   `ALTER ADD INDEX`
            -   `ALTER DROP INDEX`
            -   `ALTER MATERIALIZE INDEX`
            -   `ALTER CLEAR INDEX`
        -   `ALTER CONSTRAINT`
            -   `ALTER ADD CONSTRAINT`
            -   `ALTER DROP CONSTRAINT`
        -   `ALTER TTL`
        -   `ALTER MATERIALIZE TTL`
        -   `ALTER SETTINGS`
        -   `ALTER MOVE PARTITION`
        -   `ALTER FETCH PARTITION`
        -   `ALTER FREEZE PARTITION`
    -   `ALTER VIEW`
        -   `ALTER VIEW REFRESH`
        -   `ALTER VIEW MODIFY QUERY`
-   [CREATE](#grant-create)
    -   `CREATE DATABASE`
    -   `CREATE TABLE`
    -   `CREATE VIEW`
    -   `CREATE DICTIONARY`
    -   `CREATE TEMPORARY TABLE`
-   [DROP](#grant-drop)
    -   `DROP DATABASE`
    -   `DROP TABLE`
    -   `DROP VIEW`
    -   `DROP DICTIONARY`
-   [TRUNCATE](#grant-truncate)
-   [OPTIMIZE](#grant-optimize)
-   [SHOW](#grant-show)
    -   `SHOW DATABASES`
    -   `SHOW TABLES`
    -   `SHOW COLUMNS`
    -   `SHOW DICTIONARIES`
-   [KILL QUERY](#grant-kill-query)
-   [ACCESS MANAGEMENT](#grant-access-management)
    -   `CREATE USER`
    -   `ALTER USER`
    -   `DROP USER`
    -   `CREATE ROLE`
    -   `ALTER ROLE`
    -   `DROP ROLE`
    -   `CREATE ROW POLICY`
    -   `ALTER ROW POLICY`
    -   `DROP ROW POLICY`
    -   `CREATE QUOTA`
    -   `ALTER QUOTA`
    -   `DROP QUOTA`
    -   `CREATE SETTINGS PROFILE`
    -   `ALTER SETTINGS PROFILE`
    -   `DROP SETTINGS PROFILE`
    -   `SHOW ACCESS`
        -   `SHOW_USERS`
        -   `SHOW_ROLES`
        -   `SHOW_ROW_POLICIES`
        -   `SHOW_QUOTAS`
        -   `SHOW_SETTINGS_PROFILES`
    -   `ROLE ADMIN`
-   [SYSTEM](#grant-system)
    -   `SYSTEM SHUTDOWN`
    -   `SYSTEM DROP CACHE`
        -   `SYSTEM DROP DNS CACHE`
        -   `SYSTEM DROP MARK CACHE`
        -   `SYSTEM DROP UNCOMPRESSED CACHE`
    -   `SYSTEM RELOAD`
        -   `SYSTEM RELOAD CONFIG`
        -   `SYSTEM RELOAD DICTIONARY`
        -   `SYSTEM RELOAD EMBEDDED DICTIONARIES`
    -   `SYSTEM MERGES`
    -   `SYSTEM TTL MERGES`
    -   `SYSTEM FETCHES`
    -   `SYSTEM MOVES`
    -   `SYSTEM SENDS`
        -   `SYSTEM DISTRIBUTED SENDS`
        -   `SYSTEM REPLICATED SENDS`
    -   `SYSTEM REPLICATION QUEUES`
    -   `SYSTEM SYNC REPLICA`
    -   `SYSTEM RESTART REPLICA`
    -   `SYSTEM FLUSH`
        -   `SYSTEM FLUSH DISTRIBUTED`
        -   `SYSTEM FLUSH LOGS`
-   [INTROSPECTION](#grant-introspection)
    -   `addressToLine`
    -   `addressToSymbol`
    -   `demangle`
-   [SOURCES](#grant-sources)
    -   `FILE`
    -   `URL`
    -   `REMOTE`
    -   `YSQL`
    -   `ODBC`
    -   `JDBC`
    -   `HDFS`
    -   `S3`
-   [dictGet](#grant-dictget)

この階層がどのように扱われるかの例:

-   この `ALTER` 特典を含む他のすべての `ALTER*` 特権だ
-   `ALTER CONSTRAINT` 含む `ALTER ADD CONSTRAINT` そして `ALTER DROP CONSTRAINT` 特権だ

特権は異なるレベルで適用されます。 レベルを知ることは、特権に利用可能な構文を示唆しています。

レベル（下位から上位へ):

-   `COLUMN` — Privilege can be granted for column, table, database, or globally.
-   `TABLE` — Privilege can be granted for table, database, or globally.
-   `VIEW` — Privilege can be granted for view, database, or globally.
-   `DICTIONARY` — Privilege can be granted for dictionary, database, or globally.
-   `DATABASE` — Privilege can be granted for database or globally.
-   `GLOBAL` — Privilege can be granted only globally.
-   `GROUP` — Groups privileges of different levels. When `GROUP`-レベルの特権が付与され、使用される構文に対応するグループからの特権のみが付与されます。

許可される構文の例:

-   `GRANT SELECT(x) ON db.table TO user`
-   `GRANT SELECT ON db.* TO user`

禁止された構文の例:

-   `GRANT CREATE USER(x) ON db.table TO user`
-   `GRANT CREATE USER ON db.* TO user`

特別特典 [ALL](#grant-all) ユーザーアカウントまたはロールにすべての権限を付与します。

既定では、ユーザーアカウントまたはロールには特権はありません。

ユーザーまたはロールに権限がない場合は、次のように表示されます [NONE](#grant-none) 特権だ

実装によるクエリには、一連の特権が必要です。 たとえば、 [RENAME](../../sql-reference/statements/misc.md#misc_operations-rename) クエリには次の権限が必要です: `SELECT`, `CREATE TABLE`, `INSERT` そして `DROP TABLE`.

### SELECT {#grant-select}

実行を許可する [SELECT](../../sql-reference/statements/select/index.md) クエリ。

特権レベル: `COLUMN`.

**説明**

ユーザーの交付を受けるこの権限での実行 `SELECT` 指定した表およびデータベース内の指定した列のリストに対するクエリ。 ユーザーが他の列を含む場合、クエリはデータを返しません。

次の特権を考慮してください:

``` sql
GRANT SELECT(x,y) ON db.table TO john
```

この特権は `john` 実行するには `SELECT` データを含むクエリ `x` および/または `y` の列 `db.table` 例えば, `SELECT x FROM db.table`. `john` 実行できない `SELECT z FROM db.table`. この `SELECT * FROM db.table` また、利用できません。 このクエリを処理すると、ClickHouseはデータを返しません。 `x` そして `y`. 唯一の例外は、テーブルにのみ含まれている場合です `x` そして `y` この場合、ClickHouseはすべてのデータを返します。

### INSERT {#grant-insert}

実行を許可する [INSERT](../../sql-reference/statements/insert-into.md) クエリ。

特権レベル: `COLUMN`.

**説明**

ユーザーの交付を受けるこの権限での実行 `INSERT` 指定した表およびデータベース内の指定した列のリストに対するクエリ。 ユーザーが他の列を含む場合、指定されたクエリはデータを挿入しません。

**例**

``` sql
GRANT INSERT(x,y) ON db.table TO john
```

許可された特権は `john` にデータを挿入するには `x` および/または `y` の列 `db.table`.

### ALTER {#grant-alter}

実行を許可する [ALTER](../../sql-reference/statements/alter.md) 次の特権の階層に従ってクエリを実行します:

-   `ALTER`. レベル: `COLUMN`.
    -   `ALTER TABLE`. レベル: `GROUP`
        -   `ALTER UPDATE`. レベル: `COLUMN`. 別名: `UPDATE`
        -   `ALTER DELETE`. レベル: `COLUMN`. 別名: `DELETE`
        -   `ALTER COLUMN`. レベル: `GROUP`
            -   `ALTER ADD COLUMN`. レベル: `COLUMN`. 別名: `ADD COLUMN`
            -   `ALTER DROP COLUMN`. レベル: `COLUMN`. 別名: `DROP COLUMN`
            -   `ALTER MODIFY COLUMN`. レベル: `COLUMN`. 別名: `MODIFY COLUMN`
            -   `ALTER COMMENT COLUMN`. レベル: `COLUMN`. 別名: `COMMENT COLUMN`
            -   `ALTER CLEAR COLUMN`. レベル: `COLUMN`. 別名: `CLEAR COLUMN`
            -   `ALTER RENAME COLUMN`. レベル: `COLUMN`. 別名: `RENAME COLUMN`
        -   `ALTER INDEX`. レベル: `GROUP`. 別名: `INDEX`
            -   `ALTER ORDER BY`. レベル: `TABLE`. 別名: `ALTER MODIFY ORDER BY`, `MODIFY ORDER BY`
            -   `ALTER ADD INDEX`. レベル: `TABLE`. 別名: `ADD INDEX`
            -   `ALTER DROP INDEX`. レベル: `TABLE`. 別名: `DROP INDEX`
            -   `ALTER MATERIALIZE INDEX`. レベル: `TABLE`. 別名: `MATERIALIZE INDEX`
            -   `ALTER CLEAR INDEX`. レベル: `TABLE`. 別名: `CLEAR INDEX`
        -   `ALTER CONSTRAINT`. レベル: `GROUP`. 別名: `CONSTRAINT`
            -   `ALTER ADD CONSTRAINT`. レベル: `TABLE`. 別名: `ADD CONSTRAINT`
            -   `ALTER DROP CONSTRAINT`. レベル: `TABLE`. 別名: `DROP CONSTRAINT`
        -   `ALTER TTL`. レベル: `TABLE`. 別名: `ALTER MODIFY TTL`, `MODIFY TTL`
        -   `ALTER MATERIALIZE TTL`. レベル: `TABLE`. 別名: `MATERIALIZE TTL`
        -   `ALTER SETTINGS`. レベル: `TABLE`. 別名: `ALTER SETTING`, `ALTER MODIFY SETTING`, `MODIFY SETTING`
        -   `ALTER MOVE PARTITION`. レベル: `TABLE`. 別名: `ALTER MOVE PART`, `MOVE PARTITION`, `MOVE PART`
        -   `ALTER FETCH PARTITION`. レベル: `TABLE`. 別名: `FETCH PARTITION`
        -   `ALTER FREEZE PARTITION`. レベル: `TABLE`. 別名: `FREEZE PARTITION`
    -   `ALTER VIEW` レベル: `GROUP`
        -   `ALTER VIEW REFRESH`. レベル: `VIEW`. 別名: `ALTER LIVE VIEW REFRESH`, `REFRESH VIEW`
        -   `ALTER VIEW MODIFY QUERY`. レベル: `VIEW`. 別名: `ALTER TABLE MODIFY QUERY`

この階層がどのように扱われるかの例:

-   この `ALTER` 特典を含む他のすべての `ALTER*` 特権だ
-   `ALTER CONSTRAINT` 含む `ALTER ADD CONSTRAINT` そして `ALTER DROP CONSTRAINT` 特権だ

**ノート**

-   この `MODIFY SETTING` 権限を変更できるテーブルエンジンを設定します。 設定やサーバー構成パラメーターには影響しません。
-   この `ATTACH` 操作は必要とします [CREATE](#grant-create) 特権だ
-   この `DETACH` 操作は必要とします [DROP](#grant-drop) 特権だ
-   によって突然変異を停止するには [KILL MUTATION](../../sql-reference/statements/misc.md#kill-mutation) クエリ、あなたはこの突然変異を開始する権限を持っている必要があります。 たとえば、 `ALTER UPDATE` 問い合わせ、必要とします `ALTER UPDATE`, `ALTER TABLE`,または `ALTER` 特権だ

### CREATE {#grant-create}

実行を許可する [CREATE](../../sql-reference/statements/create.md) そして [ATTACH](../../sql-reference/statements/misc.md#attach) DDL-次の特権の階層に従ったクエリ:

-   `CREATE`. レベル: `GROUP`
    -   `CREATE DATABASE`. レベル: `DATABASE`
    -   `CREATE TABLE`. レベル: `TABLE`
    -   `CREATE VIEW`. レベル: `VIEW`
    -   `CREATE DICTIONARY`. レベル: `DICTIONARY`
    -   `CREATE TEMPORARY TABLE`. レベル: `GLOBAL`

**ノート**

-   削除し、作成したテーブルは、ユーザーニーズ [DROP](#grant-drop).

### DROP {#grant-drop}

実行を許可する [DROP](../../sql-reference/statements/misc.md#drop) そして [DETACH](../../sql-reference/statements/misc.md#detach) 次の特権の階層に従ってクエリを実行します:

-   `DROP`. レベル:
    -   `DROP DATABASE`. レベル: `DATABASE`
    -   `DROP TABLE`. レベル: `TABLE`
    -   `DROP VIEW`. レベル: `VIEW`
    -   `DROP DICTIONARY`. レベル: `DICTIONARY`

### TRUNCATE {#grant-truncate}

実行を許可する [TRUNCATE](../../sql-reference/statements/misc.md#truncate-statement) クエリ。

特権レベル: `TABLE`.

### OPTIMIZE {#grant-optimize}

実行を許可する [OPTIMIZE TABLE](../../sql-reference/statements/misc.md#misc_operations-optimize) クエリ。

特権レベル: `TABLE`.

### SHOW {#grant-show}

実行を許可する `SHOW`, `DESCRIBE`, `USE`,and `EXISTS` 次の特権の階層に従ってクエリを実行します:

-   `SHOW`. レベル: `GROUP`
    -   `SHOW DATABASES`. レベル: `DATABASE`. 実行を許可する `SHOW DATABASES`, `SHOW CREATE DATABASE`, `USE <database>` クエリ。
    -   `SHOW TABLES`. レベル: `TABLE`. 実行を許可する `SHOW TABLES`, `EXISTS <table>`, `CHECK <table>` クエリ。
    -   `SHOW COLUMNS`. レベル: `COLUMN`. 実行を許可する `SHOW CREATE TABLE`, `DESCRIBE` クエリ。
    -   `SHOW DICTIONARIES`. レベル: `DICTIONARY`. 実行を許可する `SHOW DICTIONARIES`, `SHOW CREATE DICTIONARY`, `EXISTS <dictionary>` クエリ。

**ノート**

ユーザは、 `SHOW` 特典の場合は、その他の特典に関する指定されたテーブル、辞書やデータベースです。

### KILL QUERY {#grant-kill-query}

実行を許可する [KILL](../../sql-reference/statements/misc.md#kill-query-statement) 次の特権の階層に従ってクエリを実行します:

特権レベル: `GLOBAL`.

**ノート**

`KILL QUERY` 特典でユーザを殺クエリのその他のユーザー

### ACCESS MANAGEMENT {#grant-access-management}

ユーザー、ロール、および行ポリシーを管理するクエリを実行できます。

-   `ACCESS MANAGEMENT`. レベル: `GROUP`
    -   `CREATE USER`. レベル: `GLOBAL`
    -   `ALTER USER`. レベル: `GLOBAL`
    -   `DROP USER`. レベル: `GLOBAL`
    -   `CREATE ROLE`. レベル: `GLOBAL`
    -   `ALTER ROLE`. レベル: `GLOBAL`
    -   `DROP ROLE`. レベル: `GLOBAL`
    -   `ROLE ADMIN`. レベル: `GLOBAL`
    -   `CREATE ROW POLICY`. レベル: `GLOBAL`. 別名: `CREATE POLICY`
    -   `ALTER ROW POLICY`. レベル: `GLOBAL`. 別名: `ALTER POLICY`
    -   `DROP ROW POLICY`. レベル: `GLOBAL`. 別名: `DROP POLICY`
    -   `CREATE QUOTA`. レベル: `GLOBAL`
    -   `ALTER QUOTA`. レベル: `GLOBAL`
    -   `DROP QUOTA`. レベル: `GLOBAL`
    -   `CREATE SETTINGS PROFILE`. レベル: `GLOBAL`. 別名: `CREATE PROFILE`
    -   `ALTER SETTINGS PROFILE`. レベル: `GLOBAL`. 別名: `ALTER PROFILE`
    -   `DROP SETTINGS PROFILE`. レベル: `GLOBAL`. 別名: `DROP PROFILE`
    -   `SHOW ACCESS`. レベル: `GROUP`
        -   `SHOW_USERS`. レベル: `GLOBAL`. 別名: `SHOW CREATE USER`
        -   `SHOW_ROLES`. レベル: `GLOBAL`. 別名: `SHOW CREATE ROLE`
        -   `SHOW_ROW_POLICIES`. レベル: `GLOBAL`. 別名: `SHOW POLICIES`, `SHOW CREATE ROW POLICY`, `SHOW CREATE POLICY`
        -   `SHOW_QUOTAS`. レベル: `GLOBAL`. 別名: `SHOW CREATE QUOTA`
        -   `SHOW_SETTINGS_PROFILES`. レベル: `GLOBAL`. 別名: `SHOW PROFILES`, `SHOW CREATE SETTINGS PROFILE`, `SHOW CREATE PROFILE`

この `ROLE ADMIN` 特典できるユーザーに割り当ておよび取り消す為の役割を含めるものではありませんので、ユーザーの管理のオプションです。

### SYSTEM {#grant-system}

ユーザーに実行を許可する [SYSTEM](../../sql-reference/statements/system.md) 次の特権の階層に従ってクエリを実行します。

-   `SYSTEM`. レベル: `GROUP`
    -   `SYSTEM SHUTDOWN`. レベル: `GLOBAL`. 別名: `SYSTEM KILL`, `SHUTDOWN`
    -   `SYSTEM DROP CACHE`. 別名: `DROP CACHE`
        -   `SYSTEM DROP DNS CACHE`. レベル: `GLOBAL`. 別名: `SYSTEM DROP DNS`, `DROP DNS CACHE`, `DROP DNS`
        -   `SYSTEM DROP MARK CACHE`. レベル: `GLOBAL`. 別名: `SYSTEM DROP MARK`, `DROP MARK CACHE`, `DROP MARKS`
        -   `SYSTEM DROP UNCOMPRESSED CACHE`. レベル: `GLOBAL`. 別名: `SYSTEM DROP UNCOMPRESSED`, `DROP UNCOMPRESSED CACHE`, `DROP UNCOMPRESSED`
    -   `SYSTEM RELOAD`. レベル: `GROUP`
        -   `SYSTEM RELOAD CONFIG`. レベル: `GLOBAL`. 別名: `RELOAD CONFIG`
        -   `SYSTEM RELOAD DICTIONARY`. レベル: `GLOBAL`. 別名: `SYSTEM RELOAD DICTIONARIES`, `RELOAD DICTIONARY`, `RELOAD DICTIONARIES`
        -   `SYSTEM RELOAD EMBEDDED DICTIONARIES`. レベル: `GLOBAL`. 別名:R`ELOAD EMBEDDED DICTIONARIES`
    -   `SYSTEM MERGES`. レベル: `TABLE`. 別名: `SYSTEM STOP MERGES`, `SYSTEM START MERGES`, `STOP MERGES`, `START MERGES`
    -   `SYSTEM TTL MERGES`. レベル: `TABLE`. 別名: `SYSTEM STOP TTL MERGES`, `SYSTEM START TTL MERGES`, `STOP TTL MERGES`, `START TTL MERGES`
    -   `SYSTEM FETCHES`. レベル: `TABLE`. 別名: `SYSTEM STOP FETCHES`, `SYSTEM START FETCHES`, `STOP FETCHES`, `START FETCHES`
    -   `SYSTEM MOVES`. レベル: `TABLE`. 別名: `SYSTEM STOP MOVES`, `SYSTEM START MOVES`, `STOP MOVES`, `START MOVES`
    -   `SYSTEM SENDS`. レベル: `GROUP`. 別名: `SYSTEM STOP SENDS`, `SYSTEM START SENDS`, `STOP SENDS`, `START SENDS`
        -   `SYSTEM DISTRIBUTED SENDS`. レベル: `TABLE`. 別名: `SYSTEM STOP DISTRIBUTED SENDS`, `SYSTEM START DISTRIBUTED SENDS`, `STOP DISTRIBUTED SENDS`, `START DISTRIBUTED SENDS`
        -   `SYSTEM REPLICATED SENDS`. レベル: `TABLE`. 別名: `SYSTEM STOP REPLICATED SENDS`, `SYSTEM START REPLICATED SENDS`, `STOP REPLICATED SENDS`, `START REPLICATED SENDS`
    -   `SYSTEM REPLICATION QUEUES`. レベル: `TABLE`. 別名: `SYSTEM STOP REPLICATION QUEUES`, `SYSTEM START REPLICATION QUEUES`, `STOP REPLICATION QUEUES`, `START REPLICATION QUEUES`
    -   `SYSTEM SYNC REPLICA`. レベル: `TABLE`. 別名: `SYNC REPLICA`
    -   `SYSTEM RESTART REPLICA`. レベル: `TABLE`. 別名: `RESTART REPLICA`
    -   `SYSTEM FLUSH`. レベル: `GROUP`
        -   `SYSTEM FLUSH DISTRIBUTED`. レベル: `TABLE`. 別名: `FLUSH DISTRIBUTED`
        -   `SYSTEM FLUSH LOGS`. レベル: `GLOBAL`. 別名: `FLUSH LOGS`

この `SYSTEM RELOAD EMBEDDED DICTIONARIES` によって暗黙的に付与される特権 `SYSTEM RELOAD DICTIONARY ON *.*` 特権だ

### INTROSPECTION {#grant-introspection}

使用を許可する [内省](../../operations/optimizing-performance/sampling-query-profiler.md) 機能。

-   `INTROSPECTION`. レベル: `GROUP`. 別名: `INTROSPECTION FUNCTIONS`
    -   `addressToLine`. レベル: `GLOBAL`
    -   `addressToSymbol`. レベル: `GLOBAL`
    -   `demangle`. レベル: `GLOBAL`

### SOURCES {#grant-sources}

外部データソースの使用を許可します。 に適用されます [表エンジン](../../engines/table-engines/index.md) そして [テーブル関数](../../sql-reference/table-functions/index.md#table-functions).

-   `SOURCES`. レベル: `GROUP`
    -   `FILE`. レベル: `GLOBAL`
    -   `URL`. レベル: `GLOBAL`
    -   `REMOTE`. レベル: `GLOBAL`
    -   `YSQL`. レベル: `GLOBAL`
    -   `ODBC`. レベル: `GLOBAL`
    -   `JDBC`. レベル: `GLOBAL`
    -   `HDFS`. レベル: `GLOBAL`
    -   `S3`. レベル: `GLOBAL`

この `SOURCES` 特権は、すべてのソースの使用を可能にします。 また、各ソースに個別に権限を付与することもできます。 ソースを使用するには、追加の権限が必要です。

例:

-   テーブルを作成するには [MySQLテーブルエンジン](../../engines/table-engines/integrations/mysql.md)、必要とします `CREATE TABLE (ON db.table_name)` そして `MYSQL` 特権だ
-   を使用するには [mysqlテーブル関数](../../sql-reference/table-functions/mysql.md)、必要とします `CREATE TEMPORARY TABLE` そして `MYSQL` 特権だ

### dictGet {#grant-dictget}

-   `dictGet`. 別名: `dictHas`, `dictGetHierarchy`, `dictIsIn`

ユーザーに実行を許可する [dictGet](../../sql-reference/functions/ext-dict-functions.md#dictget), [ディクタス](../../sql-reference/functions/ext-dict-functions.md#dicthas), [dictGetHierarchy](../../sql-reference/functions/ext-dict-functions.md#dictgethierarchy), [ジクチシン](../../sql-reference/functions/ext-dict-functions.md#dictisin) 機能。

特権レベル: `DICTIONARY`.

**例**

-   `GRANT dictGet ON mydb.mydictionary TO john`
-   `GRANT dictGet ON mydictionary TO john`

### ALL {#grant-all}

助成金の全ての権限を規制組織のユーザーアカウントまたは役割を担う。

### NONE {#grant-none}

権限は付与されません。

### ADMIN OPTION {#admin-option-privilege}

この `ADMIN OPTION` 特典できるユーザー補助金の役割を他のユーザーです。

[元の記事](https://clickhouse.com/docs/en/query_language/grant/) <!--hide-->
