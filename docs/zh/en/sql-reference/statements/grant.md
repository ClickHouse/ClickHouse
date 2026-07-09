---
description: 'GRANT 语句说明'
sidebar_label: 'GRANT'
sidebar_position: 38
slug: /sql-reference/statements/grant
title: 'GRANT 语句'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="grant-statement">
  # GRANT 语句
</div>

* 向 ClickHouse 用户账户或角色授予[特权](#privileges)。
* 将角色分配给用户账户或其他角色。

要撤销特权，请使用 [REVOKE](../../sql-reference/statements/revoke.md) 语句。你还可以使用 [SHOW GRANTS](../../sql-reference/statements/show.md#show-grants) 语句列出已授予的特权。

<div id="granting-privilege-syntax">
  ## 授予权限语法
</div>

```sql
GRANT [ON CLUSTER cluster_name] privilege[(column_name [,...])] [,...] ON {db.table[*]|db[*].*|*.*|table[*]|*} TO {user | role | CURRENT_USER} [,...] [WITH GRANT OPTION] [WITH REPLACE OPTION]
```

* `privilege` — 特权类型。
* `role` — ClickHouse 用户角色。
* `user` — ClickHouse 用户账户。

`WITH GRANT OPTION` 子句会向 `user` 或 `role` 授予执行 `GRANT` 查询的权限。用户可以授予与自己所拥有范围相同或更小的特权。
`WITH REPLACE OPTION` 子句会用新特权替换 `user` 或 `role` 的旧特权；如果未指定，则会追加特权。

<div id="assigning-role-syntax">
  ## 授予角色语法
</div>

```sql
GRANT [ON CLUSTER cluster_name] role [,...] TO {user | another_role | CURRENT_USER} [,...] [WITH ADMIN OPTION] [WITH REPLACE OPTION]
```

* `role` — ClickHouse 角色。
* `user` — ClickHouse 用户账户。

`WITH ADMIN OPTION` 子句会将 [ADMIN OPTION](#admin-option) 特权授予 `user` 或 `role`。
`WITH REPLACE OPTION` 子句会为 `user` 或 `role` 用新角色替换旧角色；如果未指定，则会追加角色。

<div id="grant-current-grants-syntax">
  ## GRANT CURRENT GRANTS 语法
</div>

```sql
GRANT CURRENT GRANTS{(privilege[(column_name [,...])] [,...] ON {db.table|db.*|*.*|table|*}) | ON {db.table|db.*|*.*|table|*}} TO {user | role | CURRENT_USER} [,...] [WITH GRANT OPTION] [WITH REPLACE OPTION]
```

* `privilege` — 特权类型。
* `role` — ClickHouse 用户角色。
* `user` — ClickHouse 用户账户。

使用 `CURRENT GRANTS` 语句可将所有指定的特权授予给定用户或角色。
如果未指定任何特权，则给定用户或角色将获得 `CURRENT_USER` 的所有可用特权。

<div id="usage">
  ## 用法
</div>

要使用 `GRANT`，你的账户必须拥有 `GRANT OPTION` 特权。你只能在自身账户特权范围内授予特权。

例如，管理员已通过以下查询向 `john` 账户授予特权：

```sql
GRANT SELECT(x,y) ON db.table TO john WITH GRANT OPTION
```

这意味着 `john` 有权执行：

* `SELECT x,y FROM db.table`。
* `SELECT x FROM db.table`。
* `SELECT y FROM db.table`。

`john` 不能执行 `SELECT z FROM db.table`。也不能执行 `SELECT * FROM db.table`。执行该查询时，ClickHouse 不会返回任何数据，连 `x` 和 `y` 也不例外。唯一的例外是表中仅包含 `x` 和 `y` 两列。在这种情况下，ClickHouse 会返回全部数据。

此外，`john` 还具有 `GRANT OPTION` 特权，因此可以将相同或更小范围的特权授予其他用户。

始终允许访问 `system` 数据库 (因为该数据库用于处理查询) 。

:::note
虽然新用户默认可以访问许多系统表，但如果没有授权，默认情况下他们可能无法访问所有系统表。
此外，出于安全原因，Cloud 用户访问某些系统表 (例如 `system.zookeeper`) 会受到限制。
:::

你可以在一条查询中向多个账户授予多项特权。查询 `GRANT SELECT, INSERT ON *.* TO john, robin` 允许账户 `john` 和 `robin` 对服务器上所有数据库中的所有表执行 `INSERT` 和 `SELECT` 查询。

<div id="wildcard-grants">
  ## 通配符授权
</div>

指定权限时，可以使用星号 (`*`) 代替表名或数据库名。例如，`GRANT SELECT ON db.* TO john` 查询会授予 `john` 对 `db` 数据库中所有表执行 `SELECT` 查询的权限。
你也可以省略数据库名。在这种情况下，权限将授予当前数据库。
例如，`GRANT SELECT ON * TO john` 会授予 `john` 对当前数据库中所有表执行 `SELECT` 查询的权限，`GRANT SELECT ON mytable TO john` 会授予 `john` 对当前数据库中 `mytable` 表执行 `SELECT` 查询的权限。

:::note
以下功能从 ClickHouse 24.10 版本开始可用。
:::

你还可以将星号放在表名或数据库名的末尾。此功能允许你根据表路径的抽象前缀授予权限。
示例：`GRANT SELECT ON db.my_tables* TO john`。该查询允许 `john` 对 `db` 数据库中所有以前缀 `my_tables` 开头的表执行 `SELECT` 查询。

更多示例：

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

在已获授权路径内新创建的所有表，都会自动继承其父级的全部授权。
例如，如果你执行 `GRANT SELECT ON db.* TO john` 查询，然后创建一个新表 `db.new_table`，用户 `john` 就可以执行 `SELECT * FROM db.new_table` 查询。

你**只能**为前缀指定星号：

```sql
GRANT SELECT ON db.* TO john -- correct
GRANT SELECT ON db*.* TO john -- correct

GRANT SELECT ON *.my_table TO john -- wrong
GRANT SELECT ON foo*bar TO john -- wrong
GRANT SELECT ON *suffix TO john -- wrong
GRANT SELECT(foo) ON db.table* TO john -- wrong
```

<div id="privileges">
  ## 特权
</div>

特权是授予用户的某种权限，用于执行特定类型的查询。

特权具有层级结构，允许执行哪些查询取决于特权的作用范围。

ClickHouse 中的特权层级结构如下所示：

* [`ALL`](#all)
  * [`访问管理`](#access-management)
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
  * [`内部信息`](#introspection)
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
  * [`SET DEFINER`](/zh/sql-reference/statements/create/view#sql_security)
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

此层级结构的处理方式示例如下：

* `ALTER` 特权包含所有其他 `ALTER*` 特权。
* `ALTER CONSTRAINT` 包含 `ALTER ADD CONSTRAINT`、`ALTER DROP CONSTRAINT` 和 `ALTER MODIFY CONSTRAINT` 特权。

特权可在不同级别上应用。了解各个级别有助于判断特权可使用的语法。

级别 (从低到高) ：

* `COLUMN` — 特权可授予列、表、数据库或全局。
* `TABLE` — 特权可授予表、数据库或全局。
* `VIEW` — 特权可授予视图、数据库或全局。
* `DICTIONARY` — 特权可授予字典、数据库或全局。
* `DATABASE` — 特权可授予数据库或全局。
* `GLOBAL` — 特权只能在全局级别授予。
* `GROUP` — 将不同级别的特权分组。授予 `GROUP` 级别的特权时，只会授予该组中与所用语法对应的那些特权。

允许的语法示例：

* `GRANT SELECT(x) ON db.table TO user`
* `GRANT SELECT ON db.* TO user`

不允许的语法示例：

* `GRANT CREATE USER(x) ON db.table TO user`
* `GRANT CREATE USER ON db.* TO user`

特殊特权 [ALL](#all) 会将所有特权授予某个用户账户或角色。

默认情况下，用户账户或角色没有任何特权。

如果用户或角色没有任何特权，则会显示为 [NONE](#none) 特权。

某些查询根据其实现方式需要一组特权。例如，要执行 [RENAME](../../sql-reference/statements/optimize.md) 查询，你需要以下特权：`SELECT`、`CREATE TABLE`、`INSERT` 和 `DROP TABLE`。

<div id="select">
  ### SELECT
</div>

允许执行 [SELECT](../../sql-reference/statements/select/index.md) 查询。

特权级别：`COLUMN`。

**说明**

被授予此特权的用户可以对指定数据库中指定表的指定列执行 `SELECT` 查询。如果用户在查询中包含了未指定的其他列，则查询不会返回任何数据。

请看以下特权：

```sql
GRANT SELECT(x,y) ON db.table TO john
```

此特权允许 `john` 执行任何引用 `db.table` 中 `x` 和/或 `y` 列数据的 `SELECT` 查询，例如 `SELECT x FROM db.table`。`john` 不能执行 `SELECT z FROM db.table`。同样，`SELECT * FROM db.table` 也不可执行。处理此查询时，ClickHouse 不会返回任何数据，包括 `x` 和 `y` 也不会返回。唯一的例外是表中只有 `x` 和 `y` 两列时，在这种情况下 ClickHouse 会返回全部数据。

<div id="insert">
  ### INSERT
</div>

允许执行 [INSERT](../../sql-reference/statements/insert-into.md) 查询。

特权级别：`COLUMN`。

**说明**

被授予此特权的用户，可以对指定数据库和表中的指定列执行 `INSERT` 查询。如果用户包含了未指定的其他列，则该查询不会插入任何数据。

**示例**

```sql
GRANT INSERT(x,y) ON db.table TO john
```

所授予的权限允许 `john` 向 `db.table` 的 `x` 和/或 `y` 列中插入数据。

<div id="alter">
  ### ALTER
</div>

允许按照以下特权层级执行 [ALTER](../../sql-reference/statements/alter/index.md) 查询：

* `ALTER`。级别：`COLUMN`。
  * `ALTER TABLE`。级别：`GROUP`
  * `ALTER UPDATE`。级别：`COLUMN`。别名：`UPDATE`
  * `ALTER DELETE`。级别：`COLUMN`。别名：`DELETE`
  * `ALTER COLUMN`。级别：`GROUP`
  * `ALTER ADD COLUMN`。级别：`COLUMN`。别名：`ADD COLUMN`
  * `ALTER DROP COLUMN`。级别：`COLUMN`。别名：`DROP COLUMN`
  * `ALTER MODIFY COLUMN`。级别：`COLUMN`。别名：`MODIFY COLUMN`
  * `ALTER COMMENT COLUMN`。级别：`COLUMN`。别名：`COMMENT COLUMN`
  * `ALTER CLEAR COLUMN`。级别：`COLUMN`。别名：`CLEAR COLUMN`
  * `ALTER RENAME COLUMN`。级别：`COLUMN`。别名：`RENAME COLUMN`
  * `ALTER INDEX`。级别：`GROUP`。别名：`INDEX`
  * `ALTER ORDER BY`。级别：`TABLE`。别名：`ALTER MODIFY ORDER BY`、`MODIFY ORDER BY`
  * `ALTER SAMPLE BY`。级别：`TABLE`。别名：`ALTER MODIFY SAMPLE BY`、`MODIFY SAMPLE BY`
  * `ALTER ADD INDEX`。级别：`TABLE`。别名：`ADD INDEX`
  * `ALTER DROP INDEX`。级别：`TABLE`。别名：`DROP INDEX`
  * `ALTER MATERIALIZE INDEX`。级别：`TABLE`。别名：`MATERIALIZE INDEX`
  * `ALTER CLEAR INDEX`。级别：`TABLE`。别名：`CLEAR INDEX`
  * `ALTER CONSTRAINT`。级别：`GROUP`。别名：`CONSTRAINT`
  * `ALTER ADD CONSTRAINT`。级别：`TABLE`。别名：`ADD CONSTRAINT`
  * `ALTER DROP CONSTRAINT`。级别：`TABLE`。别名：`DROP CONSTRAINT`
  * `ALTER MODIFY CONSTRAINT`。级别：`TABLE`。别名：`MODIFY CONSTRAINT`
  * `ALTER TTL`。级别：`TABLE`。别名：`ALTER MODIFY TTL`、`MODIFY TTL`
  * `ALTER MATERIALIZE TTL`。级别：`TABLE`。别名：`MATERIALIZE TTL`
  * `ALTER SETTINGS`。级别：`TABLE`。别名：`ALTER SETTING`、`ALTER MODIFY SETTING`、`MODIFY SETTING`
  * `ALTER MOVE PARTITION`。级别：`TABLE`。别名：`ALTER MOVE PART`、`MOVE PARTITION`、`MOVE PART`
  * `ALTER FETCH PARTITION`。级别：`TABLE`。别名：`ALTER FETCH PART`、`FETCH PARTITION`、`FETCH PART`
  * `ALTER FREEZE PARTITION`。级别：`TABLE`。别名：`FREEZE PARTITION`
  * `ALTER EXECUTE`。级别：`TABLE`。别名：`ALTER TABLE EXECUTE`
  * `ALTER VIEW`。级别：`GROUP`
  * `ALTER VIEW REFRESH`。级别：`VIEW`。别名：`REFRESH VIEW`
  * `ALTER VIEW MODIFY QUERY`。级别：`VIEW`。别名：`ALTER TABLE MODIFY QUERY`
  * `ALTER VIEW MODIFY SQL SECURITY`。级别：`VIEW`。别名：`ALTER TABLE MODIFY SQL SECURITY`

此层级的处理方式示例如下：

* `ALTER` 特权包含所有其他 `ALTER*` 特权。
* `ALTER CONSTRAINT` 特权包含 `ALTER ADD CONSTRAINT`、`ALTER DROP CONSTRAINT` 和 `ALTER MODIFY CONSTRAINT` 特权。

**注意**

* `MODIFY SETTING` 特权允许修改表引擎设置。它不会影响其他设置或服务器配置参数。
* `ATTACH` 操作需要 [CREATE](#create) 特权。
* `DETACH` 操作需要 [DROP](#drop) 特权。
* 若要通过 [KILL MUTATION](../../sql-reference/statements/kill.md#kill-mutation) 查询停止某个变更，你需要具备启动该变更所需的特权。例如，如果你想停止 `ALTER UPDATE` 查询，则需要拥有 `ALTER UPDATE`、`ALTER TABLE` 或 `ALTER` 特权。

<div id="backup">
  ### BACKUP
</div>

允许在查询中执行 [`BACKUP`]。有关 backups 的更多信息，请参见[&quot;Backup and Restore&quot;](/zh/operations/backup/overview)。

<div id="create">
  ### CREATE
</div>

允许按照以下特权层级执行 [CREATE](../../sql-reference/statements/create/index.md) 和 [ATTACH](../../sql-reference/statements/attach.md) DDL 查询：

* `CREATE`。级别：`GROUP`
  * `CREATE DATABASE`。级别：`DATABASE`
  * `CREATE TABLE`。级别：`TABLE`
    * `CREATE ARBITRARY TEMPORARY TABLE`。级别：`GLOBAL`
      * `CREATE TEMPORARY TABLE`。级别：`GLOBAL`
  * `CREATE VIEW`。级别：`VIEW`
  * `CREATE DICTIONARY`。级别：`DICTIONARY`

**注意**

* 要删除已创建的表，用户需要 [DROP](#drop) 特权。

<div id="cluster">
  ### CLUSTER
</div>

允许执行 `ON CLUSTER` 查询。

```sql title="Syntax"
GRANT CLUSTER ON *.* TO <username>
```

默认情况下，带有 `ON CLUSTER` 的查询要求用户具有 `CLUSTER` grant。
如果在未先授予 `CLUSTER` 特权的情况下尝试在查询中使用 `ON CLUSTER`，就会收到以下 error：

```text
Not enough privileges. To execute this query, it's necessary to have the grant CLUSTER ON *.*. 
```

可通过将 `config.xml` 中 `access_control_improvements` 部分里的 `on_cluster_queries_require_cluster_grant` 设置 (见下文) 设为 `false`，来更改默认行为。

```yaml title="config.xml"
<access_control_improvements>
    <on_cluster_queries_require_cluster_grant>true</on_cluster_queries_require_cluster_grant>
</access_control_improvements>
```

<div id="drop">
  ### DROP
</div>

允许按照以下特权层级执行 [DROP](../../sql-reference/statements/drop.md) 和 [DETACH](../../sql-reference/statements/detach.md) 语句：

* `DROP`。级别：`GROUP`
  * `DROP DATABASE`。级别：`DATABASE`
  * `DROP TABLE`。级别：`TABLE`
  * `DROP VIEW`。级别：`VIEW`
  * `DROP DICTIONARY`。级别：`DICTIONARY`

<div id="truncate">
  ### TRUNCATE
</div>

允许执行 [TRUNCATE](../../sql-reference/statements/truncate.md) 查询。

特权级别：`TABLE`。

<div id="optimize">
  ### OPTIMIZE
</div>

允许执行 [OPTIMIZE TABLE](../../sql-reference/statements/optimize.md) 语句。

特权级别：`TABLE`。

<div id="show">
  ### SHOW
</div>

允许按照以下特权层级执行 `SHOW`、`DESCRIBE`、`USE` 和 `EXISTS` 查询：

* `SHOW`。级别：`GROUP`
  * `SHOW DATABASES`。级别：`DATABASE`。允许执行 `SHOW DATABASES`、`SHOW CREATE DATABASE`、`USE <database>` 查询。
  * `SHOW TABLES`。级别：`TABLE`。允许执行 `SHOW TABLES`、`EXISTS <table>`、`CHECK <table>` 查询。
  * `SHOW COLUMNS`。级别：`COLUMN`。允许执行 `SHOW CREATE TABLE`、`DESCRIBE` 查询。
  * `SHOW DICTIONARIES`。级别：`DICTIONARY`。允许执行 `SHOW DICTIONARIES`、`SHOW CREATE DICTIONARY`、`EXISTS <dictionary>` 查询。

**说明**

如果用户拥有任何与指定表、字典或数据库相关的其他特权，则也拥有 `SHOW` 特权。

<div id="kill-query">
  ### KILL QUERY
</div>

允许按以下特权层级执行 [KILL](../../sql-reference/statements/kill.md#kill-query) 查询：

特权级别：`GLOBAL`。

**说明**

`KILL QUERY` 特权允许用户终止其他用户的查询。

<div id="access-management">
  ### 访问管理
</div>

允许用户执行管理用户、角色和行策略的查询。

* `ACCESS MANAGEMENT`。级别：`GROUP`
  * `CREATE USER`。级别：`GLOBAL`
  * `ALTER USER`。级别：`GLOBAL`
  * `DROP USER`。级别：`GLOBAL`
  * `CREATE ROLE`。级别：`GLOBAL`
  * `ALTER ROLE`。级别：`GLOBAL`
  * `DROP ROLE`。级别：`GLOBAL`
  * `ROLE ADMIN`。级别：`GLOBAL`
  * `CREATE ROW POLICY`。级别：`GLOBAL`。别名：`CREATE POLICY`
  * `ALTER ROW POLICY`。级别：`GLOBAL`。别名：`ALTER POLICY`
  * `DROP ROW POLICY`。级别：`GLOBAL`。别名：`DROP POLICY`
  * `CREATE QUOTA`。级别：`GLOBAL`
  * `ALTER QUOTA`。级别：`GLOBAL`
  * `DROP QUOTA`。级别：`GLOBAL`
  * `CREATE SETTINGS PROFILE`。级别：`GLOBAL`。别名：`CREATE PROFILE`
  * `ALTER SETTINGS PROFILE`。级别：`GLOBAL`。别名：`ALTER PROFILE`
  * `DROP SETTINGS PROFILE`。级别：`GLOBAL`。别名：`DROP PROFILE`
  * `SHOW ACCESS`。级别：`GROUP`
    * `SHOW_USERS`。级别：`GLOBAL`。别名：`SHOW CREATE USER`
    * `SHOW_ROLES`。级别：`GLOBAL`。别名：`SHOW CREATE ROLE`
    * `SHOW_ROW_POLICIES`。级别：`GLOBAL`。别名：`SHOW POLICIES`、`SHOW CREATE ROW POLICY`、`SHOW CREATE POLICY`
    * `SHOW_QUOTAS`。级别：`GLOBAL`。别名：`SHOW CREATE QUOTA`
    * `SHOW_SETTINGS_PROFILES`。级别：`GLOBAL`。别名：`SHOW PROFILES`、`SHOW CREATE SETTINGS PROFILE`、`SHOW CREATE PROFILE`
  * `ALLOW SQL SECURITY NONE`。级别：`GLOBAL`。别名：`CREATE SQL SECURITY NONE`、`SQL SECURITY NONE`、`SECURITY NONE`

`ROLE ADMIN` 特权允许用户授予和撤销任意角色，包括那些未通过 admin 选项授予该用户的角色。

<div id="system">
  ### SYSTEM
</div>

允许用户按照以下特权层级执行 [SYSTEM](../../sql-reference/statements/system.md) 查询。

* `SYSTEM`。级别：`GROUP`
  * `SYSTEM SHUTDOWN`。级别：`GLOBAL`。别名：`SYSTEM KILL`, `SHUTDOWN`
  * `SYSTEM DROP CACHE`。别名：`DROP CACHE`
    * `SYSTEM DROP DNS CACHE`。级别：`GLOBAL`。别名：`SYSTEM CLEAR DNS CACHE`, `SYSTEM DROP DNS`, `DROP DNS CACHE`, `DROP DNS`
    * `SYSTEM DROP MARK CACHE`。级别：`GLOBAL`。别名：`SYSTEM CLEAR MARK CACHE`, `SYSTEM DROP MARK`, `DROP MARK CACHE`, `DROP MARKS`
    * `SYSTEM DROP UNCOMPRESSED CACHE`。级别：`GLOBAL`。别名：`SYSTEM CLEAR UNCOMPRESSED CACHE`, `SYSTEM DROP UNCOMPRESSED`, `DROP UNCOMPRESSED CACHE`, `DROP UNCOMPRESSED`
  * `SYSTEM RELOAD`。级别：`GROUP`
    * `SYSTEM RELOAD CONFIG`。级别：`GLOBAL`。别名：`RELOAD CONFIG`
    * `SYSTEM RELOAD DICTIONARY`。级别：`GLOBAL`。别名：`SYSTEM RELOAD DICTIONARIES`, `RELOAD DICTIONARY`, `RELOAD DICTIONARIES`
      * `SYSTEM RELOAD EMBEDDED DICTIONARIES`。级别：`GLOBAL`。别名：`RELOAD EMBEDDED DICTIONARIES`
  * `SYSTEM MERGES`。级别：`TABLE`。别名：`SYSTEM STOP MERGES`, `SYSTEM START MERGES`, `STOP MERGES`, `START MERGES`
  * `SYSTEM TTL MERGES`。级别：`TABLE`。别名：`SYSTEM STOP TTL MERGES`, `SYSTEM START TTL MERGES`, `STOP TTL MERGES`, `START TTL MERGES`
  * `SYSTEM FETCHES`。级别：`TABLE`。别名：`SYSTEM STOP FETCHES`, `SYSTEM START FETCHES`, `STOP FETCHES`, `START FETCHES`
  * `SYSTEM MOVES`。级别：`TABLE`。别名：`SYSTEM STOP MOVES`, `SYSTEM START MOVES`, `STOP MOVES`, `START MOVES`
  * `SYSTEM SENDS`。级别：`GROUP`。别名：`SYSTEM STOP SENDS`, `SYSTEM START SENDS`, `STOP SENDS`, `START SENDS`
    * `SYSTEM DISTRIBUTED SENDS`。级别：`TABLE`。别名：`SYSTEM STOP DISTRIBUTED SENDS`, `SYSTEM START DISTRIBUTED SENDS`, `STOP DISTRIBUTED SENDS`, `START DISTRIBUTED SENDS`
    * `SYSTEM REPLICATED SENDS`。级别：`TABLE`。别名：`SYSTEM STOP REPLICATED SENDS`, `SYSTEM START REPLICATED SENDS`, `STOP REPLICATED SENDS`, `START REPLICATED SENDS`
  * `SYSTEM REPLICATION QUEUES`。级别：`TABLE`。别名：`SYSTEM STOP REPLICATION QUEUES`, `SYSTEM START REPLICATION QUEUES`, `STOP REPLICATION QUEUES`, `START REPLICATION QUEUES`
  * `SYSTEM SYNC REPLICA`。级别：`TABLE`。别名：`SYNC REPLICA`
  * `SYSTEM RESTART REPLICA`。级别：`TABLE`。别名：`RESTART REPLICA`
  * `SYSTEM FLUSH`。级别：`GROUP`
    * `SYSTEM FLUSH DISTRIBUTED`。级别：`TABLE`。别名：`FLUSH DISTRIBUTED`
    * `SYSTEM FLUSH LOGS`。级别：`GLOBAL`。别名：`FLUSH LOGS`

`SYSTEM RELOAD EMBEDDED DICTIONARIES` 特权会由 `SYSTEM RELOAD DICTIONARY ON *.*` 特权隐式授予。

<div id="introspection">
  ### INTROSPECTION
</div>

允许使用[内部信息](../../operations/optimizing-performance/sampling-query-profiler.md)相关函数。

* `INTROSPECTION`。级别：`GROUP`。别名：`INTROSPECTION FUNCTIONS`
  * `addressToLine`。级别：`GLOBAL`
  * `addressToLineWithInlines`。级别：`GLOBAL`
  * `addressToSymbol`。级别：`GLOBAL`
  * `demangle`。级别：`GLOBAL`

<div id="sources">
  ### 数据源
</div>

允许使用外部数据源。适用于 [表引擎](../../engines/table-engines/index.md) 和 [表函数](/zh/sql-reference/table-functions)。

* `READ`。级别：`GLOBAL_WITH_PARAMETER`
* `WRITE`。级别：`GLOBAL_WITH_PARAMETER`

可用参数：

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
从 25.7 版本开始，且仅在启用服务器设置
`access_control_improvements.enable_read_write_grants`
时，才支持对数据源的 `READ`/`WRITE` 特权分离。

否则，应使用语法 `GRANT AZURE ON *.* TO user`，这等同于新的 `GRANT READ, WRITE ON AZURE TO user`
:::

示例：

* 要使用 [MySQL 表引擎](../../engines/table-engines/integrations/mysql.md) 创建表，需要 `CREATE TABLE (ON db.table_name)` 和 `MYSQL` 特权。
* 要使用 [MySQL 表函数](../../sql-reference/table-functions/mysql.md)，需要 `CREATE TEMPORARY TABLE` 和 `MYSQL` 特权。

<div id="source-filter-grants">
  ### 来源过滤授权
</div>

:::note
此功能自 25.8 版本起可用，且仅在启用服务器设置
`access_control_improvements.enable_read_write_grants`
时可用
:::

您可以使用正则表达式过滤器向特定来源 URI 授予访问权限。这样可以细粒度地控制用户可访问哪些外部数据源。

**语法：**

```sql
GRANT READ ON S3('regexp_pattern') TO user
```

此授权将允许用户仅从与指定正则表达式匹配的 S3 URI 读取数据。

**示例：**

授予对特定 S3 存储桶路径的访问权限：

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
源过滤器以 **regexp** 作为参数，因此，授予
`GRANT READ ON URL('http://www.google.com') TO john;`

将允许执行查询

```sql
SELECT * FROM url('https://www.google.com');
SELECT * FROM url('https://www-google.com');
```

因为在正则表达式中，`.` 会被视为“任意单个字符”。
这可能会带来潜在的安全漏洞。正确的 grant 应为

```sql
GRANT READ ON URL('https://www\.google\.com') TO john;
```

:::

**使用 GRANT OPTION 进行再次授权：**

如果原始授权带有 `WITH GRANT OPTION`，则可以使用 `GRANT CURRENT GRANTS` 再次授权：

```sql
-- Original grant with GRANT OPTION
GRANT READ ON S3('s3://foo/.*') TO john WITH GRANT OPTION

-- John can now regrant this access to others
GRANT CURRENT GRANTS(READ ON S3) TO alice
```

**重要限制：**

* **不允许部分撤销：** 你不能只撤销已授予过滤器模式中的一部分。如有需要，必须先撤销整个授权，再使用新模式重新授权。
* **不允许通配符授权：** 你不能使用 `GRANT READ ON *('regexp')` 或其他类似仅含通配符的模式。必须提供具体的数据源。

<div id="dictget">
  ### dictGet
</div>

* `dictGet`。别名：`dictHas`、`dictGetHierarchy`、`dictIsIn`

允许用户调用 [dictGet](/zh/sql-reference/functions/ext-dict-functions#dictGet)、[dictHas](../../sql-reference/functions/ext-dict-functions.md#dictHas)、[dictGetHierarchy](../../sql-reference/functions/ext-dict-functions.md#dictGetHierarchy) 和 [dictIsIn](../../sql-reference/functions/ext-dict-functions.md#dictIsIn) 函数。

特权级别：`DICTIONARY`。

**示例**

* `GRANT dictGet ON mydb.mydictionary TO john`
* `GRANT dictGet ON mydictionary TO john`

<div id="displaysecretsinshowandselect">
  ### displaySecretsInShowAndSelect
</div>

如果同时启用
[`display_secrets_in_show_and_select` server setting](../../operations/server-configuration-parameters/settings#display_secrets_in_show_and_select)
和
[`format_display_secrets_in_show_and_select` format setting](../../operations/settings/formats#format_display_secrets_in_show_and_select)，
则允许用户在 `SHOW` 和 `SELECT` 查询中查看 secrets。

<div id="named-collection-admin">
  ### NAMED COLLECTION ADMIN
</div>

允许对指定的命名集合执行特定操作。在 23.7 之前，它名为 NAMED COLLECTION CONTROL；自 23.7 起新增了 NAMED COLLECTION ADMIN，同时保留 NAMED COLLECTION CONTROL 作为别名。

* `NAMED COLLECTION ADMIN`。级别：`NAMED_COLLECTION`。别名：`NAMED COLLECTION CONTROL`
  * `CREATE NAMED COLLECTION`。级别：`NAMED_COLLECTION`
  * `DROP NAMED COLLECTION`。级别：`NAMED_COLLECTION`
  * `ALTER NAMED COLLECTION`。级别：`NAMED_COLLECTION`
  * `SHOW NAMED COLLECTIONS`。级别：`NAMED_COLLECTION`。别名：`SHOW NAMED COLLECTIONS`
  * `SHOW NAMED COLLECTIONS SECRETS`。级别：`NAMED_COLLECTION`。别名：`SHOW NAMED COLLECTIONS SECRETS`
  * `NAMED COLLECTION`。级别：`NAMED_COLLECTION`。别名：`NAMED COLLECTION USAGE, USE NAMED COLLECTION`

与其他所有授权 (CREATE、DROP、ALTER、SHOW) 不同，NAMED COLLECTION 授权直到 23.7 才加入，而其余授权更早，在 22.12 就已加入。

**示例**

假设有一个名为 abc 的命名集合，我们向用户 john 授予 CREATE NAMED COLLECTION 特权。

* `GRANT CREATE NAMED COLLECTION ON abc TO john`

<div id="table-engine">
  ### TABLE ENGINE
</div>

允许在创建表时使用指定的表引擎。适用于[表引擎](../../engines/table-engines/index.md)。

**示例**

* `GRANT TABLE ENGINE ON * TO john`
* `GRANT TABLE ENGINE ON TinyLog TO john`

:::note
默认情况下，出于向后兼容考虑，使用特定表引擎创建表时会忽略授权；
不过，你可以通过在 config.xml 中将 [`table_engines_require_grant` 设置为 true](https://github.com/ClickHouse/ClickHouse/blob/df970ed64eaf472de1e7af44c21ec95956607ebb/programs/server/config.xml#L853-L855)
来更改这一行为。
:::

某些使用外部源的表引擎可能需要对相应源具备 `READ`/`WRITE` 权限。请参见[Sources](#sources)。

例如，对于 AzureBlobStorage 表引擎，可能需要以下授权。

* `GRANT READ, WRITE ON AZURE TO john`

<div id="all">
  ### ALL
</div>

<CloudNotSupportedBadge />

将受控实体上的所有特权授予用户账户或角色。

:::note
ClickHouse Cloud 不支持特权 `ALL`，其中 `default` 用户的权限有限。用户可以通过授予 `default_role`，向某个用户授予最大权限。更多详情请参见[此处](/zh/cloud/security/manage-cloud-users)。
用户还可以使用 `default` 用户执行 `GRANT CURRENT GRANTS`，以实现与 `ALL` 类似的效果。
:::

<div id="none">
  ### NONE
</div>

不授予任何特权。

<div id="admin-option">
  ### ADMIN OPTION
</div>

`ADMIN OPTION` 特权允许用户将其角色授予其他用户。