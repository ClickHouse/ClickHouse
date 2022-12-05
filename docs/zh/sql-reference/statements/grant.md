---
sidebar_position: 39
sidebar_label: 授权操作
---

# 授权 {#grant}
-   给ClickHouse的用户或角色赋予 [权限](#grant-privileges)
-   将角色分配给用户或其他角色

取消权限，使用 [REVOKE](../../sql-reference/statements/revoke.md)语句。查看已授权的权限请使用 [SHOW GRANTS](../../sql-reference/statements/show.md#show-grants-statement)。

## 授权操作语法 {#grant-privigele-syntax}

``` sql
GRANT [ON CLUSTER cluster_name] privilege[(column_name [,...])] [,...] ON {db.table|db.*|*.*|table|*} TO {user | role | CURRENT_USER} [,...] [WITH GRANT OPTION] [WITH REPLACE OPTION]
```

-   `privilege` — 权限类型
-   `role` — 用户角色
-   `user` — 用户账号

`WITH GRANT OPTION` 授予 `user` 或 `role`执行 `GRANT` 操作的权限。用户可将在自身权限范围内的权限进行授权
`WITH REPLACE OPTION` 以当前sql里的新权限替代掉 `user` 或 `role`的旧权限，如果没有该选项则是追加授权。

## 角色分配的语法 {#assign-role-syntax}

``` sql
GRANT [ON CLUSTER cluster_name] role [,...] TO {user | another_role | CURRENT_USER} [,...] [WITH ADMIN OPTION] [WITH REPLACE OPTION]
```

-   `role` —  角色
-   `user` —  用户

 `WITH ADMIN OPTION`  授予 `user` 或 `role` 执行[ADMIN OPTION](#admin-option-privilege) 的权限
`WITH REPLACE OPTION` 以当前sql里的新role替代掉 `user` 或 `role`的旧role，如果没有该选项则是追加roles。

## 用法 {#grant-usage}

使用 `GRANT`，你的账号必须有 `GRANT OPTION`的权限。用户只能将在自身权限范围内的权限进行授权

例如，管理员有权通过下面的语句给 `john`账号添加授权

``` sql
GRANT SELECT(x,y) ON db.table TO john WITH GRANT OPTION
```

这意味着 `john` 有权限执行以下操作：

-   `SELECT x,y FROM db.table`.
-   `SELECT x FROM db.table`.
-   `SELECT y FROM db.table`.

`john` 不能执行`SELECT z FROM db.table`。同样的 `SELECT * FROMdb.table` 也是不允许的。执行这个查询时，CH不会返回任何数据，甚至 `x` 和 `y`列。唯一的例外是，当表仅包含 `x`和`y`列时。这种情况下，CH返回所有数据。

同样 `john` 有权执行 `GRANT OPTION`，因此他能给其它账号进行和自己账号权限范围相同的授权。

可以使用`*` 号代替表或库名进行授权操作。例如， `GRANT SELECT ONdb.* TO john` 操作运行 `john`对 `db`库的所有表执行 `SELECT`查询。同样，你可以忽略库名。在这种情形下，权限将指向当前的数据库。例如， `GRANT SELECT ON* to john` 对当前数据库的所有表指定授权， `GARNT SELECT ON mytable to john`对当前数据库的 `mytable`表进行授权。

访问 `systen`数据库总是被允许的（因为这个数据库用来处理sql操作）
可以一次给多个账号进行多种授权操作。 `GRANT SELECT,INSERT ON *.* TO john,robin` 允许 `john`和`robin` 账号对任意数据库的任意表执行 `INSERT`和 `SELECT`操作。

## 权限 {#grant-privileges}

权限是指执行特定操作的许可

权限有层级结构。一组允许的操作依赖相应的权限范围。

权限的层级：

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

如何对待该层级的示例：
-   `ALTER` 权限包含所有其它 `ALTER *` 的权限
-   `ALTER CONSTRAINT` 包含 `ALTER ADD CONSTRAINT` 和 `ALTER DROP CONSTRAINT`权限

权限被应用到不同级别。 Knowing of a level suggests syntax available for privilege.

级别（由低到高）：

-   `COLUMN` - 可以授权到列，表，库或者全局
-   `TABLE`  -  可以授权到表，库，或全局
-   `VIEW` - 可以授权到视图，库，或全局
-   `DICTIONARY` - 可以授权到字典，库，或全局
-   `DATABASE` - 可以授权到数据库或全局
-   `GLABLE` -  可以授权到全局
-   `GROUP` - 不同级别的权限分组。当授予 `GROUP`级别的权限时， 根据所用的语法，只有对应分组中的权限才会被分配。

允许的语法示例：

-   `GRANT SELECT(x) ON db.table TO user`
-   `GRANT SELECT ON db.* TO user`

不允许的语法示例：

-   `GRANT CREATE USER(x) ON db.table TO user`
-   `GRANT CREATE USER ON db.* TO user`

特殊的权限 `ALL` 将所有权限授予给用户或角色

默认情况下，一个用户账号或角色没有可授予的权限

如果用户或角色没有任何权限，它将显示为 `NONE`权限

有些操作根据它们的实现需要一系列的权限。例如， [RENAME](../../sql-reference/statements/misc.md#misc_operations-rename)操作需要以下权限来执行：`SELECT`, `CREATE TABLE`, `INSERT` 和 `DROP TABLE`。

### SELECT {#grant-select}

允许执行 [SELECT](../../sql-reference/statements/select/index.md) 查询

权限级别: `COLUMN`.

**说明**

有该权限的用户可以对指定的表和库的指定列进行 `SELECT`查询。如果用户查询包含了其它列则结果不返回数据。

考虑如下的授权语句：

``` sql
GRANT SELECT(x,y) ON db.table TO john
```

该权限允许 `john` 对 `db.table`表的列`x`,`y`执行任意 `SELECT `查询，例如 `SELECT x FROM db.table`。 `john` 不能执行 `SELECT z FROM db.table`以及 `SELECT * FROM db.table`。执行这个查询时，CH不会返回任何数据，甚至 `x` 和 `y`列。唯一的例外是，当表仅包含 `x`和`y`列时。这种情况下，CH返回所有数据。

### INSERT {#grant-insert}

允许执行 [INSERT](../../sql-reference/statements/insert-into.md) 操作.

权限级别: `COLUMN`.

**说明**

有该权限的用户可以对指定的表和库的指定列进行 `INSERT`操作。如果用户查询包含了其它列则结果不返回数据。

**示例**

``` sql
GRANT INSERT(x,y) ON db.table TO john
```

该权限允许 `john` 对 `db.table`表的列`x`,`y`执行数据插入操作

### ALTER {#grant-alter}

允许根据下列权限层级执行 [ALTER](../../sql-reference/statements/alter.md)操作

-   `ALTER`. 级别: `COLUMN`.
    -   `ALTER TABLE`. 级别: `GROUP`
        -   `ALTER UPDATE`. 级别: `COLUMN`. 别名: `UPDATE`
        -   `ALTER DELETE`. 级别: `COLUMN`. 别名: `DELETE`
        -   `ALTER COLUMN`. 级别: `GROUP`
            -   `ALTER ADD COLUMN`. 级别: `COLUMN`. 别名: `ADD COLUMN`
            -   `ALTER DROP COLUMN`. 级别: `COLUMN`. 别名: `DROP COLUMN`
            -   `ALTER MODIFY COLUMN`. 级别: `COLUMN`. 别名: `MODIFY COLUMN`
            -   `ALTER COMMENT COLUMN`. 级别: `COLUMN`. 别名: `COMMENT COLUMN`
            -   `ALTER CLEAR COLUMN`. 级别: `COLUMN`. 别名: `CLEAR COLUMN`
            -   `ALTER RENAME COLUMN`. 级别: `COLUMN`. 别名: `RENAME COLUMN`
        -   `ALTER INDEX`. 级别: `GROUP`. 别名: `INDEX`
            -   `ALTER ORDER BY`. 级别: `TABLE`. 别名: `ALTER MODIFY ORDER BY`, `MODIFY ORDER BY`
            -   `ALTER ADD INDEX`. 级别: `TABLE`. 别名: `ADD INDEX`
            -   `ALTER DROP INDEX`. 级别: `TABLE`. 别名: `DROP INDEX`
            -   `ALTER MATERIALIZE INDEX`. 级别: `TABLE`. 别名: `MATERIALIZE INDEX`
            -   `ALTER CLEAR INDEX`. 级别: `TABLE`. 别名: `CLEAR INDEX`
        -   `ALTER CONSTRAINT`. 级别: `GROUP`. 别名: `CONSTRAINT`
            -   `ALTER ADD CONSTRAINT`. 级别: `TABLE`. 别名: `ADD CONSTRAINT`
            -   `ALTER DROP CONSTRAINT`. 级别: `TABLE`. 别名: `DROP CONSTRAINT`
        -   `ALTER TTL`. 级别: `TABLE`. 别名: `ALTER MODIFY TTL`, `MODIFY TTL`
        -   `ALTER MATERIALIZE TTL`. 级别: `TABLE`. 别名: `MATERIALIZE TTL`
        -   `ALTER SETTINGS`. 级别: `TABLE`. 别名: `ALTER SETTING`, `ALTER MODIFY SETTING`, `MODIFY SETTING`
        -   `ALTER MOVE PARTITION`. 级别: `TABLE`. 别名: `ALTER MOVE PART`, `MOVE PARTITION`, `MOVE PART`
        -   `ALTER FETCH PARTITION`. 级别: `TABLE`. 别名: `FETCH PARTITION`
        -   `ALTER FREEZE PARTITION`. 级别: `TABLE`. 别名: `FREEZE PARTITION`
    -   `ALTER VIEW` 级别: `GROUP`
        -   `ALTER VIEW REFRESH`. 级别: `VIEW`. 别名: `ALTER LIVE VIEW REFRESH`, `REFRESH VIEW`
        -   `ALTER VIEW MODIFY QUERY`. 级别: `VIEW`. 别名: `ALTER TABLE MODIFY QUERY`

如何对待该层级的示例：
-   `ALTER` 权限包含所有其它 `ALTER *` 的权限
-   `ALTER CONSTRAINT` 包含 `ALTER ADD CONSTRAINT` 和 `ALTER DROP CONSTRAINT`权限

**备注**

-    `MODIFY SETTING`权限允许修改表的引擎设置。它不会影响服务的配置参数
-    `ATTACH` 操作需要 [CREATE](#grant-create) 权限.
-    `DETACH` 操作需要 [DROP](#grant-drop) 权限.
-    要通过 [KILL MUTATION](../../sql-reference/statements/misc.md#kill-mutation) 操作来终止mutation, 你需要有发起mutation操作的权限。例如，当你想终止 `ALTER UPDATE`操作时，需要有 `ALTER UPDATE`, `ALTER TABLE`, 或 `ALTER`权限

### CREATE {#grant-create}

允许根据下面的权限层级来执行 [CREATE](../../sql-reference/statements/create.md) 和 [ATTACH](../../sql-reference/statements/misc.md#attach) DDL语句:

-   `CREATE`. 级别: `GROUP`
    -   `CREATE DATABASE`. 级别: `DATABASE`
    -   `CREATE TABLE`. 级别: `TABLE`
    -   `CREATE VIEW`. 级别: `VIEW`
    -   `CREATE DICTIONARY`. 级别: `DICTIONARY`
    -   `CREATE TEMPORARY TABLE`. 级别: `GLOBAL`

**备注**

-   删除已创建的表，用户需要 [DROP](#grant-drop)权限

### DROP {#grant-drop}

允许根据下面的权限层级来执行 [DROP](../../sql-reference/statements/misc.md#drop) 和 [DETACH](../../sql-reference/statements/misc.md#detach) :

-   `DROP`. 级别:
    -   `DROP DATABASE`. 级别: `DATABASE`
    -   `DROP TABLE`. 级别: `TABLE`
    -   `DROP VIEW`. 级别: `VIEW`
    -   `DROP DICTIONARY`. 级别: `DICTIONARY`

### TRUNCATE {#grant-truncate}

允许执行 [TRUNCATE](../../sql-reference/statements/misc.md#truncate-statement) .

权限级别: `TABLE`.

### OPTIMIZE {#grant-optimize}

允许执行 [OPTIMIZE TABLE](../../sql-reference/statements/misc.md#misc_operations-optimize) .

权限级别: `TABLE`.

### SHOW {#grant-show}

允许根据下面的权限层级来执行 `SHOW`, `DESCRIBE`, `USE`, 和 `EXISTS` :

-   `SHOW`. 级别: `GROUP`
    -   `SHOW DATABASES`. 级别: `DATABASE`. 允许执行 `SHOW DATABASES`, `SHOW CREATE DATABASE`, `USE <database>` .
    -   `SHOW TABLES`. 级别: `TABLE`. 允许执行 `SHOW TABLES`, `EXISTS <table>`, `CHECK <table>` .
    -   `SHOW COLUMNS`. 级别: `COLUMN`. 允许执行 `SHOW CREATE TABLE`, `DESCRIBE` .
    -   `SHOW DICTIONARIES`. 级别: `DICTIONARY`. 允许执行 `SHOW DICTIONARIES`, `SHOW CREATE DICTIONARY`, `EXISTS <dictionary>` .

**备注**

用户同时拥有 `SHOW`权限，当用户对指定表，字典或数据库有其它的权限时。

### KILL QUERY {#grant-kill-query}

允许根据下面的权限层级来执行 [KILL](../../sql-reference/statements/misc.md#kill-query-statement):

权限级别: `GLOBAL`.

**备注**

`KILL QUERY` 权限允许用户终止其它用户提交的操作。

### 访问管理 {#grant-access-management}

允许用户执行管理用户/角色和行规则的操作:

-   `ACCESS MANAGEMENT`. 级别: `GROUP`
    -   `CREATE USER`. 级别: `GLOBAL`
    -   `ALTER USER`. 级别: `GLOBAL`
    -   `DROP USER`. 级别: `GLOBAL`
    -   `CREATE ROLE`. 级别: `GLOBAL`
    -   `ALTER ROLE`. 级别: `GLOBAL`
    -   `DROP ROLE`. 级别: `GLOBAL`
    -   `ROLE ADMIN`. 级别: `GLOBAL`
    -   `CREATE ROW POLICY`. 级别: `GLOBAL`. 别名: `CREATE POLICY`
    -   `ALTER ROW POLICY`. 级别: `GLOBAL`. 别名: `ALTER POLICY`
    -   `DROP ROW POLICY`. 级别: `GLOBAL`. 别名: `DROP POLICY`
    -   `CREATE QUOTA`. 级别: `GLOBAL`
    -   `ALTER QUOTA`. 级别: `GLOBAL`
    -   `DROP QUOTA`. 级别: `GLOBAL`
    -   `CREATE SETTINGS PROFILE`. 级别: `GLOBAL`. 别名: `CREATE PROFILE`
    -   `ALTER SETTINGS PROFILE`. 级别: `GLOBAL`. 别名: `ALTER PROFILE`
    -   `DROP SETTINGS PROFILE`. 级别: `GLOBAL`. 别名: `DROP PROFILE`
    -   `SHOW ACCESS`. 级别: `GROUP`
        -   `SHOW_USERS`. 级别: `GLOBAL`. 别名: `SHOW CREATE USER`
        -   `SHOW_ROLES`. 级别: `GLOBAL`. 别名: `SHOW CREATE ROLE`
        -   `SHOW_ROW_POLICIES`. 级别: `GLOBAL`. 别名: `SHOW POLICIES`, `SHOW CREATE ROW POLICY`, `SHOW CREATE POLICY`
        -   `SHOW_QUOTAS`. 级别: `GLOBAL`. 别名: `SHOW CREATE QUOTA`
        -   `SHOW_SETTINGS_PROFILES`. 级别: `GLOBAL`. 别名: `SHOW PROFILES`, `SHOW CREATE SETTINGS PROFILE`, `SHOW CREATE PROFILE`

`ROLE ADMIN` 权限允许用户对角色进行分配以及撤回，包括根据管理选项尚未分配的角色

### SYSTEM {#grant-system}

允许根据下面的权限层级来执行 [SYSTEM](../../sql-reference/statements/system.md) :

-   `SYSTEM`. 级别: `GROUP`
    -   `SYSTEM SHUTDOWN`. 级别: `GLOBAL`. 别名: `SYSTEM KILL`, `SHUTDOWN`
    -   `SYSTEM DROP CACHE`. 别名: `DROP CACHE`
        -   `SYSTEM DROP DNS CACHE`. 级别: `GLOBAL`. 别名: `SYSTEM DROP DNS`, `DROP DNS CACHE`, `DROP DNS`
        -   `SYSTEM DROP MARK CACHE`. 级别: `GLOBAL`. 别名: `SYSTEM DROP MARK`, `DROP MARK CACHE`, `DROP MARKS`
        -   `SYSTEM DROP UNCOMPRESSED CACHE`. 级别: `GLOBAL`. 别名: `SYSTEM DROP UNCOMPRESSED`, `DROP UNCOMPRESSED CACHE`, `DROP UNCOMPRESSED`
    -   `SYSTEM RELOAD`. 级别: `GROUP`
        -   `SYSTEM RELOAD CONFIG`. 级别: `GLOBAL`. 别名: `RELOAD CONFIG`
        -   `SYSTEM RELOAD DICTIONARY`. 级别: `GLOBAL`. 别名: `SYSTEM RELOAD DICTIONARIES`, `RELOAD DICTIONARY`, `RELOAD DICTIONARIES`
        -   `SYSTEM RELOAD EMBEDDED DICTIONARIES`. 级别: `GLOBAL`. 别名: R`ELOAD EMBEDDED DICTIONARIES`
    -   `SYSTEM MERGES`. 级别: `TABLE`. 别名: `SYSTEM STOP MERGES`, `SYSTEM START MERGES`, `STOP MERGES`, `START MERGES`
    -   `SYSTEM TTL MERGES`. 级别: `TABLE`. 别名: `SYSTEM STOP TTL MERGES`, `SYSTEM START TTL MERGES`, `STOP TTL MERGES`, `START TTL MERGES`
    -   `SYSTEM FETCHES`. 级别: `TABLE`. 别名: `SYSTEM STOP FETCHES`, `SYSTEM START FETCHES`, `STOP FETCHES`, `START FETCHES`
    -   `SYSTEM MOVES`. 级别: `TABLE`. 别名: `SYSTEM STOP MOVES`, `SYSTEM START MOVES`, `STOP MOVES`, `START MOVES`
    -   `SYSTEM SENDS`. 级别: `GROUP`. 别名: `SYSTEM STOP SENDS`, `SYSTEM START SENDS`, `STOP SENDS`, `START SENDS`
        -   `SYSTEM DISTRIBUTED SENDS`. 级别: `TABLE`. 别名: `SYSTEM STOP DISTRIBUTED SENDS`, `SYSTEM START DISTRIBUTED SENDS`, `STOP DISTRIBUTED SENDS`, `START DISTRIBUTED SENDS`
        -   `SYSTEM REPLICATED SENDS`. 级别: `TABLE`. 别名: `SYSTEM STOP REPLICATED SENDS`, `SYSTEM START REPLICATED SENDS`, `STOP REPLICATED SENDS`, `START REPLICATED SENDS`
    -   `SYSTEM REPLICATION QUEUES`. 级别: `TABLE`. 别名: `SYSTEM STOP REPLICATION QUEUES`, `SYSTEM START REPLICATION QUEUES`, `STOP REPLICATION QUEUES`, `START REPLICATION QUEUES`
    -   `SYSTEM SYNC REPLICA`. 级别: `TABLE`. 别名: `SYNC REPLICA`
    -   `SYSTEM RESTART REPLICA`. 级别: `TABLE`. 别名: `RESTART REPLICA`
    -   `SYSTEM FLUSH`. 级别: `GROUP`
        -   `SYSTEM FLUSH DISTRIBUTED`. 级别: `TABLE`. 别名: `FLUSH DISTRIBUTED`
        -   `SYSTEM FLUSH LOGS`. 级别: `GLOBAL`. 别名: `FLUSH LOGS`


`SYSTEM RELOAD EMBEDDED DICTIONARIES` 权限隐式的通过操作 `SYSTEM RELOAD DICTIONARY ON *.*` 来进行授权.

### 内省introspection {#grant-introspection}

允许使用 [introspection](../../operations/optimizing-performance/sampling-query-profiler.md) 函数.

-   `INTROSPECTION`. 级别: `GROUP`. 别名: `INTROSPECTION FUNCTIONS`
    -   `addressToLine`. 级别: `GLOBAL`
    -   `addressToSymbol`. 级别: `GLOBAL`
    -   `demangle`. 级别: `GLOBAL`

### 数据源 {#grant-sources}

允许在 [table engines](../../engines/table-engines/index.md) 和 [table functions](../../sql-reference/table-functions/index.md#table-functions)中使用外部数据源。

-   `SOURCES`. 级别: `GROUP`
    -   `FILE`. 级别: `GLOBAL`
    -   `URL`. 级别: `GLOBAL`
    -   `REMOTE`. 级别: `GLOBAL`
    -   `YSQL`. 级别: `GLOBAL`
    -   `ODBC`. 级别: `GLOBAL`
    -   `JDBC`. 级别: `GLOBAL`
    -   `HDFS`. 级别: `GLOBAL`
    -   `S3`. 级别: `GLOBAL`

`SOURCES` 权限允许使用所有数据源。当然也可以单独对每个数据源进行授权。要使用数据源时，还需要额外的权限。

示例:

-  创建 [MySQL table engine](../../engines/table-engines/integrations/mysql.md), 需要 `CREATE TABLE (ON db.table_name)` 和 `MYSQL`权限。4
-   要使用 [mysql table function](../../sql-reference/table-functions/mysql.md)，需要 `CREATE TEMPORARY TABLE` 和 `MYSQL` 权限

### dictGet {#grant-dictget}

-   `dictGet`. 别名: `dictHas`, `dictGetHierarchy`, `dictIsIn`

允许用户执行 [dictGet](../../sql-reference/functions/ext-dict-functions.md#dictget), [dictHas](../../sql-reference/functions/ext-dict-functions.md#dicthas), [dictGetHierarchy](../../sql-reference/functions/ext-dict-functions.md#dictgethierarchy), [dictIsIn](../../sql-reference/functions/ext-dict-functions.md#dictisin) 等函数.

权限级别: `DICTIONARY`.

**示例**

-   `GRANT dictGet ON mydb.mydictionary TO john`
-   `GRANT dictGet ON mydictionary TO john`

### ALL {#grant-all}

对规定的实体（列，表，库等）给用户或角色授予所有权限

### NONE {#grant-none}

不授予任何权限

### ADMIN OPTION {#admin-option-privilege}

`ADMIN OPTION` 权限允许用户将他们的角色分配给其它用户

[原始文档](https://clickhouse.com/docs/en/query_language/grant/) <!--hide-->
