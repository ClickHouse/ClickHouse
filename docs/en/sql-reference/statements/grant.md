---
toc_priority: 38
toc_title: GRANT
---

# GRANT Statement {#grant}

-   Grants [privileges](#grant-privileges) to ClickHouse user accounts or roles.
-   Assigns roles to user accounts or to the other roles.

To revoke privileges, use the [REVOKE](../../sql-reference/statements/revoke.md) statement. Also you can list granted privileges with the [SHOW GRANTS](../../sql-reference/statements/show.md#show-grants-statement) statement.

## Granting Privilege Syntax {#grant-privigele-syntax}

``` sql
GRANT [ON CLUSTER cluster_name] privilege[(column_name [,...])] [,...] ON {db.table|db.*|*.*|table|*} TO {user | role | CURRENT_USER} [,...] [WITH GRANT OPTION]
```

-   `privilege` — Type of privilege.
-   `role` — ClickHouse user role.
-   `user` — ClickHouse user account.

The `WITH GRANT OPTION` clause grants `user` or `role` with permission to execute the `GRANT` query. Users can grant privileges of the same scope they have and less.

## Assigning Role Syntax {#assign-role-syntax}

``` sql
GRANT [ON CLUSTER cluster_name] role [,...] TO {user | another_role | CURRENT_USER} [,...] [WITH ADMIN OPTION]
```

-   `role` — ClickHouse user role.
-   `user` — ClickHouse user account.

The `WITH ADMIN OPTION` clause grants [ADMIN OPTION](#admin-option-privilege) privilege to `user` or `role`.

## Usage {#grant-usage}

To use `GRANT`, your account must have the `GRANT OPTION` privilege. You can grant privileges only inside the scope of your account privileges.

For example, administrator has granted privileges to the `john` account by the query:

``` sql
GRANT SELECT(x,y) ON db.table TO john WITH GRANT OPTION
```

It means that `john` has the permission to execute:

-   `SELECT x,y FROM db.table`.
-   `SELECT x FROM db.table`.
-   `SELECT y FROM db.table`.

`john` can’t execute `SELECT z FROM db.table`. The `SELECT * FROM db.table` also is not available. Processing this query, ClickHouse does not return any data, even `x` and `y`. The only exception is if a table contains only `x` and `y` columns. In this case ClickHouse returns all the data.

Also `john` has the `GRANT OPTION` privilege, so it can grant other users with privileges of the same or smaller scope.

Specifying privileges you can use asterisk (`*`) instead of a table or a database name. For example, the `GRANT SELECT ON db.* TO john` query allows `john` to execute the `SELECT` query over all the tables in `db` database. Also, you can omit database name. In this case privileges are granted for current database. For example, `GRANT SELECT ON * TO john` grants the privilege on all the tables in the current database, `GRANT SELECT ON mytable TO john` grants the privilege on the `mytable` table in the current database.

Access to the `system` database is always allowed (since this database is used for processing queries).

You can grant multiple privileges to multiple accounts in one query. The query `GRANT SELECT, INSERT ON *.* TO john, robin` allows accounts `john` and `robin` to execute the `INSERT` and `SELECT` queries over all the tables in all the databases on the server.

## Privileges {#grant-privileges}

Privilege is a permission to execute specific kind of queries.

Privileges have a hierarchical structure. A set of permitted queries depends on the privilege scope.

Hierarchy of privileges:

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
            -   `ALTER SAMPLE BY`
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
        -   `CREATE TEMPORARY TABLE`
    -   `CREATE VIEW`
    -   `CREATE DICTIONARY`
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

Examples of how this hierarchy is treated:

-   The `ALTER` privilege includes all other `ALTER*` privileges.
-   `ALTER CONSTRAINT` includes `ALTER ADD CONSTRAINT` and `ALTER DROP CONSTRAINT` privileges.

Privileges are applied at different levels. Knowing of a level suggests syntax available for privilege.

Levels (from lower to higher):

-   `COLUMN` — Privilege can be granted for column, table, database, or globally.
-   `TABLE` — Privilege can be granted for table, database, or globally.
-   `VIEW` — Privilege can be granted for view, database, or globally.
-   `DICTIONARY` — Privilege can be granted for dictionary, database, or globally.
-   `DATABASE` — Privilege can be granted for database or globally.
-   `GLOBAL` — Privilege can be granted only globally.
-   `GROUP` — Groups privileges of different levels. When `GROUP`-level privilege is granted, only that privileges from the group are granted which correspond to the used syntax.

Examples of allowed syntax:

-   `GRANT SELECT(x) ON db.table TO user`
-   `GRANT SELECT ON db.* TO user`

Examples of disallowed syntax:

-   `GRANT CREATE USER(x) ON db.table TO user`
-   `GRANT CREATE USER ON db.* TO user`

The special privilege [ALL](#grant-all) grants all the privileges to a user account or a role.

By default, a user account or a role has no privileges.

If a user or a role has no privileges, it is displayed as [NONE](#grant-none) privilege.

Some queries by their implementation require a set of privileges. For example, to execute the [RENAME](../../sql-reference/statements/misc.md#misc_operations-rename) query you need the following privileges: `SELECT`, `CREATE TABLE`, `INSERT` and `DROP TABLE`.

### SELECT {#grant-select}

Allows executing [SELECT](../../sql-reference/statements/select/index.md) queries.

Privilege level: `COLUMN`.

**Description**

User granted with this privilege can execute `SELECT` queries over a specified list of columns in the specified table and database. If user includes other columns then specified a query returns no data.

Consider the following privilege:

``` sql
GRANT SELECT(x,y) ON db.table TO john
```

This privilege allows `john` to execute any `SELECT` query that involves data from the `x` and/or `y` columns in `db.table`, for example, `SELECT x FROM db.table`. `john` can’t execute `SELECT z FROM db.table`. The `SELECT * FROM db.table` also is not available. Processing this query, ClickHouse does not return any data, even `x` and `y`. The only exception is if a table contains only `x` and `y` columns, in this case ClickHouse returns all the data.

### INSERT {#grant-insert}

Allows executing [INSERT](../../sql-reference/statements/insert-into.md) queries.

Privilege level: `COLUMN`.

**Description**

User granted with this privilege can execute `INSERT` queries over a specified list of columns in the specified table and database. If user includes other columns then specified a query does not insert any data.

**Example**

``` sql
GRANT INSERT(x,y) ON db.table TO john
```

The granted privilege allows `john` to insert data to the `x` and/or `y` columns in `db.table`.

### ALTER {#grant-alter}

Allows executing [ALTER](../../sql-reference/statements/alter/index.md) queries according to the following hierarchy of privileges:

-   `ALTER`. Level: `COLUMN`.
    -   `ALTER TABLE`. Level: `GROUP`
        -   `ALTER UPDATE`. Level: `COLUMN`. Aliases: `UPDATE`
        -   `ALTER DELETE`. Level: `COLUMN`. Aliases: `DELETE`
        -   `ALTER COLUMN`. Level: `GROUP`
            -   `ALTER ADD COLUMN`. Level: `COLUMN`. Aliases: `ADD COLUMN`
            -   `ALTER DROP COLUMN`. Level: `COLUMN`. Aliases: `DROP COLUMN`
            -   `ALTER MODIFY COLUMN`. Level: `COLUMN`. Aliases: `MODIFY COLUMN`
            -   `ALTER COMMENT COLUMN`. Level: `COLUMN`. Aliases: `COMMENT COLUMN`
            -   `ALTER CLEAR COLUMN`. Level: `COLUMN`. Aliases: `CLEAR COLUMN`
            -   `ALTER RENAME COLUMN`. Level: `COLUMN`. Aliases: `RENAME COLUMN`
        -   `ALTER INDEX`. Level: `GROUP`. Aliases: `INDEX`
            -   `ALTER ORDER BY`. Level: `TABLE`. Aliases: `ALTER MODIFY ORDER BY`, `MODIFY ORDER BY`
            -   `ALTER SAMPLE BY`. Level: `TABLE`. Aliases: `ALTER MODIFY SAMPLE BY`, `MODIFY SAMPLE BY`
            -   `ALTER ADD INDEX`. Level: `TABLE`. Aliases: `ADD INDEX`
            -   `ALTER DROP INDEX`. Level: `TABLE`. Aliases: `DROP INDEX`
            -   `ALTER MATERIALIZE INDEX`. Level: `TABLE`. Aliases: `MATERIALIZE INDEX`
            -   `ALTER CLEAR INDEX`. Level: `TABLE`. Aliases: `CLEAR INDEX`
        -   `ALTER CONSTRAINT`. Level: `GROUP`. Aliases: `CONSTRAINT`
            -   `ALTER ADD CONSTRAINT`. Level: `TABLE`. Aliases: `ADD CONSTRAINT`
            -   `ALTER DROP CONSTRAINT`. Level: `TABLE`. Aliases: `DROP CONSTRAINT`
        -   `ALTER TTL`. Level: `TABLE`. Aliases: `ALTER MODIFY TTL`, `MODIFY TTL`
            -   `ALTER MATERIALIZE TTL`. Level: `TABLE`. Aliases: `MATERIALIZE TTL`
        -   `ALTER SETTINGS`. Level: `TABLE`. Aliases: `ALTER SETTING`, `ALTER MODIFY SETTING`, `MODIFY SETTING`
        -   `ALTER MOVE PARTITION`. Level: `TABLE`. Aliases: `ALTER MOVE PART`, `MOVE PARTITION`, `MOVE PART`
        -   `ALTER FETCH PARTITION`. Level: `TABLE`. Aliases: `ALTER FETCH PART`, `FETCH PARTITION`, `FETCH PART`
        -   `ALTER FREEZE PARTITION`. Level: `TABLE`. Aliases: `FREEZE PARTITION`
    -   `ALTER VIEW` Level: `GROUP`
        -   `ALTER VIEW REFRESH`. Level: `VIEW`. Aliases: `ALTER LIVE VIEW REFRESH`, `REFRESH VIEW`
        -   `ALTER VIEW MODIFY QUERY`. Level: `VIEW`. Aliases: `ALTER TABLE MODIFY QUERY`

Examples of how this hierarchy is treated:

-   The `ALTER` privilege includes all other `ALTER*` privileges.
-   `ALTER CONSTRAINT` includes `ALTER ADD CONSTRAINT` and `ALTER DROP CONSTRAINT` privileges.

**Notes**

-   The `MODIFY SETTING` privilege allows modifying table engine settings. It does not affect settings or server configuration parameters.
-   The `ATTACH` operation needs the [CREATE](#grant-create) privilege.
-   The `DETACH` operation needs the [DROP](#grant-drop) privilege.
-   To stop mutation by the [KILL MUTATION](../../sql-reference/statements/misc.md#kill-mutation) query, you need to have a privilege to start this mutation. For example, if you want to stop the `ALTER UPDATE` query, you need the `ALTER UPDATE`, `ALTER TABLE`, or `ALTER` privilege.

### CREATE {#grant-create}

Allows executing [CREATE](../../sql-reference/statements/create/index.md) and [ATTACH](../../sql-reference/statements/misc.md#attach) DDL-queries according to the following hierarchy of privileges:

-   `CREATE`. Level: `GROUP`
    -   `CREATE DATABASE`. Level: `DATABASE`
    -   `CREATE TABLE`. Level: `TABLE`
        -   `CREATE TEMPORARY TABLE`. Level: `GLOBAL`
    -   `CREATE VIEW`. Level: `VIEW`
    -   `CREATE DICTIONARY`. Level: `DICTIONARY`

**Notes**

-   To delete the created table, a user needs [DROP](#grant-drop).

### DROP {#grant-drop}

Allows executing [DROP](../../sql-reference/statements/misc.md#drop) and [DETACH](../../sql-reference/statements/misc.md#detach) queries according to the following hierarchy of privileges:

-   `DROP`. Level: `GROUP`
    -   `DROP DATABASE`. Level: `DATABASE`
    -   `DROP TABLE`. Level: `TABLE`
    -   `DROP VIEW`. Level: `VIEW`
    -   `DROP DICTIONARY`. Level: `DICTIONARY`

### TRUNCATE {#grant-truncate}

Allows executing [TRUNCATE](../../sql-reference/statements/misc.md#truncate-statement) queries.

Privilege level: `TABLE`.

### OPTIMIZE {#grant-optimize}

Allows executing [OPTIMIZE TABLE](../../sql-reference/statements/misc.md#misc_operations-optimize) queries.

Privilege level: `TABLE`.

### SHOW {#grant-show}

Allows executing `SHOW`, `DESCRIBE`, `USE`, and `EXISTS` queries according to the following hierarchy of privileges:

-   `SHOW`. Level: `GROUP`
    -   `SHOW DATABASES`. Level: `DATABASE`. Allows to execute `SHOW DATABASES`, `SHOW CREATE DATABASE`, `USE <database>` queries.
    -   `SHOW TABLES`. Level: `TABLE`. Allows to execute `SHOW TABLES`, `EXISTS <table>`, `CHECK <table>` queries.
    -   `SHOW COLUMNS`. Level: `COLUMN`. Allows to execute `SHOW CREATE TABLE`, `DESCRIBE` queries.
    -   `SHOW DICTIONARIES`. Level: `DICTIONARY`. Allows to execute `SHOW DICTIONARIES`, `SHOW CREATE DICTIONARY`, `EXISTS <dictionary>` queries.

**Notes**

A user has the `SHOW` privilege if it has any other privilege concerning the specified table, dictionary or database.

### KILL QUERY {#grant-kill-query}

Allows executing [KILL](../../sql-reference/statements/misc.md#kill-query-statement) queries according to the following hierarchy of privileges:

Privilege level: `GLOBAL`.

**Notes**

`KILL QUERY` privilege allows one user to kill queries of other users.

### ACCESS MANAGEMENT {#grant-access-management}

Allows a user to execute queries that manage users, roles and row policies.

-   `ACCESS MANAGEMENT`. Level: `GROUP`
    -   `CREATE USER`. Level: `GLOBAL`
    -   `ALTER USER`. Level: `GLOBAL`
    -   `DROP USER`. Level: `GLOBAL`
    -   `CREATE ROLE`. Level: `GLOBAL`
    -   `ALTER ROLE`. Level: `GLOBAL`
    -   `DROP ROLE`. Level: `GLOBAL`
    -   `ROLE ADMIN`. Level: `GLOBAL`
    -   `CREATE ROW POLICY`. Level: `GLOBAL`. Aliases: `CREATE POLICY`
    -   `ALTER ROW POLICY`. Level: `GLOBAL`. Aliases: `ALTER POLICY`
    -   `DROP ROW POLICY`. Level: `GLOBAL`. Aliases: `DROP POLICY`
    -   `CREATE QUOTA`. Level: `GLOBAL`
    -   `ALTER QUOTA`. Level: `GLOBAL`
    -   `DROP QUOTA`. Level: `GLOBAL`
    -   `CREATE SETTINGS PROFILE`. Level: `GLOBAL`. Aliases: `CREATE PROFILE`
    -   `ALTER SETTINGS PROFILE`. Level: `GLOBAL`. Aliases: `ALTER PROFILE`
    -   `DROP SETTINGS PROFILE`. Level: `GLOBAL`. Aliases: `DROP PROFILE`
    -   `SHOW ACCESS`. Level: `GROUP`
        -   `SHOW_USERS`. Level: `GLOBAL`. Aliases: `SHOW CREATE USER`
        -   `SHOW_ROLES`. Level: `GLOBAL`. Aliases: `SHOW CREATE ROLE`
        -   `SHOW_ROW_POLICIES`. Level: `GLOBAL`. Aliases: `SHOW POLICIES`, `SHOW CREATE ROW POLICY`, `SHOW CREATE POLICY`
        -   `SHOW_QUOTAS`. Level: `GLOBAL`. Aliases: `SHOW CREATE QUOTA`
        -   `SHOW_SETTINGS_PROFILES`. Level: `GLOBAL`. Aliases: `SHOW PROFILES`, `SHOW CREATE SETTINGS PROFILE`, `SHOW CREATE PROFILE`

The `ROLE ADMIN` privilege allows a user to assign and revoke any roles including those which are not assigned to the user with the admin option.

### SYSTEM {#grant-system}

Allows a user to execute [SYSTEM](../../sql-reference/statements/system.md) queries according to the following hierarchy of privileges.

-   `SYSTEM`. Level: `GROUP`
    -   `SYSTEM SHUTDOWN`. Level: `GLOBAL`. Aliases: `SYSTEM KILL`, `SHUTDOWN`
    -   `SYSTEM DROP CACHE`. Aliases: `DROP CACHE`
        -   `SYSTEM DROP DNS CACHE`. Level: `GLOBAL`. Aliases: `SYSTEM DROP DNS`, `DROP DNS CACHE`, `DROP DNS`
        -   `SYSTEM DROP MARK CACHE`. Level: `GLOBAL`. Aliases: `SYSTEM DROP MARK`, `DROP MARK CACHE`, `DROP MARKS`
        -   `SYSTEM DROP UNCOMPRESSED CACHE`. Level: `GLOBAL`. Aliases: `SYSTEM DROP UNCOMPRESSED`, `DROP UNCOMPRESSED CACHE`, `DROP UNCOMPRESSED`
    -   `SYSTEM RELOAD`. Level: `GROUP`
        -   `SYSTEM RELOAD CONFIG`. Level: `GLOBAL`. Aliases: `RELOAD CONFIG`
        -   `SYSTEM RELOAD DICTIONARY`. Level: `GLOBAL`. Aliases: `SYSTEM RELOAD DICTIONARIES`, `RELOAD DICTIONARY`, `RELOAD DICTIONARIES`
            -   `SYSTEM RELOAD EMBEDDED DICTIONARIES`. Level: `GLOBAL`. Aliases: `RELOAD EMBEDDED DICTIONARIES`
    -   `SYSTEM MERGES`. Level: `TABLE`. Aliases: `SYSTEM STOP MERGES`, `SYSTEM START MERGES`, `STOP MERGES`, `START MERGES`
    -   `SYSTEM TTL MERGES`. Level: `TABLE`. Aliases: `SYSTEM STOP TTL MERGES`, `SYSTEM START TTL MERGES`, `STOP TTL MERGES`, `START TTL MERGES`
    -   `SYSTEM FETCHES`. Level: `TABLE`. Aliases: `SYSTEM STOP FETCHES`, `SYSTEM START FETCHES`, `STOP FETCHES`, `START FETCHES`
    -   `SYSTEM MOVES`. Level: `TABLE`. Aliases: `SYSTEM STOP MOVES`, `SYSTEM START MOVES`, `STOP MOVES`, `START MOVES`
    -   `SYSTEM SENDS`. Level: `GROUP`. Aliases: `SYSTEM STOP SENDS`, `SYSTEM START SENDS`, `STOP SENDS`, `START SENDS`
        -   `SYSTEM DISTRIBUTED SENDS`. Level: `TABLE`. Aliases: `SYSTEM STOP DISTRIBUTED SENDS`, `SYSTEM START DISTRIBUTED SENDS`, `STOP DISTRIBUTED SENDS`, `START DISTRIBUTED SENDS`
        -   `SYSTEM REPLICATED SENDS`. Level: `TABLE`. Aliases: `SYSTEM STOP REPLICATED SENDS`, `SYSTEM START REPLICATED SENDS`, `STOP REPLICATED SENDS`, `START REPLICATED SENDS`
    -   `SYSTEM REPLICATION QUEUES`. Level: `TABLE`. Aliases: `SYSTEM STOP REPLICATION QUEUES`, `SYSTEM START REPLICATION QUEUES`, `STOP REPLICATION QUEUES`, `START REPLICATION QUEUES`
    -   `SYSTEM SYNC REPLICA`. Level: `TABLE`. Aliases: `SYNC REPLICA`
    -   `SYSTEM RESTART REPLICA`. Level: `TABLE`. Aliases: `RESTART REPLICA`
    -   `SYSTEM FLUSH`. Level: `GROUP`
        -   `SYSTEM FLUSH DISTRIBUTED`. Level: `TABLE`. Aliases: `FLUSH DISTRIBUTED`
        -   `SYSTEM FLUSH LOGS`. Level: `GLOBAL`. Aliases: `FLUSH LOGS`

The `SYSTEM RELOAD EMBEDDED DICTIONARIES` privilege implicitly granted by the `SYSTEM RELOAD DICTIONARY ON *.*` privilege.

### INTROSPECTION {#grant-introspection}

Allows using [introspection](../../operations/optimizing-performance/sampling-query-profiler.md) functions.

-   `INTROSPECTION`. Level: `GROUP`. Aliases: `INTROSPECTION FUNCTIONS`
    -   `addressToLine`. Level: `GLOBAL`
    -   `addressToSymbol`. Level: `GLOBAL`
    -   `demangle`. Level: `GLOBAL`

### SOURCES {#grant-sources}

Allows using external data sources. Applies to [table engines](../../engines/table-engines/index.md) and [table functions](../../sql-reference/table-functions/index.md#table-functions).

-   `SOURCES`. Level: `GROUP`
    -   `FILE`. Level: `GLOBAL`
    -   `URL`. Level: `GLOBAL`
    -   `REMOTE`. Level: `GLOBAL`
    -   `YSQL`. Level: `GLOBAL`
    -   `ODBC`. Level: `GLOBAL`
    -   `JDBC`. Level: `GLOBAL`
    -   `HDFS`. Level: `GLOBAL`
    -   `S3`. Level: `GLOBAL`

The `SOURCES` privilege enables use of all the sources. Also you can grant a privilege for each source individually. To use sources, you need additional privileges.

Examples:

-   To create a table with the [MySQL table engine](../../engines/table-engines/integrations/mysql.md), you need `CREATE TABLE (ON db.table_name)` and `MYSQL` privileges.
-   To use the [mysql table function](../../sql-reference/table-functions/mysql.md), you need `CREATE TEMPORARY TABLE` and `MYSQL` privileges.

### dictGet {#grant-dictget}

-   `dictGet`. Aliases: `dictHas`, `dictGetHierarchy`, `dictIsIn`

Allows a user to execute [dictGet](../../sql-reference/functions/ext-dict-functions.md#dictget), [dictHas](../../sql-reference/functions/ext-dict-functions.md#dicthas), [dictGetHierarchy](../../sql-reference/functions/ext-dict-functions.md#dictgethierarchy), [dictIsIn](../../sql-reference/functions/ext-dict-functions.md#dictisin) functions.

Privilege level: `DICTIONARY`.

**Examples**

-   `GRANT dictGet ON mydb.mydictionary TO john`
-   `GRANT dictGet ON mydictionary TO john`

### ALL {#grant-all}

Grants all the privileges on regulated entity to a user account or a role.

### NONE {#grant-none}

Doesn’t grant any privileges.

### ADMIN OPTION {#admin-option-privilege}

The `ADMIN OPTION` privilege allows a user to grant their role to another user.

