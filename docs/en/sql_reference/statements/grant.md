---
toc_priority: 39
toc_title: GRANT
---


# GRANT

- Grants [privileges](#grant-privileges) to ClickHouse user accounts or roles.
- Assigns roles to user accounts or to another roles.

To revoke privileges, use the [REVOKE](revoke.md) statement. Also you can list granted privileges by the [SHOW GRANTS](show.md#show-grants-statement) statement.

## Granting Privilege Syntax {#grant-privigele-syntax}

```sql
GRANT [ON CLUSTER cluster_name] privilege[(column_name [,...])] [,...] ON {db.table|db.*|*.*|table|*} TO {user | role | CURRENT_USER} [,...] [WITH GRANT OPTION]
```
- `privilege` — Type of privilege.
- `role` — ClickHouse user role.
- `user` — ClickHouse user account.

The `WITH GRANT OPTION` clause grants `user` or `role` with permission to perform the `GRANT` query. User can grant privileges only inside the scope of their account privileges.


## Assigning Role Syntax {#assign-role-syntax}

```sql
GRANT [ON CLUSTER cluster_name] role [,...] TO {user | another_role | CURRENT_USER} [,...] [WITH ADMIN OPTION]
```

- `role` — ClickHouse user role.
- `user` — ClickHouse user account.

The `WITH ADMIN OPTION` clause sets [ADMIN OPTION](#admin-option-privilege) privilege for `user` or `role`.

## Usage {#grant-usage}

To use `GRANT`, your account must have the `GRANT OPTION` privilege. You can grant privileges only inside the scope of your account privileges.

For example, administrator has granted privileges to the `john` account by the query:

```sql
GRANT SELECT(x,y) ON db.table TO john WITH GRANT OPTION
```

It means that `john` has the permission to perform:

- `SELECT x,y FROM db.table`.
- `SELECT x FROM db.table`.
- `SELECT y FROM db.table`.

`john` can't perform `SELECT z FROM db.table`. The `SELECT * FROM db.table` also is not available. ClickHouse doesn't return any data, even `x` and `y`.

Also `john` has the `GRANT OPTION` privilege, so it can grant other users with privileges of the same or the smaller scope.

Specifying privileges you can use asterisk (`*`) instead of a table or a database name. For example, the `GRANT SELECT ON db.* TO john` query allows `john` to perform the `SELECT` query over all the tables in `db` database. Also, you can omit database name. In this case privileges are granted for current database, for example: `GRANT SELECT ON * TO john` grants the privilege on all the tables in the current database, `GRANT SELECT ON mytable TO john` grants the privilege on the `mytable` table in the current database.

You can grant multiple privileges to multiple accounts in one query. The query `GRANT SELECT, INSERT ON *.* TO john, petya` allows accounts `john` and `petya` to perform the `INSERT` and `SELECT` queries over all the tables in all the databases on the server.


## Privileges {#grant-privileges}

Privilege is a permission to perform specific kind of queries.

Privileges have an hierarchic structure. A set of permitted queries depends on the privilege scope.

Top scope privileges:

- [SELECT](#grant-select)
- [INSERT](#grant-insert)
- [ALTER](#grant-alter)
- [CREATE](#grant-create)
- [DROP](#grant-drop)
- [TRUNCATE](#grant-truncate)
- [OPTIMIZE](#grant-optimize)
- [SHOW](#grant-show)
- [EXISTS](#grant-exists)
- [KILL QUERY](#grant-kill-query)
- [CREATE USER](#grant-create-user)
- [ACCESS MANAGEMENT](#grant-access-management)
- [SYSTEM](#grant-system)
- [INTROSPECTION](#grant-introspection)
- [SOURCES](#grant-SOURCES)
- [dictGet](#grant-dictget)

The special privilege [ALL](#grant-all) grants all the privileges to a user account or a role.

By default, a user account or a role has no privileges.

If a user or role have no privileges it displayed as [NONE](#grant-none) privilege.

Some queries by their implementation require a set of privileges. For example, to perform the [RENAME](misc.md#misc_operations-rename) query you need the following privileges: `SELECT`, `CREATE TABLE`, `INSERT` and `DROP TABLE`.


### SELECT {#grant-select}

Allows to perform [SELECT](select.md) queries.

**Description**

User granted with this privilege can perform `SELECT` queries over a specified list of columns in the specified table and database. If user includes other columns then specified a query returns no data. 

Consider the following privilege:

```sql
GRANT SELECT(x,y) ON db.table TO john
```

This privilege allows `john` to perform any `SELECT` query that involves data from the `x` and/or `y` columns in `db.table`. For example, `SELECT x FROM db.table`. `john` can't perform `SELECT z FROM db.table`. The `SELECT * FROM db.table` also is not available. ClickHouse doesn't return any data, even `x` and `y`.

### INSERT {#grant-insert}

Allows to perform [INSERT](insert_into.md) queries.

**Description**

User granted with this privilege can perform `INSERT` queries over a specified list of columns in the specified table and database. If user includes other columns then specified a query doesn't insert any data. 

Consider the following privilege:

```sql
GRANT INSERT(x,y) ON db.table TO john
```

This privilege allows `john` to perform any `SELECT` query that involves data from the `x` and/or `y` columns in `db.table`. For example, `SELECT x FROM db.table`. `john` can't perform `SELECT z FROM db.table`. The `SELECT * FROM db.table` also is not available. ClickHouse doesn't return any data, even `x` and `y`.

### ALTER {#grant-alter}

Allows to perform [ALTER](alter.md) queries corresponding to the following hierarchy of privileges:

- `ALTER`
    - `ALTER TABLE`
        - `ALTER UPDATE`. Aliases: `UPDATE`
        - `ALTER DELETE`. Aliases: `DELETE`
        - `ALTER COLUMN`
            - `ALTER ADD COLUMN`. Aliases: `ADD COLUMN`
            - `ALTER DROP COLUMN`. Aliases: `DROP COLUMN`
            - `ALTER MODIFY COLUMN`. Aliases: `MODIFY COLUMN`
            - `ALTER COMMENT COLUMN`. Aliases: `COMMENT COLUMN`
            - `ALTER CLEAR COLUMN`. Aliases: `CLEAR COLUMN`
            - `ALTER RENAME COLUMN`. Aliases: `RENAME COLUMN`
        - `ALTER INDEX`. Aliases: `INDEX`
            - `ALTER ORDER BY`. Aliases: `ALTER MODIFY ORDER BY`, `MODIFY ORDER BY`
            - `ALTER ADD INDEX`. Aliases: `ADD INDEX`
            - `ALTER DROP INDEX`. Aliases: `DROP INDEX`
            - `ALTER MATERIALIZE INDEX`. Aliases: `MATERIALIZE INDEX`
            - `ALTER CLEAR INDEX`. Aliases: `CLEAR INDEX`
        - `ALTER CONSTRAINT`. Aliases: `CONSTRAINT`
            - `ALTER ADD CONSTRAINT`. Aliases: `ADD CONSTRAINT`
            - `ALTER DROP CONSTRAINT`. Aliases: `DROP CONSTRAINT`
        - `ALTER TTL`. Aliases: `ALTER MODIFY TTL`, `MODIFY TTL`
        - `ALTER MATERIALIZE TTL`. Aliases: `MATERIALIZE TTL`
        - `ALTER SETTINGS`. Aliases: `ALTER SETTING`, `ALTER MODIFY SETTING`, `MODIFY SETTING`
        - `ALTER MOVE PARTITION`. Aliases: `ALTER MOVE PART`, `MOVE PARTITION`, `MOVE PART`
        - `ALTER FETCH PARTITION`. Aliases: `FETCH PARTITION`
        - `ALTER FREEZE PARTITION`. Aliases: `FREEZE PARTITION`
    - `ALTER VIEW`
        - `ALTER VIEW REFRESH `. Aliases: `ALTER LIVE VIEW REFRESH`, `REFRESH VIEW`
        - `ALTER VIEW MODIFY QUERY`. Aliases: `ALTER TABLE MODIFY QUERY`

Examples of how this hierarchy is treated:

- The `ALTER` privilege includes all other privileges. 
- `ALTER CONSTRAINT` includes `ADD CONSTRAINT` and `DROP CONSTRAINT` privileges.

**Notes**

- The `MODIFY SETTING` privilege allows to modify table engine settings. In doesn't affect settings or server configuration parameters.
- The `ATTACH` operation needs the [CREATE](#grant-create) privilege.
- The `DETACH` operation needs the [DROP](#grant-drop) privilege.
- To stop mutation by the [KILL MUTATION](misc.md#kill-mutation-statement) query, you need to have a privilege to start this mutation. For example, if you want to stop

### CREATE {#grant-create}

Allows to perform [CREATE](create.md) and [ATTACH](misc.md#attach) DDL-queries corresponding to the following hierarchy of privileges:

- `CREATE`
    - `CREATE DATABASE`
    - `CREATE TABLE`
    - `CREATE VIEW`
    - `CREATE DICTIONARY`
    - `CREATE TEMPORARY TABLE`

**Notes**

- The `CREATE` privilege doesn't allow a grantee to delete the created table. A user needs [DROP](#grant-drop).

### DROP {#grant-drop}

Allows to perform [DROP](misc.md#drop-statement) queries corresponding to the following hierarchy of privileges:

- `DROP`
    - `DROP DATABASE`
    - `DROP TABLE`
    - `DROP VIEW`
    - `DROP DICTIONARY`


### TRUNCATE {#grant-truncate}

Allows to perform [TRUNCATE](misc.md#truncate-statement) queries.

### OPTIMIZE {#grant-optimize}

Allows to perform the [OPTIMIZE TABLE](misc.md#misc_operations-optimize) queries.


### SHOW {#grant-show}

Allows to perform `SHOW`, `DESCRIBE`, `USE`, and `EXISTS` queries, corresponding to the following hierarchy of privileges:

- `SHOW`
    - `SHOW DATABASES`. Allows to execute `SHOW DATABASES`, `SHOW CREATE DATABASE`, `USE <database>` queries.
    - `SHOW TABLES`. Allows to execute `SHOW TABLES`, `EXISTS <table>`, `CHECK <table>` queries.
    - `SHOW COLUMNS`.  Allows to execute `SHOW CREATE TABLE`, `DESCRIBE` queries.
    - `SHOW DICTIONARIES`. Allows to execute `SHOW DICTIONARIES`, `SHOW CREATE DICTIONARY`, `EXISTS <dictionary>` queries.

**Notes**

A user has the `SHOW` privilege if it has any another privilege concerning the specified table, dictionary or database.


### EXISTS {#grant-exists}

Allows to perform the [EXISTS](misc.md#exists-statement) queries.


### KILL QUERY {#grant-kill-query}

Allows to perform the [KILL](misc.md#kill-query-statement) queries corresponding to the following hierarchy of privileges:

**Notes**

`KILL QUERY` privilege allows one user to kill queries of other users.


### CREATE USER {#grant-create-user}

Allows to manage user accounts, roles and row policy by `CREATE` and `DROP` queries corresponding to the following hierarchy of privileges:

- `CREATE USER`
    - `DROP USER`
    - `CREATE ROLE`
        - `DROP ROLE`
    - `CREATE ROW POLICY`
        - `DROP ROW POLICY`
    - `CREATE QUOTA`
        - `DROP QUOTA`


### ACCESS MANAGEMENT {#grant-access-management}

Allows a user to perform queries that manage users, roles and row policies.

- `ACCESS MANAGEMENT`
    - `CREATE USER`
    - `ALTER USER`
    - `DROP USER`
    - `CREATE ROLE`
    - `ALTER ROLE`
    - `DROP ROLE`
    - `CREATE ROW POLICY`. Aliases: `CREATE POLICY`
    - `ALTER ROW POLICY`. Aliases: `ALTER POLICY`
    - `DROP ROW POLICY`. Aliases: `DROP POLICY`
    - `CREATE QUOTA`
    - `ALTER QUOTA`
    - `DROP QUOTA`
    - `CREATE SETTINGS PROFILE`. Aliases: `CREATE PROFILE`
    - `ALTER SETTINGS PROFILE`. Aliases: `ALTER PROFILE`
    - `DROP SETTINGS PROFILE`. Aliases: `DROP PROFILE`
    - `SHOW ACCESS`
        - `SHOW_USERS`. Aliases: `SHOW CREATE USER`
        - `SHOW_ROLES`. Aliases: `SHOW CREATE ROLE`
        - `SHOW_ROW_POLICIES`. Aliases: `SHOW POLICIES`, `SHOW CREATE ROW POLICY`, `SHOW CREATE POLICY`
        - `SHOW_QUOTAS`. Aliases: `SHOW CREATE QUOTA`
        - `SHOW_SETTINGS_PROFILES`. Aliases: `SHOW PROFILES`, `SHOW CREATE SETTINGS PROFILE`, `SHOW CREATE PROFILE`

The `ROLE ADMIN` privilege allows to grant and revoke the roles which are not granted to the current user with admin option.

### SYSTEM {#grant-system}

Allows a user to perform the [SYSTEM](system.md) queries corresponding to the following hierarchy of privileges.

- `SYSTEM`
    - `SYSTEM SHUTDOWN`. Aliases: `SYSTEM KILL`, `SHUTDOWN`
    - `SYSTEM DROP CACHE`. Aliases: `DROP CACHE`
        - `SYSTEM DROP DNS CACHE`. Aliases: `SYSTEM DROP DNS`, `DROP DNS CACHE`, `DROP DNS`
        - `SYSTEM DROP MARK CACHE`. Aliases: `SYSTEM DROP MARK`, `DROP MARK CACHE`, `DROP MARKS`
        - `SYSTEM DROP UNCOMPRESSED CACHE`. Aliases: `SYSTEM DROP UNCOMPRESSED`, `DROP UNCOMPRESSED CACHE`, `DROP UNCOMPRESSED`
    - `SYSTEM RELOAD`
        - `SYSTEM RELOAD CONFIG`. Aliases: `RELOAD CONFIG`
        - `SYSTEM RELOAD DICTIONARY`. Aliases: `SYSTEM RELOAD DICTIONARIES`, `RELOAD DICTIONARY`, `RELOAD DICTIONARIES`
        - `SYSTEM RELOAD EMBEDDED DICTIONARIES`. Aliases: R`ELOAD EMBEDDED DICTIONARIES`
    - `SYSTEM MERGES`. Aliases: `SYSTEM STOP MERGES`, `SYSTEM START MERGES`, `STOP MERGES`, `START MERGES`
    - `SYSTEM TTL MERGES`. Aliases: `SYSTEM STOP TTL MERGES`, `SYSTEM START TTL MERGES`, `STOP TTL MERGES`, `START TTL MERGES`
    - `SYSTEM FETCHES`. Aliases: `SYSTEM STOP FETCHES`, `SYSTEM START FETCHES`, `STOP FETCHES`, `START FETCHES`
    - `SYSTEM MOVES`. Aliases: `SYSTEM STOP MOVES`, `SYSTEM START MOVES`, `STOP MOVES`, `START MOVES`
    - `SYSTEM SENDS`. Aliases: `SYSTEM STOP SENDS`, `SYSTEM START SENDS`, `STOP SENDS`, `START SENDS`
        - `SYSTEM DISTRIBUTED SENDS`. Aliases: `SYSTEM STOP DISTRIBUTED SENDS`, `SYSTEM START DISTRIBUTED SENDS`, `STOP DISTRIBUTED SENDS`, `START DISTRIBUTED SENDS`
        - `SYSTEM REPLICATED SENDS`. Aliases: `SYSTEM STOP REPLICATED SENDS`, `SYSTEM START REPLICATED SENDS`, `STOP REPLICATED SENDS`, `START REPLICATED SENDS`
    - `SYSTEM REPLICATION QUEUES`. Aliases: `SYSTEM STOP REPLICATION QUEUES`, `SYSTEM START REPLICATION QUEUES`, `STOP REPLICATION QUEUES`, `START REPLICATION QUEUES`
    - `SYSTEM SYNC REPLICA`. Aliases: `SYNC REPLICA`
    - `SYSTEM RESTART REPLICA`. Aliases: `RESTART REPLICA`
    - `SYSTEM FLUSH`
        - `SYSTEM FLUSH DISTRIBUTED`. Aliases: `FLUSH DISTRIBUTED`
        - `SYSTEM FLUSH LOGS`. Aliases: `FLUSH LOGS`

The `SYSTEM RELOAD EMBEDDED DICTIONARIES` privilege implicitly granted by the `SYSTEM RELOAD DICTIONARY ON *.*` privilege.


### INTROSPECTION {#grant-introspection}

Allows using [introspection](../../operations/optimizing_performance/sampling_query_profiler.md) functions.

- `INTROSPECTION`. Aliases: `INTROSPECTION FUNCTIONS`
    - `addressToLine`
    - `addressToSymbol`
    - `demangle`


### SOURCES {#grant-sources}

Allows using external data sources. Applies to [table engines](../../engines/table_engines/index.md) and [table functions](../table_functions/index.md).

- `SOURCES`
    - `FILE`
    - `URL`
    - `REMOTE`
    - `YSQL`
    - `ODBC`
    - `JDBC`
    - `HDFS`
    - `S3`

The `SOURCES` privilege enables use of all the sources. Also you can grant a privilege for each source individually.

Table functions create temporary tables. Another way of creating a temporary table is the [CREATE TEMPORARY TABLE](create.md#temporary-tables) statement. Privileges for these ways of creating a table are granted independently and don't affect each other.


### dictGet {#grant-dictget}

- `dictGet`. Aliases: `dictHas`, `dictGetHierarchy`, `dictIsIn`

Allows a user to execute [dictGet](../functions/ext_dict_functions.md#dictget), [dictHas](../functions/ext_dict_functions.md#dicthas), [dictGetHierarchy](../functions/ext_dict_functions.md#dictgethierarchy), [dictIsIn](../functions/ext_dict_functions.md#dictisin) functions.

Some kinds of ClickHouse [dictionaries](../dictionaries/index.md) are not stored in a database. Use the `'no_database'` placeholder to grant a privilege to use `dictGet` with such dictionaries.

**Examples**

- `GRANT dictGet ON mydb.mydictionary TO john`
- `GRANT dictGet ON mydictionary TO john`
- `GRANT dictGet ON 'no_database'.mydictionary TO john`

### ALL {#grant-all}

Grants all the privileges on regulated entity to a user account or a role._

### NONE {#grant-none}

Doesn't grant any privileges.


### ADMIN OPTION {#admin-option-privilege}

The `ADMIN OPTION` privilege allows user can reassign their role to another user.

[Original article](https://clickhouse.tech/docs/en/query_language/grant/) <!--hide-->
