---
toc_priority: 39
toc_title: GRANT
---


# GRANT

Grants [privileges](#grant-privileges) to a ClickHouse user account or a role.

To revoke privileges, use the [REVOKE](revoke.md) statement. Also you can list granted privileges by the [SHOW GRANTS](show.md#show-grants-statement) statement.

## Syntax {#grant-syntax}

### Granting privilege to a User Account

```sql
GRANT privilege[(column_name [,...])] [,...] ON {db.table|db.*|*.*|table|*} TO {user | role | CURRENT_USER} [,...] [WITH GRANT OPTION]
```
- `privilege` — Type of privilege.
- `role` — ClickHouse user role.
- `user` — ClickHouse user account.

The `WITH GRANT OPTION` clause sets [GRANT OPTION](#grant-option-privilege) privilege for `user` or `role`.

### Assigning Role to a User Account

```sql
GRANT role [,...] TO {user | another_role | CURRENT_USER} [,...] [WITH ADMIN OPTION]
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

Specifying privileges you can use asterisk (`*`) instead of a table or a database name. For example, the `GRANT SELECT ON db.* TO john` query allows `john` to perform the `SELECT` query over all the tables in `db` database. Also, you can omit database name. In this case privileges are granted for current database.

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
- [KILL](#grant-kill)
- [CREATE USER](#grant-create-user)
- [ROLE ADMIN](#grant-role-admin)
- [SYSTEM](#grant-system)
- [INTROSPECTION](#grant-introspection)
- [dictGet](#grant-dictget)
- [Table Functions](#grant-table-functions)

The special privilege [ALL](#grant-all) grants all the privileges to a user account or a role.

By default, a user account or a role has no privileges.

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
    - `UPDATE`
    - `DELETE`
    - `ALTER COLUMN`
      - `ADD COLUMN`
      - `DROP COLUMN`
      - `MODIFY COLUMN`
      - `COMMENT COLUMN`
    - `INDEX`
      - `ALTER ORDER BY`
      - `ADD INDEX`
      - `DROP INDEX`
      - `MATERIALIZE INDEX`
      - `CLEAR INDEX`
    - `ALTER CONSTRAINT`
      - `ADD CONSTRAINT`
      - `DROP CONSTRAINT`
    - `MODIFY TTL`
    - `MODIFY SETTING`
    - `MOVE PARTITION`
    - `FETCH PARTITION`
    - `FREEZE PARTITION`
    - `ALTER VIEW`
      - `REFRESH VIEW`
      - `MODIFY VIEW QUERY`

Examples of how this hierarchy is treated:

- The `ALTER` privilege includes all other privileges. 
- `ALTER CONSTRAINT` includes `ADD CONSTRAINT` and `DROP CONSTRAINT` privileges.

**Notes**

- The `MODIFY SETTING` privilege allows to modify table engine settings. In doesn't affect settings or server configuration parameters.
- The `ATTACH` operation needs the [CREATE](#grant-create) privilege.
- The `DETACH` operation needs the [DROP](#grant-drop) privilege.
- To stop mutation by the [KILL MUTATION](misc.md#kill-mutation-statement) query, you need to have a privilege to start this mutation. For example, if you want to stop

### CREATE {#grant-create}

Allows to perform [CREATE](create.md) DDL-queries corresponding to the following hierarchy of privileges:

- `CREATE`
    - `CREATE DATABASE`
    - `CREATE TABLE`
      - `CREATE VIEW`
    - `CREATE TEMPORARY TABLE`
    - `CREATE DICTIONARY`

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

Allows to perform [TRUNCATE](misc.md#truncate-statement) queries corresponding to the following hierarchy of privileges:

- `TRUNCATE`
  - `TRUNCATE TABLE`
    - `TRUNCATE VIEW`

### OPTIMIZE {#grant-optimize}

Allows to perform the [OPTIMIZE TABLE](misc.md#misc_operations-optimize) queries.


### SHOW {#grant-show}

Allows to perform the [SHOW CREATE TABLE](show.md#show-create-table-statement) queries.

**Notes**

A user has the `SHOW` privilege if it has any another right concerning the specified table.

### EXISTS {#grant-exists}

Allows to perform the [EXISTS](misc.md#exists-statement) queries.

### KILL {#grant-kill}

Allows to perform the [KILL](misc.md#kill-query-statement) queries corresponding to the following hierarchy of privileges:

- `KILL`
  - `KILL QUERY`

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

### ROLE ADMIN {#grant-role-admin}

Allows a user to grant and revoke roles of other users.

**Syntax**

```sql
ROLE ADMIN
```

### SYSTEM {#grant-system}

Allows a user to perform the [SYSTEM](system.md) queries corresponding to the following hierarchy of privileges.

- `SYSTEM`
  - `SHUTDOWN`
  - `DROP CACHE`
  - `RELOAD CONFIG`
  - `RELOAD DICTIONARY`
  - `STOP MERGES`
  - `STOP TTL MERGES`
  - `STOP FETCHES`
  - `STOP MOVES`
  - `STOP DISTRIBUTED_SENDS`
  - `STOP REPLICATED_SENDS`
  - `SYNC REPLICA`
  - `RESTART REPLICA`
  - `FLUSH DISTRIBUTED`
  - `FLUSH LOGS`


### INTROSPECTION {#grant-introspection}

Allows using [introspection](../operations/performance/sampling_query_profiler.md) functions.

- `INTROSPECTION`
  - `addressToLine()`
  - `addressToSymbol()`
  - `demangle()`


### dictGet {#grant-dictget}

Allows a user to execute the [dictGet](functions/ext_dict_functions.md#dictget) function.

Some kinds of ClickHouse [dictionaries](dicts/index.md) are not stored in a database. Use the `'no_database'` placeholder to grant a privilege to use `dictGet()` with such dictionaries.

**Examples**

- `GRANT dictGet() ON mydb.mydictionary TO john`
- `GRANT dictGet() ON mydictionary TO john`
- `GRANT dictGet() ON 'no_database'.mydictionary TO john`

### Table Functions {#grant-table-functions}

Allows using [table functions](table_functions/index.md).

- `TABLE FUNCTIONS`
  - `file()`
  - `url()`
  - `input()`
  - `values()`
  - `numbers()`
  - `merge()`
  - `remote()`
  - `mysql()`
  - `odbc()`
  - `jdbc()`
  - `jdfs()`
  - `s3()`

The `TABLE FUNCTIONS` privilege enables use of all the table functions. Also you can grant a privilege for each function individually.

Table functions create temporary tables. Another way of creating a temporary table is the [CREATE TEMPORARY TABLE](create.md#temporary-tables) statement. Privileges for these ways of creating a table are granted independently and don't affect each other.

### ALL {#grant-all}

Grants all the privileges on regulated entity to a user account or a role.

### GRANT OPTION {#grant-option-privilege}

To use `GRANT`, a user account must have the `GRANT OPTION` privilege. User can grant privileges only inside the scope of their account privileges.

### ADMIN OPTION {#admin-option-privilege}

The `ADMIN OPTION` privilege allows user can reassign their role to another user.

[Original article](https://clickhouse.tech/docs/en/query_language/grant/) <!--hide-->
