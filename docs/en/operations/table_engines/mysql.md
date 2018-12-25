
# MySQL

The MySQL engine allows you to perform `SELECT` queries on data that is stored on a remote MySQL server.

Old format:

```
MySQL(remote_address, remote_database, remote_table_name, user, password[, replace_query, on_duplicate_clause]);
```

New format:

```
MySQL SETTINGS
  remote_address = 'host:port',
  remote_database = 'database',
  remote_table_name = 'table',
  user = 'user',
  password = 'password'
  replace_query = 0,
  on_duplicate_clause = 'on_duplicate_clause',
  mysql_variable_wait_timeout = 28800
```

Required parameters:

- `remote_address` — Address of the MySQL server.
- `remote_database` — Database name on the MySQL server.
- `remote_table_name` — Name of the table.
- `user` — The MySQL User.
- `password` — User password.

Optional parameters:

- `replace_query` — Flag that sets query substitution `INSERT INTO` to `REPLACE INTO`. If `replace_query=1`, the query is replaced.
- `on_duplicate_clause` — Adds the `ON DUPLICATE KEY on_duplicate_clause` expression to the `INSERT` query.

Example: `INSERT INTO t (c1,c2) VALUES ('a', 2) ON DUPLICATE KEY UPDATE c2 = c2 + 1`, where `on_duplicate_clause` is `UPDATE c2 = c2 + 1`. See MySQL documentation to find which `on_duplicate_clause` you can use with `ON DUPLICATE KEY` clause.

To specify `on_duplicate_clause` you need to pass `0` to the `replace_query` parameter. If you simultaneously pass `replace_query = 1` and `on_duplicate_clause`, ClickHouse generates an exception.

- `mysql_variable_wait_timeout` — MySQL system variables, require support session scope and you need to add `mysql_variable_` prefix when using it.


At this time, simple `WHERE` clauses such as ` =, !=, >, >=, <, <=` are executed on the MySQL server.

The rest of the conditions and the `LIMIT` sampling constraint are executed in ClickHouse only after the query to MySQL finishes.

The `MySQL` engine does not support the [Nullable](../../data_types/nullable.md) data type, so when reading data from MySQL tables, `NULL` is converted to default values for the specified column type (usually 0 or an empty string).


## Configuration

Similar to GraphiteMergeTree, the MySQL engine supports extended configuration using the ClickHouse config file. There are three configuration keys that you can use: global (`mysql`) and database-level (`mysql_*`) and table-level (`mysql_*_*`). The global configuration is applied first, and then the database-level and table-level configuration is applied (if it exists).

```xml
  <!--  Global configuration options for all tables of MySQL engine type -->
  <mysql>
    <remote_address>host:port</remote_address>
    <mysql_variable_wait_timeout>28800</mysql_variable_wait_timeout>
  </mysql>

  <!-- Configuration specific for "database_name" database -->
  <mysql_database_name>
    <remote_address>host:port</remote_address>
    <mysql_variable_wait_timeout>28800</mysql_variable_wait_timeout>
  </mysql_database_name>

  <!-- Configuration specific for "database_name.table_name" table-->
    <mysql_database_name_table_name>
      <remote_address>host:port</remote_address>
      <mysql_variable_wait_timeout>28800</mysql_variable_wait_timeout>
    </mysql_database_name_table_name>
```

This list can contain the all SETTINGS clauses of MySQL engine type.

[Original article](https://clickhouse.yandex/docs/en/operations/table_engines/mysql/) <!--hide-->
