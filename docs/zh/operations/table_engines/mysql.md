# MySQL

MySQL 引擎可以对存储在远程 MySQL 服务器上的数据执行 `SELECT` 查询。

老版格式：

```
MySQL(remote_address, remote_database, remote_table_name, user, password[, replace_query, on_duplicate_clause]);
```

新版格式:

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

必要参数：

- `remote_address` — MySQL 服务器地址。
- `remote_database` — 数据库的名称。
- `remote_table_name` — 表名称。
- `user` — 数据库用户。
- `password` — 用户密码。

可选参数：

- `replace_query` — 将 `INSERT INTO` 查询是否替换为 `REPLACE INTO` 的标志。如果 `replace_query=1`，则替换查询
- `on_duplicate_clause` — 将 `ON DUPLICATE KEY UPDATE 'on_duplicate_clause'` 表达式添加到 `INSERT` 查询语句中。例如：`impression = VALUES(impression) + impression`。如果需要指定 `'on_duplicate_clause'`，则需要设置 `replace_query=0`。如果同时设置 `replace_query = 1` 和 `'on_duplicate_clause'`，则会抛出异常。
- `mysql_variable_wait_timeout` — MySQL 系统变量，需要支持Session级别并且在使用时需要添加`mysql_variable_`前缀。

此时，简单的 `WHERE` 子句（例如 ` =, !=, >, >=, <, <=`）是在 MySQL 服务器上执行。

其余条件以及 `LIMIT` 采样约束语句仅在对MySQL的查询完成后才在ClickHouse中执行。

`MySQL` 引擎不支持 [Nullable](../../data_types/nullable.md) 数据类型，因此，当从MySQL表中读取数据时，`NULL` 将转换为指定列类型的默认值（通常为0或空字符串）。


## 配置

与 `GraphiteMergeTree` 类似，MySQL 引擎支持使用ClickHouse配置文件进行扩展配置。可以使用两个配置键：全局 (`mysql`) 和 库级别 (`mysql_*`)以及 表级别 (`mysql_*_*`)。首先应用全局配置，然后应用库级配置以及表级配置（如果存在）。

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

配置中可以包含所有MySQL引擎中的SETTINGS子句允许的内容。

[Original article](https://clickhouse.yandex/docs/zh/operations/table_engines/mysql/) <!--hide-->
