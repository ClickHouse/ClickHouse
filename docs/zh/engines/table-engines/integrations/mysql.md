# MySQL {#mysql}

MySQL 引擎可以对存储在远程 MySQL 服务器上的数据执行 `SELECT` 查询。

调用格式：

    MySQL('host:port', 'database', 'table', 'user', 'password'[, replace_query, 'on_duplicate_clause']);

**调用参数**

-   `host:port` — MySQL 服务器地址。
-   `database` — 数据库的名称。
-   `table` — 表名称。
-   `user` — 数据库用户。
-   `password` — 用户密码。
-   `replace_query` — 将 `INSERT INTO` 查询是否替换为 `REPLACE INTO` 的标志。如果 `replace_query=1`，则替换查询
-   `'on_duplicate_clause'` — 将 `ON DUPLICATE KEY UPDATE 'on_duplicate_clause'` 表达式添加到 `INSERT` 查询语句中。例如：`impression = VALUES(impression) + impression`。如果需要指定 `'on_duplicate_clause'`，则需要设置 `replace_query=0`。如果同时设置 `replace_query = 1` 和 `'on_duplicate_clause'`，则会抛出异常。

此时，简单的 `WHERE` 子句（例如 `=, !=, >, >=, <, <=`）是在 MySQL 服务器上执行。

其余条件以及 `LIMIT` 采样约束语句仅在对MySQL的查询完成后才在ClickHouse中执行。

`MySQL` 引擎不支持 [可为空](../../../engines/table-engines/integrations/mysql.md) 数据类型，因此，当从MySQL表中读取数据时，`NULL` 将转换为指定列类型的默认值（通常为0或空字符串）。

[原始文章](https://clickhouse.com/docs/zh/operations/table_engines/mysql/) <!--hide-->
