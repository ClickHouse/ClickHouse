# mysql {#mysql}

允许对存储在远程MySQL服务器上的数据执行`SELECT`和`INSERT`查询。

**语法**

``` sql
mysql('host:port', 'database', 'table', 'user', 'password'[, replace_query, 'on_duplicate_clause']);
```

**参数**

-   `host:port` — MySQL服务器地址.

-   `database` — 远程数据库名称.

-   `table` — 远程表名称.

-   `user` — MySQL用户.

-   `password` — 用户密码.

-   `replace_query` — 将INSERT INTO` 查询转换为 `REPLACE INTO`的标志。如果 `replace_query=1`，查询被替换。

- `on_duplicate_clause` — 添加 `ON DUPLICATE KEY on_duplicate_clause` 表达式到 `INSERT` 查询。明确规定只能使用 `replace_query = 0` ，如果你同时设置replace_query = 1`和`on_duplicate_clause`，ClickHouse将产生异常。

  示例：`INSERT INTO t (c1,c2) VALUES ('a', 2) ON DUPLICATE KEY UPDATE c2 = c2 + 1`

  `on_duplicate_clause`这里是`UPDATE c2 = c2 + 1`。请查阅MySQL文档，来找到可以和`ON DUPLICATE KEY`一起使用的 `on_duplicate_clause`子句。

简单的 `WHERE` 子句如 `=, !=, >, >=, <, <=` 将即时在MySQL服务器上执行。其余的条件和 `LIMIT` 只有在对MySQL的查询完成后，才会在ClickHouse中执行采样约束。

支持使用`|`并列进行多副本查询，示例如下：

```sql
SELECT name FROM mysql(`mysql{1|2|3}:3306`, 'mysql_database', 'mysql_table', 'user', 'password');
```

或

```sql
SELECT name FROM mysql(`mysql1:3306|mysql2:3306|mysql3:3306`, 'mysql_database', 'mysql_table', 'user', 'password');
```

**返回值**

与原始MySQL表具有相同列的表对象。

!!! note "注意"
    在`INSERT`查询中为了区分`mysql(...)`与带有列名列表的表名的表函数，你必须使用关键字`FUNCTION`或`TABLE FUNCTION`。查看如下示例。

## 用法示例 {#usage-example}

MySQL中的表:

``` text
mysql> CREATE TABLE `test`.`test` (
    ->   `int_id` INT NOT NULL AUTO_INCREMENT,
    ->   `int_nullable` INT NULL DEFAULT NULL,
    ->   `float` FLOAT NOT NULL,
    ->   `float_nullable` FLOAT NULL DEFAULT NULL,
    ->   PRIMARY KEY (`int_id`));
Query OK, 0 rows affected (0,09 sec)

mysql> insert into test (`int_id`, `float`) VALUES (1,2);
Query OK, 1 row affected (0,00 sec)

mysql> select * from test;
+------+----------+-----+----------+
| int_id | int_nullable | float | float_nullable |
+------+----------+-----+----------+
|      1 |         NULL |     2 |           NULL |
+------+----------+-----+----------+
1 row in set (0,00 sec)
```

从ClickHouse中查询数据:

``` sql
SELECT * FROM mysql('localhost:3306', 'test', 'test', 'bayonet', '123')
```

``` text
┌─int_id─┬─int_nullable─┬─float─┬─float_nullable─┐
│      1 │         ᴺᵁᴸᴸ │     2 │           ᴺᵁᴸᴸ │
└────────┴──────────────┴───────┴────────────────┘
```

替换和插入：

```sql
INSERT INTO FUNCTION mysql('localhost:3306', 'test', 'test', 'bayonet', '123', 1) (int_id, float) VALUES (1, 3);
INSERT INTO TABLE FUNCTION mysql('localhost:3306', 'test', 'test', 'bayonet', '123', 0, 'UPDATE int_id = int_id + 1') (int_id, float) VALUES (1, 4);
SELECT * FROM mysql('localhost:3306', 'test', 'test', 'bayonet', '123');
```

```text
┌─int_id─┬─float─┐
│      1 │     3 │
│      2 │     4 │
└────────┴───────┘
```

## 另请参阅 {#see-also}

-   [该 ‘MySQL’ 表引擎](../../engines/table-engines/integrations/mysql.md)
-   [使用MySQL作为外部字典的来源](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md#dicts-external_dicts_dict_sources-mysql)

[原始文章](https://clickhouse.com/docs/en/query_language/table_functions/mysql/) <!--hide-->
