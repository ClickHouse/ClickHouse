# MySQL

MySQL引擎用于将远程的MySQL服务器中的表映射到ClickHouse中，并允许您对表进行`INSERT`和`SELECT`查询，以方便您在ClickHouse与MySQL之间进行数据交换。

`MySQL`数据库引擎会将对其的查询转换为MySQL语法并发送到MySQL服务器中，因此您可以执行诸如`SHOW TABLES`或`SHOW CREATE TABLE`之类的操作。

但您无法对其执行以下操作：

- `ATTACH`/`DETACH`
- `DROP`
- `RENAME`
- `CREATE TABLE`
- `ALTER`


## CREATE DATABASE

``` sql
CREATE DATABASE [IF NOT EXISTS] db_name [ON CLUSTER cluster]
ENGINE = MySQL('host:port', 'database', 'user', 'password')
```

**MySQL数据库引擎参数**

- `host:port` — 链接的MySQL地址。
- `database` — 链接的MySQL数据库。
- `user` — 链接的MySQL用户。
- `password` — 链接的MySQL用户密码。


## 支持的类型对应

MySQL | ClickHouse
------|------------
UNSIGNED TINYINT | [UInt8](../data_types/int_uint.md)
TINYINT | [Int8](../data_types/int_uint.md)
UNSIGNED SMALLINT | [UInt16](../data_types/int_uint.md)
SMALLINT | [Int16](../data_types/int_uint.md)
UNSIGNED INT, UNSIGNED MEDIUMINT | [UInt32](../data_types/int_uint.md)
INT, MEDIUMINT | [Int32](../data_types/int_uint.md)
UNSIGNED BIGINT | [UInt64](../data_types/int_uint.md)
BIGINT | [Int64](../data_types/int_uint.md)
FLOAT | [Float32](../data_types/float.md)
DOUBLE | [Float64](../data_types/float.md)
DATE | [Date](../data_types/date.md)
DATETIME, TIMESTAMP | [DateTime](../data_types/datetime.md)
BINARY | [FixedString](../data_types/fixedstring.md)

其他的MySQL数据类型将全部都转换为[String](../data_types/string.md)。

同时以上的所有类型都支持[Nullable](../data_types/nullable.md)。


## 使用示例

在MySQL中创建表:

```
mysql> USE test;
Database changed

mysql> CREATE TABLE `mysql_table` (
    ->   `int_id` INT NOT NULL AUTO_INCREMENT,
    ->   `float` FLOAT NOT NULL,
    ->   PRIMARY KEY (`int_id`));
Query OK, 0 rows affected (0,09 sec)

mysql> insert into mysql_table (`int_id`, `float`) VALUES (1,2);
Query OK, 1 row affected (0,00 sec)

mysql> select * from mysql_table;
+--------+-------+
| int_id | value |
+--------+-------+
|      1 |     2 |
+--------+-------+
1 row in set (0,00 sec)
```

在ClickHouse中创建MySQL类型的数据库，同时与MySQL服务器交换数据：

```sql
CREATE DATABASE mysql_db ENGINE = MySQL('localhost:3306', 'test', 'my_user', 'user_password')
```
```sql
SHOW DATABASES
```
```text
┌─name─────┐
│ default  │
│ mysql_db │
│ system   │
└──────────┘
```
```sql
SHOW TABLES FROM mysql_db
```
```text
┌─name─────────┐
│  mysql_table │
└──────────────┘
```
```sql
SELECT * FROM mysql_db.mysql_table
```
```text
┌─int_id─┬─value─┐
│      1 │     2 │
└────────┴───────┘
```
```sql
INSERT INTO mysql_db.mysql_table VALUES (3,4)
```
```sql
SELECT * FROM mysql_db.mysql_table
```
```text
┌─int_id─┬─value─┐
│      1 │     2 │
│      3 │     4 │
└────────┴───────┘
```

[来源文章](https://clickhouse.yandex/docs/en/database_engines/mysql/) <!--hide-->
