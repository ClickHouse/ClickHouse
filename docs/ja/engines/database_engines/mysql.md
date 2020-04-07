---
machine_translated: true
machine_translated_rev: d734a8e46ddd7465886ba4133bff743c55190626
toc_priority: 30
toc_title: MySQL
---

# Mysql {#mysql}

リモートmysqlサーバー上のデータベースに接続し、 `INSERT` と `SELECT` ClickHouseとMySQLの間でデータを交換するためのクエリ。

その `MySQL` データベースエンジンの翻訳のクエリのMySQLサーバーでの操作を行うことができなど `SHOW TABLES` または `SHOW CREATE TABLE`.

次のクエリは実行できません:

-   `RENAME`
-   `CREATE TABLE`
-   `ALTER`

## データベースの作成 {#creating-a-database}

``` sql
CREATE DATABASE [IF NOT EXISTS] db_name [ON CLUSTER cluster]
ENGINE = MySQL('host:port', 'database', 'user', 'password')
```

**エンジン変数**

-   `host:port` — MySQL server address.
-   `database` — Remote database name.
-   `user` — MySQL user.
-   `password` — User password.

## データ型のサポート {#data_types-support}

| MySQL                            | クリックハウス                                               |
|----------------------------------|--------------------------------------------------------------|
| UNSIGNED TINYINT                 | [UInt8](../../sql_reference/data_types/int_uint.md)          |
| TINYINT                          | [Int8](../../sql_reference/data_types/int_uint.md)           |
| UNSIGNED SMALLINT                | [UInt16](../../sql_reference/data_types/int_uint.md)         |
| SMALLINT                         | [Int16](../../sql_reference/data_types/int_uint.md)          |
| UNSIGNED INT, UNSIGNED MEDIUMINT | [UInt32](../../sql_reference/data_types/int_uint.md)         |
| INT, MEDIUMINT                   | [Int32](../../sql_reference/data_types/int_uint.md)          |
| UNSIGNED BIGINT                  | [UInt64](../../sql_reference/data_types/int_uint.md)         |
| BIGINT                           | [Int64](../../sql_reference/data_types/int_uint.md)          |
| FLOAT                            | [Float32](../../sql_reference/data_types/float.md)           |
| DOUBLE                           | [Float64](../../sql_reference/data_types/float.md)           |
| DATE                             | [日付](../../sql_reference/data_types/date.md)               |
| DATETIME, TIMESTAMP              | [DateTime](../../sql_reference/data_types/datetime.md)       |
| BINARY                           | [FixedString](../../sql_reference/data_types/fixedstring.md) |

他のすべてのmysqlデータ型に変換され [文字列](../../sql_reference/data_types/string.md).

[Nullable](../../sql_reference/data_types/nullable.md) サポートされます。

## 使用例 {#examples-of-use}

MySQLのテーブル:

``` text
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
+------+-----+
| int_id | value |
+------+-----+
|      1 |     2 |
+------+-----+
1 row in set (0,00 sec)
```

ClickHouseのデータベース、MySQLサーバとのデータ交換:

``` sql
CREATE DATABASE mysql_db ENGINE = MySQL('localhost:3306', 'test', 'my_user', 'user_password')
```

``` sql
SHOW DATABASES
```

``` text
┌─name─────┐
│ default  │
│ mysql_db │
│ system   │
└──────────┘
```

``` sql
SHOW TABLES FROM mysql_db
```

``` text
┌─name─────────┐
│  mysql_table │
└──────────────┘
```

``` sql
SELECT * FROM mysql_db.mysql_table
```

``` text
┌─int_id─┬─value─┐
│      1 │     2 │
└────────┴───────┘
```

``` sql
INSERT INTO mysql_db.mysql_table VALUES (3,4)
```

``` sql
SELECT * FROM mysql_db.mysql_table
```

``` text
┌─int_id─┬─value─┐
│      1 │     2 │
│      3 │     4 │
└────────┴───────┘
```

[元の記事](https://clickhouse.tech/docs/en/database_engines/mysql/) <!--hide-->
