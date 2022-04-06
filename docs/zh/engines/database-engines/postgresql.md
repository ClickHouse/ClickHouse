---
toc_priority: 35
toc_title: PostgreSQL
---

# PostgreSQL {#postgresql}

允许连接到远程[PostgreSQL](https://www.postgresql.org)服务。支持读写操作(`SELECT`和`INSERT`查询)，以在ClickHouse和PostgreSQL之间交换数据。

在`SHOW TABLES`和`DESCRIBE TABLE`查询的帮助下，从远程PostgreSQL实时访问表列表和表结构。

支持表结构修改(`ALTER TABLE ... ADD|DROP COLUMN`)。如果`use_table_cache`参数(参见下面的引擎参数)设置为`1`，则会缓存表结构，不会检查是否被修改，但可以用`DETACH`和`ATTACH`查询进行更新。

## 创建数据库 {#creating-a-database}

``` sql
CREATE DATABASE test_database 
ENGINE = PostgreSQL('host:port', 'database', 'user', 'password'[, `use_table_cache`]);
```

**引擎参数**

-   `host:port` — PostgreSQL服务地址
-   `database` — 远程数据库名次
-   `user` — PostgreSQL用户名称
-   `password` — PostgreSQL用户密码
-   `schema` - PostgreSQL 模式
-   `use_table_cache` —  定义数据库表结构是否已缓存或不进行。可选的。默认值： `0`.

## 支持的数据类型 {#data_types-support}

| PostgerSQL       | ClickHouse                                                   |
|------------------|--------------------------------------------------------------|
| DATE             | [Date](../../sql-reference/data-types/date.md)               |
| TIMESTAMP        | [DateTime](../../sql-reference/data-types/datetime.md)       |
| REAL             | [Float32](../../sql-reference/data-types/float.md)           |
| DOUBLE           | [Float64](../../sql-reference/data-types/float.md)           |
| DECIMAL, NUMERIC | [Decimal](../../sql-reference/data-types/decimal.md)         |
| SMALLINT         | [Int16](../../sql-reference/data-types/int-uint.md)          |
| INTEGER          | [Int32](../../sql-reference/data-types/int-uint.md)          |
| BIGINT           | [Int64](../../sql-reference/data-types/int-uint.md)          |
| SERIAL           | [UInt32](../../sql-reference/data-types/int-uint.md)         |
| BIGSERIAL        | [UInt64](../../sql-reference/data-types/int-uint.md)         |
| TEXT, CHAR       | [String](../../sql-reference/data-types/string.md)           |
| INTEGER          | Nullable([Int32](../../sql-reference/data-types/int-uint.md))|
| ARRAY            | [Array](../../sql-reference/data-types/array.md)             |


## 使用示例 {#examples-of-use}

ClickHouse中的数据库，与PostgreSQL服务器交换数据:

``` sql
CREATE DATABASE test_database 
ENGINE = PostgreSQL('postgres1:5432', 'test_database', 'postgres', 'mysecretpassword', 1);
```

``` sql
SHOW DATABASES;
```

``` text
┌─name──────────┐
│ default       │
│ test_database │
│ system        │
└───────────────┘
```

``` sql
SHOW TABLES FROM test_database;
```

``` text
┌─name───────┐
│ test_table │
└────────────┘
```

从PostgreSQL表中读取数据：

``` sql
SELECT * FROM test_database.test_table;
```

``` text
┌─id─┬─value─┐
│  1 │     2 │
└────┴───────┘
```

将数据写入PostgreSQL表：

``` sql
INSERT INTO test_database.test_table VALUES (3,4);
SELECT * FROM test_database.test_table;
```

``` text
┌─int_id─┬─value─┐
│      1 │     2 │
│      3 │     4 │
└────────┴───────┘
```

在PostgreSQL中修改了表结构：

``` sql
postgre> ALTER TABLE test_table ADD COLUMN data Text
```

当创建数据库时，参数`use_table_cache`被设置为`1`，ClickHouse中的表结构被缓存，因此没有被修改:

``` sql
DESCRIBE TABLE test_database.test_table;
```
``` text
┌─name───┬─type──────────────┐
│ id     │ Nullable(Integer) │
│ value  │ Nullable(Integer) │
└────────┴───────────────────┘
```

分离表并再次附加它之后，结构被更新了:

``` sql
DETACH TABLE test_database.test_table;
ATTACH TABLE test_database.test_table;
DESCRIBE TABLE test_database.test_table;
```
``` text
┌─name───┬─type──────────────┐
│ id     │ Nullable(Integer) │
│ value  │ Nullable(Integer) │
│ data   │ Nullable(String)  │
└────────┴───────────────────┘
```

[来源文章](https://clickhouse.com/docs/en/database-engines/postgresql/) <!--hide-->
