---
sidebar_position: 32
sidebar_label: SQLite
---

# SQLite {#sqlite}

允许连接到[SQLite](https://www.sqlite.org/index.html)数据库，并支持ClickHouse和SQLite交换数据， 执行 `INSERT` 和 `SELECT` 查询。

## 创建一个数据库 {#creating-a-database}

``` sql
    CREATE DATABASE sqlite_database 
    ENGINE = SQLite('db_path')
```

**引擎参数**

-   `db_path` — SQLite 数据库文件的路径.
    
## 数据类型的支持 {#data_types-support}

|  SQLite   | ClickHouse                                              |
|---------------|---------------------------------------------------------|
| INTEGER       | [Int32](../../sql-reference/data-types/int-uint.md)     |
| REAL          | [Float32](../../sql-reference/data-types/float.md)      |
| TEXT          | [String](../../sql-reference/data-types/string.md)      |
| BLOB          | [String](../../sql-reference/data-types/string.md)      |

## 技术细节和建议 {#specifics-and-recommendations}

SQLite将整个数据库(定义、表、索引和数据本身)存储为主机上的单个跨平台文件。在写入过程中，SQLite会锁定整个数据库文件，因此写入操作是顺序执行的。读操作可以是多任务的。
SQLite不需要服务管理(如启动脚本)或基于`GRANT`和密码的访问控制。访问控制是通过授予数据库文件本身的文件系统权限来处理的。

## 使用示例 {#usage-example}

数据库在ClickHouse，连接到SQLite:

``` sql
CREATE DATABASE sqlite_db ENGINE = SQLite('sqlite.db');
SHOW TABLES FROM sqlite_db;
```

``` text
┌──name───┐
│ table1  │
│ table2  │  
└─────────┘
```

展示数据表中的内容:

``` sql
SELECT * FROM sqlite_db.table1;
```

``` text
┌─col1──┬─col2─┐
│ line1 │    1 │
│ line2 │    2 │
│ line3 │    3 │
└───────┴──────┘
```
从ClickHouse表插入数据到SQLite表:

``` sql
CREATE TABLE clickhouse_table(`col1` String,`col2` Int16) ENGINE = MergeTree() ORDER BY col2;
INSERT INTO clickhouse_table VALUES ('text',10);
INSERT INTO sqlite_db.table1 SELECT * FROM clickhouse_table;
SELECT * FROM sqlite_db.table1;
```

``` text
┌─col1──┬─col2─┐
│ line1 │    1 │
│ line2 │    2 │
│ line3 │    3 │
│ text  │   10 │
└───────┴──────┘
```
