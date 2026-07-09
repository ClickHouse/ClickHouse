---
description: '允许连接到 SQLite 数据库，并执行 `INSERT` 和 `SELECT`
  查询，以在 ClickHouse 与 SQLite 之间交换数据。'
sidebar_label: 'SQLite'
sidebar_position: 55
slug: /engines/database-engines/sqlite
title: 'SQLite'
doc_type: 'reference'
---

允许连接到 [SQLite](https://www.sqlite.org/index.html) 数据库，并执行 `INSERT` 和 `SELECT` 查询，以在 ClickHouse 与 SQLite 之间交换数据。

<div id="creating-a-database">
  ## 创建数据库
</div>

```sql
    CREATE DATABASE sqlite_database
    ENGINE = SQLite('db_path')
```

**引擎参数**

* `db_path` — SQLite 数据库文件的路径。

<div id="data_types-support">
  ## 数据类型支持
</div>

下表显示了 ClickHouse 从 SQLite 自动推断 schema 时的默认类型映射：

| SQLite  | ClickHouse                                          |
| ------- | --------------------------------------------------- |
| INTEGER | [Int32](../../sql-reference/data-types/int-uint.md) |
| REAL    | [Float32](../../sql-reference/data-types/float.md)  |
| TEXT    | [String](../../sql-reference/data-types/string.md)  |
| TEXT    | [UUID](../../sql-reference/data-types/uuid.md)      |
| BLOB    | [String](../../sql-reference/data-types/string.md)  |

当使用 [SQLite table engine](../../engines/table-engines/integrations/sqlite.md) 显式定义带有特定 ClickHouse 类型的表时，以下 ClickHouse 类型可以从 SQLite 的 TEXT 列中解析出来：

* [Date](../../sql-reference/data-types/date.md), [Date32](../../sql-reference/data-types/date32.md)
* [DateTime](../../sql-reference/data-types/datetime.md), [DateTime64](../../sql-reference/data-types/datetime64.md)
* [UUID](../../sql-reference/data-types/uuid.md)
* [Enum8, Enum16](../../sql-reference/data-types/enum.md)
* [Decimal32, Decimal64, Decimal128, Decimal256](../../sql-reference/data-types/decimal.md)
* [FixedString](../../sql-reference/data-types/fixedstring.md)
* 所有整数类型 ([UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64](../../sql-reference/data-types/int-uint.md)) 
* [Float32, Float64](../../sql-reference/data-types/float.md)

SQLite 采用动态类型，其类型访问函数会自动执行类型转换。例如，将 TEXT 列按整数读取时，如果文本无法解析为数字，则会返回 0。这意味着，如果 ClickHouse 表中定义的类型与底层 SQLite 列的类型不同，值可能会被静默转换，而不会报错。

<div id="specifics-and-recommendations">
  ## 具体说明与建议
</div>

SQLite 将整个数据库 (定义、表、索引以及数据本身) 存储为宿主机上的单个跨平台文件。写入时，SQLite 会锁定整个数据库文件，因此写操作只能顺序执行。读操作则可以并发执行。
SQLite 不需要服务管理 (例如启动脚本) ，也不需要基于 `GRANT` 和密码的访问控制。访问控制是通过数据库文件本身的文件系统权限来实现的。

<div id="usage-example">
  ## 使用示例
</div>

ClickHouse 中连接到 SQLite 的数据库：

```sql
CREATE DATABASE sqlite_db ENGINE = SQLite('sqlite.db');
SHOW TABLES FROM sqlite_db;
```

```text
┌──name───┐
│ table1  │
│ table2  │
└─────────┘
```

显示以下表：

```sql
SELECT * FROM sqlite_db.table1;
```

```text
┌─col1──┬─col2─┐
│ line1 │    1 │
│ line2 │    2 │
│ line3 │    3 │
└───────┴──────┘
```

将 ClickHouse 表中的数据插入到 SQLite 表中：

```sql
CREATE TABLE clickhouse_table(`col1` String,`col2` Int16) ENGINE = MergeTree() ORDER BY col2;
INSERT INTO clickhouse_table VALUES ('text',10);
INSERT INTO sqlite_db.table1 SELECT * FROM clickhouse_table;
SELECT * FROM sqlite_db.table1;
```

```text
┌─col1──┬─col2─┐
│ line1 │    1 │
│ line2 │    2 │
│ line3 │    3 │
│ text  │   10 │
└───────┴──────┘
```