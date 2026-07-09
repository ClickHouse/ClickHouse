---
description: '允许连接到远程 PostgreSQL 服务器上的数据库。'
sidebar_label: 'PostgreSQL'
sidebar_position: 40
slug: /engines/database-engines/postgresql
title: 'PostgreSQL'
doc_type: 'guide'
---

允许连接到远程 [PostgreSQL](https://www.postgresql.org) 服务器上的数据库。支持读写操作 (`SELECT` 和 `INSERT` 查询) ，可在 ClickHouse 与 PostgreSQL 之间交换数据。

借助 `SHOW TABLES` 和 `DESCRIBE TABLE` 查询，可实时访问远程 PostgreSQL 中的表列表和表结构。

支持修改表结构 (`ALTER TABLE ... ADD|DROP COLUMN`) 。如果 `use_table_cache` 参数 (参见下方的引擎参数) 设置为 `1`，则表结构会被缓存，且不会检查其是否发生修改，但可以通过 `DETACH` 和 `ATTACH` 查询进行更新。

<div id="creating-a-database">
  ## 创建数据库
</div>

```sql
CREATE DATABASE test_database
ENGINE = PostgreSQL('host:port', 'database', 'user', 'password'[, `schema`, `use_table_cache`]);
```

**引擎参数**

* `host:port` — PostgreSQL 服务器地址。
* `database` — 远程数据库名称。
* `user` — PostgreSQL 用户。
* `password` — 用户密码。
* `schema` — PostgreSQL schema。
* `use_table_cache` — 定义是否缓存数据库表结构。可选。默认值：`0`。

<div id="data_types-support">
  ## 支持的数据类型
</div>

| PostgreSQL       | ClickHouse                                                    |
| ---------------- | ------------------------------------------------------------- |
| DATE             | [Date](../../sql-reference/data-types/date.md)                |
| TIMESTAMP        | [日期时间](../../sql-reference/data-types/datetime.md)            |
| REAL             | [Float32](../../sql-reference/data-types/float.md)            |
| DOUBLE           | [Float64](../../sql-reference/data-types/float.md)            |
| DECIMAL, NUMERIC | [Decimal](../../sql-reference/data-types/decimal.md) (见下文说明)  |
| SMALLINT         | [Int16](../../sql-reference/data-types/int-uint.md)           |
| INTEGER          | [Int32](../../sql-reference/data-types/int-uint.md)           |
| BIGINT           | [Int64](../../sql-reference/data-types/int-uint.md)           |
| SERIAL           | [UInt32](../../sql-reference/data-types/int-uint.md)          |
| BIGSERIAL        | [UInt64](../../sql-reference/data-types/int-uint.md)          |
| TEXT, CHAR       | [String](../../sql-reference/data-types/string.md)            |
| INTEGER          | Nullable([Int32](../../sql-reference/data-types/int-uint.md)) |
| ARRAY            | [Array](../../sql-reference/data-types/array.md)              |

:::note
对于精度 `p` 大于 76 (即 `Decimal256` 支持的最大值) 的 PostgreSQL `numeric(p, 0)`，例如常用于存储 256 位整数的 `numeric(78, 0)`，将映射为 [`Int256`](../../sql-reference/data-types/int-uint.md) 而不是 `Decimal`。超出 `Int256` 范围的值将被拒绝，并返回错误。
:::

<div id="examples-of-use">
  ## 使用示例
</div>

ClickHouse 中与 PostgreSQL 服务器交换数据的数据库：

```sql
CREATE DATABASE test_database
ENGINE = PostgreSQL('postgres1:5432', 'test_database', 'postgres', 'mysecretpassword', 'schema_name',1);
```

```sql
SHOW DATABASES;
```

```text
┌─name──────────┐
│ default       │
│ test_database │
│ system        │
└───────────────┘
```

```sql
SHOW TABLES FROM test_database;
```

```text
┌─name───────┐
│ test_table │
└────────────┘
```

从 PostgreSQL 表读取数据：

```sql
SELECT * FROM test_database.test_table;
```

```text
┌─id─┬─value─┐
│  1 │     2 │
└────┴───────┘
```

向 PostgreSQL 表中写入数据：

```sql
INSERT INTO test_database.test_table VALUES (3,4);
SELECT * FROM test_database.test_table;
```

```text
┌─int_id─┬─value─┐
│      1 │     2 │
│      3 │     4 │
└────────┴───────┘
```

假设 PostgreSQL 中的表结构已发生修改：

```sql
postgre> ALTER TABLE test_table ADD COLUMN data Text
```

由于在创建数据库时将 `use_table_cache` 参数设为 `1`，ClickHouse 中的表结构已被缓存，因此不会被修改：

```sql
DESCRIBE TABLE test_database.test_table;
```

```text
┌─name───┬─type──────────────┐
│ id     │ Nullable(Integer) │
│ value  │ Nullable(Integer) │
└────────┴───────────────────┘
```

将该表分离后再重新附加，结构已更新：

```sql
DETACH TABLE test_database.test_table;
ATTACH TABLE test_database.test_table;
DESCRIBE TABLE test_database.test_table;
```

```text
┌─name───┬─type──────────────┐
│ id     │ Nullable(Integer) │
│ value  │ Nullable(Integer) │
│ data   │ Nullable(String)  │
└────────┴───────────────────┘
```

<div id="related-content">
  ## 相关内容
</div>

* 博客：[ClickHouse 和 PostgreSQL：数据世界里的天作之合 (第 1 部分) ](https://clickhouse.com/blog/migrating-data-between-clickhouse-postgres)
* 博客：[ClickHouse 和 PostgreSQL：数据世界里的天作之合 (第 2 部分) ](https://clickhouse.com/blog/migrating-data-between-clickhouse-postgres-part-2)