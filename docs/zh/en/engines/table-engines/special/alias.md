---
description: '别名表引擎会为另一张表创建一个透明代理。所有操作都会转发到目标表，而别名本身不存储任何数据。'
sidebar_label: '别名'
sidebar_position: 5
slug: /engines/table-engines/special/alias
title: '别名表引擎'
doc_type: 'reference'
---

<div id="alias-table-engine">
  # 别名表引擎
</div>

`Alias` 引擎会为另一张表创建一个代理。所有读写操作都会被转发到目标表，而别名本身不存储任何数据，只保留对目标表的引用。

<div id="creating-a-table">
  ## 创建表
</div>

```sql
CREATE TABLE [db_name.]alias_name
ENGINE = Alias(target_table)
```

或者显式指定数据库名称：

```sql
CREATE TABLE [db_name.]alias_name
ENGINE = Alias(target_db, target_table)
```

:::note
`Alias` 表不支持显式定义列。列会自动从目标表继承，以确保别名始终与目标表的 schema 保持一致。
:::

<div id="engine-parameters">
  ## 引擎参数
</div>

* **`target_db (optional)`** — 包含目标表的数据库名称。
* **`target_table`** — 目标表名称。

:::note
当省略 `target_db` 且 `target_table` 不是完全限定名时 (例如 `Alias('my_table')`) ，目标会被解析到与别名自身相同的数据库，而不是当前会话的数据库。
:::

<div id="supported-operations">
  ## 支持的操作
</div>

`Alias` 表引擎支持所有主要操作。 

<div id="operations-on-target">
  ### 对目标表的操作
</div>

以下操作会代理到目标表执行：

| 操作                           | 支持 | 说明                               |
| ---------------------------- | -- | -------------------------------- |
| `SELECT`                     | ✅  | 从目标表读取数据                         |
| `INSERT`                     | ✅  | 向目标表写入数据                         |
| `INSERT SELECT`              | ✅  | 批次插入到目标表                         |
| `ALTER TABLE ADD COLUMN`     | ✅  | 向目标表添加列                          |
| `ALTER TABLE MODIFY SETTING` | ✅  | 修改目标表的设置                         |
| `ALTER TABLE PARTITION`      | ✅  | 对目标表执行分区操作 (DETACH/ATTACH/DROP)  |
| `ALTER TABLE UPDATE`         | ✅  | 更新目标表中的行 (变更)                    |
| `ALTER TABLE DELETE`         | ✅  | 删除目标表中的行 (变更)                    |
| `OPTIMIZE TABLE`             | ✅  | 优化目标表 (合并 parts)                 |
| `TRUNCATE TABLE`             | ✅  | 截断目标表                            |

<div id="operations-on-alias">
  ### 对别名本身执行的操作
</div>

这些操作只会影响别名本身，**不会**影响目标表：

| 操作             | 支持 | 说明             |
| -------------- | -- | -------------- |
| `DROP TABLE`   | ✅  | 仅删除别名，目标表保持不变  |
| `RENAME TABLE` | ✅  | 仅重命名别名，目标表保持不变 |

<div id="usage-examples">
  ## 使用示例
</div>

<div id="basic-alias-creation">
  ### 基本别名的创建
</div>

在同一数据库中创建一个简单别名：

```sql
-- Create source table
CREATE TABLE source_data (
    id UInt32,
    name String,
    value Float64
) ENGINE = MergeTree
ORDER BY id;

-- Insert some data
INSERT INTO source_data VALUES (1, 'one', 10.1), (2, 'two', 20.2);

-- Create alias
CREATE TABLE data_alias ENGINE = Alias('source_data');

-- Query through alias
SELECT * FROM data_alias;
```

```text
┌─id─┬─name─┬─value─┐
│  1 │ one  │  10.1 │
│  2 │ two  │  20.2 │
└────┴──────┴───────┘
```

<div id="cross-database-alias">
  ### 跨数据库别名
</div>

创建一个指向其他数据库中某个表的别名：

```sql
-- Create databases
CREATE DATABASE db1;
CREATE DATABASE db2;

-- Create source table in db1
CREATE TABLE db1.events (
    timestamp DateTime,
    event_type String,
    user_id UInt32
) ENGINE = MergeTree
ORDER BY timestamp;

-- Create alias in db2 pointing to db1.events
CREATE TABLE db2.events_alias ENGINE = Alias('db1', 'events');

-- Or using database.table format
CREATE TABLE db2.events_alias2 ENGINE = Alias('db1.events');

-- Both aliases work identically
INSERT INTO db2.events_alias VALUES (now(), 'click', 100);
SELECT * FROM db2.events_alias2;
```

<div id="write-operations">
  ### 通过别名执行写入操作
</div>

所有写入操作都会转发到目标表：

```sql
CREATE TABLE metrics (
    ts DateTime,
    metric_name String,
    value Float64
) ENGINE = MergeTree
ORDER BY ts;

CREATE TABLE metrics_alias ENGINE = Alias('metrics');

-- Insert through alias
INSERT INTO metrics_alias VALUES 
    (now(), 'cpu_usage', 45.2),
    (now(), 'memory_usage', 78.5);

-- Insert with SELECT
INSERT INTO metrics_alias 
SELECT now(), 'disk_usage', number * 10 
FROM system.numbers 
LIMIT 5;

-- Verify data is in the target table
SELECT count() FROM metrics;  -- Returns 7
SELECT count() FROM metrics_alias;  -- Returns 7
```

<div id="schema-modification">
  ### Schema 变更
</div>

ALTER 操作会修改目标表的 schema：

```sql
CREATE TABLE users (
    id UInt32,
    name String
) ENGINE = MergeTree
ORDER BY id;

CREATE TABLE users_alias ENGINE = Alias('users');

-- Add column through alias
ALTER TABLE users_alias ADD COLUMN email String DEFAULT '';

-- Column is added to target table
DESCRIBE users;
```

```text
┌─name──┬─type───┬─default_type─┬─default_expression─┐
│ id    │ UInt32 │              │                    │
│ name  │ String │              │                    │
│ email │ String │ DEFAULT      │ ''                 │
└───────┴────────┴──────────────┴────────────────────┘
```

<div id="data-mutations">
  ### 数据变更
</div>

支持 UPDATE 和 DELETE 操作：

```sql
CREATE TABLE products (
    id UInt32,
    name String,
    price Float64,
    status String DEFAULT 'active'
) ENGINE = MergeTree
ORDER BY id;

CREATE TABLE products_alias ENGINE = Alias('products');

INSERT INTO products_alias VALUES 
    (1, 'item_one', 100.0, 'active'),
    (2, 'item_two', 200.0, 'active'),
    (3, 'item_three', 300.0, 'inactive');

-- Update through alias
ALTER TABLE products_alias UPDATE price = price * 1.1 WHERE status = 'active';

-- Delete through alias
ALTER TABLE products_alias DELETE WHERE status = 'inactive';

-- Changes are applied to target table
SELECT * FROM products ORDER BY id;
```

```text
┌─id─┬─name─────┬─price─┬─status─┐
│  1 │ item_one │ 110.0 │ active │
│  2 │ item_two │ 220.0 │ active │
└────┴──────────┴───────┴────────┘
```

<div id="partition-operations">
  ### 分区操作
</div>

对于分区表，分区操作会转发到目标表：

```sql
CREATE TABLE logs (
    date Date,
    level String,
    message String
) ENGINE = MergeTree
PARTITION BY toYYYYMM(date)
ORDER BY date;

CREATE TABLE logs_alias ENGINE = Alias('logs');

INSERT INTO logs_alias VALUES 
    ('2024-01-15', 'INFO', 'message1'),
    ('2024-02-15', 'ERROR', 'message2'),
    ('2024-03-15', 'INFO', 'message3');

-- Detach partition through alias
ALTER TABLE logs_alias DETACH PARTITION '202402';

SELECT count() FROM logs_alias;  -- Returns 2 (partition 202402 detached)

-- Attach partition back
ALTER TABLE logs_alias ATTACH PARTITION '202402';

SELECT count() FROM logs_alias;  -- Returns 3
```

<div id="table-optimization">
  ### 表优化
</div>

Optimize 操作会合并目标表中的 parts：

```sql
CREATE TABLE events (
    id UInt32,
    data String
) ENGINE = MergeTree
ORDER BY id;

CREATE TABLE events_alias ENGINE = Alias('events');

-- Multiple inserts create multiple parts
INSERT INTO events_alias VALUES (1, 'data1');
INSERT INTO events_alias VALUES (2, 'data2');
INSERT INTO events_alias VALUES (3, 'data3');

-- Check parts count
SELECT count() FROM system.parts 
WHERE database = currentDatabase() 
  AND table = 'events' 
  AND active;

-- Optimize through alias
OPTIMIZE TABLE events_alias FINAL;

-- Parts are merged in target table
SELECT count() FROM system.parts 
WHERE database = currentDatabase() 
  AND table = 'events' 
  AND active;  -- Returns 1
```

<div id="alias-management">
  ### 别名管理
</div>

别名可单独重命名或删除：

```sql
CREATE TABLE important_data (
    id UInt32,
    value String
) ENGINE = MergeTree
ORDER BY id;

INSERT INTO important_data VALUES (1, 'critical'), (2, 'important');

CREATE TABLE old_alias ENGINE = Alias('important_data');

-- Rename alias (target table unchanged)
RENAME TABLE old_alias TO new_alias;

-- Create another alias to same table
CREATE TABLE another_alias ENGINE = Alias('important_data');

-- Drop one alias (target table and other aliases unchanged)
DROP TABLE new_alias;

SELECT * FROM another_alias;  -- Still works
SELECT count() FROM important_data;  -- Data intact, returns 2
```