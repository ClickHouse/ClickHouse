---
description: 'PostgreSQL 表引擎允许对存储在远程 PostgreSQL 服务器上的数据执行 `SELECT` 和 `INSERT` 查询。'
sidebar_label: 'PostgreSQL'
sidebar_position: 160
slug: /engines/table-engines/integrations/postgresql
title: 'PostgreSQL 表引擎'
doc_type: 'guide'
---

PostgreSQL 表引擎允许对存储在远程 PostgreSQL 服务器上的数据执行 `SELECT` 和 `INSERT` 查询。

:::note
目前，表引擎仅支持 PostgreSQL 12 及以上版本。
:::

:::tip
欢迎了解我们的 [Managed Postgres](/zh/docs/cloud/managed-postgres) 服务。它采用与计算资源物理同址的 NVMe 存储，相比使用 EBS 等网络附加存储的替代方案，可为磁盘受限型工作负载提供最高 10 倍的性能提升，并支持你使用 ClickPipes 中的 Postgres CDC 连接器将 Postgres 数据复制到 ClickHouse。
:::

<div id="creating-a-table">
  ## 创建表
</div>

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 type1 [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 type2 [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = PostgreSQL({host:port, database, table, user, password[, schema, [, on_conflict]] | named_collection[, option=value [,..]]})
```

请参阅 [CREATE TABLE](/zh/sql-reference/statements/create/table) 查询的详细说明。

表结构可以与原始 PostgreSQL 表的结构不同：

* 列名应与原始 PostgreSQL 表中的列名一致，但也可以只使用其中部分列，且顺序可以任意。
* 列类型可以与原始 PostgreSQL 表中的列类型不同。ClickHouse 会尝试将值 [转换](../../../engines/database-engines/postgresql.md#data_types-support) 为 ClickHouse 数据类型。
* [external&#95;table&#95;functions&#95;use&#95;nulls](/zh/operations/settings/settings#external_table_functions_use_nulls) 设置定义了如何处理 Nullable 列。默认值：1。如果为 0，则该表函数不会生成 Nullable 列，而是插入默认值来代替 null 值。这同样适用于数组中的 NULL 值。

**引擎参数**

* `host:port` — PostgreSQL 服务器地址。
* `database` — 远程数据库名称。
* `table` — 远程表名，或原样传递给 PostgreSQL 的查询 (参见[传递查询而不是表名](#passing-a-query)) 。
* `user` — PostgreSQL 用户。
* `password` — 用户密码。
* `schema` — 非默认表 schema。可选。
* `on_conflict` — 冲突解决策略。示例：`ON CONFLICT DO NOTHING`。可选。注意：添加此选项会降低插入效率。

建议在生产环境中使用[命名集合](/zh/operations/named-collections.md) (自 21.11 版本起可用) 。示例如下：

```xml
<named_collections>
    <postgres_creds>
        <host>localhost</host>
        <port>5432</port>
        <user>postgres</user>
        <password>****</password>
        <schema>schema1</schema>
    </postgres_creds>
</named_collections>
```

某些参数可以通过键值参数覆盖：

```sql
SELECT * FROM postgresql(postgres_creds, table='table1');
```

<div id="implementation-details">
  ## 实现细节
</div>

PostgreSQL 端的 `SELECT` 查询会在只读 PostgreSQL 事务中以 `COPY (SELECT ...) TO STDOUT` 的形式运行，并在每次 `SELECT` 查询后提交。

诸如 `=`, `!=`, `>`, `>=`, `<`, `<=` 和 `IN` 这样的简单 `WHERE` 子句会在 PostgreSQL 服务器端执行。

所有 join、聚合、排序、`IN [ array ]` 条件以及 `LIMIT` 采样约束，都只会在对 PostgreSQL 的查询完成后由 ClickHouse 执行。

<div id="passing-a-query">
  ## 传入查询而非表名
</div>

`table` 参数不必是表名，也可以是原样传递给 PostgreSQL 的 `SELECT` 查询。表的结构会根据查询结果自动推断。该查询既可以写成子查询，也可以包装在 `query` 函数中：

```sql
CREATE TABLE pg_table ENGINE = PostgreSQL('localhost:5432', 'test', (SELECT a, b FROM t1 JOIN t2 USING (id) WHERE a > 0), 'user', 'password');
CREATE TABLE pg_table ENGINE = PostgreSQL('localhost:5432', 'test', query('SELECT a, b FROM t1 JOIN t2 USING (id) WHERE a > 0'), 'user', 'password');
```

这对于将 joins、聚合 或其他任何处理下推到 PostgreSQL 很有用。这样的表是只读的：不允许对其执行 `INSERT`。[`postgresql`](/zh/sql-reference/table-functions/postgresql) 表函数 也支持相同的语法。

:::note
子查询形式 `(SELECT ...)` 会由 ClickHouse 解析，并在发送到 server 之前按 PostgreSQL dialect (PostgreSQL 标识符引用和字符串字面量转义) 重新序列化。因此，它必须是有效的 ClickHouse SQL。若要传递 ClickHouse 无法解析的 PostgreSQL 特有语法，请使用 `query('...')` 形式，其文本会被原样发送到 PostgreSQL。

外围 ClickHouse 查询中的任何外层 `WHERE`、`LIMIT`、aggregation 等都**不会**被下推到传入的查询中——而是在拉取完整 query result 后由 ClickHouse 应用。要限制从 PostgreSQL 读取的数据，请将过滤器放在传入的查询内部。启用 [`external_table_strict_query = 1`](/zh/operations/settings/settings#external_table_strict_query) 时，无法下推的外层过滤器会被直接拒绝并抛出 Exception，而不是在本地应用。
:::

PostgreSQL 端的 `INSERT` 查询会在 PostgreSQL 事务 中以 `COPY \"table_name\" (field1, field2, ... fieldN) FROM STDIN` 的形式运行，并在每条 `INSERT` statement 后自动提交。

PostgreSQL `Array` types 会被转换为 ClickHouse arrays。

:::note
请注意：在 PostgreSQL 中，以 `type_name[]` 形式创建的数组数据，可能会在同一列的不同行中包含维度数量不同的多维数组。但在 ClickHouse 中，同一列的所有表行只允许包含维度数量相同的多维数组。
:::

支持多个副本，必须使用 `|` 列出。例如：

```sql
CREATE TABLE test_replicas (id UInt32, name String) ENGINE = PostgreSQL(`postgres{2|3|4}:5432`, 'clickhouse', 'test_replicas', 'postgres', 'mysecretpassword');
```

支持为 PostgreSQL 字典源设置副本优先级。`map` 中的数值越大，优先级越低。最高优先级为 `0`。

在下面的示例中，副本 `example01-1` 的优先级最高：

```xml
<postgresql>
    <port>5432</port>
    <user>clickhouse</user>
    <password>qwerty</password>
    <replica>
        <host>example01-1</host>
        <priority>1</priority>
    </replica>
    <replica>
        <host>example01-2</host>
        <priority>2</priority>
    </replica>
    <db>db_name</db>
    <table>table_name</table>
    <where>id=10</where>
    <invalidate_query>SQL_QUERY</invalidate_query>
</postgresql>
</source>
```

<div id="usage-example">
  ## 使用示例
</div>

<div id="table-in-postgresql">
  ### PostgreSQL 中的表
</div>

```text
postgres=# CREATE TABLE "public"."test" (
"int_id" SERIAL,
"int_nullable" INT NULL DEFAULT NULL,
"float" FLOAT NOT NULL,
"str" VARCHAR(100) NOT NULL DEFAULT '',
"float_nullable" FLOAT NULL DEFAULT NULL,
PRIMARY KEY (int_id));

CREATE TABLE

postgres=# INSERT INTO test (int_id, str, "float") VALUES (1,'test',2);
INSERT 0 1

postgresql> SELECT * FROM test;
  int_id | int_nullable | float | str  | float_nullable
 --------+--------------+-------+------+----------------
       1 |              |     2 | test |
 (1 row)
```

<div id="creating-table-in-clickhouse-and-connecting-to--postgresql-table-created-above">
  ### 在 ClickHouse 中创建表，并连接到上面创建的 PostgreSQL 表
</div>

本示例使用 [PostgreSQL 表引擎](/zh/engines/table-engines/integrations/postgresql.md)，将 ClickHouse 表连接到 PostgreSQL 表，并通过 SELECT 和 INSERT 语句对 PostgreSQL 数据库进行读写：

```sql
CREATE TABLE default.postgresql_table
(
    `float_nullable` Nullable(Float32),
    `str` String,
    `int_id` Int32
)
ENGINE = PostgreSQL('localhost:5432', 'public', 'test', 'postgres_user', 'postgres_password');
```

<div id="inserting-initial-data-from-postgresql-table-into-clickhouse-table-using-a-select-query">
  ### 使用 SELECT 查询将 PostgreSQL 表中的初始数据插入到 ClickHouse 表中
</div>

[postgresql 表函数](/zh/sql-reference/table-functions/postgresql.md) 可将数据从 PostgreSQL 复制到 ClickHouse，这通常用于在 ClickHouse 中而非 PostgreSQL 中执行查询或分析，从而提升数据的查询性能；也可用于将数据从 PostgreSQL 迁移到 ClickHouse。由于我们要将数据从 PostgreSQL 复制到 ClickHouse，因此会在 ClickHouse 中使用 MergeTree 表引擎，并将其命名为 postgresql&#95;copy:

```sql
CREATE TABLE default.postgresql_copy
(
    `float_nullable` Nullable(Float32),
    `str` String,
    `int_id` Int32
)
ENGINE = MergeTree
ORDER BY (int_id);
```

```sql
INSERT INTO default.postgresql_copy
SELECT * FROM postgresql('localhost:5432', 'public', 'test', 'postgres_user', 'postgres_password');
```

<div id="inserting-incremental-data-from-postgresql-table-into-clickhouse-table">
  ### 将 PostgreSQL 表中的增量数据插入 ClickHouse 表
</div>

如果在初始插入之后，还需要在 PostgreSQL 表与 ClickHouse 表之间进行持续同步，则可以在 ClickHouse 中使用 WHERE 子句，仅插入基于时间戳或唯一序列 ID 新增到 PostgreSQL 中的数据。

这就需要记录此前已添加的最大 ID 或时间戳，例如：

```sql
SELECT max(`int_id`) AS maxIntID FROM default.postgresql_copy;
```

然后插入 PostgreSQL 表中大于最大值的数据

```sql
INSERT INTO default.postgresql_copy
SELECT * FROM postgresql('localhost:5432', 'public', 'test', 'postgres_user', 'postgres_password')
WHERE int_id > (SELECT max(int_id) FROM default.postgresql_copy);
```

<div id="selecting-data-from-the-resulting-clickhouse-table">
  ### 从生成的 ClickHouse 表中查询数据
</div>

```sql
SELECT * FROM postgresql_copy WHERE str IN ('test');
```

```text
┌─float_nullable─┬─str──┬─int_id─┐
│           ᴺᵁᴸᴸ │ test │      1 │
└────────────────┴──────┴────────┘
```

<div id="using-non-default-schema">
  ### 使用非默认 schema
</div>

```text
postgres=# CREATE SCHEMA "nice.schema";

postgres=# CREATE TABLE "nice.schema"."nice.table" (a integer);

postgres=# INSERT INTO "nice.schema"."nice.table" SELECT i FROM generate_series(0, 99) as t(i)
```

```sql
CREATE TABLE pg_table_schema_with_dots (a UInt32)
        ENGINE PostgreSQL('localhost:5432', 'clickhouse', 'nice.table', 'postgrsql_user', 'password', 'nice.schema');
```

**另请参阅**

* [`postgresql` 表函数](../../../sql-reference/table-functions/postgresql.md)
* [使用 PostgreSQL 作为字典源](/zh/sql-reference/statements/create/dictionary/sources/postgresql)

<div id="related-content">
  ## 相关内容
</div>

* 博客：[ClickHouse 和 PostgreSQL：数据领域的天作之合 - 第 1 部分](https://clickhouse.com/blog/migrating-data-between-clickhouse-postgres)
* 博客：[ClickHouse 和 PostgreSQL：数据领域的天作之合 - 第 2 部分](https://clickhouse.com/blog/migrating-data-between-clickhouse-postgres-part-2)