---
description: '可对存储在远程 PostgreSQL 服务器上的数据执行 `SELECT` 和 `INSERT` 查询。'
sidebar_label: 'postgresql'
sidebar_position: 160
slug: /sql-reference/table-functions/postgresql
title: 'postgresql'
doc_type: 'reference'
---

可对存储在远程 PostgreSQL 服务器上的数据执行 `SELECT` 和 `INSERT` 查询。

<div id="syntax">
  ## 语法
</div>

```sql
postgresql({host:port, database, table, user, password[, schema, [, on_conflict]] | named_collection[, option=value [,..]]})
```

<div id="arguments">
  ## 参数
</div>

| 参数            | 描述                                                             |
| ------------- | -------------------------------------------------------------- |
| `host:port`   | PostgreSQL 服务器地址。                                              |
| `database`    | 远程数据库名称。                                                       |
| `table`       | 远程表名称，或原样传递给 PostgreSQL 的查询 (参见[传递查询而非表名](#passing-a-query)) 。 |
| `user`        | PostgreSQL 用户。                                                 |
| `password`    | 用户密码。                                                          |
| `schema`      | 非默认的表 schema。可选。                                               |
| `on_conflict` | 冲突解决策略。示例：`ON CONFLICT DO NOTHING`。可选。                         |

参数也可以通过[命名集合](/zh/operations/named-collections.md)传递。在这种情况下，应分别指定 `host` 和 `port`。建议在生产环境中使用这种方式。

<div id="returned_value">
  ## 返回值
</div>

一个表对象，其列与原始 PostgreSQL 表的列相同。

:::note
在 `INSERT` 查询中，为了将表函数 `postgresql(...)` 与带有列名列表的表名区分开，必须使用关键字 `FUNCTION` 或 `TABLE FUNCTION`。请参见下面的示例。
:::

<div id="implementation-details">
  ## 实现细节
</div>

在 PostgreSQL 端，`SELECT` 查询会在只读 PostgreSQL 事务中以 `COPY (SELECT ...) TO STDOUT` 的形式运行，并在每次 `SELECT` 查询后提交事务。

诸如 `=`、`!=`、`>`、`>=`、`<`、`<=` 和 `IN` 这样的简单 `WHERE` 子句会在 PostgreSQL 服务器上执行。

所有 JOIN、聚合、排序、`IN [ array ]` 条件以及 `LIMIT` 采样限制，都只会在对 PostgreSQL 的查询完成后由 ClickHouse 执行。

<div id="passing-a-query">
  ## 传入查询而不是表名
</div>

第三个参数可以不是表名，而是一个原样传递给 PostgreSQL 的 `SELECT` 查询。结果表的结构会根据查询结果自动推断出来。该查询既可以写成子查询，也可以包装在 `query` 函数中：

```sql
SELECT * FROM postgresql('localhost:5432', 'test', (SELECT a, b FROM t1 JOIN t2 USING (id) WHERE a > 0), 'user', 'password');
SELECT * FROM postgresql('localhost:5432', 'test', query('SELECT a, b FROM t1 JOIN t2 USING (id) WHERE a > 0'), 'user', 'password');
```

这对于将 join、聚合或任何其他处理下推到 PostgreSQL 很有用。这样的表是只读的：不允许对其执行 `INSERT`。[`PostgreSQL`](/zh/engines/table-engines/integrations/postgresql) 表引擎也支持相同的语法。

:::note
子查询形式 `(SELECT ...)` 由 ClickHouse 解析，并在发送到服务器之前按照 PostgreSQL 方言重新序列化 (包括 PostgreSQL 标识符引用和字符串字面量转义) 。因此，它必须是有效的 ClickHouse SQL。若要传递 ClickHouse 不解析的 PostgreSQL 特有语法，请使用 `query('...')` 形式，其文本会原样发送到 PostgreSQL。

周围 ClickHouse 查询中的任何外层 `WHERE`、`LIMIT`、聚合等都**不会**下推到传入的查询中——而是在拉取完整查询结果后由 ClickHouse 应用。要限制从 PostgreSQL 读取的数据，请将过滤条件放在传入的查询内部。使用 [`external_table_strict_query = 1`](/zh/operations/settings/settings#external_table_strict_query) 时，无法下推的外层过滤条件会直接抛出异常，而不是在本地应用。
:::

PostgreSQL 端的 `INSERT` 查询会在 PostgreSQL 事务中以 `COPY "table_name" (field1, field2, ... fieldN) FROM STDIN` 的形式运行，并在每条 `INSERT` 语句后自动提交。

PostgreSQL 的 Array 类型会转换为 ClickHouse 数组。

:::note
请注意，在 PostgreSQL 中，像 Integer[] 这样的数组数据类型列可以在不同行中包含不同维度的数组，但在 ClickHouse 中，只允许所有行中的多维数组具有相同的维度。
:::

支持多个副本，必须使用 `|` 列出。例如：

```sql
SELECT name FROM postgresql(`postgres{1|2|3}:5432`, 'postgres_database', 'postgres_table', 'user', 'password');
```

或

```sql
SELECT name FROM postgresql(`postgres1:5431|postgres2:5432`, 'postgres_database', 'postgres_table', 'user', 'password');
```

支持 PostgreSQL 字典源的副本优先级。map 中的数值越大，优先级越低。最高优先级为 `0`。

<div id="examples">
  ## 示例
</div>

PostgreSQL 中的表：

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

使用常规参数从 ClickHouse 查询数据：

```sql
SELECT * FROM postgresql('localhost:5432', 'test', 'test', 'postgresql_user', 'password') WHERE str IN ('test');
```

或使用[命名集合](/zh/operations/named-collections.md)：

```sql
CREATE NAMED COLLECTION mypg AS
        host = 'localhost',
        port = 5432,
        database = 'test',
        user = 'postgresql_user',
        password = 'password';
SELECT * FROM postgresql(mypg, table='test') WHERE str IN ('test');
```

```text
┌─int_id─┬─int_nullable─┬─float─┬─str──┬─float_nullable─┐
│      1 │         ᴺᵁᴸᴸ │     2 │ test │           ᴺᵁᴸᴸ │
└────────┴──────────────┴───────┴──────┴────────────────┘
```

插入：

```sql
INSERT INTO TABLE FUNCTION postgresql('localhost:5432', 'test', 'test', 'postgrsql_user', 'password') (int_id, float) VALUES (2, 3);
SELECT * FROM postgresql('localhost:5432', 'test', 'test', 'postgresql_user', 'password');
```

```text
┌─int_id─┬─int_nullable─┬─float─┬─str──┬─float_nullable─┐
│      1 │         ᴺᵁᴸᴸ │     2 │ test │           ᴺᵁᴸᴸ │
│      2 │         ᴺᵁᴸᴸ │     3 │      │           ᴺᵁᴸᴸ │
└────────┴──────────────┴───────┴──────┴────────────────┘
```

使用非默认 schema：

```text
postgres=# CREATE SCHEMA "nice.schema";

postgres=# CREATE TABLE "nice.schema"."nice.table" (a integer);

postgres=# INSERT INTO "nice.schema"."nice.table" SELECT i FROM generate_series(0, 99) as t(i)
```

```sql
CREATE TABLE pg_table_schema_with_dots (a UInt32)
        ENGINE PostgreSQL('localhost:5432', 'clickhouse', 'nice.table', 'postgrsql_user', 'password', 'nice.schema');
```

<div id="related">
  ## 相关
</div>

* [PostgreSQL 表引擎](../../engines/table-engines/integrations/postgresql.md)
* [将 PostgreSQL 用作字典源](/zh/sql-reference/statements/create/dictionary/sources/postgresql)

<div id="replicating-or-migrating-postgres-data-with-peerdb">
  ### 使用 PeerDB 复制或迁移 Postgres 数据
</div>

> 除了表函数外，您还可以随时使用 ClickHouse 的 [PeerDB](https://docs.peerdb.io/introduction)，在 Postgres 与 ClickHouse 之间建立持续的数据管道。PeerDB 是一款专为通过 CDC (变更数据捕获) 将数据从 Postgres 复制到 ClickHouse 而设计的工具。