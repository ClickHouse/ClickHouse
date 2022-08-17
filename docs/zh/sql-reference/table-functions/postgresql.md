---
toc_priority: 42
toc_title: postgresql
---

# postgresql {#postgresql}

允许对存储在远程 PostgreSQL 服务器上的数据进行 `SELECT` 和 `INSERT` 查询.

**语法**

``` sql
postgresql('host:port', 'database', 'table', 'user', 'password'[, `schema`])
```

**参数**

-   `host:port` — PostgreSQL 服务器地址.
-   `database` — 远程数据库名称.
-   `table` — 远程表名称.
-   `user` — PostgreSQL 用户.
-   `password` — 用户密码.
-   `schema` — 非默认的表结构. 可选.

**返回值**

一个表对象，其列数与原 PostgreSQL 表的列数相同。

!!! info "Note"
    在`INSERT`查询中，为了区分表函数`postgresql(..)`和表名以及表的列名列表，你必须使用关键字`FUNCTION`或`TABLE FUNCTION`。请看下面的例子。

## 实施细节 {#implementation-details}

`SELECT`查询在 PostgreSQL 上以 `COPY (SELECT ...) TO STDOUT` 的方式在只读的 PostgreSQL 事务中运行，每次在`SELECT`查询后提交。

简单的`WHERE`子句，如`=`、`！=`、`>`、`>=`、`<`、`<=`和`IN`，在PostgreSQL服务器上执行。

所有的连接、聚合、排序，`IN [ 数组 ]`条件和`LIMIT`采样约束只有在对PostgreSQL的查询结束后才会在ClickHouse中执行。

PostgreSQL 上的`INSERT`查询以`COPY "table_name" (field1, field2, ... fieldN) FROM STDIN`的方式在 PostgreSQL 事务中运行，每次`INSERT`语句后自动提交。

PostgreSQL 数组类型将转换为 ClickHouse 数组。

!!! info "Note"
    要小心，在 PostgreSQL 中，像 Integer[] 这样的数组数据类型列可以在不同的行中包含不同维度的数组，但在 ClickHouse 中，只允许在所有的行中有相同维度的多维数组。

支持设置 PostgreSQL 字典源中 Replicas 的优先级。地图中的数字越大，优先级就越低。`0`代表最高的优先级。

**示例**

PostgreSQL 中的表:

``` text
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

从 ClickHouse 检索数据:

```sql
SELECT * FROM postgresql('localhost:5432', 'test', 'test', 'postgresql_user', 'password') WHERE str IN ('test');
```

``` text
┌─int_id─┬─int_nullable─┬─float─┬─str──┬─float_nullable─┐
│      1 │         ᴺᵁᴸᴸ │     2 │ test │           ᴺᵁᴸᴸ │
└────────┴──────────────┴───────┴──────┴────────────────┘
```

插入数据:

```sql
INSERT INTO TABLE FUNCTION postgresql('localhost:5432', 'test', 'test', 'postgrsql_user', 'password') (int_id, float) VALUES (2, 3);
SELECT * FROM postgresql('localhost:5432', 'test', 'test', 'postgresql_user', 'password');
```

``` text
┌─int_id─┬─int_nullable─┬─float─┬─str──┬─float_nullable─┐
│      1 │         ᴺᵁᴸᴸ │     2 │ test │           ᴺᵁᴸᴸ │
│      2 │         ᴺᵁᴸᴸ │     3 │      │           ᴺᵁᴸᴸ │
└────────┴──────────────┴───────┴──────┴────────────────┘
```

使用非默认的表结构:

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

-   [PostgreSQL 表引擎](../../engines/table-engines/integrations/postgresql.md)
-   [使用 PostgreSQL 作为外部字典的来源](../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md#dicts-external_dicts_dict_sources-postgresql)

[原始文章](https://clickhouse.tech/docs/en/sql-reference/table-functions/postgresql/) <!--hide-->
