---
toc_priority: 11
toc_title: PostgreSQL
---

# PostgreSQL {#postgresql}

PostgreSQL 引擎允许 ClickHouse 对存储在远程 PostgreSQL 服务器上的数据执行 `SELECT` 和 `INSERT` 查询.

## 创建一张表 {#creating-a-table}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [TTL expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2] [TTL expr2],
    ...
) ENGINE = PostgreSQL('host:port', 'database', 'table', 'user', 'password'[, `schema`]);
```

<!-- 详情请见 [CREATE TABLE](../../../sql-reference/statements/create/table.md#create-table-query) 查询. -->

表结构可以与 PostgreSQL 源表结构不同:

-   列名应与 PostgreSQL 源表中的列名相同，但您可以按任何顺序使用其中的一些列。
-   列类型可能与源表中的列类型不同。 ClickHouse尝试将数值[映射](../../../sql-reference/functions/type-conversion-functions.md#type_conversion_function-cast) 到ClickHouse的数据类型。
-   设置 `external_table_functions_use_nulls` 来定义如何处理 Nullable 列. 默认值是 1, 当设置为 0 时 - 表函数将不会使用 nullable 列，而是插入默认值来代替 null. 这同样适用于数组数据类型中的 null 值.

**引擎参数**

-   `host:port` — PostgreSQL 服务器地址.
-   `database` — 数据库名称.
-   `table` — 表名称.
-   `user` — PostgreSQL 用户.
-   `password` — 用户密码.
-   `schema` — Non-default table schema. 可选.

## 实施细节 {#implementation-details}

在 PostgreSQL 上的 `SELECT` 查询以 `COPY (SELECT ...) TO STDOUT` 的方式在只读 PostgreSQL 事务中运行，每次 `SELECT` 查询后提交。

简单的 `WHERE` 子句，如`=`，`！=`，`>`，`>=`，`<`，`<=`，和`IN`是在PostgreSQL 服务器上执行。

所有的连接、聚合、排序、`IN [ array ]`条件和`LIMIT`采样约束都是在 PostgreSQL 的查询结束后才在ClickHouse中执行的。

在 PostgreSQL 上的 `INSERT` 查询以 `COPY "table_name" (field1, field2, ... fieldN) FROM STDIN` 的方式在 PostgreSQL 事务中运行，每条 `INSERT` 语句后自动提交。

PostgreSQL 的 `Array` 类型会被转换为 ClickHouse 数组。

!!! info "Note"
    要小心 - 一个在 PostgreSQL 中的数组数据，像`type_name[]`这样创建，可以在同一列的不同表行中包含不同维度的多维数组。但是在 ClickHouse 中，只允许在同一列的所有表行中包含相同维数的多维数组。

支持设置 PostgreSQL 字典源中 Replicas 的优先级。地图中的数字越大，优先级就越低。最高的优先级是 `0`。

在下面的例子中，副本`example01-1`有最高的优先级。

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

## 用法示例 {#usage-example}

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

ClickHouse 中的表, 从上面创建的 PostgreSQL 表中检索数据:

``` sql
CREATE TABLE default.postgresql_table
(
    `float_nullable` Nullable(Float32),
    `str` String,
    `int_id` Int32
)
ENGINE = PostgreSQL('localhost:5432', 'public', 'test', 'postges_user', 'postgres_password');
```

``` sql
SELECT * FROM postgresql_table WHERE str IN ('test');
```

``` text
┌─float_nullable─┬─str──┬─int_id─┐
│           ᴺᵁᴸᴸ │ test │      1 │
└────────────────┴──────┴────────┘
```

使用非默认的模式:

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

<!-- -   [`postgresql` 表函数](../../../sql-reference/table-functions/postgresql.md) -->
-   [使用 PostgreSQL 作为外部字典的来源](../../../sql-reference/dictionaries/external-dictionaries/external-dicts-dict-sources.md#dicts-external_dicts_dict_sources-postgresql)

[原始文章](https://clickhouse.tech/docs/en/engines/table-engines/integrations/postgresql/) <!--hide-->
