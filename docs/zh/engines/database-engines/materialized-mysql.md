---
slug: /zh/engines/database-engines/materialized-mysql
sidebar_position: 29
sidebar_label: MaterializedMySQL
---

# [experimental] MaterializedMySQL {#materialized-mysql}

!!! warning "警告"
    这是一个实验性的特性，不应该在生产中使用.


创建ClickHouse数据库，包含MySQL中所有的表，以及这些表中的所有数据。

ClickHouse服务器作为MySQL副本工作。它读取binlog并执行DDL和DML查询。

## 创建数据库 {#creating-a-database}

``` sql
CREATE DATABASE [IF NOT EXISTS] db_name [ON CLUSTER cluster]
ENGINE = MaterializedMySQL('host:port', ['database' | database], 'user', 'password') [SETTINGS ...]
[TABLE OVERRIDE table1 (...), TABLE OVERRIDE table2 (...)]
```

**引擎参数**

-   `host:port` — MySQL 服务地址.
-   `database` — MySQL 数据库名称.
-   `user` — MySQL 用户名.
-   `password` — MySQL 用户密码.

**引擎配置**


- `max_rows_in_buffer` — 允许在内存中缓存数据的最大行数(对于单个表和无法查询的缓存数据)。当超过这个数字时，数据将被物化。默认值:`65 505`。
- `max_bytes_in_buffer` - 允许在内存中缓存数据的最大字节数(对于单个表和无法查询的缓存数据)。当超过这个数字时，数据将被物化。默认值: `1 048 576 `。
- `max_rows_in_buffers` - 允许在内存中缓存数据的最大行数(用于数据库和无法查询的缓存数据)。当超过这个数字时，数据将被物化。默认值: `65 505`。
- `max_bytes_in_buffers` - 允许在内存中缓存数据的最大字节数(用于数据库和无法查询的缓存数据)。当超过这个数字时，数据将被物化。默认值: `1 048 576`。
- `max_flush_data_time ` - 允许数据在内存中缓存的最大毫秒数(对于数据库和无法查询的缓存数据)。当超过这个时间，数据将被物化。默认值: `1000`。
- `max_wait_time_when_mysql_unavailable` - MySQL不可用时的重试间隔(毫秒)。负值禁用重试。默认值:`1000`。
— `allows_query_when_mysql_lost `—允许在MySQL丢失时查询物化表。默认值:`0`(`false`)。

```sql
CREATE DATABASE mysql ENGINE = MaterializedMySQL('localhost:3306', 'db', 'user', '***')
     SETTINGS
        allows_query_when_mysql_lost=true,
        max_wait_time_when_mysql_unavailable=10000;
```

**MySQL服务器端配置**

为了`MaterializedMySQL`的正确工作，有一些必须设置的`MySQL`端配置设置:

- `default_authentication_plugin = mysql_native_password `，因为 `MaterializedMySQL` 只能授权使用该方法。
- `gtid_mode = on`，因为基于GTID的日志记录是提供正确的 `MaterializedMySQL`复制的强制要求。

:::info "注意"
当打开`gtid_mode`时，您还应该指定`enforce_gtid_consistency = on`。
:::

## 虚拟列 {#virtual-columns}

当使用`MaterializeMySQL`数据库引擎时，[ReplacingMergeTree](../../engines/table-engines/mergetree-family/replacingmergetree.md)表与虚拟的`_sign`和`_version`列一起使用。

- `_version` — 事务版本. 类型 [UInt64](../../sql-reference/data-types/int-uint.md).
- `_sign` — 删除标记. 类型 [Int8](../../sql-reference/data-types/int-uint.md). 可能的值:
    - `1` — 行没有删除,
    - `-1` — 行已被删除.

## 支持的数据类型 {#data_types-support}

| MySQL                   | ClickHouse                                                   |
|-------------------------|--------------------------------------------------------------|
| TINY                    | [Int8](../../sql-reference/data-types/int-uint.md)           |
| SHORT                   | [Int16](../../sql-reference/data-types/int-uint.md)          |
| INT24                   | [Int32](../../sql-reference/data-types/int-uint.md)          |
| LONG                    | [UInt32](../../sql-reference/data-types/int-uint.md)         |
| LONGLONG                | [UInt64](../../sql-reference/data-types/int-uint.md)         |
| FLOAT                   | [Float32](../../sql-reference/data-types/float.md)           |
| DOUBLE                  | [Float64](../../sql-reference/data-types/float.md)           |
| DECIMAL, NEWDECIMAL     | [Decimal](../../sql-reference/data-types/decimal.md)         |
| DATE, NEWDATE           | [Date](../../sql-reference/data-types/date.md)               |
| DATETIME, TIMESTAMP     | [DateTime](../../sql-reference/data-types/datetime.md)       |
| DATETIME2, TIMESTAMP2   | [DateTime64](../../sql-reference/data-types/datetime64.md)   |
| YEAR                    | [UInt16](../../sql-reference/data-types/int-uint.md)         |
| TIME                    | [Int64](../../sql-reference/data-types/int-uint.md)          |
| ENUM                    | [Enum](../../sql-reference/data-types/enum.md)               |
| STRING                  | [String](../../sql-reference/data-types/string.md)           |
| VARCHAR, VAR_STRING     | [String](../../sql-reference/data-types/string.md)           |
| BLOB                    | [String](../../sql-reference/data-types/string.md)           |
| GEOMETRY                | [String](../../sql-reference/data-types/string.md)           |
| BINARY                  | [FixedString](../../sql-reference/data-types/fixedstring.md) |
| BIT                     | [UInt64](../../sql-reference/data-types/int-uint.md)         |
| SET                     | [UInt64](../../sql-reference/data-types/int-uint.md)         |

[Nullable](../../sql-reference/data-types/nullable.md) 已经被支持.

MySQL中的Time 类型，会被ClickHouse转换成微秒来存储

不支持其他类型。如果MySQL表包含此类类型的列，ClickHouse抛出异常"Unhandled data type"并停止复制。

## 规范和推荐用法 {#specifics-and-recommendations}

### 兼容性限制 {#compatibility-restrictions}

除了数据类型的限制之外，还有一些限制与`MySQL`数据库相比有所不同，这应该在复制之前解决:

- `MySQL` 中的每个表都应该包含 `PRIMARY KEY`。
- 对于表的复制，那些包含 `ENUM` 字段值超出范围的行(在 `ENUM` 签名中指定)将不起作用。

### DDL Queries {#ddl-queries}

MySQL DDL 语句会被转换成对应的ClickHouse DDL 语句，比如： ([ALTER](../../sql-reference/statements/alter/index.md), [CREATE](../../sql-reference/statements/create.md), [DROP](../../sql-reference/statements/drop.md), [RENAME](../../sql-reference/statements/rename.md)). 如果ClickHouse 无法解析某些语句DDL 操作，则会跳过。


### 数据复制 {#data-replication}

MaterializedMySQL不支持直接的 `INSERT`， `DELETE` 和 `UPDATE` 查询。然而，它们在数据复制方面得到了支持:

- MySQL `INSERT`查询被转换为`_sign=1`的INSERT查询。
- MySQL `DELETE`查询被转换为`INSERT`，并且`_sign=-1`。
- 如果主键被修改了，MySQL的 `UPDATE` 查询将被转换为 `INSERT` 带 `_sign=1`  和INSERT 带有_sign=-1;如果主键没有被修改，则转换为`INSERT`和`_sign=1`。

###  MaterializedMySQL 数据表查询 {#select}

`SELECT` 查询从 `MaterializedMySQL`表有一些细节:

 - 如果在SELECT查询中没有指定`_version`，则 [FINAL](../../sql-reference/statements/select/from.md#select-from- FINAL)修饰符被使用，所以只有带有 `MAX(_version)`的行会返回每个主键值。

 - 如果在SELECT查询中没有指定 `_sign`，则默认使用 `WHERE _sign=1 `。所以被删除的行不是
包含在结果集中。

 - 结果包括列注释，以防MySQL数据库表中存在这些列注释。

### 索引转换 {#index-conversion}

在ClickHouse表中，MySQL的 `PRIMARY KEY` 和 `INDEX` 子句被转换为 `ORDER BY` 元组。

ClickHouse只有一个物理排序，由 `order by` 条件决定。要创建一个新的物理排序，请使用[materialized views](../../sql-reference/statements/create/view.md#materialized)。

**注意**

- `_sign=-1` 的行不会被物理地从表中删除。
- 级联 `UPDATE/DELETE` 查询不支持 `MaterializedMySQL` 引擎，因为他们在 MySQL binlog中不可见的
— 复制很容易被破坏。
— 禁止对数据库和表进行手工操作。
- `MaterializedMySQL` 受[optimize_on_insert](../../operations/settings/settings.md#optimize-on-insert)设置的影响。当MySQL服务器中的一个表发生变化时，数据会合并到 `MaterializedMySQL` 数据库中相应的表中。

### 表重写 {#table-overrides}

表覆盖可用于自定义ClickHouse DDL查询，从而允许您对应用程序进行模式优化。这对于控制分区特别有用，分区对MaterializedMySQL的整体性能非常重要。

这些是你可以对MaterializedMySQL表重写的模式转换操作:

 * 修改列类型。必须与原始类型兼容，否则复制将失败。例如，可以将`UInt32`列修改为`UInt64`，不能将 `String` 列修改为 `Array(String)`。
 * 修改 [column TTL](../table-engines/mergetree-family/mergetree.md#mergetree-column-ttl).
 * 修改 [column compression codec](../../sql-reference/statements/create/table.mdx#codecs).
 * 增加 [ALIAS columns](../../sql-reference/statements/create/table.mdx#alias).
 * 增加 [skipping indexes](../table-engines/mergetree-family/mergetree.md#table_engine-mergetree-data_skipping-indexes)
 * 增加 [projections](../table-engines/mergetree-family/mergetree.md#projections).
 请注意，当使用 `SELECT ... FINAL ` (MaterializedMySQL默认是这样做的) 时，预测优化是被禁用的，所以这里是受限的， `INDEX ... TYPE hypothesis `[在v21.12的博客文章中描述]](https://clickhouse.com/blog/en/2021/clickhouse-v21.12-released/)可能在这种情况下更有用。
 * 修改 [PARTITION BY](../table-engines/mergetree-family/custom-partitioning-key.md)
 * 修改 [ORDER BY](../table-engines/mergetree-family/mergetree.md#mergetree-query-clauses)
 * 修改 [PRIMARY KEY](../table-engines/mergetree-family/mergetree.md#mergetree-query-clauses)
 * 增加 [SAMPLE BY](../table-engines/mergetree-family/mergetree.md#mergetree-query-clauses)
 * 增加 [table TTL](../table-engines/mergetree-family/mergetree.md#mergetree-query-clauses)

```sql
CREATE DATABASE db_name ENGINE = MaterializedMySQL(...)
[SETTINGS ...]
[TABLE OVERRIDE table_name (
    [COLUMNS (
        [col_name [datatype] [ALIAS expr] [CODEC(...)] [TTL expr], ...]
        [INDEX index_name expr TYPE indextype[(...)] GRANULARITY val, ...]
        [PROJECTION projection_name (SELECT <COLUMN LIST EXPR> [GROUP BY] [ORDER BY]), ...]
    )]
    [ORDER BY expr]
    [PRIMARY KEY expr]
    [PARTITION BY expr]
    [SAMPLE BY expr]
    [TTL expr]
), ...]
```

示例:

```sql
CREATE DATABASE db_name ENGINE = MaterializedMySQL(...)
TABLE OVERRIDE table1 (
    COLUMNS (
        userid UUID,
        category LowCardinality(String),
        timestamp DateTime CODEC(Delta, Default)
    )
    PARTITION BY toYear(timestamp)
),
TABLE OVERRIDE table2 (
    COLUMNS (
        client_ip String TTL created + INTERVAL 72 HOUR
    )
    SAMPLE BY ip_hash
)
```


`COLUMNS`列表是稀疏的;根据指定修改现有列，添加额外的ALIAS列。不可能添加普通列或实体化列。具有不同类型的已修改列必须可从原始类型赋值。在执行`CREATE DATABASE` 查询时，目前还没有验证这个或类似的问题，因此需要格外小心。

您可以为还不存在的表指定重写。

!!! warning "警告"
    如果使用时不小心，很容易用表重写中断复制。例如:

    * 如果一个ALIAS列被添加了一个表覆盖，并且一个具有相同名称的列后来被添加到源MySQL表，在ClickHouse中转换后的ALTER table查询将失败并停止复制。
    * 目前可以添加引用可空列的覆盖，而非空列是必需的，例如 `ORDER BY` 或 `PARTITION BY`。这将导致CREATE TABLE查询失败，也会导致复制停止。

## 使用示例 {#examples-of-use}

 MySQL 查询语句:

``` sql
mysql> CREATE DATABASE db;
mysql> CREATE TABLE db.test (a INT PRIMARY KEY, b INT);
mysql> INSERT INTO db.test VALUES (1, 11), (2, 22);
mysql> DELETE FROM db.test WHERE a=1;
mysql> ALTER TABLE db.test ADD COLUMN c VARCHAR(16);
mysql> UPDATE db.test SET c='Wow!', b=222;
mysql> SELECT * FROM test;
```

```text
┌─a─┬───b─┬─c────┐
│ 2 │ 222 │ Wow! │
└───┴─────┴──────┘
```

ClickHouse中的数据库，与MySQL服务器交换数据:

创建的数据库和表:

``` sql
CREATE DATABASE mysql ENGINE = MaterializedMySQL('localhost:3306', 'db', 'user', '***');
SHOW TABLES FROM mysql;
```

``` text
┌─name─┐
│ test │
└──────┘
```

数据插入之后：

``` sql
SELECT * FROM mysql.test;
```

``` text
┌─a─┬──b─┐
│ 1 │ 11 │
│ 2 │ 22 │
└───┴────┘
```

删除数据后，添加列并更新:

``` sql
SELECT * FROM mysql.test;
```

``` text
┌─a─┬───b─┬─c────┐
│ 2 │ 222 │ Wow! │
└───┴─────┴──────┘
```
