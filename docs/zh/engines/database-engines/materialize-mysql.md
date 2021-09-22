---
toc_priority: 29
toc_title: "[experimental] MaterializedMySQL"
---

# [experimental] MaterializedMySQL {#materialized-mysql}

**这是一个实验性的特性，不应该在生产中使用。**

创建ClickHouse数据库，包含MySQL中所有的表，以及这些表中的所有数据。

ClickHouse服务器作为MySQL副本工作。它读取binlog并执行DDL和DML查询。

这个功能是实验性的。

## 创建数据库 {#creating-a-database}

``` sql
CREATE DATABASE [IF NOT EXISTS] db_name [ON CLUSTER cluster]
ENGINE = MaterializeMySQL('host:port', ['database' | database], 'user', 'password') [SETTINGS ...]
```

**引擎参数**

-   `host:port` — MySQL服务地址
-   `database` — MySQL数据库名称
-   `user` — MySQL用户名
-   `password` — MySQL用户密码

**引擎配置**

-   `max_rows_in_buffer` — 允许数据缓存到内存中的最大行数(对于单个表和无法查询的缓存数据)。当超过行数时，数据将被物化。默认值: `65505`。
-   `max_bytes_in_buffer` —  允许在内存中缓存数据的最大字节数(对于单个表和无法查询的缓存数据)。当超过行数时，数据将被物化。默认值: `1048576`.
-   `max_rows_in_buffers` — 允许数据缓存到内存中的最大行数(对于数据库和无法查询的缓存数据)。当超过行数时，数据将被物化。默认值: `65505`.
-   `max_bytes_in_buffers` — 允许在内存中缓存数据的最大字节数(对于数据库和无法查询的缓存数据)。当超过行数时，数据将被物化。默认值: `1048576`.
-   `max_flush_data_time` — 允许数据在内存中缓存的最大毫秒数(对于数据库和无法查询的缓存数据)。当超过这个时间时，数据将被物化。默认值: `1000`.
-   `max_wait_time_when_mysql_unavailable` — 当MySQL不可用时重试间隔(毫秒)。负值禁止重试。默认值: `1000`.
-   `allows_query_when_mysql_lost` — 当mysql丢失时，允许查询物化表。默认值: `0` (`false`).
```
CREATE DATABASE mysql ENGINE = MaterializeMySQL('localhost:3306', 'db', 'user', '***') 
     SETTINGS 
        allows_query_when_mysql_lost=true,
        max_wait_time_when_mysql_unavailable=10000;
```

**MySQL服务器端配置**

为了`MaterializeMySQL`正确的工作，有一些强制性的`MySQL`侧配置设置应该设置:

- `default_authentication_plugin = mysql_native_password`，因为`MaterializeMySQL`只能使用此方法授权。
- `gtid_mode = on`，因为要提供正确的`MaterializeMySQL`复制，基于GTID的日志记录是必须的。注意，在打开这个模式`On`时，你还应该指定`enforce_gtid_consistency = on`。

## 虚拟列 {#virtual-columns}

当使用`MaterializeMySQL`数据库引擎时，[ReplacingMergeTree](../../engines/table-engines/mergetree-family/replacingmergetree.md)表与虚拟的`_sign`和`_version`列一起使用。

- `_version` — 同步版本。 类型[UInt64](../../sql-reference/data-types/int-uint.md).
- `_sign` — 删除标记。类型 [Int8](../../sql-reference/data-types/int-uint.md). Possible values:
    - `1` — 行不会删除,
    - `-1` — 行被删除。

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
| ENUM                    | [Enum](../../sql-reference/data-types/enum.md)               |
| STRING                  | [String](../../sql-reference/data-types/string.md)           |
| VARCHAR, VAR_STRING     | [String](../../sql-reference/data-types/string.md)           |
| BLOB                    | [String](../../sql-reference/data-types/string.md)           |
| BINARY                  | [FixedString](../../sql-reference/data-types/fixedstring.md) |

不支持其他类型。如果MySQL表包含此类类型的列，ClickHouse抛出异常"Unhandled data type"并停止复制。

[Nullable](../../sql-reference/data-types/nullable.md)已经支持

## 使用方式 {#specifics-and-recommendations}

### 兼容性限制

除了数据类型的限制外，与`MySQL`数据库相比，还存在一些限制，在实现复制之前应先解决这些限制：

- `MySQL`中的每个表都应该包含`PRIMARY KEY`

- 对于包含`ENUM`字段值超出范围（在`ENUM`签名中指定）的行的表，复制将不起作用。

### DDL查询 {#ddl-queries}

MySQL DDL查询转换为相应的ClickHouse DDL查询([ALTER](../../sql-reference/statements/alter/index.md), [CREATE](../../sql-reference/statements/create/index.md), [DROP](../../sql-reference/statements/drop.md), [RENAME](../../sql-reference/statements/rename.md))。如果ClickHouse无法解析某个DDL查询，则该查询将被忽略。

### Data Replication {#data-replication}

`MaterializeMySQL`不支持直接`INSERT`, `DELETE`和`UPDATE`查询. 但是，它们是在数据复制方面支持的：

- MySQL的`INSERT`查询转换为`INSERT`并携带`_sign=1`.

- MySQL的`DELETE`查询转换为`INSERT`并携带`_sign=-1`.

- MySQL的`UPDATE`查询转换为`INSERT`并携带`_sign=-1`, `INSERT`和`_sign=1`.

### 查询MaterializeMySQL表 {#select}

`SELECT`查询`MaterializeMySQL`表有一些细节:

- 如果`_version`在`SELECT`中没有指定，则使用[FINAL](../../sql-reference/statements/select/from.md#select-from-final)修饰符。所以只有带有`MAX(_version)`的行才会被选中。

- 如果`_sign`在`SELECT`中没有指定，则默认使用`WHERE _sign=1`。因此，删除的行不会包含在结果集中。

- 结果包括列中的列注释，因为它们存在于SQL数据库表中。

### Index Conversion {#index-conversion}

MySQL的`PRIMARY KEY`和`INDEX`子句在ClickHouse表中转换为`ORDER BY`元组。

ClickHouse只有一个物理顺序，由`ORDER BY`子句决定。要创建一个新的物理顺序，使用[materialized views](../../sql-reference/statements/create/view.md#materialized)。

**Notes**

- 带有`_sign=-1`的行不会从表中物理删除。
- `MaterializeMySQL`引擎不支持级联`UPDATE/DELETE`查询。
- 复制很容易被破坏。
- 禁止对数据库和表进行手工操作。
- `MaterializeMySQL`受[optimize_on_insert](../../operations/settings/settings.md#optimize-on-insert)设置的影响。当MySQL服务器中的表发生变化时，数据会合并到`MaterializeMySQL`数据库中相应的表中。

## 使用示例 {#examples-of-use}

MySQL操作:

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
+---+------+------+ 
| a |    b |    c |
+---+------+------+ 
| 2 |  222 | Wow! |
+---+------+------+
```

ClickHouse中的数据库，与MySQL服务器交换数据:

创建的数据库和表:

``` sql
CREATE DATABASE mysql ENGINE = MaterializeMySQL('localhost:3306', 'db', 'user', '***');
SHOW TABLES FROM mysql;
```

``` text
┌─name─┐
│ test │
└──────┘
```

然后插入数据:

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

[来源文章](https://clickhouse.com/docs/en/engines/database-engines/materialize-mysql/) <!--hide-->
