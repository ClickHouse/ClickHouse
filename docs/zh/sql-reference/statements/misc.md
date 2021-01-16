---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 41
toc_title: "\u5176\u4ED6"
---

# 杂项查询 {#miscellaneous-queries}

## ATTACH {#attach}

这个查询是完全一样的 `CREATE`，但是

-   而不是这个词 `CREATE` 它使用这个词 `ATTACH`.
-   查询不会在磁盘上创建数据，但假定数据已经在适当的位置，只是将有关表的信息添加到服务器。
    执行附加查询后，服务器将知道表的存在。

如果表之前已分离 (`DETACH`），意味着其结构是已知的，可以使用速记而不限定该结构。

``` sql
ATTACH TABLE [IF NOT EXISTS] [db.]name [ON CLUSTER cluster]
```

启动服务器时使用此查询。 服务器将表元数据作为文件存储 `ATTACH` 查询，它只是在启动时运行（除了在服务器上显式创建的系统表）。

## CHECK TABLE {#check-table}

检查表中的数据是否已损坏。

``` sql
CHECK TABLE [db.]name
```

该 `CHECK TABLE` 查询将实际文件大小与存储在服务器上的预期值进行比较。 如果文件大小与存储的值不匹配，则表示数据已损坏。 例如，这可能是由查询执行期间的系统崩溃引起的。

查询响应包含 `result` 具有单行的列。 该行的值为
[布尔值](../../sql-reference/data-types/boolean.md) 类型:

-   0-表中的数据已损坏。
-   1-数据保持完整性。

该 `CHECK TABLE` 查询支持下表引擎:

-   [日志](../../engines/table-engines/log-family/log.md)
-   [TinyLog](../../engines/table-engines/log-family/tinylog.md)
-   [StripeLog](../../engines/table-engines/log-family/stripelog.md)
-   [梅树家族](../../engines/table-engines/mergetree-family/mergetree.md)

使用另一个表引擎对表执行会导致异常。

从发动机 `*Log` 家庭不提供故障自动数据恢复。 使用 `CHECK TABLE` 查询以及时跟踪数据丢失。

为 `MergeTree` 家庭发动机， `CHECK TABLE` 查询显示本地服务器上表的每个单独数据部分的检查状态。

**如果数据已损坏**

如果表已损坏，则可以将未损坏的数据复制到另一个表。 要做到这一点:

1.  创建具有与损坏的表相同结构的新表。 要执行此操作，请执行查询 `CREATE TABLE <new_table_name> AS <damaged_table_name>`.
2.  设置 [max\_threads](../../operations/settings/settings.md#settings-max_threads) 值为1以在单个线程中处理下一个查询。 要执行此操作，请运行查询 `SET max_threads = 1`.
3.  执行查询 `INSERT INTO <new_table_name> SELECT * FROM <damaged_table_name>`. 此请求将未损坏的数据从损坏的表复制到另一个表。 只有损坏部分之前的数据才会被复制。
4.  重新启动 `clickhouse-client` 要重置 `max_threads` 价值。

## DESCRIBE TABLE {#misc-describe-table}

``` sql
DESC|DESCRIBE TABLE [db.]table [INTO OUTFILE filename] [FORMAT format]
```

返回以下内容 `String` 类型列:

-   `name` — Column name.
-   `type`— Column type.
-   `default_type` — Clause that is used in [默认表达式](create.md#create-default-values) (`DEFAULT`, `MATERIALIZED` 或 `ALIAS`). 如果未指定默认表达式，则Column包含一个空字符串。
-   `default_expression` — Value specified in the `DEFAULT` 条款
-   `comment_expression` — Comment text.

嵌套的数据结构输出 “expanded” 格式。 每列分别显示，名称后面有一个点。

## DETACH {#detach}

删除有关 ‘name’ 表从服务器。 服务器停止了解表的存在。

``` sql
DETACH TABLE [IF EXISTS] [db.]name [ON CLUSTER cluster]
```

这不会删除表的数据或元数据。 在下一次服务器启动时，服务器将读取元数据并再次查找有关表的信息。
同样，一个 “detached” 表可以使用重新连接 `ATTACH` 查询（系统表除外，它们没有为它们存储元数据）。

没有 `DETACH DATABASE` 查询。

## DROP {#drop}

此查询有两种类型: `DROP DATABASE` 和 `DROP TABLE`.

``` sql
DROP DATABASE [IF EXISTS] db [ON CLUSTER cluster]
```

删除内部的所有表 ‘db’ 数据库，然后删除 ‘db’ 数据库本身。
如果 `IF EXISTS` 如果数据库不存在，则不会返回错误。

``` sql
DROP [TEMPORARY] TABLE [IF EXISTS] [db.]name [ON CLUSTER cluster]
```

删除表。
如果 `IF EXISTS` 如果表不存在或数据库不存在，则不会返回错误。

    DROP DICTIONARY [IF EXISTS] [db.]name

删除字典。
如果 `IF EXISTS` 如果表不存在或数据库不存在，则不会返回错误。

## DROP USER {#drop-user-statement}

删除用户。

### 语法 {#drop-user-syntax}

``` sql
DROP USER [IF EXISTS] name [,...] [ON CLUSTER cluster_name]
```

## DROP ROLE {#drop-role-statement}

删除角色。

已删除的角色将从授予该角色的所有实体撤销。

### 语法 {#drop-role-syntax}

``` sql
DROP ROLE [IF EXISTS] name [,...] [ON CLUSTER cluster_name]
```

## DROP ROW POLICY {#drop-row-policy-statement}

删除行策略。

已删除行策略将从分配该策略的所有实体撤销。

### 语法 {#drop-row-policy-syntax}

``` sql
DROP [ROW] POLICY [IF EXISTS] name [,...] ON [database.]table [,...] [ON CLUSTER cluster_name]
```

## DROP QUOTA {#drop-quota-statement}

删除配额。

已删除的配额将从分配配额的所有实体撤销。

### 语法 {#drop-quota-syntax}

``` sql
DROP QUOTA [IF EXISTS] name [,...] [ON CLUSTER cluster_name]
```

## DROP SETTINGS PROFILE {#drop-settings-profile-statement}

删除配额。

已删除的配额将从分配配额的所有实体撤销。

### 语法 {#drop-settings-profile-syntax}

``` sql
DROP [SETTINGS] PROFILE [IF EXISTS] name [,...] [ON CLUSTER cluster_name]
```

## EXISTS {#exists-statement}

``` sql
EXISTS [TEMPORARY] [TABLE|DICTIONARY] [db.]name [INTO OUTFILE filename] [FORMAT format]
```

返回单 `UInt8`-type column，其中包含单个值 `0` 如果表或数据库不存在，或 `1` 如果该表存在于指定的数据库中。

## KILL QUERY {#kill-query-statement}

``` sql
KILL QUERY [ON CLUSTER cluster]
  WHERE <where expression to SELECT FROM system.processes query>
  [SYNC|ASYNC|TEST]
  [FORMAT format]
```

尝试强制终止当前正在运行的查询。
要终止的查询是从系统中选择的。使用在定义的标准进程表 `WHERE` 《公约》条款 `KILL` 查询。

例:

``` sql
-- Forcibly terminates all queries with the specified query_id:
KILL QUERY WHERE query_id='2-857d-4a57-9ee0-327da5d60a90'

-- Synchronously terminates all queries run by 'username':
KILL QUERY WHERE user='username' SYNC
```

只读用户只能停止自己的查询。

默认情况下，使用异步版本的查询 (`ASYNC`），不等待确认查询已停止。

同步版本 (`SYNC`）等待所有查询停止，并在停止时显示有关每个进程的信息。
响应包含 `kill_status` 列，它可以采用以下值:

1.  ‘finished’ – The query was terminated successfully.
2.  ‘waiting’ – Waiting for the query to end after sending it a signal to terminate.
3.  The other values ​​explain why the query can't be stopped.

测试查询 (`TEST`）仅检查用户的权限并显示要停止的查询列表。

## KILL MUTATION {#kill-mutation}

``` sql
KILL MUTATION [ON CLUSTER cluster]
  WHERE <where expression to SELECT FROM system.mutations query>
  [TEST]
  [FORMAT format]
```

尝试取消和删除 [突变](alter.md#alter-mutations) 当前正在执行。 要取消的突变选自 [`system.mutations`](../../operations/system-tables/mutations.md#system_tables-mutations) 表使用由指定的过滤器 `WHERE` 《公约》条款 `KILL` 查询。

测试查询 (`TEST`）仅检查用户的权限并显示要停止的查询列表。

例:

``` sql
-- Cancel and remove all mutations of the single table:
KILL MUTATION WHERE database = 'default' AND table = 'table'

-- Cancel the specific mutation:
KILL MUTATION WHERE database = 'default' AND table = 'table' AND mutation_id = 'mutation_3.txt'
```

The query is useful when a mutation is stuck and cannot finish (e.g. if some function in the mutation query throws an exception when applied to the data contained in the table).

已经由突变所做的更改不会回滚。

## OPTIMIZE {#misc_operations-optimize}

``` sql
OPTIMIZE TABLE [db.]name [ON CLUSTER cluster] [PARTITION partition | PARTITION ID 'partition_id'] [FINAL] [DEDUPLICATE]
```

此查询尝试使用来自表引擎的表初始化表的数据部分的非计划合并 [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md) 家人

该 `OPTMIZE` 查询也支持 [MaterializedView](../../engines/table-engines/special/materializedview.md) 和 [缓冲区](../../engines/table-engines/special/buffer.md) 引擎 不支持其他表引擎。

当 `OPTIMIZE` 与使用 [ReplicatedMergeTree](../../engines/table-engines/mergetree-family/replication.md) 表引擎的家族，ClickHouse创建合并任务，并等待在所有节点上执行（如果 `replication_alter_partitions_sync` 设置已启用）。

-   如果 `OPTIMIZE` 出于任何原因不执行合并，它不通知客户端。 要启用通知，请使用 [optimize\_throw\_if\_noop](../../operations/settings/settings.md#setting-optimize_throw_if_noop) 设置。
-   如果您指定 `PARTITION`，仅优化指定的分区。 [如何设置分区表达式](alter.md#alter-how-to-specify-part-expr).
-   如果您指定 `FINAL`，即使所有数据已经在一个部分中，也会执行优化。
-   如果您指定 `DEDUPLICATE`，然后完全相同的行将被重复数据删除（所有列进行比较），这仅适用于MergeTree引擎。

!!! warning "警告"
    `OPTIMIZE` 无法修复 “Too many parts” 错误

## RENAME {#misc_operations-rename}

重命名一个或多个表。

``` sql
RENAME TABLE [db11.]name11 TO [db12.]name12, [db21.]name21 TO [db22.]name22, ... [ON CLUSTER cluster]
```

所有表都在全局锁定下重命名。 重命名表是一个轻型操作。 如果您在TO之后指定了另一个数据库，则表将被移动到此数据库。 但是，包含数据库的目录必须位于同一文件系统中（否则，将返回错误）。

## SET {#query-set}

``` sql
SET param = value
```

分配 `value` 到 `param` [设置](../../operations/settings/index.md) 对于当前会话。 你不能改变 [服务器设置](../../operations/server-configuration-parameters/index.md) 这边

您还可以在单个查询中设置指定设置配置文件中的所有值。

``` sql
SET profile = 'profile-name-from-the-settings-file'
```

有关详细信息，请参阅 [设置](../../operations/settings/settings.md).

## SET ROLE {#set-role-statement}

激活当前用户的角色。

### 语法 {#set-role-syntax}

``` sql
SET ROLE {DEFAULT | NONE | role [,...] | ALL | ALL EXCEPT role [,...]}
```

## SET DEFAULT ROLE {#set-default-role-statement}

将默认角色设置为用户。

默认角色在用户登录时自动激活。 您只能将以前授予的角色设置为默认值。 如果未向用户授予角色，ClickHouse将引发异常。

### 语法 {#set-default-role-syntax}

``` sql
SET DEFAULT ROLE {NONE | role [,...] | ALL | ALL EXCEPT role [,...]} TO {user|CURRENT_USER} [,...]
```

### 例 {#set-default-role-examples}

为用户设置多个默认角色:

``` sql
SET DEFAULT ROLE role1, role2, ... TO user
```

将所有授予的角色设置为用户的默认值:

``` sql
SET DEFAULT ROLE ALL TO user
```

从用户清除默认角色:

``` sql
SET DEFAULT ROLE NONE TO user
```

将所有授予的角色设置为默认角色，其中一些角色除外:

``` sql
SET DEFAULT ROLE ALL EXCEPT role1, role2 TO user
```

## TRUNCATE {#truncate-statement}

``` sql
TRUNCATE TABLE [IF EXISTS] [db.]name [ON CLUSTER cluster]
```

从表中删除所有数据。 当条款 `IF EXISTS` 如果该表不存在，则查询返回错误。

该 `TRUNCATE` 查询不支持 [查看](../../engines/table-engines/special/view.md), [文件](../../engines/table-engines/special/file.md), [URL](../../engines/table-engines/special/url.md) 和 [Null](../../engines/table-engines/special/null.md) 表引擎.

## USE {#use}

``` sql
USE db
```

用于设置会话的当前数据库。
当前数据库用于搜索表，如果数据库没有在查询中明确定义与表名之前的点。
使用HTTP协议时无法进行此查询，因为没有会话的概念。

[原始文章](https://clickhouse.tech/docs/en/query_language/misc/) <!--hide-->
