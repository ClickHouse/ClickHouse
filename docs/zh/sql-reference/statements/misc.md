---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 41
toc_title: "\u5176\u4ED6"
---

# 杂项查询 {#miscellaneous-queries}

## ATTACH {#attach}

与`CREATE`类似，但有所区别

-   使用关键词 `ATTACH`
-   查询不会在磁盘上创建数据。但会假定数据已经在对应位置存放，同时将与表相关的信息添加到服务器。
    执行 `ATTACH` 查询后，服务器将知道表已经被创建。

如果表之前已分离 (`DETACH`），意味着其结构是已知的，可以使用简要的写法来建立表，即不需要定义表结构的Schema细节。

``` sql
ATTACH TABLE [IF NOT EXISTS] [db.]name [ON CLUSTER cluster]
```

启动服务器时会自动触发此查询。 

服务器将表的元数据作为文件存储 `ATTACH` 查询，它只是在启动时运行。有些表例外，如系统表，它们是在服务器上显式指定的。

## CHECK TABLE {#check-table}

检查表中的数据是否已损坏。

``` sql
CHECK TABLE [db.]name
```

`CHECK TABLE` 查询会比较存储在服务器上的实际文件大小与预期值。 如果文件大小与存储的值不匹配，则表示数据已损坏。 例如，这可能是由查询执行期间的系统崩溃引起的。

查询返回一行结果，列名为 `result`, 该行的值为 [布尔值](../../sql-reference/data-types/boolean.md) 类型:

-   0-表中的数据已损坏；
-   1-数据保持完整性；

该 `CHECK TABLE` 查询支持下表引擎:

-   [Log](../../engines/table-engines/log-family/log.md)
-   [TinyLog](../../engines/table-engines/log-family/tinylog.md)
-   [StripeLog](../../engines/table-engines/log-family/stripelog.md)
-   [MergeTree 家族](../../engines/table-engines/mergetree-family/mergetree.md)

对其他不支持的表引擎的表执行会导致异常。

来自 `*Log` 家族的引擎不提供故障自动数据恢复。 使用 `CHECK TABLE` 查询及时跟踪数据丢失。

对于 `MergeTree` 家族引擎， `CHECK TABLE` 查询显示本地服务器上表的每个单独数据部分的检查状态。

**如果数据已损坏**

如果表已损坏，则可以将未损坏的数据复制到另一个表。 要做到这一点:

1.  创建一个与损坏的表结构相同的新表。 请执行查询 `CREATE TABLE <new_table_name> AS <damaged_table_name>`.
2.  将 [max_threads](../../operations/settings/settings.md#settings-max_threads) 值设置为1，以在单个线程中处理下一个查询。 要这样做，请运行查询 `SET max_threads = 1`.
3.  执行查询 `INSERT INTO <new_table_name> SELECT * FROM <damaged_table_name>`. 此请求将未损坏的数据从损坏的表复制到另一个表。 只有损坏部分之前的数据才会被复制。
4.  重新启动 `clickhouse-client` 以重置 `max_threads` 值。

## DESCRIBE TABLE {#misc-describe-table}

查看表的描述信息，返回各列的Schema，语法如下：
``` sql
DESC|DESCRIBE TABLE [db.]table [INTO OUTFILE filename] [FORMAT format]
```

返回以下 `String` 类型列:

-   `name` — 列名。
-   `type`— 列的类型。
-   `default_type` — [默认表达式](create.md#create-default-values) (`DEFAULT`, `MATERIALIZED` 或 `ALIAS`)中使用的子句。 如果没有指定默认表达式，则列包含一个空字符串。
-   `default_expression` — `DEFAULT` 子句中指定的值。
-   `comment_expression` — 注释信息。

嵌套数据结构以 “expanded” 格式输出。 每列分别显示，列名后加点号。

## DETACH {#detach}

从服务器中删除目标表信息（删除对象是表), 执行查询后,服务器视作该表已经不存在。

``` sql
DETACH TABLE [IF EXISTS] [db.]name [ON CLUSTER cluster]
```

这不会删除表的数据或元数据。 在下一次服务器启动时，服务器将读取元数据并再次查找该表。
也可以不停止服务器的情况下，使用前面介绍的 `ATTACH` 查询来重新关联该表（系统表除外，没有为它们存储元数据）。

## DROP {#drop}

删除已经存在的实体。如果指定 `IF EXISTS`， 则如果实体不存在，则不返回错误。
建议使用时添加 `IF EXISTS` 修饰符。

## DROP DATABASE {#drop-database}

删除 `db` 数据库中的所有表，然后删除 `db` 数据库本身。

语法:

``` sql
DROP DATABASE [IF EXISTS] db [ON CLUSTER cluster]
```
## DROP TABLE {#drop-table}

删除表。

语法:

``` sql
DROP [TEMPORARY] TABLE [IF EXISTS] [db.]name [ON CLUSTER cluster]
```

## DROP DICTIONARY {#drop-dictionary}

删除字典。

语法:

``` sql
DROP DICTIONARY [IF EXISTS] [db.]name
```

## DROP USER {#drop-user-statement}

删除用户。

语法:

``` sql
DROP USER [IF EXISTS] name [,...] [ON CLUSTER cluster_name]
```

## DROP ROLE {#drop-role-statement}

删除角色。

同时该角色所拥有的权限也会被收回。

语法:

``` sql
DROP ROLE [IF EXISTS] name [,...] [ON CLUSTER cluster_name]
```

## DROP ROW POLICY {#drop-row-policy-statement}

删除行策略。

已删除行策略将从分配该策略的所有实体撤销。

语法:

``` sql
DROP [ROW] POLICY [IF EXISTS] name [,...] ON [database.]table [,...] [ON CLUSTER cluster_name]
```

## DROP QUOTA {#drop-quota-statement}

删除配额。

已删除的配额将从分配该配额的所有实体撤销。

语法:

``` sql
DROP QUOTA [IF EXISTS] name [,...] [ON CLUSTER cluster_name]
```

## DROP SETTINGS PROFILE {#drop-settings-profile-statement}

删除settings配置。

已删除的settings配置将从分配该settings配置的所有实体撤销。

语法:

``` sql
DROP [SETTINGS] PROFILE [IF EXISTS] name [,...] [ON CLUSTER cluster_name]
```

## DROP VIEW {#drop-view}

删除视图。视图也可以通过 `DROP TABLE` 删除，但是 `DROP VIEW` 检查 `[db.]name` 是视图。

语法:

``` sql
DROP VIEW [IF EXISTS] [db.]name [ON CLUSTER cluster]
```

## EXISTS {#exists-statement}

``` sql
EXISTS [TEMPORARY] [TABLE|DICTIONARY] [db.]name [INTO OUTFILE filename] [FORMAT format]
```

返回单个 `UInt8` 类型的列，其中包含单个值 `0` 如果表或数据库不存在，或 `1` 如果该表存在于指定的数据库中。

## KILL QUERY {#kill-query-statement}



``` sql
KILL QUERY [ON CLUSTER cluster]
  WHERE <where expression to SELECT FROM system.processes query>
  [SYNC|ASYNC|TEST]
  [FORMAT format]
```

尝试强制终止当前正在运行的查询。
要终止的查询是使用 `KILL` 查询的 `WHERE` 子句定义的标准从system.processes表中选择的。

例:

``` sql
-- Forcibly terminates all queries with the specified query_id:
KILL QUERY WHERE query_id='2-857d-4a57-9ee0-327da5d60a90'

-- Synchronously terminates all queries run by 'username':
KILL QUERY WHERE user='username' SYNC
```

只读用户只能停止自己提交的查询。

默认情况下，使用异步版本的查询 (`ASYNC`），不需要等待确认查询已停止。

而相对的，终止同步版本 (`SYNC`）的查询会显示每步停止时间。

返回信息包含 `kill_status` 列，该列可以采用以下值:

1.  ‘finished’ – 查询已成功终止。
2.  ‘waiting’ – 发送查询信号终止后，等待查询结束。
3.  其他值，会解释为什么查询不能停止。

测试查询 (`TEST`）仅检查用户的权限，并显示要停止的查询列表。

## KILL MUTATION {#kill-mutation}

``` sql
KILL MUTATION [ON CLUSTER cluster]
  WHERE <where expression to SELECT FROM system.mutations query>
  [TEST]
  [FORMAT format]
```

尝试取消和删除当前正在执行的 [mutations](alter.md#alter-mutations) 。 要取消的mutation是使用 `KILL` 查询的WHERE子句指定的过滤器从[`system.mutations`](../../operations/system-tables/mutations.md#system_tables-mutations) 表中选择的。

测试查询 (`TEST`）仅检查用户的权限并显示要停止的mutations列表。

例:

``` sql
-- Cancel and remove all mutations of the single table:
KILL MUTATION WHERE database = 'default' AND table = 'table'

-- Cancel the specific mutation:
KILL MUTATION WHERE database = 'default' AND table = 'table' AND mutation_id = 'mutation_3.txt'
```

当mutation卡住且无法完成时，该查询是有用的(例如，当mutation查询中的某些函数在应用于表中包含的数据时抛出异常)。

Mutation已经做出的更改不会回滚。

## OPTIMIZE {#misc_operations-optimize}

``` sql
OPTIMIZE TABLE [db.]name [ON CLUSTER cluster] [PARTITION partition | PARTITION ID 'partition_id'] [FINAL] [DEDUPLICATE]
```

此查询尝试初始化 [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md)家族的表引擎的表中未计划合并数据部分。

该 `OPTMIZE` 查询也支持 [MaterializedView](../../engines/table-engines/special/materializedview.md) 和 [Buffer](../../engines/table-engines/special/buffer.md) 引擎。 不支持其他表引擎。

当 `OPTIMIZE` 与 [ReplicatedMergeTree](../../engines/table-engines/mergetree-family/replication.md) 家族的表引擎一起使用时，ClickHouse将创建一个合并任务，并等待所有节点上的执行（如果 `replication_alter_partitions_sync` 设置已启用）。

-   如果 `OPTIMIZE` 出于任何原因不执行合并，它不通知客户端。 要启用通知，请使用 [optimize_throw_if_noop](../../operations/settings/settings.md#setting-optimize_throw_if_noop) 设置。
-   如果您指定 `PARTITION`，仅优化指定的分区。 [如何设置分区表达式](alter.md#alter-how-to-specify-part-expr).
-   如果您指定 `FINAL`，即使所有数据已经在一个部分中，也会执行优化。
-   如果您指定 `DEDUPLICATE`，则将对完全相同的行进行重复数据删除（所有列进行比较），这仅适用于MergeTree引擎。

!!! warning "警告"
    `OPTIMIZE` 无法修复 “Too many parts” 错误。

## RENAME {#misc_operations-rename}

重命名一个或多个表。

``` sql
RENAME TABLE [db11.]name11 TO [db12.]name12, [db21.]name21 TO [db22.]name22, ... [ON CLUSTER cluster]
```

所有表都在全局锁定下重命名。 重命名表是一个轻型操作。 如果您在TO之后指定了另一个数据库，则表将被移动到此数据库。 但是，包含数据库的目录必须位于同一文件系统中（否则，将返回错误）。
如果您在一个查询中重命名多个表，这是一个非原子操作，它可能被部分执行，其他会话中的查询可能会接收错误 Table ... doesn't exist ...。

## SET {#query-set}

``` sql
SET param = value
```

为当前会话的 [设置](../../operations/settings/index.md) `param` 分配值 `value`。 您不能以这种方式更改 [服务器设置](../../operations/server-configuration-parameters/index.md)。

您还可以在单个查询中从指定的设置配置文件中设置所有值。

``` sql
SET profile = 'profile-name-from-the-settings-file'
```

有关详细信息，请参阅 [设置](../../operations/settings/settings.md).

## SET ROLE {#set-role-statement}

激活当前用户的角色。

``` sql
SET ROLE {DEFAULT | NONE | role [,...] | ALL | ALL EXCEPT role [,...]}
```

## SET DEFAULT ROLE {#set-default-role-statement}

将默认角色设置为用户。

默认角色在用户登录时自动激活。 您只能将以前授予的角色设置为默认值。 如果角色没有授予用户，ClickHouse会抛出异常。

``` sql
SET DEFAULT ROLE {NONE | role [,...] | ALL | ALL EXCEPT role [,...]} TO {user|CURRENT_USER} [,...]
```

### 示例 {#set-default-role-examples}

为用户设置多个默认角色:

``` sql
SET DEFAULT ROLE role1, role2, ... TO user
```

将所有授予的角色设置为用户的默认角色:

``` sql
SET DEFAULT ROLE ALL TO user
```

清除用户的默认角色:

``` sql
SET DEFAULT ROLE NONE TO user
```

将所有授予的角色设置为默认角色，但其中一些角色除外:

``` sql
SET DEFAULT ROLE ALL EXCEPT role1, role2 TO user
```

## TRUNCATE {#truncate-statement}

``` sql
TRUNCATE TABLE [IF EXISTS] [db.]name [ON CLUSTER cluster]
```

从表中删除所有数据。 当省略 `IF EXISTS`子句时，如果该表不存在，则查询返回错误。

该 `TRUNCATE` 查询不支持 [View](../../engines/table-engines/special/view.md), [File](../../engines/table-engines/special/file.md), [URL](../../engines/table-engines/special/url.md) 和 [Null](../../engines/table-engines/special/null.md) 表引擎.

## USE {#use}

``` sql
USE db
```

用于设置会话的当前数据库。
当前数据库用于搜索表，如果数据库没有在查询中明确定义与表名之前的点。
使用HTTP协议时无法进行此查询，因为没有会话的概念。

[原始文章](https://clickhouse.tech/docs/en/query_language/misc/) <!--hide-->
