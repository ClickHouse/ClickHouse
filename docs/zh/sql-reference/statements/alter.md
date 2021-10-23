---
toc_priority: 36
toc_title: ALTER
---

## ALTER {#query_language_queries_alter}

 `ALTER` 仅支持 `*MergeTree` ，`Merge`以及`Distributed`等引擎表。
 该操作有多种形式。

### 列操作 {#column-manipulations}

改变表结构：

``` sql
ALTER TABLE [db].name [ON CLUSTER cluster] ADD|DROP|CLEAR|COMMENT|MODIFY COLUMN ...
```

在语句中，配置一个或多个用逗号分隔的动作。每个动作是对某个列实施的操作行为。

支持下列动作：

-   [ADD COLUMN](#alter_add-column) — 添加列
-   [DROP COLUMN](#alter_drop-column) — 删除列
-   [CLEAR COLUMN](#alter_clear-column) — 重置列的值
-   [COMMENT COLUMN](#alter_comment-column) — 给列增加注释说明
-   [MODIFY COLUMN](#alter_modify-column) — 改变列的值类型，默认表达式以及TTL

这些动作将在下文中进行详述。

#### 增加列 {#alter_add-column}

``` sql
ADD COLUMN [IF NOT EXISTS] name [type] [default_expr] [codec] [AFTER name_after]
```

使用指定的`name`, `type`, [`codec`](../../sql-reference/statements/create.md#codecs) 以及 `default_expr` (请参见 [Default expressions](../../sql-reference/statements/create.md#create-default-values))，往表中增加新的列。


如果sql中包含 `IF NOT EXISTS` ，执行语句时如果列已经存在，CH不会报错。如果指定`AFTER name_after`（表中另一个列的名称），则新的列会加在指定列的后面。否则，新的列将被添加到表的末尾。注意，不能将新的列添加到表的开始位置， `name_after` 可以是执行该动作时已经在表中存在的任意列。

添加列仅仅是改变原有表的结构不会对已有数据产生影响。执行完 `ALTER`后磁盘中也不会出现新的数据。如果查询表时列的数据为空，那么CH会使用列的默认值来进行填充（如果有默认表达式，则使用这个；或者用0或空字符串）。当数据块完成合并(参见[MergeTree](../../engines/table-engines/mergetree-family/mergetree.md))后，磁盘中会出现该列的数据。

这种方式允许 `ALTER` 语句能马上执行。不需要增加原有数据的大小。

示例:

``` sql
ALTER TABLE visits ADD COLUMN browser String AFTER user_id
```

#### 删除列 {#alter_drop-column}

``` sql
DROP COLUMN [IF EXISTS] name
```

通过指定 `name`删除列。如果语句包含 `IF EXISTS`，执行时遇到不存在的列也不会报错。

从文件系统中删除数据。由于是删除列的整个文件，该语句几乎是立即执行完成的。


示例:

``` sql
ALTER TABLE visits DROP COLUMN browser
```

#### 清空列 {#alter_clear-column}

``` sql
CLEAR COLUMN [IF EXISTS] name IN PARTITION partition_name
```

重置指定分区中列的值。 分区名称 `partition_name` 请参见 [怎样设置分区表达式](#alter-how-to-specify-part-expr)

如果语句中包含 `IF EXISTS` ，遇到不存在的列，sql执行不会报错。


示例:

``` sql
ALTER TABLE visits CLEAR COLUMN browser IN PARTITION tuple()
```

#### 增加注释 {#alter_comment-column}

``` sql
COMMENT COLUMN [IF EXISTS] name 'comment'
```

给列增加注释说明。如果语句中包含 `IF EXISTS` ，遇到不存在的列，sql执行不会报错。

每个列都可以包含注释。如果列的注释已经存在，新的注释会替换旧的。
注释信息保存在 [DESCRIBE TABLE](../../sql-reference/statements/misc.md#misc-describe-table)查询的 `comment_expression` 字段中。

示例:

``` sql
ALTER TABLE visits COMMENT COLUMN browser 'The table shows the browser used for accessing the site.'
```

#### 修改列 {#alter_modify-column}

``` sql
MODIFY COLUMN [IF EXISTS] name [type] [default_expr] [TTL]
```
该语句可以改变 `name` 列的属性：

-   Type

-   Default expression

-   TTL

有关修改列TTL的示例，请参见 [Column TTL](../../engines/table-engines/mergetree-family/mergetree.md#mergetree-column-ttl).

如果语句中包含 `IF EXISTS` ，遇到不存在的列，sql执行不会报错。

当改变列的类型时，列的值也被转换了，如同对列使用 [toType](../../sql-reference/functions/type-conversion-functions.md)函数一样。如果只改变了默认表达式，该语句几乎不会做任何复杂操作，并且几乎是立即执行完成的。

示例:

``` sql
ALTER TABLE visits MODIFY COLUMN browser Array(String)
```

改变列的类型是唯一的复杂型动作 - 它改变了数据文件的内容。对于大型表，执行起来要花费较长的时间。
该操作分为如下处理步骤：

-   为修改的数据准备新的临时文件
-   重命名原来的文件
-   将新的临时文件改名为原来的数据文件名
-   删除原来的文件

仅仅在第一步是耗费时间的。如果该阶段执行失败，那么数据没有变化。如果执行后续的步骤中失败了，数据可以手动恢复。例外的情形是，当原来的文件从文件系统中被删除了，但是新的数据没有写入到临时文件中并且丢失了。


列操作的 `ALTER`行为是可以被复制的。这些指令会保存在ZooKeeper中，这样每个副本节点都能执行它们。所有的 `ALTER` 将按相同的顺序执行。
 The query waits for the appropriate actions to be completed on the other replicas.
然而，改变可复制表的列是可以被中断的，并且所有动作都以异步方式执行。


#### ALTER 操作限制 {#alter-query-limitations}

 `ALTER` 操作允许在嵌套的数据结构中创建和删除单独的元素（列），但是不是整个嵌套结构。添加一个嵌套数据结构的列时，你可以用类似这样的名称 `name.nested_name` 及类型 `Array(T)` 来操作。嵌套数据结构等同于
列名前带有同样前缀的多个数组列。


不支持对primary key或者sampling key中的列（在 `ENGINE` 表达式中用到的列）进行删除操作。改变包含在primary key中的列的类型时，如果操作不会导致数据的变化（例如，往Enum中添加一个值，或者将`DateTime` 类型改成 `UInt32`），那么这种操作是可行的。

如果 `ALTER` 操作不足以完成你想要的表变动操作，你可以创建一张新的表，通过 [INSERT SELECT](../../sql-reference/statements/insert-into.md#insert_query_insert-select)将数据拷贝进去，然后通过  [RENAME](../../sql-reference/statements/misc.md#misc_operations-rename)将新的表改成和原有表一样的名称，并删除原有的表。你可以使用 [clickhouse-copier](../../operations/utilities/clickhouse-copier.md) 代替 `INSERT SELECT`。

 `ALTER` 操作会阻塞对表的所有读写操作。换句话说，当一个大的 `SELECT` 语句和 `ALTER`同时执行时，`ALTER`会等待，直到 `SELECT` 执行结束。与此同时，当 `ALTER` 运行时，新的 sql 语句将会等待。


对于不存储数据的表（例如 `Merge` 及 `Distributed` 表）， `ALTER` 仅仅改变了自身的表结构，不会改变从属的表结构。例如，对 `Distributed` 表执行 ALTER 操作时，需要对其它包含该表的服务器执行该操作。

### key表达式的修改 {#manipulations-with-key-expressions}

支持下列表达式：

``` sql
MODIFY ORDER BY new_expression
```

该操作仅支持 [`MergeTree`](../../engines/table-engines/mergetree-family/mergetree.md) 系列表 (含 [replicated](../../engines/table-engines/mergetree-family/replication.md) 表)。它会将表的 [排序键](../../engines/table-engines/mergetree-family/mergetree.md)变成 `new_expression` (元组表达式)。主键仍保持不变。

该操作是轻量级的，仅会改变元数据。


### 跳过索引来更改数据 {#manipulations-with-data-skipping-indices}

该操作仅支持 [`MergeTree`](../../engines/table-engines/mergetree-family/mergetree.md) 系列表 (含 [replicated](../../engines/table-engines/mergetree-family/replication.md) 表)。
下列操作是允许的：

-   `ALTER TABLE [db].name ADD INDEX name expression TYPE type GRANULARITY value [FIRST|AFTER name]` - 在表的元数据中增加索引说明

-   `ALTER TABLE [db].name DROP INDEX name` - 从表的元数据中删除索引描述，并从磁盘上删除索引文件

由于只改变表的元数据或者删除文件，因此该操作是轻量级的，也可以被复制到其它节点（通过Zookeeper同步索引元数据）

### 更改约束 {#manipulations-with-constraints}

参见 [constraints](../../sql-reference/statements/create.md#constraints)查看更多信息。

通过下面的语法，可以添加或删除约束：

``` sql
ALTER TABLE [db].name ADD CONSTRAINT constraint_name CHECK expression;
ALTER TABLE [db].name DROP CONSTRAINT constraint_name;
```

上述语句会从表中增加或删除约束的元数据，因此会被立即处理。
对已有数据的约束检查 *将不会执行* 。

对可复制表的操作可通过Zookeeper传播到其它副本节点。

### 更改分区及文件块 {#alter_manipulations-with-partitions}

允许进行下列关于 [partitions](../../engines/table-engines/mergetree-family/custom-partitioning-key.md) 的操作：

-   [DETACH PARTITION](#alter_detach-partition) — 将分区数据移动到 `detached` ，并且忘记它
-   [DROP PARTITION](#alter_drop-partition) — 删除一个partition.
-   [ATTACH PART\|PARTITION](#alter_attach-partition) — 将`detached` 目录中的分区重新添加到表中.
-   [ATTACH PARTITION FROM](#alter_attach-partition-from) —  从表中复制数据分区到另一张表，并添加分区
-   [REPLACE PARTITION](#alter_replace-partition) — 从表中复制数据分区到其它表及副本
-   [MOVE PARTITION TO TABLE](#alter_move_to_table-partition) —   从表中复制数据分区到其它表.
-   [CLEAR COLUMN IN PARTITION](#alter_clear-column-partition) —  重置分区中某个列的值
-   [CLEAR INDEX IN PARTITION](#alter_clear-index-partition) —  重置分区中指定的二级索引
-   [FREEZE PARTITION](#alter_freeze-partition) —  创建分区的备份 
-   [FETCH PARTITION](#alter_fetch-partition) —  从其它服务器上下载分
-   [MOVE PARTITION\|PART](#alter_move-partition) —  将分区/数据块移动到另外的磁盘/卷

<!-- -->

#### 分区剥离 {#alter_detach-partition}

``` sql
ALTER TABLE table_name DETACH PARTITION partition_expr
```
将指定分区的数据移动到 `detached` 目录。服务器会忽略被分离的数据分区。只有当你使用 [ATTACH](#alter_attach-partition) 时，服务器才会知晓这部分数据。

示例:

``` sql
ALTER TABLE visits DETACH PARTITION 201901
```
从 [如何设置分区表达式](#alter-how-to-specify-part-expr)章节中获取分区表达式的设置说明。

当执行操作以后，可以对 `detached` 目录的数据进行任意操作，例如删除文件，或者放着不管。

该操作是可以复制的，它会将所有副本节点上的数据移动到 `detached` 目录。注意仅能在副本的leader节点上执行该操作。想了解副本是否是leader节点，需要在 [system.replicas](../../operations/system-tables/replicas.md#system_tables-replicas) 表执行 `SELECT` 操作。或者，可以很方便的在所有副本节点上执行 `DETACH`操作，但除leader外其它的副本节点会抛出异常。


#### 删除分区 {#alter_drop-partition}

``` sql
ALTER TABLE table_name DROP PARTITION partition_expr
```

从表中删除指定分区。该操作会将分区标记为不活跃的，然后在大约10分钟内删除全部数据。

在 [如何设置分区表达式](#alter-how-to-specify-part-expr)中获取分区表达式的设置说明。
该操作是可复制的，副本节点的数据也将被删除。


#### 删除已剥离的分区\|数据块 {#alter_drop-detached}

``` sql
ALTER TABLE table_name DROP DETACHED PARTITION|PART partition_expr
```

从`detached`目录中删除指定分区的特定部分或所有数据。访问 [如何设置分区表达式](#alter-how-to-specify-part-expr)可获取设置分区表达式的详细信息。

#### 关联分区\|数据块 {#alter_attach-partition}

``` sql
ALTER TABLE table_name ATTACH PARTITION|PART partition_expr
```
从`detached`目录中添加数据到数据表。可以添加整个分区的数据，或者单独的数据块。例如：

``` sql
ALTER TABLE visits ATTACH PARTITION 201901;
ALTER TABLE visits ATTACH PART 201901_2_2_0;
```

访问 [如何设置分区表达式](#alter-how-to-specify-part-expr)可获取设置分区表达式的详细信息。

该操作是可以复制的。副本启动器检查 `detached`目录是否有数据。如果有，该操作会检查数据的完整性。如果一切正常，该操作将数据添加到表中。其它副本节点通过副本启动器下载这些数据。

因此可以在某个副本上将数据放到 `detached`目录，然后通过 `ALTER ... ATTACH` 操作将这部分数据添加到该表的所有副本。

#### 从...关联分区 {#alter_attach-partition-from}

``` sql
ALTER TABLE table2 ATTACH PARTITION partition_expr FROM table1
```
该操作将 `table1` 表的数据分区复制到 `table2` 表的已有分区。注意`table1`表的数据不会被删除。

为保证该操作能成功运行，下列条件必须满足：

-   2张表必须有相同的结构
-    2张表必须有相同的分区键

#### 替换分区 {#alter_replace-partition}

``` sql
ALTER TABLE table2 REPLACE PARTITION partition_expr FROM table1
```
该操作将 `table1` 表的数据分区复制到 `table2`表，并替换 `table2`表的已有分区。注意`table1`表的数据不会被删除。

为保证该操作能成功运行，下列条件必须满足：

-   2张表必须有相同的结构
-   2张表必须有相同的分区键

#### 将分区移动到表 {#alter_move_to_table-partition}

``` sql
ALTER TABLE table_source MOVE PARTITION partition_expr TO TABLE table_dest
```

该操作将 `table_source`表的数据分区移动到 `table_dest`表，并删除`table_source`表的数据。

为保证该操作能成功运行，下列条件必须满足：

-   2张表必须有相同的结构
-   2张表必须有相同的分区键
-   2张表必须属于相同的引擎系列（可复制表或不可复制表）
-   2张表必须有相同的存储方式

#### 清空分区的列 {#alter_clear-column-partition}

``` sql
ALTER TABLE table_name CLEAR COLUMN column_name IN PARTITION partition_expr
```

重置指定分区的特定列的值。如果建表时使用了 `DEFAULT` 语句，该操作会将列的值重置为该默认值。

示例:

``` sql
ALTER TABLE visits CLEAR COLUMN hour in PARTITION 201902
```

#### 冻结分区 {#alter_freeze-partition}

``` sql
ALTER TABLE table_name FREEZE [PARTITION partition_expr]
```

该操作为指定分区创建一个本地备份。如果 `PARTITION` 语句省略，该操作会一次性为所有分区创建备份。

!!! 注意 "Note"
    整个备份过程不需要停止服务

注意对于老式的表，可以指定分区名前缀（例如，‘2019’），然后该操作会创建所有对应分区的备份。访问 [如何设置分区表达式](#alter-how-to-specify-part-expr)可获取设置分区表达式的详细信息。

在执行操作的同时，对于数据快照，该操作会创建到表数据的硬链接。硬链接放置在  `/var/lib/clickhouse/shadow/N/...`，也就是：
-   `/var/lib/clickhouse/` 服务器配置文件中指定的CH工作目录
-   `N` 备份的增长序号


!!! 注意 "Note"
    如果你使用 [多个磁盘存储数据表](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes)，
    那么每个磁盘上都有 `shadow/N`目录，用来保存`PARTITION` 表达式对应的数据块。

备份内部也会创建和 `/var/lib/clickhouse/` 内部一样的目录结构。该操作在所有文件上执行‘chmod’，禁止往里写入数据

当备份创建完毕，你可以从 `/var/lib/clickhouse/shadow/`复制数据到远端服务器，然后删除本地数据。注意 `ALTER t FREEZE PARTITION`操作是不能复制的，它仅在本地服务器上创建本地备份。

该操作创建备份几乎是即时的（但是首先它会等待相关表的当前操作执行完成）


`ALTER TABLE t FREEZE PARTITION` 仅仅复制数据, 而不是元数据信息. 要复制表的元数据信息, 拷贝这个文件 `/var/lib/clickhouse/metadata/database/table.sql`

从备份中恢复数据，按如下步骤操作：
1. 如果表不存在，先创建。 查看.sql 文件获取执行语句 (将`ATTACH` 替换成 `CREATE`).
2. 从 备份的 `data/database/table/`目录中将数据复制到 `/var/lib/clickhouse/data/database/table/detached/`目录
3. 运行 `ALTER TABLE t ATTACH PARTITION`操作，将数据添加到表中

恢复数据不需要停止服务进程。
想了解备份及数据恢复的更多信息，请参见 [数据备份](../../operations/backup.md) 。

#### 删除分区的索引 {#alter_clear-index-partition}

``` sql
ALTER TABLE table_name CLEAR INDEX index_name IN PARTITION partition_expr
```

该操作和 `CLEAR COLUMN`类似，但是它重置的是索引而不是列的数据。

#### 获取分区 {#alter_fetch-partition}

``` sql
ALTER TABLE table_name FETCH PARTITION partition_expr FROM 'path-in-zookeeper'
```

从另一服务器上下载分区数据。仅支持可复制引擎表。
该操作做了如下步骤：
1.  从指定数据分片上下载分区。在 path-in-zookeeper 这一参数你必须设置Zookeeper中该分片的path值。
2.  然后将已下载的数据放到 `table_name` 表的 `detached` 目录下。通过 [ATTACH PARTITION\|PART](#alter_attach-partition)将数据加载到表中。 

示例:

``` sql
ALTER TABLE users FETCH PARTITION 201902 FROM '/clickhouse/tables/01-01/visits';
ALTER TABLE users ATTACH PARTITION 201902;
```

注意:

-   `ALTER ... FETCH PARTITION` 操作不支持复制，它仅在本地服务器上将分区移动到 `detached`目录。
-   `ALTER TABLE ... ATTACH`操作是可复制的。它将数据添加到所有副本。数据从某个副本的`detached` 目录中添加进来，然后添加到邻近的副本

在开始下载之前，系统检查分区是否存在以及和表结构是否匹配。然后从健康的副本集中自动选择最合适的副本。

虽然操作叫做 `ALTER TABLE`，但是它并不能改变表结构，也不会立即改变表中可用的数据。

#### 移动分区\|数据块 {#alter_move-partition}

将 `MergeTree`引擎表的分区或数据块移动到另外的卷/磁盘中。参见 [使用多个块设备存储数据](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes)

``` sql
ALTER TABLE table_name MOVE PARTITION|PART partition_expr TO DISK|VOLUME 'disk_name'
```

 `ALTER TABLE t MOVE` 操作:

-   不支持复制，因为不同副本可以有不同的存储方式
-   如果指定的磁盘或卷没有配置，返回错误。如果存储方式中设定的数据移动条件不能满足，该操作同样报错。
-   这种情况也会报错：即将移动的数据已经由后台进程在进行移动操作时，并行的 `ALTER TABLE t MOVE`操作或者作为后台数据合并的结果。这种情形下用户不能任何额外的动作。

示例:

``` sql
ALTER TABLE hits MOVE PART '20190301_14343_16206_438' TO VOLUME 'slow'
ALTER TABLE hits MOVE PARTITION '2019-09-01' TO DISK 'fast_ssd'
```

#### 如何设置分区表达式 {#alter-how-to-specify-part-expr}

通过不同方式在 `ALTER ... PARTITION` 操作中设置分区表达式：

-   `system.parts`表  `partition`列的某个值，例如， `ALTER TABLE visits DETACH PARTITION 201901`
-   表的列表达式。支持常量及常量表达式。例如， `ALTER TABLE visits DETACH PARTITION toYYYYMM(toDate('2019-01-25'))`
-   使用分区ID。分区ID是字符串变量（可能的话有较好的可读性），在文件系统和ZooKeeper中作为分区名称。分区ID必须配置在 `PARTITION ID`中，用单引号包含，例如， `ALTER TABLE visits DETACH PARTITION ID '201901'`
-   在 [ALTER ATTACH PART](#alter_attach-partition) 和 [DROP DETACHED PART](#alter_drop-detached) 操作中，要配置块的名称，使用 [system.detached_parts](../../operations/system-tables/detached_parts.md#system_tables-detached_parts)表中 `name`列的字符串值，例如： `ALTER TABLE visits ATTACH PART '201901_1_1_0'`


设置分区时，引号使用要看分区表达式的类型。例如，对于 `String`类型，需要设置用引号(`'`)包含的名称。对于 `Date` 和 `Int*`引号就不需要了。
对于老式的表，可以用数值`201901` 或字符串 `'201901'`来设置分区。新式的表语法严格和类型一致（类似于VALUES输入的解析）

上述所有规则同样适用于 [OPTIMIZE](../../sql-reference/statements/misc.md#misc_operations-optimize) 操作。在对未分区的表进行 OPTIMIZE 操作时，如果需要指定唯一的分区，这样设置表达式`PARTITION tuple()`。例如：

``` sql
OPTIMIZE TABLE table_not_partitioned PARTITION tuple() FINAL;
```

 `ALTER ... PARTITION` 操作的示例在 [`00502_custom_partitioning_local`](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00502_custom_partitioning_local.sql) 和 [`00502_custom_partitioning_replicated_zookeeper`](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00502_custom_partitioning_replicated_zookeeper.sql) 提供了演示。

### 更改表的TTL {#manipulations-with-table-ttl}

通过以下形式的请求可以修改 [table TTL](../../engines/table-engines/mergetree-family/mergetree.md#mergetree-table-ttl)

``` sql
ALTER TABLE table-name MODIFY TTL ttl-expression
```

### ALTER操作的同步性 {#synchronicity-of-alter-queries}

对于不可复制的表，所有 `ALTER`操作都是同步执行的。对于可复制的表，ALTER操作会将指令添加到ZooKeeper中，然后会尽快的执行它们。然而，该操作可以等待其它所有副本将指令执行完毕。

对于 `ALTER ... ATTACH|DETACH|DROP`操作，可以通过设置 `replication_alter_partitions_sync` 来启用等待。可用参数值： `0` – 不需要等待; `1` – 仅等待自己执行(默认); `2` – 等待所有节点

### Mutations {#alter-mutations}

Mutations是一类允许对表的行记录进行删除或更新的ALTER操作。相较于标准的 `UPDATE` 和 `DELETE` 用于少量行操作而言，Mutations用来对表的很多行进行重量级的操作。该操作支持 `MergeTree`系列表，包含支持复制功能的表。

已有的表已经支持mutations操作（不需要转换）。但是在首次对表进行mutation操作以后，它的元数据格式变得和和之前的版本不兼容，并且不能回退到之前版本。

目前可用的命令:

``` sql
ALTER TABLE [db.]table DELETE WHERE filter_expr
```
`filter_expr`必须是 `UInt8`型。该操作将删除表中 `filter_expr`表达式值为非0的列

``` sql
ALTER TABLE [db.]table UPDATE column1 = expr1 [, ...] WHERE filter_expr
```
`filter_expr`必须是 `UInt8`型。该操作将更新表中各行 `filter_expr`表达式值为非0的指定列的值。通过 `CAST` 操作将值转换成对应列的类型。不支持对用于主键或分区键表达式的列进行更新操作。

``` sql
ALTER TABLE [db.]table MATERIALIZE INDEX name IN PARTITION partition_name
```

该操作更新 `partition_name`分区中的二级索引 `name`.
单次操作可以包含多个逗号分隔的命令。

对于 \*MergeTree引擎表，mutation操作通过重写整个数据块来实现。没有原子性保证 - 被mutation操作的数据会被替换，在mutation期间开始执行的`SELECT`查询能看到所有已经完成mutation的数据，以及还没有被mutation替换的数据。

mutation总是按照它们的创建顺序来排序并以同样顺序在每个数据块中执行。mutation操作也会部分的和Insert操作一起排序 - 在mutation提交之前插入的数据会参与mutation操作，在mutation提交之后的插入的数据则不会参与mutation。注意mutation从来不会阻塞插入操作。

mutation操作在提交后（对于可复制表，添加到Zookeeper,对于不可复制表，添加到文件系统）立即返回。mutation操作本身是根据系统的配置参数异步执行的。要跟踪mutation的进度，可以使用系统表 [`system.mutations`](../../operations/system-tables/mutations.md#system_tables-mutations)。已经成功提交的mutation操作在服务重启后仍会继续执行。一旦mutation完成提交，就不能回退了，但是如果因为某种原因操作被卡住了，可以通过 [`KILL MUTATION`](../../sql-reference/statements/misc.md#kill-mutation)操作来取消它的执行。

已完成的mutations记录不会立即删除（要保留的记录数量由 `finished_mutations_to_keep` 这一参数决定）。之前的mutation记录会被删除。

## 修改用户 {#alter-user-statement}

修改CH的用户账号

### 语法 {#alter-user-syntax}

``` sql
ALTER USER [IF EXISTS] name [ON CLUSTER cluster_name]
    [RENAME TO new_name]
    [IDENTIFIED [WITH {PLAINTEXT_PASSWORD|SHA256_PASSWORD|DOUBLE_SHA1_PASSWORD}] BY {'password'|'hash'}]
    [[ADD|DROP] HOST {LOCAL | NAME 'name' | REGEXP 'name_regexp' | IP 'address' | LIKE 'pattern'} [,...] | ANY | NONE]
    [DEFAULT ROLE role [,...] | ALL | ALL EXCEPT role [,...] ]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | PROFILE 'profile_name'] [,...]
```

### 说明 {#alter-user-dscr}

要使用 `ALTER USER`，你必须拥有 [ALTER USER](../../sql-reference/statements/grant.md#grant-access-management) 操作的权限

### Examples {#alter-user-examples}

设置默认角色：

``` sql
ALTER USER user DEFAULT ROLE role1, role2
```

如果角色之前没分配给用户，CH会抛出异常。

将所有分配的角色设为默认

``` sql
ALTER USER user DEFAULT ROLE ALL
```

如果以后给用户分配了某个角色，它将自动成为默认角色

将除了 `role1` 和 `role2`之外的其它角色 设为默认

``` sql
ALTER USER user DEFAULT ROLE ALL EXCEPT role1, role2
```

## 修改角色 {#alter-role-statement}

修改角色.

### 语法 {#alter-role-syntax}

``` sql
ALTER ROLE [IF EXISTS] name [ON CLUSTER cluster_name]
    [RENAME TO new_name]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | PROFILE 'profile_name'] [,...]
```

## 修改row policy {#alter-row-policy-statement}


修改row policy.

### 语法 {#alter-row-policy-syntax}

``` sql
ALTER [ROW] POLICY [IF EXISTS] name [ON CLUSTER cluster_name] ON [database.]table
    [RENAME TO new_name]
    [AS {PERMISSIVE | RESTRICTIVE}]
    [FOR SELECT]
    [USING {condition | NONE}][,...]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
```

## 修改配额quotas {#alter-quota-statement}

修改配额quotas.

### 语法 {#alter-quota-syntax}

``` sql
ALTER QUOTA [IF EXISTS] name [ON CLUSTER cluster_name]
    [RENAME TO new_name]
    [KEYED BY {'none' | 'user name' | 'ip address' | 'client key' | 'client key or user name' | 'client key or ip address'}]
    [FOR [RANDOMIZED] INTERVAL number {SECOND | MINUTE | HOUR | DAY | WEEK | MONTH | QUARTER | YEAR}
        {MAX { {QUERIES | ERRORS | RESULT ROWS | RESULT BYTES | READ ROWS | READ BYTES | EXECUTION TIME} = number } [,...] |
        NO LIMITS | TRACKING ONLY} [,...]]
    [TO {role [,...] | ALL | ALL EXCEPT role [,...]}]
```

## 修改settings配置 {#alter-settings-profile-statement}

修改settings配置.

### 语法 {#alter-settings-profile-syntax}

``` sql
ALTER SETTINGS PROFILE [IF EXISTS] name [ON CLUSTER cluster_name]
    [RENAME TO new_name]
    [SETTINGS variable [= value] [MIN [=] min_value] [MAX [=] max_value] [READONLY|WRITABLE] | INHERIT 'profile_name'] [,...]
```

[Original article](https://clickhouse.tech/docs/en/query_language/alter/) <!--hide-->
