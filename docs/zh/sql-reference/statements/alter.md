---
machine_translated: true
machine_translated_rev: b111334d6614a02564cf32f379679e9ff970d9b1
toc_priority: 36
toc_title: ALTER
---

## ALTER {#query_language_queries_alter}

该 `ALTER` 查询仅支持 `*MergeTree` 表，以及 `Merge`和`Distributed`. 查询有几个变体。

### 列操作 {#column-manipulations}

更改表结构。

``` sql
ALTER TABLE [db].name [ON CLUSTER cluster] ADD|DROP|CLEAR|COMMENT|MODIFY COLUMN ...
```

在查询中，指定一个或多个逗号分隔操作的列表。
每个操作都是对列的操作。

支持以下操作:

-   [ADD COLUMN](#alter_add-column) — Adds a new column to the table.
-   [DROP COLUMN](#alter_drop-column) — Deletes the column.
-   [CLEAR COLUMN](#alter_clear-column) — Resets column values.
-   [COMMENT COLUMN](#alter_comment-column) — Adds a text comment to the column.
-   [MODIFY COLUMN](#alter_modify-column) — Changes column’s type, default expression and TTL.

下面详细描述这些动作。

#### ADD COLUMN {#alter_add-column}

``` sql
ADD COLUMN [IF NOT EXISTS] name [type] [default_expr] [codec] [AFTER name_after]
```

将一个新列添加到表中，并指定 `name`, `type`, [`codec`](create.md#codecs) 和 `default_expr` （请参阅部分 [默认表达式](create.md#create-default-values)).

如果 `IF NOT EXISTS` 如果列已经存在，则查询不会返回错误。 如果您指定 `AFTER name_after` （另一列的名称），该列被添加在表列表中指定的一列之后。 否则，该列将添加到表的末尾。 请注意，没有办法将列添加到表的开头。 为了一系列的行动, `name_after` 可将该名称一栏，加入一个以前的行动。

添加列只是更改表结构，而不对数据执行任何操作。 数据不会出现在磁盘上后 `ALTER`. 如果从表中读取某一列的数据缺失，则将使用默认值填充该列（如果存在默认表达式，则执行默认表达式，或使用零或空字符串）。 合并数据部分后，该列将出现在磁盘上（请参阅 [MergeTree](../../engines/table-engines/mergetree-family/mergetree.md)).

这种方法使我们能够完成 `ALTER` 即时查询，不增加旧数据量。

示例:

``` sql
ALTER TABLE visits ADD COLUMN browser String AFTER user_id
```

#### DROP COLUMN {#alter_drop-column}

``` sql
DROP COLUMN [IF EXISTS] name
```

删除具有名称的列 `name`. 如果 `IF EXISTS` 如果指定了子句，如果该列不存在，则查询不会返回错误。

从文件系统中删除数据。 由于这将删除整个文件，查询几乎立即完成。

示例:

``` sql
ALTER TABLE visits DROP COLUMN browser
```

#### CLEAR COLUMN {#alter_clear-column}

``` sql
CLEAR COLUMN [IF EXISTS] name IN PARTITION partition_name
```

重置指定分区的列中的所有数据。 了解有关设置分区名称的详细信息 [如何指定分区表达式](#alter-how-to-specify-part-expr).

如果 `IF EXISTS` 如果指定了子句，如果该列不存在，则查询不会返回错误。

示例:

``` sql
ALTER TABLE visits CLEAR COLUMN browser IN PARTITION tuple()
```

#### COMMENT COLUMN {#alter_comment-column}

``` sql
COMMENT COLUMN [IF EXISTS] name 'comment'
```

向列添加注释。 如果 `IF EXISTS` 如果指定了子句，如果该列不存在，则查询不会返回错误。

每列可以有一个注释。 如果列的注释已存在，则新注释将复盖以前的注释。

注释存储在 `comment_expression` 由返回的列 [DESCRIBE TABLE](misc.md#misc-describe-table) 查询。

示例:

``` sql
ALTER TABLE visits COMMENT COLUMN browser 'The table shows the browser used for accessing the site.'
```

#### MODIFY COLUMN {#alter_modify-column}

``` sql
MODIFY COLUMN [IF EXISTS] name [type] [default_expr] [TTL]
```

此查询更改 `name` 列属性:

-   类型

-   默认表达式

-   TTL

        For examples of columns TTL modifying, see [Column TTL](../engines/table_engines/mergetree_family/mergetree.md#mergetree-column-ttl).

如果 `IF EXISTS` 如果指定了子句，如果该列不存在，则查询不会返回错误。

更改类型时，值将被转换为 [toType](../../sql-reference/functions/type-conversion-functions.md) 函数被应用到它们。 如果仅更改默认表达式，则查询不会执行任何复杂的操作，并且几乎立即完成。

示例:

``` sql
ALTER TABLE visits MODIFY COLUMN browser Array(String)
```

Changing the column type is the only complex action – it changes the contents of files with data. For large tables, this may take a long time.

有几个处理阶段:

-   准备具有修改数据的临时（新）文件。
-   重命名旧文件。
-   将临时（新）文件重命名为旧名称。
-   删除旧文件。

只有第一阶段需要时间。 如果在此阶段出现故障，则不会更改数据。
如果在其中一个连续阶段中出现故障，可以手动恢复数据。 例外情况是，如果旧文件从文件系统中删除，但新文件的数据没有写入磁盘并丢失。

该 `ALTER` 复制更改列的查询。 这些指令保存在ZooKeeper中，然后每个副本应用它们。 全部 `ALTER` 查询以相同的顺序运行。 查询等待对其他副本完成适当的操作。 但是，更改复制表中的列的查询可能会中断，并且所有操作都将异步执行。

#### 更改查询限制 {#alter-query-limitations}

该 `ALTER` query允许您在嵌套数据结构中创建和删除单独的元素（列），但不能创建整个嵌套数据结构。 要添加嵌套数据结构，可以添加名称如下的列 `name.nested_name` 和类型 `Array(T)`. 嵌套数据结构等效于名称在点之前具有相同前缀的多个数组列。

不支持删除主键或采样键中的列（在主键中使用的列 `ENGINE` 表达式）。 只有在此更改不会导致数据被修改时，才可以更改主键中包含的列的类型（例如，允许您向枚举添加值或更改类型 `DateTime` 到 `UInt32`).

如果 `ALTER` 查询不足以使您需要的表更改，您可以创建一个新的表，使用 [INSERT SELECT](insert-into.md#insert_query_insert-select) 查询，然后使用切换表 [RENAME](misc.md#misc_operations-rename) 查询并删除旧表。 您可以使用 [ﾂ环板-ｮﾂ嘉ｯﾂ偲](../../operations/utilities/clickhouse-copier.md) 作为替代 `INSERT SELECT` 查询。

该 `ALTER` 查询阻止对表的所有读取和写入。 换句话说，如果长 `SELECT` 正在运行的时间 `ALTER` 查询，该 `ALTER` 查询将等待它完成。 同时，对同一个表的所有新查询将等待 `ALTER` 正在运行。

对于本身不存储数据的表（例如 `Merge` 和 `Distributed`), `ALTER` 只是改变了表结构，并且不改变从属表的结构。 例如，当运行ALTER时 `Distributed` 表，你还需要运行 `ALTER` 对于所有远程服务器上的表。

### 使用键表达式进行操作 {#manipulations-with-key-expressions}

支持以下命令:

``` sql
MODIFY ORDER BY new_expression
```

它只适用于在表 [`MergeTree`](../../engines/table-engines/mergetree-family/mergetree.md) 家庭（包括
[复制](../../engines/table-engines/mergetree-family/replication.md) 表）。 该命令更改
[排序键](../../engines/table-engines/mergetree-family/mergetree.md) 表
到 `new_expression` （表达式或表达式元组）。 主键保持不变。

该命令是轻量级的，因为它只更改元数据。 要保持该数据部分的属性
行按排序键表达式排序您不能添加包含现有列的表达式
到排序键（仅由列添加 `ADD COLUMN` 命令在同一个 `ALTER` 查询）。

### 使用数据跳过索引进行操作 {#manipulations-with-data-skipping-indices}

它只适用于在表 [`*MergeTree`](../../engines/table-engines/mergetree-family/mergetree.md) 家庭（包括
[复制](../../engines/table-engines/mergetree-family/replication.md) 表）。 以下操作
可用:

-   `ALTER TABLE [db].name ADD INDEX name expression TYPE type GRANULARITY value AFTER name [AFTER name2]` -将索引描述添加到表元数据。

-   `ALTER TABLE [db].name DROP INDEX name` -从表元数据中删除索引描述并从磁盘中删除索引文件。

这些命令是轻量级的，因为它们只更改元数据或删除文件。
此外，它们被复制（通过ZooKeeper同步索引元数据）。

### 使用约束进行操作 {#manipulations-with-constraints}

查看更多 [制约因素](create.md#constraints)

可以使用以下语法添加或删除约束:

``` sql
ALTER TABLE [db].name ADD CONSTRAINT constraint_name CHECK expression;
ALTER TABLE [db].name DROP CONSTRAINT constraint_name;
```

查询将从表中添加或删除有关约束的元数据，以便立即处理它们。

约束检查 *不会被执行* 在现有数据上，如果它被添加。

复制表上的所有更改都广播到ZooKeeper，因此将应用于其他副本。

### 操作与分区和零件 {#alter_manipulations-with-partitions}

下面的操作与 [分区](../../engines/table-engines/mergetree-family/custom-partitioning-key.md) 可用:

-   [DETACH PARTITION](#alter_detach-partition) – Moves a partition to the `detached` 目录和忘记它。
-   [DROP PARTITION](#alter_drop-partition) – Deletes a partition.
-   [ATTACH PART\|PARTITION](#alter_attach-partition) – Adds a part or partition from the `detached` 目录到表。
-   [REPLACE PARTITION](#alter_replace-partition) -将数据分区从一个表复制到另一个表。
-   [ATTACH PARTITION FROM](#alter_attach-partition-from) – Copies the data partition from one table to another and adds.
-   [REPLACE PARTITION](#alter_replace-partition) -将数据分区从一个表复制到另一个表并替换。
-   [MOVE PARTITION TO TABLE](#alter_move_to_table-partition) (\#alter\_move\_to\_table-partition)-将数据分区从一个表移动到另一个表。
-   [CLEAR COLUMN IN PARTITION](#alter_clear-column-partition) -重置分区中指定列的值。
-   [CLEAR INDEX IN PARTITION](#alter_clear-index-partition) -重置分区中指定的二级索引。
-   [FREEZE PARTITION](#alter_freeze-partition) – Creates a backup of a partition.
-   [FETCH PARTITION](#alter_fetch-partition) – Downloads a partition from another server.
-   [MOVE PARTITION\|PART](#alter_move-partition) – Move partition/data part to another disk or volume.

<!-- -->

#### 分离分区{#alter_detach-partition} {#detach-partition-alter-detach-partition}

``` sql
ALTER TABLE table_name DETACH PARTITION partition_expr
```

将指定分区的所有数据移动到 `detached` 目录。 服务器会忘记分离的数据分区，就好像它不存在一样。 服务器不会知道这个数据，直到你做 [ATTACH](#alter_attach-partition) 查询。

示例:

``` sql
ALTER TABLE visits DETACH PARTITION 201901
```

阅读有关在一节中设置分区表达式的信息 [如何指定分区表达式](#alter-how-to-specify-part-expr).

执行查询后，您可以对查询中的数据进行任何操作 `detached` directory — delete it from the file system, or just leave it.

This query is replicated – it moves the data to the `detached` 所有副本上的目录。 请注意，您只能对领导副本执行此查询。 要确定副本是否为领导者，请执行 `SELECT` 查询到 [系统。副本](../../operations/system-tables.md#system_tables-replicas) 桌子 或者，它更容易使 `DETACH` 对所有副本进行查询-除了领导副本之外，所有副本都会引发异常。

#### DROP PARTITION {#alter_drop-partition}

``` sql
ALTER TABLE table_name DROP PARTITION partition_expr
```

从表中删除指定的分区。 此查询将分区标记为非活动分区，并在大约10分钟内完全删除数据。

阅读有关在一节中设置分区表达式的信息 [如何指定分区表达式](#alter-how-to-specify-part-expr).

The query is replicated – it deletes data on all replicas.

#### DROP DETACHED PARTITION\|PART {#alter_drop-detached}

``` sql
ALTER TABLE table_name DROP DETACHED PARTITION|PART partition_expr
```

从中删除指定分区的指定部分或所有部分 `detached`.
了解有关在一节中设置分区表达式的详细信息 [如何指定分区表达式](#alter-how-to-specify-part-expr).

#### ATTACH PARTITION\|PART {#alter_attach-partition}

``` sql
ALTER TABLE table_name ATTACH PARTITION|PART partition_expr
```

将数据从 `detached` 目录。 可以为整个分区或单独的部分添加数据。 例:

``` sql
ALTER TABLE visits ATTACH PARTITION 201901;
ALTER TABLE visits ATTACH PART 201901_2_2_0;
```

了解有关在一节中设置分区表达式的详细信息 [如何指定分区表达式](#alter-how-to-specify-part-expr).

此查询被复制。 副本发起程序检查是否有数据在 `detached` 目录。 如果数据存在，则查询将检查其完整性。 如果一切正确，则查询将数据添加到表中。 所有其他副本都从副本发起程序下载数据。

所以你可以把数据到 `detached` 在一个副本上的目录，并使用 `ALTER ... ATTACH` 查询以将其添加到所有副本上的表中。

#### ATTACH PARTITION FROM {#alter_attach-partition-from}

``` sql
ALTER TABLE table2 ATTACH PARTITION partition_expr FROM table1
```

此查询将数据分区从 `table1` 到 `table2` 将数据添加到存在 `table2`. 请注意，数据不会从中删除 `table1`.

要使查询成功运行，必须满足以下条件:

-   两个表必须具有相同的结构。
-   两个表必须具有相同的分区键。

#### REPLACE PARTITION {#alter_replace-partition}

``` sql
ALTER TABLE table2 REPLACE PARTITION partition_expr FROM table1
```

此查询将数据分区从 `table1` 到 `table2` 并替换在现有的分区 `table2`. 请注意，数据不会从中删除 `table1`.

要使查询成功运行，必须满足以下条件:

-   两个表必须具有相同的结构。
-   两个表必须具有相同的分区键。

#### MOVE PARTITION TO TABLE {#alter_move_to_table-partition}

``` sql
ALTER TABLE table_source MOVE PARTITION partition_expr TO TABLE table_dest
```

此查询将数据分区从 `table_source` 到 `table_dest` 删除数据 `table_source`.

要使查询成功运行，必须满足以下条件:

-   两个表必须具有相同的结构。
-   两个表必须具有相同的分区键。
-   两个表必须是相同的引擎系列。 （已复制或未复制)
-   两个表必须具有相同的存储策略。

#### CLEAR COLUMN IN PARTITION {#alter_clear-column-partition}

``` sql
ALTER TABLE table_name CLEAR COLUMN column_name IN PARTITION partition_expr
```

重置分区中指定列中的所有值。 如果 `DEFAULT` 创建表时确定了子句，此查询将列值设置为指定的默认值。

示例:

``` sql
ALTER TABLE visits CLEAR COLUMN hour in PARTITION 201902
```

#### FREEZE PARTITION {#alter_freeze-partition}

``` sql
ALTER TABLE table_name FREEZE [PARTITION partition_expr]
```

此查询创建指定分区的本地备份。 如果 `PARTITION` 子句被省略，查询一次创建所有分区的备份。

!!! note "注"
    在不停止服务器的情况下执行整个备份过程。

请注意，对于旧式表，您可以指定分区名称的前缀（例如, ‘2019’)-然后查询为所有相应的分区创建备份。 阅读有关在一节中设置分区表达式的信息 [如何指定分区表达式](#alter-how-to-specify-part-expr).

在执行时，对于数据快照，查询将创建指向表数据的硬链接。 硬链接被放置在目录中 `/var/lib/clickhouse/shadow/N/...`，哪里:

-   `/var/lib/clickhouse/` 是配置中指定的工作ClickHouse目录。
-   `N` 是备份的增量编号。

!!! note "注"
    如果您使用 [用于在表中存储数据的一组磁盘](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes)，该 `shadow/N` 目录出现在每个磁盘上，存储由匹配的数据部分 `PARTITION` 表达。

在备份内部创建的目录结构与在备份内部创建的目录结构相同 `/var/lib/clickhouse/`. 查询执行 ‘chmod’ 对于所有文件，禁止写入它们。

创建备份后，您可以从以下位置复制数据 `/var/lib/clickhouse/shadow/` 然后将其从本地服务器中删除。 请注意， `ALTER t FREEZE PARTITION` 不复制查询。 它仅在本地服务器上创建本地备份。

查询几乎立即创建备份（但首先它会等待对相应表的当前查询完成运行）。

`ALTER TABLE t FREEZE PARTITION` 仅复制数据，而不复制表元数据。 若要备份表元数据，请复制该文件 `/var/lib/clickhouse/metadata/database/table.sql`

要从备份还原数据，请执行以下操作:

1.  如果表不存在，则创建该表。 要查看查询，请使用。sql文件（替换 `ATTACH` 在它与 `CREATE`).
2.  从复制数据 `data/database/table/` 目录内的备份到 `/var/lib/clickhouse/data/database/table/detached/` 目录。
3.  快跑 `ALTER TABLE t ATTACH PARTITION` 将数据添加到表的查询。

从备份还原不需要停止服务器。

有关备份和还原数据的详细信息，请参阅 [数据备份](../../operations/backup.md) 科。

#### CLEAR INDEX IN PARTITION {#alter_clear-index-partition}

``` sql
ALTER TABLE table_name CLEAR INDEX index_name IN PARTITION partition_expr
```

查询的工作原理类似于 `CLEAR COLUMN`，但它重置索引而不是列数据。

#### FETCH PARTITION {#alter_fetch-partition}

``` sql
ALTER TABLE table_name FETCH PARTITION partition_expr FROM 'path-in-zookeeper'
```

从另一台服务器下载分区。 此查询仅适用于复制的表。

查询执行以下操作:

1.  从指定的分片下载分区。 在 ‘path-in-zookeeper’ 您必须在ZooKeeper中指定分片的路径。
2.  然后查询将下载的数据放到 `detached` 的目录 `table_name` 桌子 使用 [ATTACH PARTITION\|PART](#alter_attach-partition) 查询将数据添加到表中。

例如:

``` sql
ALTER TABLE users FETCH PARTITION 201902 FROM '/clickhouse/tables/01-01/visits';
ALTER TABLE users ATTACH PARTITION 201902;
```

请注意:

-   该 `ALTER ... FETCH PARTITION` 查询不被复制。 它将分区放置在 `detached` 仅在本地服务器上的目录。
-   该 `ALTER TABLE ... ATTACH` 复制查询。 它将数据添加到所有副本。 数据被添加到从副本之一 `detached` 目录，以及其他-从相邻的副本。

在下载之前，系统会检查分区是否存在并且表结构匹配。 从正常副本中自动选择最合适的副本。

虽然查询被调用 `ALTER TABLE`，它不会更改表结构，并且不会立即更改表中可用的数据。

#### MOVE PARTITION\|PART {#alter_move-partition}

将分区或数据部分移动到另一个卷或磁盘 `MergeTree`-发动机表。 看 [使用多个块设备进行数据存储](../../engines/table-engines/mergetree-family/mergetree.md#table_engine-mergetree-multiple-volumes).

``` sql
ALTER TABLE table_name MOVE PARTITION|PART partition_expr TO DISK|VOLUME 'disk_name'
```

该 `ALTER TABLE t MOVE` 查询:

-   不复制，因为不同的副本可能具有不同的存储策略。
-   如果未配置指定的磁盘或卷，则返回错误。 如果无法应用存储策略中指定的数据移动条件，Query还会返回错误。
-   可以在返回错误的情况下，当要移动的数据已经被后台进程移动时，并发 `ALTER TABLE t MOVE` 查询或作为后台数据合并的结果。 在这种情况下，用户不应该执行任何其他操作。

示例:

``` sql
ALTER TABLE hits MOVE PART '20190301_14343_16206_438' TO VOLUME 'slow'
ALTER TABLE hits MOVE PARTITION '2019-09-01' TO DISK 'fast_ssd'
```

#### 如何设置分区表达式 {#alter-how-to-specify-part-expr}

您可以在以下内容中指定分区表达式 `ALTER ... PARTITION` 以不同方式查询:

-   作为从值 `partition` 列 `system.parts` 桌子 例如, `ALTER TABLE visits DETACH PARTITION 201901`.
-   作为来自表列的表达式。 支持常量和常量表达式。 例如, `ALTER TABLE visits DETACH PARTITION toYYYYMM(toDate('2019-01-25'))`.
-   使用分区ID。 分区ID是用作文件系统和ZooKeeper中分区名称的分区的字符串标识符（如果可能的话，人类可读）。 分区ID必须在指定 `PARTITION ID` 子句，用单引号。 例如, `ALTER TABLE visits DETACH PARTITION ID '201901'`.
-   在 [ALTER ATTACH PART](#alter_attach-partition) 和 [DROP DETACHED PART](#alter_drop-detached) 查询时，要指定部件的名称，请将字符串文字与来自 `name` 列 [系统。detached\_parts](../../operations/system-tables.md#system_tables-detached_parts) 桌子 例如, `ALTER TABLE visits ATTACH PART '201901_1_1_0'`.

指定分区时引号的使用取决于分区表达式的类型。 例如，对于 `String` 类型，你必须在引号中指定其名称 (`'`). 为 `Date` 和 `Int*` 类型不需要引号。

对于旧式表，您可以将分区指定为数字 `201901` 或者一个字符串 `'201901'`. 对于类型，新样式表的语法更严格（类似于值输入格式的解析器）。

上述所有规则也适用于 [OPTIMIZE](misc.md#misc_operations-optimize) 查询。 如果在优化非分区表时需要指定唯一的分区，请设置表达式 `PARTITION tuple()`. 例如:

``` sql
OPTIMIZE TABLE table_not_partitioned PARTITION tuple() FINAL;
```

的例子 `ALTER ... PARTITION` 查询在测试中演示 [`00502_custom_partitioning_local`](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00502_custom_partitioning_local.sql) 和 [`00502_custom_partitioning_replicated_zookeeper`](https://github.com/ClickHouse/ClickHouse/blob/master/tests/queries/0_stateless/00502_custom_partitioning_replicated_zookeeper.sql).

### 使用表TTL进行操作 {#manipulations-with-table-ttl}

你可以改变 [表TTL](../../engines/table-engines/mergetree-family/mergetree.md#mergetree-table-ttl) 请填写以下表格:

``` sql
ALTER TABLE table-name MODIFY TTL ttl-expression
```

### ALTER查询的同步性 {#synchronicity-of-alter-queries}

对于不可复制的表，所有 `ALTER` 查询是同步执行的。 对于可复制的表，查询仅添加相应操作的说明 `ZooKeeper`，并尽快执行操作本身。 但是，查询可以等待在所有副本上完成这些操作。

为 `ALTER ... ATTACH|DETACH|DROP` 查询，您可以使用 `replication_alter_partitions_sync` 设置设置等待。
可能的值: `0` – do not wait; `1` – only wait for own execution (default); `2` – wait for all.

### 突变 {#alter-mutations}

突变是允许更改或删除表中的行的ALTER查询变体。 与标准相比 `UPDATE` 和 `DELETE` 用于点数据更改的查询，mutations适用于更改表中大量行的繁重操作。 支持的 `MergeTree` 表引擎系列，包括具有复制支持的引擎。

现有表可以按原样进行突变（无需转换），但是在将第一次突变应用于表之后，其元数据格式将与以前的服务器版本不兼容，并且无法回退到以前的版本。

当前可用的命令:

``` sql
ALTER TABLE [db.]table DELETE WHERE filter_expr
```

该 `filter_expr` 必须是类型 `UInt8`. 查询删除表中此表达式采用非零值的行。

``` sql
ALTER TABLE [db.]table UPDATE column1 = expr1 [, ...] WHERE filter_expr
```

该 `filter_expr` 必须是类型 `UInt8`. 此查询将指定列的值更新为行中相应表达式的值。 `filter_expr` 取非零值。 使用以下命令将值转换为列类型 `CAST` 接线员 不支持更新用于计算主键或分区键的列。

``` sql
ALTER TABLE [db.]table MATERIALIZE INDEX name IN PARTITION partition_name
```

查询将重新生成二级索引 `name` 在分区中 `partition_name`.

一个查询可以包含多个用逗号分隔的命令。

For\*MergeTree表的突变通过重写整个数据部分来执行。 没有原子性-部分被取代为突变的部分，只要他们准备好和 `SELECT` 在突变期间开始执行的查询将看到来自已经突变的部件的数据以及来自尚未突变的部件的数据。

突变完全按其创建顺序排序，并以该顺序应用于每个部分。 突变也使用插入进行部分排序-在提交突变之前插入到表中的数据将被突变，之后插入的数据将不会被突变。 请注意，突变不会以任何方式阻止插入。

Mutation查询在添加mutation条目后立即返回（如果将复制的表复制到ZooKeeper，则将非复制的表复制到文件系统）。 突变本身使用系统配置文件设置异步执行。 要跟踪突变的进度，您可以使用 [`system.mutations`](../../operations/system-tables.md#system_tables-mutations) 桌子 即使重新启动ClickHouse服务器，成功提交的突变仍将继续执行。 一旦提交，没有办法回滚突变，但如果突变由于某种原因被卡住，可以使用 [`KILL MUTATION`](misc.md#kill-mutation) 查询。

已完成突变的条目不会立即删除（保留条目的数量由 `finished_mutations_to_keep` 存储引擎参数）。 旧的突变条目将被删除。

[原始文章](https://clickhouse.tech/docs/en/query_language/alter/) <!--hide-->
