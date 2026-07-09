---
description: 'COLUMN 相关文档'
sidebar_label: 'COLUMN'
sidebar_position: 37
slug: /sql-reference/statements/alter/column
title: '列操作'
doc_type: 'reference'
---

一组用于更改表结构的查询。

语法：

```sql
ALTER [TEMPORARY] TABLE [db].name [ON CLUSTER cluster] ADD|DROP|RENAME|CLEAR|COMMENT|{MODIFY|ALTER}|MATERIALIZE COLUMN ...
```

在查询中，指定一个由一个或多个以逗号分隔的操作组成的列表。
每个操作都是对列执行的一项操作。

支持以下操作：

* [ADD COLUMN](#add-column) — 向表中添加新列。
* [DROP COLUMN](#drop-column) — 删除该列。
* [RENAME COLUMN](#rename-column) — 重命名现有列。
* [CLEAR COLUMN](#clear-column) — 重置列值。
* [COMMENT COLUMN](#comment-column) — 为列添加文本注释。
* [MODIFY COLUMN](#modify-column) — 更改列的类型、默认表达式、生存时间 (TTL) 和列设置。
* [MODIFY COLUMN REMOVE](#modify-column-remove) — 删除列的一项属性。
* [MODIFY COLUMN MODIFY SETTING](#modify-column-modify-setting) - 更改列设置。
* [MODIFY COLUMN RESET SETTING](#modify-column-reset-setting) - 重置列设置。
* [MODIFY COLUMN ADD ENUM VALUES](#modify-column-add-enum-values) - 向枚举添加新值。
* [MATERIALIZE COLUMN](#materialize-column) — 在缺少该列的 parts 中将其物化。
  下文将详细介绍这些操作。

<div id="add-column">
  ## ADD COLUMN
</div>

```sql
ADD COLUMN [IF NOT EXISTS] name [type] [default_expr] [codec] [AFTER name_after | FIRST]
```

向表中添加一个新列，并为其指定 `name`、`type`、[`codec`](../create/table.md/#column_compression_codec) 和 `default_expr` (参见[默认表达式](/zh/sql-reference/statements/create/table#default_values)一节) 。

如果包含 `IF NOT EXISTS` 子句，那么当列已存在时，查询不会返回错误。如果指定了 `AFTER name_after` (另一列的名称) ，则该列会被添加到表列列表中该指定列之后。如果想将列添加到表的开头，请使用 `FIRST` 子句。否则，该列会被添加到表的末尾。对于一连串操作，`name_after` 也可以是前面某个操作中添加的列名。

添加列只会更改表结构，不会对数据执行任何操作。执行 `ALTER` 后，数据不会立即出现在磁盘上。读取表时，如果某列的数据缺失，则会用默认值填充 (如果存在默认表达式，则执行该表达式；否则使用零或空字符串) 。该列会在合并数据分区片段后出现在磁盘上 (参见 [MergeTree](/zh/engines/table-engines/mergetree-family/mergetree.md)) 。

这种方式使我们能够立即完成 `ALTER` 查询，而不会增加旧数据的数据量。

示例：

```sql
ALTER TABLE alter_test ADD COLUMN Added1 UInt32 FIRST;
ALTER TABLE alter_test ADD COLUMN Added2 UInt32 AFTER NestedColumn;
ALTER TABLE alter_test ADD COLUMN Added3 UInt32 AFTER ToDrop;
DESC alter_test FORMAT TSV;
```

```text
Added1  UInt32
CounterID       UInt32
StartDate       Date
UserID  UInt32
VisitID UInt32
NestedColumn.A  Array(UInt8)
NestedColumn.S  Array(String)
Added2  UInt32
ToDrop  UInt32
Added3  UInt32
```

<div id="drop-column">
  ## DROP COLUMN
</div>

```sql
DROP COLUMN [IF EXISTS] name
```

删除名称为 `name` 的列。如果指定了 `IF EXISTS` 子句，即使该列不存在，查询也不会返回错误。

从文件系统中删除数据。由于这里会删除整个文件，因此查询几乎会立即完成。

:::tip
如果某列被 [materialized view](/zh/sql-reference/statements/create/view) 引用，则无法删除该列。否则会返回错误。
:::

示例：

```sql
ALTER TABLE visits DROP COLUMN browser
```

<div id="rename-column">
  ## RENAME COLUMN
</div>

```sql
RENAME COLUMN [IF EXISTS] name to new_name
```

将列 `name` 重命名为 `new_name`。如果指定了 `IF EXISTS` 子句，即使该列不存在，查询也不会返回错误。由于重命名不涉及底层数据，因此查询几乎会立即完成。

**注意**：表的键表达式中指定的列 (无论是通过 `ORDER BY` 还是 `PRIMARY KEY`) 都不能重命名。尝试修改这些列会产生 `SQL Error [524]`。

示例：

```sql
ALTER TABLE visits RENAME COLUMN webBrowser TO browser
```

<div id="clear-column">
  ## CLEAR COLUMN
</div>

```sql
CLEAR COLUMN [IF EXISTS] name IN PARTITION partition_name
```

重置指定分区中某一列的所有数据。有关如何设置分区名称的更多信息，请参阅[如何设置分区表达式](../alter/partition.md/#how-to-set-partition-expression)一节。

如果指定了 `IF EXISTS` 子句，则即使该列不存在，查询也不会返回错误。

示例：

```sql
ALTER TABLE visits CLEAR COLUMN browser IN PARTITION tuple()
```

<div id="comment-column">
  ## COMMENT COLUMN
</div>

```sql
COMMENT COLUMN [IF EXISTS] name 'Text comment'
```

为该列添加注释。如果指定了 `IF EXISTS` 子句，则在列不存在时，查询不会返回错误。

每列只能有一条注释。如果该列已有注释，新注释会覆盖原有注释。

注释存储在 [DESCRIBE TABLE](/zh/sql-reference/statements/describe-table.md) 查询返回的 `comment_expression` 列中。

示例：

```sql
ALTER TABLE visits COMMENT COLUMN browser 'This column shows the browser used for accessing the site.'
```

<div id="modify-column">
  ## MODIFY COLUMN
</div>

```sql
MODIFY COLUMN [IF EXISTS] name
    [type] [default_expr] [codec] [TTL] [settings] [AFTER name_after | FIRST]
    | ADD ENUM VALUES ( 'name' [= number] [, ...] )
ALTER COLUMN [IF EXISTS] name
    TYPE [type] [default_expr] [codec] [TTL] [settings] [AFTER name_after | FIRST]
    | ADD ENUM VALUES ( 'name' [= number] [, ...] )
```

此查询会更改 `name` 列的以下属性：

* 类型

* 默认表达式

* 压缩编解码器

* 生存时间 (TTL)

* 列级设置

* Enum/Enum8/Enum16 类型的枚举值

有关修改列压缩编解码器的示例，请参见 [列压缩编解码器](../create/table.md/#column_compression_codec)。

有关修改列 生存时间 (TTL) 的示例，请参见 [列 生存时间 (TTL)](/zh/engines/table-engines/mergetree-family/mergetree.md/#mergetree-column-ttl)。

有关修改列级设置的示例，请参见 [列级设置](/zh/engines/table-engines/mergetree-family/mergetree.md/#column-level-settings)。

如果指定了 `IF EXISTS` 子句，当该列不存在时，查询不会报错。

更改类型时，值会像应用了 [toType](/zh/sql-reference/functions/type-conversion-functions.md) 函数一样被转换。如果仅更改默认表达式，则查询不会执行任何复杂操作，几乎会立即完成。

示例：

```sql
ALTER TABLE visits MODIFY COLUMN browser Array(String)
```

更改列类型是唯一一个较复杂的操作——它会更改数据文件的内容。对于大型表，这可能需要很长时间。

该查询还可以使用 `FIRST | AFTER` 子句来调整列的顺序，参见 [ADD COLUMN](#add-column) 的说明，但在这种情况下必须指定列类型。

示例：

```sql
CREATE TABLE users (
    c1 Int16,
    c2 String
) ENGINE = MergeTree
ORDER BY c1;

DESCRIBE users;
┌─name─┬─type───┬
│ c1   │ Int16  │
│ c2   │ String │
└──────┴────────┴

ALTER TABLE users MODIFY COLUMN c2 String FIRST;

DESCRIBE users;
┌─name─┬─type───┬
│ c2   │ String │
│ c1   │ Int16  │
└──────┴────────┴

ALTER TABLE users ALTER COLUMN c2 TYPE String AFTER c1;

DESCRIBE users;
┌─name─┬─type───┬
│ c1   │ Int16  │
│ c2   │ String │
└──────┴────────┴
```

`ALTER` 查询是原子的。对于 MergeTree 表，它还是无锁的。

用于修改列的 `ALTER` 查询是复制的。相关指令会保存在 ZooKeeper 中，然后由每个副本执行。所有 `ALTER` 查询都会按相同顺序执行。该查询会等待其他副本上的相应操作完成。不过，对副本表中列进行修改的查询可能会被中断，而所有操作仍会异步执行。

:::note
将 Nullable 列更改为 Non-Nullable 时务必小心。请确保其中不含任何 NULL 值，否则读取时会出现问题。在这种情况下，变通办法是终止该变更，并将该列回退为 Nullable 类型。
:::

<div id="modify-column-remove">
  ## MODIFY COLUMN REMOVE
</div>

删除以下列属性之一：`DEFAULT`、`ALIAS`、`MATERIALIZED`、`CODEC`、`COMMENT`、`TTL`、`SETTINGS`。

语法：

```sql
ALTER TABLE table_name MODIFY COLUMN column_name REMOVE property;
```

**示例**

移除生存时间 (TTL)：

```sql
ALTER TABLE table_with_ttl MODIFY COLUMN column_ttl REMOVE TTL;
```

**另请参阅**

* [REMOVE 生存时间 (TTL)](ttl.md).

<div id="modify-column-modify-setting">
  ## MODIFY COLUMN MODIFY SETTING
</div>

修改列设置。

语法：

```sql
ALTER TABLE table_name MODIFY COLUMN column_name MODIFY SETTING name=value,...;
```

**示例**

将列的 `max_compress_block_size` 修改为 `1MB`：

```sql
ALTER TABLE table_name MODIFY COLUMN column_name MODIFY SETTING max_compress_block_size = 1048576;
```

<div id="modify-column-reset-setting">
  ## MODIFY COLUMN RESET SETTING
</div>

重置列设置，同时还会移除该表 CREATE 查询中列表达式里的设置声明。

语法：

```sql
ALTER TABLE table_name MODIFY COLUMN column_name RESET SETTING name,...;
```

**示例**

将列设置 `max_compress_block_size` 重置为默认值：

```sql
ALTER TABLE table_name MODIFY COLUMN column_name RESET SETTING max_compress_block_size;
```

<div id="modify-column-add-enum-values">
  ## MODIFY COLUMN ADD ENUM VALUES
</div>

为类型为 `Enum`、`Enum8`、`Enum16`、`Nullable(Enum)`、`Nullable(Enum8)` 或 `Nullable(Enum16)` 的列新增值

语法：

```sql
ALTER TABLE table_name MODIFY COLUMN enum_column_name ADD ENUM VALUES ('EnumName' [= number], ...);
```

**示例**

向 `enum_column_name` 列中添加两个值：

```sql
ALTER TABLE table_name MODIFY COLUMN enum_column_name ADD ENUM VALUES ('Hundred' = 100, 'HundredOne');
```

<div id="materialize-column">
  ## MATERIALIZE COLUMN
</div>

将带有 `DEFAULT` 或 `MATERIALIZED` 值表达式的列 materialize。使用 `ALTER TABLE table_name ADD COLUMN column_name MATERIALIZED` 添加 materialized 列时，现有行中缺少 materialized 值的部分不会自动填充。`MATERIALIZE COLUMN` 语句可用于在添加或更新 `DEFAULT` 或 `MATERIALIZED` 表达式后，重写现有列数据 (该操作只会更新元数据，不会更改现有数据) 。请注意，对排序键中的列执行 materialize 是无效操作，因为这可能会破坏排序顺序。
其实现方式为一个[变更](/zh/sql-reference/statements/alter/index.md#mutations)。

对于带有新的或更新后 `MATERIALIZED` 值表达式的列，所有现有行都会被重写。

对于带有新的或更新后 `DEFAULT` 值表达式的列，其行为取决于 ClickHouse 版本：

* 在 ClickHouse &lt; v24.2 中，所有现有行都会被重写。
* ClickHouse &gt;= v24.2 会区分带有 `DEFAULT` 值表达式的列中，某个行值是在插入时显式指定的，还是未显式指定、即根据 `DEFAULT` 值表达式计算得出的。如果该值是显式指定的，ClickHouse 会保持原样。如果该值是计算得出的，ClickHouse 会将其更改为新的或更新后 `MATERIALIZED` 值表达式。

语法：

```sql
ALTER TABLE [db.]table [ON CLUSTER cluster] MATERIALIZE COLUMN col [IN PARTITION partition | IN PARTITION ID 'partition_id'];
```

* 如果指定了 PARTITION，则只会在指定分区上对该列进行物化。

**示例**

```sql
DROP TABLE IF EXISTS tmp;
SET mutations_sync = 2;
CREATE TABLE tmp (x Int64) ENGINE = MergeTree() ORDER BY tuple() PARTITION BY tuple();
INSERT INTO tmp SELECT * FROM system.numbers LIMIT 5;
ALTER TABLE tmp ADD COLUMN s String MATERIALIZED toString(x);

ALTER TABLE tmp MATERIALIZE COLUMN s;

SELECT groupArray(x), groupArray(s) FROM (select x,s from tmp order by x);

┌─groupArray(x)─┬─groupArray(s)─────────┐
│ [0,1,2,3,4]   │ ['0','1','2','3','4'] │
└───────────────┴───────────────────────┘

ALTER TABLE tmp MODIFY COLUMN s String MATERIALIZED toString(round(100/x));

INSERT INTO tmp SELECT * FROM system.numbers LIMIT 5,5;

SELECT groupArray(x), groupArray(s) FROM tmp;

┌─groupArray(x)─────────┬─groupArray(s)──────────────────────────────────┐
│ [0,1,2,3,4,5,6,7,8,9] │ ['0','1','2','3','4','20','17','14','12','11'] │
└───────────────────────┴────────────────────────────────────────────────┘

ALTER TABLE tmp MATERIALIZE COLUMN s;

SELECT groupArray(x), groupArray(s) FROM tmp;

┌─groupArray(x)─────────┬─groupArray(s)─────────────────────────────────────────┐
│ [0,1,2,3,4,5,6,7,8,9] │ ['inf','100','50','33','25','20','17','14','12','11'] │
└───────────────────────┴───────────────────────────────────────────────────────┘
```

**另见**

* [MATERIALIZED](/zh/sql-reference/statements/create/view#materialized-view).

<div id="limitations">
  ## 限制
</div>

`ALTER` 查询允许你在嵌套数据结构中创建和删除单个元素 (列) ，但不支持创建或删除整个嵌套数据结构。要添加嵌套数据结构，可以添加名称形如 `name.nested_name`、类型为 `Array(T)` 的列。嵌套数据结构等价于多个数组列，这些列的名称在点号前具有相同的前缀。

对名称中带点的列进行重命名仅部分受支持。点号保留用于访问 [Nested](/zh/sql-reference/data-types/nested-data-structures/nested) 子列，因此前缀 (父名称) 必须保持不变。只有后缀 (子列名称) 可以更改。例如，`a.b` 可以重命名为 `a.c`，但不允许将 `a.b` 重命名为 `b.d`，因为这会更改 Nested 的父级前缀。

不支持删除主键或采样键中的列 (即在 `ENGINE` 表达式中使用的列) 。只有在不会导致数据被修改的情况下，才可以更改主键中列的类型 (例如，可以向枚举中添加值，或者将类型从 `DateTime` 更改为 `UInt32`) 。

如果 `ALTER` 查询不足以完成你所需的表更改，可以创建一个新表，使用 [INSERT SELECT](/zh/sql-reference/statements/insert-into.md/#inserting-the-results-of-select) 查询将数据复制到新表中，然后使用 [RENAME](/zh/sql-reference/statements/rename.md/#rename-table) 查询切换表，并删除旧表。

`ALTER` 查询会阻塞该表上的所有读写。换句话说，如果执行 `ALTER` 查询时有一个长时间运行的 `SELECT` 查询，`ALTER` 查询会等待其完成。同时，在此 `ALTER` 执行期间，针对同一张表的所有新查询也都会等待。

对于本身不存储数据的表 (例如 [Merge](/zh/sql-reference/statements/alter/index.md) 和 [Distributed](/zh/sql-reference/statements/alter/index.md)) ，`ALTER` 只会更改表结构，不会更改其下级表的结构。例如，对 `Distributed` 表执行 ALTER 时，你还需要对所有远程服务器上的表执行 `ALTER`。