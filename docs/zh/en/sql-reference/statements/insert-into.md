---
description: 'INSERT INTO 语句文档'
sidebar_label: 'INSERT INTO'
sidebar_position: 33
slug: /sql-reference/statements/insert-into
title: 'INSERT INTO 语句'
doc_type: 'reference'
---

向表中插入数据。

**语法**

```sql
INSERT INTO [TABLE] [db.]table [(c1, c2, c3)] [SETTINGS ...] VALUES (v11, v12, v13), (v21, v22, v23), ...
```

您可以使用 `(c1, c2, c3)` 指定要插入的列列表。您也可以将表达式与列[匹配器](../../sql-reference/statements/select/index.md#asterisk) (如 `*`) 和/或[修饰符](../../sql-reference/statements/select/index.md#select-modifiers) (如 [APPLY](/zh/sql-reference/statements/select/apply-modifier)、[EXCEPT](/zh/sql-reference/statements/select/except-modifier)、[REPLACE](/zh/sql-reference/statements/select/replace-modifier)) 结合使用。

例如，考虑下列表：

```sql
SHOW CREATE insert_select_testtable;
```

```text
CREATE TABLE insert_select_testtable
(
    `a` Int8,
    `b` String,
    `c` Int8
)
ENGINE = MergeTree()
ORDER BY a
```

```sql
INSERT INTO insert_select_testtable (*) VALUES (1, 'a', 1) ;
```

如果你想将数据插入除列 `b` 之外的所有列，可以使用 `EXCEPT` 关键字。参照上述语法，你需要确保插入的值数量 (`VALUES (v11, v13)`) 与指定的列数量 (`(c1, c3)`) 相同：

```sql
INSERT INTO insert_select_testtable (* EXCEPT(b)) Values (2, 2);
```

```sql
SELECT * FROM insert_select_testtable;
```

```text
┌─a─┬─b─┬─c─┐
│ 2 │   │ 2 │
└───┴───┴───┘
┌─a─┬─b─┬─c─┐
│ 1 │ a │ 1 │
└───┴───┴───┘
```

在此示例中，我们可以看到，第二个插入的行中，`a` 和 `c` 列使用传入的值填充，而 `b` 则使用默认值填充。也可以使用 `DEFAULT` 关键字来插入默认值：

```sql
INSERT INTO insert_select_testtable VALUES (1, DEFAULT, 1) ;
```

如果列列表未包含所有已有列，其余列将按以下方式填充：

* 使用表定义中指定的 `DEFAULT` 表达式计算出的值。
* 如果未定义 `DEFAULT` 表达式，则填充为 0 和空字符串。

数据可以通过 ClickHouse 支持的任意[格式](/zh/sql-reference/formats)传递给 INSERT。必须在查询中显式指定格式：

```sql
INSERT INTO [db.]table [(c1, c2, c3)] FORMAT format_name data_set
```

例如，以下查询格式与 `INSERT ... VALUES` 的基础版完全一致：

```sql
INSERT INTO [db.]table [(c1, c2, c3)] FORMAT Values (v11, v12, v13), (v21, v22, v23), ...
```

ClickHouse 会移除数据前的所有空格以及一个换行符 (如果有的话) 。构造查询时，建议将数据放在查询运算符后的新一行中；如果数据以空格开头，这一点尤为重要。

示例：

```sql
INSERT INTO t FORMAT TabSeparated
11  Hello, world!
22  Qwerty
```

您可以使用[命令行客户端](/zh/operations/utilities/clickhouse-local)或[HTTP 接口](/zh/interfaces/http)在查询之外单独插入数据。

:::note
如果您想为 `INSERT` 查询指定 `SETTINGS`，则必须在 `FORMAT` 子句*之前*指定，因为 `FORMAT format_name` 之后的所有内容都会被视为数据。例如：

```sql
INSERT INTO table SETTINGS ... FORMAT format_name data_set
```

:::

<div id="constraints">
  ## 约束
</div>

如果某个表定义了[约束](../../sql-reference/statements/create/table.md#constraints)，系统会对插入数据的每一行检查其表达式。如果其中任何一个约束不满足，服务器将引发异常，并在异常中包含约束名称和表达式，同时终止该查询。

<div id="data-type-validation">
  ## 数据类型验证
</div>

ClickHouse 仅在创建表 (`CREATE TABLE`) 和修改 schema (`ALTER TABLE`) 时，才会校验是否允许使用相应的数据类型 (由 `enable_time_time64_type`、`allow_suspicious_low_cardinality_types`、`allow_suspicious_fixed_string_types` 等设置控制) ；不会在 `INSERT` 期间进行校验。

这意味着，如果某个包含不允许的数据类型的表已经存在，那么即使服务器上禁用了相应设置，数据仍然可以插入其中。这是有意为之——表一旦创建完成，插入操作就不应被控制类型创建的设置阻止。

例如：

```sql
SET enable_time_time64_type = 1;

CREATE TABLE events
(
    `id` UInt64,
    `event_time` Time
)
ENGINE = MergeTree()
ORDER BY id;

SET enable_time_time64_type = 0;

-- This works even though the setting is now disabled.
-- The table already exists, so inserts are not blocked.
INSERT INTO events VALUES (1, '14:30:25');

-- But creating a new table with the Time type will fail.
CREATE TABLE events_new
(
    `id` UInt64,
    `event_time` Time
)
ENGINE = MergeTree()
ORDER BY id; -- ERR: TYPE_TIME_TIME64_IS_NOT_ENABLED
```

:::note
因此，只要目标表中已经有对应的列类型，较新版本的客户端 (其中某项设置默认启用) 就可以向较旧版本的服务器 (其中该设置已禁用) 插入包含不允许的数据类型的数据。校验是在 DDL 层面执行的，而不是在 DML 层面。
:::

<div id="inserting-the-results-of-select">
  ## 插入 SELECT 查询结果
</div>

**语法**

```sql
INSERT INTO [TABLE] [db.]table [(c1, c2, c3)] SELECT ...
```

列会按其在 `SELECT` 子句中的位置进行映射。不过，它们在 `SELECT` 表达式中的名称可以与 `INSERT` 目标表中的名称不同。必要时会执行类型转换。

除 Values format 外，其他任何 format 都不允许将值设置为 `now()`、`1 + 2` 等 表达式。Values format 允许有限度地使用 表达式，但不建议这样做，因为这种情况下会使用低效代码来执行这些 表达式。

不支持其他修改数据分区片段的查询：`UPDATE`、`DELETE`、`REPLACE`、`MERGE`、`UPSERT`、`INSERT UPDATE`。
不过，你可以使用 `ALTER TABLE ... DROP PARTITION` 删除旧数据。

如果 `SELECT` 子句包含表函数 [input()](../../sql-reference/table-functions/input.md)，则必须在查询末尾指定 `FORMAT` 子句。

若要向非 Nullable 数据类型的列中插入默认值而不是 `NULL`，请启用 [insert&#95;null&#95;as&#95;default](../../operations/settings/settings.md#insert_null_as_default) 设置。

`INSERT` 也支持 CTE (公用表表达式) 。例如，以下两条语句是等价的：

```sql
INSERT INTO x WITH y AS (SELECT * FROM numbers(10)) SELECT * FROM y;
WITH y AS (SELECT * FROM numbers(10)) INSERT INTO x SELECT * FROM y;
```

<div id="inserting-data-from-a-file">
  ## 从文件插入数据
</div>

**语法**

```sql
INSERT INTO [TABLE] [db.]table [(c1, c2, c3)] FROM INFILE file_name [COMPRESSION type] [SETTINGS ...] [FORMAT format_name]
```

使用上述语法可从存储在**客户端**侧的一个或多个文件中插入数据。`file_name` 和 `type` 是字符串字面量。输入文件的[格式](../../interfaces/formats.md)必须在 `FORMAT` 子句中设置。

支持压缩文件。压缩类型会根据文件扩展名自动识别，也可以在 `COMPRESSION` 子句中显式指定。支持的类型有：`'none'`、`'gzip'`、`'deflate'`、`'br'`、`'xz'`、`'zstd'`、`'lz4'`、`'bz2'`。

此功能可在[命令行客户端](../../interfaces/client.md)和 [clickhouse-local](../../operations/utilities/clickhouse-local.md) 中使用。

**示例**

<div id="single-file-with-from-infile">
  ### 使用 FROM INFILE 导入单个文件
</div>

使用[命令行客户端](../../interfaces/client.md)执行以下查询：

```bash title="Query"
echo 1,A > input.csv ; echo 2,B >> input.csv
clickhouse-client --query="CREATE TABLE table_from_file (id UInt32, text String) ENGINE=MergeTree() ORDER BY id;"
clickhouse-client --query="INSERT INTO table_from_file FROM INFILE 'input.csv' FORMAT CSV;"
clickhouse-client --query="SELECT * FROM table_from_file FORMAT PrettyCompact;"
```

```text title="Response"
┌─id─┬─text─┐
│  1 │ A    │
│  2 │ B    │
└────┴──────┘
```

<div id="multiple-files-with-from-infile-using-globs">
  ### 使用通配符通过 FROM INFILE 处理多个文件
</div>

此示例与前一个非常相似，不过这里使用 `FROM INFILE 'input_*.csv'` 从多个文件中执行插入操作。

```bash
echo 1,A > input_1.csv ; echo 2,B > input_2.csv
clickhouse-client --query="CREATE TABLE infile_globs (id UInt32, text String) ENGINE=MergeTree() ORDER BY id;"
clickhouse-client --query="INSERT INTO infile_globs FROM INFILE 'input_*.csv' FORMAT CSV;"
clickhouse-client --query="SELECT * FROM infile_globs FORMAT PrettyCompact;"
```

:::tip
除了使用 `*` 选择多个文件外，还可以使用范围 (`{1,2}` 或 `{1..9}`) 以及其他[通配符替换](/zh/sql-reference/table-functions/file.md/#globs-in-path)。以下三种写法都适用于上面的示例：

```sql
INSERT INTO infile_globs FROM INFILE 'input_*.csv' FORMAT CSV;
INSERT INTO infile_globs FROM INFILE 'input_{1,2}.csv' FORMAT CSV;
INSERT INTO infile_globs FROM INFILE 'input_?.csv' FORMAT CSV;
```

:::

<div id="inserting-using-a-table-function">
  ## 使用表函数进行插入
</div>

可以将数据插入到由[表函数](../../sql-reference/table-functions/index.md)引用的表中。

**语法**

```sql
INSERT INTO [TABLE] FUNCTION table_func ...
```

**示例**

以下查询中使用了 [remote](/zh/sql-reference/table-functions/remote) 表函数：

```sql title="Query"
CREATE TABLE simple_table (id UInt32, text String) ENGINE=MergeTree() ORDER BY id;
INSERT INTO TABLE FUNCTION remote('localhost', default.simple_table)
    VALUES (100, 'inserted via remote()');
SELECT * FROM simple_table;
```

```text title="Response"
┌──id─┬─text──────────────────┐
│ 100 │ inserted via remote() │
└─────┴───────────────────────┘
```

<div id="inserting-into-clickhouse-cloud">
  ## 向 ClickHouse Cloud 插入数据
</div>

默认情况下，ClickHouse Cloud 中的服务会提供多个副本以实现高可用性。当你连接到某个服务时，系统会与其中一个副本建立连接。

`INSERT` 成功后，数据会写入底层存储。不过，其他副本接收这些更新可能需要一些时间。因此，如果你使用另一个连接，并在其他某个副本上执行 `SELECT` 查询，更新后的数据可能暂时还不可见。

可以使用 `select_sequential_consistency` 强制副本接收最新更新。下面是一个使用此设置的 `SELECT` 查询示例：

```sql
SELECT .... SETTINGS select_sequential_consistency = 1;
```

请注意，使用 `select_sequential_consistency` 会增加 ClickHouse Keeper (ClickHouse Cloud 在内部使用该组件) 的负载，并且可能会因服务负载而导致性能下降。除非确有必要，否则我们不建议启用此设置。推荐的做法是在同一会话中执行读/写操作，或者使用采用原生协议的客户端驱动程序 (因此支持粘性连接) 。

<div id="inserting-into-a-replicated-setup">
  ## 在复制配置中插入数据
</div>

在复制配置中，数据完成复制后才会在其他副本上可见。执行 `INSERT` 后，数据会立即开始复制 (即下载到其他副本) 。这与 ClickHouse Cloud 不同：在 ClickHouse Cloud 中，数据会立即写入共享存储，副本则订阅元数据变更。

请注意，在复制配置中，`INSERTs` 有时可能需要较长时间 (约 1 秒) ，因为它需要向 ClickHouse Keeper 提交以达成分布式共识。使用 S3 作为存储也会带来额外延迟。

<div id="performance-considerations">
  ## 性能注意事项
</div>

`INSERT` 会按主键对输入数据排序，并根据分区键将其拆分到不同分区中。如果一次将数据插入多个分区，`INSERT` 查询的性能可能会明显下降。为避免这种情况：

* 尽量以较大的批次写入数据，例如每次 100,000 行。
* 在将数据上传到 ClickHouse 之前，先按分区键对数据进行分组。

在以下情况下，性能通常不会下降：

* 实时写入数据。
* 上传通常已按时间排序的数据。

<div id="asynchronous-inserts">
  ### 异步插入
</div>

对于小规模但高频的数据写入，可以使用异步插入。此类写入的数据会先合并为批次，然后再安全地插入表中。要使用异步插入，请启用 [`async_insert`](/zh/operations/settings/settings#async_insert) 设置。

使用 `async_insert` 或 [`Buffer` 表引擎](/zh/engines/table-engines/special/buffer) 都会带来额外的缓冲。

<div id="large-or-long-running-inserts">
  ### 大规模或长时间运行的插入
</div>

当插入大量数据时，ClickHouse 会通过一种称为“squashing”的过程来优化写入性能。内存中较小的已插入数据块会先被合并并压缩成更大的块，然后再写入磁盘。Squashing 可减少每次写入操作带来的额外开销。在此过程中，ClickHouse 每完成写入 [`max_insert_block_size`](/zh/operations/settings/settings#max_insert_block_size) 行后，这些已插入的数据就可供查询。

**另请参见**

* [async&#95;insert](/zh/operations/settings/settings#async_insert)
* [wait&#95;for&#95;async&#95;insert](/zh/operations/settings/settings#wait_for_async_insert)
* [wait&#95;for&#95;async&#95;insert&#95;timeout](/zh/operations/settings/settings#wait_for_async_insert_timeout)
* [async&#95;insert&#95;max&#95;data&#95;size](/zh/operations/settings/settings#async_insert_max_data_size)
* [async&#95;insert&#95;busy&#95;timeout&#95;ms](/zh/operations/settings/settings#async_insert_busy_timeout_max_ms)
* [async&#95;insert&#95;stale&#95;timeout&#95;ms](/zh/operations/settings/settings#async_insert_max_data_size)