---
description: 'File 表引擎将数据以受支持的文件格式之一（`TabSeparated`、`Native` 等）存储在文件中。'
sidebar_label: 'File'
sidebar_position: 40
slug: /engines/table-engines/special/file
title: 'File 表引擎'
doc_type: 'reference'
---

File 表引擎将数据以受支持的[文件格式](/zh/interfaces/formats#formats-overview)之一 (`TabSeparated`、`Native` 等) 存储在文件中。

使用场景：

* 将数据从 ClickHouse 导出到文件。
* 将数据从一种格式转换为另一种格式。
* 通过编辑磁盘上的文件来更新 ClickHouse 中的数据。

:::note
该引擎目前在 ClickHouse Cloud 中不可用，请[改用 S3 表函数](/zh/sql-reference/table-functions/s3.md)。
:::

<div id="usage-in-clickhouse-server">
  ## ClickHouse Server 中的使用情况
</div>

```sql
File(Format)
```

`Format` 参数用于指定一种可用的文件格式。要执行
`SELECT` 查询，该格式必须支持输入；要执行
`INSERT` 查询，则必须支持输出。可用格式列在
[Formats](/zh/interfaces/formats#formats-overview)部分。

ClickHouse 不允许为 `File` 表引擎指定文件系统路径。它会使用服务器配置中由 [path](../../../operations/server-configuration-parameters/settings.md) 设置定义的文件夹。

使用 `File(Format)` 创建表时，会在该文件夹中创建一个空子目录。当数据写入该表时，数据会被写入该子目录中的 `data.Format` 文件。

你也可以在服务器文件系统中手动创建这个子目录和文件，然后将其 [ATTACH](../../../sql-reference/statements/attach.md) 到同名表的元数据中，这样就可以从该文件查询数据。

:::note
请谨慎使用此功能，因为 ClickHouse 不会跟踪对此类文件的外部更改。通过 ClickHouse 和在 ClickHouse 外部同时写入这类文件，其结果是未定义的。
:::

<div id="example">
  ## 示例
</div>

**1.** 创建 `file_engine_table` 表：

```sql
CREATE TABLE file_engine_table (name String, value UInt32) ENGINE=File(TabSeparated)
```

默认情况下，ClickHouse 会创建文件夹 `/var/lib/clickhouse/data/default/file_engine_table`。

**2.** 手动创建 `/var/lib/clickhouse/data/default/file_engine_table/data.TabSeparated`，其内容如下：

```bash
$ cat data.TabSeparated
one 1
two 2
```

**3.** 查询数据：

```sql
SELECT * FROM file_engine_table
```

```text
┌─name─┬─value─┐
│ one  │     1 │
│ two  │     2 │
└──────┴───────┘
```

<div id="usage-in-clickhouse-local">
  ## 在 ClickHouse-local 中的用法
</div>

在 [clickhouse-local](../../../operations/utilities/clickhouse-local.md) 中，File 表引擎除 `Format` 外还接受文件路径。默认输入/输出流既可以用数字名称指定，也可以用 human-readable 名称指定，例如 `0` 或 `stdin`、`1` 或 `stdout`。此外，还可以根据额外的引擎参数或文件扩展名 (`gz`、`br` 或 `xz`) 读取和写入压缩文件。

**示例：**

```bash
$ echo -e "1,2\n3,4" | clickhouse-local -q "CREATE TABLE table (a Int64, b Int64) ENGINE = File(CSV, stdin); SELECT a, b FROM table; DROP TABLE table"
```

<div id="details-of-implementation">
  ## 实现细节
</div>

* 可并发执行多个 `SELECT` 查询，但 `INSERT` 查询之间会相互等待。
* 支持使用 `INSERT` 查询创建新文件。
* 如果文件已存在，`INSERT` 会在其中追加新值。
* 不支持：
  * `ALTER`
  * `SELECT ... SAMPLE`
  * 索引
  * 复制

<div id="partition-by">
  ## PARTITION BY
</div>

`PARTITION BY` — 可选。可以按分区键对数据进行分区，从而创建单独的文件。在大多数情况下，你不需要分区键；即使确实需要，通常也没必要细到超过按月分区。分区并不会加快查询速度 (这与 `ORDER BY` 表达式不同) 。切勿使用粒度过细的分区。不要按客户端标识符或名称对数据进行分区 (应改为将客户端标识符或名称设为 `ORDER BY` 表达式中的第一列) 。

如果按月分区，请使用 `toYYYYMM(date_column)` 表达式，其中 `date_column` 是一个 [Date](/zh/sql-reference/data-types/date.md) 类型的日期列。这里的分区名称采用 `"YYYYMM"` 格式。

<div id="virtual-columns">
  ## 虚拟列
</div>

* `_path` — 文件路径。类型：`LowCardinality(String)`。
* `_file` — 文件名。类型：`LowCardinality(String)`。
* `_size` — 文件大小 (以字节为单位) 。类型：`Nullable(UInt64)`。如果大小未知，则值为 `NULL`。
* `_time` — 文件的最后修改时间。类型：`Nullable(DateTime)`。如果时间未知，则值为 `NULL`。

<div id="settings">
  ## 设置
</div>

* [engine&#95;file&#95;empty&#95;if&#95;not&#95;exists](/zh/operations/settings/settings#engine_file_empty_if_not_exists) - 允许从不存在的文件中读取空数据。默认禁用。
* [engine&#95;file&#95;truncate&#95;on&#95;insert](/zh/operations/settings/settings#engine_file_truncate_on_insert) - 允许在 insert 之前截断文件。默认禁用。
* [engine&#95;file&#95;allow&#95;create&#95;multiple&#95;files](/zh/operations/settings/settings.md#engine_file_allow_create_multiple_files) - 如果 format 带有后缀，则允许每次 insert 时创建新文件。默认禁用。
* [engine&#95;file&#95;skip&#95;empty&#95;files](/zh/operations/settings/settings.md#engine_file_skip_empty_files) - 允许在读取时跳过空文件。默认禁用。
* [storage&#95;file&#95;read&#95;method](/zh/operations/settings/settings#engine_file_empty_if_not_exists) - 从存储文件读取数据的方法，可选值包括：`read`、`pread`、`mmap`。`mmap` 方法不适用于 clickhouse-server (它用于 clickhouse-local) 。默认值：clickhouse-server 为 `pread`，clickhouse-local 为 `mmap`。