---
description: '一种表引擎，提供类似表的接口，可从文件中 SELECT 并向文件中 INSERT，类似于 `s3` 表函数。处理本地文件时使用 `file`，处理 S3、GCS 或 MinIO 等对象存储中的桶时使用 `s3`。'
sidebar_label: 'file'
sidebar_position: 60
slug: /sql-reference/table-functions/file
title: 'file'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="file-table-function">
  # file 表函数
</div>

一种表引擎，提供类似表的接口，可对文件执行 `SELECT` 和 `INSERT`，类似于 [s3](/zh/sql-reference/table-functions/s3.md) 表函数。处理本地文件时使用 `file`，处理 S3、GCS 或 MinIO 等对象存储中的桶时使用 `s3`。

`file` 函数可在 `SELECT` 和 `INSERT` 查询中使用，用于从文件读取数据或将数据写入文件。

<div id="syntax">
  ## 语法
</div>

```sql
file([path_to_archive ::] path [,format] [,structure] [,compression])
```

对于 `SELECT` 查询，`path` 也可以是返回 `Array(String)` 的表达式：

```sql
file(['file1.csv', 'file2.csv'], 'CSV', 'column1 UInt32, column2 UInt32')
```

<div id="arguments">
  ## 参数
</div>

| 参数                | 描述                                                                                                                                                                                                                                                              |
| ----------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `path`            | 相对于 [user&#95;files&#95;path](/zh/operations/server-configuration-parameters/settings.md#user_files_path) 的文件路径，或 `SELECT` 查询中的路径 `Array(String)`。在只读模式下支持以下[通配符](#globs-in-path)：`*`、`?`、`{abc,def}` (其中 `'abc'` 和 `'def'` 为字符串) 以及 `{N..M}` (其中 `N` 和 `M` 为数字) 。 |
| `path_to_archive` | zip/tar/7z 归档文件的相对路径。支持与 `path` 相同的通配符。                                                                                                                                                                                                                         |
| `format`          | 文件的 [format](/zh/interfaces/formats)。                                                                                                                                                                                                                              |
| `structure`       | 表的结构。格式：`'column1_name column1_type, column2_name column2_type, ...'`。                                                                                                                                                                                          |
| `compression`     | 在 `SELECT` 查询中使用时，表示现有压缩类型；在 `INSERT` 查询中使用时，表示所需的压缩类型。支持的压缩类型包括 `gz`、`br`、`xz`、`zst`、`lz4` 和 `bz2`。                                                                                                                                                            |

:::tip
省略 `structure` 参数时，ClickHouse 会从 format 本身推断 schema。
不同的 format 会生成不同的默认列名和类型。
要查看特定 format 的 schema，请使用 [`DESC`](/zh/sql-reference/statements/describe-table) 和 [`format`](/zh/sql-reference/table-functions/format) 表函数。

例如：

```sql
DESC format(LineAsString, 'Hello\nWorld')
```

```response
┌─name─┬─type───┬─default_type─┬─default_expression─┬─comment─┬─codec_expression─┬─ttl_expression─┐
│ line │ String │              │                    │         │                  │                │
└──────┴────────┴──────────────┴────────────────────┴─────────┴──────────────────┴────────────────┘
```

:::

<div id="returned_value">
  ## 返回值
</div>

一个可用于读取或写入文件中数据的表。

<div id="examples-for-writing-to-a-file">
  ## 写入文件示例
</div>

<div id="write-to-a-tsv-file">
  ### 写入 TSV 文件
</div>

```sql
INSERT INTO TABLE FUNCTION
file('test.tsv', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
VALUES (1, 2, 3), (3, 2, 1), (1, 3, 2)
```

因此，数据将写入文件 `test.tsv`：

```bash
# cat /var/lib/clickhouse/user_files/test.tsv
1    2    3
3    2    1
1    3    2
```

<div id="partitioned-write-to-multiple-tsv-files">
  ### 按分区写入多个 TSV 文件
</div>

如果在向 `file` 类型的表函数插入数据时指定了 `PARTITION BY` 表达式，则会为每个分区分别创建一个文件。将数据拆分到多个独立文件中，有助于提升读取操作的性能。

```sql
INSERT INTO TABLE FUNCTION
file('test_{_partition_id}.tsv', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
PARTITION BY column3
VALUES (1, 2, 3), (3, 2, 1), (1, 3, 2)
```

因此，数据会写入三个文件：`test_1.tsv`、`test_2.tsv` 和 `test_3.tsv`。

```bash
# cat /var/lib/clickhouse/user_files/test_1.tsv
3    2    1

# cat /var/lib/clickhouse/user_files/test_2.tsv
1    3    2

# cat /var/lib/clickhouse/user_files/test_3.tsv
1    2    3
```

<div id="examples-for-reading-from-a-file">
  ## 从文件读取示例
</div>

<div id="select-from-a-csv-file">
  ### 从 CSV 文件中执行 SELECT
</div>

首先，在服务器配置中设置 `user_files_path`，并准备好文件 `test.csv`：

```bash
$ grep user_files_path /etc/clickhouse-server/config.xml
    <user_files_path>/var/lib/clickhouse/user_files/</user_files_path>

$ cat /var/lib/clickhouse/user_files/test.csv
    1,2,3
    3,2,1
    78,43,45
```

然后，将 `test.csv` 中的数据读入表中，并选取前两行：

```sql
SELECT * FROM
file('test.csv', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
LIMIT 2;
```

```text
┌─column1─┬─column2─┬─column3─┐
│       1 │       2 │       3 │
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

<div id="inserting-data-from-a-file-into-a-table">
  ### 将文件中的数据插入表中
</div>

```sql
INSERT INTO FUNCTION
file('test.csv', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
VALUES (1, 2, 3), (3, 2, 1);
```

```sql
SELECT * FROM
file('test.csv', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32');
```

```text
┌─column1─┬─column2─┬─column3─┐
│       1 │       2 │       3 │
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

从位于 `archive1.zip` 和/或 `archive2.zip` 中的 `table.csv` 读取数据：

```sql
SELECT * FROM file('user_files/archives/archive{1..2}.zip :: table.csv');
```

<div id="globs-in-path">
  ## 路径中的通配符
</div>

路径可以使用通配符。文件必须匹配整个路径模式，而不只是后缀或前缀。唯一的例外是：如果路径指向一个现有的
目录，且未使用通配符，则会在该路径后隐式添加一个 `*`，从而
选中该目录中的所有文件。

* `*` — 表示任意多个字符 (`/` 除外) ，也包括空字符串。
* `?` — 表示任意单个字符。
* `{some_string,another_string,yet_another_one}` — 替换为字符串 `'some_string'`、`'another_string'`、`'yet_another_one'` 中的任意一个。这些字符串可以包含 `/` 符号。
* `{N..M}` — 表示任意 `>= N` 且 `<= M` 的数字。
* `**` - 表示递归匹配文件夹中的所有文件。

带有 `{}` 的构造与 [remote](remote.md) 和 [hdfs](hdfs.md) 表函数类似。

<div id="examples">
  ## 示例
</div>

**示例**

假设有以下这些文件，它们的相对路径如下：

* `some_dir/some_file_1`
* `some_dir/some_file_2`
* `some_dir/some_file_3`
* `another_dir/some_file_1`
* `another_dir/some_file_2`
* `another_dir/some_file_3`

查询所有文件的总行数：

```sql
SELECT count(*) FROM file('{some,another}_dir/some_file_{1..3}', 'TSV', 'name String, value UInt32');
```

另一种可达到相同效果的路径表达式：

```sql
SELECT count(*) FROM file('{some,another}_dir/*', 'TSV', 'name String, value UInt32');
```

使用隐式 `*` 查询 `some_dir` 中的总行数：

```sql
SELECT count(*) FROM file('some_dir', 'TSV', 'name String, value UInt32');
```

:::note
如果文件列表中包含带前导零的数字范围，请对每一位数字分别使用花括号写法，或者使用 `?`。
:::

**示例**

查询名为 `file000`、`file001`、...、`file999` 的文件中的总行数：

```sql
SELECT count(*) FROM file('big_dir/file{0..9}{0..9}{0..9}', 'CSV', 'name String, value UInt32');
```

**示例**

递归查询目录 `big_dir/` 下所有文件的总行数：

```sql
SELECT count(*) FROM file('big_dir/**', 'CSV', 'name String, value UInt32');
```

**示例**

递归查询目录 `big_dir/` 中任意文件夹内所有 `file002` 文件的总行数：

```sql
SELECT count(*) FROM file('big_dir/**/file002', 'CSV', 'name String, value UInt32');
```

<div id="virtual-columns">
  ## 虚拟列
</div>

* `_path` — 文件路径。类型：`LowCardinality(String)`。
* `_file` — 文件名。类型：`LowCardinality(String)`。
* `_size` — 文件大小 (以字节为单位) 。类型：`Nullable(UInt64)`。如果文件大小未知，则值为 `NULL`。
* `_time` — 文件的最后修改时间。类型：`Nullable(DateTime)`。如果时间未知，则值为 `NULL`。

<div id="hive-style-partitioning">
  ## use_hive_partitioning 设置
</div>

当 `use_hive_partitioning` 设置为 1 时，ClickHouse 会检测路径中的 Hive 风格分区 (`/name=value/`) ，并允许在查询中将分区列作为虚拟列使用。这些虚拟列的名称将与分区路径中的名称相同。

**示例**

使用通过 Hive 风格分区生成的虚拟列

```sql
SELECT * FROM file('data/path/date=*/country=*/code=*/*.parquet') WHERE date > '2020-01-01' AND country = 'Netherlands' AND code = 42;
```

<div id="settings">
  ## 设置
</div>

| 设置项                                                                                                                                     | 描述                                                                                                                     |
| --------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| [engine&#95;file&#95;empty&#95;if&#95;not&#95;exists](/zh/operations/settings/settings#engine_file_empty_if_not_exists)                    | 允许从不存在的文件中读取空结果。默认禁用。                                                                                                  |
| [engine&#95;file&#95;truncate&#95;on&#95;insert](/zh/operations/settings/settings#engine_file_truncate_on_insert)                          | 允许在插入前截断文件。默认禁用。                                                                                                       |
| [engine&#95;file&#95;allow&#95;create&#95;multiple&#95;files](/zh/operations/settings/settings.md#engine_file_allow_create_multiple_files) | 如果格式带有后缀，则允许在每次插入时创建新文件。默认禁用。                                                                                          |
| [engine&#95;file&#95;skip&#95;empty&#95;files](/zh/operations/settings/settings.md#engine_file_skip_empty_files)                           | 允许在读取时跳过空文件。默认禁用。                                                                                                      |
| [storage&#95;file&#95;read&#95;method](/zh/operations/settings/settings#engine_file_empty_if_not_exists)                                   | 从存储文件读取数据的方法，可选值包括：read、pread、mmap (仅适用于 clickhouse-local) 。默认值：clickhouse-server 为 `pread`，clickhouse-local 为 `mmap`。 |

<div id="related">
  ## 相关
</div>

* [虚拟列](/zh/engines/table-engines/index.md#table_engines-virtual_columns)
* [处理完成后重命名文件](/zh/operations/settings/settings.md#rename_files_after_processing)