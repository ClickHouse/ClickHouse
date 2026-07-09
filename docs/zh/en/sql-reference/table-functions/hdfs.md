---
description: '从 HDFS 中的文件创建表。此表函数与 url 和 file 表函数类似。'
sidebar_label: 'hdfs'
sidebar_position: 80
slug: /sql-reference/table-functions/hdfs
title: 'hdfs'
doc_type: 'reference'
---

import ExperimentalBadge from '@theme/badges/ExperimentalBadge';
import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="hdfs-table-function">
  # hdfs 表函数
</div>

从 HDFS 中的文件创建一个表。该表函数与 [url](../../sql-reference/table-functions/url.md) 和 [file](../../sql-reference/table-functions/file.md) 表函数类似。

<div id="syntax">
  ## 语法
</div>

```sql
hdfs(URI, format, structure)
```

<div id="arguments">
  ## 参数
</div>

| Argument    | Description                                                                                           |
| ----------- | ----------------------------------------------------------------------------------------------------- |
| `URI`       | HDFS 中文件的相对 URI。文件路径在只读模式下支持以下通配符：`*`、`?`、`{abc,def}` 和 `{N..M}`，其中 `N`、`M` 为数字，`'abc'`、`'def'` 为字符串。 |
| `format`    | 文件的[格式](/zh/sql-reference/formats)。                                                                      |
| `structure` | 表的结构。格式为 `'column1_name column1_type, column2_name column2_type, ...'`。                               |

<div id="returned_value">
  ## 返回值
</div>

一个具有指定结构的表，可用于读取或写入指定文件中的数据。

**示例**

来自 `hdfs://hdfs1:9000/test` 的表，以及从中选取前两行：

```sql
SELECT *
FROM hdfs('hdfs://hdfs1:9000/test', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
LIMIT 2
```

```text
┌─column1─┬─column2─┬─column3─┐
│       1 │       2 │       3 │
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

<div id="globs_in_path">
  ## 路径中的通配符
</div>

路径中可以使用通配符。文件必须匹配整个路径模式，而不仅仅是后缀或前缀。

* `*` — 表示任意多个字符 (不包括 `/`) ，也包括空字符串。
* `**` — 表示递归匹配文件夹内的所有文件。
* `?` — 表示任意单个字符。
* `{some_string,another_string,yet_another_one}` — 替换为字符串 `'some_string'`、`'another_string'`、`'yet_another_one'` 中的任意一个。这些字符串可以包含 `/` 符号。
* `{N..M}` — 表示任意一个 `>= N` 且 `<= M` 的数字。

使用 `{}` 的写法与 [remote](remote.md) 和 [file](file.md) 表函数类似。

**示例**

1. 假设我们在 HDFS 上有多个文件，其 URI 如下：

* &#39;hdfs://hdfs1:9000/some&#95;dir/some&#95;file&#95;1&#39;
* &#39;hdfs://hdfs1:9000/some&#95;dir/some&#95;file&#95;2&#39;
* &#39;hdfs://hdfs1:9000/some&#95;dir/some&#95;file&#95;3&#39;
* &#39;hdfs://hdfs1:9000/another&#95;dir/some&#95;file&#95;1&#39;
* &#39;hdfs://hdfs1:9000/another&#95;dir/some&#95;file&#95;2&#39;
* &#39;hdfs://hdfs1:9000/another&#95;dir/some&#95;file&#95;3&#39;

2. 查询这些文件中的行数：

{/* */ }

```sql
SELECT count(*)
FROM hdfs('hdfs://hdfs1:9000/{some,another}_dir/some_file_{1..3}', 'TSV', 'name String, value UInt32')
```

3. 查询这两个目录下所有文件的行数：

{/* */ }

```sql
SELECT count(*)
FROM hdfs('hdfs://hdfs1:9000/{some,another}_dir/*', 'TSV', 'name String, value UInt32')
```

:::note
如果文件列表中包含带前导零的数字范围，请对每一位分别使用花括号写法，或使用 `?`。
:::

**示例**

查询名为 `file000`、`file001`、...、`file999` 的文件中的数据：

```sql
SELECT count(*)
FROM hdfs('hdfs://hdfs1:9000/big_dir/file{0..9}{0..9}{0..9}', 'CSV', 'name String, value UInt32')
```

<div id="virtual-columns">
  ## 虚拟列
</div>

* `_path` — 文件路径。类型：`LowCardinality(String)`。
* `_file` — 文件名。类型：`LowCardinality(String)`。
* `_size` — 文件大小 (单位为字节) 。类型：`Nullable(UInt64)`。如果大小未知，则值为 `NULL`。
* `_time` — 文件的最后修改时间。类型：`Nullable(DateTime)`。如果时间未知，则值为 `NULL`。

<div id="hive-style-partitioning">
  ## `use_hive_partitioning` 设置
</div>

当 `use_hive_partitioning` 设置为 1 时，ClickHouse 会检测路径中的 Hive 风格分区 (`/name=value/`) ，并允许在查询中将分区列作为虚拟列使用。这些虚拟列的名称将与分区路径中的名称相同。

**示例**

使用通过 Hive 风格分区生成的虚拟列

```sql
SELECT * FROM HDFS('hdfs://hdfs1:9000/data/path/date=*/country=*/code=*/*.parquet') WHERE date > '2020-01-01' AND country = 'Netherlands' AND code = 42;
```

<div id="storage-settings">
  ## 存储设置
</div>

* [hdfs&#95;truncate&#95;on&#95;insert](/zh/operations/settings/settings.md#hdfs_truncate_on_insert) - 允许在插入前先截断文件。默认禁用。
* [hdfs&#95;create&#95;new&#95;file&#95;on&#95;insert](/zh/operations/settings/settings.md#hdfs_create_new_file_on_insert) - 如果 format 带有后缀，则允许每次插入时创建新文件。默认禁用。
* [hdfs&#95;skip&#95;empty&#95;files](/zh/operations/settings/settings.md#hdfs_skip_empty_files) - 允许在读取时跳过空文件。默认禁用。

<div id="related">
  ## 相关
</div>

* [虚拟列](../../engines/table-engines/index.md#table_engines-virtual_columns)