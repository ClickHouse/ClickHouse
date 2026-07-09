---
description: '支持在集群内多个节点上并行处理与指定路径匹配的文件。发起节点会与工作节点建立连接，展开文件路径中的通配符，并将文件读取任务分派给工作节点。每个工作节点都会向发起节点请求下一个要处理的文件，如此循环，直到所有任务完成（即所有文件均已读取）。'
sidebar_label: 'fileCluster'
sidebar_position: 61
slug: /sql-reference/table-functions/fileCluster
title: 'fileCluster'
doc_type: 'reference'
---

支持在集群内多个节点上并行处理与指定路径匹配的文件。发起节点会与工作节点建立连接，展开文件路径中的通配符，并将文件读取任务分派给工作节点。每个工作节点都会向发起节点请求下一个要处理的文件，如此循环，直到所有任务完成 (即所有文件均已读取) 。

:::note
只有当所有节点上与最初指定路径匹配的文件集合完全一致，且这些文件在不同节点上的内容也保持一致时，此函数才能&#95;正确&#95;运行。
如果这些文件在各节点之间存在差异，则返回值无法预先确定，并且取决于工作节点向发起节点请求任务的顺序。
:::

<div id="syntax">
  ## 语法
</div>

```sql
fileCluster(cluster_name, path[, format, structure, compression_method])
```

<div id="arguments">
  ## 参数
</div>

| 参数                   | 说明                                                                                                                                          |
| -------------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| `cluster_name`       | 用于构建远程和本地服务器地址集合及连接参数的集群名称。                                                                                                                 |
| `path`               | 相对于 [user&#95;files&#95;path](/zh/operations/server-configuration-parameters/settings.md#user_files_path) 的文件路径。文件路径也支持 [通配符](#globs-in-path)。 |
| `format`             | 文件的[格式](/zh/sql-reference/formats)。类型：[String](../../sql-reference/data-types/string.md)。                                                      |
| `structure`          | `'UserID UInt64, Name String'` 格式的表结构。用于确定列名和类型。类型：[String](../../sql-reference/data-types/string.md)。                                      |
| `compression_method` | 压缩方法。支持的压缩类型包括 `gz`、`br`、`xz`、`zst`、`lz4` 和 `bz2`。                                                                                          |

<div id="returned_value">
  ## 返回值
</div>

一个采用指定格式和结构、并包含与指定 path 匹配的文件中数据的表。

**示例**

给定一个名为 `my_cluster` 的集群，以及设置项 `user_files_path` 的以下值：

```bash
$ grep user_files_path /etc/clickhouse-server/config.xml
    <user_files_path>/var/lib/clickhouse/user_files/</user_files_path>
```

另外，假设各集群节点的 `user_files_path` 中都存在文件 `test1.csv` 和 `test2.csv`，且它们在不同节点上的内容完全相同：

```bash
$ cat /var/lib/clickhouse/user_files/test1.csv
    1,"file1"
    11,"file11"

$ cat /var/lib/clickhouse/user_files/test2.csv
    2,"file2"
    22,"file22"
```

例如，可以在集群的每个节点上执行以下两条查询来创建这些文件：

```sql
INSERT INTO TABLE FUNCTION file('file1.csv', 'CSV', 'i UInt32, s String') VALUES (1,'file1'), (11,'file11');
INSERT INTO TABLE FUNCTION file('file2.csv', 'CSV', 'i UInt32, s String') VALUES (2,'file2'), (22,'file22');
```

现在，通过 `fileCluster` 表函数读取 `test1.csv` 和 `test2.csv` 中的数据内容：

```sql
SELECT * FROM fileCluster('my_cluster', 'file{1,2}.csv', 'CSV', 'i UInt32, s String') ORDER BY i, s
```

```response
┌──i─┬─s──────┐
│  1 │ file1  │
│ 11 │ file11 │
└────┴────────┘
┌──i─┬─s──────┐
│  2 │ file2  │
│ 22 │ file22 │
└────┴────────┘
```

<div id="globs-in-path">
  ## 路径中的通配符
</div>

FileCluster 支持 [File](../../sql-reference/table-functions/file.md#globs-in-path) 表函数支持的所有通配模式。

<div id="related">
  ## 相关内容
</div>

* [File 表函数](../../sql-reference/table-functions/file.md)