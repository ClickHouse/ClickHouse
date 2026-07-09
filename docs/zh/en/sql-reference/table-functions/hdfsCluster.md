---
description: '允许从指定 集群 中的多个节点并行处理 HDFS 中的文件。'
sidebar_label: 'hdfsCluster'
sidebar_position: 81
slug: /sql-reference/table-functions/hdfsCluster
title: 'hdfsCluster'
doc_type: 'reference'
---

允许从指定 集群 中的多个节点并行处理 HDFS 中的文件。在 initiator 节点上，它会与 集群 中的所有节点建立 connection，展开 HDFS 文件路径中的 asterisk，并动态分发各个文件。在工作线程所在节点上，它会向 initiator 节点请求下一个要处理的 task 并执行处理。该过程会重复进行，直到所有 task 都处理完成。

<div id="syntax">
  ## 语法
</div>

```sql
hdfsCluster(cluster_name, URI, format, structure)
```

<div id="arguments">
  ## 参数
</div>

| 参数             | 说明                                                                                                                                                                                                     |
| -------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `cluster_name` | 用于构建远程和本地服务器地址集合及连接参数的集群名称。                                                                                                                                                                            |
| `URI`          | 指向单个文件或一组文件的 URI。在 `readonly` 模式下支持以下通配符：`*`、`**`、`?`、`{'abc','def'}` 和 `{N..M}`，其中 `N`、`M` 为数字，`abc`、`def` 为字符串。更多信息，请参见 [路径中的通配符](../../engines/table-engines/integrations/s3.md#wildcards-in-path)。 |
| `format`       | 文件的[格式](/zh/sql-reference/formats)。                                                                                                                                                                       |
| `structure`    | 表的结构。格式为 `'column1_name column1_type, column2_name column2_type, ...'`。                                                                                                                                |

<div id="returned_value">
  ## 返回值
</div>

返回一个具有指定结构的表，用于读取指定文件中的数据。

<div id="examples">
  ## 示例
</div>

1. 假设我们有一个名为 `cluster_simple` 的 ClickHouse 集群，以及 HDFS 上 URI 如下的多个文件：

* &#39;hdfs://hdfs1:9000/some&#95;dir/some&#95;file&#95;1&#39;
* &#39;hdfs://hdfs1:9000/some&#95;dir/some&#95;file&#95;2&#39;
* &#39;hdfs://hdfs1:9000/some&#95;dir/some&#95;file&#95;3&#39;
* &#39;hdfs://hdfs1:9000/another&#95;dir/some&#95;file&#95;1&#39;
* &#39;hdfs://hdfs1:9000/another&#95;dir/some&#95;file&#95;2&#39;
* &#39;hdfs://hdfs1:9000/another&#95;dir/some&#95;file&#95;3&#39;

2. 查询这些文件中的行数：

```sql
SELECT count(*)
FROM hdfsCluster('cluster_simple', 'hdfs://hdfs1:9000/{some,another}_dir/some_file_{1..3}', 'TSV', 'name String, value UInt32')
```

3. 查询这两个目录下所有文件中的行数：

```sql
SELECT count(*)
FROM hdfsCluster('cluster_simple', 'hdfs://hdfs1:9000/{some,another}_dir/*', 'TSV', 'name String, value UInt32')
```

:::note
如果文件列表中包含带前导零的数字范围，请对每一位数字分别使用花括号写法，或使用 `?`。
:::

<div id="related">
  ## 相关内容
</div>

* [HDFS 引擎](../../engines/table-engines/integrations/hdfs.md)
* [HDFS 表函数](../../sql-reference/table-functions/hdfs.md)