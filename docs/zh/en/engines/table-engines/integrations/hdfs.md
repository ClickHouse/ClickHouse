---
description: '该引擎支持通过 ClickHouse 在 HDFS 上管理数据，从而与 Apache Hadoop 生态系统集成。该引擎与 File 表引擎和 URL 引擎类似，但提供了 Hadoop 特有的功能。'
sidebar_label: 'HDFS'
sidebar_position: 80
slug: /engines/table-engines/integrations/hdfs
title: 'HDFS 表引擎'
doc_type: 'reference'
---

import CloudNotSupportedBadge from '@theme/badges/CloudNotSupportedBadge';

<div id="hdfs-table-engine">
  # HDFS 表引擎
</div>

<CloudNotSupportedBadge />

该引擎允许通过 ClickHouse 管理 [HDFS](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html) 上的数据，从而提供与 [Apache Hadoop](https://en.wikipedia.org/wiki/Apache_Hadoop) 生态系统的集成。该引擎与 [File 表引擎](/zh/engines/table-engines/special/file) 和 [URL](/zh/engines/table-engines/special/url) 引擎类似，但提供了 Hadoop 特有的功能。

此功能不受 ClickHouse 工程师支持，且众所周知质量不太稳定。如遇任何问题，请自行修复并提交拉取请求。

<div id="usage">
  ## 用法
</div>

```sql
ENGINE = HDFS(URI, format)
```

**引擎参数**

* `URI` - HDFS 中的完整文件 URI。`URI` 的路径部分可以包含通配符。在这种情况下，该表将为只读。
* `格式` - 指定一种可用的文件格式。要执行
  `SELECT` 查询，该格式必须支持输入；要执行
  `INSERT` 查询，则必须支持输出。可用格式见
  [格式](/zh/sql-reference/formats#formats-overview) 部分。
* [PARTITION BY expr]

<div id="partition-by">
  ### PARTITION BY
</div>

`PARTITION BY` —— 可选。在大多数情况下，不需要分区键；即使需要，通常也无需使用比按月更细粒度的分区键。分区不会加快查询速度 (这不同于 `ORDER BY` 表达式) 。切勿使用过细粒度的分区。不要按客户端标识符或名称对数据进行分区 (应将客户端标识符或名称作为 `ORDER BY` 表达式中的第一列) 。

如果按月分区，请使用 `toYYYYMM(date_column)` 表达式，其中 `date_column` 是类型为 [Date](/zh/sql-reference/data-types/date.md) 的日期列。此处分区名称采用 `"YYYYMM"` 格式。

**示例：**

**1.** 设置 `hdfs_engine_table` 表：

```sql
CREATE TABLE hdfs_engine_table (name String, value UInt32) ENGINE=HDFS('hdfs://hdfs1:9000/other_storage', 'TSV')
```

**2.** 写入文件：

```sql
INSERT INTO hdfs_engine_table VALUES ('one', 1), ('two', 2), ('three', 3)
```

**3.** 查询数据：

```sql
SELECT * FROM hdfs_engine_table LIMIT 2
```

```text
┌─name─┬─value─┐
│ one  │     1 │
│ two  │     2 │
└──────┴───────┘
```

<div id="implementation-details">
  ## 实现细节
</div>

* 读取和写入可以并行执行。
* 不支持：

  * `ALTER` 和 `SELECT...SAMPLE` 操作。
  * 索引。
  * 支持 [零拷贝](../../../operations/storing-data.md#zero-copy)复制，但不推荐。

  :::note 零拷贝复制尚未达到生产可用状态
  在 ClickHouse 22.8 及更高版本中，零拷贝复制默认处于禁用状态。此功能不建议在生产环境中使用。
  :::

**路径中的通配符**

路径中的多个部分都可以使用通配符。只有存在且匹配整个路径模式的文件才会被处理。文件列表是在 `SELECT` 时确定的 (而不是在 `CREATE` 时) 。

* `*` — 匹配任意数量的任意字符 (`/` 除外) ，也包括空字符串。
* `?` — 匹配任意单个字符。
* `{some_string,another_string,yet_another_one}` — 匹配字符串 `'some_string'`、`'another_string'`、`'yet_another_one'` 中的任意一个。
* `{N..M}` — 匹配从 N 到 M 范围内的任意数字，包括两个端点。

带有 `{}` 的写法与 [remote](../../../sql-reference/table-functions/remote.md) table function 类似。

**示例**

1. 假设我们在 HDFS 上有几个 TSV 格式的文件，其 URI 如下：

   * &#39;hdfs://hdfs1:9000/some&#95;dir/some&#95;file&#95;1&#39;
   * &#39;hdfs://hdfs1:9000/some&#95;dir/some&#95;file&#95;2&#39;
   * &#39;hdfs://hdfs1:9000/some&#95;dir/some&#95;file&#95;3&#39;
   * &#39;hdfs://hdfs1:9000/another&#95;dir/some&#95;file&#95;1&#39;
   * &#39;hdfs://hdfs1:9000/another&#95;dir/some&#95;file&#95;2&#39;
   * &#39;hdfs://hdfs1:9000/another&#95;dir/some&#95;file&#95;3&#39;

2. 可以通过几种方式创建一个包含这六个文件的表：

{/* */ }

```sql
CREATE TABLE table_with_range (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/{some,another}_dir/some_file_{1..3}', 'TSV')
```

另一种方法：

```sql
CREATE TABLE table_with_question_mark (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/{some,another}_dir/some_file_?', 'TSV')
```

该表包含两个目录中的所有文件 (所有文件都应符合查询中所描述的 格式 和 schema) ：

```sql
CREATE TABLE table_with_asterisk (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/{some,another}_dir/*', 'TSV')
```

:::note
如果文件列表中的数字范围带有前导零，请为每一位分别使用花括号语法，或使用 `?`。
:::

**示例**

创建一个表，其文件名为 `file000`、`file001`、...、`file999`：

```sql
CREATE TABLE big_table (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/big_dir/file{0..9}{0..9}{0..9}', 'CSV')
```

<div id="configuration">
  ## 配置
</div>

与 GraphiteMergeTree 类似，HDFS 引擎支持通过 ClickHouse 配置文件进行扩展配置。你可以使用两个配置项：全局级 (`hdfs`) 和用户级 (`hdfs_*`) 。系统会先应用全局配置，然后再应用用户级配置 (如果存在) 。

```xml
<!-- Global configuration options for HDFS engine type -->
<hdfs>
  <hadoop_kerberos_keytab>/tmp/keytab/clickhouse.keytab</hadoop_kerberos_keytab>
  <hadoop_kerberos_principal>clickuser@TEST.CLICKHOUSE.TECH</hadoop_kerberos_principal>
  <hadoop_security_authentication>kerberos</hadoop_security_authentication>
</hdfs>

<!-- Configuration specific for user "root" -->
<hdfs_root>
  <hadoop_kerberos_principal>root@TEST.CLICKHOUSE.TECH</hadoop_kerberos_principal>
</hdfs_root>
```

<div id="configuration-options">
  ### 配置选项
</div>

<div id="supported-by-libhdfs3">
  #### libhdfs3 支持的配置
</div>

| **参数**                                                                  | **默认值**                           |
| ----------------------------------------------------------------------- | --------------------------------- |
| rpc&#95;client&#95;connect&#95;tcpnodelay                               | true                              |
| dfs&#95;client&#95;read&#95;shortcircuit                                | true                              |
| output&#95;replace-datanode-on-failure                                  | true                              |
| input&#95;notretry-another-node                                         | false                             |
| input&#95;localread&#95;mappedfile                                      | true                              |
| dfs&#95;client&#95;use&#95;legacy&#95;blockreader&#95;local             | false                             |
| rpc&#95;client&#95;ping&#95;interval                                    | 10  * 1000                        |
| rpc&#95;client&#95;connect&#95;timeout                                  | 600 * 1000                        |
| rpc&#95;client&#95;read&#95;timeout                                     | 3600 * 1000                       |
| rpc&#95;client&#95;write&#95;timeout                                    | 3600 * 1000                       |
| rpc&#95;client&#95;socket&#95;linger&#95;timeout                        | -1                                |
| rpc&#95;client&#95;connect&#95;retry                                    | 10                                |
| rpc&#95;client&#95;timeout                                              | 3600 * 1000                       |
| dfs&#95;default&#95;replica                                             | 3                                 |
| input&#95;connect&#95;timeout                                           | 600 * 1000                        |
| input&#95;read&#95;timeout                                              | 3600 * 1000                       |
| input&#95;write&#95;timeout                                             | 3600 * 1000                       |
| input&#95;localread&#95;default&#95;buffersize                          | 1 * 1024 * 1024                   |
| dfs&#95;prefetchsize                                                    | 10                                |
| input&#95;read&#95;getblockinfo&#95;retry                               | 3                                 |
| input&#95;localread&#95;blockinfo&#95;cachesize                         | 1000                              |
| input&#95;read&#95;max&#95;retry                                        | 60                                |
| output&#95;default&#95;chunksize                                        | 512                               |
| output&#95;default&#95;packetsize                                       | 64 * 1024                         |
| output&#95;default&#95;write&#95;retry                                  | 10                                |
| output&#95;connect&#95;timeout                                          | 600 * 1000                        |
| output&#95;read&#95;timeout                                             | 3600 * 1000                       |
| output&#95;write&#95;timeout                                            | 3600 * 1000                       |
| output&#95;close&#95;timeout                                            | 3600 * 1000                       |
| output&#95;packetpool&#95;size                                          | 1024                              |
| output&#95;heartbeat&#95;interval                                       | 10 * 1000                         |
| dfs&#95;client&#95;failover&#95;max&#95;attempts                        | 15                                |
| dfs&#95;client&#95;read&#95;shortcircuit&#95;streams&#95;cache&#95;size | 256                               |
| dfs&#95;client&#95;socketcache&#95;expiryMsec                           | 3000                              |
| dfs&#95;client&#95;socketcache&#95;capacity                             | 16                                |
| dfs&#95;default&#95;blocksize                                           | 64 * 1024 * 1024                  |
| dfs&#95;default&#95;uri                                                 | &quot;hdfs://localhost:9000&quot; |
| hadoop&#95;security&#95;authentication                                  | &quot;simple&quot;                |
| hadoop&#95;security&#95;kerberos&#95;ticket&#95;cache&#95;path          | &quot;&quot;                      |
| dfs&#95;client&#95;log&#95;severity                                     | &quot;INFO&quot;                  |
| dfs&#95;domain&#95;socket&#95;path                                      | &quot;&quot;                      |

[HDFS Configuration Reference](https://hawq.apache.org/docs/userguide/2.3.0.0-incubating/reference/HDFSConfigurationParameterReference.html) 可能对其中部分参数有所说明。

<div id="clickhouse-extras">
  #### ClickHouse 额外配置
</div>

| **参数**                            | **默认值**      |
| --------------------------------- | ------------ |
| hadoop&#95;kerberos&#95;keytab    | &quot;&quot; |
| hadoop&#95;kerberos&#95;principal | &quot;&quot; |
| libhdfs3&#95;conf                 | &quot;&quot; |

<div id="limitations">
  ### 限制
</div>

* `hadoop_security_kerberos_ticket_cache_path` 和 `libhdfs3_conf` 只能设为全局配置，不能针对特定用户单独设置

<div id="kerberos-support">
  ## Kerberos 支持
</div>

如果 `hadoop_security_authentication` 参数的值为 `kerberos`，ClickHouse 会通过 Kerberos 进行身份验证。
参数见[此处](#clickhouse-extras)，其中 `hadoop_security_kerberos_ticket_cache_path` 可能会有帮助。
请注意，由于 libhdfs3 的限制，目前只支持传统方式，
datanode 之间的通信不会受到 SASL 保护 (`HADOOP_SECURE_DN_USER` 是这种
安全方式的可靠标识) 。可参考 `tests/integration/test_storage_kerberized_hdfs/hdfs_configs/bootstrap.sh`。

如果指定了 `hadoop_kerberos_keytab`、`hadoop_kerberos_principal` 或 `hadoop_security_kerberos_ticket_cache_path`，则会使用 Kerberos 身份验证。在这种情况下，`hadoop_kerberos_keytab` 和 `hadoop_kerberos_principal` 是必填项。

<div id="namenode-ha">
  ## HDFS Namenode HA 支持
</div>

libhdfs3 支持 HDFS Namenode HA。

* 将 `hdfs-site.xml` 从某个 HDFS 节点复制到 `/etc/clickhouse-server/`。
* 在 ClickHouse 配置文件中添加以下内容：

```xml
  <hdfs>
    <libhdfs3_conf>/etc/clickhouse-server/hdfs-site.xml</libhdfs3_conf>
  </hdfs>
```

* 然后，将 `hdfs-site.xml` 中 `dfs.nameservices` 的值作为 HDFS URI 中的 namenode 地址使用。例如，将 `hdfs://appadmin@192.168.101.11:8020/abc/` 替换为 `hdfs://appadmin@my_nameservice/abc/`。

<div id="virtual-columns">
  ## 虚拟列
</div>

* `_path` — 文件路径。类型：`LowCardinality(String)`。
* `_file` — 文件名。类型：`LowCardinality(String)`。
* `_size` — 文件大小 (以字节为单位) 。类型：`Nullable(UInt64)`。如果大小未知，则该值为 `NULL`。
* `_time` — 文件的最后修改时间。类型：`Nullable(DateTime)`。如果时间未知，则该值为 `NULL`。

<div id="storage-settings">
  ## 存储设置
</div>

* [hdfs&#95;truncate&#95;on&#95;insert](/zh/operations/settings/settings.md#hdfs_truncate_on_insert) - 允许在向文件插入数据前先将其截断。默认禁用。
* [hdfs&#95;create&#95;new&#95;file&#95;on&#95;insert](/zh/operations/settings/settings.md#hdfs_create_new_file_on_insert) - 如果 格式 带有后缀，则允许在每次插入时创建一个新文件。默认禁用。
* [hdfs&#95;skip&#95;empty&#95;files](/zh/operations/settings/settings.md#hdfs_skip_empty_files) - 允许在读取时跳过空文件。默认禁用。

**另请参见**

* [虚拟列](../../../engines/table-engines/index.md#table_engines-virtual_columns)