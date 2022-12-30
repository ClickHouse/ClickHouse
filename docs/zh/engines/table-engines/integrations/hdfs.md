---
sidebar_position: 36
sidebar_label: HDFS
---

# HDFS {#table_engines-hdfs}

这个引擎提供了与 [Apache Hadoop](https://en.wikipedia.org/wiki/Apache_Hadoop) 生态系统的集成，允许通过 ClickHouse 管理 [HDFS](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html) 上的数据。这个引擎类似于
[文件](../../../engines/table-engines/special/file.md#table_engines-file) 和 [URL](../../../engines/table-engines/special/url.md#table_engines-url) 引擎，但提供了 Hadoop 的特定功能。

## 用法 {#usage}

``` sql
ENGINE = HDFS(URI, format)
```

`URI` 参数是 HDFS 中整个文件的 URI。
`format` 参数指定一种可用的文件格式。 执行
`SELECT` 查询时，格式必须支持输入，以及执行
`INSERT` 查询时，格式必须支持输出. 你可以在 [格式](../../../interfaces/formats.md#formats) 章节查看可用的格式。
路径部分 `URI` 可能包含 glob 通配符。 在这种情况下，表将是只读的。

**示例:**

**1.** 设置 `hdfs_engine_table` 表:

``` sql
CREATE TABLE hdfs_engine_table (name String, value UInt32) ENGINE=HDFS('hdfs://hdfs1:9000/other_storage', 'TSV')
```

**2.** 填充文件:

``` sql
INSERT INTO hdfs_engine_table VALUES ('one', 1), ('two', 2), ('three', 3)
```

**3.** 查询数据:

``` sql
SELECT * FROM hdfs_engine_table LIMIT 2
```

``` text
┌─name─┬─value─┐
│ one  │     1 │
│ two  │     2 │
└──────┴───────┘
```

## 实施细节 {#implementation-details}

-   读取和写入可以并行
-   不支持:
    -   `ALTER` 和 `SELECT...SAMPLE` 操作。
    -   索引。
    -   复制。

**路径中的通配符**

多个路径组件可以具有 globs。 对于正在处理的文件应该存在并匹配到整个路径模式。 文件列表的确定是在 `SELECT` 的时候进行（而不是在 `CREATE` 的时候）。

-   `*` — 替代任何数量的任何字符，除了 `/` 以及空字符串。
-   `?` — 代替任何单个字符.
-   `{some_string,another_string,yet_another_one}` — 替代任何字符串 `'some_string', 'another_string', 'yet_another_one'`.
-   `{N..M}` — 替换 N 到 M 范围内的任何数字，包括两个边界的值.

带 `{}` 的结构类似于 [远程](../../../sql-reference/table-functions/remote.md) 表函数。

**示例**

1.  假设我们在 HDFS 上有几个 TSV 格式的文件，文件的 URI 如下:

-   ‘hdfs://hdfs1:9000/some_dir/some_file_1’
-   ‘hdfs://hdfs1:9000/some_dir/some_file_2’
-   ‘hdfs://hdfs1:9000/some_dir/some_file_3’
-   ‘hdfs://hdfs1:9000/another_dir/some_file_1’
-   ‘hdfs://hdfs1:9000/another_dir/some_file_2’
-   ‘hdfs://hdfs1:9000/another_dir/some_file_3’

1.  有几种方法可以创建由所有六个文件组成的表:

<!-- -->

``` sql
CREATE TABLE table_with_range (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/{some,another}_dir/some_file_{1..3}', 'TSV')
```

另一种方式:

``` sql
CREATE TABLE table_with_question_mark (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/{some,another}_dir/some_file_?', 'TSV')
```

表由两个目录中的所有文件组成（所有文件都应满足query中描述的格式和模式):

``` sql
CREATE TABLE table_with_asterisk (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/{some,another}_dir/*', 'TSV')
```

!!! warning "警告"
    如果文件列表包含带有前导零的数字范围，请单独使用带有大括号的构造或使用 `?`.

**示例**

创建具有名为文件的表 `file000`, `file001`, … , `file999`:

``` sql
CREARE TABLE big_table (name String, value UInt32) ENGINE = HDFS('hdfs://hdfs1:9000/big_dir/file{0..9}{0..9}{0..9}', 'CSV')
```

## 配置 {#configuration}

与 GraphiteMergeTree 类似，HDFS 引擎支持使用 ClickHouse 配置文件进行扩展配置。有两个配置键可以使用：全局 (`hdfs`) 和用户级别 (`hdfs_*`)。首先全局配置生效，然后用户级别配置生效 (如果用户级别配置存在) 。

``` xml
  <!-- HDFS 引擎类型的全局配置选项 -->
  <hdfs>
	<hadoop_kerberos_keytab>/tmp/keytab/clickhouse.keytab</hadoop_kerberos_keytab>
	<hadoop_kerberos_principal>clickuser@TEST.CLICKHOUSE.TECH</hadoop_kerberos_principal>
	<hadoop_security_authentication>kerberos</hadoop_security_authentication>
  </hdfs>

  <!-- 用户 "root" 的指定配置 -->
  <hdfs_root>
	<hadoop_kerberos_principal>root@TEST.CLICKHOUSE.TECH</hadoop_kerberos_principal>
  </hdfs_root>
```

### 可选配置选项及其默认值的列表
#### libhdfs3 支持的


| **参数**                                               | **默认值**               |
| rpc\_client\_connect\_tcpnodelay                      | true                    |
| dfs\_client\_read\_shortcircuit                       | true                    |
| output\_replace-datanode-on-failure                   | true                    |
| input\_notretry-another-node                          | false                   |
| input\_localread\_mappedfile                          | true                    |
| dfs\_client\_use\_legacy\_blockreader\_local          | false                   |
| rpc\_client\_ping\_interval                           | 10  * 1000              |
| rpc\_client\_connect\_timeout                         | 600 * 1000              |
| rpc\_client\_read\_timeout                            | 3600 * 1000             |
| rpc\_client\_write\_timeout                           | 3600 * 1000             |
| rpc\_client\_socekt\_linger\_timeout                  | -1                      |
| rpc\_client\_connect\_retry                           | 10                      |
| rpc\_client\_timeout                                  | 3600 * 1000             |
| dfs\_default\_replica                                 | 3                       |
| input\_connect\_timeout                               | 600 * 1000              |
| input\_read\_timeout                                  | 3600 * 1000             |
| input\_write\_timeout                                 | 3600 * 1000             |
| input\_localread\_default\_buffersize                 | 1 * 1024 * 1024         |
| dfs\_prefetchsize                                     | 10                      |
| input\_read\_getblockinfo\_retry                      | 3                       |
| input\_localread\_blockinfo\_cachesize                | 1000                    |
| input\_read\_max\_retry                               | 60                      |
| output\_default\_chunksize                            | 512                     |
| output\_default\_packetsize                           | 64 * 1024               |
| output\_default\_write\_retry                         | 10                      |
| output\_connect\_timeout                              | 600 * 1000              |
| output\_read\_timeout                                 | 3600 * 1000             |
| output\_write\_timeout                                | 3600 * 1000             |
| output\_close\_timeout                                | 3600 * 1000             |
| output\_packetpool\_size                              | 1024                    |
| output\_heeartbeat\_interval                          | 10 * 1000               |
| dfs\_client\_failover\_max\_attempts                  | 15                      |
| dfs\_client\_read\_shortcircuit\_streams\_cache\_size | 256                     |
| dfs\_client\_socketcache\_expiryMsec                  | 3000                    |
| dfs\_client\_socketcache\_capacity                    | 16                      |
| dfs\_default\_blocksize                               | 64 * 1024 * 1024        |
| dfs\_default\_uri                                     | "hdfs://localhost:9000" |
| hadoop\_security\_authentication                      | "simple"                |
| hadoop\_security\_kerberos\_ticket\_cache\_path       | ""                      |
| dfs\_client\_log\_severity                            | "INFO"                  |
| dfs\_domain\_socket\_path                             | ""                      |


[HDFS 配置参考](https://hawq.apache.org/docs/userguide/2.3.0.0-incubating/reference/HDFSConfigurationParameterReference.html) 也许会解释一些参数的含义.

#### ClickHouse 额外的配置 {#clickhouse-extras}

| **参数**                                              | **默认值**               |
|hadoop\_kerberos\_keytab                               | ""                      |
|hadoop\_kerberos\_principal                            | ""                      |
|hadoop\_kerberos\_kinit\_command                       | kinit                   |

#### 限制 {#limitations}
  * hadoop\_security\_kerberos\_ticket\_cache\_path 只能在全局配置, 不能指定用户

## Kerberos 支持 {#kerberos-support}

如果 hadoop\_security\_authentication 参数的值为 'kerberos' ，ClickHouse 将通过 Kerberos 设施进行认证。
[这里的](#clickhouse-extras) 参数和 hadoop\_security\_kerberos\_ticket\_cache\_path 也许会有帮助.
注意，由于 libhdfs3 的限制，只支持老式的方法。
数据节点的安全通信无法由 SASL 保证 ( HADOOP\_SECURE\_DN\_USER 是这种安全方法的一个可靠指标)
使用 tests/integration/test\_storage\_kerberized\_hdfs/hdfs_configs/bootstrap.sh 脚本作为参考。

如果指定了 hadoop\_kerberos\_keytab, hadoop\_kerberos\_principal 或者 hadoop\_kerberos\_kinit\_command ，将会调用 kinit 工具.在此情况下， hadoop\_kerberos\_keytab 和 hadoop\_kerberos\_principal 参数是必须配置的. kinit 工具和 krb5 配置文件是必要的.

## 虚拟列 {#virtual-columns}

-   `_path` — 文件路径.
-   `_file` — 文件名.

**另请参阅**

-   [虚拟列](../index.md#table_engines-virtual_columns)

[原始文章](https://clickhouse.com/docs/en/operations/table_engines/hdfs/) <!--hide-->
