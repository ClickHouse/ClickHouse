---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 36
toc_title: HDFS
---

# HDFS {#table_engines-hdfs}

该引擎提供了集成 [Apache Hadoop](https://en.wikipedia.org/wiki/Apache_Hadoop) 生态系统通过允许管理数据 [HDFS](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/HdfsDesign.html)通过ClickHouse. 这个引擎是相似的
到 [文件](../special/file.md#table_engines-file) 和 [URL](../special/url.md#table_engines-url) 引擎，但提供Hadoop特定的功能。

## 用途 {#usage}

``` sql
ENGINE = HDFS(URI, format)
```

该 `URI` 参数是HDFS中的整个文件URI。
该 `format` 参数指定一种可用的文件格式。 执行
`SELECT` 查询时，格式必须支持输入，并执行
`INSERT` queries – for output. The available formats are listed in the
[格式](../../../interfaces/formats.md#formats) 科。
路径部分 `URI` 可能包含水珠。 在这种情况下，表将是只读的。

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

**路径中的水珠**

多个路径组件可以具有globs。 对于正在处理的文件应该存在并匹配到整个路径模式。 文件列表确定在 `SELECT` （不在 `CREATE` 时刻）。

-   `*` — Substitutes any number of any characters except `/` 包括空字符串。
-   `?` — Substitutes any single character.
-   `{some_string,another_string,yet_another_one}` — Substitutes any of strings `'some_string', 'another_string', 'yet_another_one'`.
-   `{N..M}` — Substitutes any number in range from N to M including both borders.

建筑与 `{}` 类似于 [远程](../../../sql-reference/table-functions/remote.md) 表功能。

**示例**

1.  假设我们在HDFS上有几个TSV格式的文件，其中包含以下Uri:

-   ‘hdfs://hdfs1:9000/some\_dir/some\_file\_1’
-   ‘hdfs://hdfs1:9000/some\_dir/some\_file\_2’
-   ‘hdfs://hdfs1:9000/some\_dir/some\_file\_3’
-   ‘hdfs://hdfs1:9000/another\_dir/some\_file\_1’
-   ‘hdfs://hdfs1:9000/another\_dir/some\_file\_2’
-   ‘hdfs://hdfs1:9000/another\_dir/some\_file\_3’

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

## 虚拟列 {#virtual-columns}

-   `_path` — Path to the file.
-   `_file` — Name of the file.

**另请参阅**

-   [虚拟列](../index.md#table_engines-virtual_columns)

[原始文章](https://clickhouse.tech/docs/en/operations/table_engines/hdfs/) <!--hide-->
