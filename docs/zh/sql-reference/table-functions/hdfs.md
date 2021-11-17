---
toc_priority: 45
toc_title: hdfs
---

# hdfs {#hdfs}

根据HDFS中的文件创建表。 该表函数类似于 [url](url.md) 和 [文件](file.md)。

``` sql
hdfs(URI, format, structure)
```

**输入参数**

-   `URI` — HDFS中文件的相对URI。 在只读模式下，文件路径支持以下通配符: `*`, `?`, `{abc,def}` 和 `{N..M}` ，其中 `N`, `M` 是数字, \``'abc', 'def'` 是字符串。
-   `format` — 文件的[格式](../../interfaces/formats.md#formats)。
-   `structure` — 表的结构。格式  `'column1_name column1_type, column2_name column2_type, ...'`。

**返回值**

具有指定结构的表，用于读取或写入指定文件中的数据。

**示例**

表来自 `hdfs://hdfs1:9000/test` 并从中选择前两行:

``` sql
SELECT *
FROM hdfs('hdfs://hdfs1:9000/test', 'TSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
LIMIT 2
```

``` text
┌─column1─┬─column2─┬─column3─┐
│       1 │       2 │       3 │
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

**路径中的通配符**

多个路径组件可以具有通配符。 对于要处理的文件必须存在并与整个路径模式匹配（不仅后缀或前缀）。

-   `*` — 替换任意数量的任何字符，除了 `/` 包括空字符串。
-   `?` — 替换任何单个字符。
-   `{some_string,another_string,yet_another_one}` — 替换任何字符串 `'some_string', 'another_string', 'yet_another_one'`。
-   `{N..M}` — 替换范围从N到M的任何数字（包括两个边界）。

使用 `{}` 的构造类似于 [remote](../../sql-reference/table-functions/remote.md))表函数。

**示例**

1.  假设我们在HDFS上有几个带有以下URI的文件:

-   ‘hdfs://hdfs1:9000/some_dir/some_file_1’
-   ‘hdfs://hdfs1:9000/some_dir/some_file_2’
-   ‘hdfs://hdfs1:9000/some_dir/some_file_3’
-   ‘hdfs://hdfs1:9000/another_dir/some_file_1’
-   ‘hdfs://hdfs1:9000/another_dir/some_file_2’
-   ‘hdfs://hdfs1:9000/another_dir/some_file_3’

2.  查询这些文件中的行数:

<!-- -->

``` sql
SELECT count(*)
FROM hdfs('hdfs://hdfs1:9000/{some,another}_dir/some_file_{1..3}', 'TSV', 'name String, value UInt32')
```

3.  查询这两个目录的所有文件中的行数:

<!-- -->

``` sql
SELECT count(*)
FROM hdfs('hdfs://hdfs1:9000/{some,another}_dir/*', 'TSV', 'name String, value UInt32')
```

!!! warning "警告"
    如果您的文件列表包含带前导零的数字范围，请对每个数字分别使用带有大括号的结构或使用 `?`。

**示例**

从名为 `file000`, `file001`, … , `file999`的文件中查询数据:

``` sql
SELECT count(*)
FROM hdfs('hdfs://hdfs1:9000/big_dir/file{0..9}{0..9}{0..9}', 'CSV', 'name String, value UInt32')
```

## 虚拟列 {#virtual-columns}

-   `_path` — 文件路径。
-   `_file` — 文件名称。

**另请参阅**

-   [虚拟列](https://clickhouse.com/docs/en/operations/table_engines/#table_engines-virtual_columns)

[原始文章](https://clickhouse.com/docs/en/query_language/table_functions/hdfs/) <!--hide-->
