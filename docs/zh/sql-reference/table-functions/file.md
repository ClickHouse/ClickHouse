---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 37
toc_title: "\u6587\u4EF6"
---

# 文件 {#file}

从文件创建表。 此表函数类似于 [url](url.md) 和 [hdfs](hdfs.md) 一些的。

``` sql
file(path, format, structure)
```

**输入参数**

-   `path` — The relative path to the file from [user_files_path](../../operations/server-configuration-parameters/settings.md#server_configuration_parameters-user_files_path). 只读模式下的globs后的文件支持路径: `*`, `?`, `{abc,def}` 和 `{N..M}` 哪里 `N`, `M` — numbers, \``'abc', 'def'` — strings.
-   `format` — The [格式](../../interfaces/formats.md#formats) 的文件。
-   `structure` — Structure of the table. Format `'column1_name column1_type, column2_name column2_type, ...'`.

**返回值**

具有指定结构的表，用于读取或写入指定文件中的数据。

**示例**

设置 `user_files_path` 和文件的内容 `test.csv`:

``` bash
$ grep user_files_path /etc/clickhouse-server/config.xml
    <user_files_path>/var/lib/clickhouse/user_files/</user_files_path>

$ cat /var/lib/clickhouse/user_files/test.csv
    1,2,3
    3,2,1
    78,43,45
```

表从`test.csv` 并从中选择前两行:

``` sql
SELECT *
FROM file('test.csv', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32')
LIMIT 2
```

``` text
┌─column1─┬─column2─┬─column3─┐
│       1 │       2 │       3 │
│       3 │       2 │       1 │
└─────────┴─────────┴─────────┘
```

``` sql
-- getting the first 10 lines of a table that contains 3 columns of UInt32 type from a CSV file
SELECT * FROM file('test.csv', 'CSV', 'column1 UInt32, column2 UInt32, column3 UInt32') LIMIT 10
```

**路径中的水珠**

多个路径组件可以具有globs。 对于正在处理的文件应该存在并匹配到整个路径模式（不仅后缀或前缀）。

-   `*` — Substitutes any number of any characters except `/` 包括空字符串。
-   `?` — Substitutes any single character.
-   `{some_string,another_string,yet_another_one}` — Substitutes any of strings `'some_string', 'another_string', 'yet_another_one'`.
-   `{N..M}` — Substitutes any number in range from N to M including both borders.

建筑与 `{}` 类似于 [远程表功能](../../sql-reference/table-functions/remote.md)).

**示例**

1.  假设我们有几个具有以下相对路径的文件:

-   ‘some_dir/some_file_1’
-   ‘some_dir/some_file_2’
-   ‘some_dir/some_file_3’
-   ‘another_dir/some_file_1’
-   ‘another_dir/some_file_2’
-   ‘another_dir/some_file_3’

1.  查询这些文件中的行数:

<!-- -->

``` sql
SELECT count(*)
FROM file('{some,another}_dir/some_file_{1..3}', 'TSV', 'name String, value UInt32')
```

1.  查询这两个目录的所有文件中的行数:

<!-- -->

``` sql
SELECT count(*)
FROM file('{some,another}_dir/*', 'TSV', 'name String, value UInt32')
```

!!! warning "警告"
    如果您的文件列表包含带前导零的数字范围，请单独使用带大括号的构造或使用 `?`.

**示例**

从名为 `file000`, `file001`, … , `file999`:

``` sql
SELECT count(*)
FROM file('big_dir/file{0..9}{0..9}{0..9}', 'CSV', 'name String, value UInt32')
```

## 虚拟列 {#virtual-columns}

-   `_path` — Path to the file.
-   `_file` — Name of the file.

**另请参阅**

-   [虚拟列](https://clickhouse.tech/docs/en/operations/table_engines/#table_engines-virtual_columns)

[原始文章](https://clickhouse.tech/docs/en/query_language/table_functions/file/) <!--hide-->
