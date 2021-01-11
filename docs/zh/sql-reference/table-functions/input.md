---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 46
toc_title: "\u8F93\u5165"
---

# 输入 {#input}

`input(structure)` -表功能，允许有效地转换和插入数据发送到
服务器与给定结构的表与另一种结构。

`structure` -以下格式发送到服务器的数据结构 `'column1_name column1_type, column2_name column2_type, ...'`.
例如, `'id UInt32, name String'`.

此功能只能用于 `INSERT SELECT` 查询，只有一次，但其他行为像普通表函数
（例如，它可以用于子查询等。).

数据可以以任何方式像普通发送 `INSERT` 查询并传递任何可用 [格式](../../interfaces/formats.md#formats)
必须在查询结束时指定（不像普通 `INSERT SELECT`).

这个功能的主要特点是，当服务器从客户端接收数据时，它同时将其转换
根据表达式中的列表 `SELECT` 子句并插入到目标表中。 临时表
不创建所有传输的数据。

**例**

-   让 `test` 表具有以下结构 `(a String, b String)`
    和数据 `data.csv` 具有不同的结构 `(col1 String, col2 Date, col3 Int32)`. 查询插入
    从数据 `data.csv` 进 `test` 同时转换的表如下所示:

<!-- -->

``` bash
$ cat data.csv | clickhouse-client --query="INSERT INTO test SELECT lower(col1), col3 * col3 FROM input('col1 String, col2 Date, col3 Int32') FORMAT CSV";
```

-   如果 `data.csv` 包含相同结构的数据 `test_structure` 作为表 `test` 那么这两个查询是相等的:

<!-- -->

``` bash
$ cat data.csv | clickhouse-client --query="INSERT INTO test FORMAT CSV"
$ cat data.csv | clickhouse-client --query="INSERT INTO test SELECT * FROM input('test_structure') FORMAT CSV"
```

[原始文章](https://clickhouse.tech/docs/en/query_language/table_functions/input/) <!--hide-->
