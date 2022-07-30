---
sidebar_position: 46
sidebar_label: input
---

# input {#input}

`input(structure)` -表函数，可以有效地将发送给服务器的数据转换为具有给定结构的数据并将其插入到具有其他结构的表中。

`structure` -发送到服务器的数据结构的格式 `'column1_name column1_type, column2_name column2_type, ...'`。
例如, `'id UInt32, name String'`。

该函数只能在 `INSERT SELECT` 查询中使用，并且只能使用一次，但在其他方面，行为类似于普通的表函数
（例如，它可以用于子查询等。).

数据可以像普通 `INSERT` 查询一样发送，并以必须在查询末尾指定的任何可用[格式](../../interfaces/formats.md#formats)
传递（与普通 `INSERT SELECT`不同)。

该函数的主要特点是，当服务器从客户端接收数据时，它会同时根据 `SELECT` 子句中的表达式列表将其转换，并插入到目标表中。
不会创建包含所有已传输数据的临时表。

**例**

-   让 `test` 表具有以下结构 `(a String, b String)`
    并且 `data.csv` 中的数据具有不同的结构 `(col1 String, col2 Date, col3 Int32)`。
    将数据从 `data.csv` 插入到 `test` 表中，同时进行转换的查询如下所示:

<!-- -->

``` bash
$ cat data.csv | clickhouse-client --query="INSERT INTO test SELECT lower(col1), col3 * col3 FROM input('col1 String, col2 Date, col3 Int32') FORMAT CSV";
```

-   如果 `data.csv` 包含与表 `test` 相同结构 `test_structure` 的数据，那么这两个查询是相等的:

<!-- -->

``` bash
$ cat data.csv | clickhouse-client --query="INSERT INTO test FORMAT CSV"
$ cat data.csv | clickhouse-client --query="INSERT INTO test SELECT * FROM input('test_structure') FORMAT CSV"
```

[原始文章](https://clickhouse.com/docs/en/query_language/table_functions/input/) <!--hide-->
