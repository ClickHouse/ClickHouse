# 文件(输入格式) {#table_engines-file}

数据源是以 Clickhouse 支持的一种输入格式（TabSeparated，Native等）存储数据的文件。

用法示例：

-   从 ClickHouse 导出数据到文件。
-   将数据从一种格式转换为另一种格式。
-   通过编辑磁盘上的文件来更新 ClickHouse 中的数据。

## 在 ClickHouse 服务器中的使用 {#zai-clickhouse-fu-wu-qi-zhong-de-shi-yong}

    File(Format)

选用的 `Format` 需要支持 `INSERT` 或 `SELECT` 。有关支持格式的完整列表，请参阅 [格式](../../../interfaces/formats.md#formats)。

ClickHouse 不支持给 `File` 指定文件系统路径。它使用服务器配置中 [路径](../../../operations/server-configuration-parameters/settings.md) 设定的文件夹。

使用 `File(Format)` 创建表时，它会在该文件夹中创建空的子目录。当数据写入该表时，它会写到该子目录中的 `data.Format` 文件中。

你也可以在服务器文件系统中手动创建这些子文件夹和文件，然后通过 [ATTACH](../../../engines/table-engines/special/file.md) 将其创建为具有对应名称的表，这样你就可以从该文件中查询数据了。

!!! 注意 "注意"
    注意这个功能，因为 ClickHouse 不会跟踪这些文件在外部的更改。在 ClickHouse 中和 ClickHouse 外部同时写入会造成结果是不确定的。

**示例：**

**1.** 创建 `file_engine_table` 表：

``` sql
CREATE TABLE file_engine_table (name String, value UInt32) ENGINE=File(TabSeparated)
```

默认情况下，Clickhouse 会创建目录 `/var/lib/clickhouse/data/default/file_engine_table` 。

**2.** 手动创建 `/var/lib/clickhouse/data/default/file_engine_table/data.TabSeparated` 文件，并且包含内容：

``` bash
$ cat data.TabSeparated
one 1
two 2
```

**3.** 查询这些数据:

``` sql
SELECT * FROM file_engine_table
```

    ┌─name─┬─value─┐
    │ one  │     1 │
    │ two  │     2 │
    └──────┴───────┘

## 在 Clickhouse-local 中的使用 {#zai-clickhouse-local-zhong-de-shi-yong}

使用 [clickhouse-local](../../../operations/utilities/clickhouse-local.md) 时，File 引擎除了 `Format` 之外，还可以接收文件路径参数。可以使用数字或名称来指定标准输入/输出流，例如 `0` 或 `stdin`，`1` 或 `stdout`。
**例如：**

``` bash
$ echo -e "1,2\n3,4" | clickhouse-local -q "CREATE TABLE table (a Int64, b Int64) ENGINE = File(CSV, stdin); SELECT a, b FROM table; DROP TABLE table"
```

## 功能实现 {#gong-neng-shi-xian}

-   读操作可支持并发，但写操作不支持
-   不支持:
    -   `ALTER`
    -   `SELECT ... SAMPLE`
    -   索引
    -   副本

[来源文章](https://clickhouse.tech/docs/en/operations/table_engines/file/) <!--hide-->
