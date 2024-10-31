---
slug: /zh/sql-reference/statements/insert-into
---
## INSERT INTO 语句 {#insert}

INSERT INTO 语句主要用于向系统中添加数据.

查询的基本格式:

``` sql
INSERT INTO [TABLE] [db.]table [(c1, c2, c3)] VALUES (v11, v12, v13), (v21, v22, v23), ...
```

您可以在查询中指定要插入的列的列表，如：`[(c1, c2, c3)]`。您还可以使用列[匹配器](../../sql-reference/statements/select/index.md#asterisk)的表达式，例如`*`和/或[修饰符](../../sql-reference/statements/select/index.md#select-modifiers)，例如 [APPLY](../../sql-reference/statements/select/index.md#apply-modifier)， [EXCEPT](../../sql-reference/statements/select/index.md#apply-modifier)， [REPLACE](../../sql-reference/statements/select/index.md#replace-modifier)。

例如，考虑该表:

``` sql
SHOW CREATE insert_select_testtable;
```

```text
CREATE TABLE insert_select_testtable
(
    `a` Int8,
    `b` String,
    `c` Int8
)
ENGINE = MergeTree()
ORDER BY a
```

``` sql
INSERT INTO insert_select_testtable (*) VALUES (1, 'a', 1) ;
```

如果要在除了'b'列以外的所有列中插入数据，您需要传递和括号中选择的列数一样多的值:

``` sql
INSERT INTO insert_select_testtable (* EXCEPT(b)) Values (2, 2);
```

``` sql
SELECT * FROM insert_select_testtable;
```

```
┌─a─┬─b─┬─c─┐
│ 2 │   │ 2 │
└───┴───┴───┘
┌─a─┬─b─┬─c─┐
│ 1 │ a │ 1 │
└───┴───┴───┘
```

在这个示例中，我们看到插入的第二行的`a`和`c`列的值由传递的值填充，而`b`列由默认值填充。

对于存在于表结构中但不存在于插入列表中的列，它们将会按照如下方式填充数据：

-   如果存在`DEFAULT`表达式，根据`DEFAULT`表达式计算被填充的值。
-   如果没有定义`DEFAULT`表达式，则填充零或空字符串。

如果 [strict_insert_defaults=1](../../operations/settings/settings.md)，你必须在查询中列出所有没有定义`DEFAULT`表达式的列。

数据可以以ClickHouse支持的任何 [输入输出格式](../../interfaces/formats.md#formats) 传递给INSERT。格式的名称必须显示的指定在查询中：

``` sql
INSERT INTO [db.]table [(c1, c2, c3)] FORMAT format_name data_set
```

例如，下面的查询所使用的输入格式就与上面INSERT ... VALUES的中使用的输入格式相同：

``` sql
INSERT INTO [TABLE] [db.]table [(c1, c2, c3)] FORMAT Values (v11, v12, v13), (v21, v22, v23), ...
```

ClickHouse会清除数据前所有的空白字符与一个换行符（如果有换行符的话）。所以在进行查询时，我们建议您将数据放入到输入输出格式名称后的新的一行中去（如果数据是以空白字符开始的，这将非常重要）。

示例:

``` sql
INSERT INTO t FORMAT TabSeparated
11  Hello, world!
22  Qwerty
```

在使用命令行客户端或HTTP客户端时，你可以将具体的查询语句与数据分开发送。更多具体信息，请参考«[客户端](../../interfaces/index.md#interfaces)»部分。

### 限制 {#constraints}

如果表中有一些[限制](../../sql-reference/statements/create/table.mdx#constraints),，数据插入时会逐行进行数据校验，如果这里面包含了不符合限制条件的数据，服务将会抛出包含限制信息的异常，这个语句也会被停止执行。

### 使用`SELECT`的结果写入 {#inserting-the-results-of-select}

``` sql
INSERT INTO [TABLE] [db.]table [(c1, c2, c3)] SELECT ...
```

写入与SELECT的列的对应关系是使用位置来进行对应的，尽管它们在SELECT表达式与INSERT中的名称可能是不同的。如果需要，会对它们执行对应的类型转换。

除了VALUES格式之外，其他格式中的数据都不允许出现诸如`now()`，`1 + 2`等表达式。VALUES格式允许您有限度的使用这些表达式，但是不建议您这么做，因为执行这些表达式总是低效的。

系统不支持的其他用于修改数据的查询：`UPDATE`, `DELETE`, `REPLACE`, `MERGE`, `UPSERT`, `INSERT UPDATE`。
但是，您可以使用 `ALTER TABLE ... DROP PARTITION`查询来删除一些旧的数据。

如果 `SELECT` 查询中包含了 [input()](../../sql-reference/table-functions/input.md) 函数，那么 `FORMAT` 必须出现在查询语句的最后。

如果某一列限制了值不能是NULL，那么插入NULL的时候就会插入这个列类型的默认数据，可以通过设置 [insert_null_as_default](../../operations/settings/settings.md#insert_null_as_default) 插入NULL。

### 从文件向表中插入数据 {#inserting-data-from-a-file}

``` sql
INSERT INTO [db.]table [(c1, c2, c3)] FROM INFILE file_name [COMPRESSION type] FORMAT format_name
```
使用上面的语句可以从客户端的文件上读取数据并插入表中，`file_name` 和 `type` 都是 `String` 类型，输入文件的[格式](../../interfaces/formats.md) 一定要在 `FORMAT` 语句中设置。

支持读取压缩文件。默认会去读文件的拓展名作为文件的压缩方式，或者也可以在 `COMPRESSION` 语句中指明，支持的文件压缩格式如下：`'none'`， `'gzip'`， `'deflate'`， `'br'`， `'xz'`， `'zstd'`， `'lz4'`， `'bz2'`。

这个功能在 [command-line client](../../interfaces/cli.md) 和 [clickhouse-local](../../operations/utilities/clickhouse-local.md) 是可用的。

**样例**

```bash
echo 1,A > input.csv ; echo 2,B >> input.csv
clickhouse-client --query="CREATE TABLE table_from_file (id UInt32, text String) ENGINE=MergeTree() ORDER BY id;"
clickhouse-client --query="INSERT INTO table_from_file FROM INFILE 'input.csv' FORMAT CSV;"
clickhouse-client --query="SELECT * FROM table_from_file FORMAT PrettyCompact;"
```

结果:

```text
┌─id─┬─text─┐
│  1 │ A    │
│  2 │ B    │
└────┴──────┘
```

### 插入表函数 {#inserting-into-table-function}

数据可以通过 [table functions](../../sql-reference/table-functions/index.md) 方法插入。
``` sql
INSERT INTO [TABLE] FUNCTION table_func ...
```

**例如**

可以这样使用[remote](../../sql-reference/table-functions/index.md#remote) 表函数:

``` sql
CREATE TABLE simple_table (id UInt32, text String) ENGINE=MergeTree() ORDER BY id;
INSERT INTO TABLE FUNCTION remote('localhost', default.simple_table) 
    VALUES (100, 'inserted via remote()');
SELECT * FROM simple_table;
```

结果:

``` text
┌──id─┬─text──────────────────┐
│ 100 │ inserted via remote() │
└─────┴───────────────────────┘
```


### 性能的注意事项 {#xing-neng-de-zhu-yi-shi-xiang}

在进行`INSERT`时将会对写入的数据进行一些处理，按照主键排序，按照月份对数据进行分区等。所以如果在您的写入数据中包含多个月份的混合数据时，将会显著的降低`INSERT`的性能。为了避免这种情况：

-   数据总是以尽量大的batch进行写入，如每次写入100,000行。
-   数据在写入ClickHouse前预先的对数据进行分组。

在以下的情况下，性能不会下降：

-   数据总是被实时的写入。
-   写入的数据已经按照时间排序。

也可以异步的、小规模的插入数据，这些数据会被合并成多个批次，然后安全地写入到表中，通过设置[async_insert](../../operations/settings/settings.md#async-insert)，可以使用异步插入的方式，请注意，异步插入的方式只支持HTTP协议，并且不支持数据去重。
