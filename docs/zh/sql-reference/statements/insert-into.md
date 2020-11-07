## INSERT {#insert}

INSERT查询主要用于向系统中添加数据.

查询的基本格式:

``` sql
INSERT INTO [db.]table [(c1, c2, c3)] VALUES (v11, v12, v13), (v21, v22, v23), ...
```

您可以在查询中指定插入的列的列表，如：`[(c1, c2, c3)]`。对于存在于表结构中但不存在于插入列表中的列，它们将会按照如下方式填充数据：

-   如果存在`DEFAULT`表达式，根据`DEFAULT`表达式计算被填充的值。
-   如果没有定义`DEFAULT`表达式，则填充零或空字符串。

如果 [strict_insert_defaults=1](../../operations/settings/settings.md)，你必须在查询中列出所有没有定义`DEFAULT`表达式的列。

数据可以以ClickHouse支持的任何 [输入输出格式](../../interfaces/formats.md#formats) 传递给INSERT。格式的名称必须显示的指定在查询中：

``` sql
INSERT INTO [db.]table [(c1, c2, c3)] FORMAT format_name data_set
```

例如，下面的查询所使用的输入格式就与上面INSERT … VALUES的中使用的输入格式相同：

``` sql
INSERT INTO [db.]table [(c1, c2, c3)] FORMAT Values (v11, v12, v13), (v21, v22, v23), ...
```

ClickHouse会清除数据前所有的空白字符与一行摘要信息（如果需要的话）。所以在进行查询时，我们建议您将数据放入到输入输出格式名称后的新的一行中去（如果数据是以空白字符开始的，这将非常重要）。

示例:

``` sql
INSERT INTO t FORMAT TabSeparated
11  Hello, world!
22  Qwerty
```

在使用命令行客户端或HTTP客户端时，你可以将具体的查询语句与数据分开发送。更多具体信息，请参考«[客户端](../../interfaces/index.md#interfaces)»部分。

### 使用`SELECT`的结果写入 {#insert_query_insert-select}

``` sql
INSERT INTO [db.]table [(c1, c2, c3)] SELECT ...
```

写入与SELECT的列的对应关系是使用位置来进行对应的，尽管它们在SELECT表达式与INSERT中的名称可能是不同的。如果需要，会对它们执行对应的类型转换。

除了VALUES格式之外，其他格式中的数据都不允许出现诸如`now()`，`1 + 2`等表达式。VALUES格式允许您有限度的使用这些表达式，但是不建议您这么做，因为执行这些表达式总是低效的。

系统不支持的其他用于修改数据的查询：`UPDATE`, `DELETE`, `REPLACE`, `MERGE`, `UPSERT`, `INSERT UPDATE`。
但是，您可以使用 `ALTER TABLE ... DROP PARTITION`查询来删除一些旧的数据。

### 性能的注意事项 {#xing-neng-de-zhu-yi-shi-xiang}

在进行`INSERT`时将会对写入的数据进行一些处理，按照主键排序，按照月份对数据进行分区等。所以如果在您的写入数据中包含多个月份的混合数据时，将会显著的降低`INSERT`的性能。为了避免这种情况：

-   数据总是以尽量大的batch进行写入，如每次写入100,000行。
-   数据在写入ClickHouse前预先的对数据进行分组。

在以下的情况下，性能不会下降：

-   数据总是被实时的写入。
-   写入的数据已经按照时间排序。

[来源文章](https://clickhouse.tech/docs/en/query_language/insert_into/) <!--hide-->
