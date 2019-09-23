## CREATE DATABASE

该查询用于根据指定名称创建数据库。

``` sql
CREATE DATABASE [IF NOT EXISTS] db_name
```

数据库其实只是用于存放表的一个目录。
如果查询中存在`IF NOT EXISTS`，则当数据库已经存在时，该查询不会返回任何错误。


## CREATE TABLE {#create-table-query}

对于`CREATE TABLE`，存在以下几种方式。

```sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = engine
```

在指定的‘db’数据库中创建一个名为‘name’的表，如果查询中没有包含‘db’，则默认使用当前选择的数据库作为‘db’。后面的是包含在括号中的表结构以及表引擎的声明。
其中表结构声明是一个包含一组列描述声明的组合。如果表引擎是支持索引的，那么可以在表引擎的参数中对其进行说明。

在最简单的情况下，列描述是指`名称 类型`这样的子句。例如： `RegionID UInt32`。
但是也可以为列另外定义默认值表达式（见后文）。

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name AS [db2.]name2 [ENGINE = engine]
```

创建一个与`db2.name2`具有相同结构的表，同时你可以对其指定不同的表引擎声明。如果没有表引擎声明，则创建的表将与`db2.name2`使用相同的表引擎。

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name ENGINE = engine AS SELECT ...
```

使用指定的引擎创建一个与`SELECT`子句的结果具有相同结构的表，并使用`SELECT`子句的结果填充它。

以上所有情况，如果指定了`IF NOT EXISTS`，那么在该表已经存在的情况下，查询不会返回任何错误。在这种情况下，查询几乎不会做任何事情。

在`ENGINE`子句后还可能存在一些其他的子句，更详细的信息可以参考 [表引擎](../operations/table_engines/index.md) 中关于建表的描述。

### 默认值 {#create-default-values}

在列描述中你可以通过以下方式之一为列指定默认表达式：`DEFAULT expr`，`MATERIALIZED expr`，`ALIAS expr`。
示例：`URLDomain String DEFAULT domain(URL)`。

如果在列描述中未定义任何默认表达式，那么系统将会根据类型设置对应的默认值，如：数值类型为零、字符串类型为空字符串、数组类型为空数组、日期类型为‘0000-00-00’以及时间类型为‘0000-00-00 00:00:00’。不支持使用NULL作为普通类型的默认值。

如果定义了默认表达式，则可以不定义列的类型。如果没有明确的定义类的类型，则使用默认表达式的类型。例如：`EventDate DEFAULT toDate(EventTime)` - 最终‘EventDate’将使用‘Date’作为类型。

如果同时指定了默认表达式与列的类型，则将使用类型转换函数将默认表达式转换为指定的类型。例如：`Hits UInt32 DEFAULT 0`与`Hits UInt32 DEFAULT toUInt32(0)`意思相同。

默认表达式可以包含常量或表的任意其他列。当创建或更改表结构时，系统将会运行检查，确保不会包含循环依赖。对于INSERT, 它仅检查表达式是否是可以解析的 - 它们可以从中计算出所有需要的列的默认值。

`DEFAULT expr`

普通的默认值，如果INSERT中不包含指定的列，那么将通过表达式计算它的默认值并填充它。

`MATERIALIZED expr`

物化表达式，被该表达式指定的列不能包含在INSERT的列表中，因为它总是被计算出来的。
对于INSERT而言，不需要考虑这些列。
另外，在SELECT查询中如果包含星号，此列不会被用来替换星号，这是因为考虑到数据转储，在使用`SELECT *`查询出的结果总能够被'INSERT'回表。

`ALIAS expr`

别名。这样的列不会存储在表中。
它的值不能够通过INSERT写入，同时使用SELECT查询星号时，这些列也不会被用来替换星号。
但是它们可以显示的用于SELECT中，在这种情况下，在查询分析中别名将被替换。

当使用ALTER查询对添加新的列时，不同于为所有旧数据添加这个列，对于需要在旧数据中查询新列，只会在查询时动态计算这个新列的值。但是如果新列的默认表示中依赖其他列的值进行计算，那么同样会加载这些依赖的列的数据。

如果你向表中添加一个新列，并在之后的一段时间后修改它的默认表达式，则旧数据中的值将会被改变。请注意，在运行后台合并时，缺少的列的值将被计算后写入到合并后的数据部分中。

不能够为nested类型的列设置默认值。

### Constraints {#constraints}

WARNING: This feature is experimental. Correct work is not guaranteed on non-MergeTree family engines.

Along with columns descriptions constraints could be defined:

``sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1] [compression_codec] [TTL expr1],
    ...
    CONSTRAINT constraint_name_1 CHECK boolean_expr_1,
    ...
) ENGINE = engine
```

`boolean_expr_1` could by any boolean expression. If constraints are defined for the table, each of them will be checked for every row in `INSERT` query. If any constraint is not satisfied — server will raise an exception with constraint name and checking expression.

Adding large amount of constraints can negatively affect performance of big `INSERT` queries.

### TTL expression

Defines storage time for values. Can be specified only for MergeTree-family tables. For the detailed description, see [TTL for columns and tables](../operations/table_engines/mergetree.md#table_engine-mergetree-ttl).

## Column Compression Codecs

By default, ClickHouse applies to columns the compression method, defined in [server settings](../operations/server_settings/settings.md#compression). Also, you can define compression method for each individual column in the `CREATE TABLE` query.

```
CREATE TABLE codec_example
(
    dt Date CODEC(ZSTD),
    ts DateTime CODEC(LZ4HC),
    float_value Float32 CODEC(NONE),
    double_value Float64 CODEC(LZ4HC(9))
    value Float32 CODEC(Delta, ZSTD)
)
ENGINE = <Engine>
...
```

If a codec is specified, the default codec doesn't apply. Codecs can be combined in a pipeline, for example, `CODEC(Delta, ZSTD)`. To select the best codecs combination for you project, pass benchmarks, similar to described in the Altinity [New Encodings to Improve ClickHouse Efficiency](https://www.altinity.com/blog/2019/7/new-encodings-to-improve-clickhouse) article.

!!!warning
    You cannot decompress ClickHouse database files with external utilities, for example, `lz4`. Use the special utility, [clickhouse-compressor](https://github.com/ClickHouse/ClickHouse/tree/master/dbms/programs/compressor).

Compression is supported for the table engines:

- [*MergeTree](../operations/table_engines/mergetree.md) family
- [*Log](../operations/table_engines/log_family.md) family
- [Set](../operations/table_engines/set.md)
- [Join](../operations/table_engines/join.md)

ClickHouse supports common purpose codecs and specialized codecs.

### Specialized codecs {#create-query-specialized-codecs}

These codecs are designed to make compression more effective using specifities of the data. Some of this codecs don't compress data by itself, but they prepare data to be compressed better by common purpose codecs.

Specialized codecs:

- `Delta(delta_bytes)` — Compression approach in which raw values are replaced by the difference of two neighboring values, except for the first value that stays unchanged. Up to `delta_bytes` are used for storing delta values, so `delta_bytes` is the maximum size of raw values. Possible `delta_bytes` values: 1, 2, 4, 8. The default value for `delta_bytes` is `sizeof(type)` if equal to 1, 2, 4, or 8. In all other cases, it's 1.
- `DoubleDelta` — Calculates delta of deltas and writes it in compact binary form. Optimal compression rates are achieved for monotonic sequences with a constant stride, such as time series data. Can be used with any fixed-width type. Implements the algorithm used in Gorilla TSDB, extending it to support 64-bit types. Uses 1 extra bit for 32-byte deltas: 5-bit prefixes instead of 4-bit prefixes. For additional information, see Compressing Time Stamps in [Gorilla: A Fast, Scalable, In-Memory Time Series Database](http://www.vldb.org/pvldb/vol8/p1816-teller.pdf).
- `Gorilla` — Calculates XOR between current and previous value and writes it in compact binary form. Efficient when storing a series of floating point values that change slowly, because the best compression rate is achieved when neighboring values are binary equal. Implements the algorithm used in Gorilla TSDB, extending it to support 64-bit types. For additional information, see Compressing Values in [Gorilla: A Fast, Scalable, In-Memory Time Series Database](http://www.vldb.org/pvldb/vol8/p1816-teller.pdf).
- `T64` — Compression approach that crops unused high bits of values in integer data types (including `Enum`, `Date` and `DateTime`). At each step of its algorithm, codec takes a block of 64 values, puts them into 64x64 bit matrix, transposes it, crops the unused bits of values and returns the rest as a sequence. Unused bits are the bits, that don't differ between maximum and minimum values in the whole data part for which the compression is used.

`DoubleDelta` and `Gorilla` codecs are used in Gorilla TSDB as the components of its compressing algorithm. Gorilla approach is effective in scenarios when there is a sequence of slowly changing values with their timestamps. Timestamps are effectively compressed by the `DoubleDelta` codec, and values are effectively compressed by the `Gorilla` codec. For example, to get an effectively stored table, you can create it in the following configuration:

```sql
CREATE TABLE codec_example
(
    timestamp DateTime CODEC(DoubleDelta),
    slow_values Float32 CODEC(Gorilla)
)
ENGINE = MergeTree()
```

### Common purpose codecs {#create-query-common-purpose-codecs}

Codecs:

- `NONE` — No compression.
- `LZ4` — Lossless [data compression algorithm](https://github.com/lz4/lz4) used by default. Applies LZ4 fast compression.
- `LZ4HC[(level)]` — LZ4 HC (high compression) algorithm with configurable level. Default level: 9. Setting `level <= 0` applies the default level. Possible levels: [1, 12]. Recommended level range: [4, 9].
- `ZSTD[(level)]` — [ZSTD compression algorithm](https://en.wikipedia.org/wiki/Zstandard) with configurable `level`. Possible levels: [1, 22]. Default value: 1.

High compression levels useful for asymmetric scenarios, like compress once, decompress a lot of times. Greater levels stands for better compression and higher CPU usage.

## 临时表

ClickHouse支持临时表，其具有以下特征：

- 当回话结束时，临时表将随会话一起消失，这包含链接中断。
- 临时表仅能够使用Memory表引擎。
- 无法为临时表指定数据库。它是在数据库之外创建的。
- 如果临时表与另一个表名称相同，那么当在查询时没有显示的指定db的情况下，将优先使用临时表。
- 对于分布式处理，查询中使用的临时表将被传递到远程服务器。

可以使用下面的语法创建一个临时表：

```sql
CREATE TEMPORARY TABLE [IF NOT EXISTS] table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
)
```

大多数情况下，临时表不是手动创建的，只有在分布式查询处理中使用`(GLOBAL) IN`时为外部数据创建。更多信息，可以参考相关章节。

## 分布式DDL查询 （ON CLUSTER 子句）

对于 `CREATE`， `DROP`， `ALTER`，以及`RENAME`查询，系统支持其运行在整个集群上。
例如，以下查询将在`cluster`集群的所有节点上创建名为`all_hits`的`Distributed`表：

``` sql
CREATE TABLE IF NOT EXISTS all_hits ON CLUSTER cluster (p Date, i Int32) ENGINE = Distributed(cluster, default, hits)
```

为了能够正确的运行这种查询，每台主机必须具有相同的cluster声明（为了简化配置的同步，你可以使用zookeeper的方式进行配置）。同时这些主机还必须链接到zookeeper服务器。
这个查询将最终在集群的每台主机上运行，即使一些主机当前处于不可用状态。同时它还保证了所有的查询在单台主机中的执行顺序。
replicated系列表还没有支持`ALTER`查询。

## CREATE VIEW

``` sql
CREATE [MATERIALIZED] VIEW [IF NOT EXISTS] [db.]table_name [TO[db.]name] [ENGINE = engine] [POPULATE] AS SELECT ...
```

创建一个视图。它存在两种可选择的类型：普通视图与物化视图。

普通视图不存储任何数据，只是执行从另一个表中的读取。换句话说，普通视图只是保存了视图的查询，当从视图中查询时，此查询被作为子查询用于替换FROM子句。

举个例子，假设你已经创建了一个视图：

``` sql
CREATE VIEW view AS SELECT ...
```

还有一个查询：

``` sql
SELECT a, b, c FROM view
```

这个查询完全等价于：

``` sql
SELECT a, b, c FROM (SELECT ...)
```

物化视图存储的数据是由相应的SELECT查询转换得来的。

在创建物化视图时，你还必须指定表的引擎 - 将会使用这个表引擎存储数据。

目前物化视图的工作原理：当将数据写入到物化视图中SELECT子句所指定的表时，插入的数据会通过SELECT子句查询进行转换并将最终结果插入到视图中。

如果创建物化视图时指定了POPULATE子句，则在创建时将该表的数据插入到物化视图中。就像使用`CREATE TABLE ... AS SELECT ...`一样。否则，物化视图只会包含在物化视图创建后的新写入的数据。我们不推荐使用POPULATE，因为在视图创建期间写入的数据将不会写入其中。

当一个`SELECT`子句包含`DISTINCT`, `GROUP BY`, `ORDER BY`, `LIMIT`时，请注意，这些仅会在插入数据时在每个单独的数据块上执行。例如，如果你在其中包含了`GROUP BY`，则只会在查询期间进行聚合，但聚合范围仅限于单个批的写入数据。数据不会进一步被聚合。但是当你使用一些其他数据聚合引擎时这是例外的，如：`SummingMergeTree`。

目前对物化视图执行`ALTER`是不支持的，因此这可能是不方便的。如果物化视图是使用的`TO [db.]name`的方式进行构建的，你可以使用`DETACH`语句现将视图剥离，然后使用`ALTER`运行在目标表上，然后使用`ATTACH`将之前剥离的表重新加载进来。

视图看起来和普通的表相同。例如，你可以通过`SHOW TABLES`查看到它们。

没有单独的删除视图的语法。如果要删除视图，请使用`DROP TABLE`。

[来源文章](https://clickhouse.yandex/docs/en/query_language/create/) <!--hide-->
