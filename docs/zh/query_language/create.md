## 创建数据库

创建 `db_name` 数据库。

```sql
CREATE DATABASE [IF NOT EXISTS] db_name
```

数据库是一个包含多个表的目录，如果在CREATE DATABASE语句中包含`IF NOT EXISTS`，则在数据库已经存在的情况下查询也不会返回错误。

<a name="query_language-queries-create_table"></a>

## 创建表

`CREATE TABLE` 语句有几种形式.

```sql
CREATE [TEMPORARY] TABLE [IF NOT EXISTS] [db.]name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1]，
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2]，
    ...
) ENGINE = engine
```

如果`db`没有设置， 在数据库`db`中或者当前数据库中， 创建一个表名为`name`的表， 在括号和`engine` 引擎中指定结构。 表的结构是一个列描述的列表。 如果引擎支持索引， 则他们将是表引擎的参数。

表结构是一个列描述的列表。 如果引擎支持索引， 他们以表引擎的参数表示。

在最简单的情况， 一个列描述是'命名类型'。 例如: RegionID UInt32。 对于默认值， 表达式也能够被定义。

```sql
CREATE [TEMPORARY] TABLE [IF NOT EXISTS] [db.]name AS [db2.]name2 [ENGINE = engine]
```

创建一个表， 其结构与另一个表相同。 你能够为此表指定一个不同的引擎。 如果引擎没有被指定， 相同的引擎将被用于`db2。name2`表上。

```sql
CREATE [TEMPORARY] TABLE [IF NOT EXISTS] [db.]name ENGINE = engine AS SELECT ...
```

创建一个表，其结构类似于 SELECT 查询后的结果， 带有`engine` 引擎， 从 SELECT查询数据填充它。

在所有情况下，如果`IF NOT EXISTS`被指定， 如果表已经存在， 查询并不返回一个错误。 在这种情况下， 查询并不做任何事情。

### 默认值

列描述能够为默认值指定一个表达式， 其中一个方法是:DEFAULT expr， MATERIALIZED expr， ALIAS expr。 
例如: URLDomain String DEFAULT domain(URL)。

如果默认值的一个表达式没有定义， 如果字段是数字类型， 默认值是将设置为0， 如果是字符类型， 则设置为空字符串， 日期类型则设置为 0000-00-00 或者 0000-00-00 00:00:00(时间戳)。 NULLs 则不支持。

如果默认表达式被定义， 字段类型是可选的。 如果没有明确的定义类型， 则将使用默认表达式。 例如: EventDate DEFAULT toDate(EventTime) – `Date` 类型将用于 `EventDate` 字段。

如果数据类型和默认表达式被明确定义， 此表达式将使用函数被转换为特定的类型。 例如: Hits UInt32 DEFAULT 0 与 Hits UInt32 DEFAULT toUInt32(0)是等价的。

默认表达是可能被定义为一个任意的表达式，如表的常量和字段。 当创建和更改表结构时， 它将检查表达式是否包含循环。 对于 INSERT操作来说， 它将检查表达式是否可解析 – 所有的字段通过传参后进行计算。

`DEFAULT expr`

正常的默认值。 如果 INSERT 查询并没有指定对应的字段， 它将通过计算对应的表达式来填充。

`物化表达式`

物化表达式。 此类型字段并没有指定插入操作， 因为它经常执行计算任务。 对一个插入操作， 无字段列表， 那么这些字段将不考虑。 另外， 当在一个SELECT查询语句中使用星号时， 此字段并不被替换。 这将保证INSERT INTO SELECT * FROM 的不可变性。

`别名表达式`

别名。 此字段不存储在表中。 
此列的值不插入到表中， 当在一个SELECT查询语句中使用星号时，此字段并不被替换。 
它能够用在 SELECTs中，如果别名在查询解析时被扩展。

当使用更新查询添加一个新的字段， 这些列的旧值不被写入。 相反， 新字段没有值，当读取旧值时， 表达式将被计算。 然而，如果运行表达式需要不同的字段， 这些字段将被读取 ， 但是仅读取相关的数据块。

如果你添加一个新的字段到表中， 然后改变它的默认表达式， 对于使用的旧值将更改(对于此数据， 值不保存在磁盘上)。 当运行背景线程时， 缺少合并数据块的字段数据写入到合并数据块中。

在嵌套数据结构中设置默认值是不允许的。


### 临时表

在任何情况下， 如果临时表被指定， 一个临时表将被创建。 临时表有如下的特性:

- 当会话结束后， 临时表将删除，或者连接丢失。
- 一个临时表使用内存表引擎创建。 其他的表引擎不支持临时表。
- 数据库不能为一个临时表指定。 它将创建在数据库之外。
- 如果一个临时表与另外的表有相同的名称 ，一个查询指定了表名并没有指定数据库， 将使用临时表。
- 对于分布式查询处理， 查询中的临时表将被传递给远程服务器。

在大多数情况下， 临时表并不能手工创建， 但当查询外部数据或使用分布式全局(GLOBAL)IN时，可以创建临时表。 

分布式 DDL 查询 (ON CLUSTER clause)
----------------------------------------------

`CREATE`， `DROP`， `ALTER`， 和 `RENAME` 查询支持在集群上分布式执行。 例如， 如下的查询在集群中的每个机器节点上创建了 all_hits Distributed 表:

```sql
CREATE TABLE IF NOT EXISTS all_hits ON CLUSTER cluster (p Date， i Int32) ENGINE = Distributed(cluster， default， hits)
```

为了正确执行这些语句，每个节点必须有相同的集群设置(为了简化同步配置，可以使用 zookeeper 来替换)。 这些节点也可以连接到ZooKeeper 服务器。
查询语句会在每个节点上执行， 而`ALTER`查询目前暂不支持在同步表(replicated table)上执行。



## CREATE VIEW

```sql
CREATE [MATERIALIZED] VIEW [IF NOT EXISTS] [db.]name [TO[db.]name] [ENGINE = engine] [POPULATE] AS SELECT ...
```

创建一个视图。 有两种类型的视图: 正常视图和物化(MATERIALIZED)视图。

当创建一个物化视图时， 你必须指定表引擎 – 此表引擎用于存储数据

一个物化视图工作流程如下所示: 当插入数据到SELECT 查询指定的表中时， 插入数据部分通过SELECT查询部分来转换， 结果插入到视图中。

正常视图不保存任何数据， 但是可以从任意表中读取数据。 换句话说，正常视图可以看作是查询结果的一个结果缓存。 当从一个视图中读取数据时， 此查询可以看做是 FROM语句的子查询。

例如， 假设你已经创建了一个视图:

```sql
CREATE VIEW view AS SELECT ...
```

写了一个查询语句:

```sql
SELECT a, b, c FROM view
```
此查询完全等价于子查询:

```sql
SELECT a, b, c FROM (SELECT ...)
```

物化视图保存由SELECT语句查询转换的数据。

当创建一个物化视图时，你必须指定一个引擎 – 存储数据的目标引擎。

一个物化视图使用流程如下:  当插入数据到 SELECT 指定的表时， 插入数据部分通过SELECT 来转换， 同时结果被插入到视图中。


如果你指定了 POPULATE， 当创建时， 现有的表数据被插入到了视图中， 类似于 `CREATE TABLE ... AS SELECT ...` . 否则， 在创建视图之后，查询仅包含表中插入的数据. 我们不建议使用 POPULATE， 在视图创建过程中，插入到表中的数据不插入到其中.

一个`SELECT`查询可以包含 `DISTINCT`， `GROUP BY`， `ORDER BY`， `LIMIT`。。。 对应的转换在每个数据块上独立执行。 例如， 如果 GROUP BY 被设置， 数据将在插入过程中进行聚合， 但仅是在一个插入数据包中。数据不再进一步聚合。 当使用一个引擎时， 如SummingMergeTree，它将独立执行数据聚合。

视图看起来和正常表相同。 例如， 你可以使用 SHOW TABLES来列出视图表的相关信息。

物化视图的`ALTER`查询执行还没有完全开发出来， 因此使用上可能不方便。 如果物化视图使用 `TO [db。]name`， 你能够 `DETACH` 视图， 在目标表运行 `ALTER`， 然后 `ATTACH` 之前的 `DETACH`视图。

视图看起来和正常表相同。 例如， 你可以使用 `SHOW TABLES` 来列出视图表的相关信息。

因此并没有一个单独的SQL语句来删除视图。 为了删除一个视图， 可以使用  `DROP TABLE`。

