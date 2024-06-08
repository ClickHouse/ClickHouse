---
slug: /zh/sql-reference/operators/in
machine_translated: true
machine_translated_rev: 5decc73b5dc60054f19087d3690c4eb99446a6c3
---

# IN 操作符 {#select-in-operators}

该 `IN`, `NOT IN`, `GLOBAL IN`，和 `GLOBAL NOT IN` 运算符是单独考虑的，因为它们的功能相当丰富。

运算符的左侧是单列或元组。

例:

``` sql
SELECT UserID IN (123, 456) FROM ...
SELECT (CounterID, UserID) IN ((34, 123), (101500, 456)) FROM ...
```

如果左侧是索引中的单列，而右侧是一组常量，则系统将使用索引处理查询。

请不要列举太多具体的常量 (比方说 几百万条)。如果数据集非常大，请把它放在一张临时表里（例如，参考章节[用于查询处理的外部数据](../../engines/table-engines/special/external-data.md)），然后使用子查询。

运算符的右侧可以是一组常量表达式、一组带有常量表达式的元组（如上面的示例所示），或括号中的数据库表或SELECT子查询的名称。

如果运算符的右侧是表的名称（例如, `UserID IN users`），这相当于子查询 `UserID IN (SELECT * FROM users)`. 使用与查询一起发送的外部数据时，请使用此选项。 例如，查询可以与一组用户Id一起发送到 ‘users’ 应过滤的临时表。

如果运算符的右侧是具有Set引擎的表名（始终位于RAM中的准备好的数据集），则不会为每个查询重新创建数据集。

子查询可以指定多个用于筛选元组的列。
示例:

``` sql
SELECT (CounterID, UserID) IN (SELECT CounterID, UserID FROM ...) FROM ...
```

IN运算符左侧和右侧的列应具有相同的类型。

IN运算符和子查询可能出现在查询的任何部分，包括聚合函数和lambda函数。
示例:

``` sql
SELECT
    EventDate,
    avg(UserID IN
    (
        SELECT UserID
        FROM test.hits
        WHERE EventDate = toDate('2014-03-17')
    )) AS ratio
FROM test.hits
GROUP BY EventDate
ORDER BY EventDate ASC
```

``` text
┌──EventDate─┬────ratio─┐
│ 2014-03-17 │        1 │
│ 2014-03-18 │ 0.807696 │
│ 2014-03-19 │ 0.755406 │
│ 2014-03-20 │ 0.723218 │
│ 2014-03-21 │ 0.697021 │
│ 2014-03-22 │ 0.647851 │
│ 2014-03-23 │ 0.648416 │
└────────────┴──────────┘
```

对于3月17日后的每一天，计算3月17日访问该网站的用户所做的浏览量百分比。
IN子句中的子查询始终只在单个服务器上运行一次。 没有依赖子查询。

## 空处理 {#in-null-processing}

在请求处理过程中， `IN` 运算符假定运算的结果 [NULL](../../sql-reference/syntax.md#null-literal) 总是等于 `0`，无论是否 `NULL` 位于操作员的右侧或左侧。 `NULL` 值不包含在任何数据集中，彼此不对应，并且在以下情况下无法进行比较 [transform_null_in=0](../../operations/settings/settings.md#transform_null_in).

下面是一个例子 `t_null` 表:

``` text
┌─x─┬────y─┐
│ 1 │ ᴺᵁᴸᴸ │
│ 2 │    3 │
└───┴──────┘
```

运行查询 `SELECT x FROM t_null WHERE y IN (NULL,3)` 为您提供以下结果:

``` text
┌─x─┐
│ 2 │
└───┘
```

你可以看到，在其中的行 `y = NULL` 被抛出的查询结果。 这是因为ClickHouse无法决定是否 `NULL` 包含在 `(NULL,3)` 设置，返回 `0` 作为操作的结果，和 `SELECT` 从最终输出中排除此行。

``` sql
SELECT y IN (NULL, 3)
FROM t_null
```

``` text
┌─in(y, tuple(NULL, 3))─┐
│                     0 │
│                     1 │
└───────────────────────┘
```

## 分布式子查询 {#select-distributed-subqueries}

带子查询的IN-s有两个选项（类似于连接）：normal `IN` / `JOIN` 和 `GLOBAL IN` / `GLOBAL JOIN`. 它们在分布式查询处理的运行方式上有所不同。

:::info "注意"
请记住，下面描述的算法可能会有不同的工作方式取决于 [设置](../../operations/settings/settings.md) `distributed_product_mode` 设置。
:::

当使用常规IN时，查询被发送到远程服务器，并且它们中的每个服务器都在运行子查询 `IN` 或 `JOIN` 条款

使用时 `GLOBAL IN` / `GLOBAL JOINs`，首先所有的子查询都运行 `GLOBAL IN` / `GLOBAL JOINs`，并将结果收集在临时表中。 然后将临时表发送到每个远程服务器，其中使用此临时数据运行查询。

对于非分布式查询，请使用常规 `IN` / `JOIN`.

在使用子查询时要小心 `IN` / `JOIN` 用于分布式查询处理的子句。

让我们来看看一些例子。 假设集群中的每个服务器都有一个正常的 **local_table**. 每个服务器还具有 **distributed_table** 表与 **分布** 类型，它查看群集中的所有服务器。

对于查询 **distributed_table**，查询将被发送到所有远程服务器，并使用以下命令在其上运行 **local_table**.

例如，查询

``` sql
SELECT uniq(UserID) FROM distributed_table
```

将被发送到所有远程服务器

``` sql
SELECT uniq(UserID) FROM local_table
```

并且并行运行它们中的每一个，直到达到可以结合中间结果的阶段。 然后将中间结果返回给请求者服务器并在其上合并，并将最终结果发送给客户端。

现在让我们检查一个查询IN:

``` sql
SELECT uniq(UserID) FROM distributed_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM local_table WHERE CounterID = 34)
```

-   计算两个网站的受众的交集。

此查询将以下列方式发送到所有远程服务器

``` sql
SELECT uniq(UserID) FROM local_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM local_table WHERE CounterID = 34)
```

换句话说，IN子句中的数据集将在每台服务器上独立收集，仅在每台服务器上本地存储的数据中收集。

如果您已经为此情况做好准备，并且已经将数据分散到群集服务器上，以便单个用户Id的数据完全驻留在单个服务器上，则这将正常和最佳地工作。 在这种情况下，所有必要的数据将在每台服务器上本地提供。 否则，结果将是不准确的。 我们将查询的这种变体称为 “local IN”.

若要更正数据在群集服务器上随机传播时查询的工作方式，可以指定 **distributed_table** 在子查询中。 查询如下所示:

``` sql
SELECT uniq(UserID) FROM distributed_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM distributed_table WHERE CounterID = 34)
```

此查询将以下列方式发送到所有远程服务器

``` sql
SELECT uniq(UserID) FROM local_table WHERE CounterID = 101500 AND UserID IN (SELECT UserID FROM distributed_table WHERE CounterID = 34)
```

子查询将开始在每个远程服务器上运行。 由于子查询使用分布式表，因此每个远程服务器上的子查询将重新发送到每个远程服务器

``` sql
SELECT UserID FROM local_table WHERE CounterID = 34
```

例如，如果您有100台服务器的集群，则执行整个查询将需要10,000个基本请求，这通常被认为是不可接受的。

在这种情况下，应始终使用GLOBAL IN而不是IN。 让我们来看看它是如何工作的查询

``` sql
SELECT uniq(UserID) FROM distributed_table WHERE CounterID = 101500 AND UserID GLOBAL IN (SELECT UserID FROM distributed_table WHERE CounterID = 34)
```

请求者服务器将运行子查询

``` sql
SELECT UserID FROM distributed_table WHERE CounterID = 34
```

结果将被放在RAM中的临时表中。 然后请求将被发送到每个远程服务器

``` sql
SELECT uniq(UserID) FROM local_table WHERE CounterID = 101500 AND UserID GLOBAL IN _data1
```

和临时表 `_data1` 将通过查询发送到每个远程服务器（临时表的名称是实现定义的）。

这比使用正常IN更优化。 但是，请记住以下几点:

1.  创建临时表时，数据不是唯一的。 要减少通过网络传输的数据量，请在子查询中指定DISTINCT。 （你不需要为正常人做这个。)
2.  临时表将被发送到所有远程服务器。 传输不考虑网络拓扑。 例如，如果10个远程服务器驻留在与请求者服务器非常远程的数据中心中，则数据将通过通道发送10次到远程数据中心。 使用GLOBAL IN时尽量避免使用大型数据集。
3.  将数据传输到远程服务器时，无法配置网络带宽限制。 您可能会使网络过载。
4.  尝试跨服务器分发数据，以便您不需要定期使用GLOBAL IN。
5.  如果您需要经常使用GLOBAL IN，请规划ClickHouse集群的位置，以便单个副本组驻留在不超过一个数据中心中，并且它们之间具有快速网络，以便可以完全在单个数据中心内处理查询。

这也是有意义的，在指定一个本地表 `GLOBAL IN` 子句，以防此本地表仅在请求者服务器上可用，并且您希望在远程服务器上使用来自它的数据。
