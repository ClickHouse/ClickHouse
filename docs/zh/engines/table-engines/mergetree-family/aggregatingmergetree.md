# AggregatingMergeTree {#aggregatingmergetree}

该引擎继承自 [MergeTree](mergetree.md)，并改变了数据片段的合并逻辑。 ClickHouse 会将一个数据片段内所有具有相同主键（准确的说是 [排序键](../../../engines/table-engines/mergetree-family/mergetree.md)）的行替换成一行，这一行会存储一系列聚合函数的状态。

可以使用 `AggregatingMergeTree` 表来做增量数据的聚合统计，包括物化视图的数据聚合。

引擎使用以下类型来处理所有列：

-   [AggregateFunction](../../../sql-reference/data-types/aggregatefunction.md)
-   [SimpleAggregateFunction](../../../sql-reference/data-types/simpleaggregatefunction.md)

`AggregatingMergeTree` 适用于能够按照一定的规则缩减行数的情况。

## 建表 {#jian-biao}

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE = AggregatingMergeTree()
[PARTITION BY expr]
[ORDER BY expr]
[SAMPLE BY expr]
[TTL expr]
[SETTINGS name=value, ...]
```

语句参数的说明，请参阅 [建表语句描述](../../../sql-reference/statements/create.md#create-table-query)。

**子句**

创建 `AggregatingMergeTree` 表时，需用跟创建 `MergeTree` 表一样的[子句](mergetree.md)。

<details markdown="1">

<summary>已弃用的建表方法</summary>

!!! attention "注意"
    不要在新项目中使用该方法，可能的话，请将旧项目切换到上述方法。

``` sql
CREATE TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
(
    name1 [type1] [DEFAULT|MATERIALIZED|ALIAS expr1],
    name2 [type2] [DEFAULT|MATERIALIZED|ALIAS expr2],
    ...
) ENGINE [=] AggregatingMergeTree(date-column [, sampling_expression], (primary, key), index_granularity)
```

上面的所有参数的含义跟 `MergeTree` 中的一样。
</details>

## SELECT 和 INSERT {#select-he-insert}

要插入数据，需使用带有 -State- 聚合函数的 [INSERT SELECT](../../../sql-reference/statements/insert-into.md) 语句。
从 `AggregatingMergeTree` 表中查询数据时，需使用 `GROUP BY` 子句并且要使用与插入时相同的聚合函数，但后缀要改为 `-Merge` 。

对于 `SELECT` 查询的结果， `AggregateFunction` 类型的值对 ClickHouse 的所有输出格式都实现了特定的二进制表示法。在进行数据转储时，例如使用 `TabSeparated` 格式进行 `SELECT` 查询，那么这些转储数据也能直接用 `INSERT` 语句导回。

## 聚合物化视图的示例 {#ju-he-wu-hua-shi-tu-de-shi-li}

创建一个跟踪 `test.visits` 表的 `AggregatingMergeTree` 物化视图：

``` sql
CREATE MATERIALIZED VIEW test.basic
ENGINE = AggregatingMergeTree() PARTITION BY toYYYYMM(StartDate) ORDER BY (CounterID, StartDate)
AS SELECT
    CounterID,
    StartDate,
    sumState(Sign)    AS Visits,
    uniqState(UserID) AS Users
FROM test.visits
GROUP BY CounterID, StartDate;
```

向 `test.visits` 表中插入数据。

``` sql
INSERT INTO test.visits ...
```

数据会同时插入到表和视图中，并且视图 `test.basic` 会将里面的数据聚合。

要获取聚合数据，我们需要在 `test.basic` 视图上执行类似 `SELECT ... GROUP BY ...` 这样的查询 ：

``` sql
SELECT
    StartDate,
    sumMerge(Visits) AS Visits,
    uniqMerge(Users) AS Users
FROM test.basic
GROUP BY StartDate
ORDER BY StartDate;
```

[来源文章](https://clickhouse.com/docs/en/operations/table_engines/aggregatingmergetree/) <!--hide-->
