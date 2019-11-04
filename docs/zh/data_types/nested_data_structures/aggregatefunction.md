# AggregateFunction(name, types_of_arguments...) {#data_type-aggregatefunction}

聚合函数中的中间状态。可以在聚合函数中通过`-State`后缀来访问它。为了在后续访问聚合数据，请您必须使用含有`-Merge`后缀的相同聚合函数。

`AggregateFunction` — 参数化的数据类型。

**Parameters**

- 聚合函数名

    如果函数是参数化的，则还要指定其参数。

- 聚合函数参数的类型

**Example**

```sql
CREATE TABLE t
(
    column1 AggregateFunction(uniq, UInt64),
    column2 AggregateFunction(anyIf, String, UInt8),
    column3 AggregateFunction(quantiles(0.5, 0.9), UInt64)
) ENGINE = ...
```

ClickHouse中支持的聚合函数包括 [uniq](../../query_language/agg_functions/reference.md#agg_function-uniq), anyIf ([any](../../query_language/agg_functions/reference.md#agg_function-any)+[If](../../query_language/agg_functions/combinators.md#agg-functions-combinator-if)) 及 [quantiles](../../query_language/agg_functions/reference.md)

## Usage

### Data Insertion

当写入数据时，请使用含有`-State`后缀的函数的`INSERT SELECT`语句。

**Function examples**

```sql
uniqState(UserID)
quantilesState(0.5, 0.9)(SendTiming)
```

不同于相对应的`uniq`和`quantiles`，含有`-State`后缀的函数返回的是状态，而不是最终值。 也就是说，它们返回的是`AggregateFunction`类型的值。

在`SELECT`查询的结果中，`AggregateFunction`类型的值能够表示所有ClickHouse输出格式所对应的特定二进制形式。 例如，如果使用`SELECT`查询将数据转储为`TabSeparated`格式，可以使用`INSERT`查询将其转储回去。

### Data Selection

当从`AggregatingMergeTree`表中查询数据时，请使用`GROUP BY`子句，并使用含有`-Merge`后缀的相同聚合函数来写入数据。

后缀为`-Merge`的聚合函数，可以对一组状态值进行组合，然后返回完整的数据聚合结果。

例如，以下的两个查询返回相同的结果：

```sql
SELECT uniq(UserID) FROM table

SELECT uniqMerge(state) FROM (SELECT uniqState(UserID) AS state FROM table GROUP BY RegionID)
```

## Usage Example

请参阅 [AggregatingMergeTree](../../operations/table_engines/aggregatingmergetree.md) 的说明


[来源文章](https://clickhouse.yandex/docs/en/data_types/nested_data_structures/aggregatefunction/) <!--hide-->