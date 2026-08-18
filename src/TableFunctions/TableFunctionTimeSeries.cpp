#include <TableFunctions/TableFunctionTimeSeries.h>

#include <Access/Common/AccessFlags.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Storages/StorageProxy.h>
#include <Storages/StorageTimeSeries.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <TableFunctions/TableFunctionPrometheusQuery.h>
#include <TableFunctions/TableFunctionTimeSeriesSelector.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

namespace
{

/// A table function normally returns its nested storage directly, which makes the outer query
/// read it with the caller's context. TimeSeries targets are implementation tables, so keep the
/// target context with the proxy and use it for both local and Distributed reads.
class StorageTimeSeriesTargetProxy final : public StorageProxy
{
public:
    StorageTimeSeriesTargetProxy(const StorageID & table_id_, StoragePtr nested_, ContextPtr target_context_)
        : StorageProxy(table_id_)
        , nested(std::move(nested_))
        , target_context(std::move(target_context_))
    {
        auto nested_metadata = nested->getInMemoryMetadataPtr(target_context, false);
        setInMemoryMetadata(*nested_metadata);
    }

    StoragePtr getNested() const override { return nested; }

    QueryProcessingStage::Enum getQueryProcessingStage(
        ContextPtr /* context */,
        QueryProcessingStage::Enum to_stage,
        const StorageSnapshotPtr & /* storage_snapshot */,
        SelectQueryInfo & info) const override
    {
        auto nested_metadata = nested->getInMemoryMetadataPtr(target_context, false);
        auto nested_snapshot = nested->getStorageSnapshot(nested_metadata, target_context);
        return nested->getQueryProcessingStage(target_context, to_stage, nested_snapshot, info);
    }

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & /* storage_snapshot */,
        SelectQueryInfo & query_info,
        ContextPtr /* context */,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override
    {
        auto nested_metadata = nested->getInMemoryMetadataPtr(target_context, false);
        auto nested_snapshot = nested->getStorageSnapshot(nested_metadata, target_context);
        nested->read(
            query_plan,
            column_names,
            nested_snapshot,
            query_info,
            target_context,
            processed_stage,
            max_block_size,
            num_streams);
    }

private:
    const StoragePtr nested;
    const ContextPtr target_context;
};

}


template <ViewTarget::Kind target_kind>
void TableFunctionTimeSeriesTarget<target_kind>::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto & args_func = ast_function->as<ASTFunction &>();

    if (!args_func.arguments)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table function '{}' must have arguments.", name);

    auto & args = args_func.arguments->children;

    if ((args.size() != 1) && (args.size() != 2))
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                        "Table function '{}' requires one or two arguments: {}([database, ] time_series_table)", name, name);

    if (args.size() == 1)
    {
        /// timeSeriesMetrics( [my_db.]my_time_series_table )
        if (const auto * id = args[0]->as<ASTIdentifier>())
        {
            if (auto table_id = id->createTable())
                time_series_storage_id = table_id->getTableId();
        }
    }

    if (time_series_storage_id.empty())
    {
        for (auto & arg : args)
            arg = evaluateConstantExpressionOrIdentifierAsLiteral(arg, context);

        if (args.size() == 1)
        {
            /// timeSeriesMetrics( 'my_time_series_table' )
            time_series_storage_id.table_name = checkAndGetLiteralArgument<String>(args[0], "table_name");
        }
        else
        {
            /// timeSeriesMetrics( 'mydb', 'my_time_series_table' )
            time_series_storage_id.database_name = checkAndGetLiteralArgument<String>(args[0], "database_name");
            time_series_storage_id.table_name = checkAndGetLiteralArgument<String>(args[1], "table_name");
        }
    }

    if (time_series_storage_id.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Couldn't get a table name from the arguments of the {} table function", name);

    time_series_storage_id = context->resolveStorageID(time_series_storage_id);
    target_table_type_name = getTargetTable(context)->getName();
}


template <ViewTarget::Kind target_kind>
StoragePtr TableFunctionTimeSeriesTarget<target_kind>::getTargetTable(const ContextPtr & context) const
{
    auto time_series_storage = storagePtrToTimeSeries(DatabaseCatalog::instance().getTable(time_series_storage_id, context));
    return time_series_storage->getTargetTable(target_kind, context);
}


template <ViewTarget::Kind target_kind>
StoragePtr TableFunctionTimeSeriesTarget<target_kind>::executeImpl(
        const ASTPtr & /* ast_function */,
        ContextPtr context,
        const String & table_name,
        ColumnsDescription /* cached_columns */,
        bool is_insert_query) const
{
    if (is_insert_query)
    {
        context->checkAccess(AccessType::INSERT, time_series_storage_id);
        return getTargetTable(context);
    }

    checkTimeSeriesTableSelectAccess(
        context,
        time_series_storage_id,
        !context->isTimeSeriesTableFunctionReadWithOuterRowPolicyAllowed());
    auto target_context = getTimeSeriesTargetContext(context);
    auto target_table = getTargetTable(target_context);
    checkTimeSeriesTargetSelectAccess(context, time_series_storage_id, target_table);
    checkTimeSeriesTargetSelectRowPolicy(context, time_series_storage_id, target_table);
    return std::make_shared<StorageTimeSeriesTargetProxy>(
        StorageID(getDatabaseName(), table_name), std::move(target_table), std::move(target_context));
}

template <ViewTarget::Kind target_kind>
ColumnsDescription TableFunctionTimeSeriesTarget<target_kind>::getActualTableStructure(ContextPtr context, bool /* is_insert_query */) const
{
    context->checkAccess(AccessType::SELECT, time_series_storage_id);
    auto metadata_snapshot = getTargetTable(context)->getInMemoryMetadataPtr(context, false);
    return metadata_snapshot->columns;
}

template <ViewTarget::Kind target_kind>
const char * TableFunctionTimeSeriesTarget<target_kind>::getStorageEngineName() const
{
    return target_table_type_name.c_str();
}


void registerTableFunctionTimeSeries(TableFunctionFactory & factory);
void registerTableFunctionTimeSeries(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionTimeSeriesTarget<ViewTarget::Samples>>(
        {.description = R"DOCS_MD(
`timeSeriesSamples(db_name.time_series_table)` - Returns the [samples](/reference/engines/table-engines/integrations/time-series#samples-table) table
used by table `db_name.time_series_table` whose table engine is [TimeSeries](/reference/engines/table-engines/integrations/time-series):

```sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries SAMPLES samples_table
```

The function also works if the _samples_ table is inner:

```sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries SAMPLES INNER UUID '01234567-89ab-cdef-0123-456789abcdef'
```

The following queries are equivalent:

```sql
SELECT * FROM timeSeriesSamples(db_name.time_series_table);
SELECT * FROM timeSeriesSamples('db_name.time_series_table');
SELECT * FROM timeSeriesSamples('db_name', 'time_series_table');
```

<Note>
The function `timeSeriesSamples` has an alias `timeSeriesData` which is kept for backwards compatibility.
</Note>
)DOCS_MD", .category = FunctionDocumentation::Category::TableFunction}, {.allow_readonly = true});

    factory.registerAlias("timeSeriesData", "timeSeriesSamples");

    factory.registerFunction<TableFunctionTimeSeriesTarget<ViewTarget::Tags>>(
        {.description = R"DOCS_MD(
`timeSeriesTags(db_name.time_series_table)` - Returns the [tags](/reference/engines/table-engines/integrations/time-series#tags-table) table
used by table `db_name.time_series_table` whose table engine is the [TimeSeries](/reference/engines/table-engines/integrations/time-series) engine:

```sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries TAGS tags_table
```

The function also works if the _tags_ table is inner:

```sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries TAGS INNER UUID '01234567-89ab-cdef-0123-456789abcdef'
```

The following queries are equivalent:

```sql
SELECT * FROM timeSeriesTags(db_name.time_series_table);
SELECT * FROM timeSeriesTags('db_name.time_series_table');
SELECT * FROM timeSeriesTags('db_name', 'time_series_table');
```
)DOCS_MD", .category = FunctionDocumentation::Category::TableFunction}, {.allow_readonly = true});

    factory.registerFunction<TableFunctionTimeSeriesTarget<ViewTarget::Metrics>>(
        {.description = R"DOCS_MD(
`timeSeriesMetrics(db_name.time_series_table)` - Returns the [metrics](/reference/engines/table-engines/integrations/time-series#metrics-table) table
used by table `db_name.time_series_table` whose table engine is the [TimeSeries](/reference/engines/table-engines/integrations/time-series) engine:

```sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries METRICS metrics_table
```

The function also works if the _metrics_ table is inner:

```sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries METRICS INNER UUID '01234567-89ab-cdef-0123-456789abcdef'
```

The following queries are equivalent:

```sql
SELECT * FROM timeSeriesMetrics(db_name.time_series_table);
SELECT * FROM timeSeriesMetrics('db_name.time_series_table');
SELECT * FROM timeSeriesMetrics('db_name', 'time_series_table');
```
)DOCS_MD", .category = FunctionDocumentation::Category::TableFunction}, {.allow_readonly = true});

    factory.registerFunction<TableFunctionTimeSeriesSelector>(
        {.description = R"DOCS_MD(
Reads time series from a TimeSeries table filtered by a selector and with timestamps in a specified interval.
This function is similar to [range selectors](https://prometheus.io/docs/prometheus/latest/querying/basics/#range-vector-selectors) but it's used to implement [instant selectors](https://prometheus.io/docs/prometheus/latest/querying/basics/#instant-vector-selectors) too.

## Syntax {#syntax}

```sql
timeSeriesSelector('db_name', 'time_series_table', 'instant_query', min_time, max_time)
timeSeriesSelector(db_name.time_series_table, 'instant_query', min_time, max_time)
timeSeriesSelector('time_series_table', 'instant_query', min_time, max_time)
```

## Arguments {#arguments}

- `db_name` - The name of the database where a TimeSeries table is located.
- `time_series_table` - The name of a TimeSeries table.
- `instant_query` - An instant selector written in [PromQL syntax](https://prometheus.io/docs/prometheus/latest/querying/basics/#instant-vector-selectors), without `@` or `offset` modifiers.
- `min_time - Start timestamp, inclusive.
- `max_time - End timestamp, inclusive.

## Returned value {#returned-value}

The function returns three columns:
- `id` - Contains the identifiers of time series matching the specified selector.
- `timestamp` - Contains timestamps.
- `value` - Contains values.

There is no specific order for returned data.

## Example {#example}

```sql
SELECT * FROM timeSeriesSelector(mytable, 'http_requests{job="prometheus"}', now() - INTERVAL 10 MINUTES, now())
```
)DOCS_MD", .category = FunctionDocumentation::Category::TableFunction}, {.allow_readonly = true});

    factory.registerFunction<TableFunctionPrometheusQuery</* range = */ false>>(
        {.description = R"DOCS_MD(
Evaluates a prometheus query using data from a TimeSeries table.

## Syntax {#syntax}

```sql
prometheusQuery('db_name', 'time_series_table', 'promql_query', evaluation_time)
prometheusQuery(db_name.time_series_table, 'promql_query', evaluation_time)
prometheusQuery('time_series_table', 'promql_query', evaluation_time)
```

## Arguments {#arguments}

- `db_name` - The name of the database where a TimeSeries table is located.
- `time_series_table` - The name of a TimeSeries table.
- `promql_query` - A query written in [PromQL syntax](https://prometheus.io/docs/prometheus/latest/querying/basics/).
- `evaluation_time - The evaluation timestamp. To evaluate a query at the current time, use `now()` as `evaluation_time`.

## Returned value {#returned-value}

The function can returns different columns depending on the result type of the query passed to parameter `promql_query`:

| Result Type | Result Columns | Example |
|-------------|----------------|---------|
| vector      | tags Array(Tuple(String, String)), timestamp TimestampType, value ValueType | prometheusQuery(mytable, 'up') |
| matrix      | tags Array(Tuple(String, String)), time_series Array(Tuple(TimestampType, ValueType)) | prometheusQuery(mytable, 'up[1m]') |
| scalar      | scalar ValueType | prometheusQuery(mytable, '1h30m') |
| string      | string String | prometheusQuery(mytable, '"abc"') |

## Supported PromQL Features {#supported-promql-features}

### Selectors {#selectors}

Instant selectors, range selectors, label matchers (`=`, `!=`, `=~`, `!~`), offset modifiers, `@` timestamp modifiers, and subqueries.

### Functions {#functions}

| Category | Functions |
|----------|-----------|
| Range | `rate`, `irate`, `delta`, `idelta`, `increase`, `last_over_time`, `deriv`, `changes`, `resets` |
| Math | `abs`, `sgn`, `floor`, `ceil`, `sqrt`, `exp`, `ln`, `log2`, `log10`, `rad`, `deg`, `round`, `clamp`, `clamp_min`, `clamp_max` |
| Trig | `sin`, `cos`, `tan`, `asin`, `acos`, `atan`, `sinh`, `cosh`, `tanh`, `asinh`, `acosh`, `atanh` |
| DateTime | `day_of_week`, `day_of_month`, `days_in_month`, `day_of_year`, `minute`, `hour`, `month`, `year` |
| Label | `label_replace`, `label_join` |
| Type | `scalar`, `vector` |
| Histogram | `histogram_quantile` |
| Other | `time`, `pi` |

**Note**: `histogram_quantile` uses linear interpolation on classic histogram buckets (identified by the `le` label). Native histograms are not supported. The `phi` (quantile level) argument must be a constant scalar. Expressions that vary per step, such as `histogram_quantile(time() / 1000, ...)`, are rejected with a `NOT_IMPLEMENTED` exception.

### Operators {#operators}

Arithmetic (`+`, `-`, `*`, `/`, `%`, `^`, `atan2`) and comparison (`==`, `!=`, `<`, `>`, `<=`, `>=` with optional `bool`) binary operators, with `on()`/`ignoring()` and `group_left()`/`group_right()` modifiers.

Logical set operators `and`, `or`, and `unless`, with `on()`/`ignoring()` modifiers.

Unary operators `+` and `-`.

### Aggregation Operators {#aggregation-operators}

`sum`, `avg`, `min`, `max`, `count`, `stddev`, `stdvar`, `group`, `quantile`, `topk`, `bottomk`, `limitk` — with optional `by()` or `without()` modifiers.

### Not yet supported {#not-yet-supported}

- Aggregation operator `count_values`
- Range functions `predict_linear`, `avg_over_time`, `min_over_time`, `max_over_time`, `sum_over_time`, `count_over_time`, `quantile_over_time`, `stddev_over_time`, `stdvar_over_time`, `present_over_time`, `absent_over_time`, `mad_over_time`, `first_over_time`, `ts_of_min_over_time`, `ts_of_max_over_time`, `ts_of_last_over_time`, `ts_of_first_over_time`
- Function `absent`

## Example {#example}

```sql
SELECT * FROM prometheusQuery(mytable, 'rate(http_requests{job="prometheus"}[10m])[1h:10m]', now())
```
)DOCS_MD", .category = FunctionDocumentation::Category::TableFunction}, {.allow_readonly = true});
    factory.registerFunction<TableFunctionPrometheusQuery</* range = */ true>>(
        {.description = R"DOCS_MD(
Evaluates a prometheus query using data from a TimeSeries table over a range of evaluation times.

## Syntax {#syntax}

```sql
prometheusQueryRange('db_name', 'time_series_table', 'promql_query', start_time, end_time, step)
prometheusQueryRange(db_name.time_series_table, 'promql_query', start_time, end_time, step)
prometheusQueryRange('time_series_table', 'promql_query', start_time, end_time, step)
```

## Arguments {#arguments}

- `db_name` - The name of the database where a TimeSeries table is located.
- `time_series_table` - The name of a TimeSeries table.
- `promql_query` - A query written in [PromQL syntax](https://prometheus.io/docs/prometheus/latest/querying/basics/).
- `start_time` - The start time of the evaluation range.
- `end_time` - The end time of the evaluation range.
- `step` - The step used to iterate the evaluation time from `start_time` to `end_time` (inclusively).

## Returned value {#returned-value}

The function can returns different columns depending on the result type of the query passed to parameter `promql_query`:

| Result Type | Result Columns | Example |
|-------------|----------------|---------|
| vector      | tags Array(Tuple(String, String)), timestamp TimestampType, value ValueType | prometheusQuery(mytable, 'up') |
| matrix      | tags Array(Tuple(String, String)), time_series Array(Tuple(TimestampType, ValueType)) | prometheusQuery(mytable, 'up[1m]') |
| scalar      | scalar ValueType | prometheusQuery(mytable, '1h30m') |
| string      | string String | prometheusQuery(mytable, '"abc"') |

## Supported PromQL Features {#supported-promql-features}

### Selectors {#selectors}

Instant selectors, range selectors, label matchers (`=`, `!=`, `=~`, `!~`), offset modifiers, `@` timestamp modifiers, and subqueries.

### Functions {#functions}

| Category | Functions |
|----------|-----------|
| Range | `rate`, `irate`, `delta`, `idelta`, `increase`, `last_over_time`, `deriv`, `changes`, `resets` |
| Math | `abs`, `sgn`, `floor`, `ceil`, `sqrt`, `exp`, `ln`, `log2`, `log10`, `rad`, `deg`, `round`, `clamp`, `clamp_min`, `clamp_max` |
| Trig | `sin`, `cos`, `tan`, `asin`, `acos`, `atan`, `sinh`, `cosh`, `tanh`, `asinh`, `acosh`, `atanh` |
| DateTime | `day_of_week`, `day_of_month`, `days_in_month`, `day_of_year`, `minute`, `hour`, `month`, `year` |
| Label | `label_replace`, `label_join` |
| Type | `scalar`, `vector` |
| Histogram | `histogram_quantile` |
| Other | `time`, `pi` |

**Note**: `histogram_quantile` uses linear interpolation on classic histogram buckets (identified by the `le` label). Native histograms are not supported. The `phi` (quantile level) argument must be a constant scalar. Expressions that vary per step, such as `histogram_quantile(time() / 1000, ...)`, are rejected with a `NOT_IMPLEMENTED` exception.

### Operators {#operators}

Arithmetic (`+`, `-`, `*`, `/`, `%`, `^`, `atan2`) and comparison (`==`, `!=`, `<`, `>`, `<=`, `>=` with optional `bool`) binary operators, with `on()`/`ignoring()` and `group_left()`/`group_right()` modifiers.

Logical set operators `and`, `or`, and `unless`, with `on()`/`ignoring()` modifiers.

Unary operators `+` and `-`.

### Aggregation Operators {#aggregation-operators}

`sum`, `avg`, `min`, `max`, `count`, `stddev`, `stdvar`, `group`, `quantile`, `topk`, `bottomk`, `limitk` — with optional `by()` or `without()` modifiers.

### Not yet supported {#not-yet-supported}

- Aggregation operator `count_values`
- Range functions `predict_linear`, `avg_over_time`, `min_over_time`, `max_over_time`, `sum_over_time`, `count_over_time`, `quantile_over_time`, `stddev_over_time`, `stdvar_over_time`, `present_over_time`, `absent_over_time`, `mad_over_time`, `first_over_time`, `ts_of_min_over_time`, `ts_of_max_over_time`, `ts_of_last_over_time`, `ts_of_first_over_time`
- Function `absent`

## Example {#example}

```sql
SELECT * FROM prometheusQueryRange(mytable, 'rate(http_requests{job="prometheus"}[10m])[1h:10m]', now() - INTERVAL 10 MINUTES, now(), INTERVAL 1 MINUTE)
```
)DOCS_MD", .category = FunctionDocumentation::Category::TableFunction}, {.allow_readonly = true});
}

}
