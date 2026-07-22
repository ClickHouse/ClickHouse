#include <TableFunctions/TableFunctionTimeSeries.h>

#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
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
        const String & /* table_name */,
        ColumnsDescription /* cached_columns */,
        bool /* is_insert_query */) const
{
    return getTargetTable(context);
}

template <ViewTarget::Kind target_kind>
ColumnsDescription TableFunctionTimeSeriesTarget<target_kind>::getActualTableStructure(ContextPtr context, bool /* is_insert_query */) const
{
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
)DOCS_MD", .category = FunctionDocumentation::Category::TableFunction});

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
)DOCS_MD", .category = FunctionDocumentation::Category::TableFunction});

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
)DOCS_MD", .category = FunctionDocumentation::Category::TableFunction});

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

## Returned value {#returned_value}

The function returns three columns:
- `id` - Contains the identifiers of time series matching the specified selector.
- `timestamp` - Contains timestamps.
- `value` - Contains values.

There is no specific order for returned data.

## Example {#example}

```sql
SELECT * FROM timeSeriesSelector(mytable, 'http_requests{job="prometheus"}', now() - INTERVAL 10 MINUTES, now())
```
)DOCS_MD", .category = FunctionDocumentation::Category::TableFunction});

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

## Returned value {#returned_value}

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
| Range    | `rate`, `irate`, `delta`, `idelta`, `last_over_time` |
| Math     | `abs`, `sgn`, `floor`, `ceil`, `sqrt`, `exp`, `ln`, `log2`, `log10`, `rad`, `deg` |
| Trig     | `sin`, `cos`, `tan`, `asin`, `acos`, `atan`, `sinh`, `cosh`, `tanh`, `asinh`, `acosh`, `atanh` |
| DateTime | `day_of_week`, `day_of_month`, `days_in_month`, `day_of_year`, `minute`, `hour`, `month`, `year` |
| Type     | `scalar`, `vector` |
| Histogram | `histogram_quantile` |
| Other    | `time`, `pi` |

**Note**: `histogram_quantile` uses linear interpolation on classic histogram buckets (identified by the `le` label). Native histograms are not yet supported, and the `phi` (quantile level) argument must currently be a constant scalar — expressions that vary per step such as `histogram_quantile(time() / 1000, ...)` are rejected with a `NOT_IMPLEMENTED` error.

### Operators {#operators}

All arithmetic (`+`, `-`, `*`, `/`, `%`, `^`), comparison (`==`, `!=`, `<`, `>`, `<=`, `>=` with optional `bool`), and logical (`and`, `or`, `unless`) binary operators, with `on()`/`ignoring()` and `group_left()`/`group_right()` modifiers.

Unary operators `+` and `-`.

### Aggregation Operators {#aggregation-operators}

`sum`, `avg`, `min`, `max`, `count`, `stddev`, `stdvar`, `group`, `quantile`, `topk`, `bottomk`, `limitk` — with optional `by()` or `without()` modifiers.

Not yet supported: `count_values`.

## Example {#example}

```sql
SELECT * FROM prometheusQuery(mytable, 'rate(http_requests{job="prometheus"}[10m])[1h:10m]', now())
```
)DOCS_MD", .category = FunctionDocumentation::Category::TableFunction});
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

## Returned value {#returned_value}

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
| Range    | `rate`, `irate`, `delta`, `idelta`, `last_over_time` |
| Math     | `abs`, `sgn`, `floor`, `ceil`, `sqrt`, `exp`, `ln`, `log2`, `log10`, `rad`, `deg` |
| Trig     | `sin`, `cos`, `tan`, `asin`, `acos`, `atan`, `sinh`, `cosh`, `tanh`, `asinh`, `acosh`, `atanh` |
| DateTime | `day_of_week`, `day_of_month`, `days_in_month`, `day_of_year`, `minute`, `hour`, `month`, `year` |
| Type     | `scalar`, `vector` |
| Histogram | `histogram_quantile` |
| Other    | `time`, `pi` |

**Note**: `histogram_quantile` uses linear interpolation on classic histogram buckets (identified by the `le` label). Native histograms are not yet supported, and the `phi` (quantile level) argument must currently be a constant scalar — expressions that vary per step such as `histogram_quantile(time() / 1000, ...)` are rejected with a `NOT_IMPLEMENTED` error.

### Operators {#operators}

All arithmetic (`+`, `-`, `*`, `/`, `%`, `^`), comparison (`==`, `!=`, `<`, `>`, `<=`, `>=` with optional `bool`), and logical (`and`, `or`, `unless`) binary operators, with `on()`/`ignoring()` and `group_left()`/`group_right()` modifiers.

Unary operators `+` and `-`.

### Aggregation Operators {#aggregation-operators}

`sum`, `avg`, `min`, `max`, `count`, `stddev`, `stdvar`, `group`, `quantile`, `topk`, `bottomk`, `limitk` — with optional `by()` or `without()` modifiers.

Not yet supported: `count_values`.

## Example {#example}

```sql
SELECT * FROM prometheusQueryRange(mytable, 'rate(http_requests{job="prometheus"}[10m])[1h:10m]', now() - INTERVAL 10 MINUTES, now(), INTERVAL 1 MINUTE)
```
)DOCS_MD", .category = FunctionDocumentation::Category::TableFunction});
}

}
