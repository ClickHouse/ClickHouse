#include <TableFunctions/TableFunctionPrometheusQuery.h>

#include <Parsers/ASTFunction.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/Converter.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <TableFunctions/TableFunctionFactory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


template <bool over_range>
void TableFunctionPrometheusQuery<over_range>::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto & args_func = ast_function->as<ASTFunction &>();

    if (!args_func.arguments)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table function '{}' must have arguments.", name);

    auto & args = args_func.arguments->children;
    config = StoragePrometheusQuery::getConfiguration(args, context, over_range);
}


template <bool over_range>
ColumnsDescription
TableFunctionPrometheusQuery<over_range>::getActualTableStructure(ContextPtr /* context */, bool /* is_insert_query */) const
{
    PrometheusQueryToSQL::Converter converter{config.promql_query, config.evaluation_settings};
    return converter.getResultColumns();
}


template <bool over_range>
StoragePtr TableFunctionPrometheusQuery<over_range>::executeImpl(
    const ASTPtr & /* ast_function */,
    ContextPtr context,
    const String & table_name,
    ColumnsDescription /* cached_columns */,
    bool is_insert_query) const
{
    auto columns = getActualTableStructure(context, is_insert_query);
    auto res = std::make_shared<StoragePrometheusQuery>(StorageID(getDatabaseName(), table_name), columns, config);
    res->startup();
    return res;
}


template class TableFunctionPrometheusQuery</* over_range = */ false>;
template class TableFunctionPrometheusQuery</* over_range = */ true>;


void registerTableFunctionTimeSeriesPrometheusQuery(TableFunctionFactory & factory);
void registerTableFunctionTimeSeriesPrometheusQuery(TableFunctionFactory & factory)
{
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
- `evaluation_time` - The evaluation timestamp, with millisecond or finer precision. To evaluate a query at the current time, use `now()` as `evaluation_time`.

## Returned value {#returned-value}

The function returns different columns depending on the result type of the query passed to parameter `promql_query`:

| Result Type | Result Columns | Example |
|-------------|----------------|---------|
| vector      | tags Array(Tuple(String, String)), timestamp DateTime64(S, TZ), value Float64 | prometheusQuery(mytable, 'up') |
| matrix      | tags Array(Tuple(String, String)), samples Array(Tuple(DateTime64(S, TZ), Float64)) | prometheusQuery(mytable, 'up[1m]') |
| scalar      | timestamp DateTime64(S, TZ), value Float64 | prometheusQuery(mytable, '1h30m') |
| string      | timestamp DateTime64(S, TZ), value String | prometheusQuery(mytable, '"abc"') |

The values are always `Float64` regardless of the type of the values in the TimeSeries table. The scale `S` of the timestamps is the scale
of the timestamps in the table, but not less than 3 (milliseconds). The time zone `TZ` is the time zone of `evaluation_time` if it has
type `DateTime` or `DateTime64` with a time zone, otherwise it is the time zone of the timestamps in the table.

The `samples` column is named `time_series` if the `TimeSeries` table has [version](/reference/engines/table-engines/integrations/time-series#schema-versioning) 2 or earlier.

## Supported PromQL Features {#supported-promql-features}

### Selectors {#selectors}

Instant selectors, range selectors, label matchers (`=`, `!=`, `=~`, `!~`), offset modifiers, `@` timestamp modifiers, and subqueries.

### Functions {#functions}

| Category | Functions |
|----------|-----------|
| Range | `rate`, `irate`, `delta`, `idelta`, `increase`, `last_over_time`, `first_over_time`, `sum_over_time`, `avg_over_time`, `count_over_time`, `max_over_time`, `min_over_time`, `ts_of_max_over_time`, `ts_of_min_over_time`, `ts_of_last_over_time`, `ts_of_first_over_time`, `deriv`, `changes`, `resets`, `present_over_time`, `absent_over_time`, `quantile_over_time`, `mad_over_time`, `predict_linear` |
| Math | `abs`, `sgn`, `floor`, `ceil`, `sqrt`, `exp`, `ln`, `log2`, `log10`, `rad`, `deg`, `round`, `clamp`, `clamp_min`, `clamp_max` |
| Trig | `sin`, `cos`, `tan`, `asin`, `acos`, `atan`, `sinh`, `cosh`, `tanh`, `asinh`, `acosh`, `atanh` |
| DateTime | `day_of_week`, `day_of_month`, `days_in_month`, `day_of_year`, `minute`, `hour`, `month`, `year` |
| Label | `label_replace`, `label_join` |
| Type | `scalar`, `vector` |
| Histogram | `histogram_quantile` |
| Other | `time`, `pi`, `absent` |

**Note**: `histogram_quantile` uses linear interpolation on classic histogram buckets (identified by the `le` label). Native histograms are not supported. The `phi` (quantile level) argument must be a constant scalar. Expressions that vary per step, such as `histogram_quantile(time() / 1000, ...)`, are rejected with a `NOT_IMPLEMENTED` exception.

**Note**: `ts_of_min_over_time`, `ts_of_max_over_time`, `ts_of_last_over_time`, `first_over_time`, `ts_of_first_over_time` and `mad_over_time` are experimental functions in Prometheus (enabled there with `--enable-feature=promql-experimental-functions`); ClickHouse evaluates them without requiring that flag.

### Operators {#operators}

Arithmetic (`+`, `-`, `*`, `/`, `%`, `^`, `atan2`) and comparison (`==`, `!=`, `<`, `>`, `<=`, `>=` with optional `bool`) binary operators, with `on()`/`ignoring()` and `group_left()`/`group_right()` modifiers.

Logical set operators `and`, `or`, and `unless`, with `on()`/`ignoring()` modifiers.

Unary operators `+` and `-`.

### Aggregation Operators {#aggregation-operators}

`sum`, `avg`, `min`, `max`, `count`, `count_values`, `stddev`, `stdvar`, `group`, `quantile`, `topk`, `bottomk`, `limitk` — with optional `by()` or `without()` modifiers.

### Not yet supported {#not-yet-supported}

- Range functions `stddev_over_time`, `stdvar_over_time`

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
- `start_time` - The start time of the evaluation range, with millisecond or finer precision.
- `end_time` - The end time of the evaluation range, with millisecond or finer precision.
- `step` - The step used to iterate the evaluation time from `start_time` to `end_time` (inclusively).

## Returned value {#returned-value}

The function returns different columns depending on the result type of the query passed to parameter `promql_query`:

| Result Type | Result Columns | Example |
|-------------|----------------|---------|
| vector      | tags Array(Tuple(String, String)), timestamp DateTime64(S, TZ), value Float64 | prometheusQuery(mytable, 'up') |
| matrix      | tags Array(Tuple(String, String)), samples Array(Tuple(DateTime64(S, TZ), Float64)) | prometheusQuery(mytable, 'up[1m]') |
| scalar      | timestamp DateTime64(S, TZ), value Float64 | prometheusQuery(mytable, '1h30m') |
| string      | timestamp DateTime64(S, TZ), value String | prometheusQuery(mytable, '"abc"') |

The values are always `Float64` regardless of the type of the values in the TimeSeries table. The scale `S` of the timestamps is the scale
of the timestamps in the table, but not less than 3 (milliseconds). The time zone `TZ` is the time zone of `start_time` and `end_time`
if they have type `DateTime` or `DateTime64` with the same time zone, otherwise it is the time zone of the timestamps in the table.

The `samples` column is named `time_series` if the `TimeSeries` table has [version](/reference/engines/table-engines/integrations/time-series#schema-versioning) 2 or earlier.

## Supported PromQL Features {#supported-promql-features}

### Selectors {#selectors}

Instant selectors, range selectors, label matchers (`=`, `!=`, `=~`, `!~`), offset modifiers, `@` timestamp modifiers, and subqueries.

### Functions {#functions}

| Category | Functions |
|----------|-----------|
| Range | `rate`, `irate`, `delta`, `idelta`, `increase`, `last_over_time`, `first_over_time`, `sum_over_time`, `avg_over_time`, `count_over_time`, `max_over_time`, `min_over_time`, `ts_of_max_over_time`, `ts_of_min_over_time`, `ts_of_last_over_time`, `ts_of_first_over_time`, `deriv`, `changes`, `resets`, `present_over_time`, `absent_over_time`, `quantile_over_time`, `mad_over_time`, `predict_linear` |
| Math | `abs`, `sgn`, `floor`, `ceil`, `sqrt`, `exp`, `ln`, `log2`, `log10`, `rad`, `deg`, `round`, `clamp`, `clamp_min`, `clamp_max` |
| Trig | `sin`, `cos`, `tan`, `asin`, `acos`, `atan`, `sinh`, `cosh`, `tanh`, `asinh`, `acosh`, `atanh` |
| DateTime | `day_of_week`, `day_of_month`, `days_in_month`, `day_of_year`, `minute`, `hour`, `month`, `year` |
| Label | `label_replace`, `label_join` |
| Type | `scalar`, `vector` |
| Histogram | `histogram_quantile` |
| Other | `time`, `pi`, `absent` |

**Note**: `histogram_quantile` uses linear interpolation on classic histogram buckets (identified by the `le` label). Native histograms are not supported. The `phi` (quantile level) argument must be a constant scalar. Expressions that vary per step, such as `histogram_quantile(time() / 1000, ...)`, are rejected with a `NOT_IMPLEMENTED` exception.

**Note**: `ts_of_min_over_time`, `ts_of_max_over_time`, `ts_of_last_over_time`, `first_over_time`, `ts_of_first_over_time` and `mad_over_time` are experimental functions in Prometheus (enabled there with `--enable-feature=promql-experimental-functions`); ClickHouse evaluates them without requiring that flag.

### Operators {#operators}

Arithmetic (`+`, `-`, `*`, `/`, `%`, `^`, `atan2`) and comparison (`==`, `!=`, `<`, `>`, `<=`, `>=` with optional `bool`) binary operators, with `on()`/`ignoring()` and `group_left()`/`group_right()` modifiers.

Logical set operators `and`, `or`, and `unless`, with `on()`/`ignoring()` modifiers.

Unary operators `+` and `-`.

### Aggregation Operators {#aggregation-operators}

`sum`, `avg`, `min`, `max`, `count`, `count_values`, `stddev`, `stdvar`, `group`, `quantile`, `topk`, `bottomk`, `limitk` — with optional `by()` or `without()` modifiers.

### Not yet supported {#not-yet-supported}

- Range functions `stddev_over_time`, `stdvar_over_time`

## Example {#example}

```sql
SELECT * FROM prometheusQueryRange(mytable, 'rate(http_requests{job="prometheus"}[10m])[1h:10m]', now() - INTERVAL 10 MINUTES, now(), INTERVAL 1 MINUTE)
```
)DOCS_MD", .category = FunctionDocumentation::Category::TableFunction});
}

}
