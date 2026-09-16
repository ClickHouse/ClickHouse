#include <TableFunctions/TableFunctionTimeSeriesSelector.h>

#include <Parsers/ASTFunction.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <TableFunctions/TableFunctionFactory.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


void TableFunctionTimeSeriesSelector::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto & args_func = ast_function->as<ASTFunction &>();

    if (!args_func.arguments)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table function '{}' must have arguments.", name);

    auto & args = args_func.arguments->children;
    config = StorageTimeSeriesSelector::getConfiguration(args, context);
}

ColumnsDescription TableFunctionTimeSeriesSelector::getActualTableStructure(ContextPtr /* context */, bool /* is_insert_query */) const
{
    return ColumnsDescription({
        {TimeSeriesColumnNames::ID, config.table_id_type},
        {TimeSeriesColumnNames::Timestamp, config.table_timestamp_type},
        {TimeSeriesColumnNames::Value, config.table_value_type}
    });
}

StoragePtr TableFunctionTimeSeriesSelector::executeImpl(
        const ASTPtr & /* ast_function */,
        ContextPtr context,
        const String & table_name,
        ColumnsDescription /* cached_columns */,
        bool is_insert_query) const
{
    auto columns = getActualTableStructure(context, is_insert_query);
    auto res = std::make_shared<StorageTimeSeriesSelector>(StorageID(getDatabaseName(), table_name), columns, config);
    res->startup();
    return res;
}


void registerTableFunctionTimeSeriesSelector(TableFunctionFactory & factory);
void registerTableFunctionTimeSeriesSelector(TableFunctionFactory & factory)
{
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
- `min_time` - Start timestamp, inclusive.
- `max_time` - End timestamp, inclusive.

`min_time` and `max_time` can have fractions of a second even if the timestamps in the table are whole seconds:
the function selects the samples with `min_time <= timestamp <= max_time` exactly, without rounding the bounds to the type of the timestamps.

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
)DOCS_MD", .category = FunctionDocumentation::Category::TableFunction});
}

}
