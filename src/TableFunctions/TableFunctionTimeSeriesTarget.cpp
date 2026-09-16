#include <TableFunctions/TableFunctionTimeSeriesTarget.h>

#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Storages/StorageTimeSeries.h>
#include <Storages/checkAndGetLiteralArgument.h>
#include <TableFunctions/TableFunctionFactory.h>


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
        /// timeSeriesMetricFamilies( [my_db.]my_time_series_table )
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
            /// timeSeriesMetricFamilies( 'my_time_series_table' )
            time_series_storage_id.table_name = checkAndGetLiteralArgument<String>(args[0], "table_name");
        }
        else
        {
            /// timeSeriesMetricFamilies( 'mydb', 'my_time_series_table' )
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


void registerTableFunctionTimeSeriesTargets(TableFunctionFactory & factory);
void registerTableFunctionTimeSeriesTargets(TableFunctionFactory & factory)
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

    factory.registerFunction<TableFunctionTimeSeriesTarget<ViewTarget::RecentSamples>>(
        {.description = R"DOCS_MD(
`timeSeriesRecentSamples(db_name.time_series_table)` - Returns the [recent samples](/reference/engines/table-engines/integrations/time-series#recent-samples-table) table
used by table `db_name.time_series_table` whose table engine is the [TimeSeries](/reference/engines/table-engines/integrations/time-series) engine:

```sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries RECENT SAMPLES recent_samples_table
```

The function also works if the _recent samples_ table is inner:

```sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries RECENT SAMPLES INNER UUID '01234567-89ab-cdef-0123-456789abcdef'
```

The following queries are equivalent:

```sql
SELECT * FROM timeSeriesRecentSamples(db_name.time_series_table);
SELECT * FROM timeSeriesRecentSamples('db_name.time_series_table');
SELECT * FROM timeSeriesRecentSamples('db_name', 'time_series_table');
```

<Note>
The _recent samples_ table is optional: it exists if the `recent_samples_ttl_seconds` setting is not zero.
The function throws an exception if the table has no _recent samples_ table.
</Note>
)DOCS_MD", .category = FunctionDocumentation::Category::TableFunction});

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

    factory.registerFunction<TableFunctionTimeSeriesTarget<ViewTarget::TimeRanges>>(
        {.description = R"DOCS_MD(
`timeSeriesTimeRanges(db_name.time_series_table)` - Returns the [time ranges](/reference/engines/table-engines/integrations/time-series#time-ranges-table) table
used by table `db_name.time_series_table` whose table engine is the [TimeSeries](/reference/engines/table-engines/integrations/time-series) engine:

```sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries TIME RANGES time_ranges_table
```

The function also works if the _time ranges_ table is inner:

```sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries TIME RANGES INNER UUID '01234567-89ab-cdef-0123-456789abcdef'
```

The following queries are equivalent:

```sql
SELECT * FROM timeSeriesTimeRanges(db_name.time_series_table);
SELECT * FROM timeSeriesTimeRanges('db_name.time_series_table');
SELECT * FROM timeSeriesTimeRanges('db_name', 'time_series_table');
```

<Note>
The _time ranges_ table is optional: it exists in tables of [version](/reference/engines/table-engines/integrations/time-series#schema-versioning) 5 and later
if the `store_time_ranges` setting is enabled. Tables of the earlier versions keep the columns `min_time` and `max_time` in the
[tags](/reference/engines/table-engines/integrations/time-series#tags-table) table.
The function throws an exception if the table has no _time ranges_ table.
</Note>
)DOCS_MD", .category = FunctionDocumentation::Category::TableFunction});

    factory.registerFunction<TableFunctionTimeSeriesTarget<ViewTarget::MetricFamilies>>(
        {.description = R"DOCS_MD(
`timeSeriesMetricFamilies(db_name.time_series_table)` - Returns the [metric families](/reference/engines/table-engines/integrations/time-series#metric-families-table) table
used by table `db_name.time_series_table` whose table engine is the [TimeSeries](/reference/engines/table-engines/integrations/time-series) engine:

```sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries METRIC FAMILIES metric_families_table
```

The function also works if the _metric families_ table is inner:

```sql
CREATE TABLE db_name.time_series_table ENGINE=TimeSeries METRIC FAMILIES INNER UUID '01234567-89ab-cdef-0123-456789abcdef'
```

The following queries are equivalent:

```sql
SELECT * FROM timeSeriesMetricFamilies(db_name.time_series_table);
SELECT * FROM timeSeriesMetricFamilies('db_name.time_series_table');
SELECT * FROM timeSeriesMetricFamilies('db_name', 'time_series_table');
```

<Note>
The function `timeSeriesMetricFamilies` has an alias `timeSeriesMetrics` which is kept for backwards compatibility.
</Note>
)DOCS_MD", .category = FunctionDocumentation::Category::TableFunction});

    factory.registerAlias("timeSeriesMetrics", "timeSeriesMetricFamilies");
}

}
