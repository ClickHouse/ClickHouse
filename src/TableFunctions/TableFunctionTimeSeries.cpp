#include <TableFunctions/TableFunctionTimeSeries.h>

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
    return getTargetTable(context)->getInMemoryMetadataPtr()->columns;
}

template <ViewTarget::Kind target_kind>
const char * TableFunctionTimeSeriesTarget<target_kind>::getStorageTypeName() const
{
    return target_table_type_name.c_str();
}


void registerTableFunctionTimeSeries(TableFunctionFactory & factory)
{
    factory.registerFunction<TableFunctionTimeSeriesTarget<ViewTarget::Data>>(
        {.documentation = {
            .description=R"(Provides direct access to the 'data' target table for a specified TimeSeries table.)",
            .examples{{"timeSeriesData", "SELECT * from timeSeriesData('mydb', 'time_series_table');", ""}},
            .categories{"Time Series"}}
        });
    factory.registerFunction<TableFunctionTimeSeriesTarget<ViewTarget::Tags>>(
        {.documentation = {
            .description=R"(Provides direct access to the 'tags' target table for a specified TimeSeries table.)",
            .examples{{"timeSeriesTags", "SELECT * from timeSeriesTags('mydb', 'time_series_table');", ""}},
            .categories{"Time Series"}}
        });
    factory.registerFunction<TableFunctionTimeSeriesTarget<ViewTarget::Metrics>>(
        {.documentation = {
            .description=R"(Provides direct access to the 'metrics' target table for a specified TimeSeries table.)",
            .examples{{"timeSeriesMetrics", "SELECT * from timeSeriesMetrics('mydb', 'time_series_table');", ""}},
            .categories{"Time Series"}}
        });
}

}
