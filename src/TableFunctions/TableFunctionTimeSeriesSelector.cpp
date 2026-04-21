#include <TableFunctions/TableFunctionTimeSeriesSelector.h>

#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/evaluateConstantExpression.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/StorageTimeSeries.h>
#include <Storages/StorageTimeSeriesSelector.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
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


void TableFunctionTimeSeriesSelector::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto & args_func = ast_function->as<ASTFunction &>();

    if (!args_func.arguments)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table function '{}' must have arguments.", name);

    auto & args = args_func.arguments->children;

    if ((args.size() != 4) && (args.size() != 5))
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                        "Table function '{}' requires 4 or 5 arguments: {}([database, ] time_series_table, instant_selector, min_time, max_time)", name, name);

    size_t argument_index = 0;
    if (args.size() == 4)
    {
        /// timeSeriesMetrics( [my_db.]my_time_series_table )
        if (const auto * id = args[argument_index]->as<ASTIdentifier>())
        {
            if (auto table_id = id->createTable())
            {
                time_series_storage_id = table_id->getTableId();
                ++argument_index;
            }
        }
    }

    for (size_t i = argument_index; i != args.size(); ++i)
        args[i] = evaluateConstantExpressionOrIdentifierAsLiteral(args[i], context);

    if (time_series_storage_id.empty())
    {
        if (args.size() == 4)
        {
            /// timeSeriesMetrics( 'my_time_series_table' )
            time_series_storage_id.table_name = checkAndGetLiteralArgument<String>(args[argument_index++], "table_name");
        }
        else
        {
            /// timeSeriesMetrics( 'mydb', 'my_time_series_table' )
            time_series_storage_id.database_name = checkAndGetLiteralArgument<String>(args[argument_index++], "database_name");
            time_series_storage_id.table_name = checkAndGetLiteralArgument<String>(args[argument_index++], "table_name");
        }
    }

    if (time_series_storage_id.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Couldn't get a table name from the arguments of the {} table function", name);

    time_series_storage_id = context->resolveStorageID(time_series_storage_id);

    instant_selector.parse(checkAndGetLiteralArgument<String>(args[argument_index++], "instant_selector"));

    const auto * node = instant_selector.getRoot();
    if (!node || (node->node_type != PrometheusQueryTree::NodeType::InstantSelector))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Argument #{} ({}) isn't an instant selector", argument_index, instant_selector);

    min_time = args[argument_index++]->as<const ASTLiteral &>().value;
    max_time = args[argument_index++]->as<const ASTLiteral &>().value;

    chassert(argument_index == args.size());
}

ColumnsDescription TableFunctionTimeSeriesSelector::getActualTableStructure(ContextPtr context, bool /* is_insert_query */) const
{
    auto time_series_storage = storagePtrToTimeSeries(DatabaseCatalog::instance().getTable(time_series_storage_id, context));
    auto data_table = time_series_storage->getTargetTable(ViewTarget::Data, context);
    auto data_table_metadata = data_table->getInMemoryMetadataPtr();
    ColumnsDescription columns;
    columns.add(data_table_metadata->columns.get(TimeSeriesColumnNames::ID));
    columns.add(data_table_metadata->columns.get(TimeSeriesColumnNames::Timestamp));
    columns.add(data_table_metadata->columns.get(TimeSeriesColumnNames::Value));
    return columns;
}

StoragePtr TableFunctionTimeSeriesSelector::executeImpl(
        const ASTPtr & /* ast_function */,
        ContextPtr context,
        const String & table_name,
        ColumnsDescription /* cached_columns */,
        bool is_insert_query) const
{
    auto columns = getActualTableStructure(context, is_insert_query);
    auto res = std::make_shared<StorageTimeSeriesSelector>(
        StorageID(getDatabaseName(), table_name), columns, time_series_storage_id, instant_selector, min_time, max_time);
    res->startup();
    return res;
}

}
