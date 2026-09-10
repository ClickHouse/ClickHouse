#include <TableFunctions/TableFunctionTimeSeriesSelector.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
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

void TableFunctionTimeSeriesSelector::qualifyArgumentsWithDatabase(ASTs & arguments) const
{
    /// timeSeriesSelector( 'mydb', 'my_time_series_table', selector, min_time, max_time )
    size_t num_other_arguments = 3;
    if (arguments.size() < num_other_arguments + 1)
        return;

    const auto & time_series_storage_id = config.time_series_storage_id;
    arguments.erase(arguments.begin(), arguments.end() - num_other_arguments);
    arguments.insert(arguments.begin(), make_intrusive<ASTLiteral>(time_series_storage_id.table_name));
    arguments.insert(arguments.begin(), make_intrusive<ASTLiteral>(time_series_storage_id.database_name));
}

ColumnsDescription TableFunctionTimeSeriesSelector::getActualTableStructure(ContextPtr /* context */, bool /* is_insert_query */) const
{
    return ColumnsDescription({
        {TimeSeriesColumnNames::ID, config.id_data_type},
        {TimeSeriesColumnNames::Timestamp, config.timestamp_data_type},
        {TimeSeriesColumnNames::Value, config.scalar_data_type}
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

}
