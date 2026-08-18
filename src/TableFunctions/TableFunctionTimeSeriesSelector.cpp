#include <TableFunctions/TableFunctionTimeSeriesSelector.h>

#include <Access/Common/AccessFlags.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTFunction.h>
#include <Storages/StorageTimeSeries.h>
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

ColumnsDescription TableFunctionTimeSeriesSelector::getActualTableStructure(ContextPtr context, bool /* is_insert_query */) const
{
    context->checkAccess(AccessType::SELECT, config.time_series_storage_id);
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
    checkTimeSeriesTableSelectAccess(context, config.time_series_storage_id);
    auto columns = getActualTableStructure(context, is_insert_query);
    auto res = std::make_shared<StorageTimeSeriesSelector>(StorageID(getDatabaseName(), table_name), columns, config);
    res->startup();
    return res;
}

}
