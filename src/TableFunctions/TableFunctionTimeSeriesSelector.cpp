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

namespace
{

ColumnsDescription getColumnsDescription(const StorageTimeSeriesSelector::Configuration & config)
{
    return ColumnsDescription({
        {TimeSeriesColumnNames::ID, config.id_data_type},
        {TimeSeriesColumnNames::Timestamp, config.timestamp_data_type},
        {TimeSeriesColumnNames::Value, config.scalar_data_type}
    });
}

}

void TableFunctionTimeSeriesSelector::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto & args_func = ast_function->as<ASTFunction &>();

    if (!args_func.arguments)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table function '{}' must have arguments.", name);

    auto & args = args_func.arguments->children;
    parsed_args = StorageTimeSeriesSelector::parseArgumentsOnly(args, context);
}

ColumnsDescription TableFunctionTimeSeriesSelector::getActualTableStructure(ContextPtr context, bool /* is_insert_query */) const
{
    return getColumnsDescription(StorageTimeSeriesSelector::resolveConfiguration(parsed_args, context));
}

StoragePtr TableFunctionTimeSeriesSelector::executeImpl(
        const ASTPtr & /* ast_function */,
        ContextPtr context,
        const String & table_name,
        ColumnsDescription /* cached_columns */,
        bool /* is_insert_query */) const
{
    auto config = StorageTimeSeriesSelector::resolveConfiguration(parsed_args, context);
    auto res = std::make_shared<StorageTimeSeriesSelector>(
        StorageID(getDatabaseName(), table_name), getColumnsDescription(config), config);
    res->startup();
    return res;
}

}
