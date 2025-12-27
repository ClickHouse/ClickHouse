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


template <bool to_grid>
void TableFunctionTimeSeriesSelector<to_grid>::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto & args_func = ast_function->as<ASTFunction &>();
    if (!args_func.arguments)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table function '{}' must have arguments.", name);

    auto & args = args_func.arguments->children;
    config = StorageTimeSeriesSelector::getConfiguration(args, context, to_grid);
}


template <bool to_grid>
ColumnsDescription TableFunctionTimeSeriesSelector<to_grid>::getActualTableStructure(ContextPtr /* context */, bool /* is_insert_query */) const
{
    return ColumnsDescription({
        {TimeSeriesColumnNames::ID, config.id_type},
        {TimeSeriesColumnNames::Timestamp, config.timestamp_type},
        {TimeSeriesColumnNames::Value, config.scalar_type}
    });
}


template <bool to_grid>
StoragePtr TableFunctionTimeSeriesSelector<to_grid>::executeImpl(
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


template class TableFunctionTimeSeriesSelector</* to_grid = */ false>;
template class TableFunctionTimeSeriesSelector</* to_grid = */ true>;

}
