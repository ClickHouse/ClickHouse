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

}
