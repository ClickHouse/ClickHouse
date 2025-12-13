#include <TableFunctions/TableFunctionPrometheusQuery.h>

#include <Parsers/ASTFunction.h>
#include <Storages/StoragePrometheusQuery.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/getResultColumns.h>
#include <TableFunctions/TableFunctionFactory.h>


namespace DB
{

template <bool over_range>
void TableFunctionPrometheusQuery<over_range>::parseArguments(const ASTPtr & ast_function, ContextPtr context)
{
    const auto & args_func = ast_function->as<ASTFunction &>();

    if (!args_func.arguments)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Table function '{}' must have arguments.", name);

    auto & args = args_func.arguments->children;
    configuration = StoragePrometheusQuery::getConfiguration(args, context, over_range);
}


template <bool over_range>
ColumnsDescription
TableFunctionPrometheusQuery<over_range>::getActualTableStructure(ContextPtr /* context */, bool /* is_insert_query */) const
{
    return PrometheusQueryToSQL::getResultColumns(*configuration.promql_tree, configuration.evaluation_settings);
}


template <bool is_query_range>
StoragePtr TableFunctionPrometheusQuery<is_query_range>::executeImpl(
    const ASTPtr & /* ast_function */,
    ContextPtr context,
    const String & table_name,
    ColumnsDescription /* cached_columns */,
    bool is_insert_query) const
{
    auto columns = getActualTableStructure(context, is_insert_query);
    auto res = std::make_shared<StoragePrometheusQuery>(StorageID(getDatabaseName(), table_name), columns, configuration);
    res->startup();
    return res;
}


template class TableFunctionPrometheusQuery</* is_query_range = */ false>;
template class TableFunctionPrometheusQuery</* is_query_range = */ true>;

}
