#include <Storages/TimeSeries/PrometheusQueryToSQL/applySortFunction.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Common/Exception.h>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

bool isSortFunction(std::string_view function_name)
{
    return (function_name == "sort") || (function_name == "sort_desc");
}

SQLQueryPiece applySortFunction(
    const PQT::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    const auto & function_name = function_node->function_name;

    if (arguments.size() != 1)
    {
        throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                        "Function '{}' expects {} argument, but was called with {} arguments",
                        function_name, 1, arguments.size());
    }

    auto & argument = arguments[0];

    if (argument.type != ResultType::INSTANT_VECTOR)
    {
        throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                        "Function '{}' expects an argument of type {}, but expression {} has type {}",
                        function_name, ResultType::INSTANT_VECTOR,
                        getPromQLText(argument, context), argument.type);
    }

    argument.node = function_node;

    /// Range-query results have a fixed label order, and all other store methods contain at most one series.
    if (argument.store_method != StoreMethod::VECTOR_GRID || argument.start_time != argument.end_time)
    {
        argument.has_sort_order = false;
        return std::move(argument);
    }

    /// Capture the value at this point so later value transformations cannot change the chosen order.
    SelectQueryBuilder builder;
    builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));
    builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Values));

    ASTPtr value = makeASTFunction("arrayElement", make_intrusive<ASTIdentifier>(ColumnNames::Values), make_intrusive<ASTLiteral>(1u));
    ASTPtr normalized_value = (function_name == "sort") ? value->clone() : makeASTFunction("negate", value->clone());

    builder.select_list.push_back(makeASTFunction(
        "array",
        makeASTFunction("toFloat64", makeASTFunction("isNaN", std::move(value))),
        makeASTFunction("toFloat64", std::move(normalized_value))));
    builder.select_list.back()->setAlias(ColumnNames::SortKey);

    context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(argument.select_query), SQLSubqueryType::TABLE});
    builder.from_table = context.subqueries.back().name;

    argument.select_query = builder.getSelectQuery();
    argument.has_sort_order = true;
    return std::move(argument);
}

}
