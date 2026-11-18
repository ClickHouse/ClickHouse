#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionSort.h>

#include <Common/Exception.h>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    /// Checks that the function is called with exactly one argument, an instant vector.
    void checkArguments(
        const PrometheusQueryTree::Function * function_node, const std::vector<SQLQueryPiece> & arguments, const ConverterContext & context)
    {
        const auto & function_name = function_node->function_name;

        if (arguments.size() != 1)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Function '{}' expects {} arguments, but was called with {} arguments",
                            function_name, 1, arguments.size());
        }

        const auto & argument = arguments[0];

        if (argument.type != ResultType::INSTANT_VECTOR)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Function '{}' expects an argument of type {}, but expression {} has type {}",
                            function_name, ResultType::INSTANT_VECTOR,
                            getPromQLText(argument, context), argument.type);
        }
    }
}

std::optional<SortDirection> getSortDirection(std::string_view function_name)
{
    if (function_name == "sort")
        return SortDirection::ASCENDING;
    if (function_name == "sort_desc")
        return SortDirection::DESCENDING;
    return std::nullopt;
}

SQLQueryPiece applyFunctionSort(
    const PrometheusQueryTree::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    checkArguments(function_node, arguments, context);

    /// A pass-through: the data stays as it is. SQL keeps the order of rows only from the ORDER BY of the outermost
    /// SELECT, and later steps (GROUP BY, joins) reorder rows anyway, so an ORDER BY here would be lost or removed
    /// as redundant. `finalizeSQL` applies the order instead when it sees `sort` or `sort_desc` as the root node, which is why
    /// `node` is set to this function.
    SQLQueryPiece res = std::move(arguments[0]);
    res.node = function_node;
    return res;
}

}
