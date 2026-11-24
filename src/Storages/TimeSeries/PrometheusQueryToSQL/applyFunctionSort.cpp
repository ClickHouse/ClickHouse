#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionSort.h>

#include <Common/Exception.h>
#include <Parsers/Prometheus/PrometheusQueryTree.h>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    /// Checks the arguments of a PromQL sort function: `sort` and `sort_desc` take exactly one argument, an instant vector;
    /// `sort_by_label` and `sort_by_label_desc` take an instant vector and then any number of label names, as string
    /// literals.
    void checkArguments(
        const SortFunctionInfo & sort_function,
        const PrometheusQueryTree::Function * function_node,
        const std::vector<SQLQueryPiece> & arguments,
        const ConverterContext & context)
    {
        const auto & function_name = function_node->function_name;

        if (sort_function.by_label)
        {
            if (arguments.empty())
            {
                throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                                "Function '{}' expects {} or more arguments, but was called with {} arguments",
                                function_name, 1, arguments.size());
            }
        }
        else
        {
            if (arguments.size() != 1)
            {
                throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                                "Function '{}' expects 1 argument, but was called with {} arguments",
                                function_name, arguments.size());
            }
        }

        const auto & first_argument = arguments[0];

        if (first_argument.type != ResultType::INSTANT_VECTOR)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Function '{}' expects {} of type {}, but expression {} has type {}",
                            function_name, sort_function.by_label ? "the first argument" : "an argument",
                            ResultType::INSTANT_VECTOR, getPromQLText(first_argument, context), first_argument.type);
        }

        /// The label names: `getSortOrder` reads them from the string literal nodes.
        /// `sort` and `sort_desc` have no arguments after the first one.
        const auto & argument_nodes = function_node->getArguments();
        for (size_t i = 1; i < argument_nodes.size(); ++i)
        {
            if (argument_nodes[i]->node_type != PrometheusQueryTree::NodeType::StringLiteral)
            {
                throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                                "Function '{}' expects a string literal in argument #{}",
                                function_name, i + 1);
            }
        }
    }
}

bool isFunctionSort(std::string_view function_name)
{
    return getSortFunctionInfo(function_name).has_value();
}

SQLQueryPiece applyFunctionSort(
    const PrometheusQueryTree::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    const auto sort_function = getSortFunctionInfo(function_node->function_name);
    /// `applyFunction` calls this function only if `isFunctionSort` returned true.
    chassert(sort_function);

    checkArguments(*sort_function, function_node, arguments, context);

    /// A pass-through. `node` is set to this function so that `getSortOrder` can see a sort function at the root.
    SQLQueryPiece res = std::move(arguments[0]);
    res.node = function_node;
    return res;
}

std::optional<SortFunctionInfo> getSortFunctionInfo(std::string_view function_name)
{
    static constexpr SortFunctionInfo sort_functions[] = {
        {"sort", SortDirection::ASCENDING, false},
        {"sort_desc", SortDirection::DESCENDING, false},
        {"sort_by_label", SortDirection::ASCENDING, true},
        {"sort_by_label_desc", SortDirection::DESCENDING, true},
    };

    for (const auto & sort_function : sort_functions)
    {
        if (function_name == sort_function.function_name)
            return sort_function;
    }
    return std::nullopt;
}

}
