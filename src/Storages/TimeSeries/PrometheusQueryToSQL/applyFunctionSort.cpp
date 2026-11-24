#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionSort.h>

#include <Parsers/Prometheus/PrometheusQueryTree.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/checkArguments.h>


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
        if (sort_function.by_label)
            checkNumberOfArgumentsAtLeast(function_node, arguments, 1);
        else
            checkNumberOfArguments(function_node, arguments, 1);

        checkArgumentType(function_node, arguments, 0, ResultType::INSTANT_VECTOR, context);

        /// The label names. Only a string literal has this type, and `getSortOrder` reads the names from the literal nodes.
        /// `sort` and `sort_desc` have no arguments after the first one.
        for (size_t i = 1; i < arguments.size(); ++i)
            checkArgumentType(function_node, arguments, i, ResultType::STRING, context);
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
