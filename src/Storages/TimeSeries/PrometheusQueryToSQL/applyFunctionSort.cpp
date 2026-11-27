#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionSort.h>

#include <Parsers/Prometheus/PrometheusQueryTree.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/checkArguments.h>


namespace DB::PrometheusQueryToSQL
{

bool isFunctionSort(std::string_view function_name)
{
    return getSortFunctionInfo(function_name).has_value();
}

std::optional<SortFunctionInfo> getSortFunctionInfo(std::string_view function_name)
{
    static constexpr std::pair<std::string_view, SortFunctionInfo> sort_functions[] = {
        {"sort", {SortDirection::ASCENDING, false}},
        {"sort_desc", {SortDirection::DESCENDING, false}},
        {"sort_by_label", {SortDirection::ASCENDING, true}},
        {"sort_by_label_desc", {SortDirection::DESCENDING, true}},
    };

    for (const auto & [name, info] : sort_functions)
    {
        if (function_name == name)
            return info;
    }
    return std::nullopt;
}

SQLQueryPiece applyFunctionSort(
    const PrometheusQueryTree::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    const auto sort_function = getSortFunctionInfo(function_node->function_name);
    /// `applyFunction` calls this function only if `isFunctionSort` returned true.
    chassert(sort_function);

    /// `sort` and `sort_desc` take exactly one argument, an instant vector; `sort_by_label` and `sort_by_label_desc`
    /// take an instant vector and then any number of label names.
    if (sort_function->by_label)
        checkNumberOfArgumentsAtLeast(function_node, arguments, 1);
    else
        checkNumberOfArguments(function_node, arguments, 1);

    checkArgumentType(function_node, arguments, 0, ResultType::INSTANT_VECTOR, context);

    /// The label names. Only a string literal has this type, and `getSortOrder` reads the names from the literal nodes.
    for (size_t i = 1; i < arguments.size(); ++i)
        checkArgumentType(function_node, arguments, i, ResultType::STRING, context);

    /// A pass-through. `node` is set to this function so that `getSortOrder` can see a sort function at the root.
    SQLQueryPiece res = std::move(arguments[0]);
    res.node = function_node;
    return res;
}

}
