#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionSort.h>

#include <Common/Exception.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/Prometheus/PrometheusQueryTree.h>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    /// The struct holds what distinguishes the PromQL sort functions from each other.
    struct SortFunctionInfo
    {
        /// Name of the PromQL sort function.
        std::string_view function_name;

        /// Direction of the sort.
        SortDirection direction;

        /// Whether the sort is by label.
        bool by_label;
    };

    /// Returns the properties of the PromQL sort function with the specified name, or `std::nullopt` for any other name.
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

std::optional<SortOrder> getSortOrder(const SQLQueryPiece & result)
{
    /// Only this store method holds more than one series, and only it has the column `group` that the label keys read.
    if (result.store_method != StoreMethod::VECTOR_GRID)
        return std::nullopt;

    if (result.node->node_type != PrometheusQueryTree::NodeType::Function)
        return std::nullopt;

    const auto * function_node = static_cast<const PrometheusQueryTree::Function *>(result.node);
    const auto sort_function = getSortFunctionInfo(function_node->function_name);
    if (!sort_function)
        return std::nullopt;

    ASTs keys;
    if (sort_function->by_label)
    {
        /// The first argument is the instant vector; the label names follow it. `applyFunctionSort` has checked
        /// that their nodes are string literals.
        const auto & arguments = function_node->getArguments();
        for (size_t i = 1; i < arguments.size(); ++i)
        {
            const auto & label_name = static_cast<const PrometheusQueryTree::StringLiteral *>(arguments[i])->string;

            /// No series has a label with an empty name, so it decides nothing, and `timeSeriesExtractTag` does not accept one.
            if (label_name.empty())
                continue;

            /// A series without the label is ordered as if the label were empty, as in Prometheus.
            auto label_value = makeASTFunction("ifNull",
                makeASTFunction("timeSeriesExtractTag", make_intrusive<ASTIdentifier>(ColumnNames::Group), make_intrusive<ASTLiteral>(label_name)),
                make_intrusive<ASTLiteral>(String{}));
            /// The labels are compared in natural order, as sort_by_label does in Prometheus;
            /// why the mode is 1: the default mode puts host-1 before host2, and Prometheus puts it after.
            keys.emplace_back(makeASTFunction("naturalSortKey", label_value, make_intrusive<ASTLiteral>(UInt64(1))));
        }
    }
    else
    {
        keys.emplace_back(make_intrusive<ASTIdentifier>(ColumnNames::Value));
    }

    /// Ties are ordered by tags, in the same direction, to make the result deterministic.
    keys.emplace_back(make_intrusive<ASTIdentifier>(ColumnNames::Tags));

    return SortOrder{std::move(keys), sort_function->direction};
}

}
