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

    /// Returns the key that orders label values in natural order, as `sort_by_label` does in Prometheus: a value is split
    /// into pieces of digits and pieces of other characters, digit pieces are compared as numbers (`host2` before `host10`)
    /// and the other pieces as text.
    ///
    /// NOTE: The ClickHouse function `naturalSortKey` is not used because it returns one string, in which a digit piece is
    /// compared with the text next to it: it puts `host-1` before `host2` (`-` is before the digits), and Prometheus puts
    /// it after `host10`.
    ///
    /// The key is an array with one tuple (category, number length, text) per piece. A piece has either only digits or
    /// no digits, so comparing it as a string with `0` and with `:` (the character after `9`) tells which of the three
    /// tuples it gets:
    ///
    /// arrayMap(piece -> multiIf(
    ///         piece < '0', (0, 0, piece),                                            -- text before the digits
    ///         piece < ':', (1, length(trimLeft(piece, '0')), trimLeft(piece, '0')),  -- digits
    ///         (2, 0, piece)),                                                        -- text after the digits
    ///     extractAll(<label_value>, '[0-9]+|[^0-9]+'))                               -- pieces
    ///
    /// - Category: Prometheus compares a digit piece with a text piece as text, so text that starts before `0`
    ///   (such as `-` or `.`) goes before every number, and other text after.
    /// - Number length: without leading zeros, the longer number is the greater one, and numbers of the same length
    ///   compare as text. So a number of any length can be compared.
    ///
    /// Examples, in natural order:
    ///   `-1`      [(0, 0, '-'), (1, 1, '1')]
    ///   `4m5`     [(1, 1, '4'), (2, 0, 'm'), (1, 1, '5')]
    ///   `4m600`   [(1, 1, '4'), (2, 0, 'm'), (1, 3, '600')]
    ///   `007`     [(1, 1, '7')]
    ///   `host2`   [(2, 0, 'host'), (1, 1, '2')]
    ///   `host10`  [(2, 0, 'host'), (1, 2, '10')]
    ///   `host-1`  [(2, 0, 'host-'), (1, 1, '1')]
    ///
    /// Arrays are compared piece by piece, and tuples field by field: `4m5` is before `4m600` by the number length
    /// of the third piece (1 < 3), and `host10` is before `host-1` by the text of the first piece (`host` < `host-`).
    ///
    /// The key is evaluated once for each series of the result, not for each stored sample or each comparison.
    ASTPtr makeNaturalSortKey(ASTPtr label_value)
    {
        /// Every use gets its own AST node: a node must not be in two places of a tree.
        /// `piece` is the parameter of the SQL lambda below, so the identifiers must have the name given to `makeASTLambda`.
        auto piece = [] { return make_intrusive<ASTIdentifier>("piece"); };
        auto number_literal = [](UInt64 value) { return make_intrusive<ASTLiteral>(value); };
        auto string_literal = [](const char * text)
        {
            return make_intrusive<ASTLiteral>(String{text});
        };
        auto without_leading_zeros = [&]
        {
            return makeASTFunction("trimLeft", piece(), string_literal("0"));
        };

        auto text_before_digits = makeASTFunction("tuple", number_literal(0), number_literal(0), piece());
        auto digits = makeASTFunction("tuple", number_literal(1), makeASTFunction("length", without_leading_zeros()), without_leading_zeros());
        auto text_after_digits = makeASTFunction("tuple", number_literal(2), number_literal(0), piece());

        auto piece_key = makeASTFunction("multiIf",
            makeASTFunction("less", piece(), string_literal("0")), std::move(text_before_digits),
            makeASTFunction("less", piece(), string_literal(":")), std::move(digits),
            std::move(text_after_digits));

        return makeASTFunction("arrayMap",
            makeASTLambda({"piece"}, std::move(piece_key)),
            makeASTFunction("extractAll", std::move(label_value), string_literal("[0-9]+|[^0-9]+")));
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

            keys.emplace_back(makeNaturalSortKey(std::move(label_value)));
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
