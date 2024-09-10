#include <Storages/Statistics/ConditionSelectivityEstimator.h>
#include <Storages/MergeTree/RPNBuilder.h>

namespace DB
{

/// second return value represents how many columns in the node.
static std::pair<String, Int32> tryToExtractSingleColumn(const RPNBuilderTreeNode & node)
{
    if (node.isConstant())
    {
        return {};
    }

    if (!node.isFunction())
    {
        auto column_name = node.getColumnName();
        return {column_name, 1};
    }

    auto function_node = node.toFunctionNode();
    size_t arguments_size = function_node.getArgumentsSize();
    std::pair<String, Int32> result;
    for (size_t i = 0; i < arguments_size; ++i)
    {
        auto function_argument = function_node.getArgumentAt(i);
        auto subresult = tryToExtractSingleColumn(function_argument);
        if (subresult.second == 0) /// the subnode contains 0 column
            continue;
        else if (subresult.second > 1) /// the subnode contains more than 1 column
            return subresult;
        else if (result.second == 0 || result.first == subresult.first) /// subnodes contain same column.
            result = subresult;
        else
            return {"", 2};
    }
    return result;
}

static std::pair<String, Field> extractBinaryOp(const RPNBuilderTreeNode & node, const String & column_name)
{
    if (!node.isFunction())
        return {};

    auto function_node = node.toFunctionNode();
    if (function_node.getArgumentsSize() != 2)
        return {};

    String function_name = function_node.getFunctionName();

    auto lhs_argument = function_node.getArgumentAt(0);
    auto rhs_argument = function_node.getArgumentAt(1);

    auto lhs_argument_column_name = lhs_argument.getColumnName();
    auto rhs_argument_column_name = rhs_argument.getColumnName();

    bool lhs_argument_is_column = column_name == (lhs_argument_column_name);
    bool rhs_argument_is_column = column_name == (rhs_argument_column_name);

    bool lhs_argument_is_constant = lhs_argument.isConstant();
    bool rhs_argument_is_constant = rhs_argument.isConstant();

    RPNBuilderTreeNode * constant_node = nullptr;

    if (lhs_argument_is_column && rhs_argument_is_constant)
        constant_node = &rhs_argument;
    else if (lhs_argument_is_constant && rhs_argument_is_column)
        constant_node = &lhs_argument;
    else
        return {};

    Field output_value;
    DataTypePtr output_type;
    if (!constant_node->tryGetConstant(output_value, output_type))
        return {};
    return std::make_pair(function_name, output_value);
}

Float64 ConditionSelectivityEstimator::estimateRowCount(const RPNBuilderTreeNode & node) const
{
    auto result = tryToExtractSingleColumn(node);
    auto total_rows = stats.getTotalRowCount();

    if (result.second != 1)
        return default_unknown_cond_factor * total_rows;

    const String & col = result.first;
    auto [op, val] = extractBinaryOp(node, col);

    /// No statistics for column col
    if (const auto column_stat = stats.getColumnStat(col); column_stat == nullptr)
    {
        if (op == "equals")
            return default_cond_equal_factor * total_rows;
        else if (op == "less" || op == "lessOrEquals" || op == "greater" || op == "greaterOrEquals")
            return default_cond_range_factor * total_rows;
        else
            return default_unknown_cond_factor * total_rows;
    }
    else
    {
        if (op == "equals")
            return column_stat->estimateEqual(val);
        else if (op == "less" || op == "lessOrEquals")
            return column_stat->estimateLess(val);
        else if (op == "greater" || op == "greaterOrEquals")
            return column_stat->estimateGreater(val);
        else
            return default_unknown_cond_factor * total_rows;
    }

}

}
