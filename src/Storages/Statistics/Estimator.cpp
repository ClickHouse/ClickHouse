#include <Storages/Statistics/Estimator.h>
#include <Storages/MergeTree/RPNBuilder.h>

namespace DB
{

/// second return value represents how many columns in the node.
static std::pair<std::string, Int32> tryToExtractSingleColumn(const RPNBuilderTreeNode & node)
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
    std::pair<std::string, Int32> result;
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

std::pair<std::string, Float64> ConditionEstimator::extractBinaryOp(const RPNBuilderTreeNode & node, const std::string & column_name) const
{
    if (!node.isFunction())
        return {};

    auto function_node = node.toFunctionNode();
    if (function_node.getArgumentsSize() != 2)
        return {};

    std::string function_name = function_node.getFunctionName();

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

    const auto type = output_value.getType();
    Float64 value;
    if (type == Field::Types::Int64)
        value = output_value.get<Int64>();
    else if (type == Field::Types::UInt64)
        value = output_value.get<UInt64>();
    else if (type == Field::Types::Float64)
        value = output_value.get<Float64>();
    else
        return {};
    return std::make_pair(function_name, value);
}

Float64 ConditionEstimator::estimateSelectivity(const RPNBuilderTreeNode & node) const
{
    auto result = tryToExtractSingleColumn(node);
    if (result.second != 1)
    {
        return default_unknown_cond_factor;
    }
    String col = result.first;
    auto it = column_estimators.find(col);

    /// If there the estimator of the column is not found or there are no data at all,
    /// we use dummy estimation.
    bool dummy = total_count == 0;
    ColumnEstimator estimator;
    if (it != column_estimators.end())
    {
        estimator = it->second;
    }
    else
    {
        dummy = true;
    }
    auto [op, val] = extractBinaryOp(node, col);
    if (op == "equals")
    {
        if (val < - threshold || val > threshold)
            return default_normal_cond_factor;
        else
            return default_good_cond_factor;
    }
    else if (op == "less" || op == "lessThan")
    {
        if (dummy)
            return default_normal_cond_factor;
        return estimator.estimateLess(val) / total_count;
    }
    else if (op == "greater" || op == "greaterThan")
    {
        if (dummy)
            return default_normal_cond_factor;
        return estimator.estimateGreater(val) / total_count;
    }
    else
        return default_unknown_cond_factor;
}


}
