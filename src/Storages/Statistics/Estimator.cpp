#include <Storages/Statistics/Estimator.h>
#include <Storages/MergeTree/RPNBuilder.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void ConditionEstimator::ColumnEstimator::merge(std::string part_name, ColumnStatisticsPtr stats)
{
    if (estimators.contains(part_name))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "part {} has been added in column {}", part_name, stats->columnName());
    estimators[part_name] = stats;
}

Float64 ConditionEstimator::ColumnEstimator::estimateLess(Float64 val, Float64 total) const
{
    if (estimators.empty())
        return default_normal_cond_factor * total;
    Float64 result = 0;
    Float64 partial_cnt = 0;
    for (const auto & [key, estimator] : estimators)
    {
        result += estimator->estimateLess(val);
        partial_cnt += estimator->count();
    }
    return result * total / partial_cnt;
}

Float64 ConditionEstimator::ColumnEstimator::estimateGreater(Float64 val, Float64 total) const
{
    return total - estimateLess(val, total);
}

Float64 ConditionEstimator::ColumnEstimator::estimateEqual(Float64 val, Float64 total) const
{
    if (estimators.empty())
    {
        if (val < - threshold || val > threshold)
            return default_normal_cond_factor * total;
        else
            return default_good_cond_factor * total;
    }
    Float64 result = 0;
    Float64 partial_cnt = 0;
    for (const auto & [key, estimator] : estimators)
    {
        result += estimator->estimateEqual(val);
        partial_cnt += estimator->count();
    }
    return result * total / partial_cnt;
}

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

Float64 ConditionEstimator::estimateRowCount(const RPNBuilderTreeNode & node) const
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
        if (dummy)
        {
            if (val < - threshold || val > threshold)
                return default_normal_cond_factor * total_count;
            else
                return default_good_cond_factor * total_count;
        }
        return estimator.estimateEqual(val, total_count);
    }
    else if (op == "less" || op == "lessThan")
    {
        if (dummy)
            return default_normal_cond_factor * total_count;
        return estimator.estimateLess(val, total_count);
    }
    else if (op == "greater" || op == "greaterThan")
    {
        if (dummy)
            return default_normal_cond_factor * total_count;
        return estimator.estimateGreater(val, total_count);
    }
    else
        return default_unknown_cond_factor * total_count;
}

void ConditionEstimator::merge(std::string part_name, UInt64 part_count, ColumnStatisticsPtr column_stat)
{
    if (!part_names.contains(part_name))
    {
        total_count += part_count;
        part_names.insert(part_name);
    }
    if (column_stat != nullptr)
        column_estimators[column_stat->columnName()].merge(part_name, column_stat);
}

}
