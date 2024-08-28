#include <Storages/Statistics/ConditionSelectivityEstimator.h>
#include <Storages/MergeTree/RPNBuilder.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void ConditionSelectivityEstimator::ColumnSelectivityEstimator::merge(String part_name, ColumnStatisticsPtr stats)
{
    if (part_statistics.contains(part_name))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "part {} has been added in column {}", part_name, stats->columnName());
    part_statistics[part_name] = stats;
}

Float64 ConditionSelectivityEstimator::ColumnSelectivityEstimator::estimateLess(Float64 val, Float64 rows) const
{
    if (part_statistics.empty())
        return default_normal_cond_factor * rows;
    Float64 result = 0;
    Float64 part_rows = 0;
    for (const auto & [key, estimator] : part_statistics)
    {
        result += estimator->estimateLess(val);
        part_rows += estimator->rowCount();
    }
    return result * rows / part_rows;
}

Float64 ConditionSelectivityEstimator::ColumnSelectivityEstimator::estimateGreater(Float64 val, Float64 rows) const
{
    return rows - estimateLess(val, rows);
}

Float64 ConditionSelectivityEstimator::ColumnSelectivityEstimator::estimateEqual(Float64 val, Float64 rows) const
{
    if (part_statistics.empty())
    {
        if (val < - threshold || val > threshold)
            return default_normal_cond_factor * rows;
        else
            return default_good_cond_factor * rows;
    }
    Float64 result = 0;
    Float64 partial_cnt = 0;
    for (const auto & [key, estimator] : part_statistics)
    {
        result += estimator->estimateEqual(val);
        partial_cnt += estimator->rowCount();
    }
    return result * rows / partial_cnt;
}

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

std::pair<String, Float64> ConditionSelectivityEstimator::extractBinaryOp(const RPNBuilderTreeNode & node, const String & column_name) const
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

Float64 ConditionSelectivityEstimator::estimateRowCount(const RPNBuilderTreeNode & node) const
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
    bool dummy = total_rows == 0;
    ColumnSelectivityEstimator estimator;
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
                return default_normal_cond_factor * total_rows;
            else
                return default_good_cond_factor * total_rows;
        }
        return estimator.estimateEqual(val, total_rows);
    }
    else if (op == "less" || op == "lessOrEquals")
    {
        if (dummy)
            return default_normal_cond_factor * total_rows;
        return estimator.estimateLess(val, total_rows);
    }
    else if (op == "greater" || op == "greaterOrEquals")
    {
        if (dummy)
            return default_normal_cond_factor * total_rows;
        return estimator.estimateGreater(val, total_rows);
    }
    else
        return default_unknown_cond_factor * total_rows;
}

void ConditionSelectivityEstimator::merge(String part_name, UInt64 part_rows, ColumnStatisticsPtr column_stat)
{
    if (!part_names.contains(part_name))
    {
        total_rows += part_rows;
        part_names.insert(part_name);
    }
    if (column_stat != nullptr)
        column_estimators[column_stat->columnName()].merge(part_name, column_stat);
}

}
