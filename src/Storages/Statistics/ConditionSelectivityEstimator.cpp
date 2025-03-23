#include <optional>
#include <Storages/Statistics/ConditionSelectivityEstimator.h>
#include <Storages/MergeTree/RPNBuilder.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// This function tries to extract operators from the expression.
/// It supports a conjunction of simple comparison conditions (e.g. (col < val1) and (col > val2)).
/// The result is a vector of operators, each in a form of operator name and constant value. For the condition above it would be [{"less",val1},{"greater"},val2].
bool ConditionSelectivityEstimator::extractOperators(const RPNBuilderTreeNode & node, const String & qualified_column_name, std::vector<std::pair<String, Field>> & result) const
{
    if (!node.isFunction())
        return false;

    auto function_node = node.toFunctionNode();
    String function_name = function_node.getFunctionName();

    if (function_name == "and")
    {
        for (size_t i = 0; i < function_node.getArgumentsSize(); ++i)
            if (!extractOperators(function_node.getArgumentAt(i), qualified_column_name, result))
                return false;

        return true;
    }

    if (function_node.getArgumentsSize() != 2)
        return false;

    auto lhs_argument = function_node.getArgumentAt(0);
    auto rhs_argument = function_node.getArgumentAt(1);

    auto lhs_argument_column_name = lhs_argument.getColumnName();
    auto rhs_argument_column_name = rhs_argument.getColumnName();

    bool lhs_argument_is_column = qualified_column_name == (lhs_argument_column_name);
    bool rhs_argument_is_column = qualified_column_name == (rhs_argument_column_name);

    RPNBuilderTreeNode * maybe_constant_node = nullptr;
    bool non_column_argument_is_constant = lhs_argument_is_column ? rhs_argument.isConstant() : lhs_argument.isConstant();

    Field output_value;
    DataTypePtr output_type;

    auto try_get_constant_from_arg = [&](RPNBuilderTreeNode * arg, bool is_arg_constant) -> bool
    {
        if (is_arg_constant)
            return arg->tryGetConstant(output_value, output_type);

        if (arg->isFunction())
        {
            auto arg_function = arg->toFunctionNode();

            if (arg_function.getFunctionName() == "_CAST" && arg_function.getArgumentAt(0).tryGetConstant(output_value, output_type))
                return true;
        }

        return false;
    };

    if (lhs_argument_is_column)
        maybe_constant_node = &rhs_argument;
    else if (rhs_argument_is_column)
        maybe_constant_node = &lhs_argument;
    else
        return false;

    if (!try_get_constant_from_arg(maybe_constant_node, non_column_argument_is_constant))
        return false;

    result.emplace_back(std::make_pair(function_name, output_value));
    return true;
}

/// second return value represents how many columns in the node.
static std::pair<String, Int32> tryToExtractSingleColumn(const RPNBuilderTreeNode & node)
{
    if (node.isConstant())
        return {};

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
        if (subresult.second > 1) /// the subnode contains more than 1 column
            return subresult;
        if (result.second == 0 || result.first == subresult.first) /// subnodes contain same column.
            result = subresult;
        else
            return {"", 2};
    }
    return result;
}


Float64 ConditionSelectivityEstimator::estimateRowCount(const RPNBuilderTreeNode & node, const std::unordered_map<std::string, ColumnWithTypeAndName> & unqualifiedColumnsNames) const
{
    auto result = tryToExtractSingleColumn(node);
    if (result.second != 1)
        return default_unknown_cond_factor * total_rows;

    String column = result.first;
    auto it_col = unqualifiedColumnsNames.find(column);

    if (it_col != unqualifiedColumnsNames.end())
        column = it_col->second.name;
    else if (!unqualifiedColumnsNames.empty() && it_col == unqualifiedColumnsNames.end()) /// column is from another table in join step
        return total_rows;

    auto it = column_estimators.find(column);

    /// If there the estimator of the column is not found or there are no data at all,
    /// we use dummy estimation.
    bool dummy = false;
    ColumnSelectivityEstimator estimator;
    if (it != column_estimators.end())
        estimator = it->second;
    else
        dummy = true;

    std::vector<std::pair<String, Field>> operators;
    /// It means that we weren't able to extract all the operators involved in the condition, so we can't estimate the number of rows.
    if (!extractOperators(node, result.first, operators))
        return default_unknown_cond_factor * total_rows;

    Float64 curr_rows = total_rows;
    std::optional<Float64> curr_min = {};
    std::optional<Float64> curr_max = {};
    for (const auto & [op, val] : operators)
    {
        std::optional<Float64> calculated_val = {};
        if (op == "equals")
        {
            curr_rows = dummy ? default_cond_equal_factor * curr_rows : estimator.estimateEqual(val, curr_rows, calculated_val, curr_min, curr_max);
            if (calculated_val.has_value())
            {
                curr_min = calculated_val;
                curr_max = calculated_val;
            }
        }
        else if (op == "less" || op == "lessOrEquals")
        {
            curr_rows = dummy ? default_cond_range_factor * curr_rows : estimator.estimateLess(val, curr_rows, calculated_val, curr_min, curr_max);
            if (calculated_val.has_value())
                curr_max = calculated_val;
        }
        else if (op == "greater" || op == "greaterOrEquals")
        {
            curr_rows = dummy ? default_cond_range_factor * curr_rows : estimator.estimateGreater(val, curr_rows, calculated_val, curr_min, curr_max);
            if (calculated_val.has_value())
                curr_min = calculated_val;
        }
        else
            curr_rows *= default_unknown_cond_factor;
    }

    return curr_rows;
}

void ConditionSelectivityEstimator::incrementRowCount(UInt64 rows)
{
    total_rows += rows;
}

void ConditionSelectivityEstimator::addStatistics(String part_name, ColumnStatisticsPartPtr column_stat)
{
    if (column_stat != nullptr)
        column_estimators[column_stat->columnName()].addStatistics(part_name, column_stat);
}

void ConditionSelectivityEstimator::ColumnSelectivityEstimator::addStatistics(String part_name, ColumnStatisticsPartPtr stats)
{
    if (part_statistics.contains(part_name))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "part {} has been added in column {}", part_name, stats->columnName());
    part_statistics[part_name] = stats;
}

Float64 ConditionSelectivityEstimator::ColumnSelectivityEstimator::estimateLess(const Field & val, Float64 rows, std::optional<Float64> & calculated_val, std::optional<Float64> custom_min, std::optional<Float64> custom_max) const
{
    if (part_statistics.empty())
        return default_cond_range_factor * rows;
    Float64 result = 0;
    Float64 part_rows = 0;
    for (const auto & [key, estimator] : part_statistics)
    {
        result += estimator->estimateLess(val, calculated_val, custom_min, custom_max);
        part_rows += estimator->rowCount();
    }
    return result * rows / part_rows;
}

Float64 ConditionSelectivityEstimator::ColumnSelectivityEstimator::estimateGreater(const Field & val, Float64 rows, std::optional<Float64> & calculated_val, std::optional<Float64> custom_min, std::optional<Float64> custom_max) const
{
    return rows - estimateLess(val, rows, calculated_val, custom_min, custom_max);
}

Float64 ConditionSelectivityEstimator::ColumnSelectivityEstimator::estimateEqual(const Field & val, Float64 rows, std::optional<Float64> & calculated_val, std::optional<Float64> custom_min, std::optional<Float64> custom_max) const
{
    if (part_statistics.empty())
    {
        return default_cond_equal_factor * rows;
    }
    Float64 result = 0;
    Float64 partial_cnt = 0;
    for (const auto & [key, estimator] : part_statistics)
    {
        result += estimator->estimateEqual(val, calculated_val);
        partial_cnt += estimator->rowCount();
    }

    if (calculated_val.has_value() &&
            ((custom_min.has_value() && custom_min.value() > calculated_val.value()) ||
             (custom_max.has_value() && custom_max.value() < calculated_val.value())))
    {
        return 0;
    }
    return result * rows / partial_cnt;
}
}
