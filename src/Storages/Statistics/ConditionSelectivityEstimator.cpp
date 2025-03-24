#include <cfloat>
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


Float64 ConditionSelectivityEstimator::estimateRowCount(const RPNBuilderTreeNode & node, const std::unordered_map<std::string, ColumnWithTypeAndName> & unqualified_column_names) const
{
    auto result = tryToExtractSingleColumn(node);
    if (result.second != 1)
        return default_unknown_cond_factor * total_rows;

    String column = result.first;
    auto it_col = unqualified_column_names.find(column);

    /// We need unqualified_column_names in case of using this in join step.
    /// The problem is that for joins the column name in node would look something like `__table1.col`,
    /// and inside the statistic we have just `col`. So we need a way to convert one to another.
    ///
    /// Another thing is that we need to distinguish a situation, when we have a filter with column from
    /// another table in a join step. So that if we have a condition like `__table1.col > x`, and a column
    /// `col` in `__table2`, we wouldn't try to use statistics from another table for estimation (or dummy estimators
    /// if no such column exists in another table). In order to avoid that, we rely that if have a non-empty `unqualified_column_names`,
    /// then we should be always capable of converting a column from a node to a shorter name. Otherwise, this column is from another table and
    /// we shouldn't try to make any estimations at all.
    if (it_col != unqualified_column_names.end())
        column = it_col->second.name;
    else if (!unqualified_column_names.empty() && it_col == unqualified_column_names.end()) /// column is from another table in join step
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
    /// It means that we're unable to extract all the operators involved in the condition, so we can't estimate the number of rows.
    if (!extractOperators(node, result.first, operators))
        return default_unknown_cond_factor * total_rows;

    /// We support two modes - with statistics and dummy (when no statistics are present).
    ///
    /// The general idea behind the mode with statistics - for each condition we try to estimate the number of rows based on the statistic,
    /// taking into account known left and right bounds. After the estimation of the current condition we update the number of estimated rows
    /// and our left and/or right bounds (depending on the condition itself).
    ///
    /// If we use a dummy estimator, we assume that all the conditions are independent and use a constant factor to adjust the number of rows for
    /// each of the conditions. Depending on the operator, we have `default_cond_equal_factor` for `=`, `default_cond_range_factor` for `</<=/>/>=`,
    /// and `default_unknown_cond_factor` for all the others. So basically, if the selectivity of the i-th condition is F_i, then the total selectivity
    /// F = F_1 * F_2 * ... * F_n
    Float64 curr_rows = total_rows;
    std::optional<Float64> curr_left_bound = {};
    std::optional<Float64> curr_right_bound = {};
    for (const auto & [op, val] : operators)
    {
        std::optional<Float64> val_as_float_returned = {};
        if (op == "equals")
        {
            curr_rows = dummy ? default_cond_equal_factor * curr_rows : estimator.estimateEqual(val, curr_rows, curr_left_bound, curr_right_bound, val_as_float_returned);
            /// If we were able to convert the value to float during statistics estimation,
            /// then we can update both left and right bounds with this value for the equality operator.
            if (val_as_float_returned.has_value())
            {
                curr_left_bound = val_as_float_returned;
                curr_right_bound = val_as_float_returned;
            }
        }
        else if (op == "less" || op == "lessOrEquals")
        {
            curr_rows = dummy ? default_cond_range_factor * curr_rows : estimator.estimateLess(val, curr_rows, curr_left_bound, curr_right_bound, val_as_float_returned);
            /// If we were able to convert the value to float during statistics estimation,
            /// then we can update the right bound with this value for the `</<=` operator.
            if (val_as_float_returned.has_value())
                curr_right_bound = val_as_float_returned;
        }
        else if (op == "greater" || op == "greaterOrEquals")
        {
            curr_rows = dummy ? default_cond_range_factor * curr_rows : estimator.estimateGreater(val, curr_rows, curr_left_bound, curr_right_bound, val_as_float_returned);
            /// If we were able to convert the value to float during statistics estimation,
            /// then we can update the left bound with this value for the `>/>=` operator.
            if (val_as_float_returned.has_value())
                curr_left_bound = val_as_float_returned;
        }
        else
            curr_rows *= default_unknown_cond_factor;

        /// If we have 0 rows estimated, there's no point in further estimations.
        if (fabs(curr_rows) <= DBL_EPSILON)
            break;
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

Float64 ConditionSelectivityEstimator::ColumnSelectivityEstimator::estimateLess(const Field & val, Float64 rows, std::optional<Float64> left_bound, std::optional<Float64> right_bound, std::optional<Float64> & val_as_float_to_return) const
{
    if (part_statistics.empty())
        return default_cond_range_factor * rows;
    Float64 result = 0;
    Float64 part_rows = 0;
    for (const auto & [key, estimator] : part_statistics)
    {
        result += estimator->estimateLess(val, left_bound, right_bound, val_as_float_to_return);
        part_rows += estimator->rowCount();
    }
    return result * rows / part_rows;
}

Float64 ConditionSelectivityEstimator::ColumnSelectivityEstimator::estimateGreater(const Field & val, Float64 rows, std::optional<Float64> left_bound, std::optional<Float64> right_bound, std::optional<Float64> & val_as_float_to_return) const
{
    return rows - estimateLess(val, rows, left_bound, right_bound, val_as_float_to_return);
}

Float64 ConditionSelectivityEstimator::ColumnSelectivityEstimator::estimateEqual(const Field & val, Float64 rows, std::optional<Float64> left_bound, std::optional<Float64> right_bound, std::optional<Float64> & val_as_float_to_return) const
{
    if (part_statistics.empty())
    {
        return default_cond_equal_factor * rows;
    }
    Float64 result = 0;
    Float64 partial_cnt = 0;
    for (const auto & [key, estimator] : part_statistics)
    {
        result += estimator->estimateEqual(val, val_as_float_to_return);
        partial_cnt += estimator->rowCount();
    }

    /// If we have a value calculated, and at the same time it's out of some already known left/right bound,
    /// we know there shouldn't be any matching rows at all.
    if (val_as_float_to_return.has_value() &&
            ((left_bound.has_value() && left_bound.value() > val_as_float_to_return.value()) ||
             (right_bound.has_value() && right_bound.value() < val_as_float_to_return.value())))
    {
        return 0;
    }
    return result * rows / partial_cnt;
}
}
