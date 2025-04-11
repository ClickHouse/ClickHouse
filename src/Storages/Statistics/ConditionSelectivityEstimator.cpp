#include <cfloat>
#include <optional>
#include <unordered_map>
#include <Storages/Statistics/ConditionSelectivityEstimator.h>
#include <Storages/MergeTree/RPNBuilder.h>
#include <Common/logger_useful.h>
#include "Core/ColumnWithTypeAndName.h"

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// This function tries to extract operators from the expression.
/// It supports a conjunction of simple comparison conditions (e.g. (col < val1) and (col > val2)).
/// The result is a vector of operators, each in a form of operator name and constant value. For the condition above it would be [{"less",val1},{"greater"},val2].
bool ConditionSelectivityEstimator::extractOperators(const RPNBuilderTreeNode & node, const String & qualified_column_name, std::vector<std::pair<String, Field>> & result, bool try_as_or) const
{
#define MY_LOG(...) LOG_DEBUG(getLogger("Alex Estimator: extractOperators"), __VA_ARGS__)
    if (!node.isFunction())
        return false;

    auto function_node = node.toFunctionNode();
    String function_name = function_node.getFunctionName();

    if (function_name == (try_as_or ? "or" : "and"))
    {
        for (size_t i = 0; i < function_node.getArgumentsSize(); ++i)
            if (!extractOperators(function_node.getArgumentAt(i), qualified_column_name, result, try_as_or))
            {
                MY_LOG("Ignore problematic operator(s)");
                return false;
            }

        return true;
    }

    if (function_node.getArgumentsSize() != 2)
        return false;

    auto lhs_argument = function_node.getArgumentAt(0);
    auto rhs_argument = function_node.getArgumentAt(1);

    auto lhs_argument_column_name = lhs_argument.getColumnName();
    auto rhs_argument_column_name = rhs_argument.getColumnName();

    MY_LOG("left column - {}, right column - {}, expected column - {}", lhs_argument_column_name, rhs_argument_column_name, qualified_column_name);

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
        return lhs_argument.isConstant() || rhs_argument.isConstant(); // Should be one constant, another one should be a column from another join table

    if (!try_get_constant_from_arg(maybe_constant_node, non_column_argument_is_constant))
        return false;

    result.emplace_back(std::make_pair(function_name, output_value));
    return true;
#undef MY_LOG
}

/// second return value represents how many columns in the node.
static std::pair<String, Int32> tryToExtractSingleColumn(const RPNBuilderTreeNode & node, const std::unordered_map<std::string, ColumnWithTypeAndName> & unqualified_column_names)
{
#define MY_LOG(...) LOG_DEBUG(getLogger("Alex Estimator: tryToExtractSingleColumn"), __VA_ARGS__)
    bool should_check_columns_map = !unqualified_column_names.empty();
    MY_LOG("Should check columns map - {}", should_check_columns_map);

    if (node.isConstant())
    {
        MY_LOG("Some node is constant");
        return {};
    }

    if (!node.isFunction())
    {
        auto column_name = node.getColumnName();
        MY_LOG("Node is column with name {}", column_name);
        if (!should_check_columns_map || unqualified_column_names.find(column_name) != unqualified_column_names.end())
        {
            MY_LOG("Using column {}", column_name);
            return {column_name, 1};
        }
        else
        {
            MY_LOG("Ignoring column {}", column_name);
            return {};
        }
    }

    auto function_node = node.toFunctionNode();
    size_t arguments_size = function_node.getArgumentsSize();
    std::pair<String, Int32> result;
    for (size_t i = 0; i < arguments_size; ++i)
    {
        auto function_argument = function_node.getArgumentAt(i);
        auto subresult = tryToExtractSingleColumn(function_argument, unqualified_column_names);
        MY_LOG("Current result - {} and {}", result.first, result.second);
        MY_LOG("Subresult - {} and {}", subresult.first, subresult.second);
        if (subresult.second == 0) /// the subnode contains 0 column
            continue;
        if (subresult.second > 1) /// the subnode contains more than 1 column
            return subresult;
        if (result.second == 0 || result.first == subresult.first) /// subnodes contain same column.
        {
            MY_LOG("Updating result with subresult");
            result = subresult;
        }
        else
        {
            MY_LOG("Other for {} and {}, returning 2", subresult.first, subresult.second);
            return {"", 2};
        }
    }
    MY_LOG("Final result - {} and {}", result.first, result.second);
    return result;
#undef MY_LOG
}


static void mergeAndSortRanges(std::vector<std::pair<Float64, Float64>> & ranges)
{
    std::vector<std::pair<Float64, Float64>> result;
    std::sort(ranges.begin(), ranges.end());
    Float64 left = -DBL_MAX;
    Float64 right = -DBL_MAX;

    for (const auto & range : ranges)
    {
        if (left == -DBL_MAX)
        {
            left = range.first;
            right = range.second;
        }
        else if (right < range.first)
        {
            result.emplace_back(std::make_pair(left, right));
            left = range.first;
            right = range.second;
        }
        else
            right = std::max(right, range.second);
    }

    if (!ranges.empty())
        result.emplace_back(std::make_pair(left, right));

    ranges = result;
}


Float64 ConditionSelectivityEstimator::estimateRowCount(const RPNBuilderTreeNode & node, const std::unordered_map<std::string, ColumnWithTypeAndName> & unqualified_column_names) const
{
#define MY_LOG(...) LOG_DEBUG(getLogger("Alex Estimator: estimateRowCount"), __VA_ARGS__)
    auto result = tryToExtractSingleColumn(node, unqualified_column_names);
    MY_LOG("Got result - {} and {}", result.first, result.second);
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

    MY_LOG("Proceeding with result");
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
    bool is_or = false;
    /// It means that we're unable to extract all the operators involved in the condition, so we can't estimate the number of rows.
    if (!extractOperators(node, result.first, operators, false))
    {
        MY_LOG("Unable to extract and operators, size - {}", operators.size());
        operators.clear();

        if (!extractOperators(node, result.first, operators, true))
        {
            MY_LOG("Unable to extract or operators, size - {}", operators.size());
            return default_unknown_cond_factor * total_rows;
        }

        is_or = true;
        // return default_unknown_cond_factor * total_rows;
    }

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
    Float64 curr_rows = is_or ? 0 : total_rows;
    std::optional<Float64> curr_left_bound = {};
    std::optional<Float64> curr_right_bound = {};
    MY_LOG("Found {} operators", operators.size());
    if (!is_or || dummy)
        {
        for (const auto & [op, val] : operators)
        {
            MY_LOG("Inspecting operator {}", op);
            std::optional<Float64> val_as_float_returned = {};
            if (op == "equals")
            {
                if (dummy && is_or)
                    curr_rows += default_cond_equal_factor * total_rows;
                else
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
            }
            else if (op == "less" || op == "lessOrEquals")
            {
                if (dummy && is_or)
                    curr_rows += default_cond_range_factor * total_rows;
                else
                {
                    curr_rows = dummy ? default_cond_range_factor * curr_rows : estimator.estimateLess(val, curr_rows, curr_left_bound, curr_right_bound, val_as_float_returned);
                    /// If we were able to convert the value to float during statistics estimation,
                    /// then we can update the right bound with this value for the `</<=` operator.
                    if (val_as_float_returned.has_value())
                        curr_right_bound = val_as_float_returned;
                }
            }
            else if (op == "greater" || op == "greaterOrEquals")
            {
                if (dummy && is_or)
                    curr_rows += default_cond_range_factor * total_rows;
                else
                {
                    curr_rows = dummy ? default_cond_range_factor * curr_rows : estimator.estimateGreater(val, curr_rows, curr_left_bound, curr_right_bound, val_as_float_returned);
                    /// If we were able to convert the value to float during statistics estimation,
                    /// then we can update the left bound with this value for the `>/>=` operator.
                    if (val_as_float_returned.has_value())
                        curr_left_bound = val_as_float_returned;
                }
            }
            else
            {
                if (is_or)
                    curr_rows += default_unknown_cond_factor * total_rows;
                else
                    curr_rows *= default_unknown_cond_factor;
            }

            /// If we have 0 rows estimated, there's no point in further estimations.
            if (fabs(curr_rows) <= DBL_EPSILON)
                break;
        }

        return curr_rows;
    }

    std::vector<std::pair<Float64, Float64>> ranges;

    for (const auto & [op, val] : operators)
    {
        MY_LOG("Inspecting operator {}", op);
        std::optional<Float64> val_as_float_returned = {};
        /// Just to get val
        estimator.estimateLess(val, curr_rows, curr_left_bound, curr_right_bound, val_as_float_returned);

        if (!val_as_float_returned.has_value())
        {
            /// No value, can't estimate with statistics anyway (not exactly true, only for numeric values)
            double factor = static_cast<double>(default_unknown_cond_factor);
            if (op == "equals")
                factor = default_cond_equal_factor;
            else if (op == "less" || op == "lessOrEquals" || op == "greater" || op == "greaterOrEquals")
                factor = default_cond_range_factor;

            curr_rows += factor * total_rows;
            continue;
        }

        if (op == "equals")
        {
            bool can_skip = false;
            for (const auto & range : ranges)
            {
                if (range.second < val_as_float_returned.value())
                    break;

                if (range.first <= val_as_float_returned.value() && range.second >= val_as_float_returned.value())
                {
                    can_skip = true;
                    break;
                }
            }

            if (!can_skip)
            {
                curr_rows += estimator.estimateEqual(val, total_rows, {}, {}, val_as_float_returned);
                ranges.emplace_back(std::make_pair(val_as_float_returned.value(), val_as_float_returned.value()));
                mergeAndSortRanges(ranges);
            }
        }
        else if (op == "less" || op == "lessOrEquals")
        {
            std::vector<std::pair<Float64, Float64>> ranges_to_estimate;
            for (const auto & range : ranges)
            {
                if (range.first > val_as_float_returned.value())
                    break;

                ranges_to_estimate.emplace_back(std::make_pair(range.first, std::min(range.second, val_as_float_returned.value())));
            }

            Float64 total_estimation = estimator.estimateLess(val, total_rows, {}, val_as_float_returned.value(), val_as_float_returned);

            for (const auto & range : ranges_to_estimate)
                total_estimation -= estimator.estimateLess(val, total_rows, range.first, range.second, val_as_float_returned);

            total_estimation = std::max(total_estimation, 0.0);
            ranges.emplace_back(std::make_pair(-DBL_MAX, val_as_float_returned.value()));
            mergeAndSortRanges(ranges);
        }
        else if (op == "greater" || op == "greaterOrEquals")
        {
            std::vector<std::pair<Float64, Float64>> ranges_to_estimate;
            for (const auto & range : ranges)
            {
                if (range.second < val_as_float_returned.value())
                    continue;

                ranges_to_estimate.emplace_back(std::make_pair(std::max(val_as_float_returned.value(), range.first), range.second));
            }

            Float64 total_estimation = estimator.estimateLess(val, total_rows, val_as_float_returned.value(), {}, val_as_float_returned);

            for (const auto & range : ranges_to_estimate)
                total_estimation -= estimator.estimateLess(val, total_rows, range.first, range.second, val_as_float_returned);

            total_estimation = std::max(total_estimation, 0.0);
            ranges.emplace_back(std::make_pair(val_as_float_returned.value(), DBL_MAX));
            mergeAndSortRanges(ranges);
        }
        else
            curr_rows += default_unknown_cond_factor * total_rows;
    }

    return curr_rows;
#undef MY_LOG
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
