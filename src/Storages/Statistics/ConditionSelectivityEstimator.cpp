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

#define MY_LOG(...) LOG_DEBUG(getLogger("Alex Estimator"), __VA_ARGS__)

std::pair<String, Field> ConditionSelectivityEstimator::extractBinaryOp(const RPNBuilderTreeNode & node, const String & qualified_column_name) const
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

    bool lhs_argument_is_column = qualified_column_name == (lhs_argument_column_name);
    bool rhs_argument_is_column = qualified_column_name == (rhs_argument_column_name);

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

void ConditionSelectivityEstimator::doSmth(const RPNBuilderTreeNode & node, const String & qualified_column_name, std::vector<std::pair<String, Field>> & result) const
{
    if (!node.isFunction())
        return;

    auto function_node = node.toFunctionNode();
    if (function_node.getArgumentsSize() != 2)
        return;

    String function_name = function_node.getFunctionName();

    auto lhs_argument = function_node.getArgumentAt(0);
    auto rhs_argument = function_node.getArgumentAt(1);

    if (function_name == "and")
    {
        doSmth(lhs_argument, qualified_column_name, result);
        doSmth(rhs_argument, qualified_column_name, result);
        return;
    }

    auto lhs_argument_column_name = lhs_argument.getColumnName();
    auto rhs_argument_column_name = rhs_argument.getColumnName();

    bool lhs_argument_is_column = qualified_column_name == (lhs_argument_column_name);
    bool rhs_argument_is_column = qualified_column_name == (rhs_argument_column_name);

    bool lhs_argument_is_constant = lhs_argument.isConstant();
    bool rhs_argument_is_constant = rhs_argument.isConstant();

    RPNBuilderTreeNode * constant_node = nullptr;
    MY_LOG("doSmth way before if");

    std::string l_arg_col;
    std::string r_arg_col;
    bool get_from_cast = false;

    MY_LOG("doSmth before if");

    if (lhs_argument_is_column && !rhs_argument_is_constant)
    {
        MY_LOG("doSmth if 1");
        if (rhs_argument.isFunction())
        {
            MY_LOG("doSmth if 1.1-1");
            auto rhs_function = rhs_argument.toFunctionNode();
            MY_LOG("doSmth if 1.1-2");

            if (rhs_function.getFunctionName() == "_CAST")
            {
                MY_LOG("doSmth if 1.1-3");
                auto l_arg = rhs_function.getArgumentAt(0);
                MY_LOG("doSmth if 1.1-4");
                MY_LOG("doSmth if 1.1-5");
                auto r_arg = rhs_function.getArgumentAt(1);
                MY_LOG("doSmth if 1.1-6");
                MY_LOG("doSmth if 1.1-7");
                l_arg_col = l_arg.getColumnName();
                MY_LOG("doSmth if 1.1-8");
                r_arg_col = r_arg.getColumnName();
                MY_LOG("doSmth if 1.1-9");
                get_from_cast = true;
                constant_node = &l_arg;
                Field output_value;
                DataTypePtr output_type;
                if (!constant_node->tryGetConstant(output_value, output_type))
                    return;
                MY_LOG("doSmth got constant value inside if 1");
                result.emplace_back(std::make_pair(function_name, output_value));
                return;
            }
            MY_LOG("doSmth if 1.1-10");
        }
    }
    else if (!lhs_argument_is_constant && rhs_argument_is_column)
    {
        MY_LOG("doSmth if 2");
        if (lhs_argument.isFunction())
        {
            MY_LOG("doSmth if 2.1-1");
            auto lhs_function = lhs_argument.toFunctionNode();
            MY_LOG("doSmth if 2.1-2");

            if (lhs_function.getFunctionName() == "_CAST")
            {
                MY_LOG("doSmth if 2.1-3");
                auto l_arg = lhs_function.getArgumentAt(0);
                MY_LOG("doSmth if 2.1-4");
                MY_LOG("doSmth if 2.1-5");
                auto r_arg = lhs_function.getArgumentAt(1);
                MY_LOG("doSmth if 2.1-6");
                MY_LOG("doSmth if 2.1-7");
                l_arg_col = l_arg.getColumnName();
                MY_LOG("doSmth if 2.1-8");
                r_arg_col = r_arg.getColumnName();
                MY_LOG("doSmth if 2.1-9");
                get_from_cast = true;
                constant_node = &l_arg;
                Field output_value;
                DataTypePtr output_type;
                if (!constant_node->tryGetConstant(output_value, output_type))
                    return;
                MY_LOG("doSmth got constant value inside if 2");
                result.emplace_back(std::make_pair(function_name, output_value));
                return;
            }
            MY_LOG("doSmth if 2.1-10");
        }
    }
    else
    {
        MY_LOG("doSmth if 3-1");
        l_arg_col = "";
        MY_LOG("doSmth if 3-2");
        r_arg_col = "";
        MY_LOG("doSmth if 3-3");
    }

    MY_LOG("doSmth after if");
    MY_LOG("doSmth l_arg_col - {}, r_arg_col - {}", l_arg_col, r_arg_col);

    if (lhs_argument_is_column && rhs_argument_is_constant)
        constant_node = &rhs_argument;
    else if (lhs_argument_is_constant && rhs_argument_is_column)
        constant_node = &lhs_argument;
    else if (lhs_argument_is_column && get_from_cast)
        MY_LOG("doSmth got from lhs cast");
    else if (rhs_argument_is_column && get_from_cast)
        MY_LOG("doSmth got from rhs cast");
    else
        return;

    MY_LOG("doSmth got constant node - {}", constant_node->getColumnName());
    Field output_value;
    DataTypePtr output_type;
    if (!constant_node->tryGetConstant(output_value, output_type))
        return;
    MY_LOG("doSmth got constant value");
    result.emplace_back(std::make_pair(function_name, output_value));
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

Float64 ConditionSelectivityEstimator::estimateRowCount(const RPNBuilderTreeNode & node) const
{
    MY_LOG("Estimate without any unq");
    return estimateRowCount(node, std::unordered_map<std::string, ColumnWithTypeAndName>());
}


Float64 ConditionSelectivityEstimator::estimateRowCount(const RPNBuilderTreeNode & node, const std::unordered_map<std::string, ColumnWithTypeAndName> & unqualifiedColumnsNames) const
{
    MY_LOG("Estimate started");
    auto result = tryToExtractSingleColumn(node);
    MY_LOG("Estimate result count - {}", result.second);
    if (result.second != 1)
        return default_unknown_cond_factor * total_rows;

    String column = result.first;
    MY_LOG("Estimate column before transform - {}", column);
    MY_LOG("Estimate unq size - {}", unqualifiedColumnsNames.size());
    auto it_col = unqualifiedColumnsNames.find(column);

    if (it_col != unqualifiedColumnsNames.end())
    {
        column = it_col->second.name;
    }

    MY_LOG("Estimate column after transform - {}", column);
    auto it = column_estimators.find(column);

    for (const auto & e : column_estimators)
        MY_LOG("Estimate column estimator for {} was found", e.first);

    /// If there the estimator of the column is not found or there are no data at all,
    /// we use dummy estimation.
    bool dummy = false;
    ColumnSelectivityEstimator estimator;
    if (it != column_estimators.end())
    {
        MY_LOG("Estimate found estimator for a column");
        estimator = it->second;
    }
    else
    {
        MY_LOG("Estimate dummy estimator will be used");
        dummy = true;
    }

    std::vector<std::pair<String, Field>> op_result;
    doSmth(node, result.first, op_result);
    MY_LOG("Estimate op result size - {}", op_result.size());

    if (op_result.size() == 1)
    {
        auto [op, val] = extractBinaryOp(node, result.first);
        MY_LOG("Estimate op - {}", op);

        if (dummy)
        {
            if (op == "equals")
                return default_cond_equal_factor * total_rows;
            if (op == "less" || op == "lessOrEquals" || op == "greater" || op == "greaterOrEquals")
                return default_cond_range_factor * total_rows;
            return default_unknown_cond_factor * total_rows;
        }

        if (op == "equals")
            return estimator.estimateEqual(val, total_rows);
        if (op == "less" || op == "lessOrEquals")
            return estimator.estimateLess(val, total_rows, nullptr, {}, {});
        if (op == "greater" || op == "greaterOrEquals")
            return estimator.estimateGreater(val, total_rows, nullptr, {}, {});

        return default_unknown_cond_factor * total_rows;
    }
    else
    {
        Float64 curr_rows = total_rows;
        std::optional<Float64> curr_min = {};
        std::optional<Float64> curr_max = {};
        for (auto [op, val] : op_result)
        {
            MY_LOG("Estimate inside loop op - {}", op);
            MY_LOG("Estimate inside loop curr_rows before - {}", curr_rows);

            if (dummy)
            {
                continue;
                /*
                if (op == "equals")
                    curr_rows *= default_cond_equal_factor;
                else if (op == "less" || op == "lessOrEquals" || op == "greater" || op == "greaterOrEquals")
                    curr_rows *= default_cond_range_factor;
                else
                    curr_rows *= default_unknown_cond_factor;
                */
            }
            else
            {
                Float64 calculated_val = 0.0;
                if (op == "equals")
                    curr_rows = estimator.estimateEqual(val, curr_rows);
                else if (op == "less" || op == "lessOrEquals")
                {
                    curr_rows = estimator.estimateLess(val, curr_rows, &calculated_val, curr_min, curr_max);
                    curr_max = calculated_val;
                }
                else if (op == "greater" || op == "greaterOrEquals")
                {
                    curr_rows = estimator.estimateGreater(val, curr_rows, &calculated_val, curr_min, curr_max);
                    curr_min = calculated_val;
                }
                else
                    curr_rows *= default_unknown_cond_factor;
            }

            MY_LOG("Estimate inside loop curr_rows after - {}", curr_rows);
        }
        return curr_rows;
    }
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

Float64 ConditionSelectivityEstimator::ColumnSelectivityEstimator::estimateLess(const Field & val, Float64 rows, Float64 * calculated_val, std::optional<Float64> custom_min, std::optional<Float64> custom_max) const
{
    MY_LOG("estimateLess started with {} rows", rows);
    if (part_statistics.empty())
        return default_cond_range_factor * rows;
    MY_LOG("estimateLess has some stats");
    Float64 result = 0;
    Float64 part_rows = 0;
    for (const auto & [key, estimator] : part_statistics)
    {
        MY_LOG("estimateLess checking stats for part {}", key);
        result += estimator->estimateLess(val, calculated_val, custom_min, custom_max);
        part_rows += estimator->rowCount();
    }
    MY_LOG("estimateLess result - {}, part_rows - {}, rows - {}", result, part_rows, rows);
    auto to_return = result * rows / part_rows;
    MY_LOG("estimateLess final result - {}", to_return);
    return to_return;
    // return result * rows / part_rows;
}

Float64 ConditionSelectivityEstimator::ColumnSelectivityEstimator::estimateGreater(const Field & val, Float64 rows, Float64 * calculated_val, std::optional<Float64> custom_min, std::optional<Float64> custom_max) const
{
    auto to_return = rows - estimateLess(val, rows, calculated_val, custom_min, custom_max);
    MY_LOG("estimateGreater final result - {}", to_return);
    return to_return;
    // return rows - estimateLess(val, rows);
}

Float64 ConditionSelectivityEstimator::ColumnSelectivityEstimator::estimateEqual(const Field & val, Float64 rows) const
{
    if (part_statistics.empty())
    {
        return default_cond_equal_factor * rows;
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
}
