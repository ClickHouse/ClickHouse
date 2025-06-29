#include <Storages/Statistics/ConditionSelectivityEstimator.h>

#include <stack>

#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeNullable.h>
#include <Storages/MergeTree/RPNBuilder.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

Float64 ConditionSelectivityEstimator::estimateRowCount(const RPNBuilderTreeNode & node) const
{
    std::vector<RPNElement> rpn = RPNBuilder<RPNElement>(node, [&](const RPNBuilderTreeNode & node_, RPNElement & out)
    {
        return extractAtomFromTree(node_, out);
    }).extractRPN();

    /// walk through the tree and calculate selectivity for every rpn node.
    std::stack<RPNElement *> rpn_stack;
    for (auto & element : rpn)
    {
        switch (element.function)
        {
            case RPNElement::FUNCTION_AND:
            case RPNElement::FUNCTION_OR:
            {
                auto* right_element = rpn_stack.top();
                rpn_stack.pop();
                auto* left_element = rpn_stack.top();
                rpn_stack.pop();
                if (right_element->function == RPNElement::ALWAYS_TRUE || left_element->function == RPNElement::ALWAYS_FALSE)
                    rpn_stack.push(element.function == RPNElement::FUNCTION_AND ? left_element : right_element);
                else if (right_element->function == RPNElement::ALWAYS_FALSE || left_element->function == RPNElement::ALWAYS_TRUE)
                    rpn_stack.push(element.function == RPNElement::FUNCTION_AND ? right_element : left_element);
                else if (element.tryToMergeClauses(*left_element, *right_element))
                    rpn_stack.push(&element);
                else
                {
                    left_element->finalize(column_estimators, total_rows);
                    right_element->finalize(column_estimators, total_rows);
                    /// P(c1 and c2) = P(c1) * P(c2)
                    if (element.function == RPNElement::FUNCTION_AND)
                        element.selectivity = left_element->selectivity * right_element->selectivity;
                    /// P(c1 or c2) = 1 - (1 - P(c1)) * (1 - P(c2))
                    else
                        element.selectivity = 1-(1-left_element->selectivity)*(1-right_element->selectivity);
                    element.finalized = true;
                }

                break;
            }
            case RPNElement::FUNCTION_NOT:
            {
                break;
            }
            default:
                rpn_stack.push(&element);
        }
    }
    auto* final_element = rpn_stack.top();
    final_element->finalize(column_estimators, total_rows);
    return final_element->selectivity * total_rows;
}

bool ConditionSelectivityEstimator::extractAtomFromTree(const RPNBuilderTreeNode & node, RPNElement & out) const
{
    const auto * node_dag = node.getDAGNode();
    if (node_dag && node_dag->result_type->equals(DataTypeNullable(std::make_shared<DataTypeNothing>())))
    {
        /// If the inferred result type is Nullable(Nothing) at the query analysis stage,
        /// we don't analyze this node further as its condition will always be false.
        out.function = RPNElement::ALWAYS_FALSE;
        return true;
    }
    Field const_value;
    DataTypePtr const_type;
    String column_name;

    if (node.isFunction())
    {
        auto func = node.toFunctionNode();
        size_t num_args = func.getArgumentsSize();

        String func_name = func.getFunctionName();
        if (atom_map.find(func_name) == std::end(atom_map))
            return false;

        if (num_args == 2)
        {
            if (func.getArgumentAt(1).tryGetConstant(const_value, const_type))
            {
                if (const_value.isNull())
                {
                    out.function = RPNElement::ALWAYS_FALSE;
                    return true;
                }
                column_name = func.getArgumentAt(0).getColumnName();
            }
            else if (func.getArgumentAt(0).tryGetConstant(const_value, const_type))
            {
                if (const_value.isNull())
                {
                    out.function = RPNElement::ALWAYS_FALSE;
                    return true;
                }
                column_name = func.getArgumentAt(1).getColumnName();
                if (func_name == "less")
                    func_name = "greater";
                else if (func_name == "greater")
                    func_name = "less";
                else if (func_name == "greaterOrEquals")
                    func_name = "lessOrEquals";
                else if (func_name == "lessOrEquals")
                    func_name = "greaterOrEquals";
            }
            else
                return false;
            const auto atom_it = atom_map.find(func_name);
            atom_it->second(out, column_name, const_value);
            return true;
        }
    }
    return false;
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

Float64 ConditionSelectivityEstimator::ColumnSelectivityEstimator::estimateLess(const Field & val, Float64 rows) const
{
    if (part_statistics.empty())
        return default_cond_range_factor * rows;
    Float64 result = 0;
    Float64 part_rows = 0;
    for (const auto & [key, estimator] : part_statistics)
    {
        result += estimator->estimateLess(val);
        part_rows += estimator->rowCount();
    }
    return result * rows / part_rows;
}

Float64 ConditionSelectivityEstimator::ColumnSelectivityEstimator::estimateGreater(const Field & val, Float64 rows) const
{
    return rows - estimateLess(val, rows);
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

Float64 ConditionSelectivityEstimator::ColumnSelectivityEstimator::estimateRanges(const PlainRanges & ranges, Float64 rows) const
{
    Float64 partial_cnt = 0;
    Float64 result = 0;
    for (const auto & [key, estimator] : part_statistics)
    {
        for (const Range & range : ranges.ranges)
        {
            result += estimator->estimateRange(range);
        }
        partial_cnt += estimator->rowCount();
    }
    return result * rows / partial_cnt;
}

const ConditionSelectivityEstimator::AtomMap ConditionSelectivityEstimator::atom_map
{
        {
            "notEquals",
            [] (RPNElement & out, const String & column, const Field & value)
            {
                out.function = RPNElement::FUNCTION_NOT_IN_RANGE;
                out.column_ranges.emplace(column, Range(value));
            }
        },
        {
            "equals",
            [] (RPNElement & out, const String & column, const Field & value)
            {
                out.function = RPNElement::FUNCTION_IN_RANGE;
                out.column_ranges.emplace(column, Range(value));
            }
        },
        {
            "less",
            [] (RPNElement & out, const String & column, const Field & value)
            {
                out.function = RPNElement::FUNCTION_IN_RANGE;
                out.column_ranges.emplace(column, Range::createRightBounded(value, false));
            }
        },
        {
            "greater",
            [] (RPNElement & out, const String & column, const Field & value)
            {
                out.function = RPNElement::FUNCTION_IN_RANGE;
                out.column_ranges.emplace(column, Range::createLeftBounded(value, false));
            }
        },
        {
            "lessOrEquals",
            [] (RPNElement & out, const String & column, const Field & value)
            {
                out.function = RPNElement::FUNCTION_IN_RANGE;
                out.column_ranges.emplace(column, Range::createRightBounded(value, true));
            }
        },
        {
            "greaterOrEquals",
            [] (RPNElement & out, const String & column, const Field & value)
            {
                out.function = RPNElement::FUNCTION_IN_RANGE;
                out.column_ranges.emplace(column, Range::createLeftBounded(value, false));
            }
        }
};

bool ConditionSelectivityEstimator::RPNElement::tryToMergeClauses(RPNElement & lhs, RPNElement & rhs)
{
    auto canMergeWith = [&](const RPNElement & e)
    {
        return (e.function == FUNCTION_IN_RANGE
                || e.function == FUNCTION_NOT_IN_RANGE
                || e.function == function
                || e.function == FUNCTION_UNKNOWN)
                && !e.finalized;
    };
    if (canMergeWith(lhs) && canMergeWith(rhs))
    {
        for (auto & [column_name, ranges] : lhs.column_ranges)
        {
            auto rit = rhs.column_ranges.find(column_name);
            if (rit != rhs.column_ranges.end())
            {
                if (function == FUNCTION_AND)
                    column_ranges.emplace(column_name, ranges.intersectWith(rit->second));
                else
                    column_ranges.emplace(column_name, ranges.unionWith(rit->second));
            }
            else
                column_ranges.emplace(column_name, ranges);
        }
        for (auto & [column_name, ranges] : rhs.column_ranges)
        {
            if (!lhs.column_ranges.contains(column_name))
                column_ranges.emplace(column_name, ranges);
        }
        return true;
    }
    return false;
}

void ConditionSelectivityEstimator::RPNElement::finalize(const ColumnEstimators & column_estimators_, Float64 total_rows_)
{
    if (finalized)
        return;
    std::vector<Float64> estimate_results;
    for (const auto & [column_name, ranges] : column_ranges)
    {
        auto it = column_estimators_.find(column_name);
        if (it == column_estimators_.end())
        {
            selectivity = ConditionSelectivityEstimator::default_unknown_cond_factor;
        }
        else
            estimate_results.emplace_back(it->second.estimateRanges(ranges, total_rows_));
    }
    selectivity = 1.0;
    for (const auto & estimate_result : estimate_results)
    {
        if (function == FUNCTION_OR)
            selectivity *= 1-estimate_result;
        else
            selectivity *= estimate_result;
    }
    if (function == FUNCTION_OR)
        selectivity = 1-selectivity;
    finalized = true;
}

}
