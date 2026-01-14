#include <Storages/Statistics/StatisticsPartPruner.h>
#include <Storages/MergeTree/RPNBuilder.h>

#include <set>

namespace DB
{

StatisticsPartPruner::StatisticsPartPruner(std::vector<RPNElement> rpn_)
    : rpn(std::move(rpn_))
{
    for (const auto & element : rpn)
    {
        if (element.function == RPNElement::FUNCTION_IN_RANGE && !element.column_ranges.empty())
        {
            has_minmax_conditions = true;
            break;
        }
    }
    if (has_minmax_conditions)
        buildDescription();
}

std::optional<StatisticsPartPruner> StatisticsPartPruner::build(
    const StorageMetadataPtr & metadata,
    const ActionsDAG::Node * filter_node,
    ContextPtr context)
{
    if (!filter_node)
        return std::nullopt;

    std::vector<RPNElement> rpn = RPNBuilder<RPNElement>(
        filter_node,
        context,
        [&](const RPNBuilderTreeNode & node, RPNElement & out)
        {
            return ConditionSelectivityEstimator::extractAtomFromTree(metadata, node, out);
        }
    ).extractRPN();
    if (rpn.empty())
        return std::nullopt;

    StatisticsPartPruner pruner(std::move(rpn));
    if (!pruner.hasUsefulConditions())
        return std::nullopt;

    return pruner;
}

BoolMask StatisticsPartPruner::checkPartCanMatch(const Estimates & estimates) const
{
    if (rpn.empty())
        return {true, true};

    std::vector<BoolMask> rpn_stack;

    for (const auto & element : rpn)
    {
        switch (element.function)
        {
            case RPNElement::FUNCTION_IN_RANGE:
            {
                BoolMask result{true, true};
                for (const auto & [column_name, ranges] : element.column_ranges)
                {
                    BoolMask column_result = checkRangeCondition(column_name, ranges, estimates);
                    result = result & column_result;
                }

                rpn_stack.push_back(result);
                break;
            }

            case RPNElement::FUNCTION_AND:
            {
                auto arg2 = rpn_stack.back();
                rpn_stack.pop_back();
                auto arg1 = rpn_stack.back();
                rpn_stack.pop_back();
                rpn_stack.push_back(arg1 & arg2);
                break;
            }

            case RPNElement::FUNCTION_OR:
            {
                auto arg2 = rpn_stack.back();
                rpn_stack.pop_back();
                auto arg1 = rpn_stack.back();
                rpn_stack.pop_back();
                rpn_stack.push_back(arg1 | arg2);
                break;
            }

            case RPNElement::FUNCTION_NOT:
            {
                auto arg = rpn_stack.back();
                rpn_stack.pop_back();
                rpn_stack.push_back(!arg);
                break;
            }

            case RPNElement::FUNCTION_UNKNOWN:
                rpn_stack.emplace_back(true, true);
                break;

            case RPNElement::ALWAYS_TRUE:
                rpn_stack.emplace_back(true, false);
                break;

            case RPNElement::ALWAYS_FALSE:
                rpn_stack.emplace_back(false, true);
                break;
        }
    }

    return rpn_stack.empty() ? BoolMask{true, true} : rpn_stack.back();
}

BoolMask StatisticsPartPruner::checkRangeCondition(
    const String & column_name,
    const PlainRanges & condition_ranges,
    const Estimates & estimates)
{
    auto it = estimates.find(column_name);
    if (it == estimates.end())
        return {true, true};

    const Estimate & est = it->second;
    if (!est.estimated_min.has_value() || !est.estimated_max.has_value())
        return {true, true};

    Range part_range(Field(est.estimated_min.value()), true, Field(est.estimated_max.value()), true);

    bool intersects = false;
    bool contains = true;

    for (const auto & range : condition_ranges.ranges)
    {
        if (range.intersectsRange(part_range))
            intersects = true;
        if (!range.containsRange(part_range))
            contains = false;
    }

    return {intersects, !contains};
}

void StatisticsPartPruner::buildDescription()
{
    std::set<std::string> columns;
    for (const auto & element : rpn)
    {
        if (element.function == RPNElement::FUNCTION_IN_RANGE)
        {
            for (const auto & [column_name, _] : element.column_ranges)
                columns.insert(column_name);
        }
    }
    used_columns.assign(columns.begin(), columns.end());
}

}
