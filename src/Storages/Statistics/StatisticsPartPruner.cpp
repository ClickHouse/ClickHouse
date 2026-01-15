#include <Storages/Statistics/StatisticsPartPruner.h>
#include <Storages/MergeTree/RPNBuilder.h>

#include <cmath>
#include <limits>
#include <set>

namespace DB
{

/// Statistics stores min/max values as Float64, which may lose precision for large integers.
/// To ensure correctness of part pruning, we need to expand the range conservatively:
/// - min is adjusted towards negative infinity using std::nextafter
/// - max is adjusted towards positive infinity using std::nextafter
/// This ensures we never incorrectly skip parts that contain matching data.
static Float64 getConservativeMin(Float64 value)
{
    return std::nextafter(value, -std::numeric_limits<Float64>::infinity());
}

static Float64 getConservativeMax(Float64 value)
{
    return std::nextafter(value, std::numeric_limits<Float64>::infinity());
}

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

    /// Range comparison between Float64 Field and Decimal Field is incorrect.
    /// Skip optimization for Decimal types to avoid incorrect results.
    /// TODO: Remove this workaround after FieldVisitorAccurateLess is fixed.
    for (const auto & range : condition_ranges.ranges)
    {
        if (Field::isDecimal(range.left.getType()) || Field::isDecimal(range.right.getType()))
            return {true, true};
    }

    /// Apply conservative bounds to handle Float64 precision loss for large integers.
    Float64 conservative_min = getConservativeMin(est.estimated_min.value());
    Float64 conservative_max = getConservativeMax(est.estimated_max.value());
    Range part_range(Field(conservative_min), true, Field(conservative_max), true);

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
