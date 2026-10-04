#include <Planner/PlannerSorting.h>

#include <Core/Settings.h>

#include <Common/FieldAccurateComparison.h>

#include <DataTypes/DataTypeInterval.h>

#include <Interpreters/Context.h>

#include <Analyzer/ConstantNode.h>
#include <Analyzer/SortNode.h>

#include <Planner/PlannerActionsVisitor.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool compile_sort_description;
    extern const SettingsUInt64 min_count_to_compile_sort_description;
}

namespace ErrorCodes
{
    extern const int INVALID_WITH_FILL_EXPRESSION;
}

namespace
{

std::pair<Field, DataTypePtr> extractWithFillValue(const QueryTreeNodePtr & node)
{
    const auto & constant_node = node->as<ConstantNode &>();

    std::pair<Field, DataTypePtr> result;
    result.first = constant_node.getValue();
    result.second = constant_node.getResultType();

    if (!isColumnedAsNumber(result.second))
        throw Exception(ErrorCodes::INVALID_WITH_FILL_EXPRESSION, "WITH FILL expression must be constant with numeric type");

    return result;
}

std::pair<Field, std::optional<IntervalKind>> extractWithFillValueWithIntervalKind(const QueryTreeNodePtr & node)
{
    const auto & constant_node = node->as<ConstantNode &>();

    const auto & constant_node_result_type = constant_node.getResultType();
    if (const auto * type_interval = typeid_cast<const DataTypeInterval *>(constant_node_result_type.get()))
        return std::make_pair(constant_node.getValue(), type_interval->getKind());

    if (!isColumnedAsNumber(constant_node_result_type))
        throw Exception(ErrorCodes::INVALID_WITH_FILL_EXPRESSION, "WITH FILL expression must be constant with numeric type");

    return {constant_node.getValue(), {}};
}

FillColumnDescription extractWithFillDescription(const SortNode & sort_node)
{
    FillColumnDescription fill_column_description;

    if (sort_node.hasFillFrom())
    {
        auto extract_result = extractWithFillValue(sort_node.getFillFrom());
        fill_column_description.fill_from = std::move(extract_result.first);
        fill_column_description.fill_from_type = std::move(extract_result.second);
    }

    if (sort_node.hasFillTo())
    {
        auto extract_result = extractWithFillValue(sort_node.getFillTo());
        fill_column_description.fill_to = std::move(extract_result.first);
        fill_column_description.fill_to_type = std::move(extract_result.second);
    }

    if (sort_node.hasFillStep())
    {
        auto extract_result = extractWithFillValueWithIntervalKind(sort_node.getFillStep());
        fill_column_description.fill_step = std::move(extract_result.first);
        fill_column_description.step_kind = std::move(extract_result.second);
    }
    else
    {
        auto direction_value = sort_node.getSortDirection() == SortDirection::ASCENDING ? static_cast<Int64>(1) : static_cast<Int64>(-1);
        fill_column_description.fill_step = Field(direction_value);
    }

    if (sort_node.getFillStaleness())
    {
        auto extract_result = extractWithFillValueWithIntervalKind(sort_node.getFillStaleness());
        fill_column_description.fill_staleness = std::move(extract_result.first);
        fill_column_description.staleness_kind = std::move(extract_result.second);
    }

    if (const auto reason = checkFillDescription(fill_column_description, sort_node.getSortDirection() == SortDirection::ASCENDING ? 1 : -1);
        !reason.empty())
        throw Exception(ErrorCodes::INVALID_WITH_FILL_EXPRESSION, "{}", reason);

    return fill_column_description;
}

}

SortDescription extractSortDescription(const QueryTreeNodePtr & order_by_node, const PlannerContext & planner_context)
{
    auto & order_by_list_node = order_by_node->as<ListNode &>();

    SortDescription sort_column_description;
    sort_column_description.reserve(order_by_list_node.getNodes().size());

    for (const auto & sort_node : order_by_list_node.getNodes())
    {
        auto & sort_node_typed = sort_node->as<SortNode &>();

        auto column_name = calculateActionNodeName(sort_node_typed.getExpression(), planner_context);
        std::shared_ptr<Collator> collator = sort_node_typed.getCollator();
        int direction = sort_node_typed.getSortDirection() == SortDirection::ASCENDING ? 1 : -1;
        int nulls_direction = direction;

        auto nulls_sort_direction = sort_node_typed.getNullsSortDirection();
        if (nulls_sort_direction)
            nulls_direction = *nulls_sort_direction == SortDirection::ASCENDING ? 1 : -1;

        if (sort_node_typed.withFill())
        {
            FillColumnDescription fill_description = extractWithFillDescription(sort_node_typed);
            if (sort_node_typed.getColumnName().empty())
                sort_column_description.emplace_back(column_name, direction, nulls_direction, collator, true /*with_fill*/, fill_description);
            else
                sort_column_description.emplace_back(sort_node_typed.getColumnName(), column_name, direction, nulls_direction, collator, true /*with_fill*/, fill_description);
        }
        else
        {
            if (sort_node_typed.getColumnName().empty())
                sort_column_description.emplace_back(column_name, direction, nulls_direction, collator);
            else
                sort_column_description.emplace_back(sort_node_typed.getColumnName(), column_name, direction, nulls_direction, collator);
        }
    }

    const auto & settings = planner_context.getQueryContext()->getSettingsRef();
    sort_column_description.compile_sort_description = settings[Setting::compile_sort_description];
    sort_column_description.min_count_to_compile_sort_description = settings[Setting::min_count_to_compile_sort_description];

    return sort_column_description;
}

}
