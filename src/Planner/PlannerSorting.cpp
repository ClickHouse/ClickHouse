#include <Planner/PlannerSorting.h>

#include <Core/Settings.h>

#include <Common/FieldVisitorsAccurateComparison.h>

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

std::pair<Field, std::optional<IntervalKind>> extractWithFillStepValue(const QueryTreeNodePtr & node)
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
        auto extract_result = extractWithFillStepValue(sort_node.getFillStep());
        fill_column_description.fill_step = std::move(extract_result.first);
        fill_column_description.step_kind = std::move(extract_result.second);
    }
    else
    {
        auto direction_value = sort_node.getSortDirection() == SortDirection::ASCENDING ? static_cast<Int64>(1) : static_cast<Int64>(-1);
        fill_column_description.fill_step = Field(direction_value);
    }

    if (applyVisitor(FieldVisitorAccurateEquals(), fill_column_description.fill_step, Field{0}))
        throw Exception(ErrorCodes::INVALID_WITH_FILL_EXPRESSION,
            "WITH FILL STEP value cannot be zero");

    if (sort_node.getSortDirection() == SortDirection::ASCENDING)
    {
        if (applyVisitor(FieldVisitorAccurateLess(), fill_column_description.fill_step, Field{0}))
            throw Exception(ErrorCodes::INVALID_WITH_FILL_EXPRESSION,
                "WITH FILL STEP value cannot be negative for sorting in ascending direction");

        if (!fill_column_description.fill_from.isNull() && !fill_column_description.fill_to.isNull() &&
            applyVisitor(FieldVisitorAccurateLess(), fill_column_description.fill_to, fill_column_description.fill_from))
        {
            throw Exception(ErrorCodes::INVALID_WITH_FILL_EXPRESSION,
                "WITH FILL TO value cannot be less than FROM value for sorting in ascending direction");
        }
    }
    else
    {
        if (applyVisitor(FieldVisitorAccurateLess(), Field{0}, fill_column_description.fill_step))
            throw Exception(ErrorCodes::INVALID_WITH_FILL_EXPRESSION,
                "WITH FILL STEP value cannot be positive for sorting in descending direction");

        if (!fill_column_description.fill_from.isNull() && !fill_column_description.fill_to.isNull() &&
            applyVisitor(FieldVisitorAccurateLess(), fill_column_description.fill_from, fill_column_description.fill_to))
        {
            throw Exception(ErrorCodes::INVALID_WITH_FILL_EXPRESSION,
                "WITH FILL FROM value cannot be less than TO value for sorting in descending direction");
        }
    }

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
            sort_column_description.emplace_back(column_name, direction, nulls_direction, collator, true /*with_fill*/, fill_description);
        }
        else
        {
            sort_column_description.emplace_back(column_name, direction, nulls_direction, collator);
        }
    }

    const auto & settings = planner_context.getQueryContext()->getSettingsRef();
    sort_column_description.compile_sort_description = settings[Setting::compile_sort_description];
    sort_column_description.min_count_to_compile_sort_description = settings[Setting::min_count_to_compile_sort_description];

    return sort_column_description;
}

}
