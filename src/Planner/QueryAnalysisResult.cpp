#include <Planner/QueryAnalysisResult.h>
#include <base/arithmeticOverflow.h>

#include <Planner/PlannerQueryProcessingInfo.h>
#include <Planner/PlannerSorting.h>
#include <Planner/Utils.h>

#include <Analyzer/ConstantNode.h>
#include <Analyzer/QueryNode.h>

#include <Processors/QueryPlan/ShuffleStep.h>

#include <Interpreters/Context.h>
#include <Interpreters/convertColumnToType.h>

#include <DataTypes/DataTypesNumber.h>

#include <Core/Settings.h>

#include <Common/Exception.h>
#include <Common/FieldVisitorToString.h>

#include <limits>
#include <tuple>

namespace DB
{

namespace Setting
{
    extern const SettingsBool distributed_aggregation_memory_efficient;
    extern const SettingsBool enable_memory_bound_merging_of_aggregation_results;
    extern const SettingsOverflowModeGroupBy group_by_overflow_mode;
    extern const SettingsUInt64 max_rows_to_group_by;
    extern const SettingsTotalsMode totals_mode;
}

namespace ErrorCodes
{
    extern const int INVALID_LIMIT_EXPRESSION;
    extern const int NOT_IMPLEMENTED;
}

namespace
{

/// The LIMIT/OFFSET expression value can be either UInt64 or Float64, negative or positive.
std::tuple<UInt64, Float64, bool> getLimitOffsetValue(const ConstantNode & node)
{
    const IColumn & value = *node.getColumn();
    const DataTypePtr & type = node.getResultType();

    // First check if it is nonnegative limit since they are more common
    if (ColumnPtr converted = convertColumnToTypeOrNull(value, type, std::make_shared<DataTypeUInt64>()))
        return {converted->getUInt(0), 0, false};

    if (ColumnPtr converted = convertColumnToTypeOrNull(value, type, std::make_shared<DataTypeInt64>()))
    {
        Int64 int_value = converted->getInt(0);

        chassert(int_value < 0 && "nonnegative limit/offset values should be handled with UInt64");

        const UInt64 magnitude = common::negateIgnoreOverflow(static_cast<UInt64>(int_value));
        return {magnitude, 0, true};
    }

    if (ColumnPtr converted = convertColumnToTypeOrNull(value, type, std::make_shared<DataTypeFloat64>()))
        return {0, converted->getFloat64(0), false};

    throw Exception(
        ErrorCodes::INVALID_LIMIT_EXPRESSION,
        "The value {} of LIMIT/OFFSET expression is not representable as UInt64 or Int64 or Float64 in the range (0, 1)",
        applyVisitor(FieldVisitorToString(), node.getValue()));
}

}

QueryAnalysisResult QueryAnalysisResult::create(const QueryTreeNodePtr & query_tree,
    const PlannerQueryProcessingInfo & query_processing_info,
    const PlannerContextPtr & planner_context)
{
    const auto & query_node = query_tree->as<QueryNode &>();
    const auto & query_context = planner_context->getQueryContext();
    const auto & settings = query_context->getSettingsRef();

    QueryAnalysisResult result;

    result.aggregate_overflow_row = query_node.isGroupByWithTotals() && settings[Setting::max_rows_to_group_by]
        && settings[Setting::group_by_overflow_mode] == OverflowMode::ANY && settings[Setting::totals_mode] != TotalsMode::AFTER_HAVING_EXCLUSIVE;
    result.aggregate_final = query_processing_info.getToStage() > QueryProcessingStage::WithMergeableState
        && !query_node.isGroupByWithTotals() && !query_node.isGroupByWithRollup() && !query_node.isGroupByWithCube();
    result.aggregation_with_rollup_or_cube_or_grouping_sets = query_node.isGroupByWithRollup() || query_node.isGroupByWithCube() ||
        query_node.isGroupByWithGroupingSets();
    result.aggregation_should_produce_results_in_order_of_bucket_number
        = query_processing_info.getToStage() == QueryProcessingStage::WithMergeableState
        && (settings[Setting::distributed_aggregation_memory_efficient] || settings[Setting::enable_memory_bound_merging_of_aggregation_results]);

    result.query_has_array_join_in_join_tree = queryHasArrayJoinInJoinTree(query_tree);
    result.query_has_with_totals_in_any_subquery_in_join_tree = queryHasWithTotalsInAnySubqueryInJoinTree(query_tree);

    result.sort_description = extractSortDescription(query_node.getOrderByNode(), *planner_context, query_node.getProjectionNode());

    /// The sort description of `SHUFFLE` is used to merge the shuffled streams from remote servers by the internal
    /// key column, which is added only to the result of a shard (see `addShuffleStep`).
    if (query_node.isShuffle())
        result.sort_description.emplace_back(ShuffleStep::internal_key_column_name, 1, 1);

    if (query_node.hasLimit())
    {
        /// Constness of limit is validated during query analysis stage
        std::tie(result.limit_length, result.fractional_limit, result.is_limit_length_negative)
            = getLimitOffsetValue(query_node.getLimit()->as<ConstantNode &>());

        if (query_node.hasOffset() && (result.limit_length || result.fractional_limit > 0))
        {
            /// Constness of offset is validated during query analysis stage
            std::tie(result.limit_offset, result.fractional_offset, result.is_limit_offset_negative)
                = getLimitOffsetValue(query_node.getOffset()->as<ConstantNode &>());
        }
    }
    else if (query_node.hasOffset())
    {
        /// Constness of offset is validated during query analysis stage
        std::tie(result.limit_offset, result.fractional_offset, result.is_limit_offset_negative)
            = getLimitOffsetValue(query_node.getOffset()->as<ConstantNode &>());
    }

    /// Partial sort can be done if there is LIMIT, but no DISTINCT, LIMIT WITH TIES, LIMIT BY, ARRAY JOIN, NEGATIVE LIMIT, FRACTIONAL LIMIT/OFFSET
    if (result.limit_length != 0 &&
        !query_node.hasLimitAfter() &&
        !query_node.hasLimitUntil() &&
        !query_node.isDistinct() &&
        !query_node.isLimitWithTies() &&
        !query_node.hasLimitBy() &&
        !result.query_has_array_join_in_join_tree &&
        result.fractional_offset == 0 &&
        result.fractional_limit == 0 &&
        result.limit_length <= std::numeric_limits<UInt64>::max() - result.limit_offset &&
        !result.is_limit_length_negative)
    {
        result.partial_sorting_limit = result.limit_length + result.limit_offset;
    }

    if (query_node.hasLimitBy())
    {
        Float64 fractional_limitby_limit = 0;
        std::tie(result.limit_by_length, fractional_limitby_limit, result.is_limit_by_length_negative)
            = getLimitOffsetValue(query_node.getLimitByLimit()->as<ConstantNode &>());

        Float64 fractional_limitby_offset = 0;
        if (query_node.hasLimitByOffset())
            std::tie(result.limit_by_offset, fractional_limitby_offset, result.is_limit_by_offset_negative)
                = getLimitOffsetValue(query_node.getLimitByOffset()->as<ConstantNode &>());

        if (fractional_limitby_limit > 0 || fractional_limitby_offset > 0)
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Fractional LIMIT/OFFSET with LIMIT BY is not supported yet");
    }

    return result;
}

}
