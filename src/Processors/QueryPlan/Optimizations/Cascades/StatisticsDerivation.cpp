#include <Processors/QueryPlan/Optimizations/Cascades/StatisticsDerivation.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/IntersectOrExceptStep.h>
#include <Processors/QueryPlan/Optimizations/Cascades/OptimizerDefaults.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/joinOrder.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Storages/Statistics/ConditionSelectivityEstimator.h>
#include <Columns/ColumnConst.h>
#include <Storages/IStorage.h>
#include <Interpreters/Context.h>
#include <Core/Settings.h>
#include <Common/logger_useful.h>
#include <Common/Exception.h>
#include <base/types.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace Setting
{
    extern const SettingsBool allow_statistics_optimize;
}

void StatisticsDerivation::deriveStatistics(GroupId group_id)
{
    auto group = memo.getGroup(group_id);

    if (group->statistics.has_value())
        return;

    /// Pick the first logical expression to derive statistics from
    /// (all logical expressions in a group represent the same logical result)
    if (group->logical_expressions.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Group #{} has no logical expressions to derive statistics from", group_id);

    auto expression = group->logical_expressions.front();
    const IQueryPlanStep * plan_step = expression->getQueryPlanStep();

    /// Ensure all input groups have statistics first (bottom-up derivation)
    for (const auto & input : expression->inputs)
    {
        auto input_group = memo.getGroup(input.group_id);
        if (!input_group->statistics.has_value())
            deriveStatistics(input.group_id);
    }

    /// Returns the statistics of the expression's input #index; throws if the expression has
    /// fewer inputs than its step type implies.
    auto input_statistics = [&](size_t index) -> const ExpressionStatistics &
    {
        if (index >= expression->inputs.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Expression '{}' has {} inputs, statistics derivation needs input #{}",
                expression->getName(), expression->inputs.size(), index);
        return *memo.getGroup(expression->inputs[index].group_id)->statistics;
    };

    if (const auto * join_step = typeid_cast<const JoinStepLogical *>(plan_step))
    {
        group->statistics = deriveJoinStatistics(*join_step, input_statistics(0), input_statistics(1));
    }
    else if (const auto * read_step = typeid_cast<const ReadFromMergeTree *>(plan_step))
    {
        group->statistics = deriveReadStatistics(*read_step);
    }
    else if (const auto * filter_step = typeid_cast<const FilterStep *>(plan_step))
    {
        group->statistics = deriveFilterStatistics(*filter_step, input_statistics(0));
    }
    else if (const auto * expression_step = typeid_cast<const ExpressionStep *>(plan_step))
    {
        group->statistics = deriveExpressionStatistics(*expression_step, input_statistics(0));
    }
    else if (const auto * aggregating_step = typeid_cast<const AggregatingStep *>(plan_step))
    {
        group->statistics = deriveAggregatingStatistics(*aggregating_step, input_statistics(0));
    }
    else if (const auto * sorting_step = typeid_cast<const SortingStep *>(plan_step))
    {
        group->statistics = deriveSortingStatistics(*sorting_step, input_statistics(0));
    }
    else if (const auto * limit_step = typeid_cast<const LimitStep *>(plan_step))
    {
        group->statistics = deriveLimitStatistics(*limit_step, input_statistics(0));
    }
    else if (const auto * distinct_step = typeid_cast<const DistinctStep *>(plan_step); distinct_step && !distinct_step->isPreliminary())
    {
        group->statistics = deriveDistinctStatistics(*distinct_step, input_statistics(0));
    }
    else if (const auto * intersect_or_except_step = typeid_cast<const IntersectOrExceptStep *>(plan_step))
    {
        /// The output reuses the first input's header and so its column statistics. Only
        /// `INTERSECT ALL` is bounded by the smallest input; the `DISTINCT` variants keep every
        /// matching duplicate of the first input at this step (a separate `Distinct` above
        /// deduplicates), and `EXCEPT` keeps at most the first input.
        ExpressionStatistics result = input_statistics(0);
        const auto op = intersect_or_except_step->getOperator();
        if (op == IntersectOrExceptStep::Operator::INTERSECT_ALL
            || op == IntersectOrExceptStep::Operator::INTERSECT_DISTINCT)
        {
            /// Every output row also matches a row of each other input, so a column's NDV is
            /// bounded by every input's NDV at the same position. Without this the `Distinct`
            /// above would estimate from the first input's NDVs alone.
            const auto & input_headers = intersect_or_except_step->getInputHeaders();
            for (size_t input_index = 1; input_index < expression->inputs.size(); ++input_index)
            {
                const auto & other = input_statistics(input_index);
                if (op == IntersectOrExceptStep::Operator::INTERSECT_ALL)
                {
                    result.estimated_row_count = std::min(result.estimated_row_count, other.estimated_row_count);
                    result.max_row_count = std::min(result.max_row_count, other.max_row_count);
                }
                /// The distinct output rows cannot exceed any input, with or without NDVs.
                result.estimated_distinct_bound = std::min(
                    {result.estimated_distinct_bound, other.estimated_row_count, other.estimated_distinct_bound});
                for (size_t position = 0; position < input_headers.at(0)->columns(); ++position)
                {
                    auto output_column = result.column_statistics.find(input_headers.at(0)->getByPosition(position).name);
                    if (output_column == result.column_statistics.end())
                        continue;
                    auto other_column = other.column_statistics.find(input_headers.at(input_index)->getByPosition(position).name);
                    if (other_column == other.column_statistics.end())
                        continue;
                    output_column->second.num_distinct_values
                        = std::min(output_column->second.num_distinct_values, other_column->second.num_distinct_values);
                }
            }
            /// Without the clamp a row-count reduction could leave a column NDV above the row count.
            for (auto & [column_name, column_stats] : result.column_statistics)
                column_stats.num_distinct_values = std::min(column_stats.num_distinct_values,
                    static_cast<UInt64>(std::max(result.estimated_row_count, 1.0)));
        }
        result.min_row_count = 0;
        group->statistics = std::move(result);
    }
    else if (!expression->inputs.empty())
    {
        /// By default take statistics from the first input
        group->statistics = input_statistics(0);
    }
    else
    {
        group->statistics = ExpressionStatistics();
    }

    LOG_TEST(log, "Derived statistics for group #{}:\n{}",
        group_id, group->statistics->dump());
}

Float64 clampJoinRowCount(JoinKind kind, JoinStrictness strictness, Float64 base, Float64 left, Float64 right)
{
    /// Paste is position-wise (no keys): one output row per aligned pair.
    if (kind == JoinKind::Paste)
        return std::min(left, right);
    /// Cross/comma product keeps the multiplicative estimate.
    if (kind == JoinKind::Cross || kind == JoinKind::Comma)
        return base;

    /// A full join keeps unmatched rows from both sides, so it is at least the larger side regardless
    /// of strictness. (Checked before the semi/any bounds, which apply only to one preserved side.)
    if (kind == JoinKind::Full)
        return std::max({base, left, right});

    const Float64 preserved = (kind == JoinKind::Right) ? right : left;

    /// Semi/anti filter the preserved side, so the output cannot exceed it.
    if (strictness == JoinStrictness::Semi || strictness == JoinStrictness::Anti)
        return std::min(base, preserved);

    if (strictness == JoinStrictness::Any || strictness == JoinStrictness::RightAny || strictness == JoinStrictness::Asof)
    {
        /// Outer any/asof keep every preserved-side row (one match each).
        if (kind == JoinKind::Left || kind == JoinKind::Right)
            return preserved;
        /// Inner asof keeps one nearest match per left row, so it is bounded by the left side.
        if (strictness == JoinStrictness::Asof)
            return std::min(base, left);
        /// Inner any emits at most one row per matching key, so it cannot exceed either side.
        return std::min({base, left, right});
    }

    /// Strictness All/Unspecified: an outer join keeps every preserved-side row, so the output is at
    /// least the preserved side.
    if (kind == JoinKind::Left)
        return std::max(base, left);
    if (kind == JoinKind::Right)
        return std::max(base, right);
    return base;
}

Float64 clampJoinMaxRowCount(JoinKind kind, JoinStrictness strictness, Float64 product, Float64 left, Float64 right)
{
    /// Position-wise: exactly the shorter side.
    if (kind == JoinKind::Paste)
        return std::min(left, right);
    /// A full join can emit every row from both sides when the keys are disjoint.
    if (kind == JoinKind::Full)
        return std::max(product, left + right);

    const Float64 preserved = (kind == JoinKind::Right) ? right : left;
    const bool reduces_to_preserved =
        strictness == JoinStrictness::Semi || strictness == JoinStrictness::Anti
        || strictness == JoinStrictness::Any || strictness == JoinStrictness::RightAny
        || strictness == JoinStrictness::Asof;

    /// Semi/anti/any keep at most one row per preserved-side row (anti can keep the whole preserved
    /// side even when the other side is empty), so the preserved side is the upper bound.
    if (reduces_to_preserved && (kind == JoinKind::Left || kind == JoinKind::Right))
        return preserved;

    /// An outer join with matches can multiply up to the product but never drops below the preserved
    /// side (unmatched rows are kept).
    if (kind == JoinKind::Left)
        return std::max(product, left);
    if (kind == JoinKind::Right)
        return std::max(product, right);
    /// Inner (any strictness), cross, comma: the product is a valid upper bound.
    return product;
}

ExpressionStatistics StatisticsDerivation::deriveJoinStatistics(
    const JoinStepLogical & join_step,
    const ExpressionStatistics & left_statistics,
    const ExpressionStatistics & right_statistics)
{
    ExpressionStatistics statistics;
    statistics.min_row_count = 0;
    statistics.max_row_count = left_statistics.max_row_count * right_statistics.max_row_count;

    statistics.column_statistics.insert(left_statistics.column_statistics.begin(), left_statistics.column_statistics.end());
    statistics.column_statistics.insert(right_statistics.column_statistics.begin(), right_statistics.column_statistics.end());

    Float64 join_selectivity = 1.0;

    /// Columns already used in a predicate on each join side. A predicate that
    /// reuses a column is redundant (implied by transitivity from a child join)
    /// and should not contribute to selectivity.
    std::unordered_set<String> left_bound_columns;
    std::unordered_set<String> right_bound_columns;

    /// Equality key pairs, for the output column equivalences.
    std::vector<std::pair<String, String>> equi_pairs;

    for (const auto & predicate_expression : join_step.getJoinOperator().expression)
    {
        const auto & predicate = predicate_expression.asBinaryPredicate();
        auto left_column_actions = get<1>(predicate);
        auto right_column_actions = get<2>(predicate);

        if (get<0>(predicate) != JoinConditionOperator::Equals || !left_column_actions || !right_column_actions)
        {
            /// TODO: add support for non-equality operators
            LOG_TEST(log, "Skipping predicate '{}'", predicate_expression.dump());
            continue;
        }

        if (left_column_actions.fromRight() && right_column_actions.fromLeft())
            std::swap(left_column_actions, right_column_actions);
        const auto & left_column = left_column_actions.getColumnName();
        const auto & right_column = right_column_actions.getColumnName();

        equi_pairs.emplace_back(left_column, right_column);

        bool left_already_bound = !left_bound_columns.insert(left_column).second;
        bool right_already_bound = !right_bound_columns.insert(right_column).second;

        auto left_column_statistics = left_statistics.column_statistics.find(left_column);
        auto right_column_statistics = right_statistics.column_statistics.find(right_column);

        UInt64 left_number_of_distinct_values = 1;
        UInt64 right_number_of_distinct_values = 1;
        UInt64 min_number_of_distinct_values = UInt64(std::min(left_statistics.estimated_row_count, right_statistics.estimated_row_count));
        if (left_column_statistics != left_statistics.column_statistics.end())
        {
            left_number_of_distinct_values = left_column_statistics->second.num_distinct_values;
            min_number_of_distinct_values = std::min(min_number_of_distinct_values, left_number_of_distinct_values);
        }
        if (right_column_statistics != right_statistics.column_statistics.end())
        {
            right_number_of_distinct_values = right_column_statistics->second.num_distinct_values;
            min_number_of_distinct_values = std::min(min_number_of_distinct_values, right_number_of_distinct_values);
        }

        /// Estimate `JOIN` equality predicate selectivity as 1 / max(NDV(A), NDV(B)) based on assumption that distinct values have equal probabilities.
        /// An empty relation or a supplied hint can carry NDV = 0; clamp to 1, otherwise the division
        /// would produce an infinite selectivity that poisons every cost downstream.
        UInt64 max_number_of_distinct_values = std::max<UInt64>({left_number_of_distinct_values, right_number_of_distinct_values, 1});
        Float64 predicate_selectivity = 1.0 / Float64(max_number_of_distinct_values);

        /// NDV for join predicate columns can decrease if the other column has smaller NDV
        statistics.column_statistics[left_column].num_distinct_values = min_number_of_distinct_values;
        statistics.column_statistics[right_column].num_distinct_values = min_number_of_distinct_values;

        /// Predicate reuses a column already seen on one side - redundant for selectivity.
        if (left_already_bound || right_already_bound)
        {
            LOG_TEST(log, "Predicate '{} = {}' is redundant (column already bound), skipping for selectivity",
                left_column, right_column);
            continue;
        }

        LOG_TEST(log, "Predicate '{} = {}' selectivity: 1 / {}",
            left_column, right_column, 1.0 / predicate_selectivity);

        /// Multiply selectivities of predicates assuming they are independent
        join_selectivity *= predicate_selectivity;
    }

    statistics.estimated_row_count = left_statistics.estimated_row_count * right_statistics.estimated_row_count * join_selectivity;

    /// Use the join order optimizer's cardinality as a lower bound - it handles
    /// correlated predicates (e.g. composite FK joins) better than multiplicative independence.
    if (auto hint = join_step.getResultRowsEstimation())
    {
        if (Float64(*hint) > statistics.estimated_row_count)
        {
            LOG_TEST(log, "Using join order optimizer hint: {} rows (multiplicative estimate was {})",
                *hint, statistics.estimated_row_count);
            statistics.estimated_row_count = Float64(*hint);
        }
    }

    /// Constrain the inner-product estimate to the join semantics (outer joins keep the preserved side,
    /// semi/anti/any bound it). Applied after the join-order hint so a hint cannot exceed a semantic
    /// upper bound (e.g. a semi join above its preserved-side row count).
    const auto & join_operator = join_step.getJoinOperator();
    statistics.estimated_row_count = clampJoinRowCount(join_operator.kind, join_operator.strictness,
        statistics.estimated_row_count, left_statistics.estimated_row_count, right_statistics.estimated_row_count);
    statistics.max_row_count = clampJoinMaxRowCount(join_operator.kind, join_operator.strictness,
        statistics.max_row_count, left_statistics.max_row_count, right_statistics.max_row_count);

    /// Column equivalences: both inputs' classes survive (the sides do not share column names).
    /// An inner join also makes its equality keys equal on every output row, so each key pair
    /// links the two classes; other kinds keep unmatched rows, where the equality does not hold.
    statistics.equivalences = left_statistics.equivalences;
    statistics.equivalences.merge(right_statistics.equivalences);
    if (join_operator.kind == JoinKind::Inner)
        for (const auto & [left_column, right_column] : equi_pairs)
            statistics.equivalences.add(left_column, right_column);

    /// Width comes from the actual join output columns; summing both inputs double-counts join keys and
    /// can include columns the join does not emit. Use the inputs' known column sizes where available:
    /// with type defaults alone a short `String` would count as 64 bytes, and a cheap post-join shuffle
    /// could look costlier than shuffling the whole pre-join input.
    statistics.estimated_bytes_per_row = estimateRowWidth(*join_step.getOutputHeader(), statistics.column_statistics);

    for (auto & column_statistics : statistics.column_statistics)
        if (Float64(column_statistics.second.num_distinct_values) > statistics.estimated_row_count)
            column_statistics.second.num_distinct_values = UInt64(statistics.estimated_row_count);

    if (statistics.estimated_row_count < 0.01)
    {
        LOG_TEST(log, "Possibly incorrect estimation result: {}\nleft stats: {}\nright stats: {}\njoin_selectivity: {}",
            statistics.dump(), left_statistics.dump(), right_statistics.dump(), join_selectivity);
    }

    return statistics;
}

ExpressionStatistics StatisticsDerivation::deriveReadStatistics(const ReadFromMergeTree & read_step)
{
    ExpressionStatistics statistics;
    const auto & table_name = read_step.getStorageID().getTableName();

    statistics.min_row_count = 0;
    statistics.max_row_count = Float64(read_step.getStorageSnapshot()->storage.totalRows(read_step.getContext()).value_or(std::numeric_limits<UInt64>::max()));

    ReadFromMergeTree::AnalysisResultPtr analyzed_result = read_step.getAnalyzedResult();
    analyzed_result = analyzed_result ? analyzed_result : read_step.selectRangesToRead();
    if (analyzed_result)
    {
        statistics.estimated_row_count = Float64(analyzed_result->selected_rows);
        statistics.max_row_count = Float64(analyzed_result->selected_rows);
    }
    else
        statistics.estimated_row_count = 1000000;

    const Float64 physical_selected_rows = analyzed_result ? Float64(analyzed_result->selected_rows) : 0;

    if (read_step.getContext()->getSettingsRef()[Setting::allow_statistics_optimize])
    {
        /// TODO: Move this to IOptimizerStatistics implementation
        if (auto estimator = read_step.getConditionSelectivityEstimator(read_step.getAllColumnNames()))
        {
            auto prewhere_info = read_step.getPrewhereInfo();
            const ActionsDAG::Node * prewhere_node = prewhere_info
                ? static_cast<const ActionsDAG::Node *>(prewhere_info->prewhere_actions.tryFindInOutputs(prewhere_info->prewhere_column_name))
                : nullptr;
            auto relation_profile = estimator->estimateRelationProfile(nullptr, nullptr, prewhere_node);

            /// Index analysis already bounds the read: it cannot emit more than `selected_rows`.
            /// Without a `PREWHERE` the profile carries no filter, its row count is only the
            /// statistics' total, so the index-analysis estimate stays.
            if (prewhere_node)
                statistics.estimated_row_count = analyzed_result
                    ? std::min(Float64(relation_profile.rows), Float64(analyzed_result->selected_rows))
                    : Float64(relation_profile.rows);
            for (const auto & [column_name, column_stats] : relation_profile.column_stats)
                statistics.column_statistics[column_name].num_distinct_values = column_stats.num_distinct_values;
            /// The profile carries no byte sizes; leaving the default 1 byte per row would make wide
            /// tables look nearly free to move over the network.
            fillReadColumnWidths(statistics, read_step, table_name);
            statistics.estimated_bytes_per_row = estimateReadBytesPerRow(read_step, statistics);
            fillPhysicalReadBytes(statistics, physical_selected_rows);

            LOG_TEST(log, "Estimate statistics for table {}: {}", table_name, statistics.dump());
            return statistics;
        }
    }

    for (const auto & column_name : read_step.getAllColumnNames())
    {
        auto column_ndv = statistics_lookup.getNumberOfDistinctValues(table_name, column_name);
        if (column_ndv)
            statistics.column_statistics[column_name].num_distinct_values = column_ndv.value();
    }

    auto cardinality_hint = statistics_lookup.getCardinality(table_name);
    if (cardinality_hint)
        statistics.estimated_row_count = std::min<Float64>(statistics.estimated_row_count, Float64(*cardinality_hint));

    fillReadColumnWidths(statistics, read_step, table_name);
    statistics.estimated_bytes_per_row = estimateReadBytesPerRow(read_step, statistics);
    fillPhysicalReadBytes(statistics, physical_selected_rows);

    return statistics;
}

void StatisticsDerivation::fillReadColumnWidths(ExpressionStatistics & statistics, const ReadFromMergeTree & read_step, const String & table_name)
{
    /// A table-level width hint marks the parts as stand-ins, so it beats their real sizes.
    auto avg_row_bytes_hint = statistics_lookup.getAvgRowBytes(table_name);
    auto storage_widths = avg_row_bytes_hint
        ? estimateReadColumnWidthsScaledToRow(read_step, *avg_row_bytes_hint)
        : estimateReadColumnWidths(read_step);
    for (const auto & [column_name, width] : storage_widths)
        statistics.column_statistics[column_name].avg_bytes = width;

    /// A per-column hint overrides the derived width.
    for (const auto & column_name : read_step.getAllColumnNames())
    {
        auto hint = statistics_lookup.getAvgColumnBytes(table_name, column_name);
        if (hint)
            statistics.column_statistics[column_name].avg_bytes = *hint;
    }
}

namespace QueryPlanOptimizations
{
void remapColumnStats(std::unordered_map<String, ColumnStats> & mapped, const ActionsDAG & actions);
}

/// Output names that carry an input column through unchanged: `INPUT`/`ALIAS` chains only.
/// Value equality between renamed columns survives; a computed expression changes the values,
/// so it must not keep an equivalence.
static std::unordered_map<String, Names> identityOutputNames(const ActionsDAG & actions)
{
    std::unordered_map<String, Names> input_to_outputs;
    for (const auto * output : actions.getOutputs())
    {
        const auto * node = output;
        while (node->type == ActionsDAG::ActionType::ALIAS)
            node = node->children.front();
        if (node->type == ActionsDAG::ActionType::INPUT)
            input_to_outputs[node->result_name].push_back(output->result_name);
    }
    return input_to_outputs;
}

static EquivalenceClasses<String> remapEquivalences(
    const EquivalenceClasses<String> & equivalences, const ActionsDAG & actions)
{
    auto input_to_outputs = identityOutputNames(actions);
    EquivalenceClasses<String> result;
    std::unordered_set<const void *> visited_classes;
    for (const auto & [member, class_ptr] : equivalences.getMemberToClassMap())
    {
        if (!class_ptr || !visited_classes.insert(class_ptr.get()).second)
            continue;
        Names renamed;
        for (const auto & class_member : *class_ptr)
        {
            auto it = input_to_outputs.find(class_member);
            if (it != input_to_outputs.end())
                renamed.insert(renamed.end(), it->second.begin(), it->second.end());
        }
        /// A class with fewer than two surviving members says nothing, and `add` skips it.
        for (size_t i = 1; i < renamed.size(); ++i)
            result.add(renamed[0], renamed[i]);
    }
    return result;
}

namespace
{

const ActionsDAG::Node * skipAliases(const ActionsDAG::Node * node)
{
    while (node->type == ActionsDAG::ActionType::ALIAS)
        node = node->children.front();
    return node;
}

}

/// The TRUE fraction of a filter expression, estimated from the input column NDVs and
/// equivalence classes. The input of a standalone filter is an arbitrary subplan (e.g. an
/// aggregation for `HAVING`), so there are no table statistics here, and the column statistics
/// carry no value ranges: an equality uses 1/NDV, everything else uses the default factors.
Float64 estimatePredicateSelectivity(const ActionsDAG::Node * node, const ExpressionStatistics & input_statistics)
{
    node = skipAliases(node);
    /// A constant filter column: the planner removes an always-false filter, so assume true.
    if (node->type == ActionsDAG::ActionType::COLUMN)
        return 1.0;
    if (node->type != ActionsDAG::ActionType::FUNCTION)
        return CascadesDefaults::DEFAULT_UNKNOWN_SELECTIVITY;

    const String & name = node->function_base->getName();

    if (name == "and")
    {
        Float64 selectivity = 1.0;
        for (const auto * child : node->children)
            selectivity *= estimatePredicateSelectivity(child, input_statistics);
        return selectivity;
    }
    if (name == "or")
    {
        Float64 none_passes = 1.0;
        for (const auto * child : node->children)
            none_passes *= 1.0 - estimatePredicateSelectivity(child, input_statistics);
        return 1.0 - none_passes;
    }
    if (name == "not" && node->children.size() == 1)
        return 1.0 - estimatePredicateSelectivity(node->children.front(), input_statistics);

    /// A runtime join filter repeats the join selectivity, which the join estimate carries.
    if (name == "__applyFilter")
        return 1.0;
    if (name == "like" || name == "ilike")
        return CascadesDefaults::DEFAULT_LIKE_SELECTIVITY;
    if (name == "notLike" || name == "notILike")
        return 1.0 - CascadesDefaults::DEFAULT_LIKE_SELECTIVITY;
    if (name == "isNull")
        return CascadesDefaults::DEFAULT_EQUALITY_SELECTIVITY;
    if (name == "isNotNull")
        return 1.0 - CascadesDefaults::DEFAULT_EQUALITY_SELECTIVITY;

    const bool is_equals = name == "equals";
    const bool is_not_equals = name == "notEquals";
    const bool is_range = name == "less" || name == "greater" || name == "lessOrEquals" || name == "greaterOrEquals";
    if ((is_equals || is_not_equals || is_range) && node->children.size() == 2)
    {
        if (is_range)
            return CascadesDefaults::DEFAULT_RANGE_SELECTIVITY;

        const auto * left = skipAliases(node->children[0]);
        const auto * right = skipAliases(node->children[1]);
        const bool left_is_constant = left->column && isColumnConst(*left->column);
        const bool right_is_constant = right->column && isColumnConst(*right->column);

        auto column_ndv = [&](const ActionsDAG::Node * side) -> UInt64
        {
            auto it = input_statistics.column_statistics.find(side->result_name);
            return it != input_statistics.column_statistics.end() ? it->second.num_distinct_values : 0;
        };

        Float64 equal_selectivity = CascadesDefaults::DEFAULT_EQUALITY_SELECTIVITY;
        if (!left_is_constant && !right_is_constant)
        {
            /// Two columns. An equality the plan below already enforces (e.g. the keys of an
            /// inner join under this filter) holds on every row; otherwise the join-equality
            /// formula 1 / max(NDV) applies.
            auto left_class = input_statistics.equivalences.getClass(left->result_name);
            if (left_class && left_class == input_statistics.equivalences.getClass(right->result_name))
                equal_selectivity = 1.0;
            else if (UInt64 max_ndv = std::max(column_ndv(left), column_ndv(right)))
                equal_selectivity = 1.0 / Float64(max_ndv);
        }
        else
        {
            const auto * column_side = left_is_constant ? right : left;
            if (UInt64 ndv = column_ndv(column_side))
                equal_selectivity = 1.0 / Float64(ndv);
        }
        return is_equals ? equal_selectivity : 1.0 - equal_selectivity;
    }

    return CascadesDefaults::DEFAULT_UNKNOWN_SELECTIVITY;
}


ExpressionStatistics StatisticsDerivation::deriveFilterStatistics(const FilterStep & filter_step, const ExpressionStatistics & input_statistics)
{
    ExpressionStatistics result_statistics = input_statistics;
    QueryPlanOptimizations::remapColumnStats(result_statistics.column_statistics, filter_step.getExpression());
    result_statistics.equivalences = remapEquivalences(input_statistics.equivalences, filter_step.getExpression());

    const ActionsDAG::Node * filter_node = nullptr;
    for (const auto & dag_node : filter_step.getExpression().getNodes())
    {
        if (dag_node.result_name == filter_step.getFilterColumnName())
        {
            filter_node = &dag_node;
            break;
        }
    }

    if (filter_node)
    {
        const Float64 selectivity = estimatePredicateSelectivity(filter_node, input_statistics);
        result_statistics.estimated_row_count *= selectivity;
        result_statistics.min_row_count = 0;
        /// A column cannot have more distinct values than there are rows.
        for (auto & [column_name, column_stats] : result_statistics.column_statistics)
            if (Float64(column_stats.num_distinct_values) > result_statistics.estimated_row_count)
                column_stats.num_distinct_values = UInt64(result_statistics.estimated_row_count);
        LOG_TEST(getLogger("StatisticsDerivation"), "Filter '{}' selectivity: {}", filter_step.getFilterColumnName(), selectivity);
    }

    return result_statistics;
}

ExpressionStatistics StatisticsDerivation::deriveExpressionStatistics(const ExpressionStep & expression_step, const ExpressionStatistics & input_statistics)
{
    ExpressionStatistics result_statistics = input_statistics;
    QueryPlanOptimizations::remapColumnStats(result_statistics.column_statistics, expression_step.getExpression());
    result_statistics.equivalences = remapEquivalences(input_statistics.equivalences, expression_step.getExpression());
    /// Keep the input row width: most projections pass columns through, and the input width may carry
    /// storage-derived or hinted byte sizes that a header-based type-default estimate would discard.
    /// TODO: recompute only for added/dropped columns (needs per-column widths); an arrayJoin also
    /// grows the row count (preserves_number_of_rows is false), which we do not estimate yet.
    return result_statistics;
}

/// Fraction of the input rows assumed distinct for a key column without statistics.
static constexpr Float64 DEFAULT_DISTINCT_VALUES_RATIO = 0.1;

/// NDV of a group key in the input; without stats, fall back to `DEFAULT_DISTINCT_VALUES_RATIO`.
static Float64 keyDistinctValues(const String & column, const ExpressionStatistics & input_statistics)
{
    auto column_stats = input_statistics.column_statistics.find(column);
    if (column_stats != input_statistics.column_statistics.end())
        return std::min(Float64(column_stats->second.num_distinct_values), input_statistics.max_row_count);
    return DEFAULT_DISTINCT_VALUES_RATIO * input_statistics.estimated_row_count;
}

/// Estimated and maximum count of distinct value combinations of the given columns: the
/// estimate takes the largest single-column NDV, the maximum takes the product. This is the
/// output row count of an aggregation on the columns and of a `DISTINCT` over them.
static std::pair<Float64, Float64> estimateGroupCount(const Names & columns, const ExpressionStatistics & input_statistics)
{
    Float64 largest_ndv = 1;
    Float64 ndv_product = 1;
    for (const auto & column : columns)
    {
        Float64 ndv = keyDistinctValues(column, input_statistics);
        largest_ndv = std::max(largest_ndv, ndv);
        ndv_product *= ndv;
    }
    return {std::min(largest_ndv, input_statistics.estimated_row_count),
            std::min(ndv_product, input_statistics.max_row_count)};
}

ExpressionStatistics StatisticsDerivation::deriveAggregatingStatistics(const AggregatingStep & aggregating_step, const ExpressionStatistics & input_statistics)
{
    const auto & aggregator_params = aggregating_step.getAggregatorParameters();
    ExpressionStatistics aggregation_statistics;
    for (const auto & key : aggregator_params.keys)
        aggregation_statistics.column_statistics[key].num_distinct_values
            = UInt64(keyDistinctValues(key, input_statistics));

    aggregation_statistics.min_row_count = 0;
    std::tie(aggregation_statistics.estimated_row_count, aggregation_statistics.max_row_count)
        = estimateGroupCount(aggregator_params.keys, input_statistics);
    /// Group-by keys pass through with their input value sizes.
    for (auto & [column_name, column_stats] : aggregation_statistics.column_statistics)
    {
        auto input_column_statistics = input_statistics.column_statistics.find(column_name);
        if (input_column_statistics != input_statistics.column_statistics.end())
            column_stats.avg_bytes = input_column_statistics->second.avg_bytes;
    }
    /// Aggregation changes the schema (group-by keys + aggregate states), recompute from output
    /// header with the keys' known value sizes.
    aggregation_statistics.estimated_bytes_per_row = estimateRowWidth(*aggregating_step.getOutputHeader(), aggregation_statistics.column_statistics);

    /// Group keys pass through with their values, so their equivalences survive; the column
    /// statistics at this point hold exactly the group keys.
    std::unordered_set<const void *> visited_classes;
    for (const auto & [member, class_ptr] : input_statistics.equivalences.getMemberToClassMap())
    {
        if (!class_ptr || !visited_classes.insert(class_ptr.get()).second)
            continue;
        Names kept;
        for (const auto & class_member : *class_ptr)
            if (aggregation_statistics.column_statistics.contains(class_member))
                kept.push_back(class_member);
        for (size_t i = 1; i < kept.size(); ++i)
            aggregation_statistics.equivalences.add(kept[0], kept[i]);
    }

    return aggregation_statistics;
}

static void trimStatisticsByLimit(ExpressionStatistics & statistics, UInt64 limit)
{
    statistics.estimated_row_count = std::min(statistics.estimated_row_count, Float64(limit));
    statistics.max_row_count = std::min(statistics.max_row_count, Float64(limit));
    for (auto & column_statistics : statistics.column_statistics)
        if (Float64(column_statistics.second.num_distinct_values) > statistics.estimated_row_count)
            column_statistics.second.num_distinct_values = UInt64(statistics.estimated_row_count);
}

ExpressionStatistics StatisticsDerivation::deriveSortingStatistics(const SortingStep & sorting_step, const ExpressionStatistics & input_statistics)
{
    ExpressionStatistics result_statistics = input_statistics;
    /// If there is no LIMIT, then sorting does not change statistics
    if (sorting_step.getLimit())
    {
        trimStatisticsByLimit(result_statistics, sorting_step.getLimit());
    }
    return result_statistics;
}

ExpressionStatistics StatisticsDerivation::deriveLimitStatistics(const LimitStep & limit_step, const ExpressionStatistics & input_statistics)
{
    ExpressionStatistics result_statistics = input_statistics;
    /// Without a `LIMIT` value the step does not change statistics
    if (limit_step.getLimit())
    {
        trimStatisticsByLimit(result_statistics, limit_step.getLimit());
    }
    return result_statistics;
}

ExpressionStatistics StatisticsDerivation::deriveDistinctStatistics(const DistinctStep & distinct_step, const ExpressionStatistics & input_statistics)
{
    /// One output row per distinct value combination.
    ExpressionStatistics result = input_statistics;
    std::tie(result.estimated_row_count, result.max_row_count)
        = estimateGroupCount(distinct_step.getColumnNames(), input_statistics);
    result.estimated_row_count = std::min(result.estimated_row_count, input_statistics.estimated_distinct_bound);
    result.min_row_count = input_statistics.min_row_count > 0 ? 1 : 0;
    /// Every output row is distinct.
    result.estimated_distinct_bound = result.estimated_row_count;
    /// Without the clamp the row-count reduction could leave a column NDV above the row count.
    for (auto & [column_name, column_stats] : result.column_statistics)
        column_stats.num_distinct_values = std::min(column_stats.num_distinct_values,
            static_cast<UInt64>(std::max(result.estimated_row_count, 1.0)));
    return result;
}

Float64 StatisticsDerivation::estimateReadBytesPerRow(const ReadFromMergeTree & read_step, const ExpressionStatistics & statistics)
{
    /// Priority: table-level hint > per-column widths (hinted or storage-derived) > type-based estimate
    auto avg_row_bytes_hint = statistics_lookup.getAvgRowBytes(read_step.getStorageID().getTableName());
    if (avg_row_bytes_hint)
        return *avg_row_bytes_hint;

    return estimateRowWidth(*read_step.getOutputHeader(), statistics.column_statistics);
}

}
