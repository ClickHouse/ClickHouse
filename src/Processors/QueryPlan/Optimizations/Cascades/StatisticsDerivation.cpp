#include <Columns/ColumnConst.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Processors/QueryPlan/IntersectOrExceptStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Group.h>
#include <Processors/QueryPlan/Optimizations/RelationStatisticsEstimator.h>
#include <Processors/QueryPlan/Optimizations/Cascades/GroupExpression.h>
#include <Processors/QueryPlan/Optimizations/Cascades/Memo.h>
#include <Processors/QueryPlan/Optimizations/Cascades/OptimizerDefaults.h>
#include <Processors/QueryPlan/Optimizations/Cascades/StatisticsDerivation.h>
#include <Processors/QueryPlan/Optimizations/RelationStatistics.h>
#include <Processors/QueryPlan/RelationEstimateInfo.h>
#include <Processors/QueryPlan/Optimizations/joinOrderCommon.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Storages/IStorage.h>
#include <Storages/Statistics/ConditionSelectivityEstimator.h>
#include <base/types.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>

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

/// Drops the distinct-value caps and the uniqueness of a result whose values the derivation does not follow.
/// For a step that can add values to its input columns: no limit, value range or NULL fraction of the input holds.
static void clearValueBounds(ExpressionStatistics & statistics)
{
    for (auto & [_, column_stats] : statistics.column_statistics)
    {
        column_stats.max_distinct_values = 0;
        column_stats.unique = false;
        column_stats.clearValueRange();
        column_stats.null_fraction.reset();
    }
}

void StatisticsDerivation::deriveStatistics(GroupId group_id)
{
    /// The leaves first, so that the search value of a source without statistics, the largest known
    /// leaf, does not depend on the order the groups are derived in.
    if (!leaves_derived)
    {
        leaves_derived = true;
        deriveLeafGroups();
    }
    deriveGroupStatistics(group_id);
}

void StatisticsDerivation::deriveLeafGroups()
{
    const auto is_leaf = [](const GroupPtr & group)
    {
        return !group->logical_expressions.empty() && group->logical_expressions.front()->inputs.empty();
    };
    for (GroupId group_id = 0; group_id < memo.getGroupCount(); ++group_id)
    {
        const auto group = memo.getGroup(group_id);
        if (!group->statistics && is_leaf(group))
            deriveGroupStatistics(group_id);
    }
    /// A source without an estimate and without a bound takes the largest known leaf as its search
    /// value, now that every leaf is derived.
    const auto largest = largestKnownLeafRowCount();
    if (!largest)
        return;
    memo.setLargestKnownLeafRows(*largest);
    for (GroupId group_id = 0; group_id < memo.getGroupCount(); ++group_id)
    {
        const auto group = memo.getGroup(group_id);
        if (is_leaf(group) && group->statistics && group->statistics->rows_unknown
            && group->statistics->max_row_count >= Float64(std::numeric_limits<UInt64>::max()))
            group->statistics->estimated_row_count = *largest;
    }
}

void StatisticsDerivation::deriveGroupStatistics(GroupId group_id)
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
            deriveGroupStatistics(input.group_id);
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
    else if (const auto * distinct_step = typeid_cast<const DistinctStep *>(plan_step))
    {
        /// A preliminary `DISTINCT` gets the rows of a full one, as if over one stream. Over several nodes it
        /// can emit each value once per node; its physical expression records that (see `CostEstimator`),
        /// and the exchange above it is priced on those rows. The final `DISTINCT` over it keeps those rows:
        /// its input is already distinct, and without that a key without an NDV would take the default
        /// reduction twice.
        if (isOverPreliminaryDistinct(*distinct_step, *memo.getGroup(expression->inputs.at(0).group_id)))
            group->statistics = input_statistics(0);
        else
            group->statistics = deriveDistinctStatistics(*distinct_step, input_statistics(0));
    }
    else if (const auto * union_step = typeid_cast<const UnionStep *>(plan_step))
    {
        group->statistics = deriveUnionStatistics(*union_step, [&](size_t index) -> const ExpressionStatistics & { return input_statistics(index); }, expression->inputs.size());
    }
    else if (const auto * intersect_or_except_step = typeid_cast<const IntersectOrExceptStep *>(plan_step))
    {
        /// The output reuses the first input's header and so its column statistics. Only
        /// `INTERSECT ALL` is bounded by the smallest input; the `DISTINCT` variants keep every
        /// matching duplicate of the first input at this step (a separate `Distinct` above
        /// deduplicates), and `EXCEPT` keeps at most the first input.
        ExpressionStatistics result = input_statistics(0);
        for (size_t input_index = 1; input_index < expression->inputs.size(); ++input_index)
        {
            result.rows_unknown |= input_statistics(input_index).rows_unknown;
            result.addImprecisionOf(input_statistics(input_index));
        }
        result.source = RowEstimateSource::NoSource;
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
                /// The distinct output rows cannot exceed any input, with or without NDVs. An unknown
                /// input offers only its row bound: its row count and distinct bound are search values.
                if (other.rows_unknown)
                    result.estimated_distinct_bound = std::min(result.estimated_distinct_bound, other.max_row_count);
                else
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
                    output_column->second.rows_without_statistics
                        = std::max(output_column->second.rows_without_statistics, other_column->second.rows_without_statistics);
                    /// Any input's proven limit holds for the output.
                    if (const UInt64 other_max = other_column->second.max_distinct_values)
                    {
                        auto & output_max = output_column->second.max_distinct_values;
                        output_max = output_max == 0 ? other_max : std::min(output_max, other_max);
                    }
                }
            }
            /// Without the clamp a row-count reduction could leave a column NDV above the row count.
            for (auto & [column_name, column_stats] : result.column_statistics)
                column_stats.num_distinct_values = std::min(column_stats.num_distinct_values,
                    roundToRowCount(std::max(result.estimated_row_count, 1.0)));
        }
        result.min_row_count = 0;
        group->statistics = std::move(result);
    }
    else if (typeid_cast<const ArrayJoinStep *>(plan_step))
    {
        /// Rows multiply by the array sizes, which nothing here estimates: unknown, without a bound.
        ExpressionStatistics result = input_statistics(0);
        result.rows_unknown = true;
        result.min_row_count = 0;
        result.max_row_count = Float64(std::numeric_limits<UInt64>::max());
        /// The array columns become their elements, which the caps of the arrays do not bound.
        clearValueBounds(result);
        group->statistics = std::move(result);
    }
    else if (!expression->inputs.empty())
    {
        /// By default take statistics from the first input; a result derived from an input without an
        /// estimate has none either.
        group->statistics = input_statistics(0);
        /// A step not derived here can add values (`ROLLUP` and `CUBE` fill keys with defaults).
        clearValueBounds(*group->statistics);
        for (size_t input_index = 1; input_index < expression->inputs.size(); ++input_index)
        {
            group->statistics->rows_unknown |= input_statistics(input_index).rows_unknown;
            group->statistics->addImprecisionOf(input_statistics(input_index));
        }
        if (expression->inputs.size() > 1)
            group->statistics->source = RowEstimateSource::NoSource;
    }
    else
    {
        /// A source the estimator does not know: no estimate, and no bound. The search value is
        /// the largest known leaf of the query, so the unknown source is never costed as empty.
        ExpressionStatistics unknown;
        unknown.rows_unknown = true;
        unknown.estimated_row_count = largestKnownLeafRowCount().value_or(CascadesDefaults::DEFAULT_UNKNOWN_READ_ROWS);
        group->statistics = std::move(unknown);
    }

    LOG_TEST(log, "Derived statistics for group #{}:\n{}",
        group_id, group->statistics->dump());
}

std::optional<Float64> StatisticsDerivation::largestKnownLeafRowCount() const
{
    std::optional<Float64> largest;
    for (GroupId group_id = 0; group_id < memo.getGroupCount(); ++group_id)
    {
        const auto group = memo.getGroup(group_id);
        if (!group->statistics || group->statistics->rows_unknown || group->logical_expressions.empty()
            || !group->logical_expressions.front()->inputs.empty())
            continue;
        if (!largest || group->statistics->estimated_row_count > *largest)
            largest = group->statistics->estimated_row_count;
    }
    return largest;
}


ExpressionStatistics StatisticsDerivation::deriveJoinStatistics(
    const JoinStepLogical & join_step,
    const ExpressionStatistics & left_statistics,
    const ExpressionStatistics & right_statistics)
{
    const auto & join_operator = join_step.getJoinOperator();

    /// The shared estimate of every planner, from the inputs as relations: an unknown side stays
    /// unknown, an empty side decides, the key NDVs of the output narrow by the join.
    const auto join = estimateJoin(
        left_statistics.toRelationStats(), right_statistics.toRelationStats(), join_operator, join_step.probeKeysWithinPreservedSide());

    ExpressionStatistics statistics;
    statistics.min_row_count = 0;
    statistics.addImprecisionOf(left_statistics);
    statistics.addImprecisionOf(right_statistics);
    statistics.estimate_from_defaults |= join.estimate_from_defaults;
    statistics.column_statistics = join.column_stats;
    statistics.max_row_count = join.max_rows ? Float64(*join.max_rows) : Float64(std::numeric_limits<UInt64>::max());

    /// The join order optimizer's estimate wins when it built this join from the same inputs. An
    /// unknown result keeps a search value: the join order optimizer's bound, else the product.
    const Float64 join_selectivity = join.selectivity;
    const auto join_order_estimate = join_step.isEstimatedByJoinOrder() ? join_step.getResultRowsEstimation() : std::nullopt;
    if (join_order_estimate)
    {
        /// It keeps its own imprecision: the join order optimizer may have used the rows of a past execution or
        /// defaults that the inputs derived here do not have.
        statistics.estimated_row_count = Float64(*join_order_estimate);
        statistics.imprecise_estimate |= join_step.hasImpreciseEstimate();
        statistics.estimate_from_defaults |= join_step.isEstimateFromDefaults();
    }
    else if (join.rows)
        statistics.estimated_row_count = Float64(*join.rows);
    else
    {
        statistics.rows_unknown = true;
        const auto upper_bound = join_step.isEstimatedByJoinOrder() ? join_step.getResultRowsUpperBound() : std::nullopt;
        statistics.estimated_row_count = upper_bound
            ? Float64(*upper_bound)
            : left_statistics.estimated_row_count * right_statistics.estimated_row_count * join_selectivity;
        statistics.search_value_is_join_product = !upper_bound;
    }

    /// Equality key pairs, for the output column equivalences.
    std::vector<std::pair<String, String>> equi_pairs;
    for (const auto & predicate_expression : join_operator.expression)
    {
        auto [op, left_column_actions, right_column_actions] = predicate_expression.asBinaryPredicate();
        if (op != JoinConditionOperator::Equals || !left_column_actions || !right_column_actions)
            continue;
        if (left_column_actions.fromRight() && right_column_actions.fromLeft())
            std::swap(left_column_actions, right_column_actions);
        equi_pairs.emplace_back(left_column_actions.getColumnName(), right_column_actions.getColumnName());
    }

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
            column_statistics.second.num_distinct_values = roundToRowCount(statistics.estimated_row_count);

    if (statistics.estimated_row_count < 0.01)
    {
        LOG_TEST(log, "Possibly incorrect estimation result: {}\nleft stats: {}\nright stats: {}\njoin_selectivity: {}",
            statistics.dump(), left_statistics.dump(), right_statistics.dump(), join_selectivity);
    }

    return statistics;
}

/// A `PREWHERE` that keeps only listed constants proves the values of its columns, whatever estimated the rows.
static void capReadDistinctValues(const ReadFromMergeTree & read_step, ExpressionStatistics & statistics)
{
    if (const auto & prewhere_info = read_step.getPrewhereInfo())
        QueryPlanOptimizations::capDistinctValuesByFilter(
            prewhere_info->prewhere_actions.tryFindInOutputs(prewhere_info->prewhere_column_name), statistics.column_statistics,
            &prewhere_info->prewhere_actions.getOutputs());
}

ExpressionStatistics StatisticsDerivation::deriveReadStatistics(const ReadFromMergeTree & read_step)
{
    ExpressionStatistics statistics;
    const auto & table_name = read_step.getStorageID().getTableName();

    statistics.min_row_count = 0;
    statistics.max_row_count = Float64(read_step.getStorageSnapshot()->storage.totalRows(read_step.getContext()).value_or(std::numeric_limits<UInt64>::max()));

    const ReadFromMergeTree::AnalysisResultPtr analyzed_result = read_step.getAnalyzedResultForEstimation();
    if (analyzed_result)
    {
        statistics.estimated_row_count = Float64(analyzed_result->selected_rows);
        statistics.max_row_count = Float64(analyzed_result->selected_rows);
        statistics.source = RowEstimateSource::PrimaryIndex;
    }
    else
    {
        statistics.source = RowEstimateSource::NoStatistics;
        /// Nothing was analyzed, so there is no estimate. The table size bounds the read when the
        /// storage knows it; otherwise the largest known leaf of the query is the search value.
        statistics.rows_unknown = true;
        statistics.estimated_row_count = statistics.max_row_count < Float64(std::numeric_limits<UInt64>::max())
            ? statistics.max_row_count
            : largestKnownLeafRowCount().value_or(CascadesDefaults::DEFAULT_UNKNOWN_READ_ROWS);
    }

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
            auto relation_profile = QueryPlanOptimizations::estimateReadProfile(
                read_step, *estimator, /*metadata*/ nullptr, analyzed_result.get(), /*filter*/ nullptr, prewhere_node);

            /// Index analysis already bounds the read: it cannot emit more than `selected_rows`.
            /// Without a `PREWHERE` the profile carries no filter, its row count is only the
            /// statistics' total, so the index-analysis estimate stays.
            if (prewhere_node)
                statistics.estimated_row_count = analyzed_result
                    ? std::min(Float64(relation_profile.rows), Float64(analyzed_result->selected_rows))
                    : Float64(relation_profile.rows);
            /// A default in the profile makes the rows a guess, which the replication gate must not take as proven.
            statistics.estimate_from_defaults |= relation_profile.estimate_from_defaults;
            statistics.imprecise_estimate |= estimator->recordRowsWithoutStatistics(relation_profile);
            statistics.source = RowEstimateSource::Statistics;
            for (const auto & [column_name, column_stats] : relation_profile.column_stats)
            {
                auto & output_stats = statistics.column_statistics[column_name];
                output_stats.num_distinct_values = column_stats.num_distinct_values;
                output_stats.max_distinct_values = column_stats.max_distinct_values;
                output_stats.table_rows = column_stats.table_rows;
                output_stats.table_selected_rows = column_stats.table_selected_rows;
                output_stats.rows_without_statistics = column_stats.rows_without_statistics;
            }
            /// The profile carries no byte sizes; leaving the default 1 byte per row would make wide
            /// tables look nearly free to move over the network.
            fillReadColumnWidths(statistics, read_step, table_name);
            statistics.estimated_bytes_per_row = estimateReadBytesPerRow(read_step, statistics);
            fillPhysicalReadBytes(statistics, physical_selected_rows);

            LOG_TEST(log, "Estimate statistics for table {}: {}", table_name, statistics.dump());
            capReadDistinctValues(read_step, statistics);
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
    {
        statistics.estimated_row_count = std::min<Float64>(statistics.estimated_row_count, Float64(*cardinality_hint));
        statistics.rows_unknown = false;
        statistics.source = RowEstimateSource::Hint;
    }

    fillReadColumnWidths(statistics, read_step, table_name);
    statistics.estimated_bytes_per_row = estimateReadBytesPerRow(read_step, statistics);
    fillPhysicalReadBytes(statistics, physical_selected_rows);

    capReadDistinctValues(read_step, statistics);
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

Float64 estimatePredicateSelectivity(const ActionsDAG::Node * node, const ExpressionStatistics & input_statistics, bool & used_default)
{
    const auto are_equal = [&](const String & left, const String & right)
    {
        auto left_class = input_statistics.equivalences.getClass(left);
        return left_class && left_class == input_statistics.equivalences.getClass(right);
    };
    return QueryPlanOptimizations::estimatePredicateSelectivity(node, input_statistics.column_statistics, are_equal, used_default);
}

/// The rows without statistics of the columns that `predicate` reads, together: different columns can lack statistics
/// in different parts.
static UInt64 rowsWithoutStatistics(const ActionsDAG::Node & predicate, const ExpressionStatistics & input_statistics)
{
    UInt64 result = 0;
    std::unordered_set<const ActionsDAG::Node *> visited;
    std::vector<const ActionsDAG::Node *> stack{&predicate};
    while (!stack.empty())
    {
        const auto * node = stack.back();
        stack.pop_back();
        if (!visited.insert(node).second)
            continue;
        if (node->type == ActionsDAG::ActionType::INPUT)
        {
            if (auto it = input_statistics.column_statistics.find(node->result_name); it != input_statistics.column_statistics.end())
                result = saturatingAdd(result, it->second.rows_without_statistics);
            continue;
        }
        for (const auto * child : node->children)
            stack.push_back(child);
    }
    return result;
}

Float64 estimatePredicateSelectivity(const ActionsDAG::Node * node, const ExpressionStatistics & input_statistics)
{
    bool used_default = false;
    return estimatePredicateSelectivity(node, input_statistics, used_default);
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
        bool used_default = false;
        const Float64 selectivity = estimatePredicateSelectivity(filter_node, input_statistics, used_default);
        result_statistics.estimate_from_defaults |= used_default;
        result_statistics.estimated_row_count *= selectivity;
        /// The rows without statistics of a column the predicate reads could double the rows it keeps.
        result_statistics.imprecise_estimate |= Float64(rowsWithoutStatistics(*filter_node, input_statistics))
            >= std::max(result_statistics.estimated_row_count, 1.0);
        result_statistics.min_row_count = 0;
        /// A value survives the filter when any of its rows does.
        for (auto & [column_name, column_stats] : result_statistics.column_statistics)
            column_stats.num_distinct_values = QueryPlanOptimizations::distinctValuesAfterFilter(
                column_stats.num_distinct_values, roundToRowCount(input_statistics.estimated_row_count), roundToRowCount(result_statistics.estimated_row_count));
        QueryPlanOptimizations::capDistinctValuesByFilter(
            filter_node, result_statistics.column_statistics, &filter_step.getExpression().getOutputs());
        LOG_TEST(getLogger("StatisticsDerivation"), "Filter '{}' selectivity: {}", filter_step.getFilterColumnName(), selectivity);
    }

    return result_statistics;
}

ExpressionStatistics StatisticsDerivation::deriveExpressionStatistics(const ExpressionStep & expression_step, const ExpressionStatistics & input_statistics)
{
    ExpressionStatistics result_statistics = input_statistics;
    QueryPlanOptimizations::remapColumnStats(result_statistics.column_statistics, expression_step.getExpression());
    result_statistics.equivalences = remapEquivalences(input_statistics.equivalences, expression_step.getExpression());
    /// An `arrayJoin` multiplies the rows by the array sizes, which nothing here estimates: the
    /// result is unknown and has no bound.
    if (expression_step.getExpression().hasArrayJoin())
    {
        result_statistics.rows_unknown = true;
        result_statistics.min_row_count = 0;
        result_statistics.max_row_count = Float64(std::numeric_limits<UInt64>::max());
        for (auto & [_, column_statistics] : result_statistics.column_statistics)
            column_statistics.unique = false;
    }
    /// Keep the input row width: most projections pass columns through, and the input width may carry
    /// storage-derived or hinted byte sizes that a header-based type-default estimate would discard.
    /// TODO: recompute only for added/dropped columns (needs per-column widths).
    return result_statistics;
}

/// Fraction of the input rows taken as distinct when no key has statistics: the search value of a
/// group count the statistics do not determine.
static constexpr Float64 DEFAULT_DISTINCT_VALUES_RATIO = 0.1;

ExpressionStatistics StatisticsDerivation::deriveAggregatingStatistics(const AggregatingStep & aggregating_step, const ExpressionStatistics & input_statistics)
{
    const auto & aggregator_params = aggregating_step.getAggregatorParameters();
    const auto shared = QueryPlanOptimizations::estimateAggregatingStepStats(aggregating_step, input_statistics.toRelationStats());

    ExpressionStatistics aggregation_statistics;
    aggregation_statistics.estimate_from_defaults = shared.estimate_from_defaults;
    aggregation_statistics.imprecise_estimate = shared.imprecise_estimate;
    aggregation_statistics.source = shared.source;
    /// The keys with their NDVs, value sizes and domains.
    aggregation_statistics.column_statistics = shared.column_stats;
    aggregation_statistics.min_row_count = 0;
    /// Groups counted from the key NDVs alone do not depend on the input rows; a keyed aggregation of
    /// an unknown input is unknown all the same, a keyless one is its one row. Without a group count
    /// the search value is a fraction of the input.
    aggregation_statistics.rows_unknown = !shared.estimated_rows || (!aggregator_params.keys.empty() && input_statistics.rows_unknown);
    aggregation_statistics.estimated_row_count = shared.estimated_rows
        ? Float64(*shared.estimated_rows)
        : std::min(DEFAULT_DISTINCT_VALUES_RATIO * input_statistics.estimated_row_count, input_statistics.max_row_count);
    aggregation_statistics.max_row_count = shared.max_rows ? Float64(*shared.max_rows) : Float64(std::numeric_limits<UInt64>::max());
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
    QueryPlanOptimizations::limitColumnStats(statistics.column_statistics, roundToRowCount(statistics.estimated_row_count));
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
    /// Without a `LIMIT` value the step does not change statistics; `WITH TIES` can keep every row
    /// equal to the last one, so the limit bounds nothing then.
    if (limit_step.getLimit() && !limit_step.withTies())
        trimStatisticsByLimit(result_statistics, limit_step.getLimit());
    else if (limit_step.withTies())
        QueryPlanOptimizations::limitColumnStats(result_statistics.column_statistics, std::nullopt);
    return result_statistics;
}

ExpressionStatistics StatisticsDerivation::deriveUnionStatistics(
    const UnionStep & union_step, const std::function<const ExpressionStatistics &(size_t)> & input_statistics, size_t input_count)
{
    /// `UNION ALL`: rows and bounds add up, the widths are weighted by the rows. The output takes
    /// the first input's column names; a column's NDV is at most the sum over the inputs, unknown
    /// when an input's is. Equal values on every row of one input say nothing about the union, so
    /// no equivalence survives.
    ExpressionStatistics result = input_statistics(0);
    result.equivalences = {};
    const auto & input_headers = union_step.getInputHeaders();
    for (size_t input_index = 1; input_index < input_count; ++input_index)
    {
        const auto & other = input_statistics(input_index);
        result.estimated_bytes_per_row = QueryPlanOptimizations::unionWidth(
            result.estimated_bytes_per_row, result.estimated_row_count, other.estimated_bytes_per_row, other.estimated_row_count);
        QueryPlanOptimizations::addUnionColumnStats(
            result.column_statistics, input_headers.at(0)->getNames(), other.column_statistics, input_headers.at(input_index)->getNames(),
            roundToRowCount(result.estimated_row_count), roundToRowCount(other.estimated_row_count));
        result.rows_unknown |= other.rows_unknown;
        result.addImprecisionOf(other);
        result.estimated_row_count += other.estimated_row_count;
        result.min_row_count += other.min_row_count;
        result.max_row_count += other.max_row_count;
        result.estimated_distinct_bound += other.estimated_distinct_bound;
    }
    return result;
}

bool StatisticsDerivation::isOverPreliminaryDistinct(const DistinctStep & distinct_step, const Group & input_group)
{
    if (distinct_step.isPreliminary() || input_group.logical_expressions.empty())
        return false;
    const auto * input_step = typeid_cast<const DistinctStep *>(input_group.logical_expressions.front()->getQueryPlanStep());
    return input_step && input_step->isPreliminary() && input_step->getColumnNames() == distinct_step.getColumnNames();
}

ExpressionStatistics StatisticsDerivation::deriveDistinctStatistics(const DistinctStep & distinct_step, const ExpressionStatistics & input_statistics)
{
    /// One output row per distinct value combination.
    ExpressionStatistics result = input_statistics;
    const auto shared = QueryPlanOptimizations::estimateGroupStats(distinct_step.getColumnNames(), input_statistics.toRelationStats());
    const Float64 groups = shared.estimated_rows
        ? Float64(*shared.estimated_rows)
        : std::min(DEFAULT_DISTINCT_VALUES_RATIO * input_statistics.estimated_row_count, input_statistics.max_row_count);
    result.estimated_row_count = std::min(groups, input_statistics.estimated_distinct_bound);
    result.max_row_count = shared.max_rows ? Float64(*shared.max_rows) : input_statistics.max_row_count;
    result.imprecise_estimate |= shared.imprecise_estimate;
    /// Without a key NDV, or over an unknown input, the distinct rows are unknown, unless the input
    /// bounds its distinct rows below the search value: that bound is then the estimate.
    result.rows_unknown = (!shared.estimated_rows || input_statistics.rows_unknown) && !(input_statistics.estimated_distinct_bound < groups);
    result.min_row_count = input_statistics.min_row_count > 0 ? 1 : 0;
    /// The distinct rows are a search value of their own, not the product of the inputs of a join below.
    result.search_value_is_join_product = false;
    /// Every output row is distinct; an unknown result only proves its row bound, its row count is a
    /// search value.
    result.estimated_distinct_bound = result.rows_unknown ? result.max_row_count : result.estimated_row_count;
    /// Without the clamp the row-count reduction could leave a column NDV above the row count.
    for (auto & [column_name, column_stats] : result.column_statistics)
        column_stats.num_distinct_values = std::min(column_stats.num_distinct_values,
            roundToRowCount(std::max(result.estimated_row_count, 1.0)));
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
