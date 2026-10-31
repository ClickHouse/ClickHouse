#include <Processors/QueryPlan/Optimizations/RelationStatisticsEstimator.h>

#include <algorithm>
#include <ranges>

#include <fmt/ranges.h>

#include <Columns/ColumnConst.h>
#include <Core/Settings.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context.h>
#include <Processors/QueryPlan/AggregatingStep.h>
#include <Processors/QueryPlan/ArrayJoinStep.h>
#include <Processors/QueryPlan/CommonSubplanReferenceStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/FractionalLimitStep.h>
#include <Processors/QueryPlan/FractionalOffsetStep.h>
#include <Processors/QueryPlan/ITransformingStep.h>
#include <Processors/QueryPlan/JoinStepLogical.h>
#include <Processors/QueryPlan/LimitByStep.h>
#include <Processors/QueryPlan/LimitRangeStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/LogicalExchangeStep.h>
#include <Processors/QueryPlan/NegativeLimitByStep.h>
#include <Processors/QueryPlan/NegativeLimitStep.h>
#include <Processors/QueryPlan/NegativeOffsetStep.h>
#include <Processors/QueryPlan/OffsetStep.h>
#include <Processors/QueryPlan/Optimizations/RelationStatisticsUtils.h>
#include <Processors/QueryPlan/Optimizations/actionsDAGUtils.h>
#include <Processors/QueryPlan/ReadFromMemoryStorageStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/ReadFromObjectStorageStep.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Storages/Statistics/ConditionSelectivityEstimator.h>
#include <Common/logger_useful.h>
#include <Common/typeid_cast.h>

namespace DB
{

namespace Setting
{
extern const SettingsBool use_statistics;
extern const SettingsBool use_iceberg_manifest_statistics;
}

namespace QueryPlanOptimizations
{

namespace
{

/// The sizes of the arrays are unknown, so an array join is assumed to produce this many rows per input row.
constexpr Float64 array_join_rows_multiplier = 100;

String dumpStatsForLogs(const RelationStats & stats)
{
    return fmt::format(
        "{}: {} rows, columns: [{}]",
        stats.table_name.empty() ? "<unknown>" : stats.table_name,
        stats.estimated_rows ? toString(stats.estimated_rows.value()) : "unknown",
        fmt::join(
            stats.column_stats
                | std::views::transform([](const auto & p) { return fmt::format("{}: {}", p.first, p.second.num_distinct_values); }),
            ", "));
}

RelationStats estimateAggregatingStepStats(const AggregatingStep & aggregating_step, const RelationStats & input_stats)
{
    const auto & aggregator_params = aggregating_step.getAggregatorParameters();
    std::optional<Float64> total_number_of_distinct_values = 1;
    RelationStats aggregation_stats;
    /// Carry imprecision and source from the input, or the annotation is lost for aggregation subqueries.
    aggregation_stats.imprecise_estimate = input_stats.imprecise_estimate;
    aggregation_stats.source = input_stats.source;
    for (const auto & key : aggregator_params.keys)
    {
        auto key_stats = input_stats.column_stats.find(key);
        if (key_stats == input_stats.column_stats.end())
        {
            /// Cannot calculate total number of groups if we don't know NDV of any of the aggregation columns.
            /// The estimate then falls back to the input row count (an over-count of groups), so it is no longer
            /// precise. Flag it and surface a missing-statistics source so the EXPLAIN label and the
            /// join-reordering diagnostic reflect that the fallback was caused by missing column statistics.
            total_number_of_distinct_values.reset();
            aggregation_stats.imprecise_estimate = true;
            if (aggregation_stats.source == RowEstimateSource::Statistics || aggregation_stats.source == RowEstimateSource::NoSource)
                aggregation_stats.source = RowEstimateSource::NoStatistics;
            continue;
        }

        UInt64 key_number_of_distinct_values = key_stats->second.num_distinct_values;

        if (input_stats.estimated_rows)
            key_number_of_distinct_values = std::min(key_number_of_distinct_values, *input_stats.estimated_rows);

        aggregation_stats.column_stats[key].num_distinct_values = key_number_of_distinct_values;

        /// For now assume that aggregation columns are independent, so multiply their NDVs
        if (total_number_of_distinct_values)
            *total_number_of_distinct_values *= static_cast<Float64>(key_number_of_distinct_values);
    }

    if (total_number_of_distinct_values && input_stats.estimated_rows)
        total_number_of_distinct_values = std::min(*total_number_of_distinct_values, Float64(*input_stats.estimated_rows));
    else
        total_number_of_distinct_values = input_stats.estimated_rows;

    if (total_number_of_distinct_values)
        aggregation_stats.estimated_rows = toUInt64Saturating(*total_number_of_distinct_values);

    return aggregation_stats;
}

/// Rows dropped by a limit are not a value-uniform sample (e.g. a TopN keeps one end of the
/// sorted range), so the child's value ranges and NULL fraction do not describe the output.
/// Applied whenever a limit is present: the row estimate cannot prove the limit does not
/// truncate (e.g. a TopN read is already scaled down by its `__topKFilter` prewhere).
void clearColumnValueRanges(std::unordered_map<String, ColumnStats> & column_stats)
{
    for (auto & [_, stats] : column_stats)
    {
        stats.min_value.reset();
        stats.max_value.reset();
        stats.null_fraction.reset();
    }
}

/// A runtime filter prunes nothing at plan time, so it must not count as a filter when estimating.
/// True for a `__applyFilter` call or an always-true `x = x`, an `and` of such predicates, or an `or`
/// with at least one such operand. Aliases are looked through.
bool prunesNothingAtPlanTime(const ActionsDAG::Node * node)
{
    if (!node)
        return false;

    if (node->type == ActionsDAG::ActionType::ALIAS && !node->children.empty())
        return prunesNothingAtPlanTime(node->children.front());

    if (node->type != ActionsDAG::ActionType::FUNCTION || !node->function_base)
        return false;

    const auto & function_name = node->function_base->getName();
    if (function_name == "__applyFilter" || isAlwaysTrueEquality(*node))
        return true;

    if (node->children.empty())
        return false;

    if (function_name == "and")
        return std::ranges::all_of(node->children, prunesNothingAtPlanTime);

    /// One operand passes every row at plan time, so the whole `or` does. `addNullBypassForAntiJoin`
    /// builds this form for `LEFT ANTI JOIN`: `__applyFilter(...) OR isNull(key)`.
    if (function_name == "or")
        return std::ranges::any_of(node->children, prunesNothingAtPlanTime);

    return false;
}

}

RelationStats estimateReadRowsCount(QueryPlan::Node & node, const ActionsDAG::Node * filter, bool for_runtime_filter_transport)
{
    if (prunesNothingAtPlanTime(filter))
        filter = nullptr;

    IQueryPlanStep * step = node.step.get();
    if (const auto * reading = typeid_cast<const ReadFromMergeTree *>(step))
    {
        String table_display_name = reading->getStorageID().getTableName();

        /// Analyze partition and primary-key ranges before estimating the relation so column
        /// statistics come only from parts that can satisfy the query. Reuse the result for
        /// the index-based fallback below.
        ReadFromMergeTree::AnalysisResultPtr analyzed_result = reading->getAnalyzedResult();
        if (!analyzed_result)
        {
            /// Range analysis normally enforces throwing read limits and memoizes its result.
            /// At this stage, however, later planning may make the executed read exempt from those
            /// limits. In that case use an estimation-only analysis; execution will analyze again
            /// after its final read mode is known.
            analyzed_result = reading->hasThrowingReadRowLimit()
                ? reading->selectRangesToReadForEstimation(/*allow_query_condition_cache_=*/true)
                : reading->selectRangesToRead();
        }

        /// An exact empty range selection proves that the relation is empty. Other empty
        /// analysis results can be placeholders for deferred work, so only propagate zero
        /// when `has_exact_ranges` is set.
        if (analyzed_result && analyzed_result->has_exact_ranges && analyzed_result->selected_rows == 0)
            return RelationStats{.estimated_rows = 0, .table_name = table_display_name};

        /// `STREAM` defers range analysis until execution. Its placeholder result has zero
        /// selected rows but does not mean that the relation is empty.
        if (reading->getQueryInfo().isStream() && analyzed_result && analyzed_result->selected_rows == 0)
        {
            return RelationStats{
                .estimated_rows = {},
                .table_name = table_display_name,
                .imprecise_estimate = true,
                .source = RowEstimateSource::NoStatistics};
        }

        /// A PREWHERE that prunes nothing at plan time is likewise not a plan-time filter.
        const PrewhereInfoPtr prewhere_info = reading->getPrewhereInfo();
        const ActionsDAG::Node * prewhere_node = prewhere_info
            ? static_cast<const ActionsDAG::Node *>(prewhere_info->prewhere_actions.tryFindInOutputs(prewhere_info->prewhere_column_name))
            : nullptr;
        const bool has_prewhere_filter = prewhere_info && !prunesNothingAtPlanTime(prewhere_node);
        if (!has_prewhere_filter)
            prewhere_node = nullptr;

        bool is_filtered_by_index = false;
        if (analyzed_result)
        {
            UInt64 total_parts = 0;
            UInt64 total_granules = 0;
            for (const auto & idx_stat : analyzed_result->index_stats)
            {
                /// We expect the first element to be an index with None type, which is used to estimate the total amount of data in the table.
                /// Further index_stats are used to estimate amount of filtered data after applying the index.
                if (ReadFromMergeTree::IndexType::None == idx_stat.type)
                {
                    total_parts = idx_stat.num_parts_after;
                    total_granules = idx_stat.num_granules_after;
                    continue;
                }

                is_filtered_by_index = is_filtered_by_index || (total_parts && idx_stat.num_parts_after < total_parts)
                    || (total_granules && idx_stat.num_granules_after < total_granules);

                if (is_filtered_by_index)
                    break;
            }
        }
        const bool has_filter = filter || has_prewhere_filter;

        /// Runtime-filter transport may raise its hard exact-phase row cap to this estimate. Under an
        /// unindexed plan-time filter, column statistics still produce a number, but it is only a guess
        /// and would override the exact limit from the settings. So the transport gets no estimate and
        /// keeps the settings geometry. Join-order estimation keeps using statistics.
        if (for_runtime_filter_transport && has_filter && !is_filtered_by_index)
        {
            return RelationStats{
                .estimated_rows = {},
                .table_name = table_display_name,
                .imprecise_estimate = true,
                .source = RowEstimateSource::NoStatistics};
        }

        const bool use_statistics = reading->getContext()->getSettingsRef()[Setting::use_statistics];
        if (use_statistics)
        {
            if (auto estimator = reading->getConditionSelectivityEstimator(reading->getAllColumnNames(), analyzed_result))
            {
                auto relation_profile = estimator->estimateRelationProfile(reading->getStorageMetadata(), filter, prewhere_node);
                RelationStats stats{
                    .estimated_rows = relation_profile.rows,
                    .column_stats = relation_profile.column_stats,
                    .table_name = table_display_name,
                    .source = RowEstimateSource::Statistics};
                LOG_TRACE(getLogger("optimizeJoin"), "estimate statistics {}", dumpStatsForLogs(stats));
                return stats;
            }
        }
        if (auto stats_hint = parseTableStatsHint(reading->getContext(), table_display_name); !stats_hint.table_name.empty())
            return stats_hint;

        if (!analyzed_result)
            return RelationStats{
                .estimated_rows = {},
                .table_name = table_display_name,
                .imprecise_estimate = true,
                .source = RowEstimateSource::NoStatistics};

        /// If any conditions are pushed down to storage but not used in the index,
        /// we cannot precisely estimate the row count
        if (has_filter && !is_filtered_by_index)
            return RelationStats{
                .estimated_rows = {},
                .table_name = table_display_name,
                .imprecise_estimate = true,
                .source = RowEstimateSource::NoStatistics};

        return RelationStats{
            .estimated_rows = analyzed_result->selected_rows,
            .table_name = table_display_name,
            .imprecise_estimate = true,
            .source = RowEstimateSource::PrimaryIndex};
    }

    if (const auto * reading = typeid_cast<const ReadFromObjectStorageStep *>(step))
    {
        if (!reading->getContext()->getSettingsRef()[Setting::use_iceberg_manifest_statistics])
            return RelationStats{};

        auto estimate = reading->estimateReadFromDataLakeMetadata();
        if (!estimate)
            return RelationStats{};

        String table_display_name = reading->getStorageID().getTableName();
        RelationStats unknown{
            .estimated_rows = {},
            .table_name = table_display_name,
            .imprecise_estimate = true,
            .source = RowEstimateSource::DataLakeMetadata};
        if (!estimate->rows)
            return unknown;

        /// The step's filter also holds the `PREWHERE` and the row policy, so a read without it returns every row.
        const bool has_filter = filter || reading->getFilterActionsDAG() || reading->getPrewhereInfo() || reading->getRowLevelFilter();

        /// As for MergeTree without column statistics, a filter that pruned nothing gives no estimate.
        if (has_filter && !estimate->pruned_data_files && *estimate->rows != 0)
            return unknown;

        const bool exact = *estimate->rows == 0 || (!has_filter && !estimate->has_delete_files);
        return RelationStats{
            .estimated_rows = estimate->rows,
            .table_name = table_display_name,
            .imprecise_estimate = !exact,
            .source = RowEstimateSource::DataLakeMetadata};
    }

    if (const auto * reading = typeid_cast<const ReadFromMemoryStorageStep *>(step))
    {
        UInt64 estimated_rows = reading->getStorage()->totalRows({}).value_or(0);
        String table_display_name = reading->getStorage()->getName();
        return RelationStats{.estimated_rows = estimated_rows, .table_name = table_display_name, .source = RowEstimateSource::Statistics};
    }

    /// We cannot do typeid_cast<const ReadFromSystemOneStep *>(step)
    /// since this is defined in clickhouse_storages_system module,
    /// which is not linked to current module
    if (step->getName() == "ReadFromSystemOne")
    {
        /// system.one always produces exactly one row — used to implement constant SELECTs like `SELECT 1`.
        return RelationStats{.estimated_rows = 1, .table_name = "system.one"};
    }

    if (const auto * reading = typeid_cast<const CommonSubplanReferenceStep *>(step))
    {
        return estimateReadRowsCount(*reading->getSubplanReferenceRoot(), filter, for_runtime_filter_transport);
    }

    /// A join that has already been optimized enters its parent's join graph as a single relation,
    /// and the row count it settled on is the only estimate that relation has. Read it before the
    /// single-child check below, which a join, having two children, would never pass.
    if (const auto * join_step = typeid_cast<const JoinStepLogical *>(step); join_step && join_step->isOptimized())
    {
        /// The origin of a sub-join's estimate is not tracked (`NoSource`), so the parent graph does not
        /// re-report its tables as missing statistics; `imprecise_estimate` still records reliability.
        /// The name is already a chain of the sub-joins with their own estimates, so mark it composite
        /// to keep the parent from appending a second estimate to it.
        /// A filter above the join (`filter`, from the `FilterStep` branch) is not accounted for, so like
        /// a storage read with a filter it cannot use, the row count is then unknown.
        return RelationStats{
            .estimated_rows = filter ? std::nullopt : join_step->getResultRowsEstimation(),
            .column_stats = join_step->getResultColumnStats(),
            .table_name = join_step->getReadableRelationName(),
            .imprecise_estimate = filter != nullptr || join_step->hasImpreciseEstimate(),
            .composite = true};
    }

    if (node.children.size() != 1)
        return {};

    if (const auto * limit_step = typeid_cast<const LimitStep *>(step))
    {
        auto estimated = estimateReadRowsCount(*node.children.front(), filter, for_runtime_filter_transport);
        auto limit = limit_step->getLimit();
        if (!estimated.estimated_rows || estimated.estimated_rows > limit)
            estimated.estimated_rows = limit;
        clearColumnValueRanges(estimated.column_stats);
        return estimated;
    }

    const auto * expression_step = typeid_cast<const ExpressionStep *>(step);
    const auto * array_join_step = typeid_cast<const ArrayJoinStep *>(step);
    if (array_join_step || (expression_step && expression_step->getExpression().hasArrayJoin()))
    {
        /// A filter above the array join can read its output, which the statistics of the input do not describe.
        auto stats = estimateReadRowsCount(*node.children.front(), nullptr, for_runtime_filter_transport);
        if (stats.estimated_rows && *stats.estimated_rows != 0)
        {
            /// Such a filter, like the element filter of the step, leaves the row count unknown unless the input is empty.
            if (filter || (array_join_step && array_join_step->hasElementFilter()))
            {
                stats.estimated_rows.reset();
                stats.imprecise_estimate = true;
            }
            else
                stats.estimated_rows = toUInt64Saturating(Float64(*stats.estimated_rows) * array_join_rows_multiplier);
        }
        if (expression_step)
            remapColumnStats(stats.column_stats, expression_step->getExpression());
        else
            for (const auto & column : array_join_step->getColumns())
                stats.column_stats.erase(column);
        return stats;
    }

    if (expression_step)
    {
        auto stats = estimateReadRowsCount(*node.children.front(), filter, for_runtime_filter_transport);
        remapColumnStats(stats.column_stats, expression_step->getExpression());
        return stats;
    }

    if (const auto * filter_step = typeid_cast<const FilterStep *>(step))
    {
        const auto & dag = filter_step->getExpression();
        const auto * predicate = static_cast<const ActionsDAG::Node *>(dag.tryFindInOutputs(filter_step->getFilterColumnName()));
        auto stats = estimateReadRowsCount(*node.children.front(), predicate, for_runtime_filter_transport);
        remapColumnStats(stats.column_stats, filter_step->getExpression());
        return stats;
    }

    if (const auto * aggregating_step = typeid_cast<const AggregatingStep *>(step))
    {
        auto stats = estimateReadRowsCount(*node.children.front(), filter, for_runtime_filter_transport);
        auto aggregation_stats = estimateAggregatingStepStats(*aggregating_step, stats);
        return aggregation_stats;
    }

    if (const auto * sorting_step = typeid_cast<const SortingStep *>(step))
    {
        auto stats = estimateReadRowsCount(*node.children.front(), filter, for_runtime_filter_transport);
        if (sorting_step->getLimit())
        {
            if (!stats.estimated_rows || stats.estimated_rows > sorting_step->getLimit())
                stats.estimated_rows = sorting_step->getLimit();
            clearColumnValueRanges(stats.column_stats);
        }
        return stats;
    }

    /// Estimates must see through exchanges: they do not change row counts, and an
    /// already-distributed subtree would otherwise report unknown cardinality, degrading
    /// broadcast-vs-shuffle and join order decisions. Runtime-filter transport sizes the
    /// exact phase from this estimate, and the build filter sits below the join shuffle, so
    /// the walk has to cross exchanges on every build.
    if (dynamic_cast<LogicalExchangeStep *>(step))
        return estimateReadRowsCount(*node.children.front(), filter, for_runtime_filter_transport);

    if (const auto * transform = dynamic_cast<const ITransformingStep *>(step);
        transform && transform->getTransformTraits().preserves_number_of_rows)
        return estimateReadRowsCount(*node.children.front(), filter, for_runtime_filter_transport);

    return {};
}

namespace
{

/// The `ColumnConst` holding a column's value in every row (nullptr when unknown), or std::nullopt when not constant.
using ConstantValue = std::optional<ColumnPtr>;

/// Classifies each plan column and DAG node once: an output can depend on several columns that share inputs.
class ConstantColumnFinder
{
public:
    ConstantValue findConstantValue(const QueryPlan::Node & node, const String & column_name);

private:
    ConstantValue findStepConstant(const QueryPlan::Node & node, const String & column_name);
    ConstantValue findInputConstant(const QueryPlan::Node & node, const String & input_name);
    ConstantValue findPassedThroughConstant(const QueryPlan::Node & child, const ColumnWithTypeAndName & column);
    ConstantValue findExpressionConstant(const QueryPlan::Node & node, const ActionsDAG & actions, const String & column_name);
    ConstantValue findUnionConstant(const QueryPlan::Node & node, const Block & header, const String & column_name);
    bool isConstantNode(const QueryPlan::Node & node, const ActionsDAG::Node & dag_node);

    std::unordered_map<const QueryPlan::Node *, std::unordered_map<String, ConstantValue>> column_values;
    std::unordered_map<const ActionsDAG::Node *, bool> constant_dag_nodes;
};

ConstantValue ConstantColumnFinder::findConstantValue(const QueryPlan::Node & node, const String & column_name)
{
    if (auto it = column_values.find(&node); it != column_values.end())
    {
        if (auto value_it = it->second.find(column_name); value_it != it->second.end())
            return value_it->second;
    }
    auto value = findStepConstant(node, column_name);
    column_values[&node].emplace(column_name, value);
    return value;
}

ConstantValue ConstantColumnFinder::findInputConstant(const QueryPlan::Node & node, const String & input_name)
{
    if (node.children.size() != 1)
        return std::nullopt;
    return findConstantValue(*node.children.front(), input_name);
}

ConstantValue ConstantColumnFinder::findPassedThroughConstant(const QueryPlan::Node & child, const ColumnWithTypeAndName & column)
{
    const auto * child_column = child.step->getOutputHeader()->findByName(column.name);
    if (!child_column || !child_column->type->equals(*column.type))
        return std::nullopt;
    return findConstantValue(child, column.name);
}

/// A deterministic, stateless function of constants holds one value in every row.
bool ConstantColumnFinder::isConstantNode(const QueryPlan::Node & node, const ActionsDAG::Node & dag_node)
{
    if (auto it = constant_dag_nodes.find(&dag_node); it != constant_dag_nodes.end())
        return it->second;
    bool constant = false;
    if (dag_node.type == ActionsDAG::ActionType::COLUMN)
        constant = dag_node.column != nullptr;
    else if (dag_node.type == ActionsDAG::ActionType::INPUT)
        constant = findInputConstant(node, dag_node.result_name).has_value();
    else if (dag_node.type == ActionsDAG::ActionType::ALIAS)
        constant = isConstantNode(node, *dag_node.children.front());
    else if (dag_node.type == ActionsDAG::ActionType::FUNCTION)
        constant = dag_node.function_base && dag_node.function_base->isDeterministic() && !dag_node.function_base->isStateful()
            && !dag_node.children.empty()
            && std::ranges::all_of(dag_node.children, [&](const auto * child) { return isConstantNode(node, *child); });
    constant_dag_nodes.emplace(&dag_node, constant);
    return constant;
}

ConstantValue ConstantColumnFinder::findExpressionConstant(const QueryPlan::Node & node, const ActionsDAG & actions, const String & column_name)
{
    for (const auto * output : actions.getOutputs())
    {
        if (output->result_name != column_name)
            continue;

        const auto * source = output;
        while (true)
        {
            auto hop = describeActionsDAGLineageHop(*source);
            if (!hop || (hop->kind != ActionsDAGLineageKind::Identity && hop->kind != ActionsDAGLineageKind::ValuePreserving))
                break;
            source = source->children.front();
        }

        if (source->type == ActionsDAG::ActionType::COLUMN && source->column)
            return source->column;
        if (source->type == ActionsDAG::ActionType::INPUT)
        {
            if (auto value = findInputConstant(node, source->result_name))
                return value;
        }
        if (isConstantNode(node, *output))
            return ColumnPtr{};
    }
    return std::nullopt;
}

/// A column of a union is constant when it holds one value in every branch.
ConstantValue ConstantColumnFinder::findUnionConstant(const QueryPlan::Node & node, const Block & header, const String & column_name)
{
    for (size_t position = 0; position < header.columns(); ++position)
    {
        const auto & column = header.getByPosition(position);
        if (column.name != column_name || !column.type->isComparable())
            continue;

        ColumnPtr value;
        bool is_constant = !node.children.empty();
        for (size_t i = 0; i < node.children.size() && is_constant; ++i)
        {
            const auto & child = *node.children[i];
            const auto & child_header = *child.step->getOutputHeader();
            ConstantValue child_value;
            if (position < child_header.columns())
                child_value = findConstantValue(child, child_header.getByPosition(position).name);
            is_constant = child_value && *child_value
                && (!value || (value->structureEquals(**child_value) && value->compareAt(0, 0, **child_value, 1) == 0));
            if (is_constant)
                value = *child_value;
        }
        if (is_constant)
            return value;
    }
    return std::nullopt;
}

/// Steps that only reorder or select the rows of their single input.
bool keepsInputValues(const IQueryPlanStep & step)
{
    return typeid_cast<const SortingStep *>(&step) || typeid_cast<const DistinctStep *>(&step)
        || typeid_cast<const LimitStep *>(&step) || typeid_cast<const LimitByStep *>(&step)
        || typeid_cast<const LimitRangeStep *>(&step) || typeid_cast<const OffsetStep *>(&step)
        || typeid_cast<const FractionalLimitStep *>(&step) || typeid_cast<const FractionalOffsetStep *>(&step)
        || typeid_cast<const NegativeLimitStep *>(&step) || typeid_cast<const NegativeLimitByStep *>(&step)
        || typeid_cast<const NegativeOffsetStep *>(&step);
}

ConstantValue ConstantColumnFinder::findStepConstant(const QueryPlan::Node & node, const String & column_name)
{
    const IQueryPlanStep * step = node.step.get();
    const auto & header = *step->getOutputHeader();

    const auto * column = header.findByName(column_name);
    if (!column)
        return std::nullopt;
    if (column->column && isColumnConst(*column->column))
        return column->column;

    if (const auto * expression_step = typeid_cast<const ExpressionStep *>(step))
        return findExpressionConstant(node, expression_step->getExpression(), column_name);
    if (const auto * filter_step = typeid_cast<const FilterStep *>(step))
        return findExpressionConstant(node, filter_step->getExpression(), column_name);
    if (typeid_cast<const UnionStep *>(step))
        return findUnionConstant(node, header, column_name);
    if (const auto * aggregating_step = typeid_cast<const AggregatingStep *>(step))
    {
        const auto & keys = aggregating_step->getParams().keys;
        if (!aggregating_step->isGroupingSets() && node.children.size() == 1 && std::ranges::find(keys, column_name) != keys.end())
            return findConstantValue(*node.children.front(), column_name);
        return std::nullopt;
    }
    if (keepsInputValues(*step))
    {
        if (node.children.size() == 1)
            return findPassedThroughConstant(*node.children.front(), *column);
        return std::nullopt;
    }
    if (const auto * reference_step = typeid_cast<const CommonSubplanReferenceStep *>(step))
    {
        if (const auto * subplan_root = reference_step->getSubplanReferenceRoot())
            return findPassedThroughConstant(*subplan_root, *column);
        return std::nullopt;
    }
    if (const auto * join_step = typeid_cast<const JoinStepLogical *>(step))
    {
        /// The rows of a null-supplying side can hold NULL or default values.
        const auto kind = join_step->getJoinOperator().kind;
        const bool keeps_both_sides = kind == JoinKind::Inner || kind == JoinKind::Cross || kind == JoinKind::Comma;
        if (node.children.size() != 2)
            return std::nullopt;
        if (keeps_both_sides || kind == JoinKind::Left)
        {
            if (auto value = findPassedThroughConstant(*node.children[0], *column))
                return value;
        }
        if (keeps_both_sides || kind == JoinKind::Right)
            return findPassedThroughConstant(*node.children[1], *column);
    }
    return std::nullopt;
}

}

bool isConstantColumn(const QueryPlan::Node & node, const String & column_name)
{
    return ConstantColumnFinder().findConstantValue(node, column_name).has_value();
}

}
}
