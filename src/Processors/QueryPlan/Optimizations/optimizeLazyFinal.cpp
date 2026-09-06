#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/PreparedSets.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/DistinctStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/InputSelectorStep.h>
#include <Processors/QueryPlan/JoinLazyColumnsStep.h>
#include <Processors/QueryPlan/LazilyReadFromMergeTree.h>
#include <Processors/QueryPlan/LazilyUnorderedReadFromMergeTree.h>
#include <Processors/QueryPlan/LazyFinalKeyAnalysisStep.h>
#include <Processors/QueryPlan/LazyReadReplacingFinalStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/PartsSplitter.h>
#include <Processors/QueryPlan/UnionStep.h>
#include <Processors/Sources/LazyFinalSharedState.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/projectionsCommon.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/Transforms/LazyMaterializingTransform.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeIOSettings.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace QueryPlanOptimizations
{

/// Clone only the filter computation sub-DAG from a larger DAG.
/// The result DAG has the filter column as an output plus all its inputs as pass-through outputs.
/// This avoids column renames or removals that the original DAG might do.
static ActionsDAG cloneFilterSubDAG(const ActionsDAG & dag, const String & filter_column_name)
{
    const auto * filter_node = &dag.findInOutputs(filter_column_name);
    auto sub_dag = ActionsDAG::cloneSubDAG({filter_node}, /*remove_aliases=*/ false);

    const auto * filter_output = sub_dag.getOutputs().front();
    for (const auto * input : sub_dag.getInputs())
        if (input != filter_output)
            sub_dag.getOutputs().push_back(input);

    return sub_dag;
}

/// Expose the given intermediate nodes (by result name) as outputs of `dag`, if present and not
/// already exposed. Used so that the set-building read's prewhere keeps the computed columns that
/// a WHERE `FilterStep` above the reading step consumes as inputs.
static void exposeNodesAsDAGOutputs(ActionsDAG & dag, const NameSet & names)
{
    if (names.empty())
        return;

    std::unordered_set<const ActionsDAG::Node *> outputs(dag.getOutputs().begin(), dag.getOutputs().end());
    for (const auto & node : dag.getNodes())
    {
        if (names.contains(node.result_name) && !outputs.contains(&node))
        {
            dag.getOutputs().push_back(&node);
            outputs.insert(&node);
        }
    }
}

/// Collect the pre-FINAL (non-deferred) filters of `reading_step` for the winner-selection read.
///
/// A filter the query applies BEFORE the FINAL merge constrains which rows participate in
/// deduplication. The lazy path's deduplication is the `argMax` aggregation over the
/// winner-selection read, so the filter must be applied on that read's input: otherwise the
/// aggregation picks a row the filter excludes and the key produces no row at all, instead of the
/// highest-versioned row that does satisfy the filter.
///
/// Returns nullopt when a non-deferred filter exists but cannot be pushed into the winner-selection
/// read safely, in which case the caller must leave the plan untouched.
static std::optional<LazyFinalPreFinalFilters> collectPreFinalFilters(
    const ReadFromMergeTree * reading_step,
    const StorageSnapshotPtr & storage_snapshot,
    const StorageMetadataPtr & metadata_snapshot,
    const MergeTreeData & data)
{
    const auto & query_info = reading_step->getQueryInfo();

    /// Deferral is recorded by `deferFiltersAfterFinalIfNeeded` and does NOT remove the filter from
    /// `query_info` until pipeline build, so the accessors below - not the presence of the filter -
    /// are what tells the two orderings apart.
    const bool has_non_deferred_row_level_filter
        = query_info.row_level_filter && !reading_step->getDeferredRowLevelFilter();
    const bool has_non_deferred_prewhere
        = query_info.prewhere_info && !reading_step->getDeferredPrewhereInfo();

    LazyFinalPreFinalFilters result;
    if (!has_non_deferred_row_level_filter && !has_non_deferred_prewhere)
        return result;

    /// The aggregation reads these; a filter must not consume them away. The containment test below
    /// only fires when the predicate IS one of these bare columns; in every other case what protects
    /// the aggregation's inputs is `cloneFilterSubDAG` re-exposing each predicate input as a
    /// pass-through output.
    NameSet required_columns;
    for (const auto & column : metadata_snapshot->getSortingKey().expression->getRequiredColumnsWithTypes())
        required_columns.insert(column.name);
    if (!data.merging_params.version_column.empty())
        required_columns.insert(data.merging_params.version_column);
    if (!data.merging_params.is_deleted_column.empty())
        required_columns.insert(data.merging_params.is_deleted_column);

    /// A predicate that can observe rows FINAL would have eliminated must not be evaluated on a
    /// different row set than the one the query already evaluates it on. `cloneFilterSubDAG` keeps
    /// only the predicate computation, so these checks see the predicate itself.
    auto is_safe_to_push = [](const ActionsDAG & dag) { return !dag.hasNonDeterministic() && !dag.hasStatefulFunctions(); };

    NameSet extra_columns_seen;
    const auto & columns_description = storage_snapshot->metadata->getColumns();
    auto collect_storage_inputs = [&](const ActionsDAG & dag)
    {
        for (const auto * input : dag.getInputs())
        {
            const auto & name = input->result_name;
            /// Only storage columns can be requested from the winner-selection read: the
            /// `ReadFromMergeTree` constructor resolves the requested names through
            /// `StorageSnapshot::getSampleBlockForColumns`, which throws for anything else.
            /// A derived input is produced by the cloned filters themselves.
            if (!columns_description.hasColumnOrSubcolumn(GetColumnsOptions::All, name)
                && !storage_snapshot->metadata->virtuals.has(name))
                continue;
            if (extra_columns_seen.insert(name).second)
                result.extra_columns.push_back(name);
        }
    };

    if (has_non_deferred_row_level_filter)
    {
        auto cloned = std::make_shared<FilterDAGInfo>();
        /// `cloneFilterSubDAG` exposes every input of the predicate as a pass-through output, so no
        /// column the aggregation needs can be erased by the filter's own DAG.
        cloned->actions = cloneFilterSubDAG(query_info.row_level_filter->actions, query_info.row_level_filter->column_name);
        cloned->column_name = query_info.row_level_filter->column_name;
        cloned->do_remove_column
            = query_info.row_level_filter->do_remove_column && !required_columns.contains(cloned->column_name);

        if (!is_safe_to_push(cloned->actions))
            return std::nullopt;

        collect_storage_inputs(cloned->actions);
        result.row_level_filter = std::move(cloned);
    }

    if (has_non_deferred_prewhere)
    {
        auto cloned = std::make_shared<PrewhereInfo>(query_info.prewhere_info->clone());
        cloned->prewhere_actions
            = cloneFilterSubDAG(query_info.prewhere_info->prewhere_actions, query_info.prewhere_info->prewhere_column_name);
        cloned->remove_prewhere_column
            = query_info.prewhere_info->remove_prewhere_column && !required_columns.contains(cloned->prewhere_column_name);
        /// Nothing above the winner-selection read consumes the predicate column, so the rows have
        /// to be dropped by the read itself rather than by a later step.
        cloned->need_filter = true;

        if (!is_safe_to_push(cloned->prewhere_actions))
            return std::nullopt;

        collect_storage_inputs(cloned->prewhere_actions);
        result.prewhere_info = std::move(cloned);
    }

    return result;
}

/// Add a FilterStep that keeps only rows where is_deleted == 0.
/// If remove_is_deleted_column is true, the is_deleted column is also removed from output
/// (used when the column was added internally and not requested by the query).
static void addIsDeletedFilter(QueryPlan & plan, const String & is_deleted_column, const ContextPtr & context, bool remove_is_deleted_column)
{
    const auto & header = *plan.getCurrentHeader();

    ActionsDAG dag;

    /// Add columns before is_deleted as inputs/outputs to preserve column order.
    /// When is_deleted is kept (user requested it), we need explicit inputs
    /// for preceding columns so their order is maintained relative to is_deleted.
    const ActionsDAG::Node * col_node = nullptr;
    for (const auto & col : header)
    {
        if (col.name == is_deleted_column)
        {
            col_node = &dag.addInput(col.name, col.type);
            if (!remove_is_deleted_column)
                dag.getOutputs().push_back(col_node);
            break;
        }

        if (!remove_is_deleted_column)
        {
            const auto * input = &dag.addInput(col.name, col.type);
            dag.getOutputs().push_back(input);
        }
    }

    /// `createNonIntersectingPlan` must guarantee that `is_deleted_column` is in the
    /// upstream header. Throw rather than passing a null pointer to `addFunction` —
    /// any future regression in the upstream invariant should surface, not turn into UB.
    if (!col_node)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Lazy FINAL optimization: column `{}` is missing from the non-intersecting reading step header. "
            "This is a bug in `createNonIntersectingPlan`.",
            is_deleted_column);

    auto zero_type = std::make_shared<DataTypeUInt8>();
    auto zero_column = zero_type->createColumnConst(0, Field(UInt8(0)));
    const auto * zero_node = &dag.addColumn(std::move(zero_column), std::move(zero_type), "__is_deleted_zero");

    auto equals_func = FunctionFactory::instance().get("equals", context);
    const auto * filter_node = &dag.addFunction(equals_func, {col_node, zero_node}, "__is_deleted_filter");

    dag.getOutputs().push_back(filter_node);

    plan.addStep(std::make_unique<FilterStep>(
        plan.getCurrentHeader(), std::move(dag), "__is_deleted_filter", /*remove_filter_column=*/ true));
}

/// Ensure `column_name` (if present as a DAG input) is also exposed as a DAG output,
/// so that `ActionsDAG::updateHeader` does not erase it from the block.
/// Used by the lazy-FINAL non-intersecting plan to keep the `is_deleted` column
/// available for the downstream `is_deleted = 0` filter, even when prewhere/row-level
/// filter has consumed it.
static bool exposeInputAsDAGOutput(ActionsDAG & dag, const String & column_name)
{
    std::unordered_set<const ActionsDAG::Node *> outputs(dag.getOutputs().begin(), dag.getOutputs().end());
    bool added = false;
    for (const auto * input : dag.getInputs())
    {
        if (input->result_name == column_name && !outputs.contains(input))
        {
            dag.getOutputs().push_back(input);
            added = true;
            break;
        }
    }
    return added;
}

/// Create a non-FINAL ReadFromMergeTree plan for non-intersecting parts,
/// with optional is_deleted filter and WHERE filter applied.
/// Returns nullopt if PK analysis prunes all granules (nothing to read).
static std::optional<QueryPlan> createNonIntersectingPlan(
    RangesInDataParts parts,
    ReadFromMergeTree * reading_step,
    FilterStep * filter_step)
{
    const auto & data = reading_step->getMergeTreeData();
    const auto & merging_params = data.merging_params;
    const auto & context = reading_step->getContext();

    SelectQueryInfo non_final_query_info = reading_step->getQueryInfo();
    if (non_final_query_info.table_expression_modifiers)
        non_final_query_info.table_expression_modifiers->setHasFinal(false);

    auto columns = reading_step->getAllColumnNames();
    bool is_deleted_added = false;
    if (!merging_params.is_deleted_column.empty())
    {
        if (std::ranges::find(columns, merging_params.is_deleted_column) == columns.end())
        {
            columns.push_back(merging_params.is_deleted_column);
            is_deleted_added = true;
        }

        /// If the prewhere or row-level filter on the ORIGINAL (FINAL) reading step
        /// consumed `is_deleted` as an input but did not expose it as an output,
        /// the column is missing from the read step's output header. Two consequences
        /// for the non-intersecting plan we are about to build:
        ///   1. `addIsDeletedFilter` reads `is_deleted` from the read step's output,
        ///      so we must keep the column in the output of `non_final_reading`.
        ///   2. The non-intersecting plan is later unioned with the FINAL plan
        ///      (whose output also lacks `is_deleted` for the same prewhere reason).
        ///      To keep the two union branches header-compatible, `addIsDeletedFilter`
        ///      must then drop `is_deleted` from its output.
        ///
        /// Clone the filter info(s) before mutating to avoid affecting the original
        /// reading step's prewhere/row-level filter (shared via `getQueryInfo()`).
        const auto & original_output_header = reading_step->getOutputHeader();
        const bool is_deleted_in_original_output
            = original_output_header && original_output_header->has(merging_params.is_deleted_column);
        if (!is_deleted_in_original_output)
        {
            bool exposed = false;
            if (non_final_query_info.prewhere_info)
            {
                auto cloned_prewhere = std::make_shared<PrewhereInfo>(non_final_query_info.prewhere_info->clone());
                exposed |= exposeInputAsDAGOutput(cloned_prewhere->prewhere_actions, merging_params.is_deleted_column);
                non_final_query_info.prewhere_info = std::move(cloned_prewhere);
            }
            if (non_final_query_info.row_level_filter)
            {
                const auto & original = *non_final_query_info.row_level_filter;
                auto cloned_row_level = std::make_shared<FilterDAGInfo>();
                cloned_row_level->actions = original.actions.clone();
                cloned_row_level->column_name = original.column_name;
                cloned_row_level->do_remove_column = original.do_remove_column;
                exposed |= exposeInputAsDAGOutput(cloned_row_level->actions, merging_params.is_deleted_column);
                non_final_query_info.row_level_filter = std::move(cloned_row_level);
            }

            /// If we successfully re-exposed `is_deleted` in the prewhere/row-level
            /// filter outputs, the column will be present in `non_final_reading`'s
            /// output header. We must drop it again after `addIsDeletedFilter` has
            /// done its work, to keep header parity with the FINAL branch.
            if (exposed)
                is_deleted_added = true;
        }
    }

    auto non_final_reading = std::make_unique<ReadFromMergeTree>(
        std::make_shared<RangesInDataParts>(std::move(parts)),
        reading_step->getMutationsSnapshot(),
        columns,
        data,
        data.getSettings(),
        non_final_query_info,
        reading_step->getStorageSnapshot(),
        context,
        reading_step->getMaxBlockSize(),
        reading_step->getNumStreams(),
        getMaxAddedBlocks(reading_step),
        getLogger("optimizeLazyFinal"),
        nullptr,
        false);

    non_final_reading->disableQueryConditionCache();

    /// The synthetic step inherits the filter rewritten to `__text_index_*` virtual columns, but not the read tasks that produce them
    /// from the index.
    /// Copy them over, otherwise the filter drops every row.
    if (const IndexReadTasks & index_read_tasks = reading_step->getIndexReadTasks(); !index_read_tasks.empty())
        non_final_reading->setIndexReadTasks(index_read_tasks); // Pass by value

    if (filter_step)
        non_final_reading->addFilter(filter_step->getExpression().clone(), filter_step->getFilterColumnName());
    non_final_reading->SourceStepWithFilterBase::applyFilters();

    /// Skip if PK analysis prunes all granules — nothing to read.
    auto analysis = non_final_reading->selectRangesToRead();
    if (analysis && analysis->parts_with_ranges.empty())
        return std::nullopt;

    QueryPlan plan;
    plan.addStep(std::move(non_final_reading));

    if (!merging_params.is_deleted_column.empty())
        addIsDeletedFilter(plan, merging_params.is_deleted_column, context, is_deleted_added);

    return plan;
}

struct SplitResult
{
    enum class Outcome
    {
        /// The reading node was replaced in place (all parts non-intersecting): nothing left to do.
        PlanReplaced,
        /// Lazy FINAL does not apply to this read; the reading step was left untouched and the plan
        /// must keep the ordinary FINAL read.
        DeclinedLeaveUntouched,
        /// The caller must build the lazy branch. `non_intersecting_plan` is the half to union with
        /// it, or null when the split produced no non-intersecting half.
        BuildLazyBranch,
    };

    std::unique_ptr<QueryPlan> non_intersecting_plan;
    /// Declining is the default so that a return which names no outcome fails closed.
    Outcome outcome = Outcome::DeclinedLeaveUntouched;
};

/// Try to split parts into non-intersecting and intersecting by primary key.
/// If all parts are non-intersecting, replaces the plan node directly and returns PlanReplaced.
/// Otherwise, if allow_partial_split and lazy_branch_available are both set, returns BuildLazyBranch
/// together with a plan for the non-intersecting parts (or null when there are none), and updates the
/// reading step's analyzed result to contain only intersecting parts; if either is unset, leaves the
/// reading step untouched and returns DeclinedLeaveUntouched to stop.
/// The two returns for a key this splitter cannot analyse sit above the checks at the bottom, so they
/// re-test them and yield BuildLazyBranch only when all three preconditions hold.
static SplitResult trySplitNonIntersectingParts(
    ReadFromMergeTree * reading_step,
    ReadFromMergeTree::AnalysisResultPtr analyzed_result,
    FilterStep * filter_step,
    QueryPlan::Node * read_node,
    QueryPlan & query_plan,
    bool allow_partial_split,
    bool lazy_branch_available)
{
    const auto & metadata_snapshot = reading_step->getStorageMetadata();
    const auto & primary_key = metadata_snapshot->getPrimaryKey();
    const auto & sorting_key = metadata_snapshot->getSortingKey();

    /// The two returns below for a key this splitter cannot analyse skip the checks at the bottom of
    /// this function, so they must re-test them: a direct text-index read cannot produce its virtual
    /// columns through the lazy source, and a query that stops reading early (read-in-order or a small
    /// limit) needs the ordinary FINAL read because the lazy replacement is unordered.
    const bool unanalysable_key_may_build_lazy_branch
        = lazy_branch_available && allow_partial_split && reading_step->getIndexReadTasks().empty();

    if (!isSafePrimaryKey(primary_key))
        return {
            .non_intersecting_plan = nullptr,
            .outcome = unanalysable_key_may_build_lazy_branch
                ? SplitResult::Outcome::BuildLazyBranch
                : SplitResult::Outcome::DeclinedLeaveUntouched};

    bool in_reverse_order = false;
    if (!sorting_key.reverse_flags.empty())
    {
        size_t num_pk = primary_key.expression_list_ast->children.size();
        in_reverse_order = sorting_key.reverse_flags[0];
        for (size_t i = 1; i < num_pk && i < sorting_key.reverse_flags.size(); ++i)
        {
            if (in_reverse_order != sorting_key.reverse_flags[i])
                return {
                    .non_intersecting_plan = nullptr,
                    .outcome = unanalysable_key_may_build_lazy_branch
                        ? SplitResult::Outcome::BuildLazyBranch
                        : SplitResult::Outcome::DeclinedLeaveUntouched};
        }
    }

    auto split = splitPartsRanges(reading_step->getParts(), in_reverse_order, getLogger("optimizeLazyFinal"));

    if (split.intersecting_parts_ranges.empty())
    {
        /// All parts are non-intersecting — no FINAL needed at all.
        auto plan = createNonIntersectingPlan(
            std::move(split.non_intersecting_parts_ranges), reading_step, filter_step);

        if (!plan)
            return {.non_intersecting_plan = nullptr, .outcome = SplitResult::Outcome::DeclinedLeaveUntouched};

        auto expected_header = reading_step->getOutputHeader();
        query_plan.replaceNodeWithPlan(read_node, std::move(*plan), expected_header);
        return {.non_intersecting_plan = nullptr, .outcome = SplitResult::Outcome::PlanReplaced};
    }

    /// The set/true-branch machinery built for intersecting parts reads through the lazy true-branch
    /// source, which cannot produce the `__text_index_*` virtual columns of a direct read from a text
    /// index. Leave the reading step untouched so the query falls back to a regular FINAL read.
    /// Must precede every BuildLazyBranch return, including the two unanalysable-key ones above, which
    /// re-test it through `unanalysable_key_may_build_lazy_branch`.
    if (!reading_step->getIndexReadTasks().empty())
        return {.non_intersecting_plan = nullptr, .outcome = SplitResult::Outcome::DeclinedLeaveUntouched};

    /// For queries that can stop reading early the set-building plan is a pessimization, and the
    /// partial split alone does not preserve the reading order; keep the regular FINAL read.
    if (!allow_partial_split)
        return {.non_intersecting_plan = nullptr, .outcome = SplitResult::Outcome::DeclinedLeaveUntouched};

    /// Everything below hands the intersecting parts to the lazy branch (with or without a
    /// non-intersecting half to union). When that branch cannot be built the whole read must stay an
    /// ordinary FINAL read, so decline here - before `analyzed_result` is narrowed at all.
    if (!lazy_branch_available)
        return {.non_intersecting_plan = nullptr, .outcome = SplitResult::Outcome::DeclinedLeaveUntouched};

    /// All parts intersect, so there is no non-intersecting half to union and the reading step is left
    /// as it is - but the caller still builds the lazy branch over the whole part set.
    if (split.non_intersecting_parts_ranges.empty())
        return {.non_intersecting_plan = nullptr, .outcome = SplitResult::Outcome::BuildLazyBranch};

    /// Update the original reading step to only have intersecting parts.
    /// Adjust index_stats by subtracting the non-intersecting contribution,
    /// then add a NonIntersectingSplit entry showing the split.
    if (analyzed_result)
    {
        analyzed_result->parts_with_ranges = std::move(split.intersecting_parts_ranges);

        size_t intersecting_marks = 0;
        size_t intersecting_ranges = 0;
        size_t intersecting_rows = 0;
        for (const auto & part : analyzed_result->parts_with_ranges)
        {
            intersecting_marks += part.getMarksCount();
            intersecting_ranges += part.ranges.size();
            intersecting_rows += part.getRowsCount();
        }

        auto num_parts = analyzed_result->parts_with_ranges.size();
        analyzed_result->total_parts = num_parts;
        analyzed_result->parts_before_pk = num_parts;
        analyzed_result->selected_parts = num_parts;
        analyzed_result->selected_ranges = intersecting_ranges;
        analyzed_result->selected_marks = intersecting_marks;
        analyzed_result->selected_marks_pk = intersecting_marks;
        analyzed_result->total_marks_pk = intersecting_marks;
        analyzed_result->selected_rows = intersecting_rows;

        /// Add a new index entry for the non-intersecting split.
        /// Earlier entries keep their original numbers (which include non-intersecting parts).
        analyzed_result->index_stats.emplace_back(ReadFromMergeTree::IndexStat{
            .type = ReadFromMergeTree::IndexType::NonIntersectingSplit,
            .description = "Split non-intersecting parts for lazy FINAL",
            .num_parts_after = num_parts,
            .num_granules_after = intersecting_marks});

        reading_step->setAnalyzedResult(analyzed_result);
    }

    auto plan = createNonIntersectingPlan(
        std::move(split.non_intersecting_parts_ranges), reading_step, filter_step);

    /// The reading step has already been narrowed to the intersecting parts above, so the caller must
    /// build the lazy branch over them; there is just no non-intersecting half left to union.
    if (!plan)
        return {.non_intersecting_plan = nullptr, .outcome = SplitResult::Outcome::BuildLazyBranch};

    return {
        .non_intersecting_plan = std::make_unique<QueryPlan>(std::move(*plan)),
        .outcome = SplitResult::Outcome::BuildLazyBranch};
}

void optimizeLazyFinal(const Stack & stack, QueryPlan & query_plan, QueryPlan::Nodes & nodes [[maybe_unused]], const QueryPlanOptimizationSettings & optimization_settings)
{
    /// Match ReadFromMergeTree at the bottom of the stack.
    /// This runs after optimizePrimaryKeyConditionAndLimit, so the WHERE filter
    /// is already pushed into the reading step for PK analysis.
    auto * read_node = stack.back().node;
    auto * reading_step = typeid_cast<ReadFromMergeTree *>(read_node->step.get());
    if (!reading_step)
        return;

    if (!reading_step->isQueryWithFinal())
        return;

    /// Only ReplacingMergeTree is supported.
    const auto & data = reading_step->getMergeTreeData();
    if (data.merging_params.mode != MergeTreeData::MergingParams::Replacing)
        return;

    /// The steps above may rely on the reading step producing rows in the order of the
    /// sorting key, but the set-building replacement plan does not produce rows in any
    /// particular order. Besides, read-in-order queries can often stop early, while the
    /// set-building phase would read all filtered rows up front. Such queries may still
    /// use the full non-intersecting replacement, which preserves both the order and the
    /// early exit — the flag is checked before the partial split below.
    const bool reading_in_order = reading_step->readsInOrder();

    /// Skip if projection was applied. A non-null analysis result alone does not imply
    /// a projection: join-order estimation runs index analysis for join relations and
    /// memoizes the result (see estimateReadRowsCount in optimizeJoin.cpp).
    if (auto analyzed = reading_step->getAnalyzedResult(); analyzed && analyzed->readFromProjection())
        return;

    /// Find a LIMIT that applies directly to the reading step's output (only Expression/Filter
    /// steps in between). Such a limit lets the query stop reading early, which the set-building
    /// phase would defeat. Any other step consumes the whole stream and stops the search.
    size_t limit_above_reading = 0;
    for (size_t i = stack.size() - 1; i-- > 0;)
    {
        auto * step = stack[i].node->step.get();
        if (const auto * expression_step = typeid_cast<ExpressionStep *>(step))
        {
            /// arrayJoin changes the number of rows, a limit above it is not comparable to selected_rows.
            if (expression_step->getExpression().hasArrayJoin())
                break;
            continue;
        }
        if (const auto * filter_step_above = typeid_cast<FilterStep *>(step))
        {
            if (filter_step_above->getExpression().hasArrayJoin())
                break;
            continue;
        }
        /// DISTINCT stops reading as soon as its pushed-down limit hint of distinct rows is
        /// produced, so any non-zero hint means the query terminates reading early.
        if (const auto * distinct_step = typeid_cast<DistinctStep *>(step))
        {
            limit_above_reading = distinct_step->getLimitHint();
            break;
        }
        /// With always_read_till_end (e.g. exact_rows_before_limit or WITH TOTALS) the query
        /// reads the whole stream anyway, so the limit does not allow to stop reading early.
        if (const auto * limit_step = typeid_cast<LimitStep *>(step); limit_step && !limit_step->alwaysReadTillEnd())
        {
            size_t limit = limit_step->getLimit();
            size_t offset = limit_step->getOffset();
            limit_above_reading = limit + offset < limit ? std::numeric_limits<size_t>::max() : limit + offset;
        }
        break;
    }

    /// Check the immediate parent for a FilterStep or InputSelectorStep.
    FilterStep * filter_step = nullptr;
    if (stack.size() >= 2)
    {
        auto * parent_step = stack[stack.size() - 2].node->step.get();
        if (auto * f = typeid_cast<FilterStep *>(parent_step))
            filter_step = f;
        else if (typeid_cast<InputSelectorStep *>(parent_step))
            return; /// Already inside an InputSelectorStep — avoid infinite re-application.
    }

    /// We need either a filter or prewhere/row_policy to make this worthwhile.
    if (!filter_step && !reading_step->getPrewhereInfo() && !reading_step->getRowLevelFilter())
        return;

    const auto & metadata_snapshot = reading_step->getStorageMetadata();
    const auto & primary_key = metadata_snapshot->getPrimaryKey();

    /// Skip if primary key is empty (ORDER BY tuple()) — no PK columns to build a set from.
    if (primary_key.column_names.empty())
        return;

    /// Whether a filter is deferred to after FINAL is decided by `deferFiltersAfterFinalIfNeeded`,
    /// which only runs as part of `optimizePrimaryKeyConditionAndLimit`. When that pass is disabled
    /// (the automatic-parallel-replicas candidate plan does exactly this) the deferral accessors are
    /// null for every filter, so a non-deferred filter is indistinguishable from a deferred one and
    /// the winner-selection read cannot be built correctly.
    const bool deferral_undeterminable
        = !optimization_settings.query_plan_optimize_primary_key
        && (reading_step->getRowLevelFilter() || reading_step->getPrewhereInfo());

    /// A predicate that is nondeterministic or stateful must not be evaluated on the extra row set
    /// the winner-selection read would see; keep the ordinary FINAL read for those.
    auto pre_final_filters = deferral_undeterminable
        ? std::optional<LazyFinalPreFinalFilters>{}
        : collectPreFinalFilters(reading_step, reading_step->getStorageSnapshot(), metadata_snapshot, data);

    /// The lazy branch needs a winner-selection read that applies the same pre-FINAL filters. The
    /// all-non-intersecting split needs no such read, so it stays available even when the branch is
    /// not - hence this is passed into the split rather than returned on.
    const bool lazy_branch_available = pre_final_filters.has_value();

    /// Run early index analysis so the analyzed (PK-filtered) parts can be used
    /// both for the non-intersecting split and for the set/true-branch plans.
    /// The WHERE filter was already pushed by optimizePrimaryKeyConditionAndLimit,
    /// so selectRangesToRead uses the PK condition for index analysis.
    /// Reuse the partition/PK/index ranges memoized on `ReadFromMergeTree` by join-order
    /// estimation. This is an analysis-time range set, not an estimated join cardinality
    /// or a row count observed during execution. The PK conditions were pushed before that
    /// pass as well, so re-running the analysis would produce the same ranges.
    auto analyzed_result = reading_step->getAnalyzedResult();
    if (!analyzed_result)
        analyzed_result = reading_step->selectRangesToRead();
    if (reading_step->getParts().empty())
        return;

    /// A limit below the number of selected rows means the query is expected to finish early.
    const bool stops_reading_early = reading_in_order
        || (limit_above_reading && analyzed_result && limit_above_reading < analyzed_result->selected_rows);

    /// Split parts into non-intersecting (unique key ranges, no FINAL needed) and
    /// intersecting (overlapping, need FINAL). This avoids running the expensive
    /// aggregation-based FINAL on parts that have no duplicates.
    /// When all parts are non-intersecting, replaceNodeWithPlan is called inside
    /// and PlanReplaced is returned, in which case we're done.
    auto split_result = trySplitNonIntersectingParts(
        reading_step, analyzed_result, filter_step, read_node, query_plan, /*allow_partial_split=*/ !stops_reading_early,
        lazy_branch_available);

    /// Tested for the one outcome that continues rather than against the ones that stop, so that a
    /// future fourth outcome keeps the ordinary FINAL read instead of falling into the lazy branch.
    if (split_result.outcome != SplitResult::Outcome::BuildLazyBranch)
        return;

    const auto & context = reading_step->getContext();
    const auto & storage_snapshot = reading_step->getStorageSnapshot();
    auto mutations_snapshot = reading_step->getMutationsSnapshot();
    auto max_block_numbers_to_read = getMaxAddedBlocks(reading_step);

    /// Use parts from the analyzed result (possibly narrowed to intersecting-only by the split).
    /// These are PK-filtered parts with narrowed mark ranges from selectRangesToRead.
    auto parts_for_set = std::make_shared<RangesInDataParts>(
        analyzed_result ? analyzed_result->parts_with_ranges : reading_step->getParts());

    /// Build the set for primary key columns only (PK is a prefix of sorting key;
    /// the remaining sorting key columns are useless for index analysis).
    SizeLimits set_size_limits(optimization_settings.max_rows_for_lazy_final, optimization_settings.max_bytes_for_lazy_final, OverflowMode::BREAK);
    auto set = std::make_shared<Set>(set_size_limits, /*max_elements_to_fill=*/ 0, /*transform_null_in=*/ false);
    set->setHeader(primary_key.sample_block.cloneWithColumns(primary_key.sample_block.cloneEmptyColumns()).getColumnsWithTypeAndName());
    set->fillSetElements();
    auto set_and_key = std::make_shared<SetAndKey>(SetAndKey{.key = "__lazy_final_set", .set = set, .external_table = nullptr});

    /// Use FutureSetFromStorage — the Set will be filled by CreatingSetStep before
    /// LazyFinalKeyAnalysisStep runs (they are in the same pipeline).
    auto future_set = std::make_shared<FutureSetFromStorage>(FutureSet::Hash{}, /*ast=*/ nullptr, set, /*storage_id=*/ std::nullopt);

    /// Build the set-building sub-plan: read columns needed for predicates
    /// (prewhere, row policy, filter) and primary key, then project to PK columns.

    const auto & primary_key_dag = primary_key.expression->getActionsDAG();

    /// Start with the original column set (covers prewhere, row policy, etc.),
    /// then append primary key source columns, filter input columns,
    /// and prewhere/row_policy input columns that might be missing.
    ///
    /// `set_columns` is the list of columns to read FROM STORAGE for the set-building
    /// `ReadFromMergeTree` (it is passed as `all_column_names`). It must contain only
    /// real storage columns/subcolumns: the `ReadFromMergeTree` constructor calls
    /// `StorageSnapshot::getSampleBlockForColumns` on it, which throws
    /// NOT_FOUND_COLUMN_IN_BLOCK for any name that is not a storage or virtual column.
    ///
    /// The `filter_step` above the reading step takes its inputs from the reading
    /// step's OUTPUT columns, which include columns PRODUCED by the reading step's
    /// prewhere (e.g. the computed `greaterOrEquals(42, id)` predicate, kept when the
    /// remaining WHERE conjunct references it) rather than read from storage. Such
    /// derived columns are regenerated by the cloned prewhere/row-level-filter on the
    /// set-building read, so they must not be requested from storage here. We only add
    /// a name to `set_columns` if the storage snapshot actually has it as a column or
    /// subcolumn, which is exactly the precondition `getSampleBlockForColumns` checks.
    const auto & set_columns_desc = storage_snapshot->metadata->getColumns();
    auto is_storage_column = [&](const String & name)
    {
        return set_columns_desc.hasColumnOrSubcolumn(GetColumnsOptions::All, name)
            || storage_snapshot->metadata->virtuals.has(name);
    };

    Names set_columns = reading_step->getAllColumnNames();
    {
        NameSet existing(set_columns.begin(), set_columns.end());

        auto add_columns = [&](const ActionsDAG & dag, bool storage_columns_only)
        {
            for (const auto * input : dag.getInputs())
            {
                /// Filter inputs may be derived columns produced by the reading step's
                /// prewhere/row-level-filter, not storage columns. Requesting those from
                /// storage would throw NOT_FOUND_COLUMN_IN_BLOCK, so skip non-storage names.
                if (storage_columns_only && !is_storage_column(input->result_name))
                    continue;
                if (existing.insert(input->result_name).second)
                    set_columns.push_back(input->result_name);
            }
        };

        add_columns(primary_key_dag, /*storage_columns_only=*/ false);
        if (filter_step)
            add_columns(filter_step->getExpression(), /*storage_columns_only=*/ true);
        if (const auto & prewhere = reading_step->getQueryInfo().prewhere_info)
            add_columns(prewhere->prewhere_actions, /*storage_columns_only=*/ false);
        if (const auto & row_filter = reading_step->getQueryInfo().row_level_filter)
            add_columns(row_filter->actions, /*storage_columns_only=*/ false);
    }

    /// Inspect the inputs of the WHERE `filter_step` we copy above the set-building read below.
    ///
    /// `filter_derived_inputs`: inputs PRODUCED by the reading step's prewhere/row-level-filter
    /// rather than read from storage (e.g. the computed `greaterOrEquals(42, id)` predicate). They
    /// were excluded from `set_columns` above (not storage columns), so the set-building read must
    /// expose them from its cloned prewhere/row-level-filter, else the copied WHERE cannot find them.
    ///
    /// `filter_all_inputs`: EVERY input the copied WHERE consumes (storage and derived). Used to
    /// decide whether the prewhere/row-level-filter column must be kept in the output: if the WHERE
    /// still consumes it we must not remove it. This is exactly the case `splitAndFillPrewhereInfo`
    /// handles by flipping `remove_prewhere_column` to false, and it includes the case where the
    /// pushed predicate is a plain storage column (e.g. `WHERE flag AND value != 7`), which is NOT
    /// in `filter_derived_inputs`.
    NameSet filter_derived_inputs;
    NameSet filter_all_inputs;
    if (filter_step)
    {
        for (const auto * input : filter_step->getExpression().getInputs())
        {
            filter_all_inputs.insert(input->result_name);
            if (!is_storage_column(input->result_name))
                filter_derived_inputs.insert(input->result_name);
        }
    }

    QueryPlan set_plan;

    {
        SelectQueryInfo set_query_info = reading_step->getQueryInfo();
        if (!set_query_info.table_expression_modifiers)
            return;
        /// Remove FINAL for the set-building read — we want all rows.
        set_query_info.table_expression_modifiers->setHasFinal(false);

        /// Fix prewhere/row_policy DAGs so they don't remove columns from output.
        /// Also expose the derived columns the WHERE filter consumes (see `filter_derived_inputs`),
        /// so the copied WHERE filter below finds its inputs in the reading step's output.
        if (set_query_info.prewhere_info)
        {
            set_query_info.prewhere_info = std::make_shared<PrewhereInfo>(set_query_info.prewhere_info->clone());
            set_query_info.prewhere_info->prewhere_actions = cloneFilterSubDAG(
                set_query_info.prewhere_info->prewhere_actions, set_query_info.prewhere_info->prewhere_column_name);
            exposeNodesAsDAGOutputs(set_query_info.prewhere_info->prewhere_actions, filter_derived_inputs);
            /// Keep the prewhere predicate column in the output when the copied WHERE filter still
            /// consumes it. `splitAndFillPrewhereInfo` flips `remove_prewhere_column` to false in the
            /// single-conjunct case where the residual WHERE references the pushed predicate; forcing
            /// removal here would erase that column and the WHERE could not resolve its input. Check
            /// ALL of the WHERE's inputs, not only derived ones: the pushed predicate can be a plain
            /// storage column (e.g. `WHERE flag AND value != 7`), which is not in `filter_derived_inputs`.
            set_query_info.prewhere_info->remove_prewhere_column
                = !filter_all_inputs.contains(set_query_info.prewhere_info->prewhere_column_name);
        }
        if (set_query_info.row_level_filter)
        {
            auto fixed = std::make_shared<FilterDAGInfo>();
            fixed->actions = cloneFilterSubDAG(set_query_info.row_level_filter->actions, set_query_info.row_level_filter->column_name);
            exposeNodesAsDAGOutputs(fixed->actions, filter_derived_inputs);
            fixed->column_name = set_query_info.row_level_filter->column_name;
            /// Same reasoning as `remove_prewhere_column` above: keep the row-level-filter column
            /// when the copied WHERE still consumes it, checking all of the WHERE's inputs.
            fixed->do_remove_column = !filter_all_inputs.contains(fixed->column_name);
            set_query_info.row_level_filter = std::move(fixed);
        }

        auto set_reading = std::make_unique<ReadFromMergeTree>(
            parts_for_set,
            mutations_snapshot,
            set_columns,
            data,
            data.getSettings(),
            set_query_info,
            storage_snapshot,
            context,
            reading_step->getMaxBlockSize(),
            reading_step->getNumStreams(),
            max_block_numbers_to_read,
            getLogger("optimizeLazyFinal"),
            /*analyzed_result_ptr=*/ nullptr,
            /*enable_parallel_reading=*/ false);

        /// This is an internal read — don't pollute or use the query condition cache.
        set_reading->disableQueryConditionCache();

        set_plan.addStep(std::move(set_reading));
    }

    /// Copy the filter on top if we have one.
    /// The original FilterStep DAG may rename columns (e.g. `value` → `__table1.value`),
    /// which we don't want. Extract just the filter computation via `cloneSubDAG`
    /// and add all inputs as pass-through outputs so columns flow through unchanged.
    if (filter_step)
    {
        const auto & filter_dag = filter_step->getExpression();
        const auto * filter_node = &filter_dag.findInOutputs(filter_step->getFilterColumnName());
        auto sub_dag = ActionsDAG::cloneSubDAG({filter_node}, /*remove_aliases=*/ false);

        /// Add all inputs as outputs so existing columns pass through.
        const auto * filter_output = sub_dag.getOutputs().front();
        for (const auto * input : sub_dag.getInputs())
            if (input != filter_output)
                sub_dag.getOutputs().push_back(input);

        set_plan.addStep(std::make_unique<FilterStep>(
            set_plan.getCurrentHeader(),
            std::move(sub_dag),
            filter_step->getFilterColumnName(),
            /*remove_filter_column=*/ true));
    }

    /// Compute primary key expression and project to PK columns only.
    /// Add all header columns as inputs so that unused ones are properly consumed
    /// and can be dropped by tryRemoveUnusedColumns.
    {
        auto dag = primary_key_dag.clone();
        NamesWithAliases projection;
        for (const auto & col : primary_key.column_names)
            projection.emplace_back(col, "");
        dag.project(projection);

        NameSet dag_inputs;
        for (const auto * input : dag.getInputs())
            dag_inputs.insert(input->result_name);
        for (const auto & col : *set_plan.getCurrentHeader())
            if (!dag_inputs.contains(col.name))
                dag.addInput(col.name, col.type);

        set_plan.addStep(std::make_unique<ExpressionStep>(set_plan.getCurrentHeader(), std::move(dag)));
    }

    /// CreatingSetStep fills the Set from the pipeline.
    set_plan.addStep(std::make_unique<CreatingSetStep>(
        set_plan.getCurrentHeader(),
        set_and_key,
        SizeLimits{},
        nullptr));

    /// The per-partition pre-deduplication for set builds (see `optimizeCreatingSetPerPartition`) is
    /// scoped to `IN (subquery)` set fills; this internal set build has its own BREAK-mode size limits
    /// above, so keep it out.
    auto set_plan_optimization_settings = optimization_settings;
    set_plan_optimization_settings.creating_set_partitions_independently = false;
    set_plan.optimize(set_plan_optimization_settings);

    /// Shared state between LazyFinalKeyAnalysisTransform and LazyReadReplacingFinalSource.
    auto shared_state = std::make_shared<LazyFinalSharedState>();

    /// Guaranteed by the split: only BuildLazyBranch reaches here, and every return of it is either
    /// below the `lazy_branch_available` gate or itself guarded by it.
    chassert(pre_final_filters.has_value());

    /// Builds the ReadFromMergeTree step with IN-set filter, runs index analysis,
    /// checks if enough marks were filtered, and signals.
    auto analysis_step = std::make_unique<LazyFinalKeyAnalysisStep>(
        set_plan.getCurrentHeader(),
        future_set,
        shared_state,
        metadata_snapshot,
        mutations_snapshot,
        storage_snapshot,
        data.getSettings(),
        data,
        max_block_numbers_to_read,
        parts_for_set,
        context,
        optimization_settings.min_filtered_ratio_for_lazy_final,
        std::move(*pre_final_filters));
    auto * analysis_step_ptr = analysis_step.get();
    set_plan.addStep(std::move(analysis_step));

    /// True branch (signal = set OK): LazyReadReplacingFinalSource + JoinLazyColumnsStep.
    QueryPlan true_plan;
    {
        true_plan.addStep(std::make_unique<LazyReadReplacingFinalStep>(
            metadata_snapshot,
            data,
            context,
            shared_state,
            analysis_step_ptr));

        auto lazy_materializing_rows = std::make_shared<LazyMaterializingRows>(*parts_for_set);

        /// Read all original columns lazily, plus columns needed by prewhere/row_policy
        /// which are applied as FilterSteps on top of the join.
        Names lazy_columns = reading_step->getAllColumnNames();
        {
            NameSet existing(lazy_columns.begin(), lazy_columns.end());
            const auto & qi = reading_step->getQueryInfo();
            if (const auto & prewhere = qi.prewhere_info)
                for (const auto * input : prewhere->prewhere_actions.getInputs())
                    if (existing.insert(input->result_name).second)
                        lazy_columns.push_back(input->result_name);
            if (const auto & row_filter = qi.row_level_filter)
                for (const auto * input : row_filter->actions.getInputs())
                    if (existing.insert(input->result_name).second)
                        lazy_columns.push_back(input->result_name);
        }
        auto lazy_header = std::make_shared<const Block>(
            storage_snapshot->getSampleBlockForColumns(lazy_columns));

        auto lazy_reading = std::make_unique<LazilyUnorderedReadFromMergeTree>(
            lazy_header,
            reading_step->getMaxBlockSize(),
            mutations_snapshot,
            storage_snapshot,
            data,
            context,
            data.getLogName());
        lazy_reading->setLazyMaterializingRows(lazy_materializing_rows);

        QueryPlan lazy_plan;
        lazy_plan.addStep(std::move(lazy_reading));

        auto join_lazy_columns = std::make_unique<JoinLazyColumnsStep>(
            true_plan.getCurrentHeader(), lazy_plan.getCurrentHeader(), lazy_materializing_rows);
        join_lazy_columns->setPassThrough(true);

        std::vector<QueryPlanPtr> join_plans;
        join_plans.emplace_back(std::make_unique<QueryPlan>(std::move(true_plan)));
        join_plans.emplace_back(std::make_unique<QueryPlan>(std::move(lazy_plan)));
        true_plan = {};
        true_plan.unitePlans(std::move(join_lazy_columns), {std::move(join_plans)});

        /// Apply row policy and prewhere as FilterSteps on top.
        const auto & query_info = reading_step->getQueryInfo();
        if (const auto & row_level_filter = query_info.row_level_filter)
        {
            true_plan.addStep(std::make_unique<FilterStep>(
                true_plan.getCurrentHeader(),
                row_level_filter->actions.clone(),
                row_level_filter->column_name,
                row_level_filter->do_remove_column));
        }
        if (const auto & prewhere_info = query_info.prewhere_info)
        {
            true_plan.addStep(std::make_unique<FilterStep>(
                true_plan.getCurrentHeader(),
                prewhere_info->prewhere_actions.clone(),
                prewhere_info->prewhere_column_name,
                prewhere_info->remove_prewhere_column));
        }
    }

    /// False branch (no signal = set truncated): fallback to original reading step.
    /// The existing FilterStep (if any) stays above InputSelectorStep and applies to both branches.
    /// Save the expected header before moving the step out of the node.
    auto expected_header = reading_step->getOutputHeader();
    QueryPlan false_plan;
    false_plan.addStep(std::move(read_node->step));

    /// Ensure both branches produce the same header (column order may differ).
    auto false_header = false_plan.getCurrentHeader();
    auto true_header = true_plan.getCurrentHeader();
    if (!blocksHaveEqualStructure(*true_header, *false_header))
    {
        auto projection_dag = ActionsDAG::makeConvertingActions(
            true_header->getColumnsWithTypeAndName(),
            false_header->getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Name,
            nullptr);
        true_plan.addStep(std::make_unique<ExpressionStep>(true_header, std::move(projection_dag)));
    }

    /// Wire up: InputSelectorStep(signal=set_plan, true=true_plan, false=false_plan).
    auto input_selector = std::make_unique<InputSelectorStep>(
        set_plan.getCurrentHeader(), false_plan.getCurrentHeader());

    QueryPlan result_plan;
    std::vector<QueryPlanPtr> selector_plans;
    selector_plans.emplace_back(std::make_unique<QueryPlan>(std::move(set_plan)));
    selector_plans.emplace_back(std::make_unique<QueryPlan>(std::move(true_plan)));
    selector_plans.emplace_back(std::make_unique<QueryPlan>(std::move(false_plan)));
    result_plan.unitePlans(std::move(input_selector), {std::move(selector_plans)});

    /// If we split non-intersecting parts, union them with the entire result.
    /// Both true and false branches now handle only intersecting parts.
    if (split_result.non_intersecting_plan)
    {
        auto union_step = std::make_unique<UnionStep>(
            SharedHeaders{result_plan.getCurrentHeader(), split_result.non_intersecting_plan->getCurrentHeader()});

        QueryPlan combined;
        std::vector<QueryPlanPtr> union_plans;
        union_plans.emplace_back(std::make_unique<QueryPlan>(std::move(result_plan)));
        union_plans.emplace_back(std::move(split_result.non_intersecting_plan));
        combined.unitePlans(std::move(union_step), {std::move(union_plans)});
        result_plan = std::move(combined);
    }

    query_plan.replaceNodeWithPlan(read_node, std::move(result_plan), expected_header);
}

}
}
