#include <Columns/ColumnConst.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/PreparedSets.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/InputSelectorStep.h>
#include <Processors/QueryPlan/JoinLazyColumnsStep.h>
#include <Processors/QueryPlan/LazilyReadFromMergeTree.h>
#include <Processors/QueryPlan/LazilyUnorderedReadFromMergeTree.h>
#include <Processors/QueryPlan/LazyFinalKeyAnalysisStep.h>
#include <Processors/QueryPlan/LazyReadReplacingFinalStep.h>
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

namespace DB::QueryPlanOptimizations
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

    const auto * zero_node = &dag.addColumn(
        ColumnWithTypeAndName(DataTypeUInt8().createColumnConst(1, Field(UInt8(0))), std::make_shared<DataTypeUInt8>(), "__is_deleted_zero"));

    auto equals_func = FunctionFactory::instance().get("equals", context);
    const auto * filter_node = &dag.addFunction(equals_func, {col_node, zero_node}, "__is_deleted_filter");

    dag.getOutputs().push_back(filter_node);

    plan.addStep(std::make_unique<FilterStep>(
        plan.getCurrentHeader(), std::move(dag), "__is_deleted_filter", /*remove_filter_column=*/ true));
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
    std::unique_ptr<QueryPlan> non_intersecting_plan;
    bool fully_replaced = false; /// True when all parts are non-intersecting and the plan was replaced in-place.
};

/// Try to split parts into non-intersecting and intersecting by primary key.
/// If all parts are non-intersecting, replaces the plan node directly and returns fully_replaced=true.
/// Otherwise returns a plan for non-intersecting parts (or nullptr if none), and updates
/// the reading step's analyzed result to contain only intersecting parts.
static SplitResult trySplitNonIntersectingParts(
    ReadFromMergeTree * reading_step,
    ReadFromMergeTree::AnalysisResultPtr analyzed_result,
    FilterStep * filter_step,
    QueryPlan::Node * read_node,
    QueryPlan & query_plan)
{
    const auto & metadata_snapshot = reading_step->getStorageMetadata();
    const auto & primary_key = metadata_snapshot->getPrimaryKey();
    const auto & sorting_key = metadata_snapshot->getSortingKey();

    if (!isSafePrimaryKey(primary_key))
        return {};

    bool in_reverse_order = false;
    if (!sorting_key.reverse_flags.empty())
    {
        size_t num_pk = primary_key.expression_list_ast->children.size();
        in_reverse_order = sorting_key.reverse_flags[0];
        for (size_t i = 1; i < num_pk && i < sorting_key.reverse_flags.size(); ++i)
        {
            if (in_reverse_order != sorting_key.reverse_flags[i])
                return {};
        }
    }

    auto split = splitPartsRanges(reading_step->getParts(), in_reverse_order, getLogger("optimizeLazyFinal"));

    if (split.intersecting_parts_ranges.empty())
    {
        /// All parts are non-intersecting — no FINAL needed at all.
        auto plan = createNonIntersectingPlan(
            std::move(split.non_intersecting_parts_ranges), reading_step, filter_step);

        if (!plan)
            return {};

        auto expected_header = reading_step->getOutputHeader();
        query_plan.replaceNodeWithPlan(read_node, std::move(*plan), expected_header);
        return {.non_intersecting_plan = nullptr, .fully_replaced = true};
    }

    if (split.non_intersecting_parts_ranges.empty())
        return {};

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

    if (!plan)
        return {};

    return {.non_intersecting_plan = std::make_unique<QueryPlan>(std::move(*plan))};
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

    /// Skip if projection was applied.
    if (reading_step->getAnalyzedResult())
        return;

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

    /// Run early index analysis so the analyzed (PK-filtered) parts can be used
    /// both for the non-intersecting split and for the set/true-branch plans.
    /// The WHERE filter was already pushed by optimizePrimaryKeyConditionAndLimit,
    /// so selectRangesToRead uses the PK condition for index analysis.
    auto analyzed_result = reading_step->selectRangesToRead();
    if (reading_step->getParts().empty())
        return;

    /// Split parts into non-intersecting (unique key ranges, no FINAL needed) and
    /// intersecting (overlapping, need FINAL). This avoids running the expensive
    /// aggregation-based FINAL on parts that have no duplicates.
    /// When all parts are non-intersecting, replaceNodeWithPlan is called inside
    /// and fully_replaced is set — in that case we're done.
    auto split_result = trySplitNonIntersectingParts(reading_step, analyzed_result, filter_step, read_node, query_plan);

    if (split_result.fully_replaced)
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
    Names set_columns = reading_step->getAllColumnNames();
    {
        NameSet existing(set_columns.begin(), set_columns.end());

        auto add_columns = [&](const ActionsDAG & dag)
        {
            for (const auto * input : dag.getInputs())
                if (existing.insert(input->result_name).second)
                    set_columns.push_back(input->result_name);
        };

        add_columns(primary_key_dag);
        if (filter_step)
            add_columns(filter_step->getExpression());
        if (const auto & prewhere = reading_step->getQueryInfo().prewhere_info)
            add_columns(prewhere->prewhere_actions);
        if (const auto & row_filter = reading_step->getQueryInfo().row_level_filter)
            add_columns(row_filter->actions);
    }

    QueryPlan set_plan;

    {
        SelectQueryInfo set_query_info = reading_step->getQueryInfo();
        if (!set_query_info.table_expression_modifiers)
            return;
        /// Remove FINAL for the set-building read — we want all rows.
        set_query_info.table_expression_modifiers->setHasFinal(false);

        /// Fix prewhere/row_policy DAGs so they don't remove columns from output.
        if (set_query_info.prewhere_info)
        {
            set_query_info.prewhere_info = std::make_shared<PrewhereInfo>(set_query_info.prewhere_info->clone());
            set_query_info.prewhere_info->prewhere_actions = cloneFilterSubDAG(
                set_query_info.prewhere_info->prewhere_actions, set_query_info.prewhere_info->prewhere_column_name);
            set_query_info.prewhere_info->remove_prewhere_column = true;
        }
        if (set_query_info.row_level_filter)
        {
            auto fixed = std::make_shared<FilterDAGInfo>();
            fixed->actions = cloneFilterSubDAG(set_query_info.row_level_filter->actions, set_query_info.row_level_filter->column_name);
            fixed->column_name = set_query_info.row_level_filter->column_name;
            fixed->do_remove_column = true;
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

    set_plan.optimize(optimization_settings);

    /// Shared state between LazyFinalKeyAnalysisTransform and LazyReadReplacingFinalSource.
    auto shared_state = std::make_shared<LazyFinalSharedState>();

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
        optimization_settings.min_filtered_ratio_for_lazy_final);
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
