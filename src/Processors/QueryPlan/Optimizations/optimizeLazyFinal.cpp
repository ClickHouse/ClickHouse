#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/PreparedSets.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/InputSelectorStep.h>
#include <Processors/QueryPlan/JoinLazyColumnsStep.h>
#include <Processors/QueryPlan/LazilyReadFromMergeTree.h>
#include <Processors/QueryPlan/LazyFinalKeyAnalysisStep.h>
#include <Processors/QueryPlan/LazyReadReplacingFinalStep.h>
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

void optimizeLazyFinal(const Stack & stack, QueryPlan & query_plan, QueryPlan::Nodes & nodes, const QueryPlanOptimizationSettings & optimization_settings)
{
    /// Skip if already inside an InputSelectorStep (avoids infinite re-application).
    for (size_t i = stack.size(); i >= 2 && i > stack.size() - 2; --i)
        if (typeid_cast<InputSelectorStep *>(stack[i - 2].node->step.get()))
            return;

    /// Look for ReadFromMergeTree with FINAL, optionally preceded by a FilterStep.
    auto * top_node = stack.back().node;
    FilterStep * filter_step = nullptr;

    if (auto * f = typeid_cast<FilterStep *>(top_node->step.get()))
    {
        if (top_node->children.size() != 1)
            return;
        filter_step = f;
        top_node = top_node->children.front();
    }

    auto * read_node = top_node;
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

    /// We need either a filter or prewhere/row_policy to make this worthwhile.
    if (!filter_step && !reading_step->getPrewhereInfo() && !reading_step->getRowLevelFilter())
        return;

    const auto & metadata_snapshot = reading_step->getStorageMetadata();
    const auto & sorting_key = metadata_snapshot->getSortingKey();
    const auto & context = reading_step->getContext();
    const auto & storage_snapshot = reading_step->getStorageSnapshot();
    auto mutations_snapshot = reading_step->getMutationsSnapshot();
    auto max_block_numbers_to_read = getMaxAddedBlocks(reading_step);

    /// Create a Set with BREAK overflow mode so we can detect truncation.
    SizeLimits set_size_limits(optimization_settings.max_rows_for_lazy_final, optimization_settings.max_bytes_for_lazy_final, OverflowMode::BREAK);
    auto set = std::make_shared<Set>(set_size_limits, /*max_elements_to_fill=*/ 0, /*transform_null_in=*/ false);
    set->setHeader(sorting_key.sample_block.cloneWithColumns(sorting_key.sample_block.cloneEmptyColumns()).getColumnsWithTypeAndName());
    auto set_and_key = std::make_shared<SetAndKey>(SetAndKey{.key = "__lazy_final_set", .set = set, .external_table = nullptr});

    /// Use FutureSetFromStorage — the Set will be filled by CreatingSetStep before
    /// LazyFinalKeyAnalysisTransform runs (they are in the same pipeline).
    auto future_set = std::make_shared<FutureSetFromStorage>(FutureSet::Hash{}, /*ast=*/ nullptr, set, /*storage_id=*/ std::nullopt);

    /// Build the set-building sub-plan: read columns needed for predicates
    /// (prewhere, row policy, filter) and sorting key, then project to key columns.

    const auto & sorting_key_dag = sorting_key.expression->getActionsDAG();

    /// Start with the original column set (covers prewhere, row policy, etc.),
    /// then append sorting key source columns, filter input columns,
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

        add_columns(sorting_key_dag);
        if (filter_step)
            add_columns(filter_step->getExpression());
        if (const auto & prewhere = reading_step->getQueryInfo().prewhere_info)
            add_columns(prewhere->prewhere_actions);
        if (const auto & row_filter = reading_step->getQueryInfo().row_level_filter)
            add_columns(row_filter->actions);
    }

    {
        String cols;
        for (const auto & c : set_columns)
            cols += c + ", ";
        String sk_inputs;
        for (const auto * input : sorting_key_dag.getInputs())
            sk_inputs += input->result_name + ", ";
        LOG_DEBUG(getLogger("optimizeLazyFinal"), "set_columns: [{}], sorting_key_dag inputs: [{}]", cols, sk_inputs);
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
            std::make_shared<RangesInDataParts>(reading_step->getParts()),
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

        /// Apply primary key analysis: add filters from prewhere, row policy, and the FilterStep.
        /// Without FINAL, PK analysis can be more aggressive.
        if (const auto & row_level_filter = set_query_info.row_level_filter)
            set_reading->addFilter(row_level_filter->actions.clone(), row_level_filter->column_name);
        if (const auto & prewhere_info = set_query_info.prewhere_info)
            set_reading->addFilter(prewhere_info->prewhere_actions.clone(), prewhere_info->prewhere_column_name);
        if (filter_step)
            set_reading->addFilter(filter_step->getExpression().clone(), filter_step->getFilterColumnName());
        set_reading->SourceStepWithFilterBase::applyFilters();

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

        LOG_DEBUG(getLogger("optimizeLazyFinal"), "sub_dag:\n{}", sub_dag.dumpDAG());
        LOG_DEBUG(getLogger("optimizeLazyFinal"), "set_plan header: {}", set_plan.getCurrentHeader()->dumpStructure());

        set_plan.addStep(std::make_unique<FilterStep>(
            set_plan.getCurrentHeader(),
            std::move(sub_dag),
            filter_step->getFilterColumnName(),
            /*remove_filter_column=*/ true));

        LOG_DEBUG(getLogger("optimizeLazyFinal"), "after filter header: {}", set_plan.getCurrentHeader()->dumpStructure());
    }

    /// Compute sorting key expression and project to key columns only.
    /// Add all header columns as inputs so that unused ones are properly consumed
    /// and can be dropped by tryRemoveUnusedColumns.
    {
        auto dag = sorting_key_dag.clone();
        NameSet dag_inputs;
        for (const auto * input : dag.getInputs())
            dag_inputs.insert(input->result_name);
        for (const auto & col : *set_plan.getCurrentHeader())
            if (!dag_inputs.contains(col.name))
                dag.addInput(col.name, col.type);

        NamesWithAliases projection;
        for (const auto & col : sorting_key.column_names)
            projection.emplace_back(col, "");
        dag.project(projection);
        set_plan.addStep(std::make_unique<ExpressionStep>(set_plan.getCurrentHeader(), std::move(dag)));
    }

    {
        WriteBufferFromOwnString out;
        set_plan.explainPlan(out, {.header = true, .actions = true});
        LOG_DEBUG(getLogger("optimizeLazyFinal"), "set_plan before tryRemoveUnusedColumns:\n{}", out.str());
    }

    /// Remove columns from ReadFromMergeTree that are not needed by the steps above.
    {
        Optimization::ExtraSettings extra{};
        tryRemoveUnusedColumns(set_plan.getRootNode(), nodes, extra);
    }

    /// CreatingSetStep fills the Set from the pipeline.
    set_plan.addStep(std::make_unique<CreatingSetStep>(
        set_plan.getCurrentHeader(),
        set_and_key,
        SizeLimits{},
        nullptr));

    /// LazyFinalKeyAnalysisStep: uses the set for index analysis, outputs signal.
    auto ranges_ptr = std::make_shared<RangesInDataParts>(reading_step->getParts());
    set_plan.addStep(std::make_unique<LazyFinalKeyAnalysisStep>(
        set_plan.getCurrentHeader(),
        future_set,
        context,
        metadata_snapshot,
        *ranges_ptr));

    /// True branch (signal = set OK): LazyReadReplacingFinalSource + JoinLazyColumnsStep.
    QueryPlan true_plan;
    {
        true_plan.addStep(std::make_unique<LazyReadReplacingFinalStep>(
            metadata_snapshot,
            mutations_snapshot,
            storage_snapshot,
            data.getSettings(),
            data,
            max_block_numbers_to_read,
            ranges_ptr,
            context));

        auto lazy_materializing_rows = std::make_shared<LazyMaterializingRows>(reading_step->getParts());

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
        {
            String cols;
            for (const auto & c : lazy_columns)
                cols += c + ", ";
            LOG_DEBUG(getLogger("optimizeLazyFinal"), "lazy_columns: [{}]", cols);
        }
        auto lazy_header = std::make_shared<const Block>(
            storage_snapshot->getSampleBlockForColumns(lazy_columns));

        auto lazy_reading = std::make_unique<LazilyReadFromMergeTree>(
            lazy_header,
            reading_step->getMaxBlockSize(),
            /*min_marks_for_concurrent_read=*/ 0,
            MergeTreeReaderSettings::createFromContext(context),
            mutations_snapshot,
            storage_snapshot,
            context,
            data.getLogName());
        lazy_reading->setLazyMaterializingRows(lazy_materializing_rows);

        QueryPlan lazy_plan;
        lazy_plan.addStep(std::move(lazy_reading));

        auto join_lazy_columns = std::make_unique<JoinLazyColumnsStep>(
            true_plan.getCurrentHeader(), lazy_plan.getCurrentHeader(), lazy_materializing_rows);

        std::vector<QueryPlanPtr> join_plans;
        join_plans.emplace_back(std::make_unique<QueryPlan>(std::move(true_plan)));
        join_plans.emplace_back(std::make_unique<QueryPlan>(std::move(lazy_plan)));
        true_plan = {};
        true_plan.unitePlans(std::move(join_lazy_columns), {std::move(join_plans)});

        /// Apply row policy and prewhere as FilterSteps on top.
        /// Use cloneFilterSubDAG to avoid column renames/removals from the original DAGs.
        const auto & query_info = reading_step->getQueryInfo();
        if (const auto & row_level_filter = query_info.row_level_filter)
        {
            true_plan.addStep(std::make_unique<FilterStep>(
                true_plan.getCurrentHeader(),
                cloneFilterSubDAG(row_level_filter->actions, row_level_filter->column_name),
                row_level_filter->column_name,
                /*remove_filter_column=*/ true));
        }
        if (const auto & prewhere_info = query_info.prewhere_info)
        {
            true_plan.addStep(std::make_unique<FilterStep>(
                true_plan.getCurrentHeader(),
                cloneFilterSubDAG(prewhere_info->prewhere_actions, prewhere_info->prewhere_column_name),
                prewhere_info->prewhere_column_name,
                /*remove_filter_column=*/ true));
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

    query_plan.replaceNodeWithPlan(read_node, std::move(result_plan), expected_header);
}

}
