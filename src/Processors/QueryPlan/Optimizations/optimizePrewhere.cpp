#include <algorithm>
#include <Core/Block.h>
#include <Core/Names.h>
#include <Core/Settings.h>
#include <Functions/FunctionsLogical.h>
#include <Functions/IFunctionAdaptors.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Cache/QueryConditionCache.h>
#include <Interpreters/Context.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/optimizePrewhere.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>
#include <Storages/MergeTree/MergeTreeWhereOptimizer.h>
#include <Storages/StorageDummy.h>
#include <Storages/StorageMerge.h>
#include <Storages/VirtualColumnUtils.h>
#include <Common/Exception.h>
#include <Common/thread_local_rng.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_calculating_subcolumns_sizes_for_merge_tree_reading;
    extern const SettingsBool optimize_move_to_prewhere;
    extern const SettingsBool optimize_move_to_prewhere_if_final;
    extern const SettingsBool optimize_move_to_prewhere_if_final_non_intersecting;
    extern const SettingsBool optimize_prewhere_after_pushdown;
    extern const SettingsBool use_query_condition_cache;
    extern const SettingsBool vector_search_with_rescoring;
}

namespace QueryPlanOptimizations
{

static void removeFromOutput(ActionsDAG & dag, const std::string name)
{
    const auto * node = &dag.findInOutputs(name);
    auto & outputs = dag.getOutputs();
    for (size_t i = 0; i < outputs.size(); ++i)
    {
        if (node == outputs[i])
        {
            outputs.erase(outputs.begin() + i);
            return;
        }
    }
}

static bool shouldSuppressPrewhereForVectorSearch(const ReadFromMergeTree & read_from_merge_tree_step, const Settings & settings)
{
    if (!read_from_merge_tree_step.getVectorSearchParameters().has_value())
        return false;

    if (read_from_merge_tree_step.isParallelReadingFromReplicas())
        return false;

    auto analyzed_result = read_from_merge_tree_step.getAnalyzedResult();
    analyzed_result = analyzed_result ? analyzed_result : read_from_merge_tree_step.selectRangesToRead();
    if (!analyzed_result)
        return false;

    if (settings[Setting::vector_search_with_rescoring])
    {
        if (read_from_merge_tree_step.isQueryWithFinal())
            return false;

        for (const auto & part_with_ranges : analyzed_result->parts_with_ranges)
        {
            if (!part_with_ranges.ranges.empty() && !part_with_ranges.read_hints.vector_search_results.has_value())
                return false;
        }

        return true;
    }

    for (const auto & part_with_ranges : analyzed_result->parts_with_ranges)
    {
        if (!part_with_ranges.ranges.empty()
            && (!part_with_ranges.read_hints.vector_search_results.has_value()
                || !part_with_ranges.read_hints.vector_search_results->distances.has_value()))
        {
            return false;
        }
    }

    return true;
}

ActionsDAG splitAndFillPrewhereInfo(
    PrewhereInfoPtr & prewhere_info,
    bool remove_prewhere_column,
    ActionsDAG filter_expression,
    const String & filter_column_name,
    const std::unordered_set<const ActionsDAG::Node *> & prewhere_nodes,
    const std::list<const ActionsDAG::Node *> & prewhere_nodes_list)
{
    prewhere_info->need_filter = true;
    prewhere_info->remove_prewhere_column = remove_prewhere_column;

    if (prewhere_info->remove_prewhere_column)
    {
        removeFromOutput(filter_expression, filter_column_name);
        auto & outputs = filter_expression.getOutputs();
        size_t size = outputs.size();
        outputs.insert(outputs.end(), prewhere_nodes.begin(), prewhere_nodes.end());
        filter_expression.removeUnusedActions(false);
        outputs.resize(size);
    }

    auto split_result = filter_expression.split(prewhere_nodes, true, true);

    /// This is the leak of abstraction.
    /// Split actions may have inputs which are needed only for PREWHERE.
    /// This is fine for ActionsDAG to have such a split, but it breaks defaults calculation.
    ///
    /// See 00950_default_prewhere for example.
    /// Table has structure `APIKey UInt8, SessionType UInt8` and default `OperatingSystem = SessionType+1`
    /// For a query with `SELECT OperatingSystem WHERE APIKey = 42 AND SessionType = 42` we push everything to PREWHERE
    /// and columns APIKey, SessionType are removed from inputs (cause only OperatingSystem is needed).
    /// However, column OperatingSystem is calculated after PREWHERE stage, based on SessionType value.
    /// If column SessionType is removed by PREWHERE actions, we use zero as default, and get a wrong result.
    ///
    /// So, here we restore removed inputs for PREWHERE actions
    {
        std::unordered_set<const ActionsDAG::Node *> first_outputs(
            split_result.first.getOutputs().begin(), split_result.first.getOutputs().end());
        for (const auto * input : split_result.first.getInputs())
        {
            if (!first_outputs.contains(input))
            {
                split_result.first.getOutputs().push_back(input);
                /// Add column to second actions as input.
                /// Do not add it to result, so it would be removed.
                split_result.second.addInput(input->result_name, input->result_type);
            }
        }
    }

    ActionsDAG::NodeRawConstPtrs conditions;
    conditions.reserve(split_result.split_nodes_mapping.size());
    for (const auto * condition : prewhere_nodes_list)
        conditions.push_back(split_result.split_nodes_mapping.at(condition));

    prewhere_info->prewhere_actions = std::move(split_result.first);

    if (conditions.size() == 1)
    {
        prewhere_info->prewhere_column_name = conditions.front()->result_name;
        if (prewhere_info->remove_prewhere_column)
        {
            /// If the prewhere column is removed, then let's add it back. If it is already in the outputs,
            /// that means the prewhere column is needed later, so let's keep it.
            auto & outputs = prewhere_info->prewhere_actions.getOutputs();
            if (std::find(outputs.begin(), outputs.end(), conditions.front()) == outputs.end())
                outputs.push_back(conditions.front());
            else
                prewhere_info->remove_prewhere_column = false;
        }
    }
    else
    {
        prewhere_info->remove_prewhere_column = true;

        FunctionOverloadResolverPtr func_builder_and = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionAnd>());
        const auto * node = &prewhere_info->prewhere_actions.addFunction(func_builder_and, std::move(conditions), {});
        prewhere_info->prewhere_column_name = node->result_name;
        prewhere_info->prewhere_actions.getOutputs().push_back(node);
    }

    return std::move(split_result.second);
}

/// Whether the condition can be evaluated on rows that the conditions before it in the WHERE would drop: none of its
/// functions can throw. The reader may evaluate several conditions of PREWHERE at once, so their order is not enough.
static bool canBeEvaluatedOnMoreRows(const ActionsDAG::Node * condition)
{
    std::vector<const ActionsDAG::Node *> stack{condition};
    while (!stack.empty())
    {
        const auto * node = stack.back();
        stack.pop_back();

        if (node->type == ActionsDAG::ActionType::ARRAY_JOIN || node->type == ActionsDAG::ActionType::PLACEHOLDER)
            return false;

        if (node->type == ActionsDAG::ActionType::FUNCTION && !node->column)
        {
            DataTypesWithConstInfo arguments;
            arguments.reserve(node->children.size());
            for (const auto * child : node->children)
                arguments.push_back({child->result_type, child->column != nullptr});
            if (node->function_base->canThrow(arguments) || node->function_base->hasObservableSideEffects())
                return false;
        }

        stack.insert(stack.end(), node->children.begin(), node->children.end());
    }
    return true;
}

/// The filter that a `FINAL` read applies as PREWHERE to the key ranges it reads without merging: the conjunction of
/// `conditions` except the ones in `moved_to_prewhere`, which the PREWHERE of all ranges already has.
/// The filter keeps all its inputs in the output, so it only drops rows and does not change the header of the read.
/// Returns nullptr if there is nothing left to apply.
static FilterDAGInfoPtr buildNonIntersectingFinalFilter(
    const std::list<const ActionsDAG::Node *> & conditions,
    const std::unordered_set<const ActionsDAG::Node *> & moved_to_prewhere)
{
    ActionsDAG::NodeRawConstPtrs remaining;
    for (const auto * condition : conditions)
        if (!moved_to_prewhere.contains(condition))
            remaining.push_back(condition);

    if (remaining.empty())
        return nullptr;

    auto dag = ActionsDAG::cloneSubDAG(remaining, /*remove_aliases=*/ false);
    const ActionsDAG::Node * filter_node = nullptr;
    if (dag.getOutputs().size() == 1)
    {
        filter_node = dag.getOutputs().front();
    }
    else
    {
        FunctionOverloadResolverPtr func_builder_and = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionAnd>());
        filter_node = &dag.addFunction(func_builder_and, dag.getOutputs(), {});
    }

    auto filter = std::make_shared<FilterDAGInfo>();
    filter->column_name = filter_node->result_name;
    /// A filter that is a column itself (`WHERE flag`) has to stay in the output.
    filter->do_remove_column = filter_node->type != ActionsDAG::ActionType::INPUT;

    auto & outputs = dag.getOutputs();
    outputs.clear();
    if (filter->do_remove_column)
        outputs.push_back(filter_node);
    for (const auto * input : dag.getInputs())
        outputs.push_back(input);

    filter->actions = std::move(dag);
    return filter;
}

/// Hash of `node` that does not depend on the names the node reads through.
static std::optional<UInt64> getNormalizedHash(const ActionsDAG::Node * node)
{
    auto dag = ActionsDAG::cloneSubDAG({node}, /*remove_aliases=*/ true);
    for (const auto & dag_node : dag.getNodes())
        if (dag_node.type == ActionsDAG::ActionType::ARRAY_JOIN || dag_node.type == ActionsDAG::ActionType::PLACEHOLDER)
            return {};

    auto normalized = ActionsDAG::buildFilterActionsDAG(dag.getOutputs(), {});
    if (!normalized || normalized->getOutputs().size() != 1)
        return {};
    return normalized->getOutputs()[0]->getHash();
}

static void setQueryConditionCacheAttribution(
    PrewhereInfo & prewhere_info, const ReadFromMergeTree & read_from_merge_tree_step, const Settings & settings)
{
    if (!settings[Setting::use_query_condition_cache] || read_from_merge_tree_step.isSelectedForTopKFilterOptimization()
        || read_from_merge_tree_step.getVectorSearchParameters().has_value())
        return;

    const auto & query_info = read_from_merge_tree_step.getQueryInfo();
    const auto & filter_actions_dag = query_info.filter_actions_dag;
    if (!filter_actions_dag || query_info.isFinal()
        || ReadFromMergeTree::filterDependsOnNonDeterministicVirtuals(read_from_merge_tree_step.getStorageMetadata()->virtuals, query_info))
        return;

    const auto & outputs = filter_actions_dag->getOutputs();
    if (outputs.size() != 1 || !VirtualColumnUtils::isDeterministic(outputs.front()))
        return;

    const auto & condition_root = prewhere_info.prewhere_actions.findInOutputs(prewhere_info.prewhere_column_name);
    if (condition_root.type != ActionsDAG::ActionType::FUNCTION || condition_root.function_base->getName() != "and")
        return;

    std::unordered_set<UInt64> filter_atom_hashes;
    for (const auto * atom : ActionsDAG::extractConjunctionAtoms(outputs.front()))
        if (auto hash = getNormalizedHash(atom))
            filter_atom_hashes.insert(*hash);

    std::vector<UInt64> conjunct_hashes;
    for (const auto * conjunct : condition_root.children)
    {
        if (!VirtualColumnUtils::isDeterministic(conjunct))
            break;
        auto hash = getNormalizedHash(conjunct);
        if (!hash || !filter_atom_hashes.contains(*hash))
            break;
        conjunct_hashes.push_back(conjunct->getHash());
    }

    if (conjunct_hashes.empty() || conjunct_hashes.size() == condition_root.children.size())
        return;

    prewhere_info.query_condition_cache_attribution = PrewhereInfo::QueryConditionCacheAttribution{
        .key = queryConditionCacheHash(outputs.front()->getHash(), queryConditionCacheSettingsSalt(settings)),
        .condition = filter_actions_dag->getNames()[0],
        .conjunct_hashes = std::move(conjunct_hashes),
    };
}

void optimizePrewhere(QueryPlan::Node & parent_node, const bool remove_unused_columns, const bool suppress_for_vector_search)
{
    /// Assume that there are at least 2 nodes:
    /// 1. FilterNode - parent_node
    /// 2. SourceStepWithFilterNode - child_node

    /// TODO: We can also check for UnionStep, such as StorageBuffer and local distributed plans.
    auto * filter_step = typeid_cast<FilterStep *>(parent_node.step.get());
    if (!filter_step)
        return;

    if (parent_node.children.size() != 1)
        return;

    auto * child_node = parent_node.children.front();

    auto * source_step_with_filter = dynamic_cast<SourceStepWithFilter *>(child_node->step.get());
    if (!source_step_with_filter)
        return;

    if (typeid_cast<ReadFromMerge *>(child_node->step.get()))
        return;

    const auto & storage_snapshot = source_step_with_filter->getStorageSnapshot();
    const auto & storage = storage_snapshot->storage;
    if (!storage.canMoveConditionsToPrewhere())
        return;

    const auto & context = source_step_with_filter->getContext();
    const auto & settings = context->getSettingsRef();

    PrewhereInfoPtr existing_prewhere_info = source_step_with_filter->getPrewhereInfo();
    if (existing_prewhere_info
        && (!settings[Setting::optimize_prewhere_after_pushdown] || !source_step_with_filter->canUpdatePrewhereInfoMultipleTimes()))
        return;

    bool is_final = source_step_with_filter->isQueryWithFinal();
    bool optimize = settings[Setting::optimize_move_to_prewhere] && (!is_final || settings[Setting::optimize_move_to_prewhere_if_final]);
    if (!optimize)
        return;

    auto * read_from_merge_tree_step = typeid_cast<ReadFromMergeTree *>(child_node->step.get());

    /// If PREWHERE is deferred after FINAL, moving conditions cannot save any reads, and the moved conditions
    /// would escape the already-made deferral decision and run before a deferred row policy
    if (is_final && read_from_merge_tree_step && read_from_merge_tree_step->isPrewhereDeferredAfterFinal())
        return;

    const auto & queried_columns = source_step_with_filter->requiredSourceColumns();

    /// Candidate parallel-replica plans have not applied filters yet. Keep their existing
    /// column-size estimates until pruning filters or analyzed parts are available.
    const bool use_pruned_parts = read_from_merge_tree_step
        && (read_from_merge_tree_step->getIndexes() || read_from_merge_tree_step->getAnalyzedResult());
    RangesInDataParts prewhere_parts;
    if (use_pruned_parts)
        prewhere_parts = read_from_merge_tree_step->getPartsForPrewhere();
    auto column_sizes = use_pruned_parts
        ? read_from_merge_tree_step->getColumnSizesForPrewhere(queried_columns, prewhere_parts)
        : storage.getColumnSizes(queried_columns, settings[Setting::allow_calculating_subcolumns_sizes_for_merge_tree_reading]);
    if (column_sizes.empty())
        return;

    /// These two optimizations conflict:
    /// - vector search lookups
    /// - PREWHERE
    /// The former is more impactful, therefore disable PREWHERE if the vector
    /// second pass can actually use the vector-search read hints.
    if (suppress_for_vector_search && read_from_merge_tree_step && shouldSuppressPrewhereForVectorSearch(*read_from_merge_tree_step, settings))
        return;

    /// Extract column compressed sizes
    std::unordered_map<std::string, UInt64> column_compressed_sizes;
    for (const auto & [name, sizes] : column_sizes)
        column_compressed_sizes[name] = sizes.data_compressed;

    /// Statistics are only used to reorder conditions, so skip if there is just one.
    const auto & filter_root_node = filter_step->getExpression().findInOutputs(filter_step->getFilterColumnName());
    const bool has_multiple_conditions = filter_root_node.type == ActionsDAG::ActionType::FUNCTION
        && filter_root_node.function_base && filter_root_node.function_base->getName() == "and";

    ConditionSelectivityEstimatorPtr selectivity_estimator;
    if (has_multiple_conditions && read_from_merge_tree_step)
        selectivity_estimator = use_pruned_parts
            ? read_from_merge_tree_step->getConditionSelectivityEstimator(queried_columns, prewhere_parts)
            : read_from_merge_tree_step->getConditionSelectivityEstimatorForPrewhere(queried_columns, &filter_root_node);

    MergeTreeWhereOptimizer where_optimizer{
        std::move(column_compressed_sizes),
        storage_snapshot,
        std::move(selectivity_estimator),
        queried_columns,
        storage.supportedPrewhereColumns(),
        storage.supportedPrewhereColumnsIncludeSubcolumns(),
        getLogger("QueryPlanOptimizePrewhere")};

    /// The existing PREWHERE and the row policy observe the values of the columns they read
    /// before the moved conditions are applied, so these columns cannot be filtered during the scan.
    NameSet columns_read_before_filter;
    if (existing_prewhere_info)
        for (const auto & input : existing_prewhere_info->prewhere_actions.getInputs())
            columns_read_before_filter.insert(input->result_name);
    if (auto row_level_filter = source_step_with_filter->getRowLevelFilter())
        for (const auto & input : row_level_filter->actions.getInputs())
            columns_read_before_filter.insert(input->result_name);
    /// `installTopKDynamicFilter` runs later and prepends `__topKFilter` to PREWHERE, so it reads its column too.
    if (read_from_merge_tree_step && read_from_merge_tree_step->hasPendingTopKDynamicFilter())
        columns_read_before_filter.insert(read_from_merge_tree_step->getTopKFilterInfo()->column_name);

    /// The conditions that a `FINAL` read can apply as PREWHERE to the key ranges it reads without merging. A row of such a range
    /// has no other version, so any deterministic condition gives the same result before and after `FINAL`. They are collected
    /// before the conditions on the sorting key are moved out of the filter below, and stay in the filter.
    /// Not with stateful functions in the WHERE, like `runningDifference`: their results depend on the rows they see.
    std::list<const ActionsDAG::Node *> non_intersecting_final_conditions;
    if (is_final && read_from_merge_tree_step && settings[Setting::optimize_move_to_prewhere_if_final_non_intersecting]
        && !filter_step->getExpression().hasStatefulFunctions())
        non_intersecting_final_conditions = where_optimizer.optimize(filter_step->getExpression(),
            filter_step->getFilterColumnName(),
            source_step_with_filter->getContext(),
            MergeTreeWhereOptimizer::FinalMode::NonIntersecting,
            columns_read_before_filter).prewhere_nodes_list;

    auto optimize_result = where_optimizer.optimize(filter_step->getExpression(),
        filter_step->getFilterColumnName(),
        source_step_with_filter->getContext(),
        is_final ? MergeTreeWhereOptimizer::FinalMode::SortingKeyOnly : MergeTreeWhereOptimizer::FinalMode::None,
        columns_read_before_filter);

    /// The WHERE evaluates a condition only on the rows that passed the conditions before it, e.g. `toUInt64(s) > 0` in
    /// `WHERE s != 'bad' AND toUInt64(s) > 0`. Keep that: apply early the conditions in the order of the WHERE, up to the
    /// first one that is applied neither early on these ranges nor as the PREWHERE of all ranges, or that can throw. The WHERE
    /// may also evaluate a cheap condition before one that can throw (`short_circuit_function_evaluation_reorder_arguments`),
    /// so a condition that can throw is applied early only if it is the only condition, and there is no other PREWHERE.
    if (!non_intersecting_final_conditions.empty())
    {
        ActionsDAG::NodeRawConstPtrs atoms;
        std::vector<const ActionsDAG::Node *> stack{&filter_step->getExpression().findInOutputs(filter_step->getFilterColumnName())};
        while (!stack.empty())
        {
            const auto * node = stack.back();
            stack.pop_back();
            if (node->type == ActionsDAG::ActionType::FUNCTION && node->function_base->getName() == "and")
                stack.insert(stack.end(), node->children.rbegin(), node->children.rend());
            else
                atoms.push_back(node);
        }

        std::unordered_set<const ActionsDAG::Node *> non_intersecting(non_intersecting_final_conditions.begin(), non_intersecting_final_conditions.end());
        non_intersecting_final_conditions.clear();
        const bool has_prewhere = existing_prewhere_info || !optimize_result.prewhere_nodes.empty();
        for (const auto * atom : atoms)
        {
            if (optimize_result.prewhere_nodes.contains(atom))
                continue;
            if (!non_intersecting.contains(atom) || ((atoms.size() != 1 || has_prewhere) && !canBeEvaluatedOnMoreRows(atom)))
                break;
            non_intersecting_final_conditions.push_back(atom);
        }
    }

    /// The filter stays above the read, so a filter built by an earlier call is replaced only by a filter
    /// built from the same or more conditions (see `optimize_prewhere_after_pushdown`).
    /// The read marks the chunks of the non-intersecting ranges with a token, and the filter above it skips
    /// the conditions they already passed for them.
    std::optional<std::pair<UInt64, std::unordered_set<UInt64>>> prefiltered_conditions;
    if (!non_intersecting_final_conditions.empty())
    {
        if (auto filter = buildNonIntersectingFinalFilter(non_intersecting_final_conditions, optimize_result.prewhere_nodes))
        {
            const UInt64 token = thread_local_rng();
            std::unordered_set<UInt64> condition_hashes;
            for (const auto * condition : non_intersecting_final_conditions)
                if (!optimize_result.prewhere_nodes.contains(condition))
                    condition_hashes.insert(condition->getHash());

            read_from_merge_tree_step->setNonIntersectingFinalFilter(std::move(filter), token);
            prefiltered_conditions.emplace(token, std::move(condition_hashes));
        }
    }

    if (optimize_result.prewhere_nodes.empty())
    {
        if (prefiltered_conditions)
            filter_step->setPrefilteredConditions(prefiltered_conditions->first, std::move(prefiltered_conditions->second));
        return;
    }

    PrewhereInfoPtr prewhere_info = std::make_shared<PrewhereInfo>();

    auto remaining_expr = splitAndFillPrewhereInfo(
        prewhere_info,
        optimize_result.fully_moved_to_prewhere && filter_step->removesFilterColumn(),
        std::move(filter_step->getExpression()),
        filter_step->getFilterColumnName(),
        optimize_result.prewhere_nodes,
        optimize_result.prewhere_nodes_list);

    if (!existing_prewhere_info && read_from_merge_tree_step)
        setQueryConditionCacheAttribution(*prewhere_info, *read_from_merge_tree_step, settings);

    if (existing_prewhere_info)
    {
        const bool existing_removable = existing_prewhere_info->remove_prewhere_column;
        const bool new_removable = prewhere_info->remove_prewhere_column;

        ActionsDAG combined = std::move(existing_prewhere_info->prewhere_actions);
        const auto * existing_filter_node = &combined.findInOutputs(existing_prewhere_info->prewhere_column_name);

        ActionsDAG::NodeRawConstPtrs new_outputs_in_combined;
        combined.mergeNodes(std::move(prewhere_info->prewhere_actions), &new_outputs_in_combined);

        const ActionsDAG::Node * new_filter_node = nullptr;
        for (const auto * node : new_outputs_in_combined)
        {
            if (node->result_name == prewhere_info->prewhere_column_name)
            {
                new_filter_node = node;
                break;
            }
        }

        FunctionOverloadResolverPtr func_builder_and
            = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionAnd>());
        const auto * and_node = &combined.addFunction(func_builder_and, {existing_filter_node, new_filter_node}, {});

        auto & outputs = combined.getOutputs();
        for (const auto * node : new_outputs_in_combined)
            if (std::ranges::find(outputs, node) == outputs.end())
                outputs.push_back(node);

        const bool same_filter = existing_filter_node == new_filter_node;
        if (existing_removable && (!same_filter || new_removable))
            std::erase(outputs, existing_filter_node);
        if (new_removable && !same_filter)
            std::erase(outputs, new_filter_node);

        outputs.push_back(and_node);

        prewhere_info->prewhere_actions = std::move(combined);
        prewhere_info->prewhere_column_name = and_node->result_name;
        prewhere_info->remove_prewhere_column = true;
        prewhere_info->need_filter = existing_prewhere_info->need_filter || prewhere_info->need_filter;
    }

    source_step_with_filter->updatePrewhereInfo(prewhere_info);

    QueryPlanStepPtr new_step;
    if (!optimize_result.fully_moved_to_prewhere)
    {
        auto new_filter_step = std::make_unique<FilterStep>(
            source_step_with_filter->getOutputHeader(),
            std::move(remaining_expr),
            filter_step->getFilterColumnName(),
            filter_step->removesFilterColumn());
        if (prefiltered_conditions)
            new_filter_step->setPrefilteredConditions(prefiltered_conditions->first, std::move(prefiltered_conditions->second));
        new_step = std::move(new_filter_step);
    }
    else
    {
        /// Have to keep this expression to change column names to column identifiers
        new_step = std::make_unique<ExpressionStep>(
            source_step_with_filter->getOutputHeader(),
            std::move(remaining_expr));
    }

    new_step->setStepDescription(*filter_step);
    parent_node.step = std::move(new_step);

    if (!remove_unused_columns)
        return;

    /// Keep the outputs as they are, and prune what the new step and the read below it no longer need. A column the
    /// read keeps anyway - `ReadFromMergeTree` with FINAL keeps the sorting key for the merge - is consumed by the step.
    removeUnusedColumns(parent_node, RemoveUnusedColumnsMode::Local);
#if defined(DEBUG_OR_SANITIZER_BUILD)
    assertBlocksHaveEqualStructure(
        *source_step_with_filter->getOutputHeader(),
        *parent_node.step->getInputHeaders()[0],
        "after removing unused columns in optimizePrewhere");
#endif
}

}

}
