#include <Core/Settings.h>
#include <Functions/FunctionsLogical.h>
#include <Functions/IFunctionAdaptors.h>
#include <Interpreters/ActionsDAG.h>
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

namespace DB
{
namespace Setting
{
    extern const SettingsBool optimize_move_to_prewhere;
    extern const SettingsBool optimize_move_to_prewhere_if_final;
    extern const SettingsBool vector_search_with_rescoring;
    extern const SettingsBool allow_reorder_prewhere_conditions;
    extern const SettingsBool allow_reorder_row_policy_and_prewhere;
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

static DataTypesWithConstInfo getDataTypesWithConstInfoFromNodes(const ActionsDAG::NodeRawConstPtrs & nodes)
{
    DataTypesWithConstInfo types;
    types.reserve(nodes.size());
    for (const auto & child : nodes)
    {
        bool is_const = child->column && isColumnConst(*child->column);
        types.push_back({child->result_type, is_const});
    }
    return types;
}

static bool predicateDoesNotThrowException(const ActionsDAG::Node * node)
{
    std::stack<const ActionsDAG::Node *> stack;
    stack.push(node);
    while (!stack.empty())
    {
        const auto * top = stack.top();
        stack.pop();

        /// This is a pessimistic check. Functions that throw exception should be isSuitableForShortCircuitArgumentsExecution.
        /// Computationally heavy functions should also apply, but it's ok to firbid reordering with row-level policy in this case.
        if (top->type == ActionsDAG::ActionType::FUNCTION
            && top->function_base->isSuitableForShortCircuitArgumentsExecution(getDataTypesWithConstInfoFromNodes(top->children)))
            return false;

        for (const auto * child : top->children)
            stack.push(child);
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
    /// Splited actions may have inputs which are needed only for PREWHERE.
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
            prewhere_info->prewhere_actions.getOutputs().push_back(conditions.front());
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

void optimizePrewhere(Stack & stack, QueryPlan::Nodes &)
{
    if (stack.size() < 2)
        return;

    auto & frame = stack.back();

    /** Assume that on stack there are at least 3 nodes:
      *
      * 1. SomeNode
      * 2. FilterNode
      * 3. SourceStepWithFilterNode
      */
    auto * source_step_with_filter = dynamic_cast<SourceStepWithFilter *>(frame.node->step.get());
    if (!source_step_with_filter)
        return;

    if (typeid_cast<ReadFromMerge *>(frame.node->step.get()))
        return;

    const auto & storage_snapshot = source_step_with_filter->getStorageSnapshot();
    const auto & storage = storage_snapshot->storage;
    if (!storage.canMoveConditionsToPrewhere())
        return;

    if (source_step_with_filter->getPrewhereInfo())
        return;

    /// TODO: We can also check for UnionStep, such as StorageBuffer and local distributed plans.
    QueryPlan::Node * filter_node = (stack.rbegin() + 1)->node;
    auto * filter_step = typeid_cast<FilterStep *>(filter_node->step.get());
    if (!filter_step)
        return;

    const auto & context = source_step_with_filter->getContext();
    const auto & settings = context->getSettingsRef();

    bool is_final = source_step_with_filter->isQueryWithFinal();
    bool optimize = settings[Setting::optimize_move_to_prewhere] && (!is_final || settings[Setting::optimize_move_to_prewhere_if_final]);
    if (!optimize)
        return;

    auto column_sizes = storage.getColumnSizes();
    if (column_sizes.empty())
        return;

    /// These two optimizations conflict:
    /// - vector search lookups with disabled rescoring
    /// - PREWHERE
    /// The former is more impactful, therefore disable PREWHERE if both may be used.
    auto * read_from_merge_tree_step = typeid_cast<ReadFromMergeTree *>(frame.node->step.get());
    if (read_from_merge_tree_step && read_from_merge_tree_step->getVectorSearchParameters().has_value() && !settings[Setting::vector_search_with_rescoring])
        return;

    /// Extract column compressed sizes
    std::unordered_map<std::string, UInt64> column_compressed_sizes;
    for (const auto & [name, sizes] : column_sizes)
        column_compressed_sizes[name] = sizes.data_compressed;

    Names queried_columns = source_step_with_filter->requiredSourceColumns();

    const auto & source_filter_actions_dag = source_step_with_filter->getFilterActionsDAG();
    MergeTreeWhereOptimizer where_optimizer{
        std::move(column_compressed_sizes),
        storage_snapshot,
        storage.getConditionSelectivityEstimatorByPredicate(storage_snapshot, source_filter_actions_dag ? &*source_filter_actions_dag : nullptr, context),
        queried_columns,
        storage.supportedPrewhereColumns(),
        getLogger("QueryPlanOptimizePrewhere")};

    auto & filter_actions_dag = filter_step->getExpression();
    auto row_level_filter = source_step_with_filter->getRowLevelFilter();

    bool remove_filter_column = filter_step->removesFilterColumn();
    auto filter_column_name = filter_step->getFilterColumnName();
    const auto * predicate = &filter_actions_dag.findInOutputs(filter_column_name);

    std::optional<ActionsDAG> combined_dag;
    if (read_from_merge_tree_step && row_level_filter
        && context->getSettingsRef()[Setting::allow_reorder_prewhere_conditions]
        && context->getSettingsRef()[Setting::allow_reorder_row_policy_and_prewhere]
        && predicateDoesNotThrowException(predicate))
    {
        combined_dag = ActionsDAG::merge(row_level_filter->actions.clone(), filter_actions_dag.clone());
        const auto * predicate_node = &combined_dag->findInOutputs(filter_column_name);
        const auto * row_level_predicate_node = &combined_dag->findInOutputs(row_level_filter->column_name);

        auto & outputs = combined_dag->getOutputs();

        if (remove_filter_column)
            outputs.erase(std::find(outputs.begin(), outputs.end(), predicate_node));
        if (row_level_filter->do_remove_column)
            outputs.erase(std::find(outputs.begin(), outputs.end(), row_level_predicate_node));

        FunctionOverloadResolverPtr func_builder_and = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionAnd>());
        predicate = &combined_dag->addFunction(func_builder_and, {row_level_predicate_node, predicate_node}, {});
        remove_filter_column = true;
        filter_column_name = predicate->result_name;
        outputs.push_back(predicate);

        row_level_filter = nullptr;
    }

    auto optimize_result = where_optimizer.optimize(predicate,
        source_step_with_filter->getContext(),
        is_final);

    if (optimize_result.prewhere_nodes.empty())
        return;

    PrewhereInfoPtr prewhere_info = std::make_shared<PrewhereInfo>();
    ActionsDAG dag = combined_dag ? std::move(*combined_dag) : std::move(filter_actions_dag);

    auto remaining_expr = splitAndFillPrewhereInfo(
        prewhere_info,
        optimize_result.fully_moved_to_prewhere && remove_filter_column,
        std::move(dag),
        filter_column_name,
        optimize_result.prewhere_nodes,
        optimize_result.prewhere_nodes_list);

    source_step_with_filter->updatePrewhereInfo(prewhere_info, row_level_filter);

    QueryPlanStepPtr new_step;
    if (!optimize_result.fully_moved_to_prewhere)
    {
        new_step = std::make_unique<FilterStep>(
            source_step_with_filter->getOutputHeader(),
            std::move(remaining_expr),
            filter_column_name,
            remove_filter_column);
    }
    else
    {
        /// Have to keep this expression to change column names to column identifiers
        new_step = std::make_unique<ExpressionStep>(
            source_step_with_filter->getOutputHeader(),
            std::move(remaining_expr));
    }

    new_step->setStepDescription(*filter_step);
    filter_node->step = std::move(new_step);
}

}

}
