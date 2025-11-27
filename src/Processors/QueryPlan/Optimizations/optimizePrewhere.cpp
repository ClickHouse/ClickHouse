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

void optimizePrewhere(QueryPlan::Node & parent_node)
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

    if (source_step_with_filter->getPrewhereInfo())
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
    auto * read_from_merge_tree_step = typeid_cast<ReadFromMergeTree *>(child_node->step.get());
    if (read_from_merge_tree_step && read_from_merge_tree_step->getVectorSearchParameters().has_value() && !settings[Setting::vector_search_with_rescoring])
        return;

    /// Extract column compressed sizes
    std::unordered_map<std::string, UInt64> column_compressed_sizes;
    for (const auto & [name, sizes] : column_sizes)
        column_compressed_sizes[name] = sizes.data_compressed;

    Names queried_columns = source_step_with_filter->requiredSourceColumns();

    MergeTreeWhereOptimizer where_optimizer{
        std::move(column_compressed_sizes),
        storage_snapshot,
        read_from_merge_tree_step ? read_from_merge_tree_step->getConditionSelectivityEstimator() : nullptr,
        queried_columns,
        storage.supportedPrewhereColumns(),
        getLogger("QueryPlanOptimizePrewhere")};

    auto optimize_result = where_optimizer.optimize(filter_step->getExpression(),
        filter_step->getFilterColumnName(),
        source_step_with_filter->getContext(),
        is_final);

    if (optimize_result.prewhere_nodes.empty())
        return;

    PrewhereInfoPtr prewhere_info = std::make_shared<PrewhereInfo>();

    auto remaining_expr = splitAndFillPrewhereInfo(
        prewhere_info,
        optimize_result.fully_moved_to_prewhere && filter_step->removesFilterColumn(),
        std::move(filter_step->getExpression()),
        filter_step->getFilterColumnName(),
        optimize_result.prewhere_nodes,
        optimize_result.prewhere_nodes_list);

    source_step_with_filter->updatePrewhereInfo(prewhere_info);

    QueryPlanStepPtr new_step;
    if (!optimize_result.fully_moved_to_prewhere)
    {
        new_step = std::make_unique<FilterStep>(
            source_step_with_filter->getOutputHeader(),
            std::move(remaining_expr),
            filter_step->getFilterColumnName(),
            filter_step->removesFilterColumn());
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
}

}

}
