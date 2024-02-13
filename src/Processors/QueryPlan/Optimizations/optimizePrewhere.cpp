#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Storages/MergeTree/MergeTreeWhereOptimizer.h>
#include <Interpreters/ActionsDAG.h>
#include <Functions/FunctionsLogical.h>
#include <Functions/IFunctionAdaptors.h>

namespace DB
{

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

void optimizePrewhere(Stack & stack, QueryPlan::Nodes &)
{
    if (stack.size() < 3)
        return;

    auto & frame = stack.back();

    /** Assume that on stack there are at least 3 nodes:
      *
      * 1. SomeNode
      * 2. FilterNode
      * 3. ReadFromMergeTreeNode
      */
    auto * read_from_merge_tree = typeid_cast<ReadFromMergeTree *>(frame.node->step.get());
    if (!read_from_merge_tree)
        return;

    const auto & storage_prewhere_info = read_from_merge_tree->getPrewhereInfo();
    if (storage_prewhere_info && storage_prewhere_info->prewhere_actions)
        return;

    QueryPlan::Node * filter_node = (stack.rbegin() + 1)->node;
    const auto * filter_step = typeid_cast<FilterStep *>(filter_node->step.get());
    if (!filter_step)
        return;

    const auto & context = read_from_merge_tree->getContext();
    const auto & settings = context->getSettingsRef();

    if (!settings.allow_experimental_analyzer)
        return;

    bool is_final = read_from_merge_tree->isQueryWithFinal();
    bool optimize_move_to_prewhere = settings.optimize_move_to_prewhere && (!is_final || settings.optimize_move_to_prewhere_if_final);
    if (!optimize_move_to_prewhere)
        return;

    const auto & storage_snapshot = read_from_merge_tree->getStorageSnapshot();

    ColumnsWithTypeAndName required_columns_after_filter;
    if (read_from_merge_tree->isQueryWithSampling())
    {
        const auto & sampling_key = storage_snapshot->getMetadataForQuery()->getSamplingKey();
        const auto & sampling_source_columns = sampling_key.expression->getRequiredColumnsWithTypes();
        for (const auto & column : sampling_source_columns)
            required_columns_after_filter.push_back(ColumnWithTypeAndName(column.type, column.name));
        const auto & sampling_result_columns = sampling_key.sample_block.getColumnsWithTypeAndName();
        required_columns_after_filter.insert(required_columns_after_filter.end(), sampling_result_columns.begin(), sampling_result_columns.end());
    }

    const auto & storage = storage_snapshot->storage;
    const auto & storage_metadata = storage_snapshot->metadata;
    auto column_sizes = storage.getColumnSizes();
    if (column_sizes.empty())
        return;

    /// Extract column compressed sizes
    std::unordered_map<std::string, UInt64> column_compressed_sizes;
    for (const auto & [name, sizes] : column_sizes)
        column_compressed_sizes[name] = sizes.data_compressed;

    Names queried_columns = read_from_merge_tree->getRealColumnNames();

    MergeTreeWhereOptimizer where_optimizer{
        std::move(column_compressed_sizes),
        storage_metadata,
        storage.getConditionEstimatorByPredicate(read_from_merge_tree->getQueryInfo(), storage_snapshot, context),
        queried_columns,
        storage.supportedPrewhereColumns(),
        getLogger("QueryPlanOptimizePrewhere")};

    auto optimize_result = where_optimizer.optimize(filter_step->getExpression(),
        filter_step->getFilterColumnName(),
        read_from_merge_tree->getContext(),
        is_final);

    if (optimize_result.prewhere_nodes.empty())
        return;

    PrewhereInfoPtr prewhere_info;
    if (storage_prewhere_info)
        prewhere_info = storage_prewhere_info->clone();
    else
        prewhere_info = std::make_shared<PrewhereInfo>();

    prewhere_info->need_filter = true;

    auto filter_expression = filter_step->getExpression();
    const auto & filter_column_name = filter_step->getFilterColumnName();

    if (optimize_result.fully_moved_to_prewhere && filter_step->removesFilterColumn())
    {
        removeFromOutput(*filter_expression, filter_column_name);
        auto & outputs = filter_expression->getOutputs();
        size_t size = outputs.size();
        outputs.insert(outputs.end(), optimize_result.prewhere_nodes.begin(), optimize_result.prewhere_nodes.end());
        filter_expression->removeUnusedActions(false);
        outputs.resize(size);
    }

    auto split_result = filter_step->getExpression()->split(optimize_result.prewhere_nodes, true);

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
        std::unordered_set<const ActionsDAG::Node *> first_outputs(split_result.first->getOutputs().begin(), split_result.first->getOutputs().end());
        for (const auto * input : split_result.first->getInputs())
        {
            if (!first_outputs.contains(input))
            {
                split_result.first->getOutputs().push_back(input);
                /// Add column to second actions as input.
                /// Do not add it to result, so it would be removed.
                split_result.second->addInput(input->result_name, input->result_type);
            }
        }
    }

    ActionsDAG::NodeRawConstPtrs conditions;
    conditions.reserve(split_result.split_nodes_mapping.size());
    for (const auto * condition : optimize_result.prewhere_nodes)
        conditions.push_back(split_result.split_nodes_mapping.at(condition));

    prewhere_info->prewhere_actions = std::move(split_result.first);
    prewhere_info->remove_prewhere_column = optimize_result.fully_moved_to_prewhere && filter_step->removesFilterColumn();

    if (conditions.size() == 1)
    {
        prewhere_info->prewhere_column_name = conditions.front()->result_name;
        prewhere_info->prewhere_actions->getOutputs().push_back(conditions.front());
    }
    else
    {
        prewhere_info->remove_prewhere_column = true;

        FunctionOverloadResolverPtr func_builder_and = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionAnd>());
        const auto * node = &prewhere_info->prewhere_actions->addFunction(func_builder_and, std::move(conditions), {});
        prewhere_info->prewhere_column_name = node->result_name;
        prewhere_info->prewhere_actions->getOutputs().push_back(node);
    }

    read_from_merge_tree->updatePrewhereInfo(prewhere_info);

    if (!optimize_result.fully_moved_to_prewhere)
    {
        filter_node->step = std::make_unique<FilterStep>(
            read_from_merge_tree->getOutputStream(),
            std::move(split_result.second),
            filter_step->getFilterColumnName(),
            filter_step->removesFilterColumn());
    }
    else
    {
        filter_node->step = std::make_unique<ExpressionStep>(
            read_from_merge_tree->getOutputStream(),
            std::move(split_result.second));
    }
}

}

}
