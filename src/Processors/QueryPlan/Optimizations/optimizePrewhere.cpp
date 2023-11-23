#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Storages/MergeTree/MergeTreeWhereOptimizer.h>
#include <Interpreters/ActionsDAG.h>
#include <Planner/ActionsChain.h>
#include <deque>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

void matchDAGOutputNodesOrderWithHeader(ActionsDAGPtr & actions_dag, const Block & expected_header)
{
    std::unordered_map<std::string, const ActionsDAG::Node *> output_name_to_node;
    for (const auto * output_node : actions_dag->getOutputs())
        output_name_to_node.emplace(output_node->result_name, output_node);

    std::unordered_set<const ActionsDAG::Node *> used_output_nodes;

    ActionsDAG::NodeRawConstPtrs updated_outputs;
    updated_outputs.reserve(expected_header.columns());

    for (const auto & column : expected_header)
    {
        auto output_node_it = output_name_to_node.find(column.name);
        if (output_node_it == output_name_to_node.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Invalid move to PREWHERE optimization. Cannot find column {} in output",
                column.name);

        updated_outputs.push_back(output_node_it->second);
        used_output_nodes.insert(output_node_it->second);
    }

    ActionsDAG::NodeRawConstPtrs unused_outputs;
    for (const auto * output_node : actions_dag->getOutputs())
    {
        if (used_output_nodes.contains(output_node))
            continue;

        unused_outputs.push_back(output_node);
    }

    auto & actions_dag_outputs = actions_dag->getOutputs();
    actions_dag_outputs = std::move(updated_outputs);
    actions_dag_outputs.insert(actions_dag_outputs.end(), unused_outputs.begin(), unused_outputs.end());
}

}


namespace QueryPlanOptimizations
{

void optimizePrewhere(Stack & stack, QueryPlan::Nodes & nodes)
{
    if (stack.size() < 3)
        return;

    const auto & frame = stack.back();

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

    const QueryPlan::Node * filter_node = (stack.rbegin() + 1)->node;
    const auto * filter_step = typeid_cast<FilterStep *>(filter_node->step.get());
    if (!filter_step)
        return;

    /** Collect required filter output columns.
      * Collect output nodes that are mapped to input nodes.
      * Collect input node to output nodes mapping.
      */
    ColumnsWithTypeAndName required_columns_after_filter;
    std::unordered_set<std::string> output_nodes_mapped_to_input;
    std::unordered_map<std::string, std::vector<std::string>> input_node_to_output_names;

    for (const auto * output_node : filter_step->getExpression()->getOutputs())
    {
        const auto * node_without_alias = output_node;
        while (node_without_alias->type == ActionsDAG::ActionType::ALIAS)
            node_without_alias = node_without_alias->children[0];

        if (node_without_alias->type == ActionsDAG::ActionType::INPUT)
        {
            output_nodes_mapped_to_input.emplace(output_node->result_name);

            auto output_names_it = input_node_to_output_names.find(node_without_alias->result_name);
            if (output_names_it == input_node_to_output_names.end())
            {
                auto [insert_it, _] = input_node_to_output_names.emplace(node_without_alias->result_name, std::vector<std::string>());
                output_names_it = insert_it;
            }

            output_names_it->second.push_back(output_node->result_name);
        }

        if (output_node->result_name == filter_step->getFilterColumnName() && filter_step->removesFilterColumn())
            continue;

        required_columns_after_filter.push_back(ColumnWithTypeAndName(output_node->result_type, output_node->result_name));
    }

    const auto & context = read_from_merge_tree->getContext();
    const auto & settings = context->getSettingsRef();

    if (!settings.allow_experimental_analyzer)
        return;

    const auto & table_expression_modifiers = read_from_merge_tree->getQueryInfo().table_expression_modifiers;
    bool is_final = table_expression_modifiers && table_expression_modifiers->hasFinal();
    bool optimize_move_to_prewhere = settings.optimize_move_to_prewhere && (!is_final || settings.optimize_move_to_prewhere_if_final);
    if (!optimize_move_to_prewhere)
        return;

    const auto & storage_snapshot = read_from_merge_tree->getStorageSnapshot();

    if (table_expression_modifiers && table_expression_modifiers->hasSampleSizeRatio())
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
        queried_columns,
        storage.supportedPrewhereColumns(),
        &Poco::Logger::get("QueryPlanOptimizePrewhere")};

    auto optimize_result = where_optimizer.optimize(filter_step->getExpression(),
        filter_step->getFilterColumnName(),
        read_from_merge_tree->getContext(),
        is_final);
    if (!optimize_result.has_value())
        return;

    PrewhereInfoPtr prewhere_info;
    if (storage_prewhere_info)
        prewhere_info = storage_prewhere_info->clone();
    else
        prewhere_info = std::make_shared<PrewhereInfo>();

    prewhere_info->need_filter = true;

    auto & prewhere_filter_actions = optimize_result->prewhere_filter_actions;

    ActionsChain actions_chain;

    std::string prewere_filter_node_name = prewhere_filter_actions->getOutputs().at(0)->result_name;
    actions_chain.addStep(std::make_unique<ActionsChainStep>(prewhere_filter_actions));

    auto & filter_actions = optimize_result->filter_actions;

    /** Merge tree where optimizer splits conjunctions in filter expression into 2 parts:
      * 1. Filter expressions.
      * 2. Prewhere filter expressions.
      *
      * There can be cases when all expressions are moved to PREWHERE, but it is not
      * enough to produce required filter output columns.
      *
      * Example: SELECT (a AND b) AS cond FROM test_table WHERE cond AND c;
      * In this example condition expressions `a`, `b`, `c` can move to PREWHERE, but PREWHERE will not contain expression `and(a, b)`.
      * It will contain only `a`, `b`, `c`, `and(a, b, c)` expressions.
      *
      * In such scenario we need to create additional step to calculate `and(a, b)` expression after PREWHERE.
      */
    bool need_additional_filter_after_prewhere = false;

    if (!filter_actions)
    {
        /// Any node from PREWHERE filter actions can be used as possible output node
        std::unordered_set<std::string> possible_prewhere_output_nodes;
        for (const auto & node : prewhere_filter_actions->getNodes())
            possible_prewhere_output_nodes.insert(node.result_name);

        for (auto & required_column : required_columns_after_filter)
        {
            if (!possible_prewhere_output_nodes.contains(required_column.name) &&
                !output_nodes_mapped_to_input.contains(required_column.name))
            {
                need_additional_filter_after_prewhere = true;
                break;
            }
        }
    }

    /** If there are additional filter actions after PREWHERE filter actions, we create filter actions dag using PREWHERE filter
      * actions output columns as filter actions dag input columns.
      * Then we merge this filter actions dag nodes with old filter step actions dag nodes, to reuse some expressions from
      * PREWHERE filter actions.
      */
    if (need_additional_filter_after_prewhere || filter_actions)
    {
        auto merged_filter_actions = std::make_shared<ActionsDAG>(actions_chain.getLastStepAvailableOutputColumns());
        merged_filter_actions->getOutputs().clear();
        merged_filter_actions->mergeNodes(std::move(*filter_step->getExpression()->clone()));

        /// Add old filter step filter column to outputs
        for (const auto & node : merged_filter_actions->getNodes())
        {
            if (node.result_name == filter_step->getFilterColumnName())
            {
                merged_filter_actions->getOutputs().push_back(&node);
                break;
            }
        }

        filter_actions = std::move(merged_filter_actions);

        /// If there is filter after PREWHERE, we can ignore filtering during PREWHERE stage
        prewhere_info->need_filter = false;

        actions_chain.addStep(std::make_unique<ActionsChainStep>(filter_actions));
    }

    auto required_output_actions = std::make_shared<ActionsDAG>(required_columns_after_filter);
    actions_chain.addStep(std::make_unique<ActionsChainStep>(required_output_actions));

    actions_chain.finalize();

    prewhere_filter_actions->projectInput(false);

    auto & prewhere_actions_chain_node = actions_chain[0];
    prewhere_info->prewhere_actions = std::move(prewhere_filter_actions);
    prewhere_info->prewhere_column_name = prewere_filter_node_name;
    prewhere_info->remove_prewhere_column = !prewhere_actions_chain_node->getChildRequiredOutputColumnsNames().contains(prewere_filter_node_name);

    read_from_merge_tree->updatePrewhereInfo(prewhere_info);

    QueryPlan::Node * replace_old_filter_node = nullptr;
    bool remove_filter_node = false;

    if (filter_actions)
    {
        filter_actions->projectInput(false);

        /// Match dag output nodes with old filter step header
        matchDAGOutputNodesOrderWithHeader(filter_actions, filter_step->getOutputStream().header);

        auto & filter_actions_chain_node = actions_chain[1];
        bool remove_filter_column = !filter_actions_chain_node->getChildRequiredOutputColumnsNames().contains(filter_step->getFilterColumnName());
        auto after_prewhere_filter_step = std::make_unique<FilterStep>(read_from_merge_tree->getOutputStream(),
            filter_actions,
            filter_step->getFilterColumnName(),
            remove_filter_column);

        auto & node = nodes.emplace_back();
        node.children.emplace_back(frame.node);
        node.step = std::move(after_prewhere_filter_step);

        replace_old_filter_node = &node;
    }
    else
    {
        auto rename_actions_dag = std::make_shared<ActionsDAG>(read_from_merge_tree->getOutputStream().header.getColumnsWithTypeAndName());
        bool apply_rename_step = false;

        ActionsDAG::NodeRawConstPtrs updated_outputs;

        /** If in output after read from merge tree there are column names without aliases,
          * apply old filter step aliases to them.
          */
        for (const auto * output_node : rename_actions_dag->getOutputs())
        {
            const auto alias_it = input_node_to_output_names.find(output_node->result_name);
            if (alias_it == input_node_to_output_names.end())
            {
                updated_outputs.push_back(output_node);
                continue;
            }

            for (auto & output_name : alias_it->second)
            {
                if (output_name == output_node->result_name)
                {
                    updated_outputs.push_back(output_node);
                    continue;
                }

                updated_outputs.push_back(&rename_actions_dag->addAlias(*output_node, output_name));
                apply_rename_step = true;
            }
        }

        rename_actions_dag->getOutputs() = std::move(updated_outputs);

        bool apply_match_step = false;

        /// If column order does not match old filter step column order, match dag output nodes with header
        if (!blocksHaveEqualStructure(read_from_merge_tree->getOutputStream().header, filter_step->getOutputStream().header))
        {
            apply_match_step = true;
            matchDAGOutputNodesOrderWithHeader(rename_actions_dag, filter_step->getOutputStream().header);
        }

        if (apply_rename_step || apply_match_step)
        {
            auto rename_step = std::make_unique<ExpressionStep>(read_from_merge_tree->getOutputStream(), rename_actions_dag);
            if (apply_rename_step)
                rename_step->setStepDescription("Change column names to column identifiers");

            auto & node = nodes.emplace_back();
            node.children.emplace_back(frame.node);
            node.step = std::move(rename_step);

            replace_old_filter_node = &node;
        }
        else
        {
            replace_old_filter_node = frame.node;
            remove_filter_node = true;
        }
    }

    QueryPlan::Node * filter_parent_node = (stack.rbegin() + 2)->node;

    for (auto & filter_parent_child : filter_parent_node->children)
    {
        if (filter_parent_child == filter_node)
        {
            filter_parent_child = replace_old_filter_node;

            size_t stack_size = stack.size();

            /** If filter step is completely replaced with PREWHERE filter actions, remove it from stack.
              * Otherwise replace old filter step with new filter step after PREWHERE.
              */
            if (remove_filter_node)
            {
                std::swap(stack[stack_size - 1], stack[stack_size - 2]);
                stack.pop_back();
            }
            else
            {
                stack[stack_size - 2] = Frame{.node = replace_old_filter_node, .next_child = 1};
            }

            break;
        }
    }
}

}

}
