#include <memory>
#include <Functions/FunctionFactory.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/LazilyReadStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Processors/QueryPlan/Optimizations/actionsDAGUtils.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Storages/MergeTree/MergeTreeLazilyReader.h>

namespace DB::QueryPlanOptimizations
{

using StepStack = std::vector<IQueryPlanStep *>;
using ActionsDAGNodeToSubcolumnTransformer = std::function<void(NameSet &, NodeReplacementMap &, ActionsDAG &, const ActionsDAG::Node *, const ContextPtr &)>;

static bool canUseLazyMaterializationForReadingStep(ReadFromMergeTree * reading)
{
    if (reading->getLazilyReadInfo())
       return false;

    if (reading->isQueryWithFinal())
        return false;

    if (reading->isQueryWithSampling())
        return false;

    if (reading->isVectorColumnReplaced())
        return false;

    return true;
}

static void removeUsedColumnNames(
    const ActionsDAG & actions,
    NameSet & lazily_read_column_name_set,
    AliasToName & alias_index,
    String filter_name = {},
    bool ignore_filter = false)
{
    const auto & actions_outputs = actions.getOutputs();

    for (const auto * output_node : actions_outputs)
    {
        const auto * node = output_node;

        while (node && node->type == ActionsDAG::ActionType::ALIAS)
        {
            /// alias has only one child
            chassert(node->children.size() == 1);
            node = node->children.front();
        }

        if (!node)
            continue;

        if (ignore_filter && !filter_name.empty() && filter_name == output_node->result_name)
            continue;

        if (node->type == ActionsDAG::ActionType::FUNCTION || node->type == ActionsDAG::ActionType::ARRAY_JOIN
            || (!filter_name.empty() && filter_name == output_node->result_name))
        {
            using ActionsNode = ActionsDAG::Node;

            std::unordered_set<const ActionsNode *> visited_nodes;
            std::stack<const ActionsNode *> stack;

            stack.push(node);
            while (!stack.empty())
            {
                const auto * current_node = stack.top();
                stack.pop();

                if (current_node->type == ActionsDAG::ActionType::INPUT)
                {
                    const auto it = alias_index.find(current_node->result_name);
                    if (it != alias_index.end())
                        lazily_read_column_name_set.erase(it->second);
                }

                for (const auto * child : current_node->children)
                {
                    if (!visited_nodes.contains(child))
                    {
                        stack.push(child);
                        visited_nodes.insert(child);
                    }
                }
            }
        }
    }

    /// Update alias name index.
    for (const auto * output_node : actions_outputs)
    {
        const auto * node = output_node;
        while (node && node->type == ActionsDAG::ActionType::ALIAS)
        {
            /// alias has only one child
            chassert(node->children.size() == 1);
            node = node->children.front();
        }
        if (node && node != output_node && node->type == ActionsDAG::ActionType::INPUT)
        {
            const auto it = alias_index.find(node->result_name);
            if (it != alias_index.end())
            {
                const auto real_column_name = it->second;
                alias_index.emplace(output_node->result_name, real_column_name);
                alias_index.erase(node->result_name);
            }
        }
    }
}

static std::pair<NameSet, AliasToName> collectLazilyReadColumnNames(
    const StepStack & steps,
    bool ignore_prewhere_filter = false)
{
    auto * read_from_merge_tree = typeid_cast<ReadFromMergeTree *>(steps.back());
    const Names & all_column_names = read_from_merge_tree->getAllColumnNames();
    auto storage_snapshot = read_from_merge_tree->getStorageSnapshot();

    NameSet lazily_read_column_name_set;
    AliasToName alias_index;

    const auto options = GetColumnsOptions(GetColumnsOptions::AllPhysical)
        .withSubcolumns(storage_snapshot->storage.supportsSubcolumns());

    for (const auto & column_name : all_column_names)
    {
        if (storage_snapshot->tryGetColumn(options, column_name))
            lazily_read_column_name_set.insert(column_name);
    }

    for (const auto & column_name : lazily_read_column_name_set)
        alias_index.emplace(column_name, column_name);

    if (const auto & row_level_filter = read_from_merge_tree->getRowLevelFilter())
        removeUsedColumnNames(row_level_filter->actions, lazily_read_column_name_set, alias_index, row_level_filter->column_name);

    if (const auto & prewhere_info = read_from_merge_tree->getPrewhereInfo())
        removeUsedColumnNames(prewhere_info->prewhere_actions, lazily_read_column_name_set, alias_index, prewhere_info->prewhere_column_name, ignore_prewhere_filter);

    for (auto step_it = steps.rbegin(); step_it != steps.rend(); ++step_it)
    {
        auto * step = *step_it;

        if (lazily_read_column_name_set.empty())
            break;

        if (auto * expression_step = typeid_cast<ExpressionStep *>(step))
        {
            removeUsedColumnNames(expression_step->getExpression(), lazily_read_column_name_set, alias_index);
            continue;
        }

        if (auto * filter_step = typeid_cast<FilterStep *>(step))
        {
            removeUsedColumnNames(filter_step->getExpression(), lazily_read_column_name_set, alias_index, filter_step->getFilterColumnName());
            continue;
        }

        if (auto * sorting_step = typeid_cast<SortingStep *>(step))
        {
            const auto & sort_description = sorting_step->getSortDescription();
            for (const auto & sort_column_description : sort_description)
            {
                const auto it = alias_index.find(sort_column_description.column_name);
                if (it == alias_index.end())
                    continue;
                lazily_read_column_name_set.erase(it->second);
            }
            continue;
        }
    }

    return std::pair(lazily_read_column_name_set, alias_index);
}

static ReadFromMergeTree * findReadingStep(QueryPlan::Node & node, StepStack & backward_path)
{
    IQueryPlanStep * step = node.step.get();
    backward_path.push_back(step);

    if (auto * read_from_merge_tree = typeid_cast<ReadFromMergeTree *>(step))
        return read_from_merge_tree;

    if (node.children.size() != 1)
        return nullptr;

    if (typeid_cast<ExpressionStep *>(step) || typeid_cast<FilterStep *>(step))
        return findReadingStep(*node.children.front(), backward_path);

    return nullptr;
}

/// Optimizes empty() and notEmpty() functions by replacing them with value size comparisons using str.size subcolumn
void optimizeFunctionStringEmpty(
    NameSet & new_inputs,
    NodeReplacementMap & replacements,
    ActionsDAG & actions_dag,
    const ActionsDAG::Node * node,
    const ContextPtr & context)
{
    const auto * input_node = node->children[0];
    String subcolumn_name = input_node->result_name + ".size";

    const auto * size_input = &actions_dag.addInput(subcolumn_name, std::make_shared<DataTypeUInt64>());

    ColumnWithTypeAndName zero_column(
        std::make_shared<DataTypeUInt64>()->createColumnConst(1, 0), std::make_shared<DataTypeUInt64>(), "0_UInt64");
    const auto * zero_const = &actions_dag.addColumn(zero_column);

    ActionsDAG::NodeRawConstPtrs children = {size_input, zero_const};
    String function_name = (node->function->getName() == "empty") ? "equals" : "notEquals";
    auto function_builder = FunctionFactory::instance().get(function_name, context);
    const auto * new_function_node = &actions_dag.addFunction(function_builder, children, {});

    replacements[node] = new_function_node;
    replacements[input_node] = size_input;

    new_inputs.insert(subcolumn_name);
}

/// Optimizes length() function by directly using str.size subcolumn
void optimizeFunctionStringLength(
    NameSet & new_inputs, NodeReplacementMap & replacements, ActionsDAG & actions_dag, const ActionsDAG::Node * node, const ContextPtr &)
{
    const auto * input_node = node->children[0];
    String subcolumn_name = input_node->result_name + ".size";

    const auto * size_input = &actions_dag.addInput(subcolumn_name, std::make_shared<DataTypeUInt64>());

    replacements[node] = size_input;
    replacements[input_node] = size_input;

    new_inputs.insert(subcolumn_name);
}


std::map<std::pair<TypeIndex, String>, ActionsDAGNodeToSubcolumnTransformer> node_transformers = {
    {
        {TypeIndex::String, "empty"},
        optimizeFunctionStringEmpty,
    },
    {
        {TypeIndex::String, "notEmpty"},
        optimizeFunctionStringEmpty,
    },
    {
        {TypeIndex::String, "length"},
        optimizeFunctionStringLength,
    },
};

static void rewritePrewhereToSubcolumns(ReadFromMergeTree * reading_step, const NameSet & lazily_read_columns)
{
    const auto & prewhere_info = reading_step->getPrewhereInfo();
    if (!prewhere_info)
        return;

    auto & actions_dag = prewhere_info->prewhere_actions;

    NameSet replace_exclusions;
    NodeSet replace_candidates;

    // Traverse all nodes and identify both replacement candidates and inputs that
    // should prevent replacement
    for (const auto & node : actions_dag.getNodes())
    {
        if (node.type != ActionsDAG::ActionType::FUNCTION)
            continue;

        ActionsDAG::Node const * input_node = nullptr;
        if (node.children.size() >= 1 && node.children.at(0)->type == ActionsDAG::ActionType::INPUT)
            input_node = node.children.at(0);

        bool may_be_optimized = input_node && lazily_read_columns.contains(input_node->result_name)
            && node_transformers.contains({input_node->result_type->getTypeId(), node.function->getName()});

        // If function node can be optimized, adding it to replace candidates.
        // Otherwise, add all function inputs (arguments) to the list of replacement
        // exclusions.
        if (may_be_optimized)
            replace_candidates.insert(&node);
        else
            for (const auto & child : node.children)
                if (child->type == ActionsDAG::ActionType::INPUT)
                    replace_exclusions.insert(child->result_name);
    }

    // Create replacement nodes from candidates, if they don't have forbidden inputs
    NodeReplacementMap replacements;
    NameSet new_inputs;
    for (const auto & node : replace_candidates)
    {
        bool has_non_replacement_inputs = std::any_of(
            node->children.begin(),
            node->children.end(),
            [&](const auto & input)
            { return input->type == ActionsDAG::ActionType::INPUT && replace_exclusions.contains(input->result_name); });

        if (has_non_replacement_inputs)
            continue;

        auto input_type = node->children.at(0)->result_type->getTypeId();
        auto transformer_it = node_transformers.find({input_type, node->function->getName()});

        if (transformer_it != node_transformers.end())
            transformer_it->second(new_inputs, replacements, actions_dag, node, reading_step->getContext());
    }

    if (replacements.empty())
        return;

    // Update prewhere actions DAG, and prewhere info
    const auto * filter_node = &actions_dag.findInOutputs(prewhere_info->prewhere_column_name);
    for (auto & output : actions_dag.getOutputs())
    {
        bool is_prewhere_column = output == filter_node;
        output = replaceNodes(actions_dag, output, replacements);
        if (is_prewhere_column)
            prewhere_info->prewhere_column_name = output->result_name;
    }
    actions_dag.removeUnusedActions(true);

    // And update reading step for rewritten prewhere actions
    reading_step->addColumnsToRead(new_inputs);
    reading_step->updatePrewhereInfo(prewhere_info);
}

static void updateStepsDataStreams(StepStack & steps_to_update)
{
    /// update output data stream for found transforms
    if (!steps_to_update.empty())
    {
        const auto * input_header = &steps_to_update.back()->getOutputHeader();
        chassert(dynamic_cast<ReadFromMergeTree *>(steps_to_update.back()));
        steps_to_update.pop_back();

        while (!steps_to_update.empty())
        {
            auto * transforming_step = dynamic_cast<ITransformingStep *>(steps_to_update.back());
            chassert(transforming_step);

            transforming_step->updateInputHeader(*input_header);
            input_header = &steps_to_update.back()->getOutputHeader();
            steps_to_update.pop_back();
        }
    }
}

bool optimizeLazyMaterialization(QueryPlan::Node & root, Stack & stack, QueryPlan::Nodes & nodes, const QueryPlanOptimizationSettings & optimization_settings)
{
    const auto & frame = stack.back();

    if (frame.node->children.size() != 1)
        return false;

    auto * limit_step = typeid_cast<LimitStep *>(frame.node->step.get());
    if (!limit_step)
        return false;

    /// it's not clear how many values will be read for LIMIT WITH TIES, so disable it
    if (limit_step->withTies())
        return false;

    auto * sorting_step = typeid_cast<SortingStep *>(frame.node->children.front()->step.get());
    if (!sorting_step)
        return false;

    if (sorting_step->getType() != SortingStep::Type::Full && sorting_step->getType() != SortingStep::Type::FinishSorting)
        return false;

    const auto limit = limit_step->getLimit();
    const auto max_limit_for_lazy_materialization = optimization_settings.max_limit_for_lazy_materialization;
    if (limit == 0 || (max_limit_for_lazy_materialization != 0 && limit > max_limit_for_lazy_materialization))
        return false;

    StepStack steps_to_update;
    steps_to_update.push_back(limit_step);
    steps_to_update.push_back(sorting_step);

    auto * sorting_node = frame.node->children.front();
    auto * reading_step = findReadingStep(*sorting_node->children.front(), steps_to_update);
    if (!reading_step)
        return false;

    if (!canUseLazyMaterializationForReadingStep(reading_step))
        return false;

    const auto & prewhere_info = reading_step->getPrewhereInfo();
    const bool should_rewrite_prewhere = optimization_settings.optimize_functions_to_subcolumns && prewhere_info
        && prewhere_info->remove_prewhere_column && reading_step->getStorageSnapshot()->storage.supportsSubcolumns();

    // If PREWHERE can be optimized, we are collecting lazily read column candidates,
    // without taking prewhere filter into account (assuming that columns used in PREWHERE are
    // allowed to be lazy), and rewriting actions for those candidate columns
    if (should_rewrite_prewhere)
    {
        auto lazily_read_column_names = collectLazilyReadColumnNames(steps_to_update, /*ignore_prewhere_filter=*/true).first;
        rewritePrewhereToSubcolumns(reading_step, lazily_read_column_names);
    }

    auto [lazily_read_column_names, alias_index] = collectLazilyReadColumnNames(steps_to_update);

    if (lazily_read_column_names.empty())
        return false;

    /// avoid applying this optimization on impractical queries for sake of implementation simplicity
    /// i.e. when no column used in query until projection, for example, select * from t order by rand() limit 10
    if (reading_step->getAllColumnNames().size() == lazily_read_column_names.size())
        return false;

    auto storage_snapshot = reading_step->getStorageSnapshot();

    /// Snapshot data may be missed, for example, in EXPLAIN query.
    if (storage_snapshot->data)
    {
        auto mutations_snapshot = assert_cast<const MergeTreeData::SnapshotData &>(*storage_snapshot->data).mutations_snapshot;
        /// Applying patches in MergeTreeLazilyReader is not implemented.
        if (mutations_snapshot->hasPatchParts())
            return false;
    }

    LazilyReadInfoPtr lazily_read_info = std::make_shared<LazilyReadInfo>();

    lazily_read_info->lazily_read_columns.reserve(lazily_read_column_names.size());

    const auto options = GetColumnsOptions(GetColumnsOptions::AllPhysical)
        .withSubcolumns(storage_snapshot->storage.supportsSubcolumns());
    for (const auto & column_name : lazily_read_column_names)
    {
        auto name_and_type = storage_snapshot->tryGetColumn(options, column_name);
        lazily_read_info->lazily_read_columns.emplace_back(
            name_and_type->type->createColumn(),
            name_and_type->type,
            name_and_type->name);
    }

    lazily_read_info->data_part_infos = std::make_shared<DataPartInfoByIndex>();

    auto lazy_column_reader = std::make_unique<MergeTreeLazilyReader>(
        sorting_step->getOutputHeader(),
        reading_step->getMergeTreeData(),
        storage_snapshot,
        lazily_read_info,
        reading_step->getContext(),
        alias_index);

    reading_step->updateLazilyReadInfo(lazily_read_info);

    QueryPlan::Node * limit_node = frame.node;
    auto lazily_read_step
        = std::make_unique<LazilyReadStep>(sorting_step->getOutputHeader(), lazily_read_info, std::move(lazy_column_reader));
    lazily_read_step->setStepDescription("Lazily Read");

    /// the root node can be a limit node when query is distributed
    /// and executed on remote node till WithMergeableStateAfterAggregationAndLimit stage
    /// see 03404_lazy_materialization_distributed.sql
    if (limit_node == &root)
    {
        chassert(stack.size() == 1);

        /// move out limit from root node and move lazy read in
        auto & new_limit_node = nodes.emplace_back();
        new_limit_node.step = std::move(limit_node->step);
        new_limit_node.children = limit_node->children;

        root.children.clear();
        root.children.push_back(&new_limit_node);
        root.step = std::move(lazily_read_step);
    }
    else
    {
        chassert(stack.size() > 1);

        auto & lazy_read_node = nodes.emplace_back();
        lazy_read_node.step = std::move(lazily_read_step);
        lazy_read_node.children.emplace_back(limit_node);

        QueryPlan::Node * limit_parent_node = (stack.rbegin() + 1)->node;
        chassert(limit_parent_node);
        for (auto & limit_parent_child : limit_parent_node->children)
        {
            if (limit_parent_child == limit_node)
            {
                limit_parent_child = &lazy_read_node;
                break;
            }
        }
    }

    updateStepsDataStreams(steps_to_update);

    return true;
}

}
