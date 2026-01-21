#include <memory>
#include <Interpreters/ActionsDAG.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/LazilyReadStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Storages/MergeTree/MergeTreeLazilyReader.h>

namespace DB::QueryPlanOptimizations
{

using StepStack = std::vector<IQueryPlanStep *>;

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
    String filter_name = {})
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

static void collectLazilyReadColumnNames(
    const StepStack & steps,
    ColumnsWithTypeAndName & lazily_read_columns,
    AliasToName & alias_index)
{
    auto * read_from_merge_tree = typeid_cast<ReadFromMergeTree *>(steps.back());
    const Names & all_column_names = read_from_merge_tree->getAllColumnNames();
    auto storage_snapshot = read_from_merge_tree->getStorageSnapshot();
    NameSet lazily_read_column_name_set;

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
        removeUsedColumnNames(prewhere_info->prewhere_actions, lazily_read_column_name_set, alias_index, prewhere_info->prewhere_column_name);

    for (auto step_it = steps.rbegin(); step_it != steps.rend(); ++step_it)
    {
        auto * step = *step_it;

        if (lazily_read_column_name_set.empty())
            return;

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

    lazily_read_columns.reserve(lazily_read_column_name_set.size());

    for (const auto & column_name : lazily_read_column_name_set)
    {
        auto name_and_type = storage_snapshot->tryGetColumn(options, column_name);
        lazily_read_columns.emplace_back(
            name_and_type->type->createColumn(),
            name_and_type->type,
            name_and_type->name);
    }
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

bool optimizeLazyMaterialization(QueryPlan::Node & root, Stack & stack, QueryPlan::Nodes & nodes, size_t max_limit_for_lazy_materialization)
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

    LazilyReadInfoPtr lazily_read_info = std::make_shared<LazilyReadInfo>();
    AliasToName alias_index;
    collectLazilyReadColumnNames(steps_to_update, lazily_read_info->lazily_read_columns, alias_index);

    if (lazily_read_info->lazily_read_columns.empty())
        return false;

    /// avoid applying this optimization on impractical queries for sake of implementation simplicity
    /// i.e. when no column used in query until projection, for example, select * from t order by rand() limit 10
    if (reading_step->getAllColumnNames().size() == lazily_read_info->lazily_read_columns.size())
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
