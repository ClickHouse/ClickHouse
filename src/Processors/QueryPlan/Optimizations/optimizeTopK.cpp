#include <Core/Field.h>
#include <Core/SortDescription.h>
#include <Functions/IFunction.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Common/logger_useful.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunctionAdaptors.h>

namespace DB
{
FunctionOverloadResolverPtr createInternalFunctionTopKFilterResolver(TopKThresholdTrackerPtr threshold_tracker_);
}

namespace DB::QueryPlanOptimizations
{

size_t tryOptimizeTopK(QueryPlan::Node * parent_node, QueryPlan::Nodes & /* nodes*/, const Optimization::ExtraSettings & settings)
{
    QueryPlan::Node * node = parent_node;

    auto * limit_step = typeid_cast<LimitStep *>(node->step.get());
    if (!limit_step)
        return 0;
    if (node->children.size() != 1)
        return 0;

    /// Cannot support LIMIT 10 WITH TIES because we don't know how many rows will be output
    if (limit_step->withTies())
        return 0;

    node = node->children.front();
    auto * sorting_step = typeid_cast<SortingStep *>(node->step.get());
    if (!sorting_step)
        return 0;
    if (node->children.size() != 1)
        return 0;

    node = node->children.front();
    auto * expression_step = typeid_cast<ExpressionStep *>(node->step.get());
    if (expression_step)
    {
        if (node->children.size() != 1)
            return 0;
        node = node->children.front();
    }

    auto * filter_step = typeid_cast<FilterStep *>(node->step.get());
    if (filter_step)
    {
        if (node->children.size() != 1)
            return 0;
        node = node->children.front();
    }

    auto * read_from_mergetree_step = typeid_cast<ReadFromMergeTree *>(node->step.get());
    if (!read_from_mergetree_step)
        return 0;

    size_t n = limit_step->getLimitForSorting();
    if (settings.max_limit_for_top_k_optimization && n > settings.max_limit_for_top_k_optimization)
        return 0;

    SortingStep::Type sorting_step_type = sorting_step->getType();
    if (sorting_step_type != SortingStep::Type::Full)
        return 0;

    const auto & sort_description = sorting_step->getSortDescription();

    /// We only support ORDER BY <single column> right now. But easy to extend and support
    /// multiple columns - just use the 1st column as the threshold and change comparisons
    /// at a couple of places (from < to <=)
    if (sort_description.size() > 1)
        return 0;

    const auto & sort_column = sorting_step->getInputHeaders().front()->getByName(sort_description.front().column_name);
    if (!sort_column.type->isValueRepresentedByNumber() || sort_column.type->isNullable())
        return 0;

    const bool where_clause = filter_step || read_from_mergetree_step->getPrewhereInfo();

    auto sort_column_name = sort_description.front().column_name;

    ///remove alias
    if (sort_column_name.contains('.'))
    {
        if (!expression_step && !filter_step)
            return 0;

        const ActionsDAG::Node * column_node = nullptr;
        if (filter_step)
            column_node = filter_step->getExpression().tryFindInOutputs(sort_column_name);
        else
            column_node = expression_step->getExpression().tryFindInOutputs(sort_column_name);

        if (unlikely(!column_node))
            return 0;

        if (column_node->type == ActionsDAG::ActionType::ALIAS)
        {
            sort_column_name = column_node->children.at(0)->result_name;
        }
        else
        {
            LOG_TRACE(getLogger("optimizeTopK"), "Could not resolve column alias {}", sort_column_name);
            return 0;
        }
    }

    TopKThresholdTrackerPtr threshold_tracker = nullptr;

    int direction = sort_description.front().direction;

    if ((settings.use_skip_indexes_for_top_k &&
            read_from_mergetree_step->isSkipIndexAvailableForTopK(sort_column_name) && settings.use_skip_indexes_on_data_read) ||
        (settings.use_top_k_dynamic_filtering && !read_from_mergetree_step->getPrewhereInfo()))
    {
        threshold_tracker = std::make_shared<TopKThresholdTracker>(direction);
        sorting_step->setTopKThresholdTracker(threshold_tracker);
    }

    if  (settings.use_top_k_dynamic_filtering &&
         !read_from_mergetree_step->getPrewhereInfo())
    {
        auto new_prewhere_info = std::make_shared<PrewhereInfo>();
        NameAndTypePair sort_column_name_and_type(sort_column_name, sort_column.type);
        new_prewhere_info->prewhere_actions = ActionsDAG({sort_column_name_and_type});

        /// Cannot use get() because need to pass an argument to constructor
        /// auto filter_function = FunctionFactory::instance().get("__topKFilter",nullptr);
        auto filter_function =  DB::createInternalFunctionTopKFilterResolver(threshold_tracker);
        const auto & prewhere_node = new_prewhere_info->prewhere_actions.addFunction(
                filter_function, {new_prewhere_info->prewhere_actions.getInputs().front()}, {});
        new_prewhere_info->prewhere_actions.getOutputs().push_back(&prewhere_node);
        new_prewhere_info->prewhere_column_name = prewhere_node.result_name;
        new_prewhere_info->remove_prewhere_column = true;
        new_prewhere_info->need_filter = true;

        LOG_TRACE(getLogger("optimizeTopK"), "New Prewhere {}", new_prewhere_info->prewhere_actions.dumpDAG());
        read_from_mergetree_step->updatePrewhereInfo(new_prewhere_info);
    }

    ///TopKThresholdTracker acts as a link between 3 components
    ///                                MergeTreeReaderIndex::canSkipMark() (skip whole granule using minmax index)
    ///                                  /
    ///         PartialSortingTransform/MergeSortingTransform --> ("publish" threshold value as sorting progresses)
    ///                                  \
    ///                                __topKFilter() (Prewhere filtering)

    read_from_mergetree_step->setTopKColumn({sort_column_name, sort_column.type, n, direction, where_clause, threshold_tracker});

    return 0;
}

}
