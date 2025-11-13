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
FunctionOverloadResolverPtr createInternalFunctionTopNFilterResolver(TopNThresholdTrackerPtr threshold_tracker_);
}

namespace DB::QueryPlanOptimizations
{

size_t tryOptimizeTopN(QueryPlan::Node * parent_node, QueryPlan::Nodes & /* nodes*/, const Optimization::ExtraSettings & settings)
{
    QueryPlan::Node * node = parent_node;

    auto * limit_step = typeid_cast<LimitStep *>(node->step.get());
    if (!limit_step)
        return 0;
    if (node->children.size() != 1)
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

    /// Extract N - TODO : is max check needed?
    size_t n = limit_step->getLimitForSorting();

    SortingStep::Type sorting_step_type = sorting_step->getType();
    if (sorting_step_type != SortingStep::Type::Full)
        return 0;

    const auto & sort_description = sorting_step->getSortDescription();

    const auto & sort_column = sorting_step->getInputHeaders().front()->getByName(sort_description.front().column_name);
    if (!sort_column.type->isValueRepresentedByNumber())
        return 0;

    const bool where_clause = filter_step || read_from_mergetree_step->getPrewhereInfo();

    auto sort_column_name = sort_description.front().column_name.substr(sort_description.front().column_name.find('.') + 1);

    NameAndTypePair sort_column_name_and_type(sort_column_name, sort_column.type);
    TopNThresholdTrackerPtr threshold_tracker = nullptr;

    int direction = sort_description.front().direction;

    /// If no WHERE clause, threshold filtering optimization not required.
    if (settings.use_top_n_dynamic_filtering && where_clause)
    {
        threshold_tracker = std::make_shared<TopNThresholdTracker>(direction);
        sorting_step->setTopNThresholdTracker(threshold_tracker);

        auto new_prewhere_info = std::make_shared<PrewhereInfo>();
        new_prewhere_info->prewhere_actions = ActionsDAG({sort_column_name_and_type});

        /// Cannot use get() because need to pass an argument to constructor
        /// auto filter_function = FunctionFactory::instance().get("__topNFilter",nullptr);
        auto filter_function =  DB::createInternalFunctionTopNFilterResolver(threshold_tracker);
        const auto & prewhere_node = new_prewhere_info->prewhere_actions.addFunction(
                filter_function, {new_prewhere_info->prewhere_actions.getInputs().front()}, {});
        new_prewhere_info->prewhere_actions.getOutputs().push_back(&prewhere_node);
        new_prewhere_info->prewhere_column_name = prewhere_node.result_name;
        new_prewhere_info->remove_prewhere_column = true;
        new_prewhere_info->need_filter = true;

        LOG_TRACE(getLogger(""), "New Prewhere {}", new_prewhere_info->prewhere_actions.dumpDAG());
        /// TODO : handle existing prewhere
        read_from_mergetree_step->updatePrewhereInfo(new_prewhere_info);
    }

    ///TopNThresholdTracker acts as a link between 3 components
    ///                                MergeTreeReaderIndex::canSkipMark() (skip whole granule using minmax index)
    ///                                  /
    ///         PartialSortingTransform --> ("publish" threshold value as sorting progresses)
    ///                                  \
    ///                                __topNFilter() (Prewhere filtering)

    read_from_mergetree_step->setTopNColumn({sort_column_name, sort_column.type, n, direction, where_clause, threshold_tracker});

    return 0;
}

}
