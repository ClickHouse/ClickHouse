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

size_t tryOptimizeTopK(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings & settings)
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
    if (!n || (settings.max_limit_for_top_k_optimization && n > settings.max_limit_for_top_k_optimization))
        return 0;

    SortingStep::Type sorting_step_type = sorting_step->getType();
    if (sorting_step_type != SortingStep::Type::Full)
        return 0;

    const auto & sort_description = sorting_step->getSortDescription();

    const size_t num_sort_columns = sort_description.size();
    auto sort_column_name = sort_description.front().column_name;

    const auto & sort_column = sorting_step->getInputHeaders().front()->getByName(sort_column_name);

    const bool where_clause = filter_step || read_from_mergetree_step->getPrewhereInfo();

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
            LOG_DEBUG(getLogger("optimizeTopK"), "Could not resolve column alias {} {}", sort_column_name, column_node->type);
            return 0;
        }
    }

    const auto & read_columns = read_from_mergetree_step->getAllColumnNames();
    if (std::find(read_columns.begin(), read_columns.end(), sort_column_name) == read_columns.end())
    {
        LOG_DEBUG(getLogger("optimizeTopK"), "Could not find column {} in ReadFromMergeTreeStep", sort_column_name);
        return 0;
    }

    TopKThresholdTrackerPtr threshold_tracker = nullptr;

    const auto & sort_col_desc = sort_description.front();

    /// The skip-index top-k path ranks granules via raw Field comparison
    /// (MinMaxGranuleItem::operator<) which does not respect nulls_direction
    /// or collation. Restrict it to types where raw Field ordering matches
    /// ORDER BY semantics. This check mirrors the guard in
    /// ReadFromMergeTree::buildIndexes for defense-in-depth.
    bool skip_index_type_eligible = sort_column.type->isValueRepresentedByNumber()
        && !sort_column.type->isNullable()
        && !sort_col_desc.collator;

    bool use_skip_index = settings.use_skip_indexes_for_top_k
        && skip_index_type_eligible
        && read_from_mergetree_step->isSkipIndexAvailableForTopK(sort_column_name);

    bool use_dynamic_filtering = settings.use_top_k_dynamic_filtering
        && !read_from_mergetree_step->getPrewhereInfo();

    /// When read-in-order optimization is enabled and the sort column is a prefix
    /// of the storage's sorting key, the engine will read data in sorted order.
    /// TopK dynamic filtering is counterproductive in this case: once the threshold
    /// is established, the prewhere rejects all subsequent rows (they are beyond
    /// the threshold in sorted order), preventing the LIMIT from triggering early
    /// pipeline cancellation, and causing a full table scan instead.
    if (use_dynamic_filtering && settings.read_in_order)
    {
        const auto & sorting_key = read_from_mergetree_step->getStorageMetadata()->getSortingKey();
        if (!sorting_key.column_names.empty() && sorting_key.column_names[0] == sort_column_name)
            use_dynamic_filtering = false;
    }

    /// The threshold tracker is needed for dynamic mark skipping during reads
    /// (use_skip_indexes_on_data_read) or for the prewhere dynamic filter.
    /// Initial top-k mark selection (getTopKMarks) does not require it.
    if ((use_skip_index && settings.use_skip_indexes_on_data_read) || use_dynamic_filtering)
    {
        threshold_tracker = std::make_shared<TopKThresholdTracker>(sort_col_desc);
        sorting_step->setTopKThresholdTracker(threshold_tracker);
    }

    bool added_step = false;

    if (use_dynamic_filtering)
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

        auto initial_header = read_from_mergetree_step->getOutputHeader();

        LOG_TRACE(getLogger("optimizeTopK"), "New Prewhere {}", new_prewhere_info->prewhere_actions.dumpDAG());
        read_from_mergetree_step->updatePrewhereInfo(new_prewhere_info);

        auto updated_header = read_from_mergetree_step->getOutputHeader();
        if (!blocksHaveEqualStructure(*initial_header, *updated_header))
        {
            auto dag = ActionsDAG::makeConvertingActions(
                updated_header->getColumnsWithTypeAndName(),
                initial_header->getColumnsWithTypeAndName(),
                ActionsDAG::MatchColumnsMode::Name, read_from_mergetree_step->getContext());

            auto converting_step = std::make_unique<ExpressionStep>(updated_header, std::move(dag));
            auto & converting_node = nodes.emplace_back();
            converting_node.step = std::move(converting_step);

            node->children.push_back(&converting_node);
            std::swap(node->step, converting_node.step);
            added_step = true;
        }
    }

    ///TopKThresholdTracker acts as a link between 3 components
    ///                                MergeTreeReaderIndex::canSkipMark() (skip whole granule using minmax index)
    ///                                  /
    ///         PartialSortingTransform/MergeSortingTransform --> ("publish" threshold value as sorting progresses)
    ///                                  \
    ///                                __topKFilter() (Prewhere filtering)

    if (use_skip_index || use_dynamic_filtering)
        read_from_mergetree_step->setTopKColumn({sort_column_name, sort_column.type, num_sort_columns, n, sort_col_desc.direction, where_clause, threshold_tracker});

    return added_step ? 1 : 0;
}

}
