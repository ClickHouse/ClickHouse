#include <Columns/Collator.h>
#include <Core/Field.h>
#include <Core/SortDescription.h>
#include <DataTypes/DataTypeTuple.h>
#include <Functions/IFunction.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Common/logger_useful.h>
#include <Common/SipHash.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionsLogical.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/FunctionTopKFilter.h>

namespace DB::QueryPlanOptimizations
{

size_t tryOptimizeTopK(QueryPlan::Node * parent_node, QueryPlan::Nodes & /*nodes*/, const Optimization::ExtraSettings & settings)
{
    /// The dynamic-filtering path injects an internal `__topKFilter` function that
    /// is created on demand with a runtime threshold tracker and is not registered
    /// in `FunctionFactory`. The skip-index-on-data-read path likewise relies on a
    /// `TopKThresholdTracker` shared between `SortingStep` and `ReadFromMergeTree`.
    /// None of this can be transmitted to remote workers, so when the plan is
    /// going to be distributed, the remote node would fail to deserialize the
    /// plan with `Unknown function __topKFilter` (or run with stale state).
    if (settings.make_distributed_plan)
        return 0;

    QueryPlan::Node * node = parent_node;

    auto * limit_step = typeid_cast<LimitStep *>(node->step.get());
    if (!limit_step)
        return 0;
    if (node->children.size() != 1)
        return 0;

    /// Cannot support LIMIT 10 WITH TIES because we don't know how many rows will be output
    if (limit_step->withTies())
        return 0;

    /// TopK filtering can skip source rows, so it is incompatible with exact rows_before_limit_at_least.
    if (limit_step->alwaysReadTillEnd())
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
        /// `arrayJoin` changes the number of rows. The dynamic top-K prewhere filter
        /// applies the threshold to source rows BEFORE the expansion, while the sort
        /// + limit operates on EXPANDED rows. Mixing the two breaks the assumption
        /// that "rows seen by the filter" equals "rows seen by the sort": the
        /// threshold can stabilize at the wrong value, letting the wrong source rows
        /// through and producing non-deterministic / incorrect results. See #82279.
        if (expression_step->getExpression().hasArrayJoin())
            return 0;
        if (node->children.size() != 1)
            return 0;
        node = node->children.front();
    }

    auto * filter_step = typeid_cast<FilterStep *>(node->step.get());
    if (filter_step)
    {
        /// Same reasoning as above: `arrayJoin` inside a `FilterStep` below the sort
        /// breaks the top-K source-row threshold assumption. See #82279.
        if (filter_step->getExpression().hasArrayJoin())
            return 0;
        if (node->children.size() != 1)
            return 0;
        node = node->children.front();
    }

    auto * read_from_mergetree_step = typeid_cast<ReadFromMergeTree *>(node->step.get());
    if (!read_from_mergetree_step)
        return 0;

    /// Already stamped by an earlier visit: this node can be revisited when another optimization
    /// requests a re-traversal, and a plan can be optimized more than once (StorageMerge child
    /// plans, set subplans). Re-running would install a second `__topKFilter` and make
    /// `setTopKColumn` fold the part-set salt into `condition_hash` twice.
    if (read_from_mergetree_step->isSelectedForTopKFilterOptimization())
        return 0;

    /// FINAL queries deduplicate overlapping parts via merging sorted transforms
    /// (e.g. `ReplacingSortedTransform`, `CollapsingSortedTransform`) which require
    /// reading all matching rows in primary-key order to determine the winning row
    /// per key. Both the dynamic prewhere filter and minmax-based granule skipping
    /// can drop rows that are needed for correct deduplication, producing wrong
    /// results when these rows are duplicates of a row that survives the top-K.
    if (read_from_mergetree_step->isQueryWithFinal())
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

    /// A row-level policy filter restricts the rows inside the reader just like a `WHERE` / `PREWHERE`,
    /// so it must count as a `where_clause` as well. Otherwise a query filtered only by a row policy leaves
    /// `where_clause == false`, `MergeTreeDataSelectExecutor` enables `perform_top_k_optimization` and narrows
    /// the read to the top-K marks before the policy runs: the policy then discards the rows in those marks
    /// and the query returns fewer rows than the `LIMIT` - or none at all - even though later marks hold rows
    /// the policy keeps.
    const bool where_clause
        = filter_step || read_from_mergetree_step->getPrewhereInfo() || read_from_mergetree_step->getRowLevelFilter();

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

    /// Dynamic and Variant columns cannot be reliably filtered: their lessOrEquals
    /// returns Nullable(UInt8) rather than UInt8, causing an "Unexpected return type"
    /// logical error when the prewhere filter is executed. Comparison functions also
    /// reject zero-sized tuples even though ORDER BY supports them. Skip the optimization
    /// for these types.
    ///
    /// For variable-length types (e.g. String, Array, Map, Tuple containing variable-length
    /// elements), the per-row threshold comparison cost can exceed its savings — most notably
    /// when the column's lex-min value dominates and few granules can be skipped. Gate that
    /// path behind an explicit opt-in. Nullable and Tuple of fixed-length types are still
    /// considered fixed-length (haveMaximumSizeOfValue forwards through them).
    const bool sort_column_is_variable_length = !sort_column.type->haveMaximumSizeOfValue();
    const auto * sort_column_tuple_type = typeid_cast<const DataTypeTuple *>(sort_column.type.get());
    bool use_dynamic_filtering = settings.use_top_k_dynamic_filtering
        && !isDynamic(sort_column.type)
        && !isVariant(sort_column.type)
        && (!sort_column_tuple_type || !sort_column_tuple_type->getElements().empty())
        && (!sort_column_is_variable_length || settings.use_top_k_dynamic_filtering_for_variable_length_types);

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

    ///TopKThresholdTracker acts as a link between 3 components
    ///                                MergeTreeReaderIndex::canSkipMark() (skip whole granule using minmax index)
    ///                                  /
    ///         PartialSortingTransform/MergeSortingTransform --> ("publish" threshold value as sorting progresses)
    ///                                  \
    ///                                __topKFilter() (Prewhere filtering)

    if (use_skip_index || use_dynamic_filtering)
    {
        TopKFilterInfo info{sort_column_name, sort_column.type, num_sort_columns, n, sort_col_desc.direction, where_clause, threshold_tracker, /*condition_hash=*/ 0, /*dynamic_filter_pending=*/ use_dynamic_filtering};

        /// Compute a deterministic hash from the planning-time parameters. Used by
        /// `updateQueryConditionCache` to partition QCC entries by TopK plan, so the same
        /// query reuses cached granule decisions and a different TopK plan (different LIMIT,
        /// sort column, direction, NULLS FIRST/LAST, COLLATE, etc.) gets a fresh entry.
        SipHash hash;
        hash.update(info.column_name);
        const String type_name = info.data_type->getName();
        hash.update(type_name);
        hash.update(info.num_sort_columns);
        hash.update(info.limit_n);
        hash.update(info.direction);
        hash.update(sort_col_desc.nulls_direction);
        if (sort_col_desc.collator)
            hash.update(sort_col_desc.collator->getLocale());
        info.condition_hash = hash.get64();

        read_from_mergetree_step->setTopKColumn(info);
    }

    return 0;
}

void installTopKDynamicFilter(QueryPlan::Node & node, QueryPlan::Nodes & nodes)
{
    if (node.children.size() != 1)
        return;

    auto * child_node = node.children.front();
    auto * read_from_mergetree_step = typeid_cast<ReadFromMergeTree *>(child_node->step.get());
    if (!read_from_mergetree_step || !read_from_mergetree_step->hasPendingTopKDynamicFilter())
        return;

    const auto & top_k_filter_info = *read_from_mergetree_step->getTopKFilterInfo();
    read_from_mergetree_step->clearPendingTopKDynamicFilter();

    NameAndTypePair sort_column_name_and_type(top_k_filter_info.column_name, top_k_filter_info.data_type);
    ActionsDAG filter_dag({sort_column_name_and_type});

    /// Cannot use FunctionFactory::get() because the resolver needs the threshold tracker.
    auto filter_function = DB::createInternalFunctionTopKFilterResolver(top_k_filter_info.threshold_tracker);
    const auto * filter_node
        = &filter_dag.addFunction(filter_function, {filter_dag.getInputs().front()}, {});
    filter_dag.getOutputs().push_back(filter_node);
    const String filter_column_name = filter_node->result_name;

    auto prewhere_info = std::make_shared<PrewhereInfo>();
    const auto & existing_prewhere_info = read_from_mergetree_step->getPrewhereInfo();
    if (existing_prewhere_info)
    {
        ActionsDAG combined = existing_prewhere_info->prewhere_actions.clone();
        const auto * existing_filter_node = &combined.findInOutputs(existing_prewhere_info->prewhere_column_name);

        ActionsDAG::NodeRawConstPtrs merged_outputs;
        combined.mergeNodes(std::move(filter_dag), &merged_outputs);

        const ActionsDAG::Node * merged_filter_node = nullptr;
        for (const auto * merged : merged_outputs)
        {
            if (merged->result_name == filter_column_name)
            {
                merged_filter_node = merged;
                break;
            }
        }
        chassert(merged_filter_node);

        /// Keep the conjunction flat. `MergeTreeSplitPrewhereIntoReadSteps` splits on the direct
        /// children of the root `and`, so nesting `and(and(a, b), __topKFilter)` would present two
        /// children and collapse a multi-condition PREWHERE into a single read step.
        ActionsDAG::NodeRawConstPtrs conditions;
        const bool existing_is_conjunction = existing_filter_node->type == ActionsDAG::ActionType::FUNCTION
            && existing_filter_node->function_base && existing_filter_node->function_base->getName() == "and";
        if (existing_is_conjunction)
            conditions = existing_filter_node->children;
        else
            conditions.push_back(existing_filter_node);
        conditions.push_back(merged_filter_node);

        FunctionOverloadResolverPtr func_builder_and
            = std::make_unique<FunctionToOverloadResolverAdaptor>(std::make_shared<FunctionAnd>());
        const auto * and_node = &combined.addFunction(func_builder_and, std::move(conditions), {});

        auto & outputs = combined.getOutputs();
        /// The merged DAG contributes the sort column as an input; it has to stay in the outputs,
        /// otherwise the PREWHERE step drops it from the header and the sort loses its key.
        for (const auto * merged : merged_outputs)
            if (std::ranges::find(outputs, merged) == outputs.end())
                outputs.push_back(merged);

        if (existing_prewhere_info->remove_prewhere_column)
            std::erase(outputs, existing_filter_node);
        std::erase(outputs, merged_filter_node);
        outputs.push_back(and_node);

        prewhere_info->prewhere_actions = std::move(combined);
        prewhere_info->prewhere_column_name = and_node->result_name;
    }
    else
    {
        prewhere_info->prewhere_actions = std::move(filter_dag);
        prewhere_info->prewhere_column_name = filter_column_name;
    }
    prewhere_info->remove_prewhere_column = true;
    prewhere_info->need_filter = true;

    auto initial_header = read_from_mergetree_step->getOutputHeader();
    read_from_mergetree_step->updatePrewhereInfo(prewhere_info);
    auto updated_header = read_from_mergetree_step->getOutputHeader();

    /// Changing the PREWHERE can change the read's output header (`updatePrewhereInfo` rebuilds it
    /// and only the prewhere column itself is erased), so restore the structure the parent expects.
    if (!blocksHaveEqualStructure(*initial_header, *updated_header))
    {
        auto dag = ActionsDAG::makeConvertingActions(
            updated_header->getColumnsWithTypeAndName(),
            initial_header->getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Name,
            read_from_mergetree_step->getContext());

        auto & converting_node = nodes.emplace_back();
        converting_node.step = std::make_unique<ExpressionStep>(updated_header, std::move(dag));
        converting_node.children.push_back(child_node);
        node.children.front() = &converting_node;
    }
}

}
