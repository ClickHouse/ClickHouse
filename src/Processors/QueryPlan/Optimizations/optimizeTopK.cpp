#include <Columns/Collator.h>
#include <Core/Field.h>
#include <Core/SortDescription.h>
#include <DataTypes/IDataType.h>
#include <Functions/IFunction.h>
#include <Processors/QueryPlan/ExpressionStep.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/LimitStep.h>
#include <Processors/QueryPlan/Optimizations/Optimizations.h>
#include <Processors/QueryPlan/Optimizations/projectionsCommon.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromMergeTree.h>
#include <Processors/QueryPlan/SortingStep.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <algorithm>
#include <Common/logger_useful.h>
#include <Common/SipHash.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Functions/FunctionFactory.h>
#include <Functions/IFunctionAdaptors.h>
#include <Functions/FunctionTopKFilter.h>

namespace DB
{
namespace Setting
{
    extern const SettingsString preferred_optimize_projection_name;
}
}

namespace DB::QueryPlanOptimizations
{

/// True when the read is already ordered by a prefix of `sort_column_name`, from the base table's
/// sorting key or from a sorting projection the second-pass chooser would select. Runs before that
/// chooser, so each gate below mirrors one of its gates and an uncertain case must return false.
static bool readWouldBeInOrderForColumn(
    ReadFromMergeTree & read_step,
    const String & sort_column_name,
    const SortColumnDescription & sort_col_desc,
    bool optimize_projection,
    bool has_query_filter)
{
    /// A collated order is never a key order: keys carry no collation.
    if (sort_col_desc.collator)
        return false;

    /// A key column is stored ASC NULLS LAST or DESC NULLS FIRST, so the opposite NULL placement is
    /// not a key order. Floats are included because NaN takes the NULL position.
    auto null_placement_is_stored_order = [&](const DataTypePtr & key_type)
    {
        return sort_col_desc.nulls_direction != -1 || !(isNullableOrLowCardinalityNullable(key_type) || isFloat(*key_type));
    };

    const auto & metadata = read_step.getStorageMetadata();

    const auto & sorting_key = metadata->getSortingKey();
    if (!sorting_key.column_names.empty() && sorting_key.column_names[0] == sort_column_name)
        return null_placement_is_stored_order(sorting_key.data_types[0]);

    /// A sorting projection can only serve the read when projection optimization is enabled.
    if (!optimize_projection)
        return false;

    /// The chooser's own eligibility gate: FINAL, sampled, distributed, unique-key and unsupported
    /// parallel-replica reads never reach a projection, so the read stays on the base table.
    if (!canUseProjectionForReadingStep(&read_step))
        return false;

    /// A filter lets the chooser reject the projection on cost, so selection is not predictable here.
    if (has_query_filter)
        return false;

    const auto & preferred_projection_name
        = read_step.getContext()->getSettingsRef()[Setting::preferred_optimize_projection_name].value;

    /// The pin narrows the candidate set only when it names an existing normal projection;
    /// otherwise every normal projection stays a candidate.
    const bool pin_narrows_candidates = !preferred_projection_name.empty()
        && metadata->projections.has(preferred_projection_name)
        && metadata->projections.get(preferred_projection_name).type == ProjectionDescription::Type::Normal;

    const auto & read_columns = read_step.getAllColumnNames();
    for (const auto & projection : metadata->projections)
    {
        if (projection.type != ProjectionDescription::Type::Normal)
            continue;

        if (pin_narrows_candidates && projection.name != preferred_projection_name)
            continue;

        const auto & proj_sorting_key = projection.metadata->getSortingKey();
        if (proj_sorting_key.column_names.empty() || proj_sorting_key.column_names[0] != sort_column_name)
            continue;

        if (!null_placement_is_stored_order(proj_sorting_key.data_types[0]))
            continue;

        /// A projection with its own WHERE stores a subset of rows, so it cannot serve the full read.
        if (projection.where_clause_ast)
            continue;

        /// The projection can serve the read in-order only if it stores every column the read needs.
        const bool stores_all_read_columns = std::ranges::all_of(
            read_columns,
            [&](const String & column) { return projection.sample_block.findByName(column) != nullptr; });

        if (!stores_all_read_columns)
            continue;

        /// A part without a usable projection part is read from the base table under a union with
        /// the projection read, and that branch is not in order. An empty part set is not in order
        /// either: the chooser drops a candidate that would read nothing.
        const auto & parts = read_step.getParts();
        const bool projection_serves_every_part = !parts.empty()
            && std::ranges::all_of(
                   parts,
                   [&](const auto & part_with_ranges)
                   {
                       const auto & created = part_with_ranges.data_part->getProjectionParts();
                       auto it = created.find(projection.name);
                       if (it == created.end() || it->second->is_broken)
                           return false;

                       /// A projection part can lack a column the re-derived projection metadata expects;
                       /// the chooser serves it from the parent part, so the read is not in order. A column
                       /// missing from both parts was added later and fills the same default on either path.
                       return std::ranges::all_of(
                           read_columns,
                           [&](const String & column)
                           {
                               if (it->second->tryGetColumn(column))
                                   return true;
                               return !part_with_ranges.data_part->tryGetColumn(column)
                                   && metadata->getColumns().hasColumnOrSubcolumn(GetColumnsOptions::AllPhysical, column);
                           });
                   });

        if (projection_serves_every_part)
            return true;
    }

    return false;
}

size_t tryOptimizeTopK(QueryPlan::Node * parent_node, QueryPlan::Nodes & nodes, const Optimization::ExtraSettings & settings)
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
    /// logical error when the prewhere filter is executed. Skip the optimization for them.
    ///
    /// For variable-length types (e.g. String, Array, Map, Tuple containing variable-length
    /// elements), the per-row threshold comparison cost can exceed its savings — most notably
    /// when the column's lex-min value dominates and few granules can be skipped. Gate that
    /// path behind an explicit opt-in. Nullable and Tuple of fixed-length types are still
    /// considered fixed-length (haveMaximumSizeOfValue forwards through them).
    const bool sort_column_is_variable_length = !sort_column.type->haveMaximumSizeOfValue();
    bool use_dynamic_filtering = settings.use_top_k_dynamic_filtering
        && !read_from_mergetree_step->getPrewhereInfo()
        && !isDynamic(sort_column.type)
        && !isVariant(sort_column.type)
        && (!sort_column_is_variable_length || settings.use_top_k_dynamic_filtering_for_variable_length_types);

    /// On an already-sorted read the prewhere rejects every row past the threshold, so the LIMIT
    /// never cancels the pipeline early and the whole table is scanned.
    if (use_dynamic_filtering && settings.read_in_order
        && readWouldBeInOrderForColumn(
               *read_from_mergetree_step, sort_column_name, sort_col_desc, settings.optimize_projection, where_clause))
        use_dynamic_filtering = false;

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
    {
        TopKFilterInfo info{sort_column_name, sort_column.type, num_sort_columns, n, sort_col_desc.direction, where_clause, threshold_tracker, /*condition_hash=*/ 0};

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

    return added_step ? 1 : 0;
}

}
