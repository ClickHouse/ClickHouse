#include <Processors/QueryPlan/ReadFromMergeTreeScoredSearch.h>

#include <Access/ContextAccess.h>
#include <Core/Block.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Storages/MergeTree/AlterConversions.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/MergeTree/ScoredSearch/ScoredSearchUtils.h>
#include <Storages/MergeTree/ScoredSearch/IScorer.h>
#include <Storages/StorageMergeTreeScoredSearchBase.h>
#include <Storages/VirtualColumnUtils.h>

#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace Setting
{
    extern const SettingsBool apply_deleted_mask;
}

ReadFromMergeTreeScoredSearch::ReadFromMergeTreeScoredSearch(
    const Names & column_names_,
    const SelectQueryInfo & query_info_,
    const StorageSnapshotPtr & storage_snapshot_,
    const ContextPtr & context_,
    SharedHeader sample_block_,
    std::shared_ptr<StorageMergeTreeScoredSearchBase> storage_,
    RangesInDataPartsPtr ranges_in_data_parts_,
    LazyBitmapSubqueryStatePtr bitmap_state_,
    StorageSnapshotPtr source_storage_snapshot_,
    MergeTreeData::MutationsSnapshotPtr mutations_snapshot_,
    std::optional<FilterDAGInfo> row_policy_,
    PreparedSetsPtr row_policy_sets_,
    size_t num_streams_)
    : SourceStepWithFilter(
        std::move(sample_block_),
        column_names_,
        query_info_,
        storage_snapshot_,
        context_)
    , storage(std::move(storage_))
    , ranges_in_data_parts(std::move(ranges_in_data_parts_))
    , bitmap_state(std::move(bitmap_state_))
    , source_storage_snapshot(std::move(source_storage_snapshot_))
    , mutations_snapshot(std::move(mutations_snapshot_))
    , row_policy(std::move(row_policy_))
    , row_policy_sets(std::move(row_policy_sets_))
    , num_streams(num_streams_)
{
    if (!bitmap_state)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ReadFromMergeTreeScoredSearch requires a non-null LazyBitmapSubqueryState");
}

bool ReadFromMergeTreeScoredSearch::hasRowsHiddenOnRead() const
{
    /// A materialized lightweight delete stores `_row_exists = 0`, and a pending one is an
    /// `UPDATE _row_exists = 0`. Both are honoured by the readers only when `apply_deleted_mask`
    /// is set, and the `_row_exists` prefilter that the caller builds behaves the same way.
    if (context->getSettingsRef()[Setting::apply_deleted_mask])
    {
        if (mutations_snapshot->getAllUpdatedColumns().contains(RowExistsColumn::name))
            return true;

        if (std::ranges::any_of(*filtered_ranges, [](const auto & entry) { return entry.data_part->hasLightweightDelete(); }))
            return true;
    }

    /// An ordinary pending `ALTER DELETE` hides rows without touching any column: it writes no
    /// `_row_exists` and contributes nothing to `getAllUpdatedColumns`, so it needs a check of its
    /// own. The readers apply it on read regardless of `apply_deleted_mask`.
    if (!mutations_snapshot->hasDataMutations())
        return false;

    return std::ranges::any_of(*filtered_ranges, [&](const auto & entry)
    {
        auto alter_conversions = MergeTreeData::getAlterConversionsForPart(entry.data_part, mutations_snapshot, context
#if CLICKHOUSE_CLOUD
            , context->getAccess()->getEnabledMaskingPolicies()
#endif
        );

        return alter_conversions->hasDeleteMutation();
    });
}

void ReadFromMergeTreeScoredSearch::applyFilters(ActionDAGNodes added_filter_nodes)
{
    /// Bypass `SourceStepWithFilter::applyFilters` because we
    /// build a filter for prefilter subquery, not for a main query.
    applied_filters = true;
    filtered_ranges = ranges_in_data_parts;

    if (!ranges_in_data_parts || ranges_in_data_parts->empty())
        return;

    ExpressionActionsPtr part_name_filter;

    /// Remap analyzer column identifiers (e.g. `__table1.category`) back to the
    /// source-table column names (`category`), exactly as `SourceStepWithFilter::applyFilters` does.
    if (auto built = ActionsDAG::buildFilterActionsDAG(added_filter_nodes.nodes, query_info.buildNodeNameToInputNodeColumn()))
    {
        filter_actions_dag = std::make_shared<const ActionsDAG>(std::move(*built));

        Block block_to_filter
        {
            {{}, std::make_shared<DataTypeString>(), "_part"},
        };

        auto dag = VirtualColumnUtils::splitFilterDagForAllowedInputs(filter_actions_dag->getOutputs().at(0), &block_to_filter, context);
        if (dag)
            part_name_filter = VirtualColumnUtils::buildFilterExpression(std::move(*dag), context);
    }

    /// `_part` names a whole part, so a predicate on it prunes the part list instead of rows.
    /// Everything below - the scorer and the bitmap subquery - works on the pruned list.
    if (part_name_filter)
    {
        filtered_ranges = std::make_shared<RangesInDataParts>(
            VirtualColumnUtils::filterDataPartsRangesWithExpression(*ranges_in_data_parts, part_name_filter, "_part"));
    }

    if (filtered_ranges->empty())
        return;

    const MergeTreeData * source_merge_tree = storage->getSourceTable();

    /// The prefilter is a conjunction of
    /// (a) the WHERE predicates that can be evaluated against source-table columns
    /// (b) the source-table row policy.
    ActionsDAG::NodeRawConstPtrs prefilter_nodes;
    std::optional<ActionsDAG> filter_split;
    std::optional<ActionsDAG> row_exists_dag;

    if (filter_actions_dag)
    {
        /// Keep only predicates evaluatable against source-table columns, subcolumns and virtuals.
        auto allowed_columns_options = GetColumnsOptions(GetColumnsOptions::All)
            .withSubcolumns()
            .withVirtuals(VirtualsKind::All, VirtualsMaterializationPlace::All);

        Block allowed_inputs;
        for (const auto & column : source_storage_snapshot->getColumns(allowed_columns_options))
        {
            /// `_part` is part metadata: the predicate on it has already pruned the
            /// part list above, and pushing it into the subquery as well would turn a
            /// metadata-only filter into a row-reading prefilter that spends the
            /// `search_topk_prefilter_max_rows` budget for nothing.
            ///
            /// `_part_index` and `_part_offset` are the bitmap subquery's own row
            /// locators (it always reads them to map matched rows back to parts).
            /// Pushing a user predicate on these same columns into the subquery
            /// would conflict with that internal use, so leave such predicates to
            /// the outer filter, where the lazy reader has materialized them.
            if (column.name == "_part" || column.name == "_part_index" || column.name == "_part_offset")
                continue;

            allowed_inputs.insert({column.type->createColumn(), column.type, column.name});
        }

        filter_split = VirtualColumnUtils::splitFilterDagForAllowedInputs(filter_actions_dag->getOutputs().at(0), &allowed_inputs, context);

        if (filter_split)
            prefilter_nodes.push_back(filter_split->getOutputs().at(0));
    }

    /// Do not split the row policy. It must not contain columns that the source cannot provide.
    if (row_policy)
    {
        prefilter_nodes.push_back(&row_policy->actions.findInOutputs(row_policy->column_name));
    }

    /// Without a prefilter the scorer reads the index directly and never goes through the
    /// `MergeTree` readers, so nothing hides the rows that a plain `SELECT` from the source table
    /// would not return. Routing the scorer through the bitmap subquery restores that: the
    /// subquery does read through the readers, so rows they drop never enter the bitmap.
    /// With a prefilter this happens anyway, because that prefilter is read the same way.
    if (prefilter_nodes.empty() && hasRowsHiddenOnRead())
    {
        row_exists_dag = ActionsDAG(NamesAndTypesList{{RowExistsColumn::name, RowExistsColumn::type}});
        prefilter_nodes.push_back(row_exists_dag->getOutputs().at(0));
    }

    if (prefilter_nodes.empty())
        return;

    /// `buildFilterActionsDAG` returns nullopt only for an empty node
    /// list, which was handled above.
    auto combined = ActionsDAG::buildFilterActionsDAG(prefilter_nodes);
    if (!combined)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Could not build the prefilter DAG from {} filter nodes", prefilter_nodes.size());

    ActionsDAG where_clause = std::move(*combined);
    const String filter_column_name = where_clause.getOutputs().at(0)->result_name;

    auto subquery = buildBitmapSubquery(
        *source_merge_tree,
        filtered_ranges,
        mutations_snapshot,
        std::move(where_clause),
        filter_column_name,
        source_storage_snapshot,
        query_info,
        context);

    if (subquery.isInitialized())
    {
        addDelayedCreatingSetsStep(subquery, row_policy_sets, context);

        subquery.optimize(QueryPlanOptimizationSettings(context));
        bitmap_state->subquery_plan = std::move(subquery);

        /// Indexed by `part_index_in_query`, which pruning preserves, so the vector is sized by
        /// the unpruned part list even when the subquery reads only a subset of it.
        auto & bitmaps = bitmap_state->bitmaps.emplace(ranges_in_data_parts->size());
        for (auto & part_bitmap : bitmaps)
            part_bitmap = std::make_shared<roaring::Roaring>();
    }
}

void ReadFromMergeTreeScoredSearch::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    if (!ranges_in_data_parts)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "ReadFromMergeTreeScoredSearch received a null shared RangesInDataParts");
    }

    if (!applied_filters)
    {
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "ReadFromMergeTreeScoredSearch: applyFilters was not invoked before "
            "initializePipeline. Scored-search correctness depends on this — WHERE / "
            "row-policy filtering and bitmap-subquery construction happen there.");
    }

    scorer_owned = storage->createScorer();
    auto row_scorer = dynamic_pointer_cast<RowScorer>(scorer_owned);

    if (!row_scorer)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ReadFromMergeTreeScoredSearch: createScorer must return a RowScorer");

    buildScoredTopKPipeline(
        *filtered_ranges,
        row_scorer,
        bitmap_state->bitmaps,
        mutations_snapshot,
        source_storage_snapshot->metadata,
        getOutputHeader(),
        query_info,
        num_streams,
        context,
        pipeline);
}

}
