#include <Processors/QueryPlan/ReadFromMergeTreeScoredSearch.h>

#include <Core/Block.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/Context.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
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

void ReadFromMergeTreeScoredSearch::applyFilters(ActionDAGNodes added_filter_nodes)
{
    /// Bypass `SourceStepWithFilter::applyFilters` because we
    /// build a filter for prefilter subquery, not for a main query.
    applied_filters = true;
    if (!ranges_in_data_parts || ranges_in_data_parts->empty())
        return;

    if (auto built = ActionsDAG::buildFilterActionsDAG(added_filter_nodes.nodes, {}))
    {
        filter_actions_dag = std::make_shared<const ActionsDAG>(std::move(*built));

        Block block_to_filter
        {
            {{}, std::make_shared<DataTypeString>(), "_part"},
        };

        auto dag = VirtualColumnUtils::splitFilterDagForAllowedInputs(filter_actions_dag->getOutputs().at(0), &block_to_filter, context);
        if (dag)
            virtual_columns_filter = VirtualColumnUtils::buildFilterExpression(std::move(*dag), context);
    }

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
            /// `_part_index` and `_part_offset` are the bitmap subquery's own row
            /// locators (it always reads them to map matched rows back to parts).
            /// Pushing a user predicate on these same columns into the subquery
            /// would conflict with that internal use, so leave such predicates to
            /// the outer filter, where the lazy reader has materialized them.
            if (column.name == "_part_index" || column.name == "_part_offset")
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

    /// With no prefilter we will bypass the _row_exists filter, so add it explicitly.
    /// With prefilter _row_exists mask will be applied automatically in MergeTree readers.
    if (prefilter_nodes.empty() && context->getSettingsRef()[Setting::apply_deleted_mask])
    {
        bool has_lightweight_deletes = std::ranges::any_of(*ranges_in_data_parts, [](const auto & entry)
        {
            return entry.data_part->hasLightweightDelete();
        });

        has_lightweight_deletes |= mutations_snapshot->getAllUpdatedColumns().contains(RowExistsColumn::name);

        if (has_lightweight_deletes)
        {
            row_exists_dag = ActionsDAG(NamesAndTypesList{{RowExistsColumn::name, RowExistsColumn::type}});
            prefilter_nodes.push_back(row_exists_dag->getOutputs().at(0));
        }
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
        ranges_in_data_parts,
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

    RangesInDataParts filtered_ranges = VirtualColumnUtils::filterDataPartsRangesWithExpression(*ranges_in_data_parts, virtual_columns_filter, "_part");

    scorer_owned = storage->createScorer();
    auto row_scorer = dynamic_pointer_cast<RowScorer>(scorer_owned);

    if (!row_scorer)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "ReadFromMergeTreeScoredSearch: createScorer must return a RowScorer");

    buildScoredTopKPipeline(
        std::move(filtered_ranges),
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
