#include <Storages/MergeTree/StorageFromMergeTreeProjection.h>

#include <Access/Common/AccessFlags.h>
#include <Access/Common/RowPolicyDefs.h>
#include <Access/EnabledRowPolicies.h>
#include <Interpreters/Context.h>
#include <Planner/Utils.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/Sources/NullSource.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/ProjectionsDescription.h>
#include <Storages/SelectQueryInfo.h>

#include <algorithm>

namespace DB
{

namespace ErrorCodes
{
    extern const int ACCESS_DENIED;
    extern const int UNKNOWN_IDENTIFIER;
    extern const int NO_SUCH_COLUMN_IN_TABLE;
}

StorageFromMergeTreeProjection::StorageFromMergeTreeProjection(
    StorageID storage_id_, StoragePtr parent_storage_, StorageMetadataPtr parent_metadata_, ProjectionDescriptionRawPtr projection_)
    : IStorage(storage_id_)
    , parent_storage(std::move(parent_storage_))
    , merge_tree(dynamic_cast<const MergeTreeData &>(*parent_storage))
    , parent_metadata(std::move(parent_metadata_))
    , projection(projection_)
{
    setInMemoryMetadata(*projection->metadata);
    setVirtuals(MergeTreeData::createVirtuals(*parent_metadata));
}

void StorageFromMergeTreeProjection::read(
    QueryPlan & query_plan,
    const Names & column_names,
    const StorageSnapshotPtr & storage_snapshot,
    SelectQueryInfo & query_info,
    ContextPtr context,
    QueryProcessingStage::Enum /*processed_stage*/,
    size_t max_block_size,
    size_t num_streams)
{
    context->checkAccess(AccessType::SELECT, parent_storage->getStorageID());

    const auto parent_storage_id = parent_storage->getStorageID();
    auto row_policy_filter = context->getRowPolicyFilter(
        parent_storage_id.getDatabaseName(), parent_storage_id.getTableName(), RowPolicyFilterType::SELECT_FILTER);

    const bool has_row_policy = row_policy_filter && !row_policy_filter->empty();

    Names read_column_names = column_names;
    if (has_row_policy)
    {
        /// aggregate projections fold many parent rows into one state, so a per-row policy can't be applied
        if (projection->type != ProjectionDescription::Type::Normal)
            throw Exception(ErrorCodes::ACCESS_DENIED,
                "Cannot read from projection `{}` of table {} under a row policy: it is not a normal "
                "projection, so the policy cannot be enforced before aggregation",
                projection->name, parent_storage_id.getNameForLogs());

        /// the policy is on the parent table; enforce it here or the projection leaks hidden rows
        if (!query_info.planner_context || !query_info.table_expression)
            throw Exception(ErrorCodes::ACCESS_DENIED,
                "Cannot enforce the row policy of table {} on projection `{}` without the analyzer",
                parent_storage_id.getNameForLogs(), projection->name);

        FilterDAGInfo filter_info;
        try
        {
            /// resolve against the projection's own columns; anything it can't provide throws below
            filter_info = buildFilterInfo(
                row_policy_filter->expression->clone(), query_info.table_expression, query_info.planner_context);
        }
        catch (const Exception & e)
        {
            if (e.code() != ErrorCodes::UNKNOWN_IDENTIFIER && e.code() != ErrorCodes::NO_SUCH_COLUMN_IN_TABLE)
                throw;
            throw Exception(ErrorCodes::ACCESS_DENIED,
                "Cannot read from projection `{}` of table {} because its row policy references a column "
                "the projection does not store, so it cannot be enforced",
                projection->name, parent_storage_id.getNameForLogs());
        }

        /// read the policy's columns even if the query didn't ask for them; reject virtuals the projection
        /// reorders (position-relative) or only exposes under a projection name absent on the parent
        static const NameSet not_row_preserving{
            "_part_offset", "_part_index", "_part_granule_offset", "_block_offset", "_block_number", "_parent_part_offset"};
        for (const auto & name : filter_info.actions.getRequiredColumnsNames())
        {
            if (not_row_preserving.contains(name))
                throw Exception(ErrorCodes::ACCESS_DENIED,
                    "Cannot read from projection `{}` of table {} because its row policy uses virtual column "
                    "`{}`, whose value the projection does not preserve, so it cannot be enforced",
                    projection->name, parent_storage_id.getNameForLogs(), name);
            if (std::find(read_column_names.begin(), read_column_names.end(), name) == read_column_names.end())
                read_column_names.push_back(name);
        }

        /// apply the policy so it runs before any user PREWHERE (a post-read filter would let PREWHERE see
        /// hidden rows); 25.8 carries the row-level filter inside PrewhereInfo
        if (!query_info.prewhere_info)
        {
            auto prewhere_info = std::make_shared<PrewhereInfo>();
            prewhere_info->prewhere_actions = std::move(filter_info.actions);
            prewhere_info->prewhere_column_name = filter_info.column_name;
            prewhere_info->remove_prewhere_column = filter_info.do_remove_column;
            prewhere_info->need_filter = true;
            query_info.prewhere_info = std::move(prewhere_info);
        }
        else if (!query_info.prewhere_info->row_level_filter)
        {
            query_info.prewhere_info->row_level_filter = std::move(filter_info.actions);
            query_info.prewhere_info->row_level_column_name = filter_info.column_name;
            query_info.prewhere_info->need_filter = true;
        }
        else
        {
            /// a row policy already applies to the table function itself (`_table_function.*`) - can't combine
            throw Exception(ErrorCodes::ACCESS_DENIED,
                "Cannot read from projection `{}` of table {}: a row policy already applies to the table "
                "function itself and cannot be combined with the parent table's row policy",
                projection->name, parent_storage_id.getNameForLogs());
        }

        /// planner left trivial-LIMIT on (it checks this tf's storage id, which has no policy); n rows then filter could yield < n
        query_info.trivial_limit = 0;
    }

    /// A UNIQUE KEY parent rejects projection reads in the MergeTreeDataSelectExecutor
    /// constructor below (the universal projection-read chokepoint), since reading a
    /// projection part bypasses the parent's delete-bitmap filter.

    const auto & snapshot_data = assert_cast<const MergeTreeData::SnapshotData &>(*storage_snapshot->data);
    const auto & parts = snapshot_data.parts;

    /// on-the-fly data mutations and patch parts are applied to the parent read but not to the projection
    /// (mutations_snapshot is cleared below), so under a policy the projection could show stale hidden rows
    if (has_row_policy
        && (snapshot_data.mutations_snapshot->hasDataMutations() || snapshot_data.mutations_snapshot->hasPatchParts()))
        throw Exception(ErrorCodes::ACCESS_DENIED,
            "Cannot read from projection `{}` of table {} under a row policy while it has unmaterialized "
            "mutations, since the projection may show stale values the policy would hide",
            projection->name, parent_storage_id.getNameForLogs());

    RangesInDataParts projection_parts;
    for (const auto & part : parts)
    {
        const auto & created_projections = part.data_part->getProjectionParts();
        auto it = created_projections.find(projection->name);
        if (it != created_projections.end())
        {
            projection_parts.push_back(
                RangesInDataPart(it->second, part.data_part, part.part_index_in_query, part.part_starting_offset_in_query));
        }
    }

    auto step = MergeTreeDataSelectExecutor(merge_tree)
                    .readFromParts(
                        std::move(projection_parts),
                        snapshot_data.mutations_snapshot->cloneEmpty(),
                        read_column_names,
                        storage_snapshot,
                        query_info,
                        context,
                        max_block_size,
                        num_streams);

    if (step)
    {
        query_plan.addStep(std::move(step));
    }
    else
    {
        Pipe pipe(std::make_shared<NullSource>(std::make_shared<const Block>(projection->sample_block)));
        auto read_from_pipe = std::make_unique<ReadFromPreparedSource>(std::move(pipe));
        read_from_pipe->setStepDescription("Read from NullSource (Projection)");
        query_plan.addStep(std::move(read_from_pipe));
    }
}

StorageSnapshotPtr
StorageFromMergeTreeProjection::getStorageSnapshot(const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context) const
{
    auto parent_storage_snapshot = merge_tree.getStorageSnapshot(metadata_snapshot, query_context);
    const auto & parent_snapshot_data = assert_cast<const MergeTreeData::SnapshotData &>(*parent_storage_snapshot->data);

    auto data = std::make_unique<MergeTreeData::SnapshotData>();
    data->parts = parent_snapshot_data.parts;
    data->mutations_snapshot = parent_snapshot_data.mutations_snapshot;

    return std::make_shared<StorageSnapshot>(*this, metadata_snapshot, ColumnsDescription{}, std::move(data));
}

}
