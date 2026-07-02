#include <Storages/MergeTree/StorageFromMergeTreeProjection.h>

#include <Access/Common/AccessFlags.h>
#include <Access/EnabledRowPolicies.h>
#include <Interpreters/Context.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadNothingStep.h>
#include <Processors/Sources/NullSource.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ACCESS_DENIED;
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
    const auto parent_id = parent_storage->getStorageID();
    context->checkAccess(AccessType::SELECT, parent_id);

    /// Projection parts hold materialized rows of the parent table, so returning them directly
    /// would bypass a SELECT row policy on the parent. The policy cannot be re-applied here:
    /// a projection may aggregate rows or omit the policy's filter columns entirely, so hidden
    /// rows still leak even when those columns are absent. Deny when a non-trivial policy exists.
    auto row_policy_filter = context->getRowPolicyFilter(
        parent_id.getDatabaseName(), parent_id.getTableName(), RowPolicyFilterType::SELECT_FILTER);
    if (row_policy_filter && !row_policy_filter->isAlwaysTrue())
        throw Exception(ErrorCodes::ACCESS_DENIED,
            "Cannot read from `mergeTreeProjection`: a row policy is applied on table {}, "
            "and reading projection data directly would bypass it", parent_id.getNameForLogs());

    /// A UNIQUE KEY parent rejects projection reads in the MergeTreeDataSelectExecutor
    /// constructor below (the universal projection-read chokepoint), since reading a
    /// projection part bypasses the parent's delete-bitmap filter.

    const auto & snapshot_data = assert_cast<const MergeTreeData::SnapshotData &>(*storage_snapshot->data);
    const auto & parts = snapshot_data.parts;

    RangesInDataParts projection_parts;
    for (const auto & part : *parts)
    {
        const auto & created_projections = part.data_part->getProjectionParts();
        auto it = created_projections.find(projection->name);
        if (it != created_projections.end())
        {
            projection_parts.push_back(
                RangesInDataPart(it->second, part.data_part, part.part_index_in_query, part.part_starting_offset_in_query));
        }
    }

    auto step = MergeTreeDataSelectExecutor(merge_tree, projection)
                    .readFromParts(
                        std::make_shared<RangesInDataParts>(projection_parts),
                        snapshot_data.mutations_snapshot->cloneEmpty(),
                        column_names,
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
        auto read_nothing = std::make_unique<ReadNothingStep>(std::make_shared<const Block>(projection->sample_block));
        read_nothing->setStepDescription("Read from NullSource (Projection)");
        query_plan.addStep(std::move(read_nothing));
    }
}

StorageSnapshotPtr
StorageFromMergeTreeProjection::getStorageSnapshot(const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context) const
{
    auto parent_storage_snapshot = merge_tree.getStorageSnapshot(metadata_snapshot, query_context);
    const auto & parent_snapshot_data = assert_cast<const MergeTreeData::SnapshotData &>(*parent_storage_snapshot->data);

    auto data = std::make_unique<MergeTreeData::SnapshotData>();
    data->storage = parent_snapshot_data.storage;
    data->parts = parent_snapshot_data.parts;
    data->mutations_snapshot = parent_snapshot_data.mutations_snapshot;

    return std::make_shared<StorageSnapshot>(*this, metadata_snapshot, std::move(data));
}

}
