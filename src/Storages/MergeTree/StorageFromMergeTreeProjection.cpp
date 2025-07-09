#include <Storages/MergeTree/StorageFromMergeTreeProjection.h>

#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/ReadFromPreparedSource.h>
#include <Processors/Sources/NullSource.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>

namespace DB
{

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
    const auto & snapshot_data = assert_cast<const MergeTreeData::SnapshotData &>(*storage_snapshot->data);
    const auto & parts = snapshot_data.parts;

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
        Pipe pipe(std::make_shared<NullSource>(projection->sample_block));
        auto read_from_pipe = std::make_unique<ReadFromPreparedSource>(std::move(pipe));
        read_from_pipe->setStepDescription("Read from NullSource (Projection)");
        query_plan.addStep(std::move(read_from_pipe));
    }
}

StorageSnapshotPtr
StorageFromMergeTreeProjection::getStorageSnapshot(const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context) const
{
    return merge_tree.getStorageSnapshot(metadata_snapshot, query_context);
}

}
