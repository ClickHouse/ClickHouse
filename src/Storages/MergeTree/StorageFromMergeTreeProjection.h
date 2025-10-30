#pragma once

#include <Storages/IStorage.h>

namespace DB
{

class MergeTreeData;

/// A Storage that allows reading from a projection of MergeTree.
class StorageFromMergeTreeProjection final : public IStorage
{
public:
    StorageFromMergeTreeProjection(
        StorageID storage_id_, StoragePtr parent_storage_, StorageMetadataPtr parent_metadata_, ProjectionDescriptionRawPtr projection_);

    String getName() const override { return "FromMergeTreeProjection"; }

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum /*processed_stage*/,
        size_t max_block_size,
        size_t num_streams) override;

    bool supportsPrewhere() const override { return true; }

    StorageSnapshotPtr getStorageSnapshot(const StorageMetadataPtr & metadata_snapshot, ContextPtr query_context) const override;

private:
    StoragePtr parent_storage;
    const MergeTreeData & merge_tree;
    StorageMetadataPtr parent_metadata;
    ProjectionDescriptionRawPtr projection;
};

}
