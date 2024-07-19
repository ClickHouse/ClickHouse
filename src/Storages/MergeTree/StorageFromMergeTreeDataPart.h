#pragma once

#include <Core/Defines.h>
#include <Storages/IStorage.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/AlterConversions.h>
#include <QueryPipeline/QueryPipelineBuilder.h>


namespace DB
{

class QueryPlan;

/// A Storage that allows reading from a single MergeTree data part.
class StorageFromMergeTreeDataPart final : public IStorage
{
public:
    /// Used in part mutation.
    explicit StorageFromMergeTreeDataPart(
        const MergeTreeData::DataPartPtr & part_,
        const MergeTreeData::MutationsSnapshotPtr & mutations_snapshot_)
        : IStorage(getIDFromPart(part_))
        , parts({part_})
        , mutations_snapshot(mutations_snapshot_)
        , storage(part_->storage)
        , partition_id(part_->info.partition_id)
    {
        setInMemoryMetadata(storage.getInMemoryMetadata());
        setVirtuals(*storage.getVirtualsPtr());
    }

    /// Used in queries with projection.
    StorageFromMergeTreeDataPart(const MergeTreeData & storage_, ReadFromMergeTree::AnalysisResultPtr analysis_result_ptr_)
        : IStorage(storage_.getStorageID()), storage(storage_), analysis_result_ptr(analysis_result_ptr_)
    {
        setInMemoryMetadata(storage.getInMemoryMetadata());
    }

    String getName() const override { return "FromMergeTreeDataPart"; }

    StorageSnapshotPtr getStorageSnapshot(const StorageMetadataPtr & metadata_snapshot, ContextPtr /*query_context*/) const override;

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

    bool supportsDynamicSubcolumnsDeprecated() const override { return true; }
    bool supportsDynamicSubcolumns() const override { return true; }

    bool supportsSubcolumns() const override { return true; }

    String getPartitionId() const
    {
        return partition_id;
    }

    String getPartitionIDFromQuery(const ASTPtr & ast, ContextPtr context) const
    {
        return storage.getPartitionIDFromQuery(ast, context);
    }

    bool materializeTTLRecalculateOnly() const;

    bool hasLightweightDeletedMask() const override
    {
        return !parts.empty() && parts.front()->hasLightweightDelete();
    }

    bool supportsLightweightDelete() const override
    {
        return !parts.empty() && parts.front()->supportLightweightDeleteMutate();
    }

private:
    const MergeTreeData::DataPartsVector parts;
    const MergeTreeData::MutationsSnapshotPtr mutations_snapshot;
    const MergeTreeData & storage;
    const String partition_id;
    const ReadFromMergeTree::AnalysisResultPtr analysis_result_ptr;

    static StorageID getIDFromPart(const MergeTreeData::DataPartPtr & part_)
    {
        auto table_id = part_->storage.getStorageID();
        return StorageID(table_id.database_name, table_id.table_name + " (part " + part_->name + ")");
    }
};

}
