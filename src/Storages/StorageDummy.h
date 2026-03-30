#pragma once

#include <Processors/QueryPlan/SourceStepWithFilter.h>

#include <Storages/SelectQueryInfo.h>
#include <Storages/IStorage.h>

namespace DB
{

class StorageDummy final : public IStorage
{
public:
    StorageDummy(
        const StorageID & table_id_,
        const ColumnsDescription & columns_,
        const StorageSnapshotPtr & original_storage_snapshot_ = nullptr,
        bool supports_replication_ = false);

    std::string getName() const override { return "StorageDummy"; }

    bool supportsSampling() const override { return true; }
    bool supportsFinal() const override { return true; }
    bool supportsPrewhere() const override { return true; }

    std::optional<NameSet> supportedPrewhereColumns() const override
    {
        return original_storage_snapshot ? original_storage_snapshot->storage.supportedPrewhereColumns() : std::nullopt;
    }

    bool supportsSubcolumns() const override { return true; }
    bool supportsDynamicSubcolumns() const override { return true; }
    bool canMoveConditionsToPrewhere() const override
    {
        return original_storage_snapshot ? original_storage_snapshot->storage.canMoveConditionsToPrewhere() : false;
    }

    bool hasEvenlyDistributedRead() const override
    {
        return original_storage_snapshot ? original_storage_snapshot->storage.hasEvenlyDistributedRead() : false;
    }

    StorageSnapshotPtr getStorageSnapshot(const StorageMetadataPtr & metadata_snapshot, ContextPtr /*query_context*/) const override
    {
        return std::make_shared<StorageSnapshot>(*this, metadata_snapshot);
    }

    QueryProcessingStage::Enum getQueryProcessingStage(
        ContextPtr local_context,
        QueryProcessingStage::Enum to_stage,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info) const override;

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr local_context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    bool supportsReplication() const override { return supports_replication; }

private:
    /// The original storage snapshot which is replaced during planning. See collectFiltersForAnalysis for example.
    StorageSnapshotPtr original_storage_snapshot;
    const bool supports_replication;
};

class ReadFromDummy final : public SourceStepWithFilter
{
public:
    explicit ReadFromDummy(
        const Names & column_names_,
        const SelectQueryInfo & query_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        const ContextPtr & context_,
        const StorageDummy & storage_);

    const StorageDummy & getStorage() const
    {
        return storage;
    }

    const StorageSnapshotPtr & getStorageSnapshot() const
    {
        return storage_snapshot;
    }

    const Names & getColumnNames() const
    {
        return column_names;
    }

    String getName() const override { return "ReadFromDummy"; }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings & settings) override;

private:
    const StorageDummy & storage;
    Names column_names;
};

}
