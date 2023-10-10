#pragma once

#include <Processors/QueryPlan/SourceStepWithFilter.h>

#include <Storages/SelectQueryInfo.h>
#include <Storages/IStorage.h>

namespace DB
{

class StorageDummy : public IStorage
{
public:
    StorageDummy(const StorageID & table_id_, const ColumnsDescription & columns_, ColumnsDescription object_columns_ = {});

    std::string getName() const override { return "StorageDummy"; }

    bool supportsSampling() const override { return true; }
    bool supportsFinal() const override { return true; }
    bool supportsPrewhere() const override { return true; }
    bool supportsSubcolumns() const override { return true; }
    bool supportsDynamicSubcolumns() const override { return true; }
    bool canMoveConditionsToPrewhere() const override { return false; }

    StorageSnapshotPtr getStorageSnapshot(const StorageMetadataPtr & metadata_snapshot, ContextPtr /*query_context*/) const override
    {
        return std::make_shared<StorageSnapshot>(*this, metadata_snapshot, object_columns);
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
private:
    const ColumnsDescription object_columns;
};

class ReadFromDummy : public SourceStepWithFilter
{
public:
    explicit ReadFromDummy(const StorageDummy & storage_,
        StorageSnapshotPtr storage_snapshot_,
        Names column_names_);

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
    StorageSnapshotPtr storage_snapshot;
    Names column_names;
};

}
