#pragma once

#include <Storages/IStorage.h>


namespace DB
{

/// For system.data_skipping_indices table - describes the data skipping indices in tables, similar to system.columns.
class StorageSystemDataSkippingIndices : public IStorage
{
public:
    explicit StorageSystemDataSkippingIndices(const StorageID & table_id_);

    std::string getName() const override { return "SystemDataSkippingIndices"; }

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    bool isSystemStorage() const override { return true; }
};

}
