#pragma once

#include <Storages/IStorage.h>


namespace DB
{

/// For system.projections table - describes the projections in tables, similar to system.data_skipping_indices.
class StorageSystemProjections : public IStorage
{
public:
    explicit StorageSystemProjections(const StorageID & table_id_);

    std::string getName() const override { return "StorageSystemProjections"; }

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
