#pragma once
#include "config.h"
#include <Storages/IStorage.h>


namespace DB
{

    class StorageLoop final : public IStorage
    {
    public:
        StorageLoop(
                const StorageID & table_id,
                StoragePtr inner_storage_);

        std::string getName() const override { return "Loop"; }

        void read(
                QueryPlan & query_plan,
                const Names & column_names,
                const StorageSnapshotPtr & storage_snapshot,
                SelectQueryInfo & query_info,
                ContextPtr context,
                QueryProcessingStage::Enum processed_stage,
                size_t max_block_size,
                size_t num_streams) override;

        bool supportsTrivialCountOptimization(const StorageSnapshotPtr &, ContextPtr) const override { return false; }

    private:
        StoragePtr inner_storage;
    };
}
