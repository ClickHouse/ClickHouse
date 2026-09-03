#pragma once

#include <Storages/StorageWithCommonVirtualColumns.h>


namespace DB
{

class StorageSystemConstraints : public StorageWithCommonVirtualColumns
{
public:
    explicit StorageSystemConstraints(const StorageID & table_id_);

    std::string getName() const override { return "SystemConstraints"; }

    static VirtualColumnsDescription createVirtuals();

    void readImpl(
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
