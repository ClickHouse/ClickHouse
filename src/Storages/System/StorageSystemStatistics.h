#pragma once

#include <Storages/IStorage.h>


namespace DB
{

/// For system.statistics table - describes the statistics in tables, similar to system.columns.
class StorageSystemStatistics : public IStorage
{
public:
    explicit StorageSystemStatistics(const StorageID & table_id_);

    std::string getName() const override { return "SystemDataSkippingIndices"; }

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        unsigned num_streams) override;

    bool isSystemStorage() const override { return true; }
};

}
