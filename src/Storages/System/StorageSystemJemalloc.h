#pragma once

#include <Storages/IStorage.h>


namespace DB
{

class Context;

class StorageSystemJemallocBins final : public IStorage
{
public:
    explicit StorageSystemJemallocBins(const StorageID & table_id_);

    std::string getName() const override { return "SystemJemallocBins"; }

    static ColumnsDescription getColumnsDescription();

    Pipe read(
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

    bool isSystemStorage() const override { return true; }

    bool supportsTransactions() const override { return true; }
};

}
