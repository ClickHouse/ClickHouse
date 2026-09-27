#pragma once

#include <Storages/IStorage.h>


namespace DB
{

class Context;


/** Implements the system table `symbols` for introspection of symbols in the ClickHouse binary.
  */
class StorageSystemSymbols final : public IStorage
{
public:
    explicit StorageSystemSymbols(const StorageID & table_id_);

    std::string getName() const override { return "SystemSymbols"; }

    Pipe read(
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
