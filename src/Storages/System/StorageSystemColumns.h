#pragma once

#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;

/** Implements system table 'columns', that allows to get information about columns for every table.
  */
class StorageSystemColumns final : public IStorage
{
public:
    explicit StorageSystemColumns(const StorageID & table_id_);

    std::string getName() const override { return "SystemColumns"; }

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
