#pragma once

#include <Storages/IStorage.h>


namespace DB
{

class Context;


/** Implements `replicas` system table, which provides information about the status of the replicated tables.
  */
class StorageSystemReplicas final : public IStorage
{
public:
    explicit StorageSystemReplicas(const StorageID & table_id_);

    std::string getName() const override { return "SystemReplicas"; }

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
