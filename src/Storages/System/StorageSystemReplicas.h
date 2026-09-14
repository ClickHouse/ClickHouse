#pragma once

#include <memory>
#include <Storages/System/IStorageSystemOneBlock.h>


namespace DB
{

class Context;
template <typename T, typename ...Ts>
class StatusRequestsPools;
class StorageReplicatedMergeTree;

/** Implements `replicas` system table, which provides information about the status of the replicated tables.
  */
class StorageSystemReplicas final : public IStorage
{
public:
    using TPools = StatusRequestsPools<StorageReplicatedMergeTree>;

    explicit StorageSystemReplicas(const StorageID & table_id_);
    ~StorageSystemReplicas() override;

    std::string getName() const override { return "SystemReplicas"; }

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

private:
    std::shared_ptr<TPools> pools;
};

}
