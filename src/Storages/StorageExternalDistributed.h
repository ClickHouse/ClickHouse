#pragma once

#include "config.h"

#include <Storages/IStorage.h>


namespace DB
{

/// Storages MySQL and PostgreSQL use ConnectionPoolWithFailover and support multiple replicas.
/// This class unites multiple storages with replicas into multiple shards with replicas.
/// A query to external database is passed to one replica on each shard, the result is united.
/// Replicas on each shard have the same priority, traversed replicas are moved to the end of the queue.
/// Similar approach is used for URL storage.
class StorageExternalDistributed final : public DB::IStorage
{
public:
    StorageExternalDistributed(
        const StorageID & table_id_,
        std::unordered_set<StoragePtr> && shards_,
        const ColumnsDescription & columns_,
        const ConstraintsDescription & constraints_,
        const String & comment);

    std::string getName() const override { return "ExternalDistributed"; }

    void read(
        QueryPlan & query_plan,
        const Names & column_names,
        const StorageSnapshotPtr & storage_snapshot,
        SelectQueryInfo & query_info,
        ContextPtr context,
        QueryProcessingStage::Enum processed_stage,
        size_t max_block_size,
        size_t num_streams) override;

private:
    using Shards = std::unordered_set<StoragePtr>;
    Shards shards;
};

}
