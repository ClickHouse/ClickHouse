#pragma once

#include <Core/QueryProcessingStage.h>
#include <Interpreters/ClusterProxy/IStreamFactory.h>
#include <Interpreters/StorageID.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/StorageSnapshot.h>

namespace DB
{

namespace ClusterProxy
{

using ColumnsDescriptionByShardNum = std::unordered_map<UInt32, ColumnsDescription>;

class SelectStreamFactory final : public IStreamFactory
{
public:
    SelectStreamFactory(
        const Block & header_,
        const ColumnsDescriptionByShardNum & objects_by_shard_,
        const StorageSnapshotPtr & storage_snapshot_,
        QueryProcessingStage::Enum processed_stage_);

    void createForShard(
        const Cluster::ShardInfo & shard_info,
        const ASTPtr & query_ast,
        const StorageID & main_table,
        const ASTPtr & table_func_ptr,
        ContextPtr context,
        std::vector<QueryPlanPtr> & local_plans,
        Shards & remote_shards,
        UInt32 shard_count) override;

private:
    const Block header;
    const ColumnsDescriptionByShardNum objects_by_shard;
    const StorageSnapshotPtr storage_snapshot;
    QueryProcessingStage::Enum processed_stage;
};

}

}
