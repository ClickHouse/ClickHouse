#include <Storages/MergeTree/ReplicatedMergeTreeClusterSink.h>
#include <Storages/MergeTree/ReplicatedMergeTreeClusterReplica.h>
#include <Storages/StorageReplicatedMergeTree.h>
#include <Storages/StorageInMemoryMetadata.h>
#include <Processors/Sinks/RemoteSink.h>
#include <Processors/Executors/PushingPipelineExecutor.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTExpressionList.h>
#include <Common/thread_local_rng.h>
#include <Parsers/queryToString.h>
#include <base/defines.h>
#include <algorithm>

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
};

namespace
{

using namespace DB;

ASTPtr createInsertToRemoteTableQuery(StorageID storage_id, const Names & column_names)
{
    auto query = std::make_shared<ASTInsertQuery>();
    query->table_id = storage_id;
    auto columns = std::make_shared<ASTExpressionList>();
    query->columns = columns;
    query->children.push_back(columns);
    for (const auto & column_name : column_names)
        columns->children.push_back(std::make_shared<ASTIdentifier>(column_name));
    return query;
}

}

namespace DB
{

ReplicatedMergeTreeClusterSink::ReplicatedMergeTreeClusterSink(
    StorageReplicatedMergeTree & storage_,
    const StorageMetadataPtr & metadata_snapshot_,
    ContextPtr context_)
    : SinkToStorage(metadata_snapshot_->getSampleBlock())
    , storage(storage_)
    , metadata_snapshot(metadata_snapshot_)
    , context(context_)
    , log(&Poco::Logger::get(storage.getLogName() + " (Cluster OutputStream)"))
{
    chassert(storage.replicated_cluster.has_value());
}

ReplicatedMergeTreeClusterSink::~ReplicatedMergeTreeClusterSink() = default;

void ReplicatedMergeTreeClusterSink::consume(Chunk chunk)
{
    auto input_block = getHeader().cloneWithColumns(chunk.detachColumns());
    const Settings & settings = context->getSettingsRef();

    BlocksWithPartition part_blocks = storage.writer.splitBlockIntoParts(
        std::move(input_block),
        settings.max_partitions_per_insert_block,
        metadata_snapshot,
        context,
        /* async_insert_info= */ {});

    for (const BlockWithPartition & replica_block : part_blocks)
    {
        MergeTreePartition partition(replica_block.partition);
        String partition_id = partition.getID(storage);

        /// FIXME: this should be done with cluster partition version check to
        /// ensure that nothing will goes to outdated replicas.
        const auto & cluster_partition = storage.replicated_cluster->getOrCreateClusterPartition(partition_id);
        ReplicatedMergeTreeClusterReplicas cluster_replicas = storage.replicated_cluster->getClusterReplicas();
        auto replicas = cluster_partition.getAllNonMigrationReplicas();
        /// Remove replicas for which we don't have enough information
        std::erase_if(replicas, [&](const auto & replica)
        {
            const auto it = cluster_replicas.find(replica);
            if (it == cluster_replicas.end())
                return true;
            if (it->second.host.empty())
                return true;
            return false;
        });
        if (replicas.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "No replicas had been assigned for partition {}", partition_id);

        /// Other replicas will catch up from this replica.
        const auto & replica_name = replicas[thread_local_rng() % replicas.size()];
        const auto & replica = cluster_replicas[replica_name];

        LOG_TRACE(log, "Sending block (partition: {}, rows: {}) to replica {}",
            partition_id,
            replica_block.block.rows(),
            replica.toStringForLog());
        sendBlockToReplica(replica, replica_block.block);
    }
}

void ReplicatedMergeTreeClusterSink::sendBlockToReplica(const ReplicatedMergeTreeClusterReplica & replica, const Block & block)
{
    const Settings & settings = context->getSettingsRef();

    Cluster::ShardInfo shard_info = replica.makeShardInfo(context);
    StorageID replica_storage_id(replica.database, replica.table);
    auto timeouts = ConnectionTimeouts::getTCPTimeoutsWithFailover(settings);
    auto results = shard_info.pool->getManyChecked(timeouts, settings, PoolMode::GET_ONE, replica_storage_id.getQualifiedName());
    if (results.empty() || results.front().entry.isNull())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Expected exactly one connection for replica {}", replica.toStringForLog());
    auto connection = std::move(results.front().entry);

    auto query_ast = createInsertToRemoteTableQuery(replica_storage_id, getHeader().getNames());
    const String & query_string = queryToString(query_ast);

    auto remote_sink = std::make_shared<RemoteSink>(*connection, timeouts, query_string, settings, context->getClientInfo());
    QueryPipeline pipeline(std::move(remote_sink));
    PushingPipelineExecutor executor(pipeline);
    executor.start();
    executor.push(block);
    executor.finish();

    /// TODO(cluster):
    /// - metrics
    /// - throttling
    /// - opentelemetry
    /// - local node handling
    /// - empty block handling
    /// - block adapting?
}

};
