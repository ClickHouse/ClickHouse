#pragma once

#include "Aliases.h"
#include "TaskShard.h"


namespace DB
{

struct ClusterPartition;
struct TaskCluster;

struct TaskTable
{
    TaskTable(TaskCluster & parent, const Poco::Util::AbstractConfiguration & config, const String & prefix, const String & table_key);

    TaskCluster & task_cluster;

    /// These functions used in checkPartitionIsDone() or checkPartitionPieceIsDone()
    /// They are implemented here not to call task_table.tasks_shard[partition_name].second.pieces[current_piece_number] etc.

    String getPartitionPath(const String & partition_name) const;

    String getPartitionAttachIsActivePath(const String & partition_name) const;

    String getPartitionAttachIsDonePath(const String & partition_name) const;

    String getPartitionPiecePath(const String & partition_name, size_t piece_number) const;

    String getCertainPartitionIsDirtyPath(const String & partition_name) const;

    String getCertainPartitionPieceIsDirtyPath(const String & partition_name, size_t piece_number) const;

    String getCertainPartitionIsCleanedPath(const String & partition_name) const;

    String getCertainPartitionPieceIsCleanedPath(const String & partition_name, size_t piece_number) const;

    String getCertainPartitionTaskStatusPath(const String & partition_name) const;

    String getCertainPartitionPieceTaskStatusPath(const String & partition_name, size_t piece_number) const;

    bool isReplicatedTable() const;

    /// These nodes are used for check-status option
    String getStatusAllPartitionCount() const;
    String getStatusProcessedPartitionsCount() const;

    /// Partitions will be split into number-of-splits pieces.
    /// Each piece will be copied independently. (10 by default)
    size_t number_of_splits;

    bool allow_to_copy_alias_and_materialized_columns{false};
    bool allow_to_drop_target_partitions{false};

    String name_in_config;

    /// Used as task ID
    String table_id;

    /// Column names in primary key
    String primary_key_comma_separated;

    /// Source cluster and table
    String cluster_pull_name;
    DatabaseAndTableName table_pull;

    /// Destination cluster and table
    String cluster_push_name;
    DatabaseAndTableName table_push;

    /// Storage of destination table
    /// (tables that are stored on each shard of target cluster)
    String engine_push_str;
    ASTPtr engine_push_ast;
    ASTPtr engine_push_partition_key_ast;

    /// First argument of Replicated...MergeTree()
    String engine_push_zk_path;
    bool is_replicated_table;

    ASTPtr rewriteReplicatedCreateQueryToPlain() const;

    /*
     * A Distributed table definition used to split data
     * Distributed table will be created on each shard of default
     * cluster to perform data copying and resharding
     * */
    String sharding_key_str;
    ASTPtr sharding_key_ast;
    ASTPtr main_engine_split_ast;

    /*
     * To copy partition piece form one cluster to another we have to use Distributed table.
     * In case of usage separate table (engine_push) for each partition piece,
     * we have to use many Distributed tables.
     * */
    ASTs auxiliary_engine_split_asts;

    /// Additional WHERE expression to filter input data
    String where_condition_str;
    ASTPtr where_condition_ast;

    /// Resolved clusters
    ClusterPtr cluster_pull;
    ClusterPtr cluster_push;

    /// Filter partitions that should be copied
    bool has_enabled_partitions = false;
    Strings enabled_partitions;
    NameSet enabled_partitions_set;

    /**
     * Prioritized list of shards
     * all_shards contains information about all shards in the table.
     * So we have to check whether particular shard have current partition or not while processing.
     */
    TasksShard all_shards;
    TasksShard local_shards;

    /// All partitions of the current table.
    ClusterPartitions cluster_partitions;
    NameSet finished_cluster_partitions;

    /// Partition names to process in user-specified order
    Strings ordered_partition_names;

    ClusterPartition & getClusterPartition(const String & partition_name);

    Stopwatch watch;
    UInt64 bytes_copied = 0;
    UInt64 rows_copied = 0;

    template <typename RandomEngine>
    void initShards(RandomEngine &&random_engine);
};

using TasksTable = std::list<TaskTable>;


template<typename RandomEngine>
inline void TaskTable::initShards(RandomEngine && random_engine)
{
    const String & fqdn_name = getFQDNOrHostName();
    std::uniform_int_distribution<uint8_t> get_urand(0, std::numeric_limits<UInt8>::max());

    // Compute the priority
    for (const auto & shard_info : cluster_pull->getShardsInfo())
    {
        TaskShardPtr task_shard = std::make_shared<TaskShard>(*this, shard_info);
        const auto & replicas = cluster_pull->getShardsAddresses().at(task_shard->indexInCluster());
        task_shard->priority = getReplicasPriority(replicas, fqdn_name, get_urand(random_engine));

        all_shards.emplace_back(task_shard);
    }

    // Sort by priority
    std::sort(all_shards.begin(), all_shards.end(),
              [](const TaskShardPtr & lhs, const TaskShardPtr & rhs)
              {
                  return ShardPriority::greaterPriority(lhs->priority, rhs->priority);
              });

    // Cut local shards
    auto it_first_remote = std::lower_bound(all_shards.begin(), all_shards.end(), 1,
                                            [](const TaskShardPtr & lhs, UInt8 is_remote)
                                            {
                                                return lhs->priority.is_remote < is_remote;
                                            });

    local_shards.assign(all_shards.begin(), it_first_remote);
}

}
