#pragma once

#include "Aliases.h"
#include "Internals.h"
#include "ClusterPartition.h"

namespace DB
{

struct TaskShard;

struct TaskTable
{
    TaskTable(TaskCluster & parent, const Poco::Util::AbstractConfiguration & config, const String & prefix,
              const String & table_key);

    TaskCluster & task_cluster;

    String getPartitionPath(const String & partition_name) const;
    String getPartitionIsDirtyPath(const String & partition_name) const;
    String getPartitionIsCleanedPath(const String & partition_name) const;
    String getPartitionTaskStatusPath(const String & partition_name) const;

    String name_in_config;

    /// Used as task ID
    String table_id;

    /// Source cluster and table
    String cluster_pull_name;
    DatabaseAndTableName table_pull;

    /// Destination cluster and table
    String cluster_push_name;
    DatabaseAndTableName table_push;

    /// Storage of destination table
    String engine_push_str;
    ASTPtr engine_push_ast;
    ASTPtr engine_push_partition_key_ast;

    /// A Distributed table definition used to split data
    String sharding_key_str;
    ASTPtr sharding_key_ast;
    ASTPtr engine_split_ast;

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

    /// Prioritized list of shards
    TasksShard all_shards;
    TasksShard local_shards;

    ClusterPartitions cluster_partitions;
    NameSet finished_cluster_partitions;

    /// Parition names to process in user-specified order
    Strings ordered_partition_names;

    ClusterPartition & getClusterPartition(const String & partition_name)
    {
        auto it = cluster_partitions.find(partition_name);
        if (it == cluster_partitions.end())
            throw Exception("There are no cluster partition " + partition_name + " in " + table_id, ErrorCodes::LOGICAL_ERROR);
        return it->second;
    }

    Stopwatch watch;
    UInt64 bytes_copied = 0;
    UInt64 rows_copied = 0;

    template <typename RandomEngine>
    void initShards(RandomEngine && random_engine);
};


struct TaskShard
{
    TaskShard(TaskTable &parent, const ShardInfo &info_) : task_table(parent), info(info_) {}

    TaskTable & task_table;

    ShardInfo info;

    UInt32 numberInCluster() const { return info.shard_num; }

    UInt32 indexInCluster() const { return info.shard_num - 1; }

    String getDescription() const;

    String getHostNameExample() const;

    /// Used to sort clusters by their proximity
    ShardPriority priority;

    /// Column with unique destination partitions (computed from engine_push_partition_key expr.) in the shard
    ColumnWithTypeAndName partition_key_column;

    /// There is a task for each destination partition
    TasksPartition partition_tasks;

    /// Which partitions have been checked for existence
    /// If some partition from this lists is exists, it is in partition_tasks
    std::set<String> checked_partitions;

    /// Last CREATE TABLE query of the table of the shard
    ASTPtr current_pull_table_create_query;

    /// Internal distributed tables
    DatabaseAndTableName table_read_shard;
    DatabaseAndTableName table_split_shard;
};


inline String TaskTable::getPartitionPath(const String & partition_name) const
{
    return task_cluster.task_zookeeper_path             // root
           + "/tables/" + table_id                      // tables/dst_cluster.merge.hits
           + "/" + escapeForFileName(partition_name);   // 201701
}

inline String TaskTable::getPartitionIsDirtyPath(const String & partition_name) const
{
    return getPartitionPath(partition_name) + "/is_dirty";
}

inline String TaskTable::getPartitionIsCleanedPath(const String & partition_name) const
{
    return getPartitionIsDirtyPath(partition_name) + "/cleaned";
}

inline String TaskTable::getPartitionTaskStatusPath(const String & partition_name) const
{
    return getPartitionPath(partition_name) + "/shards";
}

inline TaskTable::TaskTable(TaskCluster & parent, const Poco::Util::AbstractConfiguration & config, const String & prefix_,
                     const String & table_key)
        : task_cluster(parent)
{
    String table_prefix = prefix_ + "." + table_key + ".";

    name_in_config = table_key;

    cluster_pull_name = config.getString(table_prefix + "cluster_pull");
    cluster_push_name = config.getString(table_prefix + "cluster_push");

    table_pull.first = config.getString(table_prefix + "database_pull");
    table_pull.second = config.getString(table_prefix + "table_pull");

    table_push.first = config.getString(table_prefix + "database_push");
    table_push.second = config.getString(table_prefix + "table_push");

    /// Used as node name in ZooKeeper
    table_id = escapeForFileName(cluster_push_name)
               + "." + escapeForFileName(table_push.first)
               + "." + escapeForFileName(table_push.second);

    engine_push_str = config.getString(table_prefix + "engine");
    {
        ParserStorage parser_storage;
        engine_push_ast = parseQuery(parser_storage, engine_push_str, 0);
        engine_push_partition_key_ast = extractPartitionKey(engine_push_ast);
    }

    sharding_key_str = config.getString(table_prefix + "sharding_key");
    {
        ParserExpressionWithOptionalAlias parser_expression(false);
        sharding_key_ast = parseQuery(parser_expression, sharding_key_str, 0);
        engine_split_ast = createASTStorageDistributed(cluster_push_name, table_push.first, table_push.second, sharding_key_ast);
    }

    where_condition_str = config.getString(table_prefix + "where_condition", "");
    if (!where_condition_str.empty())
    {
        ParserExpressionWithOptionalAlias parser_expression(false);
        where_condition_ast = parseQuery(parser_expression, where_condition_str, 0);

        // Will use canonical expression form
        where_condition_str = queryToString(where_condition_ast);
    }

    String enabled_partitions_prefix = table_prefix + "enabled_partitions";
    has_enabled_partitions = config.has(enabled_partitions_prefix);

    if (has_enabled_partitions)
    {
        Strings keys;
        config.keys(enabled_partitions_prefix, keys);

        if (keys.empty())
        {
            /// Parse list of partition from space-separated string
            String partitions_str = config.getString(table_prefix + "enabled_partitions");
            boost::trim_if(partitions_str, isWhitespaceASCII);
            boost::split(enabled_partitions, partitions_str, isWhitespaceASCII, boost::token_compress_on);
        }
        else
        {
            /// Parse sequence of <partition>...</partition>
            for (const String & key : keys)
            {
                if (!startsWith(key, "partition"))
                    throw Exception("Unknown key " + key + " in " + enabled_partitions_prefix, ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);

                enabled_partitions.emplace_back(config.getString(enabled_partitions_prefix + "." + key));
            }
        }

        std::copy(enabled_partitions.begin(), enabled_partitions.end(), std::inserter(enabled_partitions_set, enabled_partitions_set.begin()));
    }
}

template<typename RandomEngine>
inline void TaskTable::initShards(RandomEngine && random_engine)
{
    const String & fqdn_name = getFQDNOrHostName();
    std::uniform_int_distribution<UInt8> get_urand(0, std::numeric_limits<UInt8>::max());

    // Compute the priority
    for (auto & shard_info : cluster_pull->getShardsInfo())
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


inline String DB::TaskShard::getDescription() const
{
    std::stringstream ss;
    ss << "N" << numberInCluster()
       << " (having a replica " << getHostNameExample()
       << ", pull table " + getQuotedTable(task_table.table_pull)
       << " of cluster " + task_table.cluster_pull_name << ")";
    return ss.str();
}

inline String DB::TaskShard::getHostNameExample() const
{
    auto &replicas = task_table.cluster_pull->getShardsAddresses().at(indexInCluster());
    return replicas.at(0).readableString();
}

}
