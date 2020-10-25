#pragma once

#include "Aliases.h"
#include "Internals.h"
#include "ClusterPartition.h"

#include <Core/Defines.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
    extern const int LOGICAL_ERROR;
}

struct TaskShard;

struct TaskTable
{
    TaskTable(TaskCluster & parent, const Poco::Util::AbstractConfiguration & config, const String & prefix,
              const String & table_key);

    TaskCluster & task_cluster;

    /// These functions used in checkPartitionIsDone() or checkPartitionPieceIsDone()
    /// They are implemented here not to call task_table.tasks_shard[partition_name].second.pieces[current_piece_number] etc.

    String getPartitionPath(const String & partition_name) const;

    String getPartitionAttachIsActivePath(const String & partition_name) const;

    String getPartitionAttachIsDonePath(const String & partition_name) const;

    String getPartitionPiecePath(const String & partition_name, const size_t piece_number) const;

    String getCertainPartitionIsDirtyPath(const String & partition_name) const;

    String getCertainPartitionPieceIsDirtyPath(const String & partition_name, const size_t piece_number) const;

    String getCertainPartitionIsCleanedPath(const String & partition_name) const;

    String getCertainPartitionPieceIsCleanedPath(const String & partition_name, const size_t piece_number) const;

    String getCertainPartitionTaskStatusPath(const String & partition_name) const;

    String getCertainPartitionPieceTaskStatusPath(const String & partition_name, const size_t piece_number) const;


    bool isReplicatedTable() const { return engine_push_zk_path != ""; }

    /// Partitions will be splitted into number-of-splits pieces.
    /// Each piece will be copied independently. (10 by default)
    size_t number_of_splits;

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

    ASTPtr rewriteReplicatedCreateQueryToPlain();

    /*
     * A Distributed table definition used to split data
     * Distributed table will be created on each shard of default
     * cluster to perform data copying and resharding
     * */
    String sharding_key_str;
    ASTPtr sharding_key_ast;
    ASTPtr main_engine_split_ast;

    /*
     * To copy partiton piece form one cluster to another we have to use Distributed table.
     * In case of usage separate table (engine_push) for each partiton piece,
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
     * So we have to check whether particular shard have current partiton or not while processing.
     */
    TasksShard all_shards;
    TasksShard local_shards;

    /// All partitions of the current table.
    ClusterPartitions cluster_partitions;
    NameSet finished_cluster_partitions;

    /// Parition names to process in user-specified order
    Strings ordered_partition_names;

    ClusterPartition & getClusterPartition(const String & partition_name)
    {
        auto it = cluster_partitions.find(partition_name);
        if (it == cluster_partitions.end())
            throw Exception("There are no cluster partition " + partition_name + " in " + table_id,
                            ErrorCodes::LOGICAL_ERROR);
        return it->second;
    }

    Stopwatch watch;
    UInt64 bytes_copied = 0;
    UInt64 rows_copied = 0;

    template <typename RandomEngine>
    void initShards(RandomEngine &&random_engine);
};


struct TaskShard
{
    TaskShard(TaskTable & parent, const ShardInfo & info_) : task_table(parent), info(info_)
    {
        list_of_split_tables_on_shard.assign(task_table.number_of_splits, DatabaseAndTableName());
    }

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
    DatabaseAndTableName main_table_split_shard;
    ListOfDatabasesAndTableNames list_of_split_tables_on_shard;
};


inline String TaskTable::getPartitionPath(const String & partition_name) const
{
    return task_cluster.task_zookeeper_path             // root
           + "/tables/" + table_id                      // tables/dst_cluster.merge.hits
           + "/" + escapeForFileName(partition_name);   // 201701
}

inline String TaskTable::getPartitionAttachIsActivePath(const String & partition_name) const
{
    return getPartitionPath(partition_name) + "/attach_active";
}

inline String TaskTable::getPartitionAttachIsDonePath(const String & partition_name) const
{
    return getPartitionPath(partition_name) + "/attach_is_done";
}

inline String TaskTable::getPartitionPiecePath(const String & partition_name, size_t piece_number) const
{
    assert(piece_number < number_of_splits);
    return getPartitionPath(partition_name) + "/piece_" + toString(piece_number);  // 1...number_of_splits
}

inline String TaskTable::getCertainPartitionIsDirtyPath(const String &partition_name) const
{
    return getPartitionPath(partition_name) + "/is_dirty";
}

inline String TaskTable::getCertainPartitionPieceIsDirtyPath(const String & partition_name, const size_t piece_number) const
{
    return getPartitionPiecePath(partition_name, piece_number) + "/is_dirty";
}

inline String TaskTable::getCertainPartitionIsCleanedPath(const String & partition_name) const
{
    return getCertainPartitionIsDirtyPath(partition_name) + "/cleaned";
}

inline String TaskTable::getCertainPartitionPieceIsCleanedPath(const String & partition_name, const size_t piece_number) const
{
    return getCertainPartitionPieceIsDirtyPath(partition_name, piece_number) + "/cleaned";
}

inline String TaskTable::getCertainPartitionTaskStatusPath(const String & partition_name) const
{
    return getPartitionPath(partition_name) + "/shards";
}

inline String TaskTable::getCertainPartitionPieceTaskStatusPath(const String & partition_name, const size_t piece_number) const
{
    return getPartitionPiecePath(partition_name, piece_number) + "/shards";
}

inline TaskTable::TaskTable(TaskCluster & parent, const Poco::Util::AbstractConfiguration & config,
                     const String & prefix_, const String & table_key)
        : task_cluster(parent)
{
    String table_prefix = prefix_ + "." + table_key + ".";

    name_in_config = table_key;

    number_of_splits = config.getUInt64(table_prefix + "number_of_splits", 10);

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
        engine_push_ast = parseQuery(parser_storage, engine_push_str, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
        engine_push_partition_key_ast = extractPartitionKey(engine_push_ast);
        primary_key_comma_separated = Nested::createCommaSeparatedStringFrom(extractPrimaryKeyColumnNames(engine_push_ast));
        engine_push_zk_path = extractReplicatedTableZookeeperPath(engine_push_ast);
    }

    sharding_key_str = config.getString(table_prefix + "sharding_key");

    auxiliary_engine_split_asts.reserve(number_of_splits);
    {
        ParserExpressionWithOptionalAlias parser_expression(false);
        sharding_key_ast = parseQuery(parser_expression, sharding_key_str, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);
        main_engine_split_ast = createASTStorageDistributed(cluster_push_name, table_push.first, table_push.second,
                                                            sharding_key_ast);

        for (const auto piece_number : ext::range(0, number_of_splits))
        {
            auxiliary_engine_split_asts.emplace_back
                    (
                            createASTStorageDistributed(cluster_push_name, table_push.first,
                                                        table_push.second + "_piece_" + toString(piece_number), sharding_key_ast)
                    );
        }
    }

    where_condition_str = config.getString(table_prefix + "where_condition", "");
    if (!where_condition_str.empty())
    {
        ParserExpressionWithOptionalAlias parser_expression(false);
        where_condition_ast = parseQuery(parser_expression, where_condition_str, 0, DBMS_DEFAULT_MAX_PARSER_DEPTH);

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
            for (const String &key : keys)
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

inline ASTPtr TaskTable::rewriteReplicatedCreateQueryToPlain()
{
    ASTPtr prev_engine_push_ast = engine_push_ast->clone();

    auto & new_storage_ast = prev_engine_push_ast->as<ASTStorage &>();
    auto & new_engine_ast = new_storage_ast.engine->as<ASTFunction &>();

    auto & replicated_table_arguments = new_engine_ast.arguments->children;

    /// Delete first two arguments of Replicated...MergeTree() table.
    replicated_table_arguments.erase(replicated_table_arguments.begin());
    replicated_table_arguments.erase(replicated_table_arguments.begin());

    /// Remove replicated from name
    new_engine_ast.name = new_engine_ast.name.substr(10);

    return new_storage_ast.clone();
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
    auto & replicas = task_table.cluster_pull->getShardsAddresses().at(indexInCluster());
    return replicas.at(0).readableString();
}

}
