#include "ClusterCopier.h"

#include <chrono>

#include <Poco/Util/XMLConfiguration.h>
#include <Poco/Logger.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/FormattingChannel.h>
#include <Poco/PatternFormatter.h>
#include <Poco/UUIDGenerator.h>
#include <Poco/File.h>
#include <Poco/Process.h>
#include <Poco/FileChannel.h>
#include <Poco/SplitterChannel.h>
#include <Poco/Util/HelpFormatter.h>

#include <boost/algorithm/string.hpp>
#include <pcg_random.hpp>

#include <Common/Exception.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/getFQDNOrHostName.h>
#include <Client/Connection.h>
#include <Interpreters/Context.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/InterpreterExistsQuery.h>
#include <Interpreters/InterpreterShowCreateQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Interpreters/InterpreterCreateQuery.h>

#include <common/logger_useful.h>
#include <common/ThreadPool.h>
#include <Common/typeid_cast.h>
#include <Common/ClickHouseRevision.h>
#include <Common/escapeForFileName.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Storages/StorageDistributed.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/queryToString.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTExpressionList.h>
#include <Databases/DatabaseMemory.h>
#include <DataStreams/RemoteBlockInputStream.h>
#include <DataStreams/SquashingBlockInputStream.h>
#include <Common/isLocalAddress.h>
#include <DataStreams/copyData.h>
#include <DataTypes/DataTypeString.h>
#include <DataStreams/NullBlockOutputStream.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <Functions/registerFunctions.h>
#include <TableFunctions/registerTableFunctions.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Server/StatusFile.h>
#include <Storages/registerStorages.h>
#include <Common/formatReadable.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NO_ZOOKEEPER;
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_TABLE;
    extern const int UNFINISHED;
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
}


using ConfigurationPtr = Poco::AutoPtr<Poco::Util::AbstractConfiguration>;

static ConfigurationPtr getConfigurationFromXMLString(const std::string & xml_data)
{
    std::stringstream ss(xml_data);
    Poco::XML::InputSource input_source{ss};
    return {new Poco::Util::XMLConfiguration{&input_source}};
}

namespace
{


using DatabaseAndTableName = std::pair<String, String>;

String getDatabaseDotTable(const String & database, const String & table)
{
    return backQuoteIfNeed(database) + "." + backQuoteIfNeed(table);
}

String getDatabaseDotTable(const DatabaseAndTableName & db_and_table)
{
    return getDatabaseDotTable(db_and_table.first, db_and_table.second);
}



enum class TaskState
{
    Started = 0,
    Finished,
    Unknown
};


/// Used to mark status of shard partition tasks
struct TaskStateWithOwner
{
    TaskStateWithOwner() = default;
    TaskStateWithOwner(TaskState state, const String & owner) : state(state), owner(owner) {}

    TaskState state{TaskState::Unknown};
    String owner;

    static String getData(TaskState state, const String & owner)
    {
        return TaskStateWithOwner(state, owner).toString();
    }

    String toString()
    {
        WriteBufferFromOwnString wb;
        wb << static_cast<UInt32>(state) << "\n" << escape << owner;
        return wb.str();
    }

    static TaskStateWithOwner fromString(const String & data)
    {
        ReadBufferFromString rb(data);
        TaskStateWithOwner res;
        UInt32 state;

        rb >> state >> "\n" >> escape >> res.owner;

        if (state >= static_cast<int>(TaskState::Unknown))
            throw Exception("Unknown state " + data, ErrorCodes::LOGICAL_ERROR);

        res.state = static_cast<TaskState>(state);
        return res;
    }
};


/// Hierarchical description of the tasks
struct ShardPartition;
struct TaskShard;
struct TaskTable;
struct TaskCluster;
struct ClusterPartition;

using TasksPartition = std::map<String, ShardPartition>;
using ShardInfo = Cluster::ShardInfo;
using TaskShardPtr = std::shared_ptr<TaskShard>;
using TasksShard = std::vector<TaskShardPtr>;
using TasksTable = std::list<TaskTable>;
using ClusterPartitions = std::map<String, ClusterPartition>;


/// Just destination partition of a shard
struct ShardPartition
{
    ShardPartition(TaskShard & parent, const String & name_quoted_) : task_shard(parent), name(name_quoted_) {}

    String getPartitionPath() const;
    String getCommonPartitionIsDirtyPath() const;
    String getPartitionActiveWorkersPath() const;
    String getActiveWorkerPath() const;
    String getPartitionShardsPath() const;
    String getShardStatusPath() const;

    TaskShard & task_shard;
    String name;
};


struct ShardPriority
{
    UInt8 is_remote = 1;
    size_t hostname_difference = 0;
    UInt8 random = 0;

    static bool greaterPriority(const ShardPriority & current, const ShardPriority & other)
    {
        return std::forward_as_tuple(current.is_remote, current.hostname_difference, current.random)
               < std::forward_as_tuple(other.is_remote, other.hostname_difference, other.random);
    }
};


struct TaskShard
{
    TaskShard(TaskTable & parent, const ShardInfo & info_) : task_table(parent), info(info_) {}

    TaskTable & task_table;

    ShardInfo info;
    UInt32 numberInCluster() const { return info.shard_num; }
    UInt32 indexInCluster() const { return info.shard_num - 1; }

    String getDescription() const;

    /// Used to sort clusters by thier proximity
    ShardPriority priority;

    /// Column with unique destination partitions (computed from engine_push_partition_key expr.) in the shard
    ColumnWithTypeAndName partition_key_column;

    /// There is a task for each destination partition
    TasksPartition partition_tasks;

    /// Last CREATE TABLE query of the table of the shard
    ASTPtr current_pull_table_create_query;

    /// Internal distributed tables
    DatabaseAndTableName table_read_shard;
    DatabaseAndTableName table_split_shard;
};


/// Contains all cluster shards that contain a partition (and sorted by the proximity)
struct ClusterPartition
{
    TasksShard shards; /// having that partition

    Stopwatch watch;
    UInt64 bytes_copied = 0;
    UInt64 rows_copied = 0;

    size_t total_tries = 0;
};


struct TaskTable
{
    TaskTable(TaskCluster & parent, const Poco::Util::AbstractConfiguration & config, const String & prefix,
                  const String & table_key);

    TaskCluster & task_cluster;

    String getPartitionPath(const String & partition_name) const;
    String getPartitionIsDirtyPath(const String & partition_name) const;

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

    /// Local Distributed table used to split data
    DatabaseAndTableName table_split;
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


struct TaskCluster
{
    TaskCluster(const String & task_zookeeper_path_, const String & default_local_database_)
        : task_zookeeper_path(task_zookeeper_path_),  default_local_database(default_local_database_) {}

    void loadTasks(const Poco::Util::AbstractConfiguration & config, const String & base_key = "");

    /// Set (or update) settings and max_workers param
    void reloadSettings(const Poco::Util::AbstractConfiguration & config, const String & base_key = "");

    /// Base node for all tasks. Its structure:
    ///  workers/ - directory with active workers (amount of them is less or equal max_workers)
    ///  description - node with task configuration
    ///  table_table1/ - directories with per-partition copying status
    String task_zookeeper_path;

    /// Database used to create temporary Distributed tables
    String default_local_database;

    /// Limits number of simultaneous workers
    size_t max_workers = 0;

    /// Base settings for pull and push
    Settings settings_common;
    /// Settings used to fetch data
    Settings settings_pull;
    /// Settings used to insert data
    Settings settings_push;

    String clusters_prefix;

    /// Subtasks
    TasksTable table_tasks;

    std::random_device random_device;
    pcg64 random_engine;
};


/// Atomically checks that is_dirty node is not exists, and made the remaining op
/// Returns relative number of failed operation in the second field (the passed op has 0 index)
static void checkNoNodeAndCommit(
    const zkutil::ZooKeeperPtr & zookeeper,
    const String & checking_node_path,
    zkutil::OpPtr && op,
    zkutil::MultiTransactionInfo & info)
{
    zkutil::Ops ops;
    ops.emplace_back(std::make_unique<zkutil::Op::Create>(checking_node_path, "", zookeeper->getDefaultACL(), zkutil::CreateMode::Persistent));
    ops.emplace_back(std::make_unique<zkutil::Op::Remove>(checking_node_path, -1));
    ops.emplace_back(std::move(op));

    zookeeper->tryMultiUnsafe(ops, info);
    if (info.code != ZOK && !zkutil::isUserError(info.code))
        throw info.getException();
}


// Creates AST representing 'ENGINE = Distributed(cluster, db, table, [sharding_key])
std::shared_ptr<ASTStorage> createASTStorageDistributed(
    const String & cluster_name, const String & database, const String & table, const ASTPtr & sharding_key_ast = nullptr)
{
    auto args = std::make_shared<ASTExpressionList>();
    args->children.emplace_back(std::make_shared<ASTLiteral>(cluster_name));
    args->children.emplace_back(std::make_shared<ASTIdentifier>(database));
    args->children.emplace_back(std::make_shared<ASTIdentifier>(table));
    if (sharding_key_ast)
        args->children.emplace_back(sharding_key_ast);

    auto engine = std::make_shared<ASTFunction>();
    engine->name = "Distributed";
    engine->arguments = args;

    auto storage = std::make_shared<ASTStorage>();
    storage->set(storage->engine, engine);

    return storage;
}


BlockInputStreamPtr squashStreamIntoOneBlock(const BlockInputStreamPtr & stream)
{
    return std::make_shared<SquashingBlockInputStream>(
        stream,
        std::numeric_limits<size_t>::max(),
        std::numeric_limits<size_t>::max()
    );
}

Block getBlockWithAllStreamData(const BlockInputStreamPtr & stream)
{
    return squashStreamIntoOneBlock(stream)->read();
}


/// Path getters

String TaskTable::getPartitionPath(const String & partition_name) const
{
    return task_cluster.task_zookeeper_path             // root
           + "/tables/" + table_id                      // tables/dst_cluster.merge.hits
           + "/" + escapeForFileName(partition_name);   // 201701
}

String ShardPartition::getPartitionPath() const
{
    return task_shard.task_table.getPartitionPath(name);
}

String ShardPartition::getShardStatusPath() const
{
    // /root/table_test.hits/201701/1
    return getPartitionPath() + "/shards/" + toString(task_shard.numberInCluster());
}

String ShardPartition::getPartitionShardsPath() const
{
    return getPartitionPath() + "/shards";
}

String ShardPartition::getPartitionActiveWorkersPath() const
{
    return getPartitionPath() + "/partition_active_workers";
}

String ShardPartition::getActiveWorkerPath() const
{
    return getPartitionActiveWorkersPath() + "/" + toString(task_shard.numberInCluster());
}

String ShardPartition::getCommonPartitionIsDirtyPath() const
{
    return getPartitionPath() + "/is_dirty";
}

String TaskTable::getPartitionIsDirtyPath(const String & partition_name) const
{
    return getPartitionPath(partition_name) + "/is_dirty";
}

String DB::TaskShard::getDescription() const
{
    return "№" + toString(numberInCluster())
           + " of pull table " + getDatabaseDotTable(task_table.table_pull)
           + " of cluster " + task_table.cluster_pull_name;
}



static bool isExtedndedDefinitionStorage(const ASTPtr & storage_ast)
{
    const ASTStorage & storage = typeid_cast<const ASTStorage &>(*storage_ast);
    return storage.partition_by || storage.order_by || storage.sample_by;
}

static ASTPtr extractPartitionKey(const ASTPtr & storage_ast)
{
    String storage_str = queryToString(storage_ast);

    const ASTStorage & storage = typeid_cast<const ASTStorage &>(*storage_ast);
    const ASTFunction & engine = typeid_cast<const ASTFunction &>(*storage.engine);

    if (!endsWith(engine.name, "MergeTree"))
    {
        throw Exception("Unsupported engine was specified in " + storage_str + ", only *MergeTree engines are supported",
                        ErrorCodes::BAD_ARGUMENTS);
    }

    ASTPtr arguments_ast = engine.arguments->clone();
    ASTs & arguments = typeid_cast<ASTExpressionList &>(*arguments_ast).children;

    if (isExtedndedDefinitionStorage(storage_ast))
    {
        if (storage.partition_by)
            return storage.partition_by->clone();

        static const char * all = "all";
        return std::make_shared<ASTLiteral>(Field(all, strlen(all)));
    }
    else
    {
        bool is_replicated = startsWith(engine.name, "Replicated");
        size_t min_args = is_replicated ? 3 : 1;

        if (arguments.size() < min_args)
            throw Exception("Expected at least " + toString(min_args) + " arguments in " + storage_str, ErrorCodes::BAD_ARGUMENTS);

        ASTPtr & month_arg = is_replicated ? arguments[2] : arguments[1];
        return makeASTFunction("toYYYYMM", month_arg->clone());
    }
}


TaskTable::TaskTable(TaskCluster & parent, const Poco::Util::AbstractConfiguration & config, const String & prefix_,
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
        engine_push_ast = parseQuery(parser_storage, engine_push_str);
        engine_push_partition_key_ast = extractPartitionKey(engine_push_ast);
    }

    sharding_key_str = config.getString(table_prefix + "sharding_key");
    {
        ParserExpressionWithOptionalAlias parser_expression(false);
        sharding_key_ast = parseQuery(parser_expression, sharding_key_str);
        engine_split_ast = createASTStorageDistributed(cluster_push_name, table_push.first, table_push.second, sharding_key_ast);

        table_split = DatabaseAndTableName(task_cluster.default_local_database, ".split." + table_id);
    }

    where_condition_str = config.getString(table_prefix + "where_condition", "");
    if (!where_condition_str.empty())
    {
        ParserExpressionWithOptionalAlias parser_expression(false);
        where_condition_ast = parseQuery(parser_expression, where_condition_str);

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


static ShardPriority getReplicasPriority(const Cluster::Addresses & replicas, const std::string & local_hostname, UInt8 random)
{
    ShardPriority res;

    if (replicas.empty())
        return res;

    res.is_remote = 1;
    for (auto & replica : replicas)
    {
        if (isLocalAddress(replica.resolved_address))
        {
            res.is_remote = 0;
            break;
        }
    }

    res.hostname_difference = std::numeric_limits<size_t>::max();
    for (auto & replica : replicas)
    {
        size_t difference = getHostNameDifference(local_hostname, replica.host_name);
        res.hostname_difference = std::min(difference, res.hostname_difference);
    }

    res.random = random;
    return res;
}

template<typename RandomEngine>
void TaskTable::initShards(RandomEngine && random_engine)
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
        [] (const TaskShardPtr & lhs, const TaskShardPtr & rhs)
        {
            return ShardPriority::greaterPriority(lhs->priority, rhs->priority);
        });

    // Cut local shards
    auto it_first_remote = std::lower_bound(all_shards.begin(), all_shards.end(), 1,
        [] (const TaskShardPtr & lhs, UInt8 is_remote)
        {
            return lhs->priority.is_remote < is_remote;
        });

    local_shards.assign(all_shards.begin(), it_first_remote);
}


void DB::TaskCluster::loadTasks(const Poco::Util::AbstractConfiguration & config, const String & base_key)
{
    String prefix = base_key.empty() ? "" : base_key + ".";

    clusters_prefix = prefix + "remote_servers";
    if (!config.has(clusters_prefix))
        throw Exception("You should specify list of clusters in " + clusters_prefix, ErrorCodes::BAD_ARGUMENTS);

    Poco::Util::AbstractConfiguration::Keys tables_keys;
    config.keys(prefix + "tables", tables_keys);

    for (const auto & table_key : tables_keys)
    {
        table_tasks.emplace_back(*this, config, prefix + "tables", table_key);
    }
}

void DB::TaskCluster::reloadSettings(const Poco::Util::AbstractConfiguration & config, const String & base_key)
{
    String prefix = base_key.empty() ? "" : base_key + ".";

    max_workers = config.getUInt64(prefix + "max_workers");

    settings_common = Settings();
    if (config.has(prefix + "settings"))
        settings_common.loadSettingsFromConfig(prefix + "settings", config);

    settings_pull = settings_common;
    if (config.has(prefix + "settings_pull"))
        settings_pull.loadSettingsFromConfig(prefix + "settings_pull", config);

    settings_push = settings_common;
    if (config.has(prefix + "settings_push"))
        settings_push.loadSettingsFromConfig(prefix + "settings_push", config);

    /// Override important settings
    settings_pull.load_balancing = LoadBalancing::NEAREST_HOSTNAME;
    settings_pull.limits.readonly = 1;
    settings_pull.max_threads = 1;
    settings_pull.max_block_size = std::min(8192UL, settings_pull.max_block_size.value);
    settings_pull.preferred_block_size_bytes = 0;

    settings_push.insert_distributed_timeout = 0;
    settings_push.insert_distributed_sync = 1;
}


} // end of an anonymous namespace


class ClusterCopier
{
public:

    ClusterCopier(const ConfigurationPtr & zookeeper_config_,
                  const String & task_path_,
                  const String & host_id_,
                  const String & proxy_database_name_,
                  Context & context_)
    :
        zookeeper_config(zookeeper_config_),
        task_zookeeper_path(task_path_),
        host_id(host_id_),
        working_database_name(proxy_database_name_),
        context(context_),
        log(&Poco::Logger::get("ClusterCopier"))
    {
        initZooKeeper();
    }

    void init()
    {
        auto zookeeper = getZooKeeper();

        task_description_watch_callback = [this] (zkutil::ZooKeeper &, int, int, const char *)
        {
            UInt64 version = ++task_descprtion_version;
            LOG_DEBUG(log, "Task description should be updated, local version " << version);
        };

        task_description_path = task_zookeeper_path + "/description";
        task_cluster = std::make_unique<TaskCluster>(task_zookeeper_path, working_database_name);

        reloadTaskDescription();
        task_cluster_initial_config = task_cluster_current_config;

        task_cluster->loadTasks(*task_cluster_initial_config);
        context.setClustersConfig(task_cluster_initial_config, task_cluster->clusters_prefix);

        /// Set up shards and their priority
        task_cluster->random_engine.seed(task_cluster->random_device());
        for (auto & task_table : task_cluster->table_tasks)
        {
            task_table.cluster_pull = context.getCluster(task_table.cluster_pull_name);
            task_table.cluster_push = context.getCluster(task_table.cluster_push_name);
            task_table.initShards(task_cluster->random_engine);
        }

        LOG_DEBUG(log, "Loaded " << task_cluster->table_tasks.size() << " table tasks");

        /// Compute set of partitions, assume set of partitions aren't changed during the processing
        for (auto & task_table : task_cluster->table_tasks)
        {
            LOG_DEBUG(log, "Set up table task " << task_table.table_id);

            for (const TaskShardPtr & task_shard : task_table.all_shards)
            {
                if (task_shard->info.pool == nullptr)
                {
                    throw Exception("It is impossible to have only local shards, at least port number must be different",
                                    ErrorCodes::LOGICAL_ERROR);
                }

                LOG_DEBUG(log, "Set up shard " << task_shard->getDescription());
                LOG_DEBUG(log, "There are " << task_table.all_shards.size() << " shards, " << task_table.local_shards.size() << " of them are local ones");

                auto existing_partitions_names = getShardPartitions(*task_shard);
                Strings filtered_partitions_names;

                /// Check that user specified correct partition names
                auto check_partition_format = [&] (const String & partition_text_quoted)
                {
                    const DataTypePtr & type = task_shard->partition_key_column.type;
                    MutableColumnPtr column_dummy = type->createColumn();
                    ReadBufferFromString rb(partition_text_quoted);

                    try
                    {
                        type->deserializeTextQuoted(*column_dummy, rb);
                    }
                    catch (Exception & e)
                    {
                        throw Exception("Partition " + partition_text_quoted + " has incorrect format. " + e.displayText(), ErrorCodes::BAD_ARGUMENTS);
                    }
                };

                if (task_table.has_enabled_partitions)
                {
                    /// Process partition in order specified by <enabled_partitions/>
                    for (const String & partition_name : task_table.enabled_partitions)
                    {
                        check_partition_format(partition_name);
                        auto it = existing_partitions_names.find(partition_name);

                        /// Do not process partition if it is not in enabled_partitions list
                        if (it == existing_partitions_names.end())
                        {
                            LOG_WARNING(log, "There is no enabled " << partition_name << " specified in enabled_partitions in shard "
                                             << task_shard->getDescription());
                            continue;
                        }

                        filtered_partitions_names.emplace_back(*it);
                    }

                    for (const String & partition_name : existing_partitions_names)
                    {
                        if (!task_table.enabled_partitions_set.count(partition_name))
                        {
                            LOG_DEBUG(log, "Partition " << partition_name << " will not be processed, since it is not in "
                                                        << "enabled_partitions of " << task_table.table_id);
                        }
                    }
                }
                else
                {
                    for (const String & partition_name : existing_partitions_names)
                        filtered_partitions_names.emplace_back(partition_name);
                }

                for (const String & partition_name : filtered_partitions_names)
                {
                    task_shard->partition_tasks.emplace(partition_name, ShardPartition(*task_shard, partition_name));

                    ClusterPartition & cluster_partition = task_table.cluster_partitions[partition_name];
                    cluster_partition.shards.emplace_back(task_shard);
                }

                LOG_DEBUG(log, "Will copy " << task_shard->partition_tasks.size() << " partitions from shard " << task_shard->getDescription());
            }
        }

        getZooKeeper()->createAncestors(getWorkersPath() + "/");
    }

    void reloadTaskDescription()
    {
        String task_config_str;
        zkutil::Stat stat;
        int code;

        getZooKeeper()->tryGetWatch(task_description_path, task_config_str, &stat, task_description_watch_callback, &code);
        if (code != ZOK)
            throw Exception("Can't get description node " + task_description_path, ErrorCodes::BAD_ARGUMENTS);

        LOG_DEBUG(log, "Loading description, zxid=" << task_descprtion_current_stat.czxid);
        auto config = getConfigurationFromXMLString(task_config_str);

        /// Setup settings
        task_cluster->reloadSettings(*config);
        context.getSettingsRef() = task_cluster->settings_common;

        task_cluster_current_config = config;
        task_descprtion_current_stat = stat;
    }

    void updateConfigIfNeeded()
    {
        UInt64 version_to_update = task_descprtion_version;
        if (task_descprtion_current_version == version_to_update)
            return;

        LOG_DEBUG(log, "Updating task description");
        reloadTaskDescription();

        task_descprtion_current_version = version_to_update;
    }

    static constexpr size_t max_table_tries = 1000;
    static constexpr size_t max_partition_tries = 1;

    bool tryProcessTable(TaskTable & task_table)
    {
        /// Process each partition that is present in cluster
        for (auto & elem : task_table.cluster_partitions)
        {
            const String & partition_name = elem.first;
            ClusterPartition & cluster_partition = elem.second;
            const TasksShard & shards_with_partition = cluster_partition.shards;

            if (cluster_partition.total_tries == 0)
                cluster_partition.watch.restart();
            else
                cluster_partition.watch.start();
            SCOPE_EXIT(cluster_partition.watch.stop());

            bool partition_is_done = false;
            size_t num_partition_tries = 0;

            /// Retry partition processing
            while (!partition_is_done && num_partition_tries < max_partition_tries)
            {
                ++num_partition_tries;
                ++cluster_partition.total_tries;

                LOG_DEBUG(log, "Processing partition " << partition_name << " for the whole cluster"
                               << " (" << shards_with_partition.size() << " shards)");

                size_t num_successful_shards = 0;

                /// Process each source shard and copy current partition
                /// NOTE: shards are sorted by "distance" to current host
                for (const TaskShardPtr & shard : shards_with_partition)
                {
                    auto it_shard_partition = shard->partition_tasks.find(partition_name);
                    if (it_shard_partition == shard->partition_tasks.end())
                        throw Exception("There are no such partition in a shard. This is a bug.", ErrorCodes::LOGICAL_ERROR);

                    ShardPartition & task_shard_partition = it_shard_partition->second;
                    if (processPartitionTask(task_shard_partition))
                        ++num_successful_shards;
                }

                try
                {
                    partition_is_done = (num_successful_shards == shards_with_partition.size())
                                        && checkPartitionIsDone(task_table, partition_name, shards_with_partition);
                }
                catch (...)
                {
                    tryLogCurrentException(log);
                    partition_is_done = false;
                }

                if (!partition_is_done)
                    std::this_thread::sleep_for(default_sleep_time);
            }

            if (partition_is_done)
            {
                task_table.finished_cluster_partitions.emplace(partition_name);

                task_table.bytes_copied += cluster_partition.bytes_copied;
                task_table.rows_copied += cluster_partition.rows_copied;

                double elapsed = cluster_partition.watch.elapsedSeconds();

                LOG_INFO(log, "It took " << std::fixed << std::setprecision(2) << elapsed << " seconds to copy partition " << partition_name
                                         << ": " << formatReadableSizeWithDecimalSuffix(cluster_partition.bytes_copied)
                                         << " uncompressed bytes and "
                                         << formatReadableQuantity(cluster_partition.rows_copied) << " rows are copied");

                if (cluster_partition.rows_copied)
                {
                    LOG_INFO(log, "Average partition speed: "
                        << formatReadableSizeWithDecimalSuffix(cluster_partition.bytes_copied / elapsed) << " per second.");
                }

                if (task_table.rows_copied)
                {
                    LOG_INFO(log, "Average table " << task_table.table_id << " speed: "
                                                   << formatReadableSizeWithDecimalSuffix(task_table.bytes_copied / elapsed)
                                                   << " per second.");
                }
            }
        }

        size_t required_partitions = task_table.cluster_partitions.size();
        size_t finished_partitions = task_table.finished_cluster_partitions.size();

        bool table_is_done = task_table.finished_cluster_partitions.size() >= task_table.cluster_partitions.size();
        if (!table_is_done)
        {
            LOG_INFO(log, "Table " + task_table.table_id + " is not processed yet."
                << "Copied " << finished_partitions << " of " << required_partitions << ", will retry");
        }

        return table_is_done;
    }


    void process()
    {
        for (TaskTable & task_table : task_cluster->table_tasks)
        {
            if (task_table.all_shards.empty())
                continue;

            task_table.watch.restart();

            bool table_is_done = false;
            size_t num_table_tries = 0;

            /// Retry table processing
            while (!table_is_done && num_table_tries < max_table_tries)
            {
                table_is_done = tryProcessTable(task_table);
                ++num_table_tries;
            }

            if (!table_is_done)
            {
                throw Exception("Too many tries to process table " + task_table.table_id + ". Abort remaining execution",
                                ErrorCodes::UNFINISHED);
            }
        }
    }

    /// Disables DROP PARTITION commands that used to clear data after errors
    void setSafeMode(bool is_safe_mode_ = true)
    {
        is_safe_mode = is_safe_mode_;
    }

    void setCopyFaultProbability(double copy_fault_probability_)
    {
        copy_fault_probability = copy_fault_probability_;
    }

    /** Checks that the whole partition of a table was copied. We should do it carefully due to dirty lock.
     * State of some task could be changed during the processing.
     * We have to ensure that all shards have the finished state and there are no dirty flag.
     * Moreover, we have to check status twice and check zxid, because state could be changed during the checking.
     */
    bool checkPartitionIsDone(const TaskTable & task_table, const String & partition_name, const TasksShard & shards_with_partition)
    {
        LOG_DEBUG(log, "Check that all shards processed partition " << partition_name << " successfully");

        auto zookeeper = getZooKeeper();

        Strings status_paths;
        for (auto & shard : shards_with_partition)
        {
            ShardPartition & task_shard_partition = shard->partition_tasks.find(partition_name)->second;
            status_paths.emplace_back(task_shard_partition.getShardStatusPath());
        }

        zkutil::Stat stat;
        std::vector<int64_t> zxid1, zxid2;

        try
        {
            // Check that state is Finished and remember zxid
            for (const String & path : status_paths)
            {
                TaskStateWithOwner status = TaskStateWithOwner::fromString(zookeeper->get(path, &stat));
                if (status.state != TaskState::Finished)
                {
                    LOG_INFO(log, "The task " << path << " is being rewritten by " << status.owner
                                               << ". Partition will be rechecked");
                    return false;
                }
                zxid1.push_back(stat.pzxid);
            }

            // Check that partition is not dirty
            if (zookeeper->exists(task_table.getPartitionIsDirtyPath(partition_name)))
            {
                LOG_INFO(log, "Partition " << partition_name << " become dirty");
                return false;
            }

            // Remember zxid of states again
            for (const auto & path : status_paths)
            {
                zookeeper->exists(path, &stat);
                zxid2.push_back(stat.pzxid);
            }
        }
        catch (const zkutil::KeeperException & e)
        {
            LOG_INFO(log, "A ZooKeeper error occurred while checking partition " << partition_name
                          << ". Will recheck the partition. Error: " << e.what());
            return false;
        }

        // If all task is finished and zxid is not changed then partition could not become dirty again
        for (size_t shard_num = 0; shard_num < status_paths.size(); ++shard_num)
        {
            if (zxid1[shard_num] != zxid2[shard_num])
            {
                LOG_INFO(log, "The task " << status_paths[shard_num] << " is being modified now. Partition will be rechecked");
                return false;
            }
        }

        LOG_INFO(log, "Partition " << partition_name << " is copied successfully");
        return true;
    }

protected:

    String getWorkersPath() const
    {
        return task_cluster->task_zookeeper_path + "/task_active_workers";
    }

    String getCurrentWorkerNodePath() const
    {
        return getWorkersPath() + "/" + host_id;
    }

    zkutil::EphemeralNodeHolder::Ptr createTaskWorkerNodeAndWaitIfNeed(const zkutil::ZooKeeperPtr & zookeeper,
                                                                       const String & description)
    {
        while (true)
        {
            zkutil::Stat stat;
            zookeeper->get(getWorkersPath(), &stat);

            if (static_cast<size_t>(stat.numChildren) >= task_cluster->max_workers)
            {
                LOG_DEBUG(log, "Too many workers (" << stat.numChildren << ", maximum " << task_cluster->max_workers << ")"
                    << ". Postpone processing " << description);

                std::this_thread::sleep_for(default_sleep_time);

                updateConfigIfNeeded();
            }
            else
            {
                return std::make_shared<zkutil::EphemeralNodeHolder>(getCurrentWorkerNodePath(), *zookeeper, true, false, description);
            }
        }
    }

    /// Removes MATERIALIZED and ALIAS columns from create table query
    static ASTPtr removeAliasColumnsFromCreateQuery(const ASTPtr & query_ast)
    {
        const ASTs & column_asts = typeid_cast<ASTCreateQuery &>(*query_ast).columns->children;
        auto new_columns = std::make_shared<ASTExpressionList>();

        for (const ASTPtr & column_ast : column_asts)
        {
            const ASTColumnDeclaration & column = typeid_cast<const ASTColumnDeclaration &>(*column_ast);

            if (!column.default_specifier.empty())
            {
                ColumnDefaultType type = columnDefaultTypeFromString(column.default_specifier);
                if (type == ColumnDefaultType::Materialized || type == ColumnDefaultType::Alias)
                    continue;
            }

            new_columns->children.emplace_back(column_ast->clone());
        }

        ASTPtr new_query_ast = query_ast->clone();
        ASTCreateQuery & new_query = typeid_cast<ASTCreateQuery &>(*new_query_ast);
        new_query.columns = new_columns.get();
        new_query.children.at(0) = std::move(new_columns);

        return new_query_ast;
    }

    /// Replaces ENGINE and table name in a create query
    std::shared_ptr<ASTCreateQuery> rewriteCreateQueryStorage(const ASTPtr & create_query_ast, const DatabaseAndTableName & new_table, const ASTPtr & new_storage_ast)
    {
        ASTCreateQuery & create = typeid_cast<ASTCreateQuery &>(*create_query_ast);
        auto res = std::make_shared<ASTCreateQuery>(create);

        if (create.storage == nullptr || new_storage_ast == nullptr)
            throw Exception("Storage is not specified", ErrorCodes::LOGICAL_ERROR);

        res->database = new_table.first;
        res->table = new_table.second;

        res->children.clear();
        res->set(res->columns, create.columns->clone());
        res->set(res->storage, new_storage_ast->clone());

        return res;
    }

    bool tryDropPartition(ShardPartition & task_partition, const zkutil::ZooKeeperPtr & zookeeper)
    {
        if (is_safe_mode)
            throw Exception("DROP PARTITION is prohibited in safe mode", ErrorCodes::NOT_IMPLEMENTED);

        TaskTable & task_table = task_partition.task_shard.task_table;

        String current_shards_path = task_partition.getPartitionShardsPath();
        String current_partition_active_workers_dir = task_partition.getPartitionActiveWorkersPath();
        String is_dirty_flag_path = task_partition.getCommonPartitionIsDirtyPath();
        String dirt_cleaner_path = is_dirty_flag_path + "/cleaner";

        zkutil::EphemeralNodeHolder::Ptr cleaner_holder;
        try
        {
            cleaner_holder = zkutil::EphemeralNodeHolder::create(dirt_cleaner_path, *zookeeper, host_id);
        }
        catch (zkutil::KeeperException & e)
        {
            if (e.code == ZNODEEXISTS)
            {
                LOG_DEBUG(log, "Partition " << task_partition.name << " is cleaning now by somebody, sleep");
                std::this_thread::sleep_for(default_sleep_time);
                return false;
            }

            throw;
        }

        zkutil::Stat stat;
        if (zookeeper->exists(current_partition_active_workers_dir, &stat))
        {
            if (stat.numChildren != 0)
            {
                LOG_DEBUG(log, "Partition " << task_partition.name << " contains " << stat.numChildren << " active workers, sleep");
                std::this_thread::sleep_for(default_sleep_time);
                return false;
            }
        }

        /// Remove all status nodes
        zookeeper->tryRemoveRecursive(current_shards_path);

        String query = "ALTER TABLE " + getDatabaseDotTable(task_table.table_push);
        query += " DROP PARTITION " + task_partition.name + "";

        /// TODO: use this statement after servers will be updated up to 1.1.54310
        // query += " DROP PARTITION ID '" + task_partition.name + "'";

        ClusterPtr & cluster_push = task_table.cluster_push;
        Settings settings_push = task_cluster->settings_push;

        /// It is important, DROP PARTITION must be done synchronously
        settings_push.replication_alter_partitions_sync = 2;

        LOG_DEBUG(log, "Execute distributed DROP PARTITION: " << query);
        /// Limit number of max executing replicas to 1
        size_t num_shards = executeQueryOnCluster(cluster_push, query, nullptr, &settings_push, PoolMode::GET_ONE, 1);

        if (num_shards < cluster_push->getShardCount())
        {
            LOG_INFO(log, "DROP PARTITION wasn't successfully executed on " << cluster_push->getShardCount() - num_shards << " shards");
            return false;
        }

        /// Remove the locking node
        cleaner_holder.reset();
        zookeeper->remove(is_dirty_flag_path);

        LOG_INFO(log, "Partition " << task_partition.name << " was dropped on cluster " << task_table.cluster_push_name);
        return true;
    }


    bool processPartitionTask(ShardPartition & task_partition)
    {
        bool res;

        try
        {
            res = processPartitionTaskImpl(task_partition);
        }
        catch (...)
        {
            tryLogCurrentException(log, "An error occurred while processing partition " + task_partition.name);
            res = false;
        }

        /// At the end of each task check if the config is updated
        try
        {
            updateConfigIfNeeded();
        }
        catch (...)
        {
            tryLogCurrentException(log, "An error occurred while updating the config");
        }

        return res;
    }

    bool processPartitionTaskImpl(ShardPartition & task_partition)
    {
        TaskShard & task_shard = task_partition.task_shard;
        TaskTable & task_table = task_shard.task_table;
        ClusterPartition & cluster_partition = task_table.getClusterPartition(task_partition.name);

        auto zookeeper = getZooKeeper();
        auto acl = zookeeper->getDefaultACL();

        String is_dirty_flag_path = task_partition.getCommonPartitionIsDirtyPath();
        String current_task_is_active_path = task_partition.getActiveWorkerPath();
        String current_task_status_path = task_partition.getShardStatusPath();

        /// Auxiliary functions:

        /// Creates is_dirty node to initialize DROP PARTITION
        auto create_is_dirty_node = [&] ()
        {
            auto code = zookeeper->tryCreate(is_dirty_flag_path, current_task_status_path, zkutil::CreateMode::Persistent);
            if (code != ZOK && code != ZNODEEXISTS)
                throw zkutil::KeeperException(code, is_dirty_flag_path);
        };

        /// Returns SELECT query filtering current partition and applying user filter
        auto get_select_query = [&] (const DatabaseAndTableName & from_table, const String & fields, String limit = "")
        {
            String query;
            query += "SELECT " + fields + " FROM " + getDatabaseDotTable(from_table);
            query += " WHERE (" + queryToString(task_table.engine_push_partition_key_ast) + " = " + task_partition.name + ")";
            if (!task_table.where_condition_str.empty())
                query += " AND (" + task_table.where_condition_str + ")";
            if (!limit.empty())
                query += " LIMIT " + limit;

            ParserQuery p_query(query.data() + query.size());
            return parseQuery(p_query, query);
        };


        /// Load balancing
        auto worker_node_holder = createTaskWorkerNodeAndWaitIfNeed(zookeeper, current_task_status_path);

        LOG_DEBUG(log, "Processing " << current_task_status_path);

        /// Do not start if partition is dirty, try to clean it
        if (zookeeper->exists(is_dirty_flag_path))
        {
            LOG_DEBUG(log, "Partition " << task_partition.name << " is dirty, try to drop it");

            try
            {
                tryDropPartition(task_partition, zookeeper);
            }
            catch (...)
            {
                tryLogCurrentException(log, "An error occurred while clean partition");
            }

            return false;
        }

        /// Create ephemeral node to mark that we are active and process the partition
        zookeeper->createAncestors(current_task_is_active_path);
        zkutil::EphemeralNodeHolderPtr partition_task_node_holder;
        try
        {
            partition_task_node_holder = zkutil::EphemeralNodeHolder::create(current_task_is_active_path, *zookeeper, host_id);
        }
        catch (const zkutil::KeeperException & e)
        {
            if (e.code == ZNODEEXISTS)
            {
                LOG_DEBUG(log, "Someone is already processing " << current_task_is_active_path);
                return false;
            }

            throw;
        }

        /// Exit if task has been already processed, create blocking node if it is abandoned
        {
            String status_data;
            if (zookeeper->tryGet(current_task_status_path, status_data))
            {
                TaskStateWithOwner status = TaskStateWithOwner::fromString(status_data);
                if (status.state == TaskState::Finished)
                {
                    LOG_DEBUG(log, "Task " << current_task_status_path << " has been successfully executed by " << status.owner);
                    return true;
                }

                // Task is abandoned, initialize DROP PARTITION
                LOG_DEBUG(log, "Task " << current_task_status_path << " has not been successfully finished by " << status.owner);

                create_is_dirty_node();
                return false;
            }
        }

        zookeeper->createAncestors(current_task_status_path);

        /// We need to update table definitions for each partition, it could be changed after ALTER
        createShardInternalTables(task_shard);

        /// Check that destination partition is empty if we are first worker
        /// NOTE: this check is incorrect if pull and push tables have different partition key!
        {
            ASTPtr query_select_ast = get_select_query(task_shard.table_split_shard, "count()");
            UInt64 count;
            {
                Context local_context = context;
                // Use pull (i.e. readonly) settings, but fetch data from destination servers
                local_context.getSettingsRef() = task_cluster->settings_pull;
                local_context.getSettingsRef().skip_unavailable_shards = true;

                InterpreterSelectQuery interperter(query_select_ast, local_context);

                Block block = getBlockWithAllStreamData(interperter.execute().in);
                count = (block) ? block.safeGetByPosition(0).column->getUInt(0) : 0;
            }

            if (count != 0)
            {
                zkutil::Stat stat_shards;
                zookeeper->get(task_partition.getPartitionShardsPath(), &stat_shards);

                if (stat_shards.numChildren == 0)
                {
                    LOG_WARNING(log, "There are no any workers for partition " << task_partition.name
                                     << ", but destination table contains " << count << " rows"
                                     << ". Partition will be dropped and refilled.");

                    create_is_dirty_node();
                    return false;
                }
            }
        }

        /// Try start processing, create node about it
        {
            String start_state = TaskStateWithOwner::getData(TaskState::Started, host_id);
            auto op_create = std::make_unique<zkutil::Op::Create>(current_task_status_path, start_state, acl, zkutil::CreateMode::Persistent);

            zkutil::MultiTransactionInfo info;
            checkNoNodeAndCommit(zookeeper, is_dirty_flag_path, std::move(op_create), info);

            if (info.code != ZOK)
            {
                if (info.getFailedOp().getPath() == is_dirty_flag_path)
                {
                    LOG_INFO(log, "Partition " << task_partition.name << " is dirty and will be dropped and refilled");
                    return false;
                }

                throw zkutil::KeeperException(info.code, current_task_status_path);
            }
        }

        /// Try create table (if not exists) on each shard
        {
            auto create_query_push_ast = rewriteCreateQueryStorage(task_shard.current_pull_table_create_query, task_table.table_push, task_table.engine_push_ast);
            typeid_cast<ASTCreateQuery &>(*create_query_push_ast).if_not_exists = true;
            String query = queryToString(create_query_push_ast);

            LOG_DEBUG(log, "Create destination tables. Query: " << query);
            size_t shards = executeQueryOnCluster(task_table.cluster_push, query, create_query_push_ast, &task_cluster->settings_push,
                                    PoolMode::GET_MANY);
            LOG_DEBUG(log, "Destination tables " << getDatabaseDotTable(task_table.table_push) << " have been created on " << shards
                                                 << " shards of " << task_table.cluster_push->getShardCount());
        }

        /// Do the copying
        {
            bool inject_fault = false;
            if (copy_fault_probability > 0)
            {
                std::uniform_real_distribution<> get_urand(0, 1);
                double value = get_urand(task_table.task_cluster.random_engine);
                inject_fault = value < copy_fault_probability;
            }

            // Select all fields
            ASTPtr query_select_ast = get_select_query(task_shard.table_read_shard, "*", inject_fault ? "1" : "");

            LOG_DEBUG(log, "Executing SELECT query: " << queryToString(query_select_ast));

            ASTPtr query_insert_ast;
            {
                String query;
                query += "INSERT INTO " + getDatabaseDotTable(task_shard.table_split_shard) + " VALUES ";

                ParserQuery p_query(query.data() + query.size());
                query_insert_ast = parseQuery(p_query, query);

                LOG_DEBUG(log, "Executing INSERT query: " << query);
            }

            try
            {
                /// Custom INSERT SELECT implementation
                Context context_select = context;
                context_select.getSettingsRef() = task_cluster->settings_pull;

                Context context_insert = context;
                context_insert.getSettingsRef() = task_cluster->settings_push;

                InterpreterSelectQuery interpreter_select(query_select_ast, context_select);
                BlockIO io_select = interpreter_select.execute();

                InterpreterInsertQuery interpreter_insert(query_insert_ast, context_insert);
                BlockIO io_insert = interpreter_insert.execute();

                using ExistsFuture = zkutil::ZooKeeper::ExistsFuture;
                auto future_is_dirty_checker = std::make_unique<ExistsFuture>(zookeeper->asyncExists(is_dirty_flag_path));

                Stopwatch watch(CLOCK_MONOTONIC_COARSE);
                constexpr size_t check_period_milliseconds = 500;

                /// Will asynchronously check that ZooKeeper connection and is_dirty flag appearing while copy data
                auto cancel_check = [&] ()
                {
                    if (zookeeper->expired())
                        throw Exception("ZooKeeper session is expired, cancel INSERT SELECT", ErrorCodes::UNFINISHED);

                    if (future_is_dirty_checker != nullptr)
                    {
                        zkutil::ZooKeeper::StatAndExists status;
                        try
                        {
                            status = future_is_dirty_checker->get();
                            future_is_dirty_checker.reset();
                        }
                        catch (zkutil::KeeperException & e)
                        {
                            future_is_dirty_checker.reset();

                            if (e.isTemporaryError())
                                LOG_INFO(log, "ZooKeeper is lagging: " << e.displayText());
                            else
                                throw;
                        }

                        if (status.exists)
                            throw Exception("Partition is dirty, cancel INSERT SELECT", ErrorCodes::UNFINISHED);
                    }

                    if (watch.elapsedMilliseconds() >= check_period_milliseconds)
                    {
                        watch.restart();
                        future_is_dirty_checker = std::make_unique<ExistsFuture>(zookeeper->asyncExists(is_dirty_flag_path));
                    }

                    return false;
                };

                /// Update statistics
                /// It is quite rough: bytes_copied don't take into account DROP PARTITION.
                if (auto in = dynamic_cast<IProfilingBlockInputStream *>(io_select.in.get()))
                {
                    auto update_table_stats = [&] (const Progress & progress)
                    {
                        cluster_partition.bytes_copied += progress.bytes;
                        cluster_partition.rows_copied += progress.rows;
                    };

                    in->setProgressCallback(update_table_stats);
                }

                /// Main work is here
                copyData(*io_select.in, *io_insert.out, cancel_check);

                // Just in case
                if (future_is_dirty_checker != nullptr)
                    future_is_dirty_checker.get();

                if (inject_fault)
                    throw Exception("Copy fault injection is activated", ErrorCodes::UNFINISHED);
            }
            catch (...)
            {
                tryLogCurrentException(log, "An error occurred during copying, partition will be marked as dirty");
                return false;
            }
        }

        /// Finalize the processing, change state of current partition task (and also check is_dirty flag)
        {
            String state_finished = TaskStateWithOwner::getData(TaskState::Finished, host_id);
            auto op_set = std::make_unique<zkutil::Op::SetData>(current_task_status_path, state_finished, 0);
            zkutil::MultiTransactionInfo info;
            checkNoNodeAndCommit(zookeeper, is_dirty_flag_path, std::move(op_set), info);

            if (info.code != ZOK)
            {
                if (info.getFailedOp().getPath() == is_dirty_flag_path)
                    LOG_INFO(log, "Partition " << task_partition.name << " became dirty and will be dropped and refilled");
                else
                    LOG_INFO(log, "Someone made the node abandoned. Will refill partition. " << zkutil::ZooKeeper::error2string(info.code));

                return false;
            }
        }

        LOG_INFO(log, "Partition " << task_partition.name << " copied");
        return true;
    }

    void dropAndCreateLocalTable(const ASTPtr & create_ast)
    {
        auto & create = typeid_cast<ASTCreateQuery &>(*create_ast);
        dropLocalTableIfExists({create.database, create.table});

        InterpreterCreateQuery interpreter(create_ast, context);
        interpreter.execute();
    }

    void dropLocalTableIfExists(const DatabaseAndTableName & table_name) const
    {
        auto drop_ast = std::make_shared<ASTDropQuery>();
        drop_ast->if_exists = true;
        drop_ast->database = table_name.first;
        drop_ast->table = table_name.second;

        InterpreterDropQuery interpreter(drop_ast, context);
        interpreter.execute();
    }

    bool existsRemoteTable(const DatabaseAndTableName & table, Connection & connection)
    {
        String query = "EXISTS " + getDatabaseDotTable(table);
        Block block = getBlockWithAllStreamData(std::make_shared<RemoteBlockInputStream>(
            connection, query, InterpreterExistsQuery::getSampleBlock(), context));
        return block.safeGetByPosition(0).column->getUInt(0) != 0;
    }

    String getRemoteCreateTable(const DatabaseAndTableName & table, Connection & connection, const Settings * settings = nullptr)
    {
        String query = "SHOW CREATE TABLE " + getDatabaseDotTable(table);
        Block block = getBlockWithAllStreamData(std::make_shared<RemoteBlockInputStream>(
            connection, query, InterpreterShowCreateQuery::getSampleBlock(), context, settings));

        return typeid_cast<const ColumnString &>(*block.safeGetByPosition(0).column).getDataAt(0).toString();
    }

    ASTPtr getCreateTableForPullShard(TaskShard & task_shard)
    {
        /// Fetch and parse (possibly) new definition
        auto connection_entry = task_shard.info.pool->get(&task_cluster->settings_pull);
        String create_query_pull_str = getRemoteCreateTable(task_shard.task_table.table_pull, *connection_entry,
                                                            &task_cluster->settings_pull);

        ParserCreateQuery parser_create_query;
        return parseQuery(parser_create_query, create_query_pull_str);
    }

    void createShardInternalTables(TaskShard & task_shard)
    {
        TaskTable & task_table = task_shard.task_table;

        /// We need to update table definitions for each part, it could be changed after ALTER
        task_shard.current_pull_table_create_query = getCreateTableForPullShard(task_shard);

        /// Create local Distributed tables:
        ///  a table fetching data from current shard and a table inserting data to the whole destination cluster
        String read_shard_prefix = ".read_shard_" + toString(task_shard.indexInCluster()) + ".";
        String split_shard_prefix = ".split.";
        task_shard.table_read_shard = DatabaseAndTableName(working_database_name, read_shard_prefix + task_table.table_id);
        task_shard.table_split_shard = DatabaseAndTableName(working_database_name, split_shard_prefix + task_table.table_id);

        /// Create special cluster with single shard
        String shard_read_cluster_name = read_shard_prefix + task_table.cluster_pull_name;
        ClusterPtr cluster_pull_current_shard = task_table.cluster_pull->getClusterWithSingleShard(task_shard.indexInCluster());
        context.setCluster(shard_read_cluster_name, cluster_pull_current_shard);

        auto storage_shard_ast = createASTStorageDistributed(shard_read_cluster_name, task_table.table_pull.first, task_table.table_pull.second);
        const auto & storage_split_ast = task_table.engine_split_ast;

        auto create_query_ast = removeAliasColumnsFromCreateQuery(task_shard.current_pull_table_create_query);
        auto create_table_pull_ast = rewriteCreateQueryStorage(create_query_ast, task_shard.table_read_shard, storage_shard_ast);
        auto create_table_split_ast = rewriteCreateQueryStorage(create_query_ast, task_shard.table_split_shard, storage_split_ast);

        //LOG_DEBUG(log, "Create shard reading table. Query: " << queryToString(create_table_pull_ast));
        dropAndCreateLocalTable(create_table_pull_ast);

        //LOG_DEBUG(log, "Create split table. Query: " << queryToString(create_table_split_ast));
        dropAndCreateLocalTable(create_table_split_ast);
    }


    std::set<String> getShardPartitions(TaskShard & task_shard)
    {
        createShardInternalTables(task_shard);

        TaskTable & task_table = task_shard.task_table;

        String query;
        {
            WriteBufferFromOwnString wb;
            wb << "SELECT DISTINCT " << queryToString(task_table.engine_push_partition_key_ast) << " AS partition FROM"
               << " " << getDatabaseDotTable(task_shard.table_read_shard) << " ORDER BY partition DESC";
            query = wb.str();
        }

        LOG_DEBUG(log, "Computing destination partition set, executing query: " << query);

        ParserQuery parser_query(query.data() + query.size());
        ASTPtr query_ast = parseQuery(parser_query, query);

        Context local_context = context;
        InterpreterSelectQuery interp(query_ast, local_context);
        Block block = getBlockWithAllStreamData(interp.execute().in);

        std::set<String> res;
        if (block)
        {
            ColumnWithTypeAndName & column = block.getByPosition(0);
            task_shard.partition_key_column = column;

            for (size_t i = 0; i < column.column->size(); ++i)
            {
                WriteBufferFromOwnString wb;
                column.type->serializeTextQuoted(*column.column, i, wb);
                res.emplace(wb.str());
            }
        }

        LOG_DEBUG(log, "There are " << res.size() << " destination partitions in shard " << task_shard.getDescription());

        return res;
    }

    /** Executes simple query (without output streams, for example DDL queries) on each shard of the cluster
      * Returns number of shards for which at least one replica executed query successfully
      */
    size_t executeQueryOnCluster(
        const ClusterPtr & cluster,
        const String & query,
        const ASTPtr & query_ast_ = nullptr,
        const Settings * settings = nullptr,
        PoolMode pool_mode = PoolMode::GET_ALL,
        size_t max_successful_executions_per_shard = 0) const
    {
        auto num_shards = cluster->getShardsInfo().size();
        std::vector<size_t> per_shard_num_successful_replicas(num_shards, 0);

        ASTPtr query_ast;
        if (query_ast_ == nullptr)
        {
            ParserQuery p_query(query.data() + query.size());
            query_ast = parseQuery(p_query, query);
        }
        else
            query_ast = query_ast_;


        /// We need to execute query on one replica at least
        auto do_for_shard = [&] (size_t shard_index)
        {
            const Cluster::ShardInfo & shard = cluster->getShardsInfo().at(shard_index);
            size_t & num_successful_executions = per_shard_num_successful_replicas.at(shard_index);
            num_successful_executions = 0;

            auto increment_and_check_exit = [&] ()
            {
                ++num_successful_executions;
                return max_successful_executions_per_shard && num_successful_executions >= max_successful_executions_per_shard;
            };

            size_t num_replicas = cluster->getShardsAddresses().at(shard_index).size();
            size_t num_local_replicas = shard.getLocalNodeCount();
            size_t num_remote_replicas = num_replicas - num_local_replicas;

            /// In that case we don't have local replicas, but do it just in case
            for (size_t i = 0; i < num_local_replicas; ++i)
            {
                auto interpreter = InterpreterFactory::get(query_ast, context);
                interpreter->execute();

                if (increment_and_check_exit())
                    return;
            }

            /// Will try to make as many as possible queries
            if (shard.hasRemoteConnections())
            {
                Settings current_settings = settings ? *settings : task_cluster->settings_common;
                current_settings.max_parallel_replicas = num_remote_replicas ? num_remote_replicas : 1;

                auto connections = shard.pool->getMany(&current_settings, pool_mode);

                for (auto & connection : connections)
                {
                    if (connection.isNull())
                        continue;

                    try
                    {
                        /// CREATE TABLE and DROP PARTITION queries return empty block
                        RemoteBlockInputStream stream{*connection, query, Block{}, context, &current_settings};
                        NullBlockOutputStream output{Block{}};
                        copyData(stream, output);

                        if (increment_and_check_exit())
                            return;
                    }
                    catch (const Exception & e)
                    {
                        LOG_INFO(log, getCurrentExceptionMessage(false, true));
                    }
                }
            }
        };

        {
            ThreadPool thread_pool(std::min(num_shards, getNumberOfPhysicalCPUCores()));

            for (size_t shard_index = 0; shard_index < num_shards; ++shard_index)
                thread_pool.schedule([=] { do_for_shard(shard_index); });

            thread_pool.wait();
        }

        size_t successful_shards = 0;
        for (size_t num_replicas : per_shard_num_successful_replicas)
            successful_shards += (num_replicas > 0);

        return successful_shards;
    }

    void initZooKeeper()
    {
        current_zookeeper = std::make_shared<zkutil::ZooKeeper>(*zookeeper_config, "zookeeper");
    }

    const zkutil::ZooKeeperPtr & getZooKeeper()
    {
        if (!current_zookeeper)
            throw Exception("Cannot get ZooKeeper", ErrorCodes::NO_ZOOKEEPER);

        return current_zookeeper;
    }

private:
    ConfigurationPtr zookeeper_config;
    String task_zookeeper_path;
    String task_description_path;
    String host_id;
    String working_database_name;

    UInt64 task_descprtion_current_version = 1;
    std::atomic<UInt64> task_descprtion_version{1};
    zkutil::WatchCallback task_description_watch_callback;

    ConfigurationPtr task_cluster_initial_config;
    ConfigurationPtr task_cluster_current_config;
    zkutil::Stat task_descprtion_current_stat;

    std::unique_ptr<TaskCluster> task_cluster;

    zkutil::ZooKeeperPtr current_zookeeper;

    bool is_safe_mode = false;
    double copy_fault_probability = 0.0;

    Context & context;
    Poco::Logger * log;

    std::chrono::milliseconds default_sleep_time{1000};
};


/// ClusterCopierApp


void ClusterCopierApp::initialize(Poco::Util::Application & self)
{
    Poco::Util::ServerApplication::initialize(self);

    is_help = config().has("help");
    if (is_help)
        return;

    config_xml_path = config().getString("config-file");
    task_path = config().getString("task-path");
    log_level = config().getString("log-level", "debug");
    is_safe_mode = config().has("safe-mode");
    if (config().has("copy-fault-probability"))
        copy_fault_probability = std::max(std::min(config().getDouble("copy-fault-probability"), 1.0), 0.0);
    base_dir = (config().has("base-dir")) ? config().getString("base-dir") : Poco::Path::current();

    // process_id is '<hostname>#<start_timestamp>_<pid>'
    time_t timestamp = Poco::Timestamp().epochTime();
    auto pid = Poco::Process::id();

    process_id = std::to_string(DateLUT::instance().toNumYYYYMMDDhhmmss(timestamp)) + "_" + std::to_string(pid);
    host_id = escapeForFileName(getFQDNOrHostName()) + '#' + process_id;
    process_path = Poco::Path(base_dir + "/clickhouse-copier_" + process_id).absolute().toString();
    Poco::File(process_path).createDirectories();

    setupLogging();

    std::string stderr_path = process_path + "/stderr";
    if (!freopen(stderr_path.c_str(), "a+", stderr))
        throw Poco::OpenFileException("Cannot attach stderr to " + stderr_path);
}


void ClusterCopierApp::handleHelp(const std::string &, const std::string &)
{
    Poco::Util::HelpFormatter helpFormatter(options());
    helpFormatter.setCommand(commandName());
    helpFormatter.setHeader("Copies tables from one cluster to another");
    helpFormatter.setUsage("--config-file <config-file> --task-path <task-path>");
    helpFormatter.format(std::cerr);

    stopOptionsProcessing();
}


void ClusterCopierApp::defineOptions(Poco::Util::OptionSet & options)
{
    Poco::Util::ServerApplication::defineOptions(options);

    options.addOption(Poco::Util::Option("config-file", "c", "path to config file with ZooKeeper config", true)
                          .argument("config-file").binding("config-file"));
    options.addOption(Poco::Util::Option("task-path", "", "path to task in ZooKeeper")
                          .argument("task-path").binding("task-path"));
    options.addOption(Poco::Util::Option("safe-mode", "", "disables ALTER DROP PARTITION in case of errors")
                          .binding("safe-mode"));
    options.addOption(Poco::Util::Option("copy-fault-probability", "", "the copying fails with specified probability (used to test partition state recovering)")
                          .argument("copy-fault-probability").binding("copy-fault-probability"));
    options.addOption(Poco::Util::Option("log-level", "", "sets log level")
                          .argument("log-level").binding("log-level"));
    options.addOption(Poco::Util::Option("base-dir", "", "base directory for copiers, consequitive copier launches will populate /base-dir/launch_id/* directories")
                          .argument("base-dir").binding("base-dir"));

    using Me = std::decay_t<decltype(*this)>;
    options.addOption(Poco::Util::Option("help", "", "produce this help message").binding("help")
                          .callback(Poco::Util::OptionCallback<Me>(this, &Me::handleHelp)));
}


void ClusterCopierApp::setupLogging()
{
    Poco::AutoPtr<Poco::SplitterChannel> split_channel(new Poco::SplitterChannel);

    Poco::AutoPtr<Poco::FileChannel> log_file_channel(new Poco::FileChannel);
    log_file_channel->setProperty("path", process_path + "/log.log");
    split_channel->addChannel(log_file_channel);
    log_file_channel->open();

    if (!config().getBool("application.runAsDaemon", true))
    {
        Poco::AutoPtr<Poco::ConsoleChannel> console_channel(new Poco::ConsoleChannel);
        split_channel->addChannel(console_channel);
        console_channel->open();
    }

    Poco::AutoPtr<Poco::PatternFormatter> formatter(new Poco::PatternFormatter);
    formatter->setProperty("pattern", "%L%Y-%m-%d %H:%M:%S.%i <%p> %s: %t");
    Poco::AutoPtr<Poco::FormattingChannel> formatting_channel(new Poco::FormattingChannel(formatter));
    formatting_channel->setChannel(split_channel);
    split_channel->open();

    Poco::Logger::root().setChannel(formatting_channel);
    Poco::Logger::root().setLevel(log_level);
}


void ClusterCopierApp::mainImpl()
{
    ConfigurationPtr zookeeper_configuration(new Poco::Util::XMLConfiguration(config_xml_path));
    auto log = &logger();

    StatusFile status_file(process_path + "/status");

    LOG_INFO(log, "Starting clickhouse-copier ("
        << "id " << process_id << ", "
        << "host_id " << host_id << ", "
        << "path " << process_path << ", "
        << "revision " << ClickHouseRevision::get() << ")");

    auto context = std::make_unique<Context>(Context::createGlobal());
    SCOPE_EXIT(context->shutdown());

    context->setGlobalContext(*context);
    context->setApplicationType(Context::ApplicationType::LOCAL);
    context->setPath(process_path);

    registerFunctions();
    registerAggregateFunctions();
    registerTableFunctions();
    registerStorages();

    static const std::string default_database = "_local";
    context->addDatabase(default_database, std::make_shared<DatabaseMemory>(default_database));
    context->setCurrentDatabase(default_database);

    std::unique_ptr<ClusterCopier> copier(new ClusterCopier(
        zookeeper_configuration, task_path, host_id, default_database, *context));

    copier->setSafeMode(is_safe_mode);
    copier->setCopyFaultProbability(copy_fault_probability);
    copier->init();
    copier->process();
}


int ClusterCopierApp::main(const std::vector<std::string> &)
{
    if (is_help)
        return 0;

    try
    {
        mainImpl();
    }
    catch (...)
    {
        tryLogCurrentException(&Poco::Logger::root(), __PRETTY_FUNCTION__);
        auto code = getCurrentExceptionCode();

        return (code) ? code : -1;
    }

    return 0;
}


}


int mainEntryClickHouseClusterCopier(int argc, char ** argv)
{
    try
    {
        DB::ClusterCopierApp app;
        return app.run(argc, argv);
    }
    catch (...)
    {
        std::cerr << DB::getCurrentExceptionMessage(true) << "\n";
        auto code = DB::getCurrentExceptionCode();

        return (code) ? code : -1;
    }
}
