#include "ClusterCopier.h"
#include "StatusFile.h"
#include <boost/algorithm/string.hpp>
#include <Poco/Util/XMLConfiguration.h>
#include <Poco/Logger.h>
#include <Poco/ConsoleChannel.h>
#include <Poco/FormattingChannel.h>
#include <Poco/PatternFormatter.h>
#include <Poco/Util/Application.h>
#include <Poco/UUIDGenerator.h>
#include <Poco/File.h>
#include <Poco/Process.h>
#include <chrono>

#include <Common/Exception.h>
#include <Common/ZooKeeper/ZooKeeper.h>
#include <Common/getFQDNOrHostName.h>
#include <Client/Connection.h>
#include <Interpreters/Context.h>
#include <Interpreters/Settings.h>
#include <Interpreters/Cluster.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Interpreters/InterpreterInsertQuery.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/InterpreterDropQuery.h>
#include <Interpreters/InterpreterCreateQuery.h>

#include <common/logger_useful.h>
#include <common/ApplicationServerExt.h>
#include <common/ThreadPool.h>
#include <Common/typeid_cast.h>
#include <Common/ClickHouseRevision.h>
#include <Common/escapeForFileName.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Storages/StorageDistributed.h>
#include <Parsers/ASTCheckQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/queryToString.h>
#include <Parsers/ASTDropQuery.h>
#include <Parsers/ASTLiteral.h>
#include <Databases/DatabaseMemory.h>
#include <DataStreams/RemoteBlockInputStream.h>
#include <DataStreams/SquashingBlockInputStream.h>
#include <Common/isLocalAddress.h>
#include <DataStreams/copyData.h>
#include <DataStreams/NullBlockOutputStream.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferNull.h>
#include <Functions/registerFunctions.h>
#include <TableFunctions/registerTableFunctions.h>
#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Poco/FileChannel.h>
#include <Poco/SplitterChannel.h>
#include <Poco/Util/HelpFormatter.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NO_ZOOKEEPER;
    extern const int BAD_ARGUMENTS;
    extern const int UNKNOWN_TABLE;
    extern const int UNFINISHED;
}


using ConfigurationPtr = Poco::AutoPtr<Poco::Util::AbstractConfiguration>;


template <typename T>
static ConfigurationPtr getConfigurationFromXMLString(T && xml_string)
{
    std::stringstream ss;
    ss << std::forward<T>(xml_string);

    Poco::XML::InputSource input_source(ss);
    ConfigurationPtr res(new Poco::Util::XMLConfiguration(&input_source));

    return res;
}

namespace
{

using DatabaseAndTableName = std::pair<String, String>;


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
        wb << static_cast<UInt32>(state) << "\n" << DB::escape << owner;
        return wb.str();
    }

    static TaskStateWithOwner fromString(const String & data)
    {
        ReadBufferFromString rb(data);
        TaskStateWithOwner res;
        UInt32 state;

        rb >> state >> "\n" >> DB::escape >> res.owner;

        if (state >= static_cast<int>(TaskState::Unknown))
            throw Exception("Unknown state " + data, ErrorCodes::LOGICAL_ERROR);

        res.state = static_cast<TaskState>(state);
        return res;
    }
};


/// Hierarchical description of the tasks
struct TaskPartition;
struct TaskShard;
struct TaskTable;
struct TaskCluster;

using TasksPartition = std::map<String, TaskPartition>;
using ShardInfo = Cluster::ShardInfo;
using TaskShardPtr = std::shared_ptr<TaskShard>;
using TasksShard = std::vector<TaskShardPtr>;
using TasksTable = std::list<TaskTable>;
using PartitionToShards = std::map<String, TasksShard>;

struct TaskPartition
{
    TaskPartition(TaskShard & parent, const String & name_) : task_shard(parent), name(name_) {}

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

    static bool isMorePriority(const ShardPriority & current, const ShardPriority & other)
    {
        return std::less<void>()(
            std::forward_as_tuple(current.is_remote, current.hostname_difference, current.random),
            std::forward_as_tuple(other.is_remote, other.hostname_difference, other.random)
        );
    }
};


struct TaskShard
{
    TaskShard(TaskTable & parent, const ShardInfo & info_) : task_table(parent), info(info_) {}

    TaskTable & task_table;

    ShardInfo info;
    UInt32 numberInCluster() const { return info.shard_num; }
    UInt32 indexInCluster() const { return info.shard_num - 1; }

    TasksPartition partitions;

    ShardPriority priority;
};

struct TaskTable
{
    TaskTable(TaskCluster & parent, const Poco::Util::AbstractConfiguration & config, const String & prefix,
                  const String & table_key);

    TaskCluster & task_cluster;

    String getPartitionPath(const String & partition_name) const;
    String getPartitionIsDirtyPath(const String & partition_name) const;

    /// Used as task ID
    String name_in_config;

    /// Source cluster and table
    String cluster_pull_name;
    DatabaseAndTableName table_pull;

    /// Destination cluster and table
    String cluster_push_name;
    DatabaseAndTableName table_push;

    /// Storage of destination table
    String engine_push_str;
    ASTPtr engine_push_ast;

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
    NameSet enabled_partitions;

    /// Prioritized list of shards
    TasksShard all_shards;
    TasksShard local_shards;

    PartitionToShards partition_to_shards;

    template <class URNG>
    void initShards(URNG && urng);
};

struct TaskCluster
{
    TaskCluster(const String & task_zookeeper_path_, const Poco::Util::AbstractConfiguration & config, const String & base_key, const String & default_local_database_);

    /// Base node for all tasks. Its structure:
    ///  workers/ - directory with active workers (amount of them is less or equal max_workers)
    ///  description - node with task configuration
    ///  table_table1/ - directories with per-partition copying status
    String task_zookeeper_path;

    /// Limits number of simultaneous workers
    size_t max_workers = 0;

    /// Settings used to fetch data
    Settings settings_pull;
    /// Settings used to insert data
    Settings settings_push;

    /// Subtasks
    TasksTable table_tasks;

    /// Database used to create temporary Distributed tables
    String default_local_database;

    /// Path to remote_servers in task config
    String clusters_prefix;

    std::random_device rd;
    std::mt19937 random_generator;
};


String getDatabaseDotTable(const String & database, const String & table)
{
    return backQuoteIfNeed(database) + "." + backQuoteIfNeed(table);
}

String getDatabaseDotTable(const DatabaseAndTableName & db_and_table)
{
    return getDatabaseDotTable(db_and_table.first, db_and_table.second);
}


/// Detailed status of ZooKeeper multi operation
struct MultiOpStatus
{
    int32_t code = ZOK;
    int failed_op_index = 0;
    zkutil::OpPtr failed_op;
};

/// Atomically checks that is_dirty node is not exists, and made the remaining op
/// Returns relative number of failed operation in the second field (the passed op has 0 index)
static MultiOpStatus checkNoNodeAndCommit(
    const zkutil::ZooKeeperPtr & zookeeper,
    const String & checking_node_path,
    zkutil::OpPtr && op)
{
    zkutil::Ops ops;
    ops.emplace_back(std::make_unique<zkutil::Op::Create>(checking_node_path, "", zookeeper->getDefaultACL(), zkutil::CreateMode::Persistent));
    ops.emplace_back(std::make_unique<zkutil::Op::Remove>(checking_node_path, -1));
    ops.emplace_back(std::move(op));

    MultiOpStatus res;
    zkutil::OpResultsPtr results;

    res.code = zookeeper->tryMulti(ops, &results);
    if (res.code != ZOK)
    {
        auto index = zkutil::getFailedOpIndex(results);
        res.failed_op_index = static_cast<int>(index) - 2;
        res.failed_op = ops.at(index)->clone();
    }

    return res;
}


// Creates AST representing 'ENGINE = Distributed(cluster, db, table, [sharding_key])
std::shared_ptr<ASTStorage> createASTStorageDistributed(
    const String & cluster_name, const String & database, const String & table, const ASTPtr & sharding_key_ast = nullptr)
{
    auto args = std::make_shared<ASTExpressionList>();
    args->children.emplace_back(std::make_shared<ASTLiteral>(StringRange(nullptr, nullptr), cluster_name));
    args->children.emplace_back(std::make_shared<ASTIdentifier>(StringRange(nullptr, nullptr), database));
    args->children.emplace_back(std::make_shared<ASTIdentifier>(StringRange(nullptr, nullptr), table));
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

// Path getters

String TaskTable::getPartitionPath(const String & partition_name) const
{
    return task_cluster.task_zookeeper_path                 // root
           + "/table_" + escapeForFileName(name_in_config)  // table_test.hits
           + "/" + partition_name;                          // 201701
}

String TaskPartition::getPartitionPath() const
{
    return task_shard.task_table.getPartitionPath(name);
}

String TaskPartition::getShardStatusPath() const
{
    // /root/table_test.hits/201701/1
    return getPartitionPath() + "/shards/" + toString(task_shard.numberInCluster());
}

String TaskPartition::getPartitionShardsPath() const
{
    return getPartitionPath() + "/shards";
}

String TaskPartition::getPartitionActiveWorkersPath() const
{
    return getPartitionPath() + "/active_workers";
}

String TaskPartition::getActiveWorkerPath() const
{
    return getPartitionActiveWorkersPath() + "/" + toString(task_shard.numberInCluster());
}

String TaskPartition::getCommonPartitionIsDirtyPath() const
{
    return getPartitionPath() + "/is_dirty";
}

String TaskTable::getPartitionIsDirtyPath(const String & partition_name) const
{
    return getPartitionPath(partition_name) + "/is_dirty";
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

    engine_push_str = config.getString(table_prefix + "engine");
    {
        ParserStorage parser_storage;
        engine_push_ast = parseQuery(parser_storage, engine_push_str);
    }

    sharding_key_str = config.getString(table_prefix + "sharding_key");
    {
        ParserExpressionWithOptionalAlias parser_expression(false);
        sharding_key_ast = parseQuery(parser_expression, sharding_key_str);
        engine_split_ast = createASTStorageDistributed(cluster_push_name, table_push.first, table_push.second, sharding_key_ast);

        table_split = DatabaseAndTableName(task_cluster.default_local_database, ".split." + name_in_config);
    }

    where_condition_str = config.getString(table_prefix + "where_condition", "");
    if (!where_condition_str.empty())
    {
        ParserExpressionWithOptionalAlias parser_expression(false);
        where_condition_ast = parseQuery(parser_expression, where_condition_str);

        // Will use canonical expression form
        where_condition_str = queryToString(where_condition_ast);
    }

    has_enabled_partitions = config.has(table_prefix + "enabled_partitions");
    if (has_enabled_partitions)
    {
        Strings partitions;
        String partitions_str = config.getString(table_prefix + "enabled_partitions");
        boost::trim_if(partitions_str, isWhitespaceASCII);
        boost::split(partitions, partitions_str, isWhitespaceASCII, boost::token_compress_on);
        std::copy(partitions.begin(), partitions.end(), std::inserter(enabled_partitions, enabled_partitions.begin()));
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

template<class URNG>
void TaskTable::initShards(URNG && urng)
{
    const String & fqdn_name = getFQDNOrHostName();
    std::uniform_int_distribution<UInt8> get_rand(0, std::numeric_limits<UInt8>::max());

    // Compute the priority
    for (auto & shard_info : cluster_pull->getShardsInfo())
    {
        TaskShardPtr task_shard = std::make_shared<TaskShard>(*this, shard_info);
        const auto & replicas = cluster_pull->getShardsAddresses().at(task_shard->indexInCluster());
        task_shard->priority = getReplicasPriority(replicas, fqdn_name, get_rand(urng));

        all_shards.emplace_back(task_shard);
    }

    // Sort by priority
    std::sort(all_shards.begin(), all_shards.end(),
        [] (const TaskShardPtr & lhs, const TaskShardPtr & rhs)
        {
            return ShardPriority::isMorePriority(lhs->priority, rhs->priority);
        });

    // Cut local shards
    auto it_first_remote = std::lower_bound(all_shards.begin(), all_shards.end(), 1,
        [] (const TaskShardPtr & lhs, UInt8 is_remote)
        {
            return lhs->priority.is_remote < is_remote;
        });

    local_shards.assign(all_shards.begin(), it_first_remote);
}

TaskCluster::TaskCluster(const String & task_zookeeper_path_, const Poco::Util::AbstractConfiguration & config, const String & base_key,
                         const String & default_local_database_)
{
    String prefix = base_key.empty() ? "" : base_key + ".";

    task_zookeeper_path = task_zookeeper_path_;

    default_local_database = default_local_database_;

    max_workers = config.getUInt64(prefix + "max_workers");

    if (config.has(prefix + "settings"))
    {
        settings_pull.loadSettingsFromConfig(prefix + "settings", config);
        settings_push.loadSettingsFromConfig(prefix + "settings", config);
    }

    if (config.has(prefix + "settings_pull"))
        settings_pull.loadSettingsFromConfig(prefix + "settings_pull", config);

    if (config.has(prefix + "settings_push"))
        settings_push.loadSettingsFromConfig(prefix + "settings_push", config);

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
        String description_path = task_zookeeper_path + "/description";
        String task_config_str = getZooKeeper()->get(description_path);

        task_cluster_config = getConfigurationFromXMLString(task_config_str);
        task_cluster = std::make_unique<TaskCluster>(task_zookeeper_path, *task_cluster_config, "", working_database_name);

        /// Override important settings
        Settings & settings_pull = task_cluster->settings_pull;
        settings_pull.load_balancing = LoadBalancing::NEAREST_HOSTNAME;
        settings_pull.limits.readonly = 1;
        settings_pull.max_threads = 1;
        settings_pull.max_block_size = std::min(8192UL, settings_pull.max_block_size.value);
        settings_pull.preferred_block_size_bytes = 0;

        Settings & settings_push = task_cluster->settings_push;
        settings_push.insert_distributed_timeout = 0;
        settings_push.insert_distributed_sync = 1;

        /// Set up clusters
        context.setClustersConfig(task_cluster_config, task_cluster->clusters_prefix);

        /// Set up shards and their priority
        task_cluster->random_generator.seed(task_cluster->rd());
        for (auto & task_table : task_cluster->table_tasks)
        {
            task_table.cluster_pull = context.getCluster(task_table.cluster_pull_name);
            task_table.cluster_push = context.getCluster(task_table.cluster_push_name);
            task_table.initShards(task_cluster->random_generator);
        }

        LOG_DEBUG(log, "Loaded " << task_cluster->table_tasks.size() << " table tasks");

        /// Compute set of partitions, set of partitions aren't changed
        for (auto & task_table : task_cluster->table_tasks)
        {
            for (const TaskShardPtr & task_shard : task_table.all_shards)
            {
                if (task_shard->info.pool == nullptr)
                {
                    throw Exception("It is impossible to have only local shards, at least port number must be different",
                                    ErrorCodes::LOGICAL_ERROR);
                }

                LOG_DEBUG(log, "Set up table task " << task_table.name_in_config << " ("
                               << "cluster " << task_table.cluster_pull_name
                               << ", table " << getDatabaseDotTable(task_table.table_pull)
                               << ", shard " << task_shard->info.shard_num << ")");

                LOG_DEBUG(log, "There are "
                    << task_table.all_shards.size() << " shards, and "
                    << task_table.local_shards.size() << " remote ones");

                auto connection_entry = task_shard->info.pool->get(&task_cluster->settings_pull);
                LOG_DEBUG(log, "Will get meta information for shard " << task_shard->numberInCluster()
                               << " from replica " << connection_entry->getDescription());

                Strings partitions = getRemotePartitions(task_table.table_pull, *connection_entry, &task_cluster->settings_pull);
                for (const String & partition_name : partitions)
                {
                    /// Do not process partition if it is in enabled_partitions list
                    if (task_table.has_enabled_partitions && !task_table.enabled_partitions.count(partition_name))
                    {
                        LOG_DEBUG(log, "Will skip partition " << partition_name);
                        continue;
                    }

                    task_shard->partitions.emplace(partition_name, TaskPartition(*task_shard, partition_name));
                    task_table.partition_to_shards[partition_name].emplace_back(task_shard);
                }

                LOG_DEBUG(log, "Will fetch " << task_shard->partitions.size() << " partitions");
            }
        }

        auto zookeeper = getZooKeeper();
        zookeeper->createAncestors(getWorkersPath() + "/");
    }

    void process()
    {
        for (TaskTable & task_table : task_cluster->table_tasks)
        {
            if (task_table.all_shards.empty())
                continue;

            /// An optimization: first of all, try to process all partitions of the local shards
//            for (const TaskShardPtr & shard : task_table.local_shards)
//            {
//                for (auto & task_partition : shard->partitions)
//                {
//                    LOG_DEBUG(log, "Processing partition " << task_partition.first << " for local shard " << shard->numberInCluster());
//                    processPartitionTask(task_partition.second);
//                }
//            }

            /// Then check and copy all shards until the whole partition is copied
            for (const auto & partition_with_shards : task_table.partition_to_shards)
            {
                const String & partition_name = partition_with_shards.first;
                const TasksShard & shards_with_partition = partition_with_shards.second;
                bool is_done;

                size_t num_tries = 0;
                constexpr size_t max_tries = 1000;

                Stopwatch watch;

                do
                {
                    LOG_DEBUG(log, "Processing partition " << partition_name << " for the whole cluster"
                        << " (" << shards_with_partition.size() << " shards)");

                    size_t num_successful_shards = 0;

                    for (const TaskShardPtr & shard : shards_with_partition)
                    {
                        auto it_shard_partition = shard->partitions.find(partition_name);
                        if (it_shard_partition == shard->partitions.end())
                            throw Exception("There are no such partition in a shard. This is a bug.", ErrorCodes::LOGICAL_ERROR);

                        TaskPartition & task_shard_partition = it_shard_partition->second;
                        if (processPartitionTask(task_shard_partition))
                            ++num_successful_shards;
                    }

                    try
                    {
                        is_done = (num_successful_shards == shards_with_partition.size())
                            && checkPartitionIsDone(task_table, partition_name, shards_with_partition);
                    }
                    catch (...)
                    {
                        tryLogCurrentException(log);
                        is_done = false;
                    }

                    if (!is_done)
                        std::this_thread::sleep_for(default_sleep_time);

                    ++num_tries;
                } while (!is_done && num_tries < max_tries);

                if (!is_done)
                    throw Exception("Too many retries while copying partition", ErrorCodes::UNFINISHED);
                else
                    LOG_INFO(log, "It took " << watch.elapsedSeconds() << " seconds to copy partition " << partition_name);
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
            TaskPartition & task_shard_partition = shard->partitions.find(partition_name)->second;
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
        return task_cluster->task_zookeeper_path + "/workers";
    }

    String getCurrentWorkerNodePath() const
    {
        return getWorkersPath() + "/" + host_id;
    }

    zkutil::EphemeralNodeHolder::Ptr createWorkerNodeAndWaitIfNeed(const zkutil::ZooKeeperPtr & zookeeper, const String & task_description)
    {
        while (true)
        {
            zkutil::Stat stat;
            zookeeper->get(getWorkersPath(), &stat);

            if (stat.numChildren >= task_cluster->max_workers)
            {
                LOG_DEBUG(log, "Too many workers (" << stat.numChildren << ", maximum " << task_cluster->max_workers << ")"
                    << ". Postpone processing " << task_description);
                std::this_thread::sleep_for(default_sleep_time);
            }
            else
            {
                return std::make_shared<zkutil::EphemeralNodeHolder>(getCurrentWorkerNodePath(), *zookeeper, true, false, task_description);
            }
        }
    }

    std::shared_ptr<ASTCreateQuery> rewriteCreateQueryStorage(const ASTPtr & create_query_pull, const DatabaseAndTableName & new_table,
                                     const ASTPtr & new_storage_ast)
    {
        auto & create = typeid_cast<ASTCreateQuery &>(*create_query_pull);
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

    bool tryDropPartition(TaskPartition & task_partition, const zkutil::ZooKeeperPtr & zookeeper)
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
        size_t num_shards = executeQueryOnCluster(cluster_push, query, nullptr, &settings_push, PoolMode::GET_ALL, 1);

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

    bool processPartitionTask(TaskPartition & task_partition)
    {
        try
        {
            return processPartitionTaskImpl(task_partition);
        }
        catch (...)
        {
            tryLogCurrentException(log, "An error occurred while processing partition " + task_partition.name);
            return false;
        }
    }

    bool processPartitionTaskImpl(TaskPartition & task_partition)
    {
        TaskShard & task_shard = task_partition.task_shard;
        TaskTable & task_table = task_shard.task_table;

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
            query += " WHERE (_part LIKE '" + task_partition.name + "%')";
            if (!task_table.where_condition_str.empty())
                query += " AND (" + task_table.where_condition_str + ")";
            if (!limit.empty())
                query += " LIMIT " + limit;

            ParserQuery p_query(query.data() + query.size());
            return parseQuery(p_query, query);
        };


        /// Load balancing
        auto worker_node_holder = createWorkerNodeAndWaitIfNeed(zookeeper, current_task_status_path);

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

        /// We need to update table definitions for each part, it could be changed after ALTER
        ASTPtr create_query_pull_ast;
        {
            /// Fetch and parse (possibly) new definition
            auto connection_entry = task_shard.info.pool->get(&task_cluster->settings_pull);
            String create_query_pull_str = getRemoteCreateTable(task_table.table_pull, *connection_entry, &task_cluster->settings_pull);

            ParserCreateQuery parser_create_query;
            create_query_pull_ast = parseQuery(parser_create_query, create_query_pull_str);
        }

        /// Create local Distributed tables:
        ///  a table fetching data from current shard and a table inserting data to the whole destination cluster
        DatabaseAndTableName table_shard(working_database_name, ".read_shard." + task_table.name_in_config);
        DatabaseAndTableName table_split(working_database_name, ".split." + task_table.name_in_config);
        {
            /// Create special cluster with single shard
            String shard_read_cluster_name = ".read_shard." + task_table.cluster_pull_name;
            ClusterPtr cluster_pull_current_shard = task_table.cluster_pull->getClusterWithSingleShard(task_shard.indexInCluster());
            context.setCluster(shard_read_cluster_name, cluster_pull_current_shard);

            auto storage_shard_ast = createASTStorageDistributed(shard_read_cluster_name, task_table.table_pull.first, task_table.table_pull.second);
            const auto & storage_split_ast = task_table.engine_split_ast;

            auto create_table_pull_ast = rewriteCreateQueryStorage(create_query_pull_ast, table_shard, storage_shard_ast);
            auto create_table_split_ast = rewriteCreateQueryStorage(create_query_pull_ast, table_split, storage_split_ast);

            //LOG_DEBUG(log, "Create shard reading table. Query: " << queryToString(create_table_pull_ast));
            dropAndCreateLocalTable(create_table_pull_ast);

            //LOG_DEBUG(log, "Create split table. Query: " << queryToString(create_table_split_ast));
            dropAndCreateLocalTable(create_table_split_ast);
        }

        /// Check that destination partition is empty if we are first worker
        /// NOTE: this check is incorrect if pull and push tables have different partition key!
        {
            ASTPtr query_select_ast = get_select_query(table_split, "count()");
            UInt64 count;
            {
                Context local_context = context;
                // Use pull (i.e. readonly) settings, but fetch data from destination servers
                context.getSettingsRef() = task_cluster->settings_pull;
                context.getSettingsRef().skip_unavailable_shards = true;

                InterpreterSelectQuery interperter(query_select_ast, local_context);
                BlockIO io = interperter.execute();

                Block block = getBlockWithAllStreamData(io.in);
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

            auto multi_status = checkNoNodeAndCommit(zookeeper, is_dirty_flag_path, std::move(op_create));

            if (multi_status.code != ZOK)
            {
                if (multi_status.failed_op_index < 0)
                {
                    LOG_INFO(log, "Partition " << task_partition.name << " is dirty and will be dropped and refilled");
                    return false;
                }

                throw zkutil::KeeperException(multi_status.code, current_task_status_path);
            }
        }

        /// Try create table (if not exists) on each shard
        {
            auto create_query_push_ast = rewriteCreateQueryStorage(create_query_pull_ast, task_table.table_push, task_table.engine_push_ast);
            typeid_cast<ASTCreateQuery &>(*create_query_push_ast).if_not_exists = true;
            String query = queryToString(create_query_push_ast);

            LOG_DEBUG(log, "Create remote push tables. Query: " << query);
            executeQueryOnCluster(task_table.cluster_push, query, create_query_push_ast, &task_cluster->settings_push);
        }

        /// Do the copying
        {
            bool inject_fault = false;
            if (copy_fault_probability > 0)
            {
                std::uniform_real_distribution<> dis(0, 1);
                double value = dis(task_table.task_cluster.random_generator);
                inject_fault = value < copy_fault_probability;
            }

            // Select all fields
            ASTPtr query_select_ast = get_select_query(table_shard, "*", inject_fault ? "1" : "");

            LOG_DEBUG(log, "Executing SELECT query: " << queryToString(query_select_ast));

            ASTPtr query_insert_ast;
            {
                String query;
                query += "INSERT INTO " + getDatabaseDotTable(table_split) + " VALUES ";

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
            auto multi_status = checkNoNodeAndCommit(zookeeper, is_dirty_flag_path, std::move(op_set));

            if (multi_status.code != ZOK)
            {
                if (multi_status.failed_op_index < 0)
                    LOG_INFO(log, "Partition " << task_partition.name << " became dirty and will be dropped and refilled");
                else
                    LOG_INFO(log, "Someone made the node abandoned. Will refill partition. " << ::zerror(multi_status.code));

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
        Block block = getBlockWithAllStreamData(std::make_shared<RemoteBlockInputStream>(connection, query, context));
        return block.safeGetByPosition(0).column->getUInt(0) != 0;
    }

    String getRemoteCreateTable(const DatabaseAndTableName & table, Connection & connection, const Settings * settings = nullptr)
    {
        String query = "SHOW CREATE TABLE " + getDatabaseDotTable(table);
        Block block = getBlockWithAllStreamData(std::make_shared<RemoteBlockInputStream>(connection, query, context, settings));

        return typeid_cast<ColumnString &>(*block.safeGetByPosition(0).column).getDataAt(0).toString();
    }

    Strings getRemotePartitions(const DatabaseAndTableName & table, Connection & connection, const Settings * settings = nullptr)
    {
        Block block;
        {
            WriteBufferFromOwnString wb;
            wb << "SELECT DISTINCT partition FROM system.parts WHERE"
               << " database = " << DB::quote << table.first
               << " AND table = " << DB::quote << table.second;

            block = getBlockWithAllStreamData(std::make_shared<RemoteBlockInputStream>(connection, wb.str(), context, settings));
        }

        Strings res;
        if (block)
        {
            ColumnString & partition_col = typeid_cast<ColumnString &>(*block.getByName("partition").column);
            for (size_t i = 0; i < partition_col.size(); ++i)
                res.push_back(partition_col.getDataAt(i).toString());
        }
        else
        {
            if (!existsRemoteTable(table, connection))
            {
                throw Exception("Table " + getDatabaseDotTable(table) + " is not exists on server "
                                + connection.getDescription(), ErrorCodes::UNKNOWN_TABLE);
            }
        }

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

            /// In that case we don't have local replicas, but do it just in case
            for (size_t i = 0; i < shard.getLocalNodeCount(); ++i)
            {
                auto interpreter = InterpreterFactory::get(query_ast, context);
                interpreter->execute();

                if (increment_and_check_exit())
                    return;
            }

            /// Will try to make as many as possible queries
            if (shard.hasRemoteConnections())
            {
                std::vector<IConnectionPool::Entry> connections = shard.pool->getMany(settings, pool_mode);

                for (auto & connection : connections)
                {
                    if (!connection.isNull())
                    {
                        try
                        {
                            RemoteBlockInputStream stream(*connection, query, context, settings);
                            NullBlockOutputStream output;
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
    String host_id;
    String working_database_name;

    bool is_safe_mode = false;
    double copy_fault_probability = 0.0;

    ConfigurationPtr task_cluster_config;
    std::unique_ptr<TaskCluster> task_cluster;

    zkutil::ZooKeeperPtr current_zookeeper;

    Context & context;
    Poco::Logger * log;

    std::chrono::milliseconds default_sleep_time{1000};
};


class ClusterCopierApp : public Poco::Util::ServerApplication
{
public:

    void initialize(Poco::Util::Application & self) override
    {
        Poco::Util::Application::initialize(self);

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

        // process_id is '<hostname>#<pid>_<start_timestamp>'
        process_id = std::to_string(Poco::Process::id()) + "_" + std::to_string(Poco::Timestamp().epochTime());
        host_id = escapeForFileName(getFQDNOrHostName()) + '#' + process_id;
        process_path = Poco::Path(base_dir + "/clickhouse-copier_" + process_id).absolute().toString();
        Poco::File(process_path).createDirectories();

        setupLogging();

        std::string stderr_path = process_path + "/stderr";
        if (!freopen(stderr_path.c_str(), "a+", stderr))
            throw Poco::OpenFileException("Cannot attach stderr to " + stderr_path);
    }

    void handleHelp(const std::string & name, const std::string & value)
    {
        Poco::Util::HelpFormatter helpFormatter(options());
        helpFormatter.setCommand(commandName());
        helpFormatter.setHeader("Copies tables from one cluster to another");
        helpFormatter.setUsage("--config-file <config-file> --task-path <task-path>");
        helpFormatter.format(std::cerr);

        stopOptionsProcessing();
    }

    void defineOptions(Poco::Util::OptionSet & options) override
    {
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

    void setupLogging()
    {
        Poco::AutoPtr<Poco::SplitterChannel> split_channel(new Poco::SplitterChannel);

        Poco::AutoPtr<Poco::FileChannel> log_file_channel(new Poco::FileChannel);
        log_file_channel->setProperty("path", process_path + "/log.log");
        split_channel->addChannel(log_file_channel);
        log_file_channel->open();

        if (!config().getBool("application.runAsService", true))
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

    int main(const std::vector<std::string> & args) override
    {
        if (is_help)
            return 0;

        try
        {
            mainImpl();
        }
        catch (...)
        {
            std::cerr << DB::getCurrentExceptionMessage(true) << "\n";
            auto code = getCurrentExceptionCode();

            return (code) ? code : -1;
        }

        return 0;
    }

    void mainImpl()
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


private:

    std::string config_xml_path;
    std::string task_path;
    std::string log_level = "debug";
    bool is_safe_mode = false;
    double copy_fault_probability = 0;
    bool is_help = false;

    std::string base_dir;
    std::string process_path;
    std::string process_id;
    std::string host_id;
};

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
